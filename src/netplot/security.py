"""Browser boundary for the local, unauthenticated dashboard."""

import os
from urllib.parse import urlsplit

from fastapi import HTTPException
from starlette.responses import PlainTextResponse

DEFAULT_ALLOWED_HOSTS = ("localhost", "127.0.0.1", "::1")

# Methods whose requests can be top-level document navigations. Everything
# else is a subresource request or an API call from script, and never earns
# the cross-site navigation exception below.
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

# Framing policy: the dashboard may only be framed by its own origin.
# X-Frame-Options covers legacy engines, CSP frame-ancestors modern ones.
# Together they keep cross-origin pages from overlaying netplot's
# unauthenticated destructive controls (reset/close/pause/persist) while
# same-origin embedding keeps working. Deliberately narrow: no HSTS, no
# script/connect/src directives.
FRAME_POLICY_HEADERS = (
    (b"x-frame-options", b"SAMEORIGIN"),
    (b"content-security-policy", b"frame-ancestors 'self'"),
)

# Request headers whose values decide whether a response is permitted now
# that read requests are gated on browser metadata. Shared caches must key
# on them so a response admitted for one context (a top-level navigation)
# is never replayed to a refused one (a cross-site subresource fetch).
VARY_HEADERS = (b"Origin", b"Sec-Fetch-Site", b"Sec-Fetch-Mode", b"Sec-Fetch-Dest")

# Hard cap on any single HTTP request body, enforced by this boundary
# BEFORE the application parses anything. The Pydantic field limits and
# ``normalize_*`` caps in web.py/settings.py are POST-parse: the actual
# receive path (Starlette ``Request.body()``/``json()``) first accumulates
# every ``http.request`` chunk into memory, and neither uvicorn nor the
# application bounds that total, so without this cap any client able to
# reach the server could make the process allocate an arbitrary amount
# of memory before a validator ever runs. Every legitimate netplot body
# is small JSON (the largest is a settings PUT: 64 CIDR strings plus
# short identifiers — a few KiB); 1 MiB leaves two orders of magnitude of
# headroom. Deliberately a fixed constant, not a configuration knob: no
# netplot request has a large body.
MAX_REQUEST_BODY_BYTES = 1_048_576


def _authority(value: str, scheme: str) -> tuple[str, int]:
    if not value or any(c.isspace() or c in "\\/?#@" for c in value):
        raise ValueError("invalid authority")
    parsed = urlsplit(f"{scheme}://{value}")
    if not parsed.hostname or parsed.username is not None:
        raise ValueError("invalid authority")
    return parsed.hostname.lower(), parsed.port if parsed.port is not None else (
        443 if scheme == "https" else 80
    )


def _header_values(headers, name: bytes) -> list[bytes]:
    return [value for key, value in headers if key.lower() == name]


class _RequestBodyTooLarge(HTTPException):
    """Internal signal: the request body exceeded ``MAX_REQUEST_BODY_BYTES``.

    Deliberately a framework ``HTTPException`` (FastAPI's subclass of
    Starlette's) carrying the refusal status in itself. FastAPI's request
    reader guards ``await request.body()``/``json()`` with a broad
    ``except Exception`` that rewrites arbitrary receive errors into a
    generic 400 ``{"detail": "There was an error parsing the body"}``
    while passing ``HTTPException`` through unchanged — and the
    framework's exception middleware sits *inside* this boundary, so it
    renders the exception with its own status before the boundary could
    ever see it. A plain ``Exception`` raised from the wrapped ``receive``
    is therefore masked into 400 instead of 413. Subclassing FastAPI's
    exact class — whose
    instances the app's own routes already raise and render correctly
    with their own status codes — keeps the refusal consumer-visible
    whichever layer renders it. The boundary's ``except
    _RequestBodyTooLarge`` fallback in
    :meth:`BrowserBoundaryMiddleware.__call__` remains for plain ASGI
    apps mounted behind the boundary with no inner exception handling.
    """

    def __init__(self) -> None:
        super().__init__(status_code=413, detail="Request body too large")


def _content_length(headers) -> int | None:
    """Return the declared request body size, or ``None`` when absent.

    Raises ``ValueError`` on a non-digit value or on duplicates that
    disagree (an ASGI server normally rejects these first; the boundary
    re-checks so every server gets the same contract).
    """
    values = _header_values(headers, b"content-length")
    if not values:
        return None
    if len(set(values)) > 1:
        raise ValueError("conflicting content-length")
    value = values[0].strip()
    if not value.isdigit():
        raise ValueError("invalid content-length")
    return int(value)


def _bounded_receive(receive):
    """Wrap ASGI ``receive`` so a streamed body cannot exceed the cap.

    Requests sent with ``Transfer-Encoding`` (or with no length at all)
    carry no usable Content-Length, so the cap is enforced on the fly:
    once the running total passes ``MAX_REQUEST_BODY_BYTES`` every later
    receive raises ``_RequestBodyTooLarge``, an ``HTTPException`` whose
    status is 413, so the refusal reaches the client as 413 whether the
    framework's exception handling (inside the boundary) or the
    boundary's own fallback renders it. Memory held for the request is
    bounded by the cap plus one chunk, whatever the client keeps sending.
    """
    state = {"received": 0, "refused": False}

    async def wrapped():
        if state["refused"]:
            raise _RequestBodyTooLarge()
        message = await receive()
        if message["type"] == "http.request":
            state["received"] += len(message.get("body", b""))
            if state["received"] > MAX_REQUEST_BODY_BYTES:
                state["refused"] = True
                raise _RequestBodyTooLarge()
        return message

    return wrapped


def _boundary_send(send, state=None):
    """Wrap an ASGI ``send`` so every HTTP response states its framing
    policy and cache-variation contract.

    Only the ``http.response.start`` message is extended (streamed bodies
    — SSE, exports — are untouched), existing headers are never replaced,
    and a response that already carries part of the policy keeps its own
    value (only the missing pieces are appended). When ``state`` is a
    dict, ``state["started"]`` is set once the first response starts, so
    the caller knows a response can no longer be replaced.
    """

    async def wrapped(message):
        if message["type"] == "http.response.start":
            if state is not None:
                state["started"] = True
            headers = list(message.get("headers", []))
            present = {name.lower() for name, _ in headers}
            headers.extend(
                policy for policy in FRAME_POLICY_HEADERS if policy[0] not in present
            )
            vary_tokens = {
                token.strip().lower()
                for name, value in headers
                if name.lower() == b"vary"
                for token in value.decode("latin-1").split(",")
                if token.strip()
            }
            missing = [
                token for token in VARY_HEADERS if token.lower() not in vary_tokens
            ]
            if missing:
                headers.append((b"vary", b", ".join(missing)))
            message = {**message, "headers": headers}
        await send(message)

    return wrapped


class BrowserBoundaryMiddleware:
    """Require an exact allowed host and same-origin browser requests.

    Subresource requests (``fetch``/XHR/EventSource/``<img>``/``<script>``
    loads driven by any other page — whether that page is cross-site or a
    same-site sibling origin, i.e. a different origin sharing the
    registrable domain) are refused for every method, with one carve-out:
    a top-level GET/HEAD document *navigation* from another site stays
    allowed, because that is an ordinary user action (typed URL,
    bookmark, clicked link), not embedding — framing is refused separately
    by the response policy. Unsafe methods never earn the carve-out: a
    claimed navigation does not license cross-site mutations.

    Requests without browser metadata (no Origin and no Fetch Metadata
    headers) remain available to CLI/monitoring clients such as curl and
    Prometheus. This is a browser boundary, not authentication for
    untrusted networks.

    The same boundary bounds request *bodies* before parsing: a declared
    Content-Length over :data:`MAX_REQUEST_BODY_BYTES` is refused (413)
    without reading a byte, malformed or self-contradicting lengths are
    refused (400), and streamed bodies are bounded chunk-by-chunk during
    receive so no route — present or future — can be made to allocate an
    unbounded body before its validators run.
    """

    def __init__(self, app, allowed_hosts=None):
        self.app = app
        if allowed_hosts is None:
            allowed_hosts = os.environ.get(
                "NETPLOT_ALLOWED_HOSTS", ",".join(DEFAULT_ALLOWED_HOSTS)
            ).split(",")
        self.allowed_hosts = {
            host.strip().lower() for host in allowed_hosts if host.strip()
        }
        if not self.allowed_hosts or any("*" in host for host in self.allowed_hosts):
            raise ValueError(
                "NETPLOT_ALLOWED_HOSTS requires explicit hostnames or IPs; wildcards are forbidden"
            )
        # Keep the loopback health check working when LAN names are added.
        self.allowed_hosts.update(DEFAULT_ALLOWED_HOSTS)

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        send_state = {"started": False}
        send = _boundary_send(send, send_state)
        headers = scope["headers"]
        hosts = [v.decode("latin-1") for k, v in headers if k.lower() == b"host"]
        try:
            if len(hosts) != 1:
                raise ValueError("one Host required")
            authority = _authority(hosts[0], scope["scheme"])
            if authority[0] not in self.allowed_hosts:
                raise ValueError("untrusted host")
        except ValueError:
            return await PlainTextResponse("Invalid Host", status_code=400)(
                scope, receive, send
            )
        origins = [v.decode("latin-1") for k, v in headers if k.lower() == b"origin"]
        # Only ``same-origin`` (the dashboard loading its own
        # subresources) and ``none`` (a typed-URL/bookmark navigation)
        # describe a non-foreign request. ``same-site`` — a different
        # origin that merely shares the registrable domain, e.g. a
        # sibling subdomain under a wildcard DNS zone — is as foreign as
        # ``cross-site`` for this single-origin dashboard: a page there
        # can drive no-cors subresource loads (<img>/<script>) that carry
        # no Origin header for the authority check below to reject. Any
        # other/unknown value fails closed the same way; metadata-less
        # clients (curl, Prometheus) send no header at all and stay on
        # the CLI path below.
        fetch_sites = _header_values(headers, b"sec-fetch-site")
        cross_site = any(site not in (b"same-origin", b"none") for site in fetch_sites)
        # A top-level navigation loads a whole document (Mode: navigate,
        # Dest: document); every other cross-site request is a subresource
        # or scripted read from a foreign page.
        top_level_document = (
            b"navigate" in _header_values(headers, b"sec-fetch-mode")
            and b"document" in _header_values(headers, b"sec-fetch-dest")
        )
        try:
            if len(origins) > 1:
                raise ValueError("duplicate origin")
            if cross_site and not (
                top_level_document and scope["method"] in SAFE_METHODS
            ):
                raise ValueError("cross-site request")
            if origins:
                origin = urlsplit(origins[0])
                if (
                    origin.scheme != scope["scheme"]
                    or origin.path
                    or origin.query
                    or origin.fragment
                    or _authority(origin.netloc, origin.scheme) != authority
                ):
                    raise ValueError("untrusted origin")
        except ValueError:
            return await PlainTextResponse("Untrusted Origin", status_code=403)(
                scope, receive, send
            )
        # Body-size admission BEFORE parsing. An oversized declared
        # Content-Length is refused without reading a byte (the client may
        # be told 413 while its upload is still in flight); streamed
        # bodies, which carry no usable length, are bounded on receive.
        if not _header_values(headers, b"transfer-encoding"):
            try:
                declared = _content_length(headers)
            except ValueError:
                return await PlainTextResponse(
                    "Invalid Content-Length", status_code=400
                )(scope, receive, send)
            if declared is not None and declared > MAX_REQUEST_BODY_BYTES:
                return await PlainTextResponse(
                    "Request body too large", status_code=413
                )(scope, receive, send)
        try:
            await self.app(scope, _bounded_receive(receive), send)
        except _RequestBodyTooLarge:
            if send_state["started"]:
                # A response already began; never layer a second one on
                # top of the ASGI protocol.
                raise
            # Reached only when nothing inside rendered the refusal —
            # a FastAPI app's exception middleware (which sits behind
            # this boundary) has already answered 413 by then; this
            # fallback covers plain ASGI apps mounted directly behind
            # the boundary with no exception handling of their own.
            await PlainTextResponse("Request body too large", status_code=413)(
                scope, receive, send
            )
