"""Regression checks for browser boundaries and bounded network work."""

import asyncio

import httpx
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from netplot import influx
from netplot.security import BrowserBoundaryMiddleware, MAX_REQUEST_BODY_BYTES
from netplot.session import MAX_SUBSCRIBERS_PER_SESSION, SubscriberLimitReached
from netplot.settings import DEFAULT_INTERNAL_CIDRS, SETTINGS_FILENAME
from netplot.web import create_app, SSE_CONCURRENCY_LIMIT, THROUGHPUT_CONCURRENCY_LIMIT
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware


@pytest.mark.parametrize(
    "host",
    [
        "attacker.example",
        "localhost.attacker.example",
        "localhost@attacker.example",
        "localhost/path",
    ],
)
def test_untrusted_host_cannot_read_or_write(tmp_path, host):
    with TestClient(create_app(tmp_path), base_url="http://localhost") as client:
        assert client.get("/api/traces", headers={"Host": host}).status_code == 400
        assert (
            client.put(
                "/api/settings/internal", headers={"Host": host}, json={"cidrs": []}
            ).status_code
            == 400
        )


@pytest.mark.parametrize(
    "origin",
    [
        "http://attacker.example",
        "null",
        "https://localhost",
        "http://localhost:9999",
        "http://localhost/path",
    ],
)
def test_foreign_origin_cannot_pause_known_session(tmp_path, origin):
    app = create_app(tmp_path)
    with TestClient(app, base_url="http://localhost") as client:
        session = app.state.session_manager.create("127.0.0.1")
        response = client.post(
            f"/api/traces/{session.id}/pause", headers={"Origin": origin}
        )
        assert response.status_code == 403
        assert session.state == "running"


@pytest.mark.parametrize(
    "base", ["http://localhost", "http://127.0.0.1:8000", "http://[::1]:8000"]
)
def test_local_origin_and_cli_remain_supported(tmp_path, base):
    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(tmp_path)), base_url=base
        ) as client:
            assert (
                await client.put(
                    "/api/settings/internal",
                    headers={"Origin": base},
                    json={"cidrs": []},
                )
            ).status_code == 200
            assert (
                await client.put("/api/settings/internal", json={"cidrs": []})
            ).status_code == 200
            assert (
                await client.put(
                    "/api/settings/internal",
                    headers={"Sec-Fetch-Site": "cross-site"},
                    json={"cidrs": []},
                )
            ).status_code == 403

    asyncio.run(run())


def test_explicit_lan_host(tmp_path, monkeypatch):
    monkeypatch.setenv("NETPLOT_ALLOWED_HOSTS", "netplot.example,::1")
    with TestClient(create_app(tmp_path), base_url="http://netplot.example") as client:
        assert client.get("/healthz").status_code == 200
        assert (
            client.get("/healthz", headers={"Host": "127.0.0.1:8000"}).status_code
            == 200
        )
        assert (
            client.get("/healthz", headers={"Host": "other.example"}).status_code == 400
        )


def test_https_origin_behind_proxy_only_for_trusted_peer(tmp_path, monkeypatch):
    """X-Forwarded-Proto is honored only from the trusted proxy peer.

    Runtime repro of the reverse-proxy deployment gap: the backend speaks
    plain HTTP behind a proxy at a non-loopback address, the browser sends
    an ``https://`` Origin with ``X-Forwarded-Proto: https``, and the Host
    is the externally visible name. Uvicorn's ``ProxyHeadersMiddleware``
    (driven by ``FORWARDED_ALLOW_IPS``) rewrites ``scope["scheme"]`` from
    the forwarded header only when the direct peer is explicitly trusted;
    the browser boundary then compares the Origin scheme against that
    interpreted scheme. A proxy peer outside the narrow default therefore
    keeps GET working but gets every mutation rejected (403) until that
    exact peer is trusted — and untrusted peers must stay rejected.
    """
    monkeypatch.setenv("NETPLOT_ALLOWED_HOSTS", "netplot.example")
    forwarded = {"Origin": "https://netplot.example", "X-Forwarded-Proto": "https"}
    peer = ("203.0.113.7", 51000)

    # The proxy peer is listed in FORWARDED_ALLOW_IPS: the forwarded scheme
    # is interpreted, the HTTPS Origin matches, and mutations succeed.
    trusted = TestClient(
        ProxyHeadersMiddleware(create_app(data_dir=tmp_path), trusted_hosts="203.0.113.7"),
        base_url="http://netplot.example",
        client=peer,
    )
    with trusted:
        assert trusted.get("/healthz").status_code == 200
        assert (
            trusted.put(
                "/api/settings/internal", headers=forwarded, json={"cidrs": []}
            ).status_code
            == 200
        )

    # Same peer, but the trusted list names a different proxy: the scope
    # scheme stays the backend's own HTTP, so GET still works while the
    # HTTPS-Origin mutation is refused — scheme interpretation, not a
    # config-text echo, is what is under test.
    untrusted = TestClient(
        ProxyHeadersMiddleware(create_app(data_dir=tmp_path), trusted_hosts="198.51.100.25"),
        base_url="http://netplot.example",
        client=peer,
    )
    with untrusted:
        assert untrusted.get("/healthz").status_code == 200
        assert (
            untrusted.put(
                "/api/settings/internal", headers=forwarded, json={"cidrs": []}
            ).status_code
            == 403
        )


class SlowStream(httpx.AsyncByteStream):
    closed = False

    async def __aiter__(self):
        while True:
            yield b" "
            await asyncio.sleep(0.01)

    async def aclose(self):
        self.closed = True


def test_total_deadline_closes_trickling_response():
    stream = SlowStream()

    async def run():
        with pytest.raises(influx.InfluxError, match="deadline"):
            await asyncio.wait_for(
                influx.fetch_query(
                    "http://influx",
                    "db",
                    "q",
                    timeout_s=0.06,
                    transport=httpx.MockTransport(
                        lambda r: httpx.Response(200, stream=stream)
                    ),
                ),
                1,
            )
        assert stream.closed

    asyncio.run(run())


@pytest.mark.parametrize(
    "status,headers",
    [(302, {"location": "http://other"}), (200, {"content-encoding": "gzip"})],
)
def test_redirects_and_compression_refused(status, headers):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, headers=headers, stream=SlowStream())

    with pytest.raises(influx.InfluxError):
        asyncio.run(
            influx.fetch_query(
                "http://influx", "db", "q", transport=httpx.MockTransport(handler)
            )
        )
    assert len(calls) == 1


def test_throughput_admission_and_cancellation_release(tmp_path, monkeypatch):
    async def run():
        app = create_app(tmp_path)
        started = 0
        cancelled = 0

        async def slow(*args):
            nonlocal started, cancelled
            started += 1
            try:
                await asyncio.Event().wait()
            finally:
                cancelled += 1

        monkeypatch.setattr(influx, "query_throughput", slow)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        ) as client:
            await client.put(
                "/api/settings/influx",
                json={"url": "http://influx", "database": "db", "interface": "eth0"},
            )
            tasks = [
                asyncio.create_task(client.get("/api/throughput?start=0&end=5"))
                for _ in range(THROUGHPUT_CONCURRENCY_LIMIT)
            ]
            try:
                for _ in range(100):
                    if started == len(tasks):
                        break
                    await asyncio.sleep(0.001)
                assert started == len(tasks)
                rejected = await client.get("/api/throughput?start=0&end=5")
                assert rejected.status_code == 429
                assert rejected.headers["retry-after"] == "2"
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            assert cancelled == len(tasks)
            assert app.state.throughput_semaphore._value == THROUGHPUT_CONCURRENCY_LIMIT

    asyncio.run(run())


def test_sse_admission_and_header_send_failure_release(tmp_path):
    async def run():
        app = create_app(tmp_path)
        manager = app.state.session_manager
        endpoint = next(
            r.endpoint
            for r in app.routes
            if getattr(r, "path", "") == "/api/traces/{session_id}/events"
        )
        sessions = [manager.create("127.0.0.1") for _ in range(5)]
        responses = []
        for session in sessions[:4]:
            responses.extend(
                [await endpoint(session.id) for _ in range(MAX_SUBSCRIBERS_PER_SESSION)]
            )
        assert len(responses) == SSE_CONCURRENCY_LIMIT
        with pytest.raises(HTTPException) as exc:
            await endpoint(sessions[4].id)
        assert exc.value.status_code == 429

        async def receive():
            await asyncio.Event().wait()

        async def send(message):
            raise RuntimeError("disconnected before body")

        scope = {"type": "http", "asgi": {"spec_version": "2.4"}}
        for response in responses:
            with pytest.raises(RuntimeError):
                await response(scope, receive, send)
        assert app.state.sse_semaphore._value == SSE_CONCURRENCY_LIMIT
        assert all(not s._subscribers for s in sessions)
        # Per-session admission also rejects before sending status 200.
        queues = [sessions[0].subscribe() for _ in range(MAX_SUBSCRIBERS_PER_SESSION)]
        with pytest.raises(SubscriberLimitReached):
            sessions[0].subscribe()
        with pytest.raises(HTTPException) as exc:
            await endpoint(sessions[0].id)
        assert exc.value.status_code == 429
        assert app.state.sse_semaphore._value == SSE_CONCURRENCY_LIMIT
        for queue in queues:
            sessions[0].unsubscribe(queue)

    asyncio.run(run())


def test_sse_active_disconnect_releases_admission(tmp_path):
    async def run():
        app = create_app(tmp_path)
        session = app.state.session_manager.create("127.0.0.1")
        endpoint = next(
            r.endpoint
            for r in app.routes
            if getattr(r, "path", "") == "/api/traces/{session_id}/events"
        )
        response = await endpoint(session.id)
        streaming = asyncio.Event()

        async def send(message):
            if message["type"] == "http.response.body":
                streaming.set()

        async def receive():
            await asyncio.Event().wait()

        task = asyncio.create_task(
            response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)
        )
        await asyncio.wait_for(streaming.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not session._subscribers
        assert app.state.sse_semaphore._value == SSE_CONCURRENCY_LIMIT

    asyncio.run(run())


def test_upstream_cancellation_closes_connection():
    async def run():
        stream = SlowStream()
        task = asyncio.create_task(
            influx.fetch_query(
                "http://influx",
                "db",
                "q",
                transport=httpx.MockTransport(
                    lambda r: httpx.Response(200, stream=stream)
                ),
            )
        )
        await asyncio.sleep(0.03)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.closed

    asyncio.run(run())


def test_duplicate_host_and_origin_rejected(tmp_path):
    with TestClient(create_app(tmp_path), base_url="http://localhost") as client:
        assert (
            client.get(
                "/api/traces",
                headers=[("Host", "localhost"), ("Host", "attacker.example")],
            ).status_code
            == 400
        )
        assert (
            client.put(
                "/api/settings/internal",
                json={"cidrs": []},
                headers=[
                    ("Origin", "http://localhost"),
                    ("Origin", "http://attacker.example"),
                ],
            ).status_code
            == 403
        )


def test_cross_site_subresource_read_cannot_trigger_upstream(tmp_path, monkeypatch):
    """A foreign page must not drive server-side upstream work via reads.

    Reproduces the confirmed gap at the safe-method boundary: a
    subresource-style cross-site GET (image / fetch / EventSource metadata,
    no Origin header) used to reach ``/api/throughput`` and make the server
    query the configured InfluxDB. The upstream call is counted directly,
    so the refusal — not a status-code echo — is the invariant under test.
    """
    upstream_calls = []

    async def fake_query(cfg, start, end):
        upstream_calls.append((start, end))
        return {"step_s": 5, "points": []}

    monkeypatch.setattr(influx, "query_throughput", fake_query)
    with TestClient(create_app(tmp_path), base_url="http://localhost") as client:
        configured = client.put(
            "/api/settings/influx",
            json={"url": "http://influx.example", "database": "db", "interface": "eth0"},
        )
        assert configured.status_code == 200

        # <img>/<script>-style no-cors load driven by another page.
        image_style = client.get(
            "/api/throughput?start=0&end=60",
            headers={
                "Sec-Fetch-Site": "cross-site",
                "Sec-Fetch-Mode": "no-cors",
                "Sec-Fetch-Dest": "image",
            },
        )
        assert image_style.status_code == 403
        # fetch()/EventSource-style cross-site read.
        fetch_style = client.get(
            "/api/throughput?start=0&end=60",
            headers={
                "Sec-Fetch-Site": "cross-site",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Dest": "empty",
            },
        )
        assert fetch_style.status_code == 403
        # CORS-style read that does carry a foreign Origin.
        foreign_origin = client.get(
            "/api/throughput?start=0&end=60",
            headers={"Origin": "http://attacker.example"},
        )
        assert foreign_origin.status_code == 403
        # Duplicate Origin smuggling on a read is refused too.
        duplicate_origin = client.get(
            "/api/throughput?start=0&end=60",
            headers=[
                ("Origin", "http://localhost"),
                ("Origin", "http://attacker.example"),
            ],
        )
        assert duplicate_origin.status_code == 403
        assert upstream_calls == []

        # Controls: the dashboard's own same-origin fetch and a CLI GET
        # (no browser metadata) still reach the upstream.
        assert (
            client.get(
                "/api/throughput?start=0&end=60",
                headers={"Origin": "http://localhost"},
            ).status_code
            == 200
        )
        assert client.get("/api/throughput?start=0&end=60").status_code == 200
        assert len(upstream_calls) == 2


def test_same_site_sibling_subresource_read_cannot_trigger_upstream(
    tmp_path, monkeypatch
):
    """A same-site sibling origin is a foreign page too.

    ``Sec-Fetch-Site: same-site`` means a DIFFERENT origin that shares the
    registrable domain (e.g. ``evil.netplot.example`` vs a dashboard on
    ``netplot.example`` under a wildcard DNS zone). Such a page can drive
    no-cors subresource loads that carry no Origin header, so the fetch-site
    gate — not the origin authority check — is the only thing that can keep
    them from driving server-side upstream work. The invariant is the same
    as the cross-site test: the upstream call is counted directly.
    """
    upstream_calls = []

    async def fake_query(cfg, start, end):
        upstream_calls.append((start, end))
        return {"step_s": 5, "points": []}

    monkeypatch.setattr(influx, "query_throughput", fake_query)
    with TestClient(create_app(tmp_path), base_url="http://localhost") as client:
        configured = client.put(
            "/api/settings/influx",
            json={"url": "http://influx.example", "database": "db", "interface": "eth0"},
        )
        assert configured.status_code == 200

        # <img>/<script>-style no-cors load from a sibling origin (no
        # Origin header is sent, so only the fetch-site gate applies).
        image_style = client.get(
            "/api/throughput?start=0&end=60",
            headers={
                "Sec-Fetch-Site": "same-site",
                "Sec-Fetch-Mode": "no-cors",
                "Sec-Fetch-Dest": "image",
            },
        )
        assert image_style.status_code == 403
        # fetch()/EventSource-style read from the sibling origin.
        fetch_style = client.get(
            "/api/throughput?start=0&end=60",
            headers={
                "Sec-Fetch-Site": "same-site",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Dest": "empty",
            },
        )
        assert fetch_style.status_code == 403
        # An unknown fetch-site value fails closed (browsers only emit
        # same-origin/same-site/cross-site/none; anything else is not a
        # browser and can simply omit the header instead).
        unknown_site = client.get(
            "/api/throughput?start=0&end=60",
            headers={"Sec-Fetch-Site": "internal"},
        )
        assert unknown_site.status_code == 403
        # Contradictory duplicate fetch-site values fail closed.
        duplicate_site = client.get(
            "/api/throughput?start=0&end=60",
            headers=[
                ("Sec-Fetch-Site", "same-origin"),
                ("Sec-Fetch-Site", "same-site"),
            ],
        )
        assert duplicate_site.status_code == 403
        assert upstream_calls == []

        # Controls: a same-site top-level navigation (clicked link from the
        # sibling origin) keeps the ordinary-user-action carve-out, and the
        # dashboard's own same-origin / metadata-less reads still work.
        navigation = client.get(
            "/healthz",
            headers={
                "Sec-Fetch-Site": "same-site",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Dest": "document",
            },
        )
        assert navigation.status_code == 200
        assert (
            client.get(
                "/api/throughput?start=0&end=60",
                headers={"Sec-Fetch-Site": "same-origin"},
            ).status_code
            == 200
        )
        assert client.get("/api/throughput?start=0&end=60").status_code == 200
        assert len(upstream_calls) == 2


def test_external_navigation_and_ordinary_reads_still_allowed(tmp_path):
    """Top-level navigation and metadata-less/same-origin reads stay open."""
    with TestClient(create_app(tmp_path), base_url="http://localhost") as client:
        # Followed link / redirect from another site: whole-document load.
        external_nav = client.get(
            "/healthz",
            headers={
                "Sec-Fetch-Site": "cross-site",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Dest": "document",
            },
        )
        assert external_nav.status_code == 200
        # Typed URL / bookmark.
        typed = client.get(
            "/healthz",
            headers={
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Dest": "document",
            },
        )
        assert typed.status_code == 200
        # The dashboard loading its own subresources (fetch, EventSource).
        same_origin_sub = client.get(
            "/healthz",
            headers={
                "Sec-Fetch-Site": "same-origin",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Dest": "empty",
            },
        )
        assert same_origin_sub.status_code == 200
        same_origin_with_origin = client.get(
            "/healthz",
            headers={"Sec-Fetch-Site": "same-origin", "Origin": "http://localhost"},
        )
        assert same_origin_with_origin.status_code == 200
        # CLI / monitoring: no browser metadata at all, any safe method.
        assert client.get("/healthz").status_code == 200
        assert client.head("/static/app.js").status_code == 200


def test_unsafe_cross_site_navigation_claim_still_refused(tmp_path):
    """Claimed navigation metadata must not license cross-site mutations."""
    app = create_app(tmp_path)
    with TestClient(app, base_url="http://localhost") as client:
        session = app.state.session_manager.create("127.0.0.1")
        navigation_headers = {
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Dest": "document",
        }
        with_origin = client.post(
            f"/api/traces/{session.id}/pause",
            headers={**navigation_headers, "Origin": "http://attacker.example"},
        )
        assert with_origin.status_code == 403
        assert session.state == "running"
        without_origin = client.post(
            f"/api/traces/{session.id}/pause", headers=navigation_headers
        )
        assert without_origin.status_code == 403
        assert session.state == "running"
        # Control: the same mutation from the dashboard origin still works.
        same_origin = client.post(
            f"/api/traces/{session.id}/pause",
            headers={"Origin": "http://localhost"},
        )
        assert same_origin.status_code == 200


def test_oversized_declared_body_refused_before_parsing(tmp_path):
    """A Content-Length over the cap is refused before any JSON work.

    The pre-parse receive path (Starlette ``Request.body()``/``json()``)
    accumulates the whole body into memory first; the Pydantic field
    limits and ``normalize_*`` caps only run afterwards. The boundary is
    where the allocation is bounded, so an oversized PUT must get 413
    without the settings ever being replaced — and, like every boundary
    response, it still carries the framing policy.
    """
    with TestClient(create_app(tmp_path), base_url="http://localhost") as client:
        oversized = b'{"cidrs": []}' + b" " * MAX_REQUEST_BODY_BYTES
        response = client.put(
            "/api/settings/internal",
            content=oversized,
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 413
        assert response.headers["x-frame-options"] == "SAMEORIGIN"
        # Refusal happened before parsing: stored settings are untouched.
        assert (
            client.get("/api/settings/internal").json()["cidrs"]
            == list(DEFAULT_INTERNAL_CIDRS)
        )


def test_body_exactly_at_cap_still_parsed(tmp_path):
    """The cap is a bound, not a cliff: exactly-cap bodies parse normally."""
    body = b'{"cidrs": []}'
    body += b" " * (MAX_REQUEST_BODY_BYTES - len(body))  # JSON allows it
    with TestClient(create_app(tmp_path), base_url="http://localhost") as client:
        response = client.put(
            "/api/settings/internal",
            content=body,
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 200
        assert response.json() == {"cidrs": []}


def test_streamed_body_without_content_length_is_bounded(tmp_path):
    """Chunked bodies (no Content-Length) hit the cap during receive.

    Drives the ASGI app directly so the request can omit Content-Length
    entirely (httpx always declares one): the second chunk pushes the
    running total past the cap, the read is aborted, and the visible
    result is 413 — the JSON route handler never runs and nothing is
    persisted. The refusal must survive the framework's body-parser
    error handling: FastAPI rewrites arbitrary receive errors into a
    generic 400 ("There was an error parsing the body") instead of
    letting the boundary's signal through, which would mask the cap
    refusal as an ordinary parse error. The cap signal therefore
    carries the 413 itself, so whichever layer renders it, the client
    sees the refusal, not a parse error.
    """

    async def run():
        app = create_app(tmp_path)
        chunks = []

        async def receive():
            chunks.append(1)
            if len(chunks) == 1:
                return {"type": "http.request", "body": b" " * 4096, "more_body": True}
            return {
                "type": "http.request",
                "body": b" " * MAX_REQUEST_BODY_BYTES,
                "more_body": False,
            }

        captured = {}

        async def send(message):
            if message["type"] == "http.response.start":
                captured["status"] = message["status"]
                captured["headers"] = {
                    name.lower(): value for name, value in message["headers"]
                }
            elif message["type"] == "http.response.body":
                captured["body"] = captured.get("body", b"") + message.get(
                    "body", b""
                )

        scope = {
            "type": "http",
            "asgi": {"spec_version": "2.4"},
            "http_version": "1.1",
            "method": "PUT",
            "scheme": "http",
            "path": "/api/settings/internal",
            "raw_path": b"/api/settings/internal",
            "query_string": b"",
            "root_path": "",
            "headers": [
                (b"host", b"localhost"),
                (b"content-type", b"application/json"),
            ],
            "client": ("127.0.0.1", 50000),
            "server": ("localhost", 80),
        }
        await app(scope, receive, send)
        assert captured["status"] == 413
        assert captured["headers"][b"x-frame-options"] == b"SAMEORIGIN"
        assert len(chunks) == 2  # refused the moment the total crossed the cap
        assert not (tmp_path / SETTINGS_FILENAME).exists()

    asyncio.run(run())


def test_boundary_alone_refuses_oversized_streamed_body():
    """The boundary's own 413 fallback works without FastAPI behind it.

    Inside the app, the framework's exception middleware (behind this
    boundary) renders the refusal; a plain ASGI body-reading app has no
    such layer, so the boundary itself must answer 413. Either way the
    inner reader is stopped at the cap and never sees the oversized
    chunk's bytes.
    """

    async def run():
        reads = []

        async def plain_reader(scope, receive, send):
            while True:
                message = await receive()
                if message["type"] == "http.request":
                    reads.append(len(message.get("body", b"")))
                    if not message.get("more_body", False):
                        return
                else:
                    return

        chunks = []

        async def receive():
            chunks.append(1)
            if len(chunks) == 1:
                return {"type": "http.request", "body": b" " * 4096, "more_body": True}
            return {
                "type": "http.request",
                "body": b" " * MAX_REQUEST_BODY_BYTES,
                "more_body": False,
            }

        captured = {}

        async def send(message):
            if message["type"] == "http.response.start":
                captured["status"] = message["status"]
                captured["headers"] = {
                    name.lower(): value for name, value in message["headers"]
                }
            elif message["type"] == "http.response.body":
                captured["body"] = captured.get("body", b"") + message.get(
                    "body", b""
                )

        boundary = BrowserBoundaryMiddleware(plain_reader)
        scope = {
            "type": "http",
            "asgi": {"spec_version": "2.4"},
            "http_version": "1.1",
            "method": "PUT",
            "scheme": "http",
            "path": "/api/settings/internal",
            "raw_path": b"/api/settings/internal",
            "query_string": b"",
            "root_path": "",
            "headers": [
                (b"host", b"localhost"),
                (b"content-type", b"application/json"),
            ],
            "client": ("127.0.0.1", 50000),
            "server": ("localhost", 80),
        }
        await boundary(scope, receive, send)
        assert captured["status"] == 413
        assert captured["body"] == b"Request body too large"
        assert captured["headers"][b"x-frame-options"] == b"SAMEORIGIN"
        assert len(chunks) == 2  # the oversized chunk was counted, then refused
        assert reads == [4096]  # its bytes were never handed to the reader

    asyncio.run(run())


@pytest.mark.parametrize(
    "content_length_headers",
    [
        [(b"content-length", b"not-a-number")],
        [(b"content-length", b"-5")],
        [(b"content-length", b"17"), (b"content-length", b"999")],
    ],
)
def test_malformed_content_length_refused_before_reading(
    tmp_path, content_length_headers
):
    """A non-digit or self-contradicting Content-Length is 400, unread."""

    async def run():
        app = create_app(tmp_path)
        reads = {"n": 0}

        async def receive():
            reads["n"] += 1
            return {"type": "http.request", "body": b"", "more_body": False}

        captured = {}

        async def send(message):
            if message["type"] == "http.response.start":
                captured["status"] = message["status"]

        scope = {
            "type": "http",
            "asgi": {"spec_version": "2.4"},
            "http_version": "1.1",
            "method": "PUT",
            "scheme": "http",
            "path": "/api/settings/internal",
            "raw_path": b"/api/settings/internal",
            "query_string": b"",
            "root_path": "",
            "headers": [(b"host", b"localhost"), *content_length_headers],
            "client": ("127.0.0.1", 50000),
            "server": ("localhost", 80),
        }
        await app(scope, receive, send)
        assert captured["status"] == 400
        assert reads["n"] == 0  # refused before a single body byte was read

    asyncio.run(run())


def test_framing_policy_response_contract(tmp_path):
    """Every HTTP response forbids cross-origin framing and varies caches on
    the browser metadata that gates permission.

    SAMEORIGIN / ``frame-ancestors 'self'`` (not DENY) is the contract: the
    dashboard must stay embeddable by its own origin while foreign pages
    cannot frame the unauthenticated controls. Headers appear exactly once,
    are unchanged on SSE start messages (separate test), and apply to the
    boundary's own 400/403 responses.
    """
    with TestClient(create_app(tmp_path), base_url="http://localhost") as client:
        for path in ("/", "/metrics", "/healthz", "/static/app.js"):
            response = client.get(path)
            assert response.status_code == 200, path
            assert response.headers.get_list("x-frame-options") == ["SAMEORIGIN"], path
            assert response.headers.get_list("content-security-policy") == [
                "frame-ancestors 'self'"
            ], path
            vary = {
                token.strip().lower()
                for token in response.headers.get_list("vary", split_commas=True)
            }
            assert {
                "origin",
                "sec-fetch-site",
                "sec-fetch-mode",
                "sec-fetch-dest",
            } <= vary, path

        refused = client.get(
            "/healthz",
            headers={"Sec-Fetch-Site": "cross-site", "Sec-Fetch-Dest": "image"},
        )
        assert refused.status_code == 403
        assert refused.headers["x-frame-options"] == "SAMEORIGIN"
        assert refused.headers["content-security-policy"] == "frame-ancestors 'self'"

        bad_host = client.get("/healthz", headers={"Host": "attacker.example"})
        assert bad_host.status_code == 400
        assert bad_host.headers["x-frame-options"] == "SAMEORIGIN"


def test_sse_start_message_carries_framing_policy_and_stream_flows(tmp_path):
    """The framing policy rides the SSE start message without disturbing the
    long-lived stream, and the boundary releases the subscriber on disconnect.
    """

    async def run():
        app = create_app(tmp_path)
        session = app.state.session_manager.create("127.0.0.1")
        captured = {}
        first_body = asyncio.Event()

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            if message["type"] == "http.response.start":
                captured["status"] = message["status"]
                captured["headers"] = message["headers"]
            elif message["type"] == "http.response.body" and message.get("body"):
                captured.setdefault("body", message["body"])
                first_body.set()

        scope = {
            "type": "http",
            "asgi": {"spec_version": "2.4"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": f"/api/traces/{session.id}/events",
            "raw_path": f"/api/traces/{session.id}/events".encode(),
            "query_string": b"",
            "root_path": "",
            "headers": [(b"host", b"localhost")],
            "client": ("127.0.0.1", 50000),
            "server": ("localhost", 80),
        }
        task = asyncio.create_task(app(scope, receive, send))
        await asyncio.wait_for(first_body.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        headers = {name.lower(): value for name, value in captured["headers"]}
        assert captured["status"] == 200
        assert headers[b"content-type"].startswith(b"text/event-stream")
        assert headers[b"x-frame-options"] == b"SAMEORIGIN"
        assert headers[b"content-security-policy"] == b"frame-ancestors 'self'"
        assert b"retry:" in captured["body"]  # stream bytes still flowing
        assert not session._subscribers  # disconnect still unsubscribes

    asyncio.run(run())
