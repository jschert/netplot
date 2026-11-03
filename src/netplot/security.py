"""Browser boundary for the local, unauthenticated dashboard."""

import os
from urllib.parse import urlsplit

from starlette.responses import PlainTextResponse

DEFAULT_ALLOWED_HOSTS = ("localhost", "127.0.0.1", "::1")


def _authority(value: str, scheme: str) -> tuple[str, int]:
    if not value or any(c.isspace() or c in "\\/?#@" for c in value):
        raise ValueError("invalid authority")
    parsed = urlsplit(f"{scheme}://{value}")
    if not parsed.hostname or parsed.username is not None:
        raise ValueError("invalid authority")
    return parsed.hostname.lower(), parsed.port if parsed.port is not None else (
        443 if scheme == "https" else 80
    )


class BrowserBoundaryMiddleware:
    """Require an exact allowed host and same-origin browser mutations.

    Requests without Origin remain available to CLI/monitoring clients.
    This is a browser boundary, not authentication for untrusted networks.
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
        if scope["method"] not in {"GET", "HEAD", "OPTIONS"}:
            origins = [
                v.decode("latin-1") for k, v in headers if k.lower() == b"origin"
            ]
            fetch_sites = [v for k, v in headers if k.lower() == b"sec-fetch-site"]
            try:
                if b"cross-site" in fetch_sites or len(origins) > 1:
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
        await self.app(scope, receive, send)
