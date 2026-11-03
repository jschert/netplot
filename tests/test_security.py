"""Regression checks for browser boundaries and bounded network work."""

import asyncio

import httpx
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from netplot import influx
from netplot.session import MAX_SUBSCRIBERS_PER_SESSION, SubscriberLimitReached
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
