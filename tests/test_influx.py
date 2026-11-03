"""InfluxDB throughput overlay: pure query/parse logic + the API endpoint."""

from __future__ import annotations

import asyncio
import httpx
import json
import shutil
import ssl
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from fastapi.testclient import TestClient

from netplot import influx
from netplot.influx import (
    MAX_SPAN_S,
    InfluxError,
    build_query,
    parse_points,
    step_for_span,
)
from netplot.web import create_app

# ------------------------------------------------------------- step ladder


def test_step_ladder_native_5s_short_zoom_aggregates_longer() -> None:
    assert step_for_span(60) == 5
    assert step_for_span(600) == 5
    assert step_for_span(1800) == 5
    assert step_for_span(1801) == 60
    assert step_for_span(10800) == 60
    assert step_for_span(21600) == 60
    assert step_for_span(21601) == 300
    assert step_for_span(MAX_SPAN_S) == 300  # 24h -> 288 buckets


# ---------------------------------------------------------- query building


def test_build_query_shape_and_clauses() -> None:
    q = build_query("eth0", "", 10, 1000, 1600)
    assert q == (
        'SELECT non_negative_derivative(last("bytes_recv"), 1s) AS b_in, '
        'non_negative_derivative(last("bytes_sent"), 1s) AS b_out '
        "FROM \"net\" WHERE interface = 'eth0' AND time >= 1000s AND time <= 1600s "
        'GROUP BY time(10s), "interface", "host" fill(none)'
    )


def test_build_query_includes_host_tag_clause_when_present() -> None:
    q = build_query("eth0", "fw1", 60, 0, 600)
    assert "host = 'fw1'" in q
    assert "interface = 'eth0'" in q
    assert 'GROUP BY time(60s), "interface", "host" fill(none)' in q


def test_build_query_groups_by_tags_to_preserve_series() -> None:
    # GROUP BY time() alone would silently MERGE multiple hosts' counters
    # into one series. Grouping by the tag keys keeps series separate so
    # parse_points' multiple-series guard can demand a host tag instead.
    for host in ("", "fw1"):
        for step in (5, 10, 60, 300):
            q = build_query("eth0", host, step, 0, 60)
            assert f'GROUP BY time({step}s), "interface", "host" fill(none)' in q


def test_build_query_escapes_quotes_defensively() -> None:
    # Values are charset-restricted at settings time; the builder escapes
    # again so a bypassed value cannot break out of the literal.
    q = build_query("ig'c0", "a\\b", 10, 0, 10)
    assert "interface = 'ig\\'c0'" in q
    assert "host = 'a\\\\b'" in q


# ------------------------------------------------------------ parse_points


def _influx_response(rows, columns=("time", "b_in", "b_out")) -> dict:
    return {"results": [{"series": [{"columns": list(columns), "values": rows}]}]}


def test_parse_points_converts_bytes_per_s_to_mbps_and_ns_to_seconds() -> None:
    # 125000 bytes/s == 1 Mbps; 1_250_000 bytes/s == 10 Mbps.
    payload = _influx_response([[1700000000_000000000, 125000.0, 1250000.0]])
    points = parse_points(payload)
    assert points == [{"t": 1700000000.0, "in_mbps": 1.0, "out_mbps": 10.0}]


def test_parse_points_preserves_nulls_not_zeros() -> None:
    payload = _influx_response(
        [
            [100_000000000, 125000.0, None],
            [200_000000000, None, None],
        ]
    )
    points = parse_points(payload)
    assert points[0]["out_mbps"] is None
    assert points[1]["in_mbps"] is None and points[1]["out_mbps"] is None


def test_parse_points_empty_results_is_no_data_not_error() -> None:
    assert parse_points({"results": [{}]}) == []
    assert parse_points({"results": [{"series": []}]}) == []


def test_parse_points_error_payload_raises() -> None:
    with pytest.raises(InfluxError):
        parse_points({"results": [{"error": "measurement not found"}]})
    with pytest.raises(InfluxError):
        parse_points({"error": "bad"})


def test_parse_points_multiple_series_is_ambiguous() -> None:
    payload = {
        "results": [
            {
                "series": [
                    {"columns": ["time", "b_in", "b_out"], "values": []},
                    {"columns": ["time", "b_in", "b_out"], "values": []},
                ]
            }
        ]
    }
    with pytest.raises(InfluxError, match="host tag"):
        parse_points(payload)


def test_parse_points_malformed_payloads_raise_influx_error() -> None:
    # Every malformed shape surfaces as InfluxError — never an uncaught
    # exception (which would 500 instead of 502).
    bad_payloads = [
        {"results": [{"series": "nope"}]},  # series not a list
        {"results": [{"series": {"a": 1}}]},
        {"results": [{"series": [42]}]},  # series entry not an object
        {"results": [{"series": [[1, 2, 3]]}]},
        {
            "results": [{"series": [{"columns": "time,b_in", "values": []}]}]
        },  # columns not a list
        {
            "results": [
                {"series": [{"columns": ["time", "b_in", "b_out"], "values": "nope"}]}
            ]
        },
    ]
    for payload in bad_payloads:
        with pytest.raises(InfluxError):
            parse_points(payload)


def test_parse_points_nonfinite_timestamp_raises() -> None:
    with pytest.raises(InfluxError, match="timestamp"):
        parse_points(_influx_response([[float("nan"), 1.0, 1.0]]))


def test_parse_points_negative_bandwidth_raises() -> None:
    # A non-negative derivative can never produce a negative rate; a
    # payload containing one is invalid, not data.
    with pytest.raises(InfluxError, match="negative"):
        parse_points(_influx_response([[0, -1.0, 1.0]]))


# ------------------------------------------------ numeric interpretation


def _simulate_query(samples, query, step_s):
    """Evaluate the built InfluxQL the way InfluxDB 1.x would and return
    the /query JSON payload, so tests pin the NUMERIC meaning of the
    query rather than just its text.

    Semantics modeled: GROUP BY time() buckets aligned to epoch multiples
    of ``step_s``; ``last()`` selector per bucket (value + sample time);
    ``non_negative_derivative`` = (Δvalue / Δtime) × unit with the unit
    parsed from the query, negative deltas dropped; ``fill(none)`` omits
    buckets without samples. ``samples`` is [(t, recv, sent)] ascending.
    """
    import re

    m = re.search(r'non_negative_derivative\(last\("bytes_recv"\), (\d+)s\)', query)
    assert m, f"query does not use last(): {query}"
    unit_s = int(m.group(1))
    assert f"GROUP BY time({step_s}s)" in query

    buckets: dict[int, tuple[float, float, float]] = {}
    for t, recv, sent in samples:
        k = int(t // step_s)
        if k not in buckets or t > buckets[k][0]:
            buckets[k] = (t, recv, sent)

    rows = []
    keys = sorted(buckets)
    for i, k in enumerate(keys):
        t1, r1, s1 = buckets[k]
        row = [k * step_s * 1_000_000_000, None, None]
        if i > 0:
            t0, r0, s0 = buckets[keys[i - 1]]
            dt = t1 - t0
            if dt > 0:
                if r1 >= r0:
                    row[1] = (r1 - r0) / dt * unit_s
                if s1 >= s0:
                    row[2] = (s1 - s0) / dt * unit_s
        rows.append(row)
    return {
        "results": [
            {"series": [{"columns": ["time", "b_in", "b_out"], "values": rows}]}
        ]
    }


@pytest.mark.parametrize("step_s", [5, 10, 60, 300])
def test_derivative_reads_bytes_per_second_at_every_step(step_s) -> None:
    # A counter growing at exactly 1 Mbps (125_000 B/s), sampled at the
    # source's 5 s cadence. Whatever the bucket step, the overlay must
    # report 1.0 Mbps: the derivative unit must be 1 s — a step-sized
    # unit (the old mean(field), step) bug) would inflate the reading
    # 5/10/60/300×.
    rate = 125_000.0  # B/s == 1 Mbps
    samples = [
        (i * 5, 1_000_000 + i * 5 * rate, 2_000_000 + i * 5 * rate) for i in range(124)
    ]
    query = build_query("eth0", "", step_s, samples[0][0], samples[-1][0])
    assert 'non_negative_derivative(last("bytes_recv"), 1s)' in query
    points = parse_points(_simulate_query(samples, query, step_s))
    valued = [p for p in points if p["in_mbps"] is not None]
    assert valued, step_s
    for p in valued:
        assert p["in_mbps"] == pytest.approx(1.0), (step_s, p)
        assert p["out_mbps"] == pytest.approx(1.0), (step_s, p)


# ------------------------------------------------------------- fetch_query


class BytesStream(httpx.AsyncByteStream):
    def __init__(self, body):
        self.body = body

    async def __aiter__(self):
        yield self.body


def test_fetch_query_builds_url_and_parses_json():
    def handler(request):
        assert str(request.url).startswith("http://h:8086/query?")
        assert request.url.params["db"] == "telegraf"
        assert request.url.params["epoch"] == "ns"
        return httpx.Response(200, stream=BytesStream(b'{"results": []}'))

    assert asyncio.run(
        influx.fetch_query(
            "http://h:8086/",
            "telegraf",
            "SELECT 1",
            transport=httpx.MockTransport(handler),
        )
    ) == {"results": []}


def test_fetch_query_unreachable_raises_influx_error():
    def handler(request):
        raise httpx.ConnectError("refused")

    with pytest.raises(InfluxError, match="unreachable"):
        asyncio.run(
            influx.fetch_query(
                "http://h:8086", "db", "q", transport=httpx.MockTransport(handler)
            )
        )


def test_fetch_query_caps_response_bytes():
    def handler(request):
        return httpx.Response(
            200, stream=BytesStream(b"x" * (influx.MAX_RESPONSE_BYTES + 1))
        )

    with pytest.raises(InfluxError, match="too large"):
        asyncio.run(
            influx.fetch_query(
                "http://h:8086", "db", "q", transport=httpx.MockTransport(handler)
            )
        )


def test_fetch_query_invalid_idna_host_is_influx_error_not_httpx_leak():
    """A URL the settings grammar accepts but httpx cannot build a request
    for (IDNA-unencodable host, e.g. the reserved ☃.example) must surface as
    the usual controlled InfluxError — never an httpx.InvalidURL escaping to
    the caller (which would surface as HTTP 500 at the endpoint). The mock
    transport makes the no-external-network guarantee explicit: if the
    request were ever built, it would be answered in-process."""

    def handler(request):
        raise AssertionError("invalid URL must fail before any transport use")

    with pytest.raises(InfluxError, match="invalid"):
        asyncio.run(
            influx.fetch_query(
                "http://☃.example",
                "telegraf",
                "SELECT 1",
                transport=httpx.MockTransport(handler),
            )
        )


# ------------------------------------- fetch_query over real HTTPS (TLS)


def _self_signed_cert(tmp_path, name):
    """Ephemeral self-signed cert + key for ``localhost`` / ``127.0.0.1``
    via the openssl CLI (present on standard Linux/macOS dev and CI hosts).

    Material lives only in ``tmp_path`` — never committed — and each cert
    doubles as its own trust anchor when pointed at by ``SSL_CERT_FILE``.
    """
    cert, key = tmp_path / f"{name}-cert.pem", tmp_path / f"{name}-key.pem"
    config = tmp_path / f"{name}-openssl.cnf"
    config.write_text(
        "[req]\n"
        "distinguished_name = dn\n"
        "x509_extensions = ext\n"
        "prompt = no\n"
        "[dn]\n"
        f"CN = {name}\n"
        "[ext]\n"
        "subjectAltName = DNS:localhost, IP:127.0.0.1\n"
        "basicConstraints = CA:TRUE\n"
    )
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "2",
            "-config",
            str(config),
        ],
        check=True,
        capture_output=True,
        timeout=15,
    )
    return cert, key


class _QuietHTTPServer(HTTPServer):
    """HTTPServer that swallows per-connection errors: the untrusted-cert
    test fails TLS handshakes on purpose, and socketserver would otherwise
    print tracebacks for them."""

    def handle_error(self, request, client_address):
        pass


def _start_https_server(cert, key, captured):
    """TLS-wrapped local HTTP server answering /query with a fixed body."""
    body = json.dumps(
        _influx_response([[1_700_000_000_000000000, 125000.0, 250000.0]])
    ).encode()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            captured["path"] = self.path
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = _QuietHTTPServer(("127.0.0.1", 0), Handler)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(str(cert), str(key))
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


@pytest.mark.integration
def test_fetch_query_https_honors_ssl_cert_file_and_ignores_env_proxies(
    tmp_path, monkeypatch
) -> None:
    """Real-TLS regression for the httpx move: an operator CA exported via
    SSL_CERT_FILE must verify the endpoint (standard-library trust, as a
    plain urllib fetch had), while implicit environment proxies stay
    disabled — the dead HTTPS_PROXY below must never be used."""
    if not shutil.which("openssl"):
        pytest.skip("openssl binary not available on PATH")

    cert, key = _self_signed_cert(tmp_path, "netplot-test-ca")
    monkeypatch.setenv("SSL_CERT_FILE", str(cert))
    # Nothing listens on port 9: honoring implicit proxies would fail the
    # fetch; trust_env=False must connect to the local server directly.
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    monkeypatch.setenv("https_proxy", "http://127.0.0.1:9")
    monkeypatch.setenv("NO_PROXY", "")
    monkeypatch.setenv("no_proxy", "")

    captured: dict = {}
    server = _start_https_server(cert, key, captured)
    try:
        payload = asyncio.run(
            influx.fetch_query(
                f"https://localhost:{server.server_address[1]}",
                "telegraf",
                "SELECT 1",
            )
        )
    finally:
        server.shutdown()
        server.server_close()

    # Successful bounded JSON consumption end to end over verified TLS.
    assert payload == _influx_response(
        [[1_700_000_000_000000000, 125000.0, 250000.0]]
    )
    assert "db=telegraf" in captured["path"]
    assert "epoch=ns" in captured["path"]


@pytest.mark.integration
def test_fetch_query_https_rejects_certificate_outside_trust_store(
    tmp_path, monkeypatch
) -> None:
    """Strict verification is preserved: a server certificate NOT anchored
    in SSL_CERT_FILE (nor in the platform roots) is rejected at TLS before
    any body is consumed — surfaced as the usual InfluxError, never
    silently accepted."""
    if not shutil.which("openssl"):
        pytest.skip("openssl binary not available on PATH")

    anchor, _ = _self_signed_cert(tmp_path, "netplot-trusted-ca")
    cert, key = _self_signed_cert(tmp_path, "netplot-untrusted")  # different key
    monkeypatch.setenv("SSL_CERT_FILE", str(anchor))

    captured: dict = {}
    server = _start_https_server(cert, key, captured)
    try:
        with pytest.raises(InfluxError, match="unreachable"):
            asyncio.run(
                influx.fetch_query(
                    f"https://localhost:{server.server_address[1]}",
                    "telegraf",
                    "SELECT 1",
                )
            )
    finally:
        server.shutdown()
        server.server_close()

    assert "path" not in captured  # rejected at TLS; no HTTP request served


# ------------------------------------------------------------ the endpoint


def _configured_client(tmp_path) -> TestClient:
    client = TestClient(create_app(data_dir=tmp_path), base_url="http://localhost")
    with client:
        put = client.put(
            "/api/settings/influx",
            json={
                "url": "http://influx-host:8086",
                "database": "telegraf",
                "interface": "wan",
            },
        )
        assert put.status_code == 200
    return client


def test_throughput_unconfigured_returns_400(tmp_path) -> None:
    client = TestClient(create_app(data_dir=tmp_path), base_url="http://localhost")
    with client:
        r = client.get("/api/throughput?start=0&end=60")
        assert r.status_code == 400
        assert "not configured" in r.json()["detail"]


def test_throughput_returns_normalized_points(tmp_path, monkeypatch) -> None:
    client = _configured_client(tmp_path)
    fixture = {
        "step_s": 5,
        "points": [{"t": 100.0, "in_mbps": 1.0, "out_mbps": 0.5}],
    }
    captured: dict = {}

    async def fake_query(cfg, start_s, end_s):
        captured["cfg"] = cfg
        captured["span"] = (start_s, end_s)
        return fixture

    monkeypatch.setattr("netplot.web.influx_mod.query_throughput", fake_query)
    with client:
        r = client.get("/api/throughput?start=1000&end=1600")
        assert r.status_code == 200
        body = r.json()
        assert body["start"] == 1000.0
        assert body["end"] == 1600.0
        assert body["step_s"] == 5
        assert body["points"] == fixture["points"]
        assert captured["cfg"]["interface"] == "wan"
        assert captured["cfg"]["database"] == "telegraf"
        assert captured["span"] == (1000.0, 1600.0)


def test_throughput_502_on_influx_error(tmp_path, monkeypatch) -> None:
    client = _configured_client(tmp_path)

    async def boom(cfg, start_s, end_s):
        raise InfluxError("influx unreachable: [Errno 61] Connection refused")

    monkeypatch.setattr("netplot.web.influx_mod.query_throughput", boom)
    with client:
        r = client.get("/api/throughput?start=0&end=60")
        assert r.status_code == 502
        assert "unreachable" in r.json()["detail"]


def test_throughput_invalid_destination_url_is_502_not_500(tmp_path) -> None:
    """End-to-end mapping for the accepted-but-unusable destination: the
    settings PUT accepts the reserved malformed host today (the URL grammar
    is deliberately NOT tightened in this bounded version), and the throughput query
    must turn httpx.InvalidURL into the normal 502 error path instead of an
    unhandled 500. No network is involved — the URL fails at request-build
    time inside the client."""
    client = TestClient(
        create_app(data_dir=tmp_path),
        base_url="http://localhost",
        raise_server_exceptions=False,
    )
    with client:
        put = client.put(
            "/api/settings/influx",
            json={
                "url": "http://☃.example",
                "database": "telegraf",
                "interface": "wan",
            },
        )
        assert put.status_code == 200  # grammar intentionally unchanged
        r = client.get("/api/throughput?start=0&end=60")
        assert r.status_code == 502
        assert "invalid" in r.json()["detail"].lower()


@pytest.mark.parametrize(
    "qs",
    [
        "",  # missing params
        "?start=0",  # missing end
        "?start=abc&end=60",
        "?start=NaN&end=60",
        "?start=Infinity&end=60",
        "?start=-5&end=60",
        "?start=60&end=10",
        f"?start=0&end={MAX_SPAN_S + 2}",
    ],
)
def test_throughput_rejects_malformed_ranges(tmp_path, qs: str) -> None:
    client = _configured_client(tmp_path)
    with client:
        r = client.get(f"/api/throughput{qs}")
        assert r.status_code == 400, qs


def test_query_throughput_end_to_end_against_local_http_server(tmp_path) -> None:
    """Real stdlib-HTTP round trip against a local fake Influx /query."""
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    captured: dict = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            captured["path"] = self.path
            body = json.dumps(
                _influx_response(
                    [
                        [1_700_000_000_000000000, 125000.0, 250000.0],
                        [1_700_000_005_000000000, None, None],
                    ]
                )
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        client = TestClient(create_app(data_dir=tmp_path), base_url="http://localhost")
        with client:
            put = client.put(
                "/api/settings/influx",
                json={
                    "url": f"http://127.0.0.1:{port}",
                    "database": "telegraf",
                    "interface": "wan",
                },
            )
            assert put.status_code == 200
            r = client.get("/api/throughput?start=1700000000&end=1700000600")
            assert r.status_code == 200
            body = r.json()
            assert body["step_s"] == 5
            assert body["points"][0] == {
                "t": 1700000000.0,
                "in_mbps": 1.0,
                "out_mbps": 2.0,
            }
            assert body["points"][1]["in_mbps"] is None  # gap preserved, not zeroed
            assert "db=telegraf" in captured["path"]
            assert "epoch=ns" in captured["path"]
            assert "non_negative_derivative" in captured["path"]
    finally:
        server.shutdown()
