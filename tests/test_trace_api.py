"""API tests for the trace CRUD/snapshot/SSE endpoints."""

from __future__ import annotations

import asyncio
import shutil
import time
from collections import deque

import pytest
from fastapi.testclient import TestClient

from netplot.session import Hop, Sample
from netplot.web import create_app

# ---------------------------------------------------------- create + listing


def test_post_trace_creates_running_session() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        r = client.post("/api/traces", json={"target": "127.0.0.1", "interval_s": 2.5})
        assert r.status_code == 201
        body = r.json()
        assert set(body.keys()) == {
            "id",
            "target",
            "interval_s",
            "state",
            "name",
            "persistent",
        }
        assert body["target"] == "127.0.0.1"
        assert body["interval_s"] == 2.5
        assert body["state"] == "running"
        assert body["name"] is None
        assert body["persistent"] is False
        assert isinstance(body["id"], str) and len(body["id"]) > 0


def test_post_trace_defaults_interval_to_2_5() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        r = client.post("/api/traces", json={"target": "example.com"})
        assert r.status_code == 201
        assert r.json()["interval_s"] == 2.5


def test_post_trace_422_on_flag_target() -> None:
    client = TestClient(create_app(), base_url="http://localhost", raise_server_exceptions=False)
    r = client.post("/api/traces", json={"target": "-n", "interval_s": 2.5})
    assert r.status_code == 422


def test_post_trace_422_on_shell_metachar_target() -> None:
    client = TestClient(create_app(), base_url="http://localhost")
    r = client.post("/api/traces", json={"target": "host; rm -rf /", "interval_s": 2.5})
    assert r.status_code == 422


def test_post_trace_422_on_short_interval() -> None:
    client = TestClient(create_app(), base_url="http://localhost")
    r = client.post("/api/traces", json={"target": "127.0.0.1", "interval_s": 0.5})
    assert r.status_code == 422


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
def test_post_trace_422_on_non_finite_interval(literal: str) -> None:
    client = TestClient(create_app(), base_url="http://localhost", raise_server_exceptions=False)
    # JSON clients reject non-standard non-finite values before the request is
    # sent, so exercise the server parser with the wire representation.
    body = f'{{"target":"127.0.0.1","interval_s":{literal}}}'
    r = client.post(
        "/api/traces", content=body, headers={"content-type": "application/json"}
    )
    assert r.status_code == 422


def test_post_trace_422_on_missing_target() -> None:
    client = TestClient(create_app(), base_url="http://localhost")
    r = client.post("/api/traces", json={"interval_s": 2.5})
    assert r.status_code == 422


def test_list_traces_returns_summary_shape() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        client.post("/api/traces", json={"target": "127.0.0.1", "interval_s": 2.0})
        client.post("/api/traces", json={"target": "::1", "interval_s": 3.0})

        r = client.get("/api/traces")
        assert r.status_code == 200
        body = r.json()
        assert len(body) == 2
        for item in body:
            assert set(item.keys()) == {
                "id",
                "target",
                "interval_s",
                "state",
                "created_at",
                "paused_at",
                "sample_count",
                "name",
                "persistent",
            }
        targets = sorted(item["target"] for item in body)
        assert targets == ["127.0.0.1", "::1"]


# ----------------------------------------------- multi-trace cap (P1 tabs)
#
# PingPlotter-style tabs bound the registry at MAX_SESSIONS (8) so a single
# client cannot squat unlimited probe loops. POST /api/traces beyond the cap
# returns 409 with a clear detail; the dashboard surfaces it in the status
# line when "+" or Start exceeds it.


def test_post_trace_409_when_session_cap_reached() -> None:
    """POST /api/traces returns 409 once MAX_SESSIONS sessions are live.

    Each app instance has its own SessionManager so the cap is per-app; we
    fill it with one client and assert the next POST is rejected with a
    descriptive detail. Closing a session frees a slot (recovery).
    """
    from netplot.session import MAX_SESSIONS

    with TestClient(create_app(), base_url="http://localhost") as client:
        for i in range(MAX_SESSIONS):
            r = client.post(
                "/api/traces",
                json={"target": "127.0.0.1", "interval_s": 60.0},
            )
            assert r.status_code == 201, f"setup create #{i} failed: {r.text}"

        # Cap reached: the next POST must be rejected with 409.
        overflow = client.post(
            "/api/traces", json={"target": "127.0.0.1", "interval_s": 60.0}
        )
        assert overflow.status_code == 409
        detail = overflow.json().get("detail", "")
        assert "cap" in detail.lower(), f"detail should mention cap, got: {detail!r}"
        assert str(MAX_SESSIONS) in detail, (
            f"detail should include cap value: {detail!r}"
        )

        # Listing reflects exactly MAX_SESSIONS (no leak past the cap).
        body = client.get("/api/traces").json()
        assert len(body) == MAX_SESSIONS

        # Recovery: delete one session and the next create succeeds.
        first_id = body[0]["id"]
        assert client.delete(f"/api/traces/{first_id}").status_code == 204
        again = client.post(
            "/api/traces", json={"target": "127.0.0.1", "interval_s": 60.0}
        )
        assert again.status_code == 201


# ---------------------------------------------------------- pause + resume
#
# Fixture isolation: POST /api/traces starts the REAL probe loop (the first
# round fires before the first interval sleep) and the REAL discovery task
# the moment it returns. On a fast Linux loopback a genuine ~0.05 ms ping
# sample — and a ready route with hop rows from loopback traceroute — can
# land in the server buffers before this test process issues its pause
# request, so data-controlled fixtures then see samples/hops they never
# injected. SessionManager takes its network boundaries as constructor-
# injected functions; rebinding those same instance attributes BEFORE the
# session is created swaps the boundary for cancellation-safe blocking
# stand-ins while the manager lifecycle (create/start/pause/resume/cancel)
# and every endpoint under test keep running production code. The marked
# integration tests near the end of this file bypass this helper and still
# exercise the real ping/traceroute binaries.


async def _blocked_probe(_host: str, _timeout_s: float) -> float | None:
    """Probe stand-in that never completes a round, so no sample can land.

    The sleep propagates CancelledError, so the real pause/shutdown task
    cancellation path stays exercised.
    """
    await asyncio.sleep(3600)
    return None


async def _blocked_discover(_target: str, _timeout_s: float = 90.0) -> list:
    """Discovery stand-in that never resolves, so ``route_state`` stays
    ``pending`` and no discovered-hop rows appear (target-only semantics)."""
    await asyncio.sleep(3600)
    return []


async def _fast_resolve_name(_ip: str) -> None:
    """Reverse-DNS stand-in: no resolver call (test_session.py convention)."""
    return None


async def _fast_resolve_target(target: str) -> tuple[str | None, str | None]:
    """Forward-resolution stand-in: no ``getaddrinfo`` call, no suspension.

    IP literals resolve to themselves; labels mutated by export tests use a
    documentation address. Neither path suspends or performs real DNS.
    """
    from netplot.session import _is_ip_literal

    return (target if _is_ip_literal(target) else "192.0.2.1"), None


def _control_network_boundaries(client: TestClient) -> None:
    """Bind the app's manager to the stand-ins above before any session

    starts. These are the same attributes the SessionManager constructor
    injects (probe_fn / discover_fn / resolve_name_fn), plus the
    ``_resolve_target`` boundary the discovery task awaits; the app's
    routes close over this very manager instance, so sessions created
    afterwards probe/discover/resolve through the controlled boundaries.
    Forward resolution is stubbed as well: not every session keeps an IP
    literal target (tests may mutate ``session.target`` post-create), and
    the production resolver would otherwise hit real DNS from inside the
    discovery task.
    """
    manager = client.app.state.session_manager
    manager._probe_fn = _blocked_probe
    manager._discover_fn = _blocked_discover
    manager._resolve_name_fn = _fast_resolve_name
    manager._resolve_target = _fast_resolve_target


def _create_then(
    client: TestClient, target: str = "127.0.0.1", interval_s: float = 2.0
):
    # Swap the network boundary BEFORE the POST so the session starts with
    # deterministic empty buffers; the lifecycle itself stays real.
    _control_network_boundaries(client)
    r = client.post("/api/traces", json={"target": target, "interval_s": interval_s})
    assert r.status_code == 201
    return r.json()["id"]


def test_pause_sets_state_and_returns_summary() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        r = client.post(f"/api/traces/{sid}/pause")
        assert r.status_code == 200
        body = r.json()
        assert body["state"] == "paused"
        assert body["id"] == sid


def test_pause_returns_404_for_unknown_session() -> None:
    client = TestClient(create_app(), base_url="http://localhost")
    r = client.post("/api/traces/deadbeef/pause")
    assert r.status_code == 404


def test_pause_returns_409_when_already_paused() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        r1 = client.post(f"/api/traces/{sid}/pause")
        assert r1.status_code == 200
        r2 = client.post(f"/api/traces/{sid}/pause")
        assert r2.status_code == 409


def test_resume_sets_state_back_to_running() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        client.post(f"/api/traces/{sid}/pause")
        r = client.post(f"/api/traces/{sid}/resume")
        assert r.status_code == 200
        assert r.json()["state"] == "running"


def test_resume_returns_404_for_unknown_session() -> None:
    client = TestClient(create_app(), base_url="http://localhost")
    r = client.post("/api/traces/deadbeef/resume")
    assert r.status_code == 404


def test_resume_returns_409_when_already_running() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        # Session is already running after creation.
        r = client.post(f"/api/traces/{sid}/resume")
        assert r.status_code == 409


# ------------------------------------------------------------- paused_at API
#
# The pause endpoint records the authoritative pause epoch on the session
# and surfaces it in its summary response, the list endpoint, and every
# snapshot — so any client (including one that never saw the pause happen)
# can anchor its paused-window freeze to the same server instant. Resume
# clears it.


def test_pause_response_carries_paused_at() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        before = time.time()
        r = client.post(f"/api/traces/{sid}/pause")
        after = time.time()
        assert r.status_code == 200
        body = r.json()
        assert body["state"] == "paused"
        assert isinstance(body["paused_at"], float)
        # The recorded epoch is the server's pause instant, not null.
        assert before <= body["paused_at"] <= after


def test_snapshot_carries_paused_at_only_while_paused() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        # Running: no pause instant.
        snap = client.get(f"/api/traces/{sid}/snapshot").json()
        assert snap["state"] == "running"
        assert snap["paused_at"] is None

        client.post(f"/api/traces/{sid}/pause")
        snap = client.get(f"/api/traces/{sid}/snapshot").json()
        assert snap["state"] == "paused"
        assert isinstance(snap["paused_at"], float)

        client.post(f"/api/traces/{sid}/resume")
        snap = client.get(f"/api/traces/{sid}/snapshot").json()
        assert snap["state"] == "running"
        assert snap["paused_at"] is None


def test_list_summary_carries_paused_at() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        client.post(f"/api/traces/{sid}/pause")
        items = client.get("/api/traces").json()
        match = next(s for s in items if s["id"] == sid)
        assert match["state"] == "paused"
        assert isinstance(match["paused_at"], float)


# ----------------------------------------------------------------- delete


def test_delete_removes_session() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        r = client.delete(f"/api/traces/{sid}")
        assert r.status_code == 204
        assert r.text == ""
        # Subsequent access returns 404.
        assert client.get(f"/api/traces/{sid}/snapshot").status_code == 404
        # And listing no longer includes it.
        assert all(s["id"] != sid for s in client.get("/api/traces").json())


def test_delete_returns_404_for_unknown_session() -> None:
    client = TestClient(create_app(), base_url="http://localhost")
    r = client.delete("/api/traces/deadbeef")
    assert r.status_code == 404


# --------------------------------------------------------------- snapshot


def test_snapshot_returns_404_for_unknown_session() -> None:
    client = TestClient(create_app(), base_url="http://localhost")
    r = client.get("/api/traces/deadbeef/snapshot")
    assert r.status_code == 404


@pytest.mark.parametrize("hop", [None, 1])
@pytest.mark.parametrize("focus", [0, 60])
def test_snapshot_reads_live_buffers_on_probe_event_loop(
    hop: int | None, focus: int
) -> None:
    """A real deque rejects off-loop reads, without relying on race timing."""
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        client.post(f"/api/traces/{sid}/pause")
        session = client.app.state.session_manager.get(sid)

        async def install_loop_bound_buffers() -> None:
            probe_loop = asyncio.get_running_loop()

            class LoopBoundDeque(deque):
                def __iter__(self):
                    assert asyncio.get_running_loop() is probe_loop
                    return super().__iter__()

            session.samples = LoopBoundDeque([Sample(t=time.time(), rtt_ms=7.0)])
            route_hop = Hop(index=1, ip="10.0.0.1", dns="router.lan")
            route_hop.samples = LoopBoundDeque([Sample(t=time.time(), rtt_ms=9.0)])
            session.hops = [route_hop]

        client.portal.call(install_loop_bound_buffers)
        params = {"focus": focus}
        if hop is not None:
            params["hop"] = hop
        response = client.get(f"/api/traces/{sid}/snapshot", params=params)
        assert response.status_code == 200
        body = response.json()
        assert body["stats"]["count"] == 1
        assert body["stats"]["avg_ms"] == (7.0 if hop is None else 9.0)
        if hop is None:
            assert body["hops"][0]["stats"]["avg_ms"] == 9.0
            assert body["hops"][0]["internal"] is True
        else:
            assert body["internal"] is True


def test_snapshot_payload_shape_for_empty_session() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        client.post(f"/api/traces/{sid}/pause")  # stop the probe loop

        r = client.get(f"/api/traces/{sid}/snapshot")
        assert r.status_code == 200
        body = r.json()
        assert set(body.keys()) == {
            "id",
            "target",
            "target_ip",
            "target_dns",
            "target_internal",
            "state",
            "paused_at",
            "route_state",
            "focus_s",
            "name",
            "persistent",
            "hops",
            "stats",
            "samples",
        }
        assert body["focus_s"] == 600.0  # default
        assert body["stats"]["count"] == 0
        assert body["stats"]["avg_ms"] is None
        assert body["samples"] == []
        assert body["hops"] == []
        # route_state is one of pending/ready/failed. After an immediate pause
        # discovery may have completed or not — either is acceptable; the
        # important contract is that the key exists with a known value.
        assert body["route_state"] in {"pending", "ready", "failed"}


def test_snapshot_focus_query_param_overrides_default() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        client.post(f"/api/traces/{sid}/pause")

        r = client.get(f"/api/traces/{sid}/snapshot?focus=0")
        assert r.status_code == 200
        assert r.json()["focus_s"] == 0.0

        r = client.get(f"/api/traces/{sid}/snapshot?focus=30")
        assert r.status_code == 200
        assert r.json()["focus_s"] == 30.0


@pytest.mark.parametrize("focus", ["-1", "nan", "inf", "-inf"])
def test_snapshot_rejects_invalid_focus(focus: str) -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        client.post(f"/api/traces/{sid}/pause")
        assert (
            client.get(f"/api/traces/{sid}/snapshot?focus={focus}").status_code == 422
        )


def test_snapshot_reflects_injected_samples() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        app = client.app
        session = app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        session.append_sample(5.0, t=time.time() - 30)
        session.append_sample(10.0, t=time.time() - 10)
        session.append_sample(15.0, t=time.time() - 1)

        body = client.get(f"/api/traces/{sid}/snapshot?focus=60").json()
        assert body["stats"]["count"] == 3
        assert body["stats"]["avg_ms"] == pytest.approx(10.0)
        assert body["stats"]["min_ms"] == pytest.approx(5.0)
        assert body["stats"]["cur_ms"] == pytest.approx(15.0)
        assert len(body["samples"]) == 3


# --------------------------------------------------------------- history (D1)
#
# GET /api/traces/{id}/history?hop=<index>&start=<epoch_s>&end=<epoch_s>
# returns the server's retained raw sample history for one stream, clamped to
# [start, end]. hop=0 is the target stream; hop>=1 must match a discovered
# route hop. earliest_t lets clients distinguish "no data" from "older than
# retention". Malformed params → 400; unknown hop/session → 404; resolution
# defaults to "raw" and is the only supported value (the envelope carries it
# so D2 can add bucketed resolution without breaking D1 clients). Works for
# running and paused sessions (server buffers are authoritative).


def test_history_returns_404_for_unknown_session() -> None:
    client = TestClient(create_app(), base_url="http://localhost")
    r = client.get("/api/traces/deadbeef/history?hop=0&start=0&end=9999")
    assert r.status_code == 404


def test_history_payload_shape_and_clamping() -> None:
    """hop=0 returns the target stream, clamped to [start, end], ascending."""
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        session.target_ip = "127.0.0.1"
        now = time.time()
        # Inject samples spanning 3 windows: [old, mid, recent].
        session.append_sample(1.0, t=now - 500)
        session.append_sample(2.0, t=now - 100)
        session.append_sample(None, t=now - 10)  # loss
        session.append_sample(3.0, t=now - 1)

        # Full retained window.
        r = client.get(f"/api/traces/{sid}/history?hop=0&start=0&end={now + 10}")
        assert r.status_code == 200
        body = r.json()
        assert set(body.keys()) == {"id", "hop", "resolution", "earliest_t", "samples"}
        assert body["id"] == sid
        assert body["hop"] == 0
        assert body["resolution"] == "raw"
        assert body["earliest_t"] == pytest.approx(now - 500)
        assert len(body["samples"]) == 4
        # Ascending by t.
        ts = [s["t"] for s in body["samples"]]
        assert ts == sorted(ts)
        # Loss sample carries null rtt_ms.
        assert any(s["rtt_ms"] is None for s in body["samples"])
        # IP/DNS are NOT part of the history envelope (they ride the snapshot).


def test_history_clamps_to_start_end_window() -> None:
    """samples outside [start, end] are excluded; earliest_t is unaffected."""
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        now = time.time()
        session.append_sample(1.0, t=now - 500)
        session.append_sample(2.0, t=now - 100)
        session.append_sample(3.0, t=now - 10)

        # Window covering only the last two samples.
        start = now - 150
        end = now
        r = client.get(f"/api/traces/{sid}/history?hop=0&start={start}&end={end}")
        assert r.status_code == 200
        body = r.json()
        assert len(body["samples"]) == 2
        # earliest_t is the earliest retained timestamp, NOT clamped to the
        # query window.
        assert body["earliest_t"] == pytest.approx(now - 500)
        # All returned samples fall within [start, end].
        for s in body["samples"]:
            assert start - 1 <= s["t"] <= end + 1


def test_history_sorts_out_of_order_retained_samples() -> None:
    """The wire contract remains timestamp-ascending even for odd writes."""
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        session.append_sample(3.0, t=300.0)
        session.append_sample(1.0, t=100.0)
        session.append_sample(2.0, t=200.0)

        body = client.get(f"/api/traces/{sid}/history?hop=0&start=0&end=400").json()
        assert [sample["t"] for sample in body["samples"]] == [100.0, 200.0, 300.0]
        assert body["earliest_t"] == 100.0


def test_history_empty_stream_returns_null_earliest_and_empty_samples() -> None:
    """Brand-new session: earliest_t null, samples [], 200 (not 404)."""
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        client.post(f"/api/traces/{sid}/pause")
        now = time.time()
        r = client.get(f"/api/traces/{sid}/history?hop=0&start=0&end={now + 10}")
        assert r.status_code == 200
        body = r.json()
        assert body["earliest_t"] is None
        assert body["samples"] == []


def test_history_hop_param_returns_per_hop_stream() -> None:
    """hop>=1 returns the matching route hop's stream."""
    from netplot.session import Hop
    from netplot.session import Sample as NpSample

    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        session.hops = [Hop(index=1, ip="10.0.0.1", dns="router")]
        session.route_state = "ready"
        now = time.time()
        session.hops[0].samples.append(NpSample(t=now, rtt_ms=5.0))
        session.hops[0].samples.append(NpSample(t=now + 1, rtt_ms=None))

        r = client.get(f"/api/traces/{sid}/history?hop=1&start=0&end={now + 10}")
        assert r.status_code == 200
        body = r.json()
        assert body["hop"] == 1
        assert len(body["samples"]) == 2
        assert body["samples"][0]["rtt_ms"] == 5.0
        assert body["samples"][1]["rtt_ms"] is None


def test_history_unknown_hop_returns_404() -> None:
    """hop>=1 that doesn't match a discovered route hop → 404."""
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        client.post(f"/api/traces/{sid}/pause")
        # Route pending → no hops → hop=1 is unknown.
        r = client.get(f"/api/traces/{sid}/history?hop=1&start=0&end=9999")
        assert r.status_code == 404
        # Out-of-range hop on a ready route.
        session = client.app.state.session_manager.get(sid)
        from netplot.session import Hop

        session.hops = [Hop(index=1, ip="10.0.0.1", dns=None)]
        session.route_state = "ready"
        r = client.get(f"/api/traces/{sid}/history?hop=99&start=0&end=9999")
        assert r.status_code == 404


def test_history_target_covered_hop0_and_final_hop_share_stream() -> None:
    """When the final hop IS the target (shared deque), hop=0 and hop=final
    return identical samples (the same underlying stream)."""
    from netplot.session import Hop
    from netplot.session import Sample as NpSample

    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        h = Hop(index=1, ip="127.0.0.1", dns="localhost")
        h.samples = session.samples  # shared reference → target covered
        session.hops = [h]
        session.route_state = "ready"
        session.target_ip = "127.0.0.1"
        now = time.time()
        session.samples.append(NpSample(t=now, rtt_ms=1.5))

        r0 = client.get(
            f"/api/traces/{sid}/history?hop=0&start=0&end={now + 10}"
        ).json()
        r1 = client.get(
            f"/api/traces/{sid}/history?hop=1&start=0&end={now + 10}"
        ).json()
        assert r0["samples"] == r1["samples"]
        assert r0["earliest_t"] == r1["earliest_t"]


@pytest.mark.parametrize(
    "query,detail_substr",
    [
        ("hop=0&end=100", "missing"),  # missing start
        ("hop=0&start=100", "missing"),  # missing end
        ("start=0&end=100", "missing"),  # missing hop
        ("hop=abc&start=0&end=100", "hop must be an integer"),
        ("hop=-1&start=0&end=100", "hop must be >= 0"),
        ("hop=0&start=abc&end=100", "epoch seconds"),
        ("hop=0&start=0&end=abc", "epoch seconds"),
        ("hop=0&start=200&end=100", "start must be <= end"),
        ("hop=0&start=-5&end=100", "must be >= 0"),
        ("hop=0&start=0&end=-5", "must be >= 0"),
    ],
)
def test_history_malformed_params_return_400(query: str, detail_substr: str) -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        r = client.get(f"/api/traces/{sid}/history?{query}")
        assert r.status_code == 400, f"{query} -> {r.status_code} {r.text}"
        assert detail_substr.lower() in r.json()["detail"].lower()


def test_history_non_finite_params_return_400() -> None:
    """NaN/Infinity are parsed by float() but rejected by the finite check."""
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        r = client.get(f"/api/traces/{sid}/history?hop=0&start=NaN&end=100")
        assert r.status_code == 400
        assert "finite" in r.json()["detail"].lower()


def test_history_unsupported_resolution_returns_400() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        r = client.get(
            f"/api/traces/{sid}/history?hop=0&start=0&end=9999&resolution=bucketed"
        )
        assert r.status_code == 400
        assert "resolution" in r.json()["detail"].lower()
        # Default resolution is "raw" and works.
        r2 = client.get(f"/api/traces/{sid}/history?hop=0&start=0&end=9999")
        assert r2.status_code == 200
        assert r2.json()["resolution"] == "raw"


def test_history_works_while_paused_and_while_running() -> None:
    """Server buffers are authoritative regardless of probe-loop state."""
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        # Pause FIRST so no background probe round can land between the
        # manual injects and the history read (the probe loop is cancelled).
        client.post(f"/api/traces/{sid}/pause")
        now = time.time()
        session.append_sample(1.0, t=now)
        session.append_sample(2.0, t=now + 1)

        # Paused — server buffers still served.
        r_paused = client.get(f"/api/traces/{sid}/history?hop=0&start=0&end={now + 10}")
        assert r_paused.status_code == 200
        assert len(r_paused.json()["samples"]) == 2

        # Resume — same data, still served (the buffers survived the pause).
        client.post(f"/api/traces/{sid}/resume")
        r_running = client.get(
            f"/api/traces/{sid}/history?hop=0&start=0&end={now + 10}"
        )
        assert r_running.status_code == 200
        assert len(r_running.json()["samples"]) >= 2


def test_history_is_bounded_by_server_retention() -> None:
    """A stream capped at SAMPLE_CAP only returns at most that many samples
    even when the query window would include more (the deque is the bound)."""
    from netplot.session import SAMPLE_CAP
    from netplot.session import Sample as NpSample

    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        base = time.time() - SAMPLE_CAP
        for i in range(SAMPLE_CAP + 500):  # overfill the deque
            session.samples.append(NpSample(t=base + i, rtt_ms=float(i % 100)))
        assert len(session.samples) == SAMPLE_CAP, "deque caps at SAMPLE_CAP"
        r = client.get(
            f"/api/traces/{sid}/history?hop=0&start=0&end={base + SAMPLE_CAP + 1000}"
        )
        assert r.status_code == 200
        body = r.json()
        assert len(body["samples"]) == SAMPLE_CAP


def test_history_snapshot_is_decoupled_from_live_deques() -> None:
    """Mutating the deque after the history capture must not change the
    returned payload (same immutability contract as export_snapshot)."""
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        now = time.time()
        session.append_sample(1.0, t=now)
        # Capture (on-loop, inside the handler).
        r = client.get(f"/api/traces/{sid}/history?hop=0&start=0&end={now + 10}")
        assert r.status_code == 200
        n_before = len(r.json()["samples"])
        # Mutate the live deque after the capture.
        session.append_sample(2.0, t=now + 1)
        assert len(session.samples) == n_before + 1
        # The already-returned payload is unaffected (it was serialized from
        # immutable tuples, not the live deque).
        assert len(r.json()["samples"]) == n_before


# ------------------------------------------------------------- export (CSV)
#
# GET /api/traces/{id}/export?format=csv returns the full server-held
# sample history as a flat CSV table (header: timestamp_utc,timestamp_epoch,
# hop_index,ip,dns,rtt_ms). The server's per-hop buffers are authoritative;
# hop indexing mirrors the SSE contract (hop>=1 route hops, hop 0 the target
# stream only when the route does not reach the target). Works running and
# paused; 404 unknown session; 400 unsupported format.


def test_export_returns_404_for_unknown_session() -> None:
    client = TestClient(create_app(), base_url="http://localhost")
    r = client.get("/api/traces/deadbeef/export")
    assert r.status_code == 404


def test_export_returns_400_for_unsupported_format() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        r = client.get(f"/api/traces/{sid}/export?format=json")
        assert r.status_code == 400
        # csv is the supported value; omitted defaults to csv.
        assert client.get(f"/api/traces/{sid}/export").status_code == 200
        assert client.get(f"/api/traces/{sid}/export?format=csv").status_code == 200


def _parse_csv(text: str) -> list[dict[str, str]]:
    import csv
    import io

    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def test_export_csv_shape_and_loss_encoding() -> None:
    """Header + one row per sample; rtt_ms empty for losses; sorted by t,hop.

    Target-only (route pending): samples export under hop 0 with the
    resolved target IP/DNS columns.
    """
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        app = client.app
        session = app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        session.target_ip = "127.0.0.1"
        session.target_dns = "localhost"
        now = time.time()
        session.append_sample(1.5, t=now)
        session.append_sample(None, t=now + 1)  # loss -> empty rtt_ms

        r = client.get(f"/api/traces/{sid}/export?format=csv")
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/csv")
        cdisp = r.headers["content-disposition"]
        assert cdisp.startswith('attachment; filename="netplot-127.0.0.1-')
        assert cdisp.endswith('.csv"')
        # Sanitized target + created-utc stamp present in the name.
        import re

        assert re.search(r"netplot-127\.0\.0\.1-\d{8}T\d{6}Z\.csv", cdisp)

        rows = _parse_csv(r.text)
        assert len(rows) == 2
        assert list(rows[0].keys()) == [
            "timestamp_utc",
            "timestamp_epoch",
            "hop_index",
            "ip",
            "dns",
            "rtt_ms",
        ]
        # Target-only stream -> hop_index 0, target IP/DNS carried.
        assert rows[0]["hop_index"] == "0"
        assert rows[0]["ip"] == "127.0.0.1"
        assert rows[0]["dns"] == "localhost"
        assert rows[0]["rtt_ms"] == "1.500"
        # Loss sample: empty rtt_ms, hop_index still 0.
        assert rows[1]["hop_index"] == "0"
        assert rows[1]["rtt_ms"] == ""
        # Rows ordered by timestamp ascending.
        assert float(rows[0]["timestamp_epoch"]) <= float(rows[1]["timestamp_epoch"])
        # UTC column is ISO 8601 with a trailing Z.
        assert rows[0]["timestamp_utc"].endswith("Z")


def test_export_csv_route_hops_and_target_dedup() -> None:
    """Discovered route hops export under their own index; when the final
    hop IS the target (shared deque) hop 0 is NOT emitted (no duplication)."""
    from netplot.session import Hop
    from netplot.session import Sample as NpSample

    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        app = client.app
        session = app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        # Two hops; the final hop shares the target's deque (target-covered).
        h1 = Hop(index=1, ip="10.0.0.1", dns="router")
        h2 = Hop(index=2, ip="127.0.0.1", dns="localhost")
        h2.samples = session.samples  # shared reference -> dedup applies
        session.hops = [h1, h2]
        session.route_state = "ready"
        session.target_ip = "127.0.0.1"
        session.target_dns = "localhost"
        now = time.time()
        h1.samples.append(NpSample(t=now, rtt_ms=2.0))
        session.samples.append(NpSample(t=now, rtt_ms=1.5))  # target stream

        rows = _parse_csv(client.get(f"/api/traces/{sid}/export").text)
        hop_indices = sorted(int(r["hop_index"]) for r in rows)
        # hop 0 must NOT appear: target stream is already the final hop.
        assert 0 not in hop_indices, f"unexpected hop 0 rows: {rows!r}"
        # hop 1 (router) and hop 2 (target, shared) both present.
        assert 1 in hop_indices
        assert 2 in hop_indices
        # The hop-2 row carries the target IP/DNS and the shared sample.
        hop2 = next(r for r in rows if r["hop_index"] == "2")
        assert hop2["ip"] == "127.0.0.1"
        assert hop2["dns"] == "localhost"
        assert hop2["rtt_ms"] == "1.500"


def test_export_csv_silent_tail_emits_target_as_hop_0() -> None:
    """When the route does NOT reach the target (silent tail), the target
    stream is exported as hop 0 alongside the discovered route hops."""
    from netplot.session import Hop
    from netplot.session import Sample as NpSample

    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        app = client.app
        session = app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        # Final hop is silent and NOT the target -> target stream stands alone.
        session.hops = [
            Hop(index=1, ip="10.0.0.1", dns="router"),
            Hop(index=2, ip=None, dns=None),
        ]
        session.route_state = "ready"
        session.target_ip = "133.243.238.243"
        session.target_dns = "ntp.nict.jp"
        now = time.time()
        session.hops[0].samples.append(NpSample(t=now, rtt_ms=5.0))
        session.hops[1].samples.append(NpSample(t=now, rtt_ms=None))  # silent
        session.samples.append(NpSample(t=now, rtt_ms=8.0))  # target (hop 0)

        rows = _parse_csv(client.get(f"/api/traces/{sid}/export").text)
        hop0 = next(r for r in rows if r["hop_index"] == "0")
        assert hop0["ip"] == "133.243.238.243"
        assert hop0["dns"] == "ntp.nict.jp"
        assert hop0["rtt_ms"] == "8.000"
        # Silent hop row: empty ip/dns/rtt (loss sample, preserved not collapsed).
        silent = next(r for r in rows if r["hop_index"] == "2")
        assert silent["ip"] == ""
        assert silent["dns"] == ""
        assert silent["rtt_ms"] == ""


def test_export_works_while_running_and_while_paused() -> None:
    """Export is independent of probe-loop state: a paused session's buffers
    are still authoritative and produce the same rows as while running."""
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        app = client.app
        session = app.state.session_manager.get(sid)
        session.target_ip = "127.0.0.1"
        session.append_sample(1.0, t=time.time())

        running_rows = _parse_csv(
            client.get(f"/api/traces/{sid}/export?format=csv").text
        )
        client.post(f"/api/traces/{sid}/pause")
        paused_rows = _parse_csv(
            client.get(f"/api/traces/{sid}/export?format=csv").text
        )
        assert len(paused_rows) >= len(running_rows)
        assert all(r["hop_index"] == "0" for r in paused_rows)


def test_export_filename_sanitizes_unsafe_target() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        # A target with shell metacharacters is rejected at create time, so
        # inject an unsafe label directly to exercise the sanitizer.
        sid = _create_then(client)
        app = client.app
        session = app.state.session_manager.get(sid)
        session.target = "a b/c\\d;e"
        client.post(f"/api/traces/{sid}/pause")
        cdisp = client.get(f"/api/traces/{sid}/export").headers["content-disposition"]
        # Everything outside [A-Za-z0-9.-] collapsed to '_'.
        assert 'filename="netplot-a_b_c_d_e-' in cdisp


def test_export_csv_quotes_and_neutralizes_formula_text() -> None:
    """CSV syntax and spreadsheet formula text cannot escape their cells."""
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        session.target_ip = '"=not,a formula"'
        session.target_dns = "+SUM(A1:A2)"
        session.hops = [Hop(index=1, ip="-1", dns="@cmd")]
        session.hops[0].samples.append(Sample(t=time.time(), rtt_ms=1.0))
        session.samples.append(Sample(t=time.time() + 1, rtt_ms=2.0))

        rows = _parse_csv(client.get(f"/api/traces/{sid}/export").text)
        assert rows[0]["ip"] == "'-1"
        assert rows[0]["dns"] == "'@cmd"
        target = next(row for row in rows if row["hop_index"] == "0")
        # The leading quote makes this a literal text value already; csv.writer
        # preserves the embedded quote and comma as one field.
        assert target["ip"] == '"=not,a formula"'
        assert target["dns"] == "'+SUM(A1:A2)"


def test_export_empty_session_returns_header_only() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        client.post(f"/api/traces/{sid}/pause")
        text = client.get(f"/api/traces/{sid}/export?format=csv").text
        rows = _parse_csv(text)
        assert rows == [], "no samples -> header row only, no data rows"


# ----------------------------------------------- export concurrency guard (429)
#
# The export endpoint holds a process-wide (per-app) bounded semaphore
# (EXPORT_CONCURRENCY_LIMIT, default 2) across both phases (on-loop
# snapshot capture + off-loop serialization). A request that finds all
# slots already held gets HTTP 429 + Retry-After instead of queueing.
# Queueing would re-introduce the unbounded-stall threat under sustained
# concurrent requests, so the guard is try-acquire-or-reject, not a queue.


def test_export_returns_429_when_concurrency_slots_are_held() -> None:
    """Holding both export slots forces the next request to 429 + Retry-After.

    The endpoint's non-blocking check reads the semaphore's private ``_value``
    directly (no public non-blocking acquire exists on asyncio.Semaphore), so
    the test simulates held slots by decrementing the same counter. This is
    load-bearing: it pins the exact contract the endpoint relies on.
    """
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        sem = client.app.state.export_semaphore
        assert sem._value == 2
        # Simulate two in-flight exports holding both slots.
        sem._value = 0
        try:
            r = client.get(f"/api/traces/{sid}/export?format=csv")
            assert r.status_code == 429
            assert r.headers["retry-after"] == "2"
            assert "busy" in r.json()["detail"].lower()
        finally:
            sem._value = 2
        # After release, a fresh request succeeds again.
        assert client.get(f"/api/traces/{sid}/export?format=csv").status_code == 200


def test_export_succeeds_with_one_slot_free_under_one_held() -> None:
    """One slot held -> the second concurrent export still succeeds."""
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        sem = client.app.state.export_semaphore
        sem._value = 1  # one slot held
        try:
            r = client.get(f"/api/traces/{sid}/export?format=csv")
            assert r.status_code == 200
            assert r.headers["content-type"].startswith("text/csv")
        finally:
            # The successful export acquired+released, so value is back to 1;
            # reset to 2 for cleanliness.
            sem._value = 2


def test_export_semaphore_is_released_after_normal_path() -> None:
    """A successful export must release its slot so subsequent requests work."""
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        sem = client.app.state.export_semaphore
        assert sem._value == 2
        # Two back-to-back exports should both succeed — proves release.
        for _ in range(2):
            assert client.get(f"/api/traces/{sid}/export?format=csv").status_code == 200
        assert sem._value == 2, "semaphore leaked across successful exports"


def test_export_semaphore_is_released_after_error_path() -> None:
    """An unknown-session 404 (raised before acquire) must not consume a slot,
    AND a 400 (raised before acquire) must not consume a slot either — both
    reject before the semaphore is touched. A successful export after them
    still finds the full budget available."""
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        sem = client.app.state.export_semaphore
        client.get("/api/traces/deadbeef/export")  # 404, pre-acquire
        client.get(f"/api/traces/{sid}/export?format=json")  # 400, pre-acquire
        assert sem._value == 2, "pre-acquire rejections must not consume a slot"
        assert client.get(f"/api/traces/{sid}/export").status_code == 200


# -------------------------------------- off-loop serialization phase split
#
# export_trace runs in two phases: (1) export_snapshot() captures the live
# deques into an immutable list of tuples ON the event loop; (2)
# _samples_csv_from_snapshot() serializes that snapshot OFF the event loop
# via asyncio.to_thread. Phase 2 must NOT touch the live deques — that is
# the race the prior review fixed (iterating a deque another coroutine is
# mutating off-loop raises RuntimeError mid-iteration or yields a torn
# read). These tests pin the contract by exercising the phase split
# directly: capture, then mutate the deque, then serialize, and assert the
# mutation is absent from the CSV.


def test_export_snapshot_is_decoupled_from_live_deques() -> None:
    """Mutating the deque after capture must not change the serialized CSV."""
    from netplot.web import _samples_csv_from_snapshot

    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        session.target_ip = "127.0.0.1"
        session.append_sample(1.0, t=1000.0)
        # Phase 1 — capture an immutable snapshot on-loop.
        snapshot = session.export_snapshot()
        assert len(snapshot) == 1
        # Mutate the live deque AFTER capture (simulates a probe write that
        # lands between phase 1 and phase 2). Phase 2 must not observe it.
        session.append_sample(2.0, t=2000.0)
        assert len(session.samples) == 2, "sanity: live deque did grow"
        # Phase 2 — serialize the snapshot off-loop.
        text = _samples_csv_from_snapshot(snapshot)
        rows = _parse_csv(text)
        assert len(rows) == 1, "snapshot must not reflect post-capture mutations"
        # And the surviving row is the originally-captured one.
        assert rows[0]["rtt_ms"] == "1.000"
        assert float(rows[0]["timestamp_epoch"]) == 1000.0


def test_export_snapshot_rows_are_immutable_tuples() -> None:
    """The snapshot contains tuples of immutable scalars (no deque refs)."""
    from netplot.session import Sample as NpSample

    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        session.target_ip = "127.0.0.1"
        session.samples.append(NpSample(t=1.0, rtt_ms=1.5))
        snapshot = session.export_snapshot()
        assert len(snapshot) == 1
        row = snapshot[0]
        assert isinstance(row, tuple)
        t, hop_index, ip, dns, rtt_ms = row
        # Immutable scalar types only — no deque/Sample references escape.
        assert isinstance(t, float)
        assert isinstance(hop_index, int)
        assert ip is None or isinstance(ip, str)
        assert dns is None or isinstance(dns, str)
        assert rtt_ms is None or isinstance(rtt_ms, float)


# ------------------------------------- adversarial _safe_csv_text hardening
#
# _safe_csv_text must defeat formula-injection bypasses via leading
# whitespace/control/format characters (the original naive first-char check
# missed "\t=...", "\r=...", BOM-prefixed formulas, etc.), keep records
# single-line by flattening embedded C0 controls, and never double-prefix
# an already-escaped value. Pure-function unit tests.


def test_safe_csv_text_neutralizes_formula_chars_with_leading_whitespace() -> None:
    """Leading ASCII whitespace + formula char -> apostrophe at absolute start."""
    from netplot.web import _safe_csv_text

    for ws in (" ", "\t", "\n", "\r", "\x0b", "\x0c"):
        for trigger in ("=", "+", "-", "@"):
            value = ws + trigger + "EVIL()"
            out = _safe_csv_text(value)
            # Apostrophe is at the ABSOLUTE beginning (leading ws is preserved
            # after control normalization: tab/LF/CR become spaces).
            assert out.startswith("'"), f"{value!r} -> {out!r} (no apostrophe)"
            # The trigger char is still in the cell, but inert after the prefix.
            assert trigger in out
            # The control normalization flattened tab/LF/CR to spaces.
            assert "\t" not in out and "\n" not in out and "\r" not in out, (
                f"{value!r} -> {out!r} (control not flattened)"
            )


def test_safe_csv_text_neutralizes_formula_chars_with_leading_bom_and_zwsp() -> None:
    """Leading BOM / zero-width-space / LTR mark + formula char -> prefixed."""
    from netplot.web import _safe_csv_text

    # BOM, ZWSP, LRM (LTR mark), RLM (RTL mark), WJ (word joiner) are all
    # format (Cf) characters that spreadsheet formula sniffers may skip.
    for lead in ("\ufeff", "\u200b", "\u200e", "\u200f", "\u2060"):
        for trigger in ("=", "+", "-", "@"):
            value = lead + trigger + "EVIL()"
            out = _safe_csv_text(value)
            assert out.startswith("'"), f"{value!r} -> {out!r}"


def test_safe_csv_text_flattens_embedded_newlines_to_spaces() -> None:
    """Embedded CR/LF/tab/NUL in a non-formula value are flattened to spaces
    so each cell stays a single CSV record."""
    from netplot.web import _safe_csv_text

    assert _safe_csv_text("a\nb") == "a b"
    assert _safe_csv_text("a\rb") == "a b"
    assert _safe_csv_text("a\tb") == "a b"
    assert _safe_csv_text("a\x00b") == "a b"
    assert _safe_csv_text("a\rb\nc\td") == "a b c d"


def test_safe_csv_text_does_not_double_prefix_already_escaped_values() -> None:
    """A value that already starts with an apostrophe is returned unchanged
    (no double-prefix), even if the rest of it looks like a formula."""
    from netplot.web import _safe_csv_text

    assert _safe_csv_text("'=evil") == "'=evil"
    assert _safe_csv_text("'+evil") == "'+evil"
    # Leading-whitespace-then-apostrophe also does not get double-prefixed:
    # the apostrophe is the first substantive character, not a formula trigger.
    assert _safe_csv_text("  '=evil") == "  '=evil"


def test_safe_csv_text_leaves_ordinary_values_unchanged() -> None:
    """Plain hostnames, IPs, DNS names, and already-safe text pass through."""
    from netplot.web import _safe_csv_text

    for ordinary in (
        "",
        "127.0.0.1",
        "ntp.nict.jp",
        "router.local",
        "2001:db8::1",
        "O'Brien",
        "host-name.example",
        "  trailing space only",
        "value, with comma",  # csv.writer quotes; sanitizer leaves text alone
        '"already, quoted"',
    ):
        assert _safe_csv_text(ordinary) == ordinary, f"{ordinary!r} changed"


def test_safe_csv_text_treats_none_as_empty_string() -> None:
    from netplot.web import _safe_csv_text

    assert _safe_csv_text(None) == ""


# ------------------------------------------------------------------- SSE


def test_sse_endpoint_returns_404_for_unknown_session() -> None:
    client = TestClient(create_app(), base_url="http://localhost")
    r = client.get("/api/traces/deadbeef/events")
    assert r.status_code == 404


# The SSE generator and wire format (including multi-subscriber fan-out and
# the real-HTTP path through uvicorn) are exercised in tests/test_sse.py.
# They cannot be driven through TestClient/ASGITransport here because both
# buffer the full response body before returning it to the client (see
# starlette/testclient.py _TestClientTransport.handle_request which awaits
# the full app coroutine).


# ------------------------------ integration: one real ping of loopback


@pytest.mark.integration
def test_probe_loopback_via_session_api() -> None:
    """Real-ping smoke: POST a 1 s trace against 127.0.0.1, snapshot shows
    at least one successful sample within a few seconds.

    Loopback ping requires no privileges on macOS or Linux. Skipped when the
    ping binary is unavailable (e.g. minimal sandboxes).
    """
    if not shutil.which("ping"):
        pytest.skip("ping binary not available on PATH")

    with TestClient(create_app(), base_url="http://localhost") as client:
        r = client.post("/api/traces", json={"target": "127.0.0.1", "interval_s": 1.0})
        assert r.status_code == 201
        sid = r.json()["id"]

        deadline = time.time() + 5.0
        snapshot = client.get(f"/api/traces/{sid}/snapshot?focus=60").json()
        while time.time() < deadline and snapshot["stats"]["count"] < 1:
            time.sleep(0.1)
            snapshot = client.get(f"/api/traces/{sid}/snapshot?focus=60").json()

        assert snapshot["stats"]["count"] >= 1, "no samples collected within deadline"
        successful = [s for s in snapshot["samples"] if s["rtt_ms"] is not None]
        assert successful, (
            f"loopback ping did not produce any successful samples; "
            f"samples seen: {snapshot['samples']!r}"
        )
        assert all(s["rtt_ms"] >= 0 for s in successful)


# ------------------------------------------- Slice 3: hop= + route_state


def test_snapshot_hop_param_returns_per_hop_stats() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        app = client.app
        session = app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        # Inject hops + samples directly so the test does not depend on
        # traceroute availability.
        from netplot.session import Hop
        from netplot.session import Sample as NpSample

        session.hops = [Hop(index=1, ip="10.0.0.1", dns="router")]
        session.route_state = "ready"
        now = time.time()
        for rtt in (5.0, 10.0, None):
            session.hops[0].samples.append(NpSample(t=now, rtt_ms=rtt))

        r = client.get(f"/api/traces/{sid}/snapshot?hop=1&focus=0")
        assert r.status_code == 200
        body = r.json()
        assert set(body.keys()) == {
            "id",
            "target",
            "hop",
            "ip",
            "dns",
            "internal",
            "icmp_non_responsive",
            "focus_s",
            "stats",
            "samples",
        }
        assert body["hop"] == 1
        assert body["ip"] == "10.0.0.1"
        assert body["dns"] == "router"
        assert body["stats"]["count"] == 3
        assert body["stats"]["loss_pct"] == pytest.approx(100.0 / 3.0)
        assert len(body["samples"]) == 3


def test_snapshot_hop_param_returns_404_for_unknown_hop() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        client.post(f"/api/traces/{sid}/pause")
        r = client.get(f"/api/traces/{sid}/snapshot?hop=99")
        assert r.status_code == 404


def test_snapshot_route_state_transitions_to_ready() -> None:
    """Smoke that the snapshot's route_state reflects a fake-injected route."""
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        app = client.app
        session = app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")

        # Initial state: pending.
        body = client.get(f"/api/traces/{sid}/snapshot").json()
        assert body["route_state"] in {"pending", "failed"}

        from netplot.session import Hop

        # Simulate successful discovery.
        session.hops = [
            Hop(index=1, ip="10.0.0.1", dns=None),
            Hop(index=2, ip="10.0.0.2", dns=None),
        ]
        session.route_state = "ready"

        body = client.get(f"/api/traces/{sid}/snapshot").json()
        assert body["route_state"] == "ready"
        assert len(body["hops"]) == 2
        assert [h["index"] for h in body["hops"]] == [1, 2]
        # Each hop carries stats.
        for h in body["hops"]:
            assert set(h.keys()) == {
                "index",
                "ip",
                "dns",
                "internal",
                "icmp_non_responsive",
                "stats",
            }
            assert set(h["stats"].keys()) == {
                "count",
                "avg_ms",
                "min_ms",
                "cur_ms",
                "loss_pct",
            }


def test_snapshot_exposes_target_dns_when_set() -> None:
    with TestClient(create_app(), base_url="http://localhost") as client:
        sid = _create_then(client)
        app = client.app
        session = app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        session.target_dns = "router.lan"
        session.target_ip = "10.0.0.1"

        body = client.get(f"/api/traces/{sid}/snapshot").json()
        assert body["target_dns"] == "router.lan"
        assert body["target_ip"] == "10.0.0.1"


# ------------------------------ integration: real traceroute of loopback


@pytest.mark.integration
def test_route_discovery_loopback_end_to_end() -> None:
    """Real-traceroute smoke: POST a trace against 127.0.0.1, snapshot shows
    ``route_state == 'ready'`` and at least one hop whose IP is 127.0.0.1.

    Loopback traceroute requires no privileges on macOS or Linux. Skipped
    when the traceroute binary is unavailable.
    """
    if not shutil.which("traceroute"):
        pytest.skip("traceroute binary not available on PATH")

    with TestClient(create_app(), base_url="http://localhost") as client:
        r = client.post("/api/traces", json={"target": "127.0.0.1", "interval_s": 1.0})
        assert r.status_code == 201
        sid = r.json()["id"]

        deadline = time.time() + 15.0
        snap = client.get(f"/api/traces/{sid}/snapshot").json()
        while time.time() < deadline and snap["route_state"] == "pending":
            time.sleep(0.2)
            snap = client.get(f"/api/traces/{sid}/snapshot").json()

        assert snap["route_state"] == "ready", (
            f"route discovery did not reach ready within deadline "
            f"(state={snap['route_state']}, error={snap.get('route_error')!r})"
        )
        assert len(snap["hops"]) >= 1
        assert snap["hops"][-1]["ip"] == "127.0.0.1"
        # Final hop's IP == target IP -> dedup applies. Per-hop stats
        # share the target's stream.
        app = client.app
        session = app.state.session_manager.get(sid)
        assert session is not None
        assert session.hops[-1].samples is session.samples
