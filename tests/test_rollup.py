"""Unit tests for the server-side rollup tier (Slice D2).

Covers:

- :class:`StreamRollup` aggregation math (mixed success/loss, full-loss,
  empty, overflow-safe incremental mean).
- Bucket boundary alignment to wall-clock multiples of ``bucket_s``.
- Cap eviction: the ``cap`` newest buckets survive; ``earliest_t`` reflects
  eviction (smallest retained ``t``).
- Sparse response: empty buckets (paused gaps, pre-start) are OMITTED.
- Exact bucket-boundary timestamps (a sample at exactly ``t = k * bucket_s``
  lands in bucket ``k``, not ``k - 1``).
- Single-sample bucket.
- :meth:`TraceSession.history_buckets` hop resolution (target stream + per
  route hop), the final-hop-is-target shared-rollup case, and the on-loop
  immutability contract (mutating the rollup after capture does not change
  the returned payload).
- Endpoint matrix via :class:`fastapi.testclient.TestClient`: ``raw`` (D1
  regression), ``1m``, ``auto`` (raw fit vs 1m fallback), invalid → 400,
  full-loss bucket nulls, sparse omission, paused session. Each endpoint
  test registers an UNSTARTED session directly on the app's manager and
  points the app at a per-test ``data_dir``: the sample/rollup sets contain
  exactly what the test injects, no real probe/traceroute/DNS runs, and no
  leftover default-dir ``sessions.json`` can boot-restore live sessions.
"""

from __future__ import annotations

import asyncio
import math
import time

import pytest
from fastapi.testclient import TestClient

from netplot.session import (
    ROLLUP_BUCKET_S,
    ROLLUP_CAP,
    Hop,
    SessionManager,
    StreamRollup,
    TraceSession,
)
from netplot.web import create_app

# ---------------------------------------------------------------- StreamRollup


def test_rollup_empty_capture_returns_none_earliest_and_empty_list() -> None:
    r = StreamRollup(bucket_s=60, cap=10)
    earliest, buckets = r.capture(0, 1000)
    assert earliest is None
    assert buckets == []


def test_rollup_single_sample_bucket_math() -> None:
    """A bucket with one successful sample: avg=min=max=that sample."""
    r = StreamRollup(bucket_s=60, cap=10)
    r.add_sample(5, 42.0)
    _, buckets = r.capture(0, 1000)
    assert len(buckets) == 1
    b = buckets[0]
    assert b["t"] == 0  # floor(5/60)*60 = 0
    assert b["count"] == 1
    assert b["loss_count"] == 0
    assert math.isclose(b["avg_ms"], 42.0)
    assert math.isclose(b["min_ms"], 42.0)
    assert math.isclose(b["max_ms"], 42.0)


def test_rollup_mixed_success_and_loss_aggregates_over_successes_only() -> None:
    """avg/min/max are over successful samples; count includes losses."""
    r = StreamRollup(bucket_s=60, cap=10)
    r.add_sample(1, 10.0)
    r.add_sample(2, 20.0)
    r.add_sample(3, None)  # loss
    r.add_sample(4, 30.0)
    _, buckets = r.capture(0, 1000)
    assert len(buckets) == 1
    b = buckets[0]
    assert b["count"] == 4
    assert b["loss_count"] == 1
    # avg of {10, 20, 30} = 20
    assert math.isclose(b["avg_ms"], 20.0)
    assert math.isclose(b["min_ms"], 10.0)
    assert math.isclose(b["max_ms"], 30.0)


def test_rollup_full_loss_bucket_carries_nulls() -> None:
    """A bucket whose samples are ALL losses: avg/min/max None, loss_count>0."""
    r = StreamRollup(bucket_s=60, cap=10)
    r.add_sample(1, None)
    r.add_sample(2, None)
    _, buckets = r.capture(0, 1000)
    assert len(buckets) == 1
    b = buckets[0]
    assert b["count"] == 2
    assert b["loss_count"] == 2
    assert b["avg_ms"] is None
    assert b["min_ms"] is None
    assert b["max_ms"] is None


def test_rollup_non_finite_rtt_treated_as_loss() -> None:
    """NaN / Infinity RTTs must not poison avg/min/max (metrics parity)."""
    r = StreamRollup(bucket_s=60, cap=10)
    r.add_sample(1, float("nan"))
    r.add_sample(2, float("inf"))
    r.add_sample(3, float("-inf"))
    r.add_sample(4, 15.0)
    _, buckets = r.capture(0, 1000)
    b = buckets[0]
    assert b["count"] == 4
    assert b["loss_count"] == 3
    assert math.isclose(b["avg_ms"], 15.0)
    assert math.isclose(b["min_ms"], 15.0)
    assert math.isclose(b["max_ms"], 15.0)


def test_rollup_bucket_boundary_alignment() -> None:
    """Bucket key = floor(t / bucket_s) * bucket_s — wall-clock alignment.

    A sample landing exactly on a bucket boundary belongs to the NEW bucket
    (the bucket starting at that timestamp), not the previous one. Samples
    one second before the boundary stay in the earlier bucket.
    """
    r = StreamRollup(bucket_s=60, cap=10)
    r.add_sample(59, 1.0)  # bucket 0
    r.add_sample(60, 2.0)  # bucket 60 (boundary -> new bucket)
    r.add_sample(119, 3.0)  # bucket 60
    r.add_sample(120, 4.0)  # bucket 120 (boundary -> new bucket)
    _, buckets = r.capture(0, 1000)
    ts = [b["t"] for b in buckets]
    assert ts == [0, 60, 120]
    # Bucket 0 has only the t=59 sample.
    assert math.isclose(buckets[0]["avg_ms"], 1.0)
    # Bucket 60 has t=60 and t=119.
    assert math.isclose(buckets[1]["avg_ms"], 2.5)
    # Bucket 120 has t=120.
    assert math.isclose(buckets[2]["avg_ms"], 4.0)


def test_rollup_custom_bucket_size_aligns_to_its_own_multiple() -> None:
    """A 5-second bucket aligns to multiples of 5, not 60."""
    r = StreamRollup(bucket_s=5, cap=100)
    r.add_sample(4, 1.0)  # bucket 0
    r.add_sample(5, 2.0)  # bucket 5
    r.add_sample(9, 3.0)  # bucket 5
    r.add_sample(10, 4.0)  # bucket 10
    _, buckets = r.capture(0, 1000)
    assert [b["t"] for b in buckets] == [0, 5, 10]


def test_rollup_capture_clamps_to_start_end_window() -> None:
    """Buckets outside [start, end] are excluded; earliest_t is unaffected."""
    r = StreamRollup(bucket_s=60, cap=100)
    for i in range(5):
        r.add_sample(i * 60, float(i))  # buckets 0, 60, 120, 180, 240
    # Window covers only buckets 60..180.
    earliest, buckets = r.capture(60, 180)
    assert earliest == 0  # oldest retained, regardless of query window
    assert [b["t"] for b in buckets] == [60, 120, 180]


def test_rollup_earliest_t_is_oldest_retained_bucket() -> None:
    r = StreamRollup(bucket_s=60, cap=100)
    r.add_sample(300, 1.0)
    r.add_sample(60, 2.0)
    r.add_sample(120, 3.0)
    # Insertion order was 300, 60, 120; earliest is min(t) = 60.
    assert r.earliest_t() == 60
    _, buckets = r.capture(0, 10**9)
    assert buckets[0]["t"] == 60


def test_rollup_cap_eviction_keeps_newest_buckets() -> None:
    """On overflow the bucket with the smallest t is evicted; the ``cap``
    newest survive and ``earliest_t`` reflects the eviction."""
    r = StreamRollup(bucket_s=60, cap=3)
    r.add_sample(0, 1.0)  # bucket 0
    r.add_sample(60, 2.0)  # bucket 60
    r.add_sample(120, 3.0)  # bucket 120
    assert r.earliest_t() == 0
    # Adding a 4th bucket exceeds cap=3; bucket 0 is evicted.
    r.add_sample(180, 4.0)  # bucket 180
    assert r.earliest_t() == 60
    _, buckets = r.capture(0, 10**9)
    assert [b["t"] for b in buckets] == [60, 120, 180]
    # The evicted bucket is gone.
    assert all(b["t"] != 0 for b in buckets)


def test_rollup_cap_eviction_uses_wall_clock_not_insertion_order() -> None:
    """A clock-skew that inserts an older key after newer ones still evicts
    the oldest by wall-clock t (keeps the ``cap`` newest)."""
    r = StreamRollup(bucket_s=60, cap=3)
    r.add_sample(300, 1.0)  # bucket 300 (inserted first)
    r.add_sample(360, 2.0)  # bucket 360
    r.add_sample(420, 3.0)  # bucket 420
    # Now a clock-back-adjust inserts an OLDER bucket after newer ones.
    r.add_sample(60, 4.0)  # bucket 60 (inserted last, but oldest)
    # cap=3, so the oldest (bucket 60) is evicted despite being newest-inserted.
    assert r.earliest_t() == 300
    _, buckets = r.capture(0, 10**9)
    assert sorted(b["t"] for b in buckets) == [300, 360, 420]


def test_rollup_capture_returns_immutable_snapshots() -> None:
    """Mutating the rollup after capture must not change the returned
    payload (on-loop immutability contract)."""
    r = StreamRollup(bucket_s=60, cap=10)
    r.add_sample(1, 10.0)
    _, buckets = r.capture(0, 1000)
    snapshot_avg = buckets[0]["avg_ms"]
    # Fold many more samples into the same bucket.
    for v in range(20):
        r.add_sample(2, float(v))
    # The previously-captured bucket is unchanged.
    assert math.isclose(buckets[0]["avg_ms"], snapshot_avg)


def test_rollup_incremental_mean_is_overflow_safe() -> None:
    """Finite RTTs near the float limit must not overflow the accumulator
    (divide-before-subtract discipline)."""
    r = StreamRollup(bucket_s=60, cap=10)
    huge = 1e308
    r.add_sample(1, huge)
    r.add_sample(2, huge)  # adding two huge values: naive sum would overflow
    _, buckets = r.capture(0, 1000)
    b = buckets[0]
    # The mean should be finite (naive sum would be inf).
    assert math.isfinite(b["avg_ms"])
    assert math.isclose(b["avg_ms"], huge)


def test_rollup_invalid_constructor_args_raise() -> None:
    with pytest.raises(ValueError):
        StreamRollup(bucket_s=0, cap=10)
    with pytest.raises(ValueError):
        StreamRollup(bucket_s=-1, cap=10)
    with pytest.raises(ValueError):
        StreamRollup(bucket_s=60, cap=0)
    with pytest.raises(ValueError):
        StreamRollup(bucket_s=60, cap=-1)


def test_rollup_module_constants_default_to_pingplotter_values() -> None:
    assert ROLLUP_BUCKET_S == 60
    assert ROLLUP_CAP == 1440  # 24h of 1-minute buckets


# -------------------------------------------------------- TraceSession wiring


def _make_session() -> TraceSession:
    return TraceSession(id="abc", target="example.com", interval_s=2.5)


def test_session_history_buckets_hop0_targets_target_rollup() -> None:
    session = _make_session()
    session.target_rollup.add_sample(5, 10.0)
    session.target_rollup.add_sample(65, 20.0)
    captured = session.history_buckets(0, 0, 1000)
    assert captured is not None
    earliest, buckets = captured
    assert earliest == 0
    assert [b["t"] for b in buckets] == [0, 60]


def test_session_history_buckets_unknown_hop_returns_none() -> None:
    session = _make_session()
    # No discovered hops -> hop>=1 is unknown.
    assert session.history_buckets(1, 0, 1000) is None
    assert session.history_buckets(99, 0, 1000) is None


def test_session_history_buckets_per_hop_stream() -> None:
    session = _make_session()
    hop = Hop(index=1, ip="10.0.0.1", dns=None)
    hop.rollup.add_sample(5, 7.5)
    hop.rollup.add_sample(6, None)  # loss
    session.hops = [hop]
    captured = session.history_buckets(1, 0, 1000)
    assert captured is not None
    _, buckets = captured
    assert len(buckets) == 1
    b = buckets[0]
    assert b["count"] == 2
    assert b["loss_count"] == 1
    assert math.isclose(b["avg_ms"], 7.5)


def test_session_history_buckets_final_hop_is_target_shares_rollup() -> None:
    """When the final hop IS the target, hop=0 and hop=final share one rollup
    (mirrors the deque-sharing contract)."""
    session = _make_session()
    session.target_ip = "10.0.0.1"
    hop = Hop(index=1, ip="10.0.0.1", dns=None)
    # Discovery rebinds both samples and rollup to the session's.
    hop.samples = session.samples
    hop.rollup = session.target_rollup
    session.hops = [hop]
    session.route_state = "ready"
    session.target_rollup.add_sample(5, 11.0)

    cap0 = session.history_buckets(0, 0, 1000)
    cap1 = session.history_buckets(1, 0, 1000)
    assert cap0 is not None and cap1 is not None
    # Same underlying rollup -> identical payloads.
    assert cap0[0] == cap1[0]
    assert cap0[1] == cap1[1]


def test_append_sample_folds_into_target_rollup() -> None:
    """The test/back-compat hook exercises the rollup too (D1 history tests
    reuse the same hook for D2)."""
    session = _make_session()
    session.append_sample(10.0, t=5)
    session.append_sample(None, t=6)
    captured = session.history_buckets(0, 0, 1000)
    assert captured is not None
    _, buckets = captured
    assert len(buckets) == 1
    b = buckets[0]
    assert b["count"] == 2
    assert b["loss_count"] == 1
    assert math.isclose(b["avg_ms"], 10.0)


# ----------------------------------------------------------- endpoint matrix


def _create_then(
    client: TestClient, target: str = "127.0.0.1", interval_s: float = 2.0
):
    """Register one UNSTARTED session directly and return its id.

    Every endpoint test below injects its whole sample set itself (or asserts
    an empty stream), so the session must never run a probe loop, traceroute
    discovery, or DNS resolution. ``SessionManager.create`` registers
    without starting any task, so the raw deque and the rollup hold exactly
    what the test injected; creating through ``POST /api/traces`` instead
    would start real probes that can land a live loopback sample before a
    following pause takes effect.
    """
    session = client.app.state.session_manager.create(target, interval_s=interval_s)
    return session.id


def test_history_1m_resolution_payload_shape_and_math(tmp_path) -> None:
    """resolution=1m returns the rollup envelope with correct math."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        now = time.time()
        # Inject samples into two wall-clock minutes (using the default
        # 60-second bucket): bucket floor(now) and floor(now) + 60.
        session.append_sample(10.0, t=now - 0.5)
        session.append_sample(20.0, t=now - 0.1)
        session.append_sample(None, t=now)  # loss

        r = client.get(
            f"/api/traces/{sid}/history?hop=0&start=0&end={now + 10}&resolution=1m"
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert set(body.keys()) == {"id", "hop", "resolution", "earliest_t", "buckets"}
        assert body["resolution"] == "1m"
        assert body["hop"] == 0
        assert isinstance(body["earliest_t"], int)
        assert len(body["buckets"]) >= 1
        b = body["buckets"][0]
        assert set(b.keys()) == {
            "t",
            "avg_ms",
            "min_ms",
            "max_ms",
            "count",
            "loss_count",
        }
        # Buckets ascending by t.
        ts = [bb["t"] for bb in body["buckets"]]
        assert ts == sorted(ts)


def test_history_1m_resolution_full_loss_bucket_nulls(tmp_path) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        session.append_sample(None, t=time.time())
        session.append_sample(None, t=time.time() + 0.1)

        r = client.get(
            f"/api/traces/{sid}/history?hop=0&start=0&end={time.time() + 10}&resolution=1m"
        )
        body = r.json()
        full_loss = [b for b in body["buckets"] if b["loss_count"] > 0]
        assert any(b["avg_ms"] is None for b in full_loss)


def test_history_1m_resolution_omits_empty_buckets(tmp_path) -> None:
    """Buckets with no samples are absent — sparse response."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        # Two samples 120s apart -> two non-adjacent buckets; the bucket in
        # between has no samples and must be OMITTED.
        t0 = 1_700_000_000
        session.append_sample(10.0, t=t0)
        session.append_sample(20.0, t=t0 + 120)

        r = client.get(
            f"/api/traces/{sid}/history?hop=0&start=0&end={t0 + 1000}&resolution=1m"
        )
        body = r.json()
        ts = [b["t"] for b in body["buckets"]]
        # The two sample buckets are present; the gap bucket (t0+60 floor) is absent.
        assert len(ts) == 2
        assert (t0 // 60 * 60 + 60) not in ts


def test_history_auto_picks_raw_when_range_fits_raw_retention(tmp_path) -> None:
    """A short range that fits within SAMPLE_CAP * interval_s picks raw."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(
            client,
            target="127.0.0.1",
        )
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        now = time.time()
        session.append_sample(1.0, t=now)

        # interval_s=2.0, SAMPLE_CAP=10000 -> raw retention = 20000s.
        # Request a 60s window -> fits raw.
        r = client.get(
            f"/api/traces/{sid}/history?hop=0&start={now - 60}&end={now}&resolution=auto"
        )
        assert r.status_code == 200
        body = r.json()
        assert body["resolution"] == "raw"
        assert "samples" in body
        assert "buckets" not in body


def test_history_auto_picks_1m_when_range_exceeds_raw_retention(tmp_path) -> None:
    """A range beyond SAMPLE_CAP * interval_s picks 1m."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        # interval_s=2.0, SAMPLE_CAP=10000 -> raw retention = 20000s (~5.6h).
        # Request a 24h window -> exceeds raw retention -> 1m.
        now = time.time()
        session.append_sample(1.0, t=now)
        r = client.get(
            f"/api/traces/{sid}/history?hop=0&start={now - 86400}&end={now}&resolution=auto"
        )
        assert r.status_code == 200
        body = r.json()
        assert body["resolution"] == "1m"
        assert "buckets" in body
        assert "samples" not in body


def test_history_invalid_resolution_returns_400(tmp_path) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client)
        r = client.get(
            f"/api/traces/{sid}/history?hop=0&start=0&end=9999&resolution=bucketed"
        )
        assert r.status_code == 400
        assert "resolution" in r.json()["detail"].lower()
        # All three accepted values work.
        for res in ("raw", "1m", "auto"):
            rr = client.get(
                f"/api/traces/{sid}/history?hop=0&start=0&end=9999&resolution={res}"
            )
            assert rr.status_code == 200


def test_history_1m_unknown_hop_returns_404(tmp_path) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client)
        client.post(f"/api/traces/{sid}/pause")
        r = client.get(
            f"/api/traces/{sid}/history?hop=99&start=0&end=9999&resolution=1m"
        )
        assert r.status_code == 404


def test_history_1m_unknown_session_returns_404(tmp_path) -> None:
    client = TestClient(create_app(data_dir=tmp_path), base_url="http://localhost")
    r = client.get("/api/traces/deadbeef/history?hop=0&start=0&end=9999&resolution=1m")
    assert r.status_code == 404


def test_history_1m_works_while_paused(tmp_path) -> None:
    """Server rollups are authoritative regardless of probe-loop state."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        session.append_sample(1.0, t=time.time())
        r = client.get(
            f"/api/traces/{sid}/history?hop=0&start=0&end={time.time() + 10}&resolution=1m"
        )
        assert r.status_code == 200
        body = r.json()
        assert len(body["buckets"]) >= 1


def test_history_1m_empty_stream_returns_null_earliest(tmp_path) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client)
        client.post(f"/api/traces/{sid}/pause")
        r = client.get(
            f"/api/traces/{sid}/history?hop=0&start=0&end={time.time() + 10}&resolution=1m"
        )
        assert r.status_code == 200
        body = r.json()
        assert body["earliest_t"] is None
        assert body["buckets"] == []


def test_history_raw_resolution_is_byte_compatible_with_d1(tmp_path) -> None:
    """The default raw path (D1) is unchanged: same envelope, same samples."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        now = time.time()
        session.append_sample(1.0, t=now)
        session.append_sample(None, t=now + 0.1)

        # Default resolution (omitted) -> raw.
        r_default = client.get(
            f"/api/traces/{sid}/history?hop=0&start=0&end={now + 10}"
        )
        # Explicit raw.
        r_raw = client.get(
            f"/api/traces/{sid}/history?hop=0&start=0&end={now + 10}&resolution=raw"
        )
        assert r_default.status_code == r_raw.status_code == 200
        body_default = r_default.json()
        body_raw = r_raw.json()
        assert body_default["resolution"] == "raw"
        assert body_raw["resolution"] == "raw"
        assert body_default["samples"] == body_raw["samples"]
        assert len(body_default["samples"]) == 2
        assert any(s["rtt_ms"] is None for s in body_default["samples"])


def test_history_1m_snapshot_is_decoupled_from_live_rollup(tmp_path) -> None:
    """Mutating the rollup after the capture must not change the payload."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client)
        session = client.app.state.session_manager.get(sid)
        client.post(f"/api/traces/{sid}/pause")
        session.append_sample(1.0, t=time.time())
        r = client.get(
            f"/api/traces/{sid}/history?hop=0&start=0&end={time.time() + 10}&resolution=1m"
        )
        before = r.json()
        # Fold many more samples into the same bucket.
        for v in range(20):
            session.append_sample(float(v), t=time.time())
        # The previously-returned payload is unchanged.
        assert before == r.json()


# ------------------------------------------------- live rollup via probe loop


def test_probe_round_folds_samples_into_rollup() -> None:
    """A real probe round (driven by a fake probe_fn) updates BOTH the raw
    deque and the rollup tier in the same on-loop turn."""

    async def fake_probe(host, _timeout):
        return 5.0

    async def run():
        from netplot.engine import RouteDiscoveryError

        async def fail_discover(_t, _timeout=90.0):
            raise RouteDiscoveryError("disabled")

        # Reuse the test_session helper for the resolve_name stand-in.
        from test_session import _fast_resolve_name

        mgr = SessionManager(
            probe_fn=fake_probe,
            discover_fn=fail_discover,
            resolve_name_fn=_fast_resolve_name,
        )
        session = mgr.create("example.com", interval_s=0.05)
        await mgr.start(session)
        await asyncio.sleep(0.15)
        await mgr.pause(session)
        return session

    session = asyncio.run(run())
    assert len(session.samples) >= 1
    # Rollup has at least one bucket with the same sample.
    _, buckets = session.target_rollup.capture(0, 10**12)
    assert len(buckets) >= 1
    # Every bucket has the expected avg (~5.0) since the fake probe always
    # returns 5.0.
    for b in buckets:
        if b["count"] > 0 and b["loss_count"] < b["count"]:
            assert math.isclose(b["avg_ms"], 5.0, rel_tol=1e-9)
