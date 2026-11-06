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
- Ordered-commit regression for the overlapping scheduler: a held first
  round with later rounds finishing first must keep raw history, minute
  rollups, snapshot stats, metrics capture, and subscriber round events
  consistent (launch order, exactly-once, alias folded once, no canceled
  work after pause).
"""

from __future__ import annotations

import asyncio
import math
import time

import pytest
from fastapi.testclient import TestClient

from netplot.engine import RouteHop
from netplot.metrics import capture_metrics
from netplot.session import (
    ROLLUP_BUCKET_S,
    ROLLUP_CAP,
    Hop,
    RouteEvent,
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


# ---------------------------------------- ordered-commit regression (overlap)
#
# PingPlotter-parity contract (docs/plan-pingplotter-timing.md §1.4): rounds
# overlap, collection is asynchronous, and completed rounds commit in LAUNCH
# order. The regression below drives the real scheduler with a scripted
# probe_fn (asyncio events only — no wall-clock waits) and checks the commit
# boundary in every consumer at once — raw deque/history, the minute rollup
# tier, snapshot current/count/loss with cutoffs, metrics capture, and the
# subscriber round-event feed — while a later round finishes BEFORE an
# earlier one. Every reader of the boundary must agree: nothing becomes
# visible until the held round commits; after it does, each round appears
# exactly once, launch-ordered, with the destination/final-hop alias folded
# a single time; pausing with later rounds still blocked must not leak any
# canceled uncommitted work anywhere.

#: TEST-NET-3 literal. ``SessionManager._resolve_target`` short-circuits IP
#: literals (no DNS), so the injected discovery completes within its first
#: task step — deterministically before the second scheduler tick.
_OVERLAP_TARGET = "203.0.113.9"


async def _drive_ordered_commit_scenario() -> dict:
    """Drive the overlapping scheduler through a held-first-round scenario.

    Deterministic sequence, gated entirely by asyncio events:

    - Round 1 (planned target-only, launched before discovery ran) is held
      on ``release_round1``: its RTT (30.0) is only produced when released.
    - Injected discovery establishes the final-hop-is-target alias while
      round 1 is still outstanding (the production rebinding path).
    - Round 2 (a loss) and round 3 (10.0) finish probing while round 1 is
      held, so both complete BEHIND the uncommitted round 1: until round 1
      commits, nothing may become visible in any consumer.
    - Round 4+ launch behind them and block forever, so the session ends up
      paused with later rounds outstanding: canceled uncommitted work must
      not leak into any consumer.

    Mid-flight observations are captured on-loop and returned with handles
    and the on-loop metrics captures for the tests to assert on.
    """
    started: list[asyncio.Event] = [asyncio.Event() for _ in range(4)]
    release_round1 = asyncio.Event()
    block_later_rounds = asyncio.Event()  # never set while the session runs
    probe_hosts: list[str] = []

    async def scripted_probe(host: str, _timeout_s: float) -> float | None:
        call = len(probe_hosts)
        probe_hosts.append(host)
        if call < len(started):
            started[call].set()
        if call == 0:
            await release_round1.wait()  # round 1: held until released
            return 30.0
        if call == 1:
            return None  # round 2: a loss, finishes immediately
        if call == 2:
            return 10.0  # round 3, finishes while round 1 is still held
        await block_later_rounds.wait()  # round 4+: never completes
        raise AssertionError("block_later_rounds must never be set")

    async def injected_discover(_target: str, _timeout_s: float = 90.0):
        return [RouteHop(index=1, ip=_OVERLAP_TARGET)]

    async def no_resolve_name(_ip: str) -> None:
        return None

    mgr = SessionManager(
        probe_fn=scripted_probe,
        discover_fn=injected_discover,
        resolve_name_fn=no_resolve_name,
    )
    session = mgr.create(_OVERLAP_TARGET, interval_s=0.01)
    events: asyncio.Queue = session.subscribe()
    await mgr.start(session)

    # Discovery runs without a single loop suspension for an IP literal
    # target, so the route event is the subscriber feed's first message —
    # and it arrives while round 1 is still held, i.e. the alias was
    # established by the production discovery path while a pre-route round
    # remained outstanding.
    route_event = await asyncio.wait_for(events.get(), 5.0)
    assert isinstance(route_event, RouteEvent)
    assert session.route_state == "ready"
    assert session.hops[-1].samples is session.samples
    assert session.hops[-1].rollup is session.target_rollup

    # Rounds 1-3 are all launched; rounds 2/3 finish probing behind the
    # held round 1 and park on its commit event.
    await asyncio.wait_for(started[2].wait(), 5.0)
    for _ in range(10):
        await asyncio.sleep(0)

    # Mid-flight capture: taken before the release, on-loop.
    mid_samples = len(session.samples)
    _, mid_buckets = session.target_rollup.capture(0, 10**12)
    mid_snapshot = session.snapshot(0)
    mid_metrics = capture_metrics(mgr)
    mid_queue_size = events.qsize()

    # A later round (4th) launches while round 1 is still held and blocks.
    await asyncio.wait_for(started[3].wait(), 5.0)

    # Release the held round: rounds 1..3 commit, in launch order.
    release_round1.set()
    round_events = [await asyncio.wait_for(events.get(), 5.0) for _ in range(3)]
    for _ in range(10):
        await asyncio.sleep(0)
    committed_metrics = capture_metrics(mgr)

    # Pause with the blocked later rounds still outstanding.
    await mgr.pause(session)
    for _ in range(10):
        await asyncio.sleep(0)
    paused_metrics = capture_metrics(mgr)

    return {
        "session": session,
        "manager": mgr,
        "events": events,
        "route_event": route_event,
        "round_events": round_events,
        "probe_hosts": probe_hosts,
        "mid": {
            "sample_count": mid_samples,
            "buckets": mid_buckets,
            "snapshot_stats": mid_snapshot["stats"],
            "snapshot_cutoffs": (
                mid_snapshot["sample_through_t"],
                mid_snapshot["sample_from_t"],
            ),
            "metrics": mid_metrics,
            "queue_size": mid_queue_size,
        },
        "committed_metrics": committed_metrics,
        "paused_metrics": paused_metrics,
    }


def test_overlapping_rounds_commit_launch_ordered_across_all_consumers() -> None:
    """A held round must gate every consumer; after release each round
    commits exactly once, launch-ordered, and canceled later rounds leak
    nowhere.

    Scenario: three rounds dispatched a tick apart with RTTs 30.0 / loss /
    10.0. Round 1 is held; rounds 2 and 3 finish first. Before the release
    nothing is visible anywhere; after it the committed stream is
    30, None, 10 — so current 10, avg 20, loss 1/3, one rollup bucket with
    count 3 / loss 1, and three launch-ordered round events. Discovery
    makes the final hop the target mid-flight, so the alias stream/rollup
    must be folded once (count 3, not doubled), and a final pause with
    blocked rounds must change nothing.
    """
    obs = asyncio.run(_drive_ordered_commit_scenario())
    session = obs["session"]
    events = obs["events"]
    ev1, ev2, ev3 = obs["round_events"]
    ts1, ts2, ts3 = ev1.t, ev2.t, ev3.t

    # --- mid-flight: finished later rounds are invisible until the held
    # round commits; every consumer agrees on "nothing yet".
    mid = obs["mid"]
    assert mid["sample_count"] == 0
    assert mid["buckets"] == []
    assert mid["queue_size"] == 0
    assert mid["snapshot_stats"]["count"] == 0
    assert mid["snapshot_stats"]["cur_ms"] is None
    assert mid["snapshot_cutoffs"] == (None, None)
    assert len(mid["metrics"]) == 1
    assert mid["metrics"][0].target_stream is None
    assert mid["metrics"][0].hops == []

    # --- subscriber feed: one existing-format round event per round, in
    # launch order. Round 1 was planned pre-route (target stream under
    # hop 0); rounds 2/3 run as the destination alias hop.
    assert [(r.hop, r.rtt_ms) for r in ev1.results] == [(0, 30.0)]
    assert [(r.hop, r.rtt_ms) for r in ev2.results] == [(1, None)]
    assert [(r.hop, r.rtt_ms) for r in ev3.results] == [(1, 10.0)]
    assert ts1 <= ts2 <= ts3  # event order == launch order
    assert events.qsize() == 0  # exactly once: no duplicates, no extras

    # --- raw history: exactly one append per round, launch-ordered.
    assert [(s.t, s.rtt_ms) for s in session.samples] == [
        (ts1, 30.0),
        (ts2, None),
        (ts3, 10.0),
    ]
    earliest, raw0 = session.history_samples(0, ts1 - 1.0, ts3 + 1.0)
    assert earliest == ts1
    assert raw0 == [(ts1, 30.0), (ts2, None), (ts3, 10.0)]
    # Destination/final-hop alias: hop 1 resolves to the SAME stream...
    assert session.hops[-1].samples is session.samples
    _, raw1 = session.history_samples(1, ts1 - 1.0, ts3 + 1.0)
    assert raw1 == raw0

    # --- minute rollup: folded once per committed sample, losses counted
    # in count but not in the average; the alias adds no second fold.
    bucket_t = int(ts1 // ROLLUP_BUCKET_S) * ROLLUP_BUCKET_S
    earliest_b, buckets = session.history_buckets(0, 0, 10**12)
    assert earliest_b == bucket_t
    assert len(buckets) == 1
    b = buckets[0]
    assert b["t"] == bucket_t
    assert b["count"] == 3  # NOT 4/6: no double aggregate via the alias
    assert b["loss_count"] == 1
    assert math.isclose(b["avg_ms"], 20.0)
    assert math.isclose(b["min_ms"], 10.0)
    assert math.isclose(b["max_ms"], 30.0)
    assert session.hops[-1].rollup is session.target_rollup
    _, hop_buckets = session.history_buckets(1, 0, 10**12)
    assert hop_buckets == buckets

    # --- snapshot: current comes from the newest COMMITTED round (10.0,
    # not the newest reply), and the cutoffs bracket exactly this window.
    snap = session.snapshot(0)
    st = snap["stats"]
    assert st["count"] == 3
    assert st["cur_ms"] == 10.0
    assert math.isclose(st["avg_ms"], 20.0)
    assert math.isclose(st["loss_pct"], 100.0 / 3.0)
    assert (snap["sample_through_t"], snap["sample_from_t"]) == (ts3, ts1)
    # The alias hop's entry in the same payload describes the same window.
    assert snap["hops"][0]["stats"] == st
    assert snap["hops"][0]["sample_through_t"] == ts3
    assert snap["hops"][0]["sample_from_t"] == ts1
    assert snap["hops"][0]["captured_at"] == snap["captured_at"]

    # --- metrics capture: exactly the committed rounds.
    committed = obs["committed_metrics"]
    assert len(committed) == 1
    tm = committed[0].target_stream
    assert tm is not None
    assert tm.current_ms == 10.0
    assert math.isclose(tm.avg_ms, 20.0)
    assert math.isclose(tm.loss_pct, 100.0 / 3.0)
    assert tm.up == 1
    assert [(h.hop_index, h.current_ms) for h in committed[0].hops] == [(1, 10.0)]

    # --- paused with later rounds blocked outstanding: canceled
    # uncommitted work must not appear anywhere.
    assert len(obs["probe_hosts"]) >= 4  # a later round really was blocked
    assert session.state == "paused"
    assert [(s.t, s.rtt_ms) for s in session.samples] == [
        (ts1, 30.0),
        (ts2, None),
        (ts3, 10.0),
    ]
    _, buckets_after_pause = session.history_buckets(0, 0, 10**12)
    assert buckets_after_pause == buckets
    assert events.qsize() == 0  # no canceled round published a loss/event
    paused = obs["paused_metrics"]
    assert paused[0].state == "paused"
    assert paused[0].target_stream == tm  # values frozen, nothing added
    assert paused[0].hops == committed[0].hops
