"""Unit tests for the session manager and trace session bookkeeping."""

from __future__ import annotations

import asyncio
import itertools
import time

import pytest

from netplot.session import (
    _SESSION_CLOSED,
    DEFAULT_FOCUS_S,
    MAX_SESSIONS,
    SAMPLE_CAP,
    SNAPSHOT_TAIL,
    Hop,
    ProbeResult,
    RoundEvent,
    Sample,
    SessionLimitReached,
    SessionManager,
    TraceSession,
)

# -------------------------------------------------------------------- helpers


def _make_session(
    target: str = "example.com",
    interval_s: float = 2.5,
) -> TraceSession:
    return TraceSession(
        id="abc123",
        target=target,
        interval_s=interval_s,
    )


async def _failing_discover(_target: str, _timeout_s: float = 90.0):
    """A discover_fn stand-in that fails immediately so unit tests do not
    spawn a real traceroute against ``example.com`` / loopback."""
    from netplot.engine import RouteDiscoveryError

    raise RouteDiscoveryError("disabled in unit test")


async def _fast_resolve_name(_ip: str) -> None:
    """Skip real reverse DNS in unit tests."""
    return


# ----------------------------------------------------------------- stats math


def test_stats_empty_returns_zero_count_and_none_avgs() -> None:
    session = _make_session()
    stats = session.stats(focus_s=DEFAULT_FOCUS_S)
    assert stats == {
        "count": 0,
        "avg_ms": None,
        "min_ms": None,
        "cur_ms": None,
        "loss_pct": 0.0,
    }


def test_stats_excludes_losses_from_avg_and_min() -> None:
    session = _make_session()
    now = time.time()
    # Sequence ends with a loss so we can also assert cur_ms picks up the
    # most recent sample's rtt (None) rather than the last success.
    for rtt in (10.0, None, 20.0, None, 30.0, None):
        session.samples.append(Sample(t=now, rtt_ms=rtt))

    stats = session.stats(focus_s=0)  # all samples
    assert stats["count"] == 6
    assert stats["avg_ms"] == pytest.approx(20.0)  # (10+20+30)/3
    assert stats["min_ms"] == pytest.approx(10.0)
    assert stats["loss_pct"] == pytest.approx(50.0)  # 3/6
    assert stats["cur_ms"] is None  # last sample was a loss


def test_stats_cur_is_last_sample_rtt_even_if_loss() -> None:
    session = _make_session()
    now = time.time()
    session.samples.append(Sample(t=now, rtt_ms=5.0))
    session.samples.append(Sample(t=now, rtt_ms=None))

    stats = session.stats(focus_s=0)
    assert stats["cur_ms"] is None  # last sample is the loss


def test_stats_focus_window_filters_old_samples() -> None:
    session = _make_session()
    now = time.time()
    # 5 minutes ago — outside the 60 s focus window
    session.samples.append(Sample(t=now - 300, rtt_ms=100.0))
    # recent — inside the focus window
    session.samples.append(Sample(t=now - 10, rtt_ms=5.0))
    session.samples.append(Sample(t=now - 5, rtt_ms=10.0))

    stats = session.stats(focus_s=60)
    assert stats["count"] == 2
    assert stats["avg_ms"] == pytest.approx(7.5)
    assert stats["min_ms"] == pytest.approx(5.0)
    assert stats["cur_ms"] == pytest.approx(10.0)
    assert stats["loss_pct"] == pytest.approx(0.0)


def test_stats_focus_zero_means_all() -> None:
    session = _make_session()
    now = time.time()
    session.samples.append(Sample(t=now - 9999, rtt_ms=1.0))
    stats = session.stats(focus_s=0)
    assert stats["count"] == 1


def test_snapshot_caps_returned_samples_at_snAPSHOT_tail() -> None:
    session = _make_session()
    now = time.time()
    # Timestamps ascend but END at ``now`` — at/before the snapshot's
    # capture-time anchor regardless of how long this insert loop takes.
    for i in range(SNAPSHOT_TAIL + 50):
        session.samples.append(
            Sample(t=now - (SNAPSHOT_TAIL + 50 - i) * 0.001, rtt_ms=float(i))
        )

    snap = session.snapshot(focus_s=0)
    assert len(snap["samples"]) == SNAPSHOT_TAIL
    # The tail is the most recent SNAPSHOT_TAIL samples.
    assert snap["samples"][0]["rtt_ms"] == pytest.approx(50.0)
    assert snap["samples"][-1]["rtt_ms"] == pytest.approx(float(SNAPSHOT_TAIL + 49))
    assert snap["stats"]["count"] == SNAPSHOT_TAIL + 50


def test_snapshot_payload_shape() -> None:
    session = _make_session(target="example.com")
    sample_t = time.time()
    session.append_sample(1.0, t=sample_t)

    snap = session.snapshot(focus_s=60.0)
    assert set(snap.keys()) == {
        "id",
        "target",
        "target_ip",
        "target_dns",
        "state",
        "paused_at",
        "route_state",
        "focus_s",
        "name",
        "persistent",
        "captured_at",
        "sample_through_t",
        "sample_from_t",
        "hops",
        "stats",
        "samples",
    }
    assert snap["target"] == "example.com"
    assert snap["state"] == "running"
    assert snap["paused_at"] is None
    assert snap["route_state"] == "pending"
    assert snap["target_ip"] is None
    assert snap["target_dns"] is None
    assert snap["name"] is None
    assert snap["persistent"] is False
    assert snap["hops"] == []
    assert snap["focus_s"] == 60.0
    # Snapshot cutoff contract: ``captured_at`` is the single effective
    # epoch the window was selected against (time.time() while running,
    # frozen paused_at while paused); ``sample_from_t`` /
    # ``sample_through_t`` are the extrema of the exact stats window
    # (equal to the one sample here; null for empty streams).
    assert isinstance(snap["captured_at"], float)
    assert snap["sample_from_t"] == pytest.approx(sample_t)
    assert snap["sample_through_t"] == pytest.approx(sample_t)
    assert set(snap["stats"].keys()) == {
        "count",
        "avg_ms",
        "min_ms",
        "cur_ms",
        "loss_pct",
    }
    assert snap["samples"][0]["rtt_ms"] == pytest.approx(1.0)


# ---------------------------------------------------------------- deque bound


def test_sample_buffer_is_bounded() -> None:
    session = _make_session()
    now = time.time()
    for i in range(SAMPLE_CAP + 200):
        session.samples.append(Sample(t=now, rtt_ms=float(i)))

    assert len(session.samples) == SAMPLE_CAP
    # Oldest samples dropped; the buffer holds the most recent SAMPLE_CAP.
    assert session.samples[0].rtt_ms == pytest.approx(float(200))


# -------------------------------------------------------------- summary shape


def test_summary_shape_matches_api_contract() -> None:
    session = _make_session(target="1.2.3.4", interval_s=1.5)
    session.append_sample(1.0, t=time.time())

    summary = session.summary()
    assert set(summary.keys()) == {
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
    assert summary["target"] == "1.2.3.4"
    assert summary["interval_s"] == 1.5
    assert summary["state"] == "running"
    assert summary["paused_at"] is None
    assert summary["sample_count"] == 1
    assert summary["name"] is None
    assert summary["persistent"] is False


# --------------------------------------------------------- publish/subscribe


def test_append_sample_publishes_to_subscribers() -> None:
    session = _make_session()
    q1 = session.subscribe()
    q2 = session.subscribe()

    session.append_sample(12.3, t=1000.0)

    e1 = asyncio.run(q1.get())
    e2 = asyncio.run(q2.get())
    # append_sample publishes a RoundEvent with one target result (hop=0).
    assert isinstance(e1, RoundEvent)
    assert isinstance(e2, RoundEvent)
    assert e1.t == pytest.approx(1000.0)
    assert e2.t == pytest.approx(1000.0)
    assert e1.results == [ProbeResult(hop=0, rtt_ms=12.3)]
    assert e2.results == [ProbeResult(hop=0, rtt_ms=12.3)]


def test_unsubscribe_stops_delivery() -> None:
    session = _make_session()
    q = session.subscribe()
    session.unsubscribe(q)

    session.append_sample(1.0, t=1.0)
    assert q.empty()


def test_slow_subscriber_queue_is_bounded_and_close_is_deliverable() -> None:
    session = _make_session()
    queue = session.subscribe()

    for index in range(queue.maxsize + 100):
        session.append_sample(float(index), t=float(index))

    assert queue.qsize() == queue.maxsize
    session.close()
    assert queue.get_nowait() is _SESSION_CLOSED


# ----------------------------------------------------------- manager lifecycle


def _never_completing_probe(*_a, **_kw):
    async def _probe(*_args, **_kwargs):
        await asyncio.sleep(100)

    return _probe()


def test_manager_create_and_get() -> None:
    mgr = SessionManager(
        discover_fn=_failing_discover, resolve_name_fn=_fast_resolve_name
    )
    session = mgr.create("example.com", interval_s=2.0)
    assert session.target == "example.com"
    assert session.interval_s == 2.0
    assert session.state == "running"  # default until start() called
    assert session.id in [s.id for s in mgr.list()]
    assert mgr.get(session.id) is session
    assert mgr.get("nonexistent") is None


def test_manager_pause_cancels_task_and_sets_state() -> None:
    async def run():
        mgr = SessionManager(
            discover_fn=_failing_discover, resolve_name_fn=_fast_resolve_name
        )
        session = mgr.create("example.com", interval_s=0.1)
        await mgr.start(session)
        assert session._task is not None
        assert not session._task.done()

        await mgr.pause(session)
        assert session._task is None
        assert session.state == "paused"

    asyncio.run(run())


def test_manager_resume_restarts_task() -> None:
    async def run():
        mgr = SessionManager(
            discover_fn=_failing_discover, resolve_name_fn=_fast_resolve_name
        )
        session = mgr.create("example.com", interval_s=0.1)
        await mgr.start(session)
        await mgr.pause(session)
        assert session.state == "paused"

        await mgr.resume(session)
        assert session.state == "running"
        assert session._task is not None
        await mgr.pause(session)  # cleanup

    asyncio.run(run())


# ----------------------------------------------------------- paused_at field
#
# ``paused_at`` is the authoritative epoch at which the session last entered
# the paused state. It is recorded by ``SessionManager.pause`` when the
# pause completes, cleared by ``start`` (the resume / fresh-start path), and
# surfaced in both ``summary`` and ``snapshot`` so every client attached to
# a session can anchor its paused-window freeze to the same server instant
# rather than guessing from sample timestamps (which can be absent, stale,
# or anchored to a wrong/future time).


def test_paused_at_is_none_on_a_fresh_running_session() -> None:
    """A never-paused session exposes ``paused_at=None`` everywhere."""
    session = _make_session()
    assert session.paused_at is None
    assert session.summary()["paused_at"] is None
    assert session.snapshot(focus_s=0)["paused_at"] is None


def test_pause_records_paused_at_and_summary_snapshot_expose_it() -> None:
    """``manager.pause`` stamps ``paused_at`` at the pause instant and the
    value flows through ``summary`` and ``snapshot``."""

    async def run():
        mgr = SessionManager(
            discover_fn=_failing_discover, resolve_name_fn=_fast_resolve_name
        )
        session = mgr.create("example.com", interval_s=0.1)
        await mgr.start(session)
        assert session.paused_at is None

        before = time.time()
        await mgr.pause(session)
        after = time.time()

        assert session.state == "paused"
        assert session.paused_at is not None
        # Recorded at the moment pause completed — within a tight window
        # straddling the pause call (not a future value, not stale).
        assert before <= session.paused_at <= after

        # summary() and snapshot() carry the same epoch verbatim.
        assert session.summary()["paused_at"] == session.paused_at
        assert session.snapshot(focus_s=0)["paused_at"] == session.paused_at

    asyncio.run(run())


def test_resume_clears_paused_at() -> None:
    """Resuming a paused session clears ``paused_at`` (probing restarted)."""

    async def run():
        mgr = SessionManager(
            discover_fn=_failing_discover, resolve_name_fn=_fast_resolve_name
        )
        session = mgr.create("example.com", interval_s=0.1)
        await mgr.start(session)
        await mgr.pause(session)
        assert session.paused_at is not None

        await mgr.resume(session)
        assert session.state == "running"
        assert session.paused_at is None
        assert session.summary()["paused_at"] is None
        await mgr.pause(session)  # cleanup

    asyncio.run(run())


def test_pause_then_pause_again_refreshes_paused_at() -> None:
    """A second pause cycle (resume in between) stamps a fresh ``paused_at``
    rather than retaining the stale first-pause epoch."""

    async def run():
        mgr = SessionManager(
            discover_fn=_failing_discover, resolve_name_fn=_fast_resolve_name
        )
        session = mgr.create("example.com", interval_s=0.1)
        await mgr.start(session)
        await mgr.pause(session)
        first = session.paused_at
        assert first is not None

        await asyncio.sleep(0.02)
        await mgr.resume(session)
        await asyncio.sleep(0.02)
        await mgr.pause(session)
        second = session.paused_at

        assert second is not None
        # A genuinely later pause must record a strictly later epoch.
        assert second > first

    asyncio.run(run())


def test_manager_delete_removes_session_and_cancels_task() -> None:
    async def run():
        mgr = SessionManager(
            discover_fn=_failing_discover, resolve_name_fn=_fast_resolve_name
        )
        session = mgr.create("example.com", interval_s=0.1)
        await mgr.start(session)

        await mgr.delete(session)
        assert mgr.get(session.id) is None
        assert session._task is None

    asyncio.run(run())


def test_manager_shutdown_cancels_all_tasks() -> None:
    async def run():
        mgr = SessionManager(
            discover_fn=_failing_discover, resolve_name_fn=_fast_resolve_name
        )
        s1 = mgr.create("a.example", interval_s=0.1)
        s2 = mgr.create("b.example", interval_s=0.1)
        await mgr.start(s1)
        await mgr.start(s2)
        assert s1._task is not None and s2._task is not None

        await mgr.shutdown()
        assert s1._task is None and s2._task is None

    asyncio.run(run())


# --------------------------------------------- multi-trace session cap (P1)
#
# PingPlotter-style multi-trace tabs cap the registry so a single client
# cannot squat unlimited concurrent probe loops. ``SessionManager.create``
# raises SessionLimitReached once MAX_SESSIONS is reached.


def test_max_sessions_constant_is_a_module_level_cap() -> None:
    # The cap is a module constant so it can be referenced by docs and tests
    # without constructing a manager. Sanity-check its value is in a sensible
    # range (the spec adopts 8; the exact value may be tuned but must be
    # small enough to bound resource use and large enough for typical use).
    assert MAX_SESSIONS == 8


def test_manager_create_returns_sessions_below_cap() -> None:
    mgr = SessionManager(
        discover_fn=_failing_discover, resolve_name_fn=_fast_resolve_name
    )
    for i in range(MAX_SESSIONS):
        s = mgr.create(f"target{i}.example", interval_s=1.0)
        assert mgr.get(s.id) is s
    assert len(mgr.list()) == MAX_SESSIONS


def test_manager_create_raises_session_limit_reached_at_cap() -> None:
    mgr = SessionManager(
        discover_fn=_failing_discover, resolve_name_fn=_fast_resolve_name
    )
    for i in range(MAX_SESSIONS):
        mgr.create(f"target{i}.example", interval_s=1.0)
    # The next create must raise; nothing is appended past the cap.
    with pytest.raises(SessionLimitReached) as exc_info:
        mgr.create("overflow.example", interval_s=1.0)
    err = exc_info.value
    assert err.count == MAX_SESSIONS
    assert err.cap == MAX_SESSIONS
    assert len(mgr.list()) == MAX_SESSIONS


def test_manager_create_recovers_after_a_session_is_deleted() -> None:
    """Closing a tab frees a slot — the next create() must succeed."""
    mgr = SessionManager(
        discover_fn=_failing_discover, resolve_name_fn=_fast_resolve_name
    )
    sessions = [
        mgr.create(f"t{i}.example", interval_s=1.0) for i in range(MAX_SESSIONS)
    ]
    with pytest.raises(SessionLimitReached):
        mgr.create("overflow.example", interval_s=1.0)
    # Delete one; the next create must succeed and re-fill the slot.
    asyncio.run(mgr.delete(sessions[0]))
    again = mgr.create("again.example", interval_s=1.0)
    assert again.target == "again.example"
    assert len(mgr.list()) == MAX_SESSIONS


def test_probe_loop_uses_injected_probe_fn_and_appends_sample() -> None:
    """Drive the probe loop with a fake that returns 7.5 ms immediately."""
    calls: list = []

    async def fake_probe(host: str, timeout_s: float) -> float | None:
        calls.append((host, timeout_s))
        return 7.5

    async def run():
        mgr = SessionManager(
            probe_fn=fake_probe,
            discover_fn=_failing_discover,
            resolve_name_fn=_fast_resolve_name,
            probe_timeout_s=2.5,
        )
        session = mgr.create("example.com", interval_s=0.05)
        await mgr.start(session)
        # Give at least one probe round a chance to fire.
        await asyncio.sleep(0.15)
        await mgr.pause(session)
        return session

    session = asyncio.run(run())
    assert len(calls) >= 1
    assert calls[0] == ("example.com", 2.5)
    assert len(session.samples) >= 1
    assert session.samples[0].rtt_ms == pytest.approx(7.5)


def test_probe_loop_normalizes_none_samples_without_raising() -> None:
    async def fake_probe(_host: str, _t: float) -> float | None:
        return None

    async def run():
        mgr = SessionManager(
            probe_fn=fake_probe,
            discover_fn=_failing_discover,
            resolve_name_fn=_fast_resolve_name,
            probe_timeout_s=1.0,
        )
        session = mgr.create("lossy.example", interval_s=0.05)
        await mgr.start(session)
        await asyncio.sleep(0.15)
        await mgr.pause(session)
        return session

    session = asyncio.run(run())
    assert len(session.samples) >= 1
    assert all(s.rtt_ms is None for s in session.samples)


# ============================================================ Slice 3 testing
#
# Hop dataclass, per-hop stats, route discovery wiring into the probe loop,
# target/final-hop dedup, silent-hop preservation, and concurrent probing.


async def _fast_resolve_target_discover(target: str, timeout_s: float = 90.0):
    """A discover_fn stand-in that returns immediately so unit tests do not
    spawn a real traceroute. Caller can override the returned hop list via a
    closure."""
    return []


def _mgr_with_hops(
    hops_to_return,
    *,
    probe_fn=None,
    delay_s: float = 0.0,
    target_ip: str | None = "target-ip",
):
    """Build a SessionManager whose discovery task returns ``hops_to_return``
    after sleeping ``delay_s`` seconds. Used to test probe-loop / discovery
    interactions without touching real traceroute or DNS."""

    async def fake_discover(_target, _timeout_s=90.0):
        if delay_s:
            await asyncio.sleep(delay_s)
        return list(hops_to_return)

    async def fake_probe(host, _timeout_s):
        if probe_fn is not None:
            return await probe_fn(host, _timeout_s)
        return 1.0

    class ResolvedSessionManager(SessionManager):
        async def _resolve_target(self, _target):
            return target_ip, None

    return ResolvedSessionManager(
        probe_fn=fake_probe,
        discover_fn=fake_discover,
        resolve_name_fn=_fast_resolve_name,
        probe_timeout_s=1.0,
    )


def test_hop_dataclass_default_samples_deque_is_bounded() -> None:
    hop = Hop(index=1, ip="10.0.0.1", dns=None)
    assert hop.samples.maxlen == SAMPLE_CAP
    assert len(hop.samples) == 0


def test_stats_for_all_loss_silent_hop_returns_100pct_loss() -> None:
    # A silent hop accumulates only loss samples; avg/min must be None.
    session = _make_session()
    session.hops = [Hop(index=1, ip=None, dns=None)]
    now = time.time()
    for _ in range(5):
        session.hops[0].samples.append(Sample(t=now, rtt_ms=None))

    stats = session.hop_stats(hop_index=1, focus_s=0)
    assert stats == {
        "count": 5,
        "avg_ms": None,
        "min_ms": None,
        "cur_ms": None,
        "loss_pct": pytest.approx(100.0),
    }


def test_hop_stats_returns_none_for_unknown_index() -> None:
    session = _make_session()
    session.hops = [Hop(index=1, ip="10.0.0.1", dns=None)]
    assert session.hop_stats(hop_index=99, focus_s=0) is None


def test_hop_stats_focus_window_filters_old_samples() -> None:
    session = _make_session()
    session.hops = [Hop(index=1, ip="10.0.0.1", dns=None)]
    now = time.time()
    session.hops[0].samples.append(Sample(t=now - 300, rtt_ms=100.0))
    session.hops[0].samples.append(Sample(t=now - 5, rtt_ms=5.0))

    stats = session.hop_stats(hop_index=1, focus_s=60)
    assert stats is not None
    assert stats["count"] == 1
    assert stats["avg_ms"] == pytest.approx(5.0)


def test_snapshot_hop_returns_recent_samples_for_hop() -> None:
    session = _make_session()
    session.hops = [Hop(index=1, ip="10.0.0.1", dns="router")]
    now = time.time()
    # Timestamps END at ``now`` — at/before the snapshot anchor (a stamp
    # past the capture instant would be future-dated and excluded).
    for i in range(3):
        session.hops[0].samples.append(Sample(t=now - 2 + i, rtt_ms=float(i)))

    payload = session.snapshot_hop(hop_index=1, focus_s=0)
    assert payload is not None
    assert payload["hop"] == 1
    assert payload["ip"] == "10.0.0.1"
    assert payload["dns"] == "router"
    assert payload["stats"]["count"] == 3
    assert len(payload["samples"]) == 3
    assert [s["rtt_ms"] for s in payload["samples"]] == [0.0, 1.0, 2.0]


def test_snapshot_hop_returns_none_for_unknown_hop() -> None:
    session = _make_session()
    assert session.snapshot_hop(hop_index=7, focus_s=0) is None


def test_snapshot_includes_hops_with_stats() -> None:
    session = _make_session(target="1.2.3.4")
    session.hops = [
        Hop(index=1, ip="10.0.0.1", dns="hop1"),
        Hop(index=2, ip=None, dns=None),
    ]
    session.route_state = "ready"
    now = time.time()
    session.hops[0].samples.append(Sample(t=now, rtt_ms=1.0))
    session.hops[1].samples.append(Sample(t=now, rtt_ms=None))  # silent loss

    snap = session.snapshot(focus_s=0)
    assert snap["route_state"] == "ready"
    assert len(snap["hops"]) == 2
    h0, h1 = snap["hops"]
    assert h0["index"] == 1 and h0["ip"] == "10.0.0.1" and h0["dns"] == "hop1"
    assert h0["stats"]["count"] == 1 and h0["stats"]["avg_ms"] == pytest.approx(1.0)
    assert h1["index"] == 2 and h1["ip"] is None
    # Silent hop's only sample is a loss.
    assert h1["stats"]["count"] == 1 and h1["stats"]["loss_pct"] == pytest.approx(100.0)


def test_snapshot_anchor_excludes_future_samples_and_includes_exact_edge() -> None:
    """A snapshot's captured_at anchor is a hard right edge: samples newer
    than it reach no stats, cutoff, or sample list, while a sample exactly
    at the anchor is included. focus=0 keeps every retained sample at/before
    the anchor — however old — but never the future-dated record. The
    ordinary (no-explicit-end) focus=0 stats path keeps its all-retained
    semantics, future record and all."""
    session = _make_session()
    anchor = time.time() - 120.0
    session.state = "paused"  # freeze the capture anchor like a paused trace
    session.paused_at = anchor
    session.append_sample(1.0, t=anchor - 300)
    session.append_sample(2.0, t=anchor - 10)
    session.append_sample(3.0, t=anchor)  # exact-anchor boundary → in
    session.append_sample(9.0, t=anchor + 30)  # future-dated → out

    snap = session.snapshot(focus_s=60.0)
    assert snap["captured_at"] == pytest.approx(anchor)
    assert snap["stats"]["count"] == 2
    assert snap["stats"]["cur_ms"] == pytest.approx(3.0)
    assert snap["sample_through_t"] == pytest.approx(anchor)
    assert snap["sample_from_t"] == pytest.approx(anchor - 10)
    assert all(s["t"] <= anchor for s in snap["samples"])

    wide = session.snapshot(focus_s=0)
    assert wide["stats"]["count"] == 3  # all retained at/before the anchor
    assert wide["sample_through_t"] == pytest.approx(anchor)
    assert wide["sample_from_t"] == pytest.approx(anchor - 300)
    assert all(s["t"] <= anchor for s in wide["samples"])

    # Ordinary stats carry no anchor: focus=0 still counts the whole
    # retained stream, future-dated record included.
    assert session.stats(focus_s=0)["count"] == 4


# ------------------------------------------- discovery + probe loop wiring


def test_discovery_injects_hops_into_running_loop_without_stalling() -> None:
    """Discovery completes asynchronously; the probe loop must continue
    probing the target during discovery (Slice 2 behaviour preserved)."""

    async def fake_probe(host, _timeout):
        return 7.5

    async def run():
        from netplot.engine import RouteHop

        mgr = _mgr_with_hops(
            [RouteHop(index=1, ip="10.0.0.1")],
            probe_fn=fake_probe,
            delay_s=0.2,
        )
        session = mgr.create("target.example", interval_s=0.05)
        session.target_ip = "target-ip"  # avoid real resolver dependency
        await mgr.start(session)
        # Discovery is still mid-flight at this point — assert target was
        # probed during the discovery wait.
        await asyncio.sleep(0.1)
        assert len(session.samples) >= 1, "target probe stalled during discovery"
        # Now let discovery complete.
        await asyncio.sleep(0.25)
        await mgr.pause(session)
        return session

    session = asyncio.run(run())
    assert session.route_state == "ready"
    assert len(session.hops) == 1
    assert session.hops[0].ip == "10.0.0.1"


def test_discovery_failure_degrades_to_target_only() -> None:
    """A RouteDiscoveryError from discover_fn sets route_state=failed but the
    probe loop keeps producing target samples."""

    async def fake_probe(host, _timeout):
        return 5.0

    async def failing_discover(_target, _timeout_s=90.0):
        from netplot.engine import RouteDiscoveryError

        raise RouteDiscoveryError("simulated traceroute failure")

    async def run():
        mgr = SessionManager(
            probe_fn=fake_probe,
            discover_fn=failing_discover,
            resolve_name_fn=_fast_resolve_name,
            probe_timeout_s=1.0,
        )
        session = mgr.create("target.example", interval_s=0.05)
        await mgr.start(session)
        await asyncio.sleep(0.15)
        await mgr.pause(session)
        return session

    session = asyncio.run(run())
    assert session.route_state == "failed"
    assert session.route_error
    assert session.hops == []
    # Probe loop kept running.
    assert len(session.samples) >= 1


def test_discovery_unexpected_exception_also_degrades() -> None:
    """Any non-RouteDiscoveryError exception from discover_fn still degrades
    rather than crashing the session."""

    async def fake_probe(host, _timeout):
        return 5.0

    async def buggy_discover(_target, _timeout_s=90.0):
        raise RuntimeError("oops")

    async def run():
        mgr = SessionManager(
            probe_fn=fake_probe,
            discover_fn=buggy_discover,
            resolve_name_fn=_fast_resolve_name,
            probe_timeout_s=1.0,
        )
        session = mgr.create("target.example", interval_s=0.05)
        await mgr.start(session)
        await asyncio.sleep(0.15)
        await mgr.pause(session)
        return session

    session = asyncio.run(run())
    assert session.route_state == "failed"
    assert "oops" in (session.route_error or "")


def test_target_dedup_when_final_hop_equals_target() -> None:
    """When the route's final hop IP equals the resolved target IP, the
    target is probed only once (as that hop) — not as both target and hop.
    Their sample deques become the same object."""

    probed: list = []

    async def fake_probe(host, _timeout):
        probed.append(host)
        return 1.0

    async def run():
        from netplot.engine import RouteHop

        mgr = _mgr_with_hops(
            [RouteHop(index=1, ip="127.0.0.1")],
            probe_fn=fake_probe,
            target_ip="127.0.0.1",
        )
        session = mgr.create("127.0.0.1", interval_s=0.05)
        await mgr.start(session)
        # Let at least one round fire after discovery completes.
        await asyncio.sleep(0.2)
        await mgr.pause(session)
        return session

    session = asyncio.run(run())
    assert session.route_state == "ready"
    assert len(session.hops) == 1
    assert session.hops[0].ip == "127.0.0.1"
    # Only the final hop (= target) was probed; never double-probed.
    assert all(h == "127.0.0.1" for h in probed)
    # Target's deque is literally the final hop's deque.
    assert session.hops[0].samples is session.samples
    # And both have at least one sample.
    assert len(session.samples) >= 1
    assert len(session.hops[0].samples) == len(session.samples)


def test_target_probed_separately_when_route_does_not_reach_target() -> None:
    """When the final hop's IP != target IP, target is probed alongside hops
    (no dedup)."""

    probed: set = set()

    async def fake_probe(host, _timeout):
        probed.add(host)
        return 1.0

    async def run():
        from netplot.engine import RouteHop

        mgr = _mgr_with_hops(
            [RouteHop(index=1, ip="10.0.0.1")],  # final hop != target
            probe_fn=fake_probe,
        )
        session = mgr.create("example.com", interval_s=0.05)
        session.target_ip = "93.184.216.34"  # different from final hop
        await mgr.start(session)
        await asyncio.sleep(0.2)
        await mgr.pause(session)
        return session

    session = asyncio.run(run())
    # Both the hop IP and the target name were probed.
    assert "10.0.0.1" in probed
    assert "example.com" in probed
    # Sample buffers are distinct objects.
    assert session.hops[0].samples is not session.samples


def test_silent_hop_records_loss_samples_each_round() -> None:
    """A silent hop (ip=None) is preserved in the hop list and gets a loss
    sample appended on every probe round; it is never collapsed."""

    async def fake_probe(host, _timeout):
        # Only non-silent hops are actually probed.
        return 1.0

    async def run():
        from netplot.engine import RouteHop

        mgr = _mgr_with_hops(
            [
                RouteHop(index=1, ip="10.0.0.1"),
                RouteHop(index=2, ip=None),  # silent
                RouteHop(index=3, ip="10.0.0.3"),
            ],
            probe_fn=fake_probe,
            target_ip="10.0.0.99",  # distinct from final hop -> target also probed
        )
        session = mgr.create("target", interval_s=0.05)
        events = session.subscribe()
        try:
            await mgr.start(session)

            async def wait_for_silent_hop_round():
                while True:
                    event = await events.get()
                    if isinstance(event, RoundEvent) and any(
                        result.hop == 2 for result in event.results
                    ):
                        return

            await asyncio.wait_for(wait_for_silent_hop_round(), timeout=2.0)
        finally:
            await mgr.pause(session)
            session.unsubscribe(events)
        return session

    session = asyncio.run(run())
    assert len(session.hops) == 3
    silent = session.hops[1]
    assert silent.ip is None
    assert len(silent.samples) >= 1
    # All samples on the silent hop are losses.
    assert all(s.rtt_ms is None for s in silent.samples)


def test_round_probes_all_hops_concurrently() -> None:
    """Within one probe round, all hop probes run in parallel; total round
    time is bounded by the slowest probe, not the sum."""

    in_flight = 0
    max_in_flight = 0

    async def slow_probe(host, _timeout):
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.05)
        in_flight -= 1
        return 1.0

    async def run():
        from netplot.engine import RouteHop

        hops = [RouteHop(index=i, ip=f"10.0.0.{i}") for i in range(1, 5)]
        mgr = _mgr_with_hops(hops, probe_fn=slow_probe, target_ip="10.0.0.99")
        session = mgr.create("target", interval_s=0.1)
        await mgr.start(session)
        # Let discovery complete and at least one multi-hop round fire.
        await asyncio.sleep(0.4)
        await mgr.pause(session)

    asyncio.run(run())
    # 4 hops + 1 target = 5 concurrent probes if gather runs them in parallel.
    assert max_in_flight > 1, "probes did not run concurrently"


def test_pause_cancels_discovery_task() -> None:
    """Pause must cancel a still-running discovery task, not strand it."""

    paused_cleanly = {}

    async def run():
        from netplot.engine import RouteHop

        mgr = _mgr_with_hops(
            [RouteHop(index=1, ip="10.0.0.1")],
            delay_s=1.0,  # discovery will not finish before pause
        )
        session = mgr.create("target", interval_s=0.1)
        session.target_ip = "10.0.0.99"
        await mgr.start(session)
        assert session._discover_task is not None
        await asyncio.sleep(0.05)
        await mgr.pause(session)
        # After pause, the discovery task has been awaited (done or cancelled).
        paused_cleanly["done"] = session._discover_task.done()
        paused_cleanly["cancelled"] = session._discover_task.cancelled()

    asyncio.run(run())
    assert paused_cleanly["done"] or paused_cleanly["cancelled"]


def test_resume_restarts_discovery_cancelled_by_pause() -> None:
    calls = 0

    async def discover(_target, _timeout_s=90.0):
        nonlocal calls
        calls += 1
        if calls == 1:
            await asyncio.sleep(10)
        from netplot.engine import RouteHop

        return [RouteHop(index=1, ip="10.0.0.1")]

    class ResolvedSessionManager(SessionManager):
        # Isolate forward DNS so this test pauses during traceroute, not lookup.
        async def _resolve_target(self, _target):
            return "10.0.0.99", None

    async def run():
        mgr = ResolvedSessionManager(
            discover_fn=discover,
            resolve_name_fn=_fast_resolve_name,
            probe_fn=lambda *_a, **_k: _immediate_probe(),
        )
        session = mgr.create("target", interval_s=1.0)
        await mgr.start(session)
        await asyncio.sleep(0.01)
        await mgr.pause(session)
        await mgr.resume(session)
        for _ in range(20):
            if session.route_state == "ready":
                break
            await asyncio.sleep(0.01)
        await mgr.pause(session)
        return session

    async def _immediate_probe():
        return 1.0

    session = asyncio.run(run())
    assert calls == 2
    assert session.route_state == "ready"


def test_pause_during_pending_forward_dns_cancels_discovery_and_resume_restarts() -> None:
    """Pause must stop discovery while forward DNS (getaddrinfo) is pending.

    Regression for a real defect: ``_resolve_target`` used to swallow the
    CancelledError raised into a pending ``loop.getaddrinfo``, so discovery
    ran on into the traceroute await after pause had already cancelled it,
    and ``pause()`` stalled waiting for a discovery that refused to stop.
    Everything is event-gated — no real DNS, no sleeps.
    """

    async def run():
        from netplot.engine import RouteHop

        target_ip = "203.0.113.7"
        loop = asyncio.get_running_loop()

        dns_calls = 0
        dns_entered = asyncio.Event()

        async def fake_getaddrinfo(_host, _port, **_kwargs):
            # Fake forward DNS. The first lookup parks forever (forward DNS
            # pending — exactly the window pause must be able to interrupt);
            # later lookups resolve immediately.
            nonlocal dns_calls
            dns_calls += 1
            dns_entered.set()
            if dns_calls == 1:
                await loop.create_future()
            return [(2, 1, 6, "", (target_ip, 0))]

        discover_calls = 0
        discover_entered = asyncio.Event()
        discover_release = asyncio.Event()

        async def fake_discover(_target, _timeout_s=90.0):
            nonlocal discover_calls
            discover_calls += 1
            discover_entered.set()
            if not discover_release.is_set():
                # Only reachable when the pause cancellation was swallowed
                # during the pending lookup: park like a real traceroute
                # would, so pause cannot complete.
                await loop.create_future()
            return [RouteHop(index=1, ip="10.0.113.1")]

        async def fake_probe(_host, _timeout_s):
            return 1.0

        mgr = SessionManager(
            probe_fn=fake_probe,
            discover_fn=fake_discover,
            resolve_name_fn=_fast_resolve_name,
        )
        session = mgr.create("target.example", interval_s=60.0)

        real_getaddrinfo = loop.getaddrinfo
        loop.getaddrinfo = fake_getaddrinfo  # type: ignore[method-assign]
        try:
            await mgr.start(session)
            first_discovery = session._discover_task
            assert first_discovery is not None

            # Deterministic rendezvous: discovery is parked inside the
            # forward lookup, before traceroute has started.
            await dns_entered.wait()
            assert session.route_state == "pending"

            pause_task = asyncio.create_task(mgr.pause(session))
            entered_discovery = asyncio.ensure_future(discover_entered.wait())
            done, _pending = await asyncio.wait(
                [pause_task, entered_discovery],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if pause_task not in done:
                # Defect path: the cancellation was swallowed during the
                # pending lookup, discovery ran on into fake_discover, and
                # pause is stuck awaiting it. Clean up so the loop can
                # close, then fail loudly (this is the observed hang).
                entered_discovery.cancel()
                pause_task.cancel()
                first_discovery.cancel()
                await asyncio.gather(
                    entered_discovery,
                    pause_task,
                    first_discovery,
                    return_exceptions=True,
                )
                raise AssertionError(
                    "pause() stalled: discovery continued into the "
                    "traceroute await after its task was cancelled "
                    "(CancelledError swallowed during pending forward DNS)"
                )
            entered_discovery.cancel()
            try:
                await entered_discovery
            except asyncio.CancelledError:
                pass

            # Pause completed while DNS was pending: discovery was actually
            # stopped at the lookup boundary (traceroute never started)...
            assert discover_calls == 0
            assert first_discovery.cancelled()
            assert session.route_state == "pending"  # not misfiled as failed
            assert session.state == "paused"
            assert session.paused_at is not None
            assert session._discovery_restart is True

            # Resume restarts the cancelled discovery from scratch: a fresh
            # lookup (now unblocked), a fresh discovery task, a ready route.
            discover_release.set()
            await mgr.resume(session)
            second_discovery = session._discover_task
            assert second_discovery is not None
            assert second_discovery is not first_discovery
            await second_discovery
            assert dns_calls == 2
            assert session.route_state == "ready"
            assert session.target_ip == target_ip
            assert session.target_dns is None
            assert [hop.ip for hop in session.hops] == ["10.0.113.1"]
            assert session.state == "running"
            assert session.paused_at is None

            await mgr.delete(session)
        finally:
            loop.getaddrinfo = real_getaddrinfo

    asyncio.run(run())


def test_resolve_target_swallows_ordinary_dns_failure_and_discovery_continues() -> None:
    """A non-cancellation getaddrinfo failure stays best-effort.

    ``target_ip``/``target_dns`` degrade to None, discovery still runs, and
    the session reaches a ready route (target-only dedup simply skipped).
    """

    async def run():
        from netplot.engine import RouteHop

        loop = asyncio.get_running_loop()

        async def failing_getaddrinfo(_host, _port, **_kwargs):
            raise OSError("simulated resolver failure")

        async def fake_discover(_target, _timeout_s=90.0):
            return [RouteHop(index=1, ip="10.0.113.1")]

        async def fake_probe(_host, _timeout_s):
            return 1.0

        mgr = SessionManager(
            probe_fn=fake_probe,
            discover_fn=fake_discover,
            resolve_name_fn=_fast_resolve_name,
        )
        session = mgr.create("target.example", interval_s=60.0)

        real_getaddrinfo = loop.getaddrinfo
        loop.getaddrinfo = failing_getaddrinfo  # type: ignore[method-assign]
        try:
            await mgr.start(session)
            discovery = session._discover_task
            assert discovery is not None
            await discovery
            assert session.route_state == "ready"
            assert session.target_ip is None
            assert session.target_dns is None
            assert [hop.ip for hop in session.hops] == ["10.0.113.1"]
            await mgr.delete(session)
        finally:
            loop.getaddrinfo = real_getaddrinfo

    asyncio.run(run())


# ------------------------------------------------ overlapping scheduler
#
# PingPlotter-parity scheduling regressions: deadline-driven launches with
# overlapping rounds, launch-ordered exactly-once commit, whole-round
# admission under the session probe budgets, and lifecycle cleanup that
# blocks later writes. Fakes keep every scenario deterministic.


def test_rounds_overlap_while_earlier_probe_awaits_reply() -> None:
    """Launches keep firing on the interval while earlier probes are still
    awaiting replies (interval independent of the 3 s reply window), and a
    reply arriving long after its interval — but inside its deadline — is
    committed as a success, not a loss."""
    launches: list[float] = []

    async def fake_probe(_host, _timeout_s):
        launches.append(time.monotonic())
        await asyncio.sleep(0.3)  # 6x the interval, far below the 3 s timeout
        return 5.0

    async def run():
        mgr = SessionManager(
            probe_fn=fake_probe,
            discover_fn=_failing_discover,
            resolve_name_fn=_fast_resolve_name,
            probe_timeout_s=3.0,
        )
        session = mgr.create("example.com", interval_s=0.05)
        await mgr.start(session)
        await asyncio.sleep(0.5)
        await mgr.pause(session)
        # No cancelled round may commit after pause completed.
        frozen = len(session.samples)
        await asyncio.sleep(0.4)
        assert len(session.samples) == frozen
        return session

    session = asyncio.run(run())
    # Overlap: several rounds launched while earlier probes were pending.
    assert len(launches) >= 4
    # Send cadence follows the interval grid, not the probe duration: a
    # serialized loop would show ~0.3 s gaps between launches.
    gaps = [b - a for a, b in itertools.pairwise(launches)]
    assert gaps
    assert max(gaps) < 0.25
    # Late replies are successes; committed rounds land exactly once each.
    assert len(session.samples) >= 2
    assert all(s.rtt_ms == pytest.approx(5.0) for s in session.samples)
    assert session._probe_permits == 0
    assert session._pending_rounds == 0


def test_commit_is_launch_ordered_when_later_round_finishes_first() -> None:
    """Round 1's reply is slow and round 2 finishes first: history stays
    launch-ordered, current RTT comes from the newest committed round, the
    SSE feed mirrors the deque one-to-one, and raw/rollup counts have no
    duplicates."""
    replies: list[float] = []

    async def fake_probe(_host, _timeout_s):
        n = len(replies)
        value = 11.0 if n == 0 else 22.0
        replies.append(value)
        if n == 0:
            await asyncio.sleep(0.25)  # round 1 finishes last
        return value

    async def run():
        mgr = SessionManager(
            probe_fn=fake_probe,
            discover_fn=_failing_discover,
            resolve_name_fn=_fast_resolve_name,
            probe_timeout_s=3.0,
        )
        session = mgr.create("example.com", interval_s=0.05)
        queue = session.subscribe()
        await mgr.start(session)
        await asyncio.sleep(0.5)
        await mgr.pause(session)
        events = []
        while True:
            try:
                event = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if isinstance(event, RoundEvent):
                events.append(event)
        return session, events

    session, events = asyncio.run(run())
    samples = list(session.samples)
    assert len(samples) >= 2
    assert len(events) == len(samples), "one SSE round event per committed round"
    # Launch-ordered history: the slow first round commits first even
    # though the second round finished probing earlier.
    assert samples[0].rtt_ms == pytest.approx(11.0)
    assert samples[1].rtt_ms == pytest.approx(22.0)
    ts = [s.t for s in samples]
    assert all(b > a for a, b in itertools.pairwise(ts)), "deque stays launch-ordered"
    assert [e.t for e in events] == ts, "SSE events mirror committed history"
    assert samples[-1].rtt_ms == session.stats(0)["cur_ms"], (
        "current RTT comes from the newest committed round"
    )
    # Exactly-once: the rollup folded each sample exactly once.
    _, buckets = session.target_rollup.capture(0, 10**12)
    assert sum(b["count"] for b in buckets) == len(samples)


def test_budget_admission_skips_without_fake_losses(monkeypatch) -> None:
    """Whole-round admission: a plan larger than the process budget never
    launches (no partial rounds, no fabricated losses), the pending-round
    cap halts launches with no growing task backlog, and pause releases
    every budget."""
    import netplot.session as session_module

    calls: dict[str, int] = {"n": 0}

    async def never_replies(_host, _timeout_s):
        calls["n"] += 1
        await asyncio.sleep(5.0)
        return 1.0

    async def run():
        from netplot.session import Hop

        monkeypatch.setattr(session_module, "MAX_OUTSTANDING_PROBES", 2)
        monkeypatch.setattr(session_module, "MAX_PENDING_ROUNDS", 2)
        mgr = SessionManager(
            probe_fn=never_replies,
            discover_fn=_failing_discover,
            resolve_name_fn=_fast_resolve_name,
            probe_timeout_s=3.0,
        )

        # Plan of 3 probes (2 hops + uncovered target) vs budget 2: the
        # whole-round rule admits nothing at all — no partial launch.
        big = mgr.create("target.example", interval_s=0.01)
        big.route_state = "ready"
        big.target_ip = "192.0.2.99"  # not the final hop -> target probed too
        big.hops = [
            Hop(index=1, ip="10.0.0.1", dns=None),
            Hop(index=2, ip="10.0.0.2", dns=None),
        ]
        await mgr.start(big)
        await asyncio.sleep(0.1)
        assert calls["n"] == 0
        assert len(big.samples) == 0
        assert big.hops[0].probes == 0, "skipped sends never count as losses"
        assert big.hops[1].probes == 0
        await mgr.pause(big)

        # Single-probe plan fits the budget but the pending-round cap
        # halts launches: backlog stops at the cap, no queue growth.
        small = mgr.create("example.com", interval_s=0.01)
        await mgr.start(small)
        await asyncio.sleep(0.1)
        assert small._pending_rounds == 2
        assert small._probe_permits == 2
        assert calls["n"] == 2
        assert len(small.samples) == 0, "pending work is not a loss"
        await mgr.pause(small)
        assert small._probe_permits == 0
        assert small._pending_rounds == 0
        assert len(small.samples) == 0
        assert calls["n"] == 2, "cancelled probes never complete"

        # Degenerate never-fits case: budget 0 blocks even a 1-probe plan.
        monkeypatch.setattr(session_module, "MAX_OUTSTANDING_PROBES", 0)
        blocked = mgr.create("example.com", interval_s=0.01)
        await mgr.start(blocked)
        await asyncio.sleep(0.05)
        await mgr.pause(blocked)
        assert len(blocked.samples) == 0
        assert blocked._probe_permits == 0
        assert blocked._pending_rounds == 0

    asyncio.run(run())
    assert calls["n"] == 2


def test_skipped_deadlines_resume_without_catchup_burst(monkeypatch) -> None:
    """While the budget blocks every launch the deadline grid keeps
    advancing; when capacity exists again rounds resume on the grid — one
    launch per interval, never a burst of all missed deadlines."""
    import netplot.session as session_module

    launches: list[float] = []

    async def instant(_host, _timeout_s):
        launches.append(time.monotonic())
        return 4.0

    async def run():
        monkeypatch.setattr(session_module, "MAX_OUTSTANDING_PROBES", 0)
        mgr = SessionManager(
            probe_fn=instant,
            discover_fn=_failing_discover,
            resolve_name_fn=_fast_resolve_name,
            probe_timeout_s=3.0,
        )
        session = mgr.create("example.com", interval_s=0.05)
        await mgr.start(session)
        await asyncio.sleep(0.1)  # ~5 deadlines, all skipped
        assert len(launches) == 0
        monkeypatch.setattr(session_module, "MAX_OUTSTANDING_PROBES", 45)
        await asyncio.sleep(0.15)
        await mgr.pause(session)

    asyncio.run(run())
    assert len(launches) >= 2
    gaps = [b - a for a, b in itertools.pairwise(launches)]
    # Each launch sits one interval after the previous one on the grid; a
    # catch-up burst would show near-zero gaps.
    assert all(g >= 0.045 for g in gaps)


def test_pause_discards_uncommitted_rounds_and_blocks_later_writes() -> None:
    """Pause cancels outstanding rounds, awaits their probe children, and
    no cancelled round appends, aggregates, or publishes afterwards."""
    completions: list[str] = []

    async def slow_probe(_host, _timeout_s):
        await asyncio.sleep(0.2)
        completions.append("done")
        return 7.0

    async def run():
        mgr = SessionManager(
            probe_fn=slow_probe,
            discover_fn=_failing_discover,
            resolve_name_fn=_fast_resolve_name,
            probe_timeout_s=3.0,
        )
        session = mgr.create("example.com", interval_s=0.05)
        queue = session.subscribe()
        await mgr.start(session)
        await asyncio.sleep(0.3)
        await mgr.pause(session)
        frozen_samples = len(session.samples)
        frozen_completions = len(completions)
        frozen_events = queue.qsize()
        await asyncio.sleep(0.3)  # longer than one probe duration
        assert len(session.samples) == frozen_samples
        assert len(completions) == frozen_completions
        assert queue.qsize() == frozen_events
        assert session._probe_permits == 0
        assert session._pending_rounds == 0

    asyncio.run(run())


def test_resume_single_generation_then_delete_stops_everything() -> None:
    """Resume starts exactly one fresh scheduler generation; delete stops
    probing, awaits all round children, and nothing writes afterwards."""
    launches: list[float] = []
    in_flight = 0
    peak = 0

    async def slow_probe(_host, _timeout_s):
        nonlocal in_flight, peak
        launches.append(time.monotonic())
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.15)
        in_flight -= 1
        return 2.0

    async def run():
        mgr = SessionManager(
            probe_fn=slow_probe,
            discover_fn=_failing_discover,
            resolve_name_fn=_fast_resolve_name,
            probe_timeout_s=3.0,
        )
        session = mgr.create("example.com", interval_s=0.05)
        await mgr.start(session)
        await asyncio.sleep(0.2)
        await mgr.pause(session)
        before_resume = len(session.samples)
        launches_before_resume = len(launches)
        await mgr.resume(session)
        assert session._task is not None and not session._task.done()
        assert session.state == "running"
        assert session.paused_at is None
        await asyncio.sleep(0.35)
        await mgr.delete(session)
        assert session._task is None or session._task.done()
        after_delete = len(session.samples)
        launches_after_delete = len(launches)
        await asyncio.sleep(0.2)
        assert len(session.samples) == after_delete, "no writes after delete"
        assert len(launches) == launches_after_delete, "no launches after delete"
        return session, before_resume, launches_before_resume

    session, before_resume, launches_before_resume = asyncio.run(run())
    assert len(session.samples) > before_resume, "resume produced new rounds"
    assert len(launches) > launches_before_resume, "resume relaunched the scheduler"
    assert session._closed
    assert session._probe_permits == 0
    assert session._pending_rounds == 0
    assert peak >= 2, "probes from consecutive rounds overlapped"


def test_shutdown_awaits_round_children_of_every_session() -> None:
    """Shutdown cancels and awaits every session's collectors and probe
    tasks; cancelled probes never complete and never write afterwards."""
    completions: list[str] = []

    async def slow_probe(_host, _timeout_s):
        await asyncio.sleep(0.2)
        completions.append("done")
        return 1.0

    async def run():
        mgr = SessionManager(
            probe_fn=slow_probe,
            discover_fn=_failing_discover,
            resolve_name_fn=_fast_resolve_name,
            probe_timeout_s=3.0,
        )
        sessions = [mgr.create("example.com", interval_s=0.05) for _ in range(3)]
        for session in sessions:
            await mgr.start(session)
        await asyncio.sleep(0.1)
        await mgr.shutdown()
        frozen = len(completions)
        assert frozen == 0  # every in-flight probe was cancelled, not awaited
        for session in sessions:
            assert session._task is None or session._task.done()
            assert session._probe_permits == 0
            assert session._pending_rounds == 0
        await asyncio.sleep(0.3)  # longer than one probe duration
        assert len(completions) == frozen, "no completions after shutdown"

    asyncio.run(run())


def test_discovery_handoff_keeps_alias_and_one_commit_per_round() -> None:
    """Rounds outstanding across the discovery handoff: pre-route rounds
    commit the target entry, post-route rounds commit the hop entries, the
    final-hop==target deque/rollup alias holds, and the shared stream gets
    exactly one sample per committed round."""
    launches: list[float] = []

    async def fake_probe(host, _timeout_s):
        launches.append(time.monotonic())
        await asyncio.sleep(0.12)  # spans the discovery handoff
        return 3.0

    async def run():
        from netplot.engine import RouteHop

        hops = [
            RouteHop(index=1, ip="10.0.0.1"),
            RouteHop(index=2, ip="192.0.2.10"),  # final hop IS the target
        ]
        mgr = _mgr_with_hops(
            hops,
            probe_fn=fake_probe,
            delay_s=0.2,
            target_ip="192.0.2.10",
        )
        session = mgr.create("192.0.2.10", interval_s=0.05)
        queue = session.subscribe()
        await mgr.start(session)
        await asyncio.sleep(0.8)
        await mgr.pause(session)
        events = []
        while True:
            try:
                event = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if isinstance(event, RoundEvent):
                events.append(event)
        return session, events

    session, events = asyncio.run(run())
    assert session.route_state == "ready"
    assert session.hops[-1].samples is session.samples
    assert session.hops[-1].rollup is session.target_rollup
    pre = [e for e in events if any(r.hop == 0 for r in e.results)]
    post = [e for e in events if all(r.hop != 0 for r in e.results)]
    assert pre and post, "expected committed rounds on both handoff sides"
    for event in post:
        assert [r.hop for r in event.results] == [1, 2]
        assert all(r.rtt_ms == pytest.approx(3.0) for r in event.results)
    # Exactly one committed sample per round in the shared stream — never
    # a duplicated target sample — launch-ordered, and rollups match.
    assert len(session.samples) == len(events)
    ts = [s.t for s in session.samples]
    assert all(b > a for a, b in itertools.pairwise(ts))
    assert [e.t for e in events] == ts
    _, buckets = session.target_rollup.capture(0, 10**12)
    assert sum(b["count"] for b in buckets) == len(session.samples)
    assert len(session.hops[0].samples) == len(post), (
        "intermediate hops only sample after discovery delivered them"
    )


def test_resume_persistence_rollback_stops_new_generation(monkeypatch) -> None:
    """A failed resume persist rolls back to paused: the fresh scheduler
    generation is cancelled and awaited, and no further rounds launch."""
    launches: list[float] = []

    async def fake_probe(_host, _timeout_s):
        launches.append(time.monotonic())
        return 1.0

    async def run():
        mgr = SessionManager(
            probe_fn=fake_probe,
            discover_fn=_failing_discover,
            resolve_name_fn=_fast_resolve_name,
            probe_timeout_s=3.0,
        )
        session = mgr.create("example.com", interval_s=0.05, persistent=True)
        await mgr.start(session)
        await asyncio.sleep(0.15)
        await mgr.pause(session)
        real_persist = mgr._persist_async
        persist_calls = {"n": 0}

        async def flaky_persist() -> None:
            persist_calls["n"] += 1
            if persist_calls["n"] == 1:  # injected after pause; fail resume only
                raise RuntimeError("simulated fsync failure")
            await real_persist()

        monkeypatch.setattr(mgr, "_persist_async", flaky_persist)
        with pytest.raises(RuntimeError):
            await mgr.resume(session)
        assert session.state == "paused"
        assert session.paused_at is not None
        assert session._task is None or session._task.done()
        assert session._probe_permits == 0
        assert session._pending_rounds == 0
        after_rollback = len(launches)
        frozen_samples = len(session.samples)
        await asyncio.sleep(0.15)
        assert len(launches) == after_rollback, "no new scheduler generation"
        assert len(session.samples) == frozen_samples

    asyncio.run(run())
