"""ICMP non-responsive classification (trace-local, neutral presentation).

Server-side lifetime counters (``Hop.probes`` / ``Hop.replies``) decide the
snapshot's ``icmp_non_responsive`` flag. The scenarios enumerated in the
feature brief are pinned here: no samples/startup, intermittent replies,
historical reply outside the rolling window, destination exclusion, later
recovery, and reset/changed hop.
"""

from __future__ import annotations

import asyncio
import time

from netplot.engine import RouteHop
from netplot.session import (
    ICMP_NONRESPONSIVE_MIN_PROBES,
    Hop,
    Sample,
    SessionManager,
    TraceSession,
)


def _session_with_hops(*hops: Hop, target_ip: str | None = "203.0.113.9") -> TraceSession:
    session = TraceSession(id="t", target="example.com", interval_s=1.0)
    session.route_state = "ready"
    session.target_ip = target_ip
    session.hops = list(hops)
    return session


def test_no_samples_startup_is_not_non_responsive() -> None:
    session = _session_with_hops(Hop(index=1, ip="10.0.0.1", dns=None))
    assert session.hops[0].probes == 0
    assert session.hop_icmp_non_responsive(session.hops[0]) is False
    snap = session.snapshot(focus_s=600)
    assert snap["hops"][0]["icmp_non_responsive"] is False


def test_four_failed_probes_not_yet_classified_fifth_flips() -> None:
    session = _session_with_hops(Hop(index=1, ip="10.0.0.1", dns=None))
    hop = session.hops[0]
    for _ in range(ICMP_NONRESPONSIVE_MIN_PROBES - 1):
        hop.probes += 1
    assert session.hop_icmp_non_responsive(hop) is False
    hop.probes += 1
    assert session.hop_icmp_non_responsive(hop) is True


def test_intermittent_replies_never_classified() -> None:
    session = _session_with_hops(Hop(index=1, ip="10.0.0.1", dns=None))
    hop = session.hops[0]
    hop.probes = 50
    hop.replies = 1  # replied once, 49 losses since — intermittent loss
    assert session.hop_icmp_non_responsive(hop) is False


def test_historical_reply_outside_rolling_window_not_classified() -> None:
    """A reply that aged out of the sample deque (and every rolling window)
    still disqualifies classification — counters are lifetime, not window."""
    session = _session_with_hops(Hop(index=1, ip="10.0.0.1", dns=None))
    hop = session.hops[0]
    now = time.time()
    # One successful reply an hour ago (outside the deque's retained window
    # is simulated by simply not storing it — counters still remember it).
    hop.replies = 1
    for i in range(10):
        hop.samples.append(Sample(t=now - i, rtt_ms=None))
    hop.probes = 11
    assert session.hop_icmp_non_responsive(hop) is False


def test_silent_unknown_ip_hop_becomes_non_responsive() -> None:
    session = _session_with_hops(
        Hop(index=2, ip=None, dns=None), Hop(index=3, ip="203.0.113.9", dns=None)
    )
    silent, dest = session.hops
    silent.probes = ICMP_NONRESPONSIVE_MIN_PROBES
    dest.probes = ICMP_NONRESPONSIVE_MIN_PROBES
    assert session.hop_icmp_non_responsive(silent) is True
    # Destination (final hop == target) excluded: 100% loss stays an error.
    assert session.hop_icmp_non_responsive(dest) is False


def test_destination_exclusion_requires_route_ready_and_matching_ip() -> None:
    # Final hop differs from target → it is an intermediate hop: flag applies.
    session = _session_with_hops(Hop(index=1, ip="10.0.0.1", dns=None), target_ip="203.0.113.9")
    hop = session.hops[0]
    hop.probes = 10
    assert session.hop_icmp_non_responsive(hop) is True
    # Route pending → no destination determination possible; the hop is the
    # only known one and still gets the neutral treatment once probed.
    session.route_state = "pending"
    assert session.hop_icmp_non_responsive(hop) is True


def test_later_recovery_clears_flag() -> None:
    session = _session_with_hops(Hop(index=1, ip="10.0.0.1", dns=None))
    hop = session.hops[0]
    hop.probes = ICMP_NONRESPONSIVE_MIN_PROBES
    assert session.hop_icmp_non_responsive(hop) is True
    hop.probes += 1
    hop.replies += 1  # a successful reply arrives
    assert session.hop_icmp_non_responsive(hop) is False


def test_reset_or_changed_hop_resets_counters() -> None:
    session = _session_with_hops(Hop(index=1, ip="10.0.0.1", dns=None))
    old = session.hops[0]
    old.probes = 20
    assert session.hop_icmp_non_responsive(old) is True
    # A re-discovery (e.g. discovery restart) builds fresh Hop objects with
    # zeroed counters — even for a different IP at the same index.
    session.hops = [Hop(index=1, ip="10.0.0.2", dns=None)]
    assert session.hops[0].probes == 0
    assert session.hop_icmp_non_responsive(session.hops[0]) is False


def test_hop_scoped_snapshot_carries_flag() -> None:
    session = _session_with_hops(Hop(index=1, ip="10.0.0.1", dns=None))
    session.hops[0].probes = ICMP_NONRESPONSIVE_MIN_PROBES
    payload = session.snapshot_hop(1, focus_s=600)
    assert payload is not None
    assert payload["icmp_non_responsive"] is True


# ------------------------------------------------- probe-round integration


def test_probe_round_updates_counters_and_flag_roundtrip() -> None:
    """Fake probes drive the real probe loop: an always-silent intermediate
    hop becomes non-responsive after 5 rounds; a later reply clears it; the
    destination never gets the flag."""

    async def run() -> dict:
        probe_results: dict[str, float | None] = {"10.0.0.1": None, "192.0.2.10": None}

        async def fake_probe(host, _timeout):
            return probe_results.get(host)

        async def fake_discover(_target, _timeout=90.0):
            # Intermediate hop + final hop that IS the target.
            return [RouteHop(index=1, ip="10.0.0.1"), RouteHop(index=2, ip="192.0.2.10")]

        async def fake_resolve(_ip):
            return None

        manager = SessionManager(
            probe_fn=fake_probe,
            discover_fn=fake_discover,
            resolve_name_fn=fake_resolve,
        )
        session = await manager.create_and_start("192.0.2.10", interval_s=0.01)
        try:
            # Wait for discovery to populate hops.
            for _ in range(200):
                if session.hops:
                    break
                await asyncio.sleep(0.01)
            assert session.hops, "discovery did not populate hops"

            async def wait_until(predicate, timeout_s=5.0):
                # Rounds launch on the live scheduler every interval; wait
                # until the lifetime counters reflect enough committed
                # rounds (probes are instant fakes, so this is fast).
                deadline = time.monotonic() + timeout_s
                while time.monotonic() < deadline:
                    if predicate():
                        return True
                    await asyncio.sleep(0.01)
                return False

            intermediate, destination = session.hops[0], session.hops[1]
            assert await wait_until(
                lambda: intermediate.probes >= ICMP_NONRESPONSIVE_MIN_PROBES
            ), "intermediate hop never accumulated the probe rounds"
            assert session.hop_icmp_non_responsive(intermediate) is True
            assert session.hop_icmp_non_responsive(destination) is False

            # Later recovery: the intermediate hop starts replying.
            probe_results["10.0.0.1"] = 12.5
            assert await wait_until(
                lambda: intermediate.replies >= 1
            ), "intermediate hop never committed a successful reply"
            assert session.hop_icmp_non_responsive(session.hops[0]) is False

            # Settle: stop the scheduler so the snapshot reads a quiet
            # session (pause awaits all outstanding rounds' cancellation).
            await manager.pause(session)

            snap = session.snapshot(focus_s=0)
            by_index = {h["index"]: h for h in snap["hops"]}
            assert by_index[1]["icmp_non_responsive"] is False
            assert by_index[2]["icmp_non_responsive"] is False
            return snap
        finally:
            await manager.shutdown()

    snap = asyncio.run(run())
    # Counts/loss are preserved by the classification (all-loss window for
    # hop 1 still reports its real loss percentage).
    assert snap["hops"][0]["stats"]["count"] >= ICMP_NONRESPONSIVE_MIN_PROBES
