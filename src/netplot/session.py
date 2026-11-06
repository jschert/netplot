"""In-memory trace session management.

A ``TraceSession`` owns:

- a bounded ring buffer of target probe samples (always populated, even
  before route discovery completes — Slice 2 behaviour preserved),
- a list of per-hop ``Hop`` entries once route discovery delivers them,
- an asyncio scheduler task that launches probe rounds on interval
  deadlines (target + every hop with a known IP concurrently; rounds
  overlap within bounded process/round budgets and commit in launch
  order),
- a background discovery task that runs ``traceroute`` once and then resolves
  hop names; failures degrade gracefully to target-only mode,
- a fan-out of bounded SSE subscriber queues.

A ``SessionManager`` owns the session registry and the lifecycle of those
tasks.

Stats are computed on read over a focus window; nothing derived is stored.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import threading
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from math import isfinite

from .engine import (
    RouteDiscoveryError,
    RouteHop,
    discover_route,
    probe_once,
    resolve_name,
    validate_host,
)
from .persistence import PersistDegraded, PersistentStore, ReadResult, sanitize_name

__all__ = [
    "DEFAULT_FOCUS_S",
    "DISCOVER_TIMEOUT_S",
    "HOP_SNAPSHOT_TAIL",
    "ICMP_NONRESPONSIVE_MIN_PROBES",
    "MAX_OUTSTANDING_PROBES",
    "MAX_PENDING_ROUNDS",
    "MAX_SESSIONS",
    "PROBE_TIMEOUT_S",
    "ROLLUP_BUCKET_S",
    "ROLLUP_CAP",
    "SAMPLE_CAP",
    "SNAPSHOT_TAIL",
    "Hop",
    "PersistDegraded",
    "PersistentStore",
    "ProbeResult",
    "RollupBucket",
    "RoundEvent",
    "RoundPlan",
    "RoundPlanEntry",
    "RouteEvent",
    "Sample",
    "SessionLimitReached",
    "SessionManager",
    "SessionMutationPending",
    "SessionNotRegistered",
    "StreamRollup",
    "TraceSession",
    "sanitize_name",
]

log = logging.getLogger("netplot.session")

# Maximum samples retained per session/hop. A 2.5 s interval fills this in
# ~7 h, which is comfortably more than the 10-minute default focus window.
SAMPLE_CAP = 10_000

# Default focus window for snapshot stats (last N seconds of samples).
DEFAULT_FOCUS_S = 600.0

# Maximum number of samples returned in a single snapshot payload. The SSE
# stream is the live feed; the snapshot is just a recent-window preview.
SNAPSHOT_TAIL = 100

# Maximum number of samples returned for a single hop when ``hop=`` is given.
HOP_SNAPSHOT_TAIL = 100

# Bound each live subscriber's backlog so a client that stops reading cannot
# grow process memory without limit.  Live SSE is best-effort, so events are
# dropped when this queue is full.
SUBSCRIBER_QUEUE_CAP = 256
MAX_SUBSCRIBERS_PER_SESSION = 8

# Default probe reply timeout. PingPlotter uses 3 s; the spec adopts this.
PROBE_TIMEOUT_S = 3.0

# Default overall budget for one traceroute invocation. traceroute -w 2 -m 30
# can in the worst case take ~60 s on a fully-silent path; 90 s leaves slack
# for slow DNS / process startup on cold machines.
DISCOVER_TIMEOUT_S = 90.0

# An intermediate hop is presented as "ICMP non-responsive" (neutral gray,
# not an error) once at least this many probes were sent and NO successful
# reply was ever observed for it in the current trace. Lifetime counters —
# never a rolling window — decide, so a hop that replied once and then went
# silent keeps its ordinary loss treatment (intermittent loss, not
# non-response). The destination is excluded: its 100% loss stays an error.
ICMP_NONRESPONSIVE_MIN_PROBES = 5

# Session-owned probe budgets for PingPlotter-parity overlapping rounds.
#
# ``MAX_OUTSTANDING_PROBES`` bounds concurrently in-flight ping processes
# per session. A permit is acquired when a round launches and held until
# the engine's subprocess work — spawn, reply wait, kill, and reap — has
# completed, so the budget counts real processes, not merely awaited
# replies. 45 is a netplot policy inspired by the legacy Pingman
# outstanding-request guidance, not a claim about current PingPlotter
# defaults; with the eight-session cap total ping-process concurrency
# stays bounded at 360 (excluding route discovery).
#
# ``MAX_PENDING_ROUNDS`` bounds launched-but-uncommitted rounds per
# session, including rounds that finished probing but are waiting behind
# an earlier round for launch-ordered commit.
#
# Admission is whole-round: a launch opportunity is taken only when its
# complete known-host probe plan fits the remaining process budget AND the
# pending-round count is below the cap. Otherwise the opportunity is
# omitted and the next scheduled tick retries — no partial rounds, no
# unbounded wait queue, and no fabricated loss samples for probes that
# were never sent. The actual launch timestamps expose any resulting gap.
MAX_OUTSTANDING_PROBES = 45
MAX_PENDING_ROUNDS = 45

# Maximum number of concurrent sessions the manager will hold at once.
# PingPlotter-style multi-trace tabs (P1) bound the registry so a runaway
# client (or a forgotten browser tab reopened many times) cannot squat
# forever-running probe loops. ``SessionManager.create`` raises
# :class:`SessionLimitReached` once this cap is reached; the API translates
# that into HTTP 409 so the dashboard can surface it.
MAX_SESSIONS = 8

# ----------------------------------------------------------------- rollup (D2)
#
# A server-side fixed-wall-clock 1-minute rollup tier per stream, built
# incrementally as samples arrive in the same on-loop probe write path as the
# raw deques. Bounded independent of probe interval: a 1 s interval over 24 h
# would otherwise admit ~86 400 raw samples/stream (and 30 hops × 8 sessions
# of that is GB-scale); the rollup caps at 1 440 buckets/stream regardless of
# the interval. The 12 h / 24 h timeline ranges (Slice D2) are served from
# this tier; raw retention (``SAMPLE_CAP``) and the ≤ 3 h ranges (Slice D1)
# are unchanged.
#
# Both constants are named and documented; they are NOT runtime API params.
# Testability seam: each is overridable via ``StreamRollup(bucket_s=…, cap=…)``
# for unit tests, and via the ``NETPLOT_ROLLUP_BUCKET_S`` / ``NETPLOT_ROLLUP_CAP``
# env vars (read once at import) for live end-to-end evidence with a short
# bucket size (e.g. 5 s) so a 12 h range shows bucketed rendering in seconds
# rather than minutes. Production deployments leave both unset.


def _env_int(name: str, default: int) -> int:
    """Read an integer env var, returning ``default`` on missing/invalid input.

    Used for the rollup test override seam (``NETPLOT_ROLLUP_BUCKET_S`` /
    ``NETPLOT_ROLLUP_CAP``). Invalid values fall back silently so a typo in
    the env cannot crash boot — the production defaults take over.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


#: Wall-clock bucket size in seconds (PingPlotter-faithful 1-minute default).
ROLLUP_BUCKET_S = _env_int("NETPLOT_ROLLUP_BUCKET_S", 60)

#: Maximum buckets retained per stream (24 h worth of 1-minute buckets).
ROLLUP_CAP = _env_int("NETPLOT_ROLLUP_CAP", 1440)

# A private queue marker used to tell live SSE subscribers that their session
# has been deleted (or the manager is shutting down).  A queue cannot be
# closed natively, so without an explicit marker a stream would wait forever
# after its session's probe task had been removed.
_SESSION_CLOSED = object()


class SubscriberLimitReached(Exception):
    """A session already has the maximum number of live streams."""


class SessionLimitReached(Exception):
    """Raised by :meth:`SessionManager.create` when ``MAX_SESSIONS`` is reached.

    Carries the current count and the cap so callers (the API layer) can
    surface them in the error detail without re-reading module constants.
    """

    def __init__(self, count: int, cap: int) -> None:
        super().__init__(f"session cap reached ({count}/{cap})")
        self.count = count
        self.cap = cap


class SessionNotRegistered(Exception):
    """Raised by lifecycle methods when their ``session`` argument was removed
    from the registry between caller resolution and mutation-lock acquisition.

    The caller (web layer) resolves a session via :meth:`SessionManager.get`
    BEFORE calling a lifecycle method; between the two, a concurrent delete
    can remove the session from the registry. Without this check the
    lifecycle method would mutate the stale object and return a success
    summary for a session that no longer exists in the registry or on disk
    (the persist-after-delete race: persist returns 200 with ``persistent:
    True`` though registry + disk lack it). The API translates this into
    HTTP 404 (session no longer exists).
    """

    def __init__(self, session_id: str) -> None:
        super().__init__(f"session {session_id} is no longer registered")
        self.session_id = session_id


class SessionMutationPending(Exception):
    """Raised by :meth:`SessionManager.set_persistent` when another mutation
    for the same session is already in progress.

    The persist endpoint resolves the session, then calls set_persistent
    which takes the per-session mutation lock. Without a non-blocking
    acquire, concurrent same-session requests stack blocked worker threads
    (each holding a thread-pool slot while waiting on the lock), amplifying
    the fsync/thread-pool cost. The API translates this into HTTP 409 so
    the client retries cleanly instead of the server queueing blockers.
    """


@dataclass(slots=True)
class Sample:
    """A single probe sample.

    ``rtt_ms`` is ``None`` for any failure mode (timeout, unreachable,
    parse error). ``t`` is the epoch timestamp of the round LAUNCH (the
    probe send time), matching the producer: the scheduler captures ``t``
    when a round launches, so a reply arriving later — even after several
    intervals but within its reply deadline — is still recorded at the
    instant its probe was sent.
    """

    t: float
    rtt_ms: float | None


# ------------------------------------------------------------- rollup (Slice D2)


@dataclass(slots=True)
class RollupBucket:
    """One fixed-wall-clock bucket in a stream's rollup.

    Mutable: updated incrementally on every probe sample that falls in the
    bucket's window. ``t`` is the bucket-start epoch second (wall-clock
    aligned to multiples of :data:`ROLLUP_BUCKET_S`). ``count`` includes
    losses; ``success_count`` is the denominator for the incremental mean
    (and excludes non-finite RTTs, which are treated as losses — same
    contract as :mod:`netplot.metrics`). A bucket with ``success_count == 0``
    and ``count > 0`` is a full-loss bucket; its serialized form carries
    ``avg_ms`` / ``min_ms`` / ``max_ms`` as ``None``.

    The mean uses the divide-before-subtract discipline from
    :func:`netplot.metrics.capture_metrics`: each update applies
    ``mean += (rtt / success_count) - (mean / success_count)`` after the
    count increment, so a finite RTT near a float limit does not overflow
    the accumulator. min/max are plain comparisons over the same finite
    RTTs.
    """

    t: int
    count: int = 0
    loss_count: int = 0
    success_count: int = 0
    mean: float = 0.0
    min_ms: float | None = None
    max_ms: float | None = None


class StreamRollup:
    """Incremental rollup of one stream into fixed-wall-clock buckets.

    Maintains a bounded dict of :class:`RollupBucket` keyed by bucket-start
    epoch second. Buckets are created on first sample in the window and
    updated incrementally on every subsequent sample (no separate
    aggregation pass). The dict is bounded by :attr:`_cap`; on overflow the
    bucket with the smallest ``t`` is evicted so the ``cap`` newest buckets
    survive (matching the slice spec: "eviction keeps newest 1440 buckets;
    earliest_t reflects eviction").

    Bucket math is overflow-safe via divide-before-subtract (mirrors
    :mod:`netplot.metrics`). Non-finite RTTs are treated as losses, so a
    malformed/overflowing ping value cannot poison ``mean`` / ``min`` /
    ``max`` or leak as ``NaN`` / ``+Inf`` exposition values.

    Testability: ``bucket_s`` and ``cap`` default to the module constants
    (themselves env-overridable) but can be passed explicitly per-instance
    so a test can drive short buckets without mutating process state.
    """

    __slots__ = ("_bucket_s", "_buckets", "_cap")

    def __init__(self, bucket_s: int | None = None, cap: int | None = None) -> None:
        self._bucket_s = int(bucket_s) if bucket_s is not None else ROLLUP_BUCKET_S
        self._cap = int(cap) if cap is not None else ROLLUP_CAP
        if self._bucket_s <= 0:
            raise ValueError("bucket_s must be > 0")
        if self._cap <= 0:
            raise ValueError("cap must be > 0")
        # Insertion-ordered dict; on overflow we evict by smallest t so a
        # clock-skew that inserts an older key after a newer one still keeps
        # the newest wall-clock buckets.
        self._buckets: dict[int, RollupBucket] = {}

    @property
    def bucket_s(self) -> int:
        return self._bucket_s

    @property
    def cap(self) -> int:
        return self._cap

    def add_sample(self, t: float, rtt_ms: float | None) -> RollupBucket:
        """Fold one sample into its bucket, creating it if necessary.

        Mutates state in place. Safe to call from the on-loop probe write
        path: no I/O, no allocations beyond a fresh bucket on the first
        sample of a new window, no awaits.
        """
        bucket_t = int(t // self._bucket_s) * self._bucket_s
        bucket = self._buckets.get(bucket_t)
        if bucket is None:
            bucket = RollupBucket(t=bucket_t)
            self._buckets[bucket_t] = bucket
            # Bound the dict. Eviction runs only when a NEW bucket is
            # created (not on every sample), so the per-sample cost stays
            # O(1) amortised; the rare eviction is O(cap) for the min-key
            # lookup, which is trivial at cap=1440.
            if len(self._buckets) > self._cap:
                # Evict the bucket with the smallest t so the ``cap``
                # newest survive even when a clock adjustment inserted an
                # out-of-order key.
                oldest_t = min(self._buckets)
                del self._buckets[oldest_t]
        bucket.count += 1
        # Non-finite RTTs (NaN/Infinity from malformed ping output or
        # direct test injection) are treated as losses: they are not
        # measurements and must not poison the incremental mean or the
        # min/max. This mirrors :mod:`netplot.metrics`.
        if rtt_ms is None or not isfinite(rtt_ms):
            bucket.loss_count += 1
            return bucket
        bucket.success_count += 1
        n = bucket.success_count
        # Divide before subtract: ``(rtt - mean)`` can overflow for finite
        # values with opposite signs near the float limit. Each operand is
        # bounded by max(|rtt|, |mean|) / n. Same incremental mean as
        # :mod:`netplot.metrics` (kept here independently so rollup does
        # not depend on the metrics module).
        if n == 1:
            bucket.mean = rtt_ms
        else:
            bucket.mean += (rtt_ms / n) - (bucket.mean / n)
        if bucket.min_ms is None or rtt_ms < bucket.min_ms:
            bucket.min_ms = rtt_ms
        if bucket.max_ms is None or rtt_ms > bucket.max_ms:
            bucket.max_ms = rtt_ms
        return bucket

    def earliest_t(self) -> int | None:
        """Earliest retained bucket-start epoch, or ``None`` when empty."""
        if not self._buckets:
            return None
        return min(self._buckets)

    def capture(self, start: float, end: float) -> tuple[int | None, list[dict]]:
        """Snapshot ``(earliest_t, buckets)`` for ``[start, end]``.

        Returns a list of plain dicts ascending by ``t``, clamped to
        ``[start, end]``. Empty buckets never exist in the dict (they are
        only created when a sample arrives), so the response is naturally
        sparse: gaps with no samples (paused periods, pre-start) are
        omitted. ``avg_ms`` / ``min_ms`` / ``max_ms`` are ``None`` in a
        full-loss bucket (count>0, success_count=0).

        ``earliest_t`` is the oldest retained bucket's ``t`` regardless of
        the query window, so a client can distinguish "no data" from "data
        older than retention".

        The returned dicts own no references to the live buckets — every
        value is an immutable scalar — so the caller is free to serialise
        them without racing a concurrent probe write (same on-loop
        discipline as :meth:`TraceSession.history_samples`).
        """
        if not self._buckets:
            return None, []
        earliest = min(self._buckets)
        out: list[dict] = []
        for bucket_t in sorted(self._buckets):
            if bucket_t < start:
                continue
            if bucket_t > end:
                break
            bucket = self._buckets[bucket_t]
            full_loss = bucket.success_count == 0
            out.append(
                {
                    "t": bucket_t,
                    "count": bucket.count,
                    "loss_count": bucket.loss_count,
                    "avg_ms": None if full_loss else bucket.mean,
                    "min_ms": None if full_loss else bucket.min_ms,
                    "max_ms": None if full_loss else bucket.max_ms,
                }
            )
        return earliest, out


@dataclass(slots=True)
class Hop:
    """One hop in a discovered route, plus its own sample ring buffer.

    ``ip`` is ``None`` for silent hops (``*``); they are preserved verbatim so
    the route table never collapses. ``samples`` holds the hop's own probe
    history. When the hop is the route's final hop and its IP equals the
    target's resolved IP, the session shares its own ``samples`` deque
    reference here so the target and final-hop streams are literally the same
    buffer (no double-probe, no double-append). The same sharing applies to
    ``rollup`` (Slice D2): when the final hop IS the target,
    :meth:`SessionManager._discover` rebinds ``hop.rollup`` to
    ``session.target_rollup`` so the two streams share one rollup.
    """

    index: int
    ip: str | None
    dns: str | None
    samples: deque[Sample] = field(default_factory=lambda: deque(maxlen=SAMPLE_CAP))
    rollup: StreamRollup = field(default_factory=StreamRollup)
    # Lifetime per-hop probe/reply counters for the ICMP non-responsive
    # classification (``TraceSession.hop_icmp_non_responsive``). They reset
    # naturally: every discovery run (initial, or a restart after a paused
    # discovery was cancelled) builds fresh Hop objects, and a trace reset
    # creates a whole new session.
    probes: int = 0
    replies: int = 0


@dataclass(slots=True)
class ProbeResult:
    """One element of a per-round SSE event: hop index + RTT (or loss)."""

    hop: int
    rtt_ms: float | None


@dataclass(slots=True)
class RoundEvent:
    """Per-round probe event pushed to SSE subscribers.

    ``hop`` indexes match :class:`Hop.index`; ``hop == 0`` denotes the target
    itself (used when route discovery is pending or has failed).
    """

    t: float
    results: list[ProbeResult]


@dataclass(slots=True)
class RouteEvent:
    """Discovery-completion event pushed to SSE subscribers."""

    hops: list[Hop]


@dataclass(slots=True)
class RoundPlanEntry:
    """One probe destination inside an immutable round plan.

    Captured at round launch so a discovery handoff cannot re-route an
    in-flight round. ``host`` is the address to probe (``None`` for a
    silent hop, which keeps its no-probe loss representation). ``hop`` is
    the :class:`Hop` the sample and lifetime counters belong to (``None``
    for the bare target stream). ``stream`` / ``rollup`` are the exact
    deque/rollup objects the commit appends to — captured by identity so
    discovery's final-hop==target rebinding (which points the new hop at
    the session's target deque/rollup) stays consistent with rounds
    launched before the handoff: both resolve to the same objects.
    ``dispatched`` flips on the probe task's first step so cancellation
    cleanup can distinguish a never-started probe (its permit is released
    by the scheduler's cancel sweep) from one that releases its own permit
    in its ``finally``.
    """

    hop_index: int
    host: str | None
    hop: Hop | None
    stream: deque[Sample]
    rollup: StreamRollup
    dispatched: bool = False


@dataclass(slots=True)
class RoundPlan:
    """Immutable probe plan for one round, captured at launch.

    ``t`` is the epoch launch timestamp and becomes every committed
    sample's ``t``; ``seq`` is the monotonically increasing launch-order
    sequence that orders the launch-ordered commit. Entries snapshot the
    hop list and stream identities at launch, so route discovery
    completing mid-flight cannot mutate an outstanding round, duplicate
    the target sample, or break the final-hop==target deque/rollup alias:
    a pre-discovery round carries only the target entry and commits to the
    target stream — the same object discovery rebinds as the final hop's
    stream when the final hop equals the target.
    """

    seq: int
    t: float
    entries: list[RoundPlanEntry]


class _ActiveRound:
    """Scheduler-side handle for one launched round (worker-region internal).

    ``plan`` is the immutable launch plan; ``probe_entries`` mirrors the
    plan's known-host entries in task order so commit can zip results with
    destinations; ``probe_tasks`` / ``collector`` are the round's owned
    children; ``committed`` flips in :meth:`SessionManager._commit_round`
    (a done-callback then retires the record from the scheduler's list);
    ``collector_dispatched`` flips on the collector's first step so the
    cancel sweep can distinguish a collector whose ``finally`` already
    released the pending-round slot from one cancelled before it started.
    """

    __slots__ = (
        "collector",
        "collector_dispatched",
        "committed",
        "plan",
        "probe_entries",
        "probe_tasks",
    )

    def __init__(self, plan: RoundPlan) -> None:
        self.plan = plan
        self.probe_entries: list[RoundPlanEntry] = []
        self.probe_tasks: list[asyncio.Task[float | None]] = []
        self.collector: asyncio.Task[None] | None = None
        self.collector_dispatched = False
        self.committed = False


@dataclass(slots=True)
class TraceSession:
    """One target trace: probe loop + bounded sample history + subscribers."""

    id: str
    target: str
    interval_s: float
    state: str = "running"  # "running" | "paused"
    created_at: float = field(default_factory=time.time)
    # User-visible label. ``None`` means "fall back to ``target``" (the
    # spec default). Stored verbatim; sanitization happens at the API
    # boundary (``web.CreateTraceRequest`` / the persist endpoint) and
    # again inside :func:`sanitize_name` on read.
    name: str | None = None
    # Whether this session is a persistent (background) collection whose
    # definition is written to the state file and auto-restored on boot.
    # Non-persistent sessions are simply absent from the file; nothing
    # about their lifecycle touches disk.
    persistent: bool = False
    # Epoch seconds at which the session last entered the "paused" state,
    # or ``None`` while running. Recorded by :meth:`SessionManager.pause`
    # when the pause completes and cleared by :meth:`SessionManager.start`
    # (the resume path) when probing restarts. Surfaced via
    # :meth:`summary` and :meth:`snapshot` so every client attached to the
    # session can anchor its paused-window freeze to the same authoritative
    # server instant instead of guessing from sample timestamps.
    paused_at: float | None = None
    # Target's own sample stream. Always populated by the probe loop, even
    # before/without route discovery (Slice 2 back-compat). When route
    # discovery succeeds and the final hop's IP equals ``target_ip``, the
    # final hop's ``Hop.samples`` is set to *this same deque* so target and
    # final-hop streams are unified.
    samples: deque[Sample] = field(default_factory=lambda: deque(maxlen=SAMPLE_CAP))
    # Slice D2 — fixed-wall-clock 1-minute rollup of the target stream.
    # Updated in the same on-loop probe write path as ``samples`` (no await
    # between the deque append and the rollup fold) so a concurrent history
    # read cannot observe a half-updated state. When the final hop IS the
    # target, :meth:`SessionManager._discover` rebinds ``hop.rollup`` to
    # THIS object so the two streams share one rollup (mirroring the deque
    # sharing).
    target_rollup: StreamRollup = field(default_factory=StreamRollup)
    # Discovered hops. Populated by the SessionManager's discovery task on
    # success; remains empty on failure or while pending.
    hops: list[Hop] = field(default_factory=list)
    # route_state transitions: pending -> ready | failed. Driving UI/SSE
    # behaviour. Snapshot exposes this verbatim.
    route_state: str = "pending"
    route_error: str | None = None
    # Resolved target IP (best-effort). Used to detect the final-hop-is-target
    # case so we do not double-probe. ``None`` until resolution completes
    # (or on resolution failure).
    target_ip: str | None = None
    target_dns: str | None = None
    _task: asyncio.Task[None] | None = None
    _discover_task: asyncio.Task[None] | None = None
    _discovery_restart: bool = False
    # Scheduler budgets for overlapping rounds (PingPlotter-parity
    # cadence). All three are touched only on the event loop by the
    # scheduler and its lifecycle cleanup. ``_probe_permits`` counts
    # in-flight ping processes, held from round launch through engine
    # reaping (``MAX_OUTSTANDING_PROBES``); ``_pending_rounds`` counts
    # launched-but-uncommitted rounds, including completed rounds waiting
    # behind an earlier one for launch-ordered commit
    # (``MAX_PENDING_ROUNDS``); ``_round_seq`` is the launch-order
    # sequence used to order commits.
    _probe_permits: int = 0
    _pending_rounds: int = 0
    _round_seq: int = 0
    _subscribers: set[asyncio.Queue] = field(default_factory=set)
    _closed: bool = False
    # Serialize lifecycle transitions and persistence compensation for one
    # session across ASGI tasks and persistence worker threads.
    _mutation_lock: threading.Lock = field(default_factory=threading.Lock)

    def summary(self) -> dict:
        """Public summary used by listing and state-transition endpoints."""
        return {
            "id": self.id,
            "target": self.target,
            "interval_s": self.interval_s,
            "state": self.state,
            "created_at": self.created_at,
            "paused_at": self.paused_at,
            "sample_count": len(self.samples),
            "name": self.name,
            "persistent": self.persistent,
        }

    def _window(
        self,
        focus_s: float,
        samples: deque[Sample] | None = None,
        end: float | None = None,
    ) -> list[Sample]:
        """Return samples within ``focus_s`` seconds of ``end`` (0 = all).

        Operates on ``samples`` if given (per-hop), else on the target stream.
        ``end`` is the window's right edge; ``None`` means "now". Snapshot
        capture passes one shared anchor (``captured_at``) so every window,
        stat, and cutoff inside a single payload is filtered against the
        same instant. The right edge is hard: an explicit ``end`` excludes
        samples newer than it (bounds inclusive on both sides) so a
        future-dated record cannot influence stats or cutoffs past the
        declared anchor. ``focus_s == 0`` with an explicit ``end`` keeps
        every retained sample at/before that edge; without an explicit
        ``end`` it keeps its ordinary all-retained-history semantics.
        """
        buf = self.samples if samples is None else samples
        now = time.time() if end is None else end
        if focus_s and focus_s > 0:
            cutoff = now - focus_s
            return [s for s in buf if cutoff <= s.t <= now]
        if end is None:
            return list(buf)
        return [s for s in buf if s.t <= end]

    def _capture_anchor(self) -> float:
        """Single effective epoch a snapshot's windows are anchored to.

        ``time.time()`` for a running session; ``paused_at`` for a paused
        one — frozen at the pause instant so a paused trace's window does
        not keep sliding (and eventually empty) as wall-clock time passes.
        Snapshot capture reads this once and derives every window, stat,
        and cutoff in the payload from the same value.
        """
        if self.state == "paused" and self.paused_at is not None:
            return self.paused_at
        return time.time()

    @staticmethod
    def _cutoffs(window: list[Sample]) -> tuple[float | None, float | None]:
        """``(sample_through_t, sample_from_t)`` extrema of one stats window.

        Newest and oldest committed sample ``t`` in the exact window the
        accompanying stats were computed over — losses included, since they
        count in ``stats.count`` — or ``(None, None)`` when the window is
        empty (no committed samples, no baseline to adopt).
        """
        if not window:
            return None, None
        ts = [s.t for s in window]
        return max(ts), min(ts)

    @staticmethod
    def _stats_for(window: list[Sample]) -> dict:
        """Compute read-time stats over a window of samples.

        Losses (``rtt_ms is None``) are excluded from avg/min but counted in
        ``count`` and ``loss_pct``. With no successful samples in the window,
        ``avg_ms`` and ``min_ms`` are ``None``; ``loss_pct`` reflects the
        ratio of losses to total samples seen.
        """
        count = len(window)
        successful = [s.rtt_ms for s in window if s.rtt_ms is not None]
        losses = count - len(successful)

        avg_ms = sum(successful) / len(successful) if successful else None
        min_ms = min(successful) if successful else None
        cur_ms = window[-1].rtt_ms if window else None
        loss_pct = (losses / count * 100.0) if count else 0.0

        return {
            "count": count,
            "avg_ms": avg_ms,
            "min_ms": min_ms,
            "cur_ms": cur_ms,
            "loss_pct": loss_pct,
        }

    def stats(self, focus_s: float) -> dict:
        """Stats for the target (== final hop when route is ready and matches)."""
        return self._stats_for(self._window(focus_s))

    def hop_stats(self, hop_index: int, focus_s: float) -> dict | None:
        """Stats for a specific hop index, or ``None`` if no such hop."""
        hop = next((h for h in self.hops if h.index == hop_index), None)
        if hop is None:
            return None
        return self._stats_for(self._window(focus_s, hop.samples))

    def _hop_is_destination(self, hop: Hop) -> bool:
        """True when ``hop`` is the trace destination (the target itself)."""
        return (
            self.route_state == "ready"
            and self.target_ip is not None
            and bool(self.hops)
            and hop is self.hops[-1]
            and hop.ip == self.target_ip
        )

    def hop_icmp_non_responsive(self, hop: Hop) -> bool:
        """Neutral "ICMP non-responsive" classification for one hop.

        True only when at least :data:`ICMP_NONRESPONSIVE_MIN_PROBES` probes
        were sent and no successful reply was EVER observed in the current
        trace (lifetime ``replies`` counter — a reply that has already aged
        out of every rolling window still disqualifies). The destination is
        excluded: its 100% loss remains an error. Pure read over the hop's
        counters; presentation (neutral gray + descriptive text, counts and
        loss preserved) is the client's job.
        """
        if self._hop_is_destination(hop):
            return False
        return hop.probes >= ICMP_NONRESPONSIVE_MIN_PROBES and hop.replies == 0

    def hop_samples(self, hop_index: int, focus_s: float) -> list[Sample] | None:
        """Recent samples for a specific hop index (None if unknown)."""
        hop = next((h for h in self.hops if h.index == hop_index), None)
        if hop is None:
            return None
        window = self._window(focus_s, hop.samples)
        return window[-HOP_SNAPSHOT_TAIL:]

    def snapshot(self, focus_s: float) -> dict:
        """Full snapshot payload for ``GET /api/traces/{id}/snapshot``.

        Top-level ``stats`` / ``samples`` always reflect the target's stream
        (which is the final hop's stream when route is ready and the final
        hop equals the target — they share the same deque reference).

        Cutoff contract: ``captured_at`` is the single effective epoch this
        payload was captured at and the right edge of EVERY window in it —
        ``paused_at`` (frozen) for a paused session, otherwise the
        capture-time ``time.time()``. ``sample_through_t`` /
        ``sample_from_t`` are the extrema of the exact window that produced
        the accompanying ``stats``, null when that window is empty. The
        top-level trio describes the target stream; every ``hops[]`` entry
        carries its own matching trio for its own stream. Capture must run
        on the event loop with no intervening ``await`` so a concurrent
        probe append cannot split stats from cutoffs.
        """
        captured_at = self._capture_anchor()
        window = self._window(focus_s, end=captured_at)
        tail = window[-SNAPSHOT_TAIL:]
        through_t, from_t = self._cutoffs(window)
        hops = []
        for hop in self.hops:
            # Each hop's cutoffs and stats come from ITS OWN window, filtered
            # against the same captured_at anchor as the target's window.
            hop_window = self._window(focus_s, hop.samples, end=captured_at)
            hop_through, hop_from = self._cutoffs(hop_window)
            hops.append(
                {
                    "index": hop.index,
                    "ip": hop.ip,
                    "dns": hop.dns,
                    "icmp_non_responsive": self.hop_icmp_non_responsive(hop),
                    "captured_at": captured_at,
                    "sample_through_t": hop_through,
                    "sample_from_t": hop_from,
                    "stats": self._stats_for(hop_window),
                }
            )
        return {
            "id": self.id,
            "target": self.target,
            "target_ip": self.target_ip,
            "target_dns": self.target_dns,
            "state": self.state,
            "paused_at": self.paused_at,
            "route_state": self.route_state,
            "focus_s": focus_s,
            "name": self.name,
            "persistent": self.persistent,
            "captured_at": captured_at,
            "sample_through_t": through_t,
            "sample_from_t": from_t,
            "hops": hops,
            "stats": self._stats_for(window),
            "samples": [{"t": s.t, "rtt_ms": s.rtt_ms} for s in tail],
        }

    def snapshot_hop(self, hop_index: int, focus_s: float) -> dict | None:
        """Per-hop snapshot for ``GET /api/traces/{id}/snapshot?hop=<index>``.

        Returns ``None`` when no hop has the requested index. Shape mirrors
        :meth:`snapshot` but scoped to one hop, with the same cutoff
        contract: ``captured_at`` anchors the window (``paused_at`` frozen
        when paused), and ``sample_through_t`` / ``sample_from_t`` are the
        extrema of this hop's exact stats window, null when empty.
        """
        hop = next((h for h in self.hops if h.index == hop_index), None)
        if hop is None:
            return None
        captured_at = self._capture_anchor()
        window = self._window(focus_s, hop.samples, end=captured_at)
        tail = window[-HOP_SNAPSHOT_TAIL:]
        through_t, from_t = self._cutoffs(window)
        return {
            "id": self.id,
            "target": self.target,
            "hop": hop.index,
            "ip": hop.ip,
            "dns": hop.dns,
            "icmp_non_responsive": self.hop_icmp_non_responsive(hop),
            "focus_s": focus_s,
            "captured_at": captured_at,
            "sample_through_t": through_t,
            "sample_from_t": from_t,
            "stats": self._stats_for(window),
            "samples": [{"t": s.t, "rtt_ms": s.rtt_ms} for s in tail],
        }

    def history_samples(
        self, hop_index: int, start: float, end: float
    ) -> tuple[float | None, list[tuple[float, float | None]]] | None:
        """Capture ``(earliest_t, samples)`` for one stream, clamped to ``[start, end]``.

        Used by ``GET /api/traces/{id}/history`` (Slice D1). ``hop_index == 0``
        is ALWAYS the target stream (``self.samples``), mirroring the SSE/export
        hop-numbering contract; ``hop_index >= 1`` must match a discovered route
        hop. Returns ``None`` when the hop index is unknown (no matching route
        hop); returns ``(None, [])`` when the stream is empty.

        ``earliest_t`` is the timestamp of the earliest retained sample in the
        stream, regardless of the query window, or ``None`` when the stream is
        empty — so a client can distinguish "no data" from "data older than
        retention" without a second round-trip.

        ``samples`` is a list of ``(t, rtt_ms)`` tuples ascending by ``t``,
        clamped to ``[start, end]``. ``rtt_ms`` is ``None`` for lost probes.

        Phase-split contract: this method MUST run ON the event loop. It
        iterates the live deque exactly once and copies its contents into
        tuples of immutable scalars (float / None). The returned list owns no
        references to the live deque, so the caller is free to serialize it
        (build the JSON response body) without racing a concurrent probe
        write — the same on-loop discipline as
        :meth:`export_snapshot`. The payload is bounded by
        :data:`SAMPLE_CAP` (10 000 samples/stream), so the on-loop pass is
        acceptable (no semaphore, no off-loop split needed).
        """
        stream = self._stream_for_hop(hop_index)
        if stream is None:
            return None
        captured = [(s.t, s.rtt_ms) for s in stream]
        if not captured:
            return None, []
        # Probe writes are normally chronological, but the public/test append
        # hook and clock adjustments can produce an out-of-order deque. The
        # endpoint contract is timestamp order, and earliest_t means the
        # earliest retained timestamp rather than merely the deque front.
        captured.sort(key=lambda sample: sample[0])
        earliest = captured[0][0]
        samples = [sample for sample in captured if start <= sample[0] <= end]
        return earliest, samples

    def _stream_for_hop(self, hop_index: int) -> deque[Sample] | None:
        """Resolve a hop index to its live sample deque, or ``None`` if unknown.

        ``hop_index == 0`` is always the target stream (``self.samples``),
        matching the SSE/export contract where hop 0 denotes the target stream
        used while the route is pending/failed/silent-tail, and — when the
        route's final hop IS the target — the final hop's deque is the SAME
        reference (so ``hop=0`` and ``hop=final`` return identical data by
        design). ``hop_index >= 1`` must match a discovered route hop index.
        """
        if hop_index == 0:
            return self.samples
        hop = next((h for h in self.hops if h.index == hop_index), None)
        if hop is None:
            return None
        return hop.samples

    def history_buckets(
        self, hop_index: int, start: float, end: float
    ) -> tuple[int | None, list[dict]] | None:
        """Capture ``(earliest_t, buckets)`` for one stream's rollup.

        Used by ``GET /api/traces/{id}/history?resolution=1m`` (Slice D2).
        Returns ``None`` when the hop index is unknown; returns ``(None, [])``
        when the rollup is empty. Same hop-numbering contract as
        :meth:`history_samples`: ``hop_index == 0`` is always the target
        rollup; ``hop_index >= 1`` must match a discovered route hop. When
        the final hop IS the target the two rolls share one rollup, so
        ``hop=0`` and ``hop=final`` return identical data by design.

        ``buckets`` is a list of plain dicts ascending by ``t``, clamped to
        ``[start, end]``. Empty buckets (paused gaps, pre-start) never
        exist in the rollup, so the response is naturally sparse. A
        full-loss bucket (``count > 0`` and all losses) carries
        ``avg_ms`` / ``min_ms`` / ``max_ms`` as ``None``.

        ``earliest_t`` is the oldest retained bucket's ``t`` regardless of
        the query window, or ``None`` when the rollup is empty.

        Phase-split contract: same on-loop immutable capture discipline as
        :meth:`history_samples` — :meth:`StreamRollup.capture` copies each
        bucket into a fresh dict of immutable scalars, so the returned list
        owns no references to the live rollup. The payload is bounded by
        :data:`ROLLUP_CAP` (1 440 buckets/stream), so the on-loop pass is
        acceptable.
        """
        rollup = self._rollup_for_hop(hop_index)
        if rollup is None:
            return None
        return rollup.capture(start, end)

    def _rollup_for_hop(self, hop_index: int) -> StreamRollup | None:
        """Resolve a hop index to its rollup, or ``None`` if unknown.

        Mirrors :meth:`_stream_for_hop`: ``hop_index == 0`` is always the
        target rollup; ``hop_index >= 1`` must match a discovered route
        hop. When the final hop IS the target, :meth:`SessionManager._discover`
        rebinds ``hop.rollup`` to ``self.target_rollup`` so both indexes
        resolve to the same object.
        """
        if hop_index == 0:
            return self.target_rollup
        hop = next((h for h in self.hops if h.index == hop_index), None)
        if hop is None:
            return None
        return hop.rollup

    def export_snapshot(
        self,
    ) -> list[tuple[float, int, str | None, str | None, float | None]]:
        """Capture an immutable ``(t, hop_index, ip, dns, rtt_ms)`` row snapshot.

        Mirrors the SSE hop-numbering contract: each discovered route hop
        emits its own samples under its ``index`` (>= 1), and the target
        stream emits under ``hop_index == 0`` ONLY when it is not already
        represented by the route's final hop. That is exactly the
        ``target_covered`` condition evaluated each probe round: when the
        final hop's IP equals the resolved target IP the two deques are
        shared (``hops[-1].samples is session.samples``), so emitting hop 0
        as well would duplicate every target sample. In the pending /
        failed / silent-tail cases the target stream stands alone and is
        exported as hop 0. Rows are sorted by ``(timestamp, hop_index)`` so
        each probe round groups together regardless of insertion order.

        Phase-split contract: this method MUST run ON the event loop. It
        iterates each live deque exactly once and copies its contents into
        tuples of immutable scalars (float / int / str / None). The
        returned list owns no references to the live deques, so the caller
        is free to serialize it OFF the event loop (e.g. via
        ``asyncio.to_thread``) without holding the loop and without racing
        concurrent probe writes — iterating a deque that another coroutine
        is mutating off-loop is the race the prior review fixed, so the
        snapshot/serialize split is load-bearing, not cosmetic.
        """
        streams: list[tuple[int, str | None, str | None, deque[Sample]]] = [
            (hop.index, hop.ip, hop.dns, hop.samples) for hop in self.hops
        ]
        target_shared = bool(self.hops) and self.hops[-1].samples is self.samples
        if not target_shared:
            # IP falls back to the raw target string when resolution has not
            # completed (or failed); matches the snapshot's target_ip surfacing.
            streams.append(
                (0, self.target_ip or self.target, self.target_dns, self.samples)
            )
        rows: list[tuple[float, int, str | None, str | None, float | None]] = []
        for hop_index, ip, dns, samples in streams:
            for sample in samples:
                rows.append((sample.t, hop_index, ip, dns, sample.rtt_ms))
        rows.sort(key=lambda r: (r[0], r[1]))
        return rows

    def append_sample(self, rtt_ms: float | None, t: float | None = None) -> Sample:
        """Append a target sample directly (test/back-compat hook).

        Also publishes a :class:`RoundEvent` with ``hop=0`` so SSE subscribers
        and the new wire format see this sample, and folds the sample into
        :attr:`target_rollup` (Slice D2) so the rollup tier is exercised by
        direct test injection (the same hook the D1 history tests use).
        Per-hop appends are normally done inside the probe loop, not here.
        """
        ts = time.time() if t is None else t
        sample = Sample(t=ts, rtt_ms=rtt_ms)
        self.samples.append(sample)
        self.target_rollup.add_sample(ts, rtt_ms)
        self._publish(RoundEvent(t=ts, results=[ProbeResult(hop=0, rtt_ms=rtt_ms)]))
        return sample

    def append_round(self, t: float, results: list[ProbeResult]) -> RoundEvent:
        """Publish a per-round SSE event without re-appending samples.

        Used by the probe loop after it has already appended per-hop samples
        itself; avoids a double write.
        """
        event = RoundEvent(t=t, results=results)
        self._publish(event)
        return event

    def publish_route_event(self) -> None:
        """Publish a :class:`RouteEvent` describing the current hops.

        Called by the SessionManager once route discovery completes
        successfully so subscribers learn the hop list. Subscribers that
        attach later see the hops via the snapshot endpoint and via the
        replay-on-subscribe hook in the SSE stream.
        """
        self._publish(RouteEvent(hops=list(self.hops)))

    def subscribe(self) -> asyncio.Queue:
        """Register a subscriber queue; the SSE stream uses this."""
        if len(self._subscribers) >= MAX_SUBSCRIBERS_PER_SESSION:
            raise SubscriberLimitReached("session event-stream limit reached")
        queue: asyncio.Queue = asyncio.Queue(maxsize=SUBSCRIBER_QUEUE_CAP)
        self._subscribers.add(queue)
        if self._closed:
            queue.put_nowait(_SESSION_CLOSED)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        """Drop a subscriber queue; safe to call with an unknown queue."""
        self._subscribers.discard(queue)

    def close(self) -> None:
        """Signal all current subscribers that this session is gone."""
        if self._closed:
            return
        self._closed = True
        for queue in list(self._subscribers):
            # Discard stale events so a full bounded queue cannot prevent the
            # terminal marker from reaching the stream.
            while not queue.empty():
                queue.get_nowait()
            queue.put_nowait(_SESSION_CLOSED)

    def _publish(self, event: object) -> None:
        """Push an event to every subscriber without blocking."""
        if self._closed:
            return
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # Drop the event for this subscriber rather than blocking
                # the probe loop. SSE is best-effort live feed.
                pass


class SessionManager:
    """Owns the session registry and the lifecycle of probe tasks."""

    def __init__(
        self,
        probe_fn: Callable[..., Awaitable[float | None]] = probe_once,
        discover_fn: Callable[..., Awaitable[list[RouteHop]]] = discover_route,
        resolve_name_fn: Callable[[str], Awaitable[str | None]] = resolve_name,
        probe_timeout_s: float = PROBE_TIMEOUT_S,
        discover_timeout_s: float = DISCOVER_TIMEOUT_S,
        persistence: PersistentStore | None = None,
    ) -> None:
        self._sessions: dict[str, TraceSession] = {}
        # Keep the cap check and registration atomic for threaded embedders.
        self._registry_lock = threading.Lock()
        self._probe_fn = probe_fn
        self._discover_fn = discover_fn
        self._resolve_name_fn = resolve_name_fn
        self._probe_timeout_s = probe_timeout_s
        self._discover_timeout_s = discover_timeout_s
        # Optional persistence store. When ``None`` the manager is purely
        # in-memory (the original behaviour). When set, lifecycle events
        # that touch a persistent session rewrite the state file. The
        # store is a narrow I/O adapter; this manager owns the decision
        # of WHEN to write and WHAT shape to write.
        self._persistence = persistence
        self._persist_lock = threading.Lock()

    # ------------------------------------------------------------------ CRUD

    def create(
        self,
        target: str,
        interval_s: float = 2.5,
        name: str | None = None,
        persistent: bool = False,
    ) -> TraceSession:
        """Register a new session. Caller is responsible for starting it.

        Raises :class:`SessionLimitReached` when ``MAX_SESSIONS`` sessions are
        already registered. The API layer turns this into HTTP 409 so the
        dashboard can surface it (PingPlotter-style multi-trace tabs cap the
        number of simultaneous traces a single client can spawn).

        ``name`` is the user-visible label (sanitized here; ``None`` means
        "use target"). ``persistent=True`` marks the session as a
        background collection whose definition is written to the state
        file (if a persistence store is wired) and auto-restored on boot.
        """
        with self._registry_lock:
            if len(self._sessions) >= MAX_SESSIONS:
                raise SessionLimitReached(len(self._sessions), MAX_SESSIONS)
            session = TraceSession(
                id=uuid.uuid4().hex,
                target=target,
                interval_s=float(interval_s),
                name=sanitize_name(name) if name else None,
                persistent=bool(persistent),
            )
            self._sessions[session.id] = session
        if session.persistent:
            try:
                self._persist()
            except BaseException:
                # Do not leave an in-memory persistent session behind when
                # its first durable write failed; the API must not return a
                # successful create that cannot survive a restart.
                with self._registry_lock:
                    self._sessions.pop(session.id, None)
                # A failure after os.replace may have made the attempted
                # state visible on disk. Best-effort compensation prevents
                # that partial commit from resurrecting this session.
                try:
                    self._persist()
                except BaseException:  # noqa: BLE001, S110 - best-effort compensation
                    pass
                raise
        return session

    def get(self, session_id: str) -> TraceSession | None:
        with self._registry_lock:
            return self._sessions.get(session_id)

    def list(self) -> list[TraceSession]:
        with self._registry_lock:
            return list(self._sessions.values())

    def _require_registered(self, session: TraceSession) -> None:
        """Raise :class:`SessionNotRegistered` unless ``session`` is the exact
        object currently registered under its id.

        Called by every lifecycle method AFTER acquiring the per-session
        mutation lock. Without this check, a session resolved via
        :meth:`get` before the lock can be deleted by a concurrent caller
        while we waited for the lock; the stale object would then be
        mutated and a success returned for a session the registry (and
        the state file) no longer hold.
        """
        with self._registry_lock:
            if self._sessions.get(session.id) is not session:
                raise SessionNotRegistered(session.id)

    async def create_and_start(
        self,
        target: str,
        interval_s: float = 2.5,
        name: str | None = None,
        persistent: bool = False,
    ) -> TraceSession:
        """Create then start a session in one cancellation-safe step.

        For persistent sessions, :meth:`create` writes the durable
        definition (``desired_state='running'``) and :meth:`start` then
        launches the probe task. The two are SHIELDED together so a
        client cancellation between them cannot leave a durable
        ``running`` claim with no probe task. On cancellation the created
        session is removed (compensation) so the client's cancellation is
        honoured and no durable running-without-task survives.

        For non-persistent sessions there is no durable write; the
        create+start sequence is cancellation-safe by construction (a
        cancellation between create and start leaves a transient
        in-memory session the reaper will eventually reclaim, same as
        the pre-persistence behaviour).
        """
        if not persistent:
            session = self.create(target, interval_s, name, persistent)
            await self.start(session)
            return session

        async def _inner() -> TraceSession:
            session = await asyncio.to_thread(
                self.create, target, interval_s, name, persistent
            )
            await self.start(session)
            return session

        task = asyncio.create_task(_inner())
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            # The shielded create+start continues to completion; wait for
            # it, then compensate by deleting the session so we never
            # leave a durable running session the client abandoned.
            try:
                session = await task
                await self.delete(session)
            except BaseException:  # noqa: BLE001, S110 - best-effort comp
                pass
            raise

    # ---------------------------------------------------------- persistence
    #
    # Persistence is opt-in via the ``persistence`` ctor arg. When unset
    # the manager is purely in-memory (the original behaviour, and the
    # shape every pre-Slice-B test exercises). When set, lifecycle events
    # that touch a persistent session rewrite the state file. Sample
    # history is NEVER persisted — only definitions (id/target/interval/
    # name/persistent/desired_state/created_at).

    def _persist(self) -> None:
        """Rewrite the state file from the current set of persistent sessions.

        No-op when no persistence store is wired. When there are zero
        persistent sessions the file is removed (a boot with no
        persistent sessions reads no file); otherwise the file is
        rewritten atomically in registry-insertion order. Write failures
        propagate to the lifecycle caller so a successful API response
        means the requested persistent state was durable.
        """
        store = self._persistence
        if store is None:
            return
        with self._persist_lock:
            with self._registry_lock:
                persistent_sessions = [
                    s for s in self._sessions.values() if s.persistent
                ]
            if not persistent_sessions:
                store.remove()
                return
            payload = [_serialize_session(s) for s in persistent_sessions]
            store.write(payload)

    async def _persist_async(self) -> None:
        """Run persistence I/O off the event loop."""
        store = self._persistence
        if store is None:
            return
        # Keep even the existence check off the ASGI loop.
        await asyncio.to_thread(self._persist)

    def set_persistent(
        self,
        session: TraceSession,
        persistent: bool,
        name: str | None = None,
    ) -> None:
        """Toggle a session's persistent flag and/or rename it.

        - ``persistent`` is applied unconditionally (coerced to bool).
        - ``name`` is sanitized and applied ONLY when not ``None``;
          passing ``None`` preserves the current name (so a toggle-only
          request does not clobber an existing label).
        - Rewrites the state file via :meth:`_persist` so the change is
          durable immediately.

        **Fast path:** when neither the persistent flag nor the (sanitized)
        name would change, returns without taking the lock or touching
        disk. This avoids fsync + thread-pool amplification on idempotent
        no-op POSTs (the client polls persist state on every snapshot
        adoption, which can issue repeated identical toggles).

        **Pending-mutation rejection:** uses a non-blocking lock acquire.
        If another mutation for this session is already in progress
        (pause/resume/delete/another persist), raises
        :class:`SessionMutationPending` so the API returns 409 instead of
        queueing a blocked worker thread behind the holder.

        **Stale-object guard:** after acquiring the lock, revalidates via
        :meth:`_require_registered` that THIS session is still in the
        registry; raises :class:`SessionNotRegistered` if a concurrent
        delete won the race (so persist-after-delete returns 404, not a
        bogus 200 for a session the registry + disk no longer hold).
        """
        # Validate the object before the no-op shortcut.  The shortcut must
        # avoid the mutation lock and disk I/O, but it must not turn a stale
        # object resolved before a concurrent delete into a successful 200.
        self._require_registered(session)
        # Fast path: no change → skip lock + write entirely. The unlocked
        # read of session.persistent / session.name is a benign race under
        # the GIL (atomic field reads); the worst case is a stale "no-op"
        # decision when another caller is concurrently mutating, in which
        # case the next request corrects it.
        if self._is_persist_noop(session, persistent, name):
            return
        # Non-blocking acquire: do NOT queue a blocked worker behind a
        # concurrent mutation for the same session. The API translates the
        # pending exception into 409 so the client retries cleanly.
        if not session._mutation_lock.acquire(blocking=False):
            raise SessionMutationPending(session.id)
        try:
            self._require_registered(session)
            old_persistent = session.persistent
            old_name = session.name
            session.persistent = bool(persistent)
            if name is not None:
                session.name = sanitize_name(name) or None
            try:
                self._persist()
            except BaseException:
                session.persistent = old_persistent
                session.name = old_name
                # Restore the durable representation too when the store failed
                # after changing the file but before reporting success.
                try:
                    self._persist()
                except BaseException:  # noqa: BLE001, S110 - best-effort compensation
                    pass
                raise
        finally:
            session._mutation_lock.release()

    @staticmethod
    def _is_persist_noop(
        session: TraceSession, persistent: bool, name: str | None
    ) -> bool:
        """True when this persist request would change neither flag nor name."""
        if session.persistent != bool(persistent):
            return False
        if name is not None:
            sanitized = sanitize_name(name) or None
            if session.name != sanitized:
                return False
        return True

    def rename(self, session: TraceSession, name: str | None) -> None:
        """Rename a session (sanitized). Persists only if already persistent.

        Kept as a separate entry point so the API surface can expose a
        pure rename without forcing a persist flag (though the only
        built-in caller today is :meth:`set_persistent`).
        """
        with session._mutation_lock:
            self._require_registered(session)
            old_name = session.name
            session.name = sanitize_name(name) if name else None
            if session.persistent:
                try:
                    self._persist()
                except BaseException:
                    session.name = old_name
                    # Match the combined toggle/rename path: the store may
                    # have replaced the file before reporting failure.
                    try:
                        self._persist()
                    except BaseException:  # noqa: BLE001, S110 - best-effort compensation
                        pass
                    raise

    async def restore(self) -> None:
        """Recreate persistent sessions from the state file on boot.

        Reads the state file via the store's tri-state :meth:`PersistentStore.read`
        and recreates each restored entry up to :data:`MAX_SESSIONS` (the cap
        counts SUCCESSFUL registrations, not file offsets, so invalid entries
        dropped during validation cannot exclude valid later ones). Sessions
        whose recorded desired state was ``running`` have probing started;
        sessions recorded as ``paused`` are recreated paused (``paused_at``
        set to the restore instant per the design note — the original pause
        epoch is intentionally not persisted so a long-down period does not
        anchor the freeze at a stale wall-clock).

        On **degraded** reads (corrupt / unknown schema / oversized / wrong
        shape / any entry dropped) the store quarantines the original bytes
        and enters read-only mode (subsequent persistence writes raise
        :class:`PersistDegraded`); the valid subset is still restored so the
        operator does not lose it while reviewing the quarantine. The
        degraded state surfaces on ``/healthz`` via
        :meth:`PersistentStore.is_degraded`. Resolution is operator-driven:
        review/remove the quarantine, then restart to re-read and clear
        degraded. Missing file = normal fresh start (no degraded flag).

        Duplicate ids and invalid targets in the restored set are skipped
        with a logged warning. The probe loop / traceroute discovery runs
        asynchronously via :meth:`start`, matching the fresh-create path,
        so restore does not block app serving.
        """
        store = self._persistence
        if store is None:
            return
        try:
            # Keep boot-time persistence I/O off the ASGI loop.
            result: ReadResult = await asyncio.to_thread(store.read)
        except BaseException as exc:  # noqa: BLE001 - boot must not crash
            log.warning("netplot: state file read failed (%s); starting empty", exc)
            return
        entries = result.entries
        if result.status == "degraded":
            # Loud log: every skip reason + quarantine path so monitoring
            # and the operator can see exactly what was rejected.
            for reason in result.skipped:
                log.warning("netplot: degraded restore dropped entry (%s)", reason)
            log.warning(
                "netplot: persistence store is DEGRADED (%s); "
                "restored %d valid session(s), "
                "quarantine at %s; persistence writes are BLOCKED until "
                "the operator reviews the quarantine and restarts",
                result.reason,
                len(entries),
                result.quarantine_path,
            )
        if not entries:
            return
        # Cap by SUCCESSFUL registration count, not file offset. The
        # registry check inside the lock below enforces MAX_SESSIONS on
        # actually-registered sessions, so invalid entries (dropped by the
        # store reader) and duplicate ids do not consume cap slots.
        restored = 0
        for entry in entries:
            if restored >= MAX_SESSIONS:
                log.warning(
                    "netplot: reached MAX_SESSIONS (%d) during restore; "
                    "skipping %d remaining valid session(s)",
                    MAX_SESSIONS,
                    len(entries) - restored,
                )
                break
            if not validate_host(entry["target"]):
                log.warning(
                    "netplot: skipping invalid restored target %r", entry["target"]
                )
                continue
            session = TraceSession(
                id=entry["id"],
                target=entry["target"],
                interval_s=entry["interval_s"],
                # Restored persistent sessions are persistent by definition
                # (the manager only writes persistent sessions; the reader
                # coerces the stored flag to True defensively).
                persistent=True,
                name=entry.get("name") or None,
                created_at=entry["created_at"],
            )
            with self._registry_lock:
                # An id collision with an already-restored entry would be a
                # file-shape bug; skip the duplicate rather than overwriting.
                if session.id in self._sessions:
                    log.warning(
                        "netplot: skipping duplicate restored id %s", session.id
                    )
                    continue
                if len(self._sessions) >= MAX_SESSIONS:
                    log.warning(
                        "netplot: MAX_SESSIONS reached during restore; "
                        "skipping remaining valid session(s)"
                    )
                    break
                self._sessions[session.id] = session
            restored += 1
            if entry["desired_state"] == "running":
                await self.start(session)
            else:
                # Paused-restore: probe tasks are not started. Set state and
                # a restore-time pause anchor so the snapshot/sse contract
                # surfaces a real ``paused_at`` (the freeze semantics then
                # follow the existing paused path unchanged).
                with session._mutation_lock:
                    session.state = "paused"
                    session.paused_at = time.time()

    # -------------------------------------------------------------- lifecycle

    async def start(self, session: TraceSession) -> None:
        """Start the probe loop and route discovery; mark the session running.

        Idempotent: a no-op if a task is already running for this session.
        """
        await self._acquire_mutation(session)
        try:
            await self._start_unlocked(session)
        finally:
            session._mutation_lock.release()

    async def _start_unlocked(self, session: TraceSession) -> None:
        """Start a session while its per-session mutation lock is held."""
        if session._task is not None and not session._task.done():
            session.state = "running"
            session.paused_at = None
            return
        session.state = "running"
        session.paused_at = None
        session._task = asyncio.create_task(self._run(session))
        # Discovery is best-effort: failures land in route_state/route_error
        # and the probe loop continues with target-only mode. We only launch
        # discovery when the session is still pending — paused/resumed
        # sessions keep their already-discovered hops.
        if session.route_state == "pending" and (
            session._discover_task is None or session._discovery_restart
        ):
            session._discovery_restart = False
            session._discover_task = asyncio.create_task(self._discover(session))

    async def _acquire_mutation(self, session: TraceSession) -> None:
        """Acquire a session lock without blocking or leaking on cancellation.

        ``threading.Lock.acquire`` cannot be cancelled once dispatched to a
        worker. Shield the worker future and, if the caller is cancelled while
        waiting, finish the acquisition and release it before propagating the
        cancellation; otherwise a late worker acquisition would strand every
        later lifecycle operation for this session.
        """
        acquire_task = asyncio.create_task(
            asyncio.to_thread(session._mutation_lock.acquire)
        )
        try:
            await asyncio.shield(acquire_task)
        except asyncio.CancelledError:
            await acquire_task
            session._mutation_lock.release()
            raise

    async def pause(self, session: TraceSession, *, persist: bool = True) -> None:
        await self._acquire_mutation(session)
        try:
            self._require_registered(session)
            await self._pause_unlocked(session, persist=persist)
        finally:
            session._mutation_lock.release()

    async def _pause_unlocked(
        self, session: TraceSession, *, persist: bool = True
    ) -> None:
        """Cancel the probe loop and discovery; mark the session paused."""
        old_state = session.state
        old_paused_at = session.paused_at
        tasks_to_await: list[asyncio.Task[None]] = []
        task = session._task
        session._task = None
        if task is not None and not task.done():
            task.cancel()
            tasks_to_await.append(task)
        dtask = session._discover_task
        # Do NOT clear _discover_task here if it is still running — the
        # resume path checks for None. We just cancel and let it complete.
        if dtask is not None and not dtask.done():
            dtask.cancel()
            tasks_to_await.append(dtask)
        for t in tasks_to_await:
            try:
                await t
            except asyncio.CancelledError:
                pass
            except BaseException:  # noqa: BLE001, S110 - pause must be robust
                pass
        # A discovery cancelled by pause may be restarted. Completed
        # discovery (ready or failed) remains terminal and is not repeated.
        if (
            session.route_state == "pending"
            and session._discover_task is not None
            and session._discover_task.cancelled()
        ):
            session._discovery_restart = True
        session.state = "paused"
        # Record the authoritative pause instant so every client (including
        # ones that attach later or never received a state event) can anchor
        # their paused-window freeze to the same epoch. ``time.time()`` is
        # read AFTER the probe/discovery tasks have been cancelled and
        # awaited, so this is the moment probing actually stopped.
        session.paused_at = time.time()
        # Persist the desired-state flip so a restart resumes the session in
        # the same state. ``_persist`` is a no-op when no persistence store
        # is wired or when no persistent session exists; non-persistent
        # sessions never touch disk.
        if persist and session.persistent:
            try:
                await self._persist_async()
            except BaseException:
                # A failed lifecycle write must not leave memory paused while
                # disk still requests running (or vice versa). Restore the
                # pre-transition state and best-effort rewrite the durable
                # representation when the atomic write failed after replace.
                if old_state == "running":
                    await self._start_unlocked(session)
                else:
                    session.state = old_state
                    session.paused_at = old_paused_at
                try:
                    await self._persist_async()
                except BaseException:  # noqa: BLE001, S110 - best-effort compensation
                    pass
                raise

    async def resume(self, session: TraceSession) -> None:
        await self._acquire_mutation(session)
        try:
            self._require_registered(session)
            await self._resume_unlocked(session)
        finally:
            session._mutation_lock.release()

    async def _resume_unlocked(self, session: TraceSession) -> None:
        """Restart the probe loop after a pause."""
        old_state = session.state
        old_paused_at = session.paused_at
        await self._start_unlocked(session)
        try:
            if session.persistent:
                await self._persist_async()
        except BaseException:
            # Roll back a failed resume just like a failed pause: stop the
            # newly-started workers, restore the paused marker, and repair the
            # state file when possible. Continuous storage failure remains a
            # best-effort residual, but one write failure cannot create a
            # normal memory/disk split.
            await self._pause_unlocked(session, persist=False)
            session.state = old_state
            session.paused_at = old_paused_at
            if session.persistent:
                try:
                    await self._persist_async()
                except BaseException:  # noqa: BLE001, S110 - best-effort compensation
                    pass
            raise

    async def delete(self, session: TraceSession) -> None:
        await self._acquire_mutation(session)
        try:
            self._require_registered(session)
            await self._delete_unlocked(session)
        finally:
            session._mutation_lock.release()

    async def _delete_unlocked(self, session: TraceSession) -> None:
        """Cancel the probe loop and remove the session from the registry."""
        old_state = session.state
        old_paused_at = session.paused_at
        # Stop probing without first writing a transient paused definition.
        # If the final removal fails, the existing on-disk definition still
        # matches the pre-delete state and the session can be retained.
        await self._pause_unlocked(session, persist=False)
        with self._registry_lock:
            self._sessions.pop(session.id, None)
        try:
            # Persist after the registry mutation so the removed session is
            # dropped from the file (or the file removed entirely if it was
            # the last persistent one).
            if session.persistent:
                await self._persist_async()
        except BaseException:
            # A failed delete is not allowed to leave a session absent from
            # memory while its durable definition can resurrect on reboot.
            # Restore the registry and the pre-delete lifecycle state, then
            # make a best-effort compensating write for failures reported
            # after an atomic replace.
            with self._registry_lock:
                self._sessions[session.id] = session
            if old_state == "running":
                await self._start_unlocked(session)
            else:
                session.state = old_state
                session.paused_at = old_paused_at
            try:
                await self._persist_async()
            except BaseException:  # noqa: BLE001, S110 - best-effort compensation
                pass
            raise
        session.close()

    async def shutdown(self) -> None:
        """Cancel every running session task. Used by the ASGI lifespan."""
        tasks: list[asyncio.Task[None]] = []
        for session in list(self._sessions.values()):
            task = session._task
            session._task = None
            if task is not None and not task.done():
                task.cancel()
                tasks.append(task)
            dtask = session._discover_task
            session._discover_task = None
            if dtask is not None and not dtask.done():
                dtask.cancel()
                tasks.append(dtask)
            session.close()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except BaseException:  # noqa: BLE001, S110 - shutdown must be robust
                # Probe-loop / discovery errors are already normalized inside
                # their tasks, but defend against anything escaping so
                # shutdown is robust.
                pass

    # ----------------------------------------------------------------- worker

    async def _run(self, session: TraceSession) -> None:
        """Probe scheduler: launch rounds on monotonic interval deadlines.

        The first round launches immediately on start/resume, then one
        launch opportunity per ``interval_s`` deadline — independent of
        probe completion, so a hop holding its full reply timeout cannot
        delay the next launch (PingPlotter send-cadence parity). Rounds
        overlap: collection, launch-ordered commit, and SSE publication
        run in per-round collector tasks owned by this method.

        Missed deadlines never produce catch-up bursts: after each wake-up
        the deadline advances along its interval grid past ``now``, so at
        most one round launches per tick even when the loop stalls. A
        launch opportunity whose whole probe plan does not fit the
        session's outstanding-process / pending-round budgets is omitted
        (no partial rounds, no fake losses); the next tick retries.

        This method owns every round it launched. On cancellation
        (pause / delete / shutdown) the ``finally`` cancels and awaits the
        collectors and probe tasks — which includes the engine's subprocess
        kill + reap — so no canceled round can append, aggregate, or
        publish after the lifecycle transition completes, and resume always
        starts a single fresh scheduler generation.
        """
        loop = asyncio.get_running_loop()
        rounds: list[_ActiveRound] = []
        children: set[asyncio.Task[None]] = set()
        # Launch-ordered commit chain: each collector awaits the previous
        # round's commit event before committing. Local per generation, so
        # a resumed session never inherits a stale chain from cancelled
        # rounds of an earlier generation.
        commit_chain = asyncio.Event()
        commit_chain.set()
        try:
            next_deadline = loop.time()
            while True:
                commit_chain = self._launch_round(
                    session, loop, rounds, children, commit_chain
                )
                now = loop.time()
                # Advance to the next future deadline on the interval grid
                # (no catch-up bursts for missed deadlines).
                next_deadline += session.interval_s
                while next_deadline <= now:
                    next_deadline += session.interval_s
                await asyncio.sleep(next_deadline - loop.time())
        except asyncio.CancelledError:
            # Pause / shutdown / delete — exit cleanly without raising.
            pass
        finally:
            # Cancel collectors and probe tasks, then await them so engine
            # reaping completes before this generation is reported dead.
            for record in rounds:
                if record.collector is not None and not record.collector.done():
                    record.collector.cancel()
                for task in record.probe_tasks:
                    if not task.done():
                        task.cancel()
            if children:
                # Done children were discarded via callback; cancelling a
                # done task is a no-op, so this awaits exactly the live
                # subtree. return_exceptions: cancelled probes surface as
                # results, and a broken collector must not mask cleanup.
                await asyncio.gather(*children, return_exceptions=True)
            # Final accounting sweep, run only after every collector is
            # dead so no commit or collector ``finally`` can race it:
            # - probes whose tasks were cancelled before their first step
            #   never ran their ``finally`` — release those permits here;
            # - collectors cancelled before their first step never ran
            #   their ``finally`` either — release those pending slots.
            for record in rounds:
                for entry in record.plan.entries:
                    if entry.host is not None and not entry.dispatched:
                        session._probe_permits -= 1
                if not record.collector_dispatched:
                    session._pending_rounds -= 1

    def _launch_round(
        self,
        session: TraceSession,
        loop: asyncio.AbstractEventLoop,
        rounds: list[_ActiveRound],
        children: set[asyncio.Task[None]],
        commit_chain: asyncio.Event,
    ) -> asyncio.Event:
        """Admit and launch at most one round; return the next chain event.

        Whole-round admission against the session budgets: the round
        launches only when its complete known-host probe plan fits the
        remaining outstanding-process budget AND the pending-round count
        is below :data:`MAX_PENDING_ROUNDS`. Otherwise the launch
        opportunity is omitted (the caller's next scheduled tick retries)
        and the incoming ``commit_chain`` is returned unchanged — no
        partial rounds, no unbounded waiting queue, no fabricated losses
        for probes that were never sent.

        Captures the immutable round plan — sequence, epoch launch
        timestamp, hop identities, and the exact destination stream/rollup
        objects — at launch, so route discovery completing while this
        round is outstanding cannot re-route it or duplicate the target
        sample. Must run on the event loop; contains no awaits or I/O.
        """
        # Snapshot the hop list and stream identities at launch. A plan
        # captured pre-discovery carries only the target entry; commit goes
        # to the target stream, which discovery rebinds as the final hop's
        # own stream when the final hop equals the target — the alias stays
        # correct and the target sample is never duplicated.
        current_hops = list(session.hops)

        # Only the FINAL hop counts as target-covered — an intermediate hop
        # sharing the target's IP would not dedup (a malformed route).
        target_covered = (
            session.route_state == "ready"
            and session.target_ip is not None
            and bool(current_hops)
            and current_hops[-1].ip == session.target_ip
        )

        entries: list[RoundPlanEntry] = []
        for hop in current_hops:
            entries.append(
                RoundPlanEntry(
                    hop_index=hop.index,
                    host=hop.ip,
                    hop=hop,
                    stream=hop.samples,
                    rollup=hop.rollup,
                )
            )
        if not target_covered:
            entries.append(
                RoundPlanEntry(
                    hop_index=0,
                    host=session.target,
                    hop=None,
                    stream=session.samples,
                    rollup=session.target_rollup,
                )
            )

        probes = [entry for entry in entries if entry.host is not None]
        # Whole-round admission. Silent hops keep their no-probe
        # representation and do not consume budget.
        if session._probe_permits + len(probes) > MAX_OUTSTANDING_PROBES:
            return commit_chain
        if session._pending_rounds >= MAX_PENDING_ROUNDS:
            return commit_chain

        record = _ActiveRound(
            RoundPlan(seq=session._round_seq, t=time.time(), entries=entries)
        )
        record.probe_entries = probes
        session._round_seq += 1
        session._pending_rounds += 1
        rounds.append(record)

        for entry in probes:
            # The permit is acquired here (synchronously, on-loop) and
            # held through engine spawn/wait/kill/reap: released by the
            # probe task's ``finally`` when it started running, or by the
            # cancel sweep in :meth:`_run`'s ``finally`` when cancellation
            # hit before the task's first step.
            session._probe_permits += 1
            task = loop.create_task(self._probe_one(session, entry))
            record.probe_tasks.append(task)
            children.add(task)
            task.add_done_callback(children.discard)

        # Launch-ordered commit: this round waits for its predecessor's
        # commit event; later rounds wait for this round's.
        prev_committed = commit_chain
        committed = asyncio.Event()
        record.collector = loop.create_task(
            self._collect_round(session, record, prev_committed, committed)
        )
        children.add(record.collector)
        record.collector.add_done_callback(children.discard)

        def _retire(_task: asyncio.Task[None]) -> None:
            # Committed rounds are fully accounted (samples appended once,
            # pending slot released by the collector's finally, permits by
            # the probe finallys) — drop the handle so the scheduler's
            # record list stays bounded over a long-lived session.
            if record.committed:
                rounds.remove(record)

        record.collector.add_done_callback(_retire)
        return committed

    async def _probe_one(
        self, session: TraceSession, entry: RoundPlanEntry
    ) -> float | None:
        """Run one probe with the session's reply timeout and permit held.

        The 3-second reply allowance is the protocol window only — the
        permit additionally covers engine spawn, the reply-wait watchdog,
        kill, and reap, so the outstanding-process budget counts real ping
        processes. ``dispatched`` flips before the first await: if a
        lifecycle cancellation lands before this task's first step the
        ``finally`` never runs, and :meth:`_run`'s sweep releases the
        permit instead.
        """
        entry.dispatched = True
        try:
            return await self._probe_fn(entry.host, self._probe_timeout_s)
        finally:
            session._probe_permits -= 1

    async def _collect_round(
        self,
        session: TraceSession,
        record: _ActiveRound,
        prev_committed: asyncio.Event,
        committed: asyncio.Event,
    ) -> None:
        """Collect one round's replies asynchronously, commit in launch order.

        Replies are awaited past the interval: a response taking longer
        than the configured interval but less than its reply deadline is a
        success, and pending work is never recorded as a loss. The commit
        waits for the previous round's commit event, so history, rollups,
        and the SSE feed stay launch-ordered with no duplicates even when
        a later round finishes first. Cancellation discards the round
        uncommitted — no partial writes, no false losses.

        The collector's ``finally`` releases the pending-round slot for
        every round it started, whichever way it ends (commit, discard on
        broken probe_fn, or cancellation); the cancel sweep in
        :meth:`SessionManager._run` releases the slot only for collectors
        cancelled before their first step.
        """
        record.collector_dispatched = True
        try:
            results = await asyncio.gather(*record.probe_tasks)
            await prev_committed.wait()
            self._commit_round(session, record, results)
            committed.set()
        except asyncio.CancelledError:
            raise
        except BaseException:
            # Discard the round like a cancellation: uncommitted, no
            # fabricated samples. Log instead of killing the scheduler —
            # probe_fn never raises in production (probe_once normalizes
            # everything), so this only guards injected/test fakes.
            log.exception(
                "netplot: session %s discarded round %d after collection error",
                session.id,
                record.plan.seq,
            )
        finally:
            session._pending_rounds -= 1

    def _commit_round(
        self,
        session: TraceSession,
        record: _ActiveRound,
        results: list[float | None],
    ) -> None:
        """Append one round's samples exactly once, in launch order.

        Runs synchronously on the event loop with no awaits: the deque
        appends, rollup folds, lifetime counters, and the SSE round-event
        publish are one atomic step, so a concurrent read can never
        observe a half-committed round and a cancellation can never
        interrupt one mid-write. ``results`` align with
        ``record.probe_tasks`` (gather preserves task order, which follows
        ``plan.entries`` order).
        """
        timestamp = record.plan.t
        rtt_by_index = {
            entry.hop_index: rtt
            for entry, rtt in zip(record.probe_entries, results)
            if entry.host is not None
        }
        # Per-stream appends in plan order (hops, then the target entry
        # when present). Slice D2 — the SAME sample is folded into the
        # stream's rollup in this same on-loop turn (no await between the
        # deque append and the rollup fold). Silent hops (host is None)
        # keep their loss-sample representation.
        for entry in record.plan.entries:
            rtt = rtt_by_index.get(entry.hop_index) if entry.host is not None else None
            entry.stream.append(Sample(t=timestamp, rtt_ms=rtt))
            entry.rollup.add_sample(timestamp, rtt)
            if entry.hop is not None:
                # Lifetime counters for the ICMP non-responsive
                # classification; recomputed on read so the flag flips
                # back the moment a reply lands.
                entry.hop.probes += 1
                if rtt is not None:
                    entry.hop.replies += 1

        # Per-round SSE event in hop-index order (hop=0 sorts before >=1).
        results_event = [
            ProbeResult(
                hop=entry.hop_index,
                rtt_ms=rtt_by_index.get(entry.hop_index)
                if entry.host is not None
                else None,
            )
            for entry in record.plan.entries
        ]
        results_event.sort(key=lambda r: r.hop)
        record.committed = True
        session.append_round(timestamp, results_event)

    async def _discover(self, session: TraceSession) -> None:
        """Resolve the target, run traceroute, populate hops, publish route.

        Runs as a background task launched by :meth:`start`. On any failure
        sets ``route_state='failed'`` and ``route_error`` so callers can
        inspect what went wrong; the probe loop continues in target-only mode.
        """
        try:
            # 1. Resolve target forward/reverse so we can detect the
            #    final-hop-is-target dedup case and report target_dns.
            target_ip, target_dns = await self._resolve_target(session.target)
            session.target_ip = target_ip
            session.target_dns = target_dns

            # 2. Run traceroute (raises RouteDiscoveryError on failure).
            raw_hops = await self._discover_fn(session.target, self._discover_timeout_s)

            # 3. Reverse-resolve each hop's IP (best-effort, parallel).
            resolved: list[Hop] = []
            for raw in raw_hops:
                dns: str | None = None
                if raw.ip:
                    dns = await self._resolve_name_fn(raw.ip)
                resolved.append(Hop(index=raw.index, ip=raw.ip, dns=dns))

            # 4. If the final hop equals the resolved target IP, share the
            #    target's sample deque AND its rollup so we do not
            #    double-probe/double-aggregate and the target's streams
            #    literally ARE the final hop's streams. Slice D2 mirrors the
            #    deque sharing for the rollup tier.
            if (
                resolved
                and session.target_ip is not None
                and resolved[-1].ip == session.target_ip
            ):
                resolved[-1].samples = session.samples
                resolved[-1].rollup = session.target_rollup

            # 5. Publish state. Set route_state BEFORE publishing so any
            #    subscriber that reads session state during dispatch sees the
            #    final values.
            session.hops = resolved
            session.route_state = "ready"
            session.publish_route_event()
        except asyncio.CancelledError:
            raise
        except RouteDiscoveryError as exc:
            session.route_state = "failed"
            session.route_error = str(exc)
        except BaseException as exc:  # noqa: BLE001 - degrade rather than crash
            session.route_state = "failed"
            session.route_error = f"{exc.__class__.__name__}: {exc}"

    async def _resolve_target(self, target: str) -> tuple[str | None, str | None]:
        """Best-effort forward + reverse DNS for the target.

        Returns ``(target_ip, target_dns)``. Both may be ``None`` on lookup
        failure — ordinary resolution errors are swallowed so target
        resolution never stalls the session. Cancellation is NOT swallowed:
        it propagates (same lifecycle invariant as :meth:`_discover`) so
        pause/shutdown actually stops a pending lookup instead of letting
        discovery run on into the next await.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return None, None

        ip: str | None = None
        try:
            if _is_ip_literal(target):
                ip = target
            else:
                infos = await loop.getaddrinfo(target, None)
                if infos:
                    # sockaddr[0] is the host string for both IPv4 and IPv6
                    # sockaddr tuples, but the typed union includes int port
                    # members — cast through ``str`` to keep the type checker
                    # happy without runtime overhead.
                    ip = str(infos[0][4][0])
        except asyncio.CancelledError:
            # Pause/shutdown cancelled this lookup mid-flight. Swallowing it
            # here would let _discover continue into its next await (the
            # traceroute call) after the task was already cancelled, so pause
            # would wait on a discovery that refuses to stop. Propagate.
            raise
        except BaseException:  # noqa: BLE001 - best effort
            ip = None

        name: str | None = None
        if ip:
            name = await self._resolve_name_fn(ip)

        return ip, name


def _is_ip_literal(value: str) -> bool:
    """True if ``value`` is a numeric IPv4 or IPv6 literal (no DNS)."""
    try:
        socket.inet_pton(socket.AF_INET, value)
        return True
    except OSError:
        pass
    try:
        socket.inet_pton(socket.AF_INET6, value)
        return True
    except OSError:
        return False


def _serialize_session(session: TraceSession) -> dict:
    """Serialize a persistent session's DEFINITION for the state file.

    Only the restart-restartable fields are written — never sample
    history (samples restart empty by design). ``desired_state`` is
    derived from the in-memory ``state`` so a pause/resume persisted at
    rest is honoured on the next boot.
    """
    return {
        "id": session.id,
        "target": session.target,
        "interval_s": session.interval_s,
        "name": session.name,
        "persistent": True,  # only persistent sessions are ever written
        "desired_state": session.state,  # "running" | "paused"
        "created_at": session.created_at,
    }
