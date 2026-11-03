"""FastAPI application and dashboard assets for netplot."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import math
import re
import unicodedata
from collections.abc import AsyncIterator, Callable, Iterable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse, StreamingResponse
from pydantic import BaseModel, field_validator
from starlette.staticfiles import StaticFiles

from . import influx as influx_mod
from .engine import validate_host
from .metrics import capture_metrics, render_metrics
from .persistence import (
    MAX_NAME_LEN,
    PersistentStore,
    default_data_dir,
    sanitize_name,
)
from .security import BrowserBoundaryMiddleware
from .session import (
    _SESSION_CLOSED,
    DEFAULT_FOCUS_S,
    SAMPLE_CAP,
    PersistDegraded,
    SessionLimitReached,
    SessionManager,
    SessionMutationPending,
    SessionNotRegistered,
    SubscriberLimitReached,
)
from .settings import (
    load_settings,
    make_classifier,
    normalize_cidrs,
    normalize_influx,
    save_settings,
)

__all__ = ["CreateTraceRequest", "create_app"]

log = logging.getLogger("netplot.web")

# Static dashboard assets live alongside this module under ``static/``. The
# path is computed from ``__file__`` so it resolves identically under a
# regular ``pip install``, an editable install, and a wheel build (the files
# are included via ``[tool.setuptools.package-data]`` in ``pyproject.toml``).
_STATIC_DIR = Path(__file__).resolve().parent / "static"
_INDEX_HTML = (_STATIC_DIR / "index.html").read_text(encoding="utf-8")

# Maximum number of CSV exports serialized concurrently per app (= per
# process in the single-worker deployment). CSV serialization is CPU-bound
# and each response body is materialized in memory; without a bound a single
# endpoint-reachable client can stack N serializations on the event loop
# (worst case ~310k rows: 30 hops × 10k + a separate 10k target stream) and
# retain multiple tens-of-MiB response bodies via concurrent slow downloads.
# The bound is small (2) because export is a rare, operator-triggered
# operation, not a hot path — requests that cannot acquire a slot immediately
# get HTTP 429 with a short Retry-After so the dashboard can retry cleanly.
EXPORT_CONCURRENCY_LIMIT = 2
THROUGHPUT_CONCURRENCY_LIMIT = 4
SSE_CONCURRENCY_LIMIT = 32

# Seconds the client should wait before retrying a 429'd export. Short and
# fixed: exports finish in well under a second each, so a 2 s backoff covers
# a full slot-turnover with margin.
EXPORT_RETRY_AFTER_S = "2"


class CreateTraceRequest(BaseModel):
    """Request body for ``POST /api/traces``.

    ``name`` (optional) is the user-visible session label; sanitized to
    a max of :data:`MAX_NAME_LEN` chars with control characters stripped
    (empty / null falls back to the target on the client). ``persistent``
    (optional, default false) marks the session as a background
    collection whose definition is written to the state file and
    auto-restored on the next server boot.
    """

    target: str
    interval_s: float = 2.5
    name: str | None = None
    persistent: bool = False

    @field_validator("target")
    @classmethod
    def _validate_target(cls, value: str) -> str:
        if validate_host(value) is None:
            raise ValueError("target must be a valid hostname or IP")
        return value

    @field_validator("interval_s", mode="before")
    @classmethod
    def _reject_non_finite_interval_input(cls, value: object) -> object:
        # Python's JSON decoder accepts NaN/Infinity extensions. Convert them
        # to a serializable invalid value so FastAPI can return 422 rather than
        # attempting to put a non-standard float in its validation response.
        if isinstance(value, float) and not math.isfinite(value):
            return "non-finite"
        return value

    @field_validator("interval_s")
    @classmethod
    def _validate_interval(cls, value: float) -> float:
        if not math.isfinite(value) or value < 1.0:
            raise ValueError("interval_s must be >= 1.0")
        return value

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = sanitize_name(value)
        if len(cleaned) > MAX_NAME_LEN:
            # sanitize_name already truncates, so this is defensive against a
            # future change to the sanitizer; keep the contract explicit.
            cleaned = cleaned[:MAX_NAME_LEN]
        return cleaned or None


class PersistTraceRequest(BaseModel):
    """Request body for ``POST /api/traces/{id}/persist``.

    ``persistent`` is required (the toggle is the point of the endpoint);
    ``name`` is optional and, when present, renames the session in the
    same call. Both are sanitized/validated as in
    :class:`CreateTraceRequest`.
    """

    persistent: bool
    name: str | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = sanitize_name(value)
        if len(cleaned) > MAX_NAME_LEN:
            cleaned = cleaned[:MAX_NAME_LEN]
        return cleaned or None


class InternalRangesRequest(BaseModel):
    """Body for ``PUT /api/settings/internal``.

    (Module-level like the other request models: web.py uses
    ``from __future__ import annotations``, so FastAPI resolves parameter
    annotations against module globals and a function-local class would
    silently degrade to a query param.)
    """

    cidrs: list[str]


class InfluxSettingsRequest(BaseModel):
    """Body for ``PUT /api/settings/influx``.

    All fields default to empty (no presets). ``url`` empty means
    "unconfigured" (the overlay is off). http(s) only, no userinfo, no
    query string, no fragment; a non-empty URL requires non-empty
    ``database`` and ``interface`` (charset-restricted identifiers).
    """

    url: str = ""
    database: str = ""
    interface: str = ""
    host_tag: str = ""


class SettingsRequest(BaseModel):
    """Body for ``PUT /api/settings`` — both sections in one save.

    Used by the settings dialog's Save button so the UI's one gesture is
    one transaction: either both sections validate, persist, and go live,
    or nothing changes (no partial success between the internal ranges
    and the Influx overlay). ``influx`` is kept as a raw dict so the same
    :func:`normalize_influx` validation (including unknown-field
    rejection) runs as for the individual endpoint.
    """

    cidrs: list[str]
    influx: dict


def create_app(data_dir: Path | str | None = None) -> FastAPI:
    """Build and return the netplot ASGI application.

    Session tasks are tied to the app via the ASGI lifespan so both
    ``TestClient``-driven and uvicorn-driven runs start and stop cleanly.

    ``data_dir`` selects the persistence directory (where
    ``sessions.json`` is written and read on boot). When ``None`` the
    env-var/default resolution in :func:`default_data_dir` is used
    (``$NETPLOT_DATA_DIR`` or ``<cwd>/data``). Persistent (background)
    sessions are restored from the state file on lifespan startup; a
    missing/corrupt file is logged and ignored, never crashing boot.

    Passing ``data_dir`` explicitly is the test isolation knob: pytest
    tests pass a per-test ``tmp_path`` so a written state file never
    leaks into the next run.
    """
    resolved_data_dir = Path(data_dir) if data_dir is not None else default_data_dir()
    persistence = PersistentStore(resolved_data_dir)

    manager = SessionManager(persistence=persistence)

    # Dashboard settings (internal CIDR ranges + InfluxDB overlay config).
    # Loaded once at app build; PUT endpoints rewrite the file and refresh
    # the in-memory copy + classifier. The classifier memoizes per-IP
    # results; it is rebuilt (not mutated) on every settings change.
    settings_state = {"settings": load_settings(resolved_data_dir)}
    internal_classifier = {"fn": make_classifier(settings_state["settings"]["internal_cidrs"])}

    # Settings writes share one transactional path: writers are
    # serialized by a single async lock; the FULL candidate (both
    # sections) is built under the lock, persisted off-loop, and only
    # then published to runtime state + classifier. A failed validation
    # (422 before this point) or a failed persist leaves the old runtime
    # state and the file intact — no partial success, no interleaved
    # sections from concurrent writers.
    settings_lock = asyncio.Lock()

    async def _publish_settings(candidate: Callable[[dict], dict]) -> dict:
        async with settings_lock:
            new_settings = candidate(settings_state["settings"])
            await asyncio.to_thread(save_settings, resolved_data_dir, new_settings)
            settings_state["settings"] = new_settings
            internal_classifier["fn"] = make_classifier(new_settings["internal_cidrs"])
            return new_settings

    # Bounded export concurrency guard. Lives in app closure scope (== one
    # per ASGI app instance, == one per process in the single-worker
    # deployment). Bound to whichever event loop first runs an export; that
    # is the loop Starlette runs the app on, so it stays correct across
    # requests. Exposed on ``app.state`` purely so tests can pre-acquire
    # both slots deterministically to exercise the 429 path.
    export_semaphore = asyncio.Semaphore(EXPORT_CONCURRENCY_LIMIT)
    throughput_semaphore = asyncio.Semaphore(THROUGHPUT_CONCURRENCY_LIMIT)
    sse_semaphore = asyncio.Semaphore(SSE_CONCURRENCY_LIMIT)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        # Restore persistent (background) sessions BEFORE serving so the
        # first request sees the full registry. Restore is non-blocking
        # for app serving: it recreates session objects synchronously and
        # kicks off probe/discovery tasks (``manager.start``), which then
        # proceed asynchronously exactly as a fresh create does. A
        # missing/corrupt/unknown-schema state file is logged and
        # ignored inside ``restore`` so boot never crashes here.
        try:
            await manager.restore()
        except BaseException as exc:  # noqa: BLE001 - boot must not crash
            log.warning("netplot: restore failed (%s); starting empty", exc)
        try:
            yield
        finally:
            await manager.shutdown()

    app = FastAPI(title="netplot", lifespan=lifespan)
    app.add_middleware(BrowserBoundaryMiddleware)
    app.state.throughput_semaphore = throughput_semaphore
    app.state.sse_semaphore = sse_semaphore
    app.state.session_manager = manager
    app.state.export_semaphore = export_semaphore

    # ------------------------------------------------------------- dashboard

    app.mount(
        "/static",
        StaticFiles(directory=str(_STATIC_DIR)),
        name="static",
    )

    @app.get("/", response_class=HTMLResponse)
    def dashboard() -> str:
        return _INDEX_HTML

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        """Liveness + persistence-degraded surface.

        ``status`` is ``ok`` for liveness. ``persistence`` mirrors the
        store's degraded state so monitoring can catch a degraded boot
        (corrupt / oversized / unknown-schema state file, or any session
        entry dropped during validation): ``degraded`` means the store
        is read-only, the original bytes are quarantined, and the
        operator must review the quarantine + restart to clear it.
        Absent when no persistence store is wired.
        """
        status = "ok"
        out: dict[str, str] = {"status": status}
        store = getattr(manager, "_persistence", None)
        if store is not None and store.is_degraded():
            out["persistence"] = "degraded"
        return out

    @app.get("/metrics")
    async def metrics() -> PlainTextResponse:
        """Prometheus exposition endpoint (text/plain; version=0.0.4).

        Emits one series per session plus per-target and per-hop latency/
        loss gauges over a fixed 60-sample window. See ``docs/spec.md``
        "Metrics" for the full contract.

        Performance: the handler is ``async def`` and performs both the
        on-loop capture (:func:`capture_metrics` reads live deques) and the
        pure render (:func:`render_metrics`) synchronously before returning.
        No ``await`` intervenes between the deque reads and the immutable
        copy, so a concurrent probe write cannot mutate a deque
        mid-iteration (the same on-loop discipline as
        :meth:`TraceSession.export_snapshot`). Cardinality is bounded
        (MAX_SESSIONS 8 x <= 30 hops x 60-sample window), so the whole
        scrape is a small, bounded single pass and does not stall the
        event loop meaningfully.
        """
        data = capture_metrics(manager)
        text = render_metrics(data)
        return PlainTextResponse(text, media_type="text/plain; version=0.0.4")

    # ------------------------------------------------------------- settings
    #
    # Two independent features are configured here (see
    # :mod:`netplot.settings`): the internal IP ranges whose DNS names the
    # dashboard's screenshot toggle masks (classification is SERVER-side
    # via stdlib ``ipaddress`` — no JS CIDR parser), and the optional
    # InfluxDB 1.x throughput overlay connection. Both live in
    # ``<data_dir>/settings.json`` (atomic writes, versioned schema).

    @app.get("/api/settings/internal")
    def get_internal_settings() -> dict:
        """Current internal IP ranges (CIDR strings, normalized)."""
        return {"cidrs": settings_state["settings"]["internal_cidrs"]}

    @app.put("/api/settings/internal")
    async def put_internal_settings(req: InternalRangesRequest) -> dict:
        """Replace the internal IP range list; persists to ``settings.json``.

        Invalid CIDRs → 422. The in-memory classifier is rebuilt so the
        next snapshot annotates hops against the new ranges.
        """
        try:
            normalized = normalize_cidrs(req.cidrs)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        await _publish_settings(lambda cur: {**cur, "internal_cidrs": normalized})
        return {"cidrs": normalized}

    @app.get("/api/settings/influx")
    def get_influx_settings() -> dict:
        """Current InfluxDB overlay connection settings."""
        return dict(settings_state["settings"]["influx"])

    @app.put("/api/settings/influx")
    async def put_influx_settings(req: InfluxSettingsRequest) -> dict:
        """Replace the InfluxDB overlay settings; persists to ``settings.json``."""
        try:
            normalized = normalize_influx(
                {
                    "url": req.url,
                    "database": req.database,
                    "interface": req.interface,
                    "host_tag": req.host_tag,
                }
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        await _publish_settings(lambda cur: {**cur, "influx": normalized})
        return dict(normalized)

    @app.put("/api/settings")
    async def put_settings(req: SettingsRequest) -> dict:
        """Replace BOTH settings sections in one transaction.

        Validates the full candidate first (invalid input → 422 with
        nothing changed), persists it, then publishes runtime state +
        classifier. Either both sections go live together or neither
        does; a failed persist leaves the old runtime settings and the
        file untouched. Returns ``{cidrs, influx}``.
        """
        try:
            normalized_cidrs = normalize_cidrs(req.cidrs)
            normalized_influx = normalize_influx(req.influx)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        await _publish_settings(
            lambda cur: {
                **cur,
                "internal_cidrs": normalized_cidrs,
                "influx": normalized_influx,
            }
        )
        return {"cidrs": normalized_cidrs, "influx": dict(normalized_influx)}

    @app.get("/api/throughput")
    async def throughput(start: str | None = None, end: str | None = None) -> dict:
        """Inbound/outbound interface throughput (Mbps) for ``[start, end]``.

        Backend-side InfluxDB 1.x query (async HTTP, total deadline, bounded
        concurrency). The browser never contacts the database. Returns
        ``{start, end, step_s, points: [{t, in_mbps, out_mbps}]}`` — null
        Mbps values mark missing buckets (never zeros). No credentials are
        created, stored, or sent; only the no-auth source configured in
        settings is supported.

        - 400 on missing/non-finite/negative params, ``start > end``, a
          span beyond 24 h, or when the overlay is not configured.
        - 502 when InfluxDB is unreachable, errors, is ambiguous, or
          returns a malformed/oversized response.
        """
        for name, val in (("start", start), ("end", end)):
            if val is None:
                raise HTTPException(
                    status_code=400, detail=f"missing required query param: {name}"
                )
        assert start is not None and end is not None
        try:
            start_s = float(start)
            end_s = float(end)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400, detail="start and end must be epoch seconds"
            )
        if not (math.isfinite(start_s) and math.isfinite(end_s)):
            raise HTTPException(status_code=400, detail="start and end must be finite")
        if start_s < 0 or end_s < 0:
            raise HTTPException(status_code=400, detail="start and end must be >= 0")
        if start_s > end_s:
            raise HTTPException(status_code=400, detail="start must be <= end")
        if (end_s - start_s) > influx_mod.MAX_SPAN_S + 1:
            raise HTTPException(
                status_code=400,
                detail=f"time range too large (max {influx_mod.MAX_SPAN_S}s)",
            )
        cfg = settings_state["settings"]["influx"]
        if not cfg["url"]:
            raise HTTPException(
                status_code=400,
                detail="throughput overlay not configured (set the InfluxDB URL in settings)",
            )
        if throughput_semaphore.locked():
            raise HTTPException(429, "throughput busy", headers={"Retry-After": "2"})
        await throughput_semaphore.acquire()
        try:
            result = await influx_mod.query_throughput(cfg, start_s, end_s)
        except influx_mod.InfluxError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        finally:
            throughput_semaphore.release()
        return {"start": start_s, "end": end_s, **result}

    # --------------------------------------------------------------- traces

    @app.post("/api/traces", status_code=201)
    async def create_trace(req: CreateTraceRequest) -> dict:
        try:
            # create_and_start is cancellation-safe for persistent creates:
            # the durable write + probe-task creation are shielded together
            # so a client cancellation between them cannot leave a durable
            # ``running`` definition with no probe task.
            session = await manager.create_and_start(
                req.target, req.interval_s, req.name, req.persistent
            )
        except SessionLimitReached as exc:
            # Multi-trace cap (PingPlotter-style tabs): the dashboard surfaces
            # this in the status line so the user knows to close a tab before
            # starting another trace. 409 (Conflict) is the closest fit — the
            # request conflicts with the manager's resource cap.
            raise HTTPException(
                status_code=409,
                detail=(
                    f"session cap reached ({exc.count}/{exc.cap}): close an "
                    f"existing trace before starting a new one"
                ),
            ) from exc
        except PersistDegraded:
            raise HTTPException(
                status_code=503,
                detail=(
                    "persistence store is degraded; resolve the quarantine "
                    "and restart before creating persistent sessions"
                ),
            )
        return {
            "id": session.id,
            "target": session.target,
            "interval_s": session.interval_s,
            "state": session.state,
            "name": session.name,
            "persistent": session.persistent,
        }

    @app.get("/api/traces")
    def list_traces() -> list[dict]:
        return [session.summary() for session in manager.list()]

    @app.post("/api/traces/{session_id}/pause")
    async def pause_trace(session_id: str) -> dict:
        session = manager.get(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="unknown session")
        if session.state == "paused":
            raise HTTPException(status_code=409, detail="session already paused")
        try:
            await manager.pause(session)
        except SessionNotRegistered:
            raise HTTPException(
                status_code=404, detail="session was removed concurrently"
            )
        except PersistDegraded:
            raise HTTPException(
                status_code=503,
                detail=(
                    "persistence store is degraded; resolve the quarantine "
                    "and restart before changing session state"
                ),
            )
        return session.summary()

    @app.post("/api/traces/{session_id}/resume")
    async def resume_trace(session_id: str) -> dict:
        session = manager.get(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="unknown session")
        if session.state == "running":
            raise HTTPException(status_code=409, detail="session already running")
        try:
            await manager.resume(session)
        except SessionNotRegistered:
            raise HTTPException(
                status_code=404, detail="session was removed concurrently"
            )
        except PersistDegraded:
            raise HTTPException(
                status_code=503,
                detail=(
                    "persistence store is degraded; resolve the quarantine "
                    "and restart before changing session state"
                ),
            )
        return session.summary()

    @app.post("/api/traces/{session_id}/persist")
    async def persist_trace(session_id: str, req: PersistTraceRequest) -> dict:
        """Toggle a session's persistent flag and/or rename it.

        Body ``{persistent: bool, name?: string}`` flips the
        background-collection flag (writing or removing the session from
        the state file) and, when ``name`` is present, renames the
        session in the same call. Returns the updated summary so the
        caller sees the new ``persistent`` / ``name`` fields without a
        follow-up fetch.

        **No-op fast path:** when neither the persistent flag nor the
        (sanitized) name changes, returns the current summary without
        taking the session lock or touching disk — idempotent POSTs do
        not amplify fsync + thread-pool work.

        **409 on pending mutation:** if another mutation for this session
        is already in progress (pause/resume/delete/another persist),
        returns HTTP 409 instead of queueing a blocked worker thread
        behind the holder; the client retries shortly.

        **404 on concurrent delete:** if the session was removed between
        resolution and lock acquisition, returns HTTP 404 (not a bogus
        200 for a session the registry + disk no longer hold).
        """
        session = manager.get(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="unknown session")
        try:
            await asyncio.to_thread(
                manager.set_persistent, session, req.persistent, req.name
            )
        except SessionMutationPending:
            raise HTTPException(
                status_code=409,
                detail="session mutation in progress; retry shortly",
            )
        except SessionNotRegistered:
            raise HTTPException(
                status_code=404, detail="session was removed concurrently"
            )
        except PersistDegraded as exc:
            raise HTTPException(
                status_code=503,
                detail=(
                    "persistence store is degraded; resolve the quarantine "
                    "and restart before changing persistence"
                ),
            ) from exc
        return session.summary()

    @app.delete("/api/traces/{session_id}", status_code=204)
    async def delete_trace(session_id: str) -> None:
        session = manager.get(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="unknown session")
        try:
            await manager.delete(session)
        except SessionNotRegistered:
            raise HTTPException(
                status_code=404, detail="session was removed concurrently"
            )
        except PersistDegraded:
            raise HTTPException(
                status_code=503,
                detail=(
                    "persistence store is degraded; resolve the quarantine "
                    "and restart before deleting persistent sessions"
                ),
            )

    @app.get("/api/traces/{session_id}/snapshot")
    async def snapshot_trace(
        session_id: str,
        focus: float = DEFAULT_FOCUS_S,
        hop: int | None = None,
    ) -> dict:
        # Capture on the probe event loop without yielding: worker-thread
        # iteration can race with probe writes to the live sample deques.
        session = manager.get(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="unknown session")
        if not math.isfinite(focus) or focus < 0:
            raise HTTPException(status_code=422, detail="focus must be finite and >= 0")
        classify = internal_classifier["fn"]
        if hop is not None:
            payload = session.snapshot_hop(hop, focus)
            if payload is None:
                raise HTTPException(status_code=404, detail=f"unknown hop index: {hop}")
            # Server-side internal-IP classification (stdlib ipaddress) so
            # the client never needs a CIDR parser.
            payload["internal"] = classify(payload.get("ip"))
            return payload
        payload = session.snapshot(focus)
        for hop_payload in payload["hops"]:
            hop_payload["internal"] = classify(hop_payload.get("ip"))
        payload["target_internal"] = classify(payload.get("target_ip"))
        return payload

    @app.get("/api/traces/{session_id}/history")
    async def history_trace(
        session_id: str,
        hop: str | None = None,
        start: str | None = None,
        end: str | None = None,
        resolution: str = "raw",
    ) -> dict:
        """Sample history for one stream over ``[start, end]`` (Slices D1/D2).

        ``GET /api/traces/{id}/history?hop=<index>&start=<epoch_s>&end=<epoch_s>
        &resolution=<raw|1m|auto>``.

        - ``resolution=raw`` (default, Slice D1): raw retained samples for one
          stream → ``{id, hop, resolution: "raw", earliest_t, samples:
          [{t, rtt_ms}]}``.
        - ``resolution=1m`` (Slice D2): per-minute rollup buckets →
          ``{id, hop, resolution: "1m", earliest_t, buckets: [{t, avg_ms,
          min_ms, max_ms, count, loss_count}]}``, ascending by ``t``, clamped
          to ``[start, end]`` and rollup retention (``ROLLUP_CAP`` = 1 440).
          ``t`` is the bucket-start epoch. Full-loss bucket (count>0, all
          lost): avg/min/max null. Empty buckets (paused gaps, pre-start)
          are OMITTED (sparse response). ``earliest_t`` is the oldest
          retained bucket t (null when empty).
        - ``resolution=auto`` (Slice D2): ``raw`` when the requested
          ``[start, end]`` lies fully within raw retention
          (``end - start <= SAMPLE_CAP * interval_s``), else ``1m``. The
          response carries the actual resolution used.

        - ``hop=0`` is the target stream (mirrors SSE/export numbering);
          ``hop>=1`` must match a discovered route hop index. Unknown/out-of-
          range hop → **404**; unknown session → **404**.
        - Malformed params (non-numeric, negative, start > end, missing) →
          **400**. Unsupported ``resolution`` value → **400**.
        - Works identically for running and paused sessions — the server's
          per-stream buffers + rollups are authoritative regardless of
          probe-loop state. Pause-freeze clamping (``end`` at ``paused_at``)
          is a CLIENT concern; this endpoint returns whatever the buffers
          hold.

        **Concurrency:** the handler is ``async def`` and performs the on-loop
        immutable capture (:meth:`TraceSession.history_samples` /
        :meth:`TraceSession.history_buckets`) synchronously before returning.
        No ``await`` intervenes between the deque/dict read and the immutable
        copy, so a concurrent probe write cannot mutate state mid-iteration
        (the same discipline as :meth:`export_snapshot` /
        :func:`capture_metrics`). Both payloads are bounded (raw:
        ``SAMPLE_CAP`` 10 000/stream; buckets: ``ROLLUP_CAP`` 1 440/stream),
        so on-loop serialization of the response is acceptable — no
        semaphore, no off-loop split needed.
        """
        # Param validation. All params are accepted as strings so semantic
        # errors return 400 (FastAPI's default 422 only covers type-coercion
        # failures, not the value-range / missing / ordering constraints the
        # spec requires).
        for name, val in (("hop", hop), ("start", start), ("end", end)):
            if val is None:
                raise HTTPException(
                    status_code=400, detail=f"missing required query param: {name}"
                )
        # The loop above raised on None; narrow for the type checker so the
        # parses below are known-good (``int``/``float`` accept str inputs).
        assert hop is not None and start is not None and end is not None
        try:
            hop_i = int(hop)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="hop must be an integer >= 0")
        if hop_i < 0:
            raise HTTPException(status_code=400, detail="hop must be >= 0")
        try:
            start_s = float(start)
            end_s = float(end)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400, detail="start and end must be epoch seconds"
            )
        if not (math.isfinite(start_s) and math.isfinite(end_s)):
            raise HTTPException(status_code=400, detail="start and end must be finite")
        if start_s < 0 or end_s < 0:
            raise HTTPException(status_code=400, detail="start and end must be >= 0")
        if start_s > end_s:
            raise HTTPException(status_code=400, detail="start must be <= end")
        # Resolution matrix (Slice D2): raw (D1 default), 1m (rollup), auto
        # (pick based on raw-retention fit). Any other value → 400.
        if resolution not in ("raw", "1m", "auto"):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"unsupported resolution: {resolution!r} (supported: raw, 1m, auto)"
                ),
            )
        session = manager.get(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="unknown session")
        # Resolve ``auto`` against raw retention. Raw retention in seconds is
        # SAMPLE_CAP * interval_s (the maximum wall-clock window the raw
        # deque could hold at the session's configured interval). When the
        # requested window fits, raw is preferred (full resolution); beyond
        # that the rollup tier serves the long range. The response carries
        # the actual resolution used so the client knows which buffer to
        # merge into.
        if resolution == "auto":
            raw_window_s = SAMPLE_CAP * session.interval_s
            resolution = "raw" if (end_s - start_s) <= raw_window_s else "1m"
        if resolution == "1m":
            # On-loop immutable capture of the rollup (no await between
            # read and copy).
            captured = session.history_buckets(hop_i, start_s, end_s)
            if captured is None:
                raise HTTPException(
                    status_code=404, detail=f"unknown hop index: {hop_i}"
                )
            earliest, buckets = captured
            return {
                "id": session.id,
                "hop": hop_i,
                "resolution": "1m",
                "earliest_t": earliest,
                "buckets": buckets,
            }
        # resolution == "raw" — Slice D1 behaviour unchanged.
        captured_raw = session.history_samples(hop_i, start_s, end_s)
        if captured_raw is None:
            raise HTTPException(status_code=404, detail=f"unknown hop index: {hop_i}")
        earliest, samples = captured_raw
        return {
            "id": session.id,
            "hop": hop_i,
            "resolution": "raw",
            "earliest_t": earliest,
            "samples": [{"t": t, "rtt_ms": r} for (t, r) in samples],
        }

    @app.get("/api/traces/{session_id}/export")
    async def export_trace(session_id: str, format: str = "csv") -> StreamingResponse:
        """Export the full server-held sample history as a flat table.

        ``GET /api/traces/{id}/export?format=csv`` returns ``text/csv`` with a
        ``Content-Disposition: attachment`` header so the browser downloads
        the file. Only ``csv`` is supported in this slice; any other value
        is a 400. Works for running and paused sessions alike — the server's
        per-hop buffers are authoritative regardless of probe-loop state.

        Two-phase design (load-bearing, see :meth:`export_snapshot`):

        1. **On-loop capture**: ``session.export_snapshot()`` iterates each
           live deque once into a list of immutable tuples. Fast, and atomic
           vs probe writes because nothing else runs on the loop during it.
        2. **Off-loop serialize**: ``asyncio.to_thread`` builds the CSV text
           from the immutable snapshot, so the event loop is free to service
           other requests during the CPU-bound serialization.

        A bounded semaphore (``EXPORT_CONCURRENCY_LIMIT``) wraps BOTH phases
        so at most two exports are in flight at once; requests that cannot
        acquire a slot immediately get HTTP 429 with a short Retry-After.
        """
        session = manager.get(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="unknown session")
        if format != "csv":
            raise HTTPException(
                status_code=400,
                detail=f"unsupported export format: {format!r} (supported: csv)",
            )
        # Capture all session metadata needed for the response before the
        # worker boundary.  The serializer must not be followed by a read of
        # mutable session state: a concurrent lifecycle/control operation (or
        # a test double) could otherwise make the filename describe a
        # different session state than the exported rows.
        filename = _export_filename(session)
        # Non-blocking acquire: peek at the private _value rather than
        # await'ing ``acquire()`` unconditionally. When _value > 0 the
        # subsequent acquire() completes synchronously (no await yield in
        # CPython's asyncio.Semaphore), so there is no race window between
        # the peek and the decrement on the same event loop turn; when
        # _value == 0 we 429 immediately rather than queueing behind the
        # holders (queueing would re-introduce the unbounded-stall threat
        # under sustained concurrent requests).
        if export_semaphore._value <= 0:
            raise HTTPException(
                status_code=429,
                detail=(
                    f"export busy: {EXPORT_CONCURRENCY_LIMIT} concurrent "
                    f"exports already in progress; retry shortly"
                ),
                headers={"Retry-After": EXPORT_RETRY_AFTER_S},
            )
        await export_semaphore.acquire()
        export_slot = _ExportSlot(export_semaphore)
        try:
            # Phase 1 — on-loop immutable snapshot.
            snapshot = session.export_snapshot()
            # Phase 2 — off-loop CPU-bound serialization.
            csv_text = await asyncio.to_thread(_samples_csv_from_snapshot, snapshot)
            response = _ExportStreamingResponse(
                _stream_export_body(csv_text, export_slot),
                media_type="text/csv",
                export_slot=export_slot,
                headers={
                    "Content-Disposition": f'attachment; filename="{filename}"',
                },
            )
        except BaseException:
            # If serialization or response construction fails, the streaming
            # body never takes ownership of the slot.
            export_slot.release()
            raise
        return response

    @app.get("/api/traces/{session_id}/events")
    async def trace_events(session_id: str) -> StreamingResponse:
        session = manager.get(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="unknown session")
        if sse_semaphore.locked():
            raise HTTPException(429, "event-stream limit reached", headers={"Retry-After": "2"})
        await sse_semaphore.acquire()
        slot = _ExportSlot(sse_semaphore)
        try:
            queue = session.subscribe()
        except SubscriberLimitReached as exc:
            slot.release()
            raise HTTPException(429, str(exc), headers={"Retry-After": "2"}) from exc
        return _SSEStreamingResponse(
            _sse_stream(session, queue),
            session=session, queue=queue, export_slot=slot,
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",  # disable proxy buffering
                "Connection": "keep-alive",
            },
        )

    return app


async def _sse_stream(session, queue=None) -> AsyncIterator[str]:
    """Yield a Server-Sent Events stream for one session.

    Wire format:

    - ``retry: 1000\\n\\n`` prelude so EventSource clients reconnect after 1 s
      if the stream drops.
    - ``event: ready`` block with empty JSON payload so clients can confirm
      subscription by listening for the ``ready`` event.
    - If route discovery has already completed when the client attaches, one
      ``event: route`` block immediately follows, replaying the current hop
      list so late subscribers do not miss the route.
    - One ``data:`` block per probe round:
      ``{"t": ..., "results": [{"hop": <index>, "rtt_ms": <float|null>}, ...]}``
      where ``hop == 0`` denotes the target (route pending/failed) and
      ``hop >= 1`` matches the indexes in the route event's hop list.
    - ``event: route`` block emitted live when discovery completes:
      ``{"hops": [{"index", "ip", "dns"}, ...]}``.
    - ``: keepalive`` comments when idle so proxies do not close the link.

    The per-client queue is unsubscribed on any exit path (client disconnect,
    server shutdown, or generator close).
    """
    from .session import RoundEvent, RouteEvent

    if queue is None:
        queue = session.subscribe()
    try:
        yield "retry: 1000\n\n"
        yield "event: ready\ndata: {}\n\n"
        # Replay route if discovery already completed before subscribe. This
        # gives late subscribers the hop list so they can interpret round
        # events that reference hop indexes >= 1.
        if getattr(session, "route_state", None) == "ready":
            yield _format_route_event(session.hops)
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=15.0)
            except TimeoutError:
                yield ": keepalive\n\n"
                continue
            if event is _SESSION_CLOSED:
                return
            if isinstance(event, RouteEvent):
                yield _format_route_event(event.hops)
                continue
            if isinstance(event, RoundEvent):
                payload = {
                    "t": event.t,
                    "results": [
                        {"hop": r.hop, "rtt_ms": r.rtt_ms} for r in event.results
                    ],
                }
                yield f"data: {json.dumps(payload)}\n\n"
                continue
            # Unknown event type — ignore defensively.
    finally:
        session.unsubscribe(queue)


async def _stream_export_body(csv_text: str, slot: _ExportSlot) -> AsyncIterator[str]:
    """Send one materialized export and release its slot after transmission.

    Keeping the slot until the response body finishes (including client
    disconnects) bounds retained response bodies as well as concurrent
    serialization work.  The handler owns the slot until this generator is
    returned; its ``finally`` takes ownership for the ASGI send phase.
    """
    try:
        yield csv_text
    finally:
        slot.release()


class _ExportSlot:
    """Idempotent ownership of one export semaphore slot."""

    def __init__(self, semaphore: asyncio.Semaphore) -> None:
        self._semaphore = semaphore
        self._released = False

    def release(self) -> None:
        if not self._released:
            self._released = True
            self._semaphore.release()


class _ExportStreamingResponse(StreamingResponse):
    """Close the export iterator even when ASGI send raises.

    Starlette's normal async-for cleanup does not guarantee ``aclose()`` when
    the downstream ``send`` callable raises.  The explicit close is therefore
    needed for client disconnects and transport errors; the idempotent slot
    makes this safe when the iterator's own ``finally`` already ran.
    """

    def __init__(self, *args, **kwargs) -> None:
        self._export_slot = kwargs.pop("export_slot", None)
        super().__init__(*args, **kwargs)

    async def __call__(self, scope, receive, send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            try:
                close = getattr(self.body_iterator, "aclose", None)
                if close is not None:
                    await close()
            finally:
                if self._export_slot is not None:
                    self._export_slot.release()


class _SSEStreamingResponse(_ExportStreamingResponse):
    """Release admission even if sending headers fails before iteration."""

    def __init__(self, *args, session, queue, **kwargs):
        self._session = session
        self._queue = queue
        super().__init__(*args, **kwargs)

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            self._session.unsubscribe(self._queue)


def _format_route_event(hops) -> str:
    """Serialize a list of Hop into an ``event: route`` SSE frame."""
    payload = {
        "hops": [
            {
                "index": hop.index,
                "ip": hop.ip,
                "dns": hop.dns,
            }
            for hop in hops
        ]
    }
    return f"event: route\ndata: {json.dumps(payload)}\n\n"


def _samples_csv(session) -> str:
    """Serialize a session's full sample history to a flat CSV table.

    Thin test/back-compat wrapper: captures the on-loop snapshot then
    serializes synchronously. The production path
    (:func:`export_trace`) splits the same two phases across the event
    loop and a worker thread so the snapshot capture stays on-loop
    (atomic vs probe writes) while the CPU-bound CSV build runs off-loop
    (does not stall the event loop).

    One header row followed by one row per sample, columns
    ``timestamp_utc,timestamp_epoch,hop_index,ip,dns,rtt_ms``. ``rtt_ms`` is
    empty for lost probes; ``ip``/``dns`` are empty for silent hops. The
    epoch column carries microsecond precision (fixed 6 decimals) so the
    text is deterministic and lexicographically sortable; the UTC column is
    ISO 8601 with a trailing ``Z``.
    """
    return _samples_csv_from_snapshot(session.export_snapshot())


def _samples_csv_from_snapshot(
    rows: Iterable[tuple[float, int, str | None, str | None, float | None]],
) -> str:
    """Serialize an immutable export snapshot to CSV text (off-loop safe).

    Takes the snapshot returned by :meth:`TraceSession.export_snapshot`
    directly — no session reference, no live deques — so it is safe to call
    inside ``asyncio.to_thread`` without holding the event loop and without
    racing concurrent probe writes.
    """
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(
        ["timestamp_utc", "timestamp_epoch", "hop_index", "ip", "dns", "rtt_ms"]
    )
    for t, hop_index, ip, dns, rtt_ms in rows:
        writer.writerow(
            [
                datetime.fromtimestamp(t, tz=UTC).isoformat().replace("+00:00", "Z"),
                format(t, ".6f"),
                hop_index,
                _safe_csv_text(ip),
                _safe_csv_text(dns),
                "" if rtt_ms is None else format(rtt_ms, ".3f"),
            ]
        )
    return out.getvalue()


# Spreadsheet formula-injection trigger characters. LibreOffice, Excel,
# Google Sheets, and Apple Numbers all treat a cell whose first substantive
# character is one of these as a formula. Leading whitespace and certain
# Unicode format/control characters are silently skipped by the formula
# sniffer in several of these apps, so the check below strips those first.
_FORMULA_TRIGGER_CHARS = frozenset("=+-@")


def _safe_csv_text(value: str | None) -> str:
    """Keep untrusted text cells inert when opened by spreadsheet applications.

    Two independent guarantees:

    1. **Record integrity** — any embedded C0 control character (tab, LF,
       CR, NUL, …) and DEL are replaced with a regular space so each cell
       stays single-line and printable. CSV itself tolerates embedded
       newlines only inside quoted fields, and naive consumers (and many
       log shippers) split on bare ``\\n`` regardless of quoting; a hostile
       PTR record containing an embedded newline would otherwise fragment
       one logical cell across multiple CSV records. A space (rather than a
       visible ``\\xNN`` escape) is chosen to keep records readable; this
       guard is about shape, not security — the formula guard below is the
       security boundary.

    2. **Formula-injection defense** — spreadsheet formula sniffers look at
       the first *substantive* character of a cell, skipping leading
       whitespace AND several Unicode control/format characters (BOM, ZWSP,
       LTR/RTL marks, …). A hostile PTR name like ``"\\t=WEBSERVICE(...)"``
       or ``"\\r@..."`` therefore bypasses a naive "first char is ``=``"
       check. Here we strip ANY leading Unicode whitespace/control/format
       character (``Cc``, ``Cf``, ``Co``, ``Cs`` categories plus
       ``str.isspace``), and if the first substantive character that
       remains is ``=``, ``+``, ``-``, or ``@``, an apostrophe is prefixed
       at the ABSOLUTE beginning of the (control-normalized) value. The
       apostrophe is the conventional spreadsheet text-force escape and
       remains part of the exported value. Leading characters are NOT
       silently stripped (only the embedded-control normalization above
       rewrites the value) so the data the operator sees matches what the
       server held, modulo the control-character flattening. A value that
       already starts with an apostrophe is returned unchanged so manually
       escaped cells are not double-prefixed.

    Applied to every untrusted-text cell (``ip``, ``dns``, and any future
    text column). Numeric/timestamp cells need nothing — they are formatted
    by the writer and cannot carry a formula trigger.
    """
    text = value if value is not None else ""
    # Guarantee 1: flatten embedded C0 controls + DEL to spaces. Done before
    # the formula check so a leading control char becomes a leading space,
    # which the formula-strip then handles uniformly.
    text = "".join(" " if ord(ch) < 0x20 or ord(ch) == 0x7F else ch for ch in text)
    # Already escaped — never double-prefix.
    if text.startswith("'"):
        return text
    # Guarantee 2: find the first substantive character, stripping any
    # leading Unicode whitespace / control / format character that a
    # spreadsheet formula sniffer would also skip past.
    for ch in text:
        if ch.isspace() or unicodedata.category(ch) in ("Cc", "Cf", "Co", "Cs"):
            continue
        if ch in _FORMULA_TRIGGER_CHARS:
            return "'" + text
        break
    return text


def _export_filename(session) -> str:
    """Build ``netplot-<sanitized-target>-<created-utc>.csv``.

    Anything outside ``[A-Za-z0-9.-]`` in the target collapses to ``_`` so
    the filename is portable and safe to drop straight into the
    ``Content-Disposition`` header (no quoting/escaping edge cases). The
    created-utc stamp is compact ISO (``YYYYmmddTHHMMSSZ``).
    """
    sanitized = re.sub(r"[^A-Za-z0-9.\-]", "_", session.target or "trace") or "trace"
    created = datetime.fromtimestamp(session.created_at, tz=UTC).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    return f"netplot-{sanitized}-{created}.csv"


app = create_app()
