# netplot Product & Architecture Spec

netplot is a web-based clone of PingPlotter (pingplotter.com): continuous
latency and packet-loss measurement of every hop between the user and a
target, with a live route table and per-hop timeline graphs.

Source: PingPlotter manuals and product docs researched 2026-07-28
(pingplotter.com/manual, pingplotter.com/legacy-manual, pingman.com KB).

## Product model (what we are recreating)

PingPlotter's probe model: TTL-limited probes map the route (hop 1, hop 2,
...), and every hop is probed continuously at the trace interval. Each
polling round is a "sample set". Intermediate hops answer ICMP Time
Exceeded and may drop/deprioritize those responses, so per-hop loss does
NOT imply end-to-end loss; the final target's stats are the trusted result.

### Core workflow (P0 — must exist)

1. Enter a target (DNS name or IP), set interval, Start.
2. Route is discovered; hop rows appear as hops respond. Silent hops stay
   visible as `*` / unknown — the route is never collapsed.
3. Live per-hop table: **Hop, Count, IP, DNS, Avg, Min, Cur, PL%**.
   - Count = packets sent in the focus period.
   - Avg excludes lost samples. Cur = most recent RTT, `ERR` if lost.
   - PL% = lost / sent for that hop in the focus period.
4. Per-hop latency visualization: a PingPlotter-faithful graph column (see
   the Frontend section for the full anatomy).
5. Target timeline graph: black RTT line over time, red vertical marks on
   lost samples, green/yellow/red background bands (defaults:
   green ≤ 100 ms, yellow 101–200 ms, red > 200 ms). Default range: last
   10 minutes.
6. Continuous sample history with range selector.
7. Focus window: selecting a historical region recomputes table stats for
   that period.
8. Double-click a hop row to open/hide that hop's timeline.

### P1 (desirable, later)

Per-trace pause/reset, DNS resolution, copy IP/DNS, timeline comments,
image snapshot export. CSV export shipped (see the API section's
``GET /api/traces/{id}/export``). *(Multiple simultaneous traces/tabs —
formerly listed here — shipped in the Slice 7 / multi-trace tabs work
described below. Persistent background collections — formerly listed in
P2 — shipped in the Slice B work described below.)*

### P2 (documented out of scope)

`.pp2` compatibility, alerts, UDP/TCP packet modes, remote agents/cloud,
jitter/MOS, route-change history, summaries.

### Persistent (background) collections (Slice B)

A trace can be marked as a **persistent (background) collection**.
Persistent sessions are saved to disk and auto-restored when the server
process starts, surviving container restarts/redeploys. The browser
closing already keeps a session running; this makes that intentional,
named, and restart-safe.

- **What persists:** session DEFINITIONS only — ``id``, ``target``,
  ``interval_s``, ``name``, ``persistent`` flag, desired state
  (``running``/``paused``), and ``created_at``. NOT sample history:
  samples restart empty on boot (long-term history is the next slice's
  Prometheus job; per-round disk writes would be wasteful here).
- **State file:** one JSON file at ``$NETPLOT_DATA_DIR/sessions.json``
  (env var, default ``<cwd>/data``). Atomic writes (tmp file +
  ``os.replace``) on lifecycle events only: create (if persistent),
  delete, pause, resume, persist-toggle, rename. Non-persistent sessions
  are simply absent from the file; when the persistent set becomes empty
  the file is removed so a boot with no persistent sessions reads no
  file.
- **Schema:** versioned (``version: 1``). An unknown version is ignored
  with a warning so a forward-incompatible file never crashes boot; the
  server starts empty instead. Missing file → normal fresh start.
  Present-but-unusable file (corrupt JSON, unknown schema version,
  oversized, wrong shape, unknown envelope or entry fields, OR any session entry that fails field
  validation) → **degraded mode**: the original bytes are quarantined
  (renamed to ``sessions.json.quarantine-<utc>``), the valid subset is
  restored (capped by successful-restore count, not file offset, so
  invalid entries among the first N cannot exclude valid later ones),
  every dropped entry is logged with a reason, and the store enters
  **read-only mode**: subsequent persistence writes (create, delete,
  pause, resume, persist) return **HTTP 503** with a clear detail until
  the operator resolves the quarantine (review + remove the quarantine
  file, then restart — the re-read clears degraded when the file is gone
  or clean). ``/healthz`` surfaces ``persistence: "degraded"`` so
  monitoring can catch it. Boot never crashes on any file condition.
  The oversized check runs on the file's size BEFORE any byte is read
  (and the read itself is capped at one byte past the limit), and
  quarantining moves the original bytes (hard link, streaming-copy
  fallback) without buffering them, so boot-time memory stays bounded
  no matter how large the on-disk file is.
- **Restore on startup:** the ASGI lifespan reads the state file,
  recreates persistent sessions in file (insertion) order up to
  ``MAX_SESSIONS`` (the cap counts SUCCESSFUL registrations, not file
  offsets; extras skipped with a logged warning), and starts probing
  for sessions whose desired state was ``running``. Paused sessions
  restore as paused (``paused_at`` = restore time — the original pause
  epoch is intentionally not persisted so a long-down period does not
  anchor the freeze at a stale wall-clock). Restore does not block app
  serving: it recreates session objects synchronously and kicks off
  probe/discovery tasks (the same ``manager.start`` path a fresh create
  uses), which then proceed asynchronously.
- **Cancellation safety:** persistent create+start is shielded so a
  client cancellation between the durable write and the probe-task
  launch cannot leave a durable ``running`` definition with no probe
  task. On cancellation the created session is removed (compensation).
- **Persist endpoint semantics:** ``POST /api/traces/{id}/persist`` has
  a **no-op fast path** when neither the persistent flag nor the
  (sanitized) name changes (returns the current summary without taking
  the lock or touching disk); returns **HTTP 409** when another
  mutation for that session is already pending (instead of queueing a
  blocked worker); returns **HTTP 404** if the session was removed
  between resolution and lock acquisition (the stale-object guard in
  the manager ensures lifecycle callers never mutate a deregistered
  session).
- **Reaper exemption:** persistent sessions are intentional background
  collections. Any future orphan-reaper must
  exempt them: a persistent session with no attached browser is the
  designed state, not an orphan.

#### State-file format (schema v1)

```json
{
  "version": 1,
  "sessions": [
    {
      "id": "<uuid hex>",
      "target": "<hostname or IP>",
      "interval_s": 2.5,
      "name": "<user label, sanitized; null/empty falls back to target>",
      "persistent": true,
      "desired_state": "running",
      "created_at": 1785437083.44
    }
  ]
}
```

``name`` is sanitized to a max of 60 chars with every Unicode
control / format / surrogate / private-use code point (general
categories ``Cc``, ``Cf``, ``Cs``, ``Co`` — filtered by
``unicodedata.category()``, not a hand-maintained range list) stripped
and whitespace collapsed. This is exhaustive: bidi overrides (U+202E,
U+2066–U+2069), BOM (U+FEFF), ZWSP (U+200B), soft hyphen, tags,
variation selectors, BMP + supplementary private-use, and unpaired
surrogates (which would also break the UTF-8 write → repeatable
persistence failure) are all removed. The client mirrors this policy
via Unicode property escapes (``\p{Cc}\p{Cf}\p{Cs}\p{Co}``) and treats
the server-normalized snapshot value as truth. Field validation on
read drops any entry missing required fields or carrying wrong-typed
values with a logged reason rather than crashing restore.

### Defaults adopted

- Reply timeout: 3 s (PingPlotter default).
- Probe packet size: 56 bytes (PingPlotter default) — only relevant if raw
  ICMP is adopted; subprocess ping uses the OS default.
- Interval: user-set, floor 1 s. Research found no authoritative shipped
  default; UI default is 2.5 s (PingPlotter's familiar feel), min 1.
- Focus window: default = last 10 minutes of samples ("Auto" semantics).
  `?focus=<seconds>` on the snapshot API; `focus=0` = all samples.
- Timeline + graph-column thresholds: green ≤ 100 ms, yellow 101–200 ms,
  red > 200 ms. The Cloud reference screenshot is authoritative for this
  clone.
- Timeline range selector: 1 m / 5 m / 10 m / 1 h / 3 h / 12 h / 24 h
  (Slice D1 added 1 h / 3 h; Slice D2 added 12 h / 24 h served from the
  server-side rollup tier), default 10 m. The client sample buffer retains
  ``MAX_RANGE_S + 5`` seconds of RAW samples for the ≤ 3 h ranges (10 805 s,
  unchanged from D1); the 12 h / 24 h ranges are served from a separate
  server-side fixed-wall-clock 1-minute rollup tier (Slice D2) and stored
  in a SEPARATE client bucket buffer (different resolution, never mixed
  with raw). Each panel backfills from
  ``GET /api/traces/{id}/history`` on restore/reload/reconnect/range-widen
  so the full retained window is visible immediately.

## Architecture decisions

### Probing: system binaries, not raw sockets

Raw ICMP requires root/cap_net_raw and per-OS work. The engine shells out:

- Route discovery: `traceroute -n -q 1 -w 2 -m 30 <target>` (macOS and
  Linux flags compatible; parse `N  <ip>  <ms>` lines, `*` = silent hop).
- Per-hop latency: `ping -c 1` per hop per round, run concurrently with
  asyncio subprocesses; parse `time=<ms>`. Timeout => loss sample.

The engine sits behind a narrow async interface
(`probe_once(host, timeout) -> float | None`, `discover_route(target) ->
list[hop]`) so a raw-socket engine can replace it later without touching
sessions or the API.

### Sessions

In-memory session manager (no DB). One session = one target trace.
Per-hop sample history is a bounded deque (ring buffer, cap 10_000
samples/hop). Stats are computed on read over the focus window; nothing
derived is stored.

The manager caps the registry at **``MAX_SESSIONS = 8``** concurrent
sessions (PingPlotter-style multi-trace tabs bound the registry so a
single client cannot squat unlimited concurrent probe loops).
``SessionManager.create`` raises ``SessionLimitReached`` once the cap is
reached; ``POST /api/traces`` translates that into **HTTP 409** with a
``detail`` of the form ``session cap reached (N/N): close an existing
trace before starting a new one`` so the dashboard can surface it.
Closing a tab (DELETE) frees a slot for the next create.

### Concurrency

asyncio throughout: one scheduler task per session firing probe rounds at
the configured interval; within a round all hop probes run concurrently.

### API (REST + SSE)

- `POST /api/traces` `{target, interval_s, name?, persistent?}` →
  `{id, target, interval_s, state, name, persistent}`; starts. Returns
  **HTTP 409** with a ``detail`` message when the ``MAX_SESSIONS = 8``
  concurrent-session cap is reached. ``name`` (optional) is the
  user-visible label, sanitized to a max of 60 chars with control
  characters stripped (empty/null falls back to the target on the
  client). ``persistent`` (optional, default false) marks the session
  as a background collection whose definition is written to
  ``sessions.json`` and auto-restored on the next server boot — see the
  "Persistent (background) collections" section above.
- `POST /api/traces/{id}/pause`, `POST /api/traces/{id}/resume`
  (pause response/summary carries ``paused_at``: epoch seconds recorded
  after probe/discovery tasks are cancelled and awaited; cleared on
  resume/start). For persistent sessions, pause/resume also rewrite the
  state file so a restart resumes the session in the same desired state.
  Pausing also interrupts pending forward DNS; cancellation is never treated
  as an ordinary lookup failure. Resuming restarts discovery if pause
  interrupted it, while ordinary DNS failures remain best-effort.
  If the session was removed by a concurrent delete between resolution
  and lock acquisition, returns **HTTP 404** (stale-object guard). If
  the store is degraded, returns **HTTP 503**.
- `POST /api/traces/{id}/persist` `{persistent: bool, name?: string}`
  — toggles the background-collection flag and/or renames the session
  in the same call; returns the updated summary (carrying ``name`` and
  ``persistent``). ``persistent`` is required; ``name`` is optional and,
  when present, renames. Toggling persistence on a running session does
  NOT disturb probing — the endpoint only writes the state file.
  **No-op fast path:** when neither the flag nor the (sanitized) name
  changes, returns the current summary without taking the lock or
  touching disk. **HTTP 409** when another mutation for this session is
  already pending (instead of queueing a blocked worker thread).
  **HTTP 404** if the session was removed concurrently. **HTTP 503** if
  the store is degraded.
- `DELETE /api/traces/{id}` — **HTTP 404** if the session was removed
  concurrently; **HTTP 503** if the store is degraded (for persistent
  sessions).
- `GET /healthz` → ``{status: "ok"}``; carries ``persistence:
  "degraded"`` when the store is in degraded/read-only mode (corrupt /
  oversized / unknown-schema state file, or any session entry dropped
  during validation).
- `GET /api/traces` → list of sessions `{id, target, interval_s, state,
  created_at, sample_count, paused_at, name, persistent}`.
- `GET /api/traces/{id}/snapshot?focus=<seconds>&hop=<index>` →
  `{id, target, target_ip, target_dns, target_internal, state, route_state,
  focus_s, paused_at, name, persistent, hops: [{index, ip, dns, internal,
  icmp_non_responsive, stats: {…}}], stats: {…}, samples: […]}`.
  Top-level `stats`/`samples` describe the
  target (= final hop when route is ready and target is reached);
  `hops` is the discovered route. `hop=<index>` returns a single hop's
  `stats` + up to 100 recent samples instead (404 for unknown index).
  The ``name`` and ``persistent`` fields ride the snapshot so a second
  browser window converges on the latest label/flag on the next
  snapshot fetch (same machinery as pause-state convergence).
  ``internal`` / ``target_internal`` are SERVER-side internal-range
  classifications (stdlib ``ipaddress`` over the configured CIDR list —
  see the "Settings" section); ``icmp_non_responsive`` is the trace-local
  neutral classification (see the "ICMP non-responsive presentation"
  section).
  Full and hop-scoped snapshots capture live sample buffers synchronously
  on the probe event loop, without yielding, so probe writes cannot interleave
  with deque iteration.
- `GET /api/traces/{id}/export?format=csv` → 200 ``text/csv`` with
  ``Content-Disposition: attachment; filename="netplot-<sanitized-target>-<created-utc>.csv"``.
  One flat table, header row
  ``timestamp_utc,timestamp_epoch,hop_index,ip,dns,rtt_ms``; ``rtt_ms`` is
  empty for lost probes; ``ip``/``dns`` are empty for silent hops. Commas and
  quotes are CSV-escaped, and formula-leading text in ``ip``/``dns`` is
  prefixed with an apostrophe to prevent spreadsheet formula execution (the
  check strips ANY leading Unicode whitespace/control/format character — BOM,
  ZWSP, tab, CR/LF — before deciding a cell is a formula, so a hostile PTR
  name like ``<TAB>=WEBSERVICE(...)`` cannot bypass it; embedded C0 controls
  are flattened to spaces so records stay single-line). Rows are ordered by
  ``(timestamp, hop_index)``. ``hop_index`` mirrors the SSE contract:
  discovered route hops carry their own index (>= 1) and the target stream
  carries ``hop_index == 0`` only when the route does not reach the target
  (pending / failed / silent-tail); when the final hop IS the target the two
  deques are shared, so hop 0 is not emitted (no duplication). The server's
  per-hop buffers are authoritative, so export works identically for running
  and paused sessions. ``format`` defaults to ``csv`` and is the only
  supported value; any other value is **HTTP 400**; unknown session is
  **HTTP 404**.

  **Concurrency + cost:** serialization runs in two phases — an immutable
  row snapshot is captured ON the event loop (fast, atomic vs probe writes),
  then the CSV text is built OFF the event loop via ``asyncio.to_thread`` so
  a max-size export (~310k rows: 30 hops × 10k + a separate 10k target
  stream) does not stall other requests for the ~0.5 s the build takes. A
  per-process bounded semaphore (limit 2) is held for the response's whole
  lifetime — capture, serialization, and every ASGI body send up to and
  including the terminal zero-length chunk — so a slow or stalled download
  cannot park a finished body while further exports are admitted; a request
  that finds both slots held gets **HTTP 429** with ``Retry-After: 2``
  rather than queueing. Each in-flight transfer runs under a generous total
  transfer deadline (``EXPORT_TRANSFER_DEADLINE_S`` = 120 s, covering the
  status line, headers, and all body chunks): a client (or fronting peer)
  that stops reading has its transfer aborted mid-body at the deadline —
  the client sees a truncated response, never a completed CSV — and the
  slot is returned on every exit path (completion, error, disconnect,
  cancellation, deadline). The body is sent in bounded chunks (256 KiB,
  transparently — the concatenated bytes are exactly the CSV build) so one
  export does not become a single multi-MiB transport write. Retained
  response-body memory is thereby bounded to 2 in-flight bodies, each held
  for at most the transfer deadline.

  **Exposure:** export returns the FULL server-retained history per stream
  (up to ``SAMPLE_CAP`` = 10 000 samples, ≈ hours at the default 2.5 s
  interval), not just the snapshot window. The endpoint inherits the
  deployment's no-authentication/no-ownership posture — any party that can
  reach the dashboard can download every retained sample for any session
  id, including ones it did not create. Treat the export endpoint
  (and the rest of the API) as reachable only on a trusted LAN.
- `GET /api/traces/{id}/history?hop=<index>&start=<epoch_s>&end=<epoch_s>
  &resolution=<raw|1m|auto>` → 200 ``{id, hop, resolution, earliest_t, …}``.
  Sample/bucket history for one stream, clamped to ``[start, end]``. Slices
  D1 (raw) and D2 (1m / auto).

  - ``hop=0`` is the target stream (mirrors SSE/export numbering); ``hop>=1``
    must match a discovered route hop index. Unknown/out-of-range hop →
    **HTTP 404**; unknown session → **HTTP 404**.
  - ``resolution=raw`` (default, D1): ``{..., resolution: "raw", earliest_t:
    <epoch seconds|null>, samples: [{t, rtt_ms}]}``. Raw retained sample
    history, ascending by ``t``, clamped to ``[start, end]`` and to
    server retention (``SAMPLE_CAP`` = 10 000/stream). ``rtt_ms`` is
    ``null`` for lost probes. ``earliest_t`` is the timestamp of the
    earliest retained sample for that stream (null when empty) so clients
    can distinguish "no data" from "data older than retention" without a
    second round-trip.
  - ``resolution=1m`` (D2): ``{..., resolution: "1m", earliest_t:
    <bucket-start epoch|null>, buckets: [{t, avg_ms, min_ms, max_ms,
    count, loss_count}]}`` — per-minute rollup buckets ascending by ``t``,
    clamped to ``[start, end]`` and to rollup retention (``ROLLUP_CAP`` =
    1 440 buckets = 24 h/stream). ``t`` is the bucket-start epoch
    (floor(t/60)*60). A full-loss bucket (``count > 0`` and all samples
    lost) carries ``avg_ms``/``min_ms``/``max_ms`` as ``null``. EMPTY
    buckets (no samples — paused gaps, pre-start) are OMITTED, so the
    response is naturally sparse. ``earliest_t`` is the oldest retained
    bucket's ``t`` (null when empty).
  - ``resolution=auto`` (D2): ``raw`` when the requested ``[start, end]``
    lies fully within raw retention (``end - start <= SAMPLE_CAP *
    interval_s``), else ``1m``. The response carries the ACTUAL resolution
    used so the client knows which buffer to merge into.
  - Malformed params (non-numeric, negative, ``start > end``, missing
    ``start``/``end``/``hop``) → **HTTP 400**. Unsupported ``resolution``
    value → **HTTP 400``; ``resolution`` defaults to ``"raw"`` so D1
    clients are unaffected.
  - Works identically for running and paused sessions — the server's
    per-stream raw buffers AND rollups are authoritative regardless of
    probe-loop state. Pause-freeze clamping (``end`` at ``paused_at``) is
    a CLIENT concern; the endpoint returns whatever the buffers hold.

  **Server rollup tier (Slice D2):** per sample stream (target stream +
  each route hop) the server maintains an incremental rollup of
  fixed-wall-clock 60-second buckets. Bucket key = ``floor(t/60)*60``.
  Each bucket aggregates ``count``, ``loss_count``, and ``avg``/``min``/
  ``max`` RTT over successful samples (incremental, overflow-safe — the
  same divide-before-subtract discipline as the metrics window). Cap
  ``ROLLUP_CAP = 1440`` buckets per stream (24 h); on overflow the
  smallest-``t`` bucket is evicted so the cap newest survive (and
  ``earliest_t`` reflects eviction). Buckets are updated in the SAME
  on-loop probe write path as the raw deques — no separate aggregation
  pass — so a concurrent history read cannot observe a half-updated
  state. Bounded independent of probe interval: a 1 s interval over 24 h
  would otherwise admit ~86 400 raw samples/stream (× 30 hops × 8
  sessions is GB-scale); the rollup caps at 1 440 buckets/stream
  regardless. The rollup does NOT feed ``/metrics`` or ``/export`` (those
  read the raw deques only). Both ``ROLLUP_BUCKET_S`` and ``ROLLUP_CAP``
  are named module constants; they are NOT runtime API params. They are
  overridable per-instance via ``StreamRollup(bucket_s=…, cap=…)`` for
  unit tests, and via the ``NETPLOT_ROLLUP_BUCKET_S`` /
  ``NETPLOT_ROLLUP_CAP`` env vars for live end-to-end evidence with a
  short bucket size (e.g. 5 s) so a 12 h range fills in seconds rather
  than minutes. Production deployments leave both unset.

  **Concurrency:** the handler is ``async def`` and performs the on-loop
  immutable capture (``TraceSession.history_samples`` /
  ``TraceSession.history_buckets``) synchronously before returning. No
  ``await`` intervenes between the deque/dict read and the immutable copy,
  so a concurrent probe write cannot mutate state mid-iteration (the same
  discipline as ``export_snapshot`` / ``capture_metrics``). Both payloads
  are bounded (raw: ``SAMPLE_CAP`` 10 000/stream; buckets: ``ROLLUP_CAP``
  1 440/stream), so on-loop serialization of the response is acceptable —
  no semaphore, no off-loop split needed.

  **Exposure:** same no-auth/no-ownership posture as export.
- `GET /api/settings/internal` / `PUT /api/settings/internal {cidrs: [...]}`
  — the internal IP ranges for DNS masking (default RFC1918 + the full
  IPv6 unique-local block, RFC4193 ``fc00::/7``:
  ``10.0.0.0/8``, ``172.16.0.0/12``, ``192.168.0.0/16``, ``fc00::/7``; any
  CIDRs editable). PUT validates via ``ipaddress.ip_network``
  (``strict=False`` normalizes host bits), dedupes, and caps at 64 entries;
  invalid input → **HTTP 422** with the offending value in ``detail``.
  Classification happens server-side (snapshot annotations) so the client
  needs no CIDR parser. Persisted to ``<data_dir>/settings.json``.
- `GET /api/settings/influx` / `PUT /api/settings/influx {url, database,
  interface, host_tag}` — the optional InfluxDB 1.x throughput-overlay
  connection. Generic, settings-only configuration with NO preset or
  hardcoded values: ``url``, ``database``, and ``interface`` are all
  blank by default, and a non-empty ``url`` requires explicit non-empty
  ``database`` AND ``interface`` (nothing is inferred). Optional
  ``host_tag`` (needed when the measurement carries multiple hosts).
  URL validation: http(s) only, host required, userinfo, query strings,
  and fragments rejected (a retained query could alter the ``/query``
  assembly); identifiers are charset-restricted
  (``[A-Za-z0-9._:@-]``) and escaped again at query build time. No
  credentials are created, stored, or sent — existing no-auth sources
  only in this bounded first version. Invalid input → **HTTP 422**.
- `PUT /api/settings {cidrs: [...], influx: {url, database, interface,
  host_tag}}` — combined transactional save of BOTH sections (what the
  settings dialog's Save button sends). The full candidate is validated
  first (any invalid section → **HTTP 422** with nothing changed),
  persisted to ``settings.json``, and only then published to runtime
  state + the internal-IP classifier — so there is no partial success
  (one section applied, the other rejected) and a failed persist leaves
  the old runtime settings and the file intact. Writers (combined and
  individual PUTs alike) are serialized by one async lock; concurrent
  saves cannot interleave sections. Returns ``{cidrs, influx}``.
- `GET /api/throughput?start=<epoch_s>&end=<epoch_s>` → ``{start, end,
  step_s, points: [{t, in_mbps, out_mbps}]}``. Backend-side InfluxDB query
  over the ``net`` measurement's ``bytes_recv`` (inbound/download) and
  ``bytes_sent`` (outbound/upload) cumulative counters using
  ``non_negative_derivative(last(field), 1s)`` — ``last()`` reads the
  counter's end-of-bucket value (the proven telegraf/Grafana form) and
  the fixed ``1s`` unit makes every bucket's value bytes/SECOND
  regardless of the aggregation step (a step-sized unit would inflate
  readings 10/60/300×) — plus ``GROUP BY time(step), "interface",
  "host"`` ``fill(none)``: counter-reset safe and series-preserving
  (``GROUP BY time()`` alone would silently merge multiple hosts'
  counters; with tag grouping, an ambiguous host-less config returns
  multiple series — a **502** telling the operator to set a ``host_tag``
  — instead of merged garbage). Missing buckets stay null (never zero).
  Bucket step ladder: 5 s (native source cadence) for spans ≤ 30 m,
  60 s ≤ 6 h, else 300 s (24 h → 288 points). **Transport contract:**
  async HTTPX (``httpx.AsyncClient``, per request) with a **5 s total
  deadline** (``INFLUX_TIMEOUT_S``) covering connect plus streaming the
  whole body — cancelled cleanly, closing the upstream connection;
  **no redirects** (``follow_redirects=False``); **no compression**
  (``Accept-Encoding: identity`` and any non-identity ``Content-Encoding``
  refused before the cap is enforced, so the **1 MiB cap applies to the
  raw body**, ``MAX_RESPONSE_BYTES``); **no implicit proxies**
  (``trust_env=False`` — environment proxy variables are ignored). HTTPS
  sources verify with ``ssl.create_default_context()``, honoring
  ``SSL_CERT_FILE`` / ``SSL_CERT_DIR`` and platform defaults.
  Malformed payloads (wrong shapes,
  non-finite timestamps, negative bandwidth) surface as **502**-mapped
  errors, never uncaught exceptions. A destination URL that survives the
  settings grammar but that the HTTP client itself rejects at request-build
  time (e.g. an IDNA-unencodable host such as the reserved ``☃.example``)
  is likewise mapped to a **502** ``InfluxError`` ("invalid destination
  URL") — never an unhandled **500**; rejecting such hosts at save time is
  deliberately out of scope (no URL-grammar tightening or settings
  migration in this bounded version), so the controlled **502** is the contract for
  accepted-but-unusable destinations. **400** on missing/non-numeric/
  negative params, ``start > end``, span > 24 h, or when unconfigured;
  **502** when unreachable/erroring/ambiguous. **Admission:** at most
  **4 active throughput queries per process**
  (``THROUGHPUT_CONCURRENCY_LIMIT``); a request that finds all slots held
  gets **429** with ``Retry-After: 2`` instead of queueing, and a request
  cancelled mid-flight (client disconnect included) releases its slot.
- `GET /api/traces/{id}/events` → Server-Sent Events:
  - `retry: 1000` prelude, `event: ready` block.
  - `event: route` block once when discovery completes:
    `{"hops": [{"index", "ip", "dns"}, …]}`.
  - One `data:` block per probe round:
    `{"t": …, "results": [{"hop": <index>, "rtt_ms": <float|null>}, …]}`
    where `hop == 0` denotes the target (route pending/failed) and
    `hop >= 1` matches the route event's hop indexes (final hop = target).
  - `: keepalive` comments when idle.

  **Admission + cleanup:** at most **8 subscribers per session**
  (``MAX_SUBSCRIBERS_PER_SESSION``) and **32 SSE connections per
  process** (``SSE_CONCURRENCY_LIMIT``). Beyond either bound the
  endpoint returns **429** with ``Retry-After: 2`` rather than queueing
  (both checks happen before the stream starts, so no 200 is sent for a
  rejected subscriber). A client disconnect or task cancellation
  releases both the process slot and the session subscription — streams
  abandoned mid-response cannot wedge admission. SSE responses
  deliberately carry **no transfer deadline** (unlike CSV export, whose
  120 s budget exists only to bound finite downloads): the streams are
  intentionally long-lived and are kept alive by the 15 s keepalive
  comments above.

  Chosen over WebSocket: one-way push, simpler, native `EventSource` in the
  browser.

### Metrics (Prometheus ``GET /metrics``)

netplot exposes a Prometheus exposition endpoint so a Prometheus
scraper (and downstream Grafana dashboards + latency/loss threshold alerts)
can observe every trace. The endpoint is pure formatter + bounded capture —
no framework metric registry, no added dependency.

- **Endpoint:** ``GET /metrics`` → ``200`` ``text/plain; version=0.0.4``.
  ``# HELP`` / ``# TYPE`` lines precede each family. The formatter
  (:func:`netplot.metrics.render_metrics`) is a pure function over captured
  data so it is unit-testable without HTTP; the capture
  (:func:`netplot.metrics.capture_metrics`) reads live deques on the event
  loop (no ``await`` between the read and the immutable copy — the same
  on-loop discipline as ``export_snapshot``).

- **Window:** avg and loss are computed over the last
  ``METRICS_WINDOW_SAMPLES = 60`` samples per stream, by COUNT not
  wall-clock. Named, documented, and intentionally NOT configurable in this
  slice. The capture uses ``reversed(deque)`` + ``islice`` so each scrape is
  O(window) regardless of how much history (up to ``SAMPLE_CAP`` = 10 000) a
  stream holds.

  The average is a divide-before-subtract incremental mean, not a naive sum:
  it prevents finite RTTs near opposite float limits from overflowing the
  accumulator. Like ordinary floating-point averaging it has approximately
  ``O(N * epsilon)`` rounding error for well-scaled inputs; this is negligible
  at millisecond RTT scale, while preserving finite Prometheus output for the
  numeric edge cases accepted by the capture contract.

- **Series contract** (all gauges):

  ============================================================  ====================================================
  Metric                                                         Semantics
  ============================================================  ====================================================
  ``netplot_session_info``                                       ``{session_id, target, name, persistent, state}`` = 1.
                                                                 One series per registered session. ``state`` is
                                                                 ``running`` or ``paused`` so alert rules can gate on
                                                                 it (see Paused semantics below). ``name`` is the
                                                                 sanitized user label or empty string when unset;
                                                                 ``persistent`` is ``"true"``/``"false"``.
  ``netplot_target_rtt_current_ms``                              ``{session_id, target}`` — most recent target probe
                                                                 RTT in ms. **Absent when the last probe was a loss**
                                                                 (no stale values: a loss round means no current
                                                                 value, not the last-known-good).
  ``netplot_target_rtt_avg_ms``                                  ``{session_id, target}`` — mean RTT over the last 60
                                                                 target samples (losses excluded). Absent when no
                                                                 successful samples are in the window.
  ``netplot_target_loss_pct``                                    ``{session_id, target}`` — loss percentage over the
                                                                 last 60 target samples.
  ``netplot_target_up``                                          ``{session_id, target}`` — 1 if the most recent
                                                                 target probe succeeded, else 0.
  ``netplot_hop_rtt_current_ms``                                 ``{session_id, target, hop}`` — most recent RTT per
                                                                 route hop. ``hop`` = route index (final hop = target,
                                                                 matching the SSE numbering contract). Same
                                                                 no-stale-value rule as the target current gauge.
  ``netplot_hop_loss_pct``                                       ``{session_id, target, hop}`` — per-hop loss
                                                                 percentage over the last 60 samples.
  ============================================================  ====================================================

- **Cardinality** is bounded: ``MAX_SESSIONS`` (8) × ≤ 30 hops × the 60-
  sample window. The whole scrape is a small single pass and does not stall
  the event loop meaningfully.

- **Shared-deque note:** when the final discovered hop IS the target
  (``hops[-1].samples is session.samples``), the target series and the
  final-hop series carry identical values (same deque) under different
  labels — ``netplot_target_*`` keys on ``{session_id, target}`` for
  end-to-end alerting; ``netplot_hop_*`` keys on ``{session_id, target,
  hop}`` for per-hop visibility. Both are emitted by design.

- **No metrics for streams with zero samples yet:** a session with zero
  target samples still emits ``netplot_session_info`` (so its existence and
  state are observable) but its ``netplot_target_*`` series are absent
  until the first probe lands; a discovered hop with zero samples is
  omitted from the ``netplot_hop_*`` families.

- **Paused sessions** keep emitting last-known values — their data is
  truthfully labeled via ``netplot_session_info{state="paused"}``. Alert
  rules SHOULD join against that family (for example,
  ``netplot_target_loss_pct > 50 unless on(session_id)
  (netplot_session_info{state="paused"} == 1)``) so a deliberately paused
  session does not fire false alerts. This matches the dashboard's freeze
  semantics: the server's per-stream buffers are authoritative regardless of
  probe-loop state, and a paused session's last window is the last window it
  probed.

- **Label escaping:** label values escape ``\``, ``"``, and ``\n`` per the
  Prometheus exposition spec (backslash first). Targets are validated to a
  safe charset at create time, but the sanitizer mindset from the CSV work
  applies — a hostile name (which can contain backslash, since the name
  sanitizer strips only ``Cc``/``Cf``/``Cs``/``Co`` categories) must not
  break the exposition format.

- **Exposure:** the endpoint inherits the deployment's no-auth/no-ownership
  posture — any party that can reach the dashboard can read
  every session's metrics. Treat it as reachable only on a trusted LAN.

### Frontend

Vanilla HTML/CSS/JS + `<canvas>` for the timeline (no framework, matching
the scaffold's dependency-light stance). Static assets served from the
package via ``GET /static/{app.js,style.css}``; ``GET /`` returns
``src/netplot/static/index.html``. The hop table (Slice 4) renders rows as
plain DOM and updates them per SSE round; the timeline (Slice 5) is the
only piece that uses `<canvas>`.

#### Multi-trace tabs (Slice 7 — PingPlotter-style)

The dashboard supports multiple simultaneous traces — one per tab. A tab
bar sits between the toolbar and the main content; each tab is a single
live trace, labeled by target, with a close × per tab and a "+" button to
open a fresh (unsaved) tab. The active tab is highlighted.

- **Per-tab state via a factory.** Each tab is constructed by
  ``createTab(sessionInfo)`` and owns its own ``state`` object (sessionId,
  EventSource, sample buffers, hop list / route state, focus window +
  drag-focus, timeline panels + their per-panel range dropdowns, pause /
  resume / reset bindings, focus-window selection, target / interval
  values). The toolbar elements are SHARED across tabs and always reflect
  the active tab; on tab switch, the outgoing tab's typed target/interval
  are captured into its state and the incoming tab's toolbar values are
  restored. PURE stays pure (no DOM); tab-state logic that is pure
  (``PURE.tabLabel``, ``PURE.nextActiveAfterClose``) lives in PURE with
  node unit tests.
- **Background tabs keep collecting.** A background tab's EventSource
  stays attached and its sample buffers keep filling, but every
  DOM-writing closure self-guards with ``tabManager.isActive(self)`` so
  background tabs never touch the shared DOM. Switching to a backgrounded
  tab triggers a full re-render from its buffers (no refetch — buffers
  are authoritative). The toolbar status line, route banner, hop table,
  timelines, and footer all re-render from the active tab's state.
- **Close tab.** The tab close × issues ``DELETE /api/traces/{id}``; on
  error it still removes the tab locally and surfaces the error in the
  new active tab's status line. Closing a tab frees a backend session
  slot under the ``MAX_SESSIONS = 8`` cap. Closing the last tab spawns a
  fresh idle tab so the dashboard is never empty.
- **Reload restores sessions.** On page load, ``GET /api/traces`` opens
  one tab per existing session (snapshot + SSE attach in the background).
  This prevents orphaned sessions pinging forever after a reload: every
  live trace is re-attached rather than duplicated. If no sessions exist,
  a single idle fresh tab is shown (the original single-trace view).
- **New-tab flow.** "+" opens a fresh (unsaved) idle tab and focuses the
  target input. Pressing Start in an idle fresh tab creates the session
  and binds THAT tab (no extra tab created). Starting a trace when the
  cap is exceeded surfaces the 409 in the status line.
- **Single-trace UX preserved.** With exactly one trace, the dashboard
  behaves as before (target history, focus dropdown + caption + chip +
  drag, per-panel range dropdowns, route banner, target row, silent-hop
  handling, graph connector, legend). The tab bar shows the one trace's
  tab plus the "+" button.

#### Timeline history restore + merge (Slice D1)

The original ``applySnapshot`` filtered local ``state.samples`` at/below
the snapshot watermark but never merged ``snap.samples`` back into the
timeline buffers. EventSource opens before the initial snapshot, so SSE
rounds stored in the snapshot covered those rounds, and the local copies
were then deleted — leaving timelines blank until new rounds arrived.
Same after route discovery, Focus changes, and SSE reconnects. Worst
case: mark a trace persistent, quit the browser,
reopen — the restored tab re-attaches and new samples appear, but the
server's retained history is never plotted.

- **Merge, don't discard.** ``applySnapshot`` and ``hydrateHopBaselines``
  now merge the snapshot tail into the per-hop timeline buffers via
  ``PURE.mergeSamples(existing, incoming, cap)`` (sorted union, exact-t
  dedup with incoming-wins, idempotent, bounded by ``RAW_CAP`` = 10 000).
  The watermark still guards ``pushSample`` against double-counting SSE
  rounds already covered by the snapshot, so the merge is safe: future
  SSE rounds at/below the watermark are rejected, and the buffer carries
  the snapshot's history. Watermark / seedStats bookkeeping stays
  SEPARATE from timeline storage — the
  merge only touches sample arrays.
- **History backfill.** The snapshot's 100-sample tail covers only the
  last few minutes; the server retains up to ``SAMPLE_CAP`` (10 000) raw
  samples per stream. On tab restore/reload (the ``GET /api/traces``
  re-attach path), SSE reconnect (the ``ready`` event), route discovery
  (primary hop may switch), non-primary panel open (double-click), and
  range widen (1h/3h selected), the client fetches
  ``GET /api/traces/{id}/history`` for the relevant hop(s) over the
  panel's selected range and merges. ``end`` is clamped at ``paused_at``
  via ``effNowS()`` for paused sessions. History fetches are per-tab,
  self-guarded by ``tabManager.isActive`` for DOM writes, and
  generation-guarded so a stale fetch landing after a reset/close cannot
  write into a session the tab no longer owns. The merge deduplicates
  overlapping tails, so a history fetch is always safe (idempotent over
  overlapping windows).
- **Multi-tab isolation.** Merges and fetches are per-tab; background
  tabs merge into their own buffers without painting. Switching to a
  backgrounded tab re-renders from its (already-merged) buffers.

#### Server rollup tier + 12h/24h ranges (Slice D2)

Raw retention (``SAMPLE_CAP`` = 10 000/stream) is ≈ 6.9 h at the default
2.5 s interval and ≈ 2.8 h at 1 s — insufficient for 12 h / 24 h ranges,
and raising it is memory-unsafe at short intervals (24 h @ 1 s × 30 hops
× 8 sessions is GB-scale worst case). Slice D2 adds a server-side
fixed-wall-clock 1-minute rollup tier per stream (see the history endpoint
contract for the full bucket math) and a bucket rendering path so 12 h /
24 h ranges work beyond raw retention.

- **Range dropdown extension.** Each per-panel range dropdown gains
  ``12 hours`` (43 200 s) and ``24 hours`` (86 400 s); the order is
  ``1 m / 5 m / 10 m / 1 h / 3 h / 12 h / 24 h`` and the default stays
  ``10 m``.
- **Resolution-per-range fetch.** Ranges ≤ ``RAW_RANGE_MAX_S`` (10 800 s,
  unchanged from D1) keep the D1 raw fetch (``resolution=raw``); ranges
  beyond use ``resolution=auto`` so the server resolves raw or 1m against
  raw retention. The fetch handler branches on the response's actual
  ``resolution`` field: raw responses merge into the raw buffer
  (``state.samples``), 1m responses merge into a SEPARATE bucket buffer
  (``state.buckets``). The two buffers are independent per hop and never
  mixed.
- **Bucket rendering on the timeline canvas.** When the panel's range
  exceeds ``RAW_RANGE_MAX_S`` the renderer reads from ``state.buckets``
  and draws a step line of ``avg_ms`` (bridging over omitted/empty
  buckets as the raw path bridges over individual losses), with a red
  loss mark at every bucket ``t`` whose ``loss_count > 0``. Full-loss
  buckets render ONLY the loss mark (no line point); partial-loss
  buckets still get a line point AND a loss mark. No min/max band in
  this slice (the rollup envelope carries min/max but the renderer keeps
  the D1 visual: one step line + red marks). The per-panel stats caption
  switches to the bucket form (``avg … · loss … · n … · buckets N``;
  ``cur`` is omitted because a bucket has no single current sample).
  Pure helpers (``PURE.mergeBuckets``, ``PURE.bucketsToDrawablePoints``,
  ``PURE.resolutionForRange``) are node-testable and DOM-free.
- **Table stats / seedStats / focus / graph column unchanged.** The hop
  table, focus window, and graph-column whiskers/markers all remain
  raw-only — they are driven by the live sample buffer and the snapshot
  baseline. The rollup tier does not feed the table or graph column.
- **Tick formatting for 12 h / 24 h.** The canonical tick-step set
  extends to 2 h / 4 h / 6 h so the longer ranges yield a readable tick
  count. The date-prefix logic anchors the FIRST tick with its date when
  the window crosses midnight (so a 24 h window straddling midnight
  shows TWO date labels: the window-start anchor plus the new-date
  boundary).
- **Pause-freeze semantics unchanged.** The effNowS / paused_at anchoring
  carries over to the bucket-fetch path: a paused session's history
  ``end`` clamps at ``paused_at`` via ``effNowS()`` so a restored paused
  12 h / 24 h panel shows frozen populated history instead of an empty
  panel.

#### Pause freeze + status arbitration

- **Paused views freeze.** While a session is paused, every data-window
  consumer (timeline draw, drag-selection mapping, rolling table stats,
  Focus caption, focus expiry) reads a per-tab effective time
  (``effNowS()``) that is frozen at the server-recorded ``paused_at``
  instead of wall clock — graphs stop scrolling and stats stop decaying,
  matching the PingPlotter reference. Resume unfreezes at current wall
  time (the gap is real dead time). Reload of a paused session anchors
  the freeze at ``paused_at`` from the snapshot, and the timeline history
  backfill (Slice D1) clamps its ``end`` at ``paused_at`` via
  ``effNowS()`` so a restored paused session shows frozen populated
  history instead of an empty panel.
- **Authoritative state, causal gating.** Snapshots are gated by request
  generation, not state comparison: a snapshot issued before a confirmed
  pause/resume is discarded, but with no control transition pending the
  server snapshot is authoritative and flips Running/Paused in both
  directions (multi-window convergence). An ambiguous control failure
  (request may have committed, response lost) triggers an immediate
  reconciling snapshot.
- **Derived status line.** The toolbar status is computed by pure
  ``PURE.deriveStatus(view)`` from recorded facts and written by a single
  ``renderStatus()`` — no imperative writers. Precedence: Resetting… >
  Pausing…/Resuming… > Starting… > error > Reconnecting… > Running >
  Paused > Idle. An unreconciled control error pins until one
  state-bearing snapshot arrives.

#### Toolbar (Slice 6b — PingPlotter-style controls)

The toolbar above the hop table mirrors the PingPlotter Cloud reference
layout: a side-by-side **Play / Pause** button pair, a Target combobox
with a recent-targets dropdown, an Interval preset dropdown, a Focus
dropdown, a disabled Packet-type dropdown showing "ICMP", a Reset button,
and the status line. A **Focus caption line** under the toolbar shows the
current stats window and its live local-time bounds.

- **Play / Pause split** (replaces the Slice 4 single Start/Pause/Resume
  toggle). ``#btn-play`` is the green-accented primary action (Start when
  idle, Resume when paused); ``#btn-pause`` is enabled only while running.
  Both delegate to the existing ``POST /api/traces/{id}/pause`` and
  ``/resume`` endpoints; ``state.runningKnown`` drives the disabled state
  via ``refreshButtonLabels``. The legacy ``#start-stop`` ID is gone.
- **Target combobox** — a free-text ``#target`` input (semantics
  unchanged) with an absolutely-positioned ``#target-history`` listbox of
  recent targets (most-recent-first, deduped, capped at 10 entries). The
  list is updated on each successful ``POST /api/traces`` and persisted in
  ``localStorage`` under ``netplot.targetHistory`` so it survives reloads.
  Keyboard accessible: ``ArrowDown``/``ArrowUp`` navigate, ``Enter``
  selects, ``Escape`` closes. Mouse clicks use ``mousedown`` to win the
  race against the input's blur.
- **Interval preset dropdown** — ``#interval`` is a ``<select>`` (ID
  preserved from Slice 4; the element type changed from ``<input
  type=number>``) offering ``1 / 2.5 / 5 / 10 / 30 / 60`` seconds with
  ``2.5`` preselected. The selected value feeds the same ``interval_s``
  POST body field.
- **Focus dropdown** — ``#focus-window`` selects the hop-table stats
  window: ``Auto`` (default; the rolling ``FOCUS_S`` = 600 s window) /
  ``1 minute`` / ``5 minutes`` / ``10 minutes``. The selected window is
  applied to ``PURE.statsForHopWindow`` (Count/Avg/Min/Cur/PL%/graph
  whiskers across all hops including the target row), the snapshot fetch
  ``?focus=<seconds>`` query, and the seed-baseline expiry
  (``seedUntil = snapshot's last sample t + focusS``). Changing the
  dropdown re-fetches the snapshot so the seed baseline matches the new
  window and clears any active manual drag-focus. **Manual drag-focus
  overrides the dropdown**: while a drag-focus is active, the dropdown's
  visible value is forced to ``auto`` and the table stats use the
  selection's ``[start, end]`` range; clearing the manual focus
  (focus-chip × or auto-expiry) restores the dropdown's preserved
  selection. The timeline range selector (1 m / 5 m / 10 m buttons) is
  independent — it controls what the canvas displays, not the table stats.
- **Packet-type dropdown** — ``#packet-type`` is a disabled ``<select>``
  showing ``ICMP`` (the only supported probe type; visual clone of the
  reference).
- **Focus caption** — ``#focus-caption`` under the toolbar renders
  ``Focus: <label>, <date>, <start time> – <end time>`` with live local
  timestamps. The label is ``Auto`` / ``1 minute`` / ``5 minutes`` /
  ``10 minutes`` (the dropdown selection) or ``Manual`` while a
  drag-focus is active. Bounds slide with ``now`` for the rolling window
  (refreshed on the 1 s status timer); for a manual focus, the bounds are
  the selection's. Same-calendar-date bounds render the date once before
  the time range (matching the reference); cross-midnight bounds render
  the date on both sides.

#### Background collections (Slice B)

A **Background** checkbox (``#background-toggle``) sits in the toolbar
between the packet-type dropdown and the Reset button. It reflects and
sets the active tab's persistent flag:

- **Disabled** when no session is bound (idle tab) or while a toggle
  POST is in flight; **checked** when the active tab's session is
  persistent.
- Toggling issues ``POST /api/traces/{id}/persist`` and applies an
  optimistic local flip; the response summary re-asserts the value. A
  network failure reverts the flip and surfaces the error in the status
  line.
- The toggle does not disturb probing — the server only writes the
  state file; the probe loop keeps running.

The tab label shows the session name (via :js:func:`PURE.tabDisplayLabel`,
which prefers the user-set ``name`` and falls back to the target-derived
label). A persistent tab carries a small ``⇄`` marker
(:js:func:`PURE.tabMarker`) before the label so an operator can spot
background tabs at a glance. Both the flag and the label ride the
snapshot-adoption path (:js:func:`applySnapshot` adopts ``persistent``
and ``name``), so a second browser window toggling persistence or
renaming converges on the next snapshot fetch — the same machinery
pause/resume state uses.

Disabled-state policy matches PingPlotter Cloud: ``#target`` and
``#interval`` are locked while a session is active (the resolved target
may have changed since trace start; mid-trace retarget is a future P1).
``#btn-play`` is disabled while running; ``#btn-pause`` is disabled while
idle or paused. ``#focus-window`` remains editable mid-trace. The
Focus dropdown, target history, and caption are client-only state; no
backend contract changed.

#### Graph column (Slice 6 — PingPlotter-faithful anatomy)

The hop table's rightmost column is a per-hop latency visualization that
matches the PingPlotter Cloud reference. All positioned elements derive
from one shared ``axisMax = max(201, max latency across hops)`` via
``PURE.axisPercent`` — no hardcoded percentages (the #3 defect was
reference lines hardcoded to a 1000 ms axis).

- **Header area**: a latency legend above the table at top-right — a
  horizontal gradient bar with green/yellow/red zones and "100ms" / "200ms"
  labels at the zone boundaries. The legend uses its OWN scale
  ``legendMax = max(300, axisMax)`` (decoupled from the row graph-column
  axis) so all three zones stay at truthful, legible proportions on healthy
  traces (where ``axisMax`` sits at its 201 floor) and the boundary labels
  do not drift with the data; the legend extends truthfully to ``axisMax``
  only once a real spike pushes it past 300 ms. The boundary tick labels
  live OUTSIDE the bar's ``overflow: hidden`` (in a positioning wrap around
  it) so they are never clipped. One diamond **per-hop avg marker** rides
  the same legend scale: positioned at each hop's current Avg (focus-window
  stats — the same source as the table's Avg column), the target-row marker
  larger with a contrasting outline, hops with no Avg (silent / no samples
  in window) omitted, and near-coincident markers fanned into distinct
  vertical lanes (``PURE.legendMarkerLanes``) so overlaps stay
  distinguishable. Each marker carries ``data-hop`` / ``data-avg-ms`` /
  ``data-lane`` / ``data-target`` attributes and a ``title`` tooltip
  ("hop N avg X ms", "target avg X ms") for DOM assertion. The graph column
  header itself shows the live ROW axis-max value (e.g. "260ms") right-
  aligned above the column.
- **Per row**:
  - A status cell (first column): filled green dot when the hop is
    responding, red when 100% loss in window, hollow/gray when silent
    (no IP). The class names (``status-green`` / ``status-red`` /
    ``status-hollow``) are standalone so they match the renderer's emitted
    ``status-dot status-<kind>`` (the post-6a defect was a ``.status-dot.X``
    compound selector that never matched, painting every dot transparent).
    The final target row carries a small chart glyph and bold text
    emphasis.
  - Background zones (green 0–100ms, yellow 101–200ms, red >200ms) sized
    by ``PURE.zonePercents(axisMax)``.
  - A thin dark whisker line spanning min→max with small vertical end
    caps. Degraded to markers-only (no whisker) when ``max`` is unknown,
    which is the case while only seed-snapshot stats are available — the
    combined path tracks ``max`` from the live buffer and merges any
    future ``max_ms`` shipped by the snapshot.
  - A navy ✕ marker at the average.
  - An open circle ○ at the current value, ring-colored by threshold.
  - A red hatch fill on the right edge proportional to PL%; on a 100%-loss
    row it fills the cell and no whisker/markers are drawn.
- **Red avg connector**: one absolutely-positioned `<svg>` overlay across
  the graph column, recomputed on each render and on resize. Points are
  positioned from row element offsets measured at render time (not fixed
  math) so they align with row centers even with variable row count / the
  emphasized target row.

##### Target row when the route does not reach the target (post-6a)

UDP traceroute discovery often yields a silent tail (hops respond with
`*`, `ip=null`); the final discovered hop's IP may then differ from the
resolved target IP. The backend always probes the target (hop 0) in that
case and ships `{hop: 0, rtt_ms: ...}` as the first result of every round,
so the target's stats are authoritative even when the route is incomplete.

The client binding rule (mirrors PingPlotter, where the target always
answers its final probe):

- **Route ready AND final hop IP == resolved target IP** (the common case,
  e.g. `1.1.1.1`): the final hop IS the target. No separate target row;
  hop 0 is unused. The primary timeline binds the final hop. This is the
  pre-existing correct behaviour.
- **Route ready AND final hop IP != resolved target IP** (silent tail or
  partial route, e.g. `ntp.nict.jp`): render a **TARGET ROW** as the LAST
  table row (after the final discovered hop), with:
  - status dot from the hop-0 stream's live stats;
  - a target glyph (📈) in the **Hop** cell (hop 0 is the target stream,
    not a numbered route hop, so "0" would mislead);
  - IP cell = resolved target IP, DNS cell = resolved target DNS name;
  - live Count/Avg/Min/Cur/PL%/graph from the hop-0 sample stream;
  - ``row-target`` bold emphasis and participation in the shared
    ``axisMax`` and the red avg-connector polyline (as the final point).
  - The **primary timeline panel** binds hop 0 (the target stream), titled
    with the target name/IP — not the silent final hop. Double-click on
    the target row is wired like any other row (it is the primary, so
    toggling is a no-op, identical to the final-hop-is-target case).

The decision is driven by ``PURE.targetRowNeeded(hops, target_ip,
route_state)``. The resolved target IP/DNS are obtained from the snapshot
(the SSE route event carries hops only); the route handler refetches the
top-level snapshot on discovery completion so the resolved IP is available
client-side, which also refreshes the hop-0 baseline without a count reset
(the Slice 4 seed/live watermark machinery keys the target stream as "0"
throughout, so the pending → ready transition carries counts seamlessly).

While the resolved target IP is unknown (snapshot refetch in flight) the
target row is suppressed rather than flashed.

The Slice 5 timeline graph renders one `<canvas>` per open hop:

- A **primary** panel is always shown for the target — the final hop when
  route discovery is ready, or the hop=0 target stream while it is pending
  or has failed (PingPlotter's default target view).
- **Double-clicking any hop row** toggles a stacked non-primary timeline
  panel for that hop. Each non-primary panel has a close control; the
  primary panel does not. Timeline panel state is client-only.
- **Panel header** (Slice 6c — PingPlotter Cloud reference layout): hop
  label on the left, live stats caption in the middle, a per-panel range
  dropdown on the right ("1 minute / 5 minutes / 10 minutes", default
  10 minutes), then the close button on non-primary panels.
- **X axis**: wall-clock time, HH:MM:SS labels, ticks auto-stepped for the
  panel's selected range.
- **Y axis**: latency in ms, auto-scaled to the visible data with headroom,
  floored at 201 ms so all three threshold bands are always visible.
  "Latency (ms)" is rendered as a rotated DOM label reading bottom-to-top
  along the left edge of the canvas (outside the plot area).
- **Background bands**: green ≤ 100 ms (bottom of canvas, low latency),
  yellow 101–200 ms (middle), red > 200 ms (top).
- **Step-line RTT** (Slice 6c — PingPlotter Cloud reference fidelity):
  each successful sample's value holds horizontally to the next
  successful sample's x, then a vertical jump lands on the next value at
  that x. Lost samples are skipped (the line bridges over them) and are
  painted separately as red vertical loss marks. Path geometry is built
  in ``PURE.stepPath`` (node-asserted + exposed on ``window.PURE`` for
  live-browser verification) and rendered via ``moveTo``/``lineTo``.
- **"Packet Loss %" label** (Slice 6c — PingPlotter Cloud reference):
  red rotated DOM label along the right edge of each canvas, reading
  bottom-to-top. DOM (not canvas text) so it stays crisp under DPR zoom
  and is assertable directly.
- **Red vertical marks** at lost samples.
- **Per-panel range dropdown** (Slice 6c, replaces the shared 1 m / 5 m /
  10 m segmented toolbar control): each timeline panel owns its own
  range state, independent of every other panel. Default 10 m. The
  toolbar's shared range-buttons control (and the IDs ``#range-buttons``,
  ``#range-1m``, ``#range-5m``, ``#range-10m``) were removed. **Slice D1
  extends the offered ranges to 1 m / 5 m / 10 m / 1 h / 3 h. Slice D2
  extends again to 1 m / 5 m / 10 m / 1 h / 3 h / 12 h / 24 h** (12 h / 24 h
  are served from the server-side rollup tier — see the history endpoint
  contract). The client raw-sample buffer is widened from ``FOCUS_S + 5``
  (605 s) to ``RAW_RANGE_MAX_S + 5`` (10 805 s, unchanged from D1) so the
  ≤ 3 h ranges have continuous coverage; 12 h / 24 h panels do NOT widen
  the raw buffer (they rely on bucket fetches, keeping raw retention from
  D1 unchanged). The server's ``SAMPLE_CAP`` (10 000) bounds the absolute
  raw count per stream. When a panel's range is widened, the client
  fetches ``/history`` for that hop over the new window so the
  newly-visible region is populated (raw fetch for ≤ 3 h, ``auto`` for
  12 h / 24 h — merge is idempotent in both buffers). X-axis ticks for
  ranges > 10 m use ``HH:MM`` labels (24-hour) instead of ``HH:MM:SS``;
  the Slice D2 canonical step set extends to 2 h / 4 h / 6 h so 12 h / 24 h
  yield a readable tick count. When the visible window crosses local
  midnight, the FIRST tick is anchored with its date prefix and every
  subsequent calendar-date change carries a date prefix
  (``M/D/YYYY HH:MM``); a 24 h window straddling midnight shows TWO date
  labels (window-start anchor + new-date boundary).
- **Focus window**: drag-selecting a region on any timeline recomputes the
  hop table's Count/Avg/Min/Cur/PL% for **all hops** over that period and
  shades the selection on every open timeline. Selection outside a
  panel's visible window simply doesn't paint there (the overlay is
  clipped to each panel's own [startS, endS]). A "Focus: start–end ✗"
  chip above the table clears it. Empty selection (click without drag) is
  a no-op. When the selection ages out of the client retention window
  (Slice D1: ``MAX_RANGE_S + 5`` = 10 805 s, matching the widened raw
  buffer), it auto-clears.

When a focus window is active, the table computes stats from the live
client-side sample buffer for that hop filtered to `[start, end]`. The
seedStats baseline (which has no per-sample timestamps) is bypassed for
focus analysis. Clearing focus restores the Slice 4 rolling-window
behavior that combines the snapshot baseline with live samples.

### Internal-host DNS masking (screenshot mode)

The toolbar carries a **Mask internal DNS** checkbox and a ⚙ **settings**
button that opens a settings dialog (internal CIDR list + InfluxDB overlay
connection). The CIDR list is managed server-side (``/api/settings/internal``;
default RFC1918 + IPv6 ULA); classification is done by the server with the
stdlib ``ipaddress`` module and rides the snapshot as ``hops[].internal`` /
``target_internal`` — the client never parses CIDRs.

- The toggle is **display-only**: while on, DNS names belonging to
  internal-IP hops (and the resolved target, when internal) render as the
  neutral alias ``Internal host`` in the hop table's DNS column, timeline
  panel labels, and the tab bar (labels, tooltips, close-button
  aria-labels). A custom session name that equals or contains a known
  internal name (raw target, resolved target DNS) renders as the alias
  too; unrelated custom names pass through. IP addresses and IP-derived
  labels are NEVER masked; absent names keep their ``—``/``*``
  placeholders.
- While a session is bound, the (disabled) target input shows the alias
  instead of the raw target; the operational target is kept untouched in
  the tab's state and the exact value is restored in the field the moment
  masking is turned off or the session resets. The toolbar status/error
  line and the route banner mask occurrences of the known internal names
  (server error bodies can echo the target).
- The recent-targets dropdown is suppressed while masking is on: history
  entries are idle free-text targets with no internal classification
  available client-side (classification only rides a live session's
  snapshot), so entries cannot be safely masked — the popup is hidden
  rather than guessing (no DNS lookup or backend change is invented for
  this). The raw history remains in localStorage untouched.
- The real trace target/data never changes, and API/CSV payloads keep the
  real names (the mask makes no redaction claim beyond the dashboard).
- Saving settings triggers a snapshot refetch on every tab so the
  server-side annotations converge immediately.

### ICMP non-responsive presentation (trace-local, neutral)

An intermediate hop that NEVER replied to a probe in the current trace is
presented neutrally instead of as alarming red loss:

- **Server decision, lifetime counters:** each ``Hop`` counts ``probes`` /
  ``replies`` across the whole trace. A hop is ``icmp_non_responsive`` in
  the snapshot once ≥ ``ICMP_NONRESPONSIVE_MIN_PROBES`` (5) probes were
  sent and no reply was EVER observed. Never a rolling window: a hop that
  replied once and then went silent keeps ordinary loss treatment. Silent
  unknown-IP hops reach the same neutral state through the same counters.
- **Destination excluded:** the target (hop-0 stream, the target row, or
  the final hop when it IS the target) never gets the flag — its 100 %
  loss remains an error.
- **Client presentation:** neutral gray status dot, gray Cur marker
  showing ``NR`` with a descriptive "ICMP non-responsive" tooltip/title,
  gray loss hatching and gray timeline loss marks, and an
  "ICMP non-responsive" suffix on the timeline caption. Counts and loss
  percentages stay truthful.
- **Prompt both ways:** the client extends the server flag with its own
  live evidence (sticky ``everReplied`` fact + all-loss probe count) so the
  presentation flips after the 5th failed round and reverts the moment a
  reply lands, without waiting for a snapshot. Sticky reply evidence is
  seeded from snapshot baselines and merged history so a historical reply
  that aged out of every window still disqualifies classification.
- **Reset semantics:** the classification survives only within the current
  trace — reset/new trace clears it, and a re-discovery (changed hop/IP)
  builds fresh counters. There is no persisted per-IP flag and no manual
  per-device control. No extra probing is added; existing samples/stats
  drive everything.

### Throughput overlay (InfluxDB 1.x, optional)

The primary/target timeline panel carries a **Throughput** checkbox
(disabled while no InfluxDB source is configured in settings). While on:

- The client fetches ``GET /api/throughput`` over the panel's selected
  window — on toggle, on primary range change, and on a bounded refresh
  cadence tied to the server bucket step (5 s for windows ≤ 30 minutes,
  60 s up to 6 hours, and 300 s for longer windows) while running. Paused
  tabs stop refreshing (their window is frozen). Requests are issued only
  while enabled and are generation/session-guarded so stale, out-of-order,
  or post-reset responses cannot write. A successful Influx settings save
  clears the overlay points/error on EVERY tab and bumps the generation
  (nothing fetched from the old source — in-flight, delayed, or buffered
  on a paused tab — can render or be reused), then refetches the active
  tab when the overlay is still enabled and a source is still configured,
  and disables the toggle otherwise. A backgrounded tab (paused or not)
  refetches its missing overlay window on activation — the frozen pause
  window is honored and the same generation guards apply.
- Rendering draws on the primary canvas only: a separate right-hand Mbps
  axis, two distinguishable direction lines (blue ▼ in/download,
  orange ▲ out/upload) with a legend, missing buckets rendered as path
  BREAKS — null values and timestamp gaps larger than the bucket cadence
  split the lines (``PURE.tpSegments``); never bridged, never drawn as
  zero — and the plot-area padding widened for the axis (focus drag
  mapping follows the same padding). Latency/loss layers and focus drag
  are unchanged.
- Unconfigured / unreachable / empty sources surface clearly in the legend
  ("throughput unavailable (…)" / "throughput: no data") and never break
  the existing chart. No dashboard, collector, sample persistence, or
  router-specific connection logic ships — the chart's 24 h max
  and in-memory-only history are unchanged.

## Container packaging

The image is a multi-stage build on `python:3.12-slim` (see `Dockerfile`
at the repo root).

- **Build stage**: installs `uv` (pinned), exports the frozen runtime
  dependency set from `uv.lock` to a requirements file (`uv export
  --frozen --no-dev --no-emit-project`), and builds the netplot wheel with
  `uv build --wheel`. Reproducibility of the Python dep graph comes from
  `uv.lock`, not from the resolver.
- **Runtime stage**: same `python:3.12-slim` base; `apt-get install
  --no-install-recommends iputils-ping traceroute ca-certificates` (apt
  lists cleaned in the same layer), then `pip install` of the exported
  requirements followed by `pip install --no-deps` of the built wheel. A
  non-root user (uid 1000) is created with `useradd` and the image drops
  to it via `USER`. ``ENV NETPLOT_DATA_DIR=/data`` plus a ``chown``'d
  ``/data`` and ``VOLUME /data`` make persistent (background) sessions
  writable by the non-root process; operators wanting persistence
  across container recreation mount a named volume at ``/data`` (see
  README).
- **System binaries**: `ping` and `traceroute` work unprivileged under
  Docker's default `ping_group_range`; `traceroute` uses UDP probes, so no
  `cap_net_raw` / `--privileged` is required.
- **Entrypoint / port**: `CMD ["netplot", "--host", "0.0.0.0", "--port",
  "8000"]`; `EXPOSE 8000`.
- **Health**: `HEALTHCHECK` polls `GET /healthz` via the stdlib
  (`urllib.request`) — no `curl` install, keeping the image lean.
- **OCI labels**: `org.opencontainers.image.title`, `.description`,
  `.license(s)`, and `.source` (the canonical GitHub repository URL, used
  for GHCR repository association).
- **Build inputs**: `pyproject.toml`, `uv.lock`, `README.md`, and `src/`
  are copied into the build stage. `.dockerignore` excludes tests, docs,
  screenshots, caches, and the local venv from the build context.
- **Published images**: ``ghcr.io/jschert/netplot`` is **private** and
  multi-arch (``linux/amd64`` + ``linux/arm64``). Each stable ``vX.Y.Z``
  tag publishes image tag ``X.Y.Z``; ``latest`` is moved only after
  checks and runtime smoke of both architectures succeed. The publisher
  verifies and loads the exact checksummed image artifacts that passed
  smoke, without rebuilding, then assembles consumer manifests by digest.
  Pulls require ``docker login ghcr.io`` (``read:packages``) — see the
  README Quick Start for the authenticated install path.
- **Convenience runner**: ``docker-compose.yml`` has no ``build:``
  stanza — it pulls ``ghcr.io/jschert/netplot:${NETPLOT_VERSION:-latest}``
  and maps
  ``${NETPLOT_BIND_ADDRESS:-127.0.0.1}:${NETPLOT_PORT:-8000}:8000``
  (loopback by default: the app has no authentication) with ``restart:
  unless-stopped`` and the ``netplot-data`` named volume at ``/data``.
  Developer source builds use the explicit override
  ``docker compose -f docker-compose.yml -f compose.build.yml up -d
  --build`` (``build: .`` tagged ``netplot:dev``, replacing the ghcr.io
  image reference; ports/restart/volume inherited from the base file).

## Slice plan

- **Slice 1 (done):** FastAPI shell, dashboard placeholders, CLI, tests.
- **Slice 2 (done):** Probe engine (ping) + session manager + trace CRUD/snapshot/
  SSE APIs for the final target only (no route discovery yet).
- **Slice 3 (done):** Route discovery (traceroute) + concurrent per-hop probing;
  snapshot returns the full hop table; SSE round events include per-hop
  results plus a `route` event when discovery completes; per-hop stats and
  a `hop=` query param for hop-scoped snapshots.
- **Slice 4 (done):** Live hop-table UI: route table with stats and per-hop
  latency bar, SSE-driven, Start/Pause/Reset wired to the API. Dashboard
  assets shipped as package static files (``src/netplot/static/``); vanilla
  HTML/CSS/JS, no framework or build step.
- **Slice 5 (done):** Timeline graph on canvas (RTT line, loss marks,
  threshold bands, range selector, focus window recompute), hop-row
  double-click.
- **Slice 6 (done):** Graph column (PingPlotter-faithful anatomy, graph
  axis fix): status dots, shared-axis zones, whisker/markers, loss hatch,
  red avg-connector, live legend.
- **Post-6a (done):** Status-dot visibility fix (class-name mismatch) +
  target row for routes that do not reach the target (silent tail / partial
  route), with the primary timeline binding the hop-0 target stream.
- **Slice 6b (done):** PingPlotter-style toolbar + focus controls. The
  single Start/Pause/Resume toggle was split into a green Play button
  (Start/Resume) and a Pause button; the Target input gained a
  recent-targets dropdown persisted in localStorage; the Interval number
  input became a preset dropdown (1 / 2.5 / 5 / 10 / 30 / 60 s); a Focus
  dropdown (Auto / 1 m / 5 m / 10 m) was wired so the table stats window,
  snapshot fetch, and seed-baseline expiry all follow the selected value;
  a disabled ICMP-only packet-type dropdown was added; and a Focus caption
  line under the toolbar shows the current window with live local-time
  bounds. No backend changes.
- **Slice 6c (done):** Timeline graph fidelity to the PingPlotter Cloud
  reference. The shared 1 m / 5 m / 10 m segmented toolbar control was
  replaced with a per-panel range dropdown at the top-right of EACH
  timeline panel (independent state, default 10 m); the timeline RTT
  rendering changed from diagonal connects to a STEP plot that holds each
  value horizontally to the next sample's x and then jumps vertically,
  bridging over lost samples; "Packet Loss %" was added as a red rotated
  DOM label along the right edge of each canvas and "Latency (ms)" as a
  rotated DOM label on the left. No backend changes.
- **Slice 7 (done):** Multi-trace tabs (PingPlotter-style). The dashboard
  gained a tab bar; each tab is an independent live trace. The
  single module-level ``state`` was refactored into a per-tab factory
  (``createTab``) so each tab owns its sessionId, EventSource, sample
  buffers, hop list, focus window + drag-focus, timeline panels, and
  pause/resume/reset bindings. Background tabs keep their EventSource
  attached and keep buffering samples without touching the shared DOM;
  switching to a backgrounded tab triggers a full re-render from buffers.
  Close tab issues ``DELETE /api/traces/{id}`` (best-effort; on error the
  tab is still removed and the error surfaced). Reload restores all live
  sessions as tabs via ``GET /api/traces`` (preventing orphaned pinging
  sessions). A server-side cap of ``MAX_SESSIONS = 8`` concurrent sessions
  was added to ``SessionManager``; ``create()`` beyond the cap raises
  ``SessionLimitReached`` which the API translates into HTTP 409 with a
  clear ``detail``; the frontend surfaces the 409 in the status line.
  Pure tab-state helpers (``PURE.tabLabel``,
  ``PURE.nextActiveAfterClose``) added with node unit tests.
- **Slice A (done):** CSV sample export endpoint + toolbar button (see
  ``GET /api/traces/{id}/export`` above).
- **Slice B (done):** Persistent (background) collections. A trace can
  be marked persistent; its DEFINITION (id, target, interval, name,
  persistent flag, desired state, created_at — NOT sample history) is
  written to ``$NETPLOT_DATA_DIR/sessions.json`` (env var, default
  ``<cwd>/data``) and auto-restored on the next server boot, surviving
  container restarts/redeploys. Writes are atomic (tmp + os.replace)
  and happen only on lifecycle events (create-if-persistent, delete,
  pause, resume, persist-toggle, rename). Restore is non-blocking for
  app serving; paused sessions restore as paused (``paused_at`` = restore
  time); the ``MAX_SESSIONS`` cap holds on restore (counting successful
  registrations, not file offsets). ``POST /api/traces`` accepts optional
  ``{name, persistent}``; new
  ``POST /api/traces/{id}/persist {persistent, name?}`` toggles/renames.
  ``name`` and ``persistent`` surface in summary, list, and snapshot so
  multi-window convergence rides the snapshot path. A per-tab
  ``#background-toggle`` checkbox in the toolbar reflects/sets the flag;
  the tab label shows the name with a ``⇄`` marker for persistent tabs.
  State-file schema is versioned (v1); boot never crashes on any file
  condition. **Degraded mode:** a present-but-unusable file (corrupt,
  unknown schema, oversized, wrong shape, OR any entry dropped during
  validation) triggers quarantine + read-only mode (writes return
  **HTTP 503**); ``/healthz`` surfaces ``persistence: "degraded"``.
  **Cancellation safety:** persistent create+start is shielded so a
  client cancellation cannot leave a durable ``running`` definition with
  no probe task. **Persist endpoint:** no-op fast path (no lock/write
  when nothing changes) + **HTTP 409** on pending mutation + **HTTP
  404** on concurrent delete (stale-object guard in the manager).
  **Name sanitizer:** category-based (``unicodedata.category()`` in
  ``{Cc, Cf, Cs, Co}``), exhaustive — strips bidi overrides, ZWSP, BOM,
  unpaired surrogates, and private-use; the client mirrors via Unicode
  property escapes. **First-write durability:** every newly-created
  parent directory is fsynced (not just the leaf). Pure helpers
  (``PURE.tabDisplayLabel``, ``PURE.tabMarker``,
  ``PURE.sanitizeName``) added with node unit tests.
- **Slice C (done):** Prometheus ``GET /metrics`` endpoint (feature 3 of 3).
  See the "Metrics" section above for the full series contract. Seven gauge
  families: ``netplot_session_info`` (one series per session, carrying
  ``state`` for alert gating), ``netplot_target_rtt_current_ms`` /
  ``_avg_ms`` / ``_loss_pct`` / ``_up`` (target stream over a fixed
  60-sample window; current is absent when the last probe was a loss — no
  stale values), and ``netplot_hop_rtt_current_ms`` / ``_loss_pct`` (per
  route hop, ``hop`` label = route index). Pure formatter
  (``render_metrics``) + on-loop capture (``capture_metrics``) split so the
  formatter is unit-testable without HTTP; cardinality is bounded
  (``MAX_SESSIONS`` × ≤ 30 hops × 60-sample window). Paused sessions keep
  emitting last-known values truthfully labeled via ``state="paused"``.
  Label escaping (``\``, ``"``, ``\n``) mirrors the CSV sanitizer mindset.
  Stack wiring (Prometheus scrape config, Grafana dashboards, alert rules)
  is deployment-specific configuration outside the application source.
- **Slice D1 (done):** Timeline history restore + ranges up to 3h. A new
  ``GET /api/traces/{id}/history?hop=<index>&start=<epoch_s>&end=<epoch_s>``
  endpoint serves the server's retained raw sample history for one stream
  (running or paused; server buffers authoritative), clamped to
  ``[start, end]`` and bounded by ``SAMPLE_CAP`` (10 000/stream). The
  envelope carries ``earliest_t`` (null when empty) so clients distinguish
  "no data" from "older than retention", and ``resolution: "raw"`` so a
  future D2 slice can add a bucketed resolution for 12h/24h ranges without
  breaking D1 clients. The frontend ``applySnapshot`` /
  ``hydrateHopBaselines`` now MERGE snapshot tails into the per-hop
  timeline buffers via ``PURE.mergeSamples`` (sorted union, exact-t dedup,
  idempotent, bounded) instead of discarding local samples at the
  watermark — fixing the blanking defect on initial load, route
  discovery, Focus change, and SSE reconnect. On tab restore/reload,
  SSE reconnect, route discovery, non-primary panel open, and range
  widen, the client fetches ``/history`` for the relevant hop(s) over the
  panel's selected range and merges (per-tab, ``isActive``-guarded,
  generation-guarded). Pause-freeze clamps the history ``end`` at
  ``paused_at`` via ``effNowS()`` so a restored paused session shows
  frozen populated history. The per-panel range dropdown gained
  ``1 hour`` and ``3 hours`` options (default stays 10 m); the client raw
  buffer widened from ``FOCUS_S + 5`` (605 s) to ``MAX_RANGE_S + 5``
  (10 805 s). X-axis ticks for ranges > 10 m use ``HH:MM`` labels and
  prefix the date on the first tick past midnight. Pure helpers
  (``PURE.mergeSamples``, ``PURE.formatClockHM``,
  ``PURE.crossesMidnight``) added with node unit tests. Out of scope
  (Slice D2): server-side 1-minute rollup tier, 12h/24h ranges, changes
  to ``SAMPLE_CAP``, CSV export, metrics.
- **Slice D2 (done):** Server rollup tier + 12h/24h timeline ranges.
  Per sample stream (target stream + each route hop) the server maintains
  an incremental rollup of fixed-wall-clock
  60-second buckets (``StreamRollup`` / ``RollupBucket`` in
  ``src/netplot/session.py``); bucket key = ``floor(t/60)*60``, aggregate
  is ``count`` / ``loss_count`` / ``avg`` / ``min`` / ``max`` over
  successful samples with the same overflow-safe divide-before-subtract
  incremental mean as ``metrics``. Cap ``ROLLUP_CAP = 1440`` buckets per
  stream (24 h); on overflow the smallest-``t`` bucket is evicted so the
  cap newest survive. Buckets are folded in the SAME on-loop probe write
  path as the raw deques — no separate aggregation pass — so a concurrent
  history read cannot observe a half-updated state. The history endpoint
  ``resolution`` param now accepts ``raw`` (D1 default, unchanged),
  ``1m`` (rollup buckets), and ``auto`` (``raw`` when the requested
  window fits raw retention ``SAMPLE_CAP * interval_s``, else ``1m``);
  the response carries the actual resolution used. The 1m envelope is
  ``{..., resolution: "1m", earliest_t, buckets: [{t, avg_ms, min_ms,
  max_ms, count, loss_count}]}`` ascending by ``t``; full-loss buckets
  carry null ``avg``/``min``/``max``; empty buckets (paused gaps,
  pre-start) are omitted (sparse response). The frontend range dropdown
  gains ``12 hours`` and ``24 hours``; ranges > 3 h fetch with
  ``resolution=auto``. Bucket rendering on the canvas is a step line of
  ``avg_ms`` (bridging over omitted/empty buckets as the raw path bridges
  over individual losses) with red loss marks at every bucket ``t`` whose
  ``loss_count > 0``; full-loss buckets render ONLY the loss mark (no
  line point). Bucket data is stored SEPARATELY from raw sample buffers
  per panel (``state.buckets`` vs ``state.samples``); the rendering branch
  is driven by the panel's range. Table stats, seedStats, focus windows,
  and the graph column remain raw-only — untouched. Tick canonical-step
  set extended with 2 h / 4 h / 6 h; the date-prefix logic now anchors
  the first tick with its date when the window crosses midnight (so a
  24 h window straddling midnight shows TWO date labels). Pause-freeze
  semantics unchanged. The rollup tier does NOT feed ``/metrics`` or
  ``/export`` (those read the raw deques only). Both ``ROLLUP_BUCKET_S``
  and ``ROLLUP_CAP`` are named module constants, NOT runtime API params;
  they are overridable per-instance (``StreamRollup(bucket_s=…, cap=…)``)
  for unit tests and via the ``NETPLOT_ROLLUP_BUCKET_S`` /
  ``NETPLOT_ROLLUP_CAP`` env vars for live end-to-end evidence (a 5 s
  bucket makes a 12 h panel fill in seconds). Pure helpers
  (``PURE.mergeBuckets``, ``PURE.bucketsToDrawablePoints``,
  ``PURE.resolutionForRange``) added with node unit tests. Out of scope
  (deferred): min/max whisker bands on bucketed ranges, configurable
  bucket size at runtime, CSV/metrics changes (rollup does not feed
  ``/metrics`` or ``/export``).
- **Slice E (done):** Three dashboard features. (1) **Settings + internal-host
  DNS masking:** a ⚙ settings dialog manages the internal CIDR list
  (`GET/PUT /api/settings/internal`, default RFC1918 + IPv6 ULA, persisted
  atomically to `<data_dir>/settings.json`); the snapshot annotates
  `hops[].internal` / `target_internal` classified server-side with stdlib
  `ipaddress`, and the toolbar's display-only **Mask internal DNS** toggle
  replaces internal hosts' DNS names with the neutral alias `Internal host`
  (IPs never masked; API/CSV untouched). (2) **ICMP non-responsive
  presentation:** per-hop lifetime `probes`/`replies` counters classify
  never-replied intermediate hops (≥ 5 probes, 0 replies ever — never a
  rolling window; destination excluded; silent unknown-IP hops included) as
  `icmp_non_responsive` in the snapshot; the client styles them neutral gray
  with descriptive text (dot / `NR` marker / graph hatching / timeline marks
  + caption) while preserving real counts and loss, flips promptly in both
  directions from live evidence, and resets with the trace. (3) **InfluxDB
  1.x throughput overlay:** generic, settings-only connection
  (`GET/PUT /api/settings/influx`; url/database/interface all blank by
  default — no presets, database and interface required once a URL is set)
  and `GET /api/throughput` returning normalized `{t, in_mbps, out_mbps}`
  points (stdlib HTTP backend-side, bounded timeout, redirects refused,
  1 MiB response cap, `non_negative_derivative(last(…), 1s)` —
  counter-reset safe and bytes/s at every step — `GROUP BY` the
  interface/host tags so multiple hosts error instead of merging,
  `fill(none)` preserves gaps, 5 s→300 s step ladder, span ≤ 24 h);
  the primary timeline overlays it behind a per-tab checkbox on a separate
  labeled Mbps axis with two direction lines (breaking at gaps), never
  disturbing the existing latency/loss/focus behaviour.
