# netplot implementation notes

The product and API contract live in [`spec.md`](spec.md). The root
[`README.md`](../README.md) contains setup and endpoint instructions.

## Slice 6: graph column

The hop table includes a status-dot column and a shared-axis Graph column.
The graph renders 100 ms and 200 ms latency zones, min–max whiskers with end
caps, a navy average marker, an open current marker, proportional packet-loss
hatching, and a red SVG connector joining average markers. The axis maximum
and legend proportions are recomputed from the current hop statistics; the
connector is remeasured after table renders and resize. Silent hops are
hollow, loss-only hops are red, and the final target row is emphasized.

## Post-6a: status-dot visibility + target row

Two client-side defects fixed (backend unchanged):

1. **Invisible status dots.** `renderRow` emitted
   `status-dot status-<kind>` but `style.css` used compound selectors
   `.status-dot.<kind>` that never matched, so every dot painted
   transparent. The CSS now defines standalone `.status-green` /
   `.status-red` / `.status-hollow` rules.

2. **Target stats lost on a silent-tailed route.** When the route's final
   hop does not reach the resolved target (e.g. `ntp.nict.jp` — hops 16–30
   silent), the client bound "the target" to the silent final hop and
   showed 100% loss for a healthy target. Now a **target row** is appended
   as the last table row whenever the route is ready but the final hop IP
   differs from the resolved target IP. The row shows the target glyph in
   the Hop cell, the resolved target IP/DNS, and live stats from the hop-0
   sample stream; it participates in the shared axis and the avg-connector.
   The primary timeline panel binds hop 0 (the target stream), titled with
   the target name/IP. When the final hop equals the target (e.g.
   `1.1.1.1`), the existing behaviour is unchanged. The decision uses
   `PURE.targetRowNeeded(hops, target_ip, route_state)`; the resolved
   target IP/DNS come from the snapshot, refetched on the route event.

## Slice 6b: PingPlotter-style toolbar + focus controls

Toolbar cloned from the PingPlotter Cloud reference (no backend changes).

- **Play / Pause split.** The single Start/Pause/Resume toggle was
  replaced with two buttons: a green `#btn-play` (Start when idle, Resume
  when paused) and `#btn-pause` (Pause, enabled only while running). The
  legacy `#start-stop` ID is gone; both new buttons keep the existing
  pause/resume endpoints.
- **Target history dropdown.** `#target` is a free-text input wrapped in a
  combobox with an absolutely-positioned `#target-history` listbox of
  recent targets (most-recent-first, deduped, capped at 10 entries,
  persisted in `localStorage` under `netplot.targetHistory`). Updated on
  each successful trace start; keyboard accessible (arrows navigate,
  Enter selects, Escape closes). `PURE.targetHistoryAdd` is exported and
  node-asserted.
- **Interval preset dropdown.** `#interval` is now a `<select>` (ID
  preserved; element type changed from `<input type=number>`) with 1 /
  2.5 / 5 / 10 / 30 / 60 s options, 2.5 preselected. The value feeds the
  same `interval_s` POST body. Send deadlines are independent of the
  three-second reply window; overlapping rounds preserve the selected
  cadence while the per-trace resource budget allows it.
- **Focus dropdown.** `#focus-window` (Auto / 1 minute / 5 minutes /
  10 minutes) controls the hop-table stats window — table stats, per-row
  graph whiskers, the seed/live combination in `PURE.statsForHopWindow`,
  the snapshot fetch `?focus=<seconds>`, and coverage-aware accounting all
  follow the selected window. A loading baseline combines only post-cutoff
  samples and expires when its oldest included `sample_from_t` leaves the
  window; complete raw coverage becomes the sole statistics source.
  **Manual drag-focus overrides the dropdown** until
  cleared: while active, the dropdown's visible value is forced to
  `auto`; clearing the manual focus (focus-chip × or auto-expiry)
  restores the dropdown's preserved selection. The timeline range
  selector stays independent (it controls the canvas display, not the
  table stats).
- **Packet-type dropdown.** `#packet-type` is a disabled `<select>`
  showing ICMP (the only supported probe type; visual clone only).
- **Focus caption.** `#focus-caption` under the toolbar renders
  `Focus: <label>, <date>, <start> – <end>` with live local timestamps.
  The label is the dropdown selection (or `Manual` during a drag-focus);
  the bounds slide with `now` for the rolling window. Same-calendar-date
  bounds show the date once before the time range (matching the
  reference); cross-midnight bounds show it on both sides.

PURE additions, all node-asserted: `targetHistoryAdd`, `focusWindowSeconds`,
`focusWindowLabel`, `formatClock12`, `formatDateMDY`, `formatFocusCaption`,
`adoptSnapshotBaseline`, `computeStatsWindow`, `statsForHopWindow`, `FOCUS_OPTIONS`,
`TARGET_HISTORY_MAX`. Disabled-state policy matches PingPlotter Cloud:
`#target` and `#interval` are locked mid-trace; `#focus-window` stays
editable.

## Slice 6c: timeline fidelity to the PingPlotter Cloud reference

Client-only clone of the PingPlotter Cloud timeline anatomy (no backend
changes).

- **Step-line RTT rendering.** The diagonal-connected RTT line became a
  STEP plot: each successful sample's value holds horizontally from its
  x to the next successful sample's x, then a vertical jump lands on the
  next value at that x. Lost samples are skipped (the line bridges over
  them) and remain painted as red vertical loss marks. Path geometry
  lives in `PURE.stepPath` (node-asserted for the horizontal/vertical
  invariants, gap-bridging, single-sample, and all-loss cases) and is
  rendered via `moveTo`/`lineTo`. A single isolated sample paints a
  small dot so it remains visible.
- **Per-panel range dropdown.** The shared segmented `1m / 5m / 10m`
  toolbar control was removed (along with the IDs `#range-buttons`,
  `#range-1m`, `#range-5m`, `#range-10m`) and replaced with a
  `<select class="timeline-range" data-hop="N">` at the top-right of
  EACH timeline panel header. Options: 1 minute / 5 minutes / 10
  minutes, default 10 m, independent per panel. Range state moved from
  the shared `state.rangeS` to a per-panel `state.panelRangeS` map;
  `rangeForPanel(hopIndex)` resolves the panel's value (defaulting to
  `DEFAULT_RANGE_S`). Drag-focus remains a shared time interval overlaid
  on every panel; selection outside a panel's visible window is clipped
  out by each panel's own `[startS, endS]`.
- **"Packet Loss %" right-edge label.** Red rotated DOM label along the
  right edge of each timeline canvas, reading bottom-to-top
  (`writing-mode: vertical-rl; transform: rotate(180deg)`). DOM rather
  than canvas text so it stays crisp under DPR zoom and is DOM-assertable.
- **"Latency (ms)" left axis label.** Rotated DOM label on the left edge
  of each timeline canvas, reading bottom-to-top.
- **Panel header layout.** Per the reference: hop label on the left,
  stats caption in the middle, range dropdown and (non-primary) close
  button on the right.

PURE additions, all node-asserted: `stepPath`, `rangeOptionFor`,
`RANGE_OPTIONS`. `PURE` is also exposed on `window.PURE` for browser-side
verification of step-path geometry (the live Playwright check asserts the
path invariants directly against this object rather than via pixel
sampling).

## Slice 7: multi-trace tabs (PingPlotter-style)

The dashboard now supports multiple simultaneous traces — one per tab.

- **Tab bar.** A tab bar (`<nav id="tab-bar">`) sits between the toolbar
  and the main content. Each tab is one live trace, labeled by target
  (normalized by `PURE.tabLabel`: scheme, `www.`, port, path, query, and
  fragment are stripped; IPv6 literals are preserved). The active tab is
  highlighted with `aria-selected="true"`. A "+" button at the end opens
  a fresh idle tab.
- **Per-tab factory.** The single module-level `state` was refactored
  into `createTab(sessionInfo)`, a factory returning a tab object that
  owns its own `state` (sessionId, EventSource, sample buffers, hop list,
  focus window + drag-focus, timeline panels + per-panel range
  dropdowns, pause/resume/reset bindings, focus-window selection, target
  / interval). The toolbar elements are SHARED across tabs and always
  reflect the active tab. On tab switch the outgoing idle tab's typed
  target/interval are captured into its state and the incoming tab's
  toolbar values are restored. PURE stays pure (no DOM); pure tab-state
  logic lives in `PURE.tabLabel` and `PURE.nextActiveAfterClose` with
  node unit tests.
- **Background tabs keep collecting.** A background tab's EventSource
  stays attached and its sample buffers keep filling. Every DOM-writing
  closure self-guards with `tabManager.isActive(self)` so background tabs
  never touch the shared DOM. Switching to a backgrounded tab triggers a
  full re-render from its buffers (no refetch — buffers are
  authoritative).
- **Close tab.** The tab close × issues `DELETE /api/traces/{id}`. On
  error the tab is still removed locally and the error is surfaced in the
  new active tab's status line. Closing the last tab spawns a fresh idle
  tab so the dashboard is never empty. `PURE.nextActiveAfterClose` picks
  the next tab to activate (right neighbour of the closed tab, then
  left).
- **Reload restores sessions.** On page load, `GET /api/traces` opens one
  tab per existing session (snapshot + SSE attach in the background).
  This prevents orphaned sessions pinging forever after a reload: every
  live trace is re-attached rather than duplicated. If no sessions exist,
  a single idle fresh tab is shown (the original single-trace view).
- **New-tab flow.** "+" opens a fresh idle tab and focuses the target
  input. Pressing Start in an idle fresh tab creates the session and
  binds THAT tab (no extra tab created).
- **Single-trace UX preserved.** With exactly one trace the dashboard
  behaves as before; the tab bar shows the one trace's tab plus the "+"
  button.

### Backend: session cap

`SessionManager` (session.py) caps the registry at `MAX_SESSIONS = 8`
concurrent sessions. `create()` beyond the cap raises
`SessionLimitReached(count, cap)`; `POST /api/traces` translates that
into **HTTP 409** with a `detail` of the form
`session cap reached (8/8): close an existing trace before starting a
new one`. The frontend surfaces the 409 in the status line (red,
`state-error`) when "+" or Start exceeds the cap. Closing a tab
(DELETE) frees a slot for the next create.

PURE additions, all node-asserted: `tabLabel` (target → tab label
normalisation, IPv6-safe), `nextActiveAfterClose` (pick the tab to
activate after a close).

### Known limitation: HTTP/1.1 connection ceiling

Each tab keeps its own SSE EventSource open. Browsers typically cap
concurrent HTTP/1.1 connections per origin at ~6, so a session restored to
6+ tabs can starve subsequent `fetch` calls (POST, snapshot) until an SSE
stream frees up. This is inherent to the SSE-per-tab design (which the spec
adopts so background tabs keep collecting) interacting with the browser's
HTTP/1.1 pool. HTTP/2 served by an HTTP/2-capable ASGI server or reverse
proxy, or a multiplexed push channel, would lift this per-connection limit;
standard uvicorn HTTP/1.1 mode does not provide an `--http h2` switch. Not
addressed in this slice.

## Post-7: Avg-latency legend bar improvements

Two client-side defects on the "Avg latency" legend bar fixed and live
per-hop average markers added (no backend changes; the row graph-column
axis, zone fills, whiskers, and red avg connector are unchanged).

1. **Clipped boundary labels + collapsing zones.** The legend bar held the
   three zone spans AND the two tick spans as children of a single
   `overflow: hidden` element, and the ticks were positioned at
   `top: 100%` — so the "100ms" / "200ms" labels were always clipped and
   invisible. The zones were also sized from `PURE.zonePercents(axisMax)`
   with `axisMax = max(201, worst-hop-max)`, so on a healthy trace
   (`axisMax` 201) the red zone was a ~1px sliver and on a spiky trace the
   green/yellow zones compressed, making the thresholds visually drift with
   the data. The legend now lives inside a `.graph-legend-wrap` positioning
   context (overflow visible) that holds: the gradient bar (overflow hidden
   clips ONLY the zone fills to the rounded corners), a `.graph-legend-
   markers` layer, and the two tick labels — so the ticks sit just under the
   bar and are never clipped. The legend scale is decoupled from the row
   axis via `PURE.legendMax(axisMax) = max(300, axisMax)`: all three zones
   stay at truthful, legible proportions while `axisMax ≤ 300` (the legend
   does not move), and the legend extends truthfully to `axisMax` only once
   a real spike pushes it past 300 ms. The graph-column header label still
   reports the ROW `axisMax`.
2. **Live per-hop avg markers.** One diamond marker per hop is rendered on
   the legend bar, positioned at that hop's current Avg (focus-window
   stats — the same source as the table's Avg column) via
   `PURE.legendMarkerPercent`. The target-row marker is larger with a
   contrasting red fill + dark outline; hops with no Avg (silent / no
   samples in window) get no marker. Near-coincident markers are fanned
   into distinct vertical lanes by `PURE.legendMarkerLanes` (cluster within
   ~1.5% then round-robin across 3 lanes) so overlaps stay distinguishable.
   Markers update live as rounds arrive and when the focus window changes.
   Each marker carries `data-hop` / `data-avg-ms` / `data-lane` /
   `data-target` attributes and a `title` tooltip ("hop N avg X ms" /
   "target avg X ms") for DOM assertion and accessibility. No click/hover
   cross-highlight (explicitly descoped).

PURE additions, all node-asserted: `legendMax`, `legendMarkerPercent`,
`legendMarkerLanes`, plus the exported `LEGEND_FLOOR_MS` (300) constant.

## Compose split: published-image default + source-build override

End-user installation no longer builds anything. `docker-compose.yml`
has no `build:` stanza — it pulls
`ghcr.io/jschert/netplot:${NETPLOT_VERSION:-latest}` (private image;
requires a `read:packages` `docker login ghcr.io`), maps
`${NETPLOT_BIND_ADDRESS:-127.0.0.1}:${NETPLOT_PORT:-8000}:8000`
(loopback default — the app is unauthenticated), and keeps
`restart: unless-stopped` plus the `netplot-data` named volume at
`/data` (persists `settings.json` + background session definitions
only, never sample history). Developer source builds live in the
explicit `compose.build.yml` override, merged as
`docker compose -f docker-compose.yml -f compose.build.yml up -d
--build` (adds `build: .`, retags `netplot:dev` instead of the ghcr.io
image — Compose scalar-merge replaces `image:`). The README Quick
Start documents the public, version-pinned Compose download and the
authenticated private-image pull separately. Source checkout and native
installation need no GitHub login; GHCR pulls still require package access.
