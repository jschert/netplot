/*
 * netplot dashboard — Slice 5: live hop-table + timeline graph UI.
 *
 * Layout: pure logic lives in the ``PURE`` object below (stats, formatting,
 * threshold colors, time/x mapping, focus-window math). DOM wiring (and the
 * one piece of canvas rendering) sits underneath it inside an IIFE. The pure
 * block has zero DOM references so it can be exercised with ``node --check``
 * and, when run under CommonJS, unit-tested directly.
 */
(function () {
  "use strict";

  // --------------------------------------------------------------- constants

  // Focus window in seconds. Matches the backend's DEFAULT_FOCUS_S (600).
  // This is the "Auto" rolling-stats window the hop table uses when no focus
  // selection is active and no override is selected in the Focus dropdown,
  // and the largest window the timeline offers.
  var FOCUS_S = 600;

  // Focus dropdown choices (Slice 6b). ``auto`` is the rolling default
  // (FOCUS_S); the others are explicit stats-window overrides.
  var FOCUS_OPTIONS = [
    { value: "auto", seconds: FOCUS_S, label: "Auto" },
    { value: "60", seconds: 60, label: "1 minute" },
    { value: "300", seconds: 300, label: "5 minutes" },
    { value: "600", seconds: 600, label: "10 minutes" },
  ];

  // Max number of recent targets retained in the target history dropdown and
  // localStorage (PingPlotter-style).
  var TARGET_HISTORY_MAX = 10;
  var TARGET_HISTORY_KEY = "netplot.targetHistory";

  // Latency thresholds (ms) — PingPlotter Cloud reference.
  // The Cloud reference screenshot is authoritative for this clone: zones are
  // green ≤100, yellow 101–200, red >200.
  var THRESH_GREEN = 100;
  var THRESH_YELLOW = 200;

  // Graph axis scale ceiling in ms. The visible axis spans [0, axisMax] where
  // axisMax = max(THRESH_YELLOW + 1, maxLatencyAcrossHops) so a single hop
  // never collapses the scale, both zone boundaries are always visible, and
  // rows are always comparable.
  var AXIS_FLOOR_MS = THRESH_YELLOW + 1; // 201

  // Legend bar scale floor in ms. The "Avg latency" legend bar uses its own
  // scale [0, legendMax] where legendMax = max(LEGEND_FLOOR_MS, axisMax) so
  // all three zones stay at truthful, legible proportions on healthy traces
  // (where axisMax sits at its 201 floor and would otherwise crush the red
  // zone to a sliver) and the boundary labels never drift with the data.
  // 300 ms keeps green/yellow/red each at a meaningful width while still
  // extending truthfully when a real spike pushes axisMax past 300.
  var LEGEND_FLOOR_MS = 300;

  // Timeline range choices offered by the per-panel range dropdown
  // (seconds). Slice D1 added 1h/3h; Slice D2 adds 12h/24h served from the
  // server-side 1-minute rollup tier. The client raw-sample buffer covers
  // the raw-served ranges (≤ RAW_RANGE_MAX_S); the longer ranges rely on
  // bucket fetches (resolution=auto), NOT raw buffer growth — raw retention
  // behaviour from D1 is unchanged. Default stays 10m.
  var RANGES_S = [60, 300, 600, 3600, 10800, 43200, 86400];
  var DEFAULT_RANGE_S = 600;
  // Largest range served raw (D1 unchanged). Ranges > RAW_RANGE_MAX_S use
  // resolution=auto, which the server resolves to 1m rollup buckets (raw
  // retention is SAMPLE_CAP * interval_s, far below 12h/24h at any sane
  // interval). Kept as a named constant so the raw-buffer sizing
  // (RETENTION_S, RAW_CAP) and the rendering branch stay in sync.
  var RAW_RANGE_MAX_S = 10800; // 3h — largest raw-served range
  var MAX_RANGE_S = 86400; // 24h — largest offered range (rollup-served)
  var RANGE_OPTIONS = [
    { value: "60", seconds: 60, label: "1 minute" },
    { value: "300", seconds: 300, label: "5 minutes" },
    { value: "600", seconds: 600, label: "10 minutes" },
    { value: "3600", seconds: 3600, label: "1 hour" },
    { value: "10800", seconds: 10800, label: "3 hours" },
    { value: "43200", seconds: 43200, label: "12 hours" },
    { value: "86400", seconds: 86400, label: "24 hours" },
  ];

  // Slop retained beyond the largest RAW-served range so drag-selections at
  // the edge of the rolling window still have data, and so a focus
  // selection does not auto-clear while its data is retained. Slice D2
  // keeps this at RAW_RANGE_MAX_S + 5 (10 805s): 12h/24h panels rely on
  // the bucket buffer, NOT raw buffer growth (raw retention from D1 is
  // unchanged). The server's SAMPLE_CAP (10 000) bounds the absolute count
  // per stream.
  var RETENTION_S = RAW_RANGE_MAX_S + 5;

  // Hard cap on client-side per-stream sample count. Mirrors the server's
  // SAMPLE_CAP so the buffer never exceeds the server's retention regardless
  // of the configured interval (a 1 s interval over the 3h retention window
  // would otherwise admit ~10 800 samples, slightly above the server cap).
  var RAW_CAP = 10000;

  // --------------------------------------------------------------- pure logic

  var PURE = {
    /**
     * Compute Count/Avg/Min/Max/Cur/PL% over an arbitrary [startS, endS]
     * range. ``endS`` inclusive. Losses (rtt_ms null/NaN) are counted in
     * ``count`` and ``lossPct`` but excluded from avg/min/max.
     * @param {Array<{t:number,rtt_ms:number|null}>} samples ascending by t
     * @param {number} startS epoch seconds (inclusive lower bound)
     * @param {number} endS epoch seconds (inclusive upper bound)
     * @returns {{count,avg,min,max,cur,lossPct}}
     */
    computeStatsRange: function (samples, startS, endS) {
      if (!samples || samples.length === 0) {
        return { count: 0, avg: null, min: null, max: null, cur: null, lossPct: 0 };
      }
      var win = [];
      for (var i = 0; i < samples.length; i++) {
        var s = samples[i];
        if (s.t >= startS && s.t <= endS) win.push(s);
      }
      return PURE._statsFromWindow(win);
    },

    /**
     * Compute Count/Avg/Min/Max/Cur/PL% over a rolling window
     * [nowS-focusS, nowS]. The focus window is parameterised (Slice 6b) so
     * the Focus dropdown can shrink/expand the table stats independently of
     * the timeline range.
     * @param {Array<{t:number,rtt_ms:number|null}>} samples ascending by t
     * @param {number} nowS epoch seconds to compute the window against
     * @param {number} focusS window width in seconds
     * @returns {{count,avg,min,max,cur,lossPct}|null}
     */
    computeStatsWindow: function (samples, nowS, focusS) {
      if (!samples || samples.length === 0) return null;
      var cutoff = nowS - focusS;
      var win = [];
      for (var i = 0; i < samples.length; i++) {
        if (samples[i].t >= cutoff) win.push(samples[i]);
      }
      if (win.length === 0) {
        return { count: 0, avg: null, min: null, max: null, cur: null, lossPct: 0 };
      }
      return PURE._statsFromWindow(win);
    },

    /**
     * Compute Count/Avg/Min/Max/Cur/PL% over the default focus window
     * [nowS-FOCUS_S, nowS]. Delegates to computeStatsWindow with FOCUS_S for
     * backward compatibility with the Slice 4 contract.
     * @param {Array<{t:number,rtt_ms:number|null}>} samples ascending by t
     * @param {number} nowS epoch seconds to compute the window against
     * @returns {{count,avg,min,max,cur,lossPct}|null}
     */
    computeStats: function (samples, nowS) {
      return PURE.computeStatsWindow(samples, nowS, FOCUS_S);
    },

    /**
     * Shared reducer used by both computeStats and computeStatsRange.
     */
    _statsFromWindow: function (win) {
      if (win.length === 0) {
        return { count: 0, avg: null, min: null, max: null, cur: null, lossPct: 0 };
      }
      var ok = [];
      var losses = 0;
      var sum = 0;
      for (var j = 0; j < win.length; j++) {
        var r = win[j].rtt_ms;
        if (r === null || r === undefined || isNaN(r)) {
          losses++;
        } else {
          ok.push(r);
          sum += r;
        }
      }
      var avg = ok.length ? sum / ok.length : null;
      var min = ok.length ? Math.min.apply(null, ok) : null;
      var max = ok.length ? Math.max.apply(null, ok) : null;
      var cur = win[win.length - 1].rtt_ms;
      if (cur !== null && cur !== undefined && isNaN(cur)) cur = null;
      var lossPct = win.length ? (losses / win.length) * 100 : 0;
      return { count: win.length, avg: avg, min: min, max: max, cur: cur, lossPct: lossPct };
    },

    /**
     * Format a millisecond value as a 1-decimal string. Null/NaN -> em dash.
     */
    formatMs: function (v) {
      if (v === null || v === undefined || (typeof v === "number" && isNaN(v))) {
        return "—";
      }
      return Number(v).toFixed(1);
    },

    /**
     * Format a percent as a 1-decimal string.
     */
    formatPl: function (v) {
      if (v === null || v === undefined || (typeof v === "number" && isNaN(v))) {
        return "—";
      }
      return Number(v).toFixed(1);
    },

    /**
     * Format an epoch-seconds value as HH:MM:SS in local time.
     */
    formatClock: function (s) {
      if (s === null || s === undefined || typeof s !== "number" || isNaN(s)) {
        return "--:--:--";
      }
      var d = new Date(s * 1000);
      var hh = String(d.getHours()).padStart(2, "0");
      var mm = String(d.getMinutes()).padStart(2, "0");
      var ss = String(d.getSeconds()).padStart(2, "0");
      return hh + ":" + mm + ":" + ss;
    },

    /**
     * Threshold bucket for a latency value.
     */
    latencyBucket: function (ms) {
      if (ms === null || ms === undefined || (typeof ms === "number" && isNaN(ms))) {
        return "loss";
      }
      if (ms <= THRESH_GREEN) return "green";
      if (ms <= THRESH_YELLOW) return "yellow";
      return "red";
    },

    /**
     * Convert a value in ms to a percentage position on the shared axis [0..100].
     */
    axisPercent: function (ms, axisMax) {
      if (ms === null || ms === undefined || ms <= 0) return 0;
      var pct = (ms / axisMax) * 100;
      if (pct < 0) return 0;
      if (pct > 100) return 100;
      return pct;
    },

    /**
     * Compute the shared axis maximum from per-hop max latencies.
     * Floor at AXIS_FLOOR_MS so thresholds are always visible.
     */
    axisMax: function (hopMaxes) {
      var m = AXIS_FLOOR_MS;
      for (var i = 0; i < hopMaxes.length; i++) {
        var v = hopMaxes[i];
        if (v !== null && v !== undefined && !isNaN(v) && v > m) m = v;
      }
      return m;
    },

    /**
     * Percent positions of the green/yellow and yellow/red zone boundaries
     * on the shared graph-column axis. Used by the background zone divs and
     * the legend tick labels so they cannot drift from the data scale
     * (hardcoding these to a 1000 ms axis misplaces the boundaries).
     */
    zonePercents: function (axisMax) {
      return {
        green: PURE.axisPercent(THRESH_GREEN, axisMax),
        yellow: PURE.axisPercent(THRESH_YELLOW, axisMax),
      };
    },

    /**
     * Whisker left/right percent positions for a [min, max] range on the
     * shared axis. Returns null when either bound is missing (no whisker;
     * the renderer falls back to markers only). Both percents are clamped
     * to [0, 100] by axisPercent.
     */
    whiskerPercents: function (min, max, axisMax) {
      if (min === null || min === undefined || isNaN(min)) return null;
      if (max === null || max === undefined || isNaN(max)) return null;
      if (axisMax <= 0) return null;
      return {
        left: PURE.axisPercent(min, axisMax),
        right: PURE.axisPercent(max, axisMax),
      };
    },

    /**
     * One connector point for the red avg polyline at row ``rowIndex``.
     * ``avg`` is the hop's average latency; ``axisMax`` is the shared axis
     * ceiling. ``rowTop`` and ``rowCenter`` are pixel offsets relative to
     * the graph column's content box (the caller measures live row DOM at
     * render time, so this stays correct under variable row heights and
     * counts). Returns ``{x, y}`` in those same pixel coordinates, or
     * ``null`` when the hop has no average (loss-only / silent).
     */
    connectorPoint: function (avg, axisMax, rowCenter) {
      if (avg === null || avg === undefined || isNaN(avg)) return null;
      if (axisMax <= 0) return null;
      if (rowCenter === null || rowCenter === undefined || isNaN(rowCenter)) return null;
      return {
        x: PURE.axisPercent(avg, axisMax), // percent across the column width
        y: rowCenter,                       // pixel offset from the column top
      };
    },

    // ----- Legend bar (Avg-latency) scale + per-hop avg markers ---------
    //
    // The legend bar's scale is DECOUPLED from the row graph-column axis so
    // all three zones stay legible on healthy traces. legendMax floors at
    // LEGEND_FLOOR_MS (300) and grows with axisMax only past that point.
    // Per-hop avg markers ride the same legend scale so their positions are
    // truthful against the zones.

    /**
     * Legend bar scale maximum: max(LEGEND_FLOOR_MS, axisMax). Floors at 300
     * so the legend does not move while axisMax stays under 300 (the common
     * healthy case), and extends truthfully once a real spike pushes axisMax
     * past 300. Non-finite/non-positive axisMax falls back to the floor.
     */
    legendMax: function (axisMax) {
      var a = Number(axisMax);
      if (!isFinite(a) || a <= 0) return LEGEND_FLOOR_MS;
      return Math.max(LEGEND_FLOOR_MS, a);
    },

    /**
     * Percent position [0..100] of a hop's avg on the legend bar, or null
     * when the hop has no avg (silent / loss-only / no samples in window).
     * Clamps to [0, 100] so a spike above legendMax still paints inside the
     * bar (the row graph-column shows the truthful unclamped position via
     * axisPercent; the legend marker just stays visible).
     */
    legendMarkerPercent: function (avg, legendMax) {
      if (avg === null || avg === undefined || isNaN(avg)) return null;
      if (legendMax <= 0) return null;
      var a = Number(avg);
      if (!isFinite(a) || a <= 0) return 0;
      var pct = (a / legendMax) * 100;
      if (pct < 0) return 0;
      if (pct > 100) return 100;
      return pct;
    },

    /**
     * Assign a vertical ``lane`` (0..maxLanes-1) to each marker so
     * near-coincident markers (overlapping avg percents) are spread across
     * subtle vertical offsets and stay visually distinguishable.
     *
     * Input: array of marker objects, each carrying at least ``percent``
     * (in [0, 100]). All other caller-supplied fields are preserved.
     *
     * Output: a NEW array (input is not mutated) in the SAME input order,
     * where each item is a copy of the input item plus a ``lane`` integer.
     * Markers whose percent is more than ``minSepPct`` (default 1.5, ≈3px on
     * the 180px bar) from every neighbour get lane 0. Consecutive markers
     * within ``minSepPct`` form an overlap cluster; within a cluster the
     * lanes are assigned round-robin so each member (up to ``maxLanes``,
     * default 3) lands on a distinct lane. Dense clusters (size > maxLanes)
     * wrap, which keeps the fan subtle at the cost of some overlap.
     *
     * Determinism is the point: identical input always yields identical
     * lane assignments, so the fan is DOM-assertable.
     */
    legendMarkerLanes: function (markers, opts) {
      opts = opts || {};
      var minSepPct = opts.minSepPct != null ? opts.minSepPct : 1.5;
      var maxLanes = opts.maxLanes != null ? opts.maxLanes : 3;
      if (maxLanes < 1) maxLanes = 1;
      if (!Array.isArray(markers)) return [];
      // Working copies preserve every caller-supplied field and the input
      // order; only ``lane`` is added.
      var items = markers.map(function (m) {
        var copy = {};
        if (m && typeof m === "object") {
          for (var k in m) {
            if (Object.prototype.hasOwnProperty.call(m, k)) copy[k] = m[k];
          }
        }
        copy.percent = Number(m && m.percent);
        copy.lane = 0;
        return copy;
      });
      // Cluster by percent in sorted order.
      var order = items.slice().sort(function (a, b) {
        return a.percent - b.percent;
      });
      var clusters = [];
      var current = [];
      for (var i = 0; i < order.length; i++) {
        var m = order[i];
        if (current.length === 0) {
          current.push(m);
        } else if (m.percent - current[current.length - 1].percent <= minSepPct) {
          current.push(m);
        } else {
          clusters.push(current);
          current = [m];
        }
      }
      if (current.length) clusters.push(current);
      // Round-robin lanes within each cluster; singletons stay on lane 0.
      for (var c = 0; c < clusters.length; c++) {
        var cl = clusters[c];
        if (cl.length === 1) continue;
        for (var j = 0; j < cl.length; j++) {
          cl[j].lane = j % maxLanes;
        }
      }
      return items;
    },

    /**
     * Threshold bucket boundaries exposed for assertion / docs. Returns
     * the inclusive green ceiling and the inclusive yellow ceiling.
     */
    thresholdBoundaries: function () {
      return { green: THRESH_GREEN, yellow: THRESH_YELLOW };
    },

    /**
     * PL% display bucket for color.
     */
    plBucket: function (pct) {
      if (pct === null || pct === undefined || isNaN(pct)) return "low";
      if (pct <= 0) return "low";
      if (pct < 5) return "warn";
      return "high";
    },

    /**
     * Decide whether a dedicated TARGET ROW must be appended after the
     * discovered hops. This is required when the route is ready but its
     * final hop does not reach the resolved target (silent tail, partial
     * route). The backend keeps probing hop 0 in that case; without this
     * row those samples are invisible and the primary timeline misbinds
     * to a 100%-loss silent hop.
     *
     * Returns false when the resolved target IP is unknown — the client
     * cannot confirm coverage, and the snapshot refetch triggered on the
     * route event will supply it momentarily.
     */
    targetRowNeeded: function (hops, targetIp, routeState) {
      if (!hops || hops.length === 0 || routeState !== "ready") return false;
      if (targetIp === null || targetIp === undefined) return false;
      return hops[hops.length - 1].ip !== targetIp;
    },

    /**
     * The hop index whose timeline is the "primary" (target) graph.
     * Final hop when it IS the resolved target (PingPlotter default);
     * hop 0 (the target stream) when the route does not reach the target
     * or while discovery is pending/failed.
     */
    primaryHopIndex: function (hops, targetIp, routeState) {
      if (!hops || hops.length === 0) return 0;
      if (PURE.targetRowNeeded(hops, targetIp, routeState)) return 0;
      return hops[hops.length - 1].index;
    },

    // ----- timeline geometry ------------------------------------------

    /**
     * Timeline band boundaries (ms). Always 100/200 per the PingPlotter
     * Cloud reference.
     */
    bandBoundaries: function () {
      return { green: THRESH_GREEN, yellow: THRESH_YELLOW };
    },

    /**
     * Map epoch seconds -> x pixel position in [0, width] given a visible
     * window [endS - rangeS, endS]. Out-of-range values clamp to the edges
     * so a sample just outside the window still renders on the boundary.
     */
    timeToX: function (t, endS, rangeS, width) {
      if (rangeS <= 0 || width <= 0) return 0;
      var x = ((t - (endS - rangeS)) / rangeS) * width;
      if (x < 0) return 0;
      if (x > width) return width;
      return x;
    },

    /**
     * Inverse of timeToX: x pixel -> epoch seconds.
     */
    xToTime: function (x, endS, rangeS, width) {
      if (width <= 0) return endS;
      return (x / width) * rangeS + (endS - rangeS);
    },

    /**
     * Compute the timeline y-axis maximum (ms) for a given peak latency.
     *
     * Floors at AXIS_FLOOR_MS so the green/yellow/red bands are always
     * visible (a low-latency hop must not collapse the axis). When the peak
     * exceeds the floor, applies ~10% headroom and rounds up to the next 50
     * so the highest sample sits below the top edge and labels stay tidy.
     */
    yAxisMax: function (maxLatency) {
      var m = AXIS_FLOOR_MS;
      if (
        maxLatency !== null &&
        maxLatency !== undefined &&
        !isNaN(maxLatency) &&
        maxLatency > m
      ) {
        m = Math.ceil((maxLatency * 1.1) / 50) * 50;
      }
      return m;
    },

    /**
     * Map latency ms -> y pixel position in [0, height], with 0 ms at the
     * top (y=0) and axisMax at the bottom (y=height). Null/NaN -> bottom.
     */
    msToY: function (ms, axisMax, height) {
      if (axisMax <= 0 || height <= 0) return height;
      if (ms === null || ms === undefined || isNaN(ms) || ms < 0) return height;
      var y = height - (ms / axisMax) * height;
      if (y < 0) return 0;
      if (y > height) return height;
      return y;
    },

    /**
     * Choose an x-axis tick step (seconds) such that ~targetTicks steps fit
     * in rangeS. Steps are drawn from a canonical set so labels stay tidy.
     * Slice D2 extends the canonical set with 2h/4h/6h so 12h/24h ranges
     * yield a readable tick count (otherwise 1h is the max step and 24h
     * would render 24 ticks).
     */
    tickStepS: function (rangeS, targetTicks) {
      var raw = rangeS / Math.max(1, targetTicks);
      var steps = [
        1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600,
        7200, 14400, 21600,
      ];
      for (var i = 0; i < steps.length; i++) {
        if (steps[i] >= raw) return steps[i];
      }
      return steps[steps.length - 1];
    },

    /**
     * Choose a y-axis tick step (ms) such that ~targetTicks steps fit in
     * the visible axis. Same nice-step algorithm.
     */
    yTickStepMs: function (axisMax, targetTicks) {
      var raw = axisMax / Math.max(1, targetTicks);
      var steps = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000];
      for (var i = 0; i < steps.length; i++) {
        if (steps[i] >= raw) return steps[i];
      }
      return steps[steps.length - 1];
    },

    /**
     * Build the list of tick seconds across [startS, endS] at stepS spacing,
     * aligned to epoch multiples of stepS so labels line up to wall-clock
     * minute boundaries.
     */
    xTicks: function (startS, endS, stepS) {
      if (stepS <= 0) return [];
      var first = Math.ceil(startS / stepS) * stepS;
      var out = [];
      for (var t = first; t <= endS; t += stepS) out.push(t);
      return out;
    },

    /**
     * Visible range window for the timeline: [nowS - rangeS, nowS].
     */
    rangeWindow: function (nowS, rangeS) {
      return { startS: nowS - rangeS, endS: nowS };
    },

    /**
     * Lookup the RANGE_OPTIONS entry matching a seconds value. Returns
     * null when the value is not one of the offered ranges. Used by the
     * per-panel range state to validate persisted/selected values, and
     * node-asserted as the per-panel range state mapping.
     */
    rangeOptionFor: function (seconds) {
      if (seconds === null || seconds === undefined || isNaN(seconds)) return null;
      var n = Number(seconds);
      for (var i = 0; i < RANGE_OPTIONS.length; i++) {
        if (RANGE_OPTIONS[i].seconds === n) return RANGE_OPTIONS[i];
      }
      return null;
    },

    /**
     * Step-line RTT path (Slice 6c — PingPlotter Cloud timeline fidelity).
     *
     * Each successful sample's value holds horizontally from its x to the
     * next successful sample's x, then a vertical jump lands on the next
     * sample's y at that sample's x. Lost samples are SKIPPED — they do
     * not contribute a y and do not break the line, so a step "bridges"
     * over a gap exactly the way PingPlotter's timeline does (the lost
     * sample is still painted separately as a red loss mark by the
     * caller).
     *
     * Input: ``samples`` (caller's already-time-filtered visible list,
     * ascending by t, may include null rtt_ms losses); ``endS``/``rangeS``
     * /``width`` for PURE.timeToX; ``axisMax``/``height`` for PURE.msToY.
     *
     * Output: a flat list of ``{x, y, kind}`` points where kind is
     * ``"move"`` (the first successful sample — moveTo), ``"h"`` (a
     * horizontal hold at the previous y to the next sample's x), or
     * ``"v"`` (a vertical jump at the next sample's x to its y). The
     * canvas renderer feeds these to moveTo/lineTo in order. Empty list
     * when there are no successful samples (all-loss / empty input).
     */
    stepPath: function (samples, endS, rangeS, width, axisMax, height) {
      if (!samples || samples.length === 0) return [];
      var ok = [];
      for (var i = 0; i < samples.length; i++) {
        var s = samples[i];
        if (!s || !isFinite(Number(s.t))) continue;
        var r = s.rtt_ms;
        if (r === null || r === undefined || !isFinite(Number(r))) continue;
        ok.push({
          t: Number(s.t),
          x: PURE.timeToX(Number(s.t), endS, rangeS, width),
          y: PURE.msToY(Number(r), axisMax, height),
        });
      }
      if (ok.length === 0) return [];
      // The normal sample buffer is ascending, but keep this helper
      // deterministic if a boundary response supplies samples out of order.
      // Stable ordering also preserves duplicate timestamps.
      ok.sort(function (a, b) { return a.t - b.t; });
      var path = [{ x: ok[0].x, y: ok[0].y, kind: "move" }];
      for (var k = 1; k < ok.length; k++) {
        // Hold the previous value horizontally to the next sample's x,
        // then jump vertically to the next sample's value at that x.
        path.push({ x: ok[k].x, y: ok[k - 1].y, kind: "h" });
        path.push({ x: ok[k].x, y: ok[k].y, kind: "v" });
      }
      return path;
    },

    // ----- Slice 6b: focus window + history helpers (toolbar controls) -----

    /**
     * Map a Focus dropdown value to its window size in seconds.
     * "auto" -> FOCUS_S (the rolling default); numeric strings parse as-is.
     */
    focusWindowSeconds: function (selectValue) {
      if (selectValue === "auto" || selectValue === null || selectValue === undefined) {
        return FOCUS_S;
      }
      var n = parseInt(selectValue, 10);
      if (!isFinite(n) || n <= 0) return FOCUS_S;
      return n;
    },

    /**
     * Human label for a Focus dropdown value (e.g. "Auto", "1 minute").
     */
    focusWindowLabel: function (selectValue) {
      for (var i = 0; i < FOCUS_OPTIONS.length; i++) {
        if (FOCUS_OPTIONS[i].value === selectValue) return FOCUS_OPTIONS[i].label;
      }
      return "Auto";
    },

    /**
     * Prepend ``target`` to ``history``, dedupe, and cap at TARGET_HISTORY_MAX.
     * Returns a fresh array; ``history`` is not mutated. Empty/whitespace
     * targets are ignored (no-op return of a cloned history).
     */
    targetHistoryAdd: function (history, target) {
      var t = String(target == null ? "" : target).trim();
      var base = Array.isArray(history) ? history.slice() : [];
      if (!t) return base;
      var out = base.filter(function (h) { return h !== t; });
      out.unshift(t);
      return out.slice(0, TARGET_HISTORY_MAX);
    },

    /**
     * Format epoch seconds as a 12-hour wall clock with AM/PM, matching the
     * PingPlotter Cloud reference caption (e.g. "5:46:23 PM").
     */
    formatClock12: function (s) {
      if (s === null || s === undefined || typeof s !== "number" || isNaN(s)) {
        return "--:--:-- --";
      }
      var d = new Date(s * 1000);
      var h = d.getHours();
      var m = String(d.getMinutes()).padStart(2, "0");
      var sec = String(d.getSeconds()).padStart(2, "0");
      var am = h < 12;
      var h12 = h % 12;
      if (h12 === 0) h12 = 12;
      return h12 + ":" + m + ":" + sec + " " + (am ? "AM" : "PM");
    },

    /**
     * Format epoch seconds as M/D/YYYY (no leading zeros), matching the
     * PingPlotter Cloud reference caption (e.g. "6/10/2021").
     */
    formatDateMDY: function (s) {
      if (s === null || s === undefined || typeof s !== "number" || isNaN(s)) {
        return "--/--/----";
      }
      var d = new Date(s * 1000);
      return (d.getMonth() + 1) + "/" + d.getDate() + "/" + d.getFullYear();
    },

    /** Epoch seconds -> "2026-06-10 17:36:23 UTC"; copied text labels the zone. */
    formatUtcStamp: function (s) {
      if (s === null || s === undefined || typeof s !== "number" || isNaN(s)) {
        return "—";
      }
      var d = new Date(s * 1000);
      function p(n) { return String(n).padStart(2, "0"); }
      return d.getUTCFullYear() + "-" + p(d.getUTCMonth() + 1) + "-" + p(d.getUTCDate()) +
        " " + p(d.getUTCHours()) + ":" + p(d.getUTCMinutes()) + ":" +
        p(d.getUTCSeconds()) + " UTC";
    },

    /**
     * Build the Focus caption line shown under the toolbar:
     *   "Focus: <windowLabel>, <date>, <startTime> - <endTime>"
     * When both bounds land on the same calendar date, the date is shown
     * once before the time range (matching the PingPlotter Cloud reference).
     * Null bounds yield "Focus: —".
     */
    formatFocusCaption: function (windowLabel, startS, endS) {
      if (startS === null || startS === undefined || endS === null || endS === undefined ||
          isNaN(startS) || isNaN(endS)) {
        return "Focus: " + (windowLabel || "—");
      }
      var label = windowLabel == null ? "—" : windowLabel;
      var startDate = PURE.formatDateMDY(startS);
      var endDate = PURE.formatDateMDY(endS);
      var startTime = PURE.formatClock12(startS);
      var endTime = PURE.formatClock12(endS);
      if (startDate === endDate) {
        return "Focus: " + label + ", " + startDate + ", " + startTime + " - " + endTime;
      }
      return "Focus: " + label + ", " + startDate + ", " + startTime + " - " + endDate + ", " + endTime;
    },

    /**
     * Snapshot baseline aging has no client-side expiry timer: validity
     * derives from the adopted payload's own window extrema (see
     * PURE.statsForHopWindow). The aggregate stays reusable only while its
     * OLDEST sample (``sample_from_t``) is still at/after the selected
     * window start. Paused traces freeze ``captured_at`` at the pause
     * instant and stop producing samples, so a paused baseline's oldest
     * sample never crosses the frozen window start and stays valid.
     */

    FOCUS_S: FOCUS_S,
    FOCUS_OPTIONS: FOCUS_OPTIONS,
    TARGET_HISTORY_MAX: TARGET_HISTORY_MAX,
    THRESH_GREEN: THRESH_GREEN,
    THRESH_YELLOW: THRESH_YELLOW,
    AXIS_FLOOR_MS: AXIS_FLOOR_MS,
    LEGEND_FLOOR_MS: LEGEND_FLOOR_MS,
    RANGES_S: RANGES_S,
    RANGE_OPTIONS: RANGE_OPTIONS,
    DEFAULT_RANGE_S: DEFAULT_RANGE_S,
    MAX_RANGE_S: MAX_RANGE_S,
    RAW_RANGE_MAX_S: RAW_RANGE_MAX_S,
    RAW_CAP: RAW_CAP,
    RETENTION_S: RETENTION_S,
  };

  // ----- Slice D1: timeline history merge + multi-hour formatting --------
  //
  // PURE.mergeSamples is the pure, node-testable merge used by
  // applySnapshot / hydrateHopBaselines / the history-fetch path to combine
  // a snapshot tail or a /history payload with the live per-hop buffer
  // WITHOUT blanking timelines (the old code filtered local
  // samples at/below the snapshot watermark but never merged the snapshot's
  // own samples back in). The merge is a sorted union with exact-timestamp
  // dedup (incoming wins on conflict), idempotent, and bounded by ``cap``.
  // Watermark / seedStats bookkeeping stays SEPARATE from timeline storage
  // because mergeSamples only touches sample arrays.

  /**
   * Merge two sample arrays into a new array, ascending by ``t``,
   * deduplicated by exact ``t`` (incoming wins on conflict), optionally
   * capped to the most recent ``cap`` entries.
   *
   * @param {Array<{t:number,rtt_ms:number|null}>|null} existing
   * @param {Array<{t:number,rtt_ms:number|null}>|null} incoming
   * @param {number|null} cap  optional max length; when exceeded the oldest
   *   entries are dropped (the most recent ``cap`` survive)
   * @returns {Array<{t:number,rtt_ms:number|null}>} new sorted, deduped array
   *
   * Properties pinned by node tests:
   *  - Idempotent: merge(merge(a, b), b) === merge(a, b).
   *  - Conflict resolution: on an exact ``t`` match, ``incoming``'s rtt_ms
   *    wins (so a re-fetched /history payload is authoritative over a stale
   *    local copy of the same timestamp).
   *  - Ordering: result is always ascending by ``t``.
   *  - Cap: when set and the merged length exceeds it, the OLDEST entries
   *    (lowest ``t``) are dropped so the most recent survive.
   *  - Non-finite / malformed samples (missing or NaN ``t``) are dropped.
   *  - Neither input array is mutated; fresh sample objects are returned.
   */
  PURE.mergeSamples = function (existing, incoming, cap) {
    if (!Array.isArray(existing)) existing = [];
    if (!Array.isArray(incoming)) incoming = [];
    var map = new Map();
    function add(arr) {
      for (var i = 0; i < arr.length; i++) {
        var s = arr[i];
        if (!s || typeof s.t !== "number" || !isFinite(s.t)) continue;
        // Fresh copy so the caller's array objects are never aliased into
        // the result (callers may continue to mutate their own arrays).
        map.set(s.t, { t: s.t, rtt_ms: s.rtt_ms });
      }
    }
    add(existing);
    add(incoming); // incoming overwrites on exact-t collision
    var merged = Array.from(map.values());
    merged.sort(function (a, b) { return a.t - b.t; });
    if (cap && merged.length > cap) {
      merged = merged.slice(merged.length - cap);
    }
    return merged;
  };

  /**
   * Format an epoch-seconds value as HH:MM in local time (24-hour, 2-digit).
   * Used for multi-hour timeline tick labels where HH:MM:SS would be too
   * dense. Mirrors formatClock's null/NaN handling.
   */
  PURE.formatClockHM = function (s) {
    if (s === null || s === undefined || typeof s !== "number" || isNaN(s)) {
      return "--:--";
    }
    var d = new Date(s * 1000);
    return String(d.getHours()).padStart(2, "0") + ":" +
           String(d.getMinutes()).padStart(2, "0");
  };

  /**
   * True when the visible window ``[startS, endS]`` crosses local midnight
   * (the two bounds fall on different calendar dates). Used by the timeline
   * x-axis renderer to decide whether to draw a date-boundary marker. Pure
   * so it is node-assertable alongside the rest of the formatting helpers.
   */
  PURE.crossesMidnight = function (startS, endS) {
    if (typeof startS !== "number" || typeof endS !== "number" ||
        !isFinite(startS) || !isFinite(endS)) {
      return false;
    }
    var ds = new Date(startS * 1000);
    var de = new Date(endS * 1000);
    return ds.getFullYear() !== de.getFullYear() ||
           ds.getMonth() !== de.getMonth() ||
           ds.getDate() !== de.getDate();
  };

  // ----- Slice D2: server rollup tier — bucket merge + drawable mapping ----
  //
  // PURE.mergeBuckets and PURE.bucketsToDrawablePoints are the pure, DOM-free
  // helpers used by the bucket-fetch path and the bucketed-timeline renderer.
  // Both are node-testable alongside the rest of PURE.
  //
  // Bucket shape (server /history?resolution=1m response):
  //   {t: <bucket start epoch s>, avg_ms, min_ms, max_ms, count, loss_count}
  // A full-loss bucket has avg_ms/min_ms/max_ms = null. Empty buckets
  // (paused gaps, pre-start) are NEVER present in the server response
  // (sparse), so the client sees only buckets with at least one sample.

  /**
   * Merge two bucket arrays into a new array, ascending by ``t``,
   * deduplicated by exact ``t`` (incoming wins on conflict), optionally
   * capped to the most recent ``cap`` entries. Mirrors mergeSamples so the
   * bucket-fetch path is idempotent over overlapping windows (a re-fetch
   * of the same window produces the same buffer).
   *
   * @param {Array<{t:number,...}>|null} existing
   * @param {Array<{t:number,...}>|null} incoming
   * @param {number|null} cap  optional max length; when exceeded the OLDEST
   *   buckets (lowest ``t``) are dropped
   * @returns {Array<{t:number,...}>} new sorted, deduped array
   *
   * Properties pinned by node tests:
   *  - Idempotent: merge(merge(a, b), b) === merge(a, b).
   *  - Conflict resolution: on exact ``t`` match, ``incoming`` wins.
   *  - Ordering: result ascending by ``t``.
   *  - Cap: when set and length exceeds, OLDEST (lowest t) are dropped.
   *  - Non-finite / malformed buckets (missing or NaN ``t``) are dropped.
   *  - Neither input is mutated; fresh bucket objects are returned.
   */
  PURE.mergeBuckets = function (existing, incoming, cap) {
    if (!Array.isArray(existing)) existing = [];
    if (!Array.isArray(incoming)) incoming = [];
    var map = new Map();
    function add(arr) {
      for (var i = 0; i < arr.length; i++) {
        var b = arr[i];
        if (!b || typeof b.t !== "number" || !isFinite(b.t)) continue;
        // Fresh copy so the caller's objects are never aliased in.
        map.set(b.t, {
          t: b.t,
          avg_ms: b.avg_ms,
          min_ms: b.min_ms,
          max_ms: b.max_ms,
          count: b.count,
          loss_count: b.loss_count,
        });
      }
    }
    add(existing);
    add(incoming); // incoming overwrites on exact-t collision
    var merged = Array.from(map.values());
    merged.sort(function (a, b) { return a.t - b.t; });
    if (cap && merged.length > cap) {
      merged = merged.slice(merged.length - cap);
    }
    return merged;
  };

  /**
   * Map rollup buckets to drawable timeline geometry: a step-line of
   * ``avg_ms`` (bridging over omitted/empty buckets as the raw path bridges
   * over losses) plus per-bucket loss marks. Pure / DOM-free so the
   * mapping is node-assertable.
   *
   * Input: ``buckets`` (caller-filtered to the visible window, ascending by
   * ``t``, sparse — gaps are omitted buckets, not zero buckets);
   * ``endS``/``rangeS``/``width`` for PURE.timeToX; ``axisMax``/``height``
   * for PURE.msToY.
   *
   * Output: ``{line, lossMarks}`` where:
   *  - ``line`` is a flat list of ``{x, y, kind}`` points (kind "move" for
   *    the first successful bucket, then "h" + "v" pairs in step-plot
   *    order — matches PURE.stepPath so the renderer can share its
   *    moveTo/lineTo loop). Empty when there are no successful buckets.
   *  - ``lossMarks`` is a list of ``{x, t, fullLoss}`` for every bucket
   *    with ``loss_count > 0``. ``fullLoss`` is true for buckets where
   *    every sample was a loss (no line point is emitted for them); false
   *    for partial-loss buckets (the line still passes through them).
   *
   * A bucket's loss mark is drawn at the bucket's START x (``t``), matching
   * the bucket-start semantics (the loss happened somewhere in the
   * bucket's window; the start is the canonical anchor).
   */
  PURE.bucketsToDrawablePoints = function (
    buckets, endS, rangeS, width, axisMax, height
  ) {
    if (!buckets || buckets.length === 0) return { line: [], lossMarks: [] };
    // Build the step-line points over SUCCESSFUL buckets only (avg_ms not
    // null). Full-loss buckets contribute no line point — the line bridges
    // over them to the next successful bucket, exactly as the raw path
    // bridges over individual lost samples.
    var ok = [];
    var lossMarks = [];
    for (var i = 0; i < buckets.length; i++) {
      var b = buckets[i];
      if (!b || typeof b.t !== "number" || !isFinite(b.t)) continue;
      var x = PURE.timeToX(b.t, endS, rangeS, width);
      if (b.loss_count && b.loss_count > 0) {
        var fullLoss = b.avg_ms === null || b.avg_ms === undefined ||
                       isNaN(b.avg_ms);
        lossMarks.push({ x: x, t: b.t, fullLoss: fullLoss });
      }
      if (b.avg_ms === null || b.avg_ms === undefined || isNaN(b.avg_ms)) {
        continue; // full-loss bucket: no line point
      }
      ok.push({
        t: b.t,
        x: x,
        y: PURE.msToY(b.avg_ms, axisMax, height),
      });
    }
    if (ok.length === 0) return { line: [], lossMarks: lossMarks };
    // Deterministic order (the input is already ascending, but a sparse
    // server response with duplicate t or an out-of-order tail should not
    // corrupt the path).
    ok.sort(function (a, b) { return a.t - b.t; });
    var line = [{ x: ok[0].x, y: ok[0].y, kind: "move" }];
    for (var k = 1; k < ok.length; k++) {
      line.push({ x: ok[k].x, y: ok[k - 1].y, kind: "h" });
      line.push({ x: ok[k].x, y: ok[k].y, kind: "v" });
    }
    return { line: line, lossMarks: lossMarks };
  };

  /**
   * Decide which ``resolution`` query param to send for a given range.
   * Ranges ≤ RAW_RANGE_MAX_S keep D1 raw behaviour (no resolution param —
   * the server defaults to raw); ranges > RAW_RANGE_MAX_S use ``auto`` so
   * the server picks raw when the window fits raw retention and 1m
   * otherwise (12h/24h windows always exceed raw retention at any sane
   * interval, so auto resolves to 1m in practice). Pure so it is
   * node-assertable alongside the rest of the range helpers.
   */
  PURE.resolutionForRange = function (rangeS) {
    if (typeof rangeS !== "number" || !isFinite(rangeS)) return "raw";
    return rangeS > RAW_RANGE_MAX_S ? "auto" : "raw";
  };

  // ``auto`` can legitimately return raw for a slow-interval session, but
  // long panels still require a bucket response for their separate renderer.
  PURE.bucketFallbackNeeded = function (rangeS, actualResolution) {
    return rangeS > RAW_RANGE_MAX_S && actualResolution === "raw";
  };

  PURE.pushSample = function (sampleState, hopIndex, t, rttMs) {
    var key = String(hopIndex);
    // A snapshot is the baseline. SSE can deliver events that were already
    // included in that snapshot while the connection is being attached.
    if (sampleState.seedWatermark[key] !== undefined && t <= sampleState.seedWatermark[key]) return;
    var list = sampleState.samples[key];
    if (!list) {
      list = [];
      sampleState.samples[key] = list;
    }
    list.push({ t: t, rtt_ms: rttMs });
    // Drop samples outside the retention window (time-based) AND cap the
    // absolute count at RAW_CAP (mirrors the server's SAMPLE_CAP) so the
    // buffer stays bounded regardless of interval. The time cutoff covers
    // the largest offered range (3h) so multi-hour panels have continuous
    // coverage; the count cap defends a 1 s interval over that window.
    var cutoff = t - RETENTION_S;
    while (list.length > 0 && list[0].t < cutoff) list.shift();
    while (list.length > RAW_CAP) list.shift();
  };

  // Keep expiry logic in the same time domain as sample retention.  This is
  // also used by the timer path, where no new round may arrive to trigger a
  // table render.
  PURE.focusExpired = function (focus, nowS) {
    return !!focus && focus.endS < nowS - RETENTION_S;
  };

  /**
   * The stats-window width the server actually used for a snapshot payload:
   * the echoed ``focus_s``, preserved EXACTLY. 0 is a real echo — the
   * HTTP-only "all retained" window (the Focus dropdown offers no zero
   * option, but snapshot requests do) — and must stay distinct from every
   * positive width: a baseline computed over all retained samples matches
   * only a zero-width query downstream (``seedFocus`` equality in
   * PURE.statsForHopWindow) and is suspended for the selected UI window.
   * A missing or malformed echo (non-number, NaN/±Infinity, negative) is
   * NEVER substituted with the currently selected window: it returns null
   * so adoption rejects the baseline outright instead of mislabeling the
   * aggregate's window.
   */
  PURE.snapshotFocusSeconds = function (snap) {
    var f = snap && snap.focus_s;
    return typeof f === "number" && isFinite(f) && f >= 0 ? f : null;
  };

  /**
   * Adopt one server snapshot baseline into per-stream accounting state.
   *
   * ``key`` is the stream's hop key ("0" for the target stream, the route
   * hop index otherwise). ``stats`` is the snapshot's window aggregate for
   * that stream; ``capS`` is the payload's ``captured_at`` — the single
   * effective epoch reference the server captured the whole snapshot at
   * (running captures use the server's now; a paused trace freezes it at
   * ``paused_at``, so every paused snapshot carries the same value).
   * ``throughT`` is the payload's ``sample_through_t`` and ``fromT`` its
   * ``sample_from_t`` — the latest and oldest committed sample timestamps
   * inside the SAME immutable stats window that produced ``stats`` (both
   * null for an empty stream). ``focusS`` is the stats-window width the
   * server actually used (the echoed ``focus_s``, preserved exactly — 0 is
   * the HTTP-only "all retained" window, never rewritten to the selected
   * width).
   *
   * Returns true when the payload was adopted (including the explicit
   * empty-stream clear), false when it was rejected.
   *
   * Rejection rules:
   *  - A payload without a finite ``captured_at`` is unusable: baseline
   *    aging must never fall back to the client receipt time or the latest
   *    local sample.
   *  - A payload without a finite, non-negative ``focusS`` is unusable:
   *    the echoed width identifies the window the aggregate was computed
   *    over, and substituting the selected width would mislabel it.
   *  - A NONEMPTY payload without a finite ``sample_from_t`` is unusable:
   *    validity is bounded by the aggregate's oldest sample, and inventing
   *    it client-side (e.g. from the theoretical window start) would
   *    fabricate coverage the server never reported.
   *  - A strictly OLDER ``captured_at`` than the latest snapshot seen for
   *    the stream is stale (an older snapshot response arriving after a
   *    newer one): the newer baseline and the live samples pushed past its
   *    cutoff stay untouched. Equal ``captured_at`` values adopt (paused
   *    snapshots and same-instant top-level/per-hop fetches).
   *
   * An EMPTY stream (count 0 or null cutoff) clears the stream's baseline,
   * cutoff, oldest-sample anchor, and watermark outright instead of leaving
   * a stale aggregate behind — but RETAINS the capture marker (``seedCap``
   * moves to this payload's ``captured_at``): adoption is monotonic in the
   * latest snapshot seen per stream, so an older nonempty payload arriving
   * after a newer EMPTY one cannot resurrect a stale aggregate. Equal
   * frozen paused captures still adopt. A non-empty payload records the
   * strict-after delta cutoff (``seedCut`` = ``sample_through_t``), the
   * aging anchor (``seedFrom`` = ``sample_from_t``), the capture marker
   * (``seedCap``), the window width the aggregate was computed with
   * (``seedFocus``), and the SSE dedup watermark (``seedWatermark`` =
   * ``sample_through_t``).
   *
   * Aging consumes ``sample_from_t`` (not ``captured_at`` + width): the
   * aggregate counts exactly the window's [from .. through] samples, so it
   * is reusable in a later selected window only while its oldest sample is
   * still at/after that window's start (see PURE.statsForHopWindow).
   */
  PURE.adoptSnapshotBaseline = function (sampleState, key, stats, capS, throughT, fromT, focusS) {
    var st = sampleState || {};
    if (typeof capS !== "number" || !isFinite(capS)) return false;
    // The echoed window width is part of the payload's identity. A missing
    // or malformed ``focusS`` cannot be substituted with the selected
    // window (the aggregate would be mislabeled), so the payload is
    // rejected outright — including an EMPTY payload, whose clear may only
    // apply to the width it was actually fetched for. Zero is valid and
    // distinct (the all-retained window); negative widths are invalid.
    if (typeof focusS !== "number" || !isFinite(focusS) || focusS < 0) return false;
    var prevCap = (st.seedCap || {})[key];
    if (typeof prevCap === "number" && isFinite(prevCap) && capS < prevCap) return false;
    if (!stats || !(stats.count > 0) ||
        typeof throughT !== "number" || !isFinite(throughT)) {
      ["seedStats", "seedCut", "seedFrom", "seedFocus", "seedWatermark"]
        .forEach(function (m) { if (st[m]) delete st[m][key]; });
      st.seedCap = st.seedCap || {};
      st.seedCap[key] = capS;
      return true;
    }
    // A nonempty payload without its oldest-sample timestamp cannot be aged
    // correctly (validity is bounded by sample_from_t, not the theoretical
    // window start): reject it rather than invent coverage client-side.
    if (typeof fromT !== "number" || !isFinite(fromT)) return false;
    st.seedStats = st.seedStats || {};
    st.seedCut = st.seedCut || {};
    st.seedCap = st.seedCap || {};
    st.seedFrom = st.seedFrom || {};
    st.seedFocus = st.seedFocus || {};
    st.seedWatermark = st.seedWatermark || {};
    st.seedStats[key] = stats;
    st.seedCut[key] = throughT;
    st.seedCap[key] = capS;
    st.seedFrom[key] = fromT;
    st.seedFocus[key] = focusS;
    st.seedWatermark[key] = throughT;
    return true;
  };

  /**
   * Record that the client's raw sample buffer for ``key`` is a complete
   * census of the server's samples from ``startS`` forward (a stats-window
   * history backfill was adopted; live SSE keeps it current until the
   * transport drops, which resets coverage). Coverage only ever extends
   * earlier: the minimum of the adopted window starts.
   */
  PURE.extendCoverage = function (sampleState, key, startS) {
    var st = sampleState || {};
    st.covFrom = st.covFrom || {};
    var cov = st.covFrom[key];
    st.covFrom[key] = typeof cov === "number" && cov < startS ? cov : startS;
    return st.covFrom[key];
  };

  /**
   * Release a coverage backfill's in-flight marker ONLY when the completing
   * request still owns it. Every coverage request stores a unique ownership
   * token in ``covInFlight[key]`` when it starts; a stale request's
   * completion (its token wiped by a transport break/reconnect/reset, or
   * replaced by a newer generation's backfill for the same stream) must not
   * release the newer request's marker — otherwise an old focus/reset
   * response could unlock a duplicate backfill while the current one is
   * still outstanding. Returns true when the caller owned and released the
   * marker.
   */
  PURE.releaseCoverageInFlight = function (sampleState, key, token) {
    var st = sampleState || {};
    if (!st.covInFlight || st.covInFlight[key] !== token) return false;
    st.covInFlight[key] = false;
    return true;
  };

  /**
   * Advance the per-tab connection-continuity generation and drop every
   * raw-coverage claim immediately. Called when the SSE transport breaks
   * (error) and again when it reconnects (ready): samples arriving while
   * the stream is down are missed, so no completeness claim may survive
   * the break, and in-flight backfills issued before the break must be
   * fenced from re-establishing coverage across the gap. Wiping
   * ``covInFlight`` also strips the old requests' ownership: their
   * completions release nothing, so they can never unlock or clobber a
   * newer generation's backfill. Returns the new generation.
   */
  PURE.breakCoverageContinuity = function (sampleState) {
    var st = sampleState || {};
    st.covFrom = {};
    st.covInFlight = {};
    st.covConnected = false;
    st.covGen = (typeof st.covGen === "number" ? st.covGen : 0) + 1;
    return st.covGen;
  };

  /**
   * Extend coverage ONLY while the SSE transport is connected and the
   * completing backfill belongs to the current connection generation.
   * A request issued during an outage cannot prove ongoing coverage.
   * A response that crossed a transport break/reconnect may still merge raw
   * rows upstream (the merge is idempotent and safe), but it must NEVER
   * establish completeness: its fetched window cannot include samples the
   * server recorded while the stream was down, so a claim from it would
   * present a gap as full coverage. Returns the (possibly unchanged) claim.
   */
  PURE.extendCoverageIfCurrent = function (sampleState, key, startS, capturedGen, currentGen) {
    if (!sampleState || !sampleState.covConnected || capturedGen !== currentGen) {
      return ((sampleState || {}).covFrom || {})[key];
    }
    return PURE.extendCoverage(sampleState, key, startS);
  };

  PURE.statsForHopWindow = function (sampleState, hopIndex, nowS, focusS) {
    var key = String(hopIndex);
    var list = (sampleState.samples || {})[key] || [];
    var startS = nowS - focusS;
    var cov = (sampleState.covFrom || {})[key];
    // 1. Hydrated window: a stats-window backfill plus live SSE make the raw
    //    buffer the complete, deduplicated census of the selected window, so
    //    the unique raw samples are the SOLE statistics source. The snapshot
    //    aggregate is retired the moment coverage is complete — it is a
    //    loading baseline, never an indefinitely renewable total.
    if (typeof cov === "number" && cov <= startS) {
      var raw = PURE.computeStatsWindow(list, nowS, focusS);
      return {
        source: "live",
        data: raw || { count: 0, avg: null, min: null, max: null, cur: null, lossPct: 0 },
      };
    }
    // 2. Loading baseline: the server snapshot aggregate for the stream plus
    //    buffered samples STRICTLY AFTER its matching cutoff (the payload's
    //    sample_through_t, captured from the same immutable read as the
    //    aggregate). The two parts are disjoint by construction — the buffer
    //    also carries the merged snapshot tail for graphs, which must never
    //    be counted twice. Validity is bounded by the aggregate's OLDEST
    //    sample: it counts exactly the window's [sample_from_t ..
    //    sample_through_t] samples, so it remains a complete census of the
    //    selected window only while that oldest sample is still at/after the
    //    window start (nowS <= from + focusS — inclusive: a sample exactly
    //    on the start boundary is counted). Mere window overlap is NOT
    //    enough: once the oldest sample crosses the start, the aggregate
    //    would overcount and is retired; the raw buffer (retained for
    //    graphs) becomes the deliberately-partial source until refreshed
    //    coverage lands. focusS 0 degrades cleanly: the single-instant
    //    window only admits a baseline whose oldest sample IS the instant.
    //    A baseline fetched for a different window width (echoed focus_s)
    //    is suspended until the refetched snapshot for the new window lands.
    var seed = (sampleState.seedStats || {})[key];
    var cut = (sampleState.seedCut || {})[key];
    var from = (sampleState.seedFrom || {})[key];
    var seedFocus = (sampleState.seedFocus || {})[key];
    var baselineLive =
      seed && typeof cut === "number" && isFinite(cut) &&
      typeof from === "number" && isFinite(from) &&
      seedFocus === focusS && nowS <= from + focusS;
    if (baselineLive) {
      var delta = [];
      for (var i = 0; i < list.length; i++) {
        var s = list[i];
        if (s.t > cut && s.t >= startS) delta.push(s);
      }
      if (delta.length > 0) {
        var live = PURE._statsFromWindow(delta);
        var seedSuccess = seed.count * (1 - (seed.loss_pct || 0) / 100);
        var liveSuccess = live.count * (1 - live.lossPct / 100);
        var totalSuccess = seedSuccess + liveSuccess;
        // Snapshot stats historically ship without max_ms. Whisker rendering
        // needs min AND max, so take whichever seed value is present (future
        // snapshots) and merge with the live max; if both are absent, max
        // stays null and the renderer degrades to markers-only (no whisker).
        var seedMax = seed.max_ms;
        var combinedMax;
        if (seedMax !== null && seedMax !== undefined && !isNaN(seedMax) &&
            live.max !== null && live.max !== undefined && !isNaN(live.max)) {
          combinedMax = Math.max(seedMax, live.max);
        } else if (seedMax !== null && seedMax !== undefined && !isNaN(seedMax)) {
          combinedMax = seedMax;
        } else {
          combinedMax = live.max; // may be null when only losses arrived
        }
        return { source: "combined", data: {
          count: seed.count + live.count,
          avg: totalSuccess ? ((seed.avg_ms || 0) * seedSuccess + (live.avg || 0) * liveSuccess) / totalSuccess : null,
          min: seed.min_ms === null || seed.min_ms === undefined ? live.min : (live.min === null ? seed.min_ms : Math.min(seed.min_ms, live.min)),
          max: combinedMax,
          cur: live.cur,
          lossPct: (seed.count + live.count) ? ((seed.count * (seed.loss_pct || 0) / 100 + live.count * live.lossPct / 100) / (seed.count + live.count)) * 100 : 0,
        }};
      }
      // Valid baseline, nothing after its cutoff yet: the baseline alone.
      return { source: "seed", data: {
        count: seed.count,
        avg: seed.avg_ms,
        min: seed.min_ms,
        max: seed.max_ms !== undefined ? seed.max_ms : null,
        cur: seed.cur_ms,
        lossPct: seed.loss_pct,
      }};
    }
    // 3. No hydrated coverage and no valid baseline: the raw window alone.
    //    Deliberately partial (the buffer holds only what this client has
    //    seen) rather than a stale aggregate presented as the selected
    //    window; the next snapshot/backfill refreshes coverage.
    var liveOnly = PURE.computeStatsWindow(list, nowS, focusS);
    if (liveOnly && liveOnly.count > 0) {
      return { source: "live", data: liveOnly };
    }
    return null;
  };

  /**
   * Authoritative primary-stream facts for the footer: the SAME stats object
   * the table and copied summary show for the hop (manual drag-focus wins,
   * then the Focus window), and the last committed sample time from the
   * stream's raw buffer — buffer-derived so a restored tab shows the
   * existing history's recency instead of a blank until the next live round.
   */
  PURE.primaryStreamFacts = function (sampleState, hopIndex, nowS, focusS) {
    var st = sampleState || {};
    var stats = PURE.displayStatsForHop(st, hopIndex, nowS, focusS);
    var list = (st.samples || {})[String(hopIndex)] || [];
    var lastT = list.length > 0 ? list[list.length - 1].t : null;
    return {
      stats: stats ? stats.data : null,
      lastT: typeof lastT === "number" && isFinite(lastT) ? lastT : null,
    };
  };

  PURE.statsForHop = function (sampleState, hopIndex, nowS) {
    return PURE.statsForHopWindow(sampleState, hopIndex, nowS, FOCUS_S);
  };

  // Stats exactly as the table shows them (a manual drag-focus window overrides
  // the rolling window); shared by the table and the copied summary.
  PURE.displayStatsForHop = function (sampleState, hopIndex, nowS, focusS) {
    var focus = sampleState && sampleState.focus;
    if (focus) {
      var list = (sampleState.samples || {})[String(hopIndex)] || [];
      var d = PURE.computeStatsRange(list, focus.startS, focus.endS);
      if (d.count === 0) return null;
      return { source: "focus", data: d };
    }
    return PURE.statsForHopWindow(sampleState || {}, hopIndex, nowS, focusS);
  };

  // ----- Slice 7 (multi-trace tabs): pure tab-state helpers -----------
  //
  // The dashboard now supports multiple simultaneous traces (one per tab).
  // Tab-state transitions that are pure (no DOM, no I/O) live here so they
  // can be unit-tested under node just like the rest of PURE.

  /**
   * Normalise a free-text target into a short tab label. Strips scheme,
   * ``www.``, path, query, fragment, and port so e.g.
   * ``https://www.example.com:443/x`` and ``example.com`` produce the same
   * label. Falls back to ``"New trace"`` for empty/whitespace input (the
   * fresh-idle-tab label).
   *
   * IPv6 literals are preserved intact: bracketed (``[2001:db8::1]:443``)
   * and bare (``2001:db8::1``) forms both reduce to the bare address, since
   * a naive ``:port`` strip would eat the colons in the address itself.
   */
  PURE.tabLabel = function (target) {
    var t = String(target == null ? "" : target).trim();
    if (!t) return "New trace";
    // Strip a leading scheme.
    t = t.replace(/^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//, "");
    // Take the host part before the first /, ?, or #.
    t = t.split(/[\/?#]/)[0];
    // Bracketed IPv6 literal: ``[addr]:port`` or ``[addr]``.
    if (t.charAt(0) === "[") {
      var end = t.indexOf("]");
      if (end > 0) return t.slice(1, end);
    }
    // Count colons: bare IPv6 has 2+ colons; host[:port] has at most one.
    var colonCount = 0;
    for (var i = 0; i < t.length; i++) if (t.charAt(i) === ":") colonCount++;
    if (colonCount <= 1) {
      // host[:port] — strip the port if present.
      t = t.replace(/:\d+$/, "");
      // Drop a leading "www.".
      if (t.toLowerCase().indexOf("www.") === 0) t = t.slice(4);
    }
    return t || "New trace";
  };

  /**
   * Resolve the display label for a tab from its session facts. Prefers
   * the user-set ``name``; falls back to the target-derived label via
   * :func:`PURE.tabLabel`. ``name`` is normalized here (trimmed; an empty
   * result falls through) so the server's stored name and the in-flight
   * POST value produce the same label without a second normalization site.
   *
   * Pure so the tab-bar render and node assertions share one definition.
   */
  PURE.tabDisplayLabel = function (name, target) {
    // Only a real non-empty string name overrides the target-derived label.
    // A non-string ``name`` (number, object, etc.) is treated as missing so
    // garbage never displaces the target label in the tab bar.
    var n = typeof name === "string" ? name.trim() : "";
    if (n) return n;
    return PURE.tabLabel(target);
  };

  /**
   * Return the persistent-collection marker glyph for the tab bar, or the
   * empty string for a non-persistent tab. Pure so node tests can pin the
   * marker character (a future restyle changes one constant, not a
   * render-time string built in three places).
   */
  PURE.tabMarker = function (persistent) {
    return persistent ? "⇄" : "";
  };

  // ----- Internal-host DNS masking (screenshot mode) --------------------
  //
  // The toolbar's "Mask internal DNS" toggle is DISPLAY-ONLY: when on, DNS
  // names belonging to hops/targets the SERVER classified as internal
  // (settings CIDR ranges, classified with stdlib ipaddress — no JS CIDR
  // parser) are replaced with a neutral alias everywhere they are shown.
  // IP addresses are never masked; API/CSV payloads are untouched.

  /** Neutral alias shown in place of an internal host's DNS name. */
  PURE.INTERNAL_HOST_ALIAS = "Internal host";

  /** Default internal ranges (mirrors the server's RFC1918 + RFC4193
   * fc00::/7 ULA default). */
  PURE.DEFAULT_INTERNAL_CIDRS = [
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "fc00::/7",
  ];

  /**
   * Best-effort "is this string an IP literal" check (IPv4 dotted quad or
   * anything containing a colon — IPv6). Used only to decide that a value
   * is an ADDRESS (never masked); it is not a CIDR parser and never decides
   * internal classification (that is server-side).
   */
  PURE.isIpLiteralText = function (s) {
    var t = String(s == null ? "" : s).trim();
    if (!t) return false;
    if (t.indexOf(":") !== -1) return true; // IPv6 literal
    var parts = t.split(".");
    if (parts.length !== 4) return false;
    for (var i = 0; i < parts.length; i++) {
      if (!/^\d{1,3}$/.test(parts[i])) return false;
      if (Number(parts[i]) > 255) return false;
    }
    return true;
  };

  /**
   * Mask one DNS name for screenshots: when ``maskOn`` and the owner is
   * internal, replace the name with the neutral alias. IP literals and
   * empty/missing names pass through unchanged (IPs are never masked).
   */
  PURE.maskDnsName = function (name, isInternal, maskOn) {
    if (!maskOn || !isInternal) return name;
    if (name === null || name === undefined || name === "") return name;
    if (PURE.isIpLiteralText(name)) return name;
    return PURE.INTERNAL_HOST_ALIAS;
  };

  /**
   * Collect the KNOWN internal names of one session that must not appear
   * on screen while masking is on: the raw user target, the
   * target-derived label (scheme/port stripped), and the server-resolved
   * target DNS. Empty and IP-literal candidates are dropped (IPs are
   * never masked); an unclassified/external target yields an empty list,
   * so nothing is masked before the server has classified it.
   */
  PURE.sessionMaskNames = function (target, targetDns, isInternal) {
    var out = [];
    if (!isInternal) return out;
    var raw = String(target == null ? "" : target).trim();
    // An empty target must yield no names: PURE.tabLabel falls back to its
    // "New trace" placeholder, which is not a host and must never mask.
    if (!raw) {
      // The resolved DNS can still be known without the raw target.
      var d0 = String(targetDns == null ? "" : targetDns).trim();
      if (d0 && !PURE.isIpLiteralText(d0)) out.push(d0);
      return out;
    }
    var cands = [target, targetDns, PURE.tabLabel(raw)];
    for (var i = 0; i < cands.length; i++) {
      var k = String(cands[i] == null ? "" : cands[i]).trim();
      if (!k || PURE.isIpLiteralText(k)) continue;
      if (out.indexOf(k) === -1) out.push(k);
    }
    return out;
  };

  /**
   * Mask a whole display label (the target input's value, a tab label)
   * for screenshots: when ``maskOn`` and the label equals or CONTAINS a
   * known internal name, the ENTIRE label becomes the neutral alias.
   * Whole-label replacement is deliberately conservative — splicing the
   * alias mid-string could leave fragments that still identify the host.
   * A custom session name that does not contain any known name passes
   * through unchanged.
   */
  PURE.maskSessionLabel = function (label, names, maskOn) {
    var lab = String(label == null ? "" : label);
    if (!maskOn || !lab) return lab;
    for (var i = 0; i < names.length; i++) {
      var n = String(names[i]);
      if (n && lab.toLowerCase().indexOf(n.toLowerCase()) !== -1) {
        return PURE.INTERNAL_HOST_ALIAS;
      }
    }
    return lab;
  };

  /**
   * Mask known internal names inside free-form UI text (the toolbar
   * status/error line, the route banner): each occurrence is replaced in
   * place with the neutral alias (case-insensitive), so server error
   * bodies that echo the target cannot leak it into a screenshot. Text
   * without any known name is returned unchanged.
   */
  PURE.maskText = function (text, names, maskOn) {
    var t = String(text == null ? "" : text);
    if (!maskOn || !t) return t;
    for (var i = 0; i < names.length; i++) {
      var n = String(names[i]);
      if (!n) continue;
      var re = new RegExp(n.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "gi");
      t = t.replace(re, PURE.INTERNAL_HOST_ALIAS);
    }
    return t;
  };

  /**
   * Sanitize a free-text session name on the client. Mirrors the server's
   * :func:`sanitize_name` so the UI can validate before the POST and the
   * optimistic label matches the server-round-tripped one. Strips Unicode
   * control / format / surrogate / private-use code points and ASCII
   * whitespace, collapses whitespace runs, trims, and hard-truncates to
   * ``NAME_MAX`` chars. Returns ``""`` for null / non-string / empty
   * input (the caller treats empty as "use target").
   */
  PURE.NAME_MAX = 60;
  PURE.sanitizeName = function (value) {
    if (typeof value !== "string") return "";
    // Strip every Unicode control/format/surrogate/private-use code point
    // (general categories Cc/Cf/Cs/Co) using Unicode property escapes with
    // the ``u`` flag. This mirrors the server's
    // ``unicodedata.category()``-based filter exhaustively: bidi overrides
    // (U+202E, U+2066–U+2069), ZWSP (U+200B), BOM (U+FEFF), tags,
    // variation selectors, BMP + supplementary private-use, and unpaired
    // surrogates (which would also break the server's UTF-8 write) are all
    // removed. The prior hand-maintained code-point range list silently
    // missed Cf entries (bidi, ZWSP, BOM) and every Cs code point.
    //
    // ``replace`` with the global flag walks the string once; each matched
    // code point becomes a space so visually adjacent tokens stay legible
    // (e.g. "a\\tb" → "a b", not "ab"). A trailing whitespace pass
    // collapses runs and trims.
    var cleaned = value.replace(/[\p{Cc}\p{Cf}\p{Cs}\p{Co}]/gu, " ");
    cleaned = cleaned.replace(/\s+/g, " ").trim();
    if (cleaned.length > PURE.NAME_MAX) {
      cleaned = cleaned.slice(0, PURE.NAME_MAX);
    }
    return cleaned;
  };

  /**
   * Choose the tab to activate after ``closedId`` is removed from the
   * ``ids`` list (in display order). Mirrors familiar browser behaviour:
   * prefer the immediate neighbour to the right of the closed tab, then the
   * neighbour to the left, then null when the list becomes empty. Returns
   * the chosen id (which may equal ``activeId`` when the active tab is not
   * the one being closed).
   *
   * ``ids`` may contain strings or numbers; comparisons are string-normalised
   * so callers do not have to coerce. ``closedId`` must be present in
   * ``ids``; if it is not, the function returns ``activeId`` unchanged.
   */
  PURE.nextActiveAfterClose = function (ids, activeId, closedId) {
    var arr = Array.isArray(ids) ? ids.slice() : [];
    var c = String(closedId);
    var idx = -1;
    for (var i = 0; i < arr.length; i++) {
      if (String(arr[i]) === c) { idx = i; break; }
    }
    if (idx < 0) return activeId == null ? null : String(activeId);
    // Build the post-close list.
    var after = arr.slice(0, idx).concat(arr.slice(idx + 1));
    if (after.length === 0) return null;
    // If the active tab is not the closed one, it stays active (its index
    // may shift left by one if it was to the right of the closed tab, but
    // the id itself is unchanged).
    if (activeId != null && String(activeId) !== c) {
      return String(activeId);
    }
    // Active tab is being closed: prefer the right neighbour, then the left.
    var pick = after[idx] !== undefined ? after[idx] : after[idx - 1];
    return pick == null ? null : String(pick);
  };

  // ----- Pause-freeze (PingPlotter behaviour): pure effective-time logic.
  //
  // While a tab is paused, the server stops probing. The visible time
  // window (timeline x-axis, rolling-stats cutoff, Focus caption bounds,
  // drag-focus mapping) must FREEZE so samples stay put instead of
  // drifting left and falling off as wall-clock time advances. Each tab
  // owns a ``pauseFreezeS`` field (epoch seconds, or null while running);
  // callers route every data-window "now" through ``effectiveNowS`` so a
  // frozen value replaces wall time while paused, and wall time takes
  // over again on resume (the forward jump is correct — the gap is real
  // dead time).
  //
  // The footer's elapsed-time counter (``state.startedAt``) is
  // intentionally NOT routed through this — PingPlotter keeps elapsed
  // running while paused, so the previous behaviour is preserved.

  /**
   * Resolve the effective "now" (epoch seconds) for data-window purposes.
   * Returns ``wallS`` while running (``freezeS`` null/undefined/non-finite) and
   * ``freezeS`` while paused. Pure: callers supply both timestamps so the
   * freeze transition can be unit-tested deterministically.
   */
  PURE.effectiveNowS = function (wallS, freezeS) {
    if (freezeS === null || freezeS === undefined) return wallS;
    if (typeof freezeS === "number" && !isFinite(freezeS)) return wallS;
    return freezeS;
  };

  /**
   * Best-effort latest sample timestamp across one or more sample arrays.
   * Used to set the pause freeze point when restoring an already-paused
   * session: the wall-clock pause time is not known on restore, so the
   * latest sample timestamp across hops is the closest surrogate. Returns
   * ``null`` when no sample carries a finite ``t``.
   *
   * Accepts a flat sample array ``[{t,...}]``, an array of arrays of
   * samples, or a dictionary object whose own values are sample arrays
   * (the shape of ``state.samples``). Non-finite / missing ``t`` values
   * are skipped; non-numeric keys on a dictionary are ignored.
   */
  PURE.pauseFreezeFromSamples = function (input) {
    if (input === null || input === undefined) return null;
    var latest = null;
    function consider(arr) {
      if (!Array.isArray(arr)) return;
      for (var i = 0; i < arr.length; i++) {
        var sample = arr[i];
        if (!sample) continue;
        var t = sample.t;
        if (typeof t === "number" && isFinite(t) && (latest === null || t > latest)) {
          latest = t;
        }
      }
    }
    if (Array.isArray(input)) {
      // Flat sample array vs array-of-arrays: detect by inspecting the
      // first element. An empty array falls through to consider([]).
      if (input.length > 0 && Array.isArray(input[0])) {
        for (var i = 0; i < input.length; i++) consider(input[i]);
      } else {
        consider(input);
      }
    } else if (typeof input === "object") {
      Object.keys(input).forEach(function (k) { consider(input[k]); });
    }
    return latest;
  };

  /**
   * Decide whether an arriving snapshot's ``state`` field should be adopted
   * into ``runningKnown`` / ``pauseFreezeS``.
   *
   * Returns ``"adopt"`` when no control transition is pending (the server
   * snapshot is AUTHORITATIVE for the session's run/pause state — including
   * a state value that disagrees with the local ``runningKnown``, which is
   * exactly how a second browser window learns that another window paused
   * or resumed the shared session). Returns ``"defer"`` while a pause/resume
   * POST is in flight: the POST's own response (or its ambiguous-failure
   * reconciliation) owns the state flip, so a snapshot captured mid-flight
   * — which may precede or follow the server-side commit — must not move
   * the local state either way.
   *
   * This is the policy core of the generation-based snapshot gate. The
   * companion generation guard lives in ``fetchSnapshot``: each request
   * captures ``snapshotRequestSeq`` and a confirmed control transition
   * bumps that counter, so a snapshot issued BEFORE a confirmed transition
   * is rejected outright (it can never land and reach this policy).
   */
  // ----- ICMP non-responsive presentation (neutral, trace-local) --------
  //
  // An intermediate hop that has NEVER answered a probe in the current
  // trace (after the initial 5 probes) is presented neutrally — "ICMP
  // non-responsive" — instead of as alarming red loss. Intermittent loss
  // (at least one reply ever) keeps the ordinary loss treatment; the
  // destination's 100% loss stays an error. The SERVER computes the exact
  // lifetime flag (snapshot ``icmp_non_responsive``); this pure helper
  // extends it with the client's own live evidence so the presentation
  // flips ON after the 5th failed round and OFF the moment a reply lands,
  // without waiting for the next snapshot fetch.

  PURE.ICMP_NR_MIN_PROBES = 5;

  /**
   * Decide the neutral "ICMP non-responsive" presentation for one hop.
   *
   * ``o`` fields:
   *  - isDestination: target stream / target row / final-hop-is-target —
   *    always excluded (destination 100% loss remains an error).
   *  - everReplied: sticky client-side fact — set once ANY successful reply
   *    has been observed (live round, snapshot baseline with an average, or
   *    merged history) and cleared only on reset/new trace. A historical
   *    reply that has aged out of every rolling window still disqualifies
   *    classification, so this is never "rolling-window 100% loss".
   *  - samples: the hop's live sample buffer (scanned for any success).
   *  - seed: snapshot baseline stats for the hop (``avg_ms`` non-null means
   *    a success exists in the baseline window).
   *  - serverFlag: the snapshot's lifetime ``icmp_non_responsive`` flag
   *    (authoritative for pre-attach history the client cannot see).
   */
  PURE.hopNonResponsive = function (o) {
    o = o || {};
    if (o.isDestination) return false;
    if (o.everReplied) return false;
    var list = Array.isArray(o.samples) ? o.samples : [];
    var probes = 0;
    for (var i = 0; i < list.length; i++) {
      var s = list[i];
      if (!s || typeof s.t !== "number" || !isFinite(s.t)) continue;
      probes++;
      var r = s.rtt_ms;
      if (r !== null && r !== undefined && !isNaN(r)) return false; // replied
    }
    var seed = o.seed || null;
    if (seed && seed.count > 0 && seed.avg_ms !== null && seed.avg_ms !== undefined) {
      return false; // baseline window contains a successful reply
    }
    if (o.serverFlag) return true;
    return probes >= (o.minProbes || PURE.ICMP_NR_MIN_PROBES);
  };

  // ----- Throughput overlay (InfluxDB Mbps) ------------------------------

  /**
   * Nice axis ceiling for the Mbps overlay scale: the smallest value from
   * {1,2,5}×10^k (plus the raw max) that covers every finite point. Floors
   * at 1 so an idle interface still shows a usable axis. Pure so the
   * overlay scale is node-assertable.
   */
  PURE.mbpsAxisMax = function (points) {
    var m = 0;
    var list = Array.isArray(points) ? points : [];
    for (var i = 0; i < list.length; i++) {
      var p = list[i];
      if (!p) continue;
      if (typeof p.in_mbps === "number" && isFinite(p.in_mbps) && p.in_mbps > m) m = p.in_mbps;
      if (typeof p.out_mbps === "number" && isFinite(p.out_mbps) && p.out_mbps > m) m = p.out_mbps;
    }
    if (m <= 0) return 1;
    var pow = Math.pow(10, Math.floor(Math.log(m) / Math.LN10));
    var mults = [1, 2, 5, 10];
    for (var k = 0; k < mults.length; k++) {
      if (mults[k] * pow >= m) return mults[k] * pow;
    }
    return 10 * pow;
  };

  /**
   * Right-hand Mbps axis label texts (0 / mid / max). The top label
   * carries an explicit " Mbps" unit. Pure so label content is
   * node-assertable.
   */
  PURE.tpAxisLabels = function (axisMax) {
    return [
      { v: 0, text: "0" },
      { v: axisMax / 2, text: PURE.formatMs(axisMax / 2) },
      { v: axisMax, text: PURE.formatMs(axisMax) + " Mbps" },
    ];
  };

  /**
   * Throughput path segments for one direction (pure geometry helper
   * used by the canvas renderer).
   *
   * Input: ``points`` — the server's ``[{t, in_mbps, out_mbps}]`` buckets
   * (ascending by t; ``null`` Mbps marks a missing bucket, and omitted
   * buckets appear as timestamp gaps); ``key`` — "in_mbps" or
   * "out_mbps"; ``maxGapS`` — the configured bucket cadence (the
   * response's ``step_s``; 0/undefined disables the gap rule);
   * ``endS``/``rangeS``/``width`` for PURE.timeToX; ``yFor`` maps a Mbps
   * value to a y pixel (the renderer's clamped axis mapping).
   *
   * Output: an array of SEGMENTS — a new segment starts wherever the path
   * must BREAK: at a null/non-finite value (missing bucket) or when the
   * gap to the previous included point exceeds ``maxGapS`` (omitted
   * buckets). Missing data renders as a gap — never a bridge, never a
   * zero. Each segment is a flat ``{x, y, kind}`` list in step-path
   * order ("move" then "h"/"v" pairs), matching PURE.stepPath's
   * contract so the renderer can share its moveTo/lineTo loop. Malformed
   * points (missing/non-finite ``t") act as breaks.
   */
  PURE.tpSegments = function (points, key, maxGapS, endS, rangeS, width, yFor) {
    var list = Array.isArray(points) ? points : [];
    var gap = typeof maxGapS === "number" && isFinite(maxGapS) && maxGapS > 0
      ? maxGapS : 0;
    var segs = [];
    var cur = [];
    var prevT = null;
    function closeSeg() {
      if (cur.length) segs.push(cur);
      cur = [];
    }
    for (var i = 0; i < list.length; i++) {
      var p = list[i];
      var tOk = p && typeof p.t === "number" && isFinite(p.t);
      var v = tOk ? p[key] : null;
      if (!tOk || typeof v !== "number" || !isFinite(v)) {
        closeSeg(); // missing/malformed bucket: break, never bridge
        prevT = null;
        continue;
      }
      if (prevT !== null && gap > 0 && p.t - prevT > gap) closeSeg();
      cur.push({ x: PURE.timeToX(p.t, endS, rangeS, width), y: yFor(v) });
      prevT = p.t;
    }
    closeSeg();
    return segs.map(function (seg) {
      var path = [{ x: seg[0].x, y: seg[0].y, kind: "move" }];
      for (var k = 1; k < seg.length; k++) {
        path.push({ x: seg[k].x, y: seg[k - 1].y, kind: "h" });
        path.push({ x: seg[k].x, y: seg[k].y, kind: "v" });
      }
      return path;
    });
  };

  PURE.snapshotStatePolicy = function (controlAction) {
    return controlAction === null || controlAction === undefined ? "adopt" : "defer";
  };

  /**
   * Compute the paused-window freeze epoch (seconds) for one snapshot.
   *
   * Preference order:
   *   1. ``pausedAt`` — the server-recorded pause instant. Authoritative
   *      and used UNBOUNDED (it is the actual epoch the session paused).
   *   2. Sample-based fallback — the latest finite sample ``t`` across
   *      the tab's live per-hop buffer, then the snapshot's own sample
   *      tail. BOUNDED to ``<= wallS`` so a bogus / clock-skewed / future
   *      sample timestamp can never anchor the freeze ahead of the client
   *      clock. This path only fires for sessions that predate the
   *      ``paused_at`` field (defensive; fresh deploys always have it).
   *   3. ``wallS`` — snapshot-receipt wall time as the last resort.
   *
   * ``samplesByHop`` is the tab's per-hop sample dictionary (the runtime
   * shape of ``state.samples``); ``snapSamples`` is the snapshot's flat
   * ``samples`` array. Both accept the shapes ``pauseFreezeFromSamples``
   * already handles (dict / flat array / array-of-arrays / null).
   */
  PURE.computePauseFreeze = function (pausedAt, samplesByHop, snapSamples, wallS) {
    if (typeof pausedAt === "number" && isFinite(pausedAt)) return pausedAt;
    var fallback = PURE.pauseFreezeFromSamples(samplesByHop);
    if (fallback === null && snapSamples !== null && snapSamples !== undefined) {
      fallback = PURE.pauseFreezeFromSamples(snapSamples);
    }
    if (typeof fallback === "number" && isFinite(fallback)) {
      return fallback > wallS ? wallS : fallback;
    }
    return wallS;
  };

  // ----- Status-line arbitration (root-cause refactor).
  //
  // The toolbar status line used to be written IMPERATIVELY from every
  // async source (SSE ready/error, snapshot adoption, control
  // request/confirm/fail, start, reset, close). Every pair of writers was
  // a potential race, and four consecutive review rounds each found one
  // more symmetric clobber (snapshot-vs-error, stranded preservation
  // flag, SSE ready clobbering pending/error, SSE error clobbering
  // pending/error). Each fix added another ad-hoc guard to another
  // writer. The root cause is that status was WRITTEN, not DERIVED.
  //
  // deriveStatus is the single pure precedence table that turns the
  // recorded FACTS into a { text, kind } line. Former writers now only
  // update facts and call renderStatus(); precedence — not guards —
  // decides what shows. This collapses the four guards into one table.
  //
  // Precedence (first match wins; each row is node-asserted):
  //   1. resetting              -> "Resetting…"        (DELETE in flight)
  //   2. controlAction=pause    -> "Pausing…"          (user just clicked)
  //   3. controlAction=resume   -> "Resuming…"
  //   4. startInFlight          -> "Starting…"         (POST /traces in flight)
  //   5. lastError              -> lastError           (failed transition)
  //   6. reconnecting           -> "Reconnecting…"     (SSE transport down)
  //   7. hasSession+running     -> "Running"           (steady)
  //   8. hasSession+paused      -> "Paused"            (steady)
  //   9. else                   -> "Idle"
  //
  // Rows 1–4 (lifecycle transients) outrank row 5 (errors) so a new
  // user action always replaces a stale failure. Row 5 outranks rows 6–8
  // so neither an SSE reconnect nor a steady-state derivation can mask
  // an unreconciled error (the round-3/round-4 defect class). Row 6
  // outranks the steady states so a dropped stream is reported. The
  // error-clearing POLICY (how lastError is cleared) is owned by the
  // fact-writers — notably the one-shot ``errorPinnedForReconcile`` pin
  // that keeps a control failure visible through its single
  // reconciliation snapshot; that is stateful clearing, which a pure
  // precedence table cannot express, so the pin stays as a fact.
  PURE.deriveStatus = function (view) {
    view = view || {};
    if (view.resetting) return { text: "Resetting…", kind: "pending" };
    if (view.controlAction === "pause") return { text: "Pausing…", kind: "pending" };
    if (view.controlAction === "resume") return { text: "Resuming…", kind: "pending" };
    if (view.startInFlight) return { text: "Starting…", kind: "pending" };
    if (view.lastError) return { text: view.lastError, kind: "error" };
    if (view.reconnecting) return { text: "Reconnecting…", kind: "error" };
    if (view.hasSession && view.runningKnown) return { text: "Running", kind: "running" };
    if (view.hasSession && !view.runningKnown) return { text: "Paused", kind: "pending" };
    return { text: "Idle", kind: null };
  };

  // Export-button enable predicate. The Export button downloads the server's
  // full sample history for the ACTIVE tab's session, so it is available only
  // when the tab has a session AND the server is known to hold at least one
  // sample for it. "Known" combines the two client-side channels: the live
  // SSE sample buffer (state.samples, keyed by hop index) and the snapshot
  // baseline (state.seedStats — populated on start, restore, and per-hop
  // hydration, each carrying a count). Either channel surfacing a hop with
  // samples means the export will return real rows; a freshly-started trace
  // that has not yet produced a sample (or an idle tab) keeps it disabled.
  // Pure so it is unit-assertable under node alongside the rest of PURE.
  PURE.exportEnabled = function (view) {
    view = view || {};
    if (!view.sessionId) return false;
    var live = view.samples || {};
    for (var k in live) {
      if (Object.prototype.hasOwnProperty.call(live, k)) {
        if (live[k] && live[k].length > 0) return true;
      }
    }
    var seed = view.seedStats || {};
    for (var k2 in seed) {
      if (Object.prototype.hasOwnProperty.call(seed, k2)) {
        if (seed[k2] && seed[k2].count > 0) return true;
      }
    }
    return false;
  };

  // Button enable predicate: a bound session AND usable target stats in the
  // selected window (idle/reset tabs and empty windows stay disabled).
  PURE.summaryEnabled = function (view) {
    view = view || {};
    if (!view.sessionId) return false;
    return !!(view.stats && view.stats.count > 0);
  };

  // Mask-aware label for the target stream in copied text.
  PURE.summaryTargetLabel = function (hop, isInternal, maskOn, names) {
    var h = hop || {};
    var name = h.dns || h.ip || "";
    if (h.dns) name = PURE.maskDnsName(name, !!isInternal, maskOn);
    return PURE.maskSessionLabel(name, names || [], maskOn) || "unknown";
  };

  // Plain-text summary of the CURRENT TARGET for the selected window, built
  // from the statistics the table already shows, so the text never overstates
  // coverage. Returns null when there is nothing usable to copy.
  //
  // ``seedHasMax``: a snapshot baseline without max_ms leaves a combined
  // statistic's max delta-only, which is not a full-window maximum, so it is
  // reported unavailable instead of overstated.
  PURE.buildTargetSummary = function (view) {
    view = view || {};
    var stats = view.stats;
    if (!stats || !(stats.count > 0)) return null;
    function fmt(v) {
      if (v === null || v === undefined || (typeof v === "number" && isNaN(v))) return "n/a";
      return Number(v).toFixed(1);
    }
    var max = stats.max;
    var maxNote = null;
    if (view.source === "combined" && !view.seedHasMax) {
      max = null;
      maxNote = "max unavailable: the baseline statistics carry no max, " +
        "and the recent-sample max alone does not cover the window";
    }
    var intervalS = Number(view.intervalS);
    var lines = [
      "netplot trace summary",
      "Target: " + (view.target ? String(view.target) : "unknown"),
      "Window: " + (view.windowLabel || "unknown") +
        ", " + PURE.formatUtcStamp(view.startS) + " - " + PURE.formatUtcStamp(view.endS),
      "Probe interval: " + (isFinite(intervalS) && intervalS > 0 ? intervalS + " s" : "n/a"),
      "RTT avg/min/max (ms): " + fmt(stats.avg) + " / " + fmt(stats.min) + " / " + fmt(max),
    ];
    var loss = "Packet loss: " + fmt(stats.lossPct) + "%";
    // Lost-probe counts are only recoverable from count*PL% when that product
    // is an integer. A blended baseline+delta ratio may not be, so the count is
    // omitted rather than rounded into a fabricated integer.
    var lost = stats.count * Number(stats.lossPct) / 100;
    if (isFinite(lost) && Math.abs(lost - Math.round(lost)) < 1e-6) {
      loss += " (lost " + Math.round(lost) + " of " + stats.count + " probes)";
    } else {
      loss += " (" + stats.count + " probes)";
    }
    lines.push(loss);
    if (maxNote) lines.push("Note: " + maxNote);
    return lines.join("\n");
  };

  // ----- clipboard (Copy summary) -------------------------------------

  /**
   * Copy ``text`` via the async Clipboard API, falling back to
   * execCommand("copy") when the page has no such API (plain-HTTP LAN) or it
   * rejects. Resolves true/false and never reports success when nothing was
   * copied. ``clipboard``/``doc`` are injected so the tests need no browser.
   */
  PURE.copyPlainText = function (opts) {
    opts = opts || {};
    var text = String(opts.text == null ? "" : opts.text);
    var doc = opts.doc || null;
    var clipboard = opts.clipboard || null;
    function legacy() { return PURE.execCommandCopy(doc, text); }
    if (clipboard && typeof clipboard.writeText === "function") {
      return Promise.resolve()
        .then(function () { return clipboard.writeText(text); })
        .then(function () { return true; }, function () { return legacy(); });
    }
    return Promise.resolve(legacy());
  };

  /** Legacy path: temporary off-screen textarea, execCommand("copy"), cleanup. */
  PURE.execCommandCopy = function (doc, text) {
    if (!doc || typeof doc.createElement !== "function" || !doc.body) return false;
    var ta = doc.createElement("textarea");
    ta.value = String(text == null ? "" : text);
    ta.setAttribute("readonly", "");
    if (ta.style) {
      ta.style.position = "fixed";
      ta.style.top = "-1000px";
    }
    var prev = doc.activeElement || null;
    var ok = false;
    doc.body.appendChild(ta);
    try {
      if (typeof ta.select === "function") ta.select();
      ok = !!doc.execCommand("copy");
    } catch (e) {
      ok = false;
    }
    try { doc.body.removeChild(ta); } catch (e2) { /* already detached */ }
    if (prev && typeof prev.focus === "function") prev.focus();
    return ok;
  };

  // Overlay-on-activate fetch predicate: a tab activating with an
  // enabled + configured overlay but NO points fetches now — e.g. one
  // cleared by an Influx settings save while backgrounded (only the
  // active tab is refetched on save, and tick refreshes only while
  // RUNNING, so a paused background tab would otherwise show an empty
  // overlay forever). Paused tabs are included: the pause-aware window
  // (effNowS) and the generation guards live in fetchThroughput.
  // Pure so it is unit-assertable under node alongside the rest of PURE.
  PURE.throughputFetchOnActivate = function (view) {
    view = view || {};
    if (!view.sessionId) return false;
    if (!view.throughputOn) return false;
    if (!view.influxUrl) return false;
    return !(Array.isArray(view.throughputPoints) && view.throughputPoints.length > 0);
  };

  // Export for unit checks under node (CommonJS guard).
  if (typeof module !== "undefined" && module.exports) {
    module.exports = PURE;
  }
  // Expose PURE on window for browser-side tests (Slice 6c — the live
  // Playwright check asserts stepPath geometry directly against this
  // object rather than relying on pixel sampling). Not used by app code.
  if (typeof window !== "undefined") {
    window.PURE = PURE;
  }

  // ============================================================ Slice 7
  // Multi-trace tabs (PingPlotter-style). The dashboard now supports
  // multiple simultaneous traces — one per tab. Each tab owns its state
  // (sessionId, EventSource, sample buffers, hop list, focus, timelines)
  // via a factory (``createTab``). A single ``tabManager`` owns the tab
  // array, the active tab, and the tab-bar DOM; the toolbar elements are
  // SHARED across tabs and always reflect the active tab.
  //
  // Background tabs keep their EventSource attached and keep buffering
  // samples, but every DOM-writing closure self-guards with
  // ``tabManager.isActive(self)`` so they never touch the shared DOM while
  // hidden. Switching to a backgrounded tab triggers a full re-render from
  // its buffers (no refetch — buffers are authoritative).
  //
  // Pure tab-state logic (label normalisation, next-active selection) lives
  // in ``PURE.tabLabel`` / ``PURE.nextActiveAfterClose`` and is unit-tested
  // under node alongside the rest of PURE.

  // --------------------------------------------------------------- DOM wiring

  if (typeof document === "undefined") return;

  // Page elements. The toolbar (form, target, interval, play/pause,
  // focus-window, packet-type, status, focus-caption, target-history) and
  // the main content (route-banner, hop table, timelines, status-line) are
  // SHARED across all tabs: the toolbar always reflects the active tab, and
  // the main content is re-rendered from the active tab's buffers on every
  // tab switch. The tab bar (#tab-bar) lives between them.
  var els = {
    form: document.getElementById("trace-form"),
    target: document.getElementById("target"),
    interval: document.getElementById("interval"),
    // Slice 6b: Play/Pause split replaces the single #start-stop toggle.
    btnPlay: document.getElementById("btn-play"),
    btnPause: document.getElementById("btn-pause"),
    // Slice 6b: target history dropdown (free-text input + listbox).
    targetHistory: document.getElementById("target-history"),
    // Slice 6b: Focus dropdown + packet-type dropdown + caption.
    focusWindow: document.getElementById("focus-window"),
    packetType: document.getElementById("packet-type"),
    backgroundToggle: document.getElementById("background-toggle"),
    focusCaption: document.getElementById("focus-caption"),
    reset: document.getElementById("reset"),
    exportBtn: document.getElementById("export"),
    copySummary: document.getElementById("copy-summary"),
    copySummaryStatus: document.getElementById("copy-summary-status"),
    status: document.getElementById("status"),
    routeBanner: document.getElementById("route-banner"),
    tableBody: document.getElementById("hop-table-body"),
    emptyRow: document.getElementById("empty-row"),
    statusLine: document.getElementById("status-line"),
    // Slice 6 — graph column chrome (legend, axis-max label, connector SVG).
    tableSection: document.querySelector(".table-section"),
    graphLegend: document.getElementById("graph-legend"),
    legendWrap: document.getElementById("graph-legend-wrap"),
    legendBar: document.getElementById("graph-legend-bar"),
    legendMarkers: document.getElementById("graph-legend-markers"),
    legendGreen: document.getElementById("graph-legend-green"),
    legendYellow: document.getElementById("graph-legend-yellow"),
    legendRed: document.getElementById("graph-legend-red"),
    legendTickGreen: document.getElementById("graph-legend-tick-green"),
    legendTickYellow: document.getElementById("graph-legend-tick-yellow"),
    graphAxisMax: document.getElementById("graph-axis-max"),
    graphConnector: document.getElementById("graph-connector"),
    // Slice 5 — timeline section.
    timelinesSection: document.getElementById("timelines-section"),
    timelineStack: document.getElementById("timeline-stack"),
    focusChip: document.getElementById("focus-chip"),
    focusChipText: document.getElementById("focus-chip-text"),
    focusClear: document.getElementById("focus-clear"),
    // Slice 7 — multi-trace tab bar.
    tabBar: document.getElementById("tab-bar"),
    // Internal-DNS masking toggle + settings dialog.
    maskToggle: document.getElementById("mask-toggle"),
    btnSettings: document.getElementById("btn-settings"),
    settingsDialog: document.getElementById("settings-dialog"),
    settingsForm: document.getElementById("settings-form"),
    settingsCidrs: document.getElementById("settings-cidrs"),
    settingsCidrsDefault: document.getElementById("settings-cidrs-default"),
    settingsInfluxUrl: document.getElementById("settings-influx-url"),
    settingsInfluxDb: document.getElementById("settings-influx-db"),
    settingsInfluxInterface: document.getElementById("settings-influx-interface"),
    settingsInfluxHost: document.getElementById("settings-influx-host"),
    settingsStatus: document.getElementById("settings-status"),
    settingsCancel: document.getElementById("settings-cancel"),
    settingsSave: document.getElementById("settings-save"),
  };

  // Global (shared across tabs) UI state:
  //  - maskState: the screenshot "Mask internal DNS" toggle. Display-only;
  //    when on, DNS names of server-classified internal hosts render as a
  //    neutral alias. Never changes what the server stores or exports.
  //  - serverSettings: influx overlay availability (fetched at bootstrap and
  //    after every settings save so the throughput checkbox can disable
  //    itself when unconfigured).
  var maskState = { on: false };
  var serverSettings = { influxUrl: "" };

  // ----- shared stateless helpers --------------------------------------

  function api(path, opts) {
    opts = opts || {};
    var headers = Object.assign({ "Content-Type": "application/json" }, opts.headers || {});
    return fetch(path, {
      method: opts.method || "GET",
      body: opts.body !== undefined ? opts.body : undefined,
      headers: headers,
    });
  }

  function asJson(resp) {
    if (!resp.ok) {
      throw new Error("HTTP " + resp.status + " " + resp.statusText);
    }
    return resp.json();
  }

  // Module-level DOM writer for the toolbar status line. Each tab tracks
  // its own (text, kind) in state and restores the DOM on activate; only
  // the active tab ever calls this.
  function setStatus(text, kind) {
    els.status.textContent = text;
    els.status.classList.remove("state-error", "state-pending", "state-running");
    if (kind) els.status.classList.add("state-" + kind);
  }

  // ----- global target history (shared across all tabs) ----------------
  // The recent-targets dropdown is global: targets started in any tab appear
  // in the dropdown in every tab. State lives in ``historyState`` so all
  // closures share it, persisted to localStorage under netplot.targetHistory.
  var historyState = {
    list: [],        // most-recent-first, deduped, capped at TARGET_HISTORY_MAX
    highlight: -1,   // keyboard-selected entry index, -1 none
  };

  function loadTargetHistory() {
    if (typeof localStorage === "undefined") return [];
    try {
      var raw = localStorage.getItem(TARGET_HISTORY_KEY);
      if (!raw) return [];
      var parsed = JSON.parse(raw);
      return Array.isArray(parsed)
        ? parsed.filter(function (x) { return typeof x === "string"; }).slice(0, TARGET_HISTORY_MAX)
        : [];
    } catch (e) {
      return [];
    }
  }

  function saveTargetHistory() {
    if (typeof localStorage === "undefined") return;
    try {
      localStorage.setItem(TARGET_HISTORY_KEY, JSON.stringify(historyState.list));
    } catch (e) {
      // Storage may be unavailable (private mode, quota); degrade silently.
    }
  }

  function addTargetToHistory(target) {
    var next = PURE.targetHistoryAdd(historyState.list, target);
    if (next.length === historyState.list.length &&
        next.every(function (v, i) { return v === historyState.list[i]; })) {
      return; // unchanged
    }
    historyState.list = next;
    saveTargetHistory();
    renderTargetHistory();
  }

  function isHistoryVisible() {
    return !!els.targetHistory && !els.targetHistory.hidden;
  }

  function showTargetHistory() {
    // Screenshot masking suppresses the recent-targets popup entirely:
    // history entries are free-text targets with NO internal
    // classification available client-side (classification only rides a
    // live session's snapshot), so entries cannot be safely masked.
    // Hiding beats guessing — no DNS lookup or backend change is invented
    // for this. The raw history stays in localStorage untouched.
    if (maskState.on) return;
    if (!els.targetHistory || historyState.list.length === 0) return;
    historyState.highlight = -1;
    renderTargetHistory();
    els.targetHistory.hidden = false;
    els.target.setAttribute("aria-expanded", "true");
  }

  function hideTargetHistory() {
    if (!els.targetHistory) return;
    els.targetHistory.hidden = true;
    els.target.setAttribute("aria-expanded", "false");
    historyState.highlight = -1;
  }

  function renderTargetHistory() {
    if (!els.targetHistory) return;
    while (els.targetHistory.firstChild) {
      els.targetHistory.removeChild(els.targetHistory.firstChild);
    }
    historyState.list.forEach(function (t, idx) {
      var item = document.createElement("div");
      item.className = "target-history-item";
      item.setAttribute("role", "option");
      item.textContent = t;
      item.addEventListener("mousedown", function (ev) {
        // mousedown fires before the input's blur; prevent the blur so the
        // click is registered on the item.
        ev.preventDefault();
      });
      item.addEventListener("click", function () {
        selectTargetHistory(idx);
      });
      els.targetHistory.appendChild(item);
    });
    updateHistoryHighlight();
  }

  function updateHistoryHighlight() {
    if (!els.targetHistory) return;
    var items = els.targetHistory.querySelectorAll(".target-history-item");
    items.forEach(function (item, idx) {
      item.classList.toggle("active", idx === historyState.highlight);
    });
  }

  function selectTargetHistory(idx) {
    if (idx < 0 || idx >= historyState.list.length) return;
    if (els.target) els.target.value = historyState.list[idx];
    hideTargetHistory();
    if (els.target) els.target.focus();
  }

  // ----- canvas layout constants (shared by all tabs) -----------------
  // Padding leaves room for axis labels.
  var TL_PAD_L = 44; // Y labels
  var TL_PAD_R = 12;
  var TL_PAD_T = 10;
  var TL_PAD_B = 22; // X labels
  var TL_DPR_MIN = 1;
  var TL_DPR_MAX = 3;

  // ============================================================ TabManager
  //
  // Owns the array of tabs (display order), the active tab, and the tab-bar
  // DOM. Toolbar elements are shared; their event listeners are wired ONCE
  // (in ``wireToolbar``) and dispatch to the active tab.

  var nextTabSeq = 1;

  var tabManager = (function () {
    var tabs = [];
    var active = null;

    function isActive(self) { return self === active; }
    function getActive() { return active; }
    function getTabs() { return tabs; }

    function add(tab, makeActive) {
      tab._id = "tab-" + (nextTabSeq++);
      tabs.push(tab);
      renderTabBar();
      if (makeActive || tabs.length === 1) {
        setActive(tab);
      }
    }

    function setActive(tab) {
      if (!tab || tab === active) return;
      // Capture any typed toolbar values from the outgoing tab before
      // switching (idle tabs preserve pending target/interval this way).
      if (active && typeof active.captureToolbar === "function") {
        active.captureToolbar();
      }
      active = tab;
      if (typeof tab.activate === "function") tab.activate();
      renderTabBar();
    }

    function close(tab) {
      var idx = tabs.indexOf(tab);
      if (idx < 0) return;
      // Pick the next tab to activate (right neighbour, then left).
      var nextId = PURE.nextActiveAfterClose(
        tabs.map(function (t) { return t._id; }),
        active ? active._id : null,
        tab._id
      );
      var nextTab = null;
      if (nextId !== null) {
        for (var i = 0; i < tabs.length; i++) {
          if (tabs[i]._id === nextId) { nextTab = tabs[i]; break; }
        }
      }
      tabs.splice(idx, 1);
      tab.destroy();
      if (tab === active) {
        active = null;
        if (nextTab) {
          setActive(nextTab);
        } else {
          // No tabs left: spawn a fresh idle one so the dashboard is never
          // empty (criterion 8 — preserve the single-trace idle view).
          add(createTab(null), true);
        }
      } else {
        renderTabBar();
      }
    }

    function renderTabBar() {
      if (!els.tabBar) return;
      while (els.tabBar.firstChild) {
        els.tabBar.removeChild(els.tabBar.firstChild);
      }
      tabs.forEach(function (tab) {
        var el = document.createElement("div");
        el.className = "tab" + (tab.isIdle() ? " is-idle" : "");
        el.setAttribute("role", "tab");
        el.setAttribute("aria-selected", tab === active ? "true" : "false");
        el.setAttribute("data-tab-id", tab._id);
        var marker = PURE.tabMarker(tab.isPersistent && tab.isPersistent());
        el.title = tab.isIdle()
          ? "New trace"
          : (tab.getLabel() + " — click to switch" +
              (marker ? " (background)" : ""));

        // Persistent-collection marker (⇄) precedes the label so an
        // operator can spot background tabs at a glance. Rendered only
        // when the tab's session is persistent; absent otherwise so the
        // inactive tabs stay compact.
        if (marker) {
          var mark = document.createElement("span");
          mark.className = "tab-marker";
          mark.setAttribute("aria-hidden", "true");
          mark.textContent = marker;
          el.appendChild(mark);
        }

        var label = document.createElement("span");
        label.className = "tab-label";
        label.textContent = tab.getLabel();
        el.appendChild(label);

        var close = document.createElement("button");
        close.type = "button";
        close.className = "tab-close";
        close.setAttribute("aria-label", "Close tab " + tab.getLabel());
        close.textContent = "×";
        close.addEventListener("click", function (ev) {
          ev.stopPropagation();
          tab.requestClose();
        });
        el.appendChild(close);

        el.addEventListener("click", function (ev) {
          // Clicks on the close button (above) stopPropagation.
          if (ev.target === close) return;
          setActive(tab);
        });
        els.tabBar.appendChild(el);
      });

      // "+" new-tab button at the end (PingPlotter-style). Always visible.
      var plus = document.createElement("button");
      plus.type = "button";
      plus.className = "tab-new";
      plus.setAttribute("aria-label", "Open a new trace tab");
      plus.textContent = "+";
      plus.addEventListener("click", function () {
        var fresh = createTab(null);
        add(fresh, true);
        // Focus the target input so the user can start typing immediately.
        if (els.target) els.target.focus();
      });
      els.tabBar.appendChild(plus);
    }

    return {
      isActive: isActive,
      getActive: getActive,
      getTabs: getTabs,
      add: add,
      setActive: setActive,
      close: close,
      renderTabBar: renderTabBar,
    };
  })();

  // ============================================================ createTab
  //
  // One factory invocation per tab. Each tab owns its ``state`` object and
  // the closures over it (all rendering / SSE / snapshot helpers). DOM
  // writes self-guard with ``tabManager.isActive(self)`` so background tabs
  // keep buffering samples without touching the shared DOM. ``sessionInfo``
  // is null for a fresh idle tab, or a summary dict from GET /api/traces
  // for a tab restored on page load.

  function createTab(sessionInfo) {
    var self = {};
    self._id = null; // assigned by tabManager.add()

    // Runtime state — fresh per tab.
    var state = {
      sessionId: null,
      target: null,
      // Resolved target IP/DNS (from snapshot). Used to detect whether the
      // route's final hop reaches the target so a dedicated target row is
      // rendered for the hop-0 stream when it does not.
      target_ip: null,
      target_dns: null,
      // Per-tab interval (seconds). Read from the toolbar when the trace
      // is started; restored into the toolbar when this tab is activated.
      intervalS: 2.5,
      startedAt: null, // epoch ms
      // Pause-freeze timestamp (epoch seconds). Null while running; set to
      // the wall-clock pause instant when pause is confirmed. Restored
      // paused sessions fall back to the latest sample timestamp across
      // hops (or restore time when no samples exist). All data-window
      // "now" reads route through the nowS() closure so the visible
      // window freezes here while paused. The elapsed-time footer is
      // intentionally exempt (PingPlotter keeps elapsed running).
      pauseFreezeS: null,
      hops: [], // [{index, ip, dns}]
      samples: {}, // hopIndex (string) -> [{t, rtt_ms}]  (raw, ≤ RAW_RANGE_MAX_S panels)
      // Slice D2 — rollup bucket buffer per hop, SEPARATE from raw samples
      // (different resolution). 12h/24h panels read from here; ≤ 3h panels
      // read from ``samples``. Both are per-tab and never mixed; the
      // rendering branch is driven by the panel's range (range >
      // RAW_RANGE_MAX_S → bucket rendering).
      buckets: {}, // hopIndex (string) -> [{t, avg_ms, min_ms, max_ms, count, loss_count}]
      seedStats: {}, // hopIndex (string) -> server snapshot stats for the stream
      // Snapshot baseline accounting, per stream key (contract: captured_at
      // is the single effective capture reference — frozen at paused_at
      // while paused; sample_through_t/sample_from_t are extrema of the
      // exact stats window, null when empty).
      seedCut: {}, // hopIndex (string) -> sample_through_t; delta cutoff + SSE watermark
      seedFrom: {}, // hopIndex (string) -> sample_from_t; baseline aging anchor
      seedCap: {}, // hopIndex (string) -> captured_at; latest capture marker (survives empty clears)
      seedFocus: {}, // hopIndex (string) -> focus_s the baseline window used
      seedWatermark: {}, // samples at/below this server-reported cutoff are in the baseline
      // Raw-buffer coverage: earliest t the per-hop buffer is a complete
      // census of the server's samples from (stats-window backfills + live
      // SSE). Wiped the moment the SSE transport breaks and again on
      // reconnect — samples during an outage are missed, so no completeness
      // claim may survive it. covInFlight bounds one outstanding backfill
      // per current stream; covGen is the connection-continuity generation
      // captured by coverage requests and checked on adoption, fencing
      // responses that crossed a break/reconnect from establishing coverage.
      covFrom: {},
      covInFlight: {},
      covGen: 0,
      covConnected: false,
      es: null, // EventSource
      runningKnown: false, // last known session.state == "running"
      routeState: null, // pending | ready | failed
      // Slice 5/6c — timeline UI state.
      panelRangeS: {}, // hopIndex (string) -> seconds (one of RANGES_S)
      focus: null, // {startS, endS} when a drag-selection is active, else null
      timelineHops: {}, // hopIndex (string) -> true for open non-primary timelines
      timelinePanels: {}, // hopIndex (string) -> {panel, body, canvas, ctx, statsEl, rangeSelect}
      // Async-guard counters.
      snapshotRequestSeq: 0,
      // Independent generation for history requests. Snapshot fetches are
      // frequent (route discovery, focus changes, reconnect reconciliation)
      // and must not invalidate an otherwise-current history backfill.
      historyRequestSeq: 0,
      // ``controlAction`` is BOTH a status fact (deriveStatus rows 2–3:
      // "Pausing…" / "Resuming…") and an action-guard (prevents a second
      // control POST while one is in flight). Set on request, cleared on
      // confirm/failure/close/reset. While set, snapshotStatePolicy defers
      // state adoption — the POST response owns the runningKnown flip.
      controlAction: null,
      // Status FACTS consumed by PURE.deriveStatus. ``lastError`` is the
      // single durable failure channel (control/start/reset failures,
      // validation errors); ``reconnecting`` is the SSE-transport-down
      // signal. ``errorPinnedForReconcile`` is the one-shot clearing delay
      // that keeps a control failure visible through its reconciliation
      // snapshot — a stateful policy deriveStatus cannot express, kept as
      // a fact rather than a write-guard.
      lastError: null,
      reconnecting: false,
      errorPinnedForReconcile: false,
      startRequestSeq: 0,
      startInFlight: false, // deriveStatus row 4 ("Starting…")
      controlRequestSeq: 0,
      closing: false,
      resetting: false, // deriveStatus row 1 ("Resetting…")
      focusChangeSeq: 0,
      // Slice 6b — toolbar controls (per-tab state).
      // Focus dropdown value (preserved while a manual drag-focus is active;
      // the dropdown's visible value is forced to "auto" during manual focus).
      focusWindowValue: "auto",
      // Toolbar inputs typed on an idle fresh tab are preserved when the
      // user switches away, so they're still there on switch-back.
      pendingTarget: "",
      pendingInterval: 2.5,
      // Per-tab status (text, kind) so the toolbar status line can be
      // restored on activate.
      status: { text: "Idle", kind: null },
      // Slice B — persistent (background) collection state. ``persistent``
      // mirrors the server-side flag; ``name`` is the user-visible label
      // (null -> fall back to target). Both ride the snapshot-adoption
      // path so a second window toggling persistence converges here on
      // the next snapshot (same machinery as pause state).
      persistent: false,
      name: null,
      // A persist POST in flight. Prevents a re-entrant toggle (mirrors
      // the ``controlAction`` guard on pause/resume) and lets the tab-bar
      // marker render optimistically while the round-trip confirms.
      persistInFlight: false,
      // Server-side internal classification of hops + the resolved target
      // (snapshot ``internal`` / ``target_internal``). Drives the
      // display-only DNS masking when the toolbar toggle is on.
      target_internal: false,
      // ICMP non-responsive (trace-local): sticky "this hop has replied at
      // least once" facts (keyed by hop index) + the server's lifetime
      // flag from the latest snapshot hops. Cleared with the buffers on
      // reset/new trace; new hops get fresh entries on discovery.
      hopEverReplied: {},
      hopNr: {},
      // InfluxDB throughput overlay (primary timeline only): toggle,
      // fetched points [{t, in_mbps, out_mbps}], server bucket step, error
      // text, request generation, and the last fetch time (refresh cadence).
      throughputOn: false,
      throughputPoints: [],
      throughputStep: null,
      throughputError: null,
      throughputSeq: 0,
      throughputLastFetchS: 0,
    };

    // Seed state from a restored session.
    if (sessionInfo) {
      state.sessionId = sessionInfo.id;
      state.target = sessionInfo.target;
      state.intervalS = sessionInfo.interval_s;
      state.runningKnown = (sessionInfo.state === "running");
      // Restore the server's authoritative pause instant before any history
      // request is built. Otherwise a restored paused tab queries and draws
      // against wall time until its first snapshot arrives.
      if (sessionInfo.state === "paused" &&
          typeof sessionInfo.paused_at === "number" &&
          isFinite(sessionInfo.paused_at)) {
        state.pauseFreezeS = sessionInfo.paused_at;
      }
      state.startedAt = Date.now(); // best-effort; footer "elapsed" runs from here
      state.routeState = "pending";
      // Seed persistent/name from the summary so the tab marker and
      // background-toggle reflect the server-side state at restore. The
      // next snapshot re-asserts both via the snapshot-adoption path.
      state.persistent = !!sessionInfo.persistent;
      state.name = sessionInfo.name || null;
      // Derive the restored status (Running/Paused) from the seeded facts
      // rather than hand-writing it; renderStatus stores state.status
      // regardless of active state and writes the DOM on activate.
      renderStatus();
    }

    // ----- helpers (closures over state) ------------------------------

    function isActive() { return tabManager.isActive(self); }

    /**
     * Per-tab effective "now" (epoch seconds) for every data-window
     * consumer (timeline draw, rolling-stats cutoff, Focus caption
     * bounds, drag-focus mapping, focus expiry). Returns the frozen
     * pause timestamp while paused, wall time while running. Per-tab
     * isolation: each tab's ``state.pauseFreezeS`` is its own, so a
     * paused tab never affects another tab's window. NOT used by the
     * footer's elapsed-time counter.
     */
    function effNowS() {
      return PURE.effectiveNowS(Date.now() / 1000, state.pauseFreezeS);
    }

    /**
     * Single status writer. Computes the toolbar line from the recorded
     * FACTS via PURE.deriveStatus, stores it into state.status (so activate
     * can restore it), and writes the shared toolbar DOM only when this
     * tab is active. Former writers now mutate facts (controlAction,
     * lastError, reconnecting, runningKnown, …) and call this; precedence
     * — not per-writer guards — decides what shows.
     */
    function renderStatus() {
      var derived = PURE.deriveStatus({
        resetting: state.resetting,
        startInFlight: state.startInFlight,
        controlAction: state.controlAction,
        lastError: state.lastError,
        reconnecting: state.reconnecting,
        hasSession: !!state.sessionId,
        runningKnown: state.runningKnown,
      });
      state.status = derived;
      if (isActive()) {
        // Screenshot masking: server error bodies can echo the internal
        // target; mask known names at the single DOM write point.
        setStatus(
          PURE.maskText(derived.text, sessionMaskNames(), maskState.on),
          derived.kind
        );
      }
    }

    function setRouteBanner(text, kind) {
      if (!isActive()) return;
      if (!text) {
        els.routeBanner.hidden = true;
        els.routeBanner.textContent = "";
        els.routeBanner.className = "route-banner";
        return;
      }
      els.routeBanner.hidden = false;
      els.routeBanner.textContent = PURE.maskText(text, sessionMaskNames(), maskState.on);
      els.routeBanner.className = "route-banner route-" + (kind || "pending");
    }

    function startStopLabel() {
      return state.runningKnown ? "Pause" : "Resume";
    }

    function clearTable() {
      while (els.tableBody.firstChild) {
        els.tableBody.removeChild(els.tableBody.firstChild);
      }
    }

    function showEmptyRow(text) {
      clearTable();
      var tr = document.createElement("tr");
      tr.className = "empty-row";
      var td = document.createElement("td");
      td.colSpan = 10; // status + Hop/Count/IP/DNS/Avg/Min/Cur/PL%/Graph
      td.textContent = text;
      tr.appendChild(td);
      els.tableBody.appendChild(tr);
    }

    // ----- Slice 6b: toolbar helpers ----------------------------------

    function currentFocusSeconds() {
      return PURE.focusWindowSeconds(state.focusWindowValue);
    }

    function applyFocusWindowChange(newValue) {
      state.focusWindowValue = newValue;
      ++state.focusChangeSeq;
      // Baselines fetched for the previous window width suspend themselves
      // (seedFocus no longer matches) until the refetched snapshot below
      // re-adopts for the new window; coverage is refreshed for the new
      // window start rather than adding an old aggregate to it.
      // Changing the dropdown clears any active manual drag-focus.
      if (state.focus) {
        state.focus = null;
        renderFocusChip();
      }
      renderFocusCaption();
      renderTable();
      renderTimelines();
      // Re-fetch the snapshot (and per-hop baselines) so seedStats match the
      // new focus window.
      if (state.sessionId) {
        fetchSnapshot();
        if (state.hops.length > 0) hydrateHopBaselines(state.hops);
        refreshStatsCoverage();
      }
    }

    function renderFocusCaption() {
      if (!isActive() || !els.focusCaption) return;
      if (state.focus) {
        els.focusCaption.textContent = PURE.formatFocusCaption(
          "Manual", state.focus.startS, state.focus.endS
        );
        return;
      }
      if (!state.sessionId) {
        els.focusCaption.textContent = "Focus: —";
        return;
      }
      var label = PURE.focusWindowLabel(state.focusWindowValue);
      var nowS = effNowS();
      var focusS = currentFocusSeconds();
      els.focusCaption.textContent = PURE.formatFocusCaption(
        label, nowS - focusS, nowS
      );
    }

    // ----- sample state -------------------------------------------------

    function pushSample(hopIndex, t, rttMs) {
      PURE.pushSample(state, hopIndex, t, rttMs);
    }

    function statsForHop(hopIndex, nowS) {
      return PURE.statsForHopWindow(state, hopIndex, nowS, currentFocusSeconds());
    }

    // ----- ICMP non-responsive + internal-DNS masking (per-tab) ----------

    /** Sticky-record that ``hopIndex`` produced at least one reply. */
    function noteReplied(hopIndex) {
      state.hopEverReplied[String(hopIndex)] = true;
    }

    /** Scan a sample list and sticky-record any successful reply. */
    function noteRepliesInList(hopIndex, list) {
      if (!Array.isArray(list)) return;
      for (var i = 0; i < list.length; i++) {
        var r = list[i] && list[i].rtt_ms;
        if (r !== null && r !== undefined && !isNaN(r)) {
          noteReplied(hopIndex);
          return;
        }
      }
    }

    /** True when ``hop`` represents the trace destination (the target). */
    function hopIsDestination(hop) {
      if (!hop) return false;
      if (hop.targetRow) return true;
      if (hop.index === 0) return true; // hop 0 is always the target stream
      return isTargetHop(hop); // final-hop-is-target case
    }

    /**
     * Neutral "ICMP non-responsive" presentation flag for one hop.
     * Combines the server's lifetime snapshot flag with the client's own
     * live evidence (see PURE.hopNonResponsive) so the presentation flips
     * after the 5th failed round and reverts the moment a reply lands.
     */
    function hopNonResponsiveFlag(hop) {
      if (!hop) return false;
      var key = String(hop.index);
      return PURE.hopNonResponsive({
        isDestination: hopIsDestination(hop),
        serverFlag: !!state.hopNr[key],
        samples: state.samples[key] || [],
        seed: state.seedStats[key] || null,
        everReplied: !!state.hopEverReplied[key],
      });
    }

    /** Server-side internal classification for a hop (target stream →
     *  the resolved target's flag). */
    function hopInternal(hop) {
      if (!hop) return false;
      if (hop.index === 0) return !!state.target_internal;
      return !!hop.internal;
    }

    /** Known internal names of THIS session (empty until the server
     *  classifies the target internal). Shared by every masked render:
     *  target field, tab label, toolbar status, route banner. */
    function sessionMaskNames() {
      return PURE.sessionMaskNames(state.target, state.target_dns, state.target_internal);
    }

    // ----- focus window -------------------------------------------------

    function statsForHopDisplay(hopIndex, nowS) {
      return PURE.displayStatsForHop(state, hopIndex, nowS, currentFocusSeconds());
    }

    function checkFocusExpiry(nowS) {
      if (!state.focus) return;
      if (PURE.focusExpired(state.focus, nowS)) {
        clearFocus();
      }
    }

    function setFocus(startS, endS) {
      state.focus = { startS: startS, endS: endS };
      // Manual focus overrides the Focus dropdown: force the dropdown's
      // visible value to "auto" while a manual selection is active. The user's
      // selected dropdown value is preserved in state.focusWindowValue and
      // restored on clearFocus().
      if (els.focusWindow) els.focusWindow.value = "auto";
      renderFocusChip();
      renderFocusCaption();
    }

    function clearFocus() {
      if (!state.focus) return;
      state.focus = null;
      // Restore the dropdown's visible value to the user's preserved selection.
      if (els.focusWindow) els.focusWindow.value = state.focusWindowValue;
      renderFocusChip();
      renderFocusCaption();
      renderTable();
      renderTimelines();
    }

    function renderFocusChip() {
      if (!isActive() || !els.focusChip) return;
      if (!state.focus) {
        els.focusChip.hidden = true;
        els.focusChipText.textContent = "Focus: --";
        return;
      }
      els.focusChip.hidden = false;
      els.focusChipText.textContent =
        "Focus: " + PURE.formatClock(state.focus.startS) +
        "–" + PURE.formatClock(state.focus.endS);
    }

    // ----- timelines ----------------------------------------------------

    function primaryHopIndex() {
      return PURE.primaryHopIndex(state.hops, state.target_ip, state.routeState);
    }

    function targetRowNeeded() {
      return PURE.targetRowNeeded(state.hops, state.target_ip, state.routeState);
    }

    function desiredTimelineHops() {
      var out = {};
      var primary = primaryHopIndex();
      out[String(primary)] = true;
      var known = {};
      if (state.hops.length > 0) {
        for (var i = 0; i < state.hops.length; i++) known[String(state.hops[i].index)] = true;
      } else {
        known["0"] = true;
      }
      Object.keys(state.timelineHops).forEach(function (key) {
        if (known[key]) out[key] = true;
      });
      return out;
    }

    function findHop(hopIndex) {
      for (var i = 0; i < state.hops.length; i++) {
        if (state.hops[i].index === hopIndex) return state.hops[i];
      }
      // Hop 0 is the target stream: prefer the resolved IP/DNS (available
      // from the snapshot) so the target-only / target-row labels carry the
      // real address rather than the raw user input.
      if (hopIndex === 0) {
        return {
          index: 0,
          ip: state.target_ip || state.target,
          dns: state.target_dns,
        };
      }
      return { index: hopIndex, ip: state.target, dns: null };
    }

    function timelineLabel(hopIndex) {
      var hop = findHop(hopIndex);
      // Display-only masking: an internal host's DNS name becomes the
      // neutral alias while the screenshot toggle is on (IPs pass through).
      var name = hop.dns || "";
      if (name) name = PURE.maskDnsName(name, hopInternal(hop), maskState.on);
      if (!name) name = hop.ip || "*";
      if (hopIndex === primaryHopIndex()) {
        // For the hop-0 target stream, omit the "· hop N" suffix — hop 0 is
        // the target, not a numbered route hop. When the primary is the
        // discovered final hop, include its index for reference.
        var suffix = "";
        if (state.hops.length > 0 && hopIndex !== 0) {
          suffix = " · hop " + hopIndex;
        }
        return "Target" + suffix + " — " + name;
      }
      return "Hop " + hopIndex + " — " + name;
    }

    function renderTimelines() {
      if (!isActive() || !els.timelineStack) return;
      var visible = !!state.sessionId;
      els.timelinesSection.hidden = !visible;
      if (!visible) return;

      var desired = desiredTimelineHops();
      var primary = String(primaryHopIndex());

      // Drop panels no longer desired.
      Object.keys(state.timelinePanels).forEach(function (key) {
        if (!desired[key]) {
          els.timelineStack.removeChild(state.timelinePanels[key].panel);
          delete state.timelinePanels[key];
        }
      });

      // Build panels in desired order (primary first, then numerical).
      var keys = Object.keys(desired).sort(function (a, b) {
        if (a === primary) return -1;
        if (b === primary) return 1;
        return Number(a) - Number(b);
      });
      keys.forEach(function (key, idx) {
        var entry = state.timelinePanels[key];
        if (!entry) {
          entry = buildTimelinePanel(Number(key), key === primary);
          state.timelinePanels[key] = entry;
          els.timelineStack.appendChild(entry.panel);
        }
        // Route resolution can rebind the primary role from hop 0 to the final
        // hop (or back). Keep existing panels' role-dependent chrome and saved
        // per-hop selection synchronized when that happens.
        entry.panel.querySelector(".timeline-label").textContent = timelineLabel(Number(key));
        entry.rangeSelect.value = String(rangeForPanel(Number(key)));
        entry.rangeSelect.setAttribute("aria-label", "Timeline range for hop " + key);
        entry.panel.querySelector(".timeline-close").hidden = key === primary;
        // Throughput toggle reflects global availability (settings) and this
        // tab's overlay state (panels are rebuilt on tab switches).
        if (entry.tpCheck) {
          entry.tpCheck.disabled = !serverSettings.influxUrl;
          entry.tpCheck.checked = !!state.throughputOn;
        }
        // Ensure correct DOM order.
        if (entry.panel.parentNode === els.timelineStack &&
            els.timelineStack.children[idx] !== entry.panel) {
          els.timelineStack.insertBefore(entry.panel, els.timelineStack.children[idx]);
        }
        drawTimeline(entry, Number(key), key === primary);
      });
    }

    function rangeForPanel(hopIndex) {
      var v = state.panelRangeS[String(hopIndex)];
      var opt = v === undefined ? null : PURE.rangeOptionFor(v);
      return opt ? opt.seconds : DEFAULT_RANGE_S;
    }

    function buildTimelinePanel(hopIndex, isPrimary) {
      var panel = document.createElement("div");
      panel.className = "timeline-panel";
      panel.setAttribute("data-hop", String(hopIndex));

      var header = document.createElement("div");
      header.className = "timeline-header";

      var label = document.createElement("span");
      label.className = "timeline-label";
      label.textContent = timelineLabel(hopIndex);

      var stats = document.createElement("span");
      stats.className = "timeline-stats";

      // Throughput overlay legend (primary only; populated by
      // updateThroughputLegend during each draw).
      var tpLegend = document.createElement("span");
      tpLegend.className = "timeline-tp-legend";
      tpLegend.hidden = true;

      // Throughput overlay toggle (primary panel only). Disabled until an
      // InfluxDB source is configured in settings.
      var tpCheck = null;
      var tpWrap = null;
      if (isPrimary) {
        tpWrap = document.createElement("label");
        tpWrap.className = "timeline-throughput";
        tpCheck = document.createElement("input");
        tpCheck.type = "checkbox";
        tpCheck.id = "throughput-toggle";
        tpCheck.title = "Overlay interface throughput (Mbps) from the configured InfluxDB";
        var tpText = document.createElement("span");
        tpText.textContent = "Throughput";
        tpWrap.appendChild(tpCheck);
        tpWrap.appendChild(tpText);
        tpCheck.addEventListener("change", function () {
          state.throughputOn = !!tpCheck.checked;
          state.throughputPoints = [];
          state.throughputError = null;
          if (state.throughputOn) fetchThroughput();
          renderTimelines();
        });
      }

      // Slice 6c — per-panel range dropdown at the top-right of the header.
      var rangeSelect = document.createElement("select");
      rangeSelect.className = "timeline-range";
      rangeSelect.id = "timeline-range-" + hopIndex;
      rangeSelect.setAttribute("aria-label", "Timeline range for hop " + hopIndex);
      rangeSelect.setAttribute("data-hop", String(hopIndex));
      PURE.RANGE_OPTIONS.forEach(function (opt) {
        var o = document.createElement("option");
        o.value = opt.value;
        o.textContent = opt.label;
        if (opt.seconds === DEFAULT_RANGE_S) o.selected = true;
        rangeSelect.appendChild(o);
      });
      rangeSelect.addEventListener("change", function () {
        state.panelRangeS[String(hopIndex)] = Number(rangeSelect.value);
        // Slice D1 — backfill history when widening the range so the
        // newly-visible window is populated from the server's retained
        // raw samples instead of waiting for new SSE rounds. The merge is
        // idempotent so narrowing-then-widening works cleanly.
        if (state.sessionId) fetchHistory(hopIndex);
        // Throughput overlay follows the primary panel's range.
        if (state.throughputOn && String(hopIndex) === String(primaryHopIndex())) {
          fetchThroughput();
        }
        var entry = state.timelinePanels[String(hopIndex)];
        if (entry) drawTimeline(entry, hopIndex, String(hopIndex) === String(primaryHopIndex()));
      });

      var close = document.createElement("button");
      close.type = "button";
      close.className = "timeline-close";
      close.setAttribute("aria-label", "Close timeline for hop " + hopIndex);
      close.textContent = "×";
      close.hidden = isPrimary; // Primary timeline has no close button.
      close.addEventListener("click", function () {
        delete state.timelineHops[String(hopIndex)];
        renderTimelines();
      });

      header.appendChild(label);
      header.appendChild(stats);
      header.appendChild(tpLegend);
      if (tpWrap) header.appendChild(tpWrap);
      header.appendChild(rangeSelect);
      header.appendChild(close);

      // Slice 6c — body wraps the canvas with rotated axis labels alongside
      // it: "Latency (ms)" on the left, "Packet Loss %" in red on the right.
      var body = document.createElement("div");
      body.className = "timeline-body";

      var latencyLabel = document.createElement("span");
      latencyLabel.className = "timeline-axis-label timeline-latency-label";
      latencyLabel.setAttribute("aria-hidden", "true");
      latencyLabel.textContent = "Latency (ms)";

      var lossLabel = document.createElement("span");
      lossLabel.className = "timeline-axis-label timeline-loss-label";
      lossLabel.setAttribute("aria-hidden", "true");
      lossLabel.textContent = "Packet Loss %";

      var canvas = document.createElement("canvas");
      canvas.className = "timeline-canvas";
      canvas.setAttribute("data-hop", String(hopIndex));
      attachDragHandlers(canvas, hopIndex);

      body.appendChild(latencyLabel);
      body.appendChild(canvas);
      body.appendChild(lossLabel);

      panel.appendChild(header);
      panel.appendChild(body);
      return {
        panel: panel,
        body: body,
        canvas: canvas,
        ctx: canvas.getContext("2d"),
        statsEl: stats,
        rangeSelect: rangeSelect,
        latencyLabel: latencyLabel,
        lossLabel: lossLabel,
        tpCheck: tpCheck,
        tpLegend: tpLegend,
        plotPadR: TL_PAD_R,
      };
    }

    function setupCanvas(canvas) {
      var dpr = Math.max(TL_DPR_MIN, Math.min(TL_DPR_MAX, window.devicePixelRatio || 1));
      var cssW = Math.max(0, canvas.clientWidth || canvas.parentElement.clientWidth || 0);
      var cssH = Math.max(0, canvas.clientHeight || 0);
      if (cssW === 0 || cssH === 0) return null;
      if (canvas.width !== Math.round(cssW * dpr) || canvas.height !== Math.round(cssH * dpr)) {
        canvas.width = Math.round(cssW * dpr);
        canvas.height = Math.round(cssH * dpr);
      }
      var ctx = canvas.getContext("2d");
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      return { ctx: ctx, w: cssW, h: cssH };
    }

    function drawTimeline(entry, hopIndex, isPrimary) {
      var setup = setupCanvas(entry.canvas);
      if (!setup) return;
      var ctx = setup.ctx;
      var w = setup.w;
      var h = setup.h;

      // Throughput overlay (primary panel only, user-toggled): widen the
      // right padding for the separate Mbps axis. ``plotBounds`` reads the
      // per-entry pad so focus drags stay mapped to the same plot area.
      var overlay = isPrimary && state.throughputOn;
      var padR = overlay ? 92 : TL_PAD_R;
      entry.plotPadR = padR;

      var plotW = Math.max(0, w - TL_PAD_L - padR);
      var plotH = Math.max(0, h - TL_PAD_T - TL_PAD_B);

      var nowS = effNowS();
      var rangeS = rangeForPanel(hopIndex);
      var win = PURE.rangeWindow(nowS, rangeS);
      var endS = win.endS;
      var startS = win.startS;

      // Neutral "ICMP non-responsive" presentation for this hop's timeline
      // (gray loss marks + descriptive caption; counts/loss preserved).
      var nr = hopNonResponsiveFlag(findHop(hopIndex));

      ctx.clearRect(0, 0, w, h);

      // Slice D2 — rendering branch. Ranges > RAW_RANGE_MAX_S render from
      // the SEPARATE bucket buffer (state.buckets); shorter ranges keep
      // the D1 raw path. Table stats, seedStats, focus windows, and the
      // graph column remain raw-only and are not touched here.
      var bucketed = rangeS > RAW_RANGE_MAX_S;
      var visible = [];
      var visibleBuckets = [];
      var maxRtt = null;
      if (bucketed) {
        var buckets = state.buckets[String(hopIndex)] || [];
        for (var bi = 0; bi < buckets.length; bi++) {
          var b = buckets[bi];
          if (b.t < startS || b.t > endS) continue;
          visibleBuckets.push(b);
          if (b.avg_ms !== null && b.avg_ms !== undefined && !isNaN(b.avg_ms)) {
            if (maxRtt === null || b.avg_ms > maxRtt) maxRtt = b.avg_ms;
          }
        }
      } else {
        var samples = state.samples[String(hopIndex)] || [];
        for (var i = 0; i < samples.length; i++) {
          var s = samples[i];
          if (s.t < startS || s.t > endS) continue;
          visible.push(s);
          if (s.rtt_ms !== null && s.rtt_ms !== undefined && !isNaN(s.rtt_ms)) {
            if (maxRtt === null || s.rtt_ms > maxRtt) maxRtt = s.rtt_ms;
          }
        }
      }
      var axisMax = PURE.yAxisMax(maxRtt);
      var bands = PURE.bandBoundaries();

      drawBands(ctx, TL_PAD_L, TL_PAD_T, plotW, plotH, axisMax, bands);
      drawYAxis(ctx, TL_PAD_L, TL_PAD_T, plotW, plotH, axisMax);
      drawXAxis(ctx, TL_PAD_L, TL_PAD_T, plotW, plotH, startS, endS, rangeS);

      ctx.save();
      ctx.beginPath();
      ctx.rect(TL_PAD_L, TL_PAD_T, plotW, plotH);
      ctx.clip();

      if (bucketed) {
        drawBucketLine(ctx, TL_PAD_L, TL_PAD_T, plotW, plotH,
                       visibleBuckets, endS, rangeS, axisMax, nr);
      } else {
        drawRttLine(ctx, TL_PAD_L, TL_PAD_T, plotW, plotH, visible, endS, rangeS, axisMax);
        drawLossMarks(ctx, TL_PAD_L, TL_PAD_T, plotW, plotH, visible, endS, rangeS, nr);
      }

      // Throughput overlay sits on top of the latency/loss layers (primary
      // panel, toggled). Null buckets and gaps larger than the bucket
      // cadence BREAK the direction lines (never bridged, never zero);
      // failures surface in the legend instead. Only the data lines draw
      // inside the clip — the right-hand axis labels live at x+w+4, past
      // the plot rect, and are painted after ctx.restore below.
      if (overlay) {
        drawThroughput(ctx, TL_PAD_L, TL_PAD_T, plotW, plotH,
                       state.throughputPoints, endS, rangeS, state.throughputStep || 0);
      }

      if (state.focus) {
        drawFocusOverlay(ctx, TL_PAD_L, TL_PAD_T, plotW, plotH, state.focus, endS, rangeS);
      }
      ctx.restore();

      // Outside the clip: the Mbps axis column (padR reserve) is not part
      // of the plot rect, and shares axisMax/focus geometry with the
      // clipped lines via the same PURE.mbpsAxisMax(points).
      if (overlay) {
        drawThroughputAxis(ctx, TL_PAD_L, TL_PAD_T, plotW, plotH, state.throughputPoints);
      }

      var caption = bucketed
        ? buildBucketCaption(visibleBuckets)
        : buildCaption(visible);
      if (nr) caption += " · ICMP non-responsive";
      entry.statsEl.textContent = caption;
      updateThroughputLegend(entry, overlay);
    }

    /**
     * Update the primary panel's throughput legend line: colored
     * direction swatches when data is flowing, a clear message when the
     * overlay is unconfigured/unreachable/empty. Never throws — the
     * latency chart must keep working regardless of overlay state.
     */
    function updateThroughputLegend(entry, overlay) {
      var el = entry.tpLegend;
      if (!el) return;
      if (!overlay) {
        el.hidden = true;
        return;
      }
      el.hidden = false;
      while (el.firstChild) el.removeChild(el.lastChild);
      if (state.throughputError) {
        el.className = "timeline-tp-legend tp-error";
        el.textContent = "throughput unavailable (" + state.throughputError + ")";
        return;
      }
      el.className = "timeline-tp-legend";
      if (!state.throughputPoints || state.throughputPoints.length === 0) {
        el.textContent = "throughput: no data";
        return;
      }
      var down = document.createElement("span");
      down.className = "tp-in";
      down.textContent = "▼ in";
      var up = document.createElement("span");
      up.className = "tp-out";
      up.textContent = "▲ out";
      el.appendChild(down);
      el.appendChild(document.createTextNode(" · "));
      el.appendChild(up);
      var unit = document.createElement("span");
      unit.className = "tp-unit";
      unit.textContent = "Mbps";
      el.appendChild(document.createTextNode(" "));
      el.appendChild(unit);
    }

    /**
     * Draw the inbound/outbound Mbps overlay on its own right-hand axis.
     * Two distinguishable step lines (blue = in/download, orange =
     * out/upload). Geometry comes from PURE.tpSegments: the path BREAKS at
     * null values (missing buckets) and at timestamp gaps larger than the
     * bucket cadence (omitted buckets), so missing data is a gap — never
     * bridged, never fabricated as zero. Linear Mbps→y mapping on the
     * separate right-hand axis (clamped to the plot box).
     */
    function drawThroughput(ctx, x, y, w, h, points, endS, rangeS, stepS) {
      if (!points || points.length === 0 || w <= 0 || h <= 0) return;
      var axisMax = PURE.mbpsAxisMax(points);
      // Two step lines, each split into gap-broken segments per direction.
      // The right-hand axis labels are drawn separately by
      // drawThroughputAxis (unclipped) — this function runs inside the
      // plot-rect clip, which would drop anything painted at x+w+4.
      var series = [
        { key: "in_mbps", color: "#2563eb" },
        { key: "out_mbps", color: "#d97706" },
      ];
      function yFor(val) {
        var yy = y + h - (val / axisMax) * h;
        if (yy < y) yy = y;
        if (yy > y + h) yy = y + h;
        return yy;
      }
      for (var si = 0; si < series.length; si++) {
        var s = series[si];
        var segs = PURE.tpSegments(points, s.key, stepS, endS, rangeS, w, yFor);
        ctx.strokeStyle = s.color;
        ctx.lineWidth = 1.25;
        ctx.lineJoin = "round";
        ctx.lineCap = "round";
        for (var gi = 0; gi < segs.length; gi++) {
          var seg = segs[gi];
          ctx.beginPath();
          ctx.moveTo(seg[0].x + x, seg[0].y);
          for (var k = 1; k < seg.length; k++) {
            ctx.lineTo(seg[k].x + x, seg[k].y);
          }
          ctx.stroke();
          if (seg.length === 1) {
            // Lone point in a segment: no line is drawable; mark it.
            ctx.fillStyle = s.color;
            ctx.beginPath();
            ctx.arc(seg[0].x + x, seg[0].y, 1.6, 0, Math.PI * 2);
            ctx.fill();
          }
        }
      }
    }

    /**
     * Right-hand Mbps axis labels (0 / mid / max) painted in the padR
     * column OUTSIDE the plot-rect clip — drawTimeline calls this after
     * ctx.restore, so the labels are actually visible (they sit at
     * x+w+4, past the clip edge). Same PURE.mbpsAxisMax scale as the
     * clipped data lines, keeping the overlay and axis aligned.
     */
    function drawThroughputAxis(ctx, x, y, w, h, points) {
      if (!points || points.length === 0 || w <= 0 || h <= 0) return;
      var axisMax = PURE.mbpsAxisMax(points);
      ctx.fillStyle = "#5b6470";
      ctx.font = "10px ui-monospace, SF Mono, Menlo, Consolas, monospace";
      ctx.textAlign = "left";
      ctx.textBaseline = "middle";
      var labels = PURE.tpAxisLabels(axisMax);
      for (var li = 0; li < labels.length; li++) {
        var py = y + h - (labels[li].v / axisMax) * h;
        if (py < y) py = y;
        if (py > y + h) py = y + h;
        ctx.fillText(labels[li].text, x + w + 4, py);
      }
    }

    function drawBands(ctx, x, y, w, h, axisMax, bands) {
      var gY = PURE.msToY(bands.green, axisMax, h) + y;
      var yY = PURE.msToY(bands.yellow, axisMax, h) + y;
      ctx.fillStyle = "rgba(44, 143, 62, 0.10)";
      ctx.fillRect(x, gY, w, Math.max(0, y + h - gY));
      ctx.fillStyle = "rgba(176, 123, 0, 0.10)";
      ctx.fillRect(x, yY, w, Math.max(0, gY - yY));
      ctx.fillStyle = "rgba(179, 38, 30, 0.10)";
      ctx.fillRect(x, y, w, Math.max(0, yY - y));
      ctx.strokeStyle = "rgba(0, 0, 0, 0.18)";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(x, gY + 0.5);
      ctx.lineTo(x + w, gY + 0.5);
      ctx.moveTo(x, yY + 0.5);
      ctx.lineTo(x + w, yY + 0.5);
      ctx.stroke();
    }

    function drawYAxis(ctx, x, y, w, h, axisMax) {
      var step = PURE.yTickStepMs(axisMax, 5);
      ctx.strokeStyle = "rgba(0, 0, 0, 0.08)";
      ctx.fillStyle = "#5b6470";
      ctx.font = "10px ui-monospace, SF Mono, Menlo, Consolas, monospace";
      ctx.textAlign = "right";
      ctx.textBaseline = "middle";
      ctx.lineWidth = 1;
      for (var v = 0; v <= axisMax + 0.5; v += step) {
        var py = PURE.msToY(v, axisMax, h) + y;
        ctx.beginPath();
        ctx.moveTo(x, py + 0.5);
        ctx.lineTo(x + w, py + 0.5);
        ctx.stroke();
        ctx.fillText(String(Math.round(v)), x - 6, py);
      }
    }

    function drawXAxis(ctx, x, y, w, h, startS, endS, rangeS) {
      var step = PURE.tickStepS(rangeS, 6);
      var ticks = PURE.xTicks(startS, endS, step);
      ctx.strokeStyle = "rgba(0, 0, 0, 0.08)";
      ctx.fillStyle = "#5b6470";
      ctx.font = "10px ui-monospace, SF Mono, Menlo, Consolas, monospace";
      ctx.textAlign = "center";
      ctx.textBaseline = "top";
      ctx.lineWidth = 1;
      var labelY = y + h + 4;
      // Slice D1: multi-hour ranges (rangeS > 600) use HH:MM labels (denser
      // data, coarser grid) and render a date-boundary marker when the visible
      // window crosses local midnight so the operator can tell which day a
      // tick belongs to. Short ranges keep the original HH:MM:SS labels.
      // Slice D2: 12h/24h windows can cross one midnight (and the first tick
      // is anchored with its date too, so a long window shows date prefixes
      // on BOTH the starting date and the new-date boundary — two date
      // labels for a 24h window that straddles midnight).
      var long = rangeS > 600;
      var crossMid = long && PURE.crossesMidnight(startS, endS);
      var windowStartDate = long ? PURE.formatDateMDY(startS) : null;
      var prevDate = null;
      for (var i = 0; i < ticks.length; i++) {
        var t = ticks[i];
        var px = PURE.timeToX(t, endS, rangeS, w) + x;
        ctx.beginPath();
        ctx.moveTo(px + 0.5, y);
        ctx.lineTo(px + 0.5, y + h);
        ctx.stroke();
        var label = long ? PURE.formatClockHM(t) : PURE.formatClock(t);
        if (crossMid) {
          var tickDate = PURE.formatDateMDY(t);
          // Show the date on the FIRST tick (so the window's starting date
          // is anchored even when every tick would otherwise be HH:MM),
          // and on every subsequent calendar-date change. A 1h/3h window
          // crosses at most one midnight (one date prefix beyond the
          // anchor); a 24h window starting mid-day crosses one midnight
          // (two prefixes total: start anchor + new-date boundary).
          if (i === 0 || (tickDate !== windowStartDate && tickDate !== prevDate)) {
            label = tickDate + " " + label;
          }
          prevDate = tickDate;
        }
        ctx.fillText(label, px, labelY);
      }
    }

    function drawRttLine(ctx, x, y, w, h, samples, endS, rangeS, axisMax) {
      if (samples.length === 0) return;
      var path = PURE.stepPath(samples, endS, rangeS, w, axisMax, h);
      if (path.length === 0) return;
      ctx.strokeStyle = "#1a1d21";
      ctx.lineWidth = 1.25;
      ctx.lineJoin = "round";
      ctx.lineCap = "round";
      ctx.beginPath();
      ctx.moveTo(path[0].x + x, path[0].y + y);
      for (var i = 1; i < path.length; i++) {
        ctx.lineTo(path[i].x + x, path[i].y + y);
      }
      ctx.stroke();
      if (path.length === 1) {
        ctx.fillStyle = "#1a1d21";
        ctx.beginPath();
        ctx.arc(path[0].x + x, path[0].y + y, 1.6, 0, Math.PI * 2);
        ctx.fill();
      }
    }

    function drawLossMarks(ctx, x, y, w, h, samples, endS, rangeS, nr) {
      if (samples.length === 0) return;
      // Neutral gray for an ICMP non-responsive hop: the marks preserve the
      // real loss record without the alarming red.
      ctx.strokeStyle = nr ? "#9aa3ad" : "#b3261e";
      ctx.lineWidth = 1;
      ctx.beginPath();
      for (var i = 0; i < samples.length; i++) {
        var s = samples[i];
        if (s.rtt_ms === null || s.rtt_ms === undefined || isNaN(s.rtt_ms)) {
          var px = PURE.timeToX(s.t, endS, rangeS, w) + x;
          ctx.moveTo(px + 0.5, y);
          ctx.lineTo(px + 0.5, y + h);
        }
      }
      ctx.stroke();
    }

    // Slice D2 — bucketed-timeline renderer. A step line of ``avg_ms``
    // (bridging over omitted/empty buckets as the raw path bridges losses)
    // plus a red loss mark at every bucket t with loss_count > 0. Full-loss
    // buckets (no successful samples in the window) render ONLY the loss
    // mark — the line has no point for them. Loss marks are drawn at the
    // bucket START x (the bucket's anchor t). No min/max band in this slice
    // (the rollup envelope carries them but the renderer keeps the D1
    // visual: one step line + red marks).
    function drawBucketLine(ctx, x, y, w, h, buckets, endS, rangeS, axisMax, nr) {
      if (buckets.length === 0) return;
      var drawable = PURE.bucketsToDrawablePoints(
        buckets, endS, rangeS, w, axisMax, h
      );
      // Step line of avg_ms.
      if (drawable.line.length > 0) {
        ctx.strokeStyle = "#1a1d21";
        ctx.lineWidth = 1.25;
        ctx.lineJoin = "round";
        ctx.lineCap = "round";
        ctx.beginPath();
        ctx.moveTo(drawable.line[0].x + x, drawable.line[0].y + y);
        for (var i = 1; i < drawable.line.length; i++) {
          ctx.lineTo(drawable.line[i].x + x, drawable.line[i].y + y);
        }
        ctx.stroke();
        // Single-bucket case: draw a small dot so the line is visible.
        if (drawable.line.length === 1) {
          ctx.fillStyle = "#1a1d21";
          ctx.beginPath();
          ctx.arc(drawable.line[0].x + x, drawable.line[0].y + y, 1.6, 0, Math.PI * 2);
          ctx.fill();
        }
      }
      // Loss marks (covers full-loss AND partial-loss buckets).
      if (drawable.lossMarks.length === 0) return;
      ctx.strokeStyle = nr ? "#9aa3ad" : "#b3261e";
      ctx.lineWidth = 1;
      ctx.beginPath();
      for (var m = 0; m < drawable.lossMarks.length; m++) {
        var px = drawable.lossMarks[m].x + x;
        ctx.moveTo(px + 0.5, y);
        ctx.lineTo(px + 0.5, y + h);
      }
      ctx.stroke();
    }

    function drawFocusOverlay(ctx, x, y, w, h, focus, endS, rangeS) {
      var visibleStart = endS - rangeS;
      if (focus.endS < visibleStart || focus.startS > endS) return;
      var xStart = PURE.timeToX(focus.startS, endS, rangeS, w) + x;
      var xEnd = PURE.timeToX(focus.endS, endS, rangeS, w) + x;
      var left = Math.min(xStart, xEnd);
      var width = Math.abs(xEnd - xStart);
      if (width <= 0) return;
      ctx.fillStyle = "rgba(37, 99, 235, 0.15)";
      ctx.fillRect(left, y, width, h);
      ctx.strokeStyle = "rgba(37, 99, 235, 0.75)";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(left + 0.5, y);
      ctx.lineTo(left + 0.5, y + h);
      ctx.moveTo(left + width + 0.5, y);
      ctx.lineTo(left + width + 0.5, y + h);
      ctx.stroke();
    }

    function buildCaption(visible) {
      if (visible.length === 0) return "no samples";
      var stats = PURE._statsFromWindow(visible);
      return (
        "avg " + PURE.formatMs(stats.avg) + " ms" +
        " · cur " + (stats.cur === null ? "ERR" : PURE.formatMs(stats.cur) + " ms") +
        " · loss " + PURE.formatPl(stats.lossPct) + "%" +
        " · n " + stats.count
      );
    }

    // Slice D2 — caption for the bucketed-timeline stats line. Aggregates
    // the visible buckets into a single window (avg over bucket avgs
    // weighted by success count, loss over all samples) so the per-panel
    // header line stays meaningful on 12h/24h ranges. Mirrors the raw
    // caption's shape ("avg … · loss … · n …") so the operator reads one
    // familiar format; the "cur" field is omitted because a bucket has no
    // single "current" sample (its samples are aggregated).
    function buildBucketCaption(visibleBuckets) {
      if (!visibleBuckets || visibleBuckets.length === 0) return "no samples";
      var totalSamples = 0;
      var totalLoss = 0;
      var totalSuccess = 0;
      var weightedSum = 0.0;
      for (var i = 0; i < visibleBuckets.length; i++) {
        var b = visibleBuckets[i];
        totalSamples += b.count || 0;
        totalLoss += b.loss_count || 0;
        var success = (b.count || 0) - (b.loss_count || 0);
        if (success > 0 && b.avg_ms !== null && b.avg_ms !== undefined &&
            !isNaN(b.avg_ms)) {
          totalSuccess += success;
          weightedSum += success * b.avg_ms;
        }
      }
      if (totalSamples === 0) return "no samples";
      var avg = totalSuccess > 0 ? weightedSum / totalSuccess : null;
      var lossPct = (totalLoss / totalSamples) * 100;
      return (
        "avg " + (avg === null ? "—" : PURE.formatMs(avg) + " ms") +
        " · loss " + PURE.formatPl(lossPct) + "%" +
        " · n " + totalSamples +
        " · buckets " + visibleBuckets.length
      );
    }

    function attachDragHandlers(canvas, hopIndex) {
      var dragging = false;
      var startX = 0;
      var lastX = 0;
      var DRAG_THRESHOLD = 4;

      function onDown(ev) {
        if (!state.sessionId) return;
        dragging = true;
        startX = ev.offsetX;
        lastX = ev.offsetX;
        ev.preventDefault();
      }
      function onMove(ev) {
        if (!dragging) return;
        lastX = ev.offsetX;
        drawDragPreview(canvas, hopIndex, startX, lastX);
      }
      function onUp(ev) {
        if (!dragging) return;
        dragging = false;
        var endX = ev.offsetX;
        clearDragPreview(canvas, hopIndex);
        if (Math.abs(endX - startX) < DRAG_THRESHOLD) return; // click, no drag
        applyDragSelection(canvas, hopIndex, startX, endX);
      }
      function onLeave(ev) {
        if (!dragging) return;
        dragging = false;
        if (Math.abs(lastX - startX) < DRAG_THRESHOLD) return;
        clearDragPreview(canvas, hopIndex);
        applyDragSelection(canvas, hopIndex, startX, lastX);
      }
      canvas.addEventListener("mousedown", onDown);
      canvas.addEventListener("mousemove", onMove);
      canvas.addEventListener("mouseup", onUp);
      canvas.addEventListener("mouseleave", onLeave);
    }

    function plotBounds(canvas) {
      var cssW = canvas.clientWidth || 0;
      var cssH = canvas.clientHeight || 0;
      // Use the pad the panel was last drawn with (the throughput overlay
      // widens the right padding for its Mbps axis) so drag x→time mapping
      // matches what is on screen.
      var entry = state.timelinePanels[String(canvas.getAttribute("data-hop"))];
      var padR = entry && entry.plotPadR ? entry.plotPadR : TL_PAD_R;
      return {
        x: TL_PAD_L,
        y: TL_PAD_T,
        w: Math.max(0, cssW - TL_PAD_L - padR),
        h: Math.max(0, cssH - TL_PAD_T - TL_PAD_B),
      };
    }

    function drawDragPreview(canvas, hopIndex, x1, x2) {
      var setup = setupCanvas(canvas);
      if (!setup) return;
      var ctx = setup.ctx;
      var isPrimary = String(hopIndex) === String(primaryHopIndex());
      drawTimeline(state.timelinePanels[String(hopIndex)], hopIndex, isPrimary);

      var b = plotBounds(canvas);
      var left = Math.min(x1, x2);
      var width = Math.max(1, Math.abs(x2 - x1));
      ctx.save();
      ctx.beginPath();
      ctx.rect(b.x, b.y, b.w, b.h);
      ctx.clip();
      ctx.fillStyle = "rgba(37, 99, 235, 0.18)";
      ctx.fillRect(left, b.y, width, b.h);
      ctx.restore();
    }

    function clearDragPreview(canvas, hopIndex) {
      var entry = state.timelinePanels[String(hopIndex)];
      if (!entry) return;
      var isPrimary = String(hopIndex) === String(primaryHopIndex());
      drawTimeline(entry, hopIndex, isPrimary);
    }

    function applyDragSelection(canvas, hopIndex, x1, x2) {
      var b = plotBounds(canvas);
      var nowS = effNowS();
      var rangeS = rangeForPanel(hopIndex);
      var win = PURE.rangeWindow(nowS, rangeS);
      var endS = win.endS;
      var t1 = PURE.xToTime(Math.min(x1, x2) - b.x, endS, rangeS, b.w);
      var t2 = PURE.xToTime(Math.max(x1, x2) - b.x, endS, rangeS, b.w);
      if (t1 > t2) { var tmp = t1; t1 = t2; t2 = tmp; }
      setFocus(t1, t2);
      renderTable();
      renderTimelines();
    }

    function toggleHopTimeline(hopIndex) {
      var key = String(hopIndex);
      if (key === String(primaryHopIndex())) return; // primary is always shown
      if (state.timelineHops[key]) delete state.timelineHops[key];
      else {
        state.timelineHops[key] = true;
        // Slice D1 — backfill history for the newly opened panel so its
        // timeline is populated immediately, not just from new SSE rounds.
        fetchHistory(hopIndex);
      }
      renderTimelines();
    }

    // ----- rendering ----------------------------------------------------

    function fmtIp(ip) {
      return ip || "*";
    }

    function fmtDns(dns, ip) {
      if (dns) return dns;
      if (!ip) return "*";
      return "—";
    }

    function applyCurClass(cell, rttMs, nr) {
      cell.classList.remove("ok", "loss", "nr");
      if (nr) cell.classList.add("nr");
      else if (rttMs === null || rttMs === undefined || isNaN(rttMs)) cell.classList.add("loss");
      else cell.classList.add("ok");
    }

    function applyPlClass(cell, pct) {
      cell.classList.remove("low", "warn", "high");
      cell.classList.add(PURE.plBucket(pct));
    }

    // ----- graph column -------------------------------------------------

    function buildGraphEl() {
      var wrap = document.createElement("div");
      wrap.className = "graph";
      var gz = document.createElement("div");
      gz.className = "graph-zone graph-zone-green";
      var yz = document.createElement("div");
      yz.className = "graph-zone graph-zone-yellow";
      var rz = document.createElement("div");
      rz.className = "graph-zone graph-zone-red";
      wrap.appendChild(gz);
      wrap.appendChild(yz);
      wrap.appendChild(rz);
      return wrap;
    }

    function applyZoneStyles(wrap, axisMax) {
      var zp = PURE.zonePercents(axisMax);
      var g = wrap.querySelector(".graph-zone-green");
      var y = wrap.querySelector(".graph-zone-yellow");
      var r = wrap.querySelector(".graph-zone-red");
      if (g) {
        g.style.left = "0%";
        g.style.width = zp.green + "%";
      }
      if (y) {
        y.style.left = zp.green + "%";
        y.style.width = Math.max(0, zp.yellow - zp.green) + "%";
      }
      if (r) {
        r.style.left = zp.yellow + "%";
        r.style.width = Math.max(0, 100 - zp.yellow) + "%";
      }
    }

    function renderGraph(wrap, stats, axisMax, nr) {
      while (wrap.childNodes.length > 3) wrap.removeChild(wrap.lastChild);
      applyZoneStyles(wrap, axisMax);

      if (!stats || stats.count === 0 || axisMax <= 0) {
        var empty = document.createElement("span");
        empty.className = "graph-empty";
        empty.textContent = "—";
        wrap.appendChild(empty);
        return;
      }

      var allLost = stats.avg === null && stats.min === null && stats.max === null;
      if (stats.lossPct > 0) {
        var loss = document.createElement("div");
        // Neutral gray hatch for an ICMP non-responsive hop (counts and
        // loss percentages are preserved — only the alarm color changes).
        loss.className = "graph-loss" + (nr ? " nr" : "");
        loss.style.width = PURE.axisPercent(
          Math.min(stats.lossPct, 100) / 100 * axisMax,
          axisMax
        ) + "%";
        wrap.appendChild(loss);
      }
      if (allLost) return;

      var wp = PURE.whiskerPercents(stats.min, stats.max, axisMax);
      if (wp) {
        var whisker = document.createElement("div");
        whisker.className = "graph-whisker";
        whisker.style.left = wp.left + "%";
        whisker.style.width = Math.max(wp.right - wp.left, 0.2) + "%";
        wrap.appendChild(whisker);

        var capL = document.createElement("div");
        capL.className = "graph-cap graph-cap-left";
        capL.style.left = wp.left + "%";
        wrap.appendChild(capL);

        var capR = document.createElement("div");
        capR.className = "graph-cap graph-cap-right";
        capR.style.left = wp.right + "%";
        wrap.appendChild(capR);
      }

      if (stats.avg !== null) {
        var avg = document.createElement("div");
        avg.className = "graph-avg";
        avg.style.left = PURE.axisPercent(stats.avg, axisMax) + "%";
        avg.textContent = "✕";
        wrap.appendChild(avg);
      }

      if (stats.cur !== null) {
        var cur = document.createElement("div");
        cur.className = "graph-cur " + PURE.latencyBucket(stats.cur);
        cur.style.left = PURE.axisPercent(stats.cur, axisMax) + "%";
        wrap.appendChild(cur);
      }
    }

    function statusDotKind(hop, statsData, nr) {
      if (nr) return "gray";
      if (!hop || !hop.ip) return "hollow";
      if (!statsData) return "hollow";
      if (statsData.count > 0 &&
          (statsData.avg === null && statsData.min === null && statsData.max === null)) {
        return "red";
      }
      if (statsData.lossPct >= 100) return "red";
      return "green";
    }

    function isTargetHop(hop) {
      if (!hop || state.hops.length === 0) return false;
      if (hop.index !== state.hops[state.hops.length - 1].index) return false;
      if (targetRowNeeded()) return false;
      return true;
    }

    function renderRow(hop, axisMax, nowS) {
      var tr = document.createElement("tr");
      tr.setAttribute("data-hop", String(hop.index));
      var isFinalTarget = isTargetHop(hop);
      var isTargetRow = !!hop.targetRow;
      if (isFinalTarget || isTargetRow) tr.className = "row-target";

      var stats = statsForHopDisplay(hop.index, nowS);
      var sd = stats ? stats.data : null;
      // Neutral "ICMP non-responsive" presentation (intermediate hops that
      // have never replied in this trace). Counts/loss stay truthful; only
      // the styling and the Cur marker text change.
      var nr = hopNonResponsiveFlag(hop);
      var tdStatus = document.createElement("td");
      tdStatus.className = "cell-status";
      if (isFinalTarget) {
        var glyph = document.createElement("span");
        glyph.className = "status-target-glyph";
        glyph.setAttribute("aria-hidden", "true");
        glyph.textContent = "📈";
        tdStatus.appendChild(glyph);
      }
      var dot = document.createElement("span");
      dot.className = "status-dot status-" + statusDotKind(hop, sd, nr);
      if (nr) dot.title = "ICMP non-responsive";
      tdStatus.appendChild(dot);

      var tdHop = document.createElement("td");
      tdHop.className = "cell-hop num";
      if (isTargetRow) {
        var hopGlyph = document.createElement("span");
        hopGlyph.className = "cell-hop-target-glyph";
        hopGlyph.setAttribute("aria-hidden", "true");
        hopGlyph.textContent = "📈";
        tdHop.appendChild(hopGlyph);
      } else {
        tdHop.textContent = String(hop.index);
      }

      var tdCount = document.createElement("td");
      tdCount.className = "cell-count num";

      var tdIp = document.createElement("td");
      tdIp.className = "cell-ip" + (hop.ip ? "" : " silent");
      tdIp.textContent = fmtIp(hop.ip);

      var tdDns = document.createElement("td");
      tdDns.className = "cell-dns" + (hop.dns ? "" : " silent");
      // Display-only masking: an internal host's DNS name becomes the
      // neutral alias while the screenshot toggle is on. IPs are never
      // masked; absent names keep their "—"/"*" placeholders.
      tdDns.textContent = hop.dns
        ? PURE.maskDnsName(hop.dns, hopInternal(hop), maskState.on)
        : fmtDns(hop.dns, hop.ip);

      var tdAvg = document.createElement("td");
      tdAvg.className = "cell-avg num";

      var tdMin = document.createElement("td");
      tdMin.className = "cell-min num";

      var tdCur = document.createElement("td");
      tdCur.className = "cell-cur num";

      var tdPl = document.createElement("td");
      tdPl.className = "cell-pl num";

      var tdGraph = document.createElement("td");
      tdGraph.className = "cell-graph";
      var graph = buildGraphEl();
      tdGraph.appendChild(graph);

      if (stats) {
        tdCount.textContent = String(sd.count);
        tdAvg.textContent = PURE.formatMs(sd.avg);
        tdMin.textContent = PURE.formatMs(sd.min);
        tdCur.textContent = sd.cur === null ? (nr ? "NR" : "ERR") : PURE.formatMs(sd.cur);
        if (nr) {
          tdCur.title = "ICMP non-responsive: no replies observed in this trace";
        }
        tdPl.textContent = PURE.formatPl(sd.lossPct);
        applyCurClass(tdCur, sd.cur, nr);
        applyPlClass(tdPl, sd.lossPct);
        renderGraph(graph, sd, axisMax, nr);
      } else {
        tdCount.textContent = "0";
        tdAvg.textContent = "—";
        tdMin.textContent = "—";
        tdCur.textContent = "—";
        tdPl.textContent = "0.0";
        renderGraph(graph, null, axisMax);
      }

      tr.appendChild(tdStatus);
      tr.appendChild(tdHop);
      tr.appendChild(tdCount);
      tr.appendChild(tdIp);
      tr.appendChild(tdDns);
      tr.appendChild(tdAvg);
      tr.appendChild(tdMin);
      tr.appendChild(tdCur);
      tr.appendChild(tdPl);
      tr.appendChild(tdGraph);
      return tr;
    }

    function renderConnector(axisMax) {
      var svg = els.graphConnector;
      if (!svg || !els.tableSection) return;
      while (svg.firstChild) svg.removeChild(svg.firstChild);

      var sectionRect = els.tableSection.getBoundingClientRect();
      var bodyRect = els.tableBody.getBoundingClientRect();
      if (bodyRect.width <= 0 || bodyRect.height <= 0) return;

      // Size/position the overlay from the in-cell `.graph` element (the bar
      // area), NOT the `td.cell-graph` border box. The td carries ~8px of cell
      // padding, so the td rect starts ~8px left of the bars; the whisker/avg
      // markers inside `.graph` map x as a percent of the `.graph` content box.
      // Measuring the td shifted every connector x ~8px left of the bars (bug:
      // red line rode the PL/Graph column boundary). Row-center y below still
      // uses the td rect (vertically centered on the row, unchanged).
      var firstRow = els.tableBody.querySelector("tr[data-hop]");
      var firstGraphEl = firstRow
        ? firstRow.querySelector("td.cell-graph .graph")
        : null;
      if (!firstGraphEl) return;
      var graphRect = firstGraphEl.getBoundingClientRect();
      if (graphRect.width <= 0) return;

      var svgLeft = graphRect.left - sectionRect.left;
      var svgTop = bodyRect.top - sectionRect.top;
      var svgWidth = graphRect.width;
      var svgHeight = bodyRect.height;
      svg.style.left = svgLeft + "px";
      svg.style.top = svgTop + "px";
      svg.style.width = svgWidth + "px";
      svg.style.height = svgHeight + "px";
      svg.setAttribute("viewBox", "0 0 " + svgWidth + " " + svgHeight);

      var rows = els.tableBody.querySelectorAll("tr[data-hop]");
      var points = [];
      for (var i = 0; i < rows.length; i++) {
        var tr = rows[i];
        var hopIndex = Number(tr.getAttribute("data-hop"));
        var cell = tr.querySelector("td.cell-graph");
        if (!cell) continue;
        var stats = statsForHopDisplay(hopIndex, effNowS());
        if (!stats || stats.data.avg === null || stats.data.avg === undefined) continue;
        var rowRect = cell.getBoundingClientRect();
        var rowCenter = (rowRect.top - bodyRect.top) + rowRect.height / 2;
        var pt = PURE.connectorPoint(stats.data.avg, axisMax, rowCenter);
        if (!pt) continue;
        points.push({
          x: (pt.x / 100) * svgWidth,
          y: pt.y,
        });
      }
      if (points.length === 0) return;

      var d = "M " + points[0].x.toFixed(2) + " " + points[0].y.toFixed(2);
      for (var k = 1; k < points.length; k++) {
        d += " L " + points[k].x.toFixed(2) + " " + points[k].y.toFixed(2);
      }
      var path = document.createElementNS("http://www.w3.org/2000/svg", "path");
      path.setAttribute("d", d);
      path.setAttribute("class", "graph-connector-path");
      svg.appendChild(path);
    }

    function clearGraphConnector() {
      var svg = els.graphConnector;
      if (!svg) return;
      while (svg.firstChild) svg.removeChild(svg.lastChild);
      svg.style.left = "0px";
      svg.style.top = "0px";
      svg.style.width = "0px";
      svg.style.height = "0px";
      svg.setAttribute("viewBox", "0 0 0 0");
    }

    function renderGraphColumnChrome(axisMax) {
      // The legend bar uses its own scale (floored at LEGEND_FLOOR_MS) so
      // all three zones stay at truthful, legible proportions on healthy
      // traces and the boundary labels do not drift with the data. The row
      // graph-column axis (axisMax) is unchanged — only the legend scale
      // is decoupled here.
      var legendMax = PURE.legendMax(axisMax);
      var zp = PURE.zonePercents(legendMax);
      if (els.legendGreen) {
        els.legendGreen.style.left = "0%";
        els.legendGreen.style.width = zp.green + "%";
      }
      if (els.legendYellow) {
        els.legendYellow.style.left = zp.green + "%";
        els.legendYellow.style.width = Math.max(0, zp.yellow - zp.green) + "%";
      }
      if (els.legendRed) {
        els.legendRed.style.left = zp.yellow + "%";
        els.legendRed.style.width = Math.max(0, 100 - zp.yellow) + "%";
      }
      if (els.legendTickGreen) {
        els.legendTickGreen.style.left = zp.green + "%";
        els.legendTickGreen.textContent = PURE.THRESH_GREEN + "ms";
      }
      if (els.legendTickYellow) {
        els.legendTickYellow.style.left = zp.yellow + "%";
        els.legendTickYellow.textContent = PURE.THRESH_YELLOW + "ms";
      }
      if (els.graphAxisMax) {
        // The column header still reports the ROW axis (axisMax), not the
        // legend scale: the row whiskers/markers/connector all live on
        // axisMax and the header must match them.
        els.graphAxisMax.textContent = Math.round(axisMax) + "ms";
      }
      return legendMax;
    }

    /**
     * Render one diamond marker per hop on the legend bar, positioned at the
     * hop's current Avg (focus-window stats — the same source as the table's
     * Avg column). Target-row marker is larger with a contrasting outline;
     * hops with no avg (silent / no samples in window) get NO marker. Near-
     * coincident markers are fanned into distinct vertical lanes (PURE
     * .legendMarkerLanes) so overlaps stay distinguishable. Each marker
     * carries data-hop / data-avg-ms / data-lane / data-target attributes
     * and a title tooltip for DOM assertion and accessibility.
     */
    function renderLegendMarkers(displayHops, legendMax, nowS) {
      var layer = els.legendMarkers;
      if (!layer) return;
      while (layer.firstChild) layer.removeChild(layer.lastChild);
      if (!displayHops || displayHops.length === 0) return;

      // Build the marker list with legend-scale percent positions. Hop
      // iteration order matches the table (route order, target row last),
      // which is preserved through lane assignment.
      var markers = [];
      for (var i = 0; i < displayHops.length; i++) {
        var hop = displayHops[i];
        var stats = statsForHopDisplay(hop.index, nowS);
        if (!stats || stats.data.avg === null || stats.data.avg === undefined) continue;
        var pct = PURE.legendMarkerPercent(stats.data.avg, legendMax);
        if (pct === null) continue;
        var isTarget = !!hop.targetRow || isTargetHop(hop);
        markers.push({
          id: String(hop.index),
          hopIndex: hop.index,
          isTarget: isTarget,
          avg: stats.data.avg,
          percent: pct,
        });
      }
      var laned = PURE.legendMarkerLanes(markers);
      for (var k = 0; k < laned.length; k++) {
        var m = laned[k];
        var el = document.createElement("span");
        el.className = "legend-marker" + (m.isTarget ? " legend-marker-target" : "");
        el.style.left = m.percent + "%";
        el.style.top = legendMarkerTop(m.lane);
        el.setAttribute("data-hop", m.id);
        el.setAttribute("data-avg-ms", String(Math.round(m.avg * 10) / 10));
        el.setAttribute("data-lane", String(m.lane));
        el.setAttribute("data-target", m.isTarget ? "true" : "false");
        el.title = (m.isTarget ? "target" : "hop " + m.id) +
          " avg " + PURE.formatMs(m.avg) + " ms";
        layer.appendChild(el);
      }
    }

    // Lane -> vertical offset on the bar. Lane 0 is centered; 1 and 2 nudge
    // the marker up/down by 2px so a 3-marker overlap cluster spans ~4px
    // vertically — enough to read as distinct without leaving the bar's
    // visual vicinity. The markers layer is a sibling of the (overflow:
    // hidden) bar inside the wrap, so the small overflow is not clipped.
    function legendMarkerTop(lane) {
      if (lane === 1) return "calc(50% - 2px)";
      if (lane === 2) return "calc(50% + 2px)";
      return "50%";
    }

    function renderTable() {
      if (!isActive()) return;
      var nowS = effNowS();
      checkFocusExpiry(nowS);
      // Every repaint (drag-focus, Focus dropdown, snapshot, SSE round,
      // hydrated baseline) also re-derives copy-summary availability.
      refreshCopySummaryButton();
      var displayHops = state.hops.slice();
      if (displayHops.length === 0 && (state.routeState === "pending" || state.routeState === "failed") &&
          (state.samples["0"] || state.seedStats["0"])) {
        displayHops = [{
          index: 0,
          ip: state.target_ip || state.target,
          dns: state.target_dns,
        }];
      }
      if (targetRowNeeded()) {
        displayHops.push({
          index: 0,
          ip: state.target_ip,
          dns: state.target_dns,
          targetRow: true,
        });
      }
      var hopMaxes = [];
      for (var i = 0; i < displayHops.length; i++) {
        var s = statsForHopDisplay(displayHops[i].index, nowS);
        hopMaxes.push(s ? s.data.max : null);
      }
      var axisMax = PURE.axisMax(hopMaxes);

      var legendMax = renderGraphColumnChrome(axisMax);
      renderLegendMarkers(displayHops, legendMax, nowS);

      clearTable();
      if (displayHops.length === 0) {
        showEmptyRow(state.sessionId
          ? "Waiting for route discovery…"
          : "No trace running. Enter a target and press Start.");
        renderConnector(axisMax);
        return;
      }
      for (var j = 0; j < displayHops.length; j++) {
        els.tableBody.appendChild(renderRow(displayHops[j], axisMax, nowS));
      }
      renderConnector(axisMax);
    }

    // ----- footer status-line (samples/elapsed/last-round) -------------
    // Renamed from renderStatus to free that name for the toolbar status
    // derive (above). This one writes #status-line; the toolbar derive
    // writes #status.
    function renderFooterStatus() {
      if (!isActive()) return;
      var elapsed = "0s";
      if (state.startedAt) {
        var secs = Math.max(0, Math.floor((Date.now() - state.startedAt) / 1000));
        elapsed = secs >= 60
          ? Math.floor(secs / 60) + "m" + (secs % 60) + "s"
          : secs + "s";
      }
      // Authoritative primary-stream facts: the SAME stats source the table
      // and copied summary use for the selected window (never an SSE message
      // counter, which reconnect/history restoration would give a different
      // meaning), and the last committed sample time from the primary
      // stream's raw buffer so restored history does not leave it blank.
      var primary = primaryHopIndex();
      var facts = PURE.primaryStreamFacts(state, primary, effNowS(), currentFocusSeconds());
      var lastRound = "—";
      if (facts.lastT !== null) {
        var d = new Date(facts.lastT * 1000);
        lastRound = d.toLocaleTimeString();
      }
      els.statusLine.textContent =
        "samples " + (facts.stats ? facts.stats.count : 0) +
        " · elapsed " + elapsed +
        " · last round " + lastRound;
    }

    // ----- snapshot -----------------------------------------------------

    function applySnapshot(snap) {
      if (state.closing || state.resetting) return;
      // Generation guard, not a value gate: ``fetchSnapshot`` already
      // rejected this response when a confirmed control transition (or a
      // reset/close) advanced ``snapshotRequestSeq`` past the request's
      // captured generation. So by the time we get here the snapshot is
      // generation-current. We deliberately do NOT compare ``snap.state``
      // against ``runningKnown`` — that value gate locked the UI when
      // another window paused/resumed the shared session (the snapshot is
      // how the other window learns of the change). Route, baseline, and
      // watermark payload data is applied unconditionally; the run/pause
      // state field is adopted only when no control transition is pending.
      state.routeState = snap.route_state;
      state.target = snap.target;
      state.target_ip = snap.target_ip !== undefined ? snap.target_ip : null;
      state.target_dns = snap.target_dns !== undefined ? snap.target_dns : null;
      // Slice B — adopt the persistent flag and label from the server
      // snapshot. This is the multi-window convergence path: another
      // window toggling persistence (or renaming) shows up here on the
      // next snapshot, exactly how running/paused state does. The
      // The ``persistInFlight`` guard owns these optimistic fields until the
      // toggle response confirms and advances the snapshot generation.
      // A persist POST owns these optimistic facts while in flight. Other
      // snapshots may have been captured before or after the server commit;
      // do not let one overwrite the local transition mid-flight.
      if (!state.persistInFlight) {
        if (snap.persistent !== undefined) state.persistent = !!snap.persistent;
        if (snap.name !== undefined) state.name = snap.name || null;
      }
      // Internal-classification adoption (display-only masking input) and
      // the server's lifetime ICMP non-responsive flags. A snapshot is the
      // authority for pre-attach history the client's buffers cannot see.
      if (snap.target_internal !== undefined) state.target_internal = !!snap.target_internal;
      if (snap.hops && snap.hops.length) {
        state.hopNr = {};
        for (var ni = 0; ni < snap.hops.length; ni++) {
          var nh = snap.hops[ni];
          state.hopNr[String(nh.index)] = !!nh.icmp_non_responsive;
          // A baseline average proves the hop has replied at least once —
          // sticky so the fact survives baseline-window expiry.
          if (nh.stats && nh.stats.count > 0 && nh.stats.avg_ms !== null &&
              nh.stats.avg_ms !== undefined) {
            noteReplied(nh.index);
          }
        }
      }
      if (snap.stats && snap.stats.count > 0 && snap.stats.avg_ms !== null &&
          snap.stats.avg_ms !== undefined) {
        noteReplied(0);
      }
      if (snap.hops && snap.hops.length) {
        state.hops = snap.hops.map(function (h) {
          return { index: h.index, ip: h.ip, dns: h.dns, internal: !!h.internal };
        });
        // Per-hop baseline adoption is monotonic in ``captured_at`` (see
        // adoptSnapshotBaseline): a slower per-hop or earlier snapshot
        // response cannot regress a newer baseline, and an empty stream
        // clears its baseline (retaining the capture marker) instead of
        // leaving a stale aggregate an older snapshot could resurrect.
        // Hops the snapshot no longer lists drop their baseline entries
        // outright.
        var snapHopKeys = {};
        for (var i = 0; i < snap.hops.length; i++) {
          var h = snap.hops[i];
          snapHopKeys[String(h.index)] = true;
          PURE.adoptSnapshotBaseline(
            state, String(h.index), h.stats, snap.captured_at,
            h.sample_through_t, h.sample_from_t, PURE.snapshotFocusSeconds(snap)
          );
        }
        ["seedStats", "seedCut", "seedFrom", "seedCap", "seedFocus", "seedWatermark"]
          .forEach(function (map) {
            Object.keys(state[map]).forEach(function (k) {
              if (k !== "0" && !snapHopKeys[k]) delete state[map][k];
            });
          });
      }
      if (snap.stats) {
        // Target stream (hop 0): same contract fields, same single
        // ``captured_at`` reference as the hops[] entries above. The cutoff
        // NEVER falls back to the client receipt time or the latest local
        // sample; a payload without ``captured_at`` is not adopted.
        PURE.adoptSnapshotBaseline(
          state, "0", snap.stats, snap.captured_at,
          snap.sample_through_t, snap.sample_from_t, PURE.snapshotFocusSeconds(snap)
        );
        // Merge the snapshot tail into the timeline buffer so GRAPHS keep
        // the full raw history (the old filter-without-merge blanked
        // timelines on initial load, route discovery, Focus changes, and
        // SSE reconnects). Accounting never subtracts these samples: the
        // baseline's delta cutoff (sample_through_t) already excludes them
        // from the live part of the combined stats, and once the stats
        // window is hydrated the unique raw buffer is the sole statistics
        // source.
        if (Array.isArray(snap.samples) && snap.samples.length > 0) {
          noteRepliesInList(0, snap.samples);
          state.samples["0"] = PURE.mergeSamples(
            state.samples["0"] || [], snap.samples, RAW_CAP
          );
        }
      }
      // State adoption: AUTHORITATIVE when no control transition is pending.
      // A pending transition's POST response (or its ambiguous-failure
      // reconciliation) owns the runningKnown / pauseFreezeS flip, so a
      // snapshot captured mid-flight — which may precede or follow the
      // server-side commit — must not move the local state either way.
      // When authoritative, both flips are adopted (the server is the
      // source of truth), anchoring the freeze to the server's
      // ``paused_at`` when present. The toolbar status is NOT written
      // here: the adopted facts flow through renderStatus at the end of
      // applySnapshot, and PURE.deriveStatus decides what shows.
      if (PURE.snapshotStatePolicy(state.controlAction) === "adopt") {
        if (snap.state === "paused") {
          state.runningKnown = false;
          state.pauseFreezeS = PURE.computePauseFreeze(
            snap.paused_at, state.samples, snap.samples, Date.now() / 1000
          );
        } else if (snap.state === "running") {
          state.runningKnown = true;
          // Running snapshot is authoritative: clear any residual freeze
          // (covers a stale paused-snapshot landing after a resume, and
          // the restore path for a running session).
          state.pauseFreezeS = null;
        }
        // Error-clearing policy (the one stateful piece deriveStatus
        // cannot express). A pinned snapshot is the SINGLE reconciliation
        // fetch issued right after an ambiguous control failure: keep the
        // failure message visible through it (the user has not yet seen
        // the outcome) and just clear the pin. Any later authoritative
        // snapshot confirms the steady state and clears the error so
        // cross-window convergence shows through. The previous guard
        // (``!preserveError || status.kind !== "error"`` around an
        // imperative localSetStatus) is subsumed: precedence gives
        // lastError row 5, so it shows whenever set, and this is the only
        // place snapshots decide to clear it.
        if (state.errorPinnedForReconcile) {
          state.errorPinnedForReconcile = false;
        } else {
          state.lastError = null;
        }
      }
      // The tab label depends on state.target. A background snapshot must not
      // repaint the shared tab-bar DOM; activation refreshes it after the
      // buffered state is restored.
      if (isActive()) tabManager.renderTabBar();
      // Snapshot adoption may flip target_internal (first classification)
      // or rebind target/DNS: repaint the masked target field. Guards on
      // active inside populateToolbar; the value never feeds Start while
      // the session is bound (input disabled).
      populateToolbar();
      pruneStaleTimelines();
      updateRouteBanner();
      refreshButtonLabels();
      // Refresh the selected stats window's raw coverage for every table
      // hop so accounting graduates from the snapshot baseline to the
      // unique raw buffer once the backfill lands (initial hydration and
      // any later adoption that finds the window not fully covered).
      refreshStatsCoverage();
      renderTable();
      renderTimelines();
      renderFocusCaption();
      renderStatus();        // toolbar: derive from adopted facts
      renderFooterStatus();  // footer: samples/elapsed/last-round
    }

    function updateRouteBanner() {
      if (!isActive()) return;
      if (!state.sessionId) {
        setRouteBanner(null);
        return;
      }
      if (state.routeState === "pending") {
        setRouteBanner("Discovering route…", "pending");
      } else if (state.routeState === "failed") {
        setRouteBanner("Route discovery failed — running target-only.", "failed");
      } else {
        setRouteBanner(null);
      }
    }

    function refreshButtonLabels() {
      if (!isActive()) return;
      if (!state.sessionId) {
        // Idle: Play is the Start affordance, Pause/Reset are inactive, and
        // the input controls are editable.
        if (els.btnPlay) {
          els.btnPlay.disabled = false;
          els.btnPlay.setAttribute("aria-label", "Start trace");
          els.btnPlay.textContent = "▶";
        }
        if (els.btnPause) els.btnPause.disabled = true;
        // A start POST can be in flight before it yields a session id. Keep
        // Reset available in that interval so it can immediately supersede
        // the POST (the stale response will clean up any accepted session).
        els.reset.disabled = !state.startInFlight;
        els.target.disabled = false;
        els.interval.disabled = false;
        // No session -> Background toggle is inert and unchecked. An idle
        // tab cannot persist a session that does not exist yet.
        if (els.backgroundToggle) {
          els.backgroundToggle.disabled = true;
          els.backgroundToggle.checked = false;
        }
        refreshExportButton();
        refreshCopySummaryButton();
        return;
      }
      // Active session: Play acts as Resume when paused (idle when running),
      // Pause is only enabled while running.
      if (els.btnPlay) {
        els.btnPlay.disabled = state.runningKnown;
        els.btnPlay.setAttribute(
          "aria-label",
          state.runningKnown ? "Trace running" : "Resume trace"
        );
        els.btnPlay.textContent = "▶";
      }
      if (els.btnPause) els.btnPause.disabled = !state.runningKnown;
      els.reset.disabled = false;
      els.target.disabled = true;
      els.interval.disabled = true;
      // Background toggle reflects the active tab's persistent flag and is
      // editable whenever a session is bound. Disabled while a toggle POST
      // is in flight so the user cannot queue a second flip on top.
      if (els.backgroundToggle) {
        els.backgroundToggle.disabled = !!state.persistInFlight;
        els.backgroundToggle.checked = !!state.persistent;
      }
      refreshExportButton();
      refreshCopySummaryButton();
    }

    // Export button follows sample availability: enabled only when the
    // active tab's session has at least one known sample (live SSE buffer
    // OR snapshot baseline with count > 0). Single source of truth —
    // refreshButtonLabels calls this for the shared paths; the live-SSE
    // round handler calls it directly so the button enables the moment the
    // first sample arrives.
    function refreshExportButton() {
      if (!isActive()) return;
      if (els.exportBtn) els.exportBtn.disabled = !PURE.exportEnabled(state);
    }

    // Copy summary needs usable target stats for the selected window, like the
    // summary text itself.
    function refreshCopySummaryButton() {
      if (!isActive() || !els.copySummary) return;
      var stats = statsForHopDisplay(primaryHopIndex(), effNowS());
      var enabled = PURE.summaryEnabled({
        sessionId: state.sessionId,
        stats: stats ? stats.data : null,
      });
      els.copySummary.disabled = !enabled;
      // Stale feedback from another trace must not linger on a disabled
      // control.
      if (!enabled && els.copySummaryStatus) els.copySummaryStatus.textContent = "";
    }

    // ----- SSE ----------------------------------------------------------

    function openEventSource() {
      PURE.breakCoverageContinuity(state);
      if (state.es) {
        try { state.es.close(); } catch (e) { /* ignore */ }
        state.es = null;
      }
      if (!state.sessionId) return;
      var sid = state.sessionId;
      var url = "/api/traces/" + encodeURIComponent(sid) + "/events";
      var es = new EventSource(url);
      state.es = es;
      function isCurrent() {
        return !state.closing && !state.resetting &&
          state.sessionId === sid && state.es === es;
      }

      es.addEventListener("ready", function () {
        if (!isCurrent()) return;
        // Transport (re)connected. This only clears the reconnecting fact;
        // it never overwrites a pending control or an unreconciled error —
        // deriveStatus gives those higher precedence, so a reconnect can no
        // longer mask them (the round-3 defect class). The steady state is
        // already correct (runningKnown is unchanged by a reconnect), so
        // clearing the fact is all that is needed.
        state.reconnecting = false;
        renderStatus();
        // Coverage continuity died with the transport: events during the
        // outage were missed, so every raw-buffer completeness claim is
        // dropped and re-established by the backfills below. Advancing the
        // continuity generation HERE as well (the error handler advanced it
        // at the break) fences backfills issued during the outage — they
        // captured the break generation and their windows still cannot span
        // the reconnect gap. Old-generation completions may merge raw rows
        // but can neither establish coverage nor release the current
        // requests' ownership (their tokens were wiped).
        PURE.breakCoverageContinuity(state);
        state.covConnected = true;
        // Slice D1 — reconnect gap fill. The 100-sample snapshot tail may
        // not cover the full disconnect period, so backfill raw history for
        // every open panel. mergeSamples deduplicates the tail already in
        // the buffer, so this is safe even when the snapshot covered the
        // gap. Best-effort; SSE remains the live authority.
        fetchHistoryForOpenPanels();
        // Re-establish raw coverage of the selected stats window for the
        // table hops so accounting returns to the hydrated raw buffer.
        refreshStatsCoverage();
      });

      es.addEventListener("route", function (ev) {
        if (!isCurrent()) return;
        try {
          var data = JSON.parse(ev.data || "{}");
          if (Array.isArray(data.hops)) {
            state.hops = data.hops.map(function (h) {
              return { index: h.index, ip: h.ip, dns: h.dns };
            });
            pruneStaleTimelines();
          }
          state.routeState = "ready";
          updateRouteBanner();
          renderTable();
          renderTimelines();
          if (Array.isArray(data.hops)) hydrateHopBaselines(data.hops);
          // Slice D1 — route discovery may rebind the primary from hop 0 to
          // the final hop (or back). Backfill history for the new primary
          // so its timeline isn't empty while the snapshot tail only covers
          // the last few rounds. The merge deduplicates any overlap.
          fetchHistory(primaryHopIndex());
          // New table hops need their own stats-window coverage before raw
          // samples can be the sole statistics source for their rows.
          refreshStatsCoverage();
          // Refetch the top-level snapshot so the resolved target IP/DNS is
          // available client-side. applySnapshot is idempotent.
          fetchSnapshot();
        } catch (e) {
          // Ignore malformed route events.
        }
      });

      es.addEventListener("error", function () {
        if (!isCurrent()) return;
        // Transport broken. Setting the reconnecting fact is enough —
        // deriveStatus shows "Reconnecting…" only when no pending control
        // or unreconciled error outranks it (the round-4 defect class),
        // so no per-handler guard is needed. The ready handler clears
        // this fact on the next successful connect.
        state.reconnecting = true;
        // Coverage continuity died with the transport: samples arriving
        // while the stream is down are missed, so every raw-buffer
        // completeness claim is invalidated NOW, not on reconnect, and the
        // continuity generation advances. Backfills already in flight
        // captured the pre-break generation: their completions may still
        // merge raw rows, but they can no longer establish coverage or
        // release ownership (their tokens were wiped), and the current
        // generation's backfill is triggered by the ready handler below.
        PURE.breakCoverageContinuity(state);
        renderStatus();
      });

      es.addEventListener("message", function (ev) {
        if (!isCurrent()) return;
        try {
          var data = JSON.parse(ev.data || "{}");
        } catch (e) {
          return;
        }
        if (!data || typeof data !== "object") return;
        handleRound(data);
      });
    }

    function hydrateHopBaselines(hops) {
      var sid = state.sessionId;
      if (!sid) return;
      var focusS = currentFocusSeconds();
      var focusChangeSeq = state.focusChangeSeq;
      var controlRequestSeq = state.controlRequestSeq;
      hops.forEach(function (hop) {
        var key = String(hop.index);
        api("/api/traces/" + encodeURIComponent(sid) + "/snapshot?focus=" + focusS + "&hop=" + encodeURIComponent(hop.index))
          .then(asJson)
          .then(function (snap) {
        if (state.closing || state.resetting || state.sessionId !== sid ||
                state.focusChangeSeq !== focusChangeSeq ||
                state.controlRequestSeq !== controlRequestSeq || !snap.stats) return;
            // Server-side metadata adoption (same inputs as the top-level
            // snapshot path): internal classification + lifetime
            // non-responsive flag + sticky reply evidence from the baseline.
            if (snap.internal !== undefined) {
              var hopMeta = findHop(Number(key));
              if (hopMeta && hopMeta.index !== 0 && String(hopMeta.index) === key) {
                hopMeta.internal = !!snap.internal;
              }
            }
            if (snap.icmp_non_responsive !== undefined) {
              state.hopNr[key] = !!snap.icmp_non_responsive;
            }
            if (snap.stats.count > 0 &&
                snap.stats.avg_ms !== null && snap.stats.avg_ms !== undefined) {
              noteReplied(key);
            }
            // Baseline + cutoff adoption under the same contract as the
            // top-level path: monotonic in captured_at (an older per-hop
            // response cannot regress a newer baseline), empty payloads
            // clear (retaining the capture marker), and the cutoff NEVER
            // falls back to receipt time. The tail merge keeps the whole
            // raw buffer for graphs.
            PURE.adoptSnapshotBaseline(
              state, key, snap.stats, snap.captured_at,
              snap.sample_through_t, snap.sample_from_t, PURE.snapshotFocusSeconds(snap)
            );
            if (Array.isArray(snap.samples) && snap.samples.length > 0) {
              noteRepliesInList(key, snap.samples);
              state.samples[key] = PURE.mergeSamples(
                state.samples[key] || [], snap.samples, RAW_CAP
              );
            }
            renderTable();
          })
          .catch(function () { /* live SSE remains authoritative */ });
      });
    }

    function handleRound(data) {
      if (!data || !data.results) return;
      for (var i = 0; i < data.results.length; i++) {
        var r = data.results[i];
        if (typeof r.hop !== "number") continue;
        pushSample(r.hop, data.t, r.rtt_ms);
        // Any successful reply permanently disqualifies the neutral
        // "ICMP non-responsive" presentation for this hop (trace-local).
        if (r.rtt_ms !== null && r.rtt_ms !== undefined && !isNaN(r.rtt_ms)) {
          noteReplied(r.hop);
        }
      }
      // Render only when active — background tabs just buffer samples.
      if (isActive()) {
        renderTable();
        renderTimelines();
        renderFooterStatus();
        refreshExportButton();
      }
    }

    function pruneStaleTimelines() {
      var known = {};
      if (state.hops.length > 0) {
        for (var i = 0; i < state.hops.length; i++) known[String(state.hops[i].index)] = true;
      } else {
        known["0"] = true;
      }
      known["0"] = true;
      Object.keys(state.timelineHops).forEach(function (key) {
        if (!known[key]) delete state.timelineHops[key];
      });
    }

    // ----- control flow -------------------------------------------------

    function startTrace() {
      if (state.closing || state.resetting || state.startInFlight) return;
      var target = (els.target.value || "").trim();
      var interval = parseFloat(els.interval.value);
      if (!target) {
        state.lastError = "Enter a target";
        renderStatus();
        return;
      }
      if (!isFinite(interval) || interval < 1) {
        state.lastError = "Interval must be >= 1";
        renderStatus();
        return;
      }
      state.pendingTarget = target;
      state.pendingInterval = interval;
      var startSeq = ++state.startRequestSeq;
      state.startInFlight = true;
      // Beginning a fresh transition clears any prior error (validation,
      // previous start failure) so deriveStatus shows "Starting…".
      state.lastError = null;
      renderStatus();
      refreshButtonLabels();
      api("/api/traces", {
        method: "POST",
        body: JSON.stringify({ target: target, interval_s: interval }),
      })
        .then(function (resp) {
          if (!resp.ok) {
            return resp.text().then(function (txt) {
              throw new Error("HTTP " + resp.status + ": " + txt);
            });
          }
          return resp.json();
        })
        .then(function (body) {
          if (state.closing || state.resetting || state.startRequestSeq !== startSeq) {
            // A tab can be closed while the POST is in flight. If the server
            // accepted that POST, clean up the now-unowned session rather
            // than leaking a live probe.
            if (body && body.id) {
              api("/api/traces/" + encodeURIComponent(body.id), { method: "DELETE" })
                .catch(function () { /* best-effort cleanup */ });
            }
            return;
          }
          state.startInFlight = false;
          ++state.controlRequestSeq;
          state.sessionId = body.id;
          ++state.snapshotRequestSeq;
          state.target = body.target;
          state.intervalS = body.interval_s;
          state.target_ip = null;
          state.target_dns = null;
          state.startedAt = Date.now();
          state.samples = {};
          state.buckets = {}; // Slice D2 — clear rollup buffer on start
          state.seedStats = {};
          state.seedCut = {};
          state.seedFrom = {};
          state.seedCap = {};
          state.seedFocus = {};
          state.seedWatermark = {};
          state.covFrom = {};
          state.covInFlight = {};
          state.covConnected = false;
          state.hops = [];
          // Fresh trace: reset trace-local classification facts.
          state.target_internal = false;
          state.hopEverReplied = {};
          state.hopNr = {};
          state.throughputPoints = [];
          state.throughputError = null;
          state.routeState = "pending";
          state.runningKnown = true;
          // Starting a fresh trace always runs: no freeze.
          state.pauseFreezeS = null;
          state.focus = null;
          state.timelineHops = {};
          clearTimelinePanels();
          state.panelRangeS = {};
          // Slice B — adopt the create-response persistent/name fields.
          // ``POST /api/traces`` echoes them so a create with
          // ``persistent=true`` lights the tab marker immediately without
          // waiting for the first snapshot. (The current Start handler
          // does not pass persistent/name in the POST body, so these are
          // false/null for now; the path is in place for a future UI
          // affordance.)
          state.persistent = !!body.persistent;
          state.name = body.name || null;
          state.persistInFlight = false;
          // Record this target in the global history dropdown.
          addTargetToHistory(target);
          if (els.focusWindow) els.focusWindow.value = state.focusWindowValue;
          renderFocusChip();
          refreshButtonLabels();
          // The input is now bound+disabled: repaint it so the masked
          // alias (once the first snapshot classifies the target) or the
          // raw target is what the field shows, not stale typed text.
          populateToolbar();
          updateRouteBanner();
          renderStatus(); // startInFlight=false + runningKnown=true -> "Running"
          // Attach before taking the snapshot. This closes the interval in
          // which rounds could arrive between the snapshot and SSE subscribe
          // and therefore be lost on a newly started/restored tab.
          openEventSource();
          return fetchSnapshot();
        })
        .catch(function (err) {
          if (state.closing || state.resetting || state.startRequestSeq !== startSeq) return;
          state.startInFlight = false;
          resetUiState();
          // resetUiState clears every fact and derives "Idle"; record the
          // failure afterwards so 409/cap and other start errors stay
          // visible instead of being overwritten.
          state.lastError = "Start failed: " + err.message;
          renderStatus();
        });
    }

    function fetchSnapshot() {
      var sid = state.sessionId;
      if (!sid || state.closing || state.resetting) return Promise.resolve();
      var requestSeq = ++state.snapshotRequestSeq;
      var focusS = currentFocusSeconds();
      return api("/api/traces/" + encodeURIComponent(sid) + "/snapshot?focus=" + focusS)
        .then(asJson)
        .then(function (snap) {
          if (state.sessionId !== sid || state.snapshotRequestSeq !== requestSeq) return;
          applySnapshot(snap);
        })
        .catch(function () {
          // Snapshot is best-effort; the SSE stream is the source of truth.
        });
    }

    // ----- Slice D1: raw-sample history backfill -------------------------
    //
    // The snapshot's 100-sample tail covers only the last few minutes; the
    // server retains up to SAMPLE_CAP (10 000) raw samples per stream. On
    // restore/reload/reconnect/route-discovery the timeline needs the full
    // retained window so reopening a session immediately shows history
    // instead of a blank panel that only
    // fills as new SSE rounds arrive. The merge deduplicates the snapshot
    // tail already in the buffer, so a history fetch is always safe (it is
    // idempotent over overlapping windows).
    //
    // Fetches are per-tab and self-guarded by isActive() for DOM writes
    // (background tabs merge into their own buffers without painting). The
    // request is generation-guarded by historyRequestSeq plus the session and
    // focus generations, so a stale fetch landing after reset/close/focus
    // change cannot write into an obsolete view. Coverage backfills
    // additionally capture the connection-continuity generation
    // (state.covGen): a response that crossed an SSE break/reconnect may
    // merge raw rows but cannot establish coverage. Snapshot requests use a
    // separate generation and do not cancel valid history backfills.
    //
    // Coverage mode (opts.forCoverage) backfills the SELECTED stats window
    // for a table hop and records that the raw buffer is a complete census
    // from the fetched window start forward — the accounting precondition
    // for raw-only statistics (see PURE.statsForHopWindow).

    // Ownership tokens for in-flight coverage backfills (one per request,
    // monotonic within this tab). A completion releases the stream's marker
    // only while its token still owns it (see PURE.releaseCoverageInFlight),
    // so a stale focus/reset response — or one whose marker a transport
    // break/reconnect wiped — can never release a newer generation's
    // in-flight backfill.
    var coverageRequestSeq = 0;

    function fetchHistory(hopIndex, opts) {
      opts = opts || {};
      var sid = state.sessionId;
      if (!sid || state.closing || state.resetting) return Promise.resolve();
      var forCoverage = !!opts.forCoverage;
      var key = String(hopIndex);
      // Coverage mode backfills the SELECTED stats window (Focus) instead of
      // the panel's range — always raw, since focus windows sit far below
      // RAW_RANGE_MAX_S. The in-flight flag is set only after the guards
      // pass, so a closed/reset tab never leaves it stuck.
      var rangeS = opts.rangeS != null ? opts.rangeS : rangeForPanel(hopIndex);
      // One outstanding backfill per stream: the marker carries THIS
      // request's ownership token, so a stale generation's completion (its
      // marker wiped by a reconnect/reset or replaced by a newer backfill)
      // can never release the newer request's flag.
      var covToken = null;
      var covGen = null;
      if (forCoverage) {
        covToken = ++coverageRequestSeq;
        covGen = state.covGen;
        state.covInFlight[key] = covToken;
      }
      // Slice D2 — pick the resolution based on the panel's range. Ranges
      // ≤ RAW_RANGE_MAX_S keep D1 raw behaviour (default raw); ranges
      // beyond use ``auto`` so the server resolves raw/1m against raw
      // retention (12h/24h always exceed it at sane intervals → 1m).
      var resolution = PURE.resolutionForRange(rangeS);
      // effNowS() returns paused_at while paused, so a paused session's
      // history naturally clamps end at the freeze point (the server's
      // buffers are authoritative and the freeze semantics hold).
      var endS = effNowS();
      var startS = endS - rangeS;
      var requestSeq = state.historyRequestSeq;
      var focusChangeSeq = state.focusChangeSeq;
      var url =
        "/api/traces/" + encodeURIComponent(sid) +
        "/history?hop=" + encodeURIComponent(hopIndex) +
        "&start=" + encodeURIComponent(startS) +
        "&end=" + encodeURIComponent(endS) +
        "&resolution=" + encodeURIComponent(resolution);
      return api(url)
        .then(asJson)
        .then(function (body) {
          // Terminal for every path: release the marker only while this
          // request still owns it — a newer generation's backfill may have
          // taken the marker over while this one was in flight.
          if (forCoverage) PURE.releaseCoverageInFlight(state, key, covToken);
          if (state.closing || state.resetting || state.sessionId !== sid ||
           state.historyRequestSeq !== requestSeq ||
               state.focusChangeSeq !== focusChangeSeq) return;
          if (!body) return;
          // ``auto`` is allowed to resolve to raw when a session's configured
          // interval gives it unusually long raw retention. Long panels must
          // still render the separate bucket buffer, so follow that response
          // with an explicit rollup fetch rather than silently leaving the
          // 12h/24h canvas empty (or mixing raw samples into bucket state).
          if (PURE.bucketFallbackNeeded(rangeS, body.resolution)) {
            var bucketUrl = url.replace(
              "&resolution=" + encodeURIComponent(resolution),
              "&resolution=1m"
            );
            return api(bucketUrl)
              .then(asJson)
              .then(function (bucketBody) {
                if (state.closing || state.resetting || state.sessionId !== sid ||
                    state.historyRequestSeq !== requestSeq ||
                    state.focusChangeSeq !== focusChangeSeq || !bucketBody ||
                    bucketBody.resolution !== "1m" ||
                    !Array.isArray(bucketBody.buckets)) return;
                var existingRollup = state.buckets[key] || [];
                state.buckets[key] = PURE.mergeBuckets(
                  existingRollup, bucketBody.buckets, null
                );
                if (isActive()) renderTimelines();
              });
          }
          // Slice D2 — branch on the ACTUAL resolution the server picked
          // (auto resolves to raw or 1m). Raw responses merge into
          // state.samples; 1m responses merge into the SEPARATE bucket
          // buffer (different resolution — never mixed). The two buffers
          // are independent per hop; a panel's rendering branch reads
          // whichever one matches its current range.
          if (body.resolution === "1m" && Array.isArray(body.buckets)) {
            var existingB = state.buckets[key] || [];
            state.buckets[key] = PURE.mergeBuckets(existingB, body.buckets, null);
          } else if (Array.isArray(body.samples)) {
            noteRepliesInList(hopIndex, body.samples);
            var existing = state.samples[key] || [];
            state.samples[key] = PURE.mergeSamples(existing, body.samples, RAW_CAP);
            // Adopted raw response over [startS, endS]: the buffer is now a
            // complete census of the server's samples from startS forward
            // (live SSE keeps it current until the transport drops). Only
            // RAW responses may claim coverage — buckets are a different
            // resolution and never enter the raw buffer — and only while
            // the response still belongs to the connection generation it
            // was issued on: one that crossed a transport break/reconnect
            // missed server-side samples and may merge its rows but must
            // never establish completeness (PURE.extendCoverageIfCurrent).
            if (forCoverage) {
              PURE.extendCoverageIfCurrent(state, key, startS, covGen, state.covGen);
            }
          } else {
            return;
          }
          if (isActive()) {
            // A coverage adoption can flip a hop's accounting source
            // (baseline-combined -> hydrated raw-only), so the hop table and
            // footer (same primary-stream stats source) must repaint
            // alongside the timeline; panel backfills keep the existing
            // timeline-only repaint.
            if (forCoverage) {
              renderTable();
              renderFooterStatus();
            }
            renderTimelines();
          }
        })
        .catch(function () {
          if (forCoverage) PURE.releaseCoverageInFlight(state, key, covToken);
          /* best-effort; SSE remains authoritative */
        });
    }

    /**
     * Backfill the SELECTED raw stats window (Focus) for every table hop —
     * the target stream plus each known route hop, not just open timeline
     * panels — so hop accounting can graduate from the snapshot baseline to
     * the unique raw buffer (sole statistics source) once coverage is
     * complete. One outstanding backfill per stream (covInFlight carries the
     * owning request's token); adoption is generation-guarded inside
     * fetchHistory (focus/reset sequence AND connection continuity), and a
     * terminal fetch releases the marker only while its token still owns
     * it, so the next snapshot adoption or transport break/reconnect
     * retries.
     */
    function refreshStatsCoverage() {
      var keys = { "0": true };
      for (var i = 0; i < state.hops.length; i++) {
        keys[String(state.hops[i].index)] = true;
      }
      var focusS = currentFocusSeconds();
      var startS = effNowS() - focusS;
      Object.keys(keys).forEach(function (k) {
        if (state.covInFlight[k]) return;
        var cov = state.covFrom[k];
        if (typeof cov === "number" && cov <= startS) return;
        fetchHistory(Number(k), { rangeS: focusS, forCoverage: true });
      });
    }

    function fetchHistoryForOpenPanels() {
      var sid = state.sessionId;
      if (!sid || state.closing || state.resetting) return;
      var primary = primaryHopIndex();
      // Always backfill the primary panel; also backfill any open non-primary
      // panels (a restored tab has none — timelineHops is client-only — but
      // a reconnect or route-discovery may fire with panels already open).
      var hops = [primary];
      Object.keys(state.timelineHops).forEach(function (key) {
        var idx = Number(key);
        if (isFinite(idx) && idx !== primary) hops.push(idx);
      });
      hops.forEach(function (hopIndex) { fetchHistory(hopIndex); });
    }

    // ----- InfluxDB throughput overlay (primary timeline) ----------------
    //
    // The server does the InfluxDB querying (``GET /api/throughput``) and
    // returns normalized {t, in_mbps, out_mbps} points over the primary
    // panel's selected window. Requests are issued ONLY while the overlay
    // is enabled: on toggle, on primary range change, and on a bounded
    // refresh cadence while running. Generation-guarded (stale/out-of-order
    // responses cannot write), session-guarded (reset/close/tab ownership),
    // and pause-aware (a paused tab's frozen window stops refreshing).

    function fetchThroughput() {
      if (!state.sessionId || !state.throughputOn || state.closing || state.resetting) {
        return;
      }
      var sid = state.sessionId;
      var rangeS = rangeForPanel(primaryHopIndex());
      var endS = effNowS();
      var startS = endS - rangeS;
      var seq = ++state.throughputSeq;
      state.throughputLastFetchS = Date.now() / 1000;
      api("/api/throughput?start=" + encodeURIComponent(startS) +
          "&end=" + encodeURIComponent(endS))
        .then(asJson)
        .then(function (body) {
          if (state.throughputSeq !== seq || state.closing || state.resetting ||
              state.sessionId !== sid) return;
          state.throughputPoints = Array.isArray(body.points) ? body.points : [];
          state.throughputStep = body.step_s || null;
          state.throughputError = null;
          if (isActive()) renderTimelines();
        })
        .catch(function (err) {
          if (state.throughputSeq !== seq || state.closing || state.resetting ||
              state.sessionId !== sid) return;
          state.throughputPoints = [];
          state.throughputError = err.message;
          if (isActive()) renderTimelines();
        });
    }

    function pauseOrResume(action) {
      if (!state.sessionId) return;
      if (state.controlAction) return;
      var sid = state.sessionId;
      state.controlAction = action;
      ++state.controlRequestSeq;
      // A new user-initiated control clears any prior error (and a stale
      // reconciliation pin) so the pending label ("Pausing…" / "Resuming…")
      // is what deriveStatus shows.
      state.lastError = null;
      state.errorPinnedForReconcile = false;
      // NOTE: the snapshot generation is NOT advanced here. ``applySnapshot``
      // defers state adoption while ``controlAction`` is set, so a snapshot
      // captured mid-flight cannot flip the state either way; the generation
      // bump happens at CONFIRMATION (below) to reject snapshots issued
      // before this transition was confirmed.
      renderStatus();
      api("/api/traces/" + encodeURIComponent(state.sessionId) + "/" + action, {
        method: "POST",
      })
        .then(asJson)
        .then(function (summary) {
          // Reset or starting a replacement trace may have completed while
          // this control request was in flight. Its response no longer
          // belongs to the current tab/session.
          if (state.closing || state.resetting || state.sessionId !== sid) return;
          state.controlAction = null;
          state.lastError = null;
          state.errorPinnedForReconcile = false;
          state.runningKnown = summary.state === "running";
          // Freeze the data window on confirmed pause; release it on
          // confirmed resume. The forward jump on resume is correct —
          // the gap is real dead time. Prefer the server-recorded pause
          // instant (``summary.paused_at``) over the response-receipt
          // wall time so every window anchors its freeze to the same
          // epoch.
          if (state.runningKnown) {
            state.pauseFreezeS = null;
          } else {
            var pa = summary.paused_at;
            state.pauseFreezeS =
              typeof pa === "number" && isFinite(pa) ? pa : Date.now() / 1000;
          }
          // Confirmed transition: invalidate snapshots issued before
          // confirmation so a stale in-flight response (captured before
          // the server committed) cannot land and revert runningKnown /
          // pauseFreezeS. Snapshots fetched after this point see the new
          // generation and are adopted authoritatively.
          ++state.snapshotRequestSeq;
          refreshButtonLabels();
          renderStatus();
          // Immediate re-render so the freeze (or unfreeze) is visible
          // without waiting for the next 1s tick. Background tabs'
          // render* closures no-op via isActive().
          renderTable();
          renderTimelines();
          renderFocusCaption();
        })
        .catch(function (err) {
          if (state.closing || state.resetting || state.sessionId !== sid) return;
          // Ambiguous failure: ``fetch`` rejected (network drop) after the
          // POST may already have committed server-side, OR an HTTP error
          // arrived (409 already in target state / 404 gone). In every case
          // the local runningKnown / pauseFreezeS may now be wrong, so clear
          // the pending action and RECONCILE from an authoritative server
          // snapshot. The failure message is PINNED through that one
          // reconciliation snapshot so the user sees the outcome before the
          // steady state restores it; deriveStatus's precedence (lastError
          // row 5) keeps it visible over the snapshot's adopted state and
          // over any SSE event without a per-writer guard.
          state.controlAction = null;
          state.lastError = action + " failed: " + err.message;
          state.errorPinnedForReconcile = true;
          ++state.snapshotRequestSeq;
          renderStatus();
          fetchSnapshot();
        });
    }

    function toggleBackground() {
      // Flip the active tab's persistent flag via POST /persist. The flag
      // is the source of truth on the server; the optimistic local flip
      // here is confirmed by the response summary (and re-asserted by any
      // later snapshot). The toggle does NOT disturb probing — the server
      // only writes the state file; the probe loop keeps running.
      if (!state.sessionId) return;
      if (state.persistInFlight) return;
      var sid = state.sessionId;
      var nextPersistent = !state.persistent;
      state.persistInFlight = true;
      // Optimistic flip so the tab marker and checkbox reflect the new
      // state immediately. The response summary re-asserts the value, and
      // a failure reverts it (the snapshot adoption path cannot help here
      // because persistence is not gated by ``controlAction``).
      state.persistent = nextPersistent;
      if (isActive()) {
        refreshButtonLabels();
        tabManager.renderTabBar();
      }
      api("/api/traces/" + encodeURIComponent(sid) + "/persist", {
        method: "POST",
        body: JSON.stringify({ persistent: nextPersistent }),
      })
        .then(asJson)
        .then(function (summary) {
          if (state.closing || state.resetting || state.sessionId !== sid) return;
          state.persistInFlight = false;
          state.persistent = !!summary.persistent;
          if (summary.name !== undefined) state.name = summary.name || null;
          // Reject snapshots issued before the POST confirmation. A later
          // snapshot starts a fresh generation and can converge normally.
          ++state.snapshotRequestSeq;
          if (isActive()) {
            refreshButtonLabels();
            tabManager.renderTabBar();
          }
        })
        .catch(function (err) {
          if (state.closing || state.resetting || state.sessionId !== sid) return;
          // Revert the optimistic flip and surface the failure. A network
          // drop after the server committed is possible, so issue a
          // reconciliation snapshot to resync the flag from the server
          // rather than trusting the local rollback blindly.
          state.persistInFlight = false;
          state.persistent = !nextPersistent;
          state.lastError = "Background toggle failed: " + err.message;
          // Keep the failure visible through the reconciliation snapshot,
          // matching pause/resume's error-pinning policy. The snapshot still
          // owns the eventual authoritative persistent/name state.
          state.errorPinnedForReconcile = true;
          ++state.snapshotRequestSeq;
          renderStatus();
          fetchSnapshot();
          if (isActive()) {
            refreshButtonLabels();
            tabManager.renderTabBar();
          }
        });
    }

    // ----- Copy summary (target-only plain text) -----------------------

    /** Display name of the target stream for copied text (mask-aware). */
    function summaryTargetLabel(hopIndex) {
      var hop = findHop(hopIndex);
      return PURE.summaryTargetLabel(
        hop,
        hopInternal(hop),
        maskState.on,
        sessionMaskNames()
      );
    }

    // The same stats + window the table shows; ``effNowS`` keeps a paused
    // trace's copied window frozen.
    function targetSummaryText(hopIndex, nowS) {
      var stats = statsForHopDisplay(hopIndex, nowS);
      var seed = state.seedStats[String(hopIndex)] || null;
      var focusS = currentFocusSeconds();
      return PURE.buildTargetSummary({
        target: summaryTargetLabel(hopIndex),
        source: stats ? stats.source : null,
        stats: stats ? stats.data : null,
        seedHasMax: !!(seed && seed.max_ms !== null && seed.max_ms !== undefined &&
          !isNaN(seed.max_ms)),
        windowLabel: state.focus ? "Manual" : PURE.focusWindowLabel(state.focusWindowValue),
        startS: state.focus ? state.focus.startS : nowS - focusS,
        endS: state.focus ? state.focus.endS : nowS,
        intervalS: state.intervalS,
      });
    }

    // Feedback has its own aria-live line; never the trace status line.
    function setCopySummaryStatus(text) {
      if (!isActive() || !els.copySummaryStatus) return;
      els.copySummaryStatus.textContent = text || "";
    }

    function copyTargetSummary() {
      if (!state.sessionId) return;
      var sid = state.sessionId;
      var text = targetSummaryText(primaryHopIndex(), effNowS());
      if (!text) return;
      PURE.copyPlainText({
        text: text,
        clipboard: typeof navigator !== "undefined" ? navigator.clipboard : null,
        doc: document,
      }).then(function (ok) {
        // A tab switch/reset mid-write must not label another trace's toolbar.
        if (!isActive() || state.sessionId !== sid) return;
        setCopySummaryStatus(ok ? "Copied" : "Copy failed");
      });
    }

    function exportSamples() {
      // Plain anchor download — no fetch+blob. The browser follows the
      // server's ``Content-Disposition: attachment`` and writes the file
      // (named by the server) without leaving the page. Exports the ACTIVE
      // tab's session only.
      if (!state.sessionId) return;
      var url =
        "/api/traces/" +
        encodeURIComponent(state.sessionId) +
        "/export?format=csv";
      var a = document.createElement("a");
      a.href = url;
      // download attribute omitted: the response's Content-Disposition
      // names the file. Appending to the DOM is required for the click to
      // fire reliably in Firefox.
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
    }

    function resetTrace() {
      if (!state.sessionId) {
        // The POST can be accepted by the server after the user resets the
        // still-idle tab. Invalidate it now; its stale response will delete
        // the returned session id rather than attaching it to this tab.
        if (!state.startInFlight) return;
        state.resetting = true;
        ++state.snapshotRequestSeq;
        ++state.startRequestSeq;
        ++state.controlRequestSeq;
        state.startInFlight = false;
        resetUiState(); // clears every fact and derives "Idle"
        return;
      }
      var sid = state.sessionId;
      state.resetting = true;
      ++state.snapshotRequestSeq;
      ++state.startRequestSeq;
      ++state.controlRequestSeq;
      state.startInFlight = false;
      state.controlAction = null;
      if (state.es) {
        try { state.es.close(); } catch (e) { /* ignore */ }
        state.es = null;
      }
      renderStatus(); // resetting=true -> "Resetting…"
      api("/api/traces/" + encodeURIComponent(sid), { method: "DELETE" })
        .then(function (resp) {
          if (!resp.ok && resp.status !== 404) {
            return resp.text().then(function (txt) {
              throw new Error("HTTP " + resp.status + ": " + txt);
            });
          }
        })
        .then(function () {
          resetUiState(); // derives "Idle"
        })
        .catch(function (err) {
          resetUiState();
          state.lastError = "Reset failed: " + err.message;
          renderStatus();
        });
    }

    function resetUiState() {
      if (state.es) {
        try { state.es.close(); } catch (e) { /* ignore */ }
        state.es = null;
      }
      state.sessionId = null;
      ++state.snapshotRequestSeq;
      ++state.historyRequestSeq;
      ++state.startRequestSeq;
      ++state.controlRequestSeq;
      state.startInFlight = false;
      state.closing = false;
      state.resetting = false;
      state.target = null;
      state.target_ip = null;
      state.target_dns = null;
      state.startedAt = null;
      state.hops = [];
      state.samples = {};
      state.buckets = {}; // Slice D2 — clear rollup buffer on reset
      state.seedStats = {};
      state.seedCut = {};
      state.seedFrom = {};
      state.seedCap = {};
      state.seedFocus = {};
      state.seedWatermark = {};
      state.covFrom = {};
      state.covInFlight = {};
      state.covConnected = false;
      // Trace-local classification facts reset with the buffers.
      state.target_internal = false;
      state.hopEverReplied = {};
      state.hopNr = {};
      state.throughputPoints = [];
      state.throughputError = null;
      state.throughputStep = null;
      state.throughputSeq++;
      state.throughputLastFetchS = 0;
      state.routeState = null;
      state.runningKnown = false;
      state.controlAction = null;
      state.lastError = null;
      state.reconnecting = false;
      state.errorPinnedForReconcile = false;
      state.pauseFreezeS = null;
      state.focus = null;
      state.timelineHops = {};
      clearTimelinePanels();
      state.panelRangeS = {};
      // Slice B — drop persistence state on reset so the tab marker and
      // Background toggle return to their idle (inert) presentation.
      state.persistent = false;
      state.name = null;
      state.persistInFlight = false;
      // Restore Focus dropdown's visible value.
      if (els.focusWindow) els.focusWindow.value = state.focusWindowValue;
      refreshButtonLabels();
      // The input just became editable again: restore the pending target
      // so the field never keeps (or offers Start on) the masked alias
      // that was rendered while the session was bound.
      populateToolbar();
      setRouteBanner(null);
      if (isActive()) {
        showEmptyRow("No trace running. Enter a target and press Start.");
        clearGraphConnector();
      }
      renderFocusChip();
      renderFocusCaption();
      renderTimelines();
      renderStatus();         // toolbar: derive "Idle" from cleared facts
      renderFooterStatus();   // footer: samples/elapsed/last-round
      if (isActive()) tabManager.renderTabBar();
    }

    function clearTimelinePanels() {
      if (!els.timelineStack) return;
      while (els.timelineStack.firstChild) {
        els.timelineStack.removeChild(els.timelineStack.firstChild);
      }
      state.timelinePanels = {};
    }

    // ----- toolbar sync (called by tabManager on activate / switch) ----

    function populateToolbar() {
      // Sync toolbar values from this tab's state. Bound tabs lock the
      // target/interval inputs (refreshButtonLabels sets disabled).
      if (!isActive()) return;
      if (state.sessionId) {
        // Screenshot masking: a bound session's target field shows the
        // neutral alias while the toggle is on. The input is DISABLED
        // while bound, so this display value can never feed Start — the
        // operational target stays in state.target and is restored here
        // the moment masking is off (or the session resets).
        if (els.target) {
          els.target.value = PURE.maskSessionLabel(
            state.target || "",
            sessionMaskNames(),
            maskState.on
          );
        }
        if (els.interval) els.interval.value = String(state.intervalS);
      } else {
        if (els.target) els.target.value = state.pendingTarget || "";
        if (els.interval) els.interval.value = String(state.pendingInterval);
      }
      if (els.focusWindow) els.focusWindow.value = state.focusWindowValue;
    }

    function captureToolbar() {
      // Capture typed values into state when switching AWAY from an idle
      // tab so the user's in-progress target survives the switch.
      if (!state.sessionId) {
        if (els.target) state.pendingTarget = (els.target.value || "").trim();
        if (els.interval) {
          var v = parseFloat(els.interval.value);
          if (isFinite(v) && v >= 1) state.pendingInterval = v;
        }
      }
    }

    // ----- public API (used by tabManager + toolbar wiring) ------------

    self.state = state;
    self.isIdle = function () { return state.sessionId === null; };
    self.getLabel = function () {
      if (!state.sessionId) {
        // Idle fresh tab: show the in-progress target if any, else "New trace".
        return PURE.tabLabel(state.pendingTarget || "");
      }
      // Target-derived label, or the user-set name. While masking is on,
      // a label that equals or CONTAINS a known internal name (raw
      // target, resolved DNS) renders as the neutral alias — a custom
      // name quoting the internal host leaks exactly what the mask
      // exists to hide. Unrelated custom names pass through; IP-derived
      // labels are never masked (sessionMaskNames drops IP literals).
      var label = state.name
        ? PURE.tabDisplayLabel(state.name, "")
        : PURE.tabLabel(state.target || "");
      return PURE.maskSessionLabel(label, sessionMaskNames(), maskState.on);
    };
    self.isPersistent = function () { return !!state.persistent; };
    self.activate = function () {
      // Sync toolbar + main content from this tab's buffers. Called by
      // tabManager.setActive whenever this tab becomes active.
      populateToolbar();
      // Copy-summary feedback belongs to the trace that produced it.
      if (els.copySummaryStatus) els.copySummaryStatus.textContent = "";
      // Re-derive the toolbar status from current facts (a background tab's
      // facts may have changed via buffered snapshots/events) and write the
      // DOM now that this tab is active.
      renderStatus();
      // Full re-render from buffers (no refetch — buffers are authoritative).
      pruneStaleTimelines();
      updateRouteBanner();
      refreshButtonLabels();
      renderFocusChip();
      renderFocusCaption();
      renderTable();
      // The timeline stack is shared DOM, while timelinePanels belongs to
      // the previously active tab. Drop those DOM nodes before rebuilding
      // from this tab's per-tab timeline state; otherwise a tab switch can
      // leave the prior tab's panel visible over the newly active trace.
      clearTimelinePanels();
      renderTimelines();
      renderFooterStatus();
      // An Influx settings save clears every background tab's overlay
      // points but refetches only the ACTIVE tab, and tick refreshes only
      // while RUNNING — a paused background tab would otherwise activate
      // to an empty overlay that never fills. Fetch the missing window
      // now (pause-aware via effNowS, stale-guarded via the generation
      // counter inside fetchThroughput).
      if (PURE.throughputFetchOnActivate({
        sessionId: state.sessionId,
        throughputOn: state.throughputOn,
        influxUrl: serverSettings.influxUrl,
        throughputPoints: state.throughputPoints,
      })) {
        fetchThroughput();
      }
    };
    self.destroy = function () {
      // Called by tabManager.close — close the EventSource; the backend
      // session is deleted separately in requestClose (best-effort).
      if (state.es) {
        try { state.es.close(); } catch (e) { /* ignore */ }
        state.es = null;
      }
    };
    self.requestClose = function () {
      // Tab close × handler. If bound, DELETE the backend session first
      // (best-effort: on error still close the tab locally and surface the
      // error in the new active tab's status line).
      if (state.sessionId) {
        var sid = state.sessionId;
        state.closing = true;
        ++state.snapshotRequestSeq;
        ++state.startRequestSeq;
        ++state.controlRequestSeq;
        state.startInFlight = false;
        state.controlAction = null;
        state.persistInFlight = false;
        if (state.es) { try { state.es.close(); } catch (e) {} state.es = null; }
        api("/api/traces/" + encodeURIComponent(sid), { method: "DELETE" })
          .then(function (resp) {
            if (!resp.ok && resp.status !== 404) {
              return resp.text().then(function (txt) {
                throw new Error("HTTP " + resp.status + ": " + txt);
              });
            }
          })
          .then(function () {
            tabManager.close(self);
          })
          .catch(function (err) {
            tabManager.close(self);
            var a = tabManager.getActive();
            if (a && typeof a.setStatusExternal === "function") {
              a.setStatusExternal("Close failed: " + err.message, "error");
            }
          });      } else {
        // Idle fresh tab — no backend session to delete.
        state.closing = true;
        ++state.startRequestSeq;
        ++state.controlRequestSeq;
        state.startInFlight = false;
        tabManager.close(self);
      }
    };
    // Cross-tab status escape hatch: the only caller surfaces a
    // close-DELETE failure on the newly-active tab. Routed through the
    // lastError fact so deriveStatus shows it (row 5) until a snapshot or
    // user-initiated transition clears it; only kind "error" sets it.
    self.setStatusExternal = function (text, kind) {
      state.lastError = kind === "error" ? text : null;
      renderStatus();
    };
    self.attach = function () {
      // For restored tabs: attach SSE before fetching the snapshot. Background
      // tabs accumulate samples into their buffers without touching the DOM,
      // and no round can fall into a snapshot-to-subscribe gap.
      if (!state.sessionId) return;
      openEventSource();
      fetchSnapshot();
      // Slice D1 — backfill raw history so the timeline is populated
      // immediately on reload instead of waiting
      // for new SSE rounds. The primary panel always gets history; open
      // non-primary panels (rare on restore) too.
      fetchHistoryForOpenPanels();
    };
    self.startOrResume = function () {
      if (state.controlAction) return;
      if (!state.sessionId) startTrace();
      else if (!state.runningKnown) pauseOrResume("resume");
    };
    self.pause = function () {
      if (state.sessionId && state.runningKnown && !state.controlAction) pauseOrResume("pause");
    };
    self.toggleBackground = function () { toggleBackground(); };
    self.reset = function () { resetTrace(); };
    self.exportSamples = function () { exportSamples(); };
    self.copySummary = function () { copyTargetSummary(); };
    self.applyFocusWindowValue = function (v) { applyFocusWindowChange(v); };
    self.toggleHopTimeline = function (hopIndex) { toggleHopTimeline(hopIndex); };
    self.clearFocus = function () { clearFocus(); };
    // Re-render the shared DOM from this tab's buffers (used when a global
    // display toggle such as "Mask internal DNS" flips).
    self.refreshViews = function () {
      if (!isActive()) return;
      populateToolbar();   // target field: alias on, exact target off
      renderTable();
      renderTimelines();
      renderStatus();      // status line: re-mask known names
      updateRouteBanner(); // banner text: re-mask known names
    };
    // Refetch the snapshot so server-side annotations (internal flags,
    // ICMP non-responsive flags) converge after a settings change.
    self.refreshMeta = function () {
      if (state.sessionId && !state.closing && !state.resetting) fetchSnapshot();
    };
    // A successful Influx settings save calls this on EVERY tab: drop any
    // points/error fetched from the OLD source, and bump the generation so
    // in-flight responses (this tab's or any other controller's) can never
    // write — nothing from the old source may render or be reused, even if
    // it lands after the save or while this tab is paused/inactive. Then
    // refetch when the overlay is still enabled AND a source is still
    // configured; otherwise the overlay turns itself off.
    self.onInfluxSourceChanged = function () {
      state.throughputPoints = [];
      state.throughputError = null;
      state.throughputStep = null;
      state.throughputLastFetchS = 0;
      ++state.throughputSeq; // invalidate in-flight fetches
      if (!serverSettings.influxUrl) {
        state.throughputOn = false;
      } else if (state.throughputOn && isActive()) {
        fetchThroughput();
      }
      if (isActive()) renderTimelines();
    };
    self.tick = function () {
      // 1s global tick dispatched to the active tab.
      if (!isActive()) return;
      if (state.sessionId) checkFocusExpiry(effNowS());
      // Throughput overlay refresh: only while enabled and running (a
      // paused tab's window is frozen), on a bounded cadence tied to the
      // server's bucket step (never faster than ~5s).
      if (state.sessionId && state.throughputOn && state.runningKnown) {
        var cadence = Math.max(5, state.throughputStep || 5);
        if (Date.now() / 1000 - state.throughputLastFetchS >= cadence) {
          fetchThroughput();
        }
      }
      // Discovery failure has no SSE route event (there are no hops to
      // publish), so refresh the pending snapshot until the server reports
      // ready or failed. This also covers a restore that attached before
      // discovery completed.
      if (state.sessionId && state.routeState === "pending") fetchSnapshot();
      renderFooterStatus();
      renderFocusCaption();
      if (state.sessionId) renderTimelines();
      // The rolling window slides on the tick without a table repaint.
      refreshCopySummaryButton();
    };
    self.handleResize = function () {
      if (!isActive()) return;
      renderTimelines();
      if (state.sessionId) {
        var nowS = effNowS();
        var displayHops = state.hops.length ? state.hops : [];
        var hopMaxes = [];
        for (var i = 0; i < displayHops.length; i++) {
          var s = statsForHopDisplay(displayHops[i].index, nowS);
          hopMaxes.push(s ? s.data.max : null);
        }
        renderConnector(PURE.axisMax(hopMaxes));
      }
    };

    return self;
  }

  // ----- toolbar wiring (shared; dispatched to the active tab) --------
  //
  // Wired ONCE in bootstrap. Each listener reads toolbar values and
  // dispatches to ``tabManager.getActive()``. Target-history keyboard nav
  // uses the global ``historyState``.

  function wireToolbar() {
    // Slice 6b: btn-play is type="submit" so form submit handles Start
    // (active tab idle) and Resume (active tab paused). btn-pause has its
    // own click handler.
    els.form.addEventListener("submit", function (ev) {
      ev.preventDefault();
      var a = tabManager.getActive();
      if (!a) return;
      a.startOrResume();
    });

    if (els.btnPause) {
      els.btnPause.addEventListener("click", function () {
        var a = tabManager.getActive();
        if (a) a.pause();
      });
    }

    els.reset.addEventListener("click", function () {
      var a = tabManager.getActive();
      if (a) a.reset();
    });

    if (els.exportBtn) {
      els.exportBtn.addEventListener("click", function () {
        var a = tabManager.getActive();
        if (a) a.exportSamples();
      });
    }

    if (els.copySummary) {
      els.copySummary.addEventListener("click", function () {
        var a = tabManager.getActive();
        if (a) a.copySummary();
      });
    }

    if (els.focusWindow) {
      els.focusWindow.addEventListener("change", function () {
        var a = tabManager.getActive();
        if (a) a.applyFocusWindowValue(els.focusWindow.value);
      });
    }

    if (els.backgroundToggle) {
      // Slice B — Background toggle dispatches to the active tab. The
      // checkbox's checked/disabled state is owned by
      // ``refreshButtonLabels`` (per-tab, restored on activate); this
      // listener only forwards the change. ``change`` (not ``click``)
      // fires once per toggle, after the new state is visible, so the
      // optimistic flip in ``toggleBackground`` does not race a second
      // click handler.
      els.backgroundToggle.addEventListener("change", function () {
        var a = tabManager.getActive();
        if (!a) return;
        a.toggleBackground();
      });
    }

    if (els.target) {
      els.target.addEventListener("focus", showTargetHistory);
      els.target.addEventListener("input", showTargetHistory);
      els.target.addEventListener("blur", function () {
        // Delay so a mousedown/click on a history item registers before the
        // dropdown is hidden.
        setTimeout(hideTargetHistory, 150);
      });
      els.target.addEventListener("keydown", function (ev) {
        if (historyState.list.length === 0) return;
        switch (ev.key) {
          case "ArrowDown":
            ev.preventDefault();
            if (!isHistoryVisible()) {
              showTargetHistory();
              historyState.highlight = 0;
            } else {
              historyState.highlight = Math.min(
                historyState.highlight + 1, historyState.list.length - 1
              );
            }
            updateHistoryHighlight();
            break;
          case "ArrowUp":
            ev.preventDefault();
            if (isHistoryVisible()) {
              historyState.highlight = Math.max(historyState.highlight - 1, 0);
              updateHistoryHighlight();
            }
            break;
          case "Escape":
            if (isHistoryVisible()) {
              ev.preventDefault();
              hideTargetHistory();
            }
            break;
          case "Enter":
            if (isHistoryVisible() && historyState.highlight >= 0) {
              ev.preventDefault();
              selectTargetHistory(historyState.highlight);
            }
            break;
        }
      });
    }

    // A click on a non-focusable element does not reliably blur the input in
    // every browser. Close the list explicitly when the pointer leaves the
    // combobox, while allowing item mousedown/click to complete first.
    document.addEventListener("click", function (ev) {
      if (!isHistoryVisible()) return;
      var combo = ev.target.closest ? ev.target.closest(".target-combobox") : null;
      if (!combo) hideTargetHistory();
    });

    // Hop-row double-click toggles a non-primary timeline for the active tab.
    els.tableBody.addEventListener("dblclick", function (ev) {
      var tr = ev.target.closest ? ev.target.closest("tr[data-hop]") : null;
      if (!tr) return;
      var hopIndex = Number(tr.getAttribute("data-hop"));
      if (!isFinite(hopIndex)) return;
      var a = tabManager.getActive();
      if (a) a.toggleHopTimeline(hopIndex);
    });

    // Focus chip clear button — active tab.
    if (els.focusClear) {
      els.focusClear.addEventListener("click", function () {
        var a = tabManager.getActive();
        if (a) a.clearFocus();
      });
    }

    // "Mask internal DNS" screenshot toggle — global display-only state.
    // Flipping it re-renders the active tab (target field, toolbar
    // status, table DNS cells, timeline labels, route banner, tab label)
    // and repaints the tab bar. Turning it ON also hides the
    // recent-targets popup (its unclassified entries cannot be safely
    // masked — see showTargetHistory). Data, API and CSV payloads are
    // never touched.
    if (els.maskToggle) {
      els.maskToggle.addEventListener("change", function () {
        maskState.on = !!els.maskToggle.checked;
        if (maskState.on) hideTargetHistory();
        tabManager.renderTabBar();
        var a = tabManager.getActive();
        if (a && typeof a.refreshViews === "function") a.refreshViews();
      });
    }
  }

  // ----- settings dialog (internal ranges + InfluxDB overlay) --------

  // Bumped on every openSettings so a fill from a previous (closed) open
  // can never clobber the fields of the current one.
  var settingsLoadGen = 0;

  function fillSettingsFields(internal, influx) {
    if (els.settingsCidrs) {
      els.settingsCidrs.value = (internal && internal.cidrs ? internal.cidrs : [])
        .join("\n");
    }
    var inf = influx || {};
    if (els.settingsInfluxUrl) els.settingsInfluxUrl.value = inf.url || "";
    if (els.settingsInfluxDb) els.settingsInfluxDb.value = inf.database || "";
    if (els.settingsInfluxInterface) els.settingsInfluxInterface.value = inf.interface || "";
    if (els.settingsInfluxHost) els.settingsInfluxHost.value = inf.host_tag || "";
  }

  function setSettingsStatus(text, loading) {
    if (els.settingsStatus) {
      els.settingsStatus.textContent = text || "";
      els.settingsStatus.className = "settings-status" + (loading ? " loading" : "");
    }
  }

  // Freeze the inputs + Save while settings load (Cancel stays usable —
  // it is wired to dialog.close directly and is never disabled). Saving
  // half-loaded/empty fields is how persisted CIDRs got silently wiped.
  function setSettingsLocked(disabled) {
    var controls = [
      els.settingsCidrs, els.settingsCidrsDefault, els.settingsInfluxUrl,
      els.settingsInfluxDb, els.settingsInfluxInterface, els.settingsInfluxHost,
      els.settingsSave,
    ];
    for (var i = 0; i < controls.length; i++) {
      if (controls[i]) controls[i].disabled = disabled;
    }
  }

  function openSettings() {
    if (!els.settingsDialog) return;
    var gen = ++settingsLoadGen;
    setSettingsLocked(true);
    setSettingsStatus("Loading settings…", true);
    try { els.settingsDialog.showModal(); } catch (e) { /* already open */ }
    Promise.all([
      api("/api/settings/internal").then(asJson),
      api("/api/settings/influx").then(asJson),
    ])
      .then(function (rs) {
        if (gen !== settingsLoadGen) return; // stale: this open was closed
        fillSettingsFields(rs[0], rs[1]);
        setSettingsLocked(false);
        setSettingsStatus("");
      })
      .catch(function (err) {
        if (gen !== settingsLoadGen) return; // stale: this open was closed
        // Keep the fields+Save disabled: an accidental save from these
        // empty fields would replace the persisted settings.
        fillSettingsFields(null, null);
        setSettingsStatus("Could not load settings: " + err.message);
      });
  }

  function wireSettings() {
    if (els.btnSettings) {
      els.btnSettings.addEventListener("click", openSettings);
    }
    if (els.settingsCancel && els.settingsDialog) {
      els.settingsCancel.addEventListener("click", function () {
        els.settingsDialog.close();
      });
    }
    if (els.settingsCidrsDefault && els.settingsCidrs) {
      els.settingsCidrsDefault.addEventListener("click", function () {
        els.settingsCidrs.value = PURE.DEFAULT_INTERNAL_CIDRS.join("\n");
      });
    }
    if (els.settingsForm) {
      els.settingsForm.addEventListener("submit", function (ev) {
        ev.preventDefault();
        // Never save while the fields are locked (load in flight or
        // failed) — the PUT would persist empty/half-loaded values.
        if (els.settingsSave && els.settingsSave.disabled) return;
        setSettingsStatus("");
        var cidrs = (els.settingsCidrs.value || "")
          .split("\n")
          .map(function (l) { return l.trim(); })
          .filter(function (l) { return l.length > 0; });
        var influx = {
          url: (els.settingsInfluxUrl.value || "").trim(),
          database: (els.settingsInfluxDb.value || "").trim(),
          interface: (els.settingsInfluxInterface.value || "").trim(),
          host_tag: (els.settingsInfluxHost.value || "").trim(),
        };
        // One transactional save (PUT /api/settings): the server applies
        // both sections together — a 422 or failed persist leaves both
        // the old ranges and the old overlay config in place, so there is
        // no partial-success window between the two PUTs to reconcile.
        api("/api/settings", {
          method: "PUT",
          body: JSON.stringify({ cidrs: cidrs, influx: influx }),
        })
          .then(asJson)
          .then(function (saved) {
            serverSettings.influxUrl = (saved && saved.influx && saved.influx.url) || "";
            if (els.settingsDialog) els.settingsDialog.close();
            // Server-side internal classification changed: refetch every
            // tab's snapshot so hop/target ``internal`` annotations
            // converge, then repaint. The Influx SOURCE may also have
            // changed (or been cleared): every tab drops its overlay
            // points/error, invalidates in-flight throughput responses
            // (generation bump — old-source points can never render or be
            // reused, in-flight or paused), refetches the active tab when
            // still enabled+configured, and disables the toggle otherwise.
            tabManager.getTabs().forEach(function (t) {
              if (t && typeof t.onInfluxSourceChanged === "function") {
                t.onInfluxSourceChanged();
              }
              if (t && typeof t.refreshMeta === "function") t.refreshMeta();
            });
            var a = tabManager.getActive();
            if (a && typeof a.refreshViews === "function") a.refreshViews();
          })
          .catch(function (err) {
            setSettingsStatus("Save failed: " + err.message);
          });
      });
    }
  }

  // ----- global 1s tick + resize (dispatched to active tab) -----------

  function globalTick() {
    var a = tabManager.getActive();
    if (a) a.tick();
  }

  var globalResizeTimer = null;
  function globalResize() {
    if (globalResizeTimer) clearTimeout(globalResizeTimer);
    globalResizeTimer = setTimeout(function () {
      var a = tabManager.getActive();
      if (a) a.handleResize();
    }, 120);
  }

  // ----- bootstrap -----------------------------------------------------
  //
  // 1. Wire toolbar + global timers.
  // 2. GET /api/traces → open one tab per existing session (criterion 5).
  //    This also prevents orphaned sessions pinging forever after a reload.
  // 3. If no sessions exist, show a single idle fresh tab (criterion 8).

  function bootstrap() {
    historyState.list = loadTargetHistory();
    renderTargetHistory();
    wireToolbar();
    wireSettings();
    setInterval(globalTick, 1000);
    window.addEventListener("resize", globalResize);

    // InfluxDB overlay availability (drives the throughput checkbox's
    // disabled state). Best-effort; unconfigured/unreachable leaves the
    // overlay off.
    api("/api/settings/influx")
      .then(asJson)
      .then(function (s) { serverSettings.influxUrl = (s && s.url) || ""; })
      .catch(function () { /* overlay stays disabled */ });

    api("/api/traces")
      .then(asJson)
      .then(function (sessions) {
        if (sessions && sessions.length > 0) {
          sessions.forEach(function (s) {
            var tab = createTab(s);
            tabManager.add(tab, false);
            tab.attach();
          });
          // Activate the first session tab (oldest, by GET /api/traces
          // insertion order).
          tabManager.setActive(tabManager.getTabs()[0]);
        } else {
          // No active sessions: a single idle fresh tab (the original
          // single-trace view).
          tabManager.add(createTab(null), true);
        }
      })
      .catch(function () {
        // Backend unreachable: still show a fresh idle tab so the user
        // sees the dashboard rather than a blank page.
        if (tabManager.getTabs().length === 0) {
          tabManager.add(createTab(null), true);
        }
      });
  }

  bootstrap();
})();
