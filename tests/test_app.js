"use strict";

const assert = require("assert");
const PURE = require("../src/netplot/static/app.js");

function state() {
  return {
    samples: {},
    seedStats: {},
    seedCut: {},
    seedFrom: {},
    seedCap: {},
    seedFocus: {},
    seedWatermark: {},
    covFrom: {},
    covInFlight: {},
    covGen: 0,
    covConnected: false,
  };
}

const now = 1000;

// Live-only intermediate-hop data must be rendered instead of falling back
// to the zero/dash placeholder.
{
  const s = state();
  PURE.pushSample(s, 2, 995, 12);
  PURE.pushSample(s, 2, 998, 18);
  const result = PURE.statsForHop(s, 2, now);
  assert.strictEqual(result.source, "live");
  assert.strictEqual(result.data.count, 2);
  assert.strictEqual(result.data.avg, 15);
  assert.strictEqual(result.data.cur, 18);
}

// No seed and no live samples remains the placeholder path.
assert.strictEqual(PURE.statsForHop(state(), 2, now), null);

// A live delta still combines with a live snapshot baseline: the baseline is
// adopted from the server contract fields (captured_at anchor, echoed focus
// width, sample_through_t cutoff, sample_from_t oldest-sample anchor) and
// combined ONLY with buffered samples strictly after that cutoff, so the
// merged snapshot tail in the buffer is never counted twice.
{
  const s = state();
  PURE.adoptSnapshotBaseline(
    s, "2",
    { count: 10, avg_ms: 20, min_ms: 10, cur_ms: 20, loss_pct: 10 },
    now - 300, // captured_at: baseline window still overlaps [now-600, now]
    994,       // sample_through_t: latest sample in the baseline window
    now - 400, // sample_from_t: oldest baseline sample inside the window
    PURE.FOCUS_S
  );
  PURE.pushSample(s, 2, 995, 30);
  PURE.pushSample(s, 2, 998, 30);
  const result = PURE.statsForHop(s, 2, now);
  assert.strictEqual(result.source, "combined");
  assert.strictEqual(result.data.count, 12);
  assert.ok(Math.abs(result.data.avg - (9 * 20 + 2 * 30) / 11) < 1e-9);
  assert.ok(Math.abs(result.data.lossPct - (1 / 12) * 100) < 1e-9);
}

// Mixed losses across BOTH parts keep the weighted statistics exact: the
// baseline's aggregate loss ratio and the delta's losses are combined by
// count, avg/min blend only over successes.
{
  const s = state();
  // Baseline: 10 samples, 20% loss (2 losses), avg 20 over 8 successes.
  PURE.adoptSnapshotBaseline(
    s, "1",
    { count: 10, avg_ms: 20, min_ms: 12, cur_ms: 20, loss_pct: 20 },
    now - 100, 950, 800, PURE.FOCUS_S
  );
  // Delta: 30 (success), 40 (success), loss — strictly after the cutoff.
  PURE.pushSample(s, 1, 951, 30);
  PURE.pushSample(s, 1, 952, 40);
  PURE.pushSample(s, 1, 953, null);
  const r = PURE.statsForHop(s, 1, now);
  assert.strictEqual(r.source, "combined");
  assert.strictEqual(r.data.count, 13);
  assert.ok(Math.abs(r.data.avg - (8 * 20 + 30 + 40) / 10) < 1e-9);
  assert.strictEqual(r.data.min, 12);
  assert.strictEqual(r.data.max, 40);
  assert.strictEqual(r.data.cur, null); // last sample is a loss
  assert.ok(Math.abs(r.data.lossPct - (3 / 13) * 100) < 1e-9);
}

// Acceptance repro: a three-sample baseline merged with a 29-unique-sample
// buffer counts 29, not 32. The buffer carries the baseline's own samples
// (merged snapshot tail for graphs) plus the post-cutoff live delta; the
// delta path counts only samples strictly after sample_through_t.
{
  const s = state();
  const baselineSamples = [
    { t: 970, rtt_ms: 10 },
    { t: 971, rtt_ms: 11 },
    { t: 972, rtt_ms: 12 },
  ];
  PURE.adoptSnapshotBaseline(
    s, "0",
    { count: 3, avg_ms: 11, min_ms: 10, max_ms: 12, cur_ms: 12, loss_pct: 0 },
    972, 972, 970, PURE.FOCUS_S
  );
  // Hydration merges the snapshot tail into the raw buffer (graphs keep it).
  s.samples["0"] = PURE.mergeSamples(s.samples["0"] || [], baselineSamples, null);
  // 26 live rounds after the cutoff -> 29 unique samples in the buffer.
  for (let i = 973; i <= 998; i++) PURE.pushSample(s, 0, i, 20);
  assert.strictEqual(s.samples["0"].length, 29);

  // Loading path (coverage not yet hydrated): baseline + strict delta.
  const loading = PURE.statsForHop(s, 0, now);
  assert.strictEqual(loading.source, "combined");
  assert.strictEqual(loading.data.count, 29, "29, not 32");
  assert.ok(Math.abs(loading.data.avg - (3 * 11 + 26 * 20) / 29) < 1e-9);

  // Hydrated path (stats window covered): unique raw samples are the SOLE
  // source — same count, no second addition of the baseline aggregate.
  PURE.extendCoverage(s, "0", now - PURE.FOCUS_S);
  const hydrated = PURE.statsForHop(s, 0, now);
  assert.strictEqual(hydrated.source, "live");
  assert.strictEqual(hydrated.data.count, 29);
  assert.ok(Math.abs(hydrated.data.avg - (3 * 11 + 26 * 20) / 29) < 1e-9);
  assert.strictEqual(hydrated.data.cur, 20);
}

// Baseline aging is bounded by the aggregate's OLDEST sample (sample_from_t):
// a full aggregate is reusable only while its oldest sample is still at/after
// the selected window start (inclusive boundary), then retired — never reused
// on mere window overlap, and never anchored to client receipt time.
{
  const s = state();
  PURE.adoptSnapshotBaseline(
    s, "0",
    { count: 5, avg_ms: 20, min_ms: 10, cur_ms: 20, loss_pct: 0 },
    550, // captured_at: baseline window [490, 550]
    555, // sample_through_t: latest sample in the baseline window
    541, // sample_from_t: oldest sample — the authoritative aging anchor
    60
  );
  // Strictly after the cutoff, inside the window at now=560.
  PURE.pushSample(s, 0, 556, 30);
  const edge = PURE.statsForHopWindow(s, 0, 560, 60);
  assert.strictEqual(edge.source, "combined");
  assert.strictEqual(edge.data.count, 6);
  // At from + focus (601) the oldest sample sits exactly on the window
  // start: the boundary is INCLUSIVE, the aggregate still fits the window.
  const boundary = PURE.statsForHopWindow(s, 0, 601, 60);
  assert.strictEqual(boundary.source, "combined");
  assert.strictEqual(boundary.data.count, 6);
  // One second past from + focus the oldest sample has crossed the window
  // start (mere overlap is not enough): live-only.
  const aged = PURE.statsForHopWindow(s, 0, 602, 60);
  assert.strictEqual(aged.source, "live");
  assert.strictEqual(aged.data.count, 1);
}

// Repro (baseline overcount): a full aggregate is NOT reusable while its
// window merely overlaps the selected one. Baseline count 2 (samples at 401
// and 900) captured at 1000; at now=1002 the window starts at 402, so the
// t=401 sample has aged out — the aggregate (count 2) must be retired and
// the raw buffer alone counts 1.
{
  const s = state();
  s.samples["0"] = [
    { t: 401, rtt_ms: 10 },
    { t: 900, rtt_ms: 30 },
  ];
  PURE.adoptSnapshotBaseline(
    s, "0",
    { count: 2, avg_ms: 20, min_ms: 10, cur_ms: 30, loss_pct: 0 },
    1000, 900, 401, 600
  );
  // Boundary: oldest sample exactly on the window start — aggregate exact.
  const edge = PURE.statsForHopWindow(s, 0, 1001, 600);
  assert.strictEqual(edge.source, "seed");
  assert.strictEqual(edge.data.count, 2);
  // Oldest sample crossed the window start: raw-only, count 1 (was 2).
  const aged = PURE.statsForHopWindow(s, 0, 1002, 600);
  assert.strictEqual(aged.source, "live");
  assert.strictEqual(aged.data.count, 1);
  assert.strictEqual(aged.data.avg, 30);
  // The raw buffer is retained for graphs despite the baseline retirement.
  assert.strictEqual(s.samples["0"].length, 2);
}

// Focus 0 (single-instant window, if a zero-width Focus ever reaches the
// client): the baseline is reusable only while its oldest sample IS the
// instant (nowS <= from + 0), and nothing is invented for later instants.
{
  const s = state();
  PURE.adoptSnapshotBaseline(
    s, "0",
    { count: 1, avg_ms: 10, min_ms: 10, cur_ms: 10, loss_pct: 0 },
    500, 500, 500, 0
  );
  const at = PURE.statsForHopWindow(s, 0, 500, 0);
  assert.strictEqual(at.source, "seed");
  assert.strictEqual(at.data.count, 1);
  // The instant passed: no coverage, no valid baseline — no invented stats.
  assert.strictEqual(PURE.statsForHopWindow(s, 0, 501, 0), null);
}

// A baseline fetched for a different window width is suspended on focus
// changes until the refetched snapshot for the new window is adopted.
{
  const s = state();
  PURE.adoptSnapshotBaseline(
    s, "0",
    { count: 50, avg_ms: 20, min_ms: 10, cur_ms: 20, loss_pct: 0 },
    now - 100, 995, 800, 600
  );
  PURE.pushSample(s, 0, 996, 25);
  // Same width: combined.
  assert.strictEqual(PURE.statsForHopWindow(s, 0, now, 600).source, "combined");
  // Narrowed window: seedFocus mismatch suspends the aggregate -> live-only
  // (10 raw samples in [940,1000]... only 996 exists here -> count 1).
  const narrowed = PURE.statsForHopWindow(s, 0, now, 60);
  assert.strictEqual(narrowed.source, "live");
  assert.strictEqual(narrowed.data.count, 1);
}

// Snapshot focus_s echo: preserved EXACTLY, including the HTTP-only
// all-retained 0 (the Focus dropdown offers no zero option, but snapshot
// requests do). A missing or malformed echo is rejected outright — never
// substituted with the selected window — so no mislabeled aggregate can
// masquerade as a baseline for the selected 60/600 window.
{
  const s = state();
  assert.strictEqual(PURE.snapshotFocusSeconds({ focus_s: 0 }), 0);
  assert.strictEqual(PURE.snapshotFocusSeconds({ focus_s: 600 }), 600);
  // All-retained echo (focus_s 0): adopted under its own width…
  PURE.adoptSnapshotBaseline(
    s, "0",
    { count: 50, avg_ms: 20, min_ms: 10, cur_ms: 20, loss_pct: 0 },
    995, 995, 10, 0
  );
  assert.strictEqual(s.seedFocus["0"], 0);
  // …and it never masquerades as the selected positive window: suspended
  // for both 60 and 600 (empty raw buffer -> null; the aggregate is NOT
  // reinterpreted as all-history coverage of the selected window).
  assert.strictEqual(PURE.statsForHopWindow(s, 0, now, 600), null,
    "all-retained baseline cannot serve the 600 window");
  assert.strictEqual(PURE.statsForHopWindow(s, 0, now, 60), null,
    "all-retained baseline cannot serve the 60 window");
  // Within its own width it stays exact: the single-instant window admits
  // the baseline only while its oldest sample IS the instant (10 <= 10+0),
  // and nothing is invented once the instant has passed.
  assert.strictEqual(PURE.statsForHopWindow(s, 0, 10, 0).source, "seed");
  assert.strictEqual(PURE.statsForHopWindow(s, 0, 10, 0).data.count, 50);
  assert.strictEqual(PURE.statsForHopWindow(s, 0, 11, 0), null);

  // Missing/malformed echoes: null, never the currently selected width.
  assert.strictEqual(PURE.snapshotFocusSeconds({}), null);
  assert.strictEqual(PURE.snapshotFocusSeconds({ focus_s: "60" }), null);
  assert.strictEqual(PURE.snapshotFocusSeconds({ focus_s: -60 }), null);
  assert.strictEqual(PURE.snapshotFocusSeconds({ focus_s: NaN }), null);
  assert.strictEqual(PURE.snapshotFocusSeconds({ focus_s: Infinity }), null);
  assert.strictEqual(PURE.snapshotFocusSeconds(null), null);

  // …and adoption rejects them wholesale: no baseline and no tombstone may
  // be written under an unknown window width (empty payloads included).
  const r = state();
  assert.strictEqual(PURE.adoptSnapshotBaseline(
    r, "7",
    { count: 5, avg_ms: 1, min_ms: 1, cur_ms: 1, loss_pct: 0 },
    900, 900, 850, undefined
  ), false, "missing echo rejected");
  assert.strictEqual(PURE.adoptSnapshotBaseline(
    r, "7", { count: 0, avg_ms: null, min_ms: null, cur_ms: null, loss_pct: 0 },
    901, null, null, null
  ), false, "empty payload with unknown width rejected");
  assert.strictEqual(r.seedStats["7"], undefined);
  assert.strictEqual(r.seedCap["7"], undefined);
  assert.strictEqual(r.seedFocus["7"], undefined);

  // A truthful 60-wide echo is stored as 60 and serves only the 60 window —
  // never the 600 window (suspended until the 600 snapshot lands).
  const m = state();
  PURE.adoptSnapshotBaseline(
    m, "0",
    { count: 50, avg_ms: 20, min_ms: 10, cur_ms: 20, loss_pct: 0 },
    995, 995, 941, 60
  );
  assert.strictEqual(m.seedFocus["0"], 60);
  assert.strictEqual(PURE.statsForHopWindow(m, 0, now, 600), null,
    "60-wide baseline cannot serve the 600 window");
}

// Watermarks reject only samples represented by the baseline: the cutoff is
// the payload's sample_through_t, and an empty baseline has no watermark so
// post-request SSE samples are never discarded.
{
  const s = state();
  PURE.adoptSnapshotBaseline(
    s, "2",
    { count: 2, avg_ms: 10, min_ms: 10, cur_ms: 10, loss_pct: 0 },
    now - 100, 100, 100, PURE.FOCUS_S
  );
  PURE.pushSample(s, 2, 100, 10);
  PURE.pushSample(s, 2, 101, 11);
  assert.deepStrictEqual(s.samples["2"], [{ t: 101, rtt_ms: 11 }]);
}

// Snapshot adoption guards: monotonic in captured_at, explicit empty-stream
// handling, and rejection of payloads without a capture anchor.
{
  // An OLDER snapshot arriving after a newer adoption is rejected wholesale:
  // the newer baseline survives and the live sample pushed past its cutoff
  // is preserved.
  const s = state();
  const newer = { count: 20, avg_ms: 15, min_ms: 5, cur_ms: 15, loss_pct: 0 };
  assert.strictEqual(
    PURE.adoptSnapshotBaseline(s, "3", newer, 900, 899, 850, 60), true
  );
  PURE.pushSample(s, 3, 900, 42); // live sample after the newer cutoff
  assert.strictEqual(
    PURE.adoptSnapshotBaseline(
      s, "3", { count: 5, avg_ms: 99, min_ms: 1, cur_ms: 99, loss_pct: 0 },
      850, 849, 800, 60
    ),
    false,
    "older captured_at rejected"
  );
  assert.strictEqual(s.seedStats["3"], newer);
  assert.strictEqual(s.seedCut["3"], 899);
  assert.deepStrictEqual(s.samples["3"], [{ t: 900, rtt_ms: 42 }]);

  // Equal captured_at adopts (paused snapshots and same-instant top-level /
  // per-hop fetches are all captured at the frozen anchor).
  const same = { count: 21, avg_ms: 16, min_ms: 5, cur_ms: 16, loss_pct: 0 };
  assert.strictEqual(PURE.adoptSnapshotBaseline(s, "3", same, 900, 900, 850, 60), true);
  assert.strictEqual(s.seedStats["3"], same);
  assert.strictEqual(s.seedCut["3"], 900);

  // A REPEATED (identical) snapshot is idempotent: re-adoption replaces the
  // baseline rather than accumulating it, so counts stay stable.
  const repeat = state();
  const payload = { count: 3, avg_ms: 25, min_ms: 25, cur_ms: 25, loss_pct: 0 };
  PURE.adoptSnapshotBaseline(repeat, "0", payload, 972, 972, 970, PURE.FOCUS_S);
  repeat.samples["0"] = PURE.mergeSamples(repeat.samples["0"] || [], [
    { t: 970, rtt_ms: 25 }, { t: 971, rtt_ms: 25 }, { t: 972, rtt_ms: 25 },
  ], null);
  for (let i = 973; i <= 998; i++) PURE.pushSample(repeat, 0, i, 25);
  const first = PURE.statsForHop(repeat, 0, now);
  assert.strictEqual(PURE.adoptSnapshotBaseline(repeat, "0", payload, 972, 972, 970, PURE.FOCUS_S), true);
  const second = PURE.statsForHop(repeat, 0, now);
  assert.strictEqual(second.source, first.source);
  assert.strictEqual(second.data.count, first.data.count);
  assert.strictEqual(first.data.count, 29, "baseline + 26 delta, stable across re-adoption");

  // An EMPTY snapshot clears the stream's baseline, cutoff, aging anchor,
  // and watermark outright instead of leaving the previous aggregate stale
  // — while PRESERVING the latest capture marker.
  assert.strictEqual(PURE.adoptSnapshotBaseline(s, "3", same, 901, null, null, 60), true);
  assert.strictEqual(s.seedStats["3"], undefined);
  assert.strictEqual(s.seedCut["3"], undefined);
  assert.strictEqual(s.seedCap["3"], 901);
  assert.strictEqual(s.seedWatermark["3"], undefined);
  // ...and a count-0 payload behaves identically.
  assert.strictEqual(
    PURE.adoptSnapshotBaseline(
      s, "3", { count: 0, avg_ms: null, min_ms: null, cur_ms: null, loss_pct: 0 },
      902, 902, null, 60
    ),
    true
  );
  assert.strictEqual(s.seedStats["3"], undefined);
  assert.strictEqual(s.seedCap["3"], 902);

  // A newer EMPTY snapshot preserves the capture marker, so an OLDER
  // nonempty snapshot arriving later is rejected instead of resurrecting a
  // stale aggregate behind the cleared baseline (the old code deleted the
  // marker with the baseline and re-adopted the older payload).
  assert.strictEqual(
    PURE.adoptSnapshotBaseline(
      s, "3", { count: 4, avg_ms: 50, min_ms: 5, cur_ms: 50, loss_pct: 0 },
      1010, null, null, 600
    ),
    true
  );
  assert.strictEqual(s.seedStats["3"], undefined);
  assert.strictEqual(
    PURE.adoptSnapshotBaseline(
      s, "3", { count: 4, avg_ms: 50, min_ms: 5, cur_ms: 50, loss_pct: 0 },
      1000, 999, 400, 600
    ),
    false,
    "older nonempty payload rejected behind cleared baseline"
  );
  assert.strictEqual(s.seedStats["3"], undefined);
  assert.strictEqual(s.seedCut["3"], undefined);
  assert.strictEqual(s.seedCap["3"], 1010);
  // Equal frozen paused captures still adopt after a clear.
  assert.strictEqual(
    PURE.adoptSnapshotBaseline(
      s, "3", { count: 4, avg_ms: 50, min_ms: 5, cur_ms: 50, loss_pct: 0 },
      1010, 1009, 500, 600
    ),
    true
  );
  assert.strictEqual(s.seedStats["3"].count, 4);

  // A payload without captured_at is never adopted: baseline aging must not
  // fall back to client receipt time or the latest local sample.
  assert.strictEqual(
    PURE.adoptSnapshotBaseline(
      s, "4", { count: 5, avg_ms: 1, min_ms: 1, cur_ms: 1, loss_pct: 0 },
      undefined, 900, 850, 60
    ),
    false
  );
  assert.strictEqual(s.seedStats["4"], undefined);

  // A nonempty payload without sample_from_t is rejected outright: aging is
  // bounded by the oldest sample, and inventing it client-side (e.g. from
  // the theoretical window start) would silently fabricate coverage the
  // server never reported.
  assert.strictEqual(
    PURE.adoptSnapshotBaseline(
      s, "5", { count: 5, avg_ms: 1, min_ms: 1, cur_ms: 1, loss_pct: 0 },
      900, 900, undefined, 60
    ),
    false
  );
  assert.strictEqual(s.seedStats["5"], undefined);
  // No baseline, no coverage: nothing is invented for the window.
  assert.strictEqual(PURE.statsForHopWindow(s, 5, 1000, 60), null);
}

// Coverage adoption: min of adopted window starts; a hydrated window makes
// the raw buffer the sole source even while a baseline is still valid, and
// a hydrated-but-empty window truthfully reports zero.
{
  const s = state();
  assert.strictEqual(PURE.extendCoverage(s, "0", 500), 500);
  assert.strictEqual(PURE.extendCoverage(s, "0", 700), 500, "coverage only extends earlier");
  assert.strictEqual(PURE.extendCoverage(s, "0", 300), 300);

  const s2 = state();
  PURE.adoptSnapshotBaseline(
    s2, "0",
    { count: 10, avg_ms: 20, min_ms: 10, cur_ms: 20, loss_pct: 0 },
    995, 995, 941, 60
  );
  s2.samples["0"] = [{ t: 995, rtt_ms: 20 }];
  PURE.pushSample(s2, 0, 996, 30);
  // Without coverage: combined (11).
  assert.strictEqual(PURE.statsForHopWindow(s2, 0, now, 60).data.count, 11);
  // Coverage complete: raw-only (2 unique samples), even though the
  // baseline is still valid.
  PURE.extendCoverage(s2, "0", now - 60);
  const raw = PURE.statsForHopWindow(s2, 0, now, 60);
  assert.strictEqual(raw.source, "live");
  assert.strictEqual(raw.data.count, 2);

  // Hydrated but empty: the zero count is truthful (a stale aggregate is
  // never shown for a covered window).
  const s3 = state();
  PURE.adoptSnapshotBaseline(
    s3, "0",
    { count: 10, avg_ms: 20, min_ms: 10, cur_ms: 20, loss_pct: 0 },
    400, 400, 341, 60
  );
  PURE.extendCoverage(s3, "0", 940);
  const empty = PURE.statsForHopWindow(s3, 0, 1000, 60);
  assert.strictEqual(empty.source, "live");
  assert.strictEqual(empty.data.count, 0);
}

// Coverage in-flight OWNERSHIP: the marker carries the owning request's
// token, so a stale completion (an old focus/reset generation whose marker
// was replaced by a newer backfill, or wiped by a reconnect) releases
// nothing — it can never clear a newer request's in-flight flag and unlock
// a duplicate backfill while the current one is still outstanding.
{
  const s = state();
  s.covInFlight["3"] = 12; // newer generation's backfill owns the marker
  assert.strictEqual(PURE.releaseCoverageInFlight(s, "3", 4), false,
    "stale token cannot release");
  assert.strictEqual(s.covInFlight["3"], 12, "newer request still outstanding");
  assert.strictEqual(PURE.releaseCoverageInFlight(s, "3", 12), true, "owner releases");
  assert.strictEqual(s.covInFlight["3"], false);
  assert.strictEqual(PURE.releaseCoverageInFlight(s, "3", 12), false,
    "release is one-shot");
  // Unknown stream / wiped map (reconnect, reset): nothing to release.
  assert.strictEqual(PURE.releaseCoverageInFlight(state(), "3", 12), false);
}

// Coverage CONTINUITY generation (transport break/reconnect fencing): a
// trusted claim is invalidated the moment the SSE transport breaks, a
// pre-break history completion cannot restore it, and only a completion
// from the current connection generation can. The break also wipes the old
// requests' ownership tokens, so their late completions release nothing and
// can never clear a newer request's in-flight marker.
{
  const s = state();
  s.covGen = 3;
  assert.strictEqual(PURE.extendCoverageIfCurrent(s, "0", 940, 3, s.covGen), undefined,
    "a backfill before initial SSE ready cannot establish coverage");
  s.covConnected = true;
  PURE.adoptSnapshotBaseline(
    s, "0",
    { count: 10, avg_ms: 20, min_ms: 10, cur_ms: 20, loss_pct: 0 },
    995, 995, 941, 60
  );
  s.samples["0"] = [{ t: 995, rtt_ms: 20 }];
  PURE.pushSample(s, 0, 996, 30);

  // Trusted: a current-generation completion establishes coverage and the
  // raw buffer becomes the sole statistics source (2 unique samples).
  assert.strictEqual(PURE.extendCoverageIfCurrent(s, "0", 940, 3, s.covGen), 940);
  const trusted = PURE.statsForHopWindow(s, 0, now, 60);
  assert.strictEqual(trusted.source, "live");
  assert.strictEqual(trusted.data.count, 2);

  // Transport break: claims invalidated IMMEDIATELY (accounting falls back
  // to the baseline-combined loading path — the outage may already be
  // dropping samples) and the continuity generation advances.
  assert.strictEqual(PURE.breakCoverageContinuity(s), 4);
  assert.strictEqual(s.covFrom["0"], undefined);
  assert.strictEqual(s.covInFlight["0"], undefined);
  assert.strictEqual(s.covConnected, false);
  const afterBreak = PURE.statsForHopWindow(s, 0, now, 60);
  assert.strictEqual(afterBreak.source, "combined");
  assert.strictEqual(afterBreak.data.count, 11);

  // A pre-break history completion cannot restore coverage: its fetched
  // window cannot span the outage gap, so the claim is refused and
  // accounting stays on the baseline path.
  assert.strictEqual(PURE.extendCoverageIfCurrent(s, "0", 940, 3, s.covGen), undefined,
    "stale-generation completion cannot restore coverage");
  assert.strictEqual(PURE.statsForHopWindow(s, 0, now, 60).source, "combined");
  // A focus-triggered request can start after error and capture the current
  // generation. It still cannot prove continuity while SSE is disconnected.
  assert.strictEqual(PURE.extendCoverageIfCurrent(s, "0", 940, 4, s.covGen), undefined,
    "an outage-era current-generation completion cannot claim coverage");
  assert.strictEqual(s.covFrom["0"], undefined);

  // Reconnect advances the generation again, fencing requests issued during
  // the outage (they captured the break generation and their windows still
  // cannot span the reconnect gap).
  assert.strictEqual(PURE.breakCoverageContinuity(s), 5);
  assert.strictEqual(PURE.extendCoverageIfCurrent(s, "0", 940, 4, s.covGen), undefined,
    "outage-era completion cannot claim across the gap");
  s.covConnected = true; // the current EventSource delivered ready
  // The current generation's completion CAN re-establish coverage.
  assert.strictEqual(PURE.extendCoverageIfCurrent(s, "0", 940, 5, s.covGen), 940);
  const restored = PURE.statsForHopWindow(s, 0, now, 60);
  assert.strictEqual(restored.source, "live");
  assert.strictEqual(restored.data.count, 2);

  // Ownership: the break wiped the pre-break marker; its late completion
  // releases nothing and cannot clobber the current backfill's marker.
  assert.strictEqual(PURE.breakCoverageContinuity(s), 6);
  s.covInFlight["0"] = 21; // current-generation backfill owns the stream
  assert.strictEqual(PURE.releaseCoverageInFlight(s, "0", 7), false,
    "pre-break token releases nothing");
  assert.strictEqual(s.covInFlight["0"], 21, "current request still owns the stream");
  assert.strictEqual(PURE.releaseCoverageInFlight(s, "0", 21), true, "owner releases");
}

// Focus expiry uses the same 605-second retention boundary as pushSample.
{
  const focus = { startS: 100, endS: 200 };
  assert.strictEqual(PURE.focusExpired(focus, 200 + PURE.RETENTION_S), false);
  assert.strictEqual(PURE.focusExpired(focus, 200 + PURE.RETENTION_S + 0.001), true);
  assert.strictEqual(PURE.focusExpired(null, 9999), false);
}

// ----------------------------------------------------------- Slice 5 PURE
// (timeline geometry, focus stats, band boundaries, tick stepping).

// band boundaries at PingPlotter Cloud reference: 100/200.
{
  const b = PURE.bandBoundaries();
  assert.strictEqual(b.green, 100);
  assert.strictEqual(b.yellow, 200);
}

// thresholdBoundaries mirrors bandBoundaries (exposed for docs/assertions).
{
  const t = PURE.thresholdBoundaries();
  assert.strictEqual(t.green, 100);
  assert.strictEqual(t.yellow, 200);
}

// AXIS_FLOOR_MS is the new floor (yellow + 1 = 201).
assert.strictEqual(PURE.AXIS_FLOOR_MS, 201);

// axisMax floors at AXIS_FLOOR_MS (201) and grows past it.
{
  assert.strictEqual(PURE.axisMax([]), 201);
  assert.strictEqual(PURE.axisMax([null, undefined, NaN]), 201);
  assert.strictEqual(PURE.axisMax([50, 150]), 201);
  assert.strictEqual(PURE.axisMax([200]), 201); // equal to yellow, still floor
  assert.strictEqual(PURE.axisMax([300]), 300);
  assert.strictEqual(PURE.axisMax([150, 400, 12]), 400);
}

// ---------------------------------------------- legend bar (Avg-latency) PURE
// The legend bar's scale is DECOUPLED from the row graph-column axis: it
// floors at LEGEND_FLOOR_MS (300) so all three zones stay at truthful,
// legible proportions on healthy traces and the boundary labels do not
// drift with the data; it extends truthfully once axisMax passes 300.

// LEGEND_FLOOR_MS is 300 (decoupled floor above the 201 row-axis floor).
assert.strictEqual(PURE.LEGEND_FLOOR_MS, 300);

// legendMax floors at 300 and tracks axisMax only past it.
{
  // Healthy trace: axisMax at its 201 floor -> legend stays at 300.
  assert.strictEqual(PURE.legendMax(201), 300);
  assert.strictEqual(PURE.legendMax(0), 300);
  // axisMax inside (201, 300] -> still 300 (scale does not move).
  assert.strictEqual(PURE.legendMax(250), 300);
  assert.strictEqual(PURE.legendMax(300), 300);
  // axisMax above 300 -> legend extends truthfully to axisMax.
  assert.strictEqual(PURE.legendMax(301), 301);
  assert.strictEqual(PURE.legendMax(480), 480);
  assert.strictEqual(PURE.legendMax(2000), 2000);
  // Non-finite / non-positive input falls back to the floor.
  assert.strictEqual(PURE.legendMax(null), 300);
  assert.strictEqual(PURE.legendMax(undefined), 300);
  assert.strictEqual(PURE.legendMax(NaN), 300);
  assert.strictEqual(PURE.legendMax(-50), 300);
  assert.strictEqual(PURE.legendMax("garbage"), 300);
}

// Legend scale stability: while axisMax varies inside [0, 300] the legend
// scale does not move (criterion 2). The zone boundary percents derived
// from legendMax are identical across that whole axisMax band.
{
  const pcts = [0, 201, 250, 300].map(function (am) {
    return PURE.zonePercents(PURE.legendMax(am));
  });
  for (let i = 1; i < pcts.length; i++) {
    assert.deepStrictEqual(pcts[i], pcts[0], "legend zones stable under 300");
  }
  // Once axisMax > 300 the legend extends, so boundary percents shift.
  const over300 = PURE.zonePercents(PURE.legendMax(600));
  assert.ok(over300.green < pcts[0].green, "green boundary shrinks past 300");
  assert.ok(over300.yellow < pcts[0].yellow, "yellow boundary shrinks past 300");
}

// legendMarkerPercent: avg -> percent on the legend bar; null avg -> null.
{
  // 150 ms avg on a 300 ms legend -> 50%.
  assert.ok(Math.abs(PURE.legendMarkerPercent(150, 300) - 50) < 1e-9);
  // 100 ms and 200 ms land on the zone boundaries (where the tick labels sit).
  assert.ok(Math.abs(PURE.legendMarkerPercent(100, 300) - (100 / 3)) < 1e-9);
  assert.ok(Math.abs(PURE.legendMarkerPercent(200, 300) - (200 / 3)) < 1e-9);
  // Clamps to [0, 100]: a spike above legendMax stays inside the bar.
  assert.strictEqual(PURE.legendMarkerPercent(500, 300), 100);
  assert.strictEqual(PURE.legendMarkerPercent(0, 300), 0);
  // Null / NaN avg (silent, loss-only, no samples in window) -> no marker.
  assert.strictEqual(PURE.legendMarkerPercent(null, 300), null);
  assert.strictEqual(PURE.legendMarkerPercent(undefined, 300), null);
  assert.strictEqual(PURE.legendMarkerPercent(NaN, 300), null);
  // Non-positive legendMax -> null (defensive; cannot place a marker).
  assert.strictEqual(PURE.legendMarkerPercent(150, 0), null);
  assert.strictEqual(PURE.legendMarkerPercent(150, -5), null);
}

// legendMarkerLanes: singletons and well-separated markers all get lane 0;
// overlapping markers (within minSepPct) get distinct lanes; input fields
// and order are preserved; input is not mutated.
{
  // Empty / non-array -> [].
  assert.deepStrictEqual(PURE.legendMarkerLanes([]), []);
  assert.deepStrictEqual(PURE.legendMarkerLanes(null), []);

  // Single marker -> lane 0.
  const one = PURE.legendMarkerLanes([{ id: "1", percent: 40 }]);
  assert.strictEqual(one.length, 1);
  assert.strictEqual(one[0].lane, 0);
  assert.strictEqual(one[0].id, "1", "caller fields preserved");

  // Well-separated markers (each > minSepPct apart) -> all lane 0.
  const separated = PURE.legendMarkerLanes([
    { id: "a", percent: 10 },
    { id: "b", percent: 40 },
    { id: "c", percent: 80 },
  ]);
  assert.ok(separated.every(function (m) { return m.lane === 0; }),
    "separated markers share lane 0");
  // Input order preserved.
  assert.deepStrictEqual(
    separated.map(function (m) { return m.id; }),
    ["a", "b", "c"],
    "input order preserved"
  );

  // Two overlapping markers -> distinct lanes (0 and 1).
  const pair = PURE.legendMarkerLanes([
    { id: "a", percent: 50 },
    { id: "b", percent: 50.5 }, // within default minSepPct (1.5)
  ]);
  assert.notStrictEqual(pair[0].lane, pair[1].lane,
    "overlapping pair on distinct lanes");

  // Three overlapping markers -> 0, 1, 2 (round-robin within the cluster).
  const tri = PURE.legendMarkerLanes([
    { id: "a", percent: 50 },
    { id: "b", percent: 50.4 },
    { id: "c", percent: 50.8 },
  ]);
  const lanesTri = tri.map(function (m) { return m.lane; }).sort();
  assert.deepStrictEqual(lanesTri, [0, 1, 2],
    "3-cluster uses all three lanes");

  // Two separate overlap clusters are resolved independently: cluster 1
  // uses lanes 0/1, cluster 2 also starts at lane 0.
  const twoClusters = PURE.legendMarkerLanes([
    { id: "a", percent: 20 },
    { id: "b", percent: 20.3 },
    { id: "c", percent: 70 },
    { id: "d", percent: 70.3 },
  ]);
  // a & b overlap; c & d overlap. Within each pair the lanes differ.
  const byId = {};
  twoClusters.forEach(function (m) { byId[m.id] = m.lane; });
  assert.notStrictEqual(byId.a, byId.b, "cluster 1 distinct lanes");
  assert.notStrictEqual(byId.c, byId.d, "cluster 2 distinct lanes");
  // And the singleton-equivalent of "well separated" stays 0.
  assert.ok(byId.a === 0 || byId.c === 0, "first of each cluster is lane 0");

  // Exactly minSepPct apart still counts as overlap (<=).
  const edge = PURE.legendMarkerLanes([
    { id: "a", percent: 50 },
    { id: "b", percent: 51.5 }, // == default minSepPct
  ], { minSepPct: 1.5 });
  assert.notStrictEqual(edge[0].lane, edge[1].lane,
    "boundary distance counts as overlap");

  // Just over minSepPct -> not overlapping -> both lane 0.
  const gap = PURE.legendMarkerLanes([
    { id: "a", percent: 50 },
    { id: "b", percent: 51.6 },
  ], { minSepPct: 1.5 });
  assert.strictEqual(gap[0].lane, 0);
  assert.strictEqual(gap[1].lane, 0);

  // Caller fields beyond id/percent are carried through.
  const withExtra = PURE.legendMarkerLanes([
    { id: "a", percent: 50, isTarget: false, avg: 12.3 },
    { id: "b", percent: 50.3, isTarget: true, avg: 14.1 },
  ]);
  assert.strictEqual(withExtra[0].avg, 12.3, "avg carried through");
  assert.strictEqual(withExtra[1].isTarget, true, "isTarget carried through");

  // Input array is not mutated.
  const input = [{ id: "a", percent: 50 }, { id: "b", percent: 50.3 }];
  PURE.legendMarkerLanes(input);
  assert.ok(!("lane" in input[0]), "input items not mutated");
  assert.strictEqual(input.length, 2, "input length unchanged");

  // Dense cluster (size > maxLanes) wraps: a 4-cluster with maxLanes=3 yields
  // lanes 0,1,2,0 — the 4th reuses lane 0. Keeps the fan subtle on the rare
  // very-dense case.
  const dense = PURE.legendMarkerLanes([
    { id: "a", percent: 50 },
    { id: "b", percent: 50.1 },
    { id: "c", percent: 50.2 },
    { id: "d", percent: 50.3 },
  ]);
  assert.deepStrictEqual(
    dense.map(function (m) { return m.lane; }),
    [0, 1, 2, 0],
    "dense cluster wraps within maxLanes"
  );
}

// x-mapping is monotonic non-decreasing within the visible range and
// clamps at both edges.
{
  const endS = 1000;
  const rangeS = 600;
  const width = 1200;
  const xs = [200, 500, 800, 999, 1000].map(function (t) {
    return PURE.timeToX(t, endS, rangeS, width);
  });
  assert.ok(xs[0] === 0, "left edge clamps to 0");
  assert.ok(xs[xs.length - 1] === width, "right edge clamps to width");
  for (let i = 1; i < xs.length; i++) {
    assert.ok(xs[i] >= xs[i - 1], "monotonic non-decreasing at index " + i);
  }
}

// timeToX and xToTime are inverses (within float tolerance) inside the range.
{
  const endS = 1000;
  const rangeS = 600;
  const width = 1200;
  const t = 700;
  const x = PURE.timeToX(t, endS, rangeS, width);
  const back = PURE.xToTime(x, endS, rangeS, width);
  assert.ok(Math.abs(back - t) < 1e-9, "xToTime(timeToX(t)) == t");
}

// y-scale headroom: axis max always >= AXIS_FLOOR_MS (201) and rounded up to 50.
{
  assert.ok(PURE.yAxisMax(null) >= 201, "null peak floors at AXIS_FLOOR_MS");
  assert.ok(PURE.yAxisMax(0) >= 201, "zero peak floors at AXIS_FLOOR_MS");
  assert.ok(PURE.yAxisMax(150) >= 201, "below-floor peak keeps bands visible");
  const big = PURE.yAxisMax(600);
  assert.ok(big >= 600, "peak fits below axis max");
  assert.ok(big % 50 === 0, "axis max is a multiple of 50 for tidy ticks");
}

// msToY inverts the latency axis: 0 at top, axisMax at bottom.
{
  const h = 100;
  assert.strictEqual(PURE.msToY(0, 501, h), h, "0 ms -> bottom");
  assert.strictEqual(PURE.msToY(501, 501, h), 0, "axisMax -> top");
  assert.strictEqual(PURE.msToY(null, 501, h), h, "null (loss) -> bottom");
}

// tick stepping for 1m / 5m / 10m produces sensible step sizes.
{
  const step1m = PURE.tickStepS(60, 6);
  const step5m = PURE.tickStepS(300, 6);
  const step10m = PURE.tickStepS(600, 6);
  // 1m range should yield >= 10s tick (so labels don't collide)
  assert.ok(step1m >= 10, "1m tick >= 10s, got " + step1m);
  // 5m range should yield >= 60s tick
  assert.ok(step5m >= 60, "5m tick >= 60s, got " + step5m);
  // 10m range should yield >= 60s tick
  assert.ok(step10m >= 60, "10m tick >= 60s, got " + step10m);
  // Resulting tick count stays within a readable range (targetTicks was 6).
  for (const [rangeS, step, label] of [
    [60, step1m, "1m"],
    [300, step5m, "5m"],
    [600, step10m, "10m"],
  ]) {
    const ticks = PURE.xTicks(rangeS - 0.001, rangeS * 2, step).length;
    assert.ok(ticks >= 3 && ticks <= 10, label + " tick count in [3,10], got " + ticks);
  }
}

// xTicks returns aligned epoch multiples of step within the window.
{
  const ticks = PURE.xTicks(400, 1000, 60);
  assert.ok(ticks.length > 0, "xTicks non-empty");
  for (const t of ticks) {
    assert.ok(t >= 400 && t <= 1000, "tick in window");
    assert.ok(t % 60 === 0, "tick aligned to step");
  }
}

// Focus stats equal rolling-stats when the focus selection covers the whole
// rolling window. computeStatsRange over [nowS-FOCUS_S, nowS] must match
// computeStats over the same samples for nowS.
{
  const samples = [
    { t: now - 500, rtt_ms: 10 },
    { t: now - 400, rtt_ms: null }, // loss
    { t: now - 300, rtt_ms: 20 },
    { t: now - 200, rtt_ms: 30 },
    { t: now - 100, rtt_ms: 40 },
  ];
  const rolling = PURE.computeStats(samples, now);
  const focus = PURE.computeStatsRange(samples, now - PURE.FOCUS_S, now);
  assert.strictEqual(rolling.count, focus.count);
  assert.strictEqual(rolling.avg, focus.avg);
  assert.strictEqual(rolling.min, focus.min);
  assert.strictEqual(rolling.max, focus.max);
  assert.strictEqual(rolling.cur, focus.cur);
  assert.ok(Math.abs(rolling.lossPct - focus.lossPct) < 1e-9);
}

// computeStatsRange clamps to a sub-window and excludes samples outside.
{
  const samples = [
    { t: 100, rtt_ms: 5 },
    { t: 110, rtt_ms: 10 },
    { t: 120, rtt_ms: 15 },
    { t: 130, rtt_ms: 20 },
  ];
  const r = PURE.computeStatsRange(samples, 105, 125);
  assert.strictEqual(r.count, 2, "only samples in [105,125]");
  assert.strictEqual(r.avg, 12.5);
  assert.strictEqual(r.min, 10);
  assert.strictEqual(r.max, 15);
  assert.strictEqual(r.cur, 15);
  assert.strictEqual(r.lossPct, 0);
}

// Loss-mark x positions match sample timestamps (loss samples render at
// the same x as a successful sample would for the same timestamp).
{
  const endS = 1000;
  const rangeS = 600;
  const width = 600;
  const lossT = 700;
  const lossX = PURE.timeToX(lossT, endS, rangeS, width);
  // A successful sample at the same timestamp maps to the same x.
  const okX = PURE.timeToX(lossT, endS, rangeS, width);
  assert.strictEqual(lossX, okX, "loss and success share x at same timestamp");
  // The x is in the visible plot area.
  assert.ok(lossX >= 0 && lossX <= width);
}

// rangeWindow returns [nowS - rangeS, nowS].
{
  const w = PURE.rangeWindow(1000, 60);
  assert.strictEqual(w.startS, 940);
  assert.strictEqual(w.endS, 1000);
}

// formatClock formats epoch seconds as HH:MM:SS.
{
  // 0 epoch -> 1969-12-31 19:00:00 in America/New_York or 1970-01-01 00:00:00 UTC;
  // use a value that is stable in local time by checking the format only.
  const s = PURE.formatClock(1700000000);
  assert.ok(/^\d{2}:\d{2}:\d{2}$/.test(s));
  assert.strictEqual(PURE.formatClock(null), "--:--:--");
  assert.strictEqual(PURE.formatClock(NaN), "--:--:--");
}

// RANGES_S and DEFAULT_RANGE_S are exported for the segmented control.
// Slice D1 extended the offered ranges with 1h (3600) and 3h (10800); Slice
// D2 extends again with 12h (43200) and 24h (86400) served from the server
// rollup tier.
{
  assert.deepStrictEqual(PURE.RANGES_S, [60, 300, 600, 3600, 10800, 43200, 86400]);
  assert.strictEqual(PURE.DEFAULT_RANGE_S, 600);
}

// ----------------------------------------------------------- Slice 6 PURE
// (graph-column geometry: zone percents, whisker, connector, max-tracking).
// latencyBucket now uses the 100/200 boundaries (green ≤100, yellow ≤200).
{
  assert.strictEqual(PURE.latencyBucket(0), "green");
  assert.strictEqual(PURE.latencyBucket(100), "green");
  assert.strictEqual(PURE.latencyBucket(101), "yellow");
  assert.strictEqual(PURE.latencyBucket(200), "yellow");
  assert.strictEqual(PURE.latencyBucket(201), "red");
  assert.strictEqual(PURE.latencyBucket(2000), "red");
  assert.strictEqual(PURE.latencyBucket(null), "loss");
}

// zonePercents at a few axisMax values: the green/yellow boundary percents
// must scale with axisMax rather than a hardcoded axis.
{
  // Floor case: zones sit at 100/201 and 200/201.
  const z201 = PURE.zonePercents(201);
  assert.ok(Math.abs(z201.green - (100 / 201) * 100) < 1e-9);
  assert.ok(Math.abs(z201.yellow - (200 / 201) * 100) < 1e-9);
  assert.ok(z201.green < z201.yellow, "green boundary left of yellow");

  // Mid-scale: 100/400 and 200/400.
  const z400 = PURE.zonePercents(400);
  assert.ok(Math.abs(z400.green - 25) < 1e-9);
  assert.ok(Math.abs(z400.yellow - 50) < 1e-9);

  // Large axis: boundaries collapse toward the left.
  const z2000 = PURE.zonePercents(2000);
  assert.ok(Math.abs(z2000.green - 5) < 1e-9);
  assert.ok(Math.abs(z2000.yellow - 10) < 1e-9);
}

// whiskerPercents returns left/right on the shared axis and degrades to
// null when either bound is missing (the seed-only path's graceful fallback).
{
  const w = PURE.whiskerPercents(50, 300, 600);
  assert.ok(Math.abs(w.left - (50 / 600) * 100) < 1e-9);
  assert.ok(Math.abs(w.right - (300 / 600) * 100) < 1e-9);
  assert.ok(w.left <= w.right, "left <= right");

  assert.strictEqual(PURE.whiskerPercents(null, 100, 600), null, "null min -> null");
  assert.strictEqual(PURE.whiskerPercents(10, null, 600), null, "null max -> null");
  assert.strictEqual(PURE.whiskerPercents(10, 100, 0), null, "non-positive axisMax -> null");
  assert.strictEqual(PURE.whiskerPercents(10, 100, -5), null, "negative axisMax -> null");
}

// connectorPoint: x is the avg's percent across the column, y is the supplied
// row center (callers measure live offsets); null when avg missing.
{
  const pt = PURE.connectorPoint(150, 600, 24);
  assert.ok(Math.abs(pt.x - (150 / 600) * 100) < 1e-9);
  assert.strictEqual(pt.y, 24);

  assert.strictEqual(PURE.connectorPoint(null, 600, 24), null);
  assert.strictEqual(PURE.connectorPoint(150, 0, 24), null);
  assert.strictEqual(PURE.connectorPoint(150, 600, null), null);
  assert.strictEqual(PURE.connectorPoint(150, 600, NaN), null);
}

// Combined-path max: seed stats now merge with live max so the whisker has
// both bounds. Degrades to live.max when seed has no max_ms (the typical
// snapshot case), and to null when only losses have arrived.
{
  // Live max > seed max_ms (snapshot shipped one) -> live wins.
  const s1 = state();
  PURE.adoptSnapshotBaseline(
    s1, "2",
    { count: 10, avg_ms: 20, min_ms: 10, max_ms: 25, cur_ms: 20, loss_pct: 0 },
    now - 300, 994, now - 400, PURE.FOCUS_S
  );
  PURE.pushSample(s1, 2, 995, 30);
  PURE.pushSample(s1, 2, 998, 40);
  const r1 = PURE.statsForHop(s1, 2, now);
  assert.strictEqual(r1.source, "combined");
  assert.strictEqual(r1.data.max, 40, "live max supersedes seed max_ms");

  // Seed max_ms > live max -> seed wins.
  const s2 = state();
  PURE.adoptSnapshotBaseline(
    s2, "2",
    { count: 10, avg_ms: 20, min_ms: 10, max_ms: 90, cur_ms: 20, loss_pct: 0 },
    now - 300, 994, now - 400, PURE.FOCUS_S
  );
  PURE.pushSample(s2, 2, 995, 30);
  PURE.pushSample(s2, 2, 998, 40);
  const r2 = PURE.statsForHop(s2, 2, now);
  assert.strictEqual(r2.data.max, 90, "seed max_ms wins when larger");

  // No max_ms in seed (current backend shape) -> live max; non-null when live
  // has successful samples.
  const s3 = state();
  PURE.adoptSnapshotBaseline(
    s3, "2",
    { count: 10, avg_ms: 20, min_ms: 10, cur_ms: 20, loss_pct: 0 },
    now - 300, 994, now - 400, PURE.FOCUS_S
  );
  PURE.pushSample(s3, 2, 995, 30);
  PURE.pushSample(s3, 2, 998, 40);
  const r3 = PURE.statsForHop(s3, 2, now);
  assert.strictEqual(r3.data.max, 40, "live max only when seed has no max_ms");
}

// -------------------------------------------------- target-row rule (post-6a)
// The target row is appended when the route is ready but its final hop does
// not reach the resolved target IP (silent tail / partial route). The hop-0
// stream carries the target's live stats in that case.
{
  // Final hop == target IP -> no target row (common case, e.g. 1.1.1.1).
  assert.strictEqual(
    PURE.targetRowNeeded([{ index: 5, ip: "1.1.1.1" }], "1.1.1.1", "ready"),
    false,
    "final hop == target -> no target row"
  );
  // Numeric IPv6 literals use the same resolved spelling as the backend's
  // literal-target path and must not create a duplicate target row.
  assert.strictEqual(
    PURE.targetRowNeeded([{ index: 5, ip: "2001:db8::1" }], "2001:db8::1", "ready"),
    false,
    "matching IPv6 target -> no target row"
  );
  // Final hop != target IP -> target row required (silent tail, e.g. ntp).
  assert.strictEqual(
    PURE.targetRowNeeded(
      [{ index: 1, ip: "10.0.0.1" }, { index: 30, ip: null }],
      "133.243.238.243",
      "ready"
    ),
    true,
    "final hop silent != target -> target row"
  );
  // Partial route: final hop responds but is not the target.
  assert.strictEqual(
    PURE.targetRowNeeded([{ index: 5, ip: "9.9.9.9" }], "1.1.1.1", "ready"),
    true,
    "final hop != target -> target row"
  );
  // Unknown target IP -> suppress (snapshot refetch in flight).
  assert.strictEqual(
    PURE.targetRowNeeded([{ index: 5, ip: "1.1.1.1" }], null, "ready"),
    false,
    "unknown target_ip -> suppress"
  );
  assert.strictEqual(
    PURE.targetRowNeeded([{ index: 5, ip: "1.1.1.1" }], undefined, "ready"),
    false,
    "undefined target_ip -> suppress"
  );
  // Route not ready -> no target row (pending/failed use the target-only
  // fallback in renderTable, not the target-row append).
  assert.strictEqual(
    PURE.targetRowNeeded([], "1.1.1.1", "pending"),
    false,
    "pending -> no target row"
  );
  assert.strictEqual(
    PURE.targetRowNeeded([{ index: 5, ip: "9.9.9.9" }], "1.1.1.1", "failed"),
    false,
    "failed -> no target row"
  );
}

// Primary timeline hop selection mirrors the target-row decision: hop 0
// when the route does not reach the target, the final hop when it does.
{
  // Final hop == target -> primary is the final hop (PingPlotter default).
  assert.strictEqual(
    PURE.primaryHopIndex([{ index: 5, ip: "1.1.1.1" }], "1.1.1.1", "ready"),
    5,
    "final hop == target -> primary = final hop"
  );
  // Final hop != target -> primary is hop 0 (the target stream).
  assert.strictEqual(
    PURE.primaryHopIndex(
      [{ index: 1, ip: "10.0.0.1" }, { index: 30, ip: null }],
      "133.243.238.243",
      "ready"
    ),
    0,
    "final hop != target -> primary = hop 0"
  );
  // No route yet -> primary is hop 0 (target-only view).
  assert.strictEqual(PURE.primaryHopIndex([], null, "pending"), 0);
}

// Connector final-point math with a target row: the target row contributes
// its hop-0 avg as the last polyline point, exactly as other rows do.
{
  // connectorPoint produces a valid x/y for the target row's avg.
  const pt = PURE.connectorPoint(175, 300, 240);
  assert.ok(pt !== null, "target-row connector point is non-null");
  assert.ok(Math.abs(pt.x - (175 / 300) * 100) < 1e-9, "x = avg percent");
  assert.strictEqual(pt.y, 240, "y = row center");
  // A silent final hop (no avg) yields no point, so the polyline's final
  // point comes from the target row below it.
  assert.strictEqual(PURE.connectorPoint(null, 300, 120), null);
}


// ----------------------------------------------------------- Slice 6b PURE
// (toolbar controls: target history, focus-window mapping, caption
// formatting, seed-expiry window change, parameterised stats window).

// targetHistoryAdd: prepend, dedupe, cap at TARGET_HISTORY_MAX, ignore empty.
{
  // Empty history -> single-element list.
  assert.deepStrictEqual(PURE.targetHistoryAdd([], "1.1.1.1"), ["1.1.1.1"]);
  // Add moves the entry to the front (most-recent-first).
  assert.deepStrictEqual(
    PURE.targetHistoryAdd(["1.1.1.1"], "127.0.0.1"),
    ["127.0.0.1", "1.1.1.1"]
  );
  // Dedupe: re-adding an existing target moves it to the front, no dup.
  assert.deepStrictEqual(
    PURE.targetHistoryAdd(["1.1.1.1", "127.0.0.1"], "1.1.1.1"),
    ["1.1.1.1", "127.0.0.1"]
  );
  // Cap at TARGET_HISTORY_MAX: newest is prepended, previously-last entry
  // is dropped to keep the cap, oldest entries stay.
  const filled = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
  assert.strictEqual(filled.length, PURE.TARGET_HISTORY_MAX);
  const capped = PURE.targetHistoryAdd(filled, "k");
  assert.strictEqual(capped.length, PURE.TARGET_HISTORY_MAX);
  assert.strictEqual(capped[0], "k", "newest at front");
  assert.strictEqual(capped[PURE.TARGET_HISTORY_MAX - 1], "i", "second-to-last stays at the back");
  assert.strictEqual(capped.indexOf("j"), -1, "previously-last entry dropped");
  assert.strictEqual(capped.indexOf("a"), 1, "oldest entry preserved (now at idx 1)");
  // Whitespace target is a no-op (returns a clone of history unchanged).
  assert.deepStrictEqual(PURE.targetHistoryAdd(["x"], "   "), ["x"]);
  // Trim is applied before comparing.
  assert.deepStrictEqual(
    PURE.targetHistoryAdd(["1.1.1.1"], "  1.1.1.1  "),
    ["1.1.1.1"]
  );
  // Original history is not mutated.
  const orig = ["x", "y"];
  PURE.targetHistoryAdd(orig, "z");
  assert.deepStrictEqual(orig, ["x", "y"], "input history not mutated");
}

// focusWindowSeconds maps dropdown values to seconds; "auto" and any
// non-positive/garbage value fall back to FOCUS_S.
{
  assert.strictEqual(PURE.focusWindowSeconds("auto"), PURE.FOCUS_S);
  assert.strictEqual(PURE.focusWindowSeconds("60"), 60);
  assert.strictEqual(PURE.focusWindowSeconds("300"), 300);
  assert.strictEqual(PURE.focusWindowSeconds("600"), 600);
  assert.strictEqual(PURE.focusWindowSeconds(null), PURE.FOCUS_S);
  assert.strictEqual(PURE.focusWindowSeconds(undefined), PURE.FOCUS_S);
  assert.strictEqual(PURE.focusWindowSeconds("garbage"), PURE.FOCUS_S);
  assert.strictEqual(PURE.focusWindowSeconds("-5"), PURE.FOCUS_S);
}

// focusWindowLabel maps dropdown values to human labels.
{
  assert.strictEqual(PURE.focusWindowLabel("auto"), "Auto");
  assert.strictEqual(PURE.focusWindowLabel("60"), "1 minute");
  assert.strictEqual(PURE.focusWindowLabel("300"), "5 minutes");
  assert.strictEqual(PURE.focusWindowLabel("600"), "10 minutes");
  // Unknown value falls back to "Auto".
  assert.strictEqual(PURE.focusWindowLabel("nope"), "Auto");
}

// formatClock12 produces an H:MM:SS AM/PM string and handles null/NaN.
{
  assert.strictEqual(PURE.formatClock12(null), "--:--:-- --");
  assert.strictEqual(PURE.formatClock12(NaN), "--:--:-- --");
}

// formatDateMDY produces an M/D/YYYY string and handles null/NaN.
{
  const s = PURE.formatDateMDY(1700000000);
  assert.ok(/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(s), "M/D/YYYY shape, got " + s);
  assert.strictEqual(PURE.formatDateMDY(null), "--/--/----");
  assert.strictEqual(PURE.formatDateMDY(NaN), "--/--/----");
}

// formatFocusCaption matches the PingPlotter reference shape: when both
// bounds share a calendar date, the date appears once before the time range.
{
  // 600-second window starting at t=1700000000 -> same calendar day for the
  // common case (most TZs); we only assert the shape and the label here.
  const start = 1700000000;
  const end = start + 600;
  const caption = PURE.formatFocusCaption("10 minutes", start, end);
  assert.ok(caption.startsWith("Focus: 10 minutes, "), "prefix, got " + caption);
  assert.ok(caption.indexOf(" - ") > 0, "range separator present");
  // Same-date branch: only one date token (one "M/D/YYYY" match).
  const dateMatches = caption.match(/\d{1,2}\/\d{1,2}\/\d{4}/g);
  // Could be 1 (same day) or 2 (boundary crossing midnight) — both valid.
  assert.ok(dateMatches !== null && (dateMatches.length === 1 || dateMatches.length === 2));

  // Cross-midnight case forces two dates.
  // 1700000000 may or may not be near midnight in the host TZ, so construct
  // an explicit pair that is 24h apart — that straddles at least one date
  // boundary in every TZ.
  const a = 1700000000;
  const b = a + 86400 + 60; // next day, +1 minute
  const cross = PURE.formatFocusCaption("Manual", a, b);
  const crossDates = cross.match(/\d{1,2}\/\d{1,2}\/\d{4}/g);
  assert.strictEqual(crossDates.length, 2, "cross-midnight shows two dates");
  assert.ok(cross.indexOf(" - ") > 0);

  // Null bounds -> "Focus: <label>".
  assert.strictEqual(PURE.formatFocusCaption("Auto", null, null), "Focus: Auto");
  assert.strictEqual(PURE.formatFocusCaption(null, null, null), "Focus: —");
}

// Baseline validity is window-matched (the Slice 6b replacement for
// seed-expiry-follows-the-dropdown): each Focus selection refetches the
// snapshot for that width, the echoed focus_s rides the adoption, and an
// aggregate computed for one width never combines into another width.
{
  const cap = now - 100;
  function adoptWith(focusS) {
    const s = state();
    PURE.adoptSnapshotBaseline(
      s, "0",
      { count: 10, avg_ms: 20, min_ms: 10, cur_ms: 20, loss_pct: 0 },
      cap, cap + 1, cap - 50, focusS
    );
    return s;
  }
  // Matching width: baseline participates (seed-only, nothing after cutoff).
  assert.strictEqual(
    PURE.statsForHopWindow(adoptWith(600), 0, now, 600).source, "seed"
  );
  // Mismatched width: aggregate suspended -> no stats path invents one.
  assert.strictEqual(
    PURE.statsForHopWindow(adoptWith(600), 0, now, 60), null
  );
  assert.strictEqual(
    PURE.statsForHopWindow(adoptWith(60), 0, now, 600), null
  );
}

// computeStatsWindow matches computeStats when passed FOCUS_S, and produces
// narrower counts under a smaller window (Slice 6b Focus dropdown semantics).
{
  const samples = [];
  for (let i = 0; i < 100; i++) {
    samples.push({ t: now - (100 - i), rtt_ms: i % 20 });
  }
  const def = PURE.computeStats(samples, now); // uses FOCUS_S = 600
  const same = PURE.computeStatsWindow(samples, now, PURE.FOCUS_S);
  assert.strictEqual(def.count, same.count, "computeStats delegates to computeStatsWindow");
  assert.strictEqual(def.avg, same.avg);
  assert.strictEqual(def.cur, same.cur);

  // A 60-second window includes fewer samples than a 600-second window.
  const win60 = PURE.computeStatsWindow(samples, now, 60);
  const win600 = PURE.computeStatsWindow(samples, now, 600);
  assert.ok(win60.count < win600.count, "narrower window -> fewer samples");
  assert.ok(win60.count > 0, "60s window still has samples");

  // Empty samples -> null.
  assert.strictEqual(PURE.computeStatsWindow([], now, 60), null);
  assert.strictEqual(PURE.computeStatsWindow(null, now, 60), null);
}

// statsForHopWindow matches statsForHop when passed FOCUS_S, and produces
// narrower counts under a smaller window (the seed-baseline combination
// path is parameterised end-to-end; each width uses its own snapshot
// baseline, adopted with the echoed focus_s).
{
  // Two per-width baselines over the same stream (as two snapshot fetches
  // with different Focus widths would produce); samples spanning
  // [now-200, now-5] so the 60s window excludes the older live samples.
  const s = state();
  PURE.adoptSnapshotBaseline(
    s, "2",
    { count: 50, avg_ms: 20, min_ms: 10, cur_ms: 20, loss_pct: 0 },
    now - 200, now - 370, now - 500, 600
  );
  // 10 samples inside the last ~25s; 10 samples between 100s and 370s ago.
  for (let i = 0; i < 10; i++) PURE.pushSample(s, 2, now - (5 + i * 2), 25);
  for (let i = 0; i < 10; i++) PURE.pushSample(s, 2, now - (100 + i * 30), 30);
  const def = PURE.statsForHop(s, 2, now); // uses FOCUS_S = 600
  const same = PURE.statsForHopWindow(s, 2, now, PURE.FOCUS_S);
  assert.strictEqual(def.source, same.source);
  assert.strictEqual(def.data.count, same.data.count);

  // 600s window: baseline (50) + the nineteen post-cutoff samples (ten
  // recent, nine of the older batch — the oldest sits exactly on the
  // cutoff and is excluded) -> combined.
  const w600 = PURE.statsForHopWindow(s, 2, now, 600);
  assert.strictEqual(w600.source, "combined");
  assert.strictEqual(w600.data.count, 69);
  assert.ok(w600.data.count > 50, "seed baseline still combined with live");

  // 60s window: the width-600 baseline is suspended (window mismatch), so
  // the count comes from the raw buffer alone — the ten recent samples.
  const w60 = PURE.statsForHopWindow(s, 2, now, 60);
  assert.strictEqual(w60.source, "live");
  assert.strictEqual(w60.data.count, 10);
  assert.ok(w60.data.count < w600.data.count, "narrower window -> smaller count");
}

// FOCUS_OPTIONS exposes the four dropdown choices for documentation/
// assertion; seconds and label are consistent.
{
  assert.strictEqual(PURE.FOCUS_OPTIONS.length, 4);
  const byValue = {};
  PURE.FOCUS_OPTIONS.forEach(function (o) { byValue[o.value] = o; });
  assert.strictEqual(byValue["auto"].seconds, PURE.FOCUS_S);
  assert.strictEqual(byValue["auto"].label, "Auto");
  assert.strictEqual(byValue["60"].seconds, 60);
  assert.strictEqual(byValue["60"].label, "1 minute");
  assert.strictEqual(byValue["300"].seconds, 300);
  assert.strictEqual(byValue["300"].label, "5 minutes");
  assert.strictEqual(byValue["600"].seconds, 600);
  assert.strictEqual(byValue["600"].label, "10 minutes");
}

// ----------------------------------------------------------- Slice 6c PURE
// (timeline fidelity: step-line path geometry + per-panel range state).

// RANGE_OPTIONS exposes the per-panel dropdown choices (Slice 6c replaced
// the shared segmented toolbar with a per-panel <select>; Slice D1 adds
// 1 hour / 3 hours; Slice D2 adds 12 hours / 24 hours served from the
// server rollup tier).
{
  assert.strictEqual(PURE.RANGE_OPTIONS.length, 7);
  const byValue = {};
  PURE.RANGE_OPTIONS.forEach(function (o) { byValue[o.value] = o; });
  assert.strictEqual(byValue["60"].seconds, 60);
  assert.strictEqual(byValue["60"].label, "1 minute");
  assert.strictEqual(byValue["300"].seconds, 300);
  assert.strictEqual(byValue["300"].label, "5 minutes");
  assert.strictEqual(byValue["600"].seconds, 600);
  assert.strictEqual(byValue["600"].label, "10 minutes");
  assert.strictEqual(byValue["3600"].seconds, 3600);
  assert.strictEqual(byValue["3600"].label, "1 hour");
  assert.strictEqual(byValue["10800"].seconds, 10800);
  assert.strictEqual(byValue["10800"].label, "3 hours");
  assert.strictEqual(byValue["43200"].seconds, 43200);
  assert.strictEqual(byValue["43200"].label, "12 hours");
  assert.strictEqual(byValue["86400"].seconds, 86400);
  assert.strictEqual(byValue["86400"].label, "24 hours");
  // DEFAULT_RANGE_S stays 600 (10 minutes) per spec.
  assert.strictEqual(PURE.DEFAULT_RANGE_S, 600);
  // RANGES_S still exports the seconds list (extended for 1h/3h/12h/24h).
  assert.deepStrictEqual(PURE.RANGES_S, [60, 300, 600, 3600, 10800, 43200, 86400]);
  // Slice D2 — MAX_RANGE_S is now 24h (largest offered range); the raw
  // buffer ceiling is RAW_RANGE_MAX_S (3h, unchanged from D1) — ranges
  // beyond it are served from the rollup tier.
  assert.strictEqual(PURE.MAX_RANGE_S, 86400);
  assert.strictEqual(PURE.RAW_RANGE_MAX_S, 10800);
  assert.strictEqual(PURE.RAW_CAP, 10000);
}

// rangeOptionFor: per-panel range state mapping. Returns the matching
// option or null. Used by rangeForPanel to validate persisted/selected
// values; node-asserted here as the state mapping contract. Slice D1
// extends the recognized values to include 1h and 3h.
{
  // Happy path: numeric and string inputs both resolve.
  assert.strictEqual(PURE.rangeOptionFor(60).seconds, 60);
  assert.strictEqual(PURE.rangeOptionFor(300).label, "5 minutes");
  assert.strictEqual(PURE.rangeOptionFor(600).value, "600");
  assert.strictEqual(PURE.rangeOptionFor(3600).label, "1 hour");
  assert.strictEqual(PURE.rangeOptionFor(10800).seconds, 10800);
  // Unknown / out-of-range values return null (caller falls back to
  // DEFAULT_RANGE_S).
  assert.strictEqual(PURE.rangeOptionFor(0), null);
  assert.strictEqual(PURE.rangeOptionFor(120), null);
  assert.strictEqual(PURE.rangeOptionFor(900), null);  // 15m still not offered
  assert.strictEqual(PURE.rangeOptionFor(21600), null); // 6h not offered (D2)
  assert.strictEqual(PURE.rangeOptionFor(null), null);
  assert.strictEqual(PURE.rangeOptionFor(undefined), null);
  assert.strictEqual(PURE.rangeOptionFor(NaN), null);
}

// stepPath: step-line RTT geometry. Each successful sample holds
// horizontally to the next sample's x, then jumps vertically at that x.
// Lost samples are skipped (the line bridges over them).
{
  const endS = 1000;
  const rangeS = 1000;
  const width = 1000;
  const axisMax = 200;
  const height = 100;
  // msToY inverts: y = height - (ms/axisMax)*height. So rtt=10 -> 95,
  // rtt=20 -> 90, rtt=30 -> 85.

  // --- Empty input -> no path.
  assert.deepStrictEqual(PURE.stepPath([], endS, rangeS, width, axisMax, height), []);
  assert.deepStrictEqual(PURE.stepPath(null, endS, rangeS, width, axisMax, height), []);

  // --- All-loss input -> no path (the renderer skips the RTT line and
  //     only draws red loss marks).
  const allLoss = [
    { t: 100, rtt_ms: null },
    { t: 110, rtt_ms: null },
    { t: 120, rtt_ms: NaN },
  ];
  assert.deepStrictEqual(PURE.stepPath(allLoss, endS, rangeS, width, axisMax, height), []);

  // --- Single successful sample -> just a moveTo (no horizontal/vertical
  //     pair). The renderer paints a small dot so it is visible.
  const single = [{ t: 100, rtt_ms: 10 }];
  const singlePath = PURE.stepPath(single, endS, rangeS, width, axisMax, height);
  assert.strictEqual(singlePath.length, 1);
  assert.strictEqual(singlePath[0].kind, "move");
  assert.strictEqual(singlePath[0].x, 100); // timeToX(100, 1000, 1000, 1000)
  assert.strictEqual(singlePath[0].y, 95);   // msToY(10, 200, 100)

  // --- Two successful samples -> move + one horizontal hold + one
  //     vertical jump. The horizontal y equals the first sample's y
  //     (constant-y hold); the vertical x equals the horizontal's x
  //     (jump lands at the new sample's x).
  const two = [
    { t: 100, rtt_ms: 10 },
    { t: 200, rtt_ms: 20 },
  ];
  const twoPath = PURE.stepPath(two, endS, rangeS, width, axisMax, height);
  assert.strictEqual(twoPath.length, 3);
  // move(first sample)
  assert.strictEqual(twoPath[0].kind, "move");
  assert.strictEqual(twoPath[0].x, 100);
  assert.strictEqual(twoPath[0].y, 95);
  // horizontal hold: x advances to the next sample's x, y stays.
  assert.strictEqual(twoPath[1].kind, "h");
  assert.strictEqual(twoPath[1].x, 200); // next sample's x
  assert.strictEqual(twoPath[1].y, 95);   // previous sample's y (constant)
  // vertical jump: x stays, y moves to the new sample's y.
  assert.strictEqual(twoPath[2].kind, "v");
  assert.strictEqual(twoPath[2].x, 200); // same x as the horizontal
  assert.strictEqual(twoPath[2].y, 90);   // new sample's y

  // --- Many samples + gaps: steps bridge over losses. Path geometry:
  //     every horizontal y equals the previous successful y; every
  //     vertical x equals the previous horizontal x.
  const withGaps = [
    { t: 100, rtt_ms: 10 },
    { t: 110, rtt_ms: 20 },
    { t: 120, rtt_ms: null }, // loss — skipped, the line bridges
    { t: 130, rtt_ms: null }, // loss — skipped
    { t: 140, rtt_ms: 30 },
    { t: 150, rtt_ms: null }, // trailing loss — does not add a segment
  ];
  const gapPath = PURE.stepPath(withGaps, endS, rangeS, width, axisMax, height);
  // 3 successful samples -> 1 move + 2 * (h + v) = 5 points.
  assert.strictEqual(gapPath.length, 5);
  // Invariants along the whole path.
  for (let i = 1; i < gapPath.length; i++) {
    if (gapPath[i].kind === "h") {
      // Horizontal hold: y must equal the previous point's y.
      assert.strictEqual(gapPath[i].y, gapPath[i - 1].y,
        "horizontal hold keeps previous y at index " + i);
    }
    if (gapPath[i].kind === "v") {
      // Vertical jump: x must equal the previous point's x.
      assert.strictEqual(gapPath[i].x, gapPath[i - 1].x,
        "vertical jump lands at previous x at index " + i);
    }
  }
  // The bridging across the two losses is the key fidelity assertion:
  // the horizontal between samples 110 and 140 spans their x distance
  // at the value of sample 110 (y=90), not broken by the losses.
  // samples: 100->x=100,y=95 ; 110->x=110,y=90 ; 140->x=140,y=85.
  // path: [move(100,95), h(110,95), v(110,90), h(140,90), v(140,85)]
  assert.strictEqual(gapPath[0].x, 100);
  assert.strictEqual(gapPath[3].x, 140); // horizontal hold lands at sample 140's x
  assert.strictEqual(gapPath[3].y, 90);   // y still at sample 110's value
  assert.strictEqual(gapPath[4].y, 85);   // vertical jump to sample 140's value

  // --- A loss at the head is skipped: the path's first point is the
  //     first SUCCESSFUL sample, not the first sample.
  const leadingLoss = [
    { t: 100, rtt_ms: null },
    { t: 110, rtt_ms: 15 },
    { t: 120, rtt_ms: 25 },
  ];
  const leadPath = PURE.stepPath(leadingLoss, endS, rangeS, width, axisMax, height);
  assert.strictEqual(leadPath[0].kind, "move");
  assert.strictEqual(leadPath[0].x, 110); // first successful sample's x

  // Window-edge samples are retained, while malformed/non-finite values are
  // ignored and successful samples are ordered defensively by timestamp.
  const defensive = PURE.stepPath([
    { t: 1000, rtt_ms: 30 }, // exact right edge
    { t: 0, rtt_ms: 10 },    // exact left edge, supplied out of order
    { t: 500, rtt_ms: NaN },
    { t: 700, rtt_ms: Infinity },
    { t: "bad", rtt_ms: 20 },
  ], endS, rangeS, width, axisMax, height);
  assert.strictEqual(defensive.length, 3, "two edge samples produce a step path");
  assert.strictEqual(defensive[0].x, 0, "exact window start is visible");
  assert.strictEqual(defensive[1].x, 1000, "hold reaches exact window end");
  assert.strictEqual(defensive[2].kind, "v", "out-of-order input is normalized");
}

// ----------------------------------------------------------- Slice 7 PURE
// (multi-trace tabs: label normalisation + next-active selection).

// tabLabel normalises a free-text target into a short, stable tab label.
// Scheme, www., port, path, query, fragment are stripped; empty/whitespace
// falls back to "New trace" (the idle-fresh-tab label).
{
  // Scheme stripping.
  assert.strictEqual(PURE.tabLabel("https://example.com"), "example.com");
  assert.strictEqual(PURE.tabLabel("http://example.com"), "example.com");
  assert.strictEqual(PURE.tabLabel("example.com"), "example.com");
  // Scheme + path + query + fragment.
  assert.strictEqual(
    PURE.tabLabel("https://www.example.com:443/path?q=1#frag"),
    "example.com"
  );
  // Leading "www." stripped.
  assert.strictEqual(PURE.tabLabel("www.example.com"), "example.com");
  // IPv4 literal kept whole; port stripped.
  assert.strictEqual(PURE.tabLabel("192.168.1.10:80"), "192.168.1.10");
  assert.strictEqual(PURE.tabLabel("127.0.0.1"), "127.0.0.1");
  // IPv6 literal kept whole (bracketed form canonicalised to bare address).
  assert.strictEqual(PURE.tabLabel("[2001:db8::1]:443"), "2001:db8::1");
  assert.strictEqual(PURE.tabLabel("2001:db8::1"), "2001:db8::1");
  // Empty / whitespace / null / undefined → "New trace".
  assert.strictEqual(PURE.tabLabel(""), "New trace");
  assert.strictEqual(PURE.tabLabel("   "), "New trace");
  assert.strictEqual(PURE.tabLabel(null), "New trace");
  assert.strictEqual(PURE.tabLabel(undefined), "New trace");
  // Original input is not mutated.
  const orig = "https://www.example.com/x";
  PURE.tabLabel(orig);
  assert.strictEqual(orig, "https://www.example.com/x", "input not mutated");
}

// nextActiveAfterClose: pick the tab to activate when ``closedId`` is removed.
// Mirrors familiar browser behaviour: prefer the right neighbour of the
// closed tab, then the left neighbour, then null when the list empties.
{
  // Closing a middle tab → activate the right neighbour.
  assert.strictEqual(
    PURE.nextActiveAfterClose(["a", "b", "c"], "b", "b"),
    "c",
    "close middle (active) → right neighbour"
  );
  // Closing the last tab → activate the left neighbour.
  assert.strictEqual(
    PURE.nextActiveAfterClose(["a", "b", "c"], "c", "c"),
    "b",
    "close last (active) → left neighbour"
  );
  // Closing the only tab → null (caller spawns a fresh idle tab).
  assert.strictEqual(
    PURE.nextActiveAfterClose(["only"], "only", "only"),
    null,
    "close only → null"
  );
  // Closing the first of several → activate the new first (right neighbour).
  assert.strictEqual(
    PURE.nextActiveAfterClose(["a", "b", "c"], "a", "a"),
    "b",
    "close first (active) → right neighbour"
  );
  // Closing a non-active tab → the active tab stays active (id unchanged).
  assert.strictEqual(
    PURE.nextActiveAfterClose(["a", "b", "c"], "a", "b"),
    "a",
    "close non-active → active unchanged"
  );
  // Closing a non-active tab to the LEFT of the active tab → active unchanged.
  assert.strictEqual(
    PURE.nextActiveAfterClose(["a", "b", "c"], "c", "a"),
    "c",
    "close non-active to the left → active unchanged"
  );
  // Empty list → null.
  assert.strictEqual(PURE.nextActiveAfterClose([], null, "x"), null);
  // closedId not in list → activeId unchanged (defensive).
  assert.strictEqual(
    PURE.nextActiveAfterClose(["a", "b"], "a", "zzz"),
    "a",
    "unknown closedId → active unchanged"
  );
  // Numeric ids coerce to strings consistently.
  assert.strictEqual(
    PURE.nextActiveAfterClose([1, 2, 3], 2, 2),
    "3",
    "numeric ids coerce to strings"
  );
}

// ---------------------------------------------- Pause-freeze (Slice 7+):
// Per-tab effective-time logic. While a tab is paused, the data window
// (timeline x-axis, rolling-stats cutoff, Focus caption bounds,
// drag-focus mapping) must freeze on a fixed timestamp instead of
// advancing with wall-clock time. effectiveNowS is the routing point;
// pauseFreezeFromSamples picks the freeze surrogate when restoring an
// already-paused session (the wall-clock pause instant is unknown).

// effectiveNowS: returns wallS while running (freezeS null/undefined/non-finite).
{
  assert.strictEqual(PURE.effectiveNowS(1000, null), 1000, "null freeze → wall");
  assert.strictEqual(PURE.effectiveNowS(1000, undefined), 1000, "undefined freeze → wall");
  assert.strictEqual(PURE.effectiveNowS(1000, NaN), 1000, "NaN freeze → wall");
  assert.strictEqual(PURE.effectiveNowS(1000, Infinity), 1000, "infinite freeze → wall");
  // Returns the frozen value while paused.
  assert.strictEqual(PURE.effectiveNowS(2000, 1500), 1500, "freeze overrides wall");
  // Wall advancing does not move the effective time while frozen.
  assert.strictEqual(PURE.effectiveNowS(9999, 1500), 1500, "wall drift ignored while frozen");
  // On resume the caller passes null and wall time resumes.
  assert.strictEqual(PURE.effectiveNowS(2100, null), 2100, "resume → wall");
  // WallS is returned as-is even when non-finite (caller's responsibility;
  // we only short-circuit on the freeze argument).
  assert.strictEqual(PURE.effectiveNowS(NaN, null), NaN, "non-finite wall passes through");
}

// Pause set then clear round-trip mirrors the Pause button flow.
{
  var wallPausedAt = 5000;
  // After the pause confirmation effectiveNowS reports the freeze...
  assert.strictEqual(PURE.effectiveNowS(6000, wallPausedAt), 5000);
  // ...and after resume (freeze cleared → null) it tracks wall again.
  assert.strictEqual(PURE.effectiveNowS(6000, null), 6000);
  // The forward jump on resume is intentional (the gap is real dead time):
  // wallS > frozen value is fine.
  assert.ok(PURE.effectiveNowS(6000, null) > PURE.effectiveNowS(5500, wallPausedAt));
}

// pauseFreezeFromSamples: latest sample t across hops (dictionary shape —
// the form state.samples takes at runtime).
{
  var samplesByHop = {
    "0": [{ t: 100, rtt_ms: 5 }, { t: 101, rtt_ms: 6 }],
    "1": [{ t: 99, rtt_ms: 7 }, { t: 105, rtt_ms: 8 }],
    "2": [{ t: 102, rtt_ms: null }], // loss sample still carries a timestamp
  };
  assert.strictEqual(PURE.pauseFreezeFromSamples(samplesByHop), 105, "max t across hops");
}

// Empty / no-sample inputs return null so the caller can fall back.
{
  assert.strictEqual(PURE.pauseFreezeFromSamples({}), null, "empty dict → null");
  assert.strictEqual(PURE.pauseFreezeFromSamples({ "0": [], "1": [] }), null, "empty arrays → null");
  assert.strictEqual(PURE.pauseFreezeFromSamples(null), null, "null → null");
  assert.strictEqual(PURE.pauseFreezeFromSamples(undefined), null, "undefined → null");
  assert.strictEqual(PURE.pauseFreezeFromSamples({ "0": [{ rtt_ms: 5 }] }), null, "missing t → null");
}

// Non-finite t values are skipped; a finite t later in the same array
// still wins.
{
  assert.strictEqual(
    PURE.pauseFreezeFromSamples({ "0": [{ t: NaN }, { t: Infinity }, { t: 200 }, { t: null }] }),
    200,
    "skip non-finite t"
  );
}

// Flat sample array shape (e.g. a snapshot's snap.samples).
{
  assert.strictEqual(
    PURE.pauseFreezeFromSamples([{ t: 10 }, { t: 30 }, { t: 20 }]),
    30,
    "flat array → max t"
  );
  assert.strictEqual(PURE.pauseFreezeFromSamples([]), null, "flat empty → null");
}

// Array-of-arrays shape (a caller may merge multiple sources).
{
  assert.strictEqual(
    PURE.pauseFreezeFromSamples([[{ t: 10 }, { t: 5 }], [{ t: 40 }], []]),
    40,
    "array of arrays → max t"
  );
}

// Best-effort multi-source fallback used on restore: state.samples
// dictionary first, then snap.samples flat array. Mirrors the
// applySnapshot restore path: take the latest from the dict; if null,
// try the snapshot tail; if still null, the caller falls back to wall
// time.
{
  var emptyDict = {};
  var snapTail = [{ t: 700 }, { t: 750 }];
  var restoredDict = { "1": [{ t: 800 }, { t: 760 }] };

  // Empty dict → snapshot tail wins.
  var a = PURE.pauseFreezeFromSamples(emptyDict);
  if (a === null) a = PURE.pauseFreezeFromSamples(snapTail);
  assert.strictEqual(a, 750, "empty dict → snap tail");

  // Populated dict → dict wins (fresher than the snapshot tail).
  var b = PURE.pauseFreezeFromSamples(restoredDict);
  if (b === null) b = PURE.pauseFreezeFromSamples(snapTail);
  assert.strictEqual(b, 800, "dict fresher than snap tail");

  // Both empty → null (caller falls back to restore wall time).
  var c = PURE.pauseFreezeFromSamples(emptyDict);
  if (c === null) c = PURE.pauseFreezeFromSamples([]);
  assert.strictEqual(c, null, "both empty → null");
}

// Input is not mutated.
{
  var input = { "0": [{ t: 100, rtt_ms: 5 }] };
  PURE.pauseFreezeFromSamples(input);
  assert.deepStrictEqual(input, { "0": [{ t: 100, rtt_ms: 5 }] }, "input not mutated");
}

// ------------------------------------ Generation-based snapshot state gate
//
// Replaces the value-based ``snapshotStateConflicts`` gate (which locked
// the UI when another window paused/resumed the shared session, because it
// discarded any snapshot whose ``state`` disagreed with local
// ``runningKnown``). The new policy is causal: while a control transition
// is pending the snapshot DEFERS the state flip to the POST response; with
// no transition pending the snapshot is AUTHORITATIVE and adopts the
// server state either way (the multi-window convergence path). The
// companion generation guard lives in ``fetchSnapshot``: a confirmed
// transition bumps ``snapshotRequestSeq`` so snapshots issued before
// confirmation are rejected before reaching this policy.

// snapshotStatePolicy: defer while a control transition is pending.
{
  assert.strictEqual(PURE.snapshotStatePolicy(null), "adopt",
    "no pending transition → authoritative adopt");
  assert.strictEqual(PURE.snapshotStatePolicy(undefined), "adopt",
    "undefined controlAction → adopt");
  assert.strictEqual(PURE.snapshotStatePolicy("pause"), "defer",
    "pending pause → defer state flip");
  assert.strictEqual(PURE.snapshotStatePolicy("resume"), "defer",
    "pending resume → defer state flip");
}

// The authoritative-adopt path must accept BOTH flips. This is the
// regression contract for scenario (a): window B's snapshot says "paused"
// while B's runningKnown is still true — the old gate discarded it
// forever; the new policy adopts it so B converges.
{
  // pause→running flip would be adopted (no pending transition).
  assert.strictEqual(PURE.snapshotStatePolicy(null), "adopt");
  // running→pause flip would be adopted (no pending transition).
  assert.strictEqual(PURE.snapshotStatePolicy(null), "adopt");
}

// -------------------------------------- computePauseFreeze: paused_at + bound
//
// Preference order: server ``paused_at`` (authoritative, unbounded) →
// sample-based fallback (bounded to <= wallS, for legacy sessions) → wallS.

// Server paused_at is preferred and used UNBOUNDED (it is the actual pause
// epoch; capping it would misplace the freeze under server/client skew).
{
  var wallS = 5000;
  var samplesByHop = { "0": [{ t: 4900 }] };
  // paused_at behind wall → used as-is (a legitimately earlier pause).
  assert.strictEqual(
    PURE.computePauseFreeze(4000, samplesByHop, [{ t: 4900 }], wallS),
    4000,
    "server paused_at used as-is (earlier than wall)"
  );
  // paused_at AHEAD of wall (clock skew) → still authoritative, unbounded.
  assert.strictEqual(
    PURE.computePauseFreeze(6000, samplesByHop, [{ t: 4900 }], wallS),
    6000,
    "server paused_at authoritative even ahead of client wall"
  );
}

// Legacy fallback (no paused_at): sample-based, BOUNDED to <= wallS so a
// future / clock-skewed sample timestamp cannot anchor the freeze ahead.
{
  var wallS = 5000;
  // Sample dict behind wall → used (normal restore case).
  assert.strictEqual(
    PURE.computePauseFreeze(null, { "0": [{ t: 4900 }, { t: 4950 }] }, null, wallS),
    4950,
    "dict sample fallback behind wall"
  );
  // Empty dict → fall through to snapshot tail.
  assert.strictEqual(
    PURE.computePauseFreeze(null, {}, [{ t: 4800 }, { t: 4850 }], wallS),
    4850,
    "snapshot tail fallback"
  );
  // Sample AHEAD of wall (bogus / skew) → capped at wall.
  assert.strictEqual(
    PURE.computePauseFreeze(null, { "0": [{ t: 9999 }] }, null, wallS),
    5000,
    "future sample bounded to wallS"
  );
  // Snapshot-tail sample ahead of wall → also capped.
  assert.strictEqual(
    PURE.computePauseFreeze(null, {}, [{ t: 9999 }], wallS),
    5000,
    "future snapshot tail bounded to wallS"
  );
}

// No paused_at and no samples anywhere → wallS (receipt time) last resort.
{
  assert.strictEqual(
    PURE.computePauseFreeze(null, {}, null, 5000),
    5000,
    "no data → wallS"
  );
  assert.strictEqual(
    PURE.computePauseFreeze(undefined, { "0": [] }, [], 7000),
    7000,
    "empty arrays → wallS"
  );
}

// Non-finite paused_at is ignored (defensive — falls to the sample path).
{
  assert.strictEqual(
    PURE.computePauseFreeze(NaN, { "0": [{ t: 300 }] }, null, 4000),
    300,
    "NaN paused_at ignored → sample fallback"
  );
  assert.strictEqual(
    PURE.computePauseFreeze(Infinity, { "0": [{ t: 9000 }] }, null, 4000),
    4000,
    "Infinity paused_at ignored → bounded sample (9000 capped to 4000)"
  );
}

// -------------------------------------- deriveStatus: toolbar status arbitration
//
// Root-cause refactor of the status-line defect cluster. Every async
// source used to write the toolbar status imperatively via localSetStatus,
// so each pair of writers was a race; four review rounds each found one
// more symmetric clobber. deriveStatus replaces that writer web with a
// single precedence table over FACTS. Former writers now update facts and
// call renderStatus; precedence decides what shows.
//
// These assertions pin every BRANCH and every PRECEDENCE relationship so a
// future writer cannot silently re-introduce a clobber.

// Each precedence row in isolation (the nine branches).
{
  assert.deepStrictEqual(
    PURE.deriveStatus({ resetting: true }),
    { text: "Resetting…", kind: "pending" },
    "row 1: resetting"
  );
  assert.deepStrictEqual(
    PURE.deriveStatus({ controlAction: "pause", hasSession: true, runningKnown: true }),
    { text: "Pausing…", kind: "pending" },
    "row 2: controlAction=pause"
  );
  assert.deepStrictEqual(
    PURE.deriveStatus({ controlAction: "resume", hasSession: true, runningKnown: false }),
    { text: "Resuming…", kind: "pending" },
    "row 3: controlAction=resume"
  );
  assert.deepStrictEqual(
    PURE.deriveStatus({ startInFlight: true }),
    { text: "Starting…", kind: "pending" },
    "row 4: startInFlight"
  );
  assert.deepStrictEqual(
    PURE.deriveStatus({ lastError: "Pause failed: net", hasSession: true, runningKnown: false }),
    { text: "Pause failed: net", kind: "error" },
    "row 5: lastError"
  );
  assert.deepStrictEqual(
    PURE.deriveStatus({ reconnecting: true, hasSession: true, runningKnown: true }),
    { text: "Reconnecting…", kind: "error" },
    "row 6: reconnecting"
  );
  assert.deepStrictEqual(
    PURE.deriveStatus({ hasSession: true, runningKnown: true }),
    { text: "Running", kind: "running" },
    "row 7: running steady"
  );
  assert.deepStrictEqual(
    PURE.deriveStatus({ hasSession: true, runningKnown: false }),
    { text: "Paused", kind: "pending" },
    "row 8: paused steady"
  );
  assert.deepStrictEqual(
    PURE.deriveStatus({}),
    { text: "Idle", kind: null },
    "row 9: idle (empty view)"
  );
  assert.deepStrictEqual(
    PURE.deriveStatus(),
    { text: "Idle", kind: null },
    "row 9: idle (undefined view)"
  );
}

// Precedence: row 1 (resetting) outranks EVERYTHING else. A reset in
// flight must show "Resetting…" regardless of pending controls, errors,
// reconnect state, or the steady state.
{
  var allOn = {
    resetting: true,
    controlAction: "pause",
    startInFlight: true,
    lastError: "boom",
    reconnecting: true,
    hasSession: true,
    runningKnown: true,
  };
  assert.strictEqual(PURE.deriveStatus(allOn).text, "Resetting…",
    "resetting wins over all");
  // Drop resetting alone -> next winner (controlAction) surfaces.
  var noReset = Object.assign({}, allOn, { resetting: false });
  assert.strictEqual(PURE.deriveStatus(noReset).text, "Pausing…",
    "without resetting, controlAction surfaces");
}

// Precedence: controlAction (rows 2–3) outranks startInFlight, lastError,
// reconnecting, and the steady state — the round-3/round-4 defect was an
// SSE event clobbering a pending control; this pins that a pending
// control can never be masked by a transport event or an error.
{
  var pausedCtrl = {
    controlAction: "resume",
    startInFlight: true,
    lastError: "prior fail",
    reconnecting: true,
    hasSession: true,
    runningKnown: false,
  };
  assert.strictEqual(PURE.deriveStatus(pausedCtrl).text, "Resuming…",
    "pending resume wins over error + reconnecting");
  // A pending control also outranks a set lastError (reconciliation pin
  // window): the user just acted, so their intent wins.
  var ctrlWithError = Object.assign({}, pausedCtrl, { startInFlight: false });
  assert.strictEqual(PURE.deriveStatus(ctrlWithError).text, "Resuming…",
    "pending control wins over lastError");
}

// Precedence: startInFlight (row 4) outranks lastError and reconnecting.
// A retry after a failed start must show "Starting…", not the stale error.
{
  assert.strictEqual(
    PURE.deriveStatus({ startInFlight: true, lastError: "Start failed: 409" }).text,
    "Starting…",
    "startInFlight outranks lastError"
  );
  assert.strictEqual(
    PURE.deriveStatus({ startInFlight: true, reconnecting: true }).text,
    "Starting…",
    "startInFlight outranks reconnecting"
  );
}

// Precedence: lastError (row 5) outranks reconnecting and the steady
// state. This is the core of the fix: an unreconciled control/start/reset
// failure cannot be masked by an SSE reconnect (round-3/4) nor absorbed
// into the steady Running/Paused label.
{
  assert.strictEqual(
    PURE.deriveStatus({ lastError: "Pause failed", reconnecting: true, hasSession: true, runningKnown: false }).text,
    "Pause failed",
    "lastError outranks reconnecting"
  );
  assert.strictEqual(
    PURE.deriveStatus({ lastError: "Pause failed", hasSession: true, runningKnown: true }).text,
    "Pause failed",
    "lastError outranks steady Running"
  );
  assert.strictEqual(
    PURE.deriveStatus({ lastError: "Pause failed", hasSession: true, runningKnown: false }).text,
    "Pause failed",
    "lastError outranks steady Paused"
  );
}

// Precedence: reconnecting (row 6) outranks the steady state so a dropped
// stream is reported instead of a frozen "Running/Paused".
{
  assert.strictEqual(
    PURE.deriveStatus({ reconnecting: true, hasSession: true, runningKnown: true }).text,
    "Reconnecting…",
    "reconnecting outranks steady Running"
  );
  assert.strictEqual(
    PURE.deriveStatus({ reconnecting: true, hasSession: true, runningKnown: false }).text,
    "Reconnecting…",
    "reconnecting outranks steady Paused"
  );
}

// hasSession gates the steady-state rows: without a session, neither
// Running nor Paused is shown — falls through to Idle (or lastError /
// reconnecting if set, but those imply a session in practice).
{
  assert.strictEqual(
    PURE.deriveStatus({ hasSession: false, runningKnown: true }).text,
    "Idle",
    "no session + runningKnown -> Idle (runningKnown is meaningless idle)"
  );
  assert.strictEqual(
    PURE.deriveStatus({ hasSession: false, runningKnown: false }).text,
    "Idle",
    "no session -> Idle"
  );
}

// Falsy-coercion: a transient flag set then cleared restores the lower
// row. Mirrors the renderStatus round-trip each writer performs.
{
  // Control fails -> lastError set -> user pauses again -> lastError
  // cleared, controlAction set -> "Pausing…".
  var v = { hasSession: true, runningKnown: false };
  v.lastError = "Resume failed";
  assert.strictEqual(PURE.deriveStatus(v).text, "Resume failed", "error shown");
  v.lastError = null;
  v.controlAction = "pause";
  assert.strictEqual(PURE.deriveStatus(v).text, "Pausing…", "new control clears error");
  v.controlAction = null;
  assert.strictEqual(PURE.deriveStatus(v).text, "Paused", "confirmed -> steady Paused");
}

// The reconciliation pin is NOT a deriveStatus input — it is a clearing
// POLICY owned by applySnapshot. deriveStatus only sees lastError, so the
// pin cannot leak into precedence. Assert the view shape deriveStatus
// consumes does not include it (defensive contract: adding it later would
// be a sign the policy leaked back into the table).
{
  var view = {
    resetting: false,
    startInFlight: false,
    controlAction: null,
    lastError: "Pause failed",
    reconnecting: false,
    hasSession: true,
    runningKnown: false,
  };
  assert.strictEqual(PURE.deriveStatus(view).text, "Pause failed",
    "pinned error shows via lastError alone");
  // The pin is intentionally absent from the consumed keys.
  assert.ok(!("errorPinnedForReconcile" in view),
    "pin is not a deriveStatus input");
}

// ---------------------------------------------- Sample-log export (Slice A):
// exportEnabled drives the toolbar Export button: available only when the
// active tab has a session AND the server is known to hold at least one
// sample for it (live SSE buffer OR snapshot baseline with count > 0).

// No session -> disabled.
assert.strictEqual(PURE.exportEnabled({}), false, "no session -> disabled");
assert.strictEqual(
  PURE.exportEnabled({ sessionId: null, samples: { "0": [{ t: 1 }] } }),
  false,
  "null sessionId -> disabled"
);
assert.strictEqual(
  PURE.exportEnabled({ sessionId: "abc", samples: {}, seedStats: {} }),
  false,
  "session but no samples -> disabled"
);

// Live SSE sample present -> enabled (any hop key with a non-empty array).
assert.strictEqual(
  PURE.exportEnabled({ sessionId: "abc", samples: { "0": [{ t: 1, rtt_ms: 5 }] } }),
  true,
  "live hop-0 sample -> enabled"
);
assert.strictEqual(
  PURE.exportEnabled({ sessionId: "abc", samples: { "2": [{ t: 1, rtt_ms: null }] } }),
  true,
  "a loss sample still counts as a known sample -> enabled"
);

// Snapshot baseline with count > 0 -> enabled (the restore / pre-round path
// where the SSE buffer has not filled yet but the snapshot proves samples).
assert.strictEqual(
  PURE.exportEnabled({
    sessionId: "abc",
    samples: {},
    seedStats: { "1": { count: 4, avg_ms: 10, loss_pct: 0 } },
  }),
  true,
  "seed baseline count>0 -> enabled"
);
assert.strictEqual(
  PURE.exportEnabled({
    sessionId: "abc",
    samples: {},
    seedStats: { "0": { count: 0 } },
  }),
  false,
  "seed baseline count==0 -> disabled"
);

// Either channel suffices; both empty -> disabled.
assert.strictEqual(
  PURE.exportEnabled({
    sessionId: "abc",
    samples: { "0": [], "1": [] },
    seedStats: { "0": { count: 0 } },
  }),
  false,
  "all channels empty -> disabled"
);

// Defensive: missing/garbage samples and seedStats shapes don't throw.
assert.strictEqual(PURE.exportEnabled({ sessionId: "abc" }), false);
assert.strictEqual(
  PURE.exportEnabled({ sessionId: "abc", samples: null, seedStats: null }),
  false
);
assert.strictEqual(PURE.exportEnabled(null), false);
assert.strictEqual(PURE.exportEnabled(undefined), false);

// -------------------------------- Slice B PURE: persistent background tabs.
// tabDisplayLabel prefers the user-set name and falls back to the
// target-derived label. Pure so the tab-bar render and node assertions
// share one definition (mirrors the tabLabel contract above).

// Name set -> name wins (verbatim, after trim).
assert.strictEqual(
  PURE.tabDisplayLabel("my trace", "example.com"),
  "my trace",
  "name takes precedence"
);
assert.strictEqual(
  PURE.tabDisplayLabel("  padded  ", "example.com"),
  "padded",
  "name is trimmed"
);
// Empty / whitespace / null / undefined name -> fall back to target label.
assert.strictEqual(
  PURE.tabDisplayLabel("", "example.com"),
  "example.com",
  "empty name -> target"
);
assert.strictEqual(
  PURE.tabDisplayLabel("   ", "https://www.example.com/x"),
  "example.com",
  "whitespace name -> target label (with target normalisation)"
);
assert.strictEqual(
  PURE.tabDisplayLabel(null, "192.168.1.10"),
  "192.168.1.10",
  "null name -> target label"
);
assert.strictEqual(
  PURE.tabDisplayLabel(undefined, "[2001:db8::1]:443"),
  "2001:db8::1",
  "undefined name -> target label (IPv6 normalisation)"
);
// Both empty -> "New trace" (the idle-tab label from tabLabel).
assert.strictEqual(PURE.tabDisplayLabel("", ""), "New trace");
assert.strictEqual(PURE.tabDisplayLabel(null, null), "New trace");
// Non-string name coerces to empty -> target label.
assert.strictEqual(
  PURE.tabDisplayLabel(123, "example.com"),
  "example.com",
  "non-string name coerces to empty"
);

// tabMarker returns the persistent glyph or "" for non-persistent.
assert.strictEqual(PURE.tabMarker(true), "⇄", "persistent -> ⇄ marker");
assert.strictEqual(PURE.tabMarker(false), "", "non-persistent -> empty");
assert.strictEqual(PURE.tabMarker(null), "", "null -> empty");
assert.strictEqual(PURE.tabMarker(undefined), "", "undefined -> empty");
assert.strictEqual(PURE.tabMarker(0), "", "falsy -> empty");
assert.strictEqual(PURE.tabMarker(1), "⇄", "truthy -> marker");
// Single source of truth: a future restyle changes only this constant.
assert.strictEqual(typeof PURE.tabMarker(true), "string");

// sanitizeName mirrors the server-side rule so the UI can validate before
// the POST and the optimistic label matches the server-round-tripped one.
// NAME_MAX caps the length after normalization.
assert.strictEqual(PURE.NAME_MAX, 60, "NAME_MAX is 60");

// null / non-string -> "".
assert.strictEqual(PURE.sanitizeName(null), "", "null -> empty");
assert.strictEqual(PURE.sanitizeName(undefined), "", "undefined -> empty");
assert.strictEqual(PURE.sanitizeName(42), "", "number -> empty");
assert.strictEqual(PURE.sanitizeName({}), "", "object -> empty");

// Plain strings survive (after trim).
assert.strictEqual(PURE.sanitizeName("plain"), "plain");
assert.strictEqual(PURE.sanitizeName("  padded  "), "padded");

// C0 controls / DEL / C1 controls become spaces, then whitespace collapses.
assert.strictEqual(PURE.sanitizeName("a\tb"), "a b", "tab -> space");
assert.strictEqual(PURE.sanitizeName("a\x07b"), "a b", "BEL -> space");
assert.strictEqual(PURE.sanitizeName("a\x1bb"), "a b", "ESC -> space");
assert.strictEqual(PURE.sanitizeName("a\x7fb"), "a b", "DEL -> space");
assert.strictEqual(PURE.sanitizeName("a\x9fb"), "a b", "C1 -> space");
assert.strictEqual(
  PURE.sanitizeName("line1\nline2"),
  "line1 line2",
  "newline -> space"
);
assert.strictEqual(
  PURE.sanitizeName("multiple   spaces"),
  "multiple spaces",
  "whitespace runs collapse"
);

// Hard truncation to NAME_MAX AFTER normalization.
assert.strictEqual(
  PURE.sanitizeName("a" + "b".repeat(200)).length,
  PURE.NAME_MAX,
  "long name truncated to NAME_MAX"
);
// Control chars become spaces before truncation, so length counts them.
assert.strictEqual(
  PURE.sanitizeName("a\t" + "b".repeat(PURE.NAME_MAX + 10)).length,
  PURE.NAME_MAX,
  "truncation applies after control->space"
);

// Non-control Unicode (letters, emoji, supplementary plane) is preserved.
assert.strictEqual(PURE.sanitizeName("emoji ✅ ok"), "emoji ✅ ok");
assert.strictEqual(PURE.sanitizeName("\uD83D\uDE00lead"), "\uD83D\uDE00lead");

// Unicode FORMAT / bidi / BOM / ZWSP characters (category Cf) are stripped.
// The prior hand-maintained range list silently missed these; the new
// Unicode-property-escape regex (\p{Cc}\p{Cf}\p{Cs}\p{Co}) covers them.
assert.strictEqual(
  PURE.sanitizeName("a\u202Eb"), "a b",
  "U+202E RIGHT-TO-LEFT OVERRIDE (Cf) -> space"
);
assert.strictEqual(
  PURE.sanitizeName("a\u200Bb"), "a b",
  "U+200B ZERO WIDTH SPACE (Cf) -> space"
);
assert.strictEqual(
  PURE.sanitizeName("a\uFEFFb"), "a b",
  "U+FEFF BOM / ZERO WIDTH NO-BREAK SPACE (Cf) -> space"
);
assert.strictEqual(
  PURE.sanitizeName("a\u2066b\u2069c"), "a b c",
  "U+2066/U+2069 bidi isolates (Cf) -> spaces"
);
assert.strictEqual(
  PURE.sanitizeName("a\u200Eb"), "a b",
  "U+200E LEFT-TO-RIGHT MARK (Cf) -> space"
);
assert.strictEqual(
  PURE.sanitizeName("a\u200Fb"), "a b",
  "U+200F RIGHT-TO-LEFT MARK (Cf) -> space"
);
assert.strictEqual(
  PURE.sanitizeName("a\u00ADb"), "a b",
  "U+00AD SOFT HYPHEN (Cf) -> space"
);
assert.strictEqual(
  PURE.sanitizeName("a\u202Ab"), "a b",
  "U+202A LEFT-TO-RIGHT EMBEDDING (Cf) -> space"
);
// Private-use characters (category Co) are stripped.
assert.strictEqual(
  PURE.sanitizeName("a\uE000b"), "a b",
  "U+E000 BMP private-use (Co) -> space"
);
assert.strictEqual(
  PURE.sanitizeName("a\uF8FFb"), "a b",
  "U+F8FF BMP private-use end (Co) -> space"
);
// Bidi override at start of name does not survive (spoofing defense).
assert.strictEqual(
  PURE.sanitizeName("\u202Eevil"),
  "evil",
  "leading bidi override stripped, not just replaced"
);
// Multiple Cf chars collapse to a single space.
assert.strictEqual(
  PURE.sanitizeName("a\u200B\u200B\u202Eb"),
  "a b",
  "multiple Cf chars collapse to single space"
);

// Input is not mutated.
const nameInput = "a\tb";
PURE.sanitizeName(nameInput);
assert.strictEqual(nameInput, "a\tb", "sanitizeName does not mutate input");

// ============================================================ Slice D1 PURE
// (timeline history merge + multi-hour range formatting).

// mergeSamples: sorted merge with exact-timestamp dedup (incoming wins on
// conflict), idempotent, bounded by cap.
{
  // --- Empty inputs.
  assert.deepStrictEqual(PURE.mergeSamples([], [], null), []);
  assert.deepStrictEqual(PURE.mergeSamples(null, undefined, null), []);

  // --- Simple union, ascending by t.
  const a = [{ t: 100, rtt_ms: 10 }, { t: 300, rtt_ms: 30 }];
  const b = [{ t: 200, rtt_ms: 20 }];
  const merged = PURE.mergeSamples(a, b, null);
  assert.deepStrictEqual(
    merged.map(function (s) { return s.t; }),
    [100, 200, 300],
    "union is ascending by t"
  );

  // --- Exact-t dedup: incoming wins on conflict.
  const existing = [{ t: 100, rtt_ms: 10 }, { t: 200, rtt_ms: 20 }];
  const incoming = [{ t: 100, rtt_ms: 99 }, { t: 300, rtt_ms: 30 }];
  const dup = PURE.mergeSamples(existing, incoming, null);
  assert.strictEqual(dup.length, 3, "exact-t duplicates collapse to one");
  const at100 = dup.find(function (s) { return s.t === 100; });
  assert.strictEqual(at100.rtt_ms, 99, "incoming wins on exact-t conflict");

  // --- Idempotency: merging the result with incoming again yields the
  //     same array (no new duplicates, no reordering).
  const again = PURE.mergeSamples(dup, incoming, null);
  assert.deepStrictEqual(
    again.map(function (s) { return [s.t, s.rtt_ms]; }),
    dup.map(function (s) { return [s.t, s.rtt_ms]; }),
    "merge is idempotent over repeated incoming"
  );

  // --- Cap: when set, the oldest entries are dropped so the most recent
  //     ``cap`` survive.
  const big = [];
  for (let i = 0; i < 10; i++) big.push({ t: 100 + i, rtt_ms: i });
  const capped = PURE.mergeSamples(big, [], 5);
  assert.strictEqual(capped.length, 5, "cap drops oldest");
  assert.strictEqual(capped[0].t, 105, "most-recent cap entries survive");
  assert.strictEqual(capped[4].t, 109, "last entry is the latest t");

  // --- Loss samples (rtt_ms null) are preserved, not dropped.
  const withLoss = PURE.mergeSamples(
    [{ t: 100, rtt_ms: null }, { t: 200, rtt_ms: 5 }],
    [{ t: 150, rtt_ms: null }],
    null
  );
  assert.strictEqual(withLoss.length, 3);
  assert.strictEqual(withLoss[0].rtt_ms, null);
  assert.strictEqual(withLoss[2].rtt_ms, 5);

  // --- Non-finite / malformed samples (missing or NaN t) are dropped.
  const junk = PURE.mergeSamples(
    [{ t: 100, rtt_ms: 1 }, { t: NaN, rtt_ms: 2 }, { t: Infinity, rtt_ms: 3 }, {}],
    [{ t: "bad", rtt_ms: 4 }, { t: 200, rtt_ms: 5 }],
    null
  );
  assert.strictEqual(junk.length, 2, "non-finite/malformed t dropped");
  assert.deepStrictEqual(junk.map(function (s) { return s.t; }), [100, 200]);

  // --- Neither input is mutated; fresh sample objects are returned.
  const inA = [{ t: 100, rtt_ms: 10 }];
  const inB = [{ t: 100, rtt_ms: 20 }];
  const out = PURE.mergeSamples(inA, inB, null);
  assert.strictEqual(inA[0].rtt_ms, 10, "existing input not mutated");
  assert.strictEqual(inB[0].rtt_ms, 20, "incoming input not mutated");
  assert.ok(out[0] !== inA[0] && out[0] !== inB[0], "fresh object returned");

  // --- Live SSE append after merge: a later sample at a new t sorts in.
  const seed = PURE.mergeSamples([{ t: 100, rtt_ms: 10 }], [], null);
  const afterLive = PURE.mergeSamples(seed, [{ t: 50, rtt_ms: 5 }], null);
  assert.deepStrictEqual(
    afterLive.map(function (s) { return s.t; }),
    [50, 100],
    "out-of-order live sample sorts in"
  );
}

// tickStepS for multi-hour ranges (Slice D1 adds 1h / 3h): the canonical
// step set already covers them, so 1h yields >= 10m ticks and 3h yields
// >= 30m ticks — both keep the tick count in a readable range.
{
  const step1h = PURE.tickStepS(3600, 6);
  const step3h = PURE.tickStepS(10800, 6);
  assert.ok(step1h >= 600, "1h tick >= 10m, got " + step1h);
  assert.ok(step3h >= 1800, "3h tick >= 30m, got " + step3h);
  for (const [rangeS, step, label] of [
    [3600, step1h, "1h"],
    [10800, step3h, "3h"],
  ]) {
    const ticks = PURE.xTicks(rangeS - 0.001, rangeS * 2, step).length;
    assert.ok(ticks >= 3 && ticks <= 10, label + " tick count in [3,10], got " + ticks);
  }
  // Short ranges are unchanged: 1m/5m/10m still pick the same steps.
  assert.ok(PURE.tickStepS(60, 6) >= 10, "1m tick unchanged >= 10s");
  assert.ok(PURE.tickStepS(600, 6) >= 60, "10m tick unchanged >= 60s");
}

// formatClockHM: HH:MM 24-hour label for multi-hour tick axes.
{
  // Shape: exactly HH:MM, 2-digit fields.
  const s = PURE.formatClockHM(1700000000);
  assert.ok(/^\d{2}:\d{2}$/.test(s), "HH:MM shape: " + s);
  // Null/NaN handling mirrors formatClock.
  assert.strictEqual(PURE.formatClockHM(null), "--:--");
  assert.strictEqual(PURE.formatClockHM(NaN), "--:--");
  assert.strictEqual(PURE.formatClockHM(undefined), "--:--");
}

// crossesMidnight: detects when a visible window spans a local date
// boundary. Pure so the renderer's date-marker decision is node-assertable.
{
  // Build timestamps from LOCAL date components so the assertion does not
  // depend on the test runner's timezone.
  const localNoon = Math.floor(new Date(2026, 6, 30, 12, 0, 0).getTime() / 1000);
  const localElevenPm = Math.floor(new Date(2026, 6, 30, 23, 0, 0).getTime() / 1000);
  // Same-date window -> false.
  assert.strictEqual(PURE.crossesMidnight(localNoon, localNoon + 3600), false, "same-day");
  // Window spanning midnight -> true.
  assert.strictEqual(
    PURE.crossesMidnight(localElevenPm, localElevenPm + 3 * 3600),
    true,
    "spans midnight"
  );
  // Non-finite / wrong-type inputs -> false (renderer degrades gracefully).
  assert.strictEqual(PURE.crossesMidnight(NaN, 100), false);
  assert.strictEqual(PURE.crossesMidnight("a", "b"), false);
  assert.strictEqual(PURE.crossesMidnight(null, null), false);
}

// ----------------------------------------------------------- Slice D2 PURE
// (server rollup tier: bucket merge, bucket→drawable-points mapping,
//  resolution-per-range selection, multi-hour/multi-midnight tick stepping).

// tickStepS for 12h/24h ranges: the Slice D2 canonical-step extension
// (adds 7200/14400/21600) keeps the tick count in a readable range that
// the D1 set (max 3600) could not — 24h/3600 = 24 ticks is too dense.
{
  const step12h = PURE.tickStepS(43200, 6);
  const step24h = PURE.tickStepS(86400, 6);
  assert.ok(step12h >= 7200, "12h tick >= 2h, got " + step12h);
  assert.ok(step24h >= 14400, "24h tick >= 4h, got " + step24h);
  for (const [rangeS, step, label] of [
    [43200, step12h, "12h"],
    [86400, step24h, "24h"],
  ]) {
    const ticks = PURE.xTicks(rangeS - 0.001, rangeS * 2, step).length;
    assert.ok(ticks >= 3 && ticks <= 10, label + " tick count in [3,10], got " + ticks);
  }
  // Short ranges unchanged.
  assert.ok(PURE.tickStepS(60, 6) >= 10, "1m tick unchanged >= 10s");
  assert.ok(PURE.tickStepS(600, 6) >= 60, "10m tick unchanged >= 60s");
  assert.ok(PURE.tickStepS(3600, 6) >= 600, "1h tick unchanged >= 10m");
}

// resolutionForRange: ranges ≤ RAW_RANGE_MAX_S keep D1 raw; ranges beyond
// request ``auto`` (server resolves raw/1m against raw retention).
{
  assert.strictEqual(PURE.resolutionForRange(60), "raw");
  assert.strictEqual(PURE.resolutionForRange(600), "raw");
  assert.strictEqual(PURE.resolutionForRange(3600), "raw");
  assert.strictEqual(PURE.resolutionForRange(PURE.RAW_RANGE_MAX_S), "raw");
  assert.strictEqual(PURE.resolutionForRange(PURE.RAW_RANGE_MAX_S + 1), "auto");
  assert.strictEqual(PURE.resolutionForRange(43200), "auto");
  assert.strictEqual(PURE.resolutionForRange(86400), "auto");
  // Non-finite / wrong-type inputs default to raw (renderer degrades).
  assert.strictEqual(PURE.resolutionForRange(NaN), "raw");
  assert.strictEqual(PURE.resolutionForRange("a"), "raw");
  assert.strictEqual(PURE.resolutionForRange(null), "raw");
  // A slow-interval session may make ``auto`` return raw even for a long
  // panel; the client must then issue the explicit 1m fallback rather than
  // leaving the bucket-rendered panel empty.
  assert.strictEqual(PURE.bucketFallbackNeeded(43200, "raw"), true);
  assert.strictEqual(PURE.bucketFallbackNeeded(43200, "1m"), false);
  assert.strictEqual(PURE.bucketFallbackNeeded(10800, "raw"), false);
}

// mergeBuckets: sorted merge with exact-timestamp dedup (incoming wins),
// idempotent, optional cap. Mirrors mergeSamples so the bucket-fetch path
// is idempotent over overlapping windows.
{
  // --- Empty inputs.
  assert.deepStrictEqual(PURE.mergeBuckets([], [], null), []);
  assert.deepStrictEqual(PURE.mergeBuckets(null, undefined, null), []);

  // --- Simple union, ascending by t.
  const a = [{ t: 0, avg_ms: 10, count: 5, loss_count: 0 }];
  const b = [{ t: 60, avg_ms: 20, count: 6, loss_count: 1 }];
  const merged = PURE.mergeBuckets(a, b, null);
  assert.deepStrictEqual(
    merged.map(function (x) { return x.t; }),
    [0, 60],
    "union is ascending by t"
  );

  // --- Exact-t dedup: incoming wins on conflict.
  const existing = [{ t: 0, avg_ms: 10, count: 5, loss_count: 0 }];
  const incoming = [{ t: 0, avg_ms: 99, count: 7, loss_count: 2 }];
  const dup = PURE.mergeBuckets(existing, incoming, null);
  assert.strictEqual(dup.length, 1);
  assert.strictEqual(dup[0].avg_ms, 99, "incoming wins on exact-t conflict");
  assert.strictEqual(dup[0].count, 7);

  // --- Idempotency: re-merging incoming yields the same array.
  const again = PURE.mergeBuckets(dup, incoming, null);
  assert.deepStrictEqual(
    again.map(function (x) { return [x.t, x.avg_ms]; }),
    dup.map(function (x) { return [x.t, x.avg_ms]; }),
    "mergeBuckets is idempotent over repeated incoming"
  );

  // --- Cap: when set, oldest (lowest t) are dropped.
  const big = [];
  for (let i = 0; i < 10; i++) big.push({ t: i * 60, avg_ms: i, count: 1, loss_count: 0 });
  const capped = PURE.mergeBuckets(big, [], 5);
  assert.strictEqual(capped.length, 5);
  assert.strictEqual(capped[0].t, 300, "oldest dropped — most recent 5 survive");
  assert.strictEqual(capped[4].t, 540);

  // --- Full-loss bucket (avg_ms null) is preserved, not dropped.
  const withLoss = PURE.mergeBuckets(
    [{ t: 0, avg_ms: null, min_ms: null, max_ms: null, count: 2, loss_count: 2 }],
    [],
    null
  );
  assert.strictEqual(withLoss.length, 1);
  assert.strictEqual(withLoss[0].avg_ms, null);

  // --- Non-finite / malformed buckets (missing or NaN t) are dropped.
  const junk = PURE.mergeBuckets(
    [{ t: 0, avg_ms: 1, count: 1, loss_count: 0 }, { t: NaN }, { t: Infinity }, {}],
    [{ t: "bad" }, { t: 60, avg_ms: 2, count: 1, loss_count: 0 }],
    null
  );
  assert.strictEqual(junk.length, 2);
  assert.deepStrictEqual(junk.map(function (x) { return x.t; }), [0, 60]);

  // --- Neither input is mutated; fresh objects are returned.
  const inA = [{ t: 0, avg_ms: 10, count: 1, loss_count: 0 }];
  const inB = [{ t: 0, avg_ms: 20, count: 1, loss_count: 0 }];
  const out = PURE.mergeBuckets(inA, inB, null);
  assert.strictEqual(inA[0].avg_ms, 10, "existing input not mutated");
  assert.strictEqual(inB[0].avg_ms, 20, "incoming input not mutated");
  assert.ok(out[0] !== inA[0] && out[0] !== inB[0], "fresh object returned");
}

// bucketsToDrawablePoints: maps rollup buckets to step-line geometry + loss
// marks. Step line bridges over omitted/empty buckets (gaps) AND over
// full-loss buckets; loss marks cover both partial and full-loss buckets;
// full-loss buckets contribute NO line point.
{
  // Two successful buckets, no losses -> one move + (h, v) pair.
  const endS = 600;
  const rangeS = 600;
  const width = 100;
  const axisMax = 201;
  const height = 100;
  const twoOk = [
    { t: 0, avg_ms: 10, min_ms: 10, max_ms: 10, count: 5, loss_count: 0 },
    { t: 60, avg_ms: 50, min_ms: 50, max_ms: 50, count: 6, loss_count: 0 },
  ];
  const r2 = PURE.bucketsToDrawablePoints(twoOk, endS, rangeS, width, axisMax, height);
  assert.strictEqual(r2.line.length, 3, "2 points -> move + h + v");
  assert.strictEqual(r2.line[0].kind, "move");
  assert.strictEqual(r2.line[1].kind, "h");
  assert.strictEqual(r2.line[2].kind, "v");
  assert.strictEqual(r2.lossMarks.length, 0);

  // Single successful bucket -> just a move point (line.length === 1).
  const one = PURE.bucketsToDrawablePoints([twoOk[0]], endS, rangeS, width, axisMax, height);
  assert.strictEqual(one.line.length, 1);
  assert.strictEqual(one.line[0].kind, "move");

  // Empty / null input -> empty line and empty loss marks.
  assert.deepStrictEqual(
    PURE.bucketsToDrawablePoints([], endS, rangeS, width, axisMax, height),
    { line: [], lossMarks: [] }
  );
  assert.deepStrictEqual(
    PURE.bucketsToDrawablePoints(null, endS, rangeS, width, axisMax, height),
    { line: [], lossMarks: [] }
  );

  // All-loss buckets -> NO line points, loss marks for each.
  const allLoss = [
    { t: 0, avg_ms: null, min_ms: null, max_ms: null, count: 3, loss_count: 3 },
    { t: 60, avg_ms: null, min_ms: null, max_ms: null, count: 2, loss_count: 2 },
  ];
  const rl = PURE.bucketsToDrawablePoints(allLoss, endS, rangeS, width, axisMax, height);
  assert.strictEqual(rl.line.length, 0, "all-loss -> no line");
  assert.strictEqual(rl.lossMarks.length, 2, "one loss mark per bucket");
  assert.strictEqual(rl.lossMarks[0].fullLoss, true);
  assert.strictEqual(rl.lossMarks[1].fullLoss, true);

  // Mixed: successful, full-loss gap, successful -> step line BRIDGES over
  // the gap (move at first, h+v at third — no point for the gap bucket),
  // and the gap bucket contributes a full-loss mark.
  const bridged = [
    { t: 0, avg_ms: 10, min_ms: 10, max_ms: 10, count: 5, loss_count: 0 },
    { t: 60, avg_ms: null, min_ms: null, max_ms: null, count: 4, loss_count: 4 },
    { t: 120, avg_ms: 30, min_ms: 30, max_ms: 30, count: 6, loss_count: 0 },
  ];
  const rb = PURE.bucketsToDrawablePoints(bridged, endS, rangeS, width, axisMax, height);
  // Line: move(t=0) + h(t=120, y=avg@0) + v(t=120, y=avg@120).
  assert.strictEqual(rb.line.length, 3, "bridges over the gap bucket");
  assert.strictEqual(rb.line[0].kind, "move");
  assert.strictEqual(rb.line[1].kind, "h");
  assert.strictEqual(rb.line[2].kind, "v");
  assert.strictEqual(rb.lossMarks.length, 1, "only the gap bucket has losses");
  assert.strictEqual(rb.lossMarks[0].fullLoss, true);

  // Partial-loss bucket: avg_ms present AND loss_count > 0 -> both a line
  // point AND a (non-full) loss mark.
  const partial = [
    { t: 0, avg_ms: 15, min_ms: 10, max_ms: 20, count: 4, loss_count: 1 },
  ];
  const rp = PURE.bucketsToDrawablePoints(partial, endS, rangeS, width, axisMax, height);
  assert.strictEqual(rp.line.length, 1, "partial-loss bucket still has a line point");
  assert.strictEqual(rp.lossMarks.length, 1);
  assert.strictEqual(rp.lossMarks[0].fullLoss, false);

  // x mapping: bucket t -> x via PURE.timeToX. Check the move point's x
  // equals timeToX(t, ...) so the renderer aligns the line to the bucket
  // start.
  const aligned = PURE.bucketsToDrawablePoints(
    [{ t: 60, avg_ms: 10, min_ms: 10, max_ms: 10, count: 1, loss_count: 0 }],
    endS, rangeS, width, axisMax, height
  );
  const expectedX = PURE.timeToX(60, endS, rangeS, width);
  assert.ok(Math.abs(aligned.line[0].x - expectedX) < 1e-9, "x maps via timeToX");

  // Malformed bucket (missing / NaN t) is dropped from both line and marks.
  const malformed = PURE.bucketsToDrawablePoints(
    [{ avg_ms: 10, count: 1, loss_count: 0 }, { t: NaN, avg_ms: 10 }],
    endS, rangeS, width, axisMax, height
  );
  assert.strictEqual(malformed.line.length, 0);
  assert.strictEqual(malformed.lossMarks.length, 0);
}

console.log("app.js assertions passed");

// ------------------------------------------- internal-DNS masking (PURE)
// Screenshot mode: only DNS names of internal hosts are masked; IP literals
// never are; the toggle off is a no-op.

// isIpLiteralText: IPv4 dotted quads and IPv6-ish colons are literals.
assert.strictEqual(PURE.isIpLiteralText("192.168.1.1"), true);
assert.strictEqual(PURE.isIpLiteralText("fd00::1"), true);
assert.strictEqual(PURE.isIpLiteralText("[fd00::1]"), true);
assert.strictEqual(PURE.isIpLiteralText("router.lan"), false);
assert.strictEqual(PURE.isIpLiteralText("example.com"), false);
assert.strictEqual(PURE.isIpLiteralText("1234"), false);
assert.strictEqual(PURE.isIpLiteralText(""), false);
assert.strictEqual(PURE.isIpLiteralText(null), false);
assert.strictEqual(PURE.isIpLiteralText("10.0.0.256"), false);

// maskDnsName: masks only internal DNS names while the toggle is on.
assert.strictEqual(PURE.maskDnsName("router.lan", true, true), "Internal host");
assert.strictEqual(PURE.maskDnsName("router.lan", false, true), "router.lan");
assert.strictEqual(PURE.maskDnsName("router.lan", true, false), "router.lan");
// IP literals are never masked — even internal ones.
assert.strictEqual(PURE.maskDnsName("192.168.1.1", true, true), "192.168.1.1");
assert.strictEqual(PURE.maskDnsName("fd00::1", true, true), "fd00::1");
// Missing names pass through untouched (placeholders handled by callers).
assert.strictEqual(PURE.maskDnsName("", true, true), "");
assert.strictEqual(PURE.maskDnsName(null, true, true), null);

// -------------------------------- session-name masking (PURE) ------
// Screenshot masking of the KNOWN internal target across the target field,
// tab labels (incl. custom names), status/error lines, and the route banner.

// sessionMaskNames: known internal names only, and only once classified.
{
  // Unclassified (no snapshot yet) / external target: nothing to mask.
  assert.deepStrictEqual(PURE.sessionMaskNames("vault.corp", null, false), []);
  // Internal DNS target: raw target + tabLabel-derived host (deduped).
  assert.deepStrictEqual(
    PURE.sessionMaskNames("vault.corp", null, true),
    ["vault.corp"]
  );
  // URL target: the raw form contains a colon (IP-literal heuristic) so
  // only the scheme/port-stripped host is a maskable name — containment
  // matching still covers the raw form everywhere it renders.
  assert.deepStrictEqual(
    PURE.sessionMaskNames("http://vault.corp:8443/path", null, true),
    ["vault.corp"]
  );
  // Resolved target DNS rides along when present.
  assert.deepStrictEqual(
    PURE.sessionMaskNames("vault.corp", "host-17.corp", true),
    ["vault.corp", "host-17.corp"]
  );
  // IP targets yield no maskable names (IPs are never masked).
  assert.deepStrictEqual(PURE.sessionMaskNames("10.1.2.3", "10.1.2.3", true), []);
  assert.deepStrictEqual(PURE.sessionMaskNames("fd00::1", null, true), []);
  // null/empty targets yield no names (tabLabel's "New trace"
  // placeholder must never be treated as a host), but a resolved DNS
  // alone is still maskable.
  assert.deepStrictEqual(PURE.sessionMaskNames(null, null, true), []);
  assert.deepStrictEqual(PURE.sessionMaskNames("  ", null, true), []);
  assert.deepStrictEqual(PURE.sessionMaskNames("", "host-17.corp", true), [
    "host-17.corp",
  ]);
}

// maskSessionLabel: whole-label alias when the label equals/contains a
// known internal name; unrelated custom names pass through.
{
  var names = PURE.sessionMaskNames("vault.corp", "host-17.corp", true);
  // Masked only while the toggle is on.
  assert.strictEqual(PURE.maskSessionLabel("vault.corp", names, false), "vault.corp");
  // Exact match and containment (both raw and tabLabel-derived forms).
  assert.strictEqual(PURE.maskSessionLabel("vault.corp", names, true), "Internal host");
  assert.strictEqual(
    PURE.maskSessionLabel("http://vault.corp:8443", names, true),
    "Internal host"
  );
  // Custom tab names quoting the internal host leak — alias them.
  assert.strictEqual(
    PURE.maskSessionLabel("vault.corp prod", names, true),
    "Internal host"
  );
  assert.strictEqual(
    PURE.maskSessionLabel("HOST-17.CORP upstairs", names, true),
    "Internal host"
  );
  // An unrelated custom name is user content and passes through.
  assert.strictEqual(PURE.maskSessionLabel("Prod trace", names, true), "Prod trace");
  // No known names (IP target) -> nothing ever masked.
  assert.strictEqual(PURE.maskSessionLabel("10.1.2.3", [], true), "10.1.2.3");
  // Empty label stays empty (populateToolbar's null-target case).
  assert.strictEqual(PURE.maskSessionLabel("", names, true), "");
  assert.strictEqual(PURE.maskSessionLabel(null, names, true), "");
}

// maskText: splice the alias over occurrences inside free-form status /
// banner / error text (case-insensitive); unrelated text is untouched.
{
  var tn = PURE.sessionMaskNames("vault.corp", null, true);
  assert.strictEqual(
    PURE.maskText("Start failed: HTTP 400: invalid target vault.corp", tn, true),
    "Start failed: HTTP 400: invalid target Internal host"
  );
  assert.strictEqual(
    PURE.maskText("Resolving VAULT.CORP …", tn, true),
    "Resolving Internal host …"
  );
  // Multiple occurrences are each replaced.
  assert.strictEqual(
    PURE.maskText("vault.corp -> vault.corp", tn, true),
    "Internal host -> Internal host"
  );
  // Masking off, no match, or no names: text passes through verbatim.
  assert.strictEqual(
    PURE.maskText("Start failed: vault.corp", tn, false),
    "Start failed: vault.corp"
  );
  assert.strictEqual(PURE.maskText("Route discovery failed", tn, true), "Route discovery failed");
  assert.strictEqual(PURE.maskText("Running", [], true), "Running");
  // Regex metacharacters in a known name are matched literally.
  var tricky = PURE.sessionMaskNames("vault.corp (prod)", null, true);
  assert.strictEqual(
    PURE.maskText("failed to reach vault.corp (prod) today", tricky, true),
    "failed to reach Internal host today"
  );
  // Raw URL form: the host part is spliced out; scheme/port remain (they
  // do not identify the host — the DNS name is the secret).
  var urlNames = PURE.sessionMaskNames("http://vault.corp:8443", null, true);
  assert.strictEqual(
    PURE.maskText("POST http://vault.corp:8443 refused", urlNames, true),
    "POST http://Internal host:8443 refused"
  );
  assert.strictEqual(PURE.maskText(null, tn, true), "");
}

// The default ranges mirror the server's RFC1918 + RFC4193 ULA default
// (fc00::/7 — used by the settings dialog's "Reset to defaults" button).
assert.deepStrictEqual(PURE.DEFAULT_INTERNAL_CIDRS, [
  "10.0.0.0/8",
  "172.16.0.0/12",
  "192.168.0.0/16",
  "fc00::/7",
]);

// ---------------------------------------- ICMP non-responsive (PURE)
// Neutral presentation for never-replied intermediate hops. Destination is
// excluded; any observed reply (sticky, live, or baseline) disqualifies;
// classification needs >= 5 observed probes OR the server's lifetime flag.

// Startup: no samples at all -> not classified.
assert.strictEqual(PURE.hopNonResponsive({ isDestination: false }), false);

// Fewer than 5 failed probes -> not yet classified.
{
  const samples = [1, 2, 3, 4].map((t) => ({ t, rtt_ms: null }));
  assert.strictEqual(
    PURE.hopNonResponsive({ isDestination: false, samples }),
    false
  );
}

// 5 failed probes, no reply ever -> classified.
{
  const samples = [1, 2, 3, 4, 5].map((t) => ({ t, rtt_ms: null }));
  assert.strictEqual(
    PURE.hopNonResponsive({ isDestination: false, samples }),
    true
  );
}

// Any live reply in the buffer disqualifies, even with many losses around.
{
  const samples = [
    { t: 1, rtt_ms: null },
    { t: 2, rtt_ms: 12 },
    { t: 3, rtt_ms: null },
    { t: 4, rtt_ms: null },
    { t: 5, rtt_ms: null },
    { t: 6, rtt_ms: null },
  ];
  assert.strictEqual(
    PURE.hopNonResponsive({ isDestination: false, samples }),
    false
  );
}

// Sticky everReplied (historical reply aged out of the buffer) disqualifies —
// never classify on rolling-window 100% loss alone.
{
  const samples = [1, 2, 3, 4, 5, 6].map((t) => ({ t, rtt_ms: null }));
  assert.strictEqual(
    PURE.hopNonResponsive({ isDestination: false, samples, everReplied: true }),
    false
  );
}

// A snapshot baseline carrying an average proves a historical reply.
{
  const samples = [1, 2, 3, 4, 5].map((t) => ({ t, rtt_ms: null }));
  assert.strictEqual(
    PURE.hopNonResponsive({
      isDestination: false,
      samples,
      seed: { count: 50, avg_ms: 12.5 },
    }),
    false
  );
  // An all-loss baseline (avg null) is NOT counter-evidence.
  assert.strictEqual(
    PURE.hopNonResponsive({
      isDestination: false,
      samples,
      seed: { count: 50, avg_ms: null },
    }),
    true
  );
}

// The server's lifetime flag classifies even when the local buffer has
// fewer than 5 samples (mid-trace attach: pre-attach history is unseen).
{
  assert.strictEqual(
    PURE.hopNonResponsive({ isDestination: false, samples: [], serverFlag: true }),
    true
  );
  // ...but a client-observed reply outranks a stale server flag (later
  // recovery reverts immediately).
  assert.strictEqual(
    PURE.hopNonResponsive({
      isDestination: false,
      samples: [{ t: 1, rtt_ms: 9 }],
      serverFlag: true,
    }),
    false
  );
}

// Destination is never classified (its 100% loss stays an error).
{
  const samples = [1, 2, 3, 4, 5, 6].map((t) => ({ t, rtt_ms: null }));
  assert.strictEqual(
    PURE.hopNonResponsive({ isDestination: true, samples, serverFlag: true }),
    false
  );
}

// Malformed samples (missing t) are not counted as probes.
{
  const samples = [{ rtt_ms: null }, { t: 1, rtt_ms: null }];
  assert.strictEqual(
    PURE.hopNonResponsive({ isDestination: false, samples }),
    false
  );
}

// ------------------------------------------- throughput overlay (PURE)
// Nice Mbps axis ceiling: 1/2/5 x 10^k ladder with a floor of 1.

assert.strictEqual(PURE.mbpsAxisMax([]), 1);
// Right-hand axis labels: 0 / mid / max with an explicit " Mbps" unit
// (the max label previously read "Mb"). Pure so the label content the
// unclipped drawThroughputAxis paints stays node-assertable.
{
  const labels = PURE.tpAxisLabels(100);
  assert.deepStrictEqual(
    labels.map(function (l) { return l.text; }),
    ["0", "50.0", "100.0 Mbps"]
  );
  assert.deepStrictEqual(
    labels.map(function (l) { return l.v; }),
    [0, 50, 100]
  );
}

assert.strictEqual(PURE.mbpsAxisMax([{ t: 0, in_mbps: 0, out_mbps: 0 }]), 1);
assert.strictEqual(PURE.mbpsAxisMax([{ t: 0, in_mbps: 0.7, out_mbps: 0.1 }]), 1);
assert.strictEqual(PURE.mbpsAxisMax([{ t: 0, in_mbps: 1.0, out_mbps: 0 }]), 1);
assert.strictEqual(PURE.mbpsAxisMax([{ t: 0, in_mbps: 3.0, out_mbps: 0 }]), 5);
assert.strictEqual(PURE.mbpsAxisMax([{ t: 0, in_mbps: 80, out_mbps: 0 }]), 100);
assert.strictEqual(PURE.mbpsAxisMax([{ t: 0, in_mbps: 0, out_mbps: 250 }]), 500);
assert.strictEqual(
  PURE.mbpsAxisMax([
    { t: 0, in_mbps: 120, out_mbps: 30 },
    { t: 10, in_mbps: null, out_mbps: null },
  ]),
  200
);
// Null / non-finite values are ignored by the scale.
assert.strictEqual(
  PURE.mbpsAxisMax([{ t: 0, in_mbps: null, out_mbps: undefined }, { t: 1 }]),
  1
);

// Throughput path segments: the direction lines BREAK at null values and
// at timestamp gaps larger than the bucket cadence — never bridged, never
// zero-filled. Geometry is step-path order ("move" then "h"/"v" pairs),
// matching PURE.stepPath so the renderer shares its drawing loop.
(function () {
  // endS=100, rangeS=100, width=100 -> timeToX(t) == t; y is the value.
  var segsFor = function (pts, key, gap) {
    return PURE.tpSegments(pts, key, gap, 100, 100, 100, function (v) { return v; });
  };

  // Contiguous buckets at exactly the cadence stay ONE segment.
  {
    var segs = segsFor(
      [
        { t: 0, in_mbps: 1, out_mbps: 2 },
        { t: 10, in_mbps: 3, out_mbps: 4 },
        { t: 20, in_mbps: 5, out_mbps: 6 },
      ],
      "in_mbps",
      10
    );
    assert.strictEqual(segs.length, 1);
    assert.deepStrictEqual(segs[0], [
      { x: 0, y: 1, kind: "move" },
      { x: 10, y: 1, kind: "h" },
      { x: 10, y: 3, kind: "v" },
      { x: 20, y: 3, kind: "h" },
      { x: 20, y: 5, kind: "v" },
    ]);
  }

  // A null value BREAKS the path (the null point is a gap, not a bridge).
  {
    var pts = [
      { t: 0, in_mbps: 1, out_mbps: 2 },
      { t: 10, in_mbps: null, out_mbps: 4 },
      { t: 20, in_mbps: 5, out_mbps: 6 },
    ];
    var inSegs = segsFor(pts, "in_mbps", 10);
    var outSegs = segsFor(pts, "out_mbps", 10);
    assert.strictEqual(inSegs.length, 2); // in breaks at the null
    assert.deepStrictEqual(inSegs[0], [{ x: 0, y: 1, kind: "move" }]);
    assert.deepStrictEqual(inSegs[1], [{ x: 20, y: 5, kind: "move" }]);
    assert.strictEqual(outSegs.length, 1); // other direction unaffected
  }

  // An omitted bucket (gap > cadence) breaks the path even with values
  // present on both sides; a gap EXACTLY equal to the cadence does not.
  {
    var segs3 = segsFor(
      [
        { t: 0, in_mbps: 1 },
        { t: 10, in_mbps: 1 }, // gap exactly 10 == cadence: same segment
        { t: 40, in_mbps: 1 }, // gap 30 > cadence: new segment
      ],
      "in_mbps",
      10
    );
    assert.strictEqual(segs3.length, 2);
    assert.strictEqual(segs3[0].length, 3); // move + one h/v pair (t=0,10)
    assert.deepStrictEqual(segs3[1], [{ x: 40, y: 1, kind: "move" }]);
  }

  // maxGapS 0/undefined: only nulls break (no cadence known).
  assert.strictEqual(
    segsFor([{ t: 0, in_mbps: 1 }, { t: 500, in_mbps: 1 }], "in_mbps", 0).length,
    1
  );

  // All-null / empty / malformed input yields no segments.
  var id = function (v) { return v; };
  assert.deepStrictEqual(
    PURE.tpSegments([{ t: 0, in_mbps: null }], "in_mbps", 10, 100, 100, 100, id),
    []
  );
  assert.deepStrictEqual(PURE.tpSegments([], "in_mbps", 10, 100, 100, 100, id), []);
  assert.deepStrictEqual(
    PURE.tpSegments([null, { t: NaN, in_mbps: 1 }, "x"], "in_mbps", 10, 100, 100, 100, id),
    []
  );

  // A malformed point mid-stream acts as a break.
  assert.strictEqual(
    segsFor([{ t: 0, in_mbps: 1 }, { t: 10 }, { t: 20, in_mbps: 2 }], "in_mbps", 10).length,
    2
  );
})();

// ------------------------- overlay fetch on tab activation (PURE)
// A tab activating with an enabled + configured overlay but NO points
// (cleared by a settings save while backgrounded; paused so tick skips
// it) must fetch. Has points / unconfigured / disabled / idle → no fetch.
var activateView = function (over) {
  var v = {
    sessionId: "s1",
    throughputOn: true,
    influxUrl: "http://h:8086",
    throughputPoints: [],
  };
  for (var k in over) v[k] = over[k];
  return v;
};
assert.strictEqual(PURE.throughputFetchOnActivate(activateView()), true);
// Points already present (running tab, or fetched earlier) → no refetch.
assert.strictEqual(
  PURE.throughputFetchOnActivate(
    activateView({ throughputPoints: [{ t: 1, in_mbps: 1, out_mbps: 1 }] })
  ),
  false
);
// Overlay off / source unconfigured / idle tab → no fetch.
assert.strictEqual(PURE.throughputFetchOnActivate(activateView({ throughputOn: false })), false);
assert.strictEqual(PURE.throughputFetchOnActivate(activateView({ influxUrl: "" })), false);
assert.strictEqual(PURE.throughputFetchOnActivate(activateView({ sessionId: null })), false);
// Malformed/missing view never fetches.
assert.strictEqual(PURE.throughputFetchOnActivate(null), false);
assert.strictEqual(PURE.throughputFetchOnActivate({}), false);

console.log("app.js assertions passed");

// ------------------------------------------------ Copy summary (PURE)
// The copied text describes the ACTIVE trace's TARGET only: window bounds in
// explicit UTC, probe interval, target avg/min/max RTT and packet loss. It
// reuses the table's stats for the selected window (manual drag-focus wins
// over the rolling Focus dropdown; a paused trace copies the frozen window).

// UTC stamps are timezone-independent and label the zone explicitly.
assert.strictEqual(
  PURE.formatUtcStamp(Date.UTC(2026, 5, 10, 17, 36, 23) / 1000),
  "2026-06-10 17:36:23 UTC"
);
assert.strictEqual(PURE.formatUtcStamp(NaN), "—");

// Ordinary mixed success/loss target window: exact line-for-line text.
{
  const summary = PURE.buildTargetSummary({
    target: "example.com",
    source: "live",
    stats: { count: 4, avg: 20, min: 10, max: 30, cur: 30, lossPct: 25 },
    windowLabel: "Auto",
    startS: Date.UTC(2026, 5, 10, 17, 36, 23) / 1000,
    endS: Date.UTC(2026, 5, 10, 17, 46, 23) / 1000,
    intervalS: 2.5,
  });
  assert.strictEqual(
    summary,
    [
      "netplot trace summary",
      "Target: example.com",
      "Window: Auto, 2026-06-10 17:36:23 UTC - 2026-06-10 17:46:23 UTC",
      "Probe interval: 2.5 s",
      "RTT avg/min/max (ms): 20.0 / 10.0 / 30.0",
      "Packet loss: 25.0% (lost 1 of 4 probes)",
    ].join("\n")
  );
}

// Empty data (no probes / idle / reset) has nothing to copy.
assert.strictEqual(
  PURE.buildTargetSummary({ stats: { count: 0, avg: null, min: null, max: null, lossPct: 0 } }),
  null
);
assert.strictEqual(PURE.buildTargetSummary({}), null);
assert.strictEqual(PURE.buildTargetSummary(null), null);

// All-loss window: latencies are unavailable, loss counts stay exact.
{
  const allLoss = PURE.buildTargetSummary({
    target: "down.example",
    source: "live",
    stats: { count: 3, avg: null, min: null, max: null, cur: null, lossPct: 100 },
    windowLabel: "1 minute",
    startS: 940,
    endS: 1000,
    intervalS: 1,
  });
  assert.ok(allLoss.includes("RTT avg/min/max (ms): n/a / n/a / n/a"));
  assert.ok(allLoss.includes("Packet loss: 100.0% (lost 3 of 3 probes)"));
  assert.ok(allLoss.includes("Probe interval: 1 s"));
}

// A blended ratio whose lost count is not an integer is never rounded into a
// fabricated integer.
{
  const blended = PURE.buildTargetSummary({
    target: "x",
    source: "combined",
    seedHasMax: true,
    stats: { count: 3, avg: 10, min: 10, max: 10, lossPct: 33.34 },
    windowLabel: "Auto",
    startS: 940,
    endS: 1000,
    intervalS: 2.5,
  });
  assert.ok(blended.includes("Packet loss: 33.3% (3 probes)"));
  assert.ok(!blended.includes("lost "));
}

// Unknown probe interval is reported as unavailable, not guessed.
assert.ok(
  PURE.buildTargetSummary({
    stats: { count: 1, avg: 1, min: 1, max: 1, lossPct: 0 },
  }).includes("Probe interval: n/a")
);

// Enable predicate: needs a session AND usable target stats.
assert.strictEqual(PURE.summaryEnabled({ sessionId: "s1", stats: { count: 2 } }), true);
assert.strictEqual(PURE.summaryEnabled({ sessionId: "s1", stats: { count: 0 } }), false);
assert.strictEqual(PURE.summaryEnabled({ sessionId: "s1", stats: null }), false);
assert.strictEqual(PURE.summaryEnabled({ sessionId: null, stats: { count: 2 } }), false);
assert.strictEqual(PURE.summaryEnabled(null), false);

// A manual drag-focus window overrides the rolling Focus-dropdown window.
{
  const s = state();
  PURE.pushSample(s, 0, 940, 10);
  PURE.pushSample(s, 0, 990, 20);
  PURE.pushSample(s, 0, 999, 30);
  const rolling = PURE.displayStatsForHop(s, 0, 1000, 60);
  assert.strictEqual(rolling.source, "live");
  assert.strictEqual(rolling.data.count, 3);
  s.focus = { startS: 930, endS: 995 };
  const manual = PURE.displayStatsForHop(s, 0, 1000, 60);
  assert.strictEqual(manual.source, "focus");
  assert.strictEqual(manual.data.count, 2);
  assert.strictEqual(manual.data.avg, 15);
  // A manual window with no samples is no data (button stays disabled).
  s.focus = { startS: 100, endS: 200 };
  assert.strictEqual(PURE.displayStatsForHop(s, 0, 1000, 60), null);
}

// Paused traces copy the FROZEN window: the rolling cutoff follows the pause
// instant, not wall time (which would have slid past the samples).
{
  const s = state();
  PURE.pushSample(s, 0, 970, 42);
  // Wall time is 4000s past the pause instant -> the unfrozen window is empty.
  assert.strictEqual(PURE.displayStatsForHop(s, 0, 5000, 60), null);
  const frozenNow = PURE.effectiveNowS(5000, 1000);
  assert.strictEqual(frozenNow, 1000);
  const stats = PURE.displayStatsForHop(s, 0, frozenNow, 60);
  assert.strictEqual(stats.data.count, 1);
  assert.strictEqual(stats.data.avg, 42);
  const text = PURE.buildTargetSummary({
    target: "t",
    source: stats.source,
    stats: stats.data,
    windowLabel: "Auto",
    startS: frozenNow - 60,
    endS: frozenNow,
    intervalS: 2.5,
  });
  assert.ok(text.includes(" - " + PURE.formatUtcStamp(1000)));
}

// Destination stream selection: the final hop when it reaches the target,
// hop 0 (the separate target stream) otherwise — never blindly one of them.
{
  const hops = [
    { index: 0, ip: "10.0.0.1" },
    { index: 3, ip: "203.0.113.9" },
  ];
  assert.strictEqual(PURE.primaryHopIndex(hops, "203.0.113.9", "ready"), 3);
  assert.strictEqual(PURE.primaryHopIndex(hops, "198.51.100.7", "ready"), 0);

  const s = state();
  [10, 10].forEach(function (v, i) { PURE.pushSample(s, 0, 990 + i, v); });
  [70, 70].forEach(function (v, i) { PURE.pushSample(s, 3, 990 + i, v); });
  function summaryFor(hopIndex) {
    const stats = PURE.displayStatsForHop(s, hopIndex, 1000, 60);
    return PURE.buildTargetSummary({
      target: "203.0.113.9",
      source: stats.source,
      stats: stats.data,
      windowLabel: "Auto",
      startS: 940,
      endS: 1000,
      intervalS: 2.5,
    });
  }
  assert.ok(
    summaryFor(PURE.primaryHopIndex(hops, "203.0.113.9", "ready")).includes(
      "RTT avg/min/max (ms): 70.0 / 70.0 / 70.0"
    )
  );
  assert.ok(
    summaryFor(PURE.primaryHopIndex(hops, "198.51.100.7", "ready")).includes(
      "RTT avg/min/max (ms): 10.0 / 10.0 / 10.0"
    )
  );
}

// Snapshot baselines historically ship without max_ms: the combined stat's max
// is then live-delta-only, so the summary reports it unavailable (with a note)
// instead of presenting it as a full-window maximum.
{
  const s = state();
  PURE.adoptSnapshotBaseline(
    s, "0",
    { count: 10, avg_ms: 20, min_ms: 10, cur_ms: 20, loss_pct: 0 },
    990, 990, 941, 60
  );
  PURE.pushSample(s, 0, 995, 40);
  PURE.pushSample(s, 0, 998, 45);
  let stats = PURE.displayStatsForHop(s, 0, 1000, 60);
  assert.strictEqual(stats.source, "combined");
  assert.strictEqual(stats.data.max, 45); // the table's merged value
  const base = {
    target: "t",
    source: stats.source,
    stats: stats.data,
    windowLabel: "Auto",
    startS: 940,
    endS: 1000,
    intervalS: 2.5,
  };
  const noSeedMax = PURE.buildTargetSummary(
    Object.assign({}, base, { seedHasMax: false })
  );
  assert.ok(/^RTT avg\/min\/max \(ms\): [\d.]+ \/ [\d.]+ \/ n\/a$/m.test(noSeedMax));
  assert.ok(noSeedMax.includes("Note: max unavailable"));

  // A baseline that does carry a max yields a real full-window maximum.
  s.seedStats["0"].max_ms = 50;
  stats = PURE.displayStatsForHop(s, 0, 1000, 60);
  const withSeedMax = PURE.buildTargetSummary({
    target: "t",
    source: stats.source,
    stats: stats.data,
    seedHasMax: true,
    windowLabel: "Auto",
    startS: 940,
    endS: 1000,
    intervalS: 2.5,
  });
  assert.ok(/^RTT avg\/min\/max \(ms\): [\d.]+ \/ [\d.]+ \/ 50\.0$/m.test(withSeedMax));
  assert.ok(!withSeedMax.includes("max unavailable"));
}

// Masked target: the Mask internal DNS toggle applies to copied text too.
{
  assert.strictEqual(
    PURE.summaryTargetLabel({ dns: "nas.local", ip: "192.168.1.5" }, true, true, ["nas.local"]),
    "Internal host"
  );
  assert.strictEqual(
    PURE.summaryTargetLabel({ dns: "nas.local", ip: "192.168.1.5" }, true, false, ["nas.local"]),
    "nas.local"
  );
  // External host with masking on is unchanged.
  assert.strictEqual(
    PURE.summaryTargetLabel({ dns: "example.com", ip: "203.0.113.9" }, false, true, []),
    "example.com"
  );
  // IP literals are never masked.
  assert.strictEqual(
    PURE.summaryTargetLabel({ ip: "192.168.1.5" }, true, true, ["nas.local"]),
    "192.168.1.5"
  );
  assert.strictEqual(PURE.summaryTargetLabel(null, false, false, []), "unknown");

  const maskedText = PURE.buildTargetSummary({
    target: PURE.summaryTargetLabel({ dns: "nas.local", ip: "192.168.1.5" }, true, true, ["nas.local"]),
    source: "live",
    stats: { count: 1, avg: 1, min: 1, max: 1, lossPct: 0 },
    windowLabel: "Auto",
    startS: 999,
    endS: 1000,
    intervalS: 1,
  });
  assert.ok(maskedText.includes("Target: Internal host"));
  assert.ok(!maskedText.includes("nas.local"));
}

// Availability follows the SELECTED window end-to-end, not just the sample
// buffer: samples in the rolling window enable the button, a drag-focus over
// an empty stretch disables it, clearing focus re-enables it, and a window
// that slides past the samples disables it again.
{
  const s = state();
  PURE.pushSample(s, 0, 995, 20);
  function available(nowS, focusS) {
    const stats = PURE.displayStatsForHop(s, 0, nowS, focusS);
    return PURE.summaryEnabled({ sessionId: "s1", stats: stats ? stats.data : null });
  }
  assert.strictEqual(available(1000, 60), true);
  s.focus = { startS: 100, endS: 200 };
  assert.strictEqual(available(1000, 60), false);
  s.focus = null;
  assert.strictEqual(available(1000, 60), true);
  assert.strictEqual(available(2000, 60), false);
}

// ---------------------------------------------- Footer source (primary facts)
// The footer's sample count comes from the SAME stats object the table and
// copied summary use for the primary stream (never an SSE message counter,
// which reconnect/history restoration would give a different meaning), and
// its last-sample time comes from the primary stream's raw buffer so a
// restored tab shows the existing history's recency instead of a blank.
{
  const s = state();
  // Primary stream: hop 3 with a baseline + live delta; hop 0 also present
  // but NOT primary (route ready, final hop reaches the target).
  PURE.adoptSnapshotBaseline(
    s, "3",
    { count: 7, avg_ms: 30, min_ms: 20, cur_ms: 30, loss_pct: 0 },
    990, 990, 941, 60
  );
  [0, 3].forEach(function (hop) {
    PURE.pushSample(s, hop, 991, hop === 3 ? 35 : 10);
    PURE.pushSample(s, hop, 999, hop === 3 ? 40 : 11);
  });
  const facts = PURE.primaryStreamFacts(s, PURE.primaryHopIndex(
    [{ index: 3, ip: "203.0.113.9" }], "203.0.113.9", "ready"
  ), 1000, 60);
  // Count equals the table's stats for that hop (combined 7 + 2).
  assert.strictEqual(facts.stats.count, 9);
  assert.strictEqual(facts.stats.cur, 40);
  // Last-sample time is the primary stream's own newest buffered sample.
  assert.strictEqual(facts.lastT, 999);
  // Same query the table would issue for hop 0 stays hop-0-scoped.
  const hop0 = PURE.primaryStreamFacts(s, 0, 1000, 60);
  assert.strictEqual(hop0.lastT, 999);
  assert.strictEqual(hop0.stats.count, 2);

  // Empty buffer / no stats: nulls, not fabricated values.
  const empty = PURE.primaryStreamFacts(state(), 0, 1000, 60);
  assert.strictEqual(empty.stats, null);
  assert.strictEqual(empty.lastT, null);
  // A restored tab with ONLY history in the buffer still reports recency.
  const restored = state();
  restored.samples["5"] = [{ t: 900, rtt_ms: 8 }, { t: 905, rtt_ms: 9 }];
  const r5 = PURE.primaryStreamFacts(restored, 5, 950, 60);
  assert.strictEqual(r5.stats.count, 2, "raw-only stats over the window");
  assert.strictEqual(r5.lastT, 905);
}


// ------------------------------------------- Copy summary: clipboard paths
// Success via the async Clipboard API, fallback to execCommand("copy") on
// plain-HTTP LAN (or when the API rejects), and honest failure when neither
// works. The textarea must be cleaned up and focus restored.
function fakeDoc(execOk) {
  const doc = {
    body: {
      child: null,
      appendChild(el) { doc.body.child = el; },
      removeChild(el) { if (doc.body.child === el) doc.body.child = null; },
    },
    activeElement: { focused: 0, focus() { this.focused += 1; } },
    createElement(tag) {
      return {
        tagName: tag,
        value: "",
        style: {},
        attrs: {},
        setAttribute(k, v) { this.attrs[k] = v; },
        select() { this.selected = true; },
      };
    },
    execCommand(cmd) { doc.command = cmd; return !!execOk; },
  };
  return doc;
}

(async function () {
  // Clipboard API path (secure context).
  {
    let written = null;
    const ok = await PURE.copyPlainText({
      text: "hello",
      clipboard: { writeText(t) { written = t; return Promise.resolve(); } },
      doc: fakeDoc(true),
    });
    assert.strictEqual(ok, true);
    assert.strictEqual(written, "hello");
  }

  // Rejecting Clipboard API falls back to execCommand (plain-HTTP LAN).
  {
    const doc = fakeDoc(true);
    const ok = await PURE.copyPlainText({
      text: "fallback",
      clipboard: { writeText() { return Promise.reject(new Error("denied")); } },
      doc,
    });
    assert.strictEqual(ok, true);
    assert.strictEqual(doc.command, "copy");
    assert.strictEqual(doc.body.child, null); // temporary textarea removed
    assert.strictEqual(doc.activeElement.focused, 1); // focus restored
  }

  // No Clipboard API at all: execCommand path only.
  {
    const doc = fakeDoc(true);
    assert.strictEqual(await PURE.copyPlainText({ text: "legacy", clipboard: null, doc }), true);
    assert.strictEqual(doc.command, "copy");
    assert.strictEqual(doc.body.child, null);
  }

  // Neither path works: report failure, not false success.
  {
    const doc = fakeDoc(false);
    const ok = await PURE.copyPlainText({
      text: "nope",
      clipboard: { writeText() { return Promise.reject(new Error("denied")); } },
      doc,
    });
    assert.strictEqual(ok, false);
    assert.strictEqual(doc.body.child, null);
  }

  // A throwing execCommand (some browsers refuse it) is a failure too.
  {
    const doc = {
      body: { appendChild() {}, removeChild() {} },
      createElement() { return { setAttribute() {}, style: {}, select() {}, value: "" }; },
      execCommand() { throw new Error("refused"); },
    };
    assert.strictEqual(PURE.execCommandCopy(doc, "t"), false);
  }

  // No document: nothing is claimed.
  assert.strictEqual(PURE.execCommandCopy(null, "t"), false);

  console.log("copy-summary clipboard assertions passed");
})().catch(function (err) {
  console.error(err);
  process.exit(1);
});
