"""Tests for the Prometheus ``/metrics`` endpoint and its pure formatter.

Covers (acceptance criteria from the slice spec):

1. Format validity — HELP/TYPE present, exposition parseable, label
   escaping correct for hostile target/name strings.
2. Value correctness — current/avg/loss/up per target and per hop against
   known sample sets.
3. Window boundary — sample 61 does not count.
4. Loss drops current — a loss as the last probe omits the current-RTT
   series (no stale values).
5. Multi-session — distinct series, no cross-contamination; deleted
   session's series gone on the next scrape.
6. Paused label — series present with ``state="paused"``.
7. Escaping — backslash, double-quote, and newline in label values.
8. HTTP — 200, correct Content-Type, empty-state behaviour.
9. Ordered commit — with the overlapping scheduler holding round 1 while
   later rounds finish first, capture/render describe exactly the
   committed launch-ordered rounds (nothing for pending rounds, nothing
   added by canceled rounds after a pause).

The pure formatter (:func:`render_metrics`) is exercised directly with
hand-built :class:`SessionMetrics` so value/escaping logic is pinned
without an HTTP round-trip; the capture+endpoint path is exercised
through ``TestClient`` with injected sample data.
"""

from __future__ import annotations

import asyncio
import re
import sys
import time
from math import inf, isfinite, nan

import pytest
from fastapi.testclient import TestClient

from netplot.metrics import (
    METRICS_WINDOW_SAMPLES,
    HopMetrics,
    SessionMetrics,
    TargetMetrics,
    capture_metrics,
    render_metrics,
)
from netplot.session import Hop, Sample
from netplot.web import create_app

# --------------------------------------------------------- tiny Prom parser
#
# A minimal but strict exposition-format parser for the tests. NOT a
# dependency — written here so the tests do not pull in prometheus_client.
# It validates the structural rules (HELP/TYPE, label escaping, numeric
# values) the endpoint must satisfy and lets the tests assert on parsed
# series rather than raw text.

_SAMPLE_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)"  # metric name
    r"(?:\{([^}]*)\})?"  # optional label block
    r"\s+(-?[0-9.]+)"  # value
    r"\s*$"
)
_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')
_HELP_RE = re.compile(r"^# HELP\s+([a-zA-Z_:][a-zA-Z0-9_:]*)\s+(.*)$")
_TYPE_RE = re.compile(r"^# TYPE\s+([a-zA-Z_:][a-zA-Z0-9_:]*)\s+(\w+)\s*$")


def _unescape_label(value: str) -> str:
    return value.replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")


def parse_prom(text: str) -> dict:
    """Parse exposition text into a structured dict.

    Returns ``{families: {name: {help, type, samples}}, samples: [...]}``
    where each sample is ``{"name", "labels": {...}, "value": float}``.
    Raises ``AssertionError`` (via assert) on any malformed line so a
    format regression turns into a test failure rather than a silent skip.
    """
    families: dict[str, dict] = {}
    samples: list[dict] = []
    for line in text.split("\n"):
        if not line:
            continue
        if line.startswith("#"):
            m = _HELP_RE.match(line)
            if m:
                name, help_text = m.group(1), m.group(2)
                families.setdefault(name, {"help": help_text, "type": None, "count": 0})
                families[name]["help"] = help_text
                continue
            m = _TYPE_RE.match(line)
            if m:
                name, typ = m.group(1), m.group(2)
                families.setdefault(name, {"help": None, "type": None, "count": 0})
                families[name]["type"] = typ
                continue
            # A bare comment line (e.g. ": keepalive") is not expected here.
            assert False, f"unexpected comment line: {line!r}"
        m = _SAMPLE_RE.match(line)
        assert m, f"unparseable sample line: {line!r}"
        name, label_block, value_str = m.group(1), m.group(2), m.group(3)
        labels: dict[str, str] = {}
        if label_block is not None:
            leftover = label_block
            for lm in _LABEL_RE.finditer(label_block):
                labels[lm.group(1)] = _unescape_label(lm.group(2))
                leftover = leftover.replace(lm.group(0), "", 1)
            # After removing every matched key="value" token, only commas and
            # whitespace should remain — guards against malformed labels.
            assert re.fullmatch(r"[,\s]*", leftover), (
                f"unparsed label text in {label_block!r}"
            )
        families.setdefault(name, {"help": None, "type": None, "count": 0})
        families[name]["count"] += 1
        samples.append({"name": name, "labels": labels, "value": float(value_str)})
    return {"families": families, "samples": samples}


def _find(samples: list[dict], name: str, labels: dict | None = None) -> list[dict]:
    """Return all parsed samples matching ``name`` (and optionally labels).

    ``labels`` is a SUBSET match: every given label must be present and equal
    in the sample, but the sample may carry additional labels. This lets a
    test key on ``session_id`` alone without spelling out every label.
    """
    out = []
    for s in samples:
        if s["name"] != name:
            continue
        if labels is not None and not labels.items() <= s["labels"].items():
            continue
        out.append(s)
    return out


def _one(samples: list[dict], name: str, labels: dict) -> dict:
    hits = _find(samples, name, labels)
    assert len(hits) == 1, f"expected exactly one {name}{labels}, got {hits}"
    return hits[0]


def _none(samples: list[dict], name: str, labels: dict) -> None:
    hits = _find(samples, name, labels)
    assert not hits, f"expected no {name}{labels}, got {hits}"


# ---------------------------------------------------------- pure formatter
#
# Direct render_metrics tests with hand-built SessionMetrics. These pin
# value/escaping logic without an HTTP round-trip or a live SessionManager.


def _sess(
    session_id: str = "s1",
    target: str = "127.0.0.1",
    name: str | None = None,
    persistent: bool = False,
    state: str = "running",
    target_stream: TargetMetrics | None = None,
    hops: list[HopMetrics] | None = None,
) -> SessionMetrics:
    return SessionMetrics(
        session_id=session_id,
        target=target,
        name=name,
        persistent=persistent,
        state=state,
        target_stream=target_stream,
        hops=hops or [],
    )


def test_render_empty_sessions_yields_no_samples() -> None:
    """With zero sessions the formatter emits HELP/TYPE metadata (so scrapers
    learn the metric names exist) but no sample lines."""
    parsed = parse_prom(render_metrics([]))
    assert parsed["samples"] == []
    # Every declared family still carries its HELP/TYPE metadata.
    for name in (
        "netplot_session_info",
        "netplot_target_rtt_current_ms",
        "netplot_hop_loss_pct",
    ):
        assert name in parsed["families"]


def test_render_session_info_always_present_even_without_samples() -> None:
    """A session with zero samples still emits netplot_session_info; only the
    data series are absent."""
    text = render_metrics([_sess(target_stream=None)])
    parsed = parse_prom(text)
    info = _one(
        parsed["samples"],
        "netplot_session_info",
        {
            "session_id": "s1",
            "target": "127.0.0.1",
            "name": "",
            "persistent": "false",
            "state": "running",
        },
    )
    assert info["value"] == 1.0
    # No data series for a sample-less session.
    assert _find(parsed["samples"], "netplot_target_rtt_current_ms") == []
    assert _find(parsed["samples"], "netplot_target_loss_pct") == []
    assert _find(parsed["samples"], "netplot_target_up") == []


def test_render_all_target_series_when_samples_present() -> None:
    ts = TargetMetrics(current_ms=12.5, avg_ms=11.0, loss_pct=50.0, up=1)
    text = render_metrics([_sess(target_stream=ts)])
    parsed = parse_prom(text)
    labels = {"session_id": "s1", "target": "127.0.0.1"}
    assert (
        _one(parsed["samples"], "netplot_target_rtt_current_ms", labels)["value"]
        == 12.5
    )
    assert _one(parsed["samples"], "netplot_target_rtt_avg_ms", labels)["value"] == 11.0
    assert _one(parsed["samples"], "netplot_target_loss_pct", labels)["value"] == 50.0
    assert _one(parsed["samples"], "netplot_target_up", labels)["value"] == 1.0


def test_render_loss_drops_current_and_avg_but_keeps_loss_and_up() -> None:
    """Last probe was a loss -> current_ms absent; up=0; loss_pct present."""
    ts = TargetMetrics(current_ms=None, avg_ms=None, loss_pct=100.0, up=0)
    text = render_metrics([_sess(target_stream=ts)])
    parsed = parse_prom(text)
    labels = {"session_id": "s1", "target": "127.0.0.1"}
    _none(parsed["samples"], "netplot_target_rtt_current_ms", labels)
    _none(parsed["samples"], "netplot_target_rtt_avg_ms", labels)
    assert _one(parsed["samples"], "netplot_target_loss_pct", labels)["value"] == 100.0
    assert _one(parsed["samples"], "netplot_target_up", labels)["value"] == 0.0


def test_render_hop_series_and_no_stale_on_hop_loss() -> None:
    hops = [
        HopMetrics(hop_index=1, current_ms=1.5, loss_pct=0.0),
        HopMetrics(hop_index=2, current_ms=None, loss_pct=100.0),  # last was loss
    ]
    text = render_metrics([_sess(hops=hops)])
    parsed = parse_prom(text)
    h1 = {"session_id": "s1", "target": "127.0.0.1", "hop": "1"}
    h2 = {"session_id": "s1", "target": "127.0.0.1", "hop": "2"}
    assert _one(parsed["samples"], "netplot_hop_rtt_current_ms", h1)["value"] == 1.5
    assert _one(parsed["samples"], "netplot_hop_loss_pct", h1)["value"] == 0.0
    # Hop 2 last probe was a loss -> current absent, loss present.
    _none(parsed["samples"], "netplot_hop_rtt_current_ms", h2)
    assert _one(parsed["samples"], "netplot_hop_loss_pct", h2)["value"] == 100.0


def test_render_label_escaping_backslash_quote_newline() -> None:
    """Backslash, double-quote, and newline in label values are escaped."""
    nasty = 'a\\b"c\nd'
    ts = TargetMetrics(current_ms=1.0, avg_ms=1.0, loss_pct=0.0, up=1)
    text = render_metrics(
        [_sess(session_id=nasty, target=nasty, name=nasty, target_stream=ts)]
    )
    parsed = parse_prom(text)
    # The parser unescapes, so the round-tripped labels must match the raw.
    info = _one(
        parsed["samples"],
        "netplot_session_info",
        {
            "session_id": nasty,
            "target": nasty,
            "name": nasty,
            "persistent": "false",
            "state": "running",
        },
    )
    assert info["value"] == 1.0
    # And the raw text must not contain a bare unescaped newline inside a label
    # value (the escaped form \n is two characters). A label value spans from
    # the opening quote to the closing quote on the same line, so a genuine
    # embedded newline would have broken the line-oriented format. Verify by
    # checking no raw sample line contains a literal newline char.
    for line in text.split("\n"):
        if line.startswith("netplot_session_info"):
            assert "\n" not in line  # no embedded newline survived unescaped


def test_render_name_label_empty_when_none() -> None:
    text = render_metrics([_sess(name=None)])
    parsed = parse_prom(text)
    info = _one(
        parsed["samples"],
        "netplot_session_info",
        {
            "session_id": "s1",
            "target": "127.0.0.1",
            "name": "",
            "persistent": "false",
            "state": "running",
        },
    )
    assert info["value"] == 1.0


def test_render_persistent_and_state_labels() -> None:
    text = render_metrics([_sess(persistent=True, state="paused")])
    parsed = parse_prom(text)
    _one(
        parsed["samples"],
        "netplot_session_info",
        {
            "session_id": "s1",
            "target": "127.0.0.1",
            "name": "",
            "persistent": "true",
            "state": "paused",
        },
    )


def test_render_help_and_type_for_every_family() -> None:
    """Every metric family has a HELP and a TYPE line."""
    text = render_metrics(
        [
            _sess(
                target_stream=TargetMetrics(1.0, 1.0, 0.0, 1),
                hops=[HopMetrics(1, 1.0, 0.0)],
            )
        ]
    )
    parsed = parse_prom(text)
    for name in (
        "netplot_session_info",
        "netplot_target_rtt_current_ms",
        "netplot_target_rtt_avg_ms",
        "netplot_target_loss_pct",
        "netplot_target_up",
        "netplot_hop_rtt_current_ms",
        "netplot_hop_loss_pct",
    ):
        assert name in parsed["families"], f"missing family {name}"
        fam = parsed["families"][name]
        assert fam["help"], f"{name} missing HELP text"
        assert fam["type"] == "gauge", (
            f"{name} TYPE should be gauge, got {fam['type']!r}"
        )
        assert fam["count"] >= 1, f"{name} should have at least one sample"


# ---------------------------------------------------- capture value math
#
# capture_metrics reads live deques; we inject samples directly into a
# SessionManager's sessions (via the test app) and assert the captured
# numbers. This pins the window math and the on-loop capture path.


def _make_app_with_session(tmp_path, target: str = "127.0.0.1"):
    """Create an app + client and register one UNSTARTED session directly.

    The capture tests below own their sample sets completely: every sample
    is injected into the session's deques by the test. Registering via
    ``SessionManager.create`` — without ``start`` — keeps that contract
    deterministic: no probe loop, traceroute discovery, or DNS task ever
    runs, so a fast loopback probe can never land a live sample between
    fixture setup and the capture read.

    ``tmp_path`` isolates the persistence store so a persistent-session test
    can never leak ``sessions.json`` into the next test's boot restore.
    """
    client = TestClient(create_app(data_dir=tmp_path), base_url="http://localhost")
    client.__enter__()
    session = client.app.state.session_manager.create(target, interval_s=60.0)
    return client, session.id, session


def test_capture_target_values_known_sample_set(tmp_path) -> None:
    """current = last RTT; avg = mean of successes; loss = losses/count; up=1."""
    client, _sid, session = _make_app_with_session(tmp_path)
    try:
        now = time.time()
        for rtt in (10.0, None, 20.0, None, 30.0):  # 5 samples, 3 successes
            session.samples.append(Sample(t=now, rtt_ms=rtt))
        data = capture_metrics(client.app.state.session_manager)
        assert len(data) == 1
        ts = data[0].target_stream
        assert ts is not None
        assert ts.current_ms == 30.0  # last sample succeeded
        assert ts.avg_ms == pytest.approx(20.0)  # (10+20+30)/3
        assert ts.loss_pct == pytest.approx(40.0)  # 2/5
        assert ts.up == 1
    finally:
        client.__exit__(None, None, None)


def test_capture_window_boundary_excludes_sample_61(tmp_path) -> None:
    """Exactly METRICS_WINDOW_SAMPLES samples count; the 61st (oldest) does not."""
    client, _sid, session = _make_app_with_session(tmp_path)
    try:
        now = time.time()
        # 60 samples of RTT=10.0, then push a 61st OLDER sample of RTT=1000.
        for _ in range(60):
            session.samples.append(Sample(t=now, rtt_ms=10.0))
        session.samples.appendleft(Sample(t=now - 999, rtt_ms=1000.0))
        data = capture_metrics(client.app.state.session_manager)
        ts = data[0].target_stream
        assert ts is not None
        # The 1000.0 sample is outside the 60-sample window -> avg stays 10.0.
        assert ts.avg_ms == pytest.approx(10.0)
        assert ts.loss_pct == pytest.approx(0.0)
        assert ts.current_ms == 10.0  # most recent is still 10.0
    finally:
        client.__exit__(None, None, None)


def test_capture_loss_as_last_probe_drops_current_series(tmp_path) -> None:
    """When the most recent probe is a loss, current_ms is None (no stale value)."""
    client, _sid, session = _make_app_with_session(tmp_path)
    try:
        now = time.time()
        session.samples.append(Sample(t=now, rtt_ms=5.0))
        session.samples.append(Sample(t=now + 1, rtt_ms=None))  # last = loss
        data = capture_metrics(client.app.state.session_manager)
        ts = data[0].target_stream
        assert ts is not None
        assert ts.current_ms is None  # no stale value
        assert ts.up == 0
        assert ts.avg_ms == pytest.approx(5.0)  # the one success
        assert ts.loss_pct == pytest.approx(50.0)
    finally:
        client.__exit__(None, None, None)


def test_capture_nonfinite_rtt_is_treated_as_loss(tmp_path) -> None:
    """Overflow/malformed RTTs must not leak NaN or Inf into exposition."""
    client, _sid, session = _make_app_with_session(tmp_path)
    try:
        now = time.time()
        session.samples.append(Sample(t=now, rtt_ms=inf))
        session.samples.append(Sample(t=now + 1, rtt_ms=nan))
        data = capture_metrics(client.app.state.session_manager)
        ts = data[0].target_stream
        assert ts is not None
        assert ts.current_ms is None
        assert ts.avg_ms is None
        assert ts.loss_pct == pytest.approx(100.0)
        assert ts.up == 0
    finally:
        client.__exit__(None, None, None)


def test_capture_finite_large_rtts_do_not_overflow_average(tmp_path) -> None:
    """Finite inputs must not overflow the average accumulator."""
    client, _sid, session = _make_app_with_session(tmp_path)
    try:
        now = time.time()
        session.samples.append(Sample(t=now, rtt_ms=1e308))
        session.samples.append(Sample(t=now + 1, rtt_ms=1e308))
        ts = capture_metrics(client.app.state.session_manager)[0].target_stream
        assert ts is not None
        assert ts.avg_ms == pytest.approx(1e308)
        assert ts.loss_pct == 0.0
    finally:
        client.__exit__(None, None, None)


@pytest.mark.parametrize(
    ("label", "values", "expected_successes", "expected_mean"),
    [
        ("zero", [0.0], 1, 0.0),
        ("negative", [-1.0], 1, -1.0),
        ("subnormal", [sys.float_info.min * sys.float_info.epsilon], 1, 5e-324),
        ("one_e_308", [1e-308], 1, 1e-308),
        ("realistic_mixed", [0.01, 10000.0, 3.2, 42.0], 4, 2511.3025),
        (
            "one_e_308_and_one_e_308_plus_300_orders",
            [1e-308, 1e-8, 1e292],
            3,
            1e292 / 3,
        ),
        ("one_e_308_to_one_e_308", [1e-308, 1e308], 2, 5e307),
        ("max_float", [sys.float_info.max], 1, sys.float_info.max),
        ("mixed_opposite_max", [sys.float_info.max, -sys.float_info.max], 2, 0.0),
        ("all_same_max", [sys.float_info.max] * 60, 60, sys.float_info.max),
        (
            "all_same_subnormal",
            [sys.float_info.min * sys.float_info.epsilon] * 60,
            60,
            5e-324,
        ),
        ("alternating_loss_finite", [1e308, None] * 30, 30, 1e308),
    ],
)
def test_capture_numeric_input_matrix(
    tmp_path,
    label: str,
    values: list[float | None],
    expected_successes: int,
    expected_mean: float,
) -> None:
    """Every finite numeric class remains safe through capture math.

    Negative RTTs are included because the pure capture contract accepts
    injected samples; the production ping parser cannot produce them because
    its RTT token grammar contains digits and an optional decimal only.
    """
    del label  # pytest's case id documents the matrix without affecting logic.
    client, _sid, session = _make_app_with_session(tmp_path)
    try:
        now = time.time()
        for offset, rtt in enumerate(values):
            session.samples.append(Sample(t=now + offset, rtt_ms=rtt))
        ts = capture_metrics(client.app.state.session_manager)[0].target_stream
        assert ts is not None
        assert ts.up in (0, 1)
        assert 0.0 <= ts.loss_pct <= 100.0
        if expected_successes:
            assert ts.avg_ms is not None
            assert isfinite(ts.avg_ms)
            # The matrix is also an accuracy oracle, not only a crash/finite
            # check.  The relative bound matches the documented tradeoff;
            # the small absolute bound covers the zero-mean edge case.
            assert ts.avg_ms == pytest.approx(expected_mean, rel=1e-9, abs=1e-12)
        else:
            assert ts.avg_ms is None
        if values[-1] is None:
            assert ts.current_ms is None
            assert ts.up == 0
        else:
            current = ts.current_ms
            assert current == values[-1]
            assert current is not None and isfinite(current)
            assert ts.up == 1
    finally:
        client.__exit__(None, None, None)


def test_capture_hop_values_and_shared_deque_target_hop(tmp_path) -> None:
    """Per-hop current/loss captured; final-hop-is-target shares the deque."""
    client, _sid, session = _make_app_with_session(tmp_path)
    try:
        now = time.time()
        h1 = Hop(index=1, ip="10.0.0.1", dns="r")
        h2 = Hop(index=2, ip="127.0.0.1", dns="localhost")
        h2.samples = session.samples  # shared -> final hop IS target
        session.hops = [h1, h2]
        session.route_state = "ready"
        session.target_ip = "127.0.0.1"
        # hop1: two successes; target/hop2 (shared): one success + one loss.
        h1.samples.append(Sample(t=now, rtt_ms=1.0))
        h1.samples.append(Sample(t=now, rtt_ms=3.0))
        session.samples.append(Sample(t=now, rtt_ms=8.0))
        session.samples.append(Sample(t=now + 1, rtt_ms=None))  # loss last
        data = capture_metrics(client.app.state.session_manager)
        d = data[0]
        # Hop 1: current=3.0, loss=0.
        h1m = next(h for h in d.hops if h.hop_index == 1)
        assert h1m.current_ms == 3.0
        assert h1m.loss_pct == pytest.approx(0.0)
        # Hop 2 (== target, shared deque): last was a loss -> current None.
        h2m = next(h for h in d.hops if h.hop_index == 2)
        assert h2m.current_ms is None
        assert h2m.loss_pct == pytest.approx(50.0)
        # Target stream mirrors the shared deque: current None, loss 50%.
        assert d.target_stream is not None
        assert d.target_stream.current_ms is None
        assert d.target_stream.loss_pct == pytest.approx(50.0)
    finally:
        client.__exit__(None, None, None)


def test_capture_skips_zero_sample_hop(tmp_path) -> None:
    """A discovered hop with no samples yet is omitted from hop series."""
    client, _sid, session = _make_app_with_session(tmp_path)
    try:
        session.hops = [Hop(index=1, ip="10.0.0.1", dns="r")]  # no samples
        session.route_state = "ready"
        data = capture_metrics(client.app.state.session_manager)
        assert data[0].hops == []
    finally:
        client.__exit__(None, None, None)


# ------------------------------------------------------- HTTP endpoint tests


def test_metrics_endpoint_status_and_content_type(tmp_path) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        r = client.get("/metrics")
        assert r.status_code == 200
        assert r.headers["content-type"] == "text/plain; version=0.0.4; charset=utf-8"
        # Body is parseable exposition text even with no sessions.
        parse_prom(r.text)


def test_metrics_endpoint_empty_has_no_samples(tmp_path) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        parsed = parse_prom(client.get("/metrics").text)
        assert parsed["samples"] == []


def test_metrics_endpoint_full_render_with_injected_samples(tmp_path) -> None:
    """End-to-end: inject samples, scrape /metrics, parse, assert values."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        # Data-only fixture: register the session directly (unstarted) so no
        # real probe/discovery task can add samples next to the injected set.
        session = client.app.state.session_manager.create(
            "127.0.0.1", interval_s=60.0
        )
        sid = session.id
        now = time.time()
        session.hops = [Hop(index=1, ip="10.0.0.1", dns="r")]
        session.route_state = "ready"
        session.hops[0].samples.append(Sample(t=now, rtt_ms=2.0))
        session.samples.append(Sample(t=now, rtt_ms=12.5))

        parsed = parse_prom(client.get("/metrics").text)
        labels = {"session_id": sid, "target": "127.0.0.1"}
        assert (
            _one(parsed["samples"], "netplot_target_rtt_current_ms", labels)["value"]
            == 12.5
        )
        assert _one(parsed["samples"], "netplot_target_up", labels)["value"] == 1.0
        assert (
            _one(parsed["samples"], "netplot_target_loss_pct", labels)["value"] == 0.0
        )
        hop_labels = {"session_id": sid, "target": "127.0.0.1", "hop": "1"}
        assert (
            _one(parsed["samples"], "netplot_hop_rtt_current_ms", hop_labels)["value"]
            == 2.0
        )


def test_metrics_endpoint_multi_session_no_cross_contamination(tmp_path) -> None:
    """Two sessions produce distinct series keyed by session_id/target."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        s1 = client.app.state.session_manager.create("127.0.0.1", interval_s=60.0)
        s2 = client.app.state.session_manager.create("127.0.0.2", interval_s=60.0)
        sid1, sid2 = s1.id, s2.id
        s1.samples.append(Sample(t=time.time(), rtt_ms=11.0))
        s2.samples.append(Sample(t=time.time(), rtt_ms=22.0))

        parsed = parse_prom(client.get("/metrics").text)
        v1 = _one(
            parsed["samples"],
            "netplot_target_rtt_current_ms",
            {"session_id": sid1, "target": "127.0.0.1"},
        )["value"]
        v2 = _one(
            parsed["samples"],
            "netplot_target_rtt_current_ms",
            {"session_id": sid2, "target": "127.0.0.2"},
        )["value"]
        assert v1 == 11.0
        assert v2 == 22.0
        # session_info has exactly two series.
        assert len(_find(parsed["samples"], "netplot_session_info")) == 2


def test_metrics_endpoint_deleted_session_gone_on_next_scrape(tmp_path) -> None:
    """After DELETE, the session's series vanish from /metrics."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        # Registered directly (unstarted): the DELETE lifecycle below is the
        # real endpoint/manager path, but no probe loop ever races the sample.
        session = client.app.state.session_manager.create(
            "127.0.0.1", interval_s=60.0
        )
        sid = session.id
        session.samples.append(Sample(t=time.time(), rtt_ms=5.0))
        before = parse_prom(client.get("/metrics").text)
        assert _find(before["samples"], "netplot_session_info", {"session_id": sid})

        client.delete(f"/api/traces/{sid}")

        after = parse_prom(client.get("/metrics").text)
        assert not _find(after["samples"], "netplot_session_info", {"session_id": sid})
        assert not _find(
            after["samples"], "netplot_target_rtt_current_ms", {"session_id": sid}
        )


def test_metrics_endpoint_paused_session_state_label(tmp_path) -> None:
    """A paused session still emits series with state="paused"."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        session = client.app.state.session_manager.create(
            "127.0.0.1", interval_s=60.0
        )
        sid = session.id
        # Real pause endpoint + manager transition (nothing to cancel — the
        # session was never started — but the state change under test is the
        # production path).
        client.post(f"/api/traces/{sid}/pause")
        session.samples.append(Sample(t=time.time(), rtt_ms=7.0))

        parsed = parse_prom(client.get("/metrics").text)
        info = _one(
            parsed["samples"],
            "netplot_session_info",
            {
                "session_id": sid,
                "target": "127.0.0.1",
                "name": "",
                "persistent": "false",
                "state": "paused",
            },
        )
        assert info["value"] == 1.0
        # Data series still present (last-known values, truthfully labeled).
        assert (
            _one(
                parsed["samples"],
                "netplot_target_rtt_current_ms",
                {"session_id": sid, "target": "127.0.0.1"},
            )["value"]
            == 7.0
        )


def test_metrics_endpoint_persistent_and_name_labels(tmp_path) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        session = client.app.state.session_manager.create(
            "127.0.0.1", interval_s=60.0, name="my-trace", persistent=True
        )
        sid = session.id
        client.post(f"/api/traces/{sid}/pause")
        session.samples.append(Sample(t=time.time(), rtt_ms=1.0))

        parsed = parse_prom(client.get("/metrics").text)
        _one(
            parsed["samples"],
            "netplot_session_info",
            {
                "session_id": sid,
                "target": "127.0.0.1",
                "name": "my-trace",
                "persistent": "true",
                "state": "paused",
            },
        )


def test_metrics_endpoint_label_escaping_via_unsafe_target(tmp_path) -> None:
    """An unsafe target string injected directly must be escaped in labels."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        session = client.app.state.session_manager.create(
            "127.0.0.1", interval_s=60.0
        )
        sid = session.id
        client.post(f"/api/traces/{sid}/pause")
        # Inject a hostile target + name with backslash, quote, newline.
        nasty = 'evil\\host"name\ninjected'
        session.target = nasty
        session.name = nasty
        session.samples.append(Sample(t=time.time(), rtt_ms=1.0))

        text = client.get("/metrics").text
        # Must parse cleanly — an unescaped quote/newline would break parsing.
        parsed = parse_prom(text)
        _one(
            parsed["samples"],
            "netplot_session_info",
            {
                "session_id": sid,
                "target": nasty,
                "name": nasty,
                "persistent": "false",
                "state": "paused",
            },
        )


def test_metrics_window_constant_is_60() -> None:
    """Pin the window constant; the spec says it is not configurable."""
    assert METRICS_WINDOW_SAMPLES == 60


# ------------------------------- capture vs the ordered-commit boundary
#
# The overlapping scheduler commits completed rounds in launch order, and a
# completed round parks uncommitted behind an earlier one. The regression
# reuses the ordered-commit scenario driver from test_rollup (held round 1
# with RTTs 30.0/loss/10.0 in rounds 1-3, later rounds blocked forever, then
# pause) and asserts the capture/render side of the same boundary: the
# window describes exactly the committed samples — before the held round
# commits, after the ordered commits, and after the pause that cancels the
# blocked rounds (which must not turn skipped/canceled work into losses).


def test_capture_describes_exactly_committed_rounds_across_overlap_and_pause() -> None:
    from test_rollup import _OVERLAP_TARGET, _drive_ordered_commit_scenario

    obs = asyncio.run(_drive_ordered_commit_scenario())
    session = obs["session"]

    # Before the held round commits, rounds that already finished probing
    # are NOT in the window: no pending/provisional values, no series.
    mid = obs["mid"]["metrics"]
    assert len(mid) == 1
    assert mid[0].target_stream is None
    assert mid[0].hops == []

    # After the ordered commits the window is exactly (30.0, None, 10.0):
    # current 10.0 (last COMMITTED round, not the newest reply), avg 20.0
    # over the two successes, loss 1/3, up 1.
    committed = obs["committed_metrics"]
    assert len(committed) == 1
    ts = committed[0].target_stream
    assert ts is not None
    assert ts.current_ms == 10.0
    assert ts.avg_ms == pytest.approx(20.0)
    assert ts.loss_pct == pytest.approx(100.0 / 3.0)
    assert ts.up == 1
    # Destination/final-hop alias: one hop series over the shared stream.
    assert [(h.hop_index, h.current_ms) for h in committed[0].hops] == [(1, 10.0)]
    assert committed[0].hops[0].loss_pct == pytest.approx(100.0 / 3.0)

    # Pausing with blocked later rounds outstanding flips the state label,
    # not the values: canceled uncommitted rounds are not losses.
    paused = obs["paused_metrics"]
    assert session.state == "paused"
    assert paused[0].state == "paused"
    assert paused[0].target_stream == ts
    assert paused[0].hops == committed[0].hops

    # The rendered exposition carries the committed values only.
    parsed = parse_prom(render_metrics(paused))
    base = {"session_id": session.id, "target": _OVERLAP_TARGET}
    assert (
        _one(parsed["samples"], "netplot_target_rtt_current_ms", base)["value"] == 10.0
    )
    assert _one(parsed["samples"], "netplot_target_rtt_avg_ms", base)["value"] == (
        pytest.approx(20.0)
    )
    assert _one(parsed["samples"], "netplot_target_loss_pct", base)["value"] == (
        pytest.approx(100.0 / 3.0)
    )
    assert _one(parsed["samples"], "netplot_target_up", base)["value"] == 1.0
    hop = {**base, "hop": "1"}
    assert _one(parsed["samples"], "netplot_hop_rtt_current_ms", hop)["value"] == 10.0
    assert _one(parsed["samples"], "netplot_hop_loss_pct", hop)["value"] == (
        pytest.approx(100.0 / 3.0)
    )
