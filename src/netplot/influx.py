"""InfluxDB 1.x interface-throughput query (backend-side, async HTTP).

The optional timeline overlay fetches cumulative interface counters
(``bytes_recv`` / ``bytes_sent`` on measurement ``net``, tagged by
``interface`` and optionally ``host``) from a user-configured InfluxDB 1.x
instance and returns per-bucket inbound/outbound Mbps. The server does the
querying so the browser never talks to the database directly.

Design points (bounded first version):

- **No credentials.** Only no-auth HTTP(S) sources are supported; the URL
  validator in :mod:`netplot.settings` rejects userinfo. Nothing is
  created, stored, or sent that resembles a secret.
- **Standard-library TLS trust.** HTTPS sources are verified with a
  ``ssl.create_default_context()`` context — ``SSL_CERT_FILE`` /
  ``SSL_CERT_DIR`` and the platform default roots, exactly what a plain
  urllib HTTPS fetch honors — while implicit environment proxies stay
  disabled (``trust_env=False``).
- **No arbitrary queries.** The InfluxQL statement is built entirely from
  a fixed template plus a validated identifier (charset-restricted at
  settings time) and escaped again here via :func:`_quote`.
- **No redirects.** Explicitly rejecting non-2xx responses ensures a misconfigured or
  malicious endpoint cannot bounce the request elsewhere (and never to a
  different scheme).
- **Counter-reset safe.** ``non_negative_derivative`` drops the negative
  delta produced by a counter reset instead of spiking.
- **Bytes per second at every step.** The derivative unit is fixed at
  ``1s`` — ``non_negative_derivative(last(field), 1s)`` — so a bucket's
  value is a per-second rate regardless of the aggregation step (a
  step-sized unit would inflate it 5/60/300×). ``last()`` reads the
  counter's end-of-bucket value (the proven telegraf/Grafana form), not
  a mean.
- **Series preserved.** ``GROUP BY time()`` alone would silently merge
  multiple hosts' counters into one series; grouping by the tag keys
  (``interface``, ``host``) keeps them separate, so an ambiguous
  host-less config surfaces as an error (multiple series → set a host
  tag) instead of merged garbage.
- **Missing data stays missing.** ``fill(none)`` omits empty buckets so
  gaps are never rendered as zero throughput.
- **Bounded.** The request has a cancellable total deadline,
  the time range is finite and span-capped (24 h, matching the largest
  timeline range), the bucket ladder bounds the point count (≤ 288
  points at the 24 h / 300 s step), and the response body is capped at
  :data:`MAX_RESPONSE_BYTES` (1 MiB).
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import ssl

import httpx

__all__ = [
    "INFLUX_TIMEOUT_S",
    "InfluxError",
    "build_query",
    "fetch_query",
    "parse_points",
    "query_throughput",
    "step_for_span",
]

log = logging.getLogger("netplot.influx")

#: Total request deadline in seconds, including streaming the body.
INFLUX_TIMEOUT_S = 5.0

#: Largest accepted response body (1 MiB). A real /query answer for the
#: bounded bucket ladder is a few KiB; anything larger is a misbehaving
#: endpoint and is refused rather than buffered.
MAX_RESPONSE_BYTES = 1_048_576

#: Largest accepted query span (24 h — the largest timeline range).
MAX_SPAN_S = 86_400

#: Measurement / field names (fixed template; not user-configurable).
_MEASUREMENT = "net"
_FIELD_RECV = "bytes_recv"
_FIELD_SENT = "bytes_sent"

_BYTES_PER_S_TO_MBPS = 8.0 / 1_000_000.0


class InfluxError(Exception):
    """Raised for any unreachable/invalid InfluxDB interaction.

    The message is safe to surface to the dashboard (it never includes
    the query body or credentials — none exist).
    """


def step_for_span(span_s: float) -> int:
    """Bucket step (seconds) for a query span.

    Short zooms use the source's native 5 s cadence; longer windows
    aggregate so the point count stays small (24 h → 288 points at 300 s).
    """
    if span_s <= 1_800:
        return 5
    if span_s <= 21_600:
        return 60
    return 300


def _quote(value: str) -> str:
    """Escape ``value`` as an InfluxQL string literal (defense in depth).

    Values are charset-validated at settings time; this backslash/quote
    escape makes a hand-edited settings file unable to break out of the
    literal even if that validation were bypassed.
    """
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def build_query(
    interface: str, host_tag: str, step_s: int, start_s: float, end_s: float
) -> str:
    """Build the InfluxQL SELECT for inbound/outbound derivative buckets.

    Pure function of its arguments so it is unit-testable without a
    server. ``GROUP BY time()`` buckets are wall-clock aligned by
    InfluxDB; the derivative unit is fixed at ``1s`` so every bucket's
    value is bytes per SECOND regardless of the bucket step. Grouping
    by the tag keys (not time alone) preserves per-series output: a
    host-less config over multi-host data returns multiple series
    (rejected by :func:`parse_points`) instead of silently merged
    counters.
    """
    clauses = [f"interface = {_quote(interface)}"]
    if host_tag:
        clauses.append(f"host = {_quote(host_tag)}")
    clauses.append(f"time >= {int(start_s)}s")
    clauses.append(f"time <= {int(end_s)}s")
    fields = ", ".join(
        f'non_negative_derivative(last("{field}"), 1s) AS {alias}'
        for field, alias in ((_FIELD_RECV, "b_in"), (_FIELD_SENT, "b_out"))
    )
    return (
        f'SELECT {fields} FROM "{_MEASUREMENT}" '
        f"WHERE {' AND '.join(clauses)} "
        f'GROUP BY time({step_s}s), "interface", "host" fill(none)'
    )


def parse_points(payload: object) -> list[dict]:
    """Normalize an InfluxDB query response into ``[{t, in_mbps, out_mbps}]``.

    - Exactly one series is expected (the ``interface`` — and optional
      ``host`` — tag filters should pin one). More than one series means
      the config is ambiguous (e.g. multiple hosts with no ``host_tag``):
      :class:`InfluxError`, never silently-merged data.
    - ``null`` derivative values (bucket gaps, counter resets, first
      bucket) are preserved as ``None`` — missing data is never zeroed.
    - Byte-per-second derivatives convert to Mbps (× 8 / 1e6); times are
      nanosecond epochs → epoch seconds.
    - Malformed payloads (non-list ``series``/``values``, non-object
      series entries, non-finite timestamps, negative bandwidth — which
      a ``non_negative_derivative`` can never produce) raise
      :class:`InfluxError`, never an uncaught exception.
    """
    if not isinstance(payload, dict):
        raise InfluxError("unexpected influx response (not an object)")
    if "error" in payload:
        raise InfluxError(f"influx query error: {payload['error']}")
    results = payload.get("results")
    if not isinstance(results, list) or len(results) != 1:
        raise InfluxError("unexpected influx response (results shape)")
    result = results[0]
    if not isinstance(result, dict):
        raise InfluxError("unexpected influx response (result shape)")
    if "error" in result:
        raise InfluxError(f"influx query error: {result['error']}")
    series = result.get("series")
    if series is None:
        return []  # no data in range — surfaced as "no data", not an error
    if not isinstance(series, list):
        raise InfluxError("unexpected influx response (series shape)")
    if len(series) == 0:
        return []
    if len(series) > 1:
        raise InfluxError(
            "influx returned multiple series; set a host tag to disambiguate"
        )
    one = series[0]
    if not isinstance(one, dict):
        raise InfluxError("unexpected influx response (series entry shape)")
    columns = one.get("columns")
    if not isinstance(columns, list):
        raise InfluxError(f"unexpected influx columns: {columns!r}")
    try:
        t_i = columns.index("time")
        in_i = columns.index("b_in")
        out_i = columns.index("b_out")
    except ValueError as exc:
        raise InfluxError(f"unexpected influx columns: {columns!r}") from exc

    def _mbps(value: object) -> float | None:
        if value is None:
            return None
        try:
            f = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(f):
            return None
        if f < 0:
            # non_negative_derivative can never produce this; a negative
            # rate means the payload is not what we asked for.
            raise InfluxError("influx returned negative bandwidth")
        return f * _BYTES_PER_S_TO_MBPS

    values = one.get("values")
    if values is None:
        values = []
    elif not isinstance(values, list):
        raise InfluxError("unexpected influx response (values shape)")
    points: list[dict] = []
    for row in values:
        if not isinstance(row, list) or len(row) <= max(t_i, in_i, out_i):
            continue
        t_ns = row[t_i]
        if isinstance(t_ns, bool) or not isinstance(t_ns, (int, float)):
            continue
        t_s = t_ns / 1e9
        if not math.isfinite(t_s):
            raise InfluxError("influx returned a non-finite timestamp")
        points.append(
            {
                "t": t_s,
                "in_mbps": _mbps(row[in_i]),
                "out_mbps": _mbps(row[out_i]),
            }
        )
    return points


async def fetch_query(
    url: str,
    database: str,
    query: str,
    timeout_s: float = INFLUX_TIMEOUT_S,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> dict:
    """Fetch a size-bounded response with a cancellable total time budget."""
    endpoint = url.rstrip("/") + "/query"
    body = bytearray()
    try:
        async with asyncio.timeout(timeout_s):
            async with httpx.AsyncClient(
                timeout=timeout_s,
                follow_redirects=False,
                # Preserve standard CA overrides without enabling environment proxies.
                verify=ssl.create_default_context(),
                trust_env=False,
                transport=transport,
            ) as client:
                async with client.stream(
                    "GET",
                    endpoint,
                    params={"db": database, "q": query, "epoch": "ns"},
                    headers={
                        "Accept": "application/json",
                        "Accept-Encoding": "identity",
                    },
                ) as response:
                    if not 200 <= response.status_code < 300:
                        raise InfluxError(f"influx http error {response.status_code}")
                    # Never decompress attacker-controlled bodies before enforcing
                    # the cap. Influx JSON needs no content coding.
                    if (
                        response.headers.get("content-encoding", "identity").lower()
                        != "identity"
                    ):
                        raise InfluxError(
                            "influx returned unsupported content encoding"
                        )
                    async for chunk in response.aiter_raw():
                        if len(body) + len(chunk) > MAX_RESPONSE_BYTES:
                            raise InfluxError("influx response too large")
                        body.extend(chunk)
    except (httpx.HTTPError, TimeoutError, OSError) as exc:
        raise InfluxError("influx unreachable or request deadline exceeded") from exc
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InfluxError("influx returned invalid JSON") from exc
    if not isinstance(parsed, dict):
        raise InfluxError("influx returned an unexpected payload")
    return parsed


async def query_throughput(influx_cfg: dict, start_s: float, end_s: float) -> dict:
    """End-to-end overlay query: validate range, fetch, normalize.

    Returns ``{"step_s": <int>, "points": [{t, in_mbps, out_mbps}]}``.
    Raises :class:`InfluxError` on any failure. Async; cancellation closes the upstream connection.
    """
    if not (math.isfinite(start_s) and math.isfinite(end_s)) or end_s < start_s:
        raise InfluxError("invalid time range")
    if (end_s - start_s) > MAX_SPAN_S + 1:
        raise InfluxError(f"time range too large (max {MAX_SPAN_S}s)")
    step = step_for_span(end_s - start_s)
    query = build_query(
        influx_cfg["interface"], influx_cfg.get("host_tag") or "", step, start_s, end_s
    )
    payload = await fetch_query(influx_cfg["url"], influx_cfg["database"], query)
    return {"step_s": step, "points": parse_points(payload)}
