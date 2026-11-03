"""Server-side settings: internal IP ranges and the InfluxDB overlay config.

Two independent dashboard features are configured here:

1. **Internal IP ranges** — CIDR lists (default RFC1918 + IPv6 ULA
   (RFC4193 ``fc00::/7``)) used to classify hop/target IPs as "internal".
   The snapshot API annotates hops with an ``internal`` flag computed
   server-side with the stdlib :mod:`ipaddress` module so the client
   never needs a JS CIDR parser. The dashboard's screenshot toggle then
   masks only the DNS names of internal hops (display-only; API/CSV
   payloads are untouched).

2. **InfluxDB 1.x throughput overlay** — generic connection settings
   (URL, database, interface tag, optional host tag). Fully blank by
   default — nothing is preset or inferred; a configured URL requires an
   explicit database and interface. No credentials are created or
   stored. Only no-auth HTTP(S) sources are supported in this bounded
   first version.

Storage contract
----------------

- One JSON file at ``<data_dir>/settings.json`` (sibling of the sessions
  state file; same ``NETPLOT_DATA_DIR`` resolution).
- Schema versioned (``SETTINGS_SCHEMA_VERSION``).
- Writes are atomic (tmp file in the same dir + :func:`os.replace`), the
  same convention as the sessions store.
- Missing file → defaults. Present-but-unusable file (corrupt JSON,
  unknown version, invalid values) → defaults with a logged warning; the
  next successful PUT overwrites it. No quarantine/degraded mode: the
  file is operator-editable convenience config, not durable state.
"""

from __future__ import annotations

import ipaddress
import json
import logging
import os
import re
import tempfile
from collections.abc import Callable, Iterable
from pathlib import Path
from urllib.parse import urlsplit

from .persistence import _fsync_dir, _fsync_new_parents

__all__ = [
    "DEFAULT_INTERNAL_CIDRS",
    "MAX_INTERNAL_CIDRS",
    "SETTINGS_FILENAME",
    "SETTINGS_SCHEMA_VERSION",
    "default_settings",
    "load_settings",
    "make_classifier",
    "normalize_cidrs",
    "normalize_influx",
    "parse_networks",
    "save_settings",
]

log = logging.getLogger("netplot.settings")

SETTINGS_FILENAME = "settings.json"
SETTINGS_SCHEMA_VERSION = 1

# Bound boot-time work on a corrupt/hand-edited file (the real file is a
# few hundred bytes).
MAX_SETTINGS_FILE_BYTES = 65_536

# Standard private ranges: RFC1918 (IPv4) + the full IPv6 unique-local
# block (RFC4193 fc00::/7 — covers both fc00:: and fd00:: generated
# addresses). Other ranges (e.g. CGNAT 100.64/10) are user-editable via
# the settings API.
DEFAULT_INTERNAL_CIDRS: tuple[str, ...] = (
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "fc00::/7",
)

# Cap the CIDR list so a runaway PUT cannot bloat the file/classifier.
MAX_INTERNAL_CIDRS = 64

# Identifier charset shared by database / interface / host-tag values.
# Deliberately excludes quotes, backslashes, whitespace, and control
# characters so a value can never break out of an InfluxQL string literal
# or the URL query (values are additionally escaped at query-build time).
_IDENT_RE = re.compile(r"[A-Za-z0-9._:@\-]+")

_MAX_IDENT_LEN = 200
_MAX_URL_LEN = 2000


def default_settings() -> dict:
    """Fresh settings dict with the documented defaults."""
    return {
        "version": SETTINGS_SCHEMA_VERSION,
        "internal_cidrs": list(DEFAULT_INTERNAL_CIDRS),
        "influx": {
            "url": "",
            "database": "",
            "interface": "",
            "host_tag": "",
        },
    }


# ------------------------------------------------------------- CIDR handling


def normalize_cidrs(values: Iterable[object]) -> list[str]:
    """Validate + normalize a CIDR list; raises ``ValueError`` on bad input.

    Each entry must be a string parseable by :func:`ipaddress.ip_network`
    (host bits allowed and normalized away, e.g. ``10.0.1.5/8`` →
    ``10.0.0.0/8``). Duplicates collapse (first occurrence wins, order
    preserved); the result is capped at :data:`MAX_INTERNAL_CIDRS` entries.
    An empty list is valid (mask nothing).
    """
    if not isinstance(values, (list, tuple)):
        raise ValueError("cidrs must be a list of CIDR strings")
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            raise ValueError(f"invalid CIDR (not a string): {value!r}")
        try:
            # strict=False accepts (and normalizes) host bits set.
            network = ipaddress.ip_network(value.strip(), strict=False)
        except ValueError as exc:
            raise ValueError(f"invalid CIDR {value!r}: {exc}") from exc
        normalized = str(network)
        if normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
        if len(out) > MAX_INTERNAL_CIDRS:
            raise ValueError(f"too many CIDRs (max {MAX_INTERNAL_CIDRS})")
    return out


def parse_networks(cidrs: Iterable[str]) -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
    """Parse already-normalized CIDR strings into network objects."""
    return [ipaddress.ip_network(c) for c in cidrs]


def make_classifier(cidrs: Iterable[str]) -> Callable[[str | None], bool]:
    """Build an ``ip -> is_internal`` classifier over the given CIDR list.

    Unparseable IPs (``None``, hostnames, garbage) classify as ``False``.
    Results are memoized per closure; the IP population is bounded by the
    registry (``MAX_SESSIONS`` × hops), so the cache needs no eviction.
    """
    networks = parse_networks(cidrs)
    cache: dict[str, bool] = {}

    def classify(ip: str | None) -> bool:
        if not ip:
            return False
        known = cache.get(ip)
        if known is None:
            try:
                addr = ipaddress.ip_address(ip)
            except ValueError:
                known = False
            else:
                known = any(addr in net for net in networks)
            cache[ip] = known
        return known

    return classify


# --------------------------------------------------------- influx validation


def _validate_ident(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    cleaned = value.strip()
    if len(cleaned) > _MAX_IDENT_LEN:
        raise ValueError(f"{field} is too long (max {_MAX_IDENT_LEN} chars)")
    if cleaned and _IDENT_RE.fullmatch(cleaned) is None:
        raise ValueError(
            f"{field} may only contain letters, digits, and . _ : @ - (got {cleaned!r})"
        )
    return cleaned


def validate_influx_url(value: object) -> str:
    """Validate an InfluxDB base URL; ``""`` means "unconfigured".

    Only ``http``/``https`` with a host is accepted; userinfo (embedded
    credentials), query strings, and fragments are rejected (a retained
    query could alter the ``/query`` assembly). Ports are allowed.
    """
    if not isinstance(value, str):
        raise ValueError("url must be a string")
    cleaned = value.strip()
    if not cleaned:
        return ""
    if len(cleaned) > _MAX_URL_LEN:
        raise ValueError("url is too long")
    try:
        parsed = urlsplit(cleaned)
    except ValueError as exc:
        raise ValueError(f"invalid url: {exc}") from exc
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"url scheme must be http or https (got {parsed.scheme!r})")
    if not parsed.hostname:
        raise ValueError("url must include a host")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("url must not contain credentials (no-auth sources only)")
    if parsed.query:
        raise ValueError("url must not include a query string")
    if parsed.fragment:
        raise ValueError("url must not include a fragment")
    try:
        parsed.port  # noqa: B018 - property access validates the port
    except ValueError as exc:
        raise ValueError(f"invalid url port: {exc}") from exc
    return cleaned


def normalize_influx(influx: object) -> dict:
    """Validate + normalize the influx settings dict; raises ``ValueError``.

    ``url`` empty ⇒ the overlay is unconfigured (everything else may be
    empty too — no values are preset or inferred). When a URL is set,
    BOTH ``database`` and ``interface`` are required — the overlay
    queries one specific database and interface tag.
    """
    if not isinstance(influx, dict):
        raise ValueError("influx must be an object")
    unknown = set(influx) - {"url", "database", "interface", "host_tag"}
    if unknown:
        raise ValueError(f"unknown influx field(s): {sorted(unknown)}")
    url = validate_influx_url(influx.get("url", ""))
    database = _validate_ident(influx.get("database", ""), "database")
    interface = _validate_ident(influx.get("interface", ""), "interface")
    host_tag = _validate_ident(influx.get("host_tag", ""), "host_tag")
    if url and not database:
        raise ValueError("database is required when an influx url is configured")
    if url and not interface:
        raise ValueError("interface is required when an influx url is configured")
    return {"url": url, "database": database, "interface": interface, "host_tag": host_tag}


# ------------------------------------------------------------------ file I/O


def load_settings(data_dir: Path | str) -> dict:
    """Read ``<data_dir>/settings.json``; never raises.

    Missing file → defaults. Present-but-unusable (unreadable, oversized,
    corrupt JSON, unknown version, invalid values) → defaults with a
    logged warning. The next successful PUT overwrites the bad file.

    The size limit is enforced *before* any read (stat gate), so an
    oversized operator-local file (or one swapped in for a special file)
    is never slurped into memory; the read itself is capped at one byte
    past the limit and re-checked, so a file that grows between the
    stat and the read is still rejected without an unbounded read.
    """
    path = Path(data_dir) / SETTINGS_FILENAME
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return default_settings()
    except OSError as exc:
        log.warning("netplot: settings file unreadable (%s): %s; using defaults", path, exc)
        return default_settings()
    if size > MAX_SETTINGS_FILE_BYTES:
        log.warning("netplot: settings file oversized (%d bytes); using defaults", size)
        return default_settings()
    try:
        with path.open("rb") as fh:
            # Capped at limit + 1 byte: just enough to re-check the limit
            # below, never enough for an unbounded read of a file that
            # grew (or was swapped) since the stat above.
            raw = fh.read(MAX_SETTINGS_FILE_BYTES + 1)
    except OSError as exc:
        log.warning("netplot: settings file unreadable (%s): %s; using defaults", path, exc)
        return default_settings()
    if len(raw) > MAX_SETTINGS_FILE_BYTES:
        log.warning("netplot: settings file oversized (%d bytes); using defaults", len(raw))
        return default_settings()
    try:
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("root is not an object")
        if payload.get("version") != SETTINGS_SCHEMA_VERSION:
            raise ValueError(f"unknown schema version {payload.get('version')!r}")
        cidrs = normalize_cidrs(payload.get("internal_cidrs", []))
        influx = normalize_influx(payload.get("influx", {}))
    except (
        # RecursionError: deeply nested JSON (tens of thousands of
        # brackets fit in the 64 KiB cap) can exhaust the C parser's
        # recursion budget, whose exact depth varies by interpreter
        # build and version — so it is caught explicitly rather than
        # assumed to arrive as a ValueError. Either way the contract
        # is the same: an unusable file means defaults, never an
        # exception out of ``load_settings`` (it runs inside
        # ``create_app`` at boot, where an escape would crash the
        # process).
        UnicodeDecodeError,
        json.JSONDecodeError,
        ValueError,
        RecursionError,
    ) as exc:
        log.warning("netplot: settings file unusable (%s): %s; using defaults", path, exc)
        return default_settings()
    return {
        "version": SETTINGS_SCHEMA_VERSION,
        "internal_cidrs": cidrs,
        "influx": influx,
    }


def save_settings(data_dir: Path | str, settings: dict) -> None:
    """Atomically write ``settings`` to ``<data_dir>/settings.json``.

    Same convention as the sessions store: tmp file in the target dir,
    flush + fsync, :func:`os.replace`, then a directory fsync so the
    rename survives power loss. I/O errors propagate to the caller.
    """
    directory = Path(data_dir)
    text = json.dumps(settings, ensure_ascii=False)
    _fsync_new_parents(directory)
    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=SETTINGS_FILENAME + ".", suffix=".tmp", dir=str(directory)
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, directory / SETTINGS_FILENAME)
        _fsync_dir(directory)
    except BaseException:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise
