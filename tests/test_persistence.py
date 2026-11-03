"""Tests for persistent (background) session storage and restore.

Covers the Slice B contract:

- ``PersistentStore`` round-trip, atomic write, schema-version gate,
  corrupt/missing/oversized-file handling.
- ``SessionManager`` persistence hooks: create (persistent only),
  pause/resume desired-state recording, delete removal, set_persistent
  / rename.
- Restore semantics: running/paused desired state, MAX_SESSIONS cap,
  id-order preservation, paused_at set to a real epoch on paused
  restore.
- The persist endpoint and ``POST /api/traces`` ``name`` / ``persistent``
  fields via :func:`create_app`.
- Name sanitization at the API boundary.
- Serialization fields surfaced in summary / snapshot / list / export.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from netplot.persistence import (
    MAX_NAME_LEN,
    MAX_STATE_FILE_BYTES,
    STATE_SCHEMA_VERSION,
    PersistDegraded,
    PersistentStore,
    _validate_session_entry,
    sanitize_name,
)
from netplot.session import (
    MAX_SESSIONS,
    SessionManager,
    TraceSession,
)
from netplot.web import create_app

# ---------------------------------------------------------- helper fakes


async def _failing_discover(_target: str, _timeout_s: float = 90.0):
    """A discover_fn that fails immediately so tests do not spawn traceroute."""
    from netplot.engine import RouteDiscoveryError

    raise RouteDiscoveryError("disabled in unit test")


async def _fast_resolve_name(_ip: str) -> None:
    return None


async def _instant_probe(_host: str, _timeout_s: float) -> float | None:
    """Probe fn that returns a constant latency without spawning ping."""
    return 1.0


def _manager(store: PersistentStore) -> SessionManager:
    """Build a SessionManager wired to ``store`` with fast no-op engine fakes."""
    return SessionManager(
        probe_fn=_instant_probe,
        discover_fn=_failing_discover,
        resolve_name_fn=_fast_resolve_name,
        persistence=store,
    )


# ============================================================== PersistentStore
#
# Pure I/O tests: the store knows nothing about TraceSession and reads /
# writes validated dicts. These exercise the file shape, atomicity, and
# every boot-safety branch.


def test_read_returns_empty_when_file_missing(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    result = store.read()
    assert result.status == "missing"
    assert result.entries == []


def test_write_then_read_round_trips(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    payload = [
        {
            "id": "abc",
            "target": "example.com",
            "interval_s": 2.5,
            "name": "main",
            "persistent": True,
            "desired_state": "running",
            "created_at": 1234.5,
        },
        {
            "id": "def",
            "target": "1.1.1.1",
            "interval_s": 5.0,
            "name": "",
            "persistent": True,
            "desired_state": "paused",
            "created_at": 9999.0,
        },
    ]
    store.write(payload)
    # Path resolves under the data dir; the file appears after the first write.
    assert store.path.exists()
    # Round-trip preserves order, fields, and types.
    result = store.read()
    assert result.status == "valid"
    assert result.entries == payload


def test_write_creates_data_dir_when_missing(tmp_path: Path) -> None:
    nested = tmp_path / "a" / "b" / "c"
    store = PersistentStore(nested)
    assert not nested.exists()
    store.write(
        [
            {
                "id": "x",
                "target": "t.example",
                "interval_s": 1.0,
                "name": "",
                "persistent": True,
                "desired_state": "running",
                "created_at": 1.0,
            }
        ]
    )
    assert nested.is_dir()
    assert store.path.exists()


def test_write_is_atomic_no_tmp_left_behind(tmp_path: Path) -> None:
    """Atomic swap: after write, no sessions.json.tmp* files remain in the dir.

    A crash mid-write would leave a tmp file; os.replace completes atomically,
    so on a successful write the only artifact is the final state file.
    """
    store = PersistentStore(tmp_path)
    store.write(
        [
            {
                "id": "x",
                "target": "t.example",
                "interval_s": 1.0,
                "name": "",
                "persistent": True,
                "desired_state": "running",
                "created_at": 1.0,
            }
        ]
    )
    leftovers = [p.name for p in tmp_path.iterdir() if p.name != "sessions.json"]
    assert leftovers == [], f"unexpected leftover files: {leftovers}"


def test_write_overwrites_previous_file(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    store.write(
        [
            {
                "id": "first",
                "target": "a.example",
                "interval_s": 1.0,
                "name": "",
                "persistent": True,
                "desired_state": "running",
                "created_at": 1.0,
            }
        ]
    )
    store.write(
        [
            {
                "id": "second",
                "target": "b.example",
                "interval_s": 2.0,
                "name": "",
                "persistent": True,
                "desired_state": "running",
                "created_at": 2.0,
            }
        ]
    )
    entries = store.read().entries
    assert len(entries) == 1
    assert entries[0]["id"] == "second"


def test_remove_deletes_file(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    store.write(
        [
            {
                "id": "x",
                "target": "t.example",
                "interval_s": 1.0,
                "name": "",
                "persistent": True,
                "desired_state": "running",
                "created_at": 1.0,
            }
        ]
    )
    assert store.path.exists()
    store.remove()
    assert not store.path.exists()
    # remove is idempotent: a missing file is a no-op.
    store.remove()


def test_read_corrupt_json_degrades_and_quarantines(tmp_path: Path, caplog) -> None:
    """Corrupt JSON → degraded mode, original bytes quarantined, store read-only."""
    store = PersistentStore(tmp_path)
    store.path.write_text("{not valid json", encoding="utf-8")
    with caplog.at_level("WARNING"):
        result = store.read()
    assert result.status == "degraded"
    assert result.entries == []
    assert "corrupt" in (result.reason or "")
    # Original bytes quarantined (renamed aside) so an operator can recover.
    assert result.quarantine_path is not None
    assert result.quarantine_path.exists()
    assert result.quarantine_path.read_bytes() == b"{not valid json"
    # The original state file is gone (renamed).
    assert not store.path.exists()
    # Store is now read-only: writes are refused.
    assert store.is_degraded()
    with pytest.raises(PersistDegraded):
        store.write([])
    assert any("corrupt" in rec.message for rec in caplog.records)


def test_read_unknown_schema_version_degrades(tmp_path: Path, caplog) -> None:
    store = PersistentStore(tmp_path)
    store.path.write_text(json.dumps({"version": 99, "sessions": []}), encoding="utf-8")
    with caplog.at_level("WARNING"):
        result = store.read()
    assert result.status == "degraded"
    assert "unknown schema version" in (result.reason or "")
    assert result.quarantine_path is not None
    assert store.is_degraded()
    assert any("unknown schema version" in rec.message for rec in caplog.records)


@pytest.mark.parametrize("version", [True, 1.0])
def test_read_rejects_non_integer_version_representation(
    tmp_path: Path, version
) -> None:
    store = PersistentStore(tmp_path)
    store.path.write_text(
        json.dumps({"version": version, "sessions": []}), encoding="utf-8"
    )

    result = store.read()

    assert result.status == "degraded"
    assert "unknown schema version" in (result.reason or "")
    assert result.quarantine_path is not None


def test_read_rejects_duplicate_json_object_fields(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    store.path.write_text(
        '{"version": 1, "version": 1, "sessions": []}', encoding="utf-8"
    )

    result = store.read()

    assert result.status == "degraded"
    assert "corrupt state file" in (result.reason or "")
    assert result.quarantine_path is not None


def test_read_non_object_root_degrades(tmp_path: Path, caplog) -> None:
    store = PersistentStore(tmp_path)
    store.path.write_text(json.dumps(["not", "an", "object"]), encoding="utf-8")
    with caplog.at_level("WARNING"):
        result = store.read()
    assert result.status == "degraded"
    assert "not an object" in (result.reason or "")
    assert result.quarantine_path is not None
    assert store.is_degraded()
    assert any("root is not an object" in rec.message for rec in caplog.records)


def test_read_non_list_sessions_degrades(tmp_path: Path, caplog) -> None:
    store = PersistentStore(tmp_path)
    store.path.write_text(
        json.dumps({"version": STATE_SCHEMA_VERSION, "sessions": "nope"}),
        encoding="utf-8",
    )
    with caplog.at_level("WARNING"):
        result = store.read()
    assert result.status == "degraded"
    assert "'sessions' is not a list" in (result.reason or "")
    assert store.is_degraded()
    assert any("'sessions' is not a list" in rec.message for rec in caplog.records)


def test_read_oversized_file_degrades(tmp_path: Path, caplog) -> None:
    store = PersistentStore(tmp_path)
    store.path.write_bytes(b"x" * (MAX_STATE_FILE_BYTES + 1))
    with caplog.at_level("WARNING"):
        result = store.read()
    assert result.status == "degraded"
    assert "oversized" in (result.reason or "")
    assert result.quarantine_path is not None
    # The oversized bytes were quarantined, not parsed.
    assert result.quarantine_path.stat().st_size == MAX_STATE_FILE_BYTES + 1
    assert store.is_degraded()
    assert any("oversized" in rec.message for rec in caplog.records)


def test_read_drops_invalid_entries_degrades_but_restores_valid(
    tmp_path: Path, caplog
) -> None:
    """Invalid session entries are dropped; valid siblings survive but the
    store enters degraded mode so the next rewrite cannot destroy the
    rejected-but-real definitions the operator has not yet reviewed.

    Each field is checked: missing id, non-str target, bad interval, bad
    desired_state, bad created_at all cause the entry to be skipped
    rather than crashing restore. Every skip is logged with a reason.
    """
    store = PersistentStore(tmp_path)
    raw = {
        "version": STATE_SCHEMA_VERSION,
        "sessions": [
            # valid
            {
                "id": "ok",
                "target": "ok.example",
                "interval_s": 1.0,
                "name": "",
                "persistent": True,
                "desired_state": "running",
                "created_at": 1.0,
            },
            # missing id
            {
                "target": "x.example",
                "interval_s": 1.0,
                "desired_state": "running",
                "created_at": 1.0,
            },
            # empty target
            {
                "id": "x",
                "target": "",
                "interval_s": 1.0,
                "desired_state": "running",
                "created_at": 1.0,
            },
            # bad interval
            {
                "id": "x",
                "target": "x.example",
                "interval_s": "fast",
                "desired_state": "running",
                "created_at": 1.0,
            },
            # bad desired_state
            {
                "id": "x",
                "target": "x.example",
                "interval_s": 1.0,
                "desired_state": "stopped",
                "created_at": 1.0,
            },
            # bad created_at
            {
                "id": "x",
                "target": "x.example",
                "interval_s": 1.0,
                "desired_state": "running",
                "created_at": "now",
            },
            # non-object entry
            "not-a-dict",
        ],
    }
    store.path.write_text(json.dumps(raw), encoding="utf-8")
    with caplog.at_level("WARNING"):
        result = store.read()
    # Degraded because entries were dropped; valid subset restored.
    assert result.status == "degraded"
    assert len(result.entries) == 1
    assert result.entries[0]["id"] == "ok"
    # Every dropped entry is logged with a reason.
    assert len(result.skipped) == 6
    assert any("missing or non-string id" in s for s in result.skipped)
    assert any("missing or empty target" in s for s in result.skipped)
    assert any("invalid interval_s" in s for s in result.skipped)
    assert any("invalid desired_state" in s for s in result.skipped)
    assert any("invalid created_at" in s for s in result.skipped)
    assert any("not a JSON object" in s for s in result.skipped)
    # Store is degraded → read-only.
    assert store.is_degraded()
    assert result.quarantine_path is not None


@pytest.mark.parametrize("field", ["interval_s", "created_at"])
def test_read_huge_numeric_entry_degrades_without_validator_crash(
    tmp_path: Path, field: str
) -> None:
    """JSON integers too large for float conversion are rejected safely."""
    store = PersistentStore(tmp_path)
    entry = {
        "id": "huge",
        "target": "x.example",
        "interval_s": 1.0,
        "name": "",
        "persistent": True,
        "desired_state": "running",
        "created_at": 1.0,
    }
    entry[field] = 10**1000
    store.path.write_text(
        json.dumps({"version": STATE_SCHEMA_VERSION, "sessions": [entry]}),
        encoding="utf-8",
    )

    result = store.read()

    assert result.status == "degraded"
    assert result.entries == []
    assert result.quarantine_path is not None


def test_read_rejects_non_persistent_entry(tmp_path: Path) -> None:
    """A hand-edited state entry with persistent=false is malformed."""
    store = PersistentStore(tmp_path)
    store.path.write_text(
        json.dumps(
            {
                "version": STATE_SCHEMA_VERSION,
                "sessions": [
                    {
                        "id": "x",
                        "target": "t.example",
                        "interval_s": 1.0,
                        "name": "",
                        "persistent": False,
                        "desired_state": "running",
                        "created_at": 1.0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    result = store.read()
    assert result.status == "degraded"
    assert result.entries == []
    assert any("persistent flag must be true" in reason for reason in result.skipped)


def test_read_rejects_entry_missing_name(tmp_path: Path) -> None:
    """The schema requires the name field even when its value is null."""
    store = PersistentStore(tmp_path)
    store.path.write_text(
        json.dumps(
            {
                "version": STATE_SCHEMA_VERSION,
                "sessions": [
                    {
                        "id": "x",
                        "target": "t.example",
                        "interval_s": 1.0,
                        "persistent": True,
                        "desired_state": "running",
                        "created_at": 1.0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = store.read()

    assert result.status == "degraded"
    assert result.entries == []
    assert any("missing name" in reason for reason in result.skipped)


def _matrix_entry() -> dict:
    return {
        "id": "matrix",
        "target": "x.example",
        "interval_s": 1.0,
        "name": None,
        "persistent": True,
        "desired_state": "running",
        "created_at": 1.0,
    }


@pytest.mark.parametrize(
    "field,value,expected",
    [
        # id: non-empty strings are the identifier contract; all other JSON
        # representations are rejected.  Duplicate ids are covered below at
        # the file level because uniqueness is not a per-entry property.
        ("id", None, False),
        ("id", "", False),
        ("id", 1, False),
        ("id", True, False),
        ("id", [], False),
        ("id", " ", False),
        ("id", "\u202e", False),
        ("id", "id!", False),
        ("id", "matrix", True),
        # target: validate_host supplies the bounded safe hostname/IP
        # representation and rejects controls, injection characters, and
        # overlong values.
        ("target", None, False),
        ("target", "", False),
        ("target", 1, False),
        ("target", "bad target", False),
        ("target", "x" * 256, False),
        ("target", "x.example", True),
        # interval_s: strict numeric type, finite, and floor 1.0. There is no
        # documented upper bound; a finite large value is therefore accepted
        # as a very long polling interval rather than invented policy here.
        ("interval_s", None, False),
        ("interval_s", "1", False),
        ("interval_s", True, False),
        ("interval_s", float("nan"), False),
        ("interval_s", float("inf"), False),
        ("interval_s", -1, False),
        ("interval_s", 0, False),
        ("interval_s", 1e308, True),
        ("interval_s", 1.0, True),
        # name: null is the documented fallback; wrong types reject, while
        # user text is intentionally sanitized/truncated on read.
        ("name", None, True),
        ("name", 1, False),
        ("name", {}, False),
        ("name", "a" * (MAX_NAME_LEN + 1), True),
        ("name", "a\u202eb", True),
        # persistent: persisted entries must carry the literal JSON true.
        ("persistent", None, False),
        ("persistent", 1, False),
        ("persistent", "true", False),
        ("persistent", False, False),
        ("persistent", True, True),
        # desired_state is a closed enum (bool is not a state).
        ("desired_state", None, False),
        ("desired_state", True, False),
        ("desired_state", "stopped", False),
        ("desired_state", "running" * 100, False),
        ("desired_state", "running", True),
        ("desired_state", "paused", True),
        # created_at: finite non-negative epoch within datetime's supported
        # range; zero and the upper supported epoch are intentional accepts.
        ("created_at", None, False),
        ("created_at", "1", False),
        ("created_at", True, False),
        ("created_at", float("nan"), False),
        ("created_at", float("inf"), False),
        ("created_at", -1, False),
        ("created_at", 10**1000, False),
        ("created_at", 0, True),
        ("created_at", 1.0, True),
    ],
)
def test_session_entry_validation_matrix(field, value, expected) -> None:
    entry = _matrix_entry()
    entry[field] = value
    parsed, reason = _validate_session_entry(entry)
    assert (parsed is not None) is expected, (field, value, reason)


@pytest.mark.parametrize(
    "field",
    ["id", "target", "interval_s", "name", "persistent", "desired_state", "created_at"],
)
def test_session_entry_validation_rejects_missing_required_field(field) -> None:
    entry = _matrix_entry()
    entry.pop(field)
    parsed, reason = _validate_session_entry(entry)
    assert parsed is None, (field, reason)


def test_read_rejects_unknown_entry_fields_and_degrades(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    entry = _matrix_entry()
    entry["future_field"] = "must not be silently discarded"
    store.path.write_text(
        json.dumps({"version": STATE_SCHEMA_VERSION, "sessions": [entry]}),
        encoding="utf-8",
    )
    result = store.read()
    assert result.status == "degraded"
    assert result.entries == []
    assert any("unknown field" in reason for reason in result.skipped)


def test_read_rejects_unknown_state_fields_and_degrades(tmp_path: Path) -> None:
    """The state envelope is closed; newer fields must not be discarded."""
    store = PersistentStore(tmp_path)
    store.path.write_text(
        json.dumps(
            {
                "version": STATE_SCHEMA_VERSION,
                "sessions": [],
                "future_metadata": {"format": 2},
            }
        ),
        encoding="utf-8",
    )

    result = store.read()

    assert result.status == "degraded"
    assert result.entries == []
    assert "unknown state field" in (result.reason or "")
    assert result.quarantine_path is not None


def test_read_rejects_duplicate_ids_and_degrades(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    first = _matrix_entry()
    second = _matrix_entry()
    second["target"] = "other.example"
    store.path.write_text(
        json.dumps({"version": STATE_SCHEMA_VERSION, "sessions": [first, second]}),
        encoding="utf-8",
    )
    result = store.read()
    assert result.status == "degraded"
    assert [entry["target"] for entry in result.entries] == ["x.example"]
    assert any("duplicate id" in reason for reason in result.skipped)


def test_read_normalizes_stored_name_with_controls(tmp_path: Path) -> None:
    """A stored name with control chars is sanitized on read (defensive)."""
    store = PersistentStore(tmp_path)
    store.path.write_text(
        json.dumps(
            {
                "version": STATE_SCHEMA_VERSION,
                "sessions": [
                    {
                        "id": "x",
                        "target": "t.example",
                        "interval_s": 1.0,
                        "name": "a\tb\x07c",
                        "persistent": True,
                        "desired_state": "running",
                        "created_at": 1.0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    entries = store.read().entries
    assert entries[0]["name"] == "a b c"


def test_write_payload_includes_schema_version(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    store.write([])
    payload = json.loads(store.path.read_text(encoding="utf-8"))
    assert payload["version"] == STATE_SCHEMA_VERSION
    assert payload["sessions"] == []


# ============================================================== sanitize_name


@pytest.mark.parametrize(
    "raw, expected",
    [
        (None, ""),
        (123, ""),
        ("", ""),
        ("   ", ""),
        ("plain", "plain"),
        ("  padded  ", "padded"),
        ("a\tb", "a b"),
        ("a\x07b", "a b"),  # BEL
        ("a\x1bb", "a b"),  # ESC
        ("line1\nline2", "line1 line2"),
        ("multiple   spaces", "multiple spaces"),
        ("emoji ✅ ok", "emoji ✅ ok"),  # non-control unicode preserved
        ("a" * 200, "a" * MAX_NAME_LEN),  # hard truncation
        ("\U0001f600lead", "\U0001f600lead"),  # supplementary plane kept
        # Cf (format) characters — the prior range-based regex missed these.
        ("a\u202eb", "a b"),  # U+202E RIGHT-TO-LEFT OVERRIDE (bidi spoofing)
        ("a\u200bb", "a b"),  # U+200B ZERO WIDTH SPACE
        ("a\ufeffb", "a b"),  # U+FEFF BOM / ZERO WIDTH NO-BREAK SPACE
        ("a\u2066b\u2069c", "a b c"),  # U+2066/2069 bidi isolates
        ("a\u200eb", "a b"),  # U+200E LTR mark
        ("a\u200fb", "a b"),  # U+200F RTL mark
        ("a\u00adb", "a b"),  # U+00AD SOFT HYPHEN
        ("a\u202ab", "a b"),  # U+202A LTR EMBEDDING
        ("\u202eevil", "evil"),  # leading bidi override stripped entirely
        # Co (private-use) characters — now stripped (category-based).
        ("a\ue000b", "a b"),  # U+E000 BMP private-use start
        ("a\uf8ffb", "a b"),  # U+F8FF BMP private-use end
        # Cs (surrogate) — unpaired surrogates that would break UTF-8 writes.
        ("a\ud800b", "a b"),  # U+D800 lone surrogate (high)
        ("a\udffbc", "a c"),  # U+DFFB lone surrogate (low), trailing char
        # Multiple Cf chars collapse to a single space.
        ("a\u200b\u200b\u202eb", "a b"),
    ],
)
def test_sanitize_name(raw, expected: str) -> None:
    assert sanitize_name(raw) == expected


def test_sanitize_name_surrogate_does_not_crash_utf8_encode() -> None:
    """An unpaired surrogate in a name must not crash the UTF-8 write path.

    The prior range-based regex did not strip Cs code points; a stored
    name containing a lone surrogate would pass sanitize_name, then crash
    ``json.dumps(..., ensure_ascii=False)`` → ``str.encode('utf-8')`` with
    ``UnicodeEncodeError``, producing a repeatable persistence failure.
    The category-based filter strips Cs before the write ever sees them.
    """
    cleaned = sanitize_name("bad\ud800name")
    # No surrogate survives.
    assert "\ud800" not in cleaned
    # Encoding must succeed (this would raise before the fix).
    cleaned.encode("utf-8")


def test_sanitize_name_truncates_after_normalization() -> None:
    # Control chars become spaces first, then truncation applies, so a name
    # that is exactly MAX_NAME_LEN long after normalization survives intact
    # and a longer one is cut at the cap.
    raw = "a\t" + "b" * (MAX_NAME_LEN + 10)
    out = sanitize_name(raw)
    assert len(out) == MAX_NAME_LEN
    assert out.startswith("a ")


# ============================================================== SessionManager


def test_create_non_persistent_does_not_write(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    mgr.create("example.com")
    # No persistent sessions -> no file written (and none removed: nothing
    # existed to remove).
    assert not store.path.exists()
    # The list of sessions in memory still reflects the create.
    assert len(mgr.list()) == 1


def test_create_persistent_writes_file(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    s = mgr.create("example.com", interval_s=2.5, name="hi", persistent=True)
    assert store.path.exists()
    entries = store.read().entries
    assert len(entries) == 1
    entry = entries[0]
    assert entry["id"] == s.id
    assert entry["target"] == "example.com"
    assert entry["interval_s"] == 2.5
    assert entry["name"] == "hi"
    assert entry["persistent"] is True
    assert entry["desired_state"] == "running"


def test_pause_updates_desired_state_in_file(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    s = mgr.create("example.com", persistent=True)

    asyncio.run(mgr.pause(s))
    entries = store.read().entries
    assert entries[0]["desired_state"] == "paused"


def test_resume_updates_desired_state_back_to_running(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    s = mgr.create("example.com", persistent=True)
    asyncio.run(mgr.pause(s))
    asyncio.run(mgr.resume(s))
    entries = store.read().entries
    assert entries[0]["desired_state"] == "running"


def test_delete_removes_session_from_file(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    s1 = mgr.create("a.example", persistent=True)
    s2 = mgr.create("b.example", persistent=True)
    assert len(store.read().entries) == 2

    asyncio.run(mgr.delete(s1))
    entries = store.read().entries
    assert len(entries) == 1
    assert entries[0]["id"] == s2.id


def test_delete_last_persistent_removes_file(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    s = mgr.create("a.example", persistent=True)
    assert store.path.exists()

    asyncio.run(mgr.delete(s))
    # Empty persistent set -> file removed so a boot reads nothing.
    assert not store.path.exists()


def test_set_persistent_toggle_writes_and_removes(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    s = mgr.create("example.com")  # non-persistent
    assert not store.path.exists()

    mgr.set_persistent(s, True)
    assert store.path.exists()
    assert len(store.read().entries) == 1

    mgr.set_persistent(s, False)
    # Toggling back to non-persistent leaves no persistent sessions, so the
    # file is removed.
    assert not store.path.exists()
    assert s.persistent is False


def test_set_persistent_renames_in_same_call(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    s = mgr.create("example.com", persistent=True, name="old")
    mgr.set_persistent(s, True, name="new name")
    assert s.name == "new name"
    entries = store.read().entries
    assert entries[0]["name"] == "new name"


def test_set_persistent_none_name_preserves_existing(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    s = mgr.create("example.com", persistent=True, name="kept")
    mgr.set_persistent(s, True)  # no name argument
    assert s.name == "kept"


def test_create_sanitizes_name(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    s = mgr.create("example.com", name="a\tb\x07c", persistent=True)
    assert s.name == "a b c"
    entries = store.read().entries
    assert entries[0]["name"] == "a b c"


def test_persistence_no_op_when_store_none() -> None:
    """A SessionManager without a persistence store is purely in-memory.

    This is the back-compat shape: existing embedders that do not pass a
    store never touch disk and never restore on boot.
    """
    mgr = SessionManager(
        probe_fn=_instant_probe,
        discover_fn=_failing_discover,
        resolve_name_fn=_fast_resolve_name,
        # persistence omitted
    )
    s = mgr.create("example.com", persistent=True)
    # No store -> _persist is a no-op, no exception, no file.
    mgr.set_persistent(s, False)
    asyncio.run(mgr.pause(s))
    asyncio.run(mgr.resume(s))
    # restore is a no-op too.
    asyncio.run(mgr.restore())
    assert len(mgr.list()) == 1


# ============================================================== restore


def test_restore_recreates_running_session_and_starts_probing(
    tmp_path: Path,
) -> None:
    """A persisted running session is recreated and probing resumes."""
    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    s = mgr.create("example.com", interval_s=1.0, name="lbl", persistent=True)
    created_id = s.id
    # Wait for the probe loop to attach a task so we can compare against a
    # restored session's task afterwards.
    asyncio.run(mgr.start(s))
    assert s._task is not None
    original_task = s._task
    # Shut down the first manager to simulate a process restart.
    asyncio.run(mgr.shutdown())

    # A fresh manager reading the same store restores the session.
    mgr2 = _manager(store)
    asyncio.run(mgr2.restore())
    sessions = mgr2.list()
    assert len(sessions) == 1
    restored = sessions[0]
    assert restored.id == created_id
    assert restored.target == "example.com"
    assert restored.interval_s == 1.0
    assert restored.name == "lbl"
    assert restored.persistent is True
    # Desired state was "running" -> probing restarted.
    assert restored.state == "running"
    assert restored._task is not None
    assert restored._task is not original_task  # new task, not the old one
    asyncio.run(mgr2.shutdown())


def test_restore_paused_session_restores_paused_with_paused_at(
    tmp_path: Path,
) -> None:
    """A persisted paused session restores paused with paused_at set.

    paused_at is the restore time, not the original pause epoch (per the
    design note: long downtime must not anchor the freeze at a stale
    wall-clock).
    """
    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    s = mgr.create("example.com", persistent=True)
    asyncio.run(mgr.pause(s))
    paused_id = s.id
    asyncio.run(mgr.shutdown())

    before_restore = time.time()
    mgr2 = _manager(store)
    asyncio.run(mgr2.restore())
    sessions = mgr2.list()
    assert len(sessions) == 1
    restored = sessions[0]
    assert restored.id == paused_id
    assert restored.state == "paused"
    # Paused restore: probe task must NOT be running.
    assert restored._task is None or restored._task.done()
    # paused_at is the restore-time epoch (real, not None, not the original
    # pause instant from the first manager).
    assert isinstance(restored.paused_at, float)
    assert before_restore <= restored.paused_at <= time.time()
    asyncio.run(mgr2.shutdown())


def test_restore_preserves_id_order(tmp_path: Path) -> None:
    """Sessions are recreated in file (insertion) order."""
    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    ids = []
    for i in range(3):
        s = mgr.create(f"t{i}.example", persistent=True)
        ids.append(s.id)
    asyncio.run(mgr.shutdown())

    mgr2 = _manager(store)
    asyncio.run(mgr2.restore())
    restored_ids = [s.id for s in mgr2.list()]
    assert restored_ids == ids
    asyncio.run(mgr2.shutdown())


def test_restore_caps_at_max_sessions(tmp_path: Path, caplog) -> None:
    """More than MAX_SESSIONS persistent sessions restore only the first MAX."""
    store = PersistentStore(tmp_path)
    # Bypass the manager's create-time cap by writing the file directly.
    entries = [
        {
            "id": f"id{i}",
            "target": f"t{i}.example",
            "interval_s": 60.0,
            "name": "",
            "persistent": True,
            "desired_state": "paused",  # avoid probe loop startup noise
            "created_at": float(i),
        }
        for i in range(MAX_SESSIONS + 3)
    ]
    store.write(entries)

    mgr = _manager(store)
    with caplog.at_level("WARNING"):
        asyncio.run(mgr.restore())
    sessions = mgr.list()
    assert len(sessions) == MAX_SESSIONS
    # The first MAX in file order survive.
    assert [s.id for s in sessions] == [f"id{i}" for i in range(MAX_SESSIONS)]
    assert any(
        "reached MAX_SESSIONS" in rec.message or "restoring only" in rec.message
        for rec in caplog.records
    )
    asyncio.run(mgr.shutdown())


def test_restore_skips_duplicate_ids(tmp_path: Path) -> None:
    """A duplicate id in the file is skipped, not overwritten."""
    store = PersistentStore(tmp_path)
    store.write(
        [
            {
                "id": "dup",
                "target": "first.example",
                "interval_s": 60.0,
                "name": "",
                "persistent": True,
                "desired_state": "paused",
                "created_at": 1.0,
            },
            {
                "id": "dup",
                "target": "second.example",
                "interval_s": 60.0,
                "name": "",
                "persistent": True,
                "desired_state": "paused",
                "created_at": 2.0,
            },
        ]
    )
    mgr = _manager(store)
    asyncio.run(mgr.restore())
    sessions = mgr.list()
    assert len(sessions) == 1
    assert sessions[0].target == "first.example"
    asyncio.run(mgr.shutdown())


def test_restore_empty_file_is_noop(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    store.write([])
    mgr = _manager(store)
    asyncio.run(mgr.restore())
    assert mgr.list() == []


def test_restore_missing_file_is_noop(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    asyncio.run(mgr.restore())
    assert mgr.list() == []


def test_restore_corrupt_file_is_noop_and_does_not_crash(
    tmp_path: Path, caplog
) -> None:
    store = PersistentStore(tmp_path)
    store.path.write_text("{garbage", encoding="utf-8")
    mgr = _manager(store)
    with caplog.at_level("WARNING"):
        asyncio.run(mgr.restore())
    assert mgr.list() == []


def test_restore_unknown_schema_version_is_noop(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    store.path.write_text(json.dumps({"version": 99, "sessions": []}), encoding="utf-8")
    mgr = _manager(store)
    asyncio.run(mgr.restore())
    assert mgr.list() == []


# ============================================================== API: create


def _create_then(
    client: TestClient,
    target: str = "127.0.0.1",
    interval_s: float = 2.0,
    **extra,
) -> str:
    body = {"target": target, "interval_s": interval_s, **extra}
    r = client.post("/api/traces", json=body)
    assert r.status_code == 201, r.text
    return r.json()["id"]


def test_create_with_persistent_writes_state_file(tmp_path: Path) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client, persistent=True, name="api-trace")
        # File written with the session definition.
        store = PersistentStore(tmp_path)
        assert store.path.exists()
        entries = store.read().entries
        assert len(entries) == 1
        assert entries[0]["id"] == sid
        assert entries[0]["persistent"] is True
        assert entries[0]["name"] == "api-trace"


def test_create_without_persistent_does_not_write_file(tmp_path: Path) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        _create_then(client)
        assert not PersistentStore(tmp_path).path.exists()


def test_create_response_carries_name_and_persistent(tmp_path: Path) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        r = client.post(
            "/api/traces",
            json={
                "target": "127.0.0.1",
                "interval_s": 2.0,
                "name": "named",
                "persistent": True,
            },
        )
        body = r.json()
        assert body["name"] == "named"
        assert body["persistent"] is True


def test_create_validates_name_length(tmp_path: Path) -> None:
    """A name longer than MAX_NAME_LEN is truncated, not rejected.

    The validator sanitizes (which includes truncation), so the create
    succeeds and the stored name is exactly MAX_NAME_LEN chars.
    """
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        long_name = "x" * (MAX_NAME_LEN + 50)
        r = client.post(
            "/api/traces",
            json={"target": "127.0.0.1", "interval_s": 2.0, "name": long_name},
        )
        assert r.status_code == 201
        sid = r.json()["id"]
        listing = client.get("/api/traces").json()
        match = next(s for s in listing if s["id"] == sid)
        assert len(match["name"]) == MAX_NAME_LEN


def test_create_strips_control_chars_from_name(tmp_path: Path) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        r = client.post(
            "/api/traces",
            json={"target": "127.0.0.1", "interval_s": 2.0, "name": "a\tb\x07c"},
        )
        sid = r.json()["id"]
        listing = client.get("/api/traces").json()
        match = next(s for s in listing if s["id"] == sid)
        assert match["name"] == "a b c"


# ============================================================== API: persist


def test_persist_endpoint_toggles_persistent_flag(tmp_path: Path) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client)
        # File not written yet (non-persistent).
        assert not PersistentStore(tmp_path).path.exists()

        r = client.post(
            f"/api/traces/{sid}/persist",
            json={"persistent": True},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["persistent"] is True
        assert body["id"] == sid
        # File now exists.
        assert PersistentStore(tmp_path).path.exists()

        # Toggle back off.
        r = client.post(
            f"/api/traces/{sid}/persist",
            json={"persistent": False},
        )
        assert r.status_code == 200
        assert r.json()["persistent"] is False
        # No persistent sessions -> file removed.
        assert not PersistentStore(tmp_path).path.exists()


def test_persist_endpoint_renames(tmp_path: Path) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client, persistent=True, name="old")
        r = client.post(
            f"/api/traces/{sid}/persist",
            json={"persistent": True, "name": "new name"},
        )
        assert r.status_code == 200
        assert r.json()["name"] == "new name"
        entries = PersistentStore(tmp_path).read().entries
        assert entries[0]["name"] == "new name"


def test_persist_endpoint_returns_404_for_unknown_session(
    tmp_path: Path,
) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        r = client.post(
            "/api/traces/deadbeef/persist",
            json={"persistent": True},
        )
        assert r.status_code == 404


def test_persist_endpoint_validates_name(tmp_path: Path) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client, persistent=True)
        r = client.post(
            f"/api/traces/{sid}/persist",
            json={"persistent": True, "name": "n" * 500},
        )
        assert r.status_code == 200
        listing = client.get("/api/traces").json()
        match = next(s for s in listing if s["id"] == sid)
        assert len(match["name"]) == MAX_NAME_LEN


def test_persist_endpoint_422_when_persistent_missing(tmp_path: Path) -> None:
    """``persistent`` is required on the persist endpoint."""
    client = TestClient(create_app(data_dir=tmp_path), base_url="http://localhost", raise_server_exceptions=False)
    sid = _create_then(client)
    r = client.post(f"/api/traces/{sid}/persist", json={"name": "x"})
    assert r.status_code == 422


# ============================================================== API: restore on startup


def test_create_app_restores_on_startup(tmp_path: Path) -> None:
    """A state file present at boot is restored when the app starts."""
    store = PersistentStore(tmp_path)
    store.write(
        [
            {
                "id": "boot-restore",
                "target": "127.0.0.1",
                "interval_s": 60.0,
                "name": "boot",
                "persistent": True,
                "desired_state": "running",
                "created_at": 1.0,
            }
        ]
    )
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        listing = client.get("/api/traces").json()
        assert len(listing) == 1
        assert listing[0]["id"] == "boot-restore"
        assert listing[0]["target"] == "127.0.0.1"
        assert listing[0]["persistent"] is True
        assert listing[0]["name"] == "boot"
        assert listing[0]["state"] == "running"


def test_create_app_restore_paused_session(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    store.write(
        [
            {
                "id": "paused-restore",
                "target": "127.0.0.1",
                "interval_s": 60.0,
                "name": "",
                "persistent": True,
                "desired_state": "paused",
                "created_at": 1.0,
            }
        ]
    )
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        listing = client.get("/api/traces").json()
        assert len(listing) == 1
        assert listing[0]["state"] == "paused"
        assert listing[0]["paused_at"] is not None


def test_create_app_restore_corrupt_file_does_not_crash(tmp_path: Path) -> None:
    """A corrupt state file at boot is ignored; the app still serves."""
    PersistentStore(tmp_path).path.write_text("{garbage", encoding="utf-8")
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        # App came up; listing is empty (no restore, no crash).
        assert client.get("/api/traces").json() == []
        # And a fresh create still works.
        sid = _create_then(client)
        assert client.get(f"/api/traces/{sid}/snapshot").status_code == 200


def test_create_app_restore_caps_at_max_sessions(tmp_path: Path) -> None:
    store = PersistentStore(tmp_path)
    store.write(
        [
            {
                "id": f"id{i}",
                "target": "127.0.0.1",
                "interval_s": 60.0,
                "name": "",
                "persistent": True,
                "desired_state": "paused",
                "created_at": float(i),
            }
            for i in range(MAX_SESSIONS + 2)
        ]
    )
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        listing = client.get("/api/traces").json()
        assert len(listing) == MAX_SESSIONS


# ============================================================== serialization fields


def test_summary_carries_persistence_fields() -> None:
    s = TraceSession(
        id="x", target="t.example", interval_s=1.0, name="lbl", persistent=True
    )
    summary = s.summary()
    assert summary["name"] == "lbl"
    assert summary["persistent"] is True


def test_snapshot_carries_persistence_fields() -> None:
    s = TraceSession(
        id="x", target="t.example", interval_s=1.0, name="lbl", persistent=True
    )
    snap = s.snapshot(focus_s=10.0)
    assert snap["name"] == "lbl"
    assert snap["persistent"] is True


def test_list_endpoint_includes_persistence_fields(tmp_path: Path) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        _create_then(client, persistent=True, name="listed")
        items = client.get("/api/traces").json()
        assert len(items) == 1
        assert items[0]["persistent"] is True
        assert items[0]["name"] == "listed"


def test_snapshot_endpoint_includes_persistence_fields(tmp_path: Path) -> None:
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client, persistent=True, name="snapped")
        snap = client.get(f"/api/traces/{sid}/snapshot").json()
        assert snap["persistent"] is True
        assert snap["name"] == "snapped"


# ============================================================== compose with hardening


def test_persist_toggle_while_running_does_not_disturb_probing(
    tmp_path: Path,
) -> None:
    """Toggling persistence on a running session does not pause probing.

    The persist endpoint only writes the state file; it must not cancel
    the probe task. This composes with the pause-freeze + lifecycle
    hardening from 2b40c4b (effNowS freeze, paused_at, generation-gated
    snapshots) — the persist path is orthogonal to the probe loop.
    """
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client, interval_s=1.0)
        # Grab the probe task via the manager.
        mgr = client.app.state.session_manager
        session = mgr.get(sid)
        assert session is not None
        assert session._task is not None
        task_before = session._task

        r = client.post(f"/api/traces/{sid}/persist", json={"persistent": True})
        assert r.status_code == 200

        # Same task object -> probe loop was not cancelled/restarted.
        assert session._task is task_before
        assert session.state == "running"
        assert session.paused_at is None


def test_restored_session_flows_through_snapshot_path(tmp_path: Path) -> None:
    """A restored persistent session surfaces a snapshot with the right fields.

    Restored sessions go through the same snapshot path as fresh ones
    (compose-with-hardening requirement); the snapshot must include the
    persistent/name fields and a usable state value.
    """
    store = PersistentStore(tmp_path)
    store.write(
        [
            {
                "id": "snapshot-check",
                "target": "127.0.0.1",
                "interval_s": 60.0,
                "name": "restored",
                "persistent": True,
                "desired_state": "running",
                "created_at": 1.0,
            }
        ]
    )
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        snap = client.get("/api/traces/snapshot-check/snapshot").json()
        assert snap["id"] == "snapshot-check"
        assert snap["persistent"] is True
        assert snap["name"] == "restored"
        assert snap["state"] == "running"


# ============================================================== Finding 1: degraded mode


def test_degraded_store_quarantines_and_enters_readonly(tmp_path: Path) -> None:
    """A present-but-corrupt file triggers quarantine + read-only mode."""
    store = PersistentStore(tmp_path)
    store.path.write_text("GARBAGE", encoding="utf-8")
    result = store.read()
    assert result.status == "degraded"
    assert result.quarantine_path is not None
    assert result.quarantine_path.exists()
    assert not store.path.exists()  # renamed to quarantine
    # Store is read-only: write and remove both refuse.
    assert store.is_degraded()
    with pytest.raises(PersistDegraded):
        store.write([])
    with pytest.raises(PersistDegraded):
        store.remove()


def test_degraded_store_surfaces_on_healthz(tmp_path: Path) -> None:
    """``/healthz`` carries ``persistence: "degraded"`` after a degraded boot."""
    PersistentStore(tmp_path).path.write_text("{garbage", encoding="utf-8")
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        h = client.get("/healthz").json()
        assert h["status"] == "ok"
        assert h["persistence"] == "degraded"


def test_healthz_omits_persistence_when_healthy(tmp_path: Path) -> None:
    """No degraded state → healthz carries only ``status: ok``."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        h = client.get("/healthz").json()
        assert h["status"] == "ok"
        assert "persistence" not in h


def test_missing_file_does_not_set_degraded(tmp_path: Path) -> None:
    """A fresh start (no file) is NOT degraded — normal empty boot."""
    store = PersistentStore(tmp_path)
    result = store.read()
    assert result.status == "missing"
    assert not store.is_degraded()


def test_valid_file_clears_prior_degraded(tmp_path: Path) -> None:
    """Operator resolves the quarantine; restart re-reads a clean file →
    degraded flag cleared, writes accepted again."""
    store = PersistentStore(tmp_path)
    # First boot: corrupt file → degraded.
    store.path.write_text("{garbage", encoding="utf-8")
    result1 = store.read()
    assert result1.status == "degraded"
    assert store.is_degraded()
    # Operator removes the quarantine and writes a clean file.
    assert result1.quarantine_path is not None
    result1.quarantine_path.unlink()
    store._clear_degraded()
    store.write(
        [
            {
                "id": "clean",
                "target": "127.0.0.1",
                "interval_s": 1.0,
                "name": "",
                "persistent": True,
                "desired_state": "running",
                "created_at": 1.0,
            }
        ]
    )
    # Re-read: valid → degraded cleared.
    result2 = store.read()
    assert result2.status == "valid"
    assert not store.is_degraded()


def test_degraded_restore_caps_by_successful_count_not_offset(
    tmp_path: Path, caplog
) -> None:
    """Invalid entries among the first N do NOT exclude valid later ones.

    The old offset-based cap counted file offsets; if 3 of the first
    MAX_SESSIONS entries were invalid, only MAX_SESSIONS-3 valid ones
    were restored and valid entries beyond offset MAX_SESSIONS were
    skipped. The new cap counts SUCCESSFUL registrations so all
    MAX_SESSIONS valid slots are filled from the valid subset.
    """
    store = PersistentStore(tmp_path)
    sessions = [
        {
            "id": f"valid-{i}",
            "target": "127.0.0.1",
            "interval_s": 60.0,
            "name": "",
            "persistent": True,
            "desired_state": "paused",
            "created_at": float(i),
        }
        for i in range(MAX_SESSIONS + 2)
    ]
    # Insert 3 invalid entries at the front so the first MAX_SESSIONS
    # file offsets include invalids.
    for bad in [
        {
            "id": "",
            "target": "x",
            "interval_s": 1.0,
            "desired_state": "running",
            "created_at": 1.0,
        },  # empty id
        {
            "target": "x",
            "interval_s": 1.0,
            "desired_state": "running",
            "created_at": 1.0,
        },  # missing id
        "not-a-dict",
    ]:
        sessions.insert(0, bad)
    store.write(sessions)
    mgr = _manager(store)
    with caplog.at_level("WARNING"):
        asyncio.run(mgr.restore())
    # All MAX_SESSIONS valid sessions restored despite invalids at the front.
    # (Without the fix, the old offset-based cap would have counted the 3
    # invalids as offsets 0-2, leaving only offsets 3-7 = 5 valid restored.)
    assert len(mgr.list()) == MAX_SESSIONS
    restored_ids = {s.id for s in mgr.list()}
    # The first MAX_SESSIONS valid entries in file order are restored.
    assert restored_ids == {f"valid-{i}" for i in range(MAX_SESSIONS)}
    asyncio.run(mgr.shutdown())


def test_degraded_persist_returns_503(tmp_path: Path) -> None:
    """A persist POST against a degraded store returns 503, not 500."""
    PersistentStore(tmp_path).path.write_text("{garbage", encoding="utf-8")
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        # Create a non-persistent session first (does not touch disk).
        sid = _create_then(client)
        r = client.post(f"/api/traces/{sid}/persist", json={"persistent": True})
        assert r.status_code == 503


def test_degraded_create_persistent_returns_503(tmp_path: Path) -> None:
    """Creating a persistent session against a degraded store returns 503."""
    PersistentStore(tmp_path).path.write_text("{garbage", encoding="utf-8")
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        r = client.post(
            "/api/traces",
            json={"target": "127.0.0.1", "interval_s": 2.0, "persistent": True},
        )
        assert r.status_code == 503


# ============================================================== Finding 3: cancelled persistent create


def test_cancelled_persistent_create_does_not_leave_durable_running(
    tmp_path: Path,
) -> None:
    """Cancelling a persistent create+start between the two steps must not
    leave a durable ``running`` definition with no probe task.

    Injection-based: we slow down ``start`` so the outer create_and_start
    task can be cancelled while the SHIELDED inner is between create
    (durable write done) and start (probe task not yet attached). The
    shield lets the inner complete; the outer catches CancelledError and
    compensates by deleting the created session.
    """
    store = PersistentStore(tmp_path)
    mgr = _manager(store)

    created = asyncio.Event()
    _original_start = mgr.start

    async def slow_start(session):
        # Signal that create() has finished (session is durable).
        created.set()
        # Yield control so the outer task can be cancelled while the
        # shielded inner is still mid-start.
        await asyncio.sleep(0.1)
        await _original_start(session)

    mgr.start = slow_start  # type: ignore[method-assign]

    async def driver():
        task = asyncio.create_task(mgr.create_and_start("127.0.0.1", 2.5, "x", True))
        # Wait until the durable write is done but start hasn't finished.
        await created.wait()
        await asyncio.sleep(0.01)  # ensure start is mid-flight
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(driver())

    # Compensation should have removed the session — no orphan in registry.
    sessions = mgr.list()
    assert sessions == [], f"expected no sessions after compensation, got {sessions}"
    # No durable "running" definition left behind.
    if store.path.exists():
        result = store.read()
        assert result.entries == [], "compensation should have cleared the file"

    mgr.start = _original_start  # type: ignore[method-assign]
    asyncio.run(mgr.shutdown())


def test_cancelled_persistent_create_via_shield_no_durable_leak(
    tmp_path: Path,
) -> None:
    """When the shielded create+start completes but the caller is cancelled,
    the session is removed (compensation) so no durable running session
    survives the client's cancellation."""
    store = PersistentStore(tmp_path)
    mgr = _manager(store)

    started = asyncio.Event()

    async def slow_start(session):
        # Let the caller cancel while we're in start (but shielded, so we
        # complete). Mark that we reached start.
        started.set()
        await asyncio.sleep(0.05)
        await _original_start(session)

    _original_start = mgr.start
    mgr.start = slow_start  # type: ignore[method-assign]

    async def driver():
        task = asyncio.create_task(mgr.create_and_start("127.0.0.1", 2.5, "x", True))
        await started.wait()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(driver())

    # The shielded create+start completed, then compensation deleted the
    # session. No orphan in registry.
    sessions = mgr.list()
    assert sessions == [], f"expected no sessions after compensation, got {sessions}"

    mgr.start = _original_start  # type: ignore[method-assign]
    asyncio.run(mgr.shutdown())


def test_non_cancelled_persistent_create_works_normally(tmp_path: Path) -> None:
    """Happy path: persistent create_and_start creates a running session
    with a probe task and a durable running definition."""
    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    session = asyncio.run(mgr.create_and_start("127.0.0.1", 2.5, "lbl", True))
    assert session.state == "running"
    assert session._task is not None
    entries = store.read().entries
    assert len(entries) == 1
    assert entries[0]["desired_state"] == "running"
    asyncio.run(mgr.shutdown())


# ============================================================== Finding 4: persist no-op + 409


def test_persist_noop_fast_path_no_lock_no_write(tmp_path: Path) -> None:
    """An idempotent persist POST (same flag, no name change) does not take
    the lock or touch disk."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client, persistent=True, name="lbl")
        store = PersistentStore(tmp_path)
        mtime_before = store.path.stat().st_mtime_ns

        # Same persistent=True, no name → no-op.
        r = client.post(
            f"/api/traces/{sid}/persist",
            json={"persistent": True},
        )
        assert r.status_code == 200
        assert r.json()["persistent"] is True
        # File mtime unchanged (no write).
        mtime_after = store.path.stat().st_mtime_ns
        assert mtime_after == mtime_before


def test_persist_noop_same_name_no_write(tmp_path: Path) -> None:
    """Same flag + same name → no-op (no write)."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client, persistent=True, name="keep")
        store = PersistentStore(tmp_path)
        mtime_before = store.path.stat().st_mtime_ns

        r = client.post(
            f"/api/traces/{sid}/persist",
            json={"persistent": True, "name": "keep"},
        )
        assert r.status_code == 200
        mtime_after = store.path.stat().st_mtime_ns
        assert mtime_after == mtime_before


def test_persist_returns_409_when_mutation_pending(tmp_path: Path) -> None:
    """A persist POST while a pause is in progress returns 409, not a
    blocked worker."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client, persistent=True)
        mgr = client.app.state.session_manager
        session = mgr.get(sid)
        assert session is not None

        # Hold the mutation lock to simulate an in-progress mutation.
        session._mutation_lock.acquire()
        try:
            r = client.post(
                f"/api/traces/{sid}/persist",
                json={"persistent": True, "name": "new"},
            )
            assert r.status_code == 409
            assert "mutation in progress" in r.json()["detail"]
        finally:
            session._mutation_lock.release()


# ============================================================== Finding 5: stale-object 404


def test_persist_after_delete_returns_404(tmp_path: Path) -> None:
    """A persist POST that resolves a session, then loses to a concurrent
    delete, returns 404 (not 200 with stale persistent:true)."""
    with TestClient(create_app(data_dir=tmp_path), base_url="http://localhost") as client:
        sid = _create_then(client, persistent=True)
        mgr = client.app.state.session_manager
        session = mgr.get(sid)
        assert session is not None

        # Simulate the race: delete the session from the registry while
        # holding the stale object reference.
        session._mutation_lock.acquire()
        try:
            with mgr._registry_lock:
                mgr._sessions.pop(sid, None)
            # Now persist with the stale object → should 404.
            r = client.post(
                f"/api/traces/{sid}/persist",
                json={"persistent": True, "name": "stale"},
            )
            assert r.status_code == 404
        finally:
            session._mutation_lock.release()


def test_pause_after_delete_returns_404(tmp_path: Path) -> None:
    """A pause on a concurrently-deleted session raises SessionNotRegistered.

    The guard fires after the mutation lock is acquired but before any
    state change. We test it directly because the full pause path uses a
    blocking lock acquire (cancellation-safe via shield) that would
    deadlock if we held the lock to simulate the race.
    """
    from netplot.session import SessionNotRegistered

    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    s = mgr.create("example.com", persistent=True)

    # Simulate concurrent delete: remove from registry while the stale
    # object reference is still held by a caller.
    with mgr._registry_lock:
        mgr._sessions.pop(s.id, None)

    # The guard raises: the session is no longer the registered object.
    with pytest.raises(SessionNotRegistered):
        mgr._require_registered(s)
    asyncio.run(mgr.shutdown())


def test_resume_after_delete_returns_404(tmp_path: Path) -> None:
    """A resume on a concurrently-deleted session raises SessionNotRegistered."""
    from netplot.session import SessionNotRegistered

    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    s = mgr.create("example.com", persistent=True)
    asyncio.run(mgr.pause(s))

    # Simulate concurrent delete.
    with mgr._registry_lock:
        mgr._sessions.pop(s.id, None)

    with pytest.raises(SessionNotRegistered):
        mgr._require_registered(s)
    asyncio.run(mgr.shutdown())


def test_delete_after_delete_raises_not_registered(tmp_path: Path) -> None:
    """A second delete on an already-deleted session raises."""
    from netplot.session import SessionNotRegistered

    store = PersistentStore(tmp_path)
    mgr = _manager(store)
    s = mgr.create("example.com", persistent=True)
    asyncio.run(mgr.delete(s))

    # s is now gone from the registry. A second delete on the stale object
    # raises SessionNotRegistered.
    with pytest.raises(SessionNotRegistered):
        mgr._require_registered(s)


# ============================================================== Finding 6: parent dir fsync


def test_fsync_new_parents_walks_chain(tmp_path: Path) -> None:
    """Every newly-created parent directory is fsynced, not just the leaf.

    We spy on os.fsync to verify it is called for each new parent dir
    (the leaf's parent and every ancestor up to the first pre-existing
    one). The spy does NOT change behaviour; it records call args.
    """
    from netplot.persistence import _fsync_new_parents

    nested = tmp_path / "a" / "b" / "c"
    fsync_calls: list[int] = []
    original_fsync = os.fsync

    def spy(fd: int) -> None:
        fsync_calls.append(fd)
        original_fsync(fd)

    import netplot.persistence as pmod

    real_fsync = pmod.os.fsync
    pmod.os.fsync = spy
    try:
        _fsync_new_parents(nested)
    finally:
        pmod.os.fsync = real_fsync

    # nested = tmp_path/a/b/c. tmp_path exists; a, b, c are new.
    # mkdir order: a, b, c. fsync called for a's parent (tmp_path),
    # b's parent (a), c's parent (b) → 3 fsync calls.
    assert len(fsync_calls) >= 3, (
        f"expected >=3 parent-fsync calls, got {len(fsync_calls)}"
    )
    assert nested.is_dir()


def test_fsync_new_parents_idempotent_for_existing(tmp_path: Path) -> None:
    """Pre-existing parents are not re-fsynced."""
    from netplot.persistence import _fsync_new_parents

    existing = tmp_path / "already"
    existing.mkdir()
    fsync_calls: list[int] = []
    original_fsync = os.fsync

    def spy(fd: int) -> None:
        fsync_calls.append(fd)
        original_fsync(fd)

    import netplot.persistence as pmod

    real_fsync = pmod.os.fsync
    pmod.os.fsync = spy
    try:
        _fsync_new_parents(existing)
    finally:
        pmod.os.fsync = real_fsync

    # Already exists → no mkdir, no fsync.
    assert fsync_calls == []


def test_first_write_fsyncs_parents(tmp_path: Path) -> None:
    """A store.write to a deeply-nested non-existent dir fsyncs every new
    parent before the atomic file write."""
    nested = tmp_path / "x" / "y" / "z"
    store = PersistentStore(nested)
    fsync_calls: list[int] = []
    original_fsync = os.fsync

    def spy(fd: int) -> None:
        fsync_calls.append(fd)
        original_fsync(fd)

    import netplot.persistence as pmod

    real_fsync = pmod.os.fsync
    pmod.os.fsync = spy
    try:
        store.write(
            [
                {
                    "id": "x",
                    "target": "t.example",
                    "interval_s": 1.0,
                    "name": "",
                    "persistent": True,
                    "desired_state": "running",
                    "created_at": 1.0,
                }
            ]
        )
    finally:
        pmod.os.fsync = real_fsync

    # At least: 3 parent-fsyncs (x, y, z creation) + 1 file fsync +
    # ≥1 dir fsync for the atomic replace = ≥5.
    assert len(fsync_calls) >= 5, (
        f"expected >=5 fsync calls (parents + file + dir), got {len(fsync_calls)}"
    )
    assert store.path.exists()


# ============================================================== Finding 7: category sanitizer — Unicode coverage


def test_sanitize_name_strips_all_cf_format_chars() -> None:
    """Every Cf (format) code point is stripped — the prior regex missed these."""
    # A representative sample across the Cf category.
    cf_chars = [
        "\u00ad",  # SOFT HYPHEN
        "\u200b",  # ZERO WIDTH SPACE
        "\u200c",  # ZERO WIDTH NON-JOINER
        "\u200d",  # ZERO WIDTH JOINER
        "\u200e",  # LEFT-TO-RIGHT MARK
        "\u200f",  # RIGHT-TO-LEFT MARK
        "\u202a",  # LEFT-TO-RIGHT EMBEDDING
        "\u202b",  # RIGHT-TO-LEFT EMBEDDING
        "\u202c",  # POP DIRECTIONAL FORMATTING
        "\u202d",  # LEFT-TO-RIGHT OVERRIDE
        "\u202e",  # RIGHT-TO-LEFT OVERRIDE
        "\u2060",  # WORD JOINER
        "\u2061",  # FUNCTION APPLICATION
        "\u2066",  # LEFT-TO-RIGHT ISOLATE
        "\u2067",  # RIGHT-TO-LEFT ISOLATE
        "\u2068",  # FIRST STRONG ISOLATE
        "\u2069",  # POP DIRECTIONAL ISOLATE
        "\ufeff",  # BOM / ZERO WIDTH NO-BREAK SPACE
    ]
    for ch in cf_chars:
        cleaned = sanitize_name(f"a{ch}b")
        assert cleaned == "a b", (
            f"Cf char U+{ord(ch):04X} not stripped: got {cleaned!r}"
        )


def test_sanitize_name_strips_all_co_private_use() -> None:
    """Every Co (private-use) code point is stripped."""
    co_chars = [
        "\ue000",  # BMP PUA start
        "\uf8ff",  # BMP PUA end
        "\U000f0000",  # supplementary PUA-A start
        "\U000ffffd",  # supplementary PUA-A end
    ]
    for ch in co_chars:
        cleaned = sanitize_name(f"a{ch}b")
        assert cleaned == "a b", (
            f"Co char U+{ord(ch):05X} not stripped: got {cleaned!r}"
        )


def test_sanitize_name_strips_unpaired_surrogates() -> None:
    """Unpaired surrogates (Cs) are stripped and do not crash UTF-8 encoding."""
    for code in range(0xD800, 0xDFFF + 1):
        ch = chr(code)
        cleaned = sanitize_name(f"a{ch}b")
        assert chr(code) not in cleaned
        # Must not crash on encode.
        cleaned.encode("utf-8")
