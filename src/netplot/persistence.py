"""Persistent background-collection state for netplot sessions.

A persistent (background) session is one the operator has marked as
restart-safe. Its **definition** — id, target, interval, name, persistent
flag, desired run/pause state, created_at — is written to a small JSON
file so the server can recreate it on next boot. Sample history is NOT
persisted: samples restart empty (long-term history is the next slice's
Prometheus job; per-round disk writes would be wasteful here).

Storage contract
----------------

- One JSON file at ``<data_dir>/sessions.json``.
- ``data_dir`` comes from ``NETPLOT_DATA_DIR`` (env), defaulting to
  ``<cwd>/data``. See :func:`default_data_dir`.
- Schema is versioned (``STATE_SCHEMA_VERSION``). An unknown version is
  ignored with a warning so a forward-incompatible file never crashes
  boot; the server starts empty instead.
- Writes are atomic (tmp file in the same dir + :func:`os.replace`) and
  happen ONLY on lifecycle events: create (if persistent), delete,
  pause, resume, persist-toggle, rename. Non-persistent sessions are
  simply absent from the file; when the persistent set becomes empty
  the file is removed so a boot without persistent sessions is a boot
  that reads no file.
- Missing file → normal fresh start. Present-but-unusable file (corrupt
  JSON, unknown schema version, oversized, wrong shape, OR any session
  entry that fails field validation) → **degraded mode**: the original
  bytes are quarantined (renamed aside), the valid entries are restored
  (capped by successful-restore count, not file offset), the store
  enters a read-only state that rejects subsequent persistence writes
  with :class:`PersistDegraded`, and ``/healthz`` surfaces
  ``persistence: "degraded"`` so monitoring can catch it. Resolution is
  operator-driven: review/remove the quarantine file and restart, which
  re-runs the read and clears degraded when the file is gone or clean.

The :class:`PersistentStore` is intentionally a narrow I/O adapter: it
reads/writes a list of validated dicts and knows nothing about
:class:`~netplot.session.TraceSession` internals. The
:class:`~netplot.session.SessionManager` owns the lifecycle and decides
when to call :meth:`PersistentStore.write`.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import shutil
import tempfile
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from .engine import validate_host

__all__ = [
    "DEFAULT_DATA_DIRNAME",
    "MAX_NAME_LEN",
    "MAX_STATE_FILE_BYTES",
    "STATE_FILENAME",
    "STATE_SCHEMA_VERSION",
    "PersistDegraded",
    "PersistentStore",
    "ReadResult",
    "default_data_dir",
    "sanitize_name",
]

log = logging.getLogger("netplot.persistence")

# Directory name under the process CWD when ``NETPLOT_DATA_DIR`` is unset.
# Kept as a constant so the spec/docs/README can reference one value.
DEFAULT_DATA_DIRNAME = "data"

# The single state file written inside the data dir.
STATE_FILENAME = "sessions.json"

# Bump when the on-disk shape changes in a backward-incompatible way. The
# reader ignores any file whose ``version`` is not exactly this value
# (warning logged, boot continues empty) so a newer file never crashes an
# older binary — and an older file is not silently misread.
STATE_SCHEMA_VERSION = 1

# Schema-v1 entries are deliberately closed.  Accepting an accidental field
# here would make a hand-edited/migrated definition look valid while silently
# dropping data on the next atomic rewrite.
_SESSION_ENTRY_KEYS = frozenset(
    {
        "id",
        "target",
        "interval_s",
        "name",
        "persistent",
        "desired_state",
        "created_at",
    }
)

# Keep the envelope closed for the same reason as session entries: silently
# dropping a newer envelope field during a later rewrite would lose data.
_STATE_KEYS = frozenset({"version", "sessions"})

# Maximum user-visible label length. The spec calls for ~60 chars; we strip
# control characters first, then hard-truncate, so a stored name can never
# exceed this even if a hand-edited file tries to push more through.
MAX_NAME_LEN = 60
# The normal file is only a few KB (MAX_SESSIONS definitions). Bound boot-time
# memory/JSON work when the file is corrupt or supplied by an untrusted mount.
MAX_STATE_FILE_BYTES = 1_048_576


class PersistDegraded(Exception):
    """Raised by :meth:`PersistentStore.write` / :meth:`PersistentStore.remove`
    when the store is in degraded/read-only mode.

    Degraded mode is entered on boot when the state file is present but
    unusable (corrupt, unknown schema version, oversized, wrong shape, OR
    any session entry failed validation). The original bytes are
    quarantined so an operator can recover them; until the quarantine is
    resolved (reviewed + removed, then restart) the store refuses to
    rewrite the file so the next whole-file rewrite cannot destroy the
    rejected-but-real definitions the operator has not yet reviewed.

    Carries the quarantine path so the caller/API can surface it.
    """


@dataclass(slots=True)
class ReadResult:
    """Tri-state outcome of :meth:`PersistentStore.read`.

    - ``status == "missing"``: no state file. Normal fresh start.
    - ``status == "valid"``: file present and fully usable; ``entries``
      lists every restored session dict.
    - ``status == "degraded"``: file present but partially or wholly
      unusable. ``entries`` carries whatever valid subset could be
      restored (possibly empty); ``quarantine_path`` is the path the
      original bytes were moved to (best-effort; ``None`` if quarantine
      failed). The store has entered read-only mode and subsequent
      persistence writes raise :class:`PersistDegraded`.

    ``skipped`` carries a human-readable reason per dropped entry so the
    caller (the manager / boot log) can name every rejected definition.
    """

    status: str
    entries: list[dict] = field(default_factory=list)
    quarantine_path: Path | None = None
    skipped: list[str] = field(default_factory=list)
    reason: str | None = None


# One leading-NUL / control-char strip is not enough for safety: a hostile
# or hand-edited name could embed arbitrary C0 controls, BOM, ZWSP, bidi
# overrides (U+202E, U+2066–U+2069), tags, or unpaired surrogates. The
# regex below covers the ASCII whitespace set; the CATEGORY-based pass in
# :func:`sanitize_name` is the authoritative filter (it strips every code
# point whose Unicode general category is Cc/Cf/Cs/Co). This regex is kept
# only as the second-stage whitespace-collapse helper.
_NAME_WHITESPACE_RE = re.compile(r"\s+")


# Unicode general categories stripped by :func:`sanitize_name`. ``Cc`` =
# controls (C0+C1, including tab/LF/CR/DEL), ``Cf`` = format (BOM, ZWSP,
# LTR/RTL marks, bidi overrides U+202E / U+2066–U+2069), ``Cs`` =
# surrogate (unpaired surrogates that would also break UTF-8 encoding),
# ``Co`` = private-use. A category-based filter is exhaustive where a
# hand-maintained code-point range list can silently miss Cf entries
# like U+202E / U+200B / U+FEFF and every Cs code point.
_NAME_STRIP_CATEGORIES = frozenset({"Cc", "Cf", "Cs", "Co"})


def default_data_dir() -> Path:
    """Resolve the persistence data directory.

    Order: ``$NETPLOT_DATA_DIR`` if set, else ``Path.cwd() / DEFAULT_DATA_DIRNAME``.

    The env var is read inside this function (not captured at import) so
    tests can set it per-process and so the same module serves a CLI
    started before the env is set and a container where the entrypoint
    sets it before the Python process starts.
    """
    env = os.environ.get("NETPLOT_DATA_DIR")
    if env:
        return Path(env)
    return Path.cwd() / DEFAULT_DATA_DIRNAME


def sanitize_name(value: object) -> str:
    """Normalize a free-text session name for storage/display.

    - ``None`` / non-str → ``""`` (caller treats empty as "use target").
    - Strips every Unicode control / format / surrogate / private-use
      code point by GENERAL CATEGORY (``Cc`` ``Cf`` ``Cs`` ``Co``) so the
      filter is exhaustive: bidi overrides (U+202E, U+2066–U+2069), BOM
      (U+FEFF), ZWSP (U+200B), tags (U+E0000+), and unpaired surrogates
      (which would also break UTF-8 encoding → repeatable persistence
      failure) are all removed. ASCII whitespace runs collapse to a
      single space; leading/trailing whitespace is trimmed.
    - Hard-truncated to :data:`MAX_NAME_LEN` AFTER normalization so the
      stored name can never exceed the cap.

    A category-based filter is used because a hand-maintained code-point
    range list can silently miss Cf entries (bidi overrides, ZWSP, BOM)
    and every Cs code point. The Unicode database is the authoritative
    source for "is this a control/format character".
    """
    if not isinstance(value, str):
        return ""
    # Replace each control/format/surrogate/private-use code point with a
    # space, then collapse whitespace runs and trim. Doing control→space
    # (not "") keeps visually adjacent tokens legible (e.g. "a\\tb" →
    # "a b", not "ab").
    cleaned_chars: list[str] = []
    for ch in value:
        if unicodedata.category(ch) in _NAME_STRIP_CATEGORIES:
            cleaned_chars.append(" ")
        else:
            cleaned_chars.append(ch)
    cleaned = _NAME_WHITESPACE_RE.sub(" ", "".join(cleaned_chars)).strip()
    if len(cleaned) > MAX_NAME_LEN:
        cleaned = cleaned[:MAX_NAME_LEN]
    return cleaned


class PersistentStore:
    """Read/write the ``sessions.json`` state file.

    The store is a narrow I/O adapter: it understands the file shape and
    schema version but knows nothing about :class:`TraceSession`. The
    caller (typically :class:`~netplot.session.SessionManager`) is
    responsible for translating between session objects and the dict
    shape returned by :meth:`read` / expected by :meth:`write`.

    Boot-time read is TRI-STATE (see :class:`ReadResult`):

    - **missing** — no file. Normal fresh start; the store is writable.
    - **valid** — file present and fully usable. The store is writable.
    - **degraded** — file present but partially or wholly unusable
      (corrupt JSON, unknown schema version, oversized, wrong shape, OR
      any session entry that failed validation). The original bytes are
      quarantined (best-effort rename to
      ``sessions.json.quarantine-<utc>``); valid entries are still
      restored so the operator does not lose them while the rejected
      ones await review. The store enters **read-only mode**: subsequent
      :meth:`write` / :meth:`remove` calls raise :class:`PersistDegraded`
      so the next whole-file rewrite cannot destroy the rejected-but-real
      definitions the operator has not yet reviewed. Resolution is
      operator-driven: review/remove the quarantine, then restart — the
      re-read clears degraded when the file is gone (or clean).

    The boot log is loud on degraded (WARNING with the path + quarantine
    path + per-entry skip reasons) so monitoring and operators can catch
    it; :meth:`is_degraded` lets the API surface it on ``/healthz``.
    """

    def __init__(self, data_dir: Path) -> None:
        self._dir = Path(data_dir)
        # Degraded/read-only flag. Set by :meth:`read` when the file is
        # present but unusable; cleared by a subsequent read that finds
        # the file absent (operator removed the quarantine + state) or
        # fully valid. In production ``read`` runs exactly once, at boot
        # inside ``SessionManager.restore``, before request serving; the
        # flag is only READ afterwards (``/healthz``, the write guards),
        # so the plain-bool read/write (atomic under the GIL) is seen by
        # the API as either the pre- or post-boot value, never torn.
        self._degraded: bool = False
        self._degraded_reason: str | None = None
        self._quarantine_path: Path | None = None

    @property
    def data_dir(self) -> Path:
        return self._dir

    @property
    def path(self) -> Path:
        return self._dir / STATE_FILENAME

    def is_degraded(self) -> bool:
        """True if the store is in degraded/read-only mode."""
        return self._degraded

    def degraded_reason(self) -> str | None:
        """Human-readable degraded reason (None when not degraded)."""
        return self._degraded_reason

    def quarantine_path(self) -> Path | None:
        """Path the original unusable bytes were moved to (None if N/A)."""
        return self._quarantine_path

    def read(self) -> ReadResult:
        """Read the state file and return a tri-state :class:`ReadResult`.

        - **missing**: no file present. Returns ``ReadResult("missing")``.
          The store stays writable (or clears a prior degraded flag, e.g.
          after the operator removed the quarantine + state and restarted).
        - **valid**: file present and every entry validated cleanly.
          Returns ``ReadResult("valid", entries=[...])`` and clears any
          prior degraded flag.
        - **degraded**: file present but unreadable as a whole OR any
          session entry failed validation. Quarantines the original
          bytes (best-effort), returns the valid subset in ``entries``,
          marks the store read-only (subsequent writes raise
          :class:`PersistDegraded`), and lists per-entry skip reasons in
          ``skipped``.

        Boot never crashes: every I/O / parse / validation failure is
        caught and folded into a degraded result.
        """
        def _unreadable(exc: OSError) -> ReadResult:
            # Unreadable/unstatable for reasons other than absence
            # (permissions, IO error): we cannot even get the bytes to
            # quarantine. Treat as degraded with no quarantine; operator
            # must investigate the path directly.
            reason = f"unreadable state file: {exc}"
            log.warning("netplot: %s: %s", self.path, reason)
            self._mark_degraded(reason, quarantine_path=None)
            return ReadResult(status="degraded", reason=reason)

        # Size gate BEFORE reading. MAX_STATE_FILE_BYTES exists to bound
        # boot-time memory when the file is corrupt or supplied by an
        # untrusted mount, so an oversized file must never be buffered in
        # full: stat() is O(1), and the quarantine below moves the bytes
        # (hard link, streaming-copy fallback) without reading them into
        # memory either.
        try:
            size = self.path.stat().st_size
        except FileNotFoundError:
            self._clear_degraded()
            return ReadResult(status="missing")
        except OSError as exc:
            return _unreadable(exc)

        # Oversized file: do NOT attempt to parse (bound boot-time work).
        # Quarantine the bytes so the operator can inspect / trim them.
        if size > MAX_STATE_FILE_BYTES:
            reason = (
                f"oversized state file ({size} bytes; max {MAX_STATE_FILE_BYTES})"
            )
            log.warning("netplot: %s: %s; quarantining", self.path, reason)
            qpath = self._quarantine()
            self._mark_degraded(reason, quarantine_path=qpath)
            return ReadResult(status="degraded", reason=reason, quarantine_path=qpath)

        try:
            with self.path.open("rb") as fh:
                # Bounded read: the stat above admitted at most
                # MAX_STATE_FILE_BYTES, but the file could grow between
                # the stat and this read. Never buffer more than the cap
                # plus one byte (the +1 lets the length check below
                # distinguish "fits" from "grew past the cap").
                raw = fh.read(MAX_STATE_FILE_BYTES + 1)
        except FileNotFoundError:
            self._clear_degraded()
            return ReadResult(status="missing")
        except OSError as exc:
            return _unreadable(exc)

        # Defense in depth for the stat->read race: a file that grew past
        # the cap mid-read is still rejected without being parsed, and at
        # most cap+1 bytes were ever buffered.
        if len(raw) > MAX_STATE_FILE_BYTES:
            reason = (
                f"oversized state file ({len(raw)} bytes read; "
                f"max {MAX_STATE_FILE_BYTES})"
            )
            log.warning("netplot: %s: %s; quarantining", self.path, reason)
            qpath = self._quarantine()
            self._mark_degraded(reason, quarantine_path=qpath)
            return ReadResult(status="degraded", reason=reason, quarantine_path=qpath)

        # Try to parse. Any failure → degrade + quarantine the original
        # bytes so the operator can recover them. RecursionError is not a
        # ValueError subclass: json.loads raises it for deeply-nested
        # input, and the byte cap above bounds size, not nesting depth.
        try:
            text = raw.decode("utf-8")
            payload = json.loads(text, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as exc:
            reason = f"corrupt state file: {exc}"
            log.warning("netplot: %s: %s; quarantining", self.path, reason)
            qpath = self._quarantine()
            self._mark_degraded(reason, quarantine_path=qpath)
            return ReadResult(status="degraded", reason=reason, quarantine_path=qpath)

        if not isinstance(payload, dict):
            reason = "state file root is not an object"
            log.warning("netplot: %s: %s; quarantining", self.path, reason)
            qpath = self._quarantine()
            self._mark_degraded(reason, quarantine_path=qpath)
            return ReadResult(status="degraded", reason=reason, quarantine_path=qpath)

        unknown_state_keys = set(payload) - _STATE_KEYS
        if unknown_state_keys:
            reason = f"unknown state field(s) {sorted(unknown_state_keys, key=repr)!r}"
            log.warning("netplot: %s: %s; quarantining", self.path, reason)
            qpath = self._quarantine()
            self._mark_degraded(reason, quarantine_path=qpath)
            return ReadResult(status="degraded", reason=reason, quarantine_path=qpath)

        version = payload.get("version")
        # JSON's bool is an int subclass in Python, and 1.0 compares equal to
        # 1.  The envelope version is an exact integer representation, not a
        # numerically equivalent value that could be silently coerced.
        if (
            not isinstance(version, int)
            or isinstance(version, bool)
            or version != STATE_SCHEMA_VERSION
        ):
            reason = (
                f"unknown schema version {version!r} (expected {STATE_SCHEMA_VERSION})"
            )
            log.warning("netplot: %s: %s; quarantining", self.path, reason)
            qpath = self._quarantine()
            self._mark_degraded(reason, quarantine_path=qpath)
            return ReadResult(status="degraded", reason=reason, quarantine_path=qpath)

        raw_sessions = payload.get("sessions")
        if not isinstance(raw_sessions, list):
            reason = "'sessions' is not a list"
            log.warning("netplot: %s: %s; quarantining", self.path, reason)
            qpath = self._quarantine()
            self._mark_degraded(reason, quarantine_path=qpath)
            return ReadResult(status="degraded", reason=reason, quarantine_path=qpath)

        # Per-entry validation. We collect BOTH the valid entries and the
        # per-entry skip reasons; if ANY entry is dropped the read is
        # degraded (the next whole-file rewrite would destroy the
        # rejected-but-real definition the operator has not reviewed).
        validated: list[dict] = []
        skipped: list[str] = []
        seen_ids: set[str] = set()
        for entry in raw_sessions:
            parsed, why = _validate_session_entry(entry)
            if parsed is not None:
                if parsed["id"] in seen_ids:
                    why = f"duplicate id={parsed['id']!r}"
                    parsed = None
                else:
                    seen_ids.add(parsed["id"])
            if parsed is not None:
                validated.append(parsed)
            else:
                # `why` already includes the id when known.
                skipped.append(why or "unknown reason")
                log.warning("netplot: %s: skipping entry (%s)", self.path, why)

        if skipped:
            reason = f"{len(skipped)} session entry(ies) dropped during validation"
            log.warning(
                "netplot: %s: %s; quarantining original definitions",
                self.path,
                reason,
            )
            qpath = self._quarantine()
            self._mark_degraded(reason, quarantine_path=qpath)
            return ReadResult(
                status="degraded",
                entries=validated,
                quarantine_path=qpath,
                skipped=skipped,
                reason=reason,
            )

        # Fully valid: clear any prior degraded flag (the operator may
        # have just removed the quarantine and restarted into a clean
        # file).
        self._clear_degraded()
        return ReadResult(status="valid", entries=validated)

    def write(self, sessions: Iterable[dict]) -> None:
        """Atomically write ``sessions`` (a list of validated dicts).

        The directory is created (``mkdir -p``) on first write; every
        newly-created parent directory is fsynced (see :func:`_fsync_new_parents`)
        so a crash cannot lose the hierarchy despite a successful mkdir.
        A tmp file in the SAME directory is written, flushed, and
        :func:`os.replace`'d into place so a reader never sees a partial
        file (the JSON parser would otherwise log a corrupt-file warning
        and discard real state).

        Raises :class:`PersistDegraded` when the store is in read-only /
        degraded mode — the next whole-file rewrite must NOT destroy the
        rejected definitions the operator has not yet reviewed.
        """
        self._check_writable()
        payload = {
            "version": STATE_SCHEMA_VERSION,
            "sessions": list(sessions),
        }
        text = json.dumps(payload, ensure_ascii=False)
        _fsync_new_parents(self._dir)
        # NamedTemporaryFile is opened O_EXCL inside the target dir, so a
        # parallel writer (e.g. a second worker in a future multi-worker
        # deploy) cannot collide on the tmp name. ``delete=False`` because
        # os.replace takes the name; we own cleanup on failure.
        tmp_fd, tmp_name = tempfile.mkstemp(
            prefix=STATE_FILENAME + ".",
            suffix=".tmp",
            dir=str(self._dir),
        )
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
                fh.write(text)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self.path)
            # Flush the directory entry as well as the file contents. Without
            # this, a power loss can roll the atomic name swap back.
            _fsync_dir(self._dir)
        except BaseException:
            # mkstemp created the file; if the write/replace failed, do not
            # leave the orphan tmp behind for the next writer to trip on.
            try:
                tmp_path.unlink()
            except OSError:
                pass
            raise

    def remove(self) -> None:
        """Delete the state file if it exists; no-op if absent.

        Raises :class:`PersistDegraded` in degraded mode — see
        :meth:`write`. (Removing the file is still a mutation of the
        durable representation; an unresolved quarantine means the
        operator has not yet confirmed the loss of the rejected entries
        is acceptable.)
        """
        self._check_writable()
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
        else:
            _fsync_dir(self._dir)

    # ---------------------------------------------------------- internal

    def _check_writable(self) -> None:
        if self._degraded:
            raise PersistDegraded(
                f"persistence store is degraded: {self._degraded_reason} "
                f"(quarantine at {self._quarantine_path}); resolve the "
                f"quarantine (review + remove) and restart before any "
                f"further persistence mutation"
            )

    def _mark_degraded(self, reason: str, quarantine_path: Path | None) -> None:
        self._degraded = True
        self._degraded_reason = reason
        self._quarantine_path = quarantine_path

    def _clear_degraded(self) -> None:
        self._degraded = False
        self._degraded_reason = None
        self._quarantine_path = None

    def _quarantine(self) -> Path | None:
        """Best-effort move the current state file aside and return its path.

        The original bytes are preserved (as a sibling
        ``sessions.json.quarantine-<utc>`` file) so the operator can
        review the rejected definitions and recover them manually. The
        move (not in-place rewrite) ensures a subsequent successful read
        cannot re-trigger degraded from the same bytes. Memory-safe for
        ANY file size: the primary path hard-links the file (O(1), no
        data copy), and the fallback for filesystems without links is an
        exclusive, fsynced STREAMING copy — the whole file is never
        buffered, so quarantining a hostile multi-GiB state file costs no
        more than the copy's chunk buffer. Best-effort: a failure to move
        is logged and ``None`` returned; the store still enters degraded
        mode (writes refused) — the operator must investigate the path
        directly in that case.
        """
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        try:
            self._dir.mkdir(parents=True, exist_ok=True)
            # Do not use Path.rename here: on POSIX rename replaces an
            # existing destination, so two corrupt boots in the same second
            # could destroy the only retained copy of an earlier file.  Make
            # a unique quarantine copy first, then remove the source only
            # after that copy is complete.  Hard-linking is atomic and keeps
            # the source intact until the final unlink; the exclusive
            # streaming-copy fallback covers filesystems that do not
            # support links.
            for suffix in range(1000):
                extra = "" if suffix == 0 else f"-{suffix}"
                target = self._dir / (f"{STATE_FILENAME}.quarantine-{stamp}{extra}")
                try:
                    os.link(self.path, target)
                except FileExistsError:
                    continue
                except OSError:
                    try:
                        # "xb" = O_EXCL create; copyfileobj streams in
                        # bounded chunks so even an oversized source is
                        # never fully buffered.
                        with open(target, "xb") as dst, open(self.path, "rb") as src:
                            shutil.copyfileobj(src, dst)
                            dst.flush()
                            os.fsync(dst.fileno())
                    except FileExistsError:
                        continue
                    except OSError:
                        try:
                            target.unlink()
                        except OSError:
                            pass
                        raise
                try:
                    self.path.unlink()
                except OSError:
                    # The source is still present, which is safer than
                    # deleting it after an uncertain quarantine operation.
                    pass
                _fsync_dir(self._dir)
                return target
            raise OSError("could not allocate a unique quarantine path")
        except OSError as exc:
            log.warning("netplot: failed to quarantine %s: %s", self.path, exc)
            return None


def _fsync_dir(path: Path) -> None:
    """fsync the directory entry for ``path`` (best-effort, logged).

    On platforms without ``O_DIRECTORY`` (Windows) the open will fail
    and we silently skip; on POSIX it ensures a rename/unlink/rename in
    ``path`` survives a power loss.
    """
    try:
        dir_fd = os.open(str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    except OSError as exc:  # pragma: no cover - platform-dependent
        log.warning("netplot: fsync of dir %s failed: %s", path, exc)
    finally:
        os.close(dir_fd)


def _fsync_new_parents(leaf: Path) -> None:
    """Create + fsync every newly-created parent of ``leaf``.

    ``Path.mkdir(parents=True)`` only guarantees durability of the leaf
    directory; a crash can lose the freshly-created parent chain above
    the first pre-existing ancestor. Walk down from the highest
    newly-created ancestor, mkdir+fsync each level so the whole
    hierarchy is durable. Idempotent: pre-existing parents are not
    re-created or re-fsynced.

    Needed when ``NETPLOT_DATA_DIR`` (or its parents) is created by the
    process on first write: without this, a successful first write can
    evaporate on power loss despite the leaf-dir fsync the atomic-write
    path already does.
    """
    # Build the chain of ancestors from the leaf up to the first
    # pre-existing one (inclusive of the leaf). Reverse so we mkdir
    # top-down (required: a parent must exist before its child).
    to_create: list[Path] = []
    cur = leaf
    while not cur.exists() and cur != cur.parent:
        to_create.append(cur)
        cur = cur.parent
    for p in reversed(to_create):
        try:
            p.mkdir(parents=False, exist_ok=True)
        except FileExistsError:
            # Raced with another creator; the dir is there now.
            continue
        _fsync_dir(p.parent)


# ----------------------------------------------------------------- validators
#
# Kept module-private (not in __all__) because callers go through the store.
# They are pure functions over a JSON-decoded dict so the read path is
# testable in isolation without spinning up a SessionManager.


def _validate_session_entry(entry: object) -> tuple[dict | None, str | None]:
    """Return ``(cleaned_dict, None)`` or ``(None, reason)``.

    A ``None`` first element drops the entry from the restored set (the
    manager will not recreate it). Each field is checked for both
    presence and a plausible type so a hand-edited file cannot crash
    restore with an unhandled exception. Sanitization of ``name`` runs
    again here so a stored name with control characters is normalized on
    read (defensive against a hand-edited or migrated file).

    The ``reason`` on rejection is human-readable and intended for the
    boot log so an operator can see WHICH entry was dropped and WHY — a
    bare ``None`` return made every silent drop indistinguishable and
    hid real definition loss behind an "ignoring" warning.
    """
    if not isinstance(entry, dict):
        return None, "entry is not a JSON object"
    unknown_keys = set(entry) - _SESSION_ENTRY_KEYS
    if unknown_keys:
        return None, f"unknown field(s) {sorted(unknown_keys, key=repr)!r}"
    sid = entry.get("id")
    target = entry.get("target")
    interval_s = entry.get("interval_s")
    desired_state = entry.get("desired_state")
    created_at = entry.get("created_at")
    if not isinstance(sid, str) or not sid:
        return None, "missing or non-string id"
    if re.fullmatch(r"[A-Za-z0-9._:-]+", sid) is None:
        return None, f"invalid id representation (id={sid!r})"
    if not isinstance(target, str) or not target:
        return None, f"missing or empty target (id={sid})"
    if validate_host(target) is None:
        return None, f"invalid target (id={sid})"
    if not isinstance(interval_s, (int, float)) or isinstance(interval_s, bool):
        return None, f"invalid interval_s={interval_s!r} (id={sid})"
    try:
        interval_value = float(interval_s)
    except OverflowError:
        return None, f"invalid interval_s={interval_s!r} (id={sid})"
    if not math.isfinite(interval_value) or interval_value < 1.0:
        return None, f"invalid interval_s={interval_s!r} (id={sid})"
    if desired_state not in ("running", "paused"):
        return None, f"invalid desired_state={desired_state!r} (id={sid})"
    if not isinstance(created_at, (int, float)) or isinstance(created_at, bool):
        return None, f"invalid created_at={created_at!r} (id={sid})"
    try:
        created_value = float(created_at)
    except OverflowError:
        return None, f"invalid created_at={created_at!r} (id={sid})"
    if not math.isfinite(created_value) or created_value < 0:
        return None, f"invalid created_at={created_at!r} (id={sid})"
    try:
        datetime.fromtimestamp(created_value, UTC)
    except (OverflowError, OSError, ValueError):
        return None, f"created_at out of range (id={sid})"
    if "name" not in entry:
        return None, f"missing name (id={sid})"
    if entry["name"] is not None and not isinstance(entry["name"], str):
        return None, f"invalid name type (id={sid})"
    if "persistent" not in entry or not isinstance(entry["persistent"], bool):
        return None, f"invalid persistent flag (id={sid})"
    if entry["persistent"] is not True:
        return None, f"persistent flag must be true (id={sid})"
    name = sanitize_name(entry.get("name"))
    return {
        "id": sid,
        "target": target,
        "interval_s": interval_value,
        "name": name,
        "persistent": True,
        "desired_state": desired_state,
        "created_at": created_value,
    }, None


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate JSON object members instead of silently keeping one.

    ``json.loads`` otherwise overwrites an earlier member, which could make a
    malformed envelope or entry appear valid while silently discarding data.
    Raising ``ValueError`` feeds the normal corrupt-file quarantine path.
    """
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object field {key!r}")
        result[key] = value
    return result
