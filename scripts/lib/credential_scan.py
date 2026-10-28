"""Importable, low-priority per-WAL work for the deployment metadata guard."""
from __future__ import annotations

import errno
import json
import mmap
import os
import pathlib
from typing import NamedTuple

from .archive_read import ArchiveRefusal, read_records


class CannotCompare(Exception):
    pass


class ScanResult(NamedTuple):
    credentials: set[str]
    skipped: set[str]
    logs_read: int = 0
    vanished: int = 0
    searched: int = 0
    matches: tuple[str, ...] = ()
    error: str | None = None


def lower_worker_priority():
    os.nice(10)


def chooser_selects_accounts(selection: dict) -> bool:
    # Mirror auth_routing::frozen_selection_credential, then account_routing::selects_accounts.
    # An explicit auth_method belongs to an opaque vault id, which must never be parsed.
    # Without it, AuthSelection's documented legacy format is a Broca method:provider[:account] key.
    method = selection.get("auth_method")
    if method is None:
        key = selection.get("credential_id")
        parts = key.split(":", 2) if isinstance(key, str) else []
        if len(parts) < 2 or not parts[1]:
            raise CannotCompare("a recent auth selection has no recognizable authentication method")
        method = parts[0]
    if method in ("oauth", "chatgpt"):
        return True
    if method in ("apikey", "antigravity"):
        return False
    raise CannotCompare("a recent auth selection has an unknown authentication method")


def os_error_detail(error: OSError) -> str:
    # Only class, errno and quoted paths are safe diagnostic evidence, not arbitrary OS text.
    path = json.dumps(str(error.filename)) if error.filename is not None else "(unavailable)"
    detail = f"{type(error).__name__}: errno={error.errno}, path={path}"
    if error.filename2 is not None:
        detail += f", path2={json.dumps(str(error.filename2))}"
    return detail


def error_detail(error: Exception) -> str:
    if isinstance(error, CannotCompare):
        return str(error)
    if isinstance(error, OSError):
        return os_error_detail(error)
    if isinstance(error, ArchiveRefusal):
        message = str(error)
        cause = error.__cause__
        while cause is not None:
            if isinstance(cause, (json.JSONDecodeError, UnicodeDecodeError)):
                message = message.replace(str(cause), "[parser detail withheld]")
            elif isinstance(cause, OSError):
                detail = os_error_detail(cause)
                message = message.replace(str(cause), detail) if str(cause) in message else f"{message}; {detail}"
            cause = cause.__cause__
        return f"{type(error).__name__}: {message}"
    return f"invalid or unreadable input ({type(error).__name__})"


def _recent(record: dict, cutoff: int) -> bool:
    return any(type(timestamp) is int and timestamp >= cutoff for timestamp in (
        record.get("written_at_ms"), record.get("ts_ms")
    ))


def _selection(record: dict):
    if record["type"] == "run_started":
        config = record.get("config")
        return config.get("auth_selection") if isinstance(config, dict) else None
    return record.get("auth_selection")


def _include(selection, credentials: set[str], skipped: set[str], targets: set[str] | None = None):
    if selection is None:
        return
    if not isinstance(selection, dict):
        raise CannotCompare("an auth selection is not an object")
    credential = selection.get("resolved_credential_id") or selection.get("credential_id")
    if not isinstance(credential, str) or not credential:
        raise CannotCompare("a recent auth selection has no credential id")
    if targets is not None and credential not in targets:
        return
    (credentials if chooser_selects_accounts(selection) else skipped).add(credential)


def scan_forward(path: pathlib.Path, cutoff: int, targets: set[str] | None = None) -> tuple[set[str], set[str]]:
    """Find credentials selected for recent run events, including auth refreezes.

    When supplied, targets limits attribution to the deficient vault rows.
    """
    credentials: set[str] = set()
    skipped: set[str] = set()
    selection = None
    for frame in read_records(path):
        record = frame.record
        kind = record["type"]
        if kind in ("run_started", "run_auth_refrozen"):
            selection = _selection(record)
        current = selection
        if kind in ("run_finished", "run_sealed"):
            selection = None
        if _recent(record, cutoff):
            _include(current, credentials, skipped, targets)
    return credentials, skipped


def quoted_ids(credentials) -> tuple[tuple[str, tuple[bytes, ...]], ...]:
    """Match canonical Rust UTF-8 JSON strings and the usual ASCII-escaped form."""
    return tuple((credential, tuple(sorted({
        json.dumps(credential, ensure_ascii=False).encode("utf-8"),
        json.dumps(credential, ensure_ascii=True).encode("utf-8"),
    }))) for credential in sorted(credentials))


def scan_wal(task) -> ScanResult:
    """Search bytes first; only candidate logs pay for full verified decoding."""
    path, cutoff, patterns = task
    searched = 0
    try:
        try:
            # Append-only logs not modified in the window cannot contain recent use.
            if path.stat().st_mtime_ns < cutoff * 1_000_000:
                return ScanResult(set(), set())
            with path.open("rb") as handle:
                size = os.fstat(handle.fileno()).st_size
                searched = 1
                if size == 0:
                    return ScanResult(set(), set(), searched=1)
                with mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ) as data:
                    matches = tuple(credential for credential, spellings in patterns
                                    if any(data.find(spelling) >= 0 for spelling in spellings))
            if not matches:
                return ScanResult(set(), set(), searched=1)
            # A quoted ID in tool output is not credential use. Decode every frame
            # of candidate logs and confirm a recent chooser-method selection.
            selected, skipped = scan_forward(path, cutoff, set(matches))
            return ScanResult(selected, skipped, logs_read=1, searched=1, matches=matches)
        except FileNotFoundError as error:
            if error.errno != errno.ENOENT:
                raise
            # Only a missing directory entry confirms retention won this race.
            # A dangling entry or a failed recheck must still refuse comparison.
            try:
                path.lstat()
            except FileNotFoundError as recheck:
                if recheck.errno != errno.ENOENT:
                    raise
                return ScanResult(set(), set(), vanished=1, searched=searched)
            raise
    except (CannotCompare, ArchiveRefusal, OSError, ValueError) as error:
        return ScanResult(set(), set(), error=f"{path}: {error_detail(error)}")
