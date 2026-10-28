"""Importable, low-priority per-WAL work for the deployment metadata guard."""
from __future__ import annotations

import errno
import json
import os
import pathlib
from typing import NamedTuple

from .archive_read import ArchiveRefusal, read_records, read_records_reverse


class CannotCompare(Exception):
    pass


class ScanResult(NamedTuple):
    credentials: set[str]
    skipped: set[str]
    logs_read: int = 0
    vanished: int = 0
    fallbacks: int = 0
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


def _include(selection, credentials: set[str], skipped: set[str]):
    if selection is None:
        return
    if not isinstance(selection, dict):
        raise CannotCompare("an auth selection is not an object")
    credential = selection.get("resolved_credential_id") or selection.get("credential_id")
    if not isinstance(credential, str) or not credential:
        raise CannotCompare("a recent auth selection has no credential id")
    (credentials if chooser_selects_accounts(selection) else skipped).add(credential)


def scan_forward(path: pathlib.Path, cutoff: int) -> tuple[set[str], set[str]]:
    """The full-history attribution path, also the oracle for bounded-tail tests."""
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
            _include(current, credentials, skipped)
    return credentials, skipped


def scan_reverse(path: pathlib.Path, cutoff: int) -> tuple[set[str], set[str]] | None:
    """Return tail attribution, or request a full scan for an ambiguous boundary.

    Writer activity is ordered, but queued admission times can precede earlier
    records. An old queued admission or untimed boundary cannot establish the
    cutoff, so those require the full forward path.
    This deploy guard validates every frame it uses, not an unread prefix:
    recovery and wal-verify own integrity outside the recent-credential window.
    """
    credentials: set[str] = set()
    skipped: set[str] = set()
    pending = False
    for frame in read_records_reverse(path):
        record = frame.record
        kind = record["type"]
        if kind in ("run_finished", "run_sealed"):
            # Records after a terminal have no active selection; the terminal
            # itself belongs to the selection immediately before it.
            pending = _recent(record, cutoff)
        elif kind in ("run_started", "run_auth_refrozen"):
            pending = pending or _recent(record, cutoff)
            if pending:
                _include(_selection(record), credentials, skipped)
            pending = False
            timestamp = record.get("written_at_ms" if kind == "run_started" else "ts_ms")
            if type(timestamp) is not int or timestamp <= 0:
                return None
            if timestamp < cutoff:
                if _recent(record, cutoff) or (kind == "run_started" and record.get("submission_id") is not None):
                    return None
                return credentials, skipped
        elif _recent(record, cutoff):
            pending = True
    return credentials, skipped


def scan_wal(task: tuple[pathlib.Path, int]) -> ScanResult:
    """Keep errors structured before process-pool pickling loses their cause chain."""
    path, cutoff = task
    try:
        try:
            # Append-only live logs not modified in the window need no file reads.
            if path.stat().st_mtime_ns < cutoff * 1_000_000:
                return ScanResult(set(), set())
            selected = scan_reverse(path, cutoff)
            fallback = selected is None
            if fallback:
                selected = scan_forward(path, cutoff)
            assert selected is not None
            return ScanResult(*selected, logs_read=1, fallbacks=int(fallback))
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
                return ScanResult(set(), set(), vanished=1)
            raise
    except (CannotCompare, ArchiveRefusal, OSError, ValueError) as error:
        return ScanResult(set(), set(), error=error_detail(error))
