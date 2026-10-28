#!/usr/bin/env python3
"""Refuse a deploy if a recent chooser-eligible credential lacks vault metadata."""
from __future__ import annotations

import argparse
import errno
import json
import pathlib
import sys
import time

from lib.archive_read import ArchiveRefusal, logical_wals, read_records
from lib.state_root import StateRootError, default_state_root

WEEK_MS = 7 * 24 * 60 * 60 * 1000


class CannotCompare(Exception):
    pass


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


def recent_credentials(state_root: pathlib.Path, now_ms: int, days: int = 7) -> tuple[set[str], set[str], int, int]:
    """Return recent chooser-selected ids, skipped ids, read count, and vanish count.

    Include refreezes and recent activity in older runs.

    The shared reader rejects incomplete WAL snapshots among the recent live logs.
    A deployment must not certify metadata against a partial history.
    """
    if days >= 14:
        raise CannotCompare("a live-only metadata scan requires a window shorter than the 14-day archive idle period")
    credentials: set[str] = set()
    skipped: set[str] = set()
    logs_read = 0
    logs_vanished = 0
    cutoff = now_ms - days * 24 * 60 * 60 * 1000
    # Logs fold into archives only after 14 idle days, so an archive cannot contain
    # activity from the seven-day deploy window.
    for wal in logical_wals(state_root / "wal", include_archives=False):
        # A live WAL is append-only: without a modification since the cutoff it
        # cannot contain a record written during the deploy window.
        try:
            if wal.stat().st_mtime_ns < cutoff * 1_000_000:
                continue
            frames = read_records(wal)
        except FileNotFoundError as error:
            if error.errno != errno.ENOENT:
                raise
            # Retention can remove a listed session before its stat or read.
            # Confirm that the entry is gone; an existing entry or another error
            # is not evidence of retention and must still refuse the comparison.
            try:
                wal.lstat()
            except FileNotFoundError as recheck:
                if recheck.errno != errno.ENOENT:
                    raise
                logs_vanished += 1
                continue
            raise
        logs_read += 1
        selection = None
        for frame in frames:
            record = frame.record
            kind = record["type"]
            if kind == "run_started":
                config = record.get("config")
                selection = config.get("auth_selection") if isinstance(config, dict) else None
            elif kind == "run_auth_refrozen":
                selection = record.get("auth_selection")
            current_selection = selection
            if kind in ("run_finished", "run_sealed"):
                selection = None
            if current_selection is None:
                continue
            # RunStarted uses written_at_ms; RunAuthRefrozen uses ts_ms.
            # Legacy untimed records are not evidence of recency, but
            # a later timed record can still make their active selection recent.
            if not any(type(timestamp) is int and timestamp >= cutoff for timestamp in (
                record.get("written_at_ms"), record.get("ts_ms")
            )):
                continue
            if not isinstance(current_selection, dict):
                raise CannotCompare("an auth selection is not an object")
            # The recorded resolved_credential_id avoids resolving old selections through
            # today's aliases. Older records without it use their credential_id unchanged.
            credential = current_selection.get("resolved_credential_id") or current_selection.get("credential_id")
            if not isinstance(credential, str) or not credential:
                raise CannotCompare("a recent auth selection has no credential id")
            if chooser_selects_accounts(current_selection):
                credentials.add(credential)
            else:
                skipped.add(credential)
    if not credentials and not skipped:
        raise CannotCompare(f"no credentials recorded in broca runs in the last {days} days")
    return credentials, skipped - credentials, logs_read, logs_vanished


def os_error_detail(error: OSError) -> str:
    # OS error text is not needed: class, errno and quoted paths identify the
    # failed operation without echoing arbitrary exception text into deploy logs.
    path = json.dumps(str(error.filename)) if error.filename is not None else "(unavailable)"
    detail = f"{type(error).__name__}: errno={error.errno}, path={path}"
    if error.filename2 is not None:
        detail += f", path2={json.dumps(str(error.filename2))}"
    return detail


def compare(credentials: set[str], listing: object) -> list[str]:
    if not isinstance(listing, dict) or type(listing.get("v")) is not int or listing["v"] != 1:
        raise CannotCompare("vault listing has an unknown version")
    if not isinstance(listing.get("credentials"), list):
        raise CannotCompare("vault listing has no credentials array")
    rows = listing["credentials"]
    if not rows:
        raise CannotCompare("live vault listing is empty")
    indexed = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("id"), str):
            raise CannotCompare("vault listing contains a row without a credential id")
        if row["id"] in indexed:
            raise CannotCompare("vault listing contains duplicate credential ids")
        indexed[row["id"]] = row
    missing = []
    for credential in sorted(credentials):
        row = indexed.get(credential)
        if row is None:
            missing.append(f"{credential}: absent from live vault listing")
            continue
        fields = []
        providers = row.get("provider_ids")
        if not isinstance(providers, list) or not providers or not all(
            isinstance(provider, str) and provider.strip() for provider in providers
        ):
            fields.append("provider_ids")
        method = row.get("auth_method")
        if not isinstance(method, str) or not method.strip():
            fields.append("auth_method")
        if fields:
            missing.append(f"{credential}: missing {' and '.join(fields)}")
    return missing


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vault-listing", type=pathlib.Path, required=True,
                        help="version-1 JSON output from ck auth list --json")
    parser.add_argument("--state-root", type=pathlib.Path)
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()
    if args.days <= 0:
        parser.error("--days must be positive")
    started = time.monotonic()
    try:
        credentials, skipped, logs_read, logs_vanished = recent_credentials(args.state_root or default_state_root(), int(time.time() * 1000), args.days)
        listing = json.loads(args.vault_listing.read_text())
        missing = compare(credentials, listing)
    except CannotCompare as error:
        print(f"credential-metadata: cannot compare: {error}", file=sys.stderr)
        return 2
    except ArchiveRefusal as error:
        message = str(error)
        cause = error.__cause__
        while cause is not None:
            if isinstance(cause, (json.JSONDecodeError, UnicodeDecodeError)):
                message = message.replace(str(cause), "[parser detail withheld]")
            elif isinstance(cause, OSError):
                detail = os_error_detail(cause)
                message = message.replace(str(cause), detail) if str(cause) in message else f"{message}; {detail}"
            cause = cause.__cause__
        reason = f"{type(error).__name__}: {message}"
        print(f"credential-metadata: cannot compare: {reason}", file=sys.stderr)
        return 2
    except OSError as error:
        print(f"credential-metadata: cannot compare: {os_error_detail(error)}", file=sys.stderr)
        return 2
    except (ValueError, StateRootError) as error:
        # JSON parser messages can include input text; expose only the error class.
        print(f"credential-metadata: cannot compare: invalid or unreadable input ({type(error).__name__})", file=sys.stderr)
        return 2
    if missing:
        print("credential-metadata: REFUSING deploy:", file=sys.stderr)
        for failure in missing:
            print(f"  {failure}", file=sys.stderr)
        return 1
    print(f"credential-metadata: PASS; {len(credentials)} recently used credentials selected by the chooser "
          f"have provider_ids and auth_method; read {logs_read} logs in {time.monotonic() - started:.2f}s; "
          f"{logs_vanished} logs vanished during the scan; "
          f"skipped as not chooser-selected: {', '.join(sorted(skipped)) if skipped else '(none)'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
