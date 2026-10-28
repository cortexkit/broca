#!/usr/bin/env python3
"""Refuse a deploy if a recently used credential lacks vault routing metadata."""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time

from lib.archive_read import ArchiveRefusal, logical_wals, read_records
from lib.state_root import StateRootError, default_state_root

WEEK_MS = 7 * 24 * 60 * 60 * 1000


class CannotCompare(Exception):
    pass


def recent_credentials(state_root: pathlib.Path, now_ms: int, days: int = 7) -> set[str]:
    """Include selections on recent runs, refreezes, and activity in older runs.

    The shared reader includes cold archives and rejects incomplete WAL snapshots.
    A deployment must not certify metadata against a partial history.
    """
    credentials: set[str] = set()
    cutoff = now_ms - days * 24 * 60 * 60 * 1000
    for wal in logical_wals(state_root / "wal"):
        selection = None
        for frame in read_records(wal):
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
            if not isinstance(current_selection, dict):
                raise CannotCompare("an auth selection is not an object")
            timestamp = record.get("ts_ms")
            if not isinstance(timestamp, int):
                if kind in ("run_started", "run_auth_refrozen"):
                    raise CannotCompare("an auth selection has no recorded timestamp")
                continue
            if timestamp < cutoff:
                continue
            # The recorded resolved_credential_id avoids resolving old selections through
            # today's aliases. Older records without it use their credential_id unchanged.
            credential = current_selection.get("resolved_credential_id") or current_selection.get("credential_id")
            if not isinstance(credential, str) or not credential:
                raise CannotCompare("a recent auth selection has no credential id")
            credentials.add(credential)
    if not credentials:
        raise CannotCompare(f"no credentials recorded in broca runs in the last {days} days")
    return credentials


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
    try:
        credentials = recent_credentials(args.state_root or default_state_root(), int(time.time() * 1000), args.days)
        listing = json.loads(args.vault_listing.read_text())
        missing = compare(credentials, listing)
    except (CannotCompare, ArchiveRefusal, OSError, ValueError, StateRootError):
        # Never echo a JSON parse exception or WAL contents: only the
        # projected comparison below is safe to put in deploy logs.
        print("credential-metadata: cannot compare recent broca auth selections with the vault listing", file=sys.stderr)
        return 2
    if missing:
        print("credential-metadata: REFUSING deploy:", file=sys.stderr)
        for failure in missing:
            print(f"  {failure}", file=sys.stderr)
        return 1
    print(f"credential-metadata: PASS; {len(credentials)} recently used credentials have provider_ids and auth_method")
    return 0


if __name__ == "__main__":
    sys.exit(main())
