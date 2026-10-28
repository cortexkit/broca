#!/usr/bin/env python3
"""Refuse a deploy if a recent chooser-eligible credential lacks vault metadata."""
from __future__ import annotations

import argparse
from concurrent.futures import BrokenExecutor, ProcessPoolExecutor
import json
import pathlib
import sys
import time

from lib import credential_scan as scan
from lib.credential_scan import CannotCompare, chooser_selects_accounts
from lib.state_root import StateRootError, default_state_root

WEEK_MS = 7 * 24 * 60 * 60 * 1000


def live_logs(state_root: pathlib.Path) -> list[pathlib.Path]:
    # This guard counts recent credential use, even in moved/tombstoned sessions.
    # Population discovery must not read every log for lineage or archive integrity.
    paths = []
    for name in ("wal", "wal-ephemeral", "wal-durable"):
        try:
            paths.extend(path for path in (state_root / name).iterdir() if path.suffix == ".wal")
        except FileNotFoundError:
            continue
    return sorted(paths)


def recent_credentials(state_root: pathlib.Path, now_ms: int, days: int = 7) -> tuple[set[str], set[str], int, int, int]:
    """Combine low-priority tail scans without opening old files during discovery."""
    if days >= 14:
        raise CannotCompare("a live-only metadata scan requires a window shorter than the 14-day archive idle period")
    # Archives require 14 idle days, outside this guard's seven-day deploy window.
    paths = live_logs(state_root)
    cutoff = now_ms - days * 24 * 60 * 60 * 1000
    credentials: set[str] = set()
    skipped: set[str] = set()
    logs_read = logs_vanished = fallbacks = 0
    if paths:
        pool = ProcessPoolExecutor(max_workers=min(4, len(paths)), initializer=scan.lower_worker_priority)
        try:
            for result in pool.map(scan.scan_wal, ((path, cutoff) for path in paths), chunksize=32):
                if result.error is not None:
                    raise CannotCompare(result.error)
                credentials.update(result.credentials)
                skipped.update(result.skipped)
                logs_read += result.logs_read
                logs_vanished += result.vanished
                fallbacks += result.fallbacks
        finally:
            pool.shutdown(wait=True, cancel_futures=True)
    if not credentials and not skipped:
        raise CannotCompare(f"no credentials recorded in broca runs in the last {days} days")
    return credentials, skipped - credentials, logs_read, logs_vanished, fallbacks


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
        credentials, skipped, logs_read, logs_vanished, fallbacks = recent_credentials(args.state_root or default_state_root(), int(time.time() * 1000), args.days)
        listing = json.loads(args.vault_listing.read_text())
        missing = compare(credentials, listing)
    except (CannotCompare, scan.ArchiveRefusal, OSError, ValueError, StateRootError, BrokenExecutor) as error:
        print(f"credential-metadata: cannot compare: {scan.error_detail(error)}", file=sys.stderr)
        return 2
    if missing:
        print("credential-metadata: REFUSING deploy:", file=sys.stderr)
        for failure in missing:
            print(f"  {failure}", file=sys.stderr)
        return 1
    print(f"credential-metadata: PASS; {len(credentials)} recently used credentials selected by the chooser "
          f"have provider_ids and auth_method; read {logs_read} logs in {time.monotonic() - started:.2f}s; "
          f"{logs_vanished} logs vanished during the scan; "
          f"{fallbacks} full forward fallbacks; "
          f"skipped as not chooser-selected: {', '.join(sorted(skipped)) if skipped else '(none)'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
