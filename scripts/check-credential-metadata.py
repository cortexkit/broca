#!/usr/bin/env python3
"""Refuse deploys if a model credential with recent OAuth/ChatGPT use lacks routing metadata."""
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


def check_flagged_credentials(state_root: pathlib.Path, now_ms: int, flagged, days: int = 7) -> scan.ScanResult:
    """Byte-search recent live logs; decode only candidates for deficient vault rows."""
    if days >= 14:
        raise CannotCompare("a live-only metadata scan requires a window shorter than the 14-day archive idle period")
    # Archives require 14 idle days, outside this guard's seven-day deploy window.
    patterns = scan.quoted_ids(flagged)
    if not patterns:
        return scan.ScanResult(set(), set())
    paths = live_logs(state_root)
    cutoff = now_ms - days * 24 * 60 * 60 * 1000
    credentials: set[str] = set()
    skipped: set[str] = set()
    matches: set[str] = set()
    decoded = vanished = searched = 0
    if paths:
        pool = ProcessPoolExecutor(max_workers=min(4, len(paths)), initializer=scan.lower_worker_priority)
        try:
            tasks = ((path, cutoff, patterns) for path in paths)
            for result in pool.map(scan.scan_wal, tasks, chunksize=32):
                if result.error is not None:
                    raise CannotCompare(result.error)
                credentials.update(result.credentials)
                skipped.update(result.skipped)
                matches.update(result.matches)
                decoded += result.logs_read
                vanished += result.vanished
                searched += result.searched
        finally:
            pool.shutdown(wait=True, cancel_futures=True)
    return scan.ScanResult(credentials, skipped - credentials, decoded, vanished, searched, tuple(sorted(matches)))


def flag_credentials(listing: object) -> dict[str, tuple[str, ...]]:
    if not isinstance(listing, dict) or type(listing.get("v")) is not int or listing["v"] != 1:
        raise CannotCompare("vault listing has an unknown version")
    if not isinstance(listing.get("credentials"), list):
        raise CannotCompare("vault listing has no credentials array")
    rows = listing["credentials"]
    seen: set[str] = set()
    flagged = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("id"), str) or not row["id"]:
            raise CannotCompare("vault listing contains a row without a credential id")
        credential = row["id"]
        if credential in seen:
            raise CannotCompare("vault listing contains duplicate credential ids")
        seen.add(credential)
        # The vault's llm-provider category defines model credentials. A missing
        # category is outside this guard's scope, not missing routing metadata.
        categories = row.get("categories")
        if not isinstance(categories, list) or "llm-provider" not in categories:
            continue
        if not scan.metadata_in_chooser_scope(row):
            continue
        fields = []
        providers = row.get("provider_ids")
        if not isinstance(providers, list) or not providers or not all(
            isinstance(provider, str) and provider.strip() for provider in providers
        ):
            fields.append("provider_ids")
        method = row.get("auth_method")
        if method is None:
            fields.append("auth_method")
        elif not isinstance(method, str):
            raise CannotCompare("vault listing auth_method is not a string")
        else:
            try:
                chooser_selects_accounts({"auth_method": method})
            except CannotCompare:
                fields.append("recognized auth_method")
        if fields:
            flagged[credential] = tuple(fields)
    # A used ID absent from this listing is an admission-resolution failure,
    # independent of the chooser; this guard only checks deficient live vault rows.
    return flagged


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
        flagged = flag_credentials(json.loads(args.vault_listing.read_text()))
        result = scan.ScanResult(set(), set())
        if flagged:
            result = check_flagged_credentials(args.state_root or default_state_root(),
                                               int(time.time() * 1000), flagged, args.days)
    except (CannotCompare, scan.ArchiveRefusal, OSError, ValueError, StateRootError, BrokenExecutor) as error:
        print(f"credential-metadata: cannot compare: {scan.error_detail(error)}", file=sys.stderr)
        return 2
    if result.credentials:
        print("credential-metadata: REFUSING deploy:", file=sys.stderr)
        for credential in sorted(result.credentials):
            print(f"  {credential}: missing {' and '.join(flagged[credential])}; confirmed recent chooser-method use",
                  file=sys.stderr)
        return 1
    reasons = []
    for credential, fields in sorted(flagged.items()):
        if credential in result.skipped:
            why = "only non-chooser-method use in the window"
        elif credential in result.matches:
            why = "no recent auth selection (text or older use only)"
        else:
            why = "not found in retained recent logs"
        reasons.append(f"{credential} (missing {' and '.join(fields)}; {why})")
    summary = '; '.join(reasons) if reasons else "none; all model-provider credentials have routing metadata"
    print(f"credential-metadata: PASS; elapsed {time.monotonic() - started:.2f}s; searched {result.searched} live logs; "
          f"decoded {result.logs_read} matching logs; {result.vanished} logs vanished during the scan; "
          f"flagged credentials: {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
