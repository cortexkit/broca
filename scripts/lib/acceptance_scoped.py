#!/usr/bin/env python3
"""Certify post-restart scoped AFT dispatch without displaying transcript content."""

import argparse
import json
from pathlib import Path
import re
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.archive_read import ArchiveRefusal, read_records
from lib.state_root import StateRootError, default_state_root

ADDRESS = re.compile(r"[0-9a-f]{16}")
CODE = re.compile(r"[a-z][a-z0-9_]*")
ROUTE_CODE = re.compile(r"tool/route rejected: ([a-z][a-z0-9_]*) — ")


def error_code(record):
    """Use typed codes or a fixed output.message prefix, not arbitrary message text."""
    output = record.get("result", {}).get("output", {})
    value = output.get("value", {}) if output.get("kind") == "json" else {}
    if not isinstance(value, dict):
        value = {}
    nested = value.get("error", {})
    if not isinstance(nested, dict):
        nested = {}
    for code in (record.get("reason"), output.get("code"), value.get("code"), nested.get("code")):
        if isinstance(code, str) and CODE.fullmatch(code):
            return code
    match = ROUTE_CODE.match(output.get("message", ""))
    return match[1] if match else "unclassified_error"


def scoped_start(frame, after_ms):
    record = frame.record
    principal = record.get("principal") or {}
    scope = record.get("scope") or {}
    owner = scope.get("owner") or {}
    timestamp = record.get("ts_ms")
    return (isinstance(timestamp, int) and not isinstance(timestamp, bool) and timestamp > after_ms
            and principal.get("kind") == "reserved" and bool(principal.get("module_id"))
            and owner.get("kind") == "reserved" and bool(owner.get("module_id"))
            and bool(scope.get("ref")) and isinstance(scope.get("scope_epoch"), int)
            and "scope/v1" in frame.requires)


def summarize(frames, after_ms):
    """Join results to durable dispatch intents within their own qualifying episode."""
    active = False
    dispatches = {}
    successes, errors = 0, set()
    for frame in frames:
        record = frame.record
        kind = record["type"]
        if kind == "run_started":
            active = scoped_start(frame, after_ms)
            dispatches = {}
        elif kind == "run_finished":
            active = False
        elif active and kind == "tool_dispatch_intent":
            dispatches[(record.get("batch_id"), record.get("tool_call_id"))] = record.get("module_id")
        elif active and kind == "tool_result":
            result = record.get("result") or {}
            output = result.get("output") or {}
            provider = dispatches.get((record.get("batch_id"), result.get("tool_call_id")))
            # Some undelivered errors carry attribution without an intent. They are
            # failures, not evidence that AFT executed a call.
            if provider != "aft" and output.get("module") != "aft":
                continue
            if (provider == "aft" and result.get("is_error") is False
                    and record.get("outcome") == "completed"
                    and output.get("kind") not in ("error", "denied")):
                successes += 1
            else:
                errors.add(error_code(record))
    return successes, errors


def scan(state_root, after_ms, cache):
    summaries, unavailable = {}, {}
    # Cold archives cannot contain a run admitted after this restart. Sessions
    # driven by daemon-authenticated modules can use either backup policy or a
    # retention limit, so their WALs may live in any of the three live folders.
    for folder in ("wal", "wal-durable", "wal-ephemeral"):
        for path in sorted((state_root / folder).glob("*.wal")):
            address = path.stem
            if not ADDRESS.fullmatch(address):
                continue
            try:
                stat = path.stat()
                if stat.st_mtime_ns // 1_000_000 < after_ms:
                    continue
                identity = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
                if cache.get(path, (None,))[0] != identity:
                    cache[path] = (identity, summarize(read_records(path), after_ms))
                summaries[address] = cache[path][1]
            except FileNotFoundError:
                # A WAL can be removed after enumeration but before it is opened.
                continue
            except (ArchiveRefusal, ValueError, KeyError, TypeError, AttributeError):
                # An append may have a torn tail. Retry the snapshot, but never
                # certify its partial records or echo a decoder's message content.
                unavailable[address] = "unverified_wal"
            except OSError:
                unavailable[address] = "unreadable_wal"
    return summaries, unavailable


def wait_for_traffic(state_root, after_ms, timeout=600, poll=5):
    deadline = time.monotonic() + timeout
    cache, seen_errors = {}, {}
    while True:
        summaries, unavailable = scan(state_root, after_ms, cache)
        for address, (successes, errors) in summaries.items():
            if successes:
                print(f"acceptance-scoped: PASS -- session={address} successful_aft_results={successes}")
                return 0
            if errors:
                seen_errors.setdefault(address, set()).update(errors)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(poll, remaining))
    if seen_errors:
        for address, errors in sorted(seen_errors.items()):
            print(f"acceptance-scoped: FAILED -- session={address} successful_aft_results=0 "
                  f"error_codes={','.join(sorted(errors))}", file=sys.stderr)
    elif unavailable:
        for address, code in sorted(unavailable.items()):
            print(f"acceptance-scoped: FAILED -- session={address} error_codes={code}", file=sys.stderr)
    else:
        print("acceptance-scoped: FAILED -- no scoped traffic was seen after restart "
              "(no qualifying AFT tool results)", file=sys.stderr)
    return 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", type=Path)
    parser.add_argument("--placement-report", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()
    try:
        after_ms = json.loads(args.placement_report.read_text())["restart_observed_at_ms"]
        if not isinstance(after_ms, int) or isinstance(after_ms, bool) or after_ms <= 0:
            raise ValueError
        if not 0 <= args.timeout <= 600:
            raise ValueError
        return wait_for_traffic(args.state_root or default_state_root(), after_ms, args.timeout)
    except (OSError, ValueError, KeyError, TypeError, StateRootError):
        # Report only the evidence category, not content from a report or WAL.
        print("acceptance-scoped: FAILED -- invalid restart boundary or unavailable WAL tree",
              file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
