#!/usr/bin/env python3
"""Certify post-restart scoped AFT dispatch without displaying transcript content."""

import argparse
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.state_root import StateRootError, default_state_root

ADDRESS = re.compile(r"[0-9a-f]{16}")
CODE = re.compile(r"[a-z][a-z0-9_]*")
ROUTE_CODE = re.compile(r"tool/route rejected: ([a-z][a-z0-9_]*) — ")
ROOT = Path(__file__).resolve().parents[2]


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


def store_addresses(state_root, after_ms):
    """Discover recent sessions using layout metadata, never encrypted file contents."""
    addresses = set()
    for folder in ("session-store", "session-store-durable", "session-store-ephemeral"):
        logs = state_root / folder / "logs"
        if not logs.exists():
            continue
        for path in logs.iterdir():
            if not ADDRESS.fullmatch(path.name) or not path.is_dir():
                continue
            if not (state_root / folder / "heads" / path.name / "head").is_file():
                continue
            if (path.stat().st_mtime_ns // 1_000_000 >= after_ms
                    or any(log.stat().st_mtime_ns // 1_000_000 >= after_ms for log in path.iterdir()
                           if re.fullmatch(r"[0-9a-f]{32}", log.name) and log.is_file())):
                addresses.add(path.name)
    return addresses


def invoke(command, timeout):
    return subprocess.run(command, capture_output=True, text=True, timeout=timeout)


def query(base, op, params, deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ValueError(f"{op}: observation deadline reached")
    try:
        reply = invoke(base + ["raw", "--op", op, "--params", json.dumps(params)], min(30, remaining))
    except subprocess.TimeoutExpired as error:
        raise ValueError(f"{op}: client timed out") from error
    except OSError as error:
        raise ValueError(f"{op}: client unavailable ({type(error).__name__})") from error
    if reply.returncode:
        # Raw stdout may contain transcript data; only a typed error code is safe to report.
        code = ""
        try:
            value = json.loads(reply.stdout).get("error", {}).get("code")
            if isinstance(value, str) and CODE.fullmatch(value):
                code = f" code={value}"
        except (ValueError, AttributeError):
            pass
        raise ValueError(f"{op}: client exited {reply.returncode}{code}")
    try:
        result = json.loads(reply.stdout)["result"]
        if not isinstance(result, dict):
            raise ValueError
        return result
    except (ValueError, KeyError, TypeError) as error:
        raise ValueError(f"{op}: invalid daemon reply") from error


PAGE = 500


def read_messages(base, deadline, after_ms):
    """The session's messages from its newest back to the first one written by `after_ms`: the
    newest page, then older pages only while the oldest message read is newer than the restart.
    Reading a head's whole history instead cost the daemon a full decode of the session for every old
    page while the head was running, every poll, for the length of the check."""
    messages, version, params = [], None, {"limit": PAGE}
    while True:
        params["include_display_facts"] = True
        if version is not None:
            params["history_version"] = version
        page = query(base, "session.read", params, deadline)
        rows = page["messages"]
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise ValueError("session.read: invalid messages")
        messages[:0] = rows
        version = page.get("history_version")
        if not rows:
            return messages
        oldest = rows[0]
        ordinal, written = oldest.get("ordinal"), oldest.get("written_at")
        if not isinstance(ordinal, int) or isinstance(ordinal, bool) or ordinal < 1:
            raise ValueError("session.read: invalid ordinal")
        # A message with no write time, or one written by the restart, ends the walk back: the
        # results the check counts were all written after the restart.
        if (ordinal == 1 or not isinstance(written, int) or isinstance(written, bool)
                or written <= after_ms):
            return messages
        start = max(1, ordinal - PAGE)
        params = {"from_ordinal": start, "limit": ordinal - start}


def store_scope(admission, run_id, after_ms):
    if admission.get("run_id") != run_id or admission.get("storage") != "store":
        raise ValueError("run.admission: invalid run identity or storage")
    timestamp = admission["started_at_ms"]
    if not isinstance(timestamp, int) or isinstance(timestamp, bool):
        raise ValueError("run.admission: invalid start time")
    if timestamp <= after_ms:
        return False
    scope = admission.get("scope")
    if scope is None:
        raise ValueError("this daemon's run.admission serves no scope")
    principal, owner = scope.get("principal") or {}, scope.get("owner") or {}
    return (principal.get("kind") == "reserved" and bool(principal.get("module_id"))
            and owner.get("kind") == "reserved" and bool(owner.get("module_id"))
            and bool(scope.get("ref")) and isinstance(scope.get("epoch"), int))


def store_calls(base, run_id, deadline):
    calls, after = {}, None
    while True:
        params = {"run_id": run_id, "limit": 500}
        if after is not None:
            params["after"] = after
        page = query(base, "run.tool_calls", params, deadline)
        if page.get("run_id") != run_id:
            raise ValueError("run.tool_calls: invalid run identity")
        for call in page["calls"]:
            key = call.get("call_key")
            if key:
                calls[key] = call
        following = page.get("next")
        if following is None:
            return calls
        if not isinstance(following, str) or not following or following == after:
            raise ValueError("run.tool_calls: invalid continuation")
        after = following


def summarize_store(base, messages, after_ms, deadline):
    admissions, calls, dispatches = {}, {}, {}
    dispatch_run = None
    successes, errors = 0, set()
    for entry in messages:
        run_id = (entry.get("run") or {}).get("run_id")
        message = entry.get("message") or {}
        if message.get("role") == "assistant":
            # Call ids can repeat in another step; a durable call key identifies one dispatch.
            dispatches = {call["tool_call_id"]: call.get("call_key")
                          for call in entry.get("tool_calls", [])}
            dispatch_run = run_id
            continue
        timestamp = entry.get("written_at")
        if (message.get("role") != "tool" or not isinstance(timestamp, int)
                or isinstance(timestamp, bool) or timestamp <= after_ms):
            continue
        if not run_id:
            raise ValueError("session.read: tool result has no producing run")
        if run_id not in admissions:
            admission = query(base, "run.admission", {"run_id": run_id}, deadline)
            admissions[run_id] = store_scope(admission, run_id, after_ms)
            if admissions[run_id]:
                calls[run_id] = store_calls(base, run_id, deadline)
        if not admissions[run_id]:
            continue
        for result in message.get("content", []):
            if result.get("type") != "tool_result":
                continue
            key = dispatches.get(result.get("tool_call_id")) if dispatches and dispatch_run == run_id else None
            call = calls[run_id].get(key)
            if call is None:
                raise ValueError("session.read: tool result has no recorded dispatch attribution")
            if call.get("dispatched_to") is None:
                raise ValueError("run.tool_calls: daemon serves no dispatch module")
            if call.get("dispatched_to") != "aft":
                continue
            output = result.get("output") or {}
            if (result.get("is_error") is False and call.get("outcome") == "succeeded"
                    and output.get("kind") not in ("error", "denied")):
                successes += 1
            else:
                errors.add(error_code({"result": result}))
    return successes, errors


def scan_store(state_root, after_ms, session_bin, subc, deadline):
    summaries, unavailable = {}, {}
    try:
        addresses = store_addresses(state_root, after_ms)
        if not addresses:
            return summaries, unavailable
        db = sqlite3.connect((state_root / "run-index.db").resolve().as_uri() + "?mode=ro", timeout=1, uri=True)
        try:
            for address in sorted(addresses):
                try:
                    binds = db.execute("SELECT DISTINCT project_root,harness,session FROM agent_data_session_ref "
                                       "WHERE address=?", (address,)).fetchall()
                    if len(binds) != 1 or not all(isinstance(part, str) for part in binds[0]):
                        raise ValueError("ambiguous or missing bind identity")
                    project, harness, session = binds[0]
                    base = [str(session_bin), "--subc", str(subc), "--project-root", project,
                            "--harness", harness, "--session", session]
                    messages = read_messages(base, deadline, after_ms)
                    summaries[address] = summarize_store(base, messages, after_ms, deadline)
                except ValueError as error:
                    unavailable[address] = str(error)
                except (KeyError, TypeError, AttributeError):
                    unavailable[address] = "invalid daemon metadata"
        finally:
            db.close()
    except sqlite3.Error:
        unavailable["store_inventory"] = "unreadable store bind inventory"
    except OSError:
        unavailable["store_layout"] = "unreadable store layout metadata"
    return summaries, unavailable


def wait_for_traffic(state_root, after_ms, timeout=600, poll=5, session_bin=None, subc=None):
    deadline = time.monotonic() + timeout
    seen_errors = {}
    while True:
        stores, unavailable = scan_store(state_root, after_ms, session_bin, subc,
                                         deadline if timeout else time.monotonic() + 30)
        total = sum(count for count, _ in stores.values())
        evidence = (f"examined_sessions={len(stores)} qualifying_results={total} "
                    f"could_not_tell={len(unavailable)}")
        for address, error in sorted(unavailable.items()):
            print(f"acceptance-scoped: could not tell -- session={address} error={error}", file=sys.stderr)
        for address, (successes, errors) in stores.items():
            if successes:
                print(f"acceptance-scoped: PASS -- session={address} successful_aft_results={successes} {evidence}")
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
                  f"error_codes={','.join(sorted(errors))} {evidence}", file=sys.stderr)
    elif unavailable:
        print(f"acceptance-scoped: FAILED -- could not tell: daemon evidence unavailable; {evidence}", file=sys.stderr)
    elif not stores:
        print(f"acceptance-scoped: FAILED -- could not examine any session; {evidence}", file=sys.stderr)
    else:
        print("acceptance-scoped: FAILED -- no scoped traffic was seen after restart "
              f"(no qualifying AFT tool results); {evidence}", file=sys.stderr)
    return 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", type=Path)
    target_dir = Path(os.environ.get("CARGO_TARGET_DIR") or ROOT / "target")
    parser.add_argument("--session-bin", type=Path, default=target_dir / "release/broca-session")
    parser.add_argument("--subc", type=Path, default=Path.home() / ".local/share/cortexkit/run/subc-connection.json")
    parser.add_argument("--placement-report", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()
    try:
        after_ms = json.loads(args.placement_report.read_text())["restart_observed_at_ms"]
        if not isinstance(after_ms, int) or isinstance(after_ms, bool) or after_ms <= 0:
            raise ValueError
        if not 0 <= args.timeout <= 600:
            raise ValueError
        return wait_for_traffic(args.state_root or default_state_root(), after_ms, args.timeout,
                               session_bin=args.session_bin, subc=args.subc)
    except (OSError, ValueError, KeyError, TypeError, StateRootError):
        # Report only the evidence category, not content from a report or the store.
        print("acceptance-scoped: FAILED -- invalid restart boundary or unreadable state root",
              file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
