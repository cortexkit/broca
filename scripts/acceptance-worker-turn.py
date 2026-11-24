#!/usr/bin/env python3
"""Verify a worker completed after admission recorded how providers can return tool results."""
import argparse
from collections import Counter
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import time

from lib.state_root import StateRootError, default_state_root

ROOT = Path(__file__).resolve().parents[1]
# Core labels its worker and research sessions mason and sidekick.
WORKER_CLASSES = ("mason", "sidekick")
ADDRESS = re.compile(r"[0-9a-f]{16}")
# Only RunFinishReason spellings from broca-protocol may reach diagnostic output.
FINISH_REASONS = frozenset(("completed", "max_steps", "budget_exhausted", "context_ceiling",
                            "cancelled", "interrupted", "error", "transform_unavailable"))


def store_addresses(state_root, after_ms):
    addresses = set()
    # Appending to an existing log changes the file mtime, not its folder's mtime.
    # Stat both; neither the encrypted head nor log bytes belong in this observer.
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


def store_summary(admission, status, after_ms):
    timestamp, session_class = admission["started_at_ms"], admission.get("session_class")
    if not isinstance(timestamp, int) or isinstance(timestamp, bool) or admission["storage"] != "store":
        raise ValueError("invalid store admission")
    if timestamp <= after_ms or session_class not in WORKER_CLASSES:
        return None
    fetches = admission["fetches"]
    if not isinstance(fetches, list) or not all(isinstance(fetch, dict) for fetch in fetches):
        raise ValueError("invalid store fetches")
    aft = [fetch for fetch in fetches if fetch.get("provider") == "aft"]
    error = None
    if aft and not all(isinstance(fetch.get("declarations_frozen"), dict)
                       and (fetch["declarations_frozen"].get("late_results") is True
                            or fetch["declarations_frozen"].get("result_formats")) for fetch in aft):
        error = "RunStart AFT fetch has no frozen result formats or late_results declaration"
    reason = status["state"]
    if reason in ("active", "paused"):
        reason = None
    elif not isinstance(reason, str) or reason not in FINISH_REASONS:
        reason = "unclassified_terminal"
    # The store has a record schema rather than log feature requirements or a log sequence.
    return {"ts_ms": timestamp, "seq": "store", "class": session_class,
            "candidate": bool(aft), "reason": reason, "error": error}


def scan_store_workers(state_root, after_ms, session_bin, subc, deadline):
    summaries = {}
    try:
        addresses = store_addresses(state_root, after_ms)
    except OSError:
        return summaries, {"unreadable_store_layout"}
    if not addresses:
        return summaries, set()
    try:
        # mode=ro refuses missing databases and never creates or migrates inventory tables.
        db = sqlite3.connect((state_root / "run-index.db").resolve().as_uri() + "?mode=ro", timeout=1, uri=True)
        try:
            for address in sorted(addresses):
                binds = db.execute("SELECT DISTINCT project_root,harness,session FROM agent_data_session_ref "
                                   "WHERE address=?", (address,)).fetchall()
                if len(binds) != 1 or not all(isinstance(part, str) for part in binds[0]):
                    return summaries, {"unreadable_store_inventory"}
                project, harness, session = binds[0]
                base = [str(session_bin), "--subc", str(subc), "--project-root", project,
                        "--harness", harness, "--session", session]
                # The run index is a log projection; new-storage runs never appear in it. Ask the
                # daemon for the session's latest run instead. A log also changes without a new
                # run (idle warm-ups, seals, cleanup), and run.admission's start time then rules
                # that run out as a candidate.
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return summaries, {"store_observation_timeout"}
                head = json.loads(invoke(base + ["raw", "--op", "session.head", "--params", "{}"],
                                         min(30, remaining)))["result"]
                latest = (head.get("last_run_state") or {}).get("run_id")
                if latest is None:
                    continue
                if not isinstance(latest, str):
                    return summaries, {"unreadable_store_run_inventory"}
                for run_id in (latest,):
                    params = json.dumps({"run_id": run_id})
                    replies = []
                    for op in ("run.admission", "run.status"):
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            return summaries, {"store_observation_timeout"}
                        replies.append(json.loads(invoke(base + ["raw", "--op", op, "--params", params],
                                                         min(30, remaining)))["result"])
                    admission, status = replies
                    if admission["run_id"] != run_id:
                        return summaries, {"unverified_store_admission"}
                    summary = store_summary(admission, status, after_ms)
                    if summary is not None:
                        summaries[(address, run_id)] = summary
        finally:
            db.close()
    except sqlite3.Error:
        return summaries, {"unreadable_store_inventory"}
    except (OSError, ValueError, KeyError, TypeError, AttributeError, subprocess.TimeoutExpired):
        # Client errors can contain session content or connection details; print only our reason.
        return summaries, {"unreadable_store_client"}
    return summaries, set()


def worker_counts(observed):
    candidates = [summary for summary in observed.values() if summary["candidate"]]
    classes = Counter(summary["class"] for summary in candidates)
    ignored = Counter(summary["class"] for summary in observed.values() if not summary["candidate"])
    reasons = Counter(summary["reason"] for summary in candidates if summary["reason"] is not None)
    class_counts = ",".join(f"{name}:{classes[name]}" for name in WORKER_CLASSES)
    ignored_counts = ",".join(f"{name}:{ignored[name]}" for name in WORKER_CLASSES)
    terminal_counts = ",".join(f"{reason}:{reasons[reason]}" for reason in sorted(set(reasons) | {"completed"}))
    return (f"classes={','.join(WORKER_CLASSES)} candidates={len(candidates)} "
            f"class_counts={class_counts} pending={len(candidates) - sum(reasons.values())} "
            f"terminal_counts={terminal_counts} ignored_counts={ignored_counts}")


def wait_for_worker(state_root, after_ms, timeout=600, poll=5, *, session_bin=None, subc=None):
    deadline = time.monotonic() + timeout
    observed = {}
    while True:
        # A zero wait still takes one snapshot, including bounded daemon reads.
        store_deadline = deadline if timeout else time.monotonic() + 30
        store, unavailable = scan_store_workers(state_root, after_ms, session_bin, subc, store_deadline)
        for key, summary in store.items():
            observed[key] = dict(summary, address=key[0])
        counts = worker_counts(observed)
        # Validate every candidate that fetched the aft tool provider before passing.
        # A long-running worker need not finish, but a bad later start cannot hide
        # behind an earlier success in the same session log or another session's log.
        for summary in observed.values():
            if summary["error"] is not None:
                print(f"acceptance-worker-turn: FAILED -- session={summary['address']} "
                      f"class={summary['class']} start_seq={summary['seq']}; {summary['error']}; {counts}",
                      file=sys.stderr)
                return 1
        completed = next((summary for summary in observed.values()
                          if summary["candidate"] and summary["reason"] == "completed"), None)
        if completed is not None and not unavailable:
            print(f"acceptance-worker-turn: PASS -- admitted, completed; session={completed['address']} "
                   f"class={completed['class']}; RunStart frozen declarations verified via run.admission; "
                   f"{counts}", flush=True)
            return 0
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(poll, remaining))
    has_candidates = any(summary["candidate"] for summary in observed.values())
    failure = (",".join(sorted(unavailable)) if unavailable
               else "worker_completion_timeout" if has_candidates else "no_worker_candidate")
    print(f"acceptance-worker-turn: FAILED -- {failure}; no completed post-restart AFT candidate "
          f"within {timeout:g}s; {worker_counts(observed)}", file=sys.stderr)
    return 1


def observe_worker(args, state_root):
    timeout = args.timeout if args.timeout is not None else 600
    try:
        after_ms = json.loads(args.placement_report.read_text())["restart_observed_at_ms"]
        if not isinstance(after_ms, int) or isinstance(after_ms, bool) or after_ms <= 0:
            raise ValueError
        if not 0 <= timeout <= 600:
            raise ValueError
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        raise ValueError("worker acceptance requires a valid placement report "
                         "restart boundary and a timeout between 0 and 600 seconds") from None
    return wait_for_worker(state_root, after_ms, timeout, session_bin=args.session_bin, subc=args.subc)


class ClientFailure(ValueError):
    def __init__(self, result):
        self.result = result
        super().__init__(f"broca-session exited {result.returncode}:\n{result.stdout}{result.stderr}")


def invoke(command, timeout):
    result = subprocess.run(command, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise ClientFailure(result)
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    # The client stage-release built; CARGO_TARGET_DIR moves cargo's output folder when set.
    target_dir = Path(os.environ.get("CARGO_TARGET_DIR") or ROOT / "target")
    parser.add_argument("--session-bin", type=Path, default=target_dir / "release/broca-session")
    parser.add_argument("--subc", type=Path, default=Path.home() / ".local/share/cortexkit/run/subc-connection.json")
    parser.add_argument("--state-root", type=Path)
    parser.add_argument("--placement-report", type=Path, required=True,
                        help="the placement report whose restart boundary starts the observation")
    parser.add_argument("--timeout", type=float, help="seconds to observe traffic (default 600, max 600)")
    args = parser.parse_args()
    try:
        return observe_worker(args, args.state_root or default_state_root())
    except (OSError, ValueError, KeyError, TypeError, StateRootError, subprocess.TimeoutExpired) as error:
        print(f"acceptance-worker-turn: FAILED -- {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
