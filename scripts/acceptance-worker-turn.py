#!/usr/bin/env python3
"""Send a fetch plan with a session class; verify completion and both required WAL features."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

from lib.archive_read import ArchiveRefusal, read_records
from lib.state_root import default_state_root

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/fixtures/acceptance-reader-plan.json"
CLASS = "acceptance"
REQUIRED = ("session-class/v1", "provider-declarations/v1")


def wal_name(project_root, harness, session):
    value = 0xCBF29CE484222325
    for byte in f"{project_root}\x1f{harness}\x1f{session}".encode():
        value = ((value ^ byte) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return f"{value:016x}.wal"


def check_wal(path, run_id):
    frames = read_records(path)
    starts = [frame for frame in frames if frame.record["type"] == "run_started"
              and frame.record.get("run_id") == run_id]
    if len(starts) != 1:
        raise ValueError(f"expected one RunStarted for {run_id}, found {len(starts)} in {path}")
    start = starts[0]
    missing = [feature for feature in REQUIRED if feature not in start.requires]
    if missing:
        raise ValueError("RunStarted missing required WAL feature(s): " + ", ".join(missing))
    if start.record.get("session_class") != CLASS:
        raise ValueError(f"RunStarted session_class is not {CLASS}")
    fetches = (start.record.get("manifest") or {}).get("fetches", [])
    declarations = [fetch.get("declarations_frozen") or {} for fetch in fetches]
    if not any(item.get("late_results") is True or item.get("result_formats") for item in declarations):
        raise ValueError("RunStarted manifest has no frozen result formats or late_results declaration")
    episode = []
    for frame in frames:
        if frame.seq <= start.seq:
            continue
        if frame.record["type"] == "run_started":
            break
        episode.append(frame.record)
    if not any(record["type"] == "run_finished" and record.get("reason") == "completed"
               for record in episode):
        raise ValueError(f"run {run_id} did not complete in the WAL")
    return start.requires


def invoke(command, timeout):
    result = subprocess.run(command, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise ValueError(f"broca-session exited {result.returncode}:\n{result.stdout}{result.stderr}")
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-bin", type=Path, default=ROOT / "target/release/broca-session")
    parser.add_argument("--subc", type=Path, default=Path.home() / ".local/share/cortexkit/run/subc-connection.json")
    parser.add_argument("--state-root", type=Path)
    parser.add_argument("--project-root", type=Path, default=ROOT)
    parser.add_argument("--model", default=os.environ.get("ACCEPTANCE_WORKER_MODEL", "deepseek/deepseek-v4-flash"))
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    try:
        state_root = args.state_root or default_state_root()
        project_root = args.project_root.resolve(strict=True)
        session = "acceptance-plan-" + uuid.uuid4().hex
        base = [str(args.session_bin), "--subc", str(args.subc), "--project-root", str(project_root),
                "--session", session, "--json-events"]
        # The class exercises the WAL gate without adding acceptance transcripts to Engram's backup.
        # Ask the serving daemon: a checkout's config (or its absence) cannot establish placement.
        description = json.loads(invoke(base + ["describe"], args.timeout))["result"]
        backup_classes = description["session_class"]["backup_classes"]
        if not isinstance(backup_classes, list) or not all(isinstance(item, str) for item in backup_classes):
            raise ValueError("role.describe returned invalid session_class.backup_classes")
        if CLASS in backup_classes:
            raise ValueError(f"class {CLASS} is in the daemon's backup_classes; refusing a backed-up acceptance turn")
        print(f"acceptance-worker-turn: class={CLASS}, daemon backup_classes={backup_classes}", flush=True)
        # The reader fetch-plan preset is legal unscoped; worker requires a daemon-attested session scope.
        # No tool call is needed: declarations freeze during admission, before the model turn.
        output = invoke(base + ["run", "--model", args.model, "--session-class", CLASS,
                                "--plan", str(PLAN), "--max-output-tokens", "32",
                                "--prompt", "Reply only OK. Do not call tools."], args.timeout)
        sends = [event for line in output.splitlines() if (event := json.loads(line)).get("event") == "send"]
        if len(sends) != 1 or sends[0].get("state") != "active":
            raise ValueError(f"send was not admitted as a new active run:\n{output}")
        run_id = sends[0]["run_id"]
        path = state_root / "wal" / wal_name(project_root, "broca", session)
        requires = check_wal(path, run_id)
        print(f"acceptance-worker-turn: PASS -- admitted, completed; {path} RunStarted requires {', '.join(requires)}")
        return 0
    except (ArchiveRefusal, OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as error:
        print(f"acceptance-worker-turn: FAILED -- {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
