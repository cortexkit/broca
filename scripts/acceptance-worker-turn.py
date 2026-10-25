#!/usr/bin/env python3
"""Send a fetch plan with a session class; verify completion and both required WAL features."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid

from lib.archive_read import ArchiveRefusal, read_records
from lib.state_root import default_state_root

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "scripts/fixtures/acceptance-reader-plan.json"
CLASS = "acceptance"
REQUIRED = ("session-class/v1", "provider-declarations/v1")
MODELS_FILE = ROOT / "scripts/lib/acceptance-models.txt"


def wal_name(project_root, harness, session):
    value = 0xCBF29CE484222325
    for byte in f"{project_root}\x1f{harness}\x1f{session}".encode():
        value = ((value ^ byte) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return f"{value:016x}.wal"


def checked_run(path, run_id, *, finish_reason):
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
    terminals = [record for record in episode if record["type"] == "run_finished"]
    if len(terminals) != 1 or terminals[0].get("reason") != finish_reason:
        raise ValueError(f"run {run_id} did not complete in the WAL with reason {finish_reason}")
    return start.requires, terminals[0]


def check_wal(path, run_id):
    return checked_run(path, run_id, finish_reason="completed")[0]


class ClientFailure(ValueError):
    def __init__(self, result):
        self.result = result
        super().__init__(f"broca-session exited {result.returncode}:\n{result.stdout}{result.stderr}")


def invoke(command, timeout):
    result = subprocess.run(command, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise ClientFailure(result)
    return result.stdout


def candidates(pin):
    if pin is not None:
        if len(pin.split()) != 1:
            raise ValueError("--model/ACCEPTANCE_WORKER_MODEL must pin exactly one model")
        return [pin.strip()]
    models = (os.environ.get("ACCEPTANCE_MODELS") or MODELS_FILE.read_text()).split()
    if not models:
        raise ValueError(f"no acceptance candidate models in {MODELS_FILE}")
    return models


def events_from(output):
    events = [json.loads(line) for line in output.splitlines() if line.strip()]
    if not all(isinstance(event, dict) for event in events):
        raise ValueError("broca-session emitted a non-object JSON event")
    return events


def compose_plan(base, timeout, path):
    output = invoke(base + ["compose-plan", "--items", str(PLAN)], timeout)
    plan = json.loads(output)
    digest = plan["composition_digest"]
    for provider in plan["composition"]["providers"]:
        print(f"acceptance-worker-turn: provider={provider['provider']} "
              f"tools={len(provider['tools'])} digest={digest}", flush=True)
    path.write_text(output)


def stale_admission(failure):
    if failure.result.returncode != 1:
        return False
    events = events_from(failure.result.stdout)
    errors = [event for event in events if event.get("event") == "error"]
    # Only a send refusal may be retried, never an error from an admitted run or its stream.
    return (len(errors) == 1 and all(event.get("event") in ("bound", "error") for event in events)
            and errors[0].get("operation") == "session.send"
            and errors[0].get("code") == "plan_stale")


def composed_turn(base, command, timeout, path):
    for attempt in range(2):
        compose_plan(base, timeout, path)
        try:
            return invoke(command + ["--plan", str(path)], timeout)
        except ClientFailure as error:
            if not stale_admission(error) or attempt == 1:
                raise
            print("acceptance-worker-turn: catalog changed after composition; recomposing once: "
                  + error.result.stdout.strip(), flush=True)


def failed_run(output):
    events = events_from(output)
    if any(event.get("event") in ("error", "run_paused") for event in events):
        return None
    sends = [event for event in events if event.get("event") == "send"]
    starts = [event for event in events if event.get("event") == "run_started"]
    finishes = [event for event in events if event.get("event") == "run_finished"]
    errors = [event for event in events if event.get("event") == "loop_error"]
    if any(len(items) != 1 for items in (sends, starts, finishes, errors)):
        return None
    run_id = sends[0].get("run_id")
    if (not isinstance(run_id, str) or not run_id or sends[0].get("state") != "active"
            or starts[0].get("run_id") != run_id or finishes[0].get("run_id") != run_id
            or finishes[0].get("reason") != "Error"):
        return None
    return run_id, errors[0].get("class")


def provider_refusal(terminal, observed_class):
    # JSON-events drops provider_code. Read broca's canonical category from the verified
    # WAL terminal, never infer billing or quota from vendor text or an HTTP status.
    error = terminal.get("error")
    if not isinstance(error, dict) or not isinstance(error.get("message"), str):
        return None
    error_class, code = error.get("class"), error.get("provider_code")
    if not isinstance(error_class, str):
        return None
    if code is not None and not isinstance(code, str):
        return None
    expected_class = {"rate_limited": "transient", "account_limit_exhausted": "transient",
                      "billing_exhausted": "permanent", "credit_insufficient": "permanent",
                      "request_invalid": "permanent"}.get(code or "")
    if expected_class is not None and error_class != expected_class:
        return None
    debug_classes = {"transient": ("Transient",), "permanent": ("Permanent",),
                     "provider_unavailable": ("ProviderUnavailable", "Transient")}
    if observed_class not in debug_classes.get(error_class, ()):
        return None
    # Accept the WAL's transient/provider_unavailable classes for model-leg failures,
    # including transport/stream failures; the JSON class alone is not sufficient.
    # A permanent WAL error rotates only with broca's canonical billing_exhausted code.
    if error_class in ("transient", "provider_unavailable") or (error_class == "permanent" and code == "billing_exhausted"):
        return f"{error_class}/{code}" if code else error_class
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-bin", type=Path, default=ROOT / "target/release/broca-session")
    parser.add_argument("--subc", type=Path, default=Path.home() / ".local/share/cortexkit/run/subc-connection.json")
    parser.add_argument("--state-root", type=Path)
    parser.add_argument("--project-root", type=Path, default=ROOT)
    parser.add_argument("--model", default=os.environ.get("ACCEPTANCE_WORKER_MODEL"),
                        help="pin one model instead of rotating the shared acceptance candidates")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--compose-only", action="store_true",
                        help="fetch the live composition and print provider/tool counts and digest; send nothing")
    args = parser.parse_args()
    try:
        state_root = args.state_root or default_state_root()
        project_root = args.project_root.resolve(strict=True)
        base = [str(args.session_bin), "--subc", str(args.subc), "--project-root", str(project_root),
                "--json-events"]
        # The class exercises the WAL gate without adding acceptance transcripts to Engram's backup.
        # Ask the serving daemon: a checkout's config (or its absence) cannot establish placement.
        description = json.loads(invoke(base + ["--session", "acceptance-plan-" + uuid.uuid4().hex,
                                                "describe"], args.timeout))["result"]
        backup_classes = description["session_class"]["backup_classes"]
        if not isinstance(backup_classes, list) or not all(isinstance(item, str) for item in backup_classes):
            raise ValueError("role.describe returned invalid session_class.backup_classes")
        if CLASS in backup_classes:
            raise ValueError(f"class {CLASS} is in the daemon's backup_classes; refusing a backed-up acceptance turn")
        print(f"acceptance-worker-turn: class={CLASS}, daemon backup_classes={backup_classes}", flush=True)
        with tempfile.TemporaryDirectory(prefix="broca-acceptance-plan-") as scratch:
            plan_path = Path(scratch) / "plan.json"
            if args.compose_only:
                compose_plan(base + ["--session", "acceptance-plan-" + uuid.uuid4().hex], args.timeout, plan_path)
                print("acceptance-worker-turn: COMPOSE ONLY -- no send", flush=True)
                return 0
            return run_models(base, args, state_root, project_root, plan_path)
    except (ArchiveRefusal, OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as error:
        print(f"acceptance-worker-turn: FAILED -- {error}", file=sys.stderr)
        return 1


def run_models(base, args, state_root, project_root, plan_path):
    models = candidates(args.model)
    # The reader fetch-plan preset is legal unscoped; worker requires a daemon-attested session scope.
    # No tool call is needed: declarations freeze during admission, before the model turn.
    for index, model in enumerate(models, 1):
        session = "acceptance-plan-" + uuid.uuid4().hex
        path = state_root / "wal" / wal_name(project_root, "broca", session)
        print(f"acceptance-worker-turn: attempt {index}/{len(models)} model={model}", flush=True)
        try:
            output = composed_turn(base + ["--session", session],
                                   base + ["--session", session, "run", "--model", model,
                                           "--session-class", CLASS, "--max-output-tokens", "32",
                                           "--prompt", "Reply only OK. Do not call tools."],
                                   args.timeout, plan_path)
        except ClientFailure as error:
            if error.result.returncode != 1:
                raise
            failed = failed_run(error.result.stdout)
            if failed is None:
                raise
            run_id, observed_class = failed
            # A provider refusal does not excuse a broken admission/WAL feature path.
            _, terminal = checked_run(path, run_id, finish_reason="error")
            reason = provider_refusal(terminal, observed_class)
            if reason is None:
                raise
            next_step = "trying next candidate" if index < len(models) else "no candidates remain"
            print(f"acceptance-worker-turn: model={model} retryable provider error: {reason}; {next_step}", flush=True)
            continue
        sends = [event for event in events_from(output) if event.get("event") == "send"]
        if len(sends) != 1 or sends[0].get("state") != "active":
            raise ValueError(f"send was not admitted as a new active run:\n{output}")
        run_id = sends[0]["run_id"]
        requires = check_wal(path, run_id)
        print(f"acceptance-worker-turn: PASS -- admitted, completed; model={model}; {path} RunStarted requires {', '.join(requires)}")
        return 0
    print("acceptance-worker-turn: CANNOT COMPARE -- every candidate hit a retryable provider error", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
