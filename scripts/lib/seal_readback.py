"""What became of each run that was live when broca restarted.

The runs live before a restart are named by scripts/live-runs.py. After the restart each one is
read back from what the daemon writes independently of this script: the usage export facts in
run-index.db (`terminal_reason` on a run's last segment once it ends), the restarted daemon's own
list of runs in progress, and, for a run neither of those settles, the daemon's `run.status`.
A run paused at the restart, or one that resumes only once its held tool calls answer, has no
ending fact and isn't in progress yet: only its status says what became of it.

A shutdown drain pauses a running run and the restarted daemon resumes it under the same id, or
it seals the run interrupted. It never cancels one: cancelled means a caller asked for it.
"""
from __future__ import annotations

import json
import pathlib
import sqlite3
import subprocess

# Sealed interrupted by the shutdown.
SEALED = "sealed"
# Running again under its own id after the restart.
RESUMED = "resumed"
# Ended (completed, failed, out of budget) before or after the restart: nothing was cut.
FINISHED = "finished"
# Paused and not running: waiting on a sign-in, an account reset or a held tool call.
PAUSED = "paused"
# Cancelled after the shutdown began: the drain cancelled it, which it must never do.
CANCELLED = "cancelled"
# Cancelled by its caller before the shutdown began.
CALLER_CANCELLED = "caller_cancelled"
# None of the above could be read.
UNCONFIRMED = "unconfirmed"


def run_facts(run_index: pathlib.Path, run_id: str) -> list[dict]:
    """The run's export facts in the order they were written. Raises when the index can't be read."""
    connection = sqlite3.connect(f"file:{run_index}?mode=ro", uri=True, timeout=10)
    try:
        rows = connection.execute(
            "SELECT segment_json FROM export_facts WHERE run_id = ? ORDER BY export_seq", (run_id,)
        ).fetchall()
    finally:
        connection.close()
    return [json.loads(row[0]) for row in rows]


# The run ends `run.status` reports besides interrupted and cancelled.
FINISHED_STATES = {"completed", "max_steps", "budget_exhausted", "context_ceiling", "error"}


def run_bind(run_index: pathlib.Path, address: str) -> tuple[str, str, str] | None:
    """The (project root, harness, session) the session-ref index names for a session address, or
    None when it names none or more than one."""
    try:
        connection = sqlite3.connect(f"file:{run_index}?mode=ro", uri=True, timeout=10)
        try:
            binds = connection.execute(
                "SELECT DISTINCT project_root, harness, session FROM agent_data_session_ref "
                "WHERE address = ?", (address,)).fetchall()
        finally:
            connection.close()
    except sqlite3.Error:
        return None
    if len(binds) != 1 or not all(isinstance(part, str) for part in binds[0]):
        return None
    return binds[0]


def daemon_state(broca_session: str, subc: str, bind: tuple[str, str, str], run_id: str,
                 timeout: float = 10) -> str | None:
    """The run's `state` in the daemon's `run.status` reply, or None when it can't be read."""
    project_root, harness, session = bind
    try:
        answer = subprocess.run(
            [broca_session, "--subc", subc, "status", "--project-root", project_root,
             "--harness", harness, "--session", session, "--run-id", run_id, "--raw"],
            capture_output=True, text=True, timeout=timeout)
        state = json.loads(answer.stdout)["result"]["state"]
    except (OSError, subprocess.TimeoutExpired, ValueError, KeyError, TypeError):
        return None
    return state if isinstance(state, str) else None


def classify_run(
    facts: list[dict], run_id: str, *, drain_started_ms: int | None, live_after: set[str],
    state: str | None = None,
) -> str:
    """Classify one run from its facts, the restarted daemon's runs in progress and, when given,
    its `run.status` state. An ending fact decides before the state, since it carries the time a
    cancel needs."""
    if run_id in live_after:
        return RESUMED
    ended = [fact for fact in facts if fact.get("terminal_reason")]
    if ended:
        last = ended[-1]
        reason = last["terminal_reason"]
        if reason == "cancelled":
            # Only a time strictly before the drain began rules the drain out; a missing one can't.
            occurred = last.get("occurred_at_ms")
            if drain_started_ms is not None and type(occurred) is int and occurred < drain_started_ms:
                return CALLER_CANCELLED
            return CANCELLED
        if reason == "interrupted":
            return SEALED
        return FINISHED
    if state == "active":
        return RESUMED
    if state == "paused":
        return PAUSED
    if state == "interrupted":
        return SEALED
    if state == "cancelled":
        return CANCELLED
    if state in FINISHED_STATES:
        return FINISHED
    if facts:
        return PAUSED
    return UNCONFIRMED
