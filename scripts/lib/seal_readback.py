"""What became of each run that was live when broca restarted.

The runs live before a restart are named by scripts/live-runs.py. After the restart each one is
read back from two places the daemon writes independently of this script: the usage export facts
in run-index.db, which every run writes when it pauses or ends (`terminal_reason` on its last
segment), and the restarted daemon's own list of runs in progress.

A shutdown drain pauses a running run and the restarted daemon resumes it under the same id, or
it seals the run interrupted. It never cancels one: cancelled means a caller asked for it.
"""
from __future__ import annotations

import json
import pathlib
import sqlite3

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


def classify_run(
    facts: list[dict], run_id: str, *, drain_started_ms: int | None, live_after: set[str]
) -> str:
    """Classify one run from its facts and the restarted daemon's runs in progress."""
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
    if facts:
        return PAUSED
    return UNCONFIRMED
