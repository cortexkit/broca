#!/usr/bin/env python3
"""What became of a run live at a restart, read from its export facts, the daemon's runs in
progress and its run.status."""
from __future__ import annotations

import json
import pathlib
import sqlite3
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from lib.seal_readback import (  # noqa: E402
    CALLER_CANCELLED,
    CANCELLED,
    FINISHED,
    PAUSED,
    RESUMED,
    SEALED,
    UNCONFIRMED,
    classify_run,
    daemon_state,
    run_bind,
    run_facts,
)

DRAIN = 1_000


def fact(reason: str | None, at: int = DRAIN + 5) -> dict:
    return {"terminal_reason": reason, "occurred_at_ms": at}


def classify(facts: list[dict], *, live: set[str] = frozenset(), drain: int | None = DRAIN,
             state: str | None = None) -> str:
    return classify_run(facts, "run-1", drain_started_ms=drain, live_after=set(live), state=state)


class ClassifyTests(unittest.TestCase):
    def test_a_run_running_again_resumed(self):
        self.assertEqual(classify([fact(None)], live={"run-1"}), RESUMED)

    def test_an_interrupted_end_is_sealed(self):
        self.assertEqual(classify([fact(None), fact("interrupted")]), SEALED)

    def test_a_normal_end_finished(self):
        for reason in ("completed", "error", "max_steps", "budget_exhausted", "context_ceiling"):
            with self.subTest(reason=reason):
                self.assertEqual(classify([fact(reason)]), FINISHED)

    def test_a_pause_not_running_is_paused(self):
        self.assertEqual(classify([fact(None)]), PAUSED)

    def test_no_facts_and_not_running_is_unconfirmed(self):
        self.assertEqual(classify([]), UNCONFIRMED)

    def test_a_cancel_after_the_drain_began_is_the_drain_s(self):
        self.assertEqual(classify([fact("cancelled", DRAIN)]), CANCELLED)

    def test_a_cancel_strictly_before_the_drain_is_the_caller_s(self):
        self.assertEqual(classify([fact("cancelled", DRAIN - 1)]), CALLER_CANCELLED)

    def test_a_cancel_without_a_time_or_drain_is_the_drain_s(self):
        self.assertEqual(classify([{"terminal_reason": "cancelled"}]), CANCELLED)
        self.assertEqual(classify([fact("cancelled", DRAIN - 1)], drain=None), CANCELLED)

    def test_the_last_end_decides(self):
        self.assertEqual(classify([fact("cancelled", DRAIN - 1), fact("interrupted")]), SEALED)

    def test_a_run_with_no_ending_fact_is_what_its_status_says(self):
        # A run paused at the restart, or waiting for its held tool calls to answer, writes no
        # fact and isn't in progress: before its status was asked it read as unconfirmed.
        for state, expected in (("active", RESUMED), ("paused", PAUSED),
                                ("interrupted", SEALED), ("cancelled", CANCELLED),
                                ("completed", FINISHED), ("error", FINISHED)):
            with self.subTest(state=state):
                self.assertEqual(classify([], state=state), expected)

    def test_an_ending_fact_decides_before_the_status(self):
        self.assertEqual(classify([fact("cancelled", DRAIN - 1)], state="cancelled"),
                         CALLER_CANCELLED)

    def test_an_unknown_status_leaves_the_facts_to_decide(self):
        self.assertEqual(classify([], state="something_new"), UNCONFIRMED)
        self.assertEqual(classify([fact(None)], state=None), PAUSED)


class RunFactsTests(unittest.TestCase):
    def test_facts_come_back_in_write_order_for_that_run_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = pathlib.Path(tmp) / "run-index.db"
            connection = sqlite3.connect(db)
            connection.execute(
                "CREATE TABLE export_facts (export_seq INTEGER PRIMARY KEY AUTOINCREMENT, "
                "fact_id TEXT NOT NULL UNIQUE, run_id TEXT NOT NULL, segment_json TEXT NOT NULL)")
            rows = [("f1", "run-1", fact(None)), ("f2", "run-2", fact("completed")),
                    ("f3", "run-1", fact("interrupted"))]
            for fact_id, run_id, segment in rows:
                connection.execute("INSERT INTO export_facts (fact_id, run_id, segment_json) "
                                   "VALUES (?, ?, ?)", (fact_id, run_id, json.dumps(segment)))
            connection.commit()
            connection.close()
            self.assertEqual(run_facts(db, "run-1"), [fact(None), fact("interrupted")])
            self.assertEqual(run_facts(db, "run-3"), [])

    def test_a_missing_index_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(sqlite3.OperationalError):
                run_facts(pathlib.Path(tmp) / "run-index.db", "run-1")


class DaemonStatusTests(unittest.TestCase):
    def test_a_session_address_names_its_bind_only_when_unambiguous(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = pathlib.Path(tmp) / "run-index.db"
            connection = sqlite3.connect(db)
            connection.execute("CREATE TABLE agent_data_session_ref (address TEXT, project_root TEXT, "
                               "harness TEXT, session TEXT, ref TEXT)")
            connection.executemany("INSERT INTO agent_data_session_ref VALUES (?, ?, ?, ?, ?)", [
                ("a1", "/project", "broca", "one", "r1"), ("a1", "/project", "broca", "one", "r2"),
                ("a2", "/project", "broca", "two", "r3"), ("a2", "/other", "broca", "two", "r4")])
            connection.commit()
            connection.close()
            self.assertEqual(run_bind(db, "a1"), ("/project", "broca", "one"))
            self.assertIsNone(run_bind(db, "a2"))
            self.assertIsNone(run_bind(db, "a3"))
            self.assertIsNone(run_bind(pathlib.Path(tmp) / "missing.db", "a1"))

    def test_the_state_is_read_from_the_raw_status_reply(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake = pathlib.Path(tmp) / "broca-session"
            fake.write_text("#!/bin/sh\n"
                            "case \"$*\" in *--run-id\\ run-1\\ --raw*) "
                            "echo '{\"result\":{\"state\":\"paused\"}}';; *) exit 3;; esac\n")
            fake.chmod(0o755)
            bind = ("/project", "broca", "one")
            self.assertEqual(daemon_state(str(fake), "subc", bind, "run-1"), "paused")
            self.assertIsNone(daemon_state(str(fake), "subc", bind, "run-2"))
            self.assertIsNone(daemon_state(str(pathlib.Path(tmp) / "none"), "subc", bind, "run-1"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
