#!/usr/bin/env python3
"""What became of a run live at a restart, read from its export facts and the daemon's runs."""
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
    run_facts,
)

DRAIN = 1_000


def fact(reason: str | None, at: int = DRAIN + 5) -> dict:
    return {"terminal_reason": reason, "occurred_at_ms": at}


def classify(facts: list[dict], *, live: set[str] = frozenset(), drain: int | None = DRAIN) -> str:
    return classify_run(facts, "run-1", drain_started_ms=drain, live_after=set(live))


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


if __name__ == "__main__":
    unittest.main(verbosity=2)
