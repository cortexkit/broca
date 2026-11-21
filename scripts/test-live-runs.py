#!/usr/bin/env python3
"""live-runs.py answers from the daemon's memory report, and never reads a silence as idle.

A stand-in `broca-session` prints the events the real client prints for `--json-events memory`.
"""
from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import tempfile
import textwrap
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "live-runs.py"


def session(address: str, run_id: str | None = None, session_class: str = "mason") -> dict:
    row = {"address": address, "session_class": session_class, "window_messages": 10,
           "run_in_progress": run_id is not None, "holders": {}}
    if run_id:
        row["run_id"] = run_id
    return row


def report(sessions: list[dict], *, unanswered: int = 0) -> dict:
    return {"sessions": sessions, "running_sessions": sum(s["run_in_progress"] for s in sessions),
            "unanswered_sessions": unanswered, "totals": {},
            "largest": [], "setup_parts": [], "closed_session_views": {}, "settled_runs": {}}


class LiveRunsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = pathlib.Path(self.tmp.name)
        self.answers = self.dir / "answers.json"
        self.asked = self.dir / "asked"
        self.client = self.dir / "broca-session"
        # Answers one report per call, in order, repeating the last; records each call's args.
        self.client.write_text(textwrap.dedent(f'''\
            #!/usr/bin/env python3
            import json, pathlib, sys
            asked = pathlib.Path({str(self.asked)!r})
            calls = asked.read_text().splitlines() if asked.exists() else []
            asked.write_text("\\n".join(calls + [" ".join(sys.argv[1:])]) + "\\n")
            answers = json.loads(pathlib.Path({str(self.answers)!r}).read_text())
            answer = answers[min(len(calls), len(answers) - 1)]
            if answer is None:
                print("ERROR the daemon is not running", file=sys.stderr)
                raise SystemExit(1)
            print("BOUND module=broca harness=broca")
            print(json.dumps({{"event": "bound", "session": "live-runs-probe"}}))
            print(json.dumps(answer))
        '''))
        self.client.chmod(0o755)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def run_gate(self, *answers: dict | None) -> subprocess.CompletedProcess[str]:
        self.answers.write_text(json.dumps(list(answers)))
        env = dict(os.environ, BROCA_SESSION_BIN=str(self.client), SUBC_CONNECTION="/conn.json")
        return subprocess.run([sys.executable, str(SCRIPT)], env=env, capture_output=True,
                              text=True, timeout=60)

    def calls(self) -> list[str]:
        return self.asked.read_text().splitlines()

    def test_no_run_in_progress_is_a_clear_gate(self):
        result = self.run_gate(report([session("a1"), session("a2")]))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("live runs          0", result.stdout)
        self.assertNotIn("  live  ", result.stdout)
        self.assertEqual(self.calls(), [
            "--subc /conn.json --json-events memory --project-root broca-unscoped-v1 "
            "--harness broca --session live-runs-probe"
        ])

    def test_each_run_in_progress_is_listed_with_its_id(self):
        result = self.run_gate(report([session("a1", "run-1", "head"), session("a2"),
                                       session("a3", "run-3")]))
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("live runs          2", result.stdout)
        self.assertIn("  live  a1  run-1  head", result.stdout)
        self.assertIn("  live  a3  run-3  mason", result.stdout)
        self.assertIn("restarting now would cut", result.stderr)

    def test_a_daemon_that_names_no_run_id_is_listed_with_a_question_mark(self):
        row = session("a1", "run-1")
        del row["run_id"]
        result = self.run_gate(report([row]))
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("  live  a1  ?  mason", result.stdout)

    def test_a_silent_actor_is_asked_again_then_counts_against_a_restart(self):
        result = self.run_gate(report([session("a1")], unanswered=1))
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("unanswered         1", result.stdout)
        self.assertIn("did not answer in 4 asks", result.stderr)
        self.assertEqual(len(self.calls()), 4)

    def test_an_actor_that_answers_on_a_later_ask_clears(self):
        result = self.run_gate(report([session("a1")], unanswered=1), report([session("a1")]))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("unanswered         0", result.stdout)
        self.assertEqual(len(self.calls()), 2)

    def test_no_report_cannot_compare(self):
        result = self.run_gate(None)
        self.assertEqual(result.returncode, 2, result.stdout)
        self.assertIn("cannot compare: no memory report (exit 1): ERROR the daemon is not running",
                      result.stderr)

    def test_a_missing_client_cannot_compare(self):
        self.client.unlink()
        result = self.run_gate(report([]))
        self.assertEqual(result.returncode, 2, result.stdout)
        self.assertIn("cannot compare: broca-session did not answer", result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
