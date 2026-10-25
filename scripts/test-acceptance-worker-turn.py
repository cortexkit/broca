#!/usr/bin/env python3
"""Fail closed when a classed turn did not exercise both WAL feature gates."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import struct
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("acceptance_worker", Path(__file__).with_name("acceptance-worker-turn.py"))
assert spec is not None and spec.loader is not None
acceptance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(acceptance)


def frame(record, requires=()):
    payload = json.dumps({"requires": list(requires), "record": record}).encode()
    header = struct.pack("<BQQ", 3, 1 if record["type"] == "run_started" else 2, 1)
    return struct.pack("<I", len(payload)) + header + hashlib.sha256(header + payload).digest() + payload


class WorkerAcceptance(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "session.wal"
        self.start = {"type": "run_started", "run_id": "run-under-test", "session_class": "acceptance",
                      "manifest": {"fetches": [{"declarations_frozen": {"late_results": True}}]}}

    def write_wal(self, requires=acceptance.REQUIRED, reason="completed"):
        self.path.write_bytes(frame(self.start, requires) + frame({"type": "run_finished", "reason": reason}))

    def test_completed_classed_declaring_run_passes(self):
        self.write_wal()
        self.assertEqual(acceptance.check_wal(self.path, "run-under-test"), acceptance.REQUIRED)

    def test_missing_session_class_feature_is_named(self):
        self.write_wal(("provider-declarations/v1",))
        with self.assertRaisesRegex(ValueError, r"missing required WAL feature\(s\): session-class/v1$"):
            acceptance.check_wal(self.path, "run-under-test")

    def test_missing_provider_declarations_feature_is_named(self):
        self.write_wal(("session-class/v1",))
        with self.assertRaisesRegex(ValueError, r"missing required WAL feature\(s\): provider-declarations/v1$"):
            acceptance.check_wal(self.path, "run-under-test")

    def test_both_missing_features_are_named(self):
        self.write_wal(())
        with self.assertRaisesRegex(ValueError, "session-class/v1, provider-declarations/v1"):
            acceptance.check_wal(self.path, "run-under-test")

    def test_declared_gate_without_frozen_declarations_is_not_enough(self):
        self.start["manifest"]["fetches"] = []
        self.write_wal()
        with self.assertRaisesRegex(ValueError, "no frozen result formats or late_results"):
            acceptance.check_wal(self.path, "run-under-test")

    def test_result_formats_alone_exercise_declarations(self):
        self.start["manifest"]["fetches"][0]["declarations_frozen"] = {"result_formats": {"read": "text"}}
        self.write_wal()
        self.assertEqual(acceptance.check_wal(self.path, "run-under-test"), acceptance.REQUIRED)

    def test_wrong_class_is_not_enough(self):
        self.start["session_class"] = "mason"
        self.write_wal()
        with self.assertRaisesRegex(ValueError, "session_class is not acceptance"):
            acceptance.check_wal(self.path, "run-under-test")

    def test_error_terminal_is_not_completion(self):
        self.write_wal(reason="error")
        with self.assertRaisesRegex(ValueError, "did not complete in the WAL"):
            acceptance.check_wal(self.path, "run-under-test")

    def test_other_runs_completion_cannot_certify_this_run(self):
        self.write_wal(reason="error")
        other = dict(self.start, run_id="later-run")
        self.path.write_bytes(self.path.read_bytes() + self.renumber(frame(other, acceptance.REQUIRED), 3)
                              + self.renumber(frame({"type": "run_finished", "reason": "completed"}), 4))
        with self.assertRaisesRegex(ValueError, "did not complete in the WAL"):
            acceptance.check_wal(self.path, "run-under-test")
        with self.assertRaisesRegex(ValueError, "found 0"):
            acceptance.check_wal(self.path, "absent-run")

    @staticmethod
    def renumber(data, seq):
        header = struct.pack("<BQQ", 3, seq, 1)
        return data[:4] + header + hashlib.sha256(header + data[53:]).digest() + data[53:]

    def test_unverified_wal_is_refused(self):
        self.write_wal()
        data = bytearray(self.path.read_bytes())
        data[-2] ^= 1
        self.path.write_bytes(data)
        with self.assertRaisesRegex(acceptance.ArchiveRefusal, "digest mismatch"):
            acceptance.check_wal(self.path, "run-under-test")

    def run_main(self, answers):
        output = io.StringIO()
        argv = ["acceptance-worker-turn.py", "--project-root", self.tmp.name, "--state-root", self.tmp.name]
        with patch.object(acceptance.sys, "argv", argv), patch.object(acceptance, "invoke", side_effect=answers) as invoke, \
                patch.object(acceptance, "check_wal", return_value=acceptance.REQUIRED) as check, \
                redirect_stdout(output), redirect_stderr(output):
            result = acceptance.main()
        return result, output.getvalue(), invoke, check

    def test_backup_class_refuses_before_send(self):
        result, output, invoke, check = self.run_main(['{"result":{"session_class":{"backup_classes":["head","acceptance"]}}}'])
        self.assertEqual(result, 1)
        self.assertIn("refusing a backed-up acceptance turn", output)
        self.assertEqual(invoke.call_count, 1)
        check.assert_not_called()

    def test_main_sends_class_plan_and_one_capped_turn_then_reads_wal(self):
        result, output, invoke, check = self.run_main([
            '{"result":{"session_class":{"backup_classes":["head","mason"]}}}',
            '{"event":"send","state":"active","run_id":"new-run"}',
        ])
        self.assertEqual(result, 0, output)
        self.assertEqual(invoke.call_count, 2)
        command = invoke.call_args.args[0]
        self.assertEqual(command[command.index("--session-class") + 1], "acceptance")
        self.assertEqual(command[command.index("--plan") + 1], str(acceptance.PLAN))
        self.assertEqual(command[command.index("--max-output-tokens") + 1], "32")
        self.assertEqual(check.call_args.args[1], "new-run")
        self.assertIn("PASS -- admitted, completed", output)

    def test_queued_send_cannot_pass(self):
        result, output, _, check = self.run_main([
            '{"result":{"session_class":{"backup_classes":[]}}}', '{"event":"send","state":"pending"}',
        ])
        self.assertEqual(result, 1)
        self.assertIn("not admitted as a new active run", output)
        check.assert_not_called()


if __name__ == "__main__":
    unittest.main()
