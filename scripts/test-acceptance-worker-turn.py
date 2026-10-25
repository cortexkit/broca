#!/usr/bin/env python3
"""Fail closed when a classed turn did not exercise both WAL feature gates."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import struct
import subprocess
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
        self.models = Path(self.tmp.name) / "acceptance-models.txt"
        self.models.write_text("first/model\nsecond/model\n")
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

    def run_main(self, answers, *, env=None, wal_results=None):
        output = io.StringIO()
        argv = ["acceptance-worker-turn.py", "--project-root", self.tmp.name, "--state-root", self.tmp.name]
        with patch.dict(acceptance.os.environ, env or {}, clear=True), \
                patch.object(acceptance, "MODELS_FILE", self.models), \
                patch.object(acceptance.sys, "argv", argv), patch.object(acceptance, "invoke", side_effect=answers) as invoke, \
                patch.object(acceptance, "checked_run", return_value=(acceptance.REQUIRED, self.error_terminal()),
                             side_effect=wal_results) as check, \
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

    @staticmethod
    def provider_failure(error_class="Transient", message="HTTP 429", run_id="refused-run"):
        events = [
            {"event": "send", "state": "active", "run_id": run_id},
            {"event": "run_started", "run_id": run_id},
            {"event": "loop_error", "class": error_class, "msg": message},
            {"event": "run_finished", "run_id": run_id, "reason": "Error"},
        ]
        return acceptance.ClientFailure(subprocess.CompletedProcess(
            ["broca-session"], 1, "\n".join(json.dumps(event) for event in events), ""))

    def test_first_candidate_429_rotates_and_checks_each_wal(self):
        result, output, invoke, check = self.run_main([
            '{"result":{"session_class":{"backup_classes":[]}}}', self.provider_failure(),
            '{"event":"send","state":"active","run_id":"new-run"}',
        ])
        self.assertEqual(result, 0, output)
        self.assertEqual(invoke.call_count, 3)
        commands = [call.args[0] for call in invoke.call_args_list[1:]]
        self.assertEqual([command[command.index("--model") + 1] for command in commands], ["first/model", "second/model"])
        self.assertNotEqual(commands[0][commands[0].index("--session") + 1], commands[1][commands[1].index("--session") + 1])
        self.assertEqual(check.call_count, 2)
        self.assertEqual(check.call_args_list[0].kwargs, {"finish_reason": "error"})
        self.assertEqual(check.call_args_list[1].args[1], "new-run")
        self.assertIn("attempt 1/2 model=first/model", output)
        self.assertIn("transient/rate_limited; trying next candidate", output)
        self.assertIn("attempt 2/2 model=second/model", output)

    def test_admission_refusal_fails_immediately_even_if_message_mentions_429(self):
        failure = acceptance.ClientFailure(subprocess.CompletedProcess(
            ["broca-session"], 1, '{"event":"error","msg":"send: invalid_params (HTTP 429)"}', ""))
        result, output, invoke, check = self.run_main([
            '{"result":{"session_class":{"backup_classes":[]}}}', failure,
            '{"event":"send","state":"active","run_id":"would-hide-refusal"}',
        ])
        self.assertEqual(result, 1, output)
        self.assertEqual(invoke.call_count, 2)
        self.assertIn("invalid_params", output)
        self.assertNotIn("trying next candidate", output)
        check.assert_not_called()

    def test_send_timeout_fails_immediately(self):
        failures = [
            subprocess.TimeoutExpired(["broca-session"], 120),
            acceptance.ClientFailure(subprocess.CompletedProcess(
                ["broca-session"], 1, '{"event":"error","msg":"send: subc transport: timed out after 10s"}', "")),
        ]
        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                result, output, invoke, check = self.run_main([
                    '{"result":{"session_class":{"backup_classes":[]}}}', failure,
                    '{"event":"send","state":"active","run_id":"would-hide-timeout"}',
                ])
                self.assertEqual(result, 1, output)
                self.assertEqual(invoke.call_count, 2)
                check.assert_not_called()

    def test_wal_failure_does_not_rotate_even_after_provider_refusal(self):
        for response in [self.provider_failure(), '{"event":"send","state":"active","run_id":"new-run"}']:
            with self.subTest(response=type(response).__name__):
                result, output, invoke, check = self.run_main([
                    '{"result":{"session_class":{"backup_classes":[]}}}', response,
                    '{"event":"send","state":"active","run_id":"would-hide-wal-failure"}',
                ], wal_results=[ValueError("RunStarted missing required WAL feature(s): session-class/v1")])
                self.assertEqual(result, 1, output)
                self.assertIn("missing required WAL feature", output)
                self.assertEqual(invoke.call_count, 2)
                self.assertEqual(check.call_count, 1)

    def test_worker_model_override_pins_one_candidate(self):
        result, output, invoke, _ = self.run_main([
            '{"result":{"session_class":{"backup_classes":[]}}}', self.provider_failure(),
            '{"event":"send","state":"active","run_id":"would-ignore-pin"}',
        ], env={"ACCEPTANCE_WORKER_MODEL": "pinned/model"})
        self.assertEqual(result, 2, output)
        self.assertEqual(invoke.call_count, 2)
        self.assertIn("attempt 1/1 model=pinned/model", output)
        self.assertIn("CANNOT COMPARE", output)

    @staticmethod
    def error_terminal(error_class="transient", code="rate_limited"):
        error = {"class": error_class, "message": "HTTP 429 billing exhausted text is not policy"}
        if code is not None:
            error["provider_code"] = code
        return {"type": "run_finished", "reason": "error", "error": error}

    def test_verified_terminal_classifies_quota_billing_and_transient(self):
        cases = [("transient", "rate_limited", "Transient"),
                 ("transient", "account_limit_exhausted", "Transient"),
                 ("permanent", "billing_exhausted", "Permanent"),
                 ("transient", None, "Transient"),
                 ("provider_unavailable", None, "Transient")]
        for error_class, code, observed in cases:
            with self.subTest(error_class=error_class, code=code):
                self.assertEqual(acceptance.provider_refusal(self.error_terminal(error_class, code), observed),
                                 f"{error_class}/{code}" if code else error_class)

    def test_other_errors_never_classify_as_provider_refusals(self):
        cases = [("permanent", None, "Permanent"), ("permanent", "credit_insufficient", "Permanent"),
                 ("permanent", "request_invalid", "Permanent"), ("auth_required", None, "Auth"),
                 ("context_overflow", None, "ContextOverflow"), ("unknown", None, "Unknown"),
                 ("permanent", "rate_limited", "Permanent"), ("transient", "billing_exhausted", "Transient"),
                 ("transient", "credit_insufficient", "Transient"), ("transient", "rate_limited", "Permanent")]
        for error_class, code, observed in cases:
            with self.subTest(error_class=error_class, code=code, observed=observed):
                self.assertIsNone(acceptance.provider_refusal(self.error_terminal(error_class, code), observed))
        self.assertIsNone(acceptance.provider_refusal({"error": None}, "Transient"))
        self.assertIsNone(acceptance.provider_refusal({"error": {"class": "transient"}}, "Transient"))

    def test_error_wal_is_verified_before_its_category_is_used(self):
        terminal = self.error_terminal()
        self.path.write_bytes(frame(self.start, acceptance.REQUIRED) + frame(terminal))
        requires, actual = acceptance.checked_run(self.path, "run-under-test", finish_reason="error")
        self.assertEqual(requires, acceptance.REQUIRED)
        self.assertEqual(acceptance.provider_refusal(actual, "Transient"), "transient/rate_limited")
        self.path.write_bytes(frame(self.start, ("session-class/v1",)) + frame(terminal))
        with self.assertRaisesRegex(ValueError, "missing required WAL feature.*provider-declarations/v1"):
            acceptance.checked_run(self.path, "run-under-test", finish_reason="error")

    def test_duplicate_wal_terminal_is_refused(self):
        self.write_wal()
        self.path.write_bytes(self.path.read_bytes() + self.renumber(frame(self.error_terminal()), 3))
        with self.assertRaisesRegex(ValueError, "did not complete in the WAL"):
            acceptance.checked_run(self.path, "run-under-test", finish_reason="error")

    def test_failed_run_requires_consistent_identity_and_lifecycle(self):
        original = [json.loads(line) for line in self.provider_failure().result.stdout.splitlines()]
        invalid = [original[:1], original + [original[0]], original + [original[1]], original + [original[3]],
                   original + [{"event": "error", "msg": "subscribe transport failed"}],
                   original + [{"event": "run_paused", "reason": "AuthRequired"}]]
        for index in (0, 1, 3):
            for run_id in (None, "other-run"):
                changed = json.loads(json.dumps(original))
                changed[index]["run_id"] = run_id
                invalid.append(changed)
        for events in invalid:
            with self.subTest(events=events):
                self.assertIsNone(acceptance.failed_run("\n".join(json.dumps(event) for event in events)))
        for output in ("[]", "null", "not JSON"):
            with self.subTest(output=output), self.assertRaises(ValueError):
                acceptance.failed_run(output)

    def test_abnormal_client_exit_never_rotates(self):
        failure = self.provider_failure()
        failure.result.returncode = -9
        result, output, invoke, check = self.run_main([
            '{"result":{"session_class":{"backup_classes":[]}}}', failure,
            '{"event":"send","state":"active","run_id":"would-hide-client-crash"}',
        ])
        self.assertEqual(result, 1, output)
        self.assertEqual(invoke.call_count, 2)
        check.assert_not_called()

    def test_shared_candidates_and_common_override_keep_order(self):
        with patch.object(acceptance, "MODELS_FILE", self.models), patch.dict(acceptance.os.environ, {}, clear=True):
            self.assertEqual(acceptance.candidates(None), ["first/model", "second/model"])
            with patch.dict(acceptance.os.environ, {"ACCEPTANCE_MODELS": "override/a override/b"}):
                self.assertEqual(acceptance.candidates(None), ["override/a", "override/b"])
                self.assertEqual(acceptance.candidates("pinned/model"), ["pinned/model"])
            self.models.write_text("")
            with self.assertRaisesRegex(ValueError, "no acceptance candidate models"):
                acceptance.candidates(None)
        for pin in ("", "one/model two/model"):
            with self.subTest(pin=pin), self.assertRaisesRegex(ValueError, "pin exactly one model"):
                acceptance.candidates(pin)

    def test_all_provider_refusals_report_cannot_compare(self):
        result, output, invoke, check = self.run_main([
            '{"result":{"session_class":{"backup_classes":[]}}}', self.provider_failure(), self.provider_failure(),
        ])
        self.assertEqual(result, 2, output)
        self.assertEqual(invoke.call_count, 3)
        self.assertEqual(check.call_count, 2)
        self.assertIn("model=second/model retryable provider error", output)
        self.assertIn("no candidates remain", output)
        self.assertIn("CANNOT COMPARE", output)


if __name__ == "__main__":
    unittest.main()
