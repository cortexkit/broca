#!/usr/bin/env python3
"""Deployment acceptance distinguishes folderless refusals from scoped execution."""

from contextlib import redirect_stderr, redirect_stdout
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import struct
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from lib import acceptance_scoped as scoped
from lib import acceptance_contract as contract


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def frame(seq, record, requires=None):
    version = 1 if requires is None else 3
    value = record if requires is None else {"requires": requires, "record": record}
    payload = json.dumps(value, separators=(",", ":")).encode()
    header = struct.pack("<BQQ", version, seq, 1)
    return struct.pack("<I", len(payload)) + header + hashlib.sha256(header + payload).digest() + payload


class ScopedTrafficTests(unittest.TestCase):
    def setUp(self):
        scratch = ROOT / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.address = "1234567890abcdef"
        self.principal = {"kind": "reserved", "module_id": "core"}
        self.start = {"type": "run_started", "run_id": "fixture-run", "ts_ms": 101,
                      "principal": self.principal,
                      "scope": {"owner": self.principal, "ref": "fixture-worker", "scope_epoch": 1}}
        self.intent = {"type": "tool_dispatch_intent", "batch_id": 1,
                       "tool_call_id": "fixture-call", "module_id": "aft"}
        self.result = {"type": "tool_result", "batch_id": 1, "outcome": "completed",
                       "result": {"tool_call_id": "fixture-call", "is_error": False,
                                  "output": {"kind": "text", "text": "DO-NOT-PRINT-FIXTURE-CONTENT"}}}

    def write(self, records=None, folder="wal", address=None):
        path = self.root / folder / ((address or self.address) + ".wal")
        path.parent.mkdir(exist_ok=True)
        records = records if records is not None else [self.start, self.intent, self.result]
        path.write_bytes(b"".join(frame(seq, record, ["scope/v1"] if record["type"] == "run_started" else None)
                                  for seq, record in enumerate(records, 1)))
        return path

    def wait(self, timeout=0):
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(out):
            status = scoped.wait_for_traffic(self.root, 100, timeout, poll=1)
        self.assertNotIn("DO-NOT-PRINT", out.getvalue())
        return status, out.getvalue()

    def test_fixture_matches_rust_framing_digest_pin(self):
        raw = frame(1, {"type": "turn_finished", "step_id": 1}, ["test-feature/v1"])
        self.assertEqual(raw[21:53].hex(), "e47e87cd99121c97a249b823131b808ddd189d68ca27079b641ccb4d7bc85b47")
        self.assertEqual(raw[53:], b'{"requires":["test-feature/v1"],"record":{"type":"turn_finished","step_id":1}}')

    def test_scoped_success_in_every_live_folder_reports_address_and_count(self):
        for folder in ("wal", "wal-durable", "wal-ephemeral"):
            with self.subTest(folder=folder):
                other_intent = dict(self.intent, tool_call_id="second-call")
                other_result = dict(self.result, result=dict(self.result["result"], tool_call_id="second-call"))
                path = self.write(folder=folder, records=[self.start, self.intent, self.result,
                                                         other_intent, other_result])
                status, output = self.wait()
                self.assertEqual(status, 0, output)
                self.assertIn(f"session={self.address} successful_aft_results=2", output)
                path.unlink()

    def test_no_traffic_is_reported_as_no_traffic_not_tool_errors(self):
        status, output = self.wait()
        self.assertEqual(status, 1)
        self.assertIn("no scoped traffic was seen", output)
        self.assertNotIn("error_codes", output)

    def test_old_direct_unscoped_and_other_provider_results_do_not_certify(self):
        variants = [dict(self.start, ts_ms=100), dict(self.start, ts_ms=99),
                    dict(self.start, principal={"kind": "direct"}), dict(self.start, scope=None),
                    dict(self.start, principal={"kind": "unverified"})]
        for start in variants:
            with self.subTest(start=start):
                self.write([start, self.intent, self.result])
                self.assertIn("no scoped traffic was seen", self.wait()[1])
        self.write([self.start, dict(self.intent, module_id="other-provider"), self.result])
        self.assertIn("no scoped traffic was seen", self.wait()[1])

    def test_success_cannot_leak_across_run_or_call_boundaries(self):
        for records in ([self.start, dict(self.intent, tool_call_id="other-call"), self.result],
                        [self.start, self.intent, dict(self.start, ts_ms=99), self.result],
                        [self.start, self.intent, {"type": "run_finished"}, self.result]):
            with self.subTest(records=records):
                self.write(records)
                self.assertIn("no scoped traffic was seen", self.wait()[1])

    def test_all_scoped_errors_fail_with_codes_and_no_message_bodies(self):
        for output in ({"kind": "error", "code": "permission_denied", "message": "DO-NOT-PRINT"},
                       {"kind": "error", "message": "tool/route rejected: project_required — DO-NOT-PRINT"},
                       {"kind": "json", "value": {"error": {"code": "invalid_params", "message": "DO-NOT-PRINT"}}}):
            with self.subTest(output=output):
                result = dict(self.result, result=dict(self.result["result"], is_error=True, output=output))
                self.write([self.start, self.intent, result])
                status, text = self.wait()
                self.assertEqual(status, 1)
                self.assertIn("error_codes=", text)
                self.assertNotIn("no scoped traffic", text)
                self.assertIn(next(code for code in ("permission_denied", "project_required", "invalid_params")
                                   if code in json.dumps(output)), text)

    def test_synthetic_unknown_and_missing_error_flag_never_pass(self):
        for result in (dict(self.result, outcome="unknown"),
                       dict(self.result, result={"tool_call_id": "fixture-call"}),
                       dict(self.result, result=dict(self.result["result"], output={"kind": "error"}))):
            with self.subTest(result=result):
                self.write([self.start, self.intent, result])
                status, text = self.wait()
                self.assertEqual(status, 1)
                self.assertIn("error_codes=unclassified_error", text)

    def test_corrupt_torn_and_unknown_gated_wals_are_not_evidence(self):
        path = self.write()
        raw = path.read_bytes()
        for bad in (raw[:-1], raw[:-1] + bytes([raw[-1] ^ 1]),
                    raw + frame(4, {"type": "future_record"}, ["unknown-feature/v1"])):
            with self.subTest(size=len(bad)):
                path.write_bytes(bad)
                status, output = self.wait()
                self.assertEqual(status, 1)
                self.assertIn("error_codes=unverified_wal", output)

    def test_wait_observes_new_traffic_and_stops_at_ten_minute_deadline(self):
        with patch.object(scoped.time, "monotonic", side_effect=[0, 0, 600]), \
                patch.object(scoped.time, "sleep", side_effect=lambda _: self.write()) as sleep:
            status, output = self.wait(timeout=600)
        self.assertEqual(status, 0, output)
        sleep.assert_called_once_with(1)
        with patch.object(scoped.time, "monotonic", side_effect=[0, 0, 600]), \
                patch.object(scoped.time, "sleep") as sleep:
            (self.root / "wal" / (self.address + ".wal")).unlink()
            status, output = self.wait(timeout=600)
        self.assertEqual(status, 1)
        self.assertIn("no scoped traffic was seen", output)
        sleep.assert_called_once_with(1)


class DeployScopedTests(unittest.TestCase):
    def setUp(self):
        suite = load("scoped_deploy_fixture", "scripts/test-deploy.py")
        self.fixture = suite.DeployTests("test_failed_worker_admission_never_publishes")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_scoped_leg_runs_after_both_turns_with_recorded_restart_boundary(self):
        result = self.fixture.deploy()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.fixture.assert_gated_steps(["place", "verify", "accept", "worker-accept", "scoped-accept", "publish"])
        records = dict(self.fixture.records())
        args = records["scoped-accept"]
        self.assertEqual(args[0], "--placement-report")
        report = json.loads(Path(args[1]).read_text())
        self.assertGreater(report["restart_observed_at_ms"], 0)

    def test_scoped_leg_failure_blocks_publication_and_reports_rollback(self):
        result = self.fixture.deploy(FAIL_STEP="scoped-accept")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.fixture.assert_gated_steps(["place", "verify", "accept", "worker-accept", "scoped-accept"])
        self.assertIn("SCOPED DISPATCH ACCEPTANCE FAILED; not publishing", result.stdout + result.stderr)
        self.assertIn("Rollback:", result.stdout + result.stderr)


class DirectTurnTests(unittest.TestCase):
    def setUp(self):
        suite = load("direct_turn_fixture", "scripts/test-turn-harnesses.py")
        self.fixture = suite.TurnHarnesses("test_acceptance_completed_tool_turn_still_passes")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.placed = self.fixture.home / ".local/share/cortexkit/bin/ck-broca"
        self.placed.write_text("bind-no-project/v1\n")
        client = self.fixture.root / "target/release/broca-session"
        client.write_text('''#!/usr/bin/env python3
import json, os, sys
if "describe" in sys.argv:
    print(json.dumps({"result": {"capabilities": []}}))
else:
    if not os.environ.get("NO_DISPATCH"):
        print("TOOL_CALL seq=1 id=fixture-call name=read")
    if not os.environ.get("NO_RESULT"):
        print("TOOL_RESULT seq=2 id=fixture-call is_error=" + os.environ.get("TOOL_ERROR", "true")
              + " text=" + os.environ.get("TOOL_TEXT", ""))
    if not os.environ.get("NO_COMPLETE"):
        print("RUN_FINISHED reason=Completed")
''')

    def run_turn(self, **env):
        return self.fixture.run_script("acceptance-turn.sh", BROCA_BIN=str(self.placed),
                                       ACCEPTANCE_MODELS="fixture/model", **env)

    def test_declared_build_accepts_exact_project_required_refusal(self):
        result = self.run_turn(TOOL_TEXT=contract.PROJECT_REQUIRED)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_successful_result_still_passes_on_declared_and_legacy_builds(self):
        for declaration in ("bind-no-project/v1", "legacy"):
            with self.subTest(declaration=declaration):
                self.placed.write_text(declaration + "\n")
                result = self.run_turn(TOOL_ERROR="false")
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_other_refusals_and_inexact_project_required_text_fail(self):
        for text in (contract.PROJECT_REQUIRED.replace("project_required", "permission_denied"),
                     contract.PROJECT_REQUIRED + " extra", "project_required", "is_error=false"):
            with self.subTest(text=text):
                result = self.run_turn(TOOL_TEXT=text)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    def test_legacy_build_cannot_accept_project_required(self):
        self.placed.write_text("legacy\n")
        result = self.run_turn(TOOL_TEXT=contract.PROJECT_REQUIRED)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    def test_missing_dispatch_result_or_completion_still_fails(self):
        for missing in ("NO_DISPATCH", "NO_RESULT", "NO_COMPLETE"):
            with self.subTest(missing=missing):
                result = self.run_turn(TOOL_ERROR="false", **{missing: "1"})
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    def test_failed_binary_probe_is_loud_not_legacy_or_success(self):
        self.fixture.executable(self.fixture.bins / "strings", "#!/bin/sh\nexit 7\n")
        result = self.run_turn(TOOL_ERROR="false")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("cannot probe placed binary", result.stdout + result.stderr)

    def test_role_describe_declaration_is_used_when_available(self):
        with patch.dict(contract.os.environ, {"BROCA_BIN": "/not-a-placed-binary"}), \
                patch.object(contract.subprocess, "run") as probe:
            self.assertTrue(contract.bind_no_project({"capabilities": [contract.NO_PROJECT]}))
        probe.assert_not_called()

    def test_a_later_success_does_not_hide_another_tool_error(self):
        output = ("TOOL_RESULT seq=2 id=one is_error=true text=other refusal\n"
                  "TOOL_RESULT seq=3 id=two is_error=false text=ok\n")
        self.assertFalse(contract.tool_results_pass(output, True))


class WorkerAddressTests(unittest.TestCase):
    def setUp(self):
        suite = load("worker_address_fixture", "scripts/test-acceptance-worker-turn.py")
        self.fixture = suite.WorkerAcceptance("test_main_sends_class_plan_and_one_capped_turn_then_reads_wal")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.acceptance = suite.acceptance

    def check_address(self, no_project):
        result, output, invoke, check = self.fixture.run_main([
            '{"result":{"session_class":{"backup_classes":[]}}}',
            '{"event":"send","state":"active","run_id":"new-run"}',
        ], no_project=no_project)
        self.assertEqual(result, 0, output)
        command = invoke.call_args.args[0]
        session = command[command.index("--session") + 1]
        root = "broca-unscoped-v1" if no_project else str(Path(self.fixture.tmp.name).resolve())
        expected = self.acceptance.wal_name(root, "broca", session)
        self.assertEqual(check.call_args.args[0], Path(self.fixture.tmp.name) / "wal" / expected)

    def test_declared_unscoped_build_uses_fixed_namespace_wal(self):
        self.check_address(True)

    def test_legacy_build_keeps_callers_folder_wal(self):
        self.check_address(False)

    def test_declared_build_checks_provider_error_at_namespace_wal_too(self):
        result, output, invoke, check = self.fixture.run_main([
            '{"result":{"session_class":{"backup_classes":[]}}}', self.fixture.provider_failure(),
        ], no_project=True, extra_args=("--model", "fixture/model"))
        self.assertEqual(result, 2, output)
        command = invoke.call_args.args[0]
        session = command[command.index("--session") + 1]
        expected = self.acceptance.wal_name("broca-unscoped-v1", "broca", session)
        self.assertEqual(check.call_args.args[0], Path(self.fixture.tmp.name) / "wal" / expected)
        self.assertEqual(check.call_args.kwargs, {"finish_reason": "error"})


class ScopedCommandTests(unittest.TestCase):
    def test_missing_restart_boundary_and_excessive_wait_fail_without_echoing_report(self):
        for report in ({"other": "DO-NOT-PRINT"}, {"restart_observed_at_ms": "DO-NOT-PRINT"},
                       {"restart_observed_at_ms": 100}):
            out = io.StringIO()
            with patch.object(scoped.sys, "argv", ["acceptance-scoped", "--placement-report", "fixture.json",
                                                   "--timeout", "601"]), \
                    patch.object(Path, "read_text", return_value=json.dumps(report)), \
                    patch.object(scoped, "wait_for_traffic") as wait, redirect_stderr(out):
                self.assertEqual(scoped.main(), 1)
            self.assertNotIn("DO-NOT-PRINT", out.getvalue())
            self.assertIn("invalid restart boundary", out.getvalue())
            wait.assert_not_called()


if __name__ == "__main__":
    unittest.main()
