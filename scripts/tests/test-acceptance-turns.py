#!/usr/bin/env python3
"""Deployment acceptance distinguishes folderless refusals from scoped execution."""

from contextlib import redirect_stderr, redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import struct
import subprocess
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
                self.assertIn("examined_sessions=wal:1,store:0 qualifying_results=2", output)
                path.unlink()

    def test_zero_examined_sessions_has_its_own_verdict(self):
        status, output = self.wait()
        self.assertEqual(status, 1)
        self.assertIn("could not examine any session", output)
        self.assertIn("examined_sessions=wal:0,store:0 qualifying_results=0", output)
        self.assertNotIn("no scoped traffic", output)

    def test_examined_session_without_traffic_reports_counts_not_tool_errors(self):
        self.write([self.start])
        status, output = self.wait()
        self.assertEqual(status, 1)
        self.assertIn("no scoped traffic was seen", output)
        self.assertIn("examined_sessions=wal:1,store:0 qualifying_results=0", output)
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
        self.assertIn("could not examine any session", output)
        sleep.assert_called_once_with(1)

    def store(self, folder="session-store"):
        root = self.root / folder
        (root / "heads" / self.address).mkdir(parents=True)
        (root / "heads" / self.address / "head").write_bytes(b"DO-NOT-READ-ENCRYPTED-HEAD")
        log = root / "logs" / self.address / ("0" * 32)
        log.parent.mkdir(parents=True)
        log.write_bytes(b"DO-NOT-READ-ENCRYPTED-LOG")
        with sqlite3.connect(self.root / "run-index.db") as db:
            db.execute("CREATE TABLE IF NOT EXISTS agent_data_session_ref "
                       "(address TEXT, project_root TEXT, harness TEXT, session TEXT)")
            db.execute("INSERT INTO agent_data_session_ref VALUES (?, ?, ?, ?)",
                       (self.address, "broca-unscoped-v1", "core", "DO-NOT-PRINT-SESSION"))
        self.commands = []
        self.admission = {"run_id": "fixture-run", "storage": "store", "started_at_ms": 101,
                          "scope": {"principal": self.principal, "owner": self.principal,
                                    "ref": "fixture-worker", "epoch": 1}}
        self.messages = [
            {"ordinal": 1, "run": {"run_id": "fixture-run"},
             "message": {"role": "assistant", "content": []},
             "tool_calls": [{"tool_call_id": "fixture-call", "call_key": "fixture-key",
                             "dispatched_to": "aft", "indeterminate": False}]},
            {"ordinal": 2, "written_at": 102, "run": {"run_id": "fixture-run"},
             "message": {"role": "tool", "content": [dict(self.result["result"], type="tool_result")]}}
        ]
        self.calls = [{"tool_call_id": "fixture-call", "call_key": "fixture-key",
                       "dispatched_to": "aft", "outcome": "succeeded", "ordinal": 1}]

    def reply(self, command, timeout):
        self.commands.append(command)
        op = command[command.index("--op") + 1]
        if op == "session.read":
            result = {"messages": self.messages, "history_version": 1}
        elif op == "run.admission":
            result = self.admission
        else:
            self.assertEqual(op, "run.tool_calls")
            result = {"run_id": "fixture-run", "calls": self.calls}
        return subprocess.CompletedProcess(command, 0, json.dumps({"result": result}), "")

    def test_new_storage_only_success_uses_daemon_and_counts_examined_sessions(self):
        self.store()
        with patch.object(scoped, "invoke", side_effect=self.reply, create=True), \
                patch.object(Path, "read_bytes", side_effect=AssertionError("encrypted content opened")):
            status, output = self.wait()
        self.assertEqual(status, 0, output)
        self.assertIn("examined_sessions=wal:0,store:1 qualifying_results=1", output)
        for command in self.commands:
            self.assertEqual(command[command.index("--project-root") + 1], "broca-unscoped-v1")
            self.assertEqual(command[command.index("--harness") + 1], "core")
            self.assertEqual(command[command.index("--session") + 1], "DO-NOT-PRINT-SESSION")
        params = json.loads(self.commands[0][-1])
        self.assertTrue(params["include_display_facts"])
        self.assertEqual(params["limit"], 500)

    def test_new_storage_empty_read_is_no_traffic_with_counts(self):
        self.store("session-store-durable")
        self.messages = []
        with patch.object(scoped, "invoke", side_effect=self.reply):
            status, output = self.wait()
        self.assertEqual(status, 1)
        self.assertIn("no scoped traffic was seen", output)
        self.assertIn("examined_sessions=wal:0,store:1 qualifying_results=0", output)

    def test_new_storage_pre_restart_or_errored_results_never_count(self):
        self.store("session-store-ephemeral")
        for timestamp, error, outcome in ((100, False, "succeeded"), (99, False, "succeeded"),
                                           (102, True, "failed"), (102, False, "indeterminate")):
            with self.subTest(timestamp=timestamp, error=error, outcome=outcome):
                self.messages[1]["written_at"] = timestamp
                self.messages[1]["message"]["content"][0]["is_error"] = error
                self.calls[0]["outcome"] = outcome
                with patch.object(scoped, "invoke", side_effect=self.reply):
                    status, output = self.wait()
                self.assertEqual(status, 1, output)
                self.assertIn("examined_sessions=wal:0,store:1 qualifying_results=0", output)

    def test_new_storage_requires_real_scope_and_recorded_aft_dispatch(self):
        self.store()
        variants = [dict(self.admission, started_at_ms=100),
                    dict(self.admission, scope=dict(self.admission["scope"], principal={"kind": "direct"})),
                    dict(self.admission, scope=dict(self.admission["scope"], owner={"kind": "direct"})),
                    dict(self.admission, scope=dict(self.admission["scope"], ref=""))]
        original = self.admission
        for admission in variants:
            self.admission = admission
            with self.subTest(admission=admission), patch.object(scoped, "invoke", side_effect=self.reply):
                status, output = self.wait()
            self.assertEqual(status, 1, output)
        self.admission = original
        self.calls[0]["dispatched_to"] = "other-module"
        with patch.object(scoped, "invoke", side_effect=self.reply):
            self.assertEqual(self.wait()[0], 1)

    def test_new_storage_missing_scope_is_could_not_tell_not_no_traffic(self):
        self.store()
        del self.admission["scope"]
        with patch.object(scoped, "invoke", side_effect=self.reply):
            status, output = self.wait()
        self.assertEqual(status, 1)
        self.assertIn("could not tell", output)
        self.assertIn("this daemon's run.admission serves no scope", output)
        self.assertNotIn("no scoped traffic", output)

    def test_failed_daemon_read_reports_could_not_tell_with_error(self):
        self.store()
        failure = subprocess.CompletedProcess([], 7, '{"error":{"code":"unavailable"}}', "DO-NOT-PRINT")
        with patch.object(scoped, "invoke", return_value=failure):
            status, output = self.wait()
        self.assertEqual(status, 1)
        self.assertIn("could not tell", output)
        self.assertIn("session.read: client exited 7 code=unavailable", output)
        self.assertIn("examined_sessions=wal:0,store:0 qualifying_results=0 could_not_tell=1", output)
        self.assertNotIn("no scoped traffic", output)

    def test_new_storage_pages_keep_call_identity_across_boundaries(self):
        self.store()
        original = self.reply

        def reply(command, timeout):
            if command[command.index("--op") + 1] != "session.read":
                return original(command, timeout)
            self.commands.append(command)
            params = json.loads(command[-1])
            page = ({"messages": self.messages[:1], "next_from_ordinal": 2, "history_version": 1}
                    if params["from_ordinal"] == 1 else {"messages": self.messages[1:], "history_version": 1})
            return subprocess.CompletedProcess(command, 0, json.dumps({"result": page}), "")

        with patch.object(scoped, "invoke", side_effect=reply):
            status, output = self.wait()
        self.assertEqual(status, 0, output)
        self.assertEqual(json.loads(self.commands[1][-1])["history_version"], 1)


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
        self.assertEqual(records["worker-accept"], args)

    def test_scoped_leg_failure_blocks_publication_and_reports_rollback(self):
        result = self.fixture.deploy(FAIL_STEP="scoped-accept")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.fixture.assert_gated_steps(["place", "verify", "accept", "worker-accept", "scoped-accept"])
        self.assertIn("SCOPED DISPATCH ACCEPTANCE FAILED; not publishing", result.stdout + result.stderr)
        self.assertIn("Rollback:", result.stdout + result.stderr)

    def test_direct_refusal_warns_and_runs_both_other_checks_without_claiming_pass(self):
        result = self.fixture.deploy(ACCEPT_STATUS="3")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.fixture.assert_gated_steps(["place", "verify", "accept", "worker-accept", "scoped-accept", "publish"])
        self.assertIn("WARNING -- direct tool acceptance: TOOL REFUSED (project_required), exit 3",
                      result.stdout + result.stderr)
        self.assertIn("acceptance direct: TOOL REFUSED (project_required); worker: PASS; scoped: PASS", result.stdout)
        self.assertNotIn("acceptance PASS", result.stdout)

    def test_direct_real_failure_still_stops_other_checks_and_publication(self):
        result = self.fixture.deploy(ACCEPT_STATUS="1")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.fixture.assert_gated_steps(["place", "verify", "accept"])
        self.assertIn("LIVE ACCEPTANCE FAILED", result.stdout + result.stderr)


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

    def test_project_required_has_distinct_verdict_and_exit_code(self):
        for text in (contract.PROJECT_REQUIRED, "tool/route rejected: project_required — newer explanation"):
            with self.subTest(text=text):
                result = self.run_turn(TOOL_TEXT=text)
                self.assertEqual(result.returncode, 3, result.stdout + result.stderr)
                self.assertIn("acceptance-turn: TOOL REFUSED (project_required) -- the test session has no scope, "
                              "so the tool module refused its route; dispatch, result and run completion worked", result.stdout)
                self.assertNotIn("acceptance-turn: PASS", result.stdout)
                self.assertNotIn("acceptance-turn: FAILED", result.stdout + result.stderr)

    def test_successful_result_still_passes_on_declared_and_legacy_builds(self):
        for declaration in ("bind-no-project/v1", "legacy"):
            with self.subTest(declaration=declaration):
                self.placed.write_text(declaration + "\n")
                result = self.run_turn(TOOL_ERROR="false")
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_other_tool_errors_fail_with_the_result_text(self):
        for text in (contract.PROJECT_REQUIRED.replace("project_required", "permission_denied"),
                     "project_required", "is_error=false"):
            with self.subTest(text=text):
                result = self.run_turn(TOOL_TEXT=text)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn("TOOL RESULT ERRORED -- " + text, result.stderr)

    def test_project_required_is_not_a_pass_on_legacy_builds_either(self):
        self.placed.write_text("legacy\n")
        result = self.run_turn(TOOL_TEXT=contract.PROJECT_REQUIRED)
        self.assertEqual(result.returncode, 3, result.stdout + result.stderr)

    def test_missing_dispatch_result_or_completion_still_fails(self):
        for missing in ("NO_DISPATCH", "NO_RESULT", "NO_COMPLETE"):
            with self.subTest(missing=missing):
                result = self.run_turn(TOOL_ERROR="false", **{missing: "1"})
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)

    def test_project_required_without_completed_run_is_a_failure(self):
        result = self.run_turn(TOOL_TEXT=contract.PROJECT_REQUIRED, NO_COMPLETE="1")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("RUN DID NOT COMPLETE", result.stderr)
        self.assertNotIn("acceptance-turn: TOOL REFUSED", result.stdout)

    def test_role_describe_declaration_is_used_when_available(self):
        with patch.dict(contract.os.environ, {"BROCA_BIN": "/not-a-placed-binary"}), \
                patch.object(contract.subprocess, "run") as probe:
            self.assertTrue(contract.bind_no_project({"capabilities": [contract.NO_PROJECT]}))
        probe.assert_not_called()

    def test_a_later_success_does_not_hide_another_tool_error(self):
        output = ("TOOL_RESULT seq=2 id=one is_error=true text=other refusal\n"
                  "TOOL_RESULT seq=3 id=two is_error=false text=ok\n")
        self.assertEqual(contract.tool_result_verdict(output), (1, "TOOL RESULT ERRORED -- other refusal"))


class WorkerTrafficFixture(unittest.TestCase):
    def setUp(self):
        self.worker = load("worker_traffic_fixture", "scripts/acceptance-worker-turn.py")
        scratch = ROOT / "target/scratch"
        scratch.mkdir(parents=True, exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.address = "1234567890abcdef"
        self.start = {"type": "run_started", "run_id": "DO-NOT-PRINT-RUN-ID", "ts_ms": 101,
                      "session_class": "mason",
                      "manifest": {"fetches": [{"provider": "aft", "declarations_frozen": {"late_results": True}}]}}
        self.finish = {"type": "run_finished", "reason": "completed"}

    def write(self, records=None, folder="wal", address=None, requires=None):
        path = self.root / folder / ((address or self.address) + ".wal")
        path.parent.mkdir(exist_ok=True)
        records = records if records is not None else [self.start, self.finish]
        requires = self.worker.REQUIRED if requires is None else requires
        path.write_bytes(b"".join(frame(seq, record, list(requires) if record["type"] == "run_started" else None)
                                  for seq, record in enumerate(records, 1)))
        return path

    def wait(self, timeout=0):
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(out):
            status = self.worker.wait_for_worker(self.root, 100, timeout, poll=1)
        self.assertNotIn("DO-NOT-PRINT", out.getvalue())
        return status, out.getvalue()

class WorkerTrafficTests(WorkerTrafficFixture):
    def test_completed_declaring_worker_in_every_live_folder_passes(self):
        for folder in ("wal", "wal-durable", "wal-ephemeral"):
            for session_class in ("mason", "sidekick"):
                for declaration in ({"late_results": True}, {"result_formats": {"read": "text"}}):
                    with self.subTest(folder=folder, session_class=session_class, declaration=declaration):
                        start = dict(self.start, session_class=session_class,
                                     manifest={"fetches": [{"provider": "aft", "declarations_frozen": declaration}]})
                        path = self.write([start, {"type": "message", "text": "DO-NOT-PRINT-CONTENT"}, self.finish],
                                          folder=folder)
                        status, output = self.wait()
                        self.assertEqual(status, 0, output)
                        self.assertIn(f"session={self.address} class={session_class}", output)
                        self.assertIn("admitted, completed", output)
                        self.assertIn("terminal_counts=completed:1", output)
                        path.unlink()

    def test_no_candidate_is_a_named_failure_with_both_classes_and_zero_counts(self):
        status, output = self.wait()
        self.assertEqual(status, 1)
        self.assertIn("no_worker_candidate", output)
        self.assertIn("classes=mason,sidekick candidates=0", output)
        self.assertIn("class_counts=mason:0,sidekick:0 pending=0 terminal_counts=completed:0", output)

    def test_pre_restart_and_non_worker_starts_are_not_candidates(self):
        starts = [dict(self.start, ts_ms=value) for value in (99, 100, True, "101")]
        starts += [dict(self.start, session_class=value) for value in (None, "head", "acceptance", "consult", "worker")]
        for start in starts:
            with self.subTest(start=start):
                self.write([start, self.finish])
                status, output = self.wait()
                self.assertEqual(status, 1, output)
                self.assertIn("no_worker_candidate", output)

    def test_declaration_less_aft_fetch_fails_even_with_a_later_declaring_completion(self):
        for declarations in (None, {}, {"late_results": False, "result_formats": {}}, {"call_keepalive": True}):
            with self.subTest(declarations=declarations):
                manifest = {"fetches": [{"provider": "aft", "declarations_frozen": declarations}]}
                self.write([dict(self.start, manifest=manifest), self.finish,
                            dict(self.start, ts_ms=102), self.finish])
                status, output = self.wait()
                self.assertEqual(status, 1, output)
                self.assertIn("no frozen result formats or late_results declaration", output)

    def test_runs_without_an_aft_fetch_are_ignored_and_cannot_pass(self):
        manifests = (None, {}, {"fetches": []},
                     {"fetches": [{"provider": "other", "declarations_frozen": {"late_results": True}}]},
                     {"fetches": [{"declarations_frozen": {"late_results": True}}]},
                     {"tools": [{"provider": "aft"}]})
        for manifest in manifests:
            with self.subTest(manifest=manifest):
                mason = dict(self.start, manifest=manifest)
                sidekick = dict(mason, session_class="sidekick", ts_ms=102)
                self.write([mason, self.finish, sidekick, self.finish], requires=[])
                status, output = self.wait()
                self.assertEqual(status, 1, output)
                self.assertIn("no_worker_candidate", output)
                self.assertIn("candidates=0 class_counts=mason:0,sidekick:0 pending=0", output)
                self.assertIn("terminal_counts=completed:0 ignored_counts=mason:1,sidekick:1", output)
                self.assertNotIn("missing required", output)

    def test_other_provider_declarations_cannot_certify_an_aft_fetch(self):
        for session_class in ("mason", "sidekick"):
            with self.subTest(session_class=session_class):
                manifest = {"fetches": [
                    {"provider": "other", "declarations_frozen": {"late_results": True}},
                    {"provider": "aft"},
                ]}
                self.write([dict(self.start, session_class=session_class, manifest=manifest), self.finish])
                status, output = self.wait()
                self.assertEqual(status, 1, output)
                self.assertIn(f"session={self.address} class={session_class}", output)
                self.assertIn("AFT fetch has no frozen result formats or late_results declaration", output)

    def test_planless_structured_output_sidekick_is_ignored_beside_a_completed_aft_candidate(self):
        sidekick = {
            "type": "run_started", "run_id": "DO-NOT-PRINT-SIDEKICK-RUN", "ts_ms": 102,
            "session_class": "sidekick", "retention": 3600,
            "session": {"project_root": "broca-unscoped-v1", "harness": "core", "session": "fixture-sidekick"},
            "config": {
                "model": {"provider_module_id": "fixture", "model_id": "fixture-model"},
                "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
                "response_format": {"type": "text"}, "context_epoch": 0, "renderer_version": 1,
                "frozen_render_config": {"structured_output": {
                    "mode": "final_text", "name": "result", "schema_jcs": '{"type":"object"}',
                    "enforcement": "best_effort", "reason": "route_unsupported", "delivery": "unconstrained",
                }},
            },
            "input": [], "origin": {"kind": "fresh"},
        }
        self.write([sidekick, self.finish], folder="wal-ephemeral",
                   requires=["session-class/v1", "structured-output/v1"])
        self.write(address="0000000000000000")
        status, output = self.wait()
        self.assertEqual(status, 0, output)
        self.assertIn("session=0000000000000000 class=mason", output)
        self.assertIn("candidates=1 class_counts=mason:1,sidekick:0 pending=0", output)
        self.assertIn("terminal_counts=completed:1 ignored_counts=mason:0,sidekick:1", output)

    def test_ignored_completion_does_not_certify_a_pending_aft_candidate(self):
        ignored = dict(self.start, session_class="sidekick", ts_ms=102, manifest=None)
        self.write([ignored, self.finish], folder="wal-ephemeral", requires=["session-class/v1"])
        self.write([self.start])
        status, output = self.wait()
        self.assertEqual(status, 1, output)
        self.assertIn("worker_completion_timeout", output)
        self.assertIn("candidates=1 class_counts=mason:1,sidekick:0 pending=1", output)
        self.assertIn("terminal_counts=completed:0 ignored_counts=mason:0,sidekick:1", output)

    def test_ignored_counts_are_deduplicated_across_polls(self):
        ignored = dict(self.start, session_class="sidekick", manifest=None)
        self.write([ignored, self.finish], requires=["session-class/v1"])
        with patch.object(self.worker.time, "monotonic", side_effect=[0, 0, 120]), \
                patch.object(self.worker.time, "sleep") as sleep:
            status, output = self.wait(timeout=120)
        self.assertEqual(status, 1, output)
        self.assertIn("no_worker_candidate", output)
        self.assertIn("candidates=0", output)
        self.assertIn("ignored_counts=mason:0,sidekick:1", output)
        sleep.assert_called_once_with(1)

    def test_both_required_features_are_checked_on_the_worker_start(self):
        for missing in self.worker.REQUIRED:
            with self.subTest(missing=missing):
                self.write(requires=[feature for feature in self.worker.REQUIRED if feature != missing])
                status, output = self.wait()
                self.assertEqual(status, 1, output)
                self.assertIn("missing required WAL feature(s): " + missing, output)

    def test_only_a_candidates_own_single_completed_terminal_can_certify_it(self):
        for records in ([self.start],
                        [self.start, dict(self.finish, reason="error", error={"message": "DO-NOT-PRINT-ERROR"})],
                        [self.start, dict(self.finish, reason="max_steps")],
                        [self.start, dict(self.start, session_class="head", ts_ms=102), self.finish],
                        [self.start, dict(self.start, ts_ms=99), self.finish],
                        [self.finish, self.start],
                        [self.start, self.finish, self.finish]):
            with self.subTest(records=records):
                self.write(records)
                status, output = self.wait()
                self.assertEqual(status, 1, output)
                self.assertIn("FAILED", output)

    def test_long_running_first_candidate_does_not_block_a_later_completed_candidate(self):
        self.write([self.start])
        later = dict(self.start, session_class="sidekick", ts_ms=102)
        later_address = "0000000000000000"
        with patch.object(self.worker.time, "monotonic", side_effect=[0, 0, 120]), \
                patch.object(self.worker.time, "sleep", side_effect=lambda _: self.write(
                    [later, self.finish], address=later_address)) as sleep:
            status, output = self.wait(timeout=120)
        self.assertEqual(status, 0, output)
        self.assertIn(f"session={later_address} class=sidekick", output)
        self.assertIn("candidates=2 class_counts=mason:1,sidekick:1 pending=1", output)
        self.assertIn("terminal_counts=completed:1", output)
        sleep.assert_called_once_with(1)

    def test_bad_admission_after_an_earlier_completion_beats_success_in_the_snapshot(self):
        for session_class in ("mason", "sidekick"):
            for same_wal in (True, False):
                for missing in (None, "session-class/v1", "provider-declarations/v1"):
                    with self.subTest(session_class=session_class, same_wal=same_wal, missing=missing):
                        bad = dict(self.start, session_class=session_class, ts_ms=102,
                                   manifest={"fetches": [{"provider": "aft"}]} if missing is None else self.start["manifest"])
                        requires = [feature for feature in self.worker.REQUIRED if feature != missing]
                        good_path = self.write(address="0000000000000000")
                        bad_path = good_path if same_wal else self.write([bad])
                        bad_frame = frame(3 if same_wal else 1, bad, requires)
                        bad_path.write_bytes((good_path.read_bytes() if same_wal else b"") + bad_frame)
                        status, output = self.wait()
                        self.assertEqual(status, 1, output)
                        self.assertIn(f"session={bad_path.stem} class={session_class}", output)
                        self.assertIn(missing or "no frozen result formats or late_results declaration", output)
                        self.assertNotIn("PASS", output)
                        bad_path.unlink()
                        if not same_wal:
                            good_path.unlink()

    def test_bad_admission_fails_as_soon_as_it_appears_while_another_run_is_pending(self):
        self.write([self.start])
        bad = dict(self.start, session_class="sidekick", ts_ms=102, manifest={"fetches": [{"provider": "aft"}]})
        with patch.object(self.worker.time, "monotonic", side_effect=[0, 0]), \
                patch.object(self.worker.time, "sleep", side_effect=lambda _: self.write(
                    [self.start, bad])) as sleep:
            status, output = self.wait(timeout=600)
        self.assertEqual(status, 1, output)
        self.assertIn(f"session={self.address} class=sidekick", output)
        self.assertIn("no frozen result formats or late_results declaration", output)
        sleep.assert_called_once_with(1)

    def test_other_endings_are_counted_but_do_not_block_a_later_completion(self):
        records = []
        for index, reason in enumerate(("error", "interrupted", "max_steps")):
            records.extend([dict(self.start, session_class="mason" if index % 2 == 0 else "sidekick",
                                 ts_ms=101 + index),
                            dict(self.finish, reason=reason, error={"message": "DO-NOT-PRINT-ERROR"})])
        self.write(records)
        later = dict(self.start, session_class="sidekick", ts_ms=104)
        with patch.object(self.worker.time, "monotonic", side_effect=[0, 0]), \
                patch.object(self.worker.time, "sleep", side_effect=lambda _: self.write(
                    records + [later, self.finish])) as sleep:
            status, output = self.wait(timeout=600)
        self.assertEqual(status, 0, output)
        self.assertIn("candidates=4 class_counts=mason:2,sidekick:2 pending=0", output)
        self.assertIn("terminal_counts=completed:1,error:1,interrupted:1,max_steps:1", output)
        sleep.assert_called_once_with(1)

    def test_completion_timeout_names_both_classes_and_counts_each_run_only_once(self):
        records = []
        for index, reason in enumerate(("error", "interrupted", "max_steps")):
            records.extend([dict(self.start, session_class="mason" if index % 2 == 0 else "sidekick",
                                 ts_ms=101 + index), dict(self.finish, reason=reason)])
        self.write(records + [dict(self.start, session_class="sidekick", ts_ms=104)])
        with patch.object(self.worker.time, "monotonic", side_effect=[0, 0, 5, 600]), \
                patch.object(self.worker.time, "sleep") as sleep:
            status, output = self.wait(timeout=600)
        self.assertEqual(status, 1, output)
        self.assertIn("worker_completion_timeout", output)
        self.assertIn("classes=mason,sidekick candidates=4", output)
        self.assertIn("class_counts=mason:2,sidekick:2 pending=1", output)
        self.assertIn("terminal_counts=completed:0,error:1,interrupted:1,max_steps:1", output)
        self.assertEqual(sleep.call_count, 2)

    def test_unrecognized_terminal_reasons_are_counted_without_printing_their_value(self):
        for reason in ("DO-NOT-PRINT-REASON", {"content": "DO-NOT-PRINT-REASON"}, None):
            with self.subTest(reason=reason):
                self.write([self.start, dict(self.finish, reason=reason)])
                status, output = self.wait()
                self.assertEqual(status, 1, output)
                self.assertIn("terminal_counts=completed:0,unclassified_terminal:1", output)

    def test_no_candidate_wait_is_bounded_and_observes_new_admissions(self):
        with patch.object(self.worker.time, "monotonic", side_effect=[0, 0, 120]), \
                patch.object(self.worker.time, "sleep") as sleep:
            status, output = self.wait(timeout=120)
        self.assertEqual(status, 1, output)
        self.assertIn("no_worker_candidate", output)
        sleep.assert_called_once_with(1)
        with patch.object(self.worker.time, "monotonic", side_effect=[0, 0]), \
                patch.object(self.worker.time, "sleep", side_effect=lambda _: self.write()) as sleep:
            status, output = self.wait(timeout=120)
        self.assertEqual(status, 0, output)
        sleep.assert_called_once_with(1)

    def test_unverified_snapshots_are_retried_and_never_partially_certified(self):
        path = self.write()
        raw = path.read_bytes()
        for bad in (raw + b"torn", raw[:-1] + bytes([raw[-1] ^ 1]),
                    raw + frame(3, {"type": "DO-NOT-PRINT"}, ["unknown-feature/v1"])):
            with self.subTest(size=len(bad)):
                path.write_bytes(bad)
                status, output = self.wait()
                self.assertEqual(status, 1, output)
                self.assertIn("unverified_wal", output)
        with patch.object(self.worker.time, "monotonic", side_effect=[0, 0]), \
                patch.object(self.worker.time, "sleep", side_effect=lambda _: path.write_bytes(raw)):
            status, output = self.wait(timeout=120)
        self.assertEqual(status, 0, output)


class StoreWorkerTrafficTests(WorkerTrafficFixture):
    def setUp(self):
        super().setUp()
        self.admission = {"run_id": "DO-NOT-PRINT-RUN-ID", "session_class": "sidekick",
                          "started_at_ms": 101, "storage": "store",
                          "fetches": self.start["manifest"]["fetches"]}
        self.commands = []

    def store(self, folder="session-store", address=None, changed_ms=101):
        address = address or self.address
        root = self.root / folder
        (root / "heads" / address).mkdir(parents=True)
        (root / "heads" / address / "head").write_bytes(b"DO-NOT-READ-ENCRYPTED-HEAD")
        log = root / "logs" / address / ("0" * 32)
        log.parent.mkdir(parents=True)
        log.write_bytes(b"DO-NOT-READ-ENCRYPTED-LOG")
        timestamp = changed_ms * 1_000_000
        os.utime(log, ns=(timestamp, timestamp))
        os.utime(log.parent, ns=(timestamp, timestamp))
        with sqlite3.connect(self.root / "run-index.db") as db:
            db.execute("CREATE TABLE IF NOT EXISTS agent_data_session_ref "
                       "(session_ref TEXT, address TEXT, project_root TEXT, harness TEXT, session TEXT)")
            db.execute("INSERT INTO agent_data_session_ref VALUES (?, ?, ?, ?, ?)",
                       ("f" * 32, address, "broca-unscoped-v1", "core", "DO-NOT-PRINT-SESSION"))
            db.execute("CREATE TABLE IF NOT EXISTS run_index "
                       "(run_id TEXT PRIMARY KEY, session TEXT, state TEXT, state_changed_ms INTEGER)")
            bind = {"project_root": "broca-unscoped-v1", "harness": "core", "session": "DO-NOT-PRINT-SESSION"}
            db.execute("INSERT OR REPLACE INTO run_index VALUES (?, ?, ?, ?)",
                       (self.admission["run_id"], json.dumps(bind, separators=(",", ":")), "completed", 102))
        return log

    def reply(self, command, timeout):
        self.commands.append(command)
        op = command[command.index("--op") + 1]
        if op == "session.head":
            return json.dumps({"result": {"last_run_state": {"run_id": self.admission["run_id"],
                                                              "state": "completed"}}})
        if op == "run.admission":
            return json.dumps({"result": self.admission})
        self.assertEqual(op, "run.status")
        return json.dumps({"result": {"state": "completed"}})

    def test_store_completed_sidekick_passes_in_every_storage_class(self):
        for folder in ("session-store", "session-store-durable", "session-store-ephemeral"):
            with self.subTest(folder=folder):
                self.store(folder)
                with patch.object(self.worker, "invoke", side_effect=self.reply):
                    status, output = self.wait()
                self.assertEqual(status, 0, output)
                self.assertIn("class=sidekick", output)
                self.assertIn("storage_counts=wal:0,store:1", output)
                self.assertEqual(len(self.commands), 3)
                self.commands.clear()

    def test_store_missing_frozen_declarations_fails_beside_wal_success(self):
        self.store()
        self.write()
        self.admission["fetches"] = [{"provider": "aft"}]
        with patch.object(self.worker, "invoke", side_effect=self.reply):
            status, output = self.wait()
        self.assertEqual(status, 1, output)
        self.assertIn("AFT fetch has no frozen result formats or late_results declaration", output)
        self.assertIn("storage_counts=wal:1,store:1", output)

    def test_unreadable_store_client_never_falls_through_to_wal_success(self):
        self.store()
        self.write()
        for error in (FileNotFoundError("DO-NOT-PRINT-CLIENT"),
                      ValueError("DO-NOT-PRINT-DAEMON-REFUSAL")):
            with self.subTest(error=type(error).__name__), \
                    patch.object(self.worker, "invoke", side_effect=error):
                status, output = self.wait()
            self.assertEqual(status, 1, output)
            self.assertIn("unreadable_store_client", output)
            self.assertNotIn("PASS", output)

    def test_store_log_append_is_seen_even_when_folder_mtime_predates_restart(self):
        log = self.store()
        os.utime(log.parent, ns=(99_000_000, 99_000_000))
        with patch.object(self.worker, "invoke", side_effect=self.reply):
            status, output = self.wait()
        self.assertEqual(status, 0, output)

    def test_stale_store_logs_do_not_need_a_client_or_database(self):
        self.store(changed_ms=99)
        (self.root / "run-index.db").unlink()
        with patch.object(self.worker, "invoke") as invoke:
            status, output = self.wait()
        self.assertEqual(status, 1, output)
        self.assertIn("no_worker_candidate", output)
        invoke.assert_not_called()

    def test_store_runs_come_from_the_daemon_not_the_wal_run_index(self):
        # New-storage runs never appear in run_index, a WAL projection.
        self.store()
        with sqlite3.connect(self.root / "run-index.db") as db:
            db.execute("DELETE FROM run_index")
        with patch.object(self.worker, "invoke", side_effect=self.reply):
            status, output = self.wait()
        self.assertEqual(status, 0, output)
        self.assertIn("storage_counts=wal:0,store:1", output)

    def test_store_log_written_without_a_new_run_is_not_a_candidate_or_a_failure(self):
        # A head's idle warm-up appends to its log after the restart but starts no run.
        self.store()
        self.admission["started_at_ms"] = 99
        self.write()
        with patch.object(self.worker, "invoke", side_effect=self.reply):
            status, output = self.wait()
        self.assertEqual(status, 0, output)
        self.assertIn("storage_counts=wal:1,store:0", output)

    def test_store_session_with_no_run_yet_is_skipped(self):
        self.store()
        self.write()
        with patch.object(self.worker, "invoke", return_value=json.dumps({"result": {}})):
            status, output = self.wait()
        self.assertEqual(status, 0, output)
        self.assertIn("storage_counts=wal:1,store:0", output)

    def test_missing_store_inventory_is_named_and_is_not_created(self):
        self.store()
        (self.root / "run-index.db").unlink()
        self.write()
        status, output = self.wait()
        self.assertEqual(status, 1, output)
        self.assertIn("unreadable_store_inventory", output)
        self.assertFalse((self.root / "run-index.db").exists())

    def test_store_start_boundary_and_class_are_checked_not_inferred_from_log_activity(self):
        self.store()
        for update in ({"started_at_ms": 100}, {"session_class": "head"}, {"session_class": None}):
            with self.subTest(update=update), patch.object(self.worker, "invoke", side_effect=self.reply):
                original = self.admission.copy()
                self.admission.update(update)
                status, output = self.wait()
                self.admission = original
            self.assertEqual(status, 1, output)
            self.assertIn("no_worker_candidate", output)

    def test_store_noncompleted_status_cannot_certify_admission(self):
        self.store()
        for state in ("active", "paused", "error", "interrupted", "DO-NOT-PRINT-STATUS"):
            head = {"last_run_state": {"run_id": self.admission["run_id"], "state": state}}
            with self.subTest(state=state), patch.object(self.worker, "invoke", side_effect=[
                    json.dumps({"result": head}), json.dumps({"result": self.admission}),
                    json.dumps({"result": {"state": state}})]):
                status, output = self.wait()
            self.assertEqual(status, 1, output)
            self.assertIn("worker_completion_timeout", output)

    def test_store_aft_declarations_cannot_be_borrowed_from_another_provider(self):
        self.store()
        self.admission["fetches"] = [{"provider": "other", "declarations_frozen": {"late_results": True}},
                                     {"provider": "aft"}]
        with patch.object(self.worker, "invoke", side_effect=self.reply):
            status, output = self.wait()
        self.assertEqual(status, 1, output)
        self.assertIn("AFT fetch has no frozen", output)

    def test_store_planless_worker_is_counted_but_does_not_certify_a_turn(self):
        self.store()
        self.admission["fetches"] = []
        with patch.object(self.worker, "invoke", side_effect=self.reply):
            status, output = self.wait()
        self.assertEqual(status, 1, output)
        self.assertIn("no_worker_candidate", output)
        self.assertIn("ignored_counts=mason:0,sidekick:1 storage_counts=wal:0,store:1", output)


class WorkerAddressTests(unittest.TestCase):
    def setUp(self):
        suite = load("worker_address_fixture", "scripts/test-acceptance-worker-turn.py")
        self.fixture = suite.WorkerAcceptance("test_main_sends_class_plan_and_one_capped_turn_then_reads_wal")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.acceptance = suite.acceptance

    def test_legacy_build_keeps_callers_folder_wal(self):
        result, output, invoke, check = self.fixture.run_main([
            '{"result":{"session_class":{"backup_classes":[]}}}',
            '{"event":"send","state":"active","run_id":"new-run"}',
        ])
        self.assertEqual(result, 0, output)
        command = invoke.call_args.args[0]
        session = command[command.index("--session") + 1]
        root = str(Path(self.fixture.tmp.name).resolve())
        expected = self.acceptance.wal_name(root, "broca", session)
        self.assertEqual(check.call_args.args[0], Path(self.fixture.tmp.name) / "wal" / expected)

    def test_project_free_build_observes_real_worker_without_compose_or_send(self):
        report = Path(self.fixture.tmp.name) / "placement.json"
        report.write_text('{"restart_observed_at_ms":100}')
        state_root = Path(self.fixture.tmp.name)
        path = state_root / "wal-durable/1234567890abcdef.wal"
        path.parent.mkdir()
        start = dict(self.fixture.start, session_class="mason", ts_ms=101,
                     manifest={"fetches": [{"provider": "aft", "declarations_frozen": {"late_results": True}}]})
        path.write_bytes(frame(1, start, list(self.acceptance.REQUIRED))
                         + frame(2, {"type": "run_finished", "reason": "completed"}))
        refused_fetch = self.acceptance.ClientFailure(self.acceptance.subprocess.CompletedProcess(
            [], 1, "", "compose-plan: fetch_unavailable: provider aft refused the fetch: invalid_request"))
        result, output, invoke, check = self.fixture.run_main([
            '{"result":{"session_class":{"backup_classes":["head","mason"]}}}',
        ], no_project=True, extra_args=("--placement-report", str(report), "--timeout", "0"),
            compose_answers=[refused_fetch])
        self.assertEqual(result, 0, output)
        self.assertIn("class=mason", output)
        self.assertEqual(invoke.call_count, 1)
        self.assertIn("describe", invoke.call_args.args[0])
        check.assert_not_called()

    def test_project_free_build_requires_valid_restart_boundary_and_bounded_wait(self):
        report = Path(self.fixture.tmp.name) / "placement.json"
        cases = [({"other": "DO-NOT-PRINT"}, "0"),
                 ({"restart_observed_at_ms": "DO-NOT-PRINT"}, "0"),
                 ({"restart_observed_at_ms": True}, "0"), ({"restart_observed_at_ms": 0}, "0")]
        cases += [({"restart_observed_at_ms": 100}, timeout) for timeout in ("601", "-1", "nan", "inf")]
        for boundary, timeout in cases:
            with self.subTest(boundary=boundary, timeout=timeout):
                report.write_text(json.dumps(boundary))
                result, output, invoke, check = self.fixture.run_main(['{"result":{}}'], no_project=True,
                    extra_args=("--placement-report", str(report), "--timeout", timeout))
                self.assertEqual(result, 1, output)
                self.assertIn("valid placement report restart boundary", output)
                self.assertNotIn("DO-NOT-PRINT", output)
                self.assertEqual(invoke.call_count, 1)
                check.assert_not_called()
        result, output, invoke, check = self.fixture.run_main(['{"result":{}}'], no_project=True)
        self.assertEqual(result, 1, output)
        self.assertIn("valid placement report restart boundary", output)
        self.assertEqual(invoke.call_count, 1)
        check.assert_not_called()

    def test_project_free_no_candidate_fails_without_compose_or_send(self):
        report = Path(self.fixture.tmp.name) / "placement.json"
        report.write_text('{"restart_observed_at_ms":100}')
        result, output, invoke, check = self.fixture.run_main(['{"result":{}}'], no_project=True,
            extra_args=("--placement-report", str(report), "--timeout", "0"))
        self.assertEqual(result, 1, output)
        self.assertIn("no_worker_candidate", output)
        self.assertIn("classes=mason,sidekick candidates=0", output)
        self.assertEqual(invoke.call_count, 1)
        check.assert_not_called()

    def test_project_free_compose_only_refuses_without_fetching(self):
        result, output, invoke, _ = self.fixture.run_main(['{"result":{}}'], no_project=True,
                                                       extra_args=("--compose-only",))
        self.assertEqual(result, 1, output)
        self.assertIn("compose-only requires a project-bearing build", output)
        self.assertEqual(invoke.call_count, 1)

    def test_project_free_observation_defaults_to_ten_minutes(self):
        report = Path(self.fixture.tmp.name) / "placement.json"
        report.write_text('{"restart_observed_at_ms":100}')
        with patch.object(self.acceptance, "wait_for_worker", return_value=1) as wait:
            result, output, invoke, _ = self.fixture.run_main(['{"result":{}}'], no_project=True,
                extra_args=("--placement-report", str(report)), env={"HOME": str(Path.home())})
        self.assertEqual(result, 1, output)
        wait.assert_called_once_with(Path(self.fixture.tmp.name), 100, 600,
                                     session_bin=ROOT / "target/release/broca-session",
                                     subc=Path.home() / ".local/share/cortexkit/run/subc-connection.json")
        self.assertEqual(invoke.call_args.args[1], 120)


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
