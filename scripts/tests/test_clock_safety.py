#!/usr/bin/env python3
"""Exercise the real clock safety gates without compiling or publishing a release."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
MARKER = "BROCA_TEST_CLOCK_RETENTION_CLOCK_OVERRIDE_V1"
SENTINEL = "test_clock::test_clock_feature_is_enabled_for_retention_tests"


class TestClockStaging(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="broca-clock-staging-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.artifact = self.root / "artifact"
        self.env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")

    def probe(self, content):
        self.artifact.write_bytes(content)
        return subprocess.run(
            ["bash", str(ROOT / "scripts/stage-release.sh"), "--probe-only", str(self.artifact)],
            cwd=ROOT, env=self.env, capture_output=True, text=True, timeout=30,
        )

    def test_test_clock_marker_refuses_artifact(self):
        result = self.probe(b"ordinary control\0" + MARKER.encode() + b"\0")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("artifact contains the test-clock override marker", result.stderr)
        self.assertNotIn("No --marker given", result.stdout)

    def test_frozen_compaction_clock_artifact_is_refused(self):
        result = self.probe(MARKER.encode() + b'\0{"now_ms":123456789}\0')
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("artifact contains the test-clock override marker", result.stderr)

    def test_artifact_without_test_clock_marker_is_accepted(self):
        result = self.probe(b"ordinary production artifact\0")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("test-clock absent (planted strings control reads 1)", result.stdout)

    def test_blind_marker_reader_refuses_instead_of_claiming_absence(self):
        tools = self.root / "tools"
        tools.mkdir()
        reader = tools / "strings"
        reader.write_text("#!/bin/sh\nprintf 'unrelated control\\n'\n")
        reader.chmod(0o755)
        self.env["PATH"] = f"{tools}:{os.environ['PATH']}"
        result = self.probe(b"ordinary production artifact\0")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("cannot see the planted test-clock marker control", result.stderr)

    def test_artifact_reader_failure_is_not_marker_absence(self):
        tools = self.root / "tools"
        tools.mkdir()
        reader = tools / "strings"
        reader.write_text(
            "#!/bin/sh\n"
            f'if [ "$1" = "{self.artifact}" ]; then exit 7; fi\n'
            f'exec "{shutil.which("strings")}" "$@"\n'
        )
        reader.chmod(0o755)
        self.env["PATH"] = f"{tools}:{os.environ['PATH']}"
        result = self.probe(b"ordinary production artifact\0")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("could not read the artifact", result.stderr)


# Cargo's inventories and executed nextest output are independently controlled so
# the fixture can reproduce an otherwise-green run whose clock test never existed.
CARGO = r'''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ["CLOCK_GATE_EVENTS"], "a") as log:
    log.write(json.dumps(args) + "\n")
if args[0] in ("fmt", "clippy"):
    print("fixture " + args[0])
elif args[0] == "test" and "--list" in args:
    if "--doc" in args:
        print("0 tests, 0 benchmarks")
    else:
        for target, count in (("tests/it/main.rs", 1), ("tests/live/main.rs", 0), ("tests/live_e2e.rs", 0)):
            print("Running " + target + " (fixture)")
            print(str(count) + " tests, 0 benchmarks")
elif args[0] == "test":
    print("running 1 test\ntest result: ok. 1 passed; 0 failed")
elif args[:2] == ["nextest", "list"]:
    print(json.dumps({"test-count": 1}))
elif args[:2] == ["nextest", "run"]:
    name = os.environ["CLOCK_GATE_SENTINEL"]
    if "broca-module-serve/test-clock" not in args:
        name = "ordinary_test"
    progress = os.environ.get("CLOCK_GATE_PROGRESS", "(1/1) ")
    print(os.environ.get("CLOCK_GATE_STATUS", "PASS") + " [0.001s] " + progress + "broca-module::it " + name)
    print("Summary [0.001s] 1 test run: 1 passed, 0 skipped")
else:
    raise SystemExit("unexpected cargo invocation: " + repr(args))
'''


class TestClockGate(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="broca-clock-gate-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        scripts = self.root / "scripts"
        scripts.mkdir()
        shutil.copy2(ROOT / "scripts/rust-test-gate.sh", scripts / "rust-test-gate.sh")
        tools = self.root / "tools"
        tools.mkdir()
        for name, content in (("cargo", CARGO), ("cargo-nextest", "#!/bin/sh\nexit 0\n")):
            path = tools / name
            path.write_text(content)
            path.chmod(0o755)
        self.events = self.root / "events"
        self.env = dict(
            os.environ, PATH=f"{tools}:{os.environ['PATH']}",
            CLOCK_GATE_EVENTS=str(self.events), CLOCK_GATE_SENTINEL=SENTINEL,
            CLOCK_GATE_STATUS="PASS", BROCA_ALLOW_LINUX_GATE="1",
            PYTHONDONTWRITEBYTECODE="1",
        )

    def gate(self, **overrides):
        return subprocess.run(
            ["bash", "scripts/rust-test-gate.sh"], cwd=self.root,
            env={**self.env, **overrides}, capture_output=True, text=True, timeout=30,
        )

    def test_executed_sentinel_and_feature_flags_are_required(self):
        result = self.gate()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("executed test-clock sentinel: " + SENTINEL, result.stdout)
        calls = [json.loads(line) for line in self.events.read_text().splitlines()]
        relevant = [args for args in calls if args[0] in ("clippy", "test", "nextest")]
        self.assertGreaterEqual(len(relevant), 6)
        for args in relevant:
            self.assertIn("broca-module-serve/test-clock", args)

    def test_missing_sentinel_refuses_a_green_test_run(self):
        result = self.gate(CLOCK_GATE_SENTINEL="ordinary_test")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("test-clock sentinel did not execute successfully", result.stderr)

    def test_skipped_sentinel_does_not_count_as_execution(self):
        result = self.gate(CLOCK_GATE_STATUS="SKIP")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("test-clock sentinel did not execute successfully", result.stderr)

    def test_ansi_colored_pass_is_recognized(self):
        result = self.gate(CLOCK_GATE_STATUS="\033[32mPASS\033[0m")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("executed test-clock sentinel: " + SENTINEL, result.stdout)

    def test_pass_without_progress_counter_is_recognized(self):
        result = self.gate(CLOCK_GATE_PROGRESS="")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("executed test-clock sentinel: " + SENTINEL, result.stdout)

    def test_padded_progress_counter_is_recognized(self):
        # Real nextest output pads the position to the width of the total; the first gate run
        # after this check landed printed `( 447/1252)` and the sentinel read as missing.
        result = self.gate(CLOCK_GATE_PROGRESS="( 447/1252) ")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("executed test-clock sentinel: " + SENTINEL, result.stdout)

    def test_ci_workspace_matrix_uses_the_clock_armed_gate(self):
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        matrix_job = workflow.split("  postgres-conformance:", 1)[0]
        self.assertIn("os: ubuntu", matrix_job)
        self.assertIn("os: windows", matrix_job)
        self.assertIn("run: ./scripts/rust-test-gate.sh", matrix_job)
        # The suite list lives in scripts/script-suites.sh, which CI and the local gate call.
        self.assertIn("run: scripts/script-suites.sh", matrix_job)
        suites = (ROOT / "scripts/script-suites.sh").read_text()
        self.assertIn("scripts/tests/test_clock_safety.py", suites)


if __name__ == "__main__":
    unittest.main(verbosity=2)
