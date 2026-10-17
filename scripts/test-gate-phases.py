#!/usr/bin/env python3
"""Exercise the real gate scripts with cheap Cargo/rig stand-ins, never host builds."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]

CARGO = r'''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
root = pathlib.Path.cwd()
with open(os.environ["GATE_EVENTS"], "a") as events:
    events.write(json.dumps({"args": args, "wrapper": os.environ.get("RUSTC_WRAPPER")}) + "\n")
if args[0] == "metadata":
    print(json.dumps({"packages": [{"name": "fixture"}]}))
elif args[0] == "fmt":
    sys.exit(int(os.environ.get("FAIL_FMT", "0")))
elif args[0] == "clippy":
    print("fixture clippy output")
    sys.exit(int(os.environ.get("FAIL_CLIPPY", "0")))
elif args[0] == "build":
    target = pathlib.Path(args[args.index("--target-dir") + 1])
    release = "--release" in args
    for name in (["ck-claustrum", "ck-auth"] if release else ["ck-subc", "fake-aft-stub"]):
        binary = target / ("release" if release else "debug") / name
        binary.parent.mkdir(parents=True, exist_ok=True)
        binary.write_text("#!/bin/sh\nexit 0\n")
        binary.chmod(0o755)
    print("fixture vault build" if release else "fixture subc build")
elif args[0] == "test" and "--no-run" in args:
    if "--message-format=json" in args:
        print(json.dumps({"executable": str(root / "test-binary")}, separators=(",", ":")))
    else:
        print("fixture prewarm build")
elif args[0] == "test":
    # Stop after reaching unit tests: discovery and test execution are covered
    # by the real gate, not by this fixture for its phase ordering and logging.
    print("error: fixture stops at unit tests")
    sys.exit(1)
else:
    raise SystemExit("unexpected cargo invocation: " + repr(args))
'''


def fixture_rigs(parent):
    """Create stand-ins for the four executable dependencies used by the gate test."""
    rig_dir = parent / "rigs"
    rig_dir.mkdir()
    paths = {}
    for variable, name in (
        ("SUBC_CORE_BIN", "ck-subc"),
        ("FAKE_AFT_STUB_BIN", "fake-aft-stub"),
        ("CK_CLAUSTRUM_BIN", "ck-claustrum"),
        ("CK_AUTH_BIN", "ck-auth"),
    ):
        path = rig_dir / name
        path.write_text("#!/bin/sh\nexit 0\n")
        path.chmod(0o755)
        paths[variable] = str(path)
    return paths


class GatePhases(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="broca-gate-phases-")
        self.addCleanup(self.tmp.cleanup)
        self.parent = Path(self.tmp.name)
        self.repo = self.parent / "broca"
        scripts = self.repo / "scripts"
        scripts.mkdir(parents=True)
        library = scripts / "lib"
        library.mkdir()
        shutil.copy2(ROOT / "scripts/lib/cargo-watchdog.py", library / "cargo-watchdog.py")
        for name in ("gate.sh", "rust-test-gate.sh", "rust-test-prewarm.sh"):
            shutil.copy2(ROOT / "scripts" / name, scripts / name)
        # These independent dependency gates are not the behavior under test.
        for name in ("check-path-deps.py", "test-check-path-deps.py"):
            (scripts / name).write_text("print('fixture dependency checks')\n")
        # The real suites include this file, so the fixture's gate runs a stub instead.
        self.executable(scripts / "script-suites.sh", '''#!/bin/sh
if [ -n "$FAIL_SUITES" ]; then echo "fixture suite FAIL"; exit 1; fi
echo "fixture script suites"
''')
        home = self.parent / "home"
        bins = home / ".cargo" / "bin"
        bins.mkdir(parents=True)
        self.executable(bins / "cargo", CARGO)
        for name in ("cargo-nextest", "codesign", "pgrep"):
            self.executable(bins / name, "#!/bin/sh\nexit 0\n")
        self.executable(self.repo / "test-binary", "#!/bin/sh\nexit 0\n")
        # Give every test its own log filename, including concurrently run tests.
        self.executable(bins / "date", f'''#!/bin/sh
if [ "$1" = "+%Y%m%d-%H%M%S" ]; then
    echo "{self.parent.name}"
else
    /bin/date "$@"
fi
''')
        self.log = Path("/tmp") / f"broca-gate-{self.parent.name}.log"
        self.addCleanup(self.log.unlink, missing_ok=True)
        self.events = self.parent / "events.jsonl"
        self.env = {k: v for k, v in os.environ.items() if not k.startswith(("SUBC_", "CK_", "FAKE_AFT_", "BROCA_", "FAIL_"))}
        self.env.update(
            HOME=str(home),
            PATH=f"{bins}:{os.environ['PATH']}",
            GATE_EVENTS=str(self.events),
            RUSTC_WRAPPER="fixture-machine-wrapper",
            BROCA_ALLOW_LINUX_GATE="1",
            BROCA_SUBC_TARGET_DIR=str(self.parent / "subc-target"),
            BROCA_CLAUSTRUM_TARGET_DIR=str(self.parent / "vault-target"),
        )
        # The Linux gate refuses before Cargo records events unless every rig is executable.
        self.env.update(fixture_rigs(self.parent))

    @staticmethod
    def executable(path, content):
        path.write_text(content)
        path.chmod(0o755)

    def run_gate(self, **env):
        result = subprocess.run(
            ["bash", "scripts/gate.sh"], cwd=self.repo,
            env={**self.env, **env}, capture_output=True, text=True, timeout=15,
        )
        events = [json.loads(line) for line in self.events.read_text().splitlines()]
        return result, events

    def test_fmt_failure_stops_before_clippy_and_prewarm(self):
        result, events = self.run_gate(FAIL_FMT="1")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        phases = [event["args"][0] for event in events]
        self.assertIn("fmt", phases)
        self.assertNotIn("clippy", phases)
        self.assertNotIn("test", phases)

    def test_script_suite_failure_stops_before_clippy(self):
        result, events = self.run_gate(FAIL_SUITES="1")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        phases = [event["args"][0] for event in events]
        self.assertIn("fmt", phases)
        self.assertNotIn("clippy", phases)
        self.assertNotIn("test", phases)
        self.assertIn("operator script suites failed", result.stderr)

    def test_clippy_failure_stops_before_prewarm(self):
        result, events = self.run_gate(FAIL_CLIPPY="1")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        phases = [event["args"][0] for event in events]
        self.assertIn("fmt", phases)
        self.assertIn("clippy", phases)
        self.assertNotIn("test", phases)

    def test_clippy_runs_once_before_prewarm_and_preserves_log(self):
        result, events = self.run_gate()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        phases = [event["args"][0] for event in events]
        self.assertEqual(phases.count("clippy"), 1)
        self.assertLess(phases.index("fmt"), phases.index("clippy"))
        self.assertLess(phases.index("clippy"), phases.index("test"))
        for event in events:
            self.assertEqual(event["wrapper"], "fixture-machine-wrapper")
        log = self.log.read_text()
        for marker in ("fixture script suites", "fixture clippy output", "fixture prewarm build", "prewarm done: warmed 1 executables", "fixture stops at unit tests"):
            self.assertIn(marker, log)


class RustGateFailures(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="broca-rust-gate-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        scripts = self.root / "scripts"
        scripts.mkdir()
        library = scripts / "lib"
        library.mkdir()
        shutil.copy2(ROOT / "scripts/lib/cargo-watchdog.py", library / "cargo-watchdog.py")
        shutil.copy2(ROOT / "scripts/rust-test-gate.sh", scripts)
        self.bins = self.root / "bin"
        self.bins.mkdir()
        self.capture = self.root / "captures"
        self.capture.mkdir()
        self.env = {**os.environ, "PATH": f"{self.bins}:{os.environ['PATH']}",
                    "BROCA_ALLOW_LINUX_GATE": "1", "TMPDIR": str(self.capture)}
        GatePhases.executable(self.bins / "cargo-nextest", "#!/bin/sh\nexit 0\n")
        GatePhases.executable(self.bins / "sleep", "#!/bin/sh\n/bin/sleep 0.2\n")
        GatePhases.executable(self.bins / "cargo", r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
if args[:2] == ["nextest", "list"]:
    print("invalid json" if os.environ.get("BAD_NEXTEST") else json.dumps({"test-count": 1}))
elif args[:2] == ["nextest", "run"]:
    print("PASS [0.1s] broca-module::it test_clock::test_clock_feature_is_enabled_for_retention_tests")
    print("SKIP fixture_arm: sibling absent")
    print("Summary [0.1s] 1 test run")
    sys.exit(int(os.environ.get("FAIL_NEXTEST", "0")))
elif "--list" in args and "--doc" in args:
    print("unrecognised doctest inventory" if os.environ.get("BAD_DOC") else "0 tests, 0 benchmarks")
elif "--list" in args:
    for name in ("fixture", "live", "live_e2e"):
        print(f"Running tests/{name}.rs (target/debug/{name})")
        print("1 test, 0 benchmarks")
''')

    def gate(self, **env):
        return subprocess.run(["bash", "scripts/rust-test-gate.sh"], cwd=self.root,
                              env={**self.env, **env}, text=True, capture_output=True, timeout=10)

    def test_failed_nextest_reports_skips_and_cleans_capture(self):
        result = self.gate(FAIL_NEXTEST="17")
        self.assertEqual(result.returncode, 17, result.stdout + result.stderr)
        self.assertIn("1 test(s) declined to run", result.stdout)
        self.assertEqual(list(self.capture.iterdir()), [])

    def test_nextest_parser_failure_names_count_and_stops_gate(self):
        result = self.gate(BAD_NEXTEST="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("could not count nextest integration tests", result.stderr)
        self.assertNotIn("integration test count mismatch", result.stderr)
        self.assertNotIn("0 doctests", result.stdout)
        self.assertEqual(list(self.capture.iterdir()), [])

    def test_unrecognised_doctest_inventory_fails_by_name(self):
        result = self.gate(BAD_DOC="1")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("could not count doctests", result.stderr)
        self.assertNotIn("skipped (0 doctests)", result.stdout)
        self.assertEqual(list(self.capture.iterdir()), [])

    def test_recognised_zero_doctests_still_skips(self):
        result = self.gate()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("skipped (0 doctests)", result.stdout)

    def test_deadline_kills_descendant_not_only_direct_child(self):
        pid_file = self.root / "descendant"
        child = self.root / "child.py"
        child.write_text('''import pathlib, subprocess, sys, time
p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
pathlib.Path(sys.argv[1]).write_text(str(p.pid))
time.sleep(30)
''')
        result = subprocess.run(
            [sys.executable, str(self.root / "scripts/lib/cargo-watchdog.py"),
             "--step", "deadline fixture", "--deadline-seconds", "0.3", "--",
             sys.executable, str(child), str(pid_file)],
            env={**self.env, "BROCA_CARGO_IDLE_TIMEOUT_SECONDS": "5"},
            capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(result.returncode, 124, result.stdout + result.stderr)
        self.assertIn("deadline fixture", result.stderr)
        pid = int(pid_file.read_text())
        try:
            time.sleep(0.1)
            state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)],
                                   capture_output=True, text=True).stdout.strip()
            self.assertTrue(not state or state.startswith("Z"), f"descendant {pid} still running: {state}")
        finally:
            try:
                os.kill(pid, 9)
            except ProcessLookupError:
                pass


if __name__ == "__main__":
    unittest.main(verbosity=2)
