#!/usr/bin/env python3
"""Exercise the real gate scripts with cheap Cargo/rig stand-ins, never host builds."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
