#!/usr/bin/env python3
"""Check that Cargo watchdog timeouts are named and ordinary failures pass through."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]
WATCHDOG = ROOT / "scripts/lib/cargo-watchdog.py"


class CargoWatchdogTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="broca-cargo-watchdog-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.bins = self.root / "bin"
        self.bins.mkdir()
        self.child_marker = self.root / "child-survived"
        cargo = self.bins / "cargo"
        cargo.write_text(
            "#!/usr/bin/env python3\n"
            "import os, pathlib, subprocess, sys, time\n"
            "mode = os.environ['CARGO_STUB_MODE']\n"
            "if mode == 'lock':\n"
            "    print('Blocking waiting for file lock on package cache', flush=True)\n"
            "    child = (\"import pathlib,time; time.sleep(0.6); pathlib.Path(%r).touch()\" % os.environ['CHILD_MARKER'])\n"
            "    subprocess.Popen([sys.executable, '-c', child])\n"
            "    time.sleep(10)\n"
            "elif mode == 'silent':\n"
            "    time.sleep(10)\n"
            "else:\n"
            "    print('fixture cargo failure', flush=True)\n"
            "    print('fixture cargo diagnostic', file=sys.stderr, flush=True)\n"
            "    raise SystemExit(101)\n"
        )
        cargo.chmod(0o755)
        self.env = {
            **os.environ,
            "PATH": f"{self.bins}:{os.environ['PATH']}",
            "CHILD_MARKER": str(self.child_marker),
        }

    def run_cargo(self, mode, **limits):
        env = {**self.env, "CARGO_STUB_MODE": mode, **limits}
        return subprocess.run(
            [sys.executable, str(WATCHDOG), "--step", "inventory list", "--", "cargo", "test"],
            env=env,
            capture_output=True,
            text=True,
            timeout=5,
        )

    def test_lock_timeout_names_lock_and_step_and_kills_process_group(self):
        result = self.run_cargo(
            "lock",
            BROCA_CARGO_LOCK_TIMEOUT_SECONDS="0.15",
            BROCA_CARGO_IDLE_TIMEOUT_SECONDS="2",
        )
        self.assertEqual(result.returncode, 125, result.stdout + result.stderr)
        self.assertIn("file lock on package cache", result.stderr)
        self.assertIn("inventory list", result.stderr)
        time.sleep(0.7)
        self.assertFalse(self.child_marker.exists(), "Cargo's child process survived the timeout")

    def test_idle_timeout_names_step(self):
        result = self.run_cargo(
            "silent",
            BROCA_CARGO_LOCK_TIMEOUT_SECONDS="2",
            BROCA_CARGO_IDLE_TIMEOUT_SECONDS="0.15",
        )
        self.assertEqual(result.returncode, 124, result.stdout + result.stderr)
        self.assertIn("inventory list", result.stderr)
        self.assertIn("no output progress", result.stderr)

    def test_normal_cargo_failure_status_and_output_pass_through(self):
        result = self.run_cargo("failure")
        self.assertEqual(result.returncode, 101, result.stdout + result.stderr)
        self.assertEqual(result.stdout, "fixture cargo failure\n")
        self.assertEqual(result.stderr, "fixture cargo diagnostic\n")


if __name__ == "__main__":
    unittest.main(verbosity=2)
