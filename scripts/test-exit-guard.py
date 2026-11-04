#!/usr/bin/env python3
"""EXIT cleanup must not turn a Bash 3.2 nounset error into success."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
HELPER = Path(os.environ.get("BROCA_TEST_EXIT_GUARD", ROOT / "scripts/lib/exit-guard.sh"))
BASH = "/bin/bash" if Path("/bin/bash").exists() else (shutil.which("bash") or "bash")


class ExitGuardBehaviorTests(unittest.TestCase):
    def run_script(self, body, cleanup="printf 'cleaned\\n'"):
        scratch_root = ROOT / "target/scratch"
        scratch_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="broca-exit-guard-", dir=scratch_root) as scratch:
            script = Path(scratch) / "guarded.sh"
            script.write_text(
                'set -euo pipefail\nsource "$1"\n'
                f"cleanup() {{ {cleanup}; }}\n"
                "trap 'broca_exit_guard cleanup' EXIT\n" + body + "\n"
            )
            result = subprocess.run(
                [BASH, str(script), str(HELPER)], capture_output=True, text=True
            )
        self.assertIn("cleaned\n", result.stdout)
        return result

    def test_unbound_variable_fails_after_cleanup(self):
        result = self.run_script('printf "%s\\n" "$broca_missing_variable"')
        self.assertNotEqual(result.returncode, 0, result.stderr)
        self.assertIn("broca_missing_variable", result.stderr)

    def test_finished_fallthrough_succeeds(self):
        result = self.run_script("BROCA_EXIT_GUARD_FINISHED=1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")

    def test_finished_explicit_exit_succeeds(self):
        result = self.run_script("BROCA_EXIT_GUARD_FINISHED=1\nexit 0")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_unfinished_success_names_script(self):
        result = self.run_script(":")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(len(result.stderr.splitlines()), 1)
        self.assertIn("guarded.sh: exited before successful completion", result.stderr)

    def test_failure_keeps_status(self):
        for status in (1, 23, 130, 143):
            with self.subTest(status=status):
                result = self.run_script(f"exit {status}")
                self.assertEqual(result.returncode, status, result.stderr)

    def test_cleanup_failure_keeps_failure(self):
        for body, expected in (("BROCA_EXIT_GUARD_FINISHED=1", 19), ("exit 23", 23)):
            with self.subTest(body=body):
                result = self.run_script(body, "printf 'cleaned\\n'; return 19")
                self.assertEqual(result.returncode, expected, result.stderr)


if __name__ == "__main__":
    unittest.main()
