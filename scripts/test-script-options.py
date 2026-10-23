#!/usr/bin/env python3
"""Missing option values must stop parsing with a named usage error."""

import os
from pathlib import Path
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1]


class ScriptOptionValues(unittest.TestCase):
    OPTIONS = {
        "place-release.sh": (2, ("--staged", "--session-staged", "--run-staged", "--dest", "--report")),
        "deploy.sh": (2, ("--marker", "--control")),
        "mint-lineage-specimen.sh": (2, ("--count", "--victim", "--donor", "--project-root", "--harness")),
        "verify-deploy.sh": (64, ("--marker", "--control", "--bin", "--sha", "--staged")),
    }

    def run_script(self, script, args):
        return subprocess.run(["bash", str(ROOT / "scripts" / script), *args], cwd=ROOT,
                              env=os.environ, capture_output=True, text=True, timeout=1)

    def missing_values(self, script):
        code, options = self.OPTIONS[script]
        for option in options:
            for suffix in ([], [""]):
                with self.subTest(option=option, suffix=suffix):
                    result = self.run_script(script, [option, *suffix])
                    self.assertEqual(result.returncode, code, result.stdout + result.stderr)
                    self.assertIn(f"{script.removesuffix('.sh')}: usage error: {option} requires an argument", result.stderr)

    def test_place_release_missing_values_are_named(self):
        self.missing_values("place-release.sh")

    def test_deploy_missing_values_are_named(self):
        self.missing_values("deploy.sh")

    def test_mint_missing_values_are_named(self):
        self.missing_values("mint-lineage-specimen.sh")

    def test_verify_deploy_missing_values_are_named(self):
        self.missing_values("verify-deploy.sh")

    def test_values_are_consumed_before_next_unknown_option(self):
        for script, (code, options) in self.OPTIONS.items():
            for option in options:
                with self.subTest(script=script, option=option):
                    result = self.run_script(script, [option, "fixture", "--unknown-fixture"])
                    self.assertEqual(result.returncode, code, result.stdout + result.stderr)
                    self.assertIn("unknown argument: --unknown-fixture", result.stderr)
                    self.assertNotIn("requires an argument", result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
