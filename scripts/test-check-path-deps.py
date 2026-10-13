#!/usr/bin/env python3
"""Exercise the path-source gate with real Cargo metadata and disposable manifests."""

import contextlib
import io
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


CHECK = Path(__file__).with_name("check-path-deps.py").resolve()


class PathDependencyGate(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="broca-path-gate-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.repo = self.base / "repo"
        self.repo.mkdir()
        self.environment = dict(os.environ, CARGO_NET_OFFLINE="true")
        self.manifest = self.repo / "Cargo.toml"
        self.manifest.write_text('[package]\nname = "gate-fixture"\nversion = "0.1.0"\nedition = "2021"\n')
        (self.repo / "src").mkdir()
        (self.repo / "src/lib.rs").write_text("")

    def crate(self, path, name="planted-external", version="0.1.0"):
        (path / "src").mkdir(parents=True)
        (path / "src/lib.rs").write_text("")
        (path / "Cargo.toml").write_text(
            f'[package]\nname = "{name}"\nversion = "{version}"\nedition = "2021"\n'
        )

    def run_check(self):
        lock = subprocess.run(
            ["cargo", "generate-lockfile", "--manifest-path", str(self.manifest)],
            env=self.environment, capture_output=True, text=True,
        )
        self.assertEqual(lock.returncode, 0, lock.stderr)
        return subprocess.run(
            [sys.executable, str(CHECK), "--repo-root", str(self.repo)],
            env=self.environment, capture_output=True, text=True,
        )

    def test_metadata_stderr_does_not_contaminate_json(self):
        check = runpy.run_path(str(CHECK))["check"]
        result = subprocess.CompletedProcess([], 0,
            json.dumps({"packages": [{"name": "fixture", "source": None,
                "manifest_path": str(self.manifest), "dependencies": []}]}),
            "Blocking waiting for file lock on package cache\n")
        with patch("subprocess.run", return_value=result), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(check(self.manifest, self.repo), 0)

    def test_zero_packages_is_check_failure(self):
        check = runpy.run_path(str(CHECK))["check"]
        result = subprocess.CompletedProcess([], 0, '{"packages":[]}', "")
        error = io.StringIO()
        with patch("subprocess.run", return_value=result), contextlib.redirect_stderr(error):
            self.assertEqual(check(self.manifest, self.repo), 2)
        self.assertIn("check itself failed: no local packages checked", error.getvalue())

    def test_metadata_error_is_not_a_violation(self):
        check = runpy.run_path(str(CHECK))["check"]
        result = subprocess.CompletedProcess([], 1, "", "metadata failed")
        error = io.StringIO()
        with patch("subprocess.run", return_value=result), contextlib.redirect_stderr(error):
            self.assertEqual(check(self.manifest, self.repo), 2)
        self.assertIn("check itself failed", error.getvalue())

    def test_internal_path_is_allowed(self):
        self.crate(self.repo / "internal")
        with self.manifest.open("a") as file:
            file.write('[dependencies]\nplanted-external = { path = "internal" }\n')
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("2 packages checked", result.stdout)

    def test_external_path_is_refused(self):
        self.crate(self.base / "external")
        # Save a pristine copy before adding an external path dependency to this
        # disposable manifest; no real repository manifest is modified.
        original = self.repo / "original.toml"
        shutil.copyfile(self.manifest, original)
        with self.manifest.open("a") as file:
            file.write('[dependencies]\nplanted-external = { path = "../external" }\n')
        result = self.run_check()
        self.assertEqual(result.returncode, 3, f"the check itself failed: {result.stderr}")
        self.assertIn("external path dependency: planted-external:", result.stderr)
        self.assertNotIn("planted-external", original.read_text())

    def test_external_patch_is_refused(self):
        self.crate(self.base / "external", "serde", "1.0.228")
        with self.manifest.open("a") as file:
            file.write('[dependencies]\nserde = "=1.0.228"\n'
                       '[patch.crates-io]\nserde = { path = "../external" }\n')
        result = self.run_check()
        self.assertEqual(result.returncode, 3, f"the check itself failed: {result.stderr}")
        self.assertIn("external path dependency: serde:", result.stderr)

    def test_unused_external_patch_is_refused(self):
        self.crate(self.base / "external")
        with self.manifest.open("a") as file:
            file.write('[patch.crates-io]\nplanted-external = { path = "../external" }\n')
        result = self.run_check()
        self.assertEqual(result.returncode, 3, f"the check itself failed: {result.stderr}")
        self.assertIn("external path dependency: planted-external:", result.stderr)

    def test_symlink_escape_is_refused(self):
        self.crate(self.base / "external")
        try:
            (self.repo / "internal").symlink_to(self.base / "external", target_is_directory=True)
        except OSError as error:
            self.skipTest(f"symlinks unavailable: {error}")
        with self.manifest.open("a") as file:
            file.write('[dependencies]\nplanted-external = { path = "internal" }\n')
        result = self.run_check()
        self.assertEqual(result.returncode, 3, f"the check itself failed: {result.stderr}")
        self.assertIn("external path dependency: planted-external:", result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
