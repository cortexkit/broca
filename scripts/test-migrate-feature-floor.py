#!/usr/bin/env python3
"""Checks for scripts/migrate-feature-floor.py, the one-time move of a state root's floor."""

import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).resolve().parent / "migrate-feature-floor.py"


class MigrateFeatureFloor(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name) / "broca"
        self.root.mkdir()
        self.floor = self.root / "wal-feature-floor"
        self.new_floor = self.root / "feature-floor"
        self.marker = self.root / "late-results-registered"

    def tearDown(self):
        self.tmp.cleanup()

    def run_script(self, *args):
        env = dict(os.environ, BROCA_STATE_ROOT=str(self.root))
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args], env=env, capture_output=True, text=True
        )

    def backups(self):
        folder = self.root.parent / "broca-migrations"
        return sorted(folder.iterdir()) if folder.exists() else []

    def test_an_old_floor_keeps_only_the_session_store_and_marks_held_intake(self):
        old = "provider-held/v1\nscope/v1\nsession-storage/v1\n"
        self.floor.write_text(old)
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.new_floor.read_text(), "session-storage/v1\n")
        self.assertFalse(self.floor.exists())
        self.assertFalse((self.root / "wal-feature-floor.lock").exists())
        # The old log once held a provider-held call, so the marker now carries that evidence.
        self.assertTrue(self.marker.exists())
        [backup] = self.backups()
        self.assertEqual(backup.read_text(), old)
        self.assertEqual(backup.stat().st_mode & 0o777, 0o600)

    def test_an_existing_marker_is_left_alone(self):
        self.floor.write_text("scope/v1\nsession-storage/v1\n")
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.new_floor.read_text(), "session-storage/v1\n")
        self.assertFalse(self.marker.exists(), "no provider-held evidence, so no marker")

    def test_an_unknown_name_refuses_and_changes_nothing(self):
        text = "scope/v1\nsession-storage/v2\n"
        self.floor.write_text(text)
        result = self.run_script()
        self.assertEqual(result.returncode, 2)
        self.assertIn("session-storage/v2", result.stdout)
        self.assertEqual(self.floor.read_text(), text)
        self.assertFalse(self.new_floor.exists())
        self.assertEqual(self.backups(), [])

    def test_a_dry_run_changes_nothing(self):
        text = "provider-held/v1\nsession-storage/v1\n"
        self.floor.write_text(text)
        result = self.run_script("--dry-run")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.floor.read_text(), text)
        self.assertFalse(self.marker.exists())
        self.assertEqual(self.backups(), [])

    def test_no_floor_is_nothing_to_do(self):
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(self.floor.exists())
        self.assertFalse(self.new_floor.exists())

    def test_an_old_floor_beside_a_new_one_is_removed_only_when_covered(self):
        self.floor.write_text("session-storage/v1\n")
        self.new_floor.write_text("session-storage/v1\n")
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(self.floor.exists())
        self.assertEqual(self.new_floor.read_text(), "session-storage/v1\n")
        self.floor.write_text("session-storage/v1\nsession-storage/v2\n")
        result = self.run_script()
        self.assertEqual(result.returncode, 2)
        self.assertIn("session-storage/v2", result.stdout)
        self.assertTrue(self.floor.exists())

    def test_old_folders_go_only_when_empty(self):
        for name in ("wal", "wal-durable", "media", "conversion-staging"):
            (self.root / name).mkdir()
        # The old media store's shape: empty subfolders and an empty lock file are not data,
        # while a lock file with content, or any other file, keeps the folder.
        (self.root / "media-ephemeral" / "objects").mkdir(parents=True)
        (self.root / "media-ephemeral" / "refs").mkdir()
        (self.root / "media-ephemeral" / "store.lock").write_bytes(b"")
        (self.root / "media-durable" / "refs").mkdir(parents=True)
        (self.root / "media-durable" / "store.lock").write_bytes(b"1")
        (self.root / "conversion-ledger.db").write_bytes(b"")
        (self.root / "media" / "kept.bin").write_bytes(b"x")
        (self.root / "session-store").mkdir()
        (self.root / "caps.db").write_bytes(b"caps")
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stderr)
        for name in (
            "wal", "wal-durable", "conversion-staging", "conversion-ledger.db", "media-ephemeral"
        ):
            self.assertFalse((self.root / name).exists(), name)
        self.assertTrue((self.root / "media" / "kept.bin").exists())
        self.assertTrue((self.root / "media-durable" / "store.lock").exists())
        self.assertIn("not empty", result.stdout)
        self.assertTrue((self.root / "session-store").exists())
        self.assertFalse((self.root / "caps.db").exists())
        [caps] = [b for b in self.backups() if b.name.startswith("caps.db.")]
        self.assertEqual(caps.read_bytes(), b"caps")


if __name__ == "__main__":
    unittest.main()
