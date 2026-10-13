#!/usr/bin/env python3
"""Hermetic coverage for the exact-name staging pruner."""
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("prune-staging.sh")


class PruneStagingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.seam = self.root / "staging"
        self.seam.mkdir()
        self.placed = self.root / "placed"
        self.placed.write_bytes(b"placed")
        self.names = []
        for i in range(1, 9):
            name = f"ck-broca.{i:08x}"
            data = f"release{i}".encode()
            (self.seam / name).write_bytes(data)
            digest = hashlib.sha256(data).hexdigest()
            (self.seam / (name + ".sha256.postsign")).write_text(f"{digest}  {name}\n")
            self.names.append(name)
            # Deterministic staging order rather than test execution timing.
            os.utime(self.seam / name, ns=(i * 1_000_000_000, i * 1_000_000_000))
        for i in range(1, 5):
            name = f"ck-broca.rollback-{20260101 + i}T010203Z"
            (self.seam / name).write_bytes(f"rollback{i}".encode())
            (self.seam / (name + ".sha256")).write_text("checksum\n")
        current = self.names[2]
        data = (self.seam / current).read_bytes()
        (self.seam / "ck-broca.current").write_text(
            f"{hashlib.sha256(data).hexdigest()}  {current}  20260101T000000Z\n")
        self.placed.write_bytes((self.seam / self.names[0]).read_bytes())
        self.foreign = [
            "ck-aft.06aaa7605dc996fd",
            "ck-6cf83890",
            "SUPERSEDED-20260918T210800Z.ck-broca.7d9411d3",
            "ck-subc.abcdef12",
            "fusiform-old",
            "ck-broca-notes",
        ]
        self.foreign_bytes = {}
        for name in self.foreign:
            data = name.encode()
            (self.seam / name).write_bytes(data)
            self.foreign_bytes[name] = data
            if name.startswith(("ck-aft.", "ck-6cf", "SUPERSEDED-")):
                sidecar_name = name + ".sha256.postsign"
                sidecar_bytes = ("foreign sidecar for " + name).encode()
                (self.seam / sidecar_name).write_bytes(sidecar_bytes)
                self.foreign_bytes[sidecar_name] = sidecar_bytes
                os.utime(self.seam / name, ns=(0, 0))
        (self.seam / "foreign-link").symlink_to(self.seam / self.foreign[0])

    def run_pruner(self, *args):
        return subprocess.run(["bash", str(SCRIPT), "--staging-dir", str(self.seam),
                               "--placed", str(self.placed), *args], capture_output=True, text=True)

    def test_prunes_only_exact_pairs_and_keeps_current_placed_and_newest_five(self):
        result = self.run_pruner()
        self.assertEqual(result.returncode, 0, result.stderr)
        kept = {self.names[0], self.names[2], *self.names[3:]}
        expected_removed = set(self.names) - kept
        for name, expected_bytes in self.foreign_bytes.items():
            self.assertTrue((self.seam / name).is_file(), f"foreign file was removed: {name}")
            self.assertEqual((self.seam / name).read_bytes(), expected_bytes, name)
        for name in self.names:
            self.assertEqual((self.seam / name).exists(), name in kept)
            self.assertEqual((self.seam / (name + ".sha256.postsign")).exists(), name in kept)
        for p in self.seam.glob("ck-broca.rollback-*"):
            self.assertTrue(p.is_file())
        self.assertTrue((self.seam / "foreign-link").is_symlink())

        self.assertEqual(len(list(self.seam.glob("ck-broca.rollback-20*T*.sha256"))), 2)
        self.assertEqual(len([line for line in result.stdout.splitlines() if line.startswith("deleted ")]),
                         2 * len(expected_removed) + 4)

    def test_dry_run_lists_without_deleting(self):
        before = {p.name: p.read_bytes() for p in self.seam.iterdir() if p.is_file()}
        result = self.run_pruner("--dry-run")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("would delete", result.stdout)
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.seam.iterdir() if p.is_file()})

    def test_missing_current_exits_two_without_deleting(self):
        (self.seam / "ck-broca.current").unlink()
        before = {p.name: p.read_bytes() for p in self.seam.iterdir() if p.is_file()}
        result = self.run_pruner()
        self.assertEqual(result.returncode, 2)
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.seam.iterdir() if p.is_file()})


if __name__ == "__main__":
    unittest.main(verbosity=2)
