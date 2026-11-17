#!/usr/bin/env python3
"""scripts/check-file-sizes.sh: hand-written Rust files stay under 5,000 lines, a listed file may
not grow past its listed size, test files are exempt, and a missing allowlist refuses."""

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "check-file-sizes.sh"


class CheckFileSizes(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="check-file-sizes."))
        (self.root / "scripts").mkdir()
        shutil.copy(SCRIPT, self.root / "scripts" / "check-file-sizes.sh")
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        self.allow("")

    def tearDown(self):
        shutil.rmtree(self.root)

    def allow(self, text):
        (self.root / "scripts" / "file-size-allowlist.txt").write_text(text)

    def source(self, path, lines):
        file = self.root / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text("//\n" * lines)
        subprocess.run(["git", "-C", str(self.root), "add", path], check=True)

    def check(self):
        return subprocess.run(
            ["bash", str(self.root / "scripts" / "check-file-sizes.sh")],
            capture_output=True,
            text=True,
        )

    def test_a_file_at_the_limit_passes_and_one_over_it_fails(self):
        self.source("crates/a/src/lib.rs", 5000)
        self.assertEqual(self.check().returncode, 0)
        self.source("crates/a/src/lib.rs", 5001)
        result = self.check()
        self.assertEqual(result.returncode, 1)
        self.assertIn("crates/a/src/lib.rs has 5001 lines", result.stderr)

    def test_a_listed_file_may_not_grow_past_its_size(self):
        self.source("crates/a/src/big.rs", 6000)
        self.allow("6000 crates/a/src/big.rs the reason\n")
        self.assertEqual(self.check().returncode, 0)
        self.source("crates/a/src/big.rs", 6001)
        result = self.check()
        self.assertEqual(result.returncode, 1)
        self.assertIn("may not grow past 6000", result.stderr)

    def test_a_listed_file_that_shrank_passes_and_says_so(self):
        self.source("crates/a/src/big.rs", 5500)
        self.allow("6000 crates/a/src/big.rs the reason\n")
        result = self.check()
        self.assertEqual(result.returncode, 0)
        self.assertIn("shrank to 5500", result.stdout)

    def test_test_files_are_exempt(self):
        for path in [
            "crates/a/tests/it.rs",
            "crates/a/src/tests.rs",
            "crates/a/src/run_tests.rs",
            "crates/a/src/test_rig.rs",
        ]:
            self.source(path, 9000)
        self.assertEqual(self.check().returncode, 0)

    def test_a_missing_or_malformed_allowlist_refuses(self):
        self.source("crates/a/src/lib.rs", 10)
        self.allow("not a number crates/a/src/lib.rs\n")
        self.assertEqual(self.check().returncode, 2)
        (self.root / "scripts" / "file-size-allowlist.txt").unlink()
        self.assertEqual(self.check().returncode, 2)


if __name__ == "__main__":
    unittest.main()
