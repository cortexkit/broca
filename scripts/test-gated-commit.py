#!/usr/bin/env python3
"""Drive gated-commit's path derivation with NUL-delimited git status fixtures."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class GatedCommitPaths(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="gated-commit-paths-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / "scripts").mkdir()
        shutil.copy2(ROOT / "scripts/gated-commit.sh", self.root / "scripts")
        self.bins = self.root / "bin"
        self.bins.mkdir()
        self.executable(self.bins / "git", r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
with open(os.environ["GIT_EVENTS"], "a") as f: f.write(json.dumps(args) + "\n")
if args[0] == "status":
    entries = json.loads(os.environ["STATUS_ENTRIES"])
    if "-z" in args:
        sys.stdout.buffer.write(b"\0".join(x.encode() for x in entries) + b"\0")
    else:
        while entries:
            entry = entries.pop(0)
            if "R" in entry[:2] or "C" in entry[:2]:
                source = entries.pop(0)
                print(entry[:3] + json.dumps(source) + " -> " + json.dumps(entry[3:]))
            else:
                print(entry[:3] + json.dumps(entry[3:]))
elif args[0] == "log": print("fixture commit")
''')
        self.executable(self.root / "scripts/quickcheck.sh", r'''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["CRATE_ARGS"], "w") as f: json.dump(sys.argv[1:], f)
sys.exit(17)
''')
        self.crates = self.root / "crates.json"
        self.events = self.root / "git-events"

    @staticmethod
    def executable(path, text):
        path.write_text(text)
        path.chmod(0o755)

    def commit(self, entries):
        return subprocess.run(["bash", "scripts/gated-commit.sh", "-m", "fixture"], cwd=self.root,
                              env={**os.environ, "PATH": f"{self.bins}:{os.environ['PATH']}",
                                   "STATUS_ENTRIES": json.dumps(entries), "CRATE_ARGS": str(self.crates),
                                   "GIT_EVENTS": str(self.events)},
                              text=True, capture_output=True, timeout=5)

    def assert_gated(self, entries, crates):
        result = self.commit(entries)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(json.loads(self.crates.read_text()), crates)
        events = [json.loads(line)[0] for line in self.events.read_text().splitlines()]
        self.assertNotIn("commit", events)
        self.assertNotIn("add", events)

    def test_space_in_crate_path_does_not_skip_gate(self):
        self.assert_gated([" M crates/alpha/source with spaces.rs"], ["alpha"])

    def test_rename_checks_both_source_and_destination(self):
        self.assert_gated(["R  crates/alpha/new name.rs", "crates/beta/old name.rs"], ["alpha", "beta"])

    def test_rename_out_of_root_manifest_still_gates_reader(self):
        self.assert_gated(["R  scripts/old-manifest", "Cargo.toml"], ["broca-subc"])

    def test_script_only_change_still_commits_without_crate_gate(self):
        result = self.commit([" M scripts/space in script.sh"])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(self.crates.exists())
        self.assertIn("committed", result.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
