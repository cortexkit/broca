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


class QuickcheckSelection(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="quickcheck-selection-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / "scripts").mkdir()
        shutil.copy2(ROOT / "scripts/quickcheck.sh", self.root / "scripts")
        library = self.root / "scripts/lib"
        library.mkdir()
        shutil.copy2(ROOT / "scripts/lib/cargo-watchdog.py", library / "cargo-watchdog.py")
        bins = self.root / ".cargo/bin"
        bins.mkdir(parents=True)
        self.events = self.root / "events"
        cargo = bins / "cargo"
        cargo.write_text(r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
with open(os.environ["EVENTS"], "a") as log: log.write(json.dumps(args) + "\n")
if args[0] == "metadata":
    print(json.dumps({"packages": [{"name": "changed", "targets": [{"kind": [os.environ.get("KIND", "lib")]}]}]}))
if args[0] == os.environ.get("FAIL_PHASE") or ("--doc" in args and os.environ.get("FAIL_PHASE") == "doc"):
    raise SystemExit(17)
''')
        cargo.chmod(0o755)
        self.env = dict(os.environ, HOME=str(self.root), EVENTS=str(self.events))

    def check(self, **env):
        result = subprocess.run(["bash", "scripts/quickcheck.sh", "changed"], cwd=self.root,
                                env=dict(self.env, **env), capture_output=True, text=True, timeout=10)
        commands = [json.loads(line) for line in self.events.read_text().splitlines()]
        return result, commands

    def test_selected_crate_keeps_fmt_all_target_clippy_unit_binary_and_doc_checks(self):
        result, commands = self.check()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(commands[0], ["fmt", "-p", "changed", "--", "--check"])
        self.assertEqual(commands[1], ["clippy", "--locked", "-p", "changed", "--all-targets", "--", "-D", "warnings"])
        tests = [c for c in commands if c[0] == "test"]
        self.assertEqual(tests, [["test", "--locked", "-p", "changed", "--lib", "--bins", "--quiet"],
                                 ["test", "--locked", "-p", "changed", "--doc", "--quiet"]])

    def test_binary_only_package_keeps_binary_tests_without_invalid_library_selectors(self):
        result, commands = self.check(KIND="bin")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual([c for c in commands if c[0] == "test"],
                         [["test", "--locked", "-p", "changed", "--bins", "--quiet"]])

    def test_any_failed_crate_check_stops_quickcheck(self):
        for phase in ("fmt", "clippy", "test", "doc"):
            with self.subTest(phase=phase):
                result, _ = self.check(FAIL_PHASE=phase)
                self.assertEqual(result.returncode, 17, result.stdout + result.stderr)
                self.assertNotIn("quickcheck OK", result.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
