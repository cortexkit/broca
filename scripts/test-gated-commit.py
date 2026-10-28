#!/usr/bin/env python3
"""Drive gated-commit's path derivation with NUL-delimited git status fixtures."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
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
        (self.root / ".git").mkdir()
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
elif args[:2] == ["rev-parse", "--git-path"]:
    print(os.environ["ATTESTATION_PATH"])
elif args[0] == "write-tree": print(os.environ.get("TREE_HASH", "tree"))
elif args[:2] == ["ls-files", "--others"]: print(os.environ.get("UNTRACKED", ""), end="")
elif args[0] == "diff": raise SystemExit(int(os.environ.get("UNSTAGED", "0")))
elif args[0] == "log": print("fixture commit")
''')
        self.executable(self.root / "scripts/quickcheck.sh", r'''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["CRATE_ARGS"], "w") as f: json.dump(sys.argv[1:], f)
sys.exit(17)
''')
        self.crates = self.root / "crates.json"
        self.events = self.root / "git-events"
        self.cargo_events = self.root / "cargo-events"
        self.layout_events = self.root / "layout-events"
        self.attestation = self.root / ".git" / "broca-linux-quickcheck-attestation"
        self.executable(self.bins / "cargo", r'''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["CARGO_EVENTS"], "a") as f: f.write(json.dumps(sys.argv[1:]) + "\n")
print("fixture cargo fmt")
''')
        self.executable(self.root / "scripts/check-cortexkit-layout.sh", r'''#!/bin/sh
printf 'layout\n' >> "$LAYOUT_EVENTS"
''')

    @staticmethod
    def executable(path, text):
        path.write_text(text)
        path.chmod(0o755)

    def commit(self, entries):
        return subprocess.run(["bash", "scripts/gated-commit.sh", "-m", "fixture"], cwd=self.root,
                               env={**os.environ, "PATH": f"{self.bins}:{os.environ['PATH']}",
                                    "STATUS_ENTRIES": json.dumps(entries), "CRATE_ARGS": str(self.crates),
                                    "GIT_EVENTS": str(self.events), "CARGO_EVENTS": str(self.cargo_events),
                                    "LAYOUT_EVENTS": str(self.layout_events),
                                    "ATTESTATION_PATH": str(self.attestation)},
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

    def attest(self, tree="tree", crates="alpha"):
        self.attestation.write_text(
            "format=broca-linux-quickcheck-v1\n"
            "platform=linux\n"
            f"tree={tree}\n"
            f"crates={crates}\n"
            "status=pass\n"
        )

    def test_matching_linux_attestation_skips_quickcheck_but_keeps_local_format_and_layout(self):
        self.attest()
        result = self.commit([" M crates/alpha/src/lib.rs"])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(self.crates.exists(), "quickcheck must not run for a matching attestation")
        cargo = [json.loads(line) for line in self.cargo_events.read_text().splitlines()]
        self.assertEqual(cargo, [["fmt", "--all", "--", "--check"]])
        self.assertEqual(self.layout_events.read_text(), "layout\n")
        self.assertIn("reusing passing Linux quickcheck", result.stdout)

    def test_tree_or_crate_mismatch_keeps_the_local_quickcheck(self):
        for tree, crates in (("different-tree", "alpha"), ("tree", "beta")):
            with self.subTest(tree=tree, crates=crates):
                self.events.unlink(missing_ok=True)
                self.crates.unlink(missing_ok=True)
                self.cargo_events.unlink(missing_ok=True)
                self.layout_events.unlink(missing_ok=True)
                self.attest(tree, crates)
                result = self.commit([" M crates/alpha/src/lib.rs"])
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertEqual(json.loads(self.crates.read_text()), ["alpha"])
                self.assertFalse(self.cargo_events.exists())
                self.assertFalse(self.layout_events.exists())
                self.assertNotIn("reusing passing Linux quickcheck", result.stdout)

    def test_missing_linux_attestation_keeps_the_local_quickcheck(self):
        result = self.commit([" M crates/alpha/src/lib.rs"])
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(json.loads(self.crates.read_text()), ["alpha"])
        self.assertFalse(self.cargo_events.exists())
        self.assertFalse(self.layout_events.exists())


class LinuxQuickcheckAttestation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="linux-quickcheck-attestation-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        scripts = self.root / "scripts"
        scripts.mkdir()
        shutil.copy2(ROOT / "scripts/linux-quickcheck-attest.sh", scripts)
        self.events = self.root / ".git" / "quickcheck-args"
        library = scripts / "lib"
        library.mkdir()
        quickcheck = library / "linux-quickcheck.py"
        quickcheck.write_text(
            "import os, pathlib, sys\n"
            "pathlib.Path(os.environ['QUICKCHECK_ARGS']).write_text('\\n'.join(sys.argv[1:]) + '\\n')\n"
            "print('quickcheck: ran 1 test')\n"
            "sys.exit(17 if os.environ.get('QUICKCHECK_FAIL') == '1' else 0)\n"
        )
        quickcheck.chmod(0o755)
        bins = self.root / "bin"
        bins.mkdir()
        uname = bins / "uname"
        uname.write_text("#!/bin/sh\nprintf 'Linux\\n'\n")
        uname.chmod(0o755)
        self.output = self.root / "attestation"
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        exclude = self.root / ".git" / "info" / "exclude"
        exclude.write_text("/scripts/\n/bin/\n")
        subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=self.root, check=True)
        (self.root / "tracked.txt").write_text("baseline\n")
        subprocess.run(["git", "add", "tracked.txt"], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-qm", "baseline"], cwd=self.root, check=True)
        (self.root / "tracked.txt").write_text("changed\n")
        subprocess.run(["git", "add", "tracked.txt"], cwd=self.root, check=True)

    def run_wrapper(self, **env):
        return subprocess.run(
            ["bash", "scripts/linux-quickcheck-attest.sh", "--output", str(self.output), "zeta", "alpha", "alpha"],
            cwd=self.root,
            env={**os.environ, "PATH": f"{self.root / 'bin'}:{os.environ['PATH']}",
                 "QUICKCHECK_ARGS": str(self.events), **env},
            text=True, capture_output=True, timeout=10,
        )

    def test_wrapper_attests_tree_and_sorted_crate_set_only_after_quickcheck_passes(self):
        result = self.run_wrapper()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        fields = dict(line.split("=", 1) for line in self.output.read_text().splitlines())
        self.assertEqual(fields, {
            "format": "broca-linux-quickcheck-v1", "platform": "linux",
            "tree": subprocess.check_output(["git", "write-tree"], cwd=self.root, text=True).strip(),
            "crates": "alpha,zeta", "status": "pass",
        })
        self.assertEqual(self.events.read_text().splitlines(), ["alpha", "zeta"])

    def test_wrapper_does_not_attest_a_failed_quickcheck(self):
        result = self.run_wrapper(QUICKCHECK_FAIL="1")
        self.assertEqual(result.returncode, 17, result.stdout + result.stderr)
        self.assertFalse(self.output.exists())


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
        shutil.copy2(ROOT / "scripts/quickcheck-guards.list", self.root / "scripts")
        shutil.copy2(ROOT / "scripts/quickcheck-feature-tests.list", self.root / "scripts")
        for name in ("test-portable-test-selection.py", "check-portable-test-selection.py"):
            shutil.copy2(ROOT / "scripts" / name, self.root / "scripts")
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
        self.env = dict(os.environ, HOME=str(self.root), EVENTS=str(self.events), BROCA_SUITES_PYTHON=sys.executable)

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
        self.assertEqual(tests[:2], [["test", "--locked", "-p", "changed", "--lib", "--bins", "--quiet"],
                                     ["test", "--locked", "-p", "changed", "--doc", "--quiet"]])
        self.assertIn(["test", "--locked", "-p", "broca-module", "--test", "it", "open_failed_classes::", "-F", "broca-module/test-clock", "--quiet"], tests)

    def test_binary_only_package_keeps_binary_tests_without_invalid_library_selectors(self):
        result, commands = self.check(KIND="bin")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        tests = [c for c in commands if c[0] == "test"]
        self.assertEqual(tests[0], ["test", "--locked", "-p", "changed", "--bins", "--quiet"])
        self.assertIn(["test", "--locked", "-p", "broca-module", "--test", "it", "open_failed_classes::", "-F", "broca-module/test-clock", "--quiet"], tests)

    def test_any_failed_crate_check_stops_quickcheck(self):
        for phase in ("fmt", "clippy", "test", "doc"):
            with self.subTest(phase=phase):
                result, _ = self.check(FAIL_PHASE=phase)
                self.assertEqual(result.returncode, 17, result.stdout + result.stderr)
                self.assertNotIn("quickcheck OK", result.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
