#!/usr/bin/env python3
"""Exercise fixed Cargo selection and package-scoped failures with fake tools."""

import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("linux_quickcheck", ROOT / "scripts/lib/linux-quickcheck.py")
assert SPEC is not None and SPEC.loader is not None
LANE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LANE)


class FixedQuickcheck(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        scripts = self.root / "scripts"
        (scripts / "lib").mkdir(parents=True)
        for name in ("linux-quickcheck.py", "cargo-watchdog.py"):
            shutil.copy2(ROOT / "scripts/lib" / name, scripts / "lib")
        (scripts / "quickcheck-feature-tests.list").write_text("alpha|fixtures|-\n")
        (scripts / "quickcheck-guards.list").write_text("alpha|it|guard::|-\nbeta|it|outside::|-\n")
        bins = self.root / ".cargo/bin"
        bins.mkdir(parents=True)
        self.events = self.root / "events"
        cargo = bins / "cargo"
        cargo.write_text(r'''#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys
args = sys.argv[1:]
root = pathlib.Path.cwd()
with open(os.environ['EVENTS'], 'a') as log:
    log.write(json.dumps({'args': args, 'incremental': os.environ.get('CARGO_INCREMENTAL')}) + '\n')
def source(name):
    return str(root / name / 'src/lib.rs')
if args[0] == 'metadata':
    print(json.dumps({'workspace_members': ['id-alpha', 'id-beta'], 'packages': [
        {'name': name, 'id': 'id-' + name, 'targets': [{'doctest': True, 'src_path': source(name)}]}
        for name in ('alpha', 'beta')]}))
elif args[:2] == ['nextest', 'list']:
    if os.environ.get('FAIL_BUILD'):
        print('beta failed to compile', file=sys.stderr)
        sys.exit(17)
    print(json.dumps({'rust-binaries': {}}))
elif args[0] == 'clippy':
    package = os.environ.get('LINT_PACKAGE')
    if package:
        print(json.dumps({'reason': 'compiler-message', 'package_id': 'id-' + package,
                          'message': {'level': 'warning', 'rendered': package + ' lint'}}))
    if os.environ.get('FAIL_CLIPPY'):
        print('clippy compilation failed', file=sys.stderr)
        sys.exit(18)
elif args[:2] == ['nextest', 'run']:
    assert '--binaries-metadata' in args and '--cargo-metadata' in args
    assert '-p' not in args and '--workspace' not in args
    for flag in ('--binaries-metadata', '--cargo-metadata'):
        json.loads(pathlib.Path(args[args.index(flag) + 1]).read_text())
    if os.environ.get('FAIL_TEST'):
        sys.exit(19)
elif args[0] == 'test':
    for name in ('alpha', 'beta'):
        result = subprocess.run([os.environ['RUSTDOC'], '--test', source(name)], env=os.environ)
        if result.returncode:
            sys.exit(result.returncode)
''')
        cargo.chmod(0o755)
        rustdoc = bins / "rustdoc"
        rustdoc.write_text(r'''#!/usr/bin/env python3
import os, sys
with open(os.environ['DOC_EVENTS'], 'a') as log:
    log.write(' '.join(sys.argv[1:]) + '\n')
if os.environ.get('FAIL_DOC') and any('/' + os.environ['FAIL_DOC'] + '/' in arg for arg in sys.argv):
    sys.exit(20)
''')
        rustdoc.chmod(0o755)
        self.docs = self.root / "docs"
        self.env = dict(os.environ, HOME=str(self.root), EVENTS=str(self.events),
                        DOC_EVENTS=str(self.docs), CARGO_INCREMENTAL="1")

    def run_lane(self, packages=("alpha",), **env):
        self.events.unlink(missing_ok=True)
        self.docs.unlink(missing_ok=True)
        result = subprocess.run([sys.executable, "scripts/lib/linux-quickcheck.py", *packages],
                                cwd=self.root, env=dict(self.env, **env), capture_output=True, text=True, timeout=20)
        events = [json.loads(line) for line in self.events.read_text().splitlines()]
        return result, events

    def test_three_subsets_keep_identical_workspace_build_clippy_and_doc_commands(self):
        configurations = []
        for subset in (("alpha",), ("beta",), ("alpha", "beta")):
            result, events = self.run_lane(subset)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertTrue(all(e["incremental"] == "1" for e in events))
            commands = [e["args"] for e in events]
            fixed = [c for c in commands if c[0] in ("clippy", "test") or c[:2] == ["nextest", "list"]]
            self.assertEqual(len(fixed), 3)
            for command in fixed:
                self.assertIn("--workspace", command)
                self.assertIn("--locked", command)
                self.assertNotIn("-p", command)
                for feature in LANE.FEATURES[1::2]:
                    self.assertIn(feature, command)
            clippy = next(c for c in fixed if c[0] == "clippy")
            self.assertEqual(clippy[-3:], ["--", "--cap-lints", "warn"])
            execution = next(c for c in commands if c[:2] == ["nextest", "run"])
            with patch.object(LANE, "ROOT", self.root):
                self.assertEqual(execution[execution.index("-E") + 1], LANE.test_filter(set(subset)))
            configurations.append(fixed)
        self.assertEqual(configurations[0], configurations[1])
        self.assertEqual(configurations[0], configurations[2])

    def test_only_selected_test_suites_and_guards_are_executed(self):
        expression = LANE.test_filter
        with patch.object(LANE, "ROOT", self.root):
            selected = expression({"alpha"})
        self.assertIn("package(=alpha) & (kind(lib) | kind(bin))", selected)
        self.assertIn("binary(=fixtures)", selected)
        self.assertIn("binary(=it) & test(guard::)", selected)
        self.assertNotIn("beta", selected)

    def test_outside_lint_and_doctest_failures_do_not_fail_attestation(self):
        result, _ = self.run_lane(LINT_PACKAGE="beta", FAIL_DOC="beta")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("/alpha/src/lib.rs", self.docs.read_text())
        self.assertNotIn("/beta/", self.docs.read_text())

    def test_selected_lints_tests_and_doctests_fail(self):
        for environment, expected in (({"LINT_PACKAGE": "alpha"}, 1), ({"FAIL_TEST": "1"}, 19), ({"FAIL_DOC": "alpha"}, 20)):
            with self.subTest(environment=environment):
                result, _ = self.run_lane(**environment)
                self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
                self.assertNotIn("quickcheck OK", result.stdout)

    def test_any_workspace_compilation_failure_stops_before_execution(self):
        for environment, expected in (({"FAIL_BUILD": "1"}, 17), ({"FAIL_CLIPPY": "1"}, 18)):
            with self.subTest(environment=environment):
                result, events = self.run_lane(**environment)
                self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
                self.assertFalse(any(e["args"][:2] == ["nextest", "run"] for e in events))

    def test_unknown_package_fails_instead_of_attesting_an_empty_selection(self):
        result, _ = self.run_lane(("missing",))
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("unknown workspace packages", result.stderr)

    def test_unrecognized_doctest_source_fails_closed(self):
        env = dict(self.env, BROCA_ATTEST_RUSTDOC="rustdoc", BROCA_ATTEST_DOC_SOURCES="{}")
        result = subprocess.run([sys.executable, str(self.root / "scripts/lib/linux-quickcheck.py"),
                                 "--test", "unexpected/lib.rs"], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("cannot identify the doctest source", result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
