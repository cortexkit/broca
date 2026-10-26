#!/usr/bin/env python3
"""Check quickcheck's workspace guards and feature-gated integration execution."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
GUARDS = ROOT / "scripts" / "quickcheck-guards.list"
FEATURE_TESTS = ROOT / "scripts" / "quickcheck-feature-tests.list"

CARGO = r'''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ["QUICKCHECK_EVENTS"], "a", encoding="utf-8") as events:
    events.write(json.dumps(args) + "\n")
if args and args[0] == "metadata":
    package = os.environ["QUICKCHECK_PACKAGE"]
    targets = [{"name": package.replace("-", "_"), "kind": ["lib"]}]
    if package == "broca-catalog":
            targets += [
                {"name": "code_assist_variant_resume", "kind": ["test"]},
                {"name": "default_max_output", "kind": ["test"]},
                {"name": "reasoning_variants", "kind": ["test"]},
                {"name": "service_tier", "kind": ["test"]},
                {"name": "live", "kind": ["test"]},
            ]
    else:
        targets += [{"name": "fixture", "kind": ["test"]}]
    print(json.dumps({"packages": [{"name": package, "targets": targets}]}))
elif args and args[0] == "test" and "open_failed_classes::" in args and os.environ.get("FAIL_GUARD"):
    print("fixture source-scan guard failed", file=sys.stderr)
    raise SystemExit(47)
else:
    print("fixture cargo pass")
'''


class QuickcheckGuards(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        result = subprocess.run(
            ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
            cwd=ROOT, capture_output=True, text=True, check=True,
        )
        cls.packages = {package["name"]: package for package in json.loads(result.stdout)["packages"]}
        cls.entries = []
        for line_number, line in enumerate(GUARDS.read_text().splitlines(), 1):
            if not line or line.startswith("#"):
                continue
            fields = line.split("|", maxsplit=6)
            if len(fields) != 7:
                raise AssertionError(f"{GUARDS}:{line_number}: expected seven pipe-separated fields")
            package, target, filter_name, features, source_name, symbol, reason = fields
            if not reason.strip():
                raise AssertionError(f"{GUARDS}:{line_number}: every entry needs a one-line reason")
            source = ROOT / source_name
            if not source.is_file():
                raise AssertionError(f"{GUARDS}:{line_number}: missing test source {source_name}")
            text = source.read_text()
            if package == "python":
                if not re.search(rf"\bdef\s+{re.escape(symbol)}\s*\(", text):
                    raise AssertionError(f"{GUARDS}:{line_number}: missing script test {symbol} in {source_name}")
            else:
                cargo_package = cls.packages.get(package)
                if cargo_package is None:
                    raise AssertionError(f"{GUARDS}:{line_number}: missing Cargo package {package}")
                target_exists = any(
                    target_entry["name"] == target and "test" in target_entry["kind"]
                    for target_entry in cargo_package["targets"]
                )
                if not target_exists:
                    raise AssertionError(f"{GUARDS}:{line_number}: missing integration test target {package}::{target}")
                if features != "-":
                    for feature in features.split(","):
                        feature_package, _, feature_name = feature.partition("/")
                        feature_meta = cls.packages.get(feature_package)
                        if feature_meta is None or feature_name not in feature_meta["features"]:
                            raise AssertionError(f"{GUARDS}:{line_number}: missing Cargo feature {feature}")
                function = re.search(rf"\bfn\s+{re.escape(symbol)}\s*\(", text)
                if function is None:
                    raise AssertionError(f"{GUARDS}:{line_number}: missing test {symbol} in {source_name}")
                if filter_name != "-" and source.stem not in filter_name and symbol not in filter_name:
                    raise AssertionError(f"{GUARDS}:{line_number}: filter does not select {symbol}")
                prior = [line.strip() for line in text[:function.start()].splitlines() if line.strip()]
                test_attribute = re.compile(r"#\s*\[\s*(?:tokio::)?test\b[^\]]*\]")
                if not prior or not test_attribute.fullmatch(prior[-1]):
                    raise AssertionError(f"{GUARDS}:{line_number}: {symbol} is no longer a test")
            cls.entries.append((package, target, filter_name, features, source_name, symbol))
        required = {
            ("broca-module", "it", "every_open_failed_site_chooses_a_class"),
            ("broca-module-serve", "it", "wal_known_features_prints_the_sorted_table_and_nothing_else"),
            ("broca-module-serve", "it", "broca_reads_stamp_identity_only_in_scope_adapters"),
            ("broca-module-serve", "launch_nonce", "no_shipped_code_reads_a_launch_nonce_variable_directly"),
            ("broca-module-serve", "it", "every_broca_spawn_site_pins_its_state_root"),
            ("broca-wal", "it", "no_production_path_writes_deferred_call_records"),
            ("python", "scripts/test-portable-test-selection.py", "test_exact_rig_test_exclusion_keeps_guard_test_selected"),
        }
        available = {(entry[0], entry[1], entry[5]) for entry in cls.entries}
        missing = sorted(required - available)
        if missing:
            raise AssertionError(f"{GUARDS}: required quickcheck guards are missing: {missing}")

        cls.feature_entries = []
        for line_number, line in enumerate(FEATURE_TESTS.read_text().splitlines(), 1):
            if not line or line.startswith("#"):
                continue
            fields = line.split("|", maxsplit=5)
            if len(fields) != 6:
                raise AssertionError(f"{FEATURE_TESTS}:{line_number}: expected six pipe-separated fields")
            package, target, features, source_name, symbol, reason = fields
            if not reason.strip():
                raise AssertionError(f"{FEATURE_TESTS}:{line_number}: every entry needs a one-line reason")
            cargo_package = cls.packages.get(package)
            if cargo_package is None or not any(
                entry["name"] == target and "test" in entry["kind"]
                for entry in cargo_package["targets"]
            ):
                raise AssertionError(f"{FEATURE_TESTS}:{line_number}: missing test target {package}::{target}")
            source = ROOT / source_name
            text = source.read_text()
            function = re.search(rf"\bfn\s+{re.escape(symbol)}\s*\(", text)
            if function is None:
                raise AssertionError(f"{FEATURE_TESTS}:{line_number}: missing test {symbol} in {source_name}")
            prior = [item.strip() for item in text[:function.start()].splitlines() if item.strip()]
            test_attribute = re.compile(r"#\s*\[\s*(?:tokio::)?test\b[^\]]*\]")
            if not prior or not test_attribute.fullmatch(prior[-1]):
                raise AssertionError(f"{FEATURE_TESTS}:{line_number}: {symbol} is no longer a test")
            for feature in features.split(","):
                feature_package, _, feature_name = feature.partition("/")
                feature_meta = cls.packages.get(feature_package)
                if feature_meta is None or feature_name not in feature_meta["features"]:
                    raise AssertionError(f"{FEATURE_TESTS}:{line_number}: missing Cargo feature {feature}")
            cls.feature_entries.append((package, target, features))
        if ("broca-catalog", "reasoning_variants") not in {
            (entry[0], entry[1]) for entry in cls.feature_entries
        }:
            raise AssertionError(f"{FEATURE_TESTS}: reasoning_variants must remain a quickcheck feature suite")

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="broca-quickcheck-guards-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        scripts = self.root / "scripts"
        scripts.mkdir()
        for name in ("quickcheck.sh", "quickcheck-guards.list", "quickcheck-feature-tests.list"):
            shutil.copy2(ROOT / "scripts" / name, scripts / name)
        # quickcheck runs every cargo step through the watchdog, so the fixture needs it too.
        (scripts / "lib").mkdir()
        shutil.copy2(ROOT / "scripts/lib/cargo-watchdog.py", scripts / "lib" / "cargo-watchdog.py")
        for name in ("test-portable-test-selection.py", "check-portable-test-selection.py"):
            shutil.copy2(ROOT / "scripts" / name, scripts / name)
        self.home = self.root / "home"
        (self.home / ".cargo" / "bin").mkdir(parents=True)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        cargo = self.bin / "cargo"
        cargo.write_text(CARGO)
        cargo.chmod(0o755)
        self.events = self.root / "cargo-events.jsonl"

    def run_quickcheck(self, package, **overrides):
        env = {
            **os.environ,
            "HOME": str(self.home),
            "PATH": f"{self.bin}:{os.environ['PATH']}",
            "QUICKCHECK_PACKAGE": package,
            "QUICKCHECK_EVENTS": str(self.events),
            "PYTHONDONTWRITEBYTECODE": "1",
            "BROCA_SUITES_PYTHON": sys.executable,
        }
        env.update(overrides)
        result = subprocess.run(
            ["bash", "scripts/quickcheck.sh", package], cwd=self.root,
            env=env, capture_output=True, text=True, timeout=30,
        )
        events = [json.loads(line) for line in self.events.read_text().splitlines()]
        return result, events

    def test_guards_run_for_an_unrelated_crate(self):
        result, events = self.run_quickcheck("broca-subc")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(
            ["test", "--locked", "-p", "broca-module", "--test", "it", "open_failed_classes::", "-F", "broca-module/test-clock", "--quiet"],
            events,
        )
        self.assertIn("quickcheck guards OK", result.stdout)

    def test_a_failing_guard_fails_quickcheck(self):
        result, events = self.run_quickcheck("broca-subc", FAIL_GUARD="1")
        self.assertEqual(result.returncode, 47, result.stdout + result.stderr)
        self.assertTrue(any("open_failed_classes::" in call for call in events))
        self.assertNotIn("quickcheck guards OK", result.stdout)
        self.assertNotIn("quickcheck OK:", result.stdout)

    def test_changed_catalog_runs_feature_gated_integration_targets(self):
        result, events = self.run_quickcheck("broca-catalog")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        integration_calls = [call for call in events if call[:1] == ["test"] and "--test" in call]
        catalog_call = next(
            call for call in integration_calls
            if "reasoning_variants" in call and "-p" in call and call[call.index("-p") + 1] == "broca-catalog"
        )
        self.assertEqual(catalog_call[catalog_call.index("--test") + 1], "reasoning_variants")
        self.assertIn("broca-catalog/live", catalog_call)


if __name__ == "__main__":
    unittest.main(verbosity=2)
