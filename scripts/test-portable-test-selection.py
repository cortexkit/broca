#!/usr/bin/env python3
"""Tests for the Linux preflight's source-derived rig exclusion guard."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import importlib.util

checker_path = ROOT / "scripts" / "check-portable-test-selection.py"
spec = importlib.util.spec_from_file_location("check_portable_test_selection", checker_path)
assert spec is not None and spec.loader is not None
selection = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = selection
spec.loader.exec_module(selection)


class PortableRigSelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="portable-rig-selection-")
        self.root = Path(self.temp.name)
        self.source = self.root / "crates" / "sample" / "tests" / "it.rs"
        self.source.parent.mkdir(parents=True)
        self.source.write_text(
            r'''mod mixed {
    use std::process::Command;

    #[tokio::test]
    async fn real_daemon_uses_shared_setup() {
        helpers::start_real_daemon();
    }

    #[tokio::test]
    async fn required_daemon_lane_refuses_to_skip_missing_binaries() {
        let _ = Command::new(std::env::current_exe().unwrap())
            .env("BROCA_REQUIRE_SUBC", "1")
            .output();
    }

    fn source_text_is_not_a_rig() {
        let _example = r#"std::env::var(\"BROCA_REQUIRE_SUBC\"); tokio_rig_command(fake)"#;
    }
}

mod helpers {
    fn start_real_daemon() {
        let daemon = broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
        let _child = broca_test_support::tokio_rig_command(daemon, ".", "daemon");
    }
}
'''
        )
        self.target = selection.Target("sample", "it", self.source)
        self.suite = {
            "sample::it": {
                "status": "listed",
                "testcases": {
                    "mixed::real_daemon_uses_shared_setup": {
                        "filter-match": {"status": "matches"},
                    },
                    "mixed::required_daemon_lane_refuses_to_skip_missing_binaries": {
                        "filter-match": {"status": "matches"},
                    },
                },
            }
        }

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_external_helper_module_reaches_the_test_item(self) -> None:
        main = self.source.parent / "main.rs"
        main.write_text(
            """mod mixed;
mod helpers;
"""
        )
        mixed = self.source.parent / "mixed.rs"
        mixed.write_text(
            """#[tokio::test]
async fn daemon_test_calls_external_helper() {
    helpers::start_real_daemon();
}
"""
        )
        helpers = self.source.parent / "helpers.rs"
        helpers.write_text(
            """fn start_real_daemon() {
    broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
    broca_test_support::tokio_rig_command("ck-subc", ".", "daemon");
}
"""
        )
        inventory = {
            "rust-suites": {
                "sample::it": {
                    "status": "listed",
                    "testcases": {
                        "mixed::daemon_test_calls_external_helper": {
                            "filter-match": {"status": "matches"},
                        },
                    },
                }
            }
        }
        problems = selection.check_selection(
            self.root,
            targets=[selection.Target("sample", "it", main)],
            inventory=inventory,
        )
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("mixed::daemon_test_calls_external_helper", problems[0])

    def test_unexcluded_planted_rig_test_names_its_file_and_item(self) -> None:
        problems = selection.check_selection(
            self.root,
            targets=[self.target],
            inventory={"rust-suites": self.suite},
        )
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("rig-dependent test remains selected", problems[0])
        self.assertIn("crates/sample/tests/it.rs:", problems[0])
        self.assertIn("mixed::real_daemon_uses_shared_setup", problems[0])

    def test_exact_rig_test_exclusion_keeps_guard_test_selected(self) -> None:
        inventory = {
            "rust-suites": {
                "sample::it": {
                    "status": "listed",
                    "testcases": {
                        "mixed::real_daemon_uses_shared_setup": {
                            "filter-match": {"status": "mismatch"},
                        },
                        "mixed::required_daemon_lane_refuses_to_skip_missing_binaries": {
                            "filter-match": {"status": "matches"},
                        },
                    },
                }
            }
        }
        self.assertEqual(
            selection.check_selection(self.root, targets=[self.target], inventory=inventory),
            [],
        )

    def test_repository_rig_tests_are_excluded_by_nextest_filter(self) -> None:
        targets = selection._target_metadata(ROOT)
        provider_it = next(
            target for target in targets
            if target.package == "broca-provider" and target.name == "it"
        )
        source_tests, functions, errors = selection.discover_rig_tests(provider_it, ROOT)
        self.assertFalse(errors, "\n".join(errors))
        rigged = selection._rig_functions(functions)
        marked_provider_tests = [
            test for test in source_tests
            if test.source.name == "anthropic_e2e.rs"
            and any(
                index in rigged and fn.start == test.start and fn.source == test.source
                for index, fn in enumerate(functions)
            )
        ]
        self.assertTrue(marked_provider_tests, "the shared aimock helper must reach family tests")
        problems = selection.check_selection(ROOT, targets=targets)
        self.assertFalse(problems, "\n".join(problems))


if __name__ == "__main__":
    unittest.main(verbosity=2)
