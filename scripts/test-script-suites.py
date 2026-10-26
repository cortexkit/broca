#!/usr/bin/env python3
"""scripts/script-suites.sh accounts for every operator suite file in scripts/.

A suite file that no list names is never run by CI or the local gate, so it rots while
looking like coverage. That happened to test-place-release-restart.py: it guarded the
restart-after-placement safety arms, was never registered, and stopped passing when
placement began to require a dSYM. These arms fail when a suite file is added without being
listed, and when the macOS-only suite's required mode stops turning a skip into a failure.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SUITES = ROOT / "scripts" / "script-suites.sh"
RESTART = ROOT / "scripts" / "test-place-release-restart.py"


def bash_array(text: str, name: str) -> list[str]:
    """The words of a `name=( ... )` array in script-suites.sh, with comments removed."""
    match = re.search(rf"^{name}=\(\n(.*?)^\)", text, re.S | re.M)
    if match is None:
        raise AssertionError(f"script-suites.sh has no {name}=( ... ) array")
    words: list[str] = []
    for line in match.group(1).splitlines():
        words.extend(line.split("#", 1)[0].split())
    return words


def suite_files() -> set[str]:
    """Every file under scripts/ that is laid out as a runnable test suite."""
    scripts = ROOT / "scripts"
    found = [
        *scripts.glob("test-*.py"),
        *(scripts / "tests").glob("test*.py"),
        *(scripts / "lib").glob("test_*.py"),
    ]
    return {str(path.relative_to(ROOT)) for path in found}


class InventoryTests(unittest.TestCase):
    def setUp(self) -> None:
        text = SUITES.read_text()
        self.lists = {
            name: bash_array(text, name)
            for name in ("suites", "native_macos_suites", "run_elsewhere")
        }

    def test_every_suite_file_is_listed(self) -> None:
        files = suite_files()
        # An empty scan would make the comparison below pass on nothing.
        self.assertGreater(len(files), 20, f"suite scan found too few files: {sorted(files)}")
        listed = {word for words in self.lists.values() for word in words}
        unlisted = sorted(files - listed)
        self.assertEqual(
            unlisted, [],
            "suite files that script-suites.sh never runs; add each to suites, "
            "native_macos_suites, or run_elsewhere with the place that runs it",
        )

    def test_no_suite_is_listed_twice(self) -> None:
        seen: dict[str, str] = {}
        for name, words in self.lists.items():
            for word in words:
                if not word.endswith(".py"):
                    continue
                self.assertNotIn(word, seen, f"{word} is in both {seen.get(word)} and {name}")
                seen[word] = name

    def test_the_restart_suite_runs_in_required_mode(self) -> None:
        self.assertIn("scripts/test-place-release-restart.py", self.lists["native_macos_suites"])
        self.assertIn('BROCA_REQUIRE_NATIVE_MACOS=1 python3 "$suite"', SUITES.read_text())


class RequiredModeTests(unittest.TestCase):
    """Run the restart suite where codesign cannot be found, as on a Linux host."""

    def run_restart_suite(self, required: bool) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory(prefix="script-suites-empty-path-") as empty:
            env = {key: value for key, value in os.environ.items() if key != "BROCA_REQUIRE_NATIVE_MACOS"}
            env["PATH"] = empty
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            if required:
                env["BROCA_REQUIRE_NATIVE_MACOS"] = "1"
            return subprocess.run(
                [sys.executable, str(RESTART)], env=env, capture_output=True, text=True, timeout=120,
            )

    @staticmethod
    def child_output(result: subprocess.CompletedProcess[str]) -> str:
        """Keep the child's unittest summary separate from this suite's test count."""
        lines = (result.stdout + result.stderr).splitlines()
        return "\n".join(line for line in lines if not line.startswith("Ran "))

    def test_required_mode_turns_the_skip_into_a_failure(self) -> None:
        result = self.run_restart_suite(required=True)
        self.assertNotEqual(result.returncode, 0, self.child_output(result))
        self.assertIn("BROCA_REQUIRE_NATIVE_MACOS=1 but", result.stderr)

    def test_without_required_mode_the_suite_skips(self) -> None:
        """The control: the same host without the switch reports skips, not failures."""
        result = self.run_restart_suite(required=False)
        self.assertEqual(result.returncode, 0, self.child_output(result))
        self.assertIn("skipped", result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
