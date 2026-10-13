#!/usr/bin/env python3
"""Executable verification suite for how operator scripts locate broca's state root.

The daemon resolves it as BROCA_STATE_ROOT, else $XDG_DATA_HOME/cortexkit/broca, else
$HOME/.local/share/cortexkit/broca (crates/broca-module-serve/src/state_root.rs). A script that
used a different rule would read, gate or sweep a tree the daemon is not using and still report a
clean result, so this checks the Python helper, the shell reader and every script against it.
"""

from __future__ import annotations

import os
import pathlib
import re
import subprocess
import sys
import unittest

SCRIPTS = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

from lib import state_root  # noqa: E402

# Environments paired with the root the daemon's rule gives for each.
CASES = [
    (
        "variable wins",
        {"BROCA_STATE_ROOT": "/srv/broca", "XDG_DATA_HOME": "/xdg", "HOME": "/home/u"},
        "/srv/broca",
    ),
    ("xdg over home", {"XDG_DATA_HOME": "/xdg", "HOME": "/home/u"}, "/xdg/cortexkit/broca"),
    ("home last", {"HOME": "/home/u"}, "/home/u/.local/share/cortexkit/broca"),
    (
        "empty values are unset",
        {"BROCA_STATE_ROOT": "", "XDG_DATA_HOME": "", "HOME": "/home/u"},
        "/home/u/.local/share/cortexkit/broca",
    ),
]


class PythonHelper(unittest.TestCase):
    def test_rungs_resolve_in_order(self) -> None:
        for name, env, expected in CASES:
            with self.subTest(name):
                self.assertEqual(state_root.default_state_root(env), pathlib.Path(expected))

    def test_nothing_set_refuses_by_name(self) -> None:
        with self.assertRaises(state_root.StateRootError) as caught:
            state_root.default_state_root({})
        for name in ("BROCA_STATE_ROOT", "XDG_DATA_HOME", "HOME"):
            self.assertIn(name, str(caught.exception))

    def test_relative_data_home_refuses(self) -> None:
        with self.assertRaises(state_root.StateRootError):
            state_root.default_state_root({"XDG_DATA_HOME": "relative/path"})


class ShellReader(unittest.TestCase):
    """wal-floor.sh, which place-release.sh and stage-release.sh source, agrees with the helper."""

    def shell_root(self, env: dict[str, str]) -> str:
        result = subprocess.run(
            ["bash", "-c", f'source "{SCRIPTS / "wal-floor.sh"}" && broca_state_root'],
            env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), **env},
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    def test_shell_and_python_agree_on_every_rung(self) -> None:
        for name, env, expected in CASES:
            with self.subTest(name):
                self.assertEqual(self.shell_root(env), expected)

    def test_floor_file_follows_the_state_root(self) -> None:
        result = subprocess.run(
            ["bash", "-c", f'source "{SCRIPTS / "wal-floor.sh"}" && broca_wal_floor_path'],
            env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "XDG_DATA_HOME": "/xdg", "HOME": "/h"},
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(result.stdout.strip(), "/xdg/cortexkit/broca/wal-feature-floor")


class NoScriptHardCodesTheRoot(unittest.TestCase):
    """No script spells the state root as a fixed path under HOME any more.

    A literal `.local/share/cortexkit/broca` skips the BROCA_STATE_ROOT and XDG_DATA_HOME rungs.
    Python scripts call lib/state_root.py; shell scripts expand
    `${BROCA_STATE_ROOT:-${XDG_DATA_HOME:-$HOME/.local/share}/cortexkit/broca}`.
    """

    HARD_CODED = re.compile(r"\.local/share/cortexkit/broca(?![\w-])")

    def test_no_literal_state_root_outside_comments(self) -> None:
        offenders = []
        checked = 0
        for path in sorted(SCRIPTS.rglob("*")):
            # The helper and this suite spell the path on purpose: one defines the rule, the
            # other states its expected results.
            if path.suffix not in (".py", ".sh") or path in (
                SCRIPTS / "lib" / "state_root.py",
                pathlib.Path(__file__).resolve(),
            ):
                continue
            checked += 1
            for number, line in enumerate(path.read_text().splitlines(), 1):
                if line.lstrip().startswith("#"):
                    continue
                if self.HARD_CODED.search(line):
                    offenders.append(f"{path.relative_to(SCRIPTS)}:{number}: {line.strip()}")
        self.assertGreater(checked, 20, "the scan reached too few scripts to mean anything")
        self.assertEqual(offenders, [], "\n".join(offenders))


if __name__ == "__main__":
    unittest.main()
