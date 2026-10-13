"""Compare Python path resolution with fixed supervisor homes and broca directories.

Rust reads the same JSON fixtures, so both implementations must agree with their expected paths.
"""

from __future__ import annotations

import hashlib
import json
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[3]
FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures"
sys.path.insert(0, str(ROOT))
from scripts.lib import state_root


class PathLadders(unittest.TestCase):
    def cases(self, fixture: str, count: int):
        rows = json.loads((FIXTURES / fixture).read_text(encoding="utf-8"))["cases"]
        platform = "windows" if sys.platform == "win32" else "unix"
        self.assertTrue(all(row["platform"] in ("any", "unix", "windows") for row in rows))
        rows = [row for row in rows if row["platform"] in ("any", platform)]
        self.assertEqual(len(rows), count)
        return rows

    def test_state_supervisor_golden(self):
        fixture = "cortexkit-store-types-0.2.2/data_home_resolution.json"
        self.assertEqual(
            hashlib.sha256((FIXTURES / fixture).read_bytes()).hexdigest(),
            "0390a2734f3e4ca9f853f23839e017b774b0319b9f47a39fa7dd44e47d436d09",
        )
        for row in self.cases(fixture, 8 if sys.platform == "win32" else 6):
            with self.subTest(row["name"]):
                # Passing only the row clears inherited homes without modifying os.environ.
                env = row["env"]
                if row["name"] in ("all_unset_falls_to_relative", "windows_all_unset_falls_to_relative"):
                    with self.assertRaises(state_root.StateRootError):
                        state_root.default_state_root(env)
                elif not pathlib.Path(row["expect"]).is_absolute():
                    # Subc may return a relative data home; broca requires derived data homes
                    # to be absolute so daemon state does not depend on its working directory.
                    with self.assertRaises(state_root.StateRootError):
                        state_root.default_state_root(env)
                else:
                    self.assertEqual(
                        state_root.default_state_root(env),
                        pathlib.Path(row["expect"]) / "cortexkit" / "broca",
                    )

    def test_config_supervisor_golden(self):
        fixture = "subconscious-f4e71180/config_home_resolution.json"
        self.assertEqual(
            hashlib.sha256((FIXTURES / fixture).read_bytes()).hexdigest(),
            "4b28353a4f477c4ae64ba9cad981ae11583bf417267bc25626c4c5a383b3fc9d",
        )
        for row in self.cases(fixture, 8 if sys.platform == "win32" else 6):
            with self.subTest(row["name"]):
                if row["name"] in ("all_unset_falls_to_relative", "windows_all_unset_falls_to_relative"):
                    # For the all-unset case, broca raises instead of guessing relative .config.
                    with self.assertRaises(state_root.ConfigHomeError):
                        state_root.default_config_home(row["env"])
                else:
                    self.assertEqual(
                        state_root.default_config_home(row["env"]),
                        pathlib.Path(row["expect"]) / "cortexkit" / "broca",
                    )

    def test_state_composed_golden(self):
        for row in self.cases("path_ladders.json", 15 if sys.platform == "win32" else 5):
            with self.subTest(row["name"]):
                if "state_error" in row:
                    with self.assertRaises(state_root.StateRootError) as error:
                        state_root.default_state_root(row["env"])
                    if row["state_error"] != "unresolvable":
                        self.assertIn(row["state_error"], str(error.exception))
                else:
                    self.assertEqual(state_root.default_state_root(row["env"]), pathlib.Path(row["state"]))

    def test_config_composed_golden(self):
        for row in self.cases("path_ladders.json", 15 if sys.platform == "win32" else 5):
            with self.subTest(row["name"]):
                if row["config"] is None:
                    with self.assertRaises(state_root.ConfigHomeError):
                        state_root.default_config_home(row["env"])
                else:
                    self.assertEqual(state_root.default_config_home(row["env"]), pathlib.Path(row["config"]))


if __name__ == "__main__":
    unittest.main()
