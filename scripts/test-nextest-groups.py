#!/usr/bin/env python3
"""The nextest resource groups select the test modules they are meant to cap.

Integration tests that share one test binary are put into a group by test-name prefix, and a
prefix that names no module selects nothing without any nextest error. The real-daemon cap once
listed a module that did not exist and missed six that started a daemon, and a test that boots
five aimock processes sat outside the aimock cap. These arms read .config/nextest.toml and the
binaries' module lists, so a renamed, misspelled or forgotten module fails here.
"""

from __future__ import annotations

import re
import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NEXTEST = ROOT / ".config" / "nextest.toml"

# What a module's source contains when it starts the resource its group caps. Each pattern is
# matched against the module's own file (or its directory's mod.rs).
SUBC_DAEMON = re.compile(r'\bRig::start|"ck-subc"|SUBC_CORE_BIN')
AIMOCK = re.compile(r"\brun_family_e2e\(")


def overrides() -> list[dict]:
    return tomllib.loads(NEXTEST.read_text())["profile"]["default"]["overrides"]


def prefixed_modules(filter_text: str) -> list[tuple[str, str]]:
    """(package, module) for each name inside a `test(/^(a|b)::/)` term of a filter. A term
    belongs to the last `package(=...)` written before it, which is how every filter here
    nests its clauses."""
    found: list[tuple[str, str]] = []
    for term in re.finditer(r"test\(/\^\(?([A-Za-z0-9_|]+)\)?::/\)", filter_text):
        packages = re.findall(r"package\(=([A-Za-z0-9_-]+)\)", filter_text[: term.start()])
        if not packages:
            raise AssertionError(f"no package governs {term.group(0)} in {filter_text!r}")
        found.extend((packages[-1], name) for name in term.group(1).split("|"))
    return found


def prefixes(filter_text: str) -> list[str]:
    return [name for _, name in prefixed_modules(filter_text)]


def declared_modules(crate: str) -> dict[str, Path]:
    """The test modules of a crate's tests/it binary, mapped to their source files.

    A module brought in with `#[path = "../..."]` is a helper shared with other test binaries
    (the daemon rig itself, say), not tests of this binary, so it is left out."""
    it = ROOT / "crates" / crate / "tests" / "it"
    modules: dict[str, Path] = {}
    pending_path: str | None = None
    for line in (it / "main.rs").read_text().splitlines():
        line = line.strip()
        path_attr = re.fullmatch(r'#\[path\s*=\s*"([^"]+)"\]', line)
        if path_attr:
            pending_path = path_attr.group(1)
            continue
        declared = re.fullmatch(r"(?:pub\s+)?mod\s+([a-z0-9_]+);", line)
        if declared:
            name = declared.group(1)
            if pending_path is not None:
                if pending_path.startswith("../"):
                    pending_path = None
                    continue
                source = it / pending_path
            elif (it / f"{name}.rs").exists():
                source = it / f"{name}.rs"
            else:
                source = it / name / "mod.rs"
            modules[name] = source
        if not line.startswith("#[") and line:
            pending_path = None
    return modules


def group_prefixes(group: str, package: str) -> set[str]:
    names: set[str] = set()
    for override in overrides():
        if override.get("test-group") == group:
            names.update(name for owner, name in prefixed_modules(override["filter"])
                         if owner == package)
    return names


class NextestGroupTests(unittest.TestCase):
    def test_every_prefix_names_a_declared_module(self) -> None:
        checked = 0
        for override in overrides():
            for package, name in prefixed_modules(override["filter"]):
                checked += 1
                self.assertIn(name, declared_modules(package),
                              f"{package}'s it binary has no test module {name}")
        # An empty walk would pass while checking nothing.
        self.assertGreater(checked, 15)

    def assert_capped(self, crate: str, group: str, marker: re.Pattern[str]) -> None:
        modules = declared_modules(crate)
        users = sorted(name for name, path in modules.items()
                       if path.exists() and marker.search(path.read_text()))
        self.assertGreater(len(users), 3, f"found too few {group} users in {crate}: {users}")
        capped = group_prefixes(group, crate)
        self.assertEqual(sorted(set(users) - capped), [],
                         f"{crate} modules that start what the {group} group caps, outside it")

    def test_every_real_daemon_module_is_in_the_subc_spine_cap(self) -> None:
        self.assert_capped("broca-subc", "subc_spine", SUBC_DAEMON)

    def test_every_aimock_module_is_in_the_aimock_cap(self) -> None:
        self.assert_capped("broca-provider", "aimock", AIMOCK)

    def test_the_five_mock_test_takes_the_whole_aimock_group(self) -> None:
        caps = tomllib.loads(NEXTEST.read_text())["test-groups"]
        first = next(o for o in overrides()
                     if "mixed_family_concurrency_e2e" in prefixes(o["filter"]))
        self.assertEqual(first.get("test-group"), "aimock")
        self.assertEqual(first.get("threads-required"), caps["aimock"]["max-threads"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
