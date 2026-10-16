#!/usr/bin/env python3
"""Refuse a preflight filter that selects tests needing external rigs."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent.parent
RIG_CALL = re.compile(
    r"\b(?:subc_bin|tokio_rig_command|rig_command|claustrum_bin|auth_bin|"
    r"start_aimock|spawn_aimock|launch_aimock|aimock_command)\s*(?:<[^;{}]*>)?\s*\("
    r"|\b(?:AiMock|Aimock)::(?:start|new|spawn|launch)\s*(?:<[^;{}]*>)?\s*\("
)
RIG_ENV_READ = re.compile(
    r'\b(?:std::)?env::(?:var|var_os)\s*\(\s*"(?:BROCA_REQUIRE_SUBC|BROCA_REQUIRE_VAULT|BROCA_REQUIRE_AIMOCK|'
    r'SUBC_CORE_BIN|FAKE_AFT_STUB_BIN|CK_CLAUSTRUM_BIN|CK_AUTH_BIN|CK_CLAUSTRUM_CHECKOUT|AIMOCK_CMD|AIMOCK_BASE_URL)"'
)
CALL = re.compile(r"\b([A-Za-z_]\w*(?:::[A-Za-z_]\w*)*)\s*(?:<[^;{}]*>)?\s*\(")
FN = re.compile(r"\bfn\s+([A-Za-z_]\w*)\b")
MODULE = re.compile(r"\bmod\s+([A-Za-z_]\w*)\s*([;{])")
TEST_ATTRIBUTE = re.compile(r"(?:^|::)(?:test|rstest|test_case)(?:\s|\(|,|$)")
GENERIC_HELPERS = {
    "build", "check", "connect", "default", "drop", "from", "get", "new",
    "required", "resolve", "run", "setup", "start", "wait", "with",
}


@dataclass(frozen=True)
class Function:
    name: str
    owner: str | None
    body: str
    source: Path
    start: int
    rig_seed: bool

    @property
    def key(self) -> str:
        return f"{self.owner}::{self.name}" if self.owner else self.name


@dataclass(frozen=True)
class RigTest:
    package: str
    target: str
    name: str
    source: Path
    line: int
    start: int


@dataclass(frozen=True)
class Target:
    package: str
    name: str
    source: Path


def _mask_rust(
    source: str, *, keep_strings: bool = False, keep_raw_strings: bool = False
) -> str:
    """Blank comments and strings while preserving offsets and line numbers."""
    out = list(source)
    i = 0
    n = len(source)

    def blank(start: int, end: int) -> None:
        for pos in range(start, end):
            if out[pos] not in "\r\n":
                out[pos] = " "

    while i < n:
        if source.startswith("//", i):
            end = source.find("\n", i + 2)
            if end < 0:
                end = n
            blank(i, end)
            i = end
            continue
        if source.startswith("/*", i):
            start = i
            i += 2
            depth = 1
            while i < n and depth:
                if source.startswith("/*", i):
                    depth += 1
                    i += 2
                elif source.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            blank(start, i)
            continue

        # Raw strings and raw byte strings can contain Rust delimiters and comments.
        raw_start = i
        if source.startswith("br", i):
            quote = i + 2
        elif source.startswith("r", i) and i + 1 < n and source[i + 1] in '\"#':
            quote = i + 1
        else:
            quote = n
        if quote < n:
            hashes = 0
            while quote < n and source[quote] == "#":
                hashes += 1
                quote += 1
            if quote < n and source[quote] == '\"':
                terminator = '\"' + "#" * hashes
                end = source.find(terminator, quote + 1)
                end = n if end < 0 else end + len(terminator)
                if not keep_strings or not keep_raw_strings:
                    blank(raw_start, end)
                i = end
                continue

        # Normal strings and byte strings.
        quote_at = i + 1 if source.startswith('b"', i) else i
        if quote_at < n and source[quote_at] == '"':
            start = i
            i = quote_at + 1
            while i < n:
                if source[i] == "\\":
                    i += 2
                elif source[i] == '"':
                    i += 1
                    break
                else:
                    i += 1
            if not keep_strings:
                blank(start, i)
            continue

        # Character literals are not needed by the checker; blank them so braces in
        # character constants cannot affect module/function boundaries.
        if source[i] == "'" and i + 2 < n:
            end = i + 1
            if source[end] == "\\":
                end += 2
            else:
                end += 1
            if end < n and source[end] == "'":
                blank(i, end + 1)
                i = end + 1
                continue
        i += 1
    return "".join(out)


def _pairs(mask: str) -> dict[int, int]:
    pairs: dict[int, int] = {}
    stack: list[tuple[str, int]] = []
    closing = {")": "(", "]": "[", "}": "{"}
    for i, char in enumerate(mask):
        if char in "([{":
            stack.append((char, i))
        elif char in closing:
            if stack and stack[-1][0] == closing[char]:
                _, start = stack.pop()
                pairs[start] = i
    return pairs


def _impl_owners(mask: str, pairs: dict[int, int]) -> list[tuple[int, int, str]]:
    owners: list[tuple[int, int, str]] = []
    for match in re.finditer(r"\bimpl\b", mask):
        open_at = mask.find("{", match.end())
        if open_at < 0 or open_at not in pairs:
            continue
        header = mask[match.end():open_at]
        # Inherent and trait impls both end with the implemented type before `{`.
        header = re.sub(r"\s+where\b.*$", "", header, flags=re.S).strip()
        owner_match = re.search(r"([A-Za-z_]\w*(?:::[A-Za-z_]\w*)*)(?:\s*<.*>)?$", header)
        if owner_match:
            owners.append((open_at, pairs[open_at], owner_match.group(1).split("::")[-1]))
    return owners


def _functions(path: Path, source: str) -> list[Function]:
    mask = _mask_rust(source)
    pairs = _pairs(mask)
    owners = _impl_owners(mask, pairs)
    functions: list[Function] = []
    for match in FN.finditer(mask):
        open_at = mask.find("{", match.end())
        semicolon = mask.find(";", match.end())
        if open_at < 0 or open_at not in pairs or (semicolon >= 0 and semicolon < open_at):
            continue
        owner = next((name for start, end, name in owners if start < match.start() < end), None)
        body = mask[open_at + 1:pairs[open_at]]
        literal_body = _mask_rust(source[open_at + 1:pairs[open_at]], keep_strings=True)
        functions.append(
            Function(
                match.group(1), owner, body, path, match.start(),
                bool(RIG_CALL.search(body) or RIG_ENV_READ.search(literal_body)),
            )
        )
    return functions


def _body_open(mask: str, match: re.Match[str], pairs: dict[int, int]) -> int | None:
    open_at = mask.find("{", match.end())
    semicolon = mask.find(";", match.end())
    if open_at < 0 or open_at not in pairs or (semicolon >= 0 and semicolon < open_at):
        return None
    return open_at


def _is_test_attribute(mask: str, scope_start: int, item_start: int) -> bool:
    prefix = mask[scope_start:item_start]
    boundary = max(prefix.rfind("}"), prefix.rfind(";"))
    preamble = prefix[boundary + 1:]
    attributes = re.findall(r"#\s*\[([^\]]*)\]", preamble)
    return any(TEST_ATTRIBUTE.search(attr) for attr in attributes)


def _path_attribute(source: str, mask: str, scope_start: int, item_start: int) -> str | None:
    prefix = mask[scope_start:item_start]
    boundary = max(prefix.rfind("}"), prefix.rfind(";"))
    start = scope_start + boundary + 1
    attr = re.search(r"#\s*\[\s*path\s*=\s*\"([^\"]+)\"\s*\]", source[start:item_start])
    return attr.group(1) if attr else None


def _module_file(
    current_file: Path, name: str, path_attr: str | None, *, crate_root: bool
) -> Path | None:
    if path_attr is not None:
        candidate = (current_file.parent / path_attr).resolve()
        return candidate if candidate.is_file() else None
    base = (
        current_file.parent
        if crate_root or current_file.name in {"mod.rs", "main.rs", "lib.rs"}
        else current_file.parent / current_file.stem
    )
    for candidate in (base / f"{name}.rs", base / name / "mod.rs"):
        if candidate.is_file():
            return candidate.resolve()
    return None


def discover_rig_tests(target: Target, root: Path) -> tuple[list[RigTest], list[Function], list[str]]:
    tests: list[RigTest] = []
    functions: list[Function] = []
    errors: list[str] = []
    seen: set[tuple[Path, tuple[str, ...]]] = set()

    def visit(path: Path, module_path: tuple[str, ...]) -> None:
        path = path.resolve()
        key = (path, module_path)
        if key in seen:
            return
        seen.add(key)
        try:
            source = path.read_text()
        except OSError as error:
            errors.append(f"cannot read test module {path.relative_to(root)}: {error}")
            return
        mask = _mask_rust(source)
        pairs = _pairs(mask)
        functions.extend(_functions(path, source))

        def scan_scope(start: int, end: int, parts: tuple[str, ...]) -> None:
            for match in FN.finditer(mask, start, end):
                # Ignore functions nested in blocks; inline modules are scanned recursively.
                if not _is_top_level(mask, start, match.start()):
                    continue
                if not _is_test_attribute(mask, start, match.start()):
                    continue
                open_at = _body_open(mask, match, pairs)
                if open_at is None:
                    errors.append(f"cannot find body for test {path.relative_to(root)}::{match.group(1)}")
                    continue
                qualified = "::".join((*parts, match.group(1)))
                tests.append(
                    RigTest(target.package, target.name, qualified, path,
                            source.count("\n", 0, match.start()) + 1, match.start())
                )

        # Parse all module declarations at this scope, then recurse into child modules.
        def scan_modules(start: int, end: int, parts: tuple[str, ...]) -> None:
            for match in MODULE.finditer(mask, start, end):
                if not _is_top_level(mask, start, match.start()):
                    continue
                name, terminator = match.groups()
                if terminator == "{":
                    open_at = mask.find("{", match.end() - 1, end)
                    close_at = pairs.get(open_at)
                    if close_at is None:
                        errors.append(f"unclosed module {path.relative_to(root)}::{name}")
                        continue
                    scan_scope(open_at + 1, close_at, (*parts, name))
                    scan_modules(open_at + 1, close_at, (*parts, name))
                else:
                    path_attr = _path_attribute(source, mask, start, match.start())
                    child = _module_file(path, name, path_attr, crate_root=not module_path)
                    if child is None:
                        errors.append(
                            f"cannot resolve test module {path.relative_to(root)}::{name}"
                        )
                        continue
                    visit(child, (*parts, name))

        # Each module-body call starts at its own item scope. A brace-depth map keeps
        # function-local `mod` declarations from being treated as Rust module items.
        scan_scope(0, len(mask), module_path)
        scan_modules(0, len(mask), module_path)

    visit(target.source, ())
    return tests, functions, errors


def _is_top_level(mask: str, start: int, offset: int) -> bool:
    depth = 0
    for char in mask[start:offset]:
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
    return depth == 0


def _call_paths(body: str) -> set[str]:
    return {match.group(1) for match in CALL.finditer(body)}


def _rig_functions(functions: list[Function]) -> set[int]:
    """Follow direct rig setup through uniquely named helpers in one test target."""
    rigged = {index for index, function in enumerate(functions) if function.rig_seed}
    name_counts: dict[str, int] = {}
    for function in functions:
        name_counts[function.name] = name_counts.get(function.name, 0) + 1

    changed = True
    while changed:
        changed = False
        by_source: dict[Path, tuple[set[str], set[str]]] = {}
        for index in rigged:
            function = functions[index]
            keys, names = by_source.setdefault(function.source, (set(), set()))
            keys.add(function.key)
            names.add(function.name)
        # Method names like `Rig::start` are ambiguous across modules; only free
        # helpers with unique names can be followed across source files.
        external_names = {
            functions[index].name for index in rigged
            if functions[index].owner is None
        }

        for index, function in enumerate(functions):
            if index in rigged:
                continue
            local_keys, local_names = by_source.get(function.source, (set(), set()))
            for call in _call_paths(function.body):
                final = call.rsplit("::", 1)[-1]
                if (
                    call in local_keys
                    or (final in local_names and final not in GENERIC_HELPERS)
                    or (final in external_names and final not in GENERIC_HELPERS and name_counts[final] == 1)
                ):
                    rigged.add(index)
                    changed = True
                    break
    return rigged



def _target_metadata(root: Path) -> list[Target]:
    result = subprocess.run(
        ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
        cwd=root, capture_output=True, text=True, check=False,
    )
    if result.returncode:
        raise RuntimeError(f"cargo metadata failed ({result.returncode}):\n{result.stderr}")
    metadata = json.loads(result.stdout)
    targets: list[Target] = []
    for package in metadata["packages"]:
        for target in package["targets"]:
            if "test" not in target["kind"]:
                continue
            source = Path(target["src_path"]).resolve()
            if "tests" not in source.parts:
                continue
            targets.append(Target(package["name"], target["name"], source))
    return targets


def _portable_filter(root: Path) -> str:
    result = subprocess.run(
        ["bash", "-c", 'source scripts/portable-test-selection.conf; printf "%s" "$portable_integration_filter"'],
        cwd=root, capture_output=True, text=True, check=False,
    )
    if result.returncode:
        raise RuntimeError(f"could not load portable test filter:\n{result.stderr}")
    return result.stdout


def _nextest_inventory(root: Path, expression: str) -> dict:
    command = [
        "cargo", "nextest", "list", "--locked", "--workspace",
        "-F", "broca-provider/live", "-F", "broca-catalog/live",
        "-F", "broca-module-serve/test-clock", "-E", expression, "-T", "json",
    ]
    result = subprocess.run(command, cwd=root, capture_output=True, text=True, check=False)
    if result.returncode:
        raise RuntimeError(
            "cargo nextest list failed while checking the portable integration filter "
            f"({result.returncode}):\n{result.stdout}{result.stderr}"
        )
    return json.loads(result.stdout)


def check_selection(root: Path, targets: Iterable[Target] | None = None, inventory: dict | None = None) -> list[str]:
    """Return diagnostics for rig-dependent tests left in the filtered inventory."""
    root = root.resolve()
    targets = list(targets if targets is not None else _target_metadata(root))
    if inventory is None:
        inventory = _nextest_inventory(root, _portable_filter(root))
    suites = inventory.get("rust-suites", {})
    diagnostics: list[str] = []
    for target in targets:
        tests, functions, errors = discover_rig_tests(target, root)
        diagnostics.extend(errors)
        rigged_functions = _rig_functions(functions)
        for test in tests:
            local_rigged = {index for index in rigged_functions if functions[index].source == test.source}
            rig_names = {functions[index].name for index in local_rigged}
            rig_keys = {functions[index].key for index in local_rigged}
            test_indices = [
                index for index, fn in enumerate(functions)
                if fn.start == test.start and fn.source == test.source
            ]
            test_functions = [functions[index] for index in test_indices]
            test_calls = set().union(*(_call_paths(fn.body) for fn in test_functions)) if test_functions else set()
            is_rigged = any(index in local_rigged for index in test_indices)
            if not is_rigged:
                is_rigged = any(
                    call in rig_keys or
                    (call.rsplit("::", 1)[-1] in rig_names and call.rsplit("::", 1)[-1] not in GENERIC_HELPERS)
                    for call in test_calls
                )
            if not is_rigged:
                continue

            suite_key = f"{test.package}::{test.target}"
            suite = suites.get(suite_key)
            testcase = suite.get("testcases", {}).get(test.name) if suite else None
            relpath = test.source.relative_to(root).as_posix()
            if testcase is None and suite is not None and suite.get("status") == "skipped":
                # Nextest omits testcase rows when an entire binary is rejected by a
                # package/binary clause. The binary is still outside the preflight.
                continue
            if testcase is None:
                diagnostics.append(
                    f"cannot classify rig-dependent test {relpath}:{test.line} {test.name}: "
                    f"not present in nextest inventory {suite_key}"
                )
            elif testcase.get("filter-match", {}).get("status") == "matches":
                diagnostics.append(
                    f"rig-dependent test remains selected by portable preflight: "
                    f"{relpath}:{test.line} {test.name} (nextest {suite_key})"
                )
            elif testcase.get("filter-match", {}).get("status") != "mismatch":
                diagnostics.append(
                    f"cannot classify nextest selection for rig-dependent test "
                    f"{relpath}:{test.line} {test.name}: {testcase.get('filter-match')}"
                )
    return diagnostics


def main() -> int:
    try:
        problems = check_selection(ROOT)
    except (OSError, RuntimeError, json.JSONDecodeError) as error:
        print(f"portable-test-selection: {error}", file=sys.stderr)
        return 2
    if problems:
        print("portable-test-selection: rig tests must be excluded from the Linux preflight:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1
    print("portable-test-selection: every classified daemon/vault/mock test is excluded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
