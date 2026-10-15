#!/usr/bin/env python3
"""Reject mutation platform claims whose named tests are compiled out.

Only broca's supported OSes (Linux, macOS and Windows) are implicit platforms.
This is a source check, not a replacement for compiling or replaying mutations.
Unknown cfg predicates and unsupported test runners fail closed.
"""

import argparse
import ast
from dataclasses import dataclass
from pathlib import Path
import re
import shlex
import sys
import tempfile
import tomllib
import unittest

PLATFORMS = ("linux", "macos", "windows")


@dataclass(frozen=True)
class Token:
    text: str
    line: int


def tokens(source):
    """Keep strings atomic so comments, braces and fake tests cannot become items."""
    result = []
    i, line = 0, 1
    pattern = re.compile(
        r'\s+|//[^\n]*|b?r\#*"|b?"(?:\\.|[^"\\])*"|'
        r"b?'(?:\\.|[^'\\])'|[A-Za-z_][A-Za-z_0-9]*|.", re.S
    )
    while i < len(source):
        start, start_line = i, line
        if source.startswith("/*", i):
            depth = 1
            i += 2
            while depth:
                if i >= len(source):
                    raise ValueError(f"unterminated comment at line {start_line}")
                if source.startswith("/*", i):
                    depth += 1
                    i += 2
                elif source.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
        else:
            match = pattern.match(source, i)
            if match is None:
                raise ValueError(f"cannot tokenize Rust at line {start_line}")
            text = match.group()
            i = match.end()
            if re.fullmatch(r'b?r\#*"', text):
                delimiter = '"' + text[text.index('r') + 1:-1]
                end = source.find(delimiter, i)
                if end < 0:
                    raise ValueError(f"unterminated raw string at line {start_line}")
                i = end + len(delimiter)
            if not text.isspace() and not text.startswith("//"):
                result.append(Token(source[start:i], start_line))
        line += source[start:i].count("\n")
    return result


def group(ts, start):
    pairs = {"(": ")", "[": "]", "{": "}"}
    stack = [pairs[ts[start].text]]
    i = start + 1
    while stack and i < len(ts):
        text = ts[i].text
        if text in pairs:
            stack.append(pairs[text])
        elif text in pairs.values():
            if text != stack.pop():
                raise ValueError(f"unbalanced Rust delimiters at line {ts[i].line}")
        i += 1
    if stack:
        raise ValueError(f"unclosed Rust delimiter at line {ts[start].line}")
    return ts[start + 1:i - 1], i


def split_args(ts):
    args, current, i = [], [], 0
    while i < len(ts):
        if ts[i].text in ("(", "[", "{"):
            _, end = group(ts, i)
            current.extend(ts[i:end])
            i = end
        elif ts[i].text == ",":
            args.append(current)
            current = []
            i += 1
        else:
            current.append(ts[i])
            i += 1
    if current:
        args.append(current)
    return args


def cfg(ts, platform, features):
    text = [t.text for t in ts]
    if len(text) == 1:
        if text[0] == "unix":
            return platform != "windows"
        if text[0] == "windows":
            return platform == "windows"
        if text[0] == "test":
            return True
    if len(text) == 3 and text[1] == "=":
        value = ast.literal_eval(text[2])
        if not isinstance(value, str):
            raise ValueError(f"cfg value must be a string: {text[2]}")
        if text[0] == "target_os":
            return platform == value
        if text[0] == "feature":
            return value in features
    if len(text) >= 3 and text[1] == "(":
        inner, end = group(ts, 1)
        if end == len(ts):
            # Evaluate every operand: short-circuiting would hide unknown predicates.
            values = [cfg(arg, platform, features) for arg in split_args(inner)]
            if text[0] == "all":
                return all(values)
            if text[0] == "any":
                return any(values)
            if text[0] == "not" and len(values) == 1:
                return not values[0]
    raise ValueError(f"cannot evaluate cfg({''.join(text)})")


@dataclass(frozen=True)
class Gate:
    path: Path
    line: int
    expression: tuple

    def evaluate(self, platform, features):
        try:
            return cfg(self.expression, platform, features)
        except (ValueError, SyntaxError) as error:
            raise ValueError(f"{self.path}:{self.line}: {error}") from error

    def describe(self):
        return f"{self.path}:{self.line}: cfg({''.join(t.text for t in self.expression)})"


def attribute_gates(attr, path):
    if not attr or attr[0].text not in ("cfg", "cfg_attr"):
        return []
    if len(attr) < 3 or attr[1].text != "(":
        raise ValueError(f"{path}:{attr[0].line}: malformed cfg attribute")
    inner, end = group(attr, 1)
    if end != len(attr):
        raise ValueError(f"{path}:{attr[0].line}: malformed cfg attribute")
    if attr[0].text == "cfg":
        return [Gate(path, attr[0].line, tuple(inner))]
    args = split_args(inner)
    if len(args) < 2:
        raise ValueError(f"{path}:{attr[0].line}: malformed cfg_attr")
    gates = []
    for nested in args[1:]:
        if nested[0].text == "path":
            raise ValueError(f"{path}:{attr[0].line}: conditional module paths are unsupported")
        for gate in attribute_gates(nested, path):
            # cfg_attr(P, cfg(Q)) permits the item exactly when not(P) or Q.
            expr = tokens("any(not(" + "".join(t.text for t in args[0]) + ")," +
                          "".join(t.text for t in gate.expression) + ")")
            gates.append(Gate(path, attr[0].line, tuple(expr)))
    return gates


def test_attributes(attrs, path):
    markers = []
    for attr in attrs:
        head = ''.join(t.text for t in attr).split('(', 1)[0]
        if head in ("test", "tokio::test"):
            return []
        if attr and attr[0].text == "cfg_attr":
            inner, _ = group(attr, 1)
            args = split_args(inner)
            if len(args) >= 2:
                nested = test_attributes(args[1:], path)
                if nested is not None:
                    expression = "".join(t.text for t in args[0])
                    for gate in nested:
                        expression = "all(" + expression + "," + "".join(t.text for t in gate.expression) + ")"
                    markers.append((attr[0].line, expression))
    if markers:
        expr = tokens("any(" + ",".join(expression for _, expression in markers) + ")")
        return [Gate(path, markers[0][0], tuple(expr))]
    return None


@dataclass
class Test:
    path: Path
    line: int
    gates: list


class RustTests:
    def __init__(self, root):
        self.tests = {}
        self.stack = []
        self.file(root, (), [], root.parent)

    def file(self, path, names, gates, module_dir):
        if path in self.stack:
            raise ValueError(f"recursive module inclusion: {path}")
        self.stack.append(path)
        try:
            self.scope(tokens(path.read_text(encoding="utf-8")), path, names, gates, module_dir, path.parent)
        finally:
            self.stack.pop()

    def scope(self, ts, path, names, inherited, module_dir, path_dir):
        attrs, items, inner_gates, macros, i = [], [], [], {}, 0
        while i < len(ts):
            if ts[i].text == "#":
                j = i + 1
                inner = ts[j].text == "!"
                j += int(inner)
                if ts[j].text != "[":
                    raise ValueError(f"{path}:{ts[i].line}: malformed attribute")
                attr, i = group(ts, j)
                if inner:
                    inner_gates.extend(attribute_gates(attr, path))
                else:
                    attrs.append(attr)
                continue
            if ts[i].text == "macro_rules" and ts[i + 1].text == "!":
                macro_name = ts[i + 2].text
                body, i = group(ts, i + 3)
                macros[macro_name] = (body, attrs)
                attrs = []
                continue
            if i + 2 < len(ts) and ts[i + 1].text == "!" and ts[i + 2].text in ("(", "[", "{"):
                body, end = group(ts, i + 2)
                items.append(("macro", ts[i], attrs, body))
                attrs = []
                i = end
                continue
            # Only inspect items in module scopes, never function bodies.
            j = i
            while j < len(ts) and ts[j].text not in ("fn", "mod", ";", "{"):
                if ts[j].text in ("(", "["):
                    _, j = group(ts, j)
                else:
                    j += 1
            kind = ts[j].text if j < len(ts) else ""
            if kind in ("fn", "mod"):
                name = ts[j + 1]
                j += 2
            else:
                name = None
            while j < len(ts) and ts[j].text not in (";", "{"):
                if ts[j].text in ("(", "["):
                    _, j = group(ts, j)
                else:
                    j += 1
            body = None
            if j < len(ts) and ts[j].text == "{":
                body, end = group(ts, j)
            else:
                end = j + 1
            items.append((kind, name, attrs, body))
            attrs = []
            i = end
        gates = inherited + inner_gates
        for kind, name, attrs, body in items:
            item_gates = gates + [g for attr in attrs for g in attribute_gates(attr, path)]
            if kind == "fn":
                marker_gates = test_attributes(attrs, path)
                if marker_gates is not None:
                    full_name = "::".join((*names, name.text))
                    self.tests.setdefault(full_name, []).append(Test(path, name.line, item_gates + marker_gates))
            elif kind == "macro":
                if name.text == "include":
                    if len(body) != 1 or not body[0].text.startswith('"'):
                        raise ValueError(f"{path}:{name.line}: non-literal include! is unsupported")
                    child = path.parent / ast.literal_eval(body[0].text)
                    self.file(child, names, item_gates, module_dir)
                elif name.text in macros:
                    template, macro_attrs = macros[name.text]
                    if any(t.text in ("test", "mod", "include") for t in template):
                        expanded = expand_test_macro(template, body, path, name.line)
                        macro_gates = [g for attr in macro_attrs for g in attribute_gates(attr, path)]
                        self.scope(expanded, path, names, item_gates + macro_gates, module_dir, path_dir)
            elif kind == "mod":
                child_names = (*names, name.text)
                if body is not None:
                    self.scope(body, path, child_names, item_gates, module_dir / name.text,
                               module_dir / name.text)
                else:
                    paths = [attr for attr in attrs if attr and attr[0].text == "path"]
                    if paths:
                        attr = paths[0]
                        if len(paths) != 1 or len(attr) != 3 or attr[1].text != "=":
                            raise ValueError(f"{path}:{name.line}: unsupported module path")
                        child = path_dir / ast.literal_eval(attr[2].text)
                    else:
                        candidates = [module_dir / (name.text + ".rs"), module_dir / name.text / "mod.rs"]
                        existing = [p for p in candidates if p.is_file()]
                        if len(existing) != 1:
                            raise ValueError(f"{path}:{name.line}: cannot resolve module {name.text}")
                        child = existing[0]
                    child_dir = child.parent if paths or child.name == "mod.rs" else child.with_suffix("")
                    self.file(child, child_names, item_gates, child_dir)


def expand_test_macro(template, arguments, path, line):
    """Expand the single-arm ident/literal test macros used by capture fixtures."""
    matcher, end = group(template, 0)
    if [t.text for t in template[end:end + 3]] != ["=", ">", "{"]:
        raise ValueError(f"{path}:{line}: unsupported test macro arm")
    expansion, end = group(template, end + 2)
    if any(t.text != ";" for t in template[end:]):
        raise ValueError(f"{path}:{line}: multiple test macro arms are unsupported")
    patterns, values = split_args(matcher), split_args(arguments)
    if len(patterns) != len(values):
        raise ValueError(f"{path}:{line}: test macro argument count mismatch")
    bindings = {}
    for pattern, value in zip(patterns, values):
        texts = [t.text for t in pattern]
        if len(texts) != 4 or texts[0] != "$" or texts[2] != ":" or texts[3] not in ("ident", "literal") or len(value) != 1:
            raise ValueError(f"{path}:{line}: unsupported test macro matcher")
        bindings[texts[1]] = value[0]
    result, i = [], 0
    while i < len(expansion):
        if expansion[i].text == "$":
            if i + 1 >= len(expansion) or expansion[i + 1].text not in bindings:
                raise ValueError(f"{path}:{line}: unsupported test macro substitution")
            result.append(bindings[expansion[i + 1].text])
            i += 2
        else:
            result.append(expansion[i])
            i += 1
    return result


def enabled_features(manifest, requested, defaults=True, all_features=False):
    definitions = dict(manifest.get("features", {}))
    suppressed = {value[4:] for values in definitions.values() for value in values if value.startswith("dep:")}
    for name, dependency in manifest.get("dependencies", {}).items():
        if isinstance(dependency, dict) and dependency.get("optional") and name not in suppressed:
            definitions.setdefault(name, [])
    active, pending = set(), list(definitions) if all_features else [*requested]
    if defaults and "default" in definitions:
        pending.append("default")
    for feature in list(pending):
        if "/" in feature:
            dependency, _ = feature.split("/", 1)
            if dependency == manifest["package"]["name"]:
                pending[pending.index(feature)] = feature.split("/", 1)[1]
            elif dependency in {**manifest.get("dependencies", {}), **manifest.get("dev-dependencies", {})}:
                pending.remove(feature)
            else:
                raise ValueError(f"unknown dependency feature {feature}")
    while pending:
        name = pending.pop()
        if name in active:
            continue
        if name not in definitions:
            raise ValueError(f"unknown package feature {name}")
        active.add(name)
        for value in definitions[name]:
            if value in definitions:
                pending.append(value)
            elif not value.startswith("dep:") and "/" not in value:
                # Optional dependencies define implicit features unless dep: suppresses them.
                dependency = manifest.get("dependencies", {}).get(value, {})
                if not isinstance(dependency, dict) or not dependency.get("optional"):
                    raise ValueError(f"cannot resolve feature {name} -> {value}")
                active.add(value)
    return active


def target_root(manifest, directory, target):
    parts = shlex.split(target)
    if parts == ["--lib"]:
        entry = manifest.get("lib", {})
        path = entry.get("path", "src/lib.rs")
    elif len(parts) == 2 and parts[0] in ("--test", "--bin"):
        kind, name = parts[0][2:], parts[1]
        entries = [entry for entry in manifest.get(kind, []) if entry.get("name") == name]
        if len(entries) > 1:
            raise ValueError(f"duplicate Cargo {kind} target {name}")
        entry = entries[0] if entries else {}
        if kind == "test":
            candidates = [f"tests/{name}.rs", f"tests/{name}/main.rs"]
            if not entries and not manifest["package"].get("autotests", True):
                raise ValueError(f"Cargo.toml disables automatic test target {name}")
        else:
            candidates = (["src/main.rs"] if name == manifest["package"]["name"] else
                          [f"src/bin/{name}.rs", f"src/bin/{name}/main.rs"])
            if not entries and not manifest["package"].get("autobins", True):
                raise ValueError(f"Cargo.toml disables automatic bin target {name}")
        existing = [p for p in candidates if (directory / p).is_file()]
        path = entry.get("path") or (existing[0] if len(existing) == 1 else None)
        if path is None:
            raise ValueError(f"cannot resolve Cargo target {target}")
    else:
        raise ValueError(f"unsupported Cargo test target {target!r}")
    if entry.get("harness", True) is not True or entry.get("test", True) is not True:
        raise ValueError(f"Cargo target {target} does not use the test harness")
    return directory / path, entry.get("required-features", [])


def target_field_line(path, target, field):
    kind = shlex.split(target)[0][2:]
    lines = path.read_text(encoding="utf-8").splitlines()
    headers = [i for i, line in enumerate(lines) if re.match(r'^\s*\[\[?[^\]"\n]+\]\]?\s*(?:#.*)?$', line)]
    for start, end in zip(headers, [*headers[1:], len(lines)]):
        header = lines[start].strip()
        if header not in (f"[[{kind}]]", f"[{kind}]"):
            continue
        block = tomllib.loads("\n".join(lines[start:end]))[kind]
        entry = block[0] if isinstance(block, list) else block
        if kind != "lib" and entry.get("name") != shlex.split(target)[1]:
            continue
        for i in range(start + 1, end):
            if re.match(rf"\s*{re.escape(field)}\s*=", lines[i]):
                return i + 1
    raise ValueError(f"cannot locate {field} for Cargo target {target} in {path}")


def python_tests(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    tests = {}
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            for method in node.body:
                if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    tests[f"{node.name}.{method.name}"] = method.lineno
    return tests


def command_args(row):
    command = row.get("command", [])
    if command[:3] == ["python3", "-B", "scripts/mutation-bash.py"]:
        command = ["bash", *command[3:]]
    if command[:2] == ["bash", "-c"]:
        # These wrappers print the diff before one test command; arbitrary shell
        # logic is rejected rather than guessing which test it will execute.
        if len(command) != 5 or command[3:] != ["--", "{test}"]:
            raise ValueError("unsupported test shell wrapper")
        prefix = "git diff --stat; "
        if not command[2].startswith(prefix):
            raise ValueError("unsupported test shell wrapper")
        command = shlex.split(command[2][len(prefix):])
        # Leading NAME=value assignments only set the environment (for example
        # BROCA_REQUIRE_SUBC=1); they do not change which test runs.
        while command and re.fullmatch(r"[A-Z_][A-Z0-9_]*=\S*", command[0]):
            command = command[1:]
    if command[:1] == ["python3"] and "-c" in command:
        code = ast.parse(command[command.index("-c") + 1])
        calls = [node for node in ast.walk(code) if isinstance(node, ast.Call) and
                 isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name) and
                 node.func.value.id == "subprocess" and node.func.attr == "run"]
        if calls:
            if len(calls) != 1 or not calls[0].args or not isinstance(calls[0].args[0], ast.List):
                raise ValueError("unsupported Python cargo wrapper")
            command = []
            for node in calls[0].args[0].elts:
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    command.append(node.value)
                elif ast.unparse(node) == "sys.argv[1]":
                    command.append("{test}")
                else:
                    raise ValueError("non-literal Python cargo argument")
    return command


def cargo_invocation(row):
    if row.get("runner", "cargo") == "cargo":
        return row["package"], row["target"], row.get("features", []), True, False
    if row.get("runner") != "command":
        raise ValueError(f"unsupported test runner {row.get('runner')}")
    command = command_args(row)
    if command[:2] != ["cargo", "test"]:
        return None
    package, target, features, defaults, all_features = None, None, [], True, False
    i = 2
    while i < len(command):
        arg = command[i]
        if arg == "--":
            if command[i + 1:] != ["--exact"]:
                raise ValueError("unsupported libtest arguments in command row")
            break
        if arg in ("-p", "--package", "--features", "-F", "--test", "--bin"):
            value = command[i + 1]
            if arg in ("-p", "--package"):
                package = value
            elif arg in ("--features", "-F"):
                features.extend(re.split(r"[\s,]+", value))
            else:
                if target is not None:
                    raise ValueError("multiple cargo test targets")
                target = f"{arg} {value}"
            i += 2
        elif arg == "--lib":
            if target is not None:
                raise ValueError("multiple cargo test targets")
            target = arg
            i += 1
        elif arg == "--no-default-features":
            defaults = False
            i += 1
        elif arg == "--all-features":
            all_features = True
            i += 1
        elif arg in ("--locked", "{test}", "$1"):
            i += 1
        else:
            raise ValueError(f"unsupported cargo test argument {arg!r}")
    if package is None or target is None:
        raise ValueError("cargo command requires one package and one test target")
    features = [f.removeprefix(package + "/") for f in features]
    return package, target, features, defaults, all_features


def verify_catalogue(root, catalogue):
    root = Path(root)
    manifests = {}
    for path in (root / "crates").glob("*/Cargo.toml"):
        manifest = tomllib.loads(path.read_text(encoding="utf-8"))
        manifests[manifest["package"]["name"]] = (path, manifest)
    controls = catalogue.get("control")
    if not isinstance(controls, list) or not controls:
        return ["catalogue must contain non-empty [[control]] rows"], 0
    errors, cache, count = [], {}, 0
    for row in controls:
        row_id = row["id"]
        named = row.get("expect_red", [])
        test_label = ", ".join(map(str, named)) if isinstance(named, list) else repr(named)
        try:
            if not isinstance(named, list) or not named or not all(isinstance(name, str) for name in named):
                raise ValueError("expected test names must be a non-empty string list")
            platforms = row.get("platforms", list(PLATFORMS))
            if not platforms or any(p not in PLATFORMS for p in platforms):
                raise ValueError(f"unsupported platforms {platforms!r}")
            invocation = cargo_invocation(row)
            if invocation is None:
                path = root / row["test_file"]
                command = command_args(row)
                python_wrapper = False
                if command[:1] == ["python3"] and "-c" in command:
                    code = ast.parse(command[command.index("-c") + 1])
                    python_wrapper = any(isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                                         and node.func.attr == "spec_from_file_location" and len(node.args) == 2
                                         and isinstance(node.args[1], ast.Constant) and node.args[1].value == row["test_file"]
                                         for node in ast.walk(code))
                if path.suffix != ".py" or not command or command[0] != "python3" or not (row["test_file"] in command or python_wrapper):
                    raise ValueError(f"unsupported command test runner: {command!r}")
                if path not in cache:
                    cache[path] = python_tests(path)
                for name in named:
                    if name not in cache[path]:
                        raise ValueError(f"test {name}: not found in {path}")
                count += len(named) * len(platforms)
                continue
            package, target_arg, requested, defaults, all_features = invocation
            manifest_path, manifest = manifests[package]
            features = enabled_features(manifest, requested, defaults, all_features)
            target, required = target_root(manifest, manifest_path.parent, target_arg)
            missing = set(required) - features
            if missing:
                line = target_field_line(manifest_path, target_arg, "required-features")
                raise ValueError(f"tests {', '.join(named)}: {manifest_path}:{line}: target requires features {sorted(missing)}")
            if target not in cache:
                cache[target] = RustTests(target).tests
            for name in named:
                definitions = cache[target].get(name, [])
                if not definitions and target_arg.startswith("--test "):
                    # Some catalogue names prepend the integration target. An actual
                    # module with that name takes precedence over the redundant prefix.
                    prefix = target_arg.split()[1] + "::"
                    if name.startswith(prefix):
                        definitions = cache[target].get(name.removeprefix(prefix), [])
                if not definitions:
                    raise ValueError(f"test {name}: not found in Cargo target {target}")
                for platform in platforms:
                    failures, compiled = [], False
                    for definition in definitions:
                        excluded = []
                        for gate in definition.gates:
                            if not gate.evaluate(platform, features):
                                excluded.append(gate.describe())
                        compiled |= not excluded
                        failures.extend(excluded)
                    if not compiled:
                        errors.append(f"{row_id}: test {name} does not compile on {platform}: " + "; ".join(dict.fromkeys(failures)))
                    count += 1
        except (ValueError, KeyError, OSError, SyntaxError, StopIteration, IndexError) as error:
            errors.append(f"{row_id}: tests {test_label}: {error}")
    return errors, count


class PlatformTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.manifest = ' [package]\nname = "probe"\nversion = "0.1.0"\n'
        self.put("crates/probe/Cargo.toml", self.manifest)
        self.put("crates/probe/src/lib.rs", "mod sample { #[test] fn probe() {} }\n")
        self.row = {"id": "platform-probe", "runner": "cargo", "package": "probe",
                    "target": "--lib", "test_file": "crates/probe/src/lib.rs",
                    "expect_red": ["sample::probe"]}

    def put(self, path, text):
        destination = self.root / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(text, encoding="utf-8")

    def check(self):
        return verify_catalogue(self.root, {"control": [self.row]})[0]

    def test_portable_test_accepts_all_supported_platforms(self):
        self.assertEqual(self.check(), [])
        self.row["platforms"] = ["windows"]
        self.assertEqual(self.check(), [])

    def test_rejects_gated_function_with_row_test_and_line(self):
        self.put("crates/probe/src/lib.rs", 'mod sample {\n#[cfg(unix)]\n#[test] fn probe() {}\n}\n')
        errors = self.check()
        self.assertEqual(len(errors), 1)
        self.assertIn("platform-probe: test sample::probe does not compile on windows", errors[0])
        self.assertIn("crates/probe/src/lib.rs:2: cfg(unix)", errors[0])
        self.row["platforms"] = ["linux", "macos"]
        self.assertEqual(self.check(), [])

    def test_enclosing_inline_module_gate(self):
        self.put("crates/probe/src/lib.rs", '#[cfg(windows)]\nmod sample { #[tokio::test] async fn probe() {} }')
        self.assertEqual(len(self.check()), 2)
        self.row["platforms"] = ["windows"]
        self.assertEqual(self.check(), [])

    def test_file_and_external_module_declaration_gates_are_both_checked(self):
        self.put("crates/probe/src/lib.rs", '#[cfg(not(windows))]\nmod sample;')
        self.put("crates/probe/src/sample.rs", '#![cfg(target_os = "macos")]\n#[test] fn probe() {}')
        errors = self.check()
        self.assertEqual(len(errors), 2)
        self.assertIn("crates/probe/src/sample.rs:1:", errors[0])
        self.assertIn("crates/probe/src/lib.rs:1:", errors[1])
        self.row["platforms"] = ["macos"]
        self.assertEqual(self.check(), [])

    def test_root_crate_gate_and_explicit_target_path(self):
        self.put("crates/probe/Cargo.toml", self.manifest + '\n[[test]]\nname = "contract"\npath = "proofs/contract.rs"\n')
        self.put("crates/probe/proofs/contract.rs", '#![cfg(unix)]\n#[test] fn probe() {}')
        self.row.update(target="--test contract", expect_red=["probe"])
        self.assertEqual(len(self.check()), 1)
        self.assertIn("proofs/contract.rs:1: cfg(unix)", self.check()[0])
        self.row["expect_red"] = ["contract::probe"]
        self.assertEqual(len(self.check()), 1)

    def test_path_override_inline_paths_and_include_preserve_gates(self):
        self.put("crates/probe/src/lib.rs", '#[path = "alternate.rs"] mod outer;')
        self.put("crates/probe/src/alternate.rs", 'mod sample { #[path = "actual.rs"] mod inner; }')
        self.put("crates/probe/src/sample/actual.rs", '#![cfg(unix)]\ninclude!("body.rs");')
        self.put("crates/probe/src/sample/body.rs", '#[test] fn probe() {}')
        self.row["expect_red"] = ["outer::sample::inner::probe"]
        self.assertEqual(len(self.check()), 1)
        self.assertIn("actual.rs:1: cfg(unix)", self.check()[0])

    def test_nested_default_module_directories(self):
        self.put("crates/probe/src/lib.rs", 'mod sample;')
        self.put("crates/probe/src/sample.rs", 'mod inner;')
        self.put("crates/probe/src/sample/inner/mod.rs", '#[test] fn probe() {}')
        self.row["expect_red"] = ["sample::inner::probe"]
        self.assertEqual(self.check(), [])

    def test_all_any_not_target_os_and_feature_closure(self):
        self.put("crates/probe/Cargo.toml", self.manifest + '\n[features]\ndefault = ["bundle"]\nbundle = ["live"]\nlive = []\n')
        self.put("crates/probe/src/lib.rs", '#[cfg(all(feature = "live", any(target_os = "linux", not(unix))))]\nmod sample { #[test] fn probe() {} }')
        errors = self.check()
        self.assertEqual(len(errors), 1)
        self.assertIn("on macos", errors[0])
        self.row["platforms"] = ["linux", "windows"]
        self.assertEqual(self.check(), [])

    def test_missing_requested_feature_excludes_test_not_silently_ignored(self):
        self.put("crates/probe/Cargo.toml", self.manifest + '\n[features]\nlive = []\n')
        self.put("crates/probe/src/lib.rs", '#[cfg(feature = "live")]\nmod sample { #[test] fn probe() {} }')
        self.assertEqual(len(self.check()), 3)
        self.row["features"] = ["live"]
        self.assertEqual(self.check(), [])

    def test_required_features_with_multiple_explicit_test_targets(self):
        manifest = self.manifest + '''
[features]
live = []
[[test]]
name = "other"
path = "proofs/contract.rs"
required-features = ["live"]
[[test]]
name = "contract"
path = "proofs/contract.rs"
required-features = [
    "live",
]
'''
        self.put("crates/probe/Cargo.toml", manifest)
        self.put("crates/probe/proofs/contract.rs", '#[test] fn probe() {}')
        self.row.update(target="--test contract", expect_red=["probe"])
        errors = self.check()
        self.assertEqual(len(errors), 1)
        self.assertIn("Cargo.toml:14: target requires features ['live']", errors[0])
        self.row["features"] = ["live"]
        self.assertEqual(self.check(), [])

    def test_unknown_predicates_fail_even_inside_short_circuited_any(self):
        self.put("crates/probe/src/lib.rs", '#[cfg(any(unix, target_arch = "x86_64"))]\nmod sample { #[test] fn probe() {} }')
        errors = self.check()
        self.assertEqual(len(errors), 1)
        self.assertIn('platform-probe: tests sample::probe:', errors[0])
        self.assertIn('lib.rs:1: cannot evaluate cfg(target_arch="x86_64")', errors[0])

    def test_empty_catalogue_and_test_lists_fail_closed(self):
        self.assertEqual(verify_catalogue(self.root, {})[0], ["catalogue must contain non-empty [[control]] rows"])
        self.row["expect_red"] = []
        self.assertIn("expected test names must be a non-empty string list", self.check()[0])

    def test_missing_tests_unknown_features_and_platforms_fail_closed(self):
        self.row["expect_red"] = ["sample::missing"]
        self.assertIn("not found", self.check()[0])
        self.row["features"] = ["undeclared"]
        self.assertIn("unknown package feature", self.check()[0])
        self.row["platforms"] = ["plan9"]
        self.assertIn("unsupported platforms", self.check()[0])

    def test_cfg_attr_conditional_gate_and_test_marker(self):
        self.put("crates/probe/src/lib.rs", 'mod sample {\n#[cfg_attr(windows, cfg(unix))]\n#[test] fn probe() {}\n}')
        self.assertEqual(len(self.check()), 1)
        self.put("crates/probe/src/lib.rs", 'mod sample {\n#[cfg_attr(unix, test)]\nfn probe() {}\n}')
        self.assertEqual(len(self.check()), 1)
        self.assertIn("on windows", self.check()[0])

    def test_strings_comments_and_function_bodies_cannot_plant_fake_tests(self):
        self.put("crates/probe/src/lib.rs", '''
// #[cfg(windows)] mod sample { #[test] fn fake() {} }
/* /* nested */ #[cfg(windows)] */
const SOURCE: &[u8] = br##"a quote: " #[cfg(windows)] mod sample {}"##;
mod sample { #[test] fn probe() { let s = "#[cfg(windows)]"; } }
''')
        self.assertEqual(self.check(), [])

    def test_macro_generated_tests_keep_definition_and_invocation_gates(self):
        self.put("crates/probe/src/lib.rs", '''
mod sample {
macro_rules! case { ($name:ident, $path:literal) => { #[test] fn $name() {} }; }
#[cfg(unix)] case!(probe, "fixture");
}
''')
        self.assertEqual(len(self.check()), 1)
        self.assertIn("lib.rs:4: cfg(unix)", self.check()[0])

    def test_cargo_command_feature_options_and_shell_wrappers(self):
        self.put("crates/probe/Cargo.toml", self.manifest + '\n[features]\ndefault = ["live"]\nlive = []\n')
        self.put("crates/probe/src/lib.rs", '#[cfg(feature = "live")] mod sample { #[test] fn probe() {} }')
        self.row.update(runner="command", command=["cargo", "test", "--locked", "-p", "probe", "--no-default-features", "--lib", "{test}", "--", "--exact"])
        self.assertEqual(len(self.check()), 3)
        self.row["command"] = ["bash", "-c", 'git diff --stat; cargo test --locked -p probe -F probe/live --lib "$1" -- --exact', "--", "{test}"]
        self.assertEqual(self.check(), [])
        self.row["command"] = ["cargo", "test", "--locked", "-p", "probe", "--all-features", "--lib", "{test}", "--", "--exact"]
        self.assertEqual(self.check(), [])

    def test_python_subprocess_cargo_wrapper_checks_real_rust_target(self):
        self.put("crates/probe/src/lib.rs", '#![cfg(unix)]\nmod sample { #[test] fn probe() {} }')
        self.row.update(runner="command", command=["python3", "-c", "import subprocess, sys; subprocess.run(['cargo', 'test', '--locked', '-p', 'probe', '--lib', sys.argv[1], '--', '--exact'])", "{test}"])
        self.assertEqual(len(self.check()), 1)
        self.assertIn("lib.rs:1: cfg(unix)", self.check()[0])

    def test_native_bash_launcher_checks_underlying_target_gates(self):
        self.put("crates/probe/src/lib.rs", '#![cfg(unix)]\nmod sample { #[test] fn probe() {} }')
        self.row.update(runner="command", command=["python3", "-B", "scripts/mutation-bash.py", "-c",
                        'git diff --stat; cargo test --locked -p probe --lib "$1" -- --exact', "--", "{test}"])
        self.assertEqual(len(self.check()), 1)
        self.assertIn("lib.rs:1: cfg(unix)", self.check()[0])

    def test_python_tests_are_checked_and_unsupported_commands_refuse(self):
        self.put("proof.py", 'class Proof:\n    def test_probe(self):\n        pass\n')
        self.row.update(runner="command", test_file="proof.py", expect_red=["Proof.test_probe"], command=["python3", "-B", "proof.py", "{test}"])
        self.assertEqual(self.check(), [])
        self.row["command"] = ["magic-runner", "proof.py"]
        self.assertIn("unsupported command test runner", self.check()[0])


class CatalogueTests(unittest.TestCase):
    root = Path(__file__).resolve().parent.parent

    def test_corrected_catalogue(self):
        catalogue = tomllib.loads((self.root / "mutations.toml").read_text(encoding="utf-8"))
        errors, count = verify_catalogue(self.root, catalogue)
        self.assertEqual(errors, [])
        self.assertGreater(count, 2000)

    def test_live_file_anchor_proof_cannot_claim_windows(self):
        rows = {"wal-anchored-damaged-length-before-record"}
        catalogue = tomllib.loads((self.root / "mutations.toml").read_text(encoding="utf-8"))
        for row in catalogue["control"]:
            if row["id"] in rows:
                row.pop("platforms")
        errors, _ = verify_catalogue(self.root, catalogue)
        self.assertEqual({error.split(":", 1)[0] for error in errors}, rows)
        self.assertEqual(len(errors), 1)
        for error in errors:
            self.assertIn("does not compile on windows", error)
            self.assertIn("cfg(unix)", error)

    def test_six_retention_platform_reversions_in_fixture_copy_are_rejected(self):
        rows = {
            "retention-reader-boot-enrollment", "retention-writer-admission",
            "retention-own-tombstone-before-older",
            "retention-tombstone-never-shadows-fresh-lineage-real-daemon",
            "retention-real-daemon-marker-title", "retention-shared-clock-advances-daemon",
        }
        text = (self.root / "mutations.toml").read_text(encoding="utf-8")
        # Drop each named row's platforms line, wherever it sits in that row's block.
        blocks = re.split(r"(?m)^(?=\[\[control\]\]\s*$)", text)
        for i, block in enumerate(blocks):
            match = re.search(r'(?m)^id\s*=\s*"([^"]+)"', block)
            if match and match.group(1) in rows:
                stripped = re.sub(r'(?m)^platforms\s*=\s*\["linux", "macos"\]\n', "", block, count=1)
                self.assertNotEqual(stripped, block, match.group(1))
                blocks[i] = stripped
        text = "".join(blocks)
        with tempfile.TemporaryDirectory() as scratch:
            copy = Path(scratch) / "mutations.toml"
            copy.write_text(text, encoding="utf-8")
            errors, _ = verify_catalogue(self.root, tomllib.loads(copy.read_text(encoding="utf-8")))
        self.assertEqual({error.split(":", 1)[0] for error in errors}, rows)
        self.assertEqual(len(errors), 6)
        for error in errors:
            self.assertIn("does not compile on windows", error)
            self.assertIn("llm_runner_conformance.rs:21: cfg(unix)", error)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalogue", default="mutations.toml")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    args = parser.parse_args()
    catalogue = tomllib.loads(Path(args.catalogue).read_text(encoding="utf-8"))
    errors, count = verify_catalogue(args.root, catalogue)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        raise SystemExit(1)
    print(f"Mutation platforms: {len(catalogue.get('control', []))} rows, {count} test/platform claims checked.")


if __name__ == "__main__":
    if sys.argv[1:2] == ["--self-test"]:
        unittest.main(argv=[sys.argv[0], *sys.argv[2:]], verbosity=2)
    else:
        main()
