#!/usr/bin/env python3
"""Build one workspace configuration, then check only the attested packages."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[2]
# Keep these identical to rust-test-gate.sh. Even doctests and clippy must select
# the workspace: a later `-p` invocation would resolve a different feature graph.
FEATURES = [
    "-F", "broca-provider/live", "-F", "broca-provider/test-harness",
    "-F", "broca-catalog/live", "-F", "broca-module-serve/test-clock",
]
FIXED = ["--workspace", "--locked", *FEATURES]


def cargo(*args, capture=False, env=None):
    command = [sys.executable, str(ROOT / "scripts/lib/cargo-watchdog.py"),
               "--step", "linux quickcheck cargo " + " ".join(args), "--", "cargo", *args]
    print("==> " + " ".join(["cargo", *args]), flush=True)
    result = subprocess.run(command, text=True, env=env,
                            stdout=subprocess.PIPE if capture else None)
    if result.returncode and result.stdout:
        print(result.stdout, file=sys.stderr)
    result.check_returncode()
    return result.stdout


def rows(filename):
    for line in (ROOT / "scripts" / filename).read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            yield line.split("|")


def test_filter(wanted):
    # nextest consumes the already-built binary inventory; filters never reach
    # Cargo's package selection or create another set of dependency artifacts.
    selections = [f"(package(={p}) & (kind(lib) | kind(bin)))" for p in sorted(wanted)]
    for package, target, *_ in rows("quickcheck-feature-tests.list"):
        if package in wanted:
            selections.append(f"(package(={package}) & binary(={target}))")
    for package, target, name, *_ in rows("quickcheck-guards.list"):
        if package in wanted:
            suffix = "" if name == "-" else f" & test({name})"
            selections.append(f"(package(={package}) & binary(={target}){suffix})")
    return " | ".join(selections)


def check_lints(output, wanted_ids):
    failures = 0
    for line in output.splitlines():
        message = json.loads(line)
        if message.get("reason") != "compiler-message":
            continue
        diagnostic = message["message"]
        # Capping lints at warn lets compilation finish even for an unselected
        # crate with deny-level lints. Rust compilation errors still fail Cargo.
        if diagnostic["level"] == "error" or (
                message["package_id"] in wanted_ids and diagnostic["level"] == "warning"):
            print(diagnostic.get("rendered") or diagnostic["message"], file=sys.stderr)
            failures += 1
    if failures:
        raise RuntimeError(f"{failures} selected clippy warnings or compiler errors")


def rustdoc():
    args = sys.argv[1:]
    if "--test" in args:
        sources = json.loads(os.environ["BROCA_ATTEST_DOC_SOURCES"])
        matches = [sources[str(Path(arg).resolve())] for arg in args
                   if str(Path(arg).resolve()) in sources]
        if len(matches) != 1:
            raise RuntimeError("cannot identify the doctest source package")
        if not matches[0]:
            return
    # Cargo must still resolve the entire workspace for doctests, but rustdoc
    # must not compile or execute examples belonging to unselected packages.
    subprocess.run([os.environ["BROCA_ATTEST_RUSTDOC"], *args], check=True)


def main(wanted):
    os.chdir(ROOT)
    os.environ["PATH"] = str(Path.home() / ".cargo/bin") + os.pathsep + os.environ["PATH"]
    os.environ.setdefault("CARGO_BUILD_JOBS", "2")
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    if not wanted:
        raise RuntimeError("at least one workspace package is required")
    metadata_text = cargo("metadata", "--locked", "--format-version", "1", *FEATURES, capture=True)
    metadata = json.loads(metadata_text)
    packages = {p["name"]: p for p in metadata["packages"] if p["id"] in metadata["workspace_members"]}
    unknown = wanted - packages.keys()
    if unknown:
        raise RuntimeError("unknown workspace packages: " + ", ".join(sorted(unknown)))
    selected = [arg for package in sorted(wanted) for arg in ("-p", package)]
    cargo("fmt", *selected, "--", "--check")

    # The inventory builds all test targets without listing or executing them.
    # Reuse it directly instead of asking Cargo to build the selected packages.
    with tempfile.TemporaryDirectory(prefix="broca-fixed-quickcheck-") as temporary:
        temporary = Path(temporary)
        binaries = temporary / "binaries.json"
        binaries.write_text(cargo("nextest", "list", *FIXED, "--lib", "--bins", "--tests",
                                  "--list-type", "binaries-only", "--message-format", "json", capture=True))
        metadata_path = temporary / "metadata.json"
        metadata_path.write_text(metadata_text)
        check_lints(cargo("clippy", *FIXED, "--all-targets", "--message-format=json", "--",
                          "--cap-lints", "warn", capture=True), {packages[p]["id"] for p in wanted})
        cargo("nextest", "run", "--cargo-metadata", str(metadata_path), "--binaries-metadata", str(binaries),
              "--ignore-default-filter", "--no-tests", "pass", "-E", test_filter(wanted))

    env = os.environ.copy()
    env["BROCA_ATTEST_RUSTDOC"] = env.get("RUSTDOC") or shutil.which("rustdoc") or "rustdoc"
    env["BROCA_ATTEST_DOC_SOURCES"] = json.dumps({
        str(Path(t["src_path"]).resolve()): p["name"] in wanted
        for p in packages.values() for t in p["targets"] if t.get("doctest")
    })
    env["RUSTDOC"] = str(Path(__file__).resolve())
    cargo("test", *FIXED, "--doc", "--quiet", env=env)
    for package, target, name, *_ in rows("quickcheck-guards.list"):
        if package == "python":
            subprocess.run([sys.executable, "-B", target, name], check=True)
    print("==> quickcheck OK: " + " ".join(sorted(wanted)), flush=True)


if __name__ == "__main__":
    try:
        if os.environ.get("BROCA_ATTEST_RUSTDOC"):
            rustdoc()
        else:
            main(set(sys.argv[1:]))
    except (RuntimeError, ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
        print(f"linux-quickcheck: {error}", file=sys.stderr)
        sys.exit(error.returncode if isinstance(error, subprocess.CalledProcessError) else 1)
