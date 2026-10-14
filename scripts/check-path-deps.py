#!/usr/bin/env python3
"""Reject resolved or declared Cargo path dependencies outside the repository."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys


def inside(path, root):
    try:
        Path(path).resolve().relative_to(root)
        return True
    except ValueError:
        return False


# cargo holds the machine-wide package-cache lock while it resolves, so a stalled
# fetch blocks every other cargo command on the host. Bound each attempt.
METADATA_TIMEOUT_SECS = 300


def run_metadata(manifest, offline):
    command = ["cargo", "metadata", "--locked", "--all-features", "--format-version", "1",
               "--manifest-path", str(manifest)]
    if offline:
        command.insert(2, "--offline")
    return subprocess.run(command, capture_output=True, text=True, timeout=METADATA_TIMEOUT_SECS)


def check(manifest, root):
    root = root.resolve()
    # Offline first: a warm cache answers without touching the network or holding the
    # cache lock while waiting on it. A fresh machine (CI) falls back to fetching.
    try:
        result = run_metadata(manifest, offline=True)
        if result.returncode:
            result = run_metadata(manifest, offline=False)
    except subprocess.TimeoutExpired:
        print(f"path dependency check itself failed: cargo metadata did not finish within "
              f"{METADATA_TIMEOUT_SECS}s", file=sys.stderr)
        return 2
    if result.returncode:
        sys.stderr.write(result.stderr)
        print("path dependency check itself failed: cargo metadata failed", file=sys.stderr)
        return 2
    metadata = json.loads(result.stdout)
    packages = metadata["packages"]
    path_count = sum(package["source"] is None for package in packages)
    if not packages or not path_count:
        print("path dependency check itself failed: no local packages checked", file=sys.stderr)
        return 2
    print(f"path dependency check: {len(packages)} packages checked ({path_count} local)")
    violations = set()
    # Cargo omits unused patches from packages but reports their source in its
    # metadata diagnostics. Check those too, so an inactive patch cannot hide an
    # external checkout until a later dependency change activates it.
    for name, path in re.findall(r"[Pp]atch `([^` ]+) v[^`]+ \(([^\n]+)\)` was not used", result.stderr):
        if Path(path).is_absolute() and not inside(path, root):
            violations.add((name, path))
    for package in packages:
        # Registry/git cache directories are external by design, not path sources.
        if package["source"] is None and not inside(package["manifest_path"], root):
            violations.add((package["name"], str(Path(package["manifest_path"]).parent)))
        # Metadata also exposes optional/target-specific declarations, even when
        # the dependency is absent from the resolved graph on the current host.
        for dependency in package["dependencies"]:
            path = dependency.get("path")
            if path and package["source"] is None and not inside(path, root):
                violations.add((dependency["name"], path))
    if violations:
        for name, path in sorted(violations):
            print(f"external path dependency: {name}: {path}", file=sys.stderr)
        return 3
    print("path dependency check: no external paths")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--manifest-path", type=Path)
    args = parser.parse_args()
    root = args.repo_root.resolve()
    try:
        return check(args.manifest_path or root / "Cargo.toml", root)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"path dependency check itself failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
