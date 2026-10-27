"""Compile real crate/API probes without parsing Rust source or adding workspace targets."""

import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
_BUILDS = {}


def build(package, *, features=(), release=False, codegen=False):
    key = (package, tuple(features), release, codegen)
    if key not in _BUILDS:
        command = ["cargo", "build" if codegen else "check", "--locked", "--lib",
                   "-p", package, "--message-format=json"]
        if features:
            command += ["--features", ",".join(features)]
        if release:
            command += ["--release"]
        result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=900)
        artifacts = []
        for line in result.stdout.splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("reason") == "compiler-artifact" and event["target"]["name"] == package.replace("-", "_"):
                artifacts += [Path(name) for name in event["filenames"]
                              if name.endswith(".rlib" if codegen else ".rmeta")]
        _BUILDS[key] = result, artifacts
    return _BUILDS[key]


def api_probe(package, source, *, features=(), release=False):
    result, artifacts = build(package, features=features, release=release)
    if result.returncode or len(artifacts) != 1:
        raise AssertionError(f"cannot build {package} for API probe:\n{result.stdout}\n{result.stderr}")
    scratch = ROOT / "target/scratch"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="api-probe-", dir=scratch) as tmp:
        path = Path(tmp) / "probe.rs"
        path.write_text(source)
        return subprocess.run(
            [os.environ.get("RUSTC", "rustc"), "--edition=2021", "--crate-type=lib",
             "--emit=metadata", "--crate-name=api_probe", str(path), "--out-dir", tmp,
             "--extern", f"{package.replace('-', '_')}={artifacts[0]}",
             "-L", f"dependency={artifacts[0].parent}"],
            cwd=ROOT, capture_output=True, text=True, timeout=60,
        )
