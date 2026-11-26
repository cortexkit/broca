#!/usr/bin/env python3
"""Validate public release inputs and inventory; do not mutate Git or GitHub."""

import hashlib
import json
from pathlib import Path
import re
import shutil
import sys
import tomllib
import zipfile


VERSION = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
WINDOWS_NOTE = "Windows: release incomplete (native suite has not run)."
ARCHIVES = {
    f"{binary}-{os}-{arch}.zip"
    for binary in ("ck-broca", "broca-session")
    for os in ("darwin", "linux", "windows")
    for arch in ("arm64", "x64")
}
ASSETS = ARCHIVES | {name + ".sha256" for name in ARCHIVES} | {"release-manifest.json"}


def version_key(value):
    if not isinstance(value, str) or not VERSION.fullmatch(value):
        raise ValueError(f"expected X.Y.Z without leading zeroes, got {value!r}")
    return tuple(map(int, value.split(".")))


def inputs(version):
    version_key(version)
    notes = Path(f".cortexkit/release-notes/v{version}.md")
    body = notes.read_text() if notes.is_file() else ""
    if not body.strip():
        raise ValueError(f"missing or empty release notes: {notes}")
    if WINDOWS_NOTE not in body.splitlines():
        raise ValueError(f"{notes} must contain the line: {WINDOWS_NOTE}")
    manifest = json.loads(Path("release-manifest.json").read_text())
    if not isinstance(manifest, dict):
        raise ValueError("release-manifest.json must be a JSON object")
    floor = manifest.get("requires_core")
    try:
        version_key(floor)
    except ValueError as error:
        raise ValueError(f"release-manifest.json requires_core: {error}") from error
    windows = manifest.get("windows", {})
    if (not isinstance(windows, dict) or windows.get("status") != "release incomplete"
            or windows.get("native_suite_passed") is not False):
        raise ValueError("release-manifest.json must mark Windows release incomplete until its native suite runs")
    print(floor)


def workspace_version(version):
    version_key(version)
    package = tomllib.loads(Path("crates/broca-module-serve/Cargo.toml").read_text())["package"]
    resolved = package["version"]
    if resolved == {"workspace": True}:
        resolved = tomllib.loads(Path("Cargo.toml").read_text())["workspace"]["package"]["version"]
    if resolved != version:
        raise ValueError(f"broca-module-serve version is {resolved}, not {version}")


def release_available(version, pages):
    proposed = version_key(version)
    if not isinstance(pages, list) or not pages or not all(isinstance(page, list) for page in pages):
        raise ValueError("cannot read the GitHub release list")
    for page in pages:
        for release in page:
            tag = release["tag_name"]
            if tag == "v" + version:
                state = "draft" if release["draft"] else "published"
                raise ValueError(f"GitHub release v{version} already exists ({state}); inspect it before retrying")
            if not release["draft"] and tag.startswith("v") and VERSION.fullmatch(tag[1:]):
                if proposed <= version_key(tag[1:]):
                    raise ValueError(f"v{version} must be newer than published release {tag}")


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def check_sidecar(archive):
    sidecar = archive.with_name(archive.name + ".sha256")
    match = re.fullmatch(r"([a-fA-F0-9]{64}) [ *](.+)\n?", sidecar.read_text())
    if not match or match[2] != archive.name:
        raise ValueError(f"{sidecar.name} must name exactly {archive.name}")
    if digest(archive) != match[1].lower():
        raise ValueError(f"{archive.name} checksum mismatch")


def unpack_core(archive, destination):
    archive = Path(archive)
    check_sidecar(archive)
    binary = Path(destination) / "ck-subc"
    # Do not extract arbitrary archive paths or accept duplicate root executables.
    with zipfile.ZipFile(archive) as bundle:
        if bundle.namelist().count(binary.name) != 1:
            raise ValueError("public core ZIP must contain one root ck-subc")
        with bundle.open(binary.name) as source, binary.open("xb") as stream:
            shutil.copyfileobj(source, stream)
    binary.chmod(0o755)
    print(binary.resolve())


def assert_names(names):
    if set(names) != ASSETS or len(names) != len(ASSETS):
        raise ValueError(f"wrong release asset set: missing {sorted(ASSETS - set(names))}, "
                         f"unexpected {sorted(set(names) - ASSETS)}, {len(names)} total")


def check_assets(directory):
    assert_names([entry.name for entry in directory.iterdir() if entry.is_file()])
    for name in sorted(ARCHIVES):
        check_sidecar(directory / name)


def inventory(tag, release):
    if release["tag_name"] != tag or release["draft"] is not True:
        raise ValueError(f"expected the draft for {tag}")
    assets = release["assets"]
    assert_names([asset["name"] for asset in assets])
    for asset in sorted(assets, key=lambda item: item["name"]):
        if asset["state"] != "uploaded" or type(asset["id"]) is not int or asset["id"] <= 0:
            raise ValueError(f"{asset['name']} is not an uploaded release asset")
        print(asset["id"], asset["name"])


def main():
    command, *args = sys.argv[1:]
    if command == "version":
        workspace_version(args[0])
    elif command == "inputs":
        inputs(args[0])
    elif command == "available":
        release_available(args[0], json.load(sys.stdin))
    elif command == "unpack-core":
        unpack_core(*args)
    elif command == "check-assets":
        check_assets(Path(args[0]))
    elif command == "inventory":
        inventory(args[0], json.load(sys.stdin))
    elif command == "index":
        component = json.loads(Path(args[0]).read_text()).get("components", {}).get("broca", {})
        return 0 if (component.get("version"), component.get("requires_core")) == tuple(args[1:]) else 1
    else:
        raise ValueError(f"unknown command: {command}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, KeyError, OSError, zipfile.BadZipFile) as error:
        print(f"release: {error}", file=sys.stderr)
        sys.exit(2)
