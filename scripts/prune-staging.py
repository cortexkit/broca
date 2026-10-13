#!/usr/bin/env python3
"""Prune only complete ck-broca release and rollback pairs from the shared staging directory."""
from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import re
import sys

SEAM = Path.home() / ".local/share/cortexkit/staging"
PLACED = Path.home() / ".local/share/cortexkit/bin/ck-broca"
RELEASE = re.compile(r"^ck-broca\.([0-9a-f]{8,})$")
ROLLBACK = re.compile(r"^ck-broca\.rollback-([0-9TZ]+)$")


def fail(message: str) -> int:
    print(f"prune-staging: {message}", file=sys.stderr)
    return 2


def digest(path: Path) -> str | None:
    try:
        h = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--keep", type=int, default=5)
    parser.add_argument("--staging-dir", type=Path, default=SEAM)
    parser.add_argument("--placed", type=Path, default=PLACED)
    args = parser.parse_args()
    if args.keep < 0:
        return fail("--keep must be nonnegative")
    seam = args.staging_dir
    current = seam / "ck-broca.current"
    try:
        if current.is_symlink() or not current.is_file():
            return fail("ck-broca.current is missing or not a regular file")
        fields = current.read_text().split()
        if len(fields) != 3 or not re.fullmatch(r"[0-9a-f]{64}", fields[0]) or not RELEASE.fullmatch(fields[1]):
            return fail("ck-broca.current is unparseable")
    except OSError:
        return fail("ck-broca.current is missing or unreadable")
    placed_sha = digest(args.placed) if not args.placed.is_symlink() else None
    if placed_sha is None:
        return fail("placed binary sha cannot be read")

    releases = []
    rollbacks = []
    try:
        for path in seam.iterdir():
            if path.is_symlink() or not path.is_file():
                continue
            rm = RELEASE.fullmatch(path.name)
            bm = ROLLBACK.fullmatch(path.name)
            if rm:
                side = seam / (path.name + ".sha256.postsign")
                if not side.is_symlink() and side.is_file():
                    releases.append((path, side, path.stat().st_mtime_ns))
            elif bm:
                side = seam / (path.name + ".sha256")
                if not side.is_symlink() and side.is_file():
                    rollbacks.append((path, side, path.stat().st_mtime_ns))
    except OSError as exc:
        return fail(f"cannot inspect staging directory: {exc}")

    keep = {fields[1]}
    for binary, sidecar, _ in sorted(releases, key=lambda item: item[2], reverse=True)[:args.keep]:
        keep.add(binary.name)
    # The signed staged digest is the first token of shasum -c format.
    for binary, sidecar, _ in releases:
        try:
            side_sha = sidecar.read_text().split()[0]
        except (OSError, IndexError):
            continue
        if side_sha == placed_sha:
            keep.add(binary.name)
    newest_rollbacks = {item[0].name for item in sorted(rollbacks, key=lambda item: item[2], reverse=True)[:2]}

    deletions = []
    for binary, sidecar, _ in releases:
        if binary.name not in keep:
            deletions.extend((binary, sidecar))
    for binary, sidecar, _ in rollbacks:
        if binary.name not in newest_rollbacks:
            deletions.extend((binary, sidecar))
    total = 0
    for path in deletions:
        try:
            size = path.lstat().st_size
            total += size
            print(f"{'would delete' if args.dry_run else 'deleted'} {path.name} ({size} bytes)")
            if not args.dry_run:
                path.unlink()
        except OSError as exc:
            return fail(f"cannot remove {path.name}: {exc}")
    print(f"{'would free' if args.dry_run else 'freed'} {total} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
