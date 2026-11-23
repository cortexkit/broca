#!/usr/bin/env python3
"""One-time migration of a state root's feature floor and the old log format's empty folders.

The floor lists the storage formats a machine's sessions are written in, and placement refuses a
build that does not know one of them (scripts/feature-floor.sh). Up to 0.4.111 it was the file
`wal-feature-floor` and builds printed their table for `--wal-known-features`; from 0.4.112 the
file is `feature-floor` and the flag `--known-features`. Before 0.4.104 the floor also listed the
frame features of the old log format. Run this once per machine, before placing 0.4.112 or later.

What it does, under the floor's lock file (the one a running ck-broca takes to raise it):
- refuses, changing nothing, when the floor names something it does not recognise;
- copies the floor to a backup outside the state root;
- when the floor names `provider-held/v1` (the old log once held a provider-held call) and the
  `late-results-registered` marker is missing, creates the marker: the marker alone is the
  evidence that a missing late-results.db cannot prove a session has no held call;
- writes the floor as `feature-floor`, keeping only `session-storage/v1` (or empty, if the floor
  never named it), and removes `wal-feature-floor` and its lock file;
- removes the folders the old log format kept its sessions and media in, and the staging
  folders and ledger of the one-time conversion that moved those sessions into the session store,
  each only when it is empty; one that holds anything is named and kept;
- moves `caps.db` (the spend caps of a removed feature) and its SQLite side files into the
  backup folder outside the state root.

It can run before or after 0.4.112 is placed. A build before 0.4.112 that raises its floor after
this ran writes `wal-feature-floor` again, and 0.4.112 writes `feature-floor` on its first session
write; a later run then finds both, and removes the old one when the new one names everything it
does (refusing otherwise).
Usage: migrate-feature-floor.py [--dry-run]
"""

import datetime
import fcntl
import os
import pathlib
import sys

from lib import state_root

SESSION_STORAGE = "session-storage/v1"
OLD_LOG_FEATURES = {
    "account-limit-pause/v1", "agent-run-scopes/v1", "archive-index/v2",
    "auth-selection-method/v1", "bill-attempts/v1", "bill-vault-facts/v1",
    "chatgpt-codex-wire/v1", "client-message-id/v1", "compaction-ride-wait/v1",
    "compaction-v2/v1", "compaction-view-meta/v1", "compaction-view-negotiation/v1",
    "compaction-view/v1", "compaction-wait/v1", "compaction/v1", "context-ceiling/v1",
    "deferred-call/v1", "delivery-order/v1", "dispatch-module/v1", "flow-scopes/v1",
    "frame-stamps/v1", "google-rpc-error-details/v1", "input-token-cap/v1",
    "interrupt-reason/v1", "media-content/v1", "message-marks/v1", "message-times/v1",
    "openrouter-reasoning/v1", "output-reservation/v1", "plan-manifest/v1",
    "plan-reference-slots/v1", "provider-declarations/v1", "provider-fallback-block/v1",
    "provider-held/v1", "provider-text-state/v1", "refusal-fallback/v1", "restart-pause/v1",
    "retention/v1", "rides-v2/v1", "run-cancel-at/v1", "runner-tags/v1", "runner-tags/v2",
    "scope/v1", "service-tier-queue/v1", "session-change/v1", "session-class/v1",
    "speaks-for-name/v1", "steer-queue/v1", "step-transforms/v1",
    "structured-output-downgrade/v1", "structured-output-paragraph/v1", "structured-output/v1",
    "tool-name-aliases/v1", "tool-reply/v1", "tool-schema-pins/v1", "tool-times/v1",
    "turn-directives/v1", "turn-system/v1", "visibility-late/v1", "wal-recovery/v1",
    "writer-id/v1",
}


def fsync_dir(path: pathlib.Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_private(path: pathlib.Path, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)


# What the old log format and its one-time conversion to the session store left in a state root.
# Nothing reads or creates them any more; each is removed only when empty.
OLD_FOLDERS = (
    "wal", "wal-archive", "wal-archive-durable", "wal-durable", "wal-ephemeral",
    "media", "media-durable", "media-ephemeral", "conversion-recovery", "conversion-staging",
)
OLD_FILES = ("conversion-ledger.db",)
# Spend caps were removed; their database is kept as a backup rather than deleted.
CAPS_FILES = ("caps.db", "caps.db-shm", "caps.db-wal")


def remove_old_folders(root: pathlib.Path, dry: bool) -> None:
    backups = root.parent / "broca-migrations"
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    for name in CAPS_FILES:
        path = root / name
        if not path.is_file():
            continue
        target = backups / f"{name}.{stamp}"
        print(f"moved   {path} to {target}")
        if not dry:
            backups.mkdir(mode=0o700, exist_ok=True)
            os.rename(path, target)
    for name in OLD_FOLDERS:
        path = root / name
        if not path.is_dir():
            continue
        if any(path.iterdir()):
            print(f"kept    {path}: not empty")
            continue
        print(f"removed {path}")
        if not dry:
            path.rmdir()
    for name in OLD_FILES:
        path = root / name
        if not path.is_file():
            continue
        if path.stat().st_size:
            print(f"kept    {path}: not empty")
            continue
        print(f"removed {path}")
        if not dry:
            path.unlink()
    if not dry:
        fsync_dir(root)


def main() -> int:
    dry = "--dry-run" in sys.argv[1:]
    root = state_root.default_state_root()
    floor = root / "wal-feature-floor"
    new_floor = root / "feature-floor"
    if not floor.exists():
        print(f"no floor at {floor}; nothing to move")
        remove_old_folders(root, dry)
        return 0
    lock = os.open(root / "wal-feature-floor.lock", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock, fcntl.LOCK_EX)
    if new_floor.exists():
        old_names = {line.strip() for line in floor.read_text().splitlines() if line.strip()}
        new_names = {line.strip() for line in new_floor.read_text().splitlines() if line.strip()}
        if not old_names <= new_names:
            missing = ", ".join(sorted(old_names - new_names))
            print(f"REFUSING: {floor} names {missing}, which {new_floor} does not")
            return 2
        print(f"removed {floor}: {new_floor} already names everything it does")
        if not dry:
            floor.unlink()
            (root / "wal-feature-floor.lock").unlink()
            fsync_dir(root)
        remove_old_folders(root, dry)
        return 0
    text = floor.read_bytes()
    names = {line.strip() for line in text.decode().splitlines() if line.strip()}
    unknown = sorted(names - OLD_LOG_FEATURES - {SESSION_STORAGE})
    if unknown:
        print(f"REFUSING: {floor} names features this script does not know: {', '.join(unknown)}")
        return 2
    keep = [SESSION_STORAGE] if SESSION_STORAGE in names else []
    marker = root / "late-results-registered"
    needs_marker = "provider-held/v1" in names and not marker.exists()
    print(f"floor   {floor}: {len(names)} names, keeps {keep or 'none'} in {new_floor}")
    print(f"marker  {marker}: {'created' if needs_marker else 'unchanged'}")
    if dry:
        remove_old_folders(root, dry)
        print("dry run: nothing changed")
        return 0
    backups = root.parent / "broca-migrations"
    backups.mkdir(mode=0o700, exist_ok=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = backups / f"wal-feature-floor.{stamp}"
    write_private(backup, text)
    fsync_dir(backups)
    print(f"backup  {backup}")
    if needs_marker:
        write_private(marker, b"")
        fsync_dir(root)
    temp = root / f".feature-floor.{os.getpid()}.migrate.tmp"
    write_private(temp, "".join(f"{name}\n" for name in keep).encode())
    os.rename(temp, new_floor)
    fsync_dir(root)
    floor.unlink()
    (root / "wal-feature-floor.lock").unlink()
    os.close(lock)
    fsync_dir(root)
    print(f"floor   now {new_floor}: {new_floor.read_text()!r}")
    remove_old_folders(root, dry)
    return 0


if __name__ == "__main__":
    sys.exit(main())
