#!/usr/bin/env python3
"""One-time migration: shrink a state root's feature floor to the session store's one name.

Before 0.4.104 the floor (`wal-feature-floor` in broca's state root) also listed the frame
features of the old log format, and every build's `--wal-known-features` had to keep printing
them so placement would not refuse it. 0.4.104 knows only `session-storage/v1`, so placement
refuses it on a machine whose floor still lists the old names. Run this once per machine, before
placing 0.4.104 or later. Builds before 0.4.104 know every name, so a rollback stays placeable.

What it does, under the floor's lock file (the one a running ck-broca takes to raise it):
- refuses, changing nothing, when the floor names something it does not recognise;
- copies the floor to a backup outside the state root;
- when the floor names `provider-held/v1` (the old log once held a provider-held call) and the
  `late-results-registered` marker is missing, creates the marker: from 0.4.104 the marker alone
  is the evidence that a missing late-results.db cannot prove a session has no held call;
- rewrites the floor to `session-storage/v1` (or empty, if the floor never named it).

Safe while ck-broca runs: a running build raises the floor only with `session-storage/v1`.
Usage: shrink-feature-floor.py [--dry-run]
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


def main() -> int:
    dry = "--dry-run" in sys.argv[1:]
    root = state_root.default_state_root()
    floor = root / "wal-feature-floor"
    if not floor.exists():
        print(f"no floor at {floor}; nothing to do")
        return 0
    lock = os.open(root / "wal-feature-floor.lock", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock, fcntl.LOCK_EX)
    text = floor.read_bytes()
    names = {line.strip() for line in text.decode().splitlines() if line.strip()}
    unknown = sorted(names - OLD_LOG_FEATURES - {SESSION_STORAGE})
    if unknown:
        print(f"REFUSING: {floor} names features this script does not know: {', '.join(unknown)}")
        return 2
    keep = [SESSION_STORAGE] if SESSION_STORAGE in names else []
    marker = root / "late-results-registered"
    needs_marker = "provider-held/v1" in names and not marker.exists()
    print(f"floor   {floor}: {len(names)} names, keeps {keep or 'none'}")
    print(f"marker  {marker}: {'created' if needs_marker else 'unchanged'}")
    if dry:
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
    temp = root / f".wal-feature-floor.{os.getpid()}.shrink.tmp"
    write_private(temp, "".join(f"{name}\n" for name in keep).encode())
    os.rename(temp, floor)
    fsync_dir(root)
    print(f"floor   now {floor.read_text()!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
