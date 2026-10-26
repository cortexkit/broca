#!/usr/bin/env python3
"""Remove ZERO-BYTE WAL files, and nothing else.

These are lineages that were opened and never ran: broca creates a session's WAL eagerly on
open, so a session that never admitted a run leaves a file holding nothing at all.

WHY REMOVING THEM IS SAFE, from source rather than inference. `read_file_capped` maps
NotFound to `Ok(Vec::new())`, and a zero-byte read produces the same empty buffer, so a
reader cannot distinguish an absent file from a present empty one -- both decode to zero
records. Deleting is therefore invisible to every read path, and the next real open recreates
the file. (`wal_file_exists` can now tell them apart, which is what makes this claim
CHECKABLE rather than merely argued -- see the control below.)

WHY THIS IS WORTH DOING AT ALL, given they are harmless: engram's published backup manifest
has a whole-fleet budget of 60,000 object references and broca's tree is the largest entry.
ENGRAM refused to skip these on their side, and was right to -- a zero-byte file is a file,
and silently omitting it from a capture makes a restored tree differ from the captured one.
The correct fix is at the source, where the semantics belong to broca.

*** THE SIXTEEN THIS MUST NOT TOUCH ***

Files of exactly 69 bytes are NOT empty. That is one lineage identity frame: no records, but
a minted WalLineageId on disk. Deleting one is not a no-op -- the next open mints a FRESH id,
and consumers read a changed lineage_id as LINEAGE RECREATION, which tells them to discard a
prefix and reread. So sweeping those would inject a false recreation signal into two other
modules while tidying this one.

That distinction was not mine. I offered all 650 sub-1KiB files to ENGRAM as free; they
counted rather than accepting the number, and the 634/16 split is theirs.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys

from lib import state_root
from lib import liveness
from lib.archive_read import ArchiveRefusal, move_exclusions

LINEAGE_FRAME_LEN = 69


def classify(wal_dir: pathlib.Path) -> tuple[list[pathlib.Path], int, int]:
    """Partition the tree into (sweepable, lineage-frame-only, content-bearing)."""
    sweepable: list[pathlib.Path] = []
    lineage_only = 0
    content = 0
    excluded = move_exclusions(wal_dir)
    for path in wal_dir.iterdir():
        if path.suffix != ".wal" or path.stem in excluded:
            continue
        size = path.stat().st_size
        if size == 0:
            sweepable.append(path)
        elif size <= LINEAGE_FRAME_LEN:
            # Exactly one identity frame, or a partial one. Either way it is not empty and
            # not ours to remove: the id on disk is load-bearing for consumers.
            lineage_only += 1
        else:
            content += 1
    return sweepable, lineage_only, content


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-root", default=str(state_root.default_state_root()))
    ap.add_argument("--apply", action="store_true", help="delete; default is a dry run")
    args = ap.parse_args()

    root = pathlib.Path(os.path.expanduser(args.state_root))
    wal_dir = root / "wal"
    if not wal_dir.is_dir():
        print(f"sweep: cannot compare -- no wal directory at {wal_dir}", file=sys.stderr)
        return 2

    sweepable, lineage_only, content = classify(wal_dir)
    total = len(sweepable) + lineage_only + content

    # COUNTED INDEPENDENTLY OF `classify`, and this is not redundancy.
    #
    # The post-sweep guard below compares before against after. If both sides come from
    # `classify`, the guard shares the classifier's opinion of what is protected -- so a
    # classifier that wrongly calls a 69-byte file sweepable reports it as sweepable BOTH
    # times, the comparison matches, and the guard passes while the protected class is being
    # deleted.
    #
    # A guard drawn from the thing it guards can only confirm self-consistency. This counts
    # the bytes directly.
    protected_before = sum(
        1 for p in wal_dir.iterdir() if p.suffix == ".wal" and p.stat().st_size == LINEAGE_FRAME_LEN
    )
    if total == 0:
        # An empty tree makes "0 to sweep" true and meaningless. Refusing here keeps a wrong
        # path from reading as a clean result.
        print(f"sweep: cannot compare -- no WAL files under {wal_dir}", file=sys.stderr)
        return 2

    print(f"    WAL files                              {total:,}")
    print(f"    content-bearing (untouched)            {content:,}")
    print(f"    lineage-frame-only, {LINEAGE_FRAME_LEN}b (PROTECTED)   {lineage_only:,}")
    print(f"    zero-byte (sweepable)                  {len(sweepable):,}")

    if not args.apply:
        print()
        print("    dry run. Re-run with --apply to delete the zero-byte files.")
        return 0

    # Check before the audit manifest as well as the unlinks. An idle writer has
    # no descriptors to inspect, and an unavailable process probe is not stopped.
    refusal = liveness.probe(root).refusal("sweep")
    if refusal:
        print(refusal, file=sys.stderr)
        return 3

    manifest_path = pathlib.Path(os.path.expanduser("~")) / f"sweep-manifest-{os.getpid()}.json"
    manifest = {"state_root": str(root), "removed": [p.name for p in sweepable]}
    manifest_path.write_text(json.dumps(manifest, indent=2))
    # Written BEFORE the act it describes: a crash mid-sweep otherwise leaves no record of
    # which files were removed, and a zero-byte file leaves no trace once unlinked.
    if json.loads(manifest_path.read_text())["removed"] != manifest["removed"]:
        print("sweep: manifest did not read back intact; refusing", file=sys.stderr)
        return 2

    removed = 0
    excluded = move_exclusions(wal_dir)
    for path in sweepable:
        if path.stem in excluded:
            continue
        if path.stat().st_size != 0:
            # Re-checked immediately before the unlink: the scan is not the act.
            continue
        path.unlink()
        removed += 1

    after_sweepable, after_lineage, after_content = classify(wal_dir)
    print()
    print(f"    removed                                {removed:,}")
    print(f"    manifest                               {manifest_path}")
    print(f"    zero-byte remaining                    {len(after_sweepable):,}")
    print(f"    lineage-frame-only remaining           {after_lineage:,}")
    print(f"    content-bearing remaining              {after_content:,}")

    protected_after = sum(
        1 for p in wal_dir.iterdir() if p.suffix == ".wal" and p.stat().st_size == LINEAGE_FRAME_LEN
    )
    if protected_after != protected_before or after_content != content:
        print()
        print(
            f"sweep: A PROTECTED FILE WAS REMOVED -- {LINEAGE_FRAME_LEN}-byte files went\n"
            f"  {protected_before} -> {protected_after}, content-bearing {content} -> {after_content}.\n"
            f"  The manifest at {manifest_path} lists what was unlinked.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ArchiveRefusal as error:
        print(f"sweep: cannot compare -- {error}", file=sys.stderr)
        sys.exit(2)
