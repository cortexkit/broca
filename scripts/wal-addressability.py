#!/usr/bin/env python3
"""Report WAL files no store row can name, and resolve a bind triple without touching it.

TWO MODES, ONE FILE, because they are the same question asked at two scales.

    wal-addressability.py                          tree-wide report
    wal-addressability.py --key ROOT HARNESS SESS   one triple -> hash, size

WHY THIS EXISTS. Broca has NO path that deletes a WAL -- no prune, no GC, no orphan
reclaim -- which was verified at source rather than assumed. That makes an orphaned WAL
INERT rather than at risk, and inverts the failure mode: unreachable transcripts do not
get collected, they ACCUMULATE SILENTLY AND FOREVER. CKTUI had 24 sessions dark for four
months; they surfaced only because an unrelated migration happened to use those rows as
a subject list. Nothing in either system was going to say so.

So this REPORTS and never acts. A cleaner would have deleted that 1.8 MB and been
correct about its own count -- which is the whole argument for the shape.

WHY A VERIFIER MUST NOT BE DRAWN FROM A PLAN. Two migrations this month each verified
themselves and each missed the same class. CKTUI's August re-key confirmed every file
arrived at its new hash with bytes intact; mine confirmed every migrated session had its
messages at the new key. Both were true, and both audited only the rows their operation
had selected -- the subject list defined the work AND the question, so a row the plan
never selected was outside both. This check enumerates the FILESYSTEM and asks whether
the store can name what is there, which is the only form that can catch a plan whose
scope was wrong.

Run it before and after any migration. The count must not increase.
"""

from __future__ import annotations

import argparse
import hashlib
import struct
import json
import pathlib
import sqlite3
import sys

from lib import state_root
from lib.archive_read import ArchiveRefusal, move_exclusions

US = "\x1f"
# A lone lineage identity frame. A WAL holding only this has no records: minted by a
# lookup that created what it was checking for, not by a session that ran.
LINEAGE_ONLY_BYTES = 69


def fnv1a64(text: str) -> str:
    """The WAL filename hash. Must match broca-core's wal_writer.rs exactly."""
    h = 0xCBF29CE484222325
    for byte in text.encode():
        h ^= byte
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return f"{h:016x}"


def addressable_keys(db_path: pathlib.Path) -> set[str]:
    """Every WAL hash the store can currently name, from BOTH session-keyed tables.

    meta and run_index store the SAME identity in DIFFERENT ENCODINGS -- a unit-separated
    triple and a JSON object. Reading only one of them under-reports what the store can
    reach, which is how a migration once derived its subject list from meta and left 1,475
    index-only sessions behind.
    """
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    unparsed_rows = 0
    keys = {fnv1a64(session) for (session,) in db.execute("SELECT session FROM meta")}
    for (raw,) in db.execute("SELECT DISTINCT session FROM run_index"):
        try:
            identity = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            # A malformed index row makes the census partial: its reachable WAL would
            # otherwise be falsely reported as unaddressable. Count it explicitly.
            unparsed_rows += 1
            continue
        keys.add(
            fnv1a64(
                f"{identity.get('project_root', '')}{US}"
                f"{identity.get('harness', '')}{US}"
                f"{identity.get('session', '')}"
            )
        )
    db.close()
    if unparsed_rows:
        # REFUSE rather than return a short key set. Every missing key turns a reachable WAL into
        # a reported orphan, and this report's whole purpose is telling an operator which files
        # nobody can reach -- a false entry here sends them to re-key or delete a healthy session.
        raise SystemExit(
            f"wal-addressability: cannot compare -- {unparsed_rows} run_index row(s) had an "
            "unreadable session identity, so the key set is short and every WAL those rows name "
            "would be reported unaddressable. Fix the rows or the parse before trusting a count."
        )
    return keys


def archived_addresses(wal_dir: pathlib.Path) -> set[str]:
    """Every address a sealed archive can serve, resolved in ONE pass.

    `archived_size` answers this for a single address and walks every member header to
    do it. That cost nothing before the fold -- with no archive directory it returned
    immediately -- and is now 27,693 reads per call, so a per-session loop over it does
    not finish. This exists for the loops; `archived_size` stays for single lookups that
    also want the size.

    It calls `archived_size` with a sentinel that cannot be a real address, so the two
    share ONE parser and one definition of reachable. Writing a second header walk here
    is what I started with, and I had the field offsets wrong from memory -- address at
    8:24 and length as <Q at 24, not what I typed. A duplicate parser is also the thing
    that drifts the first time the format moves.
    """
    found: set[str] = set()
    archived_size(wal_dir, "0" * 16, collect=found)
    return found


def archived_size(
    wal_dir: pathlib.Path, addr: str, collect: set[str] | None = None
) -> int | None:
    """Resolve sealed members newest-first; never label a corrupt payload reachable.

    Headers are the index-recovery authority. Unsealed passes still own live files
    and must not count as reachable archives, even if a complete member is present.
    """
    paths = []
    for name in ("wal-archive", "wal-archive-durable"):
        for path in (wal_dir.parent / name).glob("*.ark"):
            stamp = path.name.removeprefix("fold-").removesuffix(".ark")
            if not stamp.isdecimal() or path.name != f"fold-{int(stamp)}.ark":
                raise ValueError(f"invalid archive filename: {path}")
            paths.append((int(stamp), path))
    for _, path in sorted(paths, reverse=True):
        with path.open("rb") as archive:
            size = archive.seek(0, 2)
            if size < 8:
                continue
            archive.seek(-8, 2)
            if archive.read(8)[4:] != b"WALK":
                continue
            archive.seek(0)
            seen = set()
            while archive.tell() < size - 8:
                magic = archive.read(4)
                if magic != b"WALM":
                    break  # Trailing index, not a member.
                header = magic + archive.read(60)
                if len(header) != 64:
                    raise ValueError(f"truncated archive header: {path}")
                version, reserved = struct.unpack_from("<HH", header, 4)
                name = header[8:24].decode("ascii")
                length = struct.unpack_from("<Q", header, 24)[0]
                if (version != 1 or reserved != 0 or len(name) != 16
                        or any(c not in "0123456789abcdefABCDEF" for c in name)
                        or name in seen or archive.tell() + length > size - 8):
                    raise ValueError(f"invalid archive member: {path}")
                seen.add(name)
                if collect is not None:
                    # Inventory mode for `archived_addresses`: record the address, then
                    # fall through so the payload is DIGEST-VERIFIED like any other.
                    #
                    # My first version skipped the digest and a test caught it: this
                    # function's callers treat "resolved" as "reachable", and a member
                    # whose payload is corrupt is not reachable. Dropping verification
                    # would have made the audit report a damaged archive as healthy --
                    # trading the property for speed, when the cost being fixed was
                    # O(sessions x members) header walks, not the hashing. One pass over
                    # the container hashes 2.6 GB in a few seconds.
                    collect.add(name)
                elif name != addr:
                    archive.seek(length, 1)
                    continue
                digest = hashlib.sha256()
                remaining = length
                while remaining:
                    chunk = archive.read(min(remaining, 1024 * 1024))
                    if not chunk:
                        raise ValueError(f"truncated archive payload: {path}")
                    digest.update(chunk)
                    remaining -= len(chunk)
                if digest.digest() != header[32:64]:
                    raise ValueError(f"archive digest mismatch: {path}: {name}")
                if collect is not None:
                    # Inventory mode walks the WHOLE container. Returning here ends the
                    # walk after ONE member -- which a single-member fixture cannot
                    # distinguish from walking all of them, so the suite stayed green
                    # while the audit called 27,693 archived sessions missing.
                    continue
                return length
    return None


def unreachable_from_store(
    db_path: pathlib.Path, wal_dir: pathlib.Path
) -> tuple[list[tuple], int]:
    """Sessions the store NAMES but whose WAL is not where the name resolves.

    THE OTHER AXIS, and the one a fold does not break. The filesystem walk above asks
    "is every file named by someone" -- a question whose population COLLAPSES when 11,130
    files become ~12 archives, so it measures a different quantity before and after and
    cannot fail against the operation it is meant to guard.

    This asks "is every session the store names still reachable", and the store's population
    is exactly what a fold must not change. It conserves across the operation, which is what
    makes it usable as a before/after gate.

    Reports prefix_len so the caller can tell a session that lost its forensic record from
    one that lost transcript: a row with prefix_len > 0 still serves its materialized prefix,
    while a row with prefix_len == 0 has nothing to serve at all.
    """
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    # BOTH TABLES, for the same reason `addressable_keys` reads both. This check used to
    # iterate meta alone while the forward check used the union -- so an INDEX-ONLY
    # session whose WAL is absent fell outside both halves: no file for the filesystem
    # loop, no meta row for this one. 6,758 of 35,820 store identities are index-only
    # here (one-shot sends skip store projection), so the blind spot was 19% of the
    # population the heading claims to cover.
    #
    # An earlier comment on the caller resolved the same asymmetry by narrowing the
    # DENOMINATOR to match this numerator. That made the ratio honest and left the
    # omitted sessions omitted -- the population was the thing to fix, not the fraction.
    sessions: dict[str, int] = {}
    for session, prefix_len in db.execute("SELECT session, prefix_len FROM meta"):
        sessions[session] = prefix_len or 0
    for (raw,) in db.execute("SELECT DISTINCT session FROM run_index"):
        try:
            identity = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            # Counted and reported by `addressable_keys`, which refuses on unparsed rows
            # before this runs. Skipping here cannot hide it.
            continue
        key = (f"{identity.get('project_root', '')}{US}"
               f"{identity.get('harness', '')}{US}"
               f"{identity.get('session', '')}")
        # meta's prefix_len wins where both name the session; index-only rows have no
        # materialized prefix by definition, so 0 is the fact rather than a default.
        sessions.setdefault(key, 0)
    db.close()

    # RESOLVE THE ARCHIVE ONCE, not per session. `archived_size` walks every member
    # header on every call, which cost nothing before the fold -- with no archive
    # directory it returned immediately -- and is now 27,693 header reads each. Against
    # 35,820 store identities that is ~1e9 reads and the audit does not finish.
    # Collecting the addresses once makes the loop a set lookup.
    #
    # Same newest-first, sealed-only rule as `archived_size`: this reuses its resolver
    # rather than re-deriving one, so a change to what counts as reachable cannot leave
    # the two disagreeing.
    archived = archived_addresses(wal_dir)

    missing = []
    for session, prefix_len in sessions.items():
        addr = fnv1a64(session)
        if not (wal_dir / f"{addr}.wal").exists() and addr not in archived:
            missing.append((session, prefix_len))
    return missing, len(sessions)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-root", default=str(state_root.default_state_root()))
    ap.add_argument(
        "--key",
        nargs=3,
        metavar=("ROOT", "HARNESS", "SESSION"),
        help="resolve one bind triple to its WAL hash and size, opening nothing",
    )
    args = ap.parse_args()

    root = pathlib.Path(args.state_root).expanduser()
    wal_dir = root / "wal"
    # Hidden WALs still hold history, but readers must not serve them here. Refuse
    # the file/store counts and single-session lookup rather than reporting moved
    # bytes as reachable history or as missing files needing repair.
    move_exclusions(wal_dir, refuse_sidecars=True)

    # THE PROBE. A client asking "does my history exist" through a session read gets an
    # answer by MINTING the lineage it asked about -- a lookup that creates what it is
    # checking for cannot answer whether it was there. Both collisions that blocked this
    # month's migration were made exactly that way. This is a stat.
    if args.key:
        key = f"{args.key[0]}{US}{args.key[1]}{US}{args.key[2]}"
        path = wal_dir / f"{fnv1a64(key)}.wal"
        # Show the separator, because U+001F is INVISIBLE in a terminal and the fields run
        # together: ".../alfonso-tuibrocacktui-foreign-a" reads as one malformed string and
        # hides which field a mismatch is in -- exactly the question this probe is for.
        print(f"    key    {key.replace(US, ' <US> ')}")
        print(f"    hash   {fnv1a64(key)}")
        if not path.exists():
            try:
                archived = archived_size(wal_dir, fnv1a64(key))
            except (OSError, ValueError) as error:
                print(f"    archive cannot be read: {error}", file=sys.stderr)
                return 2
            if archived is not None:
                print(f"    file   ARCHIVED  {archived:,} bytes")
                return 0
            print("    file   ABSENT")
            return 1
        size = path.stat().st_size
        note = "  (lineage frame only -- no records)" if size == LINEAGE_ONLY_BYTES else ""
        print(f"    file   PRESENT  {size:,} bytes{note}")
        return 0

    db_path = root / "run-index.db"
    if not db_path.exists():
        print(f"wal-addressability: cannot compare -- no run-index at {db_path}", file=sys.stderr)
        return 2
    if not wal_dir.is_dir():
        print(f"wal-addressability: cannot compare -- no wal directory at {wal_dir}", file=sys.stderr)
        return 2

    # A store that cannot be read is CANNOT-COMPARE, not a finding. Letting the sqlite error
    # escape exits 1, which this script also uses for "orphans found" -- so a wrong path or a
    # truncated database would read as a real result.
    try:
        keys = addressable_keys(db_path)
    except sqlite3.Error as err:
        print(f"wal-addressability: cannot compare -- store unreadable: {err}", file=sys.stderr)
        return 2
    # Refuse on an empty store rather than reporting a clean tree. Zero addressable keys
    # makes EVERY file an orphan, which is a loud wrong answer; but a store that reads as
    # empty because the query failed would otherwise report the tree as maximally broken
    # and send someone hunting a migration bug that does not exist.
    if not keys:
        print("wal-addressability: cannot compare -- the store names no sessions", file=sys.stderr)
        return 2

    total = orphans = orphan_bytes = lineage_only = 0
    worst: list[tuple[int, str]] = []
    for path in wal_dir.iterdir():
        if path.suffix != ".wal":
            continue
        total += 1
        if path.stem in keys:
            continue
        size = path.stat().st_size
        if size <= LINEAGE_ONLY_BYTES:
            # Empty by construction: an opened lineage that never ran, or a minted probe.
            # Counted separately because it is not lost history and padding the headline
            # number with it would make a real regression harder to see.
            lineage_only += 1
            continue
        orphans += 1
        orphan_bytes += size
        worst.append((size, path.name))

    if total == 0 and not any(archived_size(wal_dir, key) is not None for key in keys):
        print(f"wal-addressability: cannot compare -- no WAL files under {wal_dir}", file=sys.stderr)
        return 2

    print(f"    WAL files on disk                      {total:,}")
    print(f"    addressable from a store row           {total - orphans - lineage_only:,}")
    print(f"    empty (lineage frame only, or smaller) {lineage_only:,}")
    print(f"    CONTENT-BEARING AND UNADDRESSABLE      {orphans:,}  ({orphan_bytes:,} bytes)")
    for size, name in sorted(worst, reverse=True)[:5]:
        print(f"      {name}  {size:,} bytes")
    if orphans:
        print()
        print("    These hold transcript content no store row can name. Nothing deletes them,")
        print("    so they are inert rather than at risk -- but they are also invisible to every")
        print("    reader. Investigate rather than clean: the cause is usually a rename that")
        print("    moved one side of the (root, harness, session) triple and not the other.")

    # Report the store axis beside the filesystem axis. Its denominator and numerator
    # must use the same union of session-keyed tables, including index-only sessions.
    try:
        missing, meta_rows = unreachable_from_store(db_path, wal_dir)
    except (OSError, ValueError, sqlite3.Error) as error:
        print(f"wal-addressability: cannot compare -- {error}", file=sys.stderr)
        return 2
    with_content = [m for m in missing if m[1] > 0]
    print()
    print("    -- store axis: sessions the store NAMES whose WAL is not there --")
    print(f"    named by the store (meta + index)      {meta_rows:,}")
    print(f"    WAL missing at the resolved hash       {len(missing):,}")
    print(f"      ...of which hold a materialized prefix {len(with_content):,}")
    if with_content:
        print()
        print("    Those serve their store prefix and cannot be re-projected or audited from")
        print("    source. Whether that prefix is COMPLETE is a separate question this check")
        print("    does not answer -- a terminal run with message rows equal to prefix_len has")
        print("    lost only its forensic record, while a non-terminal one may be short. Check")
        print("    the run state before calling any of these a truncated transcript.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ArchiveRefusal as error:
        print(f"wal-addressability: cannot compare -- {error}", file=sys.stderr)
        sys.exit(2)
