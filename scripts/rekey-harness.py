#!/usr/bin/env python3
"""Re-key legacy harness spellings in the run-index and rename their WAL files.

The bind triple is `project_root<US>harness<US>session`. It is the store's `meta.session` key AND
the input to the FNV-1a hash that names the WAL file, so a store re-key WITHOUT the matching
rename sends every lookup to a file that is not there. Both halves move together or neither does.

WHY THIS CANNOT RUN AGAINST A LIVE DAEMON. Renaming a WAL that a writer holds open leaves that
writer's descriptor on the moved inode while any REOPEN at the old key mints a fresh empty WAL --
a split lineage, which is strictly worse than the read-miss window a migration window costs. The
script refuses unless the module is down.

WHAT IT REFUSES RATHER THAN GUESSES:
  - a live ck-broca process
  - a target store key that already exists (two rows would collapse into one)
  - a target WAL filename that already exists (a rename would OVERWRITE a transcript)
  - a harness value it was not explicitly told to migrate

THE COLLISION CASE IS REAL, NOT THEORETICAL. Measured before this script existed: of 19,932
runner-era WALs, exactly ONE has a target name already taken -- a 139 KB transcript whose target
is a 0-byte lineage some later bind created and never wrote. `mv` would have destroyed the real
one. 1 in 19,932 is precisely the density a spot-check misses, which is why the walk is
exhaustive and the default is refusal.

Usage:
    scripts/rekey-harness.py --from runner --to broca [--state-root DIR] [--apply]

Default is a dry run. Exit 0 clean, 1 refused on a checked condition, 2 cannot compare.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import pathlib
import shutil
import sqlite3
import subprocess
import sys

from lib import state_root
from lib import liveness
from lib.archive_read import ArchiveRefusal, logical_wals, move_exclusions

US = "\x1f"


def fnv1a64(data: bytes) -> int:
    h = 0xCBF29CE484222325
    for b in data:
        h ^= b
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return h


def wal_name(key: str) -> str:
    return f"{fnv1a64(key.encode()):016x}.wal"


def writer_is_live_on(wal_dir: pathlib.Path) -> bool:
    """Refuse both live and indeterminate writers, before SQLite or backups."""
    refusal = liveness.probe(wal_dir.parent).refusal("rekey")
    if refusal:
        print(refusal, file=sys.stderr)
        return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="src", required=True, help="harness spelling to retire")
    ap.add_argument("--to", dest="dst", required=True, help="canonical harness spelling")
    ap.add_argument("--state-root", default=str(state_root.default_state_root()))
    ap.add_argument("--apply", action="store_true", help="perform the migration (default: dry run)")
    # EXACT roots to leave alone, repeatable. Exact rather than pattern, deliberately.
    #
    # A live client that still declares the OLD harness must keep its sessions addressable: the
    # triple is both the store key and the input to the WAL filename hash, and a read for a
    # migrated triple returns an EMPTY RANGE rather than an error -- so re-keying under a client
    # that has not moved renders its history as ordinary empty sessions, silently.
    #
    # My first version matched roots by SUBSTRING and was wrong in BOTH directions at once:
    #   over-matched  .../cortexkit/brocatui/sandbox and .../sandbox/deadroot, which merely
    #                 CONTAIN the name and belong to nobody
    #   under-matched .../cortexkit/idtest/real, a real client session that does not live under
    #                 a project directory at all
    # The authoritative list of roots a client binds lives in THAT CLIENT'S STORE, not in the
    # shape of a path string. Ask the owner; do not infer.
    ap.add_argument(
        "--exclude-root",
        action="append",
        default=[],
        help="exact project_root to leave under the old harness (repeatable)",
    )
    args = ap.parse_args()

    if args.src == args.dst:
        print("rekey: --from and --to are identical; nothing to migrate", file=sys.stderr)
        return 2

    root = pathlib.Path(args.state_root)
    db_path = root / "run-index.db"
    wal_dir = root / "wal"
    # Renaming a WAL changes its address but leaves its .moving/.moved marker
    # behind, allowing readers to reopen a session that belongs to another machine.
    # Refuse even when the marked source or destination has only archived bytes.
    try:
        move_exclusions(wal_dir, refuse_sidecars=True)
    except ArchiveRefusal as error:
        print(f"rekey: cannot compare -- {error}", file=sys.stderr)
        return 2
    if not db_path.exists():
        print(f"rekey: cannot compare -- no run-index at {db_path}", file=sys.stderr)
        return 2
    if not any((root / name).is_dir() for name in ("wal", "wal-ephemeral", "wal-durable")):
        print(f"rekey: cannot compare -- no wal directory at {wal_dir}", file=sys.stderr)
        return 2

    if args.apply and writer_is_live_on(wal_dir):
        return 3

    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    # Destination occupancy is the union of meta and run_index. One-shot sends can
    # skip projection, so a destination may have an index row without a meta row.
    all_keys = {s for (s,) in db.execute("SELECT session FROM meta")}
    for (raw,) in db.execute("SELECT DISTINCT session FROM run_index"):
        try:
            row = json.loads(raw)
        except (TypeError, ValueError):
            continue  # a malformed row cannot name a destination; subjects reports it
        if isinstance(row, dict) and {"project_root", "harness", "session"} <= row.keys():
            all_keys.add(f"{row['project_root']}{US}{row['harness']}{US}{row['session']}")
    src_infix = f"{US}{args.src}{US}"
    dst_infix = f"{US}{args.dst}{US}"

    # SUBJECTS ARE THE UNION OF BOTH TABLES, NOT META ALONE.
    #
    # A session can exist in run_index with NO meta row -- a lineage that ran before it was ever
    # materialized. Measured on the live store at the time this was written: 1,480 such runner
    # sessions, of which 1,475 hold a NON-EMPTY WAL.
    #
    # Deriving subjects from meta alone strands every one of them, and does it INVISIBLY: the
    # run_index update below is a global pattern rewrite, so those rows move to the new harness
    # while their WAL stays at the old hash. A canonical read then finds an index identity whose
    # WAL is absent, treats the missing path as an empty range, and returns an EMPTY TRANSCRIPT
    # for a session that has one. Worse, a continuation mints a fresh WAL at the new hash, so the
    # old history is not merely unreachable -- it is superseded, and re-running this script cannot
    # heal it because the index no longer names the old spelling.
    #
    # run_index stores the SAME identity in a DIFFERENT ENCODING (a JSON object), so the union has
    # to convert before comparing. Comparing the encodings directly would silently find nothing in
    # common and quietly reproduce the meta-only behaviour.
    # Excluded roots are compared as EXACT first path components, never as substrings, so a
    # directory that merely contains an excluded name is not swept along with it.
    excluded = set(args.exclude_root)

    def is_excluded(key: str) -> bool:
        return key.split(US)[0] in excluded

    subjects_set = {k for k in all_keys if src_infix in k and not is_excluded(k)}
    index_sessions_by_key: dict[str, list[str]] = {}
    index_only = 0
    for (raw,) in db.execute("SELECT DISTINCT session FROM run_index"):
        try:
            identity = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            # A row whose identity does not parse cannot be re-keyed safely, and guessing its
            # shape is how a migration corrupts what it was asked to move.
            continue
        if identity.get("harness") != args.src:
            continue
        key = f"{identity.get('project_root', '')}{US}{args.src}{US}{identity.get('session', '')}"
        index_sessions_by_key.setdefault(key, []).append(raw)
        if is_excluded(key):
            continue
        if key not in subjects_set:
            subjects_set.add(key)
            index_only += 1
    subjects = sorted(subjects_set)

    # A migration over zero rows is a no-op, not a success. Say so rather than printing a clean
    # summary that reads as work done.
    if not subjects:
        print(f"rekey: no rows carry harness {args.src!r}; nothing to do")
        return 0

    # ARCHIVE MEMBERSHIP, resolved BEFORE planning. An absent live WAL used to mean the
    # log was gone; since the first archive fold it usually means the log was MOVED, and a
    # member is addressed by the hash of the triple it was archived under. Re-keying the
    # store without relocating the member leaves history reachable only by an identity the
    # store no longer holds: production resolution hashes the REQUESTED triple and reads
    # exactly that address, so it never rediscovers the old member through its embedded
    # provenance. Refusing is the smallest safe answer; migrating one would mean extracting
    # it under quiescence and carrying it through the normal rename and verification plan.
    #
    # IMPORTED RATHER THAN REIMPLEMENTED, for the same reason the addressability gate is:
    # one definition of "this session has a log somewhere", so this check cannot drift into
    # disagreeing with the reader it is protecting.
    try:
        population = logical_wals(wal_dir)
    except ArchiveRefusal as error:
        print(f"rekey: cannot compare -- {error}", file=sys.stderr)
        return 2
    live_paths = {entry.stem: entry for entry in population if isinstance(entry, pathlib.Path)}
    archived_addresses = {entry.address for entry in population if not isinstance(entry, pathlib.Path)}

    plan: list[tuple[str, str, pathlib.Path | None, pathlib.Path | None]] = []
    key_collisions: list[str] = []
    wal_collisions: list[tuple[str, pathlib.Path]] = []
    missing_wal = 0
    archived_sources: list[tuple[str, str]] = []
    archived_destinations: list[tuple[str, str, str]] = []
    tombstoned_sources = 0

    for old_key in subjects:
        new_key = old_key.replace(src_infix, dst_infix, 1)
        if new_key in all_keys:
            key_collisions.append(new_key)
            continue
        address = wal_name(old_key).removesuffix(".wal")
        old_wal = live_paths.get(address)
        if old_wal is None:
            # A filtered WAL still on disk belongs to deletion recovery. Re-keying its
            # rows or renaming it would separate it from its address's tombstone.
            if any((directory / wal_name(old_key)).exists() for directory in (wal_dir, root / "wal-ephemeral", root / "wal-durable")):
                tombstoned_sources += 1
                continue
            if address in archived_addresses:
                # NOT "already gone" -- archived. Stranding it is silent: the store would
                # move, the bytes would survive, and nothing would ever read them again.
                archived_sources.append((old_key, address))
                continue
            # A row whose WAL is genuinely absent re-keys cleanly; nothing to rename.
            missing_wal += 1
            plan.append((old_key, new_key, None, None))
            continue
        new_wal = old_wal.with_name(wal_name(new_key))
        destination = next((directory / new_wal.name for directory in (wal_dir, root / "wal-ephemeral", root / "wal-durable")
                            if (directory / new_wal.name).exists()), None)
        if destination is not None:
            wal_collisions.append((old_key, destination))
            continue
        # A folded destination has no live WAL. Check the archive before renaming:
        # live-first resolution would otherwise shadow its archived transcript.
        new_address = wal_name(new_key).removesuffix(".wal")
        if new_address in archived_addresses:
            archived_destinations.append((old_key, new_key, new_address))
            continue
        plan.append((old_key, new_key, old_wal, new_wal))

    print(f"    harness {args.src!r} -> {args.dst!r}")
    print(f"    rows carrying the old spelling   {len(subjects)}")
    print(f"    ...of which INDEX-ONLY (no meta)  {index_only}")
    print(f"    migratable                       {len(plan)}")
    print(f"    rows with no WAL on disk         {missing_wal}")
    print(f"    tombstoned WALs left untouched   {tombstoned_sources}")
    print(f"    store-key collisions             {len(key_collisions)}")
    print(f"    WAL-name collisions              {len(wal_collisions)}")

    if archived_sources:
        print()
        print("    REFUSING: some rows' logs are ARCHIVED, not missing.")
        print("    Re-keying the store would leave their history addressable only by an")
        print("    identity the store no longer holds. The bytes survive; nothing reads them.")
        for old_key, address in archived_sources[:5]:
            print(f"      archived: {address}.wal  for session {old_key.split(US)[2][:46]}")
        if len(archived_sources) > 5:
            print(f"      ... and {len(archived_sources) - 5} more")
        print("    Unfold them first (store-maintenance wal-fold --unfold), then re-run.")
        return 2

    if key_collisions or wal_collisions:
        print()
        print("    REFUSING: the target already exists for some rows.")
        for k in key_collisions[:5]:
            print(f"      store key taken: {k.split(US)[2][:60]}")
        for old_key, path in wal_collisions[:5]:
            size = path.stat().st_size
            print(f"      wal taken:       {path.name}  {size} bytes")
            print(f"        for session:   {old_key.split(US)[2][:56]}")
            print(
                "        resolve by hand: the empty side is discardable ONLY if it has no store"
                " row and zero bytes"
            )
        return 1

    if archived_destinations:
        print()
        print("    REFUSING: some DESTINATIONS already hold an ARCHIVED log.")
        print("    Renaming onto that address does not merge the two histories -- the")
        print("    reader resolves live first, so the moved source would SHADOW the")
        print("    destination's archived transcript. Its bytes stay in the container")
        print("    and nothing reads them again. Same silence as an archived source,")
        print("    from the other end of the move.")
        for old_key, new_key, address in archived_destinations[:5]:
            print(f"      destination {address}.wal is archived; {old_key.split(US)[-1]} would take it")
        if len(archived_destinations) > 5:
            print(f"      ... and {len(archived_destinations) - 5} more")
        print("    Unfold them first (store-maintenance wal-fold --unfold), then re-run.")
        # EXIT 2, matching the archived-SOURCE arm above rather than the collision arms
        # below. Both archive cases are "there is folded state in the way, unfold it and
        # come back", which is this repo's cannot-compare code; a plain collision is a
        # planning refusal. Same remedy, same code.
        return 2

    if not args.apply:
        print()
        print("    dry run; re-run with --apply to migrate")
        return 0

    # THE ONLY CHECK HERE NOT DRAWN FROM THE PLAN.
    #
    # Every other verification in this script asks whether the rows it SELECTED arrived, which
    # cannot detect a subject list whose scope was wrong -- the list defines both the work and
    # the question. Two migrations this month each verified themselves that way and each missed
    # the same class: CKTUI's August re-key confirmed files arrived at their new hashes while
    # leaving 65 store rows pointing at the old root, and mine confirmed migrated sessions had
    # their messages at the new key while nearly stranding 1,475 index-only sessions.
    #
    # This one enumerates the FILESYSTEM and asks whether the store can name what is there. It
    # is the only form that can catch a plan that never considered a file.
    #
    # IMPORTED RATHER THAN REIMPLEMENTED: one definition of "addressable", so the reporter and
    # this gate cannot drift into disagreeing about what an orphan is. The filename has a hyphen,
    # so it cannot be a normal import.
    spec = importlib.util.spec_from_file_location(
        "wal_addressability", pathlib.Path(__file__).parent / "wal-addressability.py"
    )
    if spec is None or spec.loader is None:
        print("rekey: cannot load wal-addressability.py; refusing to migrate blind", file=sys.stderr)
        return 2
    wa = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(wa)

    def count_unaddressable() -> int:
        keys = wa.addressable_keys(db_path)
        # An empty key set makes every file an orphan, which would turn this gate into a
        # guaranteed refusal after the migration rather than a measurement.
        if not keys:
            raise RuntimeError("store names no sessions")
        return sum(
            1
            for p in logical_wals(wal_dir, include_archives=False)
            if p.stem not in keys
            and p.stat().st_size > wa.LINEAGE_ONLY_BYTES
        )

    try:
        unaddressable_before = count_unaddressable()
    except (RuntimeError, sqlite3.Error) as err:
        print(f"rekey: cannot establish a baseline orphan count ({err}); refusing", file=sys.stderr)
        return 2
    print(f"    content-bearing WALs no store row can name (before): {unaddressable_before}")

    backup = db_path.with_suffix(f".pre-rekey-{os.getpid()}.db")
    shutil.copy2(db_path, backup)
    print(f"    backup: {backup}")
    db.close()

    # THE REVERSAL MANIFEST, WRITTEN BEFORE THE FIRST RENAME AND OUTSIDE THE STATE ROOT.
    #
    # A RE-KEYED FILE IS BYTE-IDENTICAL TO ONE THAT NEVER MOVED. So once this run completes, the
    # set of files it touched exists NOWHERE: not in the store (which this run rewrites), not in
    # the filesystem (every WAL looks alike), and not in the backup (which records the old keys
    # but not which of them this run acted on). A reversal would have to re-derive hashes from a
    # store the migration had just rewritten, which is circular.
    #
    # Suggested by CKTUI from their own 65-file re-key, where it was the only reason the operation
    # stayed undoable a day later by a process that was not there when it ran. Costs ~20 KB.
    #
    # OUTSIDE the state root is the load-bearing part: a manifest living inside anything the
    # migration also rewrites is not a manifest. It goes beside the backup's parent, not under it.
    #
    # The index-only sessions make this sharper still -- 1,480 of them exist in run_index and not
    # in meta, a population visible only in this operation's own bookkeeping.
    manifest_path = pathlib.Path(
        os.path.expanduser(f"~/rekey-manifest-{args.src}-to-{args.dst}-{os.getpid()}.json")
    )
    manifest = {
        "from_harness": args.src,
        "to_harness": args.dst,
        "state_root": str(root),
        "db_backup": str(backup),
        "excluded_roots": sorted(excluded),
        "written_before_first_rename": True,
        "renames": [
            {
                "old_wal": old_wal.name,
                "new_wal": new_wal.name,
                "bytes": old_wal.stat().st_size,
                "old_key_session": old_key.split(US)[2],
            }
            for old_key, _new_key, old_wal, new_wal in plan
            if old_wal is not None and new_wal is not None
        ],
        "rekeyed_without_wal": [
            old_key.split(US)[2] for old_key, _n, old_wal, _nw in plan if old_wal is None
        ],
    }
    manifest_path.write_text(json.dumps(manifest, indent=1))
    # Fail loud rather than proceed unrecoverably: if the manifest cannot be read back, the
    # operation has no undo and must not start.
    verify = json.loads(manifest_path.read_text())
    if len(verify["renames"]) != len(manifest["renames"]):
        print("rekey: manifest did not read back intact; refusing to start", file=sys.stderr)
        return 1
    print(f"    manifest: {manifest_path}  ({len(manifest['renames'])} renames recorded)")

    # WAL renames first, then the store re-key. EITHER ORDER leaves an inconsistent middle if the
    # process dies, and neither is recoverable by the daemon on its own -- so the recovery is
    # re-running this script, which is idempotent: an already-renamed WAL reads as
    # old-absent/new-present and its row re-keys on the same pass.
    renamed = 0
    write_db = sqlite3.connect(str(db_path))
    try:
        for _old_key, _new_key, old_wal, new_wal in plan:
            if old_wal is not None and new_wal is not None and old_wal.exists():
                old_wal.rename(new_wal)
                renamed += 1
        with write_db:
            for old_key, new_key, _o, _n in plan:
                # ALL THREE SESSION-KEYED TABLES MOVE TOGETHER, IN ONE TRANSACTION.
                #
                # An earlier version of this script updated `meta` alone. `message.session` is an
                # independent primary-key component with NO cascade, so 100,416 message rows would
                # have kept the old key while their metadata moved: session.read would find
                # metadata claiming a materialized prefix and none of its messages, returning an
                # empty or tail-only transcript. The lineage ids would agree, so the full-WAL
                # fallback that exists for exactly this shape would never fire. WAL bytes survive;
                # transcript ACCESS does not.
                write_db.execute(
                    "UPDATE meta SET session=? WHERE session=?", (new_key, old_key)
                )
                write_db.execute(
                    "UPDATE message SET session=? WHERE session=?", (new_key, old_key)
                )

                # run_index is keyed by the SAME identity in a DIFFERENT ENCODING: a JSON object,
                # not the unit-separated triple. Use the exact source encodings collected while
                # building the plan so excluded and collision-refused lineages cannot be swept up
                # by a global harness replacement.
                #
                # Surgery on the string rather than parse-and-reserialize, deliberately: the key
                # must stay byte-identical to what serde writes, and a round trip through a JSON
                # library can reorder fields or change separators. Replacing one field's value
                # touches nothing else.
                for index_session in index_sessions_by_key.get(old_key, []):
                    write_db.execute(
                        "UPDATE run_index SET session = replace(session, ?, ?) WHERE session=?",
                        (
                            f'"harness":"{args.src}"',
                            f'"harness":"{args.dst}"',
                            index_session,
                        ),
                    )
    finally:
        write_db.close()

    print(f"    WAL files renamed  {renamed}")
    print(f"    store rows re-keyed {len(plan)}")

    # VERIFY FROM THE READER'S SIDE, NOT THE WRITER'S.
    #
    # The version of this check that shipped first asked only about `meta` and WAL filenames --
    # exactly the two things the script writes. It would have printed "verified" over 100,416
    # orphaned message rows, because a guard derived from what the writer touched cannot see what
    # the writer MISSED. The question is not "did my updates land" but "can a reader resolve
    # everything it needs".
    verify = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)

    # Enumerate the session-keyed tables from the SCHEMA rather than listing them here. A future
    # table would otherwise be silently excluded from both the migration and its verification,
    # which is how a two-of-three migration passes.
    session_tables = []
    for (name,) in verify.execute("SELECT name FROM sqlite_master WHERE type='table'"):
        cols = [r[1] for r in verify.execute(f"PRAGMA table_info({name})")]
        if "session" in cols:
            session_tables.append(name)
    if len(session_tables) < 3:
        print(
            f"    VERIFY FAILED: expected at least 3 session-keyed tables, found "
            f"{session_tables}; the schema changed and this migration may be incomplete",
            file=sys.stderr,
        )
        return 1

    # Verify only identities the plan selected. Source identities outside this set are excluded or
    # collision-refused and must retain their old spelling. The independent filesystem scope check
    # below still detects a plan that omitted a lineage and stranded its WAL.
    planned_sources = {old_key for old_key, _new_key, _old_wal, _new_wal in plan}
    planned_sources.update(
        index_session
        for old_key, _new_key, _old_wal, _new_wal in plan
        for index_session in index_sessions_by_key.get(old_key, [])
    )
    residue = {}
    for table in session_tables:
        n = sum(
            1
            for (session,) in verify.execute(f"SELECT session FROM {table}")
            if session in planned_sources
        )
        if n:
            residue[table] = n
    verify.close()

    if residue:
        print(f"    VERIFY FAILED: rows still carry {args.src!r}: {residue}", file=sys.stderr)
        return 1

    # Every migrated row must resolve to a WAL that exists, or the lineage is dark. This is the
    # check that would have caught a rename/re-key half-application.
    dark = sum(
        1
        for _o, new_key, _ow, new_wal in plan
        if new_wal is not None and not new_wal.exists()
    )
    if dark:
        print(f"    VERIFY FAILED: {dark} migrated rows have no WAL at the new key", file=sys.stderr)
        return 1

    # A migrated session must have its MESSAGES at the new key, not merely its metadata. This is
    # the specific check whose absence made the first version dangerous: meta moving without
    # message is exactly the state that reads as a successful migration and serves an empty
    # transcript.
    recheck = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    stranded = 0
    for _o, new_key, _ow, _nw in plan:
        want = recheck.execute(
            "SELECT prefix_len FROM meta WHERE session=?", (new_key,)
        ).fetchone()
        if not want or not want[0]:
            continue  # nothing materialized for this session; no messages to strand
        have = recheck.execute(
            "SELECT COUNT(*) FROM message WHERE session=?", (new_key,)
        ).fetchone()[0]
        if have == 0:
            stranded += 1
    recheck.close()
    if stranded:
        print(
            f"    VERIFY FAILED: {stranded} migrated sessions claim a materialized prefix but "
            f"have no messages at the new key",
            file=sys.stderr,
        )
        return 1

    print("    verified: every session-keyed table migrated, and every materialized")
    print("              session has its messages at the new key")

    # The scope check. A migration that moved a file out of reach of every store row has done
    # the damage this whole script exists to avoid, and the per-subject verification above
    # cannot see it -- the stranded file was never a subject.
    try:
        unaddressable_after = count_unaddressable()
    except (RuntimeError, sqlite3.Error) as err:
        print(f"rekey: MIGRATION APPLIED but the orphan count could not be re-taken ({err}).")
        print("       Run scripts/wal-addressability.py by hand before trusting this run.")
        return 1
    print(f"    content-bearing WALs no store row can name (after):  {unaddressable_after}")
    if unaddressable_after > unaddressable_before:
        print()
        print(
            f"rekey: THIS MIGRATION STRANDED "
            f"{unaddressable_after - unaddressable_before} TRANSCRIPT(S)."
        )
        print("       They hold content and no store row can name them. Nothing deletes them,")
        print("       so they are recoverable -- reverse with the manifest above, which records")
        print("       every rename this run performed.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
