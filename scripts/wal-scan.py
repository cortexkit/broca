#!/usr/bin/env python3
"""Scan the WAL corpus for a pattern, WITH A MANDATORY POSITIVE CONTROL.

WHY THIS EXISTS. Five separate ad-hoc WAL scans failed in one day (2026-09-02), all silently,
all in the reassuring direction, all well-formed enough to look like findings:

  * `"type":"text"` guessed instead of `"kind":"text"`      -> 0 hits, read as "no repeats"
  * `"call_id"` / `"name"` guessed instead of the real keys -> 0 hits, read as "no loops"
  * `ls -t .../wal/*.wal` over 30,000+ files                -> ARG_MAX, empty filename,
                                                               grep read nothing, 3 clean zeros
  * a file split at the first `#[cfg(test)]`                -> production sites attributed to
                                                               the test module, "covered"
  * a marker separating by one occurrence                   -> technically discriminating,
                                                               useless as evidence

Every one of those was a ZERO that meant "the instrument did not look", presented as a zero that
meant "the thing is not there". A scanner cannot tell those apart on its own -- but a CONTROL
can, and the control is the only part anyone ever skips because the scan already "worked".

So this refuses to report anything until a pattern you assert MUST match actually matches. The
control is not optional and there is no flag to turn it off, because the whole failure mode is
that the control is what gets dropped when you are in a hurry.

WHAT A GOOD CONTROL LOOKS LIKE. Not "any byte in the file" -- it must ride the SAME axis as the
question, one level broader. Hunting byte-identical repeat successes for `zoom`? The control is
"paired tool results of any kind". If the field shape is wrong, the broad query dies too and the
refusal fires. A control that cannot fail for the same reason the query can is decoration.

AND THE LIMIT OF THAT, measured while testing this script: `"type":"text"` and `"kind":"text"`
BOTH occur in a WAL. The first is a content block, the second is a tool-result output. So the
exact field name that produced one of the failures above still passes as a control -- it matches,
just for an unrelated reason. THIS SCRIPT CANNOT CATCH A CONTROL ON THE WRONG AXIS; it can only
catch a control that does not fire at all. Choosing the axis is still yours, and the way to check
it is to confirm the control's own count is plausible for what you asked, not merely non-zero.

Scans digest-verified record JSON, not binary headers, lineage metadata or gated
envelopes. Original record whitespace and escapes are retained for fixed-byte queries.
Unknown required features, corrupt frames and torn tails refuse the whole scan.

USAGE
    scripts/wal-scan.py --control '"kind":"text"' --pattern '"tool_name":"zoom"'
    scripts/wal-scan.py --control run_started --pattern budget_fired --newer-than 2h
    scripts/wal-scan.py --control '"role":"tool"' --pattern 'has now failed' --regex-off

EXIT
    0  the corpus was read, the control matched, results below are measured
    1  a pattern matched when --expect-absent was given (a regression witness)
    2  cannot compare -- no files selected, or the control matched nothing
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys
import time

from lib import state_root
from lib.archive_read import ArchivedWal, ArchiveRefusal, in_window, logical_wals, read_records, recency

DEFAULT_ROOT = str(state_root.default_state_root())


def select_files(wal_dir: pathlib.Path, newer_than_s: float | None, limit: int | None):
    """Select both live roots and archives, excluding lineage-matched tombstones."""
    cutoff = time.time() - newer_than_s if newer_than_s is not None else None
    warned = set()
    out = []
    for wal in logical_wals(wal_dir):
        if cutoff is not None:
            try:
                if not in_window(wal, cutoff, warned):
                    continue
            except OSError:
                pass  # Preserve unreadable live files for the refusal at the read site.
        out.append(wal)
    if limit and any(isinstance(wal, ArchivedWal) for wal in out):
        print("archive scope: --limit orders archived members by their activity upper bound, "
              "not unavailable source mtime", file=sys.stderr)
    out.sort(key=recency, reverse=True)
    return out[:limit] if limit else out


def duration(text: str) -> float:
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd])", text.strip())
    if not m:
        raise argparse.ArgumentTypeError(f"expected a duration like 30m, 2h, 7d -- got {text!r}")
    return float(m.group(1)) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[m.group(2)]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state-root", default=DEFAULT_ROOT)
    ap.add_argument(
        "--control",
        required=True,
        help="a pattern that MUST match at least once. Pick one on the same axis as --pattern, "
        "one level broader, so a wrong field name kills both and the refusal fires.",
    )
    ap.add_argument("--pattern", action="append", default=[], help="what to count (repeatable)")
    ap.add_argument("--newer-than", type=duration, default=None, help="e.g. 30m, 2h, 7d")
    ap.add_argument("--limit", type=int, default=None, help="scan at most N newest files")
    ap.add_argument("--regex", action="store_true", help="treat patterns as regex (default: fixed bytes)")
    ap.add_argument(
        "--expect-absent",
        action="store_true",
        help="exit 1 if any --pattern matches; for standing witnesses over a property that must hold",
    )
    args = ap.parse_args()

    wal_dir = pathlib.Path(args.state_root).expanduser() / "wal"
    if not any((wal_dir.parent / name).is_dir() for name in ("wal", "wal-ephemeral", "wal-archive")):
        print(f"wal-scan: cannot compare -- no WAL directory at {wal_dir}", file=sys.stderr)
        return 2

    files = select_files(wal_dir, args.newer_than, args.limit)
    if not files:
        # NOT a clean result. An empty selection answers nothing, and reporting zeros over it is
        # precisely the ARG_MAX failure this script exists to prevent.
        print(
            f"wal-scan: cannot compare -- no WAL files selected under {wal_dir}"
            + (f" newer than {args.newer_than:.0f}s" if args.newer_than else ""),
            file=sys.stderr,
        )
        return 2

    def compile_one(pat: str):
        return re.compile(pat.encode() if args.regex else re.escape(pat).encode())

    control = compile_one(args.control)
    patterns = [(p, compile_one(p)) for p in args.pattern]

    control_hits = 0
    control_files = 0
    hits = {p: 0 for p, _ in patterns}
    hit_files = {p: set() for p, _ in patterns}
    unreadable = 0

    for path in files:
        try:
            frames = read_records(path)
        except OSError:
            unreadable += 1
            continue
        n = sum(len(control.findall(frame.payload)) for frame in frames)
        if n:
            control_hits += n
            control_files += 1
        for text, rx in patterns:
            k = sum(len(rx.findall(frame.payload)) for frame in frames)
            if k:
                hits[text] += k
                hit_files[text].add(path.name)

    scanned = len(files) - unreadable
    print(f"    files selected      {len(files):,}")
    print(f"    files read          {scanned:,}" + (f"   ({unreadable} unreadable)" if unreadable else ""))
    print(f"    CONTROL {args.control!r}")
    print(f"      matches           {control_hits:,} across {control_files:,} files")

    if unreadable:
        # COUNTING IS NOT ACTING. This number was printed on line 161 and then ignored, so a
        # scan that could not read part of the corpus still returned a verdict over the part it
        # could -- and --expect-absent turns that undercount into a POSITIVE certification:
        # "the pattern is absent" when the file holding the counterexample was never opened.
        #
        # One readable file satisfies the control while another can remain unread.
        # Counts must not certify absence unless the entire corpus was readable.
        print(
            f"wal-scan: cannot compare -- {unreadable} file(s) could not be read, so this scan\n"
            "  covered less than the corpus and its counts are lower bounds. An --expect-absent\n"
            "  verdict over a partial read is a false all-clear.",
            file=sys.stderr,
        )
        return 2
    if control_hits == 0:
        # The whole point. A control that does not fire means the corpus was not read the way the
        # query assumes -- wrong field name, wrong encoding, wrong directory -- and every zero
        # below would be an artefact of that rather than a measurement.
        print(
            f"wal-scan: cannot compare -- the control {args.control!r} matched nothing across "
            f"{scanned:,} files. The query is not reading what it thinks it is: check the field "
            f"spelling against real bytes before trusting any count.",
            file=sys.stderr,
        )
        return 2

    for text, _ in patterns:
        print(f"    {text!r}")
        print(f"      matches           {hits[text]:,} across {len(hit_files[text]):,} files")
        for name in sorted(hit_files[text])[:5]:
            print(f"        {name}")

    if args.expect_absent:
        offenders = {t: hits[t] for t, _ in patterns if hits[t]}
        if offenders:
            print(
                f"wal-scan: REGRESSION -- expected absent, found {offenders}",
                file=sys.stderr,
            )
            return 1

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ArchiveRefusal, OSError) as error:
        print(f"wal-scan: REFUSING: {error}", file=sys.stderr)
        raise SystemExit(2)
