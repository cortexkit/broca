#!/usr/bin/env python3
"""Count budget-finalize turns that persisted tool calls nothing can answer.

A finalize turn is the one tool-DISABLED turn a tool-call budget grants. A provider that
ignores `ToolChoice::None` and returns tool calls anyway used to have them written to the WAL
and published to subscribers, with no `ToolResult` ever following. Fixed in v0.3.34 by
filtering before the record is written.

This is the standing witness for that fix. The baseline at the time of the fix was FOUR
historical leaks; every scan that still reads four is a positive observation that the filter
holds, and the first five means it regressed. That property is why the number is printed even
when nothing is wrong: a checker that only speaks up on failure cannot be distinguished from
one that stopped running.

Exit 0 when the count is at or below the baseline, 1 when it grew, 2 when the scan could not
decide (no WALs, or a file it could not read -- an unreadable WAL is unknown, never clean).
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

from lib import state_root, wal_gate
from lib.archive_read import ArchiveRefusal, logical_wals

# Four leaking turns, holding seven calls between them, were present when the engine began
# stripping tool calls from finalize steps. Keep that number: it is the line that separates
# known-historical leaks from new ones, and raising it to make a scan pass would erase the
# only signal that the stripping still works.
# Archive safety is accidental: folding a baseline leak lowers this live-only
# count and the baseline guard refuses loudly rather than certifying a clean tree.
# Keep that refusal; this instrument is not an archive-aware historical census.
BASELINE = 4

# WAL frame layout, from crates/broca-wal/src/framing.rs: a little-endian u32 payload length,
# a version byte, seq and fence as u64, a 32-byte digest, then the JSON payload.
HEADER_LEN = 4 + 1 + 8 + 8 + 32
RECORD_FRAMING_VERSION = 1
LINEAGE_FRAMING_VERSION = 2
GATED_FRAMING_VERSION = wal_gate.GATED_FRAMING_VERSION
LINEAGE_PAYLOAD_LEN = 16
MAX_PAYLOAD_LEN = 64 * 1024 * 1024


def records(raw: bytes) -> tuple[list[dict], bool, list[str]]:
    """Decode record bodies in order; return them with whether the file ended cleanly.

    The third element names the features of a gated (version-3) frame this script does not
    know. When it is non-empty a newer broca build wrote the file: no records are returned for
    it, because broca refuses such a WAL whole, and the file is not corrupt either.

    The second element is what keeps this honest on a live tree. Stopping early is normal --
    the writer may be mid-append -- but a scan that stops early has NOT examined the trailing
    episode, and reporting it as clean is a claim about records it never read. Measured on the
    live corpus: cutting one byte off a known-leaking WAL takes the count from 1 to 0 and the
    exit code from 1 to 0, so a run concurrent with an append could report a false clean.

    Reading FRAMES rather than searching bytes is what makes the leak count trustworthy. Tool
    call inputs are arbitrary provider-supplied JSON, so any marker this scan looks for can also
    appear INSIDE one -- a call whose input happens to contain a tool-result shape would, under
    a substring search, appear to answer itself and vanish from the count. Decoding gives every
    match a position in the record structure, which a byte offset cannot supply.
    """
    out: list[dict] = []
    pos = 0
    end = len(raw)
    while pos + HEADER_LEN <= end:
        length = int.from_bytes(raw[pos : pos + 4], "little")
        version = raw[pos + 4]
        seq = int.from_bytes(raw[pos + 5 : pos + 13], "little")
        fence = int.from_bytes(raw[pos + 13 : pos + 21], "little")
        if length == 0 or length > MAX_PAYLOAD_LEN:
            return out, False, []
        body_start = pos + HEADER_LEN
        body_end = body_start + length
        if body_end > end:
            # A torn trailing frame is normal on a live file: the writer is mid-append.
            return out, False, []
        if version == LINEAGE_FRAMING_VERSION:
            if length != LINEAGE_PAYLOAD_LEN or seq != 0 or fence != 0:
                return out, False, []
            pos = body_end
            continue
        if version == GATED_FRAMING_VERSION:
            try:
                missing, envelope = wal_gate.open_gated(raw[body_start:body_end])
            except wal_gate.MalformedGatedFrame:
                return out, False, []
            if missing:
                return [], True, missing
            out.append(envelope["record"])
            pos = body_end
            continue
        if version != RECORD_FRAMING_VERSION:
            return out, False, []
        try:
            out.append(json.loads(raw[body_start:body_end]))
        except ValueError:
            return out, False, []
        pos = body_end
    return out, pos == end, []


def scan(
    wal_dir: pathlib.Path,
) -> tuple[int, int, dict[str, int], int, int, list[tuple[str, list[str]]]]:
    """Return (finalize turns, leaks, leaks by model, files scanned, files ending mid-frame,
    newer-writer files with the features they require that this script does not know)."""
    finalize = leaks = scanned = partial = 0
    by_model: dict[str, int] = {}
    newer_writer: list[tuple[str, list[str]]] = []
    for path in logical_wals(wal_dir, include_archives=False):
        scanned += 1
        if path.stat().st_size == 0:
            continue
        try:
            raw = path.read_bytes()
        except OSError as error:
            # An unreadable WAL leaves the leak count unknown, so treating it as clean would
            # let a permission change or a transient IO error read as a passing check.
            print(f"cannot compare: cannot read {path.name}: {error}", file=sys.stderr)
            raise SystemExit(2) from error

        bodies, complete, missing = records(raw)
        if missing:
            newer_writer.append((path.name, missing))
            continue
        if not complete:
            partial += 1

        model = "unknown"
        pending: set[str] = set()
        in_finalize = False
        for body in bodies:
            kind = body.get("type")
            if kind == "run_started":
                spec = (body.get("config") or {}).get("model") or {}
                if spec.get("provider_module_id"):
                    model = f"{spec['provider_module_id']}/{spec.get('model_id', '?')}"
            elif kind == "budget_fired" and body.get("kind") == "finalize":
                finalize += 1
                in_finalize = True
            elif kind == "model_step_finished" and in_finalize:
                message = body.get("assistant_message") or {}
                for block in message.get("content") or []:
                    if isinstance(block, dict) and block.get("type") == "tool_call":
                        pending.add(str(block.get("tool_call_id")))
            elif kind in ("tool_result", "tool_no_dispatch"):
                # An answered call is not a leak, whichever way it was answered.
                pending.discard(str(body.get("tool_call_id")))
            elif kind == "run_finished":
                # The episode ended, so any call still pending can never be answered. Count
                # one leak per TURN rather than per call: a single turn can carry several, so
                # a call count would move whenever a provider changes how many it emits, which
                # says nothing about whether the stripping held.
                if pending:
                    leaks += 1
                    by_model[model] = by_model.get(model, 0) + 1
                pending.clear()
                in_finalize = False
    return finalize, leaks, by_model, scanned, partial, newer_writer


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-root",
        default=str(state_root.default_state_root()),
    )
    parser.add_argument(
        "--baseline",
        type=int,
        default=BASELINE,
        help="historical leak count to compare against (default: the count when the fix shipped)",
    )
    args = parser.parse_args()

    wal_dir = pathlib.Path(args.state_root) / "wal"
    if not any((wal_dir.parent / name).is_dir() for name in ("wal", "wal-ephemeral", "wal-durable")):
        print(f"cannot compare: no wal directory at {wal_dir}", file=sys.stderr)
        return 2

    try:
        finalize, leaks, by_model, scanned, partial, newer_writer = scan(wal_dir)
    except ArchiveRefusal as error:
        print(f"cannot compare: {error}", file=sys.stderr)
        return 2

    # An empty corpus is no evidence: with nothing examined there is nothing to have found,
    # and reporting that as clean makes a missing tree indistinguishable from a healthy one.
    if scanned == 0:
        print(f"cannot compare: no wal files under {wal_dir}", file=sys.stderr)
        return 2
    # Finalize turns are the population this check is about. Zero of them means the query
    # found nothing to judge, which is not the same as finding nothing wrong.
    if finalize == 0:
        print("cannot compare: no finalize turns in the corpus", file=sys.stderr)
        return 2

    print(f"wal files scanned    {scanned}")
    print(f"ended mid-frame      {partial}")
    print(f"newer-writer         {len(newer_writer)}")
    for name, missing in newer_writer:
        print(f"  newer-writer {name}: requires {', '.join(missing)}")
    print(f"finalize turns       {finalize}")
    print(f"leaked tool calls    {leaks}  (baseline {args.baseline})")
    for model, count in sorted(by_model.items()):
        print(f"  {model}: {count}")

    # A file whose frames run out mid-record was not fully examined, so its trailing episode
    # is unjudged. Saying "no new leaks" would extend a clean verdict over records this run
    # never decoded -- the same refusal the unreadable-file arm makes, for the same reason.
    if partial:
        print(
            f"\ncannot compare: {partial} file(s) ended mid-frame, so their last episode was "
            f"not examined. Re-run when the writer is idle, or accept a partial verdict by "
            f"scanning a copied tree.",
            file=sys.stderr,
        )
        return 2
    # A newer broca build wrote these files, so this script cannot read them. Their bytes are
    # intact -- this is not damage -- but their episodes were not examined, and a clean verdict
    # would cover records nobody decoded, exactly as for a file that ended mid-frame.
    if newer_writer:
        print(
            f"\ncannot compare: {len(newer_writer)} file(s) were written by a newer broca build "
            f"(newer-writer) and were not examined. Run the find-finalize-leaks.py that ships "
            f"with that build.",
            file=sys.stderr,
        )
        return 2

    if leaks > args.baseline:
        print(
            f"\nREGRESSION: {leaks - args.baseline} finalize turn(s) leaked since the fix",
            file=sys.stderr,
        )
        return 1

    # Fewer than the baseline is not an improvement, it is a BROKEN SCAN. WAL files are
    # append-only and the historical leaks live in records that can never change, so the count
    # below the baseline can only mean this run did not see the whole corpus -- a wrong state
    # root, a partial tree, a filter that matched less than it should. Falling through to "no
    # new leaks" would extend a clean verdict over a corpus that was never read, which is the
    # one answer this script must never give.
    if leaks < args.baseline:
        print(
            f"cannot compare: found {leaks} historical leak(s) against a baseline of "
            f"{args.baseline}; append-only WALs cannot lose leaks, so this scan is not "
            f"seeing the full corpus",
            file=sys.stderr,
        )
        return 2

    print("\nno new leaks since the fix")
    return 0


if __name__ == "__main__":
    sys.exit(main())
