#!/usr/bin/env python3
"""Find runs that completed successfully while producing no assistant text.

This is the detector for a whole class of normalizer defect, not for one bug.
When a provider family's normalizer misses the event carrying a model's output,
the run does not fail: it completes, reports success, records real output token
usage, and returns nothing. There is no error to surface and no retry to
observe, so a caller that does not check for empty content sees a healthy turn.
Nobody files a report about a reply that was never there, which makes "we have
had no complaints" worthless as evidence. This query is the positive form of
that question, asked of durable state.

The signature is family-independent: a terminal record marking the run complete,
non-trivial output tokens billed, and zero text content blocks. Any future gap
of the same shape lands in it.

Usage:
    scripts/find-empty-completions.py [--hours N] [--min-output-tokens N] [--state-root PATH]

Exits non-zero when suspects are found, so it can gate a check.

This reads raw WAL bytes rather than decoding framed records: it is a triage
scan, so a suspect is a pointer to a WAL worth reading properly, not a verdict.
Skewing toward false positives is deliberate — the failure it hunts is silent,
so missing one costs more than investigating an extra file.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys
import time

from lib import state_root
from lib.archive_read import ArchivedWal, ArchiveRefusal, in_window, logical_wals

# Assistant text appears as a tagged content block. Both spellings are matched
# because the enum's serialized form differs across record versions.
TEXT_BLOCK = re.compile(r'"type"\s*:\s*"text"|"Text"\s*:')
OUTPUT_TOKENS = re.compile(r'"output_tokens"\s*:\s*(\d+)')
COMPLETED = re.compile(r'"Completed"|"reason"\s*:\s*"completed"')

# A tool-only turn legitimately carries no text, so it is not a suspect.
TOOL_CALL = re.compile(r'"tool_call_id"|"ToolCall"')

# One record and its payload. The trailing lookahead stops at the next record so a
# step's usage and content cannot be read from a neighbour.
RECORD = re.compile(
    r'"type"\s*:\s*"(run_started|model_step_finished|run_finished)".*?'
    r'(?="type"\s*:\s*"(?:run_started|step_started|model_step_finished|'
    r'tool_batch_started|tool_dispatch_intent|tool_result|tool_batch_finished|'
    r'turn_finished|run_finished|run_paused|queued|retracted)"|\Z)',
    re.S,
)

# The assistant's own content array cannot be matched with a regex: a reasoning block
# carries an ESCAPED provider payload that itself contains "content":[], so a lazy match
# stops inside the blob and reports an empty turn for a step that emitted text. Find the
# array's opening bracket and match brackets, ignoring anything inside a string.
def assistant_content(record: str) -> str | None:
    marker = record.find('"assistant_message"')
    if marker < 0:
        return None
    key = record.find('"content"', marker)
    if key < 0:
        return None
    start = record.find("[", key)
    if start < 0:
        return None
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(record)):
        char = record[index]
        if escaped:
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == '"':
            in_string = not in_string
        elif not in_string:
            if char == "[":
                depth += 1
            elif char == "]":
                depth -= 1
                if depth == 0:
                    return record[start : index + 1]
    return None


def scan(
    path: pathlib.Path, min_output_tokens: int, seen: dict[str, int]
) -> tuple[int, int] | None:
    """Return (output_tokens, assistant_blocks) for the first suspect episode.

    Correlation is the whole job. An earlier version matched the four patterns against
    the WHOLE FILE, which cannot distinguish assistant output from the user's own prompt:
    a lineage whose input carried a text block ratified itself as healthy, and the
    text-block control -- added to prove the scan was not vacuous -- ratified it too,
    because it fired on that same input. Measured against production, that predicate hid
    all 63 real suspects and reported a clean sweep of 16,101 files.

    So this walks episodes: an assistant message counts only when it is the assistant's,
    and usage counts only when it belongs to the episode being judged.

    Records which patterns matched anything at all in `seen`, so the caller can tell a
    quiet corpus from a pattern that no longer matches.
    """
    try:
        raw = path.read_bytes().decode("utf-8", "replace")
    except OSError:
        return None

    episode_tokens = 0
    episode_blocks = 0
    episode_open = False
    suspect: tuple[int, int] | None = None

    for match in RECORD.finditer(raw):
        kind = match.group(1)
        if kind == "run_started":
            episode_open = True
            episode_tokens = 0
            episode_blocks = 0
            continue
        if not episode_open:
            continue
        if kind == "model_step_finished":
            body = match.group(0)
            blocks = assistant_content(body)
            if blocks is not None:
                if TEXT_BLOCK.search(blocks):
                    seen["text_block"] += 1
                    episode_blocks += 1
                if TOOL_CALL.search(blocks):
                    seen["tool_call"] += 1
                    episode_blocks += 1
            tokens = OUTPUT_TOKENS.search(body)
            if tokens:
                seen["output_tokens"] += 1
                episode_tokens += int(tokens.group(1))
            continue
        if kind == "run_finished":
            if COMPLETED.search(match.group(0)):
                seen["completed"] += 1
                if episode_tokens >= min_output_tokens and episode_blocks == 0:
                    suspect = suspect or (episode_tokens, 0)
            episode_open = False

    return suspect


# Where the module keeps its durable state, newest layout first. Both are listed because a
# tree that has not been moved yet is still valid, and a scan that silently picked one after a
# move would report a clean sweep over a partial history -- the failure this script exists to
# rule out. When both exist the run stops and asks, rather than guessing. The first is the
# daemon's own default rule (BROCA_STATE_ROOT, else the data home).
DEFAULT_STATE_ROOTS = (
    state_root.default_state_root(),
    pathlib.Path.home() / ".local/share/cortexkit/run/broca-state",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=float, default=24.0)
    parser.add_argument("--min-output-tokens", type=int, default=20)
    parser.add_argument(
        "--state-root",
        default=None,
        help="defaults to the first existing candidate in DEFAULT_STATE_ROOTS",
    )
    args = parser.parse_args()

    if args.state_root is not None:
        root = pathlib.Path(args.state_root)
        if not root.exists():
            print(f"state root not found: {root}", file=sys.stderr)
            return 2
    else:
        existing = [c for c in DEFAULT_STATE_ROOTS if c.exists()]
        if not existing:
            tried = ", ".join(str(c) for c in DEFAULT_STATE_ROOTS)
            print(f"no state root found; tried: {tried}", file=sys.stderr)
            return 2
        if len(existing) > 1:
            # Both paths present means a move left the old tree behind, and picking either
            # one silently would scan a partial history while reporting a full sweep.
            found = ", ".join(str(c) for c in existing)
            print(
                f"ambiguous state root, found several: {found}\n"
                "pass --state-root to say which one to scan",
                file=sys.stderr,
            )
            return 2
        root = existing[0]
        print(f"scanning state root: {root}", file=sys.stderr)

    cutoff = time.time() - args.hours * 3600
    scanned = 0
    skipped = 0
    unreadable = 0
    suspects = []
    oldest = None
    newest = None
    seen = {"completed": 0, "output_tokens": 0, "text_block": 0, "tool_call": 0}

    warned = set()
    # Resolve wal/ and wal-ephemeral/ before windowing. Only a tombstone matching
    # the file's lineage ID removes it, so an older session at the same key cannot hide it.
    for wal in logical_wals(root / "wal"):
        try:
            if not in_window(wal, cutoff, warned):
                skipped += 1
                continue
            mtime = None if isinstance(wal, ArchivedWal) else wal.stat().st_mtime
        except OSError:
            # COUNTED, not swallowed. `skipped` below counts files OUTSIDE the window -- a
            # deliberate filter. A WAL whose stat fails is a file this sweep could not look at,
            # and reporting "no suspects" over a population that quietly shrank is the failure
            # this script exists to prevent.
            unreadable += 1
            continue
        if mtime is not None:
            oldest = mtime if oldest is None else min(oldest, mtime)
            newest = mtime if newest is None else max(newest, mtime)
        scanned += 1
        hit = scan(wal, args.min_output_tokens, seen)
        if hit:
            suspects.append((wal, hit[0]))

    # Report the window in absolute terms and say what was left out. The window is
    # anchored on the moment of the run, so "the last 24h" at 22:00 excludes most of
    # the previous calendar day -- and a clean result over a window nobody can see the
    # edges of reads as "this is not happening" when it may only mean "not here".
    # The failure this hunts is silent by nature, so an unstated scope is the one thing
    # that would make a clean report worthless.
    def stamp(value: float) -> str:
        try:
            return time.strftime("%Y-%m-%d %H:%M", time.localtime(value))
        except (OSError, OverflowError, ValueError):
            # Windows localtime refuses pre-epoch cutoffs from wide scan windows.
            # Formatting the scope must not prevent reporting the files examined.
            return f"Unix timestamp {value:.0f}"

    print(
        f"scanned {scanned} WAL(s) written since {stamp(cutoff)}"
        f" ({args.hours:g}h window), skipped {skipped} older"
    )
    if oldest is not None:
        print(f"  covering live-file activity from {stamp(oldest)} to {stamp(newest)}")
    if unreadable:
        # REFUSE. Unlike `skipped`, this is not a window the caller chose -- it is a part of the
        # tree the sweep could not see, and a clean verdict over it would be a false all-clear.
        print(
            f"  REFUSING: {unreadable} WAL(s) could not be stat'd, so this sweep covered less"
            " than the tree and a clean result cannot be trusted",
            file=sys.stderr,
        )
        return 2
    if skipped:
        print(
            f"  a clean result says nothing about the {skipped} older WAL(s);"
            f" widen --hours to include them"
        )
    # A clean report is only worth what the patterns are worth. These are matched
    # against raw bytes, so a record-format change does not raise an error here -- it
    # silently stops matching, and a detector that matches nothing reports exactly what
    # a healthy tree reports. The corpus supplies its own control: any real tree holds
    # completed runs that billed output and produced text, so a pattern finding none of
    # those across the whole scan has stopped working, whatever the tree looks like.
    #
    # Only patterns every scanned file should reach can serve as controls. The tool-call
    # check is deliberately excluded: it is reached only after the text check returns, so
    # it sees the narrow population of turns that produced no text at all. Requiring it to
    # match would fail on healthy corpora -- a control has to be sound before it is strict,
    # or it trains the reader to ignore it.
    # Scanning nothing is not a clean result. Every count below is zero over an empty
    # corpus, including the ones that exist to catch a broken scan -- so the controls
    # never run, no pattern is ever questioned, and the run reports the same "nothing
    # found" a healthy scan does. A moved state root or a window with no traffic in it
    # both land here, and neither says anything about whether the failure is occurring.
    if not scanned:
        print(
            "\nNO WALs EXAMINED — this run proves nothing.\n"
            "Every check below is vacuous over an empty corpus. Widen --hours, or "
            "confirm --state-root points at a tree that has WALs in it.",
            file=sys.stderr,
        )
        return 2

    if scanned:
        controls = ("completed", "output_tokens", "text_block")
        dead = [name for name in controls if seen[name] == 0]
        if dead:
            print(
                f"\nPATTERNS MATCHED NOTHING across {scanned} file(s): {', '.join(sorted(dead))}",
                file=sys.stderr,
            )
            print(
                "Refusing to report a result: these patterns should match in any real\n"
                "corpus, so a zero means the scan no longer reads what it is scanning.\n"
                "Check them against the current WAL record shape before trusting output.",
                file=sys.stderr,
            )
            return 2

    if not suspects:
        print("no completed runs with billed output and no assistant content")
        return 0

    print(f"\n{len(suspects)} SUSPECT(S) — completed, billed output, no content:")
    for wal, tokens in sorted(suspects, key=lambda s: -s[1]):
        print(f"  {tokens:>7} output tokens  {wal}")
    print("\nRead these WALs directly before concluding; this scan is triage.")
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ArchiveRefusal as error:
        print(f"REFUSING: {error}", file=sys.stderr)
        sys.exit(2)
