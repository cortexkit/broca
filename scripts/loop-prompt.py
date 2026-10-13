#!/usr/bin/env python3
"""Assemble a hunt prompt for one THE LOOP round.

The prompt is the protocol's template body with the exclusion digest spliced in where
the template says the ledger goes. Both templates live in the protocol file so the rules
and the shape stay in one place; this only transcribes them.

Why this is a script rather than a sed line: the launcher used to slice the templates by
hard-coded line numbers, which are silently wrong the moment the protocol file is edited.
Editing the protocol is the normal case -- it is how a round's lessons become the next
round's rules -- so the extraction has to find its own boundaries. A prompt that lost its
task text or its exclusions would still look like a prompt, and the round would be spent
before anyone noticed.

Usage: loop-prompt.py <perf|bug> <digest-path> [protocol-path]
Writes the assembled prompt to stdout. Refuses rather than emitting a prompt it cannot
vouch for.
"""

import pathlib
import os
import sys

MARKER = "<ledger contents>"
HEADERS = {
    "perf": "## Mason prompt template — PERF",
    "bug": "## Mason prompt template — BUG",
}
# The protocol is internal: it lives under .cortexkit/internal-docs/ (gitignored, backed up by
# Engram), per the workspace's layout convention.
DEFAULT_PROTOCOL = pathlib.Path(".cortexkit/internal-docs/ledgers/loop-protocol.md")
# A template that has lost its task text or its output shape is the failure this script
# exists to catch, so both are required in the extracted body.
REQUIRED = ("TASK: Find exactly ONE", "OUTPUT (exactly this shape)")


def extract(lines: list[str], header: str) -> list[str]:
    """Return the fenced body under `header`, found by structure rather than position."""
    try:
        start = next(n for n, line in enumerate(lines) if line.startswith(header))
    except StopIteration:
        raise SystemExit(f"loop-prompt: no section titled {header!r}")
    try:
        open_fence = next(n for n in range(start, len(lines)) if lines[n].strip() == "```")
        close_fence = next(
            n for n in range(open_fence + 1, len(lines)) if lines[n].strip() == "```"
        )
    except StopIteration:
        raise SystemExit(f"loop-prompt: unterminated code fence under {header!r}")
    return lines[open_fence + 1 : close_fence]


def main() -> int:
    if len(sys.argv) < 3:
        raise SystemExit("usage: loop-prompt.py <perf|bug> <digest-path> [protocol-path]")
    lane = sys.argv[1]
    if lane not in HEADERS:
        raise SystemExit(f"loop-prompt: lane must be perf or bug, got {lane!r}")

    digest_path = pathlib.Path(sys.argv[2])
    if not digest_path.exists():
        raise SystemExit(f"loop-prompt: digest not found at {digest_path}")
    digest = digest_path.read_text().rstrip("\n")
    # An empty digest means the hunter is told nothing is covered, and it will spend the
    # round re-finding swept ground. That is the expensive failure, so refuse it.
    if not digest.strip():
        raise SystemExit(f"loop-prompt: digest at {digest_path} is empty")
    # AND NON-EMPTY IS NOT THE SAME AS "IS A DIGEST". The emptiness check above asks
    # whether there is text and claims the file is a digest, which are different
    # questions -- loop-exclusions.py REFUSES by printing its reason, so a caller who
    # redirects stderr into the digest (`> d.txt 2>&1`) writes a file that is decidedly
    # not empty and contains no entries at all. That is how this fired: a refusal
    # message became the exclusions section, and the round would have re-investigated
    # every closed finding while every gate reported fine.
    #
    # So check for the thing that makes it a digest. The floor is deliberately low --
    # it is here to separate "a digest" from "some other text", not to assert a
    # particular size, and it must not need raising every time the ledger grows.
    entries = sum(1 for line in digest.splitlines() if line.startswith("- R"))
    if entries < 10:
        raise SystemExit(
            f"loop-prompt: {digest_path} holds {entries} exclusion entries, which is not a "
            f"digest. Regenerate with `python3 scripts/loop-exclusions.py > {digest_path}` "
            f"and KEEP STDERR SEPARATE -- if the generator refused, its reason is on stderr "
            f"and merging it in produces exactly this file."
        )

    protocol = pathlib.Path(sys.argv[3]) if len(sys.argv) > 3 else DEFAULT_PROTOCOL
    # A SURFACE STEER MUST REACH THE PROMPT, NOT THE WORK ITEM'S acceptance_criteria.
    # Learned by accusing two consecutive workers of ignoring a named surface: the prompt
    # they receive says "find ONE bug in this codebase" and never mentioned the surface,
    # because the steer lived in a separate field. They followed their instructions; I did
    # not deliver mine. BROCA_LOOP_SURFACE is spliced directly above the TASK line, which is
    # the only text a worker is guaranteed to read.
    # THE SURFACE DECISION IS MANDATORY, because an OPTIONAL scope is one I forget.
    #
    # An unset variable and a deliberate unscoped round were indistinguishable, so
    # omitting the scope silently produced an unscoped prompt -- and I then scored two
    # workers down for "ignoring" a gate that lived in the work-graph's
    # acceptance_criteria field and never reached them. One of those deductions was 23
    # points on a delivery that found false verification evidence in a release card.
    # The comment below this one already records the first two instances; adding the
    # mechanism did not stop me, because using it stayed optional.
    #
    # `any` is a real answer and the common one. What is refused is SILENCE, so the
    # decision is made every round rather than defaulted by forgetting.
    surface = os.environ.get("BROCA_LOOP_SURFACE")
    if surface is None:
        raise SystemExit(
            "loop-prompt: REFUSING -- BROCA_LOOP_SURFACE is unset.\n"
            "  A scope written anywhere but this variable does NOT reach the worker:\n"
            "  the work graph's acceptance_criteria is a separate field they never see.\n"
            "  Set it to the surface to hunt, or to 'any' for a deliberately unscoped round:\n"
            "    BROCA_LOOP_SURFACE='crates/ only; no findings in scripts/' scripts/loop-prompt.py ...\n"
            "    BROCA_LOOP_SURFACE=any scripts/loop-prompt.py ..."
        )
    surface = "" if surface.strip() in {"any", "ANY"} else surface.strip()
    if not protocol.exists():
        raise SystemExit(f"loop-prompt: protocol not found at {protocol}")

    body = extract(protocol.read_text().splitlines(), HEADERS[lane])

    missing = [needle for needle in REQUIRED if not any(needle in l for l in body)]
    if missing:
        raise SystemExit(
            f"loop-prompt: {lane} template is missing {missing} -- "
            "the extraction found the wrong region"
        )
    if not any(MARKER in line for line in body):
        raise SystemExit(
            f"loop-prompt: {lane} template has no {MARKER} line, so the exclusions "
            "would be dropped silently"
        )

    out = []
    for line in body:
        if MARKER in line:
            out.append(digest)
        else:
            # Splice the surface ABOVE the TASK line rather than below it. A constraint that
            # follows the task reads as a hint; one that precedes it scopes the task.
            if surface and line.lstrip().startswith("TASK: Find exactly ONE"):
                out.append("SCOPE FOR THIS ROUND, AND IT REPLACES \"this codebase\" BELOW:")
                out.append(surface)
                out.append("")
                out.append(
                    "Report on THAT surface, or report an HONEST EMPTY on it naming what you"
                )
                out.append(
                    "checked and how. Do NOT substitute a finding from elsewhere in the tree:"
                )
                out.append(
                    "a named surface is named because nobody else has looked at it."
                )
                out.append("")
            out.append(line)
    sys.stdout.write("\n".join(out) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
