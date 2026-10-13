#!/usr/bin/env python3
"""Find comments that state a rule in full above a condition that implements less.

THREE INSTANCES IN ONE WEEK is what motivated this, and they were found one at a time by
unrelated audits rather than by looking:

  1. sweep-empty-wals.py  "A DAEMON THAT IS RUNNING IS A WRITER, DESCRIPTORS OR NOT"
                          above `if running AND root == <hardcoded default>` -- so a daemon on
                          any other state root swept clean, deleting an open WAL.
  2. main.rs              LkgErrorClass promises "authoritative credential replies never lose
                          to an older cached token", and the eviction matched ONE variant, so
                          a revoked handle survived a vault outage.
  3. main.rs              `NeedsReauth` named one INSTANCE of its class, which is why the
                          mapping carried only that instance.

The shape is not carelessness. Someone states the rule correctly, then writes the condition for
the case in front of them, and the comment is what makes the narrowing invisible afterwards --
a reader checking the code against its comment sees the rule and stops.

WHY THE CONTROL IS MANDATORY HERE. My first three attempts at this detector returned zero across
the whole tree, and zero was wrong every time: the window was too short, because the rule heads a
comment BLOCK and the condition sits past an intervening setup statement. A detector for
"claims that outrun their implementation" returning a false zero would be the joke this file
exists to prevent, so it reconstructs instance 1 from git and refuses if it cannot find it.

MEASURED AND DECLINED: A SECOND SHAPE, rule-in-doc-comment versus body. The reclaim in
wal_fold.rs documented "Emptiness is decided by reading the index, never by file size" and then
called `rescan()`, the recovery scanner -- so it deleted an archive holding the only copy of a
transcript. This detector cannot see that: it pairs a rule with a CONDITION, and there the
contradiction was a call several lines into a body with no condition narrowed.

A doc/body detector was built and measured before being declined. Across crates/: 917 documented
functions, 3 matched a named-mechanism rule grammar, 2 were comparable, 1 candidate emitted and it
was a false positive. LIVE REACH OF TWO FUNCTIONS, for ~465 lines of hand-rolled Rust lexing.

The false positive is the ceiling, not a tuning failure: `the_declared_cadence_tracks_the_real_timer`
says "READ FROM the timer" and reaches CATALOG_POLL_INTERVAL two calls down -- the mechanism is
semantically present and lexically absent. PROSE NAMES A CONCEPT, CODE NAMES AN IDENTIFIER, and
they coincide by luck. Widening the grammar to close that gap is what produces noise; keeping it
narrow is what produced a reach of 2. There is no setting that is both.

Reconsider only if the doc/body shape is observed several more times. One instance is not a
population, and 3-out-of-917 is what over-fitting to it looks like from outside.

Exit 0 clean, 1 candidates found (INSPECT THEM -- they are candidates, not defects), 2 cannot
compare.
"""

import pathlib
import re
import subprocess
import sys

# A rule stated in absolute terms. Deliberately broad: the cost of a candidate is one read,
# and the cost of a miss is the class this file exists to catch.
ABSOLUTE = re.compile(r"\b(ALWAYS|NEVER|ANY|EVERY|ALL|REGARDLESS|OR NOT|MUST|CANNOT)\b")
# A condition carrying a conjunct. The conjunct is what narrows the stated rule.
NARROWING = re.compile(r"^\s*(if|\} else if|elif)\b.*(&&|\band\b)")
# Instance 1, before its fix -- the known-present specimen this detector must be able to find.
CONTROL_COMMIT = "86fc33de~1"
CONTROL_PATH = "scripts/sweep-empty-wals.py"
# The rule and its condition are separated by the rest of the comment block plus any setup
# statement. Twelve lines covers instance 1 at seven; shorter windows silently found nothing.
WINDOW = 12


def scan(path: str, text: str) -> list[tuple[str, int, str, str]]:
    out = []
    lines = text.split("\n")
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not (stripped.startswith("//") or stripped.startswith("#")):
            continue
        if not ABSOLUTE.search(line):
            continue
        for j in range(i + 1, min(i + WINDOW, len(lines))):
            if NARROWING.match(lines[j]):
                out.append((path, j + 1, stripped[:70], lines[j].strip()[:70]))
                break
    return out


def main() -> int:
    root = pathlib.Path(__file__).resolve().parent.parent

    # THE CONTROL RUNS FIRST AND ITS FAILURE IS FATAL. A scan whose instrument cannot find a
    # specimen it is known to contain reports absence of evidence as evidence of absence.
    try:
        specimen = subprocess.run(
            ["git", "-C", str(root), "show", f"{CONTROL_COMMIT}:{CONTROL_PATH}"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    except (subprocess.CalledProcessError, FileNotFoundError) as error:
        print(
            f"find-narrowed-rules: CANNOT COMPARE -- the control specimen "
            f"{CONTROL_COMMIT}:{CONTROL_PATH} could not be read ({error}). Without it this scan "
            "cannot show it is able to find anything.",
            file=sys.stderr,
        )
        return 2

    if not scan("control", specimen):
        print(
            "find-narrowed-rules: CANNOT COMPARE -- the detector did not find the known instance "
            f"in {CONTROL_COMMIT}:{CONTROL_PATH}. It is blind, and a zero from it would mean "
            "nothing. Widen WINDOW or the patterns until this passes.",
            file=sys.stderr,
        )
        return 2

    targets = [p for p in root.joinpath("crates").rglob("*.rs") if "/tests/" not in str(p)]
    targets += sorted(root.joinpath("scripts").glob("*.py"))
    if not targets:
        print("find-narrowed-rules: CANNOT COMPARE -- no files matched.", file=sys.stderr)
        return 2

    hits = []
    for path in targets:
        hits += scan(str(path.relative_to(root)), path.read_text(errors="ignore"))

    print(f"find-narrowed-rules: control OK, {len(targets)} files scanned")
    if not hits:
        print("  no candidates")
        return 0

    print(f"  {len(hits)} candidate(s) -- READ EACH ONE; a candidate is not a defect")
    for path, line, rule, cond in hits:
        print(f"\n  {path}:{line}")
        print(f"    rule: {rule}")
        print(f"    cond: {cond}")
    print(
        "\n  Ask of each: does the condition implement the whole rule the comment states?\n"
        "  A rule split across ADJACENT conditions is fine -- check the following lines before\n"
        "  reporting one."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
