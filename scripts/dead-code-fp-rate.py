#!/usr/bin/env python3
"""Measure the FALSE-POSITIVE RATE of AFT's dead-code headline.

Existed first as a throwaway heredoc, which is exactly how the wrong version comes back:
a rule with no home cannot be corrected, only re-typed. The before/after comparison AFT
asked for is only meaningful if both sides run provably the same code, so it lives here.

USAGE
    aft_inspect(sections="dead_code")            # copy the headline symbols
    scripts/dead-code-fp-rate.py symbols.txt     # one `path::symbol` per line

Reads the headline list (NOT the "test-only usage" bucket, which is already an honest
classification) and reports, per symbol, whether any CALL-SHAPED reference to it exists
anywhere in crates/.

WHAT THIS INSTRUMENT CANNOT DECIDE, stated because a verdict without its limits is a vote:

  * For a METHOD it cannot resolve the receiver's type. `.tools()` on an unrelated struct
    counts as a hit for `capability.rs::tools`. So the FALSE-POSITIVE count is an UPPER
    BOUND and the TRUE-DEAD count is a LOWER BOUND. The error runs one way, which is what
    makes the bound usable.
  * It matches call syntax, not semantics. A call inside `#[cfg(never)]` still counts.

WHY CALL SYNTAX RATHER THAN THE BARE IDENTIFIER: the first version of this matched the
identifier and reported 27 of 30 "has references" — measuring same-named locals, since
`session`, `empty`, `version` and `log` are ordinary words. That run was not a weaker
measurement of the right thing, it was a measurement of a different thing.
"""

import pathlib
import re
import sys

CRATES = pathlib.Path("crates")


def load_sources() -> dict[pathlib.Path, list[str]]:
    return {p: p.read_text(errors="ignore").splitlines() for p in CRATES.rglob("*.rs")}


def in_test_mod(lines: list[str], index: int) -> bool:
    """Whether `index` sits below a `mod tests` opener in the same file.

    Deliberately crude: it does not track brace depth, so a file with a `mod tests` block
    followed by more production code would misclassify the tail. That has not occurred in
    this tree, and the alternative is parsing Rust, which is the job of the tool being
    measured rather than of the thing measuring it.
    """
    return any(re.match(r"\s*mod tests", line) for line in lines[:index])


def classify(symbol: str, defn_file: str, sources) -> list[tuple[str, str, str]]:
    # `.sym(`, `::sym(`, or a bare `sym(` — a CALL, not a mention.
    call = re.compile(rf"(\.|::|\b){re.escape(symbol)}\s*\(")
    definition = re.compile(rf"\s*(pub )?(async )?fn {re.escape(symbol)}\b")
    hits = []
    for path, lines in sources.items():
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith("//") or definition.match(line):
                continue
            if not call.search(line):
                continue
            same_file = str(path) == defn_file
            test = "/tests/" in str(path) or in_test_mod(lines, i)
            kind = (
                "own-file/test" if same_file and test
                else "own-file/prod" if same_file
                else "other/test" if test
                else "other/PROD"
            )
            hits.append((kind, f"{path}:{i + 1}", stripped[:64]))
    return hits


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    listing = pathlib.Path(sys.argv[1])
    if not listing.is_file():
        print(f"dead-code-fp-rate: no symbol list at {listing}", file=sys.stderr)
        return 2
    if not CRATES.is_dir():
        print("dead-code-fp-rate: run from the repository root", file=sys.stderr)
        return 2

    entries = []
    for raw in listing.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "::" not in line:
            print(f"dead-code-fp-rate: cannot parse {line!r} (want path::symbol)", file=sys.stderr)
            return 2
        path, _, symbol = line.rpartition("::")
        entries.append((path.strip(), symbol.strip()))

    # An empty list would report a flawless 0/0 rate, which reads as good news.
    if not entries:
        print("dead-code-fp-rate: cannot compare — the symbol list is empty", file=sys.stderr)
        return 2

    sources = load_sources()
    if not sources:
        print("dead-code-fp-rate: cannot compare — no .rs files under crates/", file=sys.stderr)
        return 2

    true_dead = false_pos = 0
    print(f"{'VERDICT':11} {'SYMBOL':44} CALL SITES")
    for defn_file, symbol in entries:
        hits = classify(symbol, defn_file, sources)
        if not hits:
            true_dead += 1
            print(f"{'TRUE-dead':11} {symbol:44} none")
            continue
        false_pos += 1
        kinds = sorted({k for k, _, _ in hits})
        print(f"{'FALSE-POS':11} {symbol:44} {len(hits)} call(s)  {kinds}")
        for kind, loc, text in hits[:2]:
            print(f"{'':11} {kind:16} {loc.replace('crates/', '')}  {text}")

    total = true_dead + false_pos
    print(f"\nTRUE-dead {true_dead}   FALSE-POS {false_pos}   of {total}"
          f"   ({100 * false_pos / total:.1f}% headline false-positive, UPPER bound)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
