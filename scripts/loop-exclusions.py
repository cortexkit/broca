#!/usr/bin/env python3
"""Distill the loop ledger into the compact exclusion list hunters actually need.

A hunter must know two things: which sites are already fixed (so it does not
re-report them) and which classes are closed. It does not need the full
mechanism prose — that is the reviewer's record. Inlining the whole ledger grew
the hunt prompt past a worker's context budget (117 KB at round 74, which failed
at session start), so the prompt embeds this digest instead.

Usage: scripts/loop-exclusions.py [ledger-path] > digest.md
"""

import re
import sys
from pathlib import Path

# The ledger lives under .cortexkit/, which .gitignore excludes -- it is a local working
# record, not repository content, and it does not survive a clone. This script is tracked and
# the file it reads is not, so a fresh checkout gets "ledger not found" and exit 1 rather than
# a silently empty exclusion list. That refusal is the intended behaviour: an empty list would
# send a round hunting ground that is already swept.
# The ledger is internal: it lives under .cortexkit/internal-docs/ (gitignored, backed up by
# Engram), per the workspace's layout convention. Edits to it are not commits.
DEFAULT_LEDGER = Path(".cortexkit/internal-docs/ledgers/loop-ledger.md")
# Sentences that carry a standing constraint rather than narrative detail.
KEEP = re.compile(
    r"(do not (re-)?(report|flag|raise|re-open|reopen)|do not \"verify\"|class CLOSED|CLASS CLOSED|"
    r"now swept|NOW SWEPT|accepted residual|deliberate residual|open hunt seam|open seam|banked|"
    r"BUILD PIN|CALIBRATION STANDING|INSTRUMENT NOTE)",
    re.IGNORECASE,
)
# Entries recording something already known outside a round. These are short and
# entirely load-bearing, so they are kept whole rather than filtered by sentence:
# a hunter that misses one spends its round rediscovering known work.
# REFUTED belongs here and is the most load-bearing member: a finding investigated and
# DISPROVEN. Without it in the digest a later round re-discovers the same claim and reaches
# the same wrong conclusion, having paid for the investigation twice. It is kept whole
# because the refutation only transfers with its evidence -- "not a bug" alone reads as an
# opinion a hunter is free to overturn, while the source predicate and the vendor quote that
# killed it are what actually stop the re-find.
VERBATIM_PREFIX = re.compile(
    r"^\[(BANKED|ACCEPTED|CLASS SWEEP|BUILD PIN|CALIBRATION|INSTRUMENT|REFUTED|DECLINED"
    r"|STANDING)[^\]]*\]"
)
# Shipped features and releases. They are ledger history, not hunt exclusions, so
# they are skipped deliberately — declared here rather than falling through the
# filter, which would make an intentional skip indistinguishable from a loss.
# SETTLED records how a delivery was verified and STANDING records a practice rule. Both are
# ledger history addressed to ME, not ground a hunter must avoid, so they are skipped for the
# same reason [X] is -- declared here rather than falling through, because an intentional skip
# and a lost entry look identical in the output otherwise. The drop-detector refused these on
# first write, which is what sent me here instead of letting them vanish.
# STANDING was here too, and that was wrong for one kind of entry. A practice rule addressed
# to me ("use quickcheck", "diff against the merge base") is genuinely ledger history. But a
# STANDING entry that names a DEFECT CLASS is exactly what a hunter needs: r121 recorded "a
# cursor that advances past content it never emitted", and r133 independently found the third
# member of that class without ever being shown the rule, because this filter removed it. The
# hunter did the work twice-over -- the find was new and correct, but it could not say which
# class it belonged to, and I could not tell a fresh instance from a re-discovery without
# checking myself. Class descriptions travel; practice rules do not.
# PROCESS is here for the same reason SETTLED is, and it was added after I reached for the tag
# FIVE times and got a refusal each time. A [PROCESS] entry records something about how I ran
# the round -- a denominator I supplied that was stale, a steer that never reached the prompt --
# which is addressed to me and is not ground a hunter must avoid. The refusals were CORRECT
# (an unknown tag must not be silently dropped), but a tag I keep reaching for is a category
# the vocabulary is missing, not a mistake to keep re-editing out of the prose.
IGNORED_PREFIX = re.compile(r"^\[(X|PROCESS|SETTLED [^\]]+)\]")
# Catches a round finding written in some other shape (a markdown heading, a bold run) so a
# new format is a loud drop rather than a silent omission.
NEW_SHAPE_ROUND = re.compile(r"^[#*\s]*R\d+\b|^\*\*(BUG|PERF|SECURITY)\b", re.IGNORECASE)
# A header introducing prose carries no finding of its own. Match the following
# paragraphs independently and distinguish an intentional header skip from a loss.
HEADER_ONLY = re.compile(r"^\[R\d+[A-Za-z0-9-]*\]\s+\w+[^—]*:\s*$")
# Commit references may be parenthesized or bare; outcomes may contain several words.
# Round tags may have alphanumeric suffixes. A site/description split is optional,
# since a rejected entry may explain its disposition without naming a site.
# Prose entries need no commit hash and lane matching is case-insensitive.
# Followup and dormant lanes record work or intentionally open ground.
# Unknown lane words must stop extraction instead of silently dropping an entry.
PROSE_FINDING = re.compile(
    r"^\[X\]\s+R(\d+[A-Za-z0-9-]*)\s+(perf|bug|followup|dormant)\b", re.IGNORECASE
)

# Require file:line so a bare crate name cannot suppress an entire crate.
# Rust, Python, and shell source locations are valid sites.
PATH_TOKEN = re.compile(r"[\w./-]+\.(?:rs|py|sh):\d+")

FINDING = re.compile(
    r"^\[R(\d+[A-Za-z0-9-]*)\]\s+(perf|bug)\s+([A-Z]+(?:\s+[A-Z]+)*)\s*"
    r"(\([^)]*\)|[0-9a-f]{7,}\S*)?\s*—\s*([^—]+)(?:—|$)"
)


def sentences(text: str):
    return re.split(r"(?<=[.!?])\s+", text)


def main() -> int:
    ledger = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_LEDGER
    if not ledger.exists():
        print(f"error: ledger not found at {ledger}", file=sys.stderr)
        return 1

    fixed: list[str] = []
    standing: list[str] = []
    dropped_markers: list[str] = []
    siteless: list[str] = []

    # Prose entries put their site after an '[X] R<n> <lane>' opener. Extract those
    # separately from the older '[R<n>] lane FIXED (sha) — site — description' form.
    raw_lines = ledger.read_text(encoding="utf-8").splitlines()
    # An unsupported prose lane matches neither parser, so refuse it explicitly.
    # Silently skipping it would omit closed ground from the digest.
    UNKNOWN_LANE = re.compile(r"^\[X\]\s+R(\d+[A-Za-z0-9-]*)\s+([A-Za-z-]+)")
    mislaned: list[str] = []
    round_numbers: list[str] = []
    for index, raw in enumerate(raw_lines):
        stripped = raw.strip()
        opener = PROSE_FINDING.match(stripped)
        if not opener:
            stray = UNKNOWN_LANE.match(stripped)
            if stray:
                mislaned.append(f"R{stray.group(1)} {stray.group(2)}")
            continue
        round_no, lane = opener.groups()
        round_numbers.append(round_no)
        # The site is the first path-like token in the two lines after the opener; entries
        # occasionally carry a lead-in clause before naming it.
        window = " ".join(raw_lines[index + 1 : index + 3])
        site = PATH_TOKEN.search(window)
        if site:
            # THE DISPOSITION, not a blanket "(fixed)". This printed "(fixed)" for every entry
            # including declined and reported-not-fixed ones, which asserts an outcome the
            # parser does not know. The failure mode is worse than re-reporting: a hunter told
            # a site is fixed, who then finds the code unchanged, reports THAT -- "the fix did
            # not work" -- and the round is spent on a defect that was a deliberate decision.
            #
            # Read from the entry's own prose, which already spells these in caps by habit.
            # Anything unmarked stays "fixed", which is the majority and the historical meaning.
            disposition = "fixed"
            haystack = (stripped + " " + window).upper()
            if "NOT FIXED" in haystack or "REPORTED, NOT" in haystack:
                disposition = "reported, deliberately not fixed"
            elif "HONEST EMPTY" in haystack:
                # An empty investigation names where it looked, not a modified site.
                disposition = "investigated; no qualifying finding, site unchanged"
            elif "DECLINED" in haystack:
                disposition = "declined; the site is open by decision"
            elif lane.lower() == "dormant" or "DORMANT" in haystack:
                disposition = "dormant; open by decision, see the reversing threshold"
            fixed.append(f"- R{round_no} {lane}: {site.group(0)} ({disposition})")
        else:
            siteless.append(f"R{round_no} {lane}")

    # Missing sites are reported, while unsupported lanes refuse extraction because
    # they reach no part of the digest. Missing interior rounds are reported rather
    # than refused: a round can legitimately produce no entry.
    recorded_rounds = {int(r) for r in round_numbers if r.isdigit()}
    if recorded_rounds:
        span = range(min(recorded_rounds), max(recorded_rounds) + 1)
        absent = [r for r in span if r not in recorded_rounds]
        if absent:
            print(
                f"loop-exclusions: rounds with no ledger entry at all: "
                f"{', '.join(f'R{r}' for r in absent)}. If those rounds ran, their closed "
                f"sites are missing from this digest and a hunter can re-find them.",
                file=sys.stderr,
            )

        # Interior gap checks cannot detect missing trailing rounds. Always report
        # the highest recorded round so the caller can compare with its current work.
        print(
            f"loop-exclusions: highest round recorded is R{max(recorded_rounds)}. "
            f"If the round you are about to dispatch is more than one past it, the rounds in "
            f"between closed sites this digest does not carry.",
            file=sys.stderr,
        )

    if mislaned:
        print(
            f"loop-exclusions: {len(mislaned)} '[X] R<n> <lane>' entries name a lane that is "
            f"neither perf nor bug, so they match no parser and reach no part of the digest: "
            f"{', '.join(mislaned)}. Relabel them to the lane that produced the finding and "
            f"say DECLINED or DORMANT in the prose when that is the disposition.",
            file=sys.stderr,
        )
        return 2

    if siteless:
            print(
                f"loop-exclusions: {len(siteless)} fixed findings name no file:line, so a hunter "
                f"cannot check itself against them: {', '.join(siteless[:6])}"
                + (" ..." if len(siteless) > 6 else "")
                # Sites are read after the opener, so a path on the opener is misplaced.
                + "\n  (the site is read from the TWO LINES AFTER the '[X] R<n> <lane>' opener, "
                "so a path on the opener line itself does not count)",
                file=sys.stderr,
            )

    for raw in raw_lines:
        line = raw.strip()
        if not line:
            continue
        if VERBATIM_PREFIX.match(line):
            standing.append(f"- {line}")
            continue
        if IGNORED_PREFIX.match(line) or HEADER_ONLY.match(line):
            continue
        produced = False
        match = FINDING.match(line)
        if match:
            round_no, lane, verb, _sha, site = match.groups()
            fixed.append(f"- R{round_no} {lane}: {site.strip()} ({verb.lower()})")
            produced = True
        for sentence in sentences(line):
            if KEEP.search(sentence) and len(sentence) < 400:
                standing.append(f"- {sentence.strip()}")
                produced = True
        # A bracketed line is a deliberate entry, so contributing nothing means this
        # filter just ate one. Prose lines are reviewer commentary and are meant to be
        # dropped, so they stay quiet — flagging them too buries the real losses in
        # noise, and a warning nobody reads is the same as no warning.
        if not produced and line.startswith("["):
            dropped_markers.append(line)
        # Non-bracketed round headings and bold findings are deliberate entries too;
        # refuse an unsupported shape rather than silently omit it.
        elif not produced and NEW_SHAPE_ROUND.search(line):
            dropped_markers.append(line)

    # Refuse BEFORE writing anything. The documented invocation redirects stdout to a
    # file, and a shell redirect creates that file whatever the exit code is -- so a
    # refusal that printed first would still leave a well-formed, empty digest on disk
    # for the next reader to pick up. An exit code only protects a caller that checks it;
    # not producing the artifact protects every caller.
    #
    # Zero of both kinds means the extraction rules no longer match the ledger's shape,
    # not that there is nothing to exclude: a ledger that parsed always yields entries.
    # The per-entry drop warning below cannot cover this, because total failure leaves no
    # marked entries to warn about -- the more completely it fails, the quieter it gets.
    if not fixed and not standing:
        print(
            "loop-exclusions: REFUSING an empty digest — a ledger that parsed correctly "
            "always yields entries, so this means the extraction rules no longer match "
            "the ledger's shape. A hunter given this would re-report everything.",
            file=sys.stderr,
        )
        return 2

    # Decide every refusal before writing stdout: redirected partial output could
    # otherwise be mistaken for a complete digest.
    if dropped_markers:
        print(
            f"loop-exclusions: REFUSING to emit a digest — {len(dropped_markers)} marked entr"
            f"{'y' if len(dropped_markers) == 1 else 'ies'} matched no rule:",
            file=sys.stderr,
        )
        for line in dropped_markers:
            print(f"  {line[:120]}", file=sys.stderr)
        print(
            "loop-exclusions: TWO CAUSES, and the fix differs.\n"
            "  (a) It is an ENTRY in the wrong shape. Rewrite it as '[X] R<n> <lane>' with the\n"
            "      site on one of the next two lines. Unsupported headings and bold entries\n"
            "      are not parsed as findings.\n"
            "  (b) It is PROSE that begins with a round token. Reword so the line does not\n"
            "      START with R<n>. Deliberate entries must use a supported shape.\n"
            "loop-exclusions: fix the entry shape or teach the parser, then re-run. Nothing was\n"
            "  written, so a stale or partial digest cannot be mistaken for a complete one.",
            file=sys.stderr,
        )
        return 2

    # Keep siteless entries in a separate prose block, not the mechanically checkable
    # fixed-site list. Reporting them only on stderr would still omit closed ground.
    siteless_block = (
        [
            "\n## Closed findings with no checkable site — read these, they are not re-reportable\n",
            "\n".join(f"- {entry} (closed; the ledger entry names no file:line)" for entry in siteless),
        ]
        if siteless
        else []
    )

    digest = "\n".join(
        [
            "# Already covered — do not re-report any of these\n",
            "## Fixed sites (a finding at one of these is a repeat)\n",
            "\n".join(fixed),
            "\n## Standing constraints, closed classes, and open seams\n",
            # Preserve order, drop duplicates.
            "\n".join(dict.fromkeys(standing)),
        ]
        + siteless_block
    )
    print(digest)

    # Report the delta on stderr so it reaches the operator without entering the
    # digest. A filter that quietly turns N entries into fewer is indistinguishable
    # from one that had nothing to drop.
    print(
        f"loop-exclusions: {len(fixed)} fixed sites, {len(standing)} standing entries",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
