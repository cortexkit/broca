#!/usr/bin/env python3
"""Check that every terminal run left a usage-export fact behind.

ASTRO reconciles billing against the export lane, and nothing on broca's side blocks on
it: they poll on their own schedule, so a run that finishes without emitting its fact
produces no error, no failed request, and no waiting caller. The gap surfaces days later
as a shortfall in someone else's ledger.

The repair for a lost fact exists -- `recover_durable_work` backfills it -- but both of
its call sites fire on actor open, so it only reaches sessions that are touched again. A
crash that loses a fact is itself a reason a session goes quiet, which makes the trigger
anti-correlated with the need. This script is the check that does not depend on traffic.

It has to be a join rather than a gauge. From the run index alone, a run that never
emitted a fact looks exactly like a run that has not finished yet; only comparing against
terminal state separates them.

Exit codes: 0 clean, 1 missing facts found, 2 cannot compare.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sqlite3
import sys

from lib import state_root

# Run states that mean the run reached a terminal and therefore owes a fact. Kept as an
# explicit allowlist rather than "not still running": a state added later should make this
# check fail loudly on an unknown value rather than be swept into either side by a NOT IN.
TERMINAL_STATES = (
    "completed",
    "error",
    "max_steps",
    "budget_exhausted",
    "interrupted",
    "cancelled",
    # ADDED AFTER THE ENGINE HAD IT. TransformUnavailable has been a terminal since
    # the prompt-reduction seam shipped, and this tuple never learned it -- so a single
    # such row made the audit exit 2 for the WHOLE database, including every unrelated
    # run. The fail-loud rule above worked exactly as written; what failed is that an
    # ENUMERATION IS ITSELF A CLAIM, and nothing checked this one against the engine.
    # `--self-test` now does, so the next terminal added in Rust fails here at CI time
    # rather than the first time an operator runs the audit.
    "transform_unavailable",
    # A run sealed because its request stayed at or over the context ceiling after the
    # compaction wait. It is a durable RunFinished like any other terminal and owes a fact.
    "context_ceiling",
)

# States a run passes through without owing a fact yet. Every state the index can hold must
# appear in one of these two tuples, so that adding a terminal to the engine without
# teaching this script about it stops the audit instead of quietly shrinking its scope.
NON_TERMINAL_STATES = ("active", "paused")

# Where the module keeps its durable state, newest layout first. Both are listed because a
# tree that has not been moved yet is still valid, and picking one silently after a move
# would audit a partial history while reporting a full sweep. The first is the daemon's own
# default rule (BROCA_STATE_ROOT, else the data home).
DEFAULT_STATE_ROOTS = (
    state_root.default_state_root(),
    pathlib.Path.home() / ".local/share/cortexkit/run/broca-state",
)


def resolve_state_root(explicit: str | None) -> pathlib.Path | None:
    if explicit is not None:
        root = pathlib.Path(explicit)
        if not root.exists():
            print(f"state root not found: {root}", file=sys.stderr)
            return None
        return root

    existing = [candidate for candidate in DEFAULT_STATE_ROOTS if candidate.exists()]
    if not existing:
        tried = ", ".join(str(candidate) for candidate in DEFAULT_STATE_ROOTS)
        print(f"no state root found; tried: {tried}", file=sys.stderr)
        return None
    if len(existing) > 1:
        found = ", ".join(str(candidate) for candidate in existing)
        print(
            f"ambiguous state root, found several: {found}\n"
            "pass --state-root to say which one to audit",
            file=sys.stderr,
        )
        return None
    return existing[0]



def _self_test() -> int:
    """Prove the enumeration matches the engine, and that both outcomes still work.

    THE FIRST CHECK IS THE POINT. TERMINAL_STATES is a hand-written copy of a rule that
    lives in Rust, and it was wrong for as long as the transform seam has existed: the
    engine wrote `transform_unavailable`, this script did not know it, and one such row
    would have exited 2 for the whole database. Nothing compared the two lists, so the
    copy could only be corrected by an operator hitting it.

    So this reads the SPELLINGS OUT OF context.rs rather than restating them here. A
    second hand-written list would drift in exactly the way the first one did.
    """
    import sqlite3 as _sq

    root = pathlib.Path(__file__).resolve().parent.parent
    src = root / "crates" / "broca-core" / "src" / "context.rs"
    if not src.is_file():
        print(f"self-test CANNOT COMPARE: {src} not found", file=sys.stderr)
        return 2
    engine = set(re.findall(r'RunIndexState::\w+ => "([a-z_]+)"', src.read_text()))
    if len(engine) < 5:
        print(
            f"self-test CANNOT COMPARE: found only {len(engine)} states in {src.name}; "
            "the match arms this scrapes have probably moved",
            file=sys.stderr,
        )
        return 2
    known = set(TERMINAL_STATES) | set(NON_TERMINAL_STATES)
    missing = engine - known
    if missing:
        print(
            "self-test FAILED: the engine writes states this script does not classify: "
            f"{sorted(missing)}. Add each to TERMINAL_STATES (it owes an export fact) or "
            "to NON_TERMINAL_STATES (it does not owe one yet).",
            file=sys.stderr,
        )
        return 1
    stale = known - engine
    if stale:
        print(
            f"self-test FAILED: this script classifies states the engine no longer writes: "
            f"{sorted(stale)}. A stale entry silently widens or narrows the join.",
            file=sys.stderr,
        )
        return 1

    # And the behaviour, both arms, so the tuple edit above cannot pass while the join breaks.
    for state in sorted(TERMINAL_STATES):
        con = _sq.connect(":memory:")
        con.execute(
            "CREATE TABLE run_index (run_id TEXT, state TEXT, state_changed_ms INTEGER)"
        )
        con.execute("CREATE TABLE export_facts (run_id TEXT, segment_json TEXT)")
        # TWO runs, deliberately. The audit refuses with 2 when EITHER side is empty --
        # its own vacuity guard, since zero-missing over an empty table proves nothing. A
        # one-run fixture therefore exercises that guard instead of the anti-join, and the
        # missing-fact arm would pass for the wrong reason. The companion keeps both counts
        # non-zero so the join is what is actually under test.
        con.execute("INSERT INTO run_index VALUES ('r1', ?, 1)", (state,))
        con.execute("INSERT INTO run_index VALUES ('r2', 'completed', 1)")
        con.execute("INSERT INTO export_facts VALUES ('r1', '{}')")
        con.execute("INSERT INTO export_facts VALUES ('r2', '{}')")
        if _audit(con) != 0:
            print(f"self-test FAILED: a {state} run WITH its fact should be clean", file=sys.stderr)
            return 1
        con.execute("DELETE FROM export_facts WHERE run_id = 'r1'")
        if _audit(con) != 1:
            print(
                f"self-test FAILED: a {state} run MISSING its fact should be reported, "
                "not swept past",
                file=sys.stderr,
            )
            return 1

    con = _sq.connect(":memory:")
    con.execute(
            "CREATE TABLE run_index (run_id TEXT, state TEXT, state_changed_ms INTEGER)"
        )
    con.execute("CREATE TABLE export_facts (run_id TEXT, segment_json TEXT)")
    con.execute(
        "INSERT INTO run_index VALUES ('r1', 'a_state_nobody_taught_us', 1)"
    )
    if _audit(con) != 2:
        print("self-test FAILED: a genuinely unknown state must still refuse", file=sys.stderr)
        return 1

    print(
        f"self-test OK: {len(engine)} engine states all classified; "
        f"{len(TERMINAL_STATES)} terminal arms proven both ways; unknown state refuses"
    )
    return 0


def _audit(conn, show: int = 10) -> int:
    """The audit itself, over an open connection.

    SEPARATED FROM main() so the self-test can drive it against an in-memory database.
    A check whose only entry point resolves a state root and opens a file can be proven
    against production or not at all, which is how this script's own state enumeration
    went years without anything comparing it to the engine.
    """
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for required in ("run_index", "export_facts"):
        if required not in tables:
            print(f"{db} has no {required} table; wrong database?", file=sys.stderr)
            return 2

    # The allowlist above only fails loudly if something actually checks it. Without this,
    # a new terminal spelling is silently excluded from `terminal` while its facts still
    # land in `facts`, so the audit reports a fact surplus -- which reads like a defect in
    # the lane and is really a defect in this script. That happened once, with `cancelled`.
    known = set(TERMINAL_STATES) | set(NON_TERMINAL_STATES)
    seen = {row[0] for row in conn.execute("SELECT DISTINCT state FROM run_index")}
    unknown = sorted(seen - known)
    if unknown:
        print(
            f"unknown run states in the index: {', '.join(unknown)}\n"
            "classify each as terminal or non-terminal before trusting this audit",
            file=sys.stderr,
        )
        return 2

    placeholders = ",".join("?" for _ in TERMINAL_STATES)
    terminal = conn.execute(
        f"SELECT COUNT(*) FROM run_index WHERE state IN ({placeholders})",
        TERMINAL_STATES,
    ).fetchone()[0]
    facts = conn.execute("SELECT COUNT(*) FROM export_facts").fetchone()[0]
    # A run's facts are its segments, so facts >= terminal runs and the surplus is multi-segment
    # runs. Naming the surplus turns two magnitudes into a conservation identity: if it ever goes
    # NEGATIVE, facts are missing in a way the anti-join below cannot see, because that join only
    # finds runs with NO fact and not runs with too few. Learned from the neighbouring seam --
    # a consumer and I both grouped the same rows, agreed on every total, and held different
    # populations; their four groups summed to 1,308 against a population of 1,297 and neither
    # of us asked what our own groups added up to. A count says how many, a range says which.
    surplus = facts - terminal

    # A zero from a query that could not have seen anything reads exactly like a zero from a
    # healthy lane. Refuse to report "clean" over a corpus with nothing in it: an empty run
    # index or an empty fact table means this audit proves nothing, not that all is well.
    if terminal == 0 or facts == 0:
        print(
            f"cannot compare: terminal runs={terminal}, export facts={facts}\n"
            "one of these is empty, so a zero-missing result would be vacuous",
            file=sys.stderr,
        )
        return 2

    missing = conn.execute(
        f"""
        SELECT r.run_id, r.state, r.state_changed_ms
          FROM run_index r
          LEFT JOIN export_facts e ON e.run_id = r.run_id
         WHERE r.state IN ({placeholders})
           AND e.run_id IS NULL
         ORDER BY r.rowid DESC
        """,
        TERMINAL_STATES,
    ).fetchall()

    print(f"terminal runs   {terminal}")
    print(f"export facts    {facts}")
    print(f"extra segments  {surplus}" if surplus >= 0 else f"MISSING SEGMENTS {-surplus}")
    print(f"missing facts   {len(missing)}")

    # The anti-join above names WHICH runs are missing, so this audit does not have the
    # defect a bare count comparison would. Worth stating because the two look alike in the
    # output: `terminal runs 19080 / export facts 19082` is a pair of totals, and totals can
    # agree while describing different populations. Measured tonight against a consumer's
    # independent store: both sides counted 487 affected xAI facts, which only became
    # evidence of the same facts once we compared the composition (341 unknown / 146
    # subscription_included on each). If a future check here reports agreement without
    # naming rows, it has weakened to a total comparison whatever its output looks like.

    if not missing:
        return 0

    print()
    print("terminal runs with no export fact (newest first):")
    for run_id, state, changed_ms in missing[: show]:
        when = changed_ms if changed_ms is not None else "unknown"
        print(f"  {run_id}  state={state}  state_changed_ms={when}")
    if len(missing) > show:
        print(f"  ... and {len(missing) - show} more")
    print()
    print(
        "A missing fact is recoverable: emission is idempotent by fact_id, so reopening the\n"
        "session backfills it. A WRONG fact would not be -- the lane is append-only with no\n"
        "update or delete, and ASTRO reads it by monotonic cursor."
    )
    return 1



def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", default=None)
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="prove the state enumeration still matches the engine, and exit",
    )
    parser.add_argument(
        "--show",
        type=int,
        default=10,
        help="how many missing runs to print (default 10)",
    )
    args = parser.parse_args()

    if args.self_test:
        return _self_test()

    root = resolve_state_root(args.state_root)
    if root is None:
        return 2

    db = root / "run-index.db"
    if not db.exists():
        print(f"run index not found: {db}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)

    return _audit(conn, args.show)

if __name__ == "__main__":
    sys.exit(main())
