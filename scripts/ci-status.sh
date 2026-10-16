#!/usr/bin/env bash
# Report CI's conclusion for ONE commit, and refuse rather than report a clean zero.
#
# WHY THIS IS A SCRIPT AND NOT `gh run list --limit 1`. Two ways the obvious invocations lie,
# both measured here on 2026-09-06 while verifying v0.3.74:
#
#   gh run list --commit efc44b17            -> 0 runs
#   gh run list --commit <full sha>          -> 1 run, success
#
# `--commit` MATCHES ONLY A FULL SHA. A short one returns an empty list with exit 0, which is
# byte-identical to "CI has not run for this commit" — the answer you get when you are about to
# tag and want reassurance. I read that zero as an oddity and moved past it.
#
# The fallback is worse. `gh run list --limit 1` and reading the top entry is correct ONLY while
# your sha is the newest; a push landing between your commit and your check hands you a
# different commit's conclusion, reported as yours. That is the failure that puts a red sha in
# a release tag while every command exited 0.
#
# So: resolve the sha with `git rev-parse` (never accept the caller's spelling), query by the
# full sha, and REFUSE with exit 2 when the result set is empty — because "no runs" and "the
# query did not match" are the same output and only one of them is safe to act on.
#
# Usage: ci-status.sh [<commit-ish> [--only] [required-workflow-name ...]]  default HEAD
# Without --only, every run on the commit must have concluded success. With --only, only runs
# of the named workflows are judged (each must be present); other workflows' runs are ignored.
# Exit 0 all judged runs concluded success · 1 one failed or is still going · 2 cannot compare.

set -euo pipefail

REF="${1:-HEAD}"
if [[ "$#" -gt 0 ]]; then shift; fi

command -v gh >/dev/null 2>&1 || {
  echo "ci-status: gh not on PATH -- cannot compare" >&2
  exit 2
}

FULL="$(git rev-parse --verify "${REF}^{commit}" 2>/dev/null || true)"
if [ -z "$FULL" ]; then
  echo "ci-status: '$REF' does not resolve to a commit -- cannot compare" >&2
  exit 2
fi
# ^{commit} matters for a tag: `git rev-parse v0.3.74` yields the TAG object, and no CI run is
# ever attached to one. That form returns an empty set for a release that CI passed.

RUN_LIMIT=100
RUNS="$(gh run list --commit "$FULL" --limit "$RUN_LIMIT" --json conclusion,status,name,databaseId,headSha 2>/dev/null || true)"
if [ -z "$RUNS" ]; then
  echo "ci-status: gh returned nothing for ${FULL:0:12} -- cannot compare" >&2
  exit 2
fi

# A TRUNCATED PAGE IS NOT A COMPLETE SET, and this script's whole claim is about the
# COMPLETE set: "all runs concluded success". `gh run list` defaults to 20 and silently
# returns the newest page, so 20 green runs printed OK while an older failure sat
# unseen. Raising the limit alone only MOVES the boundary -- the guard is refusing when
# the count reaches it, so the number can be generous without being load-bearing.
RETURNED="$(printf '%s' "$RUNS" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' 2>/dev/null || echo x)"
case "$RETURNED" in
  ''|*[!0-9]*)
    echo "ci-status: could not count the runs gh returned -- cannot compare" >&2
    exit 2 ;;
esac
if [ "$RETURNED" -ge "$RUN_LIMIT" ]; then
  echo "ci-status: gh returned $RETURNED runs, the query limit -- the set may be truncated, cannot compare" >&2
  exit 2
fi


# TWO stdin redirections on one command is a silent disaster: `python3 - <<'PY' <<<"$RUNS"`
# makes the LAST one win, so python reads the JSON as its SCRIPT. A JSON array of objects is a
# valid Python list literal, so it evaluates, does nothing, and EXITS 0 — the checker built to
# stop false passes was a false pass in every arm. Caught only because a known-red commit and a
# commit with no runs both reported 0. Pass the payload by PATH; stdin has one slot.
RUNS_FILE="$(mktemp)"
trap 'rm -f "$RUNS_FILE"' EXIT
printf '%s' "$RUNS" > "$RUNS_FILE"

python3 - "$FULL" "$RUNS_FILE" "$@" <<'PY'
import json, sys
full = sys.argv[1]
with open(sys.argv[2]) as fh:
    runs = json.load(fh)

# THE REFUSAL THIS SCRIPT EXISTS FOR. An empty set means "no run matched", and the two causes --
# CI genuinely has not started, and the query failed to match -- produce the same empty list.
# Neither is a pass, and reporting either as one is how a red sha reaches a tag.
if not runs:
    print(f"ci-status: NO RUNS for {full[:12]} -- cannot compare", file=sys.stderr)
    print("  either CI has not started, or nothing matched. Both are refusals, not passes.",
          file=sys.stderr)
    raise SystemExit(2)

print(f"    commit {full[:12]}   runs {len(runs)}")
required = sys.argv[3:]
only = bool(required) and required[0] == "--only"
if only:
    required = required[1:]
    if not required:
        print("ci-status: --only needs at least one workflow name", file=sys.stderr)
        raise SystemExit(2)
# A green unrelated workflow cannot stand in for a missing required workflow.
missing = set(required) - {r.get("name") for r in runs}
if missing:
    print("ci-status: missing required workflows: " + ", ".join(sorted(missing)), file=sys.stderr)
    raise SystemExit(2)
if any(r.get("headSha") != full for r in runs):
    print("ci-status: a returned run names a different commit -- cannot compare", file=sys.stderr)
    raise SystemExit(2)
if only:
    runs = [r for r in runs if r.get("name") in set(required)]
bad = []
for r in runs:
    concl = r.get("conclusion") or "-"
    print(f"      {r.get('status',''):12s} {concl:10s} {r.get('name','')[:38]}")
    if r.get("status") != "completed" or concl != "success":
        bad.append(r)

if bad:
    # A SHA THAT JUST LANDED VIA THE TRAIN CARRIES TWO RUNS OF THE SAME WORKFLOW, both
    # from `push`: one on the train ref, one on master after the fast-forward. So for a
    # few minutes after a successful train this refuses with "1 of 2 not concluded"
    # while the FIRST run is already green. That is correct and not a failure -- the
    # second run is the same code -- but do not read it as a problem, and do not tag on
    # it either: an unconcluded run is not a passed one.
    print(f"ci-status: {len(bad)} of {len(runs)} run(s) not concluded success", file=sys.stderr)
    raise SystemExit(1)
print(f"ci-status: OK ({len(runs)} run(s) success)")
PY
