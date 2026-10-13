#!/usr/bin/env bash
# Drive one live tool-bearing turn through the deployed daemon.
#
# THIS EXISTS BECAUSE THE COMMAND WAS HAND-TYPED EVERY RELEASE AND HAS THREE INDEPENDENT WAYS
# TO BE WRONG, each of which fails differently and none of which says "your invocation is
# wrong":
#
#   1. GLOBAL FLAGS PRECEDE THE SUBCOMMAND. `run --subc ...` is rejected as an unexpected
#      argument; --subc, --project-root and --session all belong before `run`.
#   2. THE CONNECTION FILE IS subc-connection.json. Guessing `subc.json` produces a transport
#      stat error that reads like the daemon being down, while both daemons are alive.
#   3. --tools TAKES A JSON ARRAY OF SCHEMAS, not a tool name. A bare name fails json parsing
#      AFTER the bind succeeds, so the failure looks like a tool problem rather than a syntax
#      one.
#
# Reconstructing those three from memory each release is exactly the shape that has produced
# false verdicts all through this work: an instrument that answers a question adjacent to the
# one asked. A wrong invocation prints nothing useful and reads like a broken deployment, which
# is the most expensive possible confusion during a placement.
#
# Usage: scripts/acceptance-turn.sh [expected-version]
# Exits 0 only if the turn dispatched a tool, got a non-error result, and completed.

set -euo pipefail

# ONE POSITIONAL ARG, AND ANYTHING FLAG-SHAPED IS A MISTAKE. The model and prompt are fixed
# here on purpose (see the header), so `--model X` is not an option this script declined to
# implement -- it is a caller believing they steered a turn they did not steer.
#
# Without this guard `--model` lands in EXPECT and the failure reads "version --model NOT in
# final text", which describes a version mismatch that did not happen and says nothing about
# the flag being ignored. Measured 2026-09-06: I passed --model/--prompt to verify a specific
# model, got a turn on the DEFAULT model, and the tool-call id (`toolu_`, Anthropic-shaped)
# was the only thing in the output that showed it. To drive a specific model, call
# broca-session directly -- global flags BEFORE the subcommand.
for arg in "$@"; do
  case "$arg" in
    -*)
      echo "acceptance-turn: unknown flag '$arg' -- this script takes one optional positional" >&2
      echo "  expected-version and drives a FIXED model and prompt. It cannot steer a turn." >&2
      exit 2
      ;;
  esac
done
[ "$#" -le 1 ] || {
  echo "acceptance-turn: $# arguments -- expected at most one (expected-version)" >&2
  exit 2
}

EXPECT="${1:-}"
BIN="./target/release/broca-session"
CONN="$HOME/.local/share/cortexkit/run/subc-connection.json"

[ -x "$BIN" ] && [ -f "$CONN" ] || {
  echo "acceptance-turn: need $BIN and $CONN" >&2
  exit 2
}

# A real schema rather than a name, per (3). filePath is what the tool module expects.
TOOLS='[{"name":"read","description":"Read a file from disk","input_schema":{"type":"object","properties":{"filePath":{"type":"string"}},"required":["filePath"]}}]'
SID="acceptance-$(date +%s)-$$"
OUT="$(mktemp)"
trap 'rm -f "$OUT"' EXIT

# CANDIDATE MODELS, TRIED IN ORDER, AND THE LIST IS THE POINT.
#
# This script used to drive ONE model, so a placement check measured that provider's quota as
# much as it measured the deploy. Measured 2026-09-06 verifying v0.3.78: anthropic/claude-opus-5
# returned 429 (a spent OAuth 5-hour window), antigravity-gemini-3.8-flash returned 429, and
# deepseek-v4-flash completed. Three providers for one verification, on a binary that was
# correctly placed.
#
# The loud failure is not the dangerous one. The dangerous one is the inverse: an operator
# reading a 429 as "that is just Anthropic" on a placement that genuinely broke. A rotation
# removes the confusion instead of asking the reader to resolve it.
#
# DELIBERATELY SPANS THREE WIRE FAMILIES (Anthropic, Google Code-Assist, OpenAI-compatible)
# rather than three models on one provider, so a single upstream outage cannot wall the whole
# list. Order is by how much of the render path each exercises, not by cost.
MODELS="${ACCEPTANCE_MODELS:-anthropic/claude-opus-5 google/antigravity-gemini-3.8-flash deepseek/deepseek-v4-flash}"

served=0
refused=""
for model in $MODELS; do
  SID="acceptance-$(date +%s)-$$-$(echo "$model" | tr -c 'a-z0-9' '-')"
  # Global flags before `run`, per (1).
  timeout 240 "$BIN" --subc "$CONN" --project-root "$PWD" --session "$SID" \
    run --model "$model" --tools "$TOOLS" \
    --prompt 'Use the read tool on Cargo.toml, then reply with only the version line.' \
    > "$OUT" 2>&1 || true

  # A PROVIDER REFUSAL IS NOT A DEPLOY FAILURE, and the two must not share an exit code.
  # 429 and auth_required say the daemon reached the provider and the provider declined --
  # which is itself evidence the binary serves, but not the evidence this script is for.
      # A RUN THAT PAUSED FOR AUTH IS A REFUSAL TOO, and it does NOT emit LOOP_ERROR.
      # Measured: with the default oauth:anthropic credential in needs_reauth, the turn
      # produced RUN_PAUSED and no LOOP_ERROR at all, so the old detector read it as
      # SERVED and then failed the three assertions with nothing to explain why. A stale
      # credential on the first candidate looked exactly like a broken deploy.
      if grep -aqE 'RUN_PAUSED|reason=Paused' "$OUT"; then
        echo "acceptance-turn: $model paused for re-authentication, trying the next"
        refused="$refused $model(auth)"
        continue
      fi
  if grep -aqE 'LOOP_ERROR.*(429|401|403|rate_limit|insufficient_quota|auth_required)' "$OUT"; then
    echo "acceptance-turn: $model refused by the provider (quota or auth), trying the next" >&2
    refused="$refused $model"
    continue
  fi
  served=1
  echo "acceptance-turn: served by $model"
  break
done

if [ "$served" -eq 0 ]; then
  echo "acceptance-turn: CANNOT COMPARE -- every candidate model was refused by its provider:" >&2
  echo "   $refused" >&2
  echo "  The daemon reached each provider and each declined, so this says NOTHING about the" >&2
  echo "  placement either way. Wait for a window, or set ACCEPTANCE_MODELS to a provider with" >&2
  echo "  headroom. Exit 2 is 'cannot compare', distinct from 1 which means the turn failed." >&2
  cat "$OUT" >&2
  exit 2
fi

# `|| true` IS LOAD-BEARING UNDER `set -o pipefail`. This line is a DIAGNOSTIC echo, and
# when the turn fails it matches nothing -- so the pipeline returns non-zero and the shell
# exits HERE, before the three assertions below can say which leg failed. Measured: the
# script printed one line, "served by <model>", and exited 1 with no reason. A failure
# reporter that dies on the failure it exists to report is worse than no reporter.
grep -aE "TOOL_CALL|TOOL_RESULT|RUN_FINISHED|FINAL_TEXT" "$OUT" | cut -c1-100 | sed 's/^/  /' || true

# Assert the three things that distinguish a served turn from a bind that went nowhere. A run
# can reach RUN_FINISHED without ever dispatching, which is why the tool legs are separate.
fail=0
grep -aq "TOOL_CALL" "$OUT"              || { echo "acceptance-turn: NO TOOL DISPATCH" >&2; fail=1; }
grep -aq "is_error=false" "$OUT"         || { echo "acceptance-turn: TOOL RESULT MISSING OR ERRORED" >&2; fail=1; }
grep -aq "reason=Completed" "$OUT"       || { echo "acceptance-turn: RUN DID NOT COMPLETE" >&2; fail=1; }

# NO VERSION CHECK HERE, DELIBERATELY, AND THIS USED TO BE ONE.
#
# The prompt reads Cargo.toml FROM THE SOURCE TREE. Nothing in this transcript reflects the
# DAEMON's version -- so the old `grep "$EXPECT" "$OUT"` matched the file the tool returned,
# not the binary that served it. Proven against the live fleet: with 0.3.81 deployed and
# 0.3.82 in the working tree, `acceptance-turn.sh 0.3.82` printed PASS (version 0.3.82).
#
# Narrowing the grep to FINAL_TEXT would NOT have fixed it. That only asks whether the model
# correctly echoed a file it had just read -- a test of the model, not of the daemon. The
# check was unfixable in principle, because the quantity it claimed to measure is absent
# from everything this script can see.
#
# The daemon's version is established where it is observable: `ck-broca --version`, the
# running-vs-disk inode match, the sha256 against the release card, and the string/symbol
# discriminators -- all in scripts/verify-deploy.sh. THIS script answers a different and
# still-necessary question: does the deployed binary dispatch a tool, get a result back, and
# complete a run. Those three legs above are sound and are the whole point.
if [ -n "$EXPECT" ]; then
  echo "acceptance-turn: IGNORING the version argument '$EXPECT' -- this script cannot" >&2
  echo "  observe the daemon's version (it reads the source tree's Cargo.toml). Use" >&2
  echo "  scripts/verify-deploy.sh, which checks the binary rather than a file it served." >&2
fi

if [ "$fail" -ne 0 ]; then
  echo "acceptance-turn: FAILED \u2014 full output follows" >&2
  cat "$OUT" >&2
  exit 1
fi

# Name what was proven, not what was asked for. The old form echoed the version argument into
# the PASS line, so the summary asserted a version this script never observed -- the same claim
# the check itself used to make, surviving in the output after the check was removed.
echo "acceptance-turn: PASS -- tool dispatched, result returned, run completed"
