#!/usr/bin/env bash
# Reproduce a run that no management operation can reach.
#
# A bind is refused when its project_root does not exist. That refusal happens before any
# request body is read, so it takes down cancel and status along with send -- and cancel is
# the only exit from a durably-paused run, which is how a run becomes permanently stranded.
#
# The run this creates completes normally. That is deliberate: the refusal is a property of
# the bind, not of the run's state, so a completed run demonstrates the mechanism with far
# less setup than arranging a pause. What a paused run adds is the consequence, not the
# cause -- for that, verify against a genuinely paused run behind a vanished root as well.
#
# Exit codes: 0 = reproduced, 1 = NOT reproduced (the wedge is gone -- the expected result
# once the bind stops refusing), 2 = the harness itself failed and proved nothing.
set -uo pipefail

CONN="${SUBC_CONNECTION_FILE:-$HOME/.local/share/cortexkit/run/subc-connection.json}"
CLIENT="${BROCA_SESSION_BIN:-./target/release/broca-session}"
MODEL="${REPRO_MODEL:-anthropic/claude-opus-5}"
ROOT="${REPRO_ROOT:-$HOME/.local/share/cortexkit/broca-repro-$$}"
SESSION="repro-vanished-root-$(date +%s)-$$"

fail_harness() { echo "HARNESS FAILED: $*" >&2; rm -rf "$ROOT"; exit 2; }

[ -x "$CLIENT" ] || fail_harness "no broca-session at $CLIENT (set BROCA_SESSION_BIN)"
[ -f "$CONN" ] || fail_harness "no subc connection file at $CONN (set SUBC_CONNECTION_FILE)"

mkdir -p "$ROOT" || fail_harness "could not create $ROOT"
echo "root    $ROOT"
echo "session $SESSION"

# A real run, so the later probe is a management op against something that exists rather
# than a lookup for a run that was never admitted.
SEND=$("$CLIENT" --subc "$CONN" --project-root "$ROOT" --harness runner --session "$SESSION" \
    run --prompt "Reply with exactly: ok" --model "$MODEL" 2>&1)
RUN_ID=$(printf '%s\n' "$SEND" | sed -n 's/.*RUN_STARTED[^ ]* run_id=\([^ ]*\).*/\1/p' | head -1)
printf '%s\n' "$SEND" | grep -q "RUN_FINISHED" || fail_harness "run did not finish:
$SEND"
[ -n "$RUN_ID" ] || RUN_ID=$(printf '%s\n' "$SEND" | sed -n 's/.*run_id=\([A-Za-z0-9_-]*\).*/\1/p' | head -1)
[ -n "$RUN_ID" ] || fail_harness "could not read a run_id from:
$SEND"
echo "run     $RUN_ID"

# CONTROL. Without this a later failure is equally consistent with a broken harness, and the
# reproduction would prove nothing about the bind.
"$CLIENT" --subc "$CONN" --project-root "$ROOT" --harness runner --session "$SESSION" \
    status --run-id "$RUN_ID" >/dev/null 2>&1 \
    || fail_harness "status failed while the root still exists, so this harness cannot show anything"
echo "control status reachable while the root exists"

rm -rf "$ROOT"
[ -d "$ROOT" ] && fail_harness "could not remove $ROOT"
echo "removed the root"

PROBE=$("$CLIENT" --subc "$CONN" --project-root "$ROOT" --harness runner --session "$SESSION" \
    status --run-id "$RUN_ID" 2>&1)

if printf '%s\n' "$PROBE" | grep -q "invalid_project_root"; then
    echo "REPRODUCED: the run is unreachable"
    printf '%s\n' "$PROBE" | sed 's/^/  /'
    echo
    echo "The same bind refusal applies to run.cancel, which is the only exit from a paused"
    echo "run -- so a run in this state that had paused could never be ended."
    exit 0
fi

if printf '%s\n' "$PROBE" | grep -qE "STATUS|state="; then
    echo "NOT REPRODUCED: status answered with the root gone -- the wedge is fixed"
    printf '%s\n' "$PROBE" | sed 's/^/  /'
    exit 1
fi

fail_harness "status neither refused the bind nor answered:
$PROBE"
