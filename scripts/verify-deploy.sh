#!/usr/bin/env bash
# Verify the placed module and its clients. Only ck-broca is a running process: clients
# prove identity through --version and the staged post-sign digest, not process inodes.
#
# This existed only as commands typed fresh after every release, which is why a defect in it
# survived for weeks: the pid resolver matched the LAST field of the ps line against the binary
# path, so it resolved nothing for any process invoked with arguments -- and ck-broca always
# carries --subc. Every green it ever produced was luck. A ritual nobody automated is a ritual
# nobody can fix once.
#
# Three layers, because each answers a question the others cannot:
#   1. inode    the running text is the file on disk (catches a replace-in-place that never
#               restarted, and a restart that picked up a different file)
#   2. strings  the placed artifact contains the code this release added (catches a correct
#               inode holding the previous build)
#   3. turn     the daemon actually serves (catches a binary that is right and wedged)
#
# Layer 3 is not run here -- it costs a real model call and belongs in a human's hands. This
# prints the exact command.
#
# Usage: scripts/verify-deploy.sh --marker <string-this-release-adds> --control <string-in-both>
set -u

BIN="${BROCA_BIN:-$HOME/.local/share/cortexkit/bin/ck-broca}"
STATE_ROOT="${BROCA_STATE_ROOT:-${XDG_DATA_HOME:-$HOME/.local/share}/cortexkit/broca}"
MARKER=""
CONTROL=""
STAGED=""

while [ $# -gt 0 ]; do
  case "$1" in
    --marker|--control|--bin|--sha|--staged)
      if [ "$#" -lt 2 ] || [ -z "${2:-}" ]; then
        echo "verify-deploy: usage error: $1 requires an argument" >&2
        exit 64
      fi
      ;;
  esac
  case "$1" in
    --marker)  MARKER="${2:-}"; shift 2 ;;
    --control) CONTROL="${2:-}"; shift 2 ;;
    --bin)     BIN="${2:-}"; shift 2 ;;
    --sha)     EXPECT_SHA="${2:-}"; shift 2 ;;
    --staged)  STAGED="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 64 ;;
  esac
done

[ -n "$MARKER" ] && [ -n "$CONTROL" ] || {
  echo "usage: scripts/verify-deploy.sh --marker <string this release adds> --control <string in both>" >&2
  echo "  the control is not optional: a marker reading 0 proves nothing if the probe itself is broken" >&2
  exit 64
}
[ -x "$BIN" ] || { echo "no executable at $BIN" >&2; exit 2; }

fail=0

# --- layer 1: running text == file on disk -----------------------------------------------
# Use the same process and open-image probe as the release wait.
source "$(dirname "${BASH_SOURCE[0]}")/running-image.sh"
PID="$(broca_pid_for_binary "$BIN")" || { echo "REFUSED: unknown process identity at $BIN"; exit 2; }

# Validate the pid BEFORE lsof. An empty pid makes `lsof -p ""` match every open file on the
# box, which yields a plausible inode from an unrelated process -- a false all-clear rather
# than a failure. This guard is why a resolver that never worked produced refusals instead of
# wrong answers.
if [ -z "$PID" ] || ! ps -p "$PID" >/dev/null 2>&1; then
  if [ "$BROCA_DEPLOY_HOST" = Linux ]; then
    echo "REFUSED: running process absent or identity unknown at $BIN"
  else
  echo "REFUSED: no running process found at $BIN"
  fi
  echo "  cannot compare a running artifact to a placed one when nothing is running"
  exit 2
fi

RUNNING_INODE="$(broca_running_inode "$PID")"
DISK_INODE="$(broca_file_inode "$BIN")"
if [ -z "$RUNNING_INODE" ] || [ -z "$DISK_INODE" ]; then
  echo "REFUSED: could not read both inodes (running='$RUNNING_INODE' disk='$DISK_INODE')"
  exit 2
fi
if [ "$RUNNING_INODE" = "$DISK_INODE" ]; then
  printf "  inode    %-12s == %-12s MATCH  (pid %s)\n" "$RUNNING_INODE" "$DISK_INODE" "$PID"
else
  printf "  inode    %-12s != %-12s MISMATCH -- the running process predates the placement\n" \
    "$RUNNING_INODE" "$DISK_INODE"
  fail=1
fi

# --- signing posture of the RUNNING image -----------------------------------------------------
# The daemon hands modules a secret launch nonce, and only the hardened runtime (flag `runtime`, and no
# com.apple.security.get-task-allow entitlement) keeps a same-user process from attaching a
# debugger and reading it. place-release checks this on the copy it places; this checks it on
# the file the process actually maps, which catches a placement by hand or by another seat.
#
# codesign reads a PATH, and macOS has no /proc/<pid>/exe to read the mapped file directly. So
# the path is only the running image when its inode matched above, and the final re-stat at the
# end of this script is what proves it still named that inode while codesign read it.
source "$(dirname "${BASH_SOURCE[0]}")/signing-posture.sh"
if [ "$BROCA_DEPLOY_HOST" = Darwin ]; then
if [ "$RUNNING_INODE" != "$DISK_INODE" ]; then
  echo "  signing  NOT CHECKED -- $BIN is not the file pid $PID maps (inode mismatch above)"
  fail=1
elif ! command -v codesign >/dev/null 2>&1; then
  echo "  signing  NOT CHECKED -- codesign is not available to read $BIN"
  fail=1
else
  RUN_FLAGS="$(broca_codesign_flags "$BIN" || true)"
  if RUN_ENTITLEMENTS="$(broca_codesign_entitlements "$BIN")"; then
    ents_readable=1
  else
    ents_readable=0
  fi
  if ! broca_flags_have_runtime "$RUN_FLAGS"; then
    printf "  signing  flags=%s  NOT HARDENED -- the running image lacks 'runtime' (%s)\n" \
      "${RUN_FLAGS:-unreadable}" "$BIN"
    fail=1
  elif [ "$ents_readable" -eq 0 ]; then
    printf "  signing  flags=%s  entitlements UNREADABLE -- cannot confirm no get-task-allow (%s)\n" \
      "$RUN_FLAGS" "$BIN"
    fail=1
  elif broca_entitlements_allow_debugger "$RUN_ENTITLEMENTS"; then
    printf "  signing  flags=%s  GET-TASK-ALLOW -- the running image lets a debugger attach (%s)\n" \
      "$RUN_FLAGS" "$BIN"
    fail=1
  else
    printf "  signing  flags=%s  hardened, no get-task-allow\n" "$RUN_FLAGS"
  fi
fi
else
  echo "  signing  not applicable on Linux; no macOS debugger-attachment protection claimed"
fi

if ! MODULE_VERSION_LINE="$("$BIN" --version)" || ! [[ "$MODULE_VERSION_LINE" =~ ^ck-broca\ [0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "REFUSED: invalid deployed --version at $BIN: '$MODULE_VERSION_LINE'" >&2
  exit 2
fi
printf "  version  %s\n" "$MODULE_VERSION_LINE"
source "$(dirname "${BASH_SOURCE[0]}")/release-clients.sh"
if [ -z "$STAGED" ]; then
  STAGED="$HOME/.local/share/cortexkit/staging/ck-broca.$(git -C "$(dirname "${BASH_SOURCE[0]}")/.." rev-parse --short=8 HEAD)"
fi
for client in broca-session broca-run; do
  broca_verify_client "$(dirname "$BIN")/$client" "${MODULE_VERSION_LINE#ck-broca }" \
    "$(broca_release_client_path "$STAGED" "$client")" || fail=1
done

# SUBC's rule, banked after they placed a new ck-models into the cortexkit bin dir while the
# one an operator actually runs resolved from ~/.local/bin: reporting a version read from a
# file you placed does not establish that anyone RUNS that file. `--version` on the wrong
# binary returns a true statement about the wrong thing, which is worse than a missing check
# because there is nothing absent to notice.
#
# Broca has nothing on PATH today -- the daemon is supervised with an explicit path and every
# CLI takes `--subc`, so this arm is a null. It is here anyway because the day it stops being
# a null is the day someone installs a broca CLI into ~/.local/bin, and nobody will re-derive
# this reasoning first. A NULL ARM COSTS A LINE; RE-DERIVING WHY IT WAS SAFE COSTS AN OUTAGE.
RESOLVED="$(command -v "$(basename "$BIN")" 2>/dev/null || true)"
if [ -z "$RESOLVED" ]; then
  printf "  path     %-24s not on PATH -- nothing resolves it by name\n" "$(basename "$BIN")"
elif [ "$RESOLVED" = "$BIN" ]; then
  printf "  path     %-24s resolves to the verified binary\n" "$(basename "$BIN")"
else
  printf "  path     %s resolves to %s, NOT the binary verified above\n" "$(basename "$BIN")" "$RESOLVED"
  echo "           An operator invoking it by name runs a different file than everything"
  echo "           checked here. Place to the resolved path, or remove the stale one."
  fail=1
fi

# The inode check above and this hash are blind in complementary directions, which is why
# both are here and neither is redundant. Inode identity proves the running text is the file
# at this path and says nothing about WHICH BYTES those are -- a replace-in-place keeps the
# inode and changes everything. A hash proves the bytes and says nothing about whether that
# file is the one EXECUTING -- a stale process runs happily beside a correct file on disk.
# Agreement rules out both stale-process and replaced-file; either alone rules out neither.
#
# Pass --sha to compare against the artifact you staged. Omitted, it prints for the record:
# an unchecked hash is still worth having in the log, but it is not a check.
PLACED_SHA="$(shasum -a 256 "$BIN" 2>/dev/null | awk '{print $1}')"
if [ -z "$PLACED_SHA" ]; then
  echo "REFUSED: could not hash the placed binary at $BIN"
  exit 2
fi
if [ "$BROCA_DEPLOY_HOST" = Linux ]; then
  RUNNING_SHA="$(broca_running_sha "$PID")"
  if [ -z "$RUNNING_SHA" ]; then
    echo "REFUSED: could not hash the running image for pid $PID"
    exit 2
  fi
  if [ "$RUNNING_SHA" != "$PLACED_SHA" ]; then
    echo "  running sha256 $RUNNING_SHA MISMATCH -- running bytes differ from placed bytes"
    fail=1
  else
    echo "  running sha256 $RUNNING_SHA MATCH"
  fi
fi
if [ -n "${EXPECT_SHA:-}" ]; then
  if [ "$PLACED_SHA" = "$EXPECT_SHA" ]; then
    printf "  sha256   %s  MATCH\n" "$PLACED_SHA"
  else
    printf "  sha256   %s\n           %s  MISMATCH -- placed bytes are not the staged bytes\n" \
      "$PLACED_SHA" "$EXPECT_SHA"
    fail=1
  fi
else
  printf "  sha256   %s  (no --sha given, not checked)\n" "$PLACED_SHA"
fi

# --- layer 2: the placed artifact carries this release's code ------------------------------
# Read from the PLACED path, never from a staging copy: the question is what got placed.
# A VARIANT OR TYPE NAME IS NOT A MARKER in the string table -- rustc emits Display format
# strings and error text, not identifiers, and private fn/type names inline away in release
# builds. But a code-only release genuinely introduces NO new string, and its evidence lives in
# the SYMBOL table instead: a new type reaches nm through its monomorphizations even when
# `strings` cannot see it. Searching only one table makes those releases look unplaced, which
# is the same wrong answer as an absent marker and arrives wearing a refusal.
#
# Found by this script refusing on v0.3.46, whose marker is an nm-visible type and whose
# control was equally invisible -- the refusal was correct and the probe was one table short.
# stage-release.sh already searched both; this one was a release behind.
#
# -F, because a marker is a STRING and grep would otherwise read it as a regex. Two ways that
# bites, both silent: an invalid pattern (`content[`) makes grep refuse, leaving the count
# EMPTY so the numeric tests below error and evaluate false -- every check skipped, exit 0. And
# a valid one (`run_index.db`) matches `run_indexXdb`, inflating the count instead. Both end
# with a verification that did not verify.
count_in_binary() {
    s="$(strings "$1" 2>/dev/null | grep -F -c -- "$2" || true)"
    n="$(nm -a "$1" 2>/dev/null | grep -F -c -- "$2" || true)"
    echo $(( ${s:-0} + ${n:-0} ))
}
m_count="$(count_in_binary "$BIN" "$MARKER")"
c_count="$(count_in_binary "$BIN" "$CONTROL")"
printf "  marker   %-44s %s\n" "$MARKER" "$m_count"
printf "  control  %-44s %s\n" "$CONTROL" "$c_count"
# The probe has to prove it produced an answer before any answer is believed. Without this a
# failed count reaches `[ "" -eq 0 ]`, which is false, so the guard it belongs to never fires.
for probe_count in "$m_count" "$c_count"; do
  case "$probe_count" in
    "" | *[!0-9]*)
      echo "REFUSED: a count probe returned no number (marker='$m_count' control='$c_count')"
      echo "  the binary may be unreadable; a missing count cannot be read as a missing marker"
      exit 2
      ;;
  esac
done
if [ "$c_count" -eq 0 ]; then
  echo "REFUSED: the control string is absent, so the probe itself is not working"
  echo "  a marker count of $m_count means nothing until the control proves strings can find anything"
  exit 2
fi
if [ "$m_count" -eq 0 ]; then
  echo "  the marker is absent while the control is present: the placed binary predates this release"
  fail=1
fi

# Every leg above the inode check reads the PATH again -- the signing check and the version
# probe read and execute it, the hash and the string counts re-open it. So the legs are sequential observations that each
# re-resolve their subject, and a placement landing mid-run would have them describing two
# different artifacts while reporting as though they described one. Nothing above notices,
# because each leg is individually correct about whatever it happened to read.
#
# Re-stat now and compare against the inode the first leg pinned. Unchanged means every leg
# read one artifact; changed means the run straddled a placement and its verdict is void --
# which is a refusal rather than a failure, since neither answer was wrong, they were about
# different files.
# Non-vacuity, and the limit of it: a rename-into-place changes the inode (measured: 830007919
# -> 830007920 on a copy), so the comparison discriminates a real swap. It could not be driven
# end-to-end here, because a swapped path has no running process bound to it and the pid leg
# refuses first -- the arm this guard covers needs a live daemon replaced mid-run, which is not
# something to stage against production to prove a point.
FINAL_INODE="$(broca_file_inode "$BIN")"
if [ -z "$FINAL_INODE" ]; then
  echo "REFUSED: could not re-stat $BIN to confirm the legs shared one subject"
  exit 2
fi
if [ "$FINAL_INODE" != "$DISK_INODE" ]; then
  echo "REFUSED: $BIN changed under this run (inode $DISK_INODE -> $FINAL_INODE)"
  echo "  the legs above describe two different artifacts; re-run against a settled binary"
  exit 2
fi
if [ "$BROCA_DEPLOY_HOST" = Linux ] &&
   [ "$(broca_running_inode "$PID")" != "$RUNNING_INODE" ]; then
  echo "REFUSED: running image changed or became unreadable during verification"
  exit 2
fi

# --- export lane continuity ----------------------------------------------------------------
DB="$STATE_ROOT/run-index.db"
if [ -r "$DB" ]; then
  read -r facts high <<<"$(sqlite3 "file:$DB?mode=ro" \
    'SELECT COUNT(*), COALESCE(MAX(export_seq),0) FROM export_facts;' 2>/dev/null | tr '|' ' ')"
  printf "  export   %s facts, high_water %s\n" "${facts:-?}" "${high:-?}"
fi

# --- layer 3: the daemon actually serves ---------------------------------------------------
echo
echo "  layers 1-2 checked the artifact. Nothing above proves the daemon SERVES -- run the"
echo "  acceptance turn, and use a tool-bearing one so route open, dispatch, result and"
echo "  completion are all exercised rather than just generation:"
echo
echo "    bash scripts/acceptance-turn.sh"
echo "    python3 scripts/acceptance-worker-turn.py"
echo
# POINTS AT THE SCRIPT RATHER THAN INLINING THE RECIPE. The inlined form drifted from the
# script that owns it, and a reader following the printed prompt would see the version in the
# reply and read it as a version check. It is not one: the turn reads Cargo.toml FROM THE
# SOURCE TREE, so that string is a file the daemon relayed, never the daemon's version. The
# version is established above, by --version on the destination binary and the marker
# differential. See the comment in scripts/acceptance-turn.sh for the live proof.
echo "  expect: TOOL_CALL read -> TOOL_RESULT is_error=false -> RUN_FINISHED Completed"
echo "  then: classed fetch-plan turn completed; RunStarted WAL requires session-class/v1"
echo "        and provider-declarations/v1 (not inferred from the session reply)"

# --- a standing question, not a check -------------------------------------------------------
#
# ASK OF EVERY GAUGE: WHEN DID THIS LAST SHOW A DIFFERENT VALUE?
#
# ALF's framing, and it has a retroactive hit on the sharpest instance in this repo.
# `projection_failures` read 0 in production for weeks -- not because projections succeeded, but
# because both fetch_add sites carried #[cfg(test)] while the reader did not, so the release
# binary published 0 regardless (r126, e269833). The test PASSED, because cfg(test) enabled the
# increments it asserted on: it proved the counter works in a configuration production never
# runs. A gauge stuck on one value, an assertion that could not fail, and a green suite, all
# three agreeing and all three wrong.
#
# The trap is that "it should be zero, those events are rare" is a GOOD argument and is exactly
# the sentence anyone would have written about projection_failures the day before it was found.
# Rarity and un-instrumented are indistinguishable from the outside; only a fault injection
# separates them.
#
# As of 2026-09-03 these have never shown a second value in production:
#
#     projection_failures                was the r126 defect; now mutation-guarded
#     frames_dropped_stale_epoch         0 since v0.3.33
#     frames_dropped_unmatched_terminal  0 since v0.3.33
#     catalog_refusals / _total          0 since v0.3.43
#
# and these are demonstrably alive:
#
#     runs_in_flight                     observed 1 mid-generation, 0 idle
#     vault status                       went Degraded during the claustrum rename
#
# This is not wired as a gate: a stuck gauge is a candidate for investigation, not a reason to
# refuse a deploy, and a script cannot tell rare from broken either. It is printed so the
# question gets asked on a schedule rather than when something already feels wrong.
echo
echo "  gauges with no second value ever observed (candidates for fault injection, not faults):"
echo "    projection_failures, frames_dropped_stale_epoch, frames_dropped_unmatched_terminal,"
echo "    catalog_refusals -- see the note above this line before concluding they are healthy"

exit "$fail"
