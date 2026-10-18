#!/usr/bin/env bash
# Place a staged ck-broca over the live one ATOMICALLY, or refuse.
#
# This existed only as a hand-typed `cp` after every release, which is how a real hazard ran
# unnoticed for a day: A COPY OVER A LIVE BINARY IS A TRUNCATE-THEN-WRITE. For the duration of
# the write the on-disk image is PARTIAL, and anything that execs it in that window gets a
# corrupt binary. Nothing did, which is not the same as it being safe.
#
# rename(2) within one filesystem is atomic: a reader sees either the whole old image or the
# whole new one, never a half. That is the only correct primitive here.
#
# The inode consequence is downstream of that choice and worth stating, because it silently
# invalidates a check used everywhere else: `cp` PRESERVES THE INODE (it rewrites in place), so
# a post-place inode comparison passes whether or not the new bytes landed. Placement by rename
# gives a NEW inode, which is what makes the running-vs-disk inode check discriminate at all.
# Verify with sha and a behavioural probe regardless; the inode check is only meaningful as a
# consequence of doing placement the safe way.
#
# Usage:
#   scripts/place-release.sh --staged /tmp/ck-broca-065 [--dest <path>] [--skip-gate] [--restart]
#   scripts/place-release.sh --restart-only [--dest <path>] [--live-approved]
# --live-approved permits a restart with known live runs, logging the invoking OS user.
# It never bypasses an unreadable gate or the new-pid/placed-inode wait.
# --report FILE writes the rollback path, restart pid and pre-restart live WAL names as JSON.
# On Darwin the staged binary needs an adjacent .dSYM; placement renames that bundle beside the
# destination and saves an existing bundle beside its matching rollback binary.
#
# A plain placement (--staged <path> without --restart) places the staged binary at --dest
# atomically using rename(2). Because rename preserves the running process's existing inode,
# placement alone does not cut live runs and proceeds even when runs are live, printing one
# line noting that runs are live and that the placed binary is not loaded until a restart,
# which re-checks the gate.
#
# The live-run gate only refuses when this invocation will restart (--restart, --restart-only).
#
# --restart waits up to 90 seconds for a NEW pid running the placed inode before returning.
# It loads the placed binary too, and is the way to place a live module. The restart runs
# only if the placement succeeded AND a second live-run check, taken just before the restart,
# is still clear. Without it the operator types the restart by hand, creating a race window:
# if a run starts in the seconds after placement, a hand-typed restart cuts the run to reload
# the same binary. A later restart must check the gate again, even when the bytes are already on disk.
#
# --restart-only rechecks the live-run gate and loads bytes already at --dest, without placing.
# Exit codes: 0 placed or restarted, 1 refused on a checked condition,
# 2 cannot compare / bad invocation, 3 placed but NOT restarted because a run went live after
# the first check (use --restart-only when the gate is clear; the new bytes are on disk),
# 4 restart requested but the new on-disk image was not observed before timeout.
#
# Test seams: PLACE_RELEASE_GATE_CMD replaces the live-run check (a shell command whose exit
# code means 0 clear / 1 live / other cannot tell), PLACE_RELEASE_CK replaces the `ck` binary.
# PLACE_RELEASE_WAIT_SECONDS and PLACE_RELEASE_POLL_SECONDS shorten the wait in tests.
# PLACE_RELEASE_RUN_SECONDS bounds each run of the candidate binary (default 30).
# BROCA_STATE_ROOT moves the state root, and with it the WAL feature floor file.
#
# THE PLACEMENT GATE refuses, naming the check and the file it read, unless the signed temp copy
# (the exact bytes the rename will publish) passes all of:
#   hardened-downgrade  the deployed binary is hardened and the copy is not
#   runtime-flag        the copy's code signature carries the hardened-runtime flag
#   get-task-allow      the copy's entitlements let a debugger attach
#   smoke-run           the copy fails to start: `--version` must exit 0 and name a version
#   wal-feature-floor   the copy cannot read feature names this machine's WALs already use

set -euo pipefail

DEST="${HOME}/.local/share/cortexkit/bin/ck-broca"
STAGED=""
SKIP_GATE=0
RESTART=0
RESTART_ONLY=0
STAGED_PROVIDED=0
LIVE_APPROVED=0
REPORT=""

while [ $# -gt 0 ]; do
  case "$1" in
    --staged|--dest|--report)
      if [ "$#" -lt 2 ] || [ -z "${2:-}" ]; then
        echo "place-release: usage error: $1 requires an argument" >&2
        exit 2
      fi
      ;;
  esac
  case "$1" in
    --staged) STAGED="${2:-}"; STAGED_PROVIDED=1; shift 2 ;;
    --dest)   DEST="${2:-}"; shift 2 ;;
    --skip-gate) SKIP_GATE=1; shift ;;
    --restart) RESTART=1; shift ;;
    --restart-only) RESTART_ONLY=1; shift ;;
    --live-approved) LIVE_APPROVED=1; shift ;;
    --report) REPORT="$2"; shift 2 ;;
    *) echo "place-release: unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [ "$LIVE_APPROVED" -eq 1 ]; then
  if { [ "$RESTART" -eq 0 ] && [ "$RESTART_ONLY" -eq 0 ]; } || [ "$SKIP_GATE" -eq 1 ]; then
    echo "place-release: --live-approved requires --restart or --restart-only, without --skip-gate" >&2
    exit 2
  fi
  APPROVER="$(id -un)" || exit 2
  echo "== live restart approved by $APPROVER (uid $(id -u)) at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
fi

if [ "$RESTART_ONLY" -eq 1 ]; then
  if [ "$STAGED_PROVIDED" -eq 1 ] || [ "$RESTART" -eq 1 ] || [ "$SKIP_GATE" -eq 1 ]; then
    echo "place-release: --restart-only cannot be combined with --staged, --restart or --skip-gate" >&2
    exit 2
  fi
else
  [ -n "$STAGED" ] || { echo "place-release: --staged is required" >&2; exit 2; }
  [ -f "$STAGED" ] || { echo "place-release: staged binary not found: $STAGED" >&2; exit 2; }
fi
[ -f "$DEST" ] || { echo "place-release: destination not found: $DEST" >&2; exit 2; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$REPO_ROOT/scripts/signing-posture.sh"
source "$REPO_ROOT/scripts/wal-floor.sh"
source "$REPO_ROOT/scripts/running-image.sh"
source "$REPO_ROOT/scripts/dsym.sh"
GATE_LOG="$(mktemp "${TMPDIR:-/tmp}/place-gate.XXXXXX")"
ACTIVE_RUNS_FILE="$(mktemp "${TMPDIR:-/tmp}/place-runs.XXXXXX")"
TMP=""
TMP_DSYM=""
TMP_DSYM_PARENT=""
OLD_DSYM_TEMP=""
DSYM_SWAP_STARTED=0
PLACEMENT_COMMITTED=0
cleanup_place() {
  rm -f "${TMP:-}"
  [ -z "$TMP_DSYM_PARENT" ] || rm -rf "$TMP_DSYM_PARENT"
  if [ "$DSYM_SWAP_STARTED" -eq 1 ] && [ "$PLACEMENT_COMMITTED" -eq 0 ]; then
    rm -rf "${DEST}.dSYM"
    if [ -n "$OLD_DSYM_TEMP" ] && [ -d "$OLD_DSYM_TEMP" ]; then
      mv "$OLD_DSYM_TEMP" "${DEST}.dSYM" || echo "place-release: could not restore the previous dSYM from $OLD_DSYM_TEMP" >&2
    fi
  fi
  rm -f "$GATE_LOG" "$ACTIVE_RUNS_FILE"
}
trap cleanup_place EXIT

# The live-run gate answers "is any run live RIGHT NOW". It only refuses when this invocation
# will restart (--restart, --restart-only), because placement alone is an atomic rename that
# keeps the running process on its existing inode without cutting live work.
# Its own exit codes are load-bearing: 1 means a live run, 2 means it could not tell.
run_gate() {
  set +e
  if [ -n "${PLACE_RELEASE_GATE_CMD:-}" ]; then
    bash -c "$PLACE_RELEASE_GATE_CMD" >"$GATE_LOG" 2>&1
  else
    python3 "$REPO_ROOT/scripts/live-runs.py" >"$GATE_LOG" 2>&1
  fi
  gate_rc=$?
  set -e
}

write_report() {
  [ -n "$REPORT" ] || return 0
  python3 - "$REPORT" "${BACKUP:-}" "${1:-}" "$GATE_LOG" "$REPO_ROOT/scripts" "$ACTIVE_RUNS_FILE" <<'PY'
import json, pathlib, sys
path, backup, pid, gate, scripts, active_runs_file = sys.argv[1:]
snapshot = pathlib.Path(gate).read_text() if pathlib.Path(gate).exists() else ""
sessions = [line.split()[1] for line in snapshot.splitlines() if line.startswith("  live  ")]
sys.path.insert(0, scripts)
from lib import state_root
from lib.archive_read import logical_wals
from lib.seal_readback import CANCELLED, SEALED, UNCONFIRMED, active_run_id, classify_run

active_path = pathlib.Path(active_runs_file)
seal_error = ""
if pid:
    try:
        saved = json.loads(active_path.read_text())
        active_runs = saved["runs"]
        errors = [saved["error"]] if saved.get("error") else []
    except Exception as error:
        active_runs = [{"session": name, "run_id": None} for name in sessions]
        errors = [f"cannot read pre-restart run snapshot: {error}"]
    statuses = {SEALED: [], UNCONFIRMED: [], CANCELLED: [], "idle": []}
    try:
        wals = {
            wal.name: wal
            for wal in logical_wals(
                state_root.default_state_root() / "wal", include_archives=False
            )
        }
    except Exception as error:
        wals = {}
        errors.append(str(error))
    for active in active_runs:
        name, run_id = active.get("session"), active.get("run_id")
        status = UNCONFIRMED
        # live-runs.py lists sessions by recent writes, so a session whose last run had already
        # finished can appear. The snapshot read its WAL and found no open run: nothing to seal.
        # A missing or unreadable WAL never sets this, so it stays unconfirmed.
        if active.get("no_open_run") is True:
            status = "idle"
        elif isinstance(name, str) and isinstance(run_id, str):
            wal = wals.get(name)
            if wal is not None:
                try:
                    status = classify_run(wal, run_id)
                except Exception as error:
                    errors.append(str(error))
        statuses[status].append({"session": name, "run_id": run_id})
    seal_error = "; ".join(errors)
else:
    active_runs = [{"session": name, "run_id": None} for name in sessions]
    errors = []
    try:
        wals = {
            wal.name: wal
            for wal in logical_wals(
                state_root.default_state_root() / "wal", include_archives=False
            )
        }
        for active in active_runs:
            wal = wals.get(active["session"])
            if wal is not None:
                active["run_id"] = active_run_id(wal)
                if active["run_id"] is None:
                    active["no_open_run"] = True
    except Exception as error:
        errors.append(str(error))
    seal_error = "; ".join(errors)
    active_path.write_text(json.dumps({"runs": active_runs, "error": seal_error}))
    statuses = {SEALED: [], UNCONFIRMED: [], CANCELLED: [], "idle": []}

runs_sealed = statuses[SEALED]
sessions_idle = [run["session"] for run in statuses["idle"]]
runs_unconfirmed = statuses[UNCONFIRMED]
runs_cancelled = statuses[CANCELLED]
sessions_sealed = [run["session"] for run in runs_sealed]
sessions_unconfirmed = [run["session"] for run in runs_unconfirmed]
sessions_cancelled = [run["session"] for run in runs_cancelled]
for run in runs_cancelled:
    print(
        f"place-release: FAILURE -- shutdown drain cancelled run {run['run_id']} "
        f"in {run['session']}; a drain must not cancel a run.",
        file=sys.stderr,
    )
pathlib.Path(path).write_text(json.dumps({"backup": backup, "pid": pid or None,
                                       "sessions_at_restart": sessions,
                                       "active_runs_at_restart": active_runs,
                                       "runs_sealed": runs_sealed,
                                       "runs_unconfirmed": runs_unconfirmed,
                                       "runs_cancelled": runs_cancelled,
                                       "sessions_sealed": sessions_sealed,
                                       "sessions_unconfirmed": sessions_unconfirmed,
                                       "sessions_cancelled": sessions_cancelled,
                                       "sessions_idle": sessions_idle,
                                       "seal_error": seal_error,
                                       "live_run_snapshot": snapshot}))
PY
}

# The live-run gate applies only when this invocation will restart (--restart, --restart-only).
# A plain placement proceeds with runs live, and prints one line saying N runs are live and
# that the binary is NOT loaded until a restart, which re-checks.
if [ "$SKIP_GATE" -eq 0 ]; then
  run_gate
  if [ "$RESTART" -eq 1 ] || [ "$RESTART_ONLY" -eq 1 ]; then
    case "$gate_rc" in
      0) echo "== gate: no live runs" ;;
      1) if [ "$LIVE_APPROVED" -eq 1 ]; then
           echo "== gate: live runs accepted by $APPROVER"
           cat "$GATE_LOG"
         else
           echo "place-release: REFUSING -- a run is live. Restart cuts it." >&2
         grep -aE "live runs|session" "$GATE_LOG" | head -3 >&2 || true
           exit 1
         fi ;;
      *) echo "place-release: REFUSING -- gate could not determine live runs (exit $gate_rc)." >&2
         echo "  A gate that cannot tell must not be read as a clear gate." >&2
         exit 2 ;;
    esac
  else
    case "$gate_rc" in
      0) echo "== gate: no live runs" ;;
      1) live_count=$(grep -aE "live runs" "$GATE_LOG" 2>/dev/null | sed -E 's/.*live runs[^0-9]*([0-9]+).*/\1/' | head -1 || true)
         # The gate said a run is live but its count did not parse: say so rather than invent one.
         if ! [[ "$live_count" =~ ^[0-9]+$ ]]; then
           runs_str="runs are live (count unreadable)"
         elif [ "$live_count" -eq 1 ]; then
           runs_str="1 run is live"
         else
           runs_str="$live_count runs are live"
         fi
         echo "== gate: $runs_str -- binary is NOT loaded until a restart, which re-checks" ;;
      *) echo "== gate: live runs could not be determined (exit $gate_rc) -- binary is NOT loaded until a restart, which re-checks" ;;
    esac
  fi
fi

# Require a new process running the inode currently on disk, not just a new pid.
restart_and_wait() {
  local expected_inode="$1" expected_sha="$2"
  local old_pid old_inode new_pid new_inode wait_seconds poll_seconds deadline
  old_pid="$(broca_pid_for_binary "$DEST")" || { echo "place-release: REFUSING -- unknown process identity" >&2; return 2; }
  old_inode="$(broca_running_inode "$old_pid" || true)"
  if [ -n "$old_pid" ] && [ -z "$old_inode" ]; then
    echo "place-release: REFUSING -- unknown running-image identity (pid $old_pid)" >&2
    return 2
  fi
  "${PLACE_RELEASE_CK:-ck}" module restart broca
  wait_seconds="${PLACE_RELEASE_WAIT_SECONDS:-90}"
  poll_seconds="${PLACE_RELEASE_POLL_SECONDS:-1}"
  deadline=$((SECONDS + wait_seconds))
  new_pid=""
  new_inode=""
  while :; do
    new_pid="$(broca_pid_for_binary "$DEST" || true)"
    new_inode="$(broca_running_inode "$new_pid" || true)"
    if [ -n "$new_pid" ] && [ "$new_pid" != "$old_pid" ] &&
       [ -n "$new_inode" ] && [ "$new_inode" = "$expected_inode" ]; then
      break
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "place-release: TIMED OUT waiting for placed image (old pid=${old_pid:-none} inode=${old_inode:-unknown}; latest pid=${new_pid:-none} inode=${new_inode:-unknown}; placed inode=$expected_inode)" >&2
      return 4
    fi
    sleep "$poll_seconds"
  done
  echo "== restarted at $(date -u +%H:%M:%SZ) (pid $new_pid)"
  write_report "$new_pid"
  echo "   Verify:  scripts/verify-deploy.sh --sha $expected_sha --marker <new> --control <known>"
}

if [ "$RESTART_ONLY" -eq 1 ]; then
  write_report
  disk_inode="$(broca_file_inode "$DEST")"
  disk_sha="$(shasum -a 256 "$DEST" | awk '{print $1}')"
  restart_and_wait "$disk_inode" "$disk_sha"
  exit $?
fi

# FILE KIND, before anything is copied or signed. A staged path is just a path: a text file, a
# script, a truncated download and a Mach-O binary all sit there identically, and every arm below
# this point would happily process the wrong one -- cat copies it, chmod makes it executable,
# codesign on this host refuses a non-binary but that is a property of THIS host's codesign, not a
# check. PLEX put a plain text file through their own staging gate this week; mine would have been
# stopped by the signing posture, which is an accident rather than an arm.
#
# Both sides, not just the staged one: comparing kinds catches a staged artifact built for another
# architecture as well as one that is not a binary at all, and it states what the deployed image
# actually is rather than assuming.
# First line only: for a universal binary `file -b` adds one line per architecture, and those
# lines carry the file's own path, so two files of the same kind would never compare equal.
dest_kind="$(broca_release_kind "$DEST" || true)"
if ! staged_kind="$(broca_release_kind "$STAGED")"; then
  if [ "$BROCA_DEPLOY_HOST" = Darwin ]; then
    echo "place-release: REFUSING -- the staged artifact is not a Mach-O binary." >&2
  else
    echo "place-release: REFUSING -- the staged artifact is not a native executable ($BROCA_DEPLOY_HOST)." >&2
  fi
    echo "  staged   $STAGED" >&2
    echo "           $staged_kind" >&2
    echo "  deployed $dest_kind" >&2
    exit 1
fi
if [ "$staged_kind" != "$dest_kind" ]; then
  echo "place-release: REFUSING -- staged and deployed are different kinds of file." >&2
  echo "  staged   $staged_kind" >&2
  echo "  deployed $dest_kind" >&2
  echo "  An architecture or format change is a deploy decision, not something to discover after" >&2
  echo "  the rename. Place it deliberately if that is what you mean." >&2
  exit 1
fi

STAGED_DSYM="${STAGED}.dSYM"
DEST_DSYM="${DEST}.dSYM"
if [ "$BROCA_DEPLOY_HOST" = Darwin ]; then
  if [ ! -d "$STAGED_DSYM" ] || [ -L "$STAGED_DSYM" ]; then
    echo "place-release: REFUSING -- the staged Mach-O has no dSYM directory: $STAGED_DSYM" >&2
    exit 1
  fi
  if [ -e "$DEST_DSYM" ] && { [ ! -d "$DEST_DSYM" ] || [ -L "$DEST_DSYM" ]; }; then
    echo "place-release: REFUSING -- the deployed dSYM path is not a directory: $DEST_DSYM" >&2
    exit 2
  fi
fi

# Same-directory temp is REQUIRED, not tidiness: rename(2) is only atomic within one filesystem,
# and /tmp is frequently a different one.
TMP="$(mktemp "${DEST}.place.XXXXXX")"
# Keep the existing exit trap: it removes both this temp image and the per-invocation gate log.

cat "$STAGED" >"$TMP"
chmod 755 "$TMP"
if [ "$BROCA_DEPLOY_HOST" = Darwin ]; then
  TMP_DSYM_PARENT="$(mktemp -d "${DEST_DSYM}.place.XXXXXX")"
  TMP_DSYM="$TMP_DSYM_PARENT/$(basename "$DEST_DSYM")"
  mkdir "$TMP_DSYM"
  if ! cp -R "$STAGED_DSYM/." "$TMP_DSYM/"; then
    echo "place-release: REFUSING -- could not copy the staged dSYM beside the destination." >&2
    exit 1
  fi
fi

# Sign the TEMP, before it becomes reachable at the destination path. Signing after the rename
# would mutate the live binary in place -- reintroducing exactly the partial-image window this
# script exists to close.
#
# --identifier is not optional: without it codesign derives an identifier from the output
# filename (here a mktemp name), and a changing identifier silently revokes macOS TCC grants.
#
# `-o runtime` is not optional either. `codesign -f` REPLACES the signature whole, so the
# hardened runtime stage-release signed in is gone unless this line asks for it again. It once
# did not, so every placed build ran unhardened whatever staging did. The gate below reads the
# flag back from these bytes rather than trusting this line.
#
# codesign is required, not skipped when absent: the file-kind check above only admits Mach-O,
# which only runs on macOS, and a copy that cannot be signed cannot have its posture checked.
if [ "$BROCA_DEPLOY_HOST" = Darwin ]; then
if ! command -v codesign >/dev/null 2>&1; then
  echo "place-release: REFUSING -- codesign is not available, so the copy cannot be signed or" >&2
  echo "  its hardened runtime checked. file: $TMP" >&2
  exit 1
fi
codesign -f -s - -o runtime --identifier ck-broca "$TMP" >/dev/null 2>&1 \
  || { echo "place-release: REFUSING -- codesign failed. file: $TMP" >&2; exit 1; }
fi
if [ "$BROCA_DEPLOY_HOST" = Darwin ] && ! broca_dsym_matches_binary "$TMP" "$TMP_DSYM"; then
  echo "place-release: REFUSING -- dwarfdump --uuid does not report matching UUIDs for" >&2
  echo "  the signed placement copy $TMP and its dSYM $TMP_DSYM." >&2
  exit 1
fi

BEFORE_SHA="$(shasum -a 256 "$DEST" | awk '{print $1}')"
STAGED_SHA="$(shasum -a 256 "$TMP" | awk '{print $1}')"

if [ "$BEFORE_SHA" = "$STAGED_SHA" ]; then
  echo "place-release: REFUSING -- staged and deployed are byte-identical; nothing to place." >&2
  echo "  This usually means the wrong artifact was staged." >&2
  exit 1
fi

# ==== THE PLACEMENT GATE ======================================================================
#
# Every check below reads $TMP: the signed copy whose bytes the rename will publish, after the
# last step that changes them. Never the staged file. The re-sign above is exactly the step that
# made a staged-artifact check worthless (it silently replaced a hardened signature with an
# unhardened one), so a check on anything but these bytes answers a question about a different
# file. Each refusal names its check and the file, and leaves the deployed binary untouched.
gate_refuse() { # <check> <file> <detail lines...>
  local check="$1" file="$2"
  shift 2
  echo "place-release: REFUSING -- placement gate check '$check' failed." >&2
  echo "  file  $file" >&2
  local line
  for line in "$@"; do
    echo "  $line" >&2
  done
  echo "  Nothing has been placed; the deployed binary is unchanged." >&2
  exit 1
}

# Run the candidate with a hard time limit, so a build that wedges at startup cannot hang the
# placement. perl's alarm survives exec, so the process that runs IS the temp binary -- the one
# whose signature is being tested, not a wrapper around it -- and SIGALRM ends it at the limit.
run_candidate() {
  perl -e 'alarm shift; exec @ARGV or exit 127' "${PLACE_RELEASE_RUN_SECONDS:-30}" "$@" </dev/null
}

if [ "$BROCA_DEPLOY_HOST" = Darwin ]; then
PLACED_FLAGS="$(broca_codesign_flags "$TMP" || true)"
DEPLOYED_FLAGS="$(broca_codesign_flags "$DEST" || true)"

# hardened-downgrade FIRST, because it is the more specific diagnosis of the same symptom as
# runtime-flag: when the running binary is hardened, an unhardened replacement is a regression
# of a property production already has, not merely a build that lacks it.
if broca_flags_have_runtime "$DEPLOYED_FLAGS" && ! broca_flags_have_runtime "$PLACED_FLAGS"; then
  gate_refuse hardened-downgrade "$TMP" \
    "the deployed binary is hardened (flags=$DEPLOYED_FLAGS, $DEST)" \
    "and the copy that would replace it is not (flags=${PLACED_FLAGS:-unreadable})."
fi

if ! broca_flags_have_runtime "$PLACED_FLAGS"; then
  gate_refuse runtime-flag "$TMP" \
    "codesign -dv reports flags=${PLACED_FLAGS:-unreadable}, without 'runtime'." \
    "Without the hardened runtime any same-user process can attach a debugger and read the" \
    "secret launch nonce the daemon gives the module."
fi

if ! PLACED_ENTITLEMENTS="$(broca_codesign_entitlements "$TMP")"; then
  gate_refuse get-task-allow "$TMP" \
    "codesign could not read the copy's entitlements, so their absence cannot be confirmed."
fi
if broca_entitlements_allow_debugger "$PLACED_ENTITLEMENTS"; then
  gate_refuse get-task-allow "$TMP" \
    "the entitlements contain com.apple.security.get-task-allow, which lets a debugger attach" \
    "whatever the hardened runtime says."
fi
fi

# smoke-run: the smallest real start. A signature that does not validate, or a hardened runtime
# the binary cannot live under, shows up here as the kernel killing it at exec -- a failure no
# flag check can see. Requiring the version line, not just exit 0, also refuses a file that runs
# but is not ck-broca.
set +e
SMOKE_OUT="$(run_candidate "$TMP" --version 2>/dev/null)"
smoke_rc=$?
set -e
SMOKE_LINE="${SMOKE_OUT%%$'\n'*}"
smoke_re='^ck-broca [0-9]+\.[0-9]+\.[0-9]+'
if [ "$smoke_rc" -ne 0 ] || ! [[ $SMOKE_LINE =~ $smoke_re ]]; then
  gate_refuse smoke-run "$TMP" \
    "'--version' exited $smoke_rc and printed '${SMOKE_LINE}'; expected exit 0 and 'ck-broca <version>'." \
    "An exit status above 128 is a signal: macOS kills a binary whose signature does not validate."
fi

# wal-feature-floor: the floor lists the WAL feature names already written on this machine, and a
# build whose reader does not know one of them would refuse those WALs. Placing it would roll the
# machine back to a reader that cannot read its own data. See scripts/wal-floor.sh.
WAL_FLOOR_FILE="$(broca_wal_floor_path)"
if ! FLOOR_NAMES="$(broca_wal_floor_names "$WAL_FLOOR_FILE")"; then
  echo "place-release: REFUSING -- placement gate check 'wal-feature-floor' cannot read the floor." >&2
  echo "  file  $WAL_FLOOR_FILE" >&2
  echo "  A floor nobody can read is not an empty floor. Nothing has been placed." >&2
  exit 2
fi
set +e
KNOWN_FEATURES="$(run_candidate "$TMP" --wal-known-features 2>/dev/null)"
known_rc=$?
set -e
if [ "$known_rc" -eq 0 ]; then
  # Exact whole-line membership, done in the shell: a `grep -q` fed by a pipe would exit on its
  # first match and could kill the writer with SIGPIPE under pipefail.
  missing=""
  while IFS= read -r name; do
    [ -n "$name" ] || continue
    case $'\n'"$KNOWN_FEATURES"$'\n' in
      *$'\n'"$name"$'\n'*) : ;;
      *) missing="${missing:+$missing, }$name" ;;
    esac
  done <<<"$FLOOR_NAMES"
  if [ -n "$missing" ]; then
    gate_refuse wal-feature-floor "$TMP" \
      "the floor at $WAL_FLOOR_FILE lists feature(s) this build does not know: $missing" \
      "WALs on this machine were written with them, and this build would refuse to read them."
  fi
  FLOOR_VERDICT="covered by the copy's --wal-known-features"
elif [ -n "$FLOOR_NAMES" ]; then
  # A build older than the flag cannot say what it knows. With a non-empty floor, that is a build
  # that predates every gated feature, so it cannot read what the floor lists.
  gate_refuse wal-feature-floor "$TMP" \
    "'--wal-known-features' exited $known_rc, so this build cannot report which WAL features it" \
    "reads, and the floor at $WAL_FLOOR_FILE is not empty."
else
  # No floor, or an empty one: no WAL frame on this machine names a feature, so any build can read
  # every WAL here and an older one is as placeable as it always was.
  FLOOR_VERDICT="empty, not checked (the copy predates --wal-known-features)"
fi

# ROLLBACK SNAPSHOT, in SUBC's naming rather than ours. Both seats place broca, and a
# rollback sweep that greps one prefix cannot see the other's snapshots -- SUBC read this
# tree and concluded no 0.3.85 rollback existed when it was sitting in the bin dir under
# `.bak-*`. The two properties that make our paths indistinguishable to a later reader:
# the `ck-<module>.rollback-<UTC>` prefix, so one sweep finds both; and the .sha256 sidecar,
# so the snapshot is verifiable without trusting its filename.
#
# UTC, not local: the old name used `date +...` with no zone, so two placements two hours
# apart across a DST edge could sort wrong. The sidecar is written in `shasum -c` format
# (two spaces, basename) so `cd <dir> && shasum -c <file>.sha256` works unmodified.
ROLLBACK_DIR="${ROLLBACK_DIR:-$HOME/.local/share/cortexkit/staging}"
mkdir -p "$ROLLBACK_DIR"
# CLAIM THE NAME EXCLUSIVELY. A plain `cp` to a second-resolution timestamp silently
# overwrote the PREVIOUS snapshot when two placements landed in the same UTC second, and
# the verify below could not see it: it hashes the newly written snapshot against the
# newly written sidecar, so the pair agrees and the arm passes while an earlier rollback
# is gone. A true answer about the wrong pair -- the exact shape this script's other arms
# exist to catch. Found by an audit 40 minutes after this convention shipped.
#
# `set -C` makes the redirect fail if the path exists, so the loop cannot race a concurrent
# creator the way a test-then-copy would. The prefix is preserved either way, so SUBC's
# `ck-<module>.rollback-*` sweeps still see both.
BACKUP_BASE="${ROLLBACK_DIR}/$(basename "$DEST").rollback-$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP="$BACKUP_BASE"
attempt=0
until ( set -C; : > "$BACKUP" ) 2>/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -gt 50 ]; then
    echo "place-release: REFUSING -- cannot claim a rollback name under $BACKUP_BASE" >&2
    echo "  50 candidates were already taken. Nothing has been placed." >&2
    exit 2
  fi
  BACKUP="${BACKUP_BASE}.${attempt}"
done
cp "$DEST" "$BACKUP"
chmod 700 "$BACKUP"
if [ "$BROCA_DEPLOY_HOST" = Darwin ] && [ -d "$DEST_DSYM" ] && [ ! -L "$DEST_DSYM" ]; then
  ROLLBACK_DSYM="${BACKUP}.dSYM"
  if ! cp -R "$DEST_DSYM" "$ROLLBACK_DSYM" || ! diff -qr "$DEST_DSYM" "$ROLLBACK_DSYM" >/dev/null; then
    rm -rf "$ROLLBACK_DSYM"
    echo "place-release: REFUSING -- could not preserve the deployed dSYM with its rollback binary." >&2
    exit 2
  fi
elif [ "$BROCA_DEPLOY_HOST" = Darwin ] && [ -e "$DEST_DSYM" ]; then
  echo "place-release: REFUSING -- the deployed dSYM path is not a directory: $DEST_DSYM" >&2
  exit 2
fi
( cd "$ROLLBACK_DIR" && shasum -a 256 "$(basename "$BACKUP")" > "$(basename "$BACKUP").sha256" )
if ! ( cd "$ROLLBACK_DIR" && shasum -c "$(basename "$BACKUP").sha256" >/dev/null 2>&1 ); then
  echo "place-release: REFUSING -- the rollback sidecar does not verify its own snapshot." >&2
  echo "  A rollback nobody can verify is not a rollback. Nothing has been placed." >&2
  exit 2
fi

write_report

BEFORE_INODE="$(broca_file_inode "$DEST")"

if [ "$BROCA_DEPLOY_HOST" = Darwin ]; then
  if [ -e "$DEST_DSYM" ]; then
    OLD_DSYM_TEMP="${DEST_DSYM}.previous.$$"
    attempt=0
    while [ -e "$OLD_DSYM_TEMP" ]; do
      attempt=$((attempt + 1))
      OLD_DSYM_TEMP="${DEST_DSYM}.previous.$$.${attempt}"
    done
    if ! mv "$DEST_DSYM" "$OLD_DSYM_TEMP"; then
      echo "place-release: REFUSING -- could not move the deployed dSYM aside for atomic replacement." >&2
      exit 2
    fi
  fi
  DSYM_SWAP_STARTED=1
  if ! mv "$TMP_DSYM" "$DEST_DSYM"; then
    echo "place-release: REFUSING -- could not atomically place the dSYM directory: $DEST_DSYM" >&2
    exit 2
  fi
  TMP_DSYM=""
  rmdir "$TMP_DSYM_PARENT"
  TMP_DSYM_PARENT=""
fi

if ! mv -f "$TMP" "$DEST"; then
  echo "place-release: REFUSING -- could not atomically place the binary: $DEST" >&2
  exit 2
fi
PLACEMENT_COMMITTED=1
TMP=""
if [ -n "$OLD_DSYM_TEMP" ]; then
  rm -rf "$OLD_DSYM_TEMP"
  OLD_DSYM_TEMP=""
fi

AFTER_INODE="$(broca_file_inode "$DEST")"
AFTER_SHA="$(shasum -a 256 "$DEST" | awk '{print $1}')"

echo "== placed atomically"
echo "   dest      $DEST"
echo "   backup    $BACKUP"
if [ "$BROCA_DEPLOY_HOST" = Darwin ]; then
  echo "   dSYM      $DEST_DSYM"
  [ ! -d "${BACKUP}.dSYM" ] || echo "   backup dSYM ${BACKUP}.dSYM"
fi
echo "   sha       ${BEFORE_SHA:0:16} -> ${AFTER_SHA:0:16}"
echo "   inode     ${BEFORE_INODE} -> ${AFTER_INODE}"
echo "   version   ${SMOKE_LINE}"
if [ "$BROCA_DEPLOY_HOST" = Darwin ]; then
  echo "   signing   flags=${PLACED_FLAGS}, no get-task-allow (read from the placed copy)"
else
  echo "   signing   not applicable on Linux; ELF is unsigned (no macOS debugger-attachment protection claimed)"
fi
echo "   floor     $(broca_wal_floor_summary "$WAL_FLOOR_FILE") -- ${FLOOR_VERDICT}"
echo "             ($WAL_FLOOR_FILE)"

if [ "$AFTER_SHA" != "$STAGED_SHA" ]; then
  echo "place-release: PLACED BYTES DO NOT MATCH THE STAGED ARTIFACT." >&2
  echo "  Restore: mv '$BACKUP' '$DEST'" >&2
  exit 1
fi

# A same-inode result after a rename means the rename did not happen as understood -- most
# likely the temp and destination were on different filesystems and something fell back to a
# copy. Report it rather than let a later inode check inherit a false premise.
if [ "$BEFORE_INODE" = "$AFTER_INODE" ]; then
  echo "place-release: WARNING -- inode unchanged across a rename." >&2
  echo "  Placement may have degraded to a copy; do not trust a post-place inode check." >&2
fi

if [ "$RESTART" -eq 0 ]; then
  echo
  echo "   NOT YET RUNNING. Load it with --restart next time, or by hand ONLY after re-checking"
  echo "   that no run is live:  ck module restart broca"
  echo "   Then verify:  scripts/verify-deploy.sh --sha $AFTER_SHA --marker <new> --control <known>"
  exit 0
fi

# THE SECOND CHECK. The first gate ran before the copy, the signing and the rollback snapshot, so
# a run can have started since. That window is exactly how the 2026-09-24 run was cut. Re-check
# immediately before the restart: if a run is live now, leave the new bytes on disk unloaded,
# which harms nothing, and say so.
if [ "$SKIP_GATE" -eq 0 ]; then
  run_gate
  if [ "$gate_rc" -eq 1 ] && [ "$LIVE_APPROVED" -eq 1 ]; then
    echo "== gate: live runs accepted immediately before restart by $APPROVER"
    cat "$GATE_LOG"
  elif [ "$gate_rc" -ne 0 ]; then
    echo "place-release: PLACED but NOT restarted -- the live-run gate is no longer clear (exit $gate_rc)." >&2
    grep -aE "live runs|session" "$GATE_LOG" | head -3 >&2 || true
    echo "  The new binary is on disk and not loaded. Restart when the gate is clear:" >&2
    echo "    scripts/place-release.sh --restart-only --dest '$DEST'" >&2
    exit 3
  fi
fi

write_report
restart_and_wait "$AFTER_INODE" "$AFTER_SHA"
