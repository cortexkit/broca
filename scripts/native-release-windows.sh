#!/usr/bin/env bash
# Run the shared native release lanes against the rigs staged by Windows native CI.
set -uo pipefail
logdir=${1:?usage: bash scripts/native-release-windows.sh LOG_DIR [LANE ...]}
shift
only=" $* "
mkdir -p "$logdir" && logdir=$(cd "$logdir" && pwd) || exit 2
cd "$(dirname "$0")/.." || exit 2

export CARGO_TERM_COLOR=never
unset BROCA_CONFORMANCE_SKIP_CRASH_KILLS

status=0
selected=0
started=$SECONDS
table="$logdir/summary.md"
printf '| Lane | Ran | Passed | Failed | Ignored | Filtered out | Seconds | Result |\n|:---|---:|---:|---:|---:|---:|---:|:---|\n' > "$table"

run() {
  local name=$1
  shift
  if [[ "$only" != '  ' && "$only" != *" $name "* ]]; then
    return
  fi
  ((selected+=1))
  local log="$logdir/$name.log" begin=$SECONDS result=PASS exit_code=0
  local exclusion
  if exclusion=$(native_release_lane_exclusion windows "$name"); then
    printf 'NOT APPLICABLE: %s\n' "$exclusion" > "$log"
    printf '| %s | — | — | — | — | — | — | NOT APPLICABLE: %s |\n' "$name" "$exclusion" >> "$table"
    return
  fi
  local -a command=() output=(-- --show-output)
  local arg
  for arg in "$@"; do
    command+=("$arg")
    # Cargo must keep running the other binaries after a failed test; libtest must
    # show output even from passing tests so explicitly logged skips are visible.
    case "$arg" in
      test) command+=(--no-fail-fast) ;;
      --) output=(--show-output) ;;
    esac
  done
  echo "==> $name"
  nice -n 10 env CARGO_INCREMENTAL=0 "${command[@]}" "${output[@]}" > "$log" 2>&1 || exit_code=$?

  local passed failed ignored filtered
  read -r passed failed ignored filtered < <(awk '
    /^test result:/ {p+=$4; f+=$6; i+=$8; s+=$12}
    END {print p+0, f+0, i+0, s+0}' "$log")
  if ((exit_code != 0 || failed != 0)); then
    result="FAIL (cargo $exit_code)"
  elif ((passed + failed == 0)); then
    result='NO TESTS RAN'
  fi
  if [[ "$result" != PASS ]]; then
    status=1
    grep -E '^(error|test .* FAILED)|panicked at' "$log" || true
  fi
  # Put individual binaries' counts (including empty binaries) in the job log.
  grep -E '^[[:space:]]*Running |^test result:' "$log" || true
  printf '| %s | %d | %d | %d | %d | %d | %d | %s |\n' \
    "$name" "$((passed + failed))" "$passed" "$failed" "$ignored" "$filtered" "$((SECONDS - begin))" "$result" >> "$table"
}

# shellcheck source=scripts/native-release-lanes.sh
source scripts/native-release-lanes.sh
if ((selected == 0)); then
  echo "No native release lane matched:$only" >&2
  exit 2
fi
printf '\nTotal suite elapsed: %d seconds (includes Cargo builds, excludes fixture setup).\n' \
  "$((SECONDS - started))" >> "$table"
if [[ -n ${GITHUB_STEP_SUMMARY:-} ]]; then
  cat "$table" >> "$GITHUB_STEP_SUMMARY"
fi
cat "$table"
exit "$status"
