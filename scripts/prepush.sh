#!/usr/bin/env bash
# Dispatch builds to motor; never fall back to building on the caller's machine.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
export PYTHONDONTWRITEBYTECODE=1
source scripts/lib/prepush-report.sh

if [[ $# -gt 0 ]]; then
  if [[ $# == 2 && $1 == --lane ]]; then
    exec bash scripts/lib/prepush-lane.sh "$2"
  fi
  echo 'usage: bash scripts/prepush.sh [--lane fast|linux-operator|linux|linux-portable|linux-unit|linux-integration-{1,2,3,4}|linux-production|linux-msrv|linux-wal|linux-deploy|windows|macos]' >&2
  exit 2
fi
command -v ck >/dev/null || {
  echo 'prepush: ck motor run is required; no local fallback. See docs/prepush.md for split AFT runon commands.' >&2
  exit 200
}
mkdir -p target/scratch
logs="$(mktemp -d "$root/target/scratch/prepush.XXXXXX")"
candidate="$(git rev-parse HEAD)"
if [[ -n "$(git status --porcelain)" ]]; then
  echo 'prepush: NOT-RUN fast: commit the candidate and start from a clean worktree' >&2
  exit 1
fi
checkouts="${BROCA_PREPUSH_LANE_POOL:-$(dirname "$root")/broca-prepush-lanes}"
mkdir -p "$checkouts"
names=() pids=() results=() durations=()
started=$SECONDS

prepare_lane_worktree() {
  local name="$1" destination="$checkouts/$1" common expected
  if [[ ! -e "$destination" ]]; then
    git worktree add --quiet --detach "$destination" "$candidate" || return 1
  fi
  common="$(git -C "$destination" rev-parse --path-format=absolute --git-common-dir)" || return 1
  expected="$(git rev-parse --path-format=absolute --git-common-dir)" || return 1
  [[ "$common" == "$expected" ]] || {
    echo "prepush: lane pool entry belongs to another repository: $destination" >&2
    return 1
  }
  [[ -z "$(git -C "$destination" status --porcelain)" ]] || {
    echo "prepush: lane worktree is dirty; refusing to update it: $destination" >&2
    return 1
  }
  # Ignored or empty inputs are invisible to status, but are not ours to overwrite.
  if [[ "$name" == windows && ( -e "$destination/prepush-inputs" || -L "$destination/prepush-inputs" ) ]]; then
    echo "prepush: refusing existing Windows input path: $destination/prepush-inputs" >&2
    return 1
  fi
  git -C "$destination" checkout --quiet --detach "$candidate" || return 1
  [[ "$(git -C "$destination" rev-parse HEAD)" == "$candidate" ]]
}

timestamp_ms() {
  python3 -c 'import time; print(time.time_ns() // 1000000)'
}

launch() {
  local name="$1" platform="$2" cpus="$3" prepared="${4:-0}"
  local args=(--platform "$platform")
  [[ "$platform" != linux ]] || args+=(--net)
  args+=(--timeout 7200 --cwd "$checkouts/$name" --record-file "$logs/$name.motor.jsonl")
  [[ -z "$cpus" ]] || args+=(--vcpus "$cpus")
  names+=("$name")
  (
    begin=$SECONDS
    begin_ms="$(timestamp_ms)"
    rc=0
    requested_ms="$begin_ms"
    ready=0
    if [[ "$prepared" == 1 && "$name" == windows ]]; then
      status="$(git -C "$checkouts/windows" status --porcelain --untracked-files=normal)"
      if [[ "$(git -C "$checkouts/windows" rev-parse HEAD)" == "$candidate" && "$status" == '?? prepush-inputs/' ]]; then
        ready=1
      fi
    elif prepare_lane_worktree "$name"; then
      ready=1
    fi
    if [[ "$ready" != 1 ]]; then
      echo "prepush: NOT-RUN $name: could not prepare its isolated checkout"
      rc=204
    else
      requested_ms="$(timestamp_ms)"
      ck motor run "${args[@]}" -- "bash scripts/prepush.sh --lane $name" || rc=$?
    fi
    if [[ "$name" == windows && "$prepared" == 1 && "$rc" != 201 ]]; then
      rm -rf -- "$checkouts/windows/prepush-inputs"
    fi
    ended_ms="$(timestamp_ms)"
    python3 scripts/lib/prepush-motor-timing.py "$logs/$name.motor.jsonl" "$requested_ms" \
      "$((requested_ms - begin_ms))" "$((ended_ms - begin_ms))" > "$logs/$name.timing.json"
    printf '%s\n' "$((SECONDS - begin))" > "$logs/$name.seconds"
    exit "$rc"
  ) > "$logs/$name.log" 2>&1 &
  pids+=("$!")
  echo "prepush: launched $name ($platform${cpus:+,$cpus}); log: $logs/$name.log"
}

collect() {
  local i="$1" rc=0 elapsed
  wait "${pids[$i]}" || rc=$?
  results[$i]="$rc"
  elapsed=unknown
  [[ ! -f "$logs/${names[$i]}.seconds" ]] || read -r elapsed < "$logs/${names[$i]}.seconds"
  durations[$i]="$elapsed"
  cat "$logs/${names[$i]}.log"
}

summarize() {
  local i failed=0 state
  echo '==> prepush lane summary (wall seconds, including dispatch)'
  for i in "${!names[@]}"; do
    state="$(dispatch_state "${results[$i]}")"
    printf '%-24s %-10s exit=%-3s wall=%ss' "${names[$i]}" "$state" "${results[$i]}" "${durations[$i]}"
    if [[ -f "$logs/${names[$i]}.timing.json" ]]; then
      python3 - "$logs/${names[$i]}.timing.json" <<'PY'
import json, sys
with open(sys.argv[1]) as source:
    timing = json.load(source)
def seconds(key):
    value = timing[key]
    return '-' if value is None else f'{value / 1000:.3f}s'
print(f" checkout={seconds('checkout_ms')} request-to-start={seconds('request_to_start_ms')} run={seconds('run_ms')}")
PY
    else
      printf '\n'
    fi
    if [[ ${results[$i]} != 0 ]]; then
      failed=1
      echo "--- FIRST ERRORS ${names[$i]} ($state) ---"
      first_errors "$logs/${names[$i]}.log"
      # Replay the complete failed lane, not a grep that could lose failing test names.
      echo "--- $state ${names[$i]}: complete output ---"
      cat "$logs/${names[$i]}.log"
    fi
  done
  if [[ "$(git rev-parse HEAD)" != "$candidate" || -n "$(git status --porcelain)" ]]; then
    echo 'prepush: FAILED candidate identity: HEAD/worktree changed during the gate'
    failed=1
  fi
  echo "prepush: total wall $((SECONDS - started))s; logs: $logs"
  echo 'The integrating agent also runs the native release gates: see docs/prepush.md.'
  return "$failed"
}

# Metadata, formatting and static guards do not compile the workspace: two CPUs.
launch fast linux 2
collect 0
if [[ ${results[0]} != 0 ]]; then
  echo 'prepush: heavy lanes NOT RUN because fast checks failed'
  summarize
  exit 1
fi
# Each lane has a different checkout: motor serializes jobs from one workspace.
# Python/operator fixtures use at most two compiler workers, not a full build.
launch linux-operator linux 2
# Portable clippy plus lib/bin/integration compilation builds the full workspace.
launch linux-portable linux 8
# The unit lane builds all workspace lib/bin/doc targets with eight Cargo workers.
launch linux-unit linux 8
# Each shard compiles a full workspace archive; execution remains capped at four.
for shard in 1 2 3 4; do launch "linux-integration-$shard" linux 8; done
# Three-package lint/lib tests use four workers and need no large-memory lease.
launch linux-production linux 4
# Workspace metadata/type checking uses four workers rather than release codegen.
launch linux-msrv linux 4
# Two small WAL libraries and their tests use two workers.
launch linux-wal linux 2
# Optimized module/session/run compilation is a real full-stack build: eight workers.
launch linux-deploy linux 8
# Native macOS capacity is provided by its runner; commands remain nice -n 10.
launch macos macos ''

# All offline bytes stay inside the Windows worktree; no sibling-path capability.
windows_begin=$SECONDS
windows_ready=0
windows_inputs_owned=0
if prepare_lane_worktree windows > "$logs/windows-inputs.log" 2>&1; then
  windows_inputs_owned=1
  if python3 scripts/prepare-prepush-windows.py --output "$checkouts/windows/prepush-inputs" \
     --source-cache "$checkouts/windows/target/prepush-source-cache" \
     --cache "$root/target/scratch/prepush-offline-cache" --require-snapshot-visible >> "$logs/windows-inputs.log" 2>&1; then
    windows_ready=1
  fi
fi
cat "$logs/windows-inputs.log"
echo "prepush: Windows input preparation wall $((SECONDS - windows_begin))s"
if [[ "$windows_ready" == 1 ]]; then
  # Windows compiles the full archive and workspace test binaries: eight workers.
  launch windows windows 8 1
else
  names+=(windows)
  pids+=(0)
  results[${#names[@]}-1]=204
  durations[${#names[@]}-1]="$((SECONDS - windows_begin))"
  cp "$logs/windows-inputs.log" "$logs/windows.log"
  echo 'prepush: NOT-RUN windows: offline input preparation failed'
  [[ "$windows_inputs_owned" != 1 ]] || rm -rf -- "$checkouts/windows/prepush-inputs"
fi
for i in "${!names[@]}"; do
  [[ "$i" == 0 || "${pids[$i]}" == 0 ]] || collect "$i"
done
summarize
