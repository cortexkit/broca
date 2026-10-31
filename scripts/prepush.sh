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
  echo 'usage: bash scripts/prepush.sh [--lane fast|linux-small|linux-operator|linux|linux-lint|linux-portable|linux-unit|linux-integration-{1,2,3,4}|linux-production|linux-msrv|linux-wal|linux-deploy|windows|macos]' >&2
  exit 2
fi
command -v ck >/dev/null || {
  echo 'prepush: ck motor run is required; no local fallback. See docs/prepush.md for split AFT runon commands.' >&2
  exit 200
}
mkdir -p target/scratch
logs="$(mktemp -d "$root/target/scratch/prepush.XXXXXX")"
candidate="$(git rev-parse HEAD)"
tracked_inputs="$(git ls-files -- prepush-inputs)"
if [[ -n "$tracked_inputs" ]]; then
  printf 'prepush: NOT-RUN: generated prepush-inputs paths must not be tracked:\n%s\n' "$tracked_inputs" >&2
  exit 200
fi
if [[ -n "$(git status --porcelain)" ]]; then
  echo 'prepush: NOT-RUN fast: commit the candidate and start from a clean worktree' >&2
  exit 1
fi
# Side worktrees belong in the scratch project area, never in core's worker pool.
checkout_id="$(basename "$root")-$(python3 -c 'import hashlib,sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest()[:12])' "$root")"
checkouts="${BROCA_PREPUSH_LANE_POOL:-$HOME/Work/Projects/CortexKit-scratch/broca-prepush-lanes/$checkout_id}"
mkdir -p "$checkouts"
names=() pids=() results=() durations=() requests=()
started=$SECONDS
# Long native/archive lanes start first; short jobs fill released slots, not Motor's queue.
plan_names=() plan_platforms=() plan_cpus=()
queue_lane() {
  plan_names+=("$1") plan_platforms+=("$2") plan_cpus+=("$3")
}
# Full CI unit/doc took 981s at four workers; start it before shorter compilation jobs.
queue_lane linux-unit linux 4
# Shard 4 took 699s at eight workers and is the longest completed integration partition.
queue_lane linux-integration-4 linux 8
# Production took 603s at four workers; it shares the first wave with unit and three eight-CPU jobs.
queue_lane linux-production linux 4
# Shard 2 took 582s at eight workers; retain its compiler reservation in the initial wave.
queue_lane linux-integration-2 linux 8
# Windows builds fixtures/archive and runs all four CI integration partitions sequentially.
queue_lane windows windows 8
# Shard 3 took 575s at eight workers and is first in line for released full-build capacity.
queue_lane linux-integration-3 linux 8
# Shard 1 took 399s at eight workers; its archive still uses the full workspace graph.
queue_lane linux-integration-1 linux 8
# Operator/WAL took 316s at two workers and can backfill a partially released reservation.
queue_lane linux-small linux 2
# Deployment took 228s at eight workers, including its required native fixture build.
queue_lane linux-deploy linux 8
# Native and Windows-target clippy took 73s + 88s at eight workers; tests run in their CI lanes.
queue_lane linux-lint linux 8
# Native macOS's targeted contention/WAL checks took 114s; two workers under nice -n 10.
queue_lane macos macos 2
# Minimum supported Rust version (MSRV) checking took 81s at eight workers.
queue_lane linux-msrv linux 8
cpu_budget=32
planned_cpus=0
for cpus in "${plan_cpus[@]}"; do planned_cpus=$((planned_cpus + cpus)); done
echo "prepush: total requested vCPUs: at most $cpu_budget concurrent ($planned_cpus across queued lanes); fast uses 2 alone"

lane_directory() {
  # Lint replaces portable dispatch; retain that checkout's warm compiler cache.
  if [[ "$1" == linux-lint ]]; then
    printf '%s\n' "$checkouts/linux-portable"
  else
    printf '%s\n' "$checkouts/$1"
  fi
}

prepare_lane_worktree() {
  local name="$1" destination common expected
  destination="$(lane_directory "$name")"
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

windows_input_python() {
  local candidate
  for candidate in python3 python3.14 python3.13 python3.12 python3.11 python; do
    if command -v "$candidate" >/dev/null && "$candidate" -c 'import tomllib' >/dev/null 2>&1; then
      command -v "$candidate"
      return 0
    fi
  done
  echo 'prepush: NOT-RUN windows: Cargo input preparation requires an installed Python 3.11+ with tomllib' >&2
  return 1
}

launch() {
  local name="$1" platform="$2" cpus="$3" prepared="${4:-0}"
  local destination
  destination="$(lane_directory "$name")"
  local args=(--platform "$platform")
  [[ "$platform" != linux ]] || args+=(--net)
  args+=(--timeout 7200 --cwd "$destination" --record-file "$logs/$name.motor.jsonl")
  case "$name" in
    linux-unit|linux-integration-*)
      args+=(--return-path "$checkouts/$name/.cortexkit/internal-docs/investigations/prepush-portable-coverage-$name.json") ;;
  esac
  [[ -z "$cpus" ]] || args+=(--vcpus "$cpus")
  local index="${#names[@]}"
  local begin=$SECONDS begin_ms ready=0 status
  begin_ms="$(timestamp_ms)"
  # Worktrees share Git metadata; serialize checkout preparation on the caller.
  {
    if [[ "$prepared" == 1 && "$name" == windows ]]; then
      status="$(git -C "$checkouts/windows" status --porcelain --untracked-files=normal)" || status=unreadable
      if [[ "$(git -C "$checkouts/windows" rev-parse HEAD)" == "$candidate" && "$status" == '?? prepush-inputs/' ]]; then
        ready=1
      fi
    elif prepare_lane_worktree "$name"; then
      ready=1
    fi
  } > "$logs/$name.prepare.log" 2>&1
  names+=("$name") requests+=("$cpus")
  (
    if [[ ${scheduling:-0} == 1 ]]; then
      trap 'printf "%s\n" "$index" >&3' EXIT
    fi
    rc=0
    requested_ms="$begin_ms"
    cat "$logs/$name.prepare.log"
    if [[ "$ready" != 1 ]]; then
      echo "prepush: NOT-RUN $name: could not prepare its isolated checkout"
      rc=204
    else
      requested_ms="$(timestamp_ms)"
      command="bash scripts/prepush.sh --lane $name"
      if [[ "$platform" == windows ]]; then
        command='export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-C:/t}"; bash scripts/prepush.sh --lane windows'
      fi
      ck motor run "${args[@]}" -- "$command" || rc=$?
    fi
    if [[ "$name" == windows && "$prepared" == 1 && "$rc" != 201 ]]; then
      rm -rf -- "$checkouts/windows/prepush-inputs"
    fi
    ended_ms="$(timestamp_ms)"
    python3 scripts/lib/prepush-motor-timing.py "$logs/$name.motor.jsonl" "$requested_ms" \
      "$((requested_ms - begin_ms))" "$((ended_ms - begin_ms))" --vcpus "$cpus" > "$logs/$name.timing.json"
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
    if [[ "$state" == PASS && "${names[$i]}" == windows ]] &&
       grep -Fq 'owner-distinct check not run: token cannot assign a foreign owner' "$logs/windows.log"; then
      state=PARTIAL
    fi
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
  python3 scripts/lib/prepush-motor-timing.py --critical "$logs" "$gate_started_ms" "$(timestamp_ms)" "$cpu_budget" || failed=1
  echo "prepush: total wall $((SECONDS - started))s; logs: $logs"
  echo 'The integrating agent also runs the native release gates: see docs/prepush.md.'
  return "$failed"
}

# Metadata, formatting and static guards do not compile the workspace: two CPUs.
gate_started_ms="$(timestamp_ms)"
launch fast linux 2
collect 0
if [[ ${results[0]} != 0 ]]; then
  echo 'prepush: heavy lanes NOT RUN because fast checks failed'
  summarize
  exit 1
fi
# All offline bytes stay inside the Windows worktree; no sibling-path capability.
windows_begin=$SECONDS
windows_ready=0
windows_inputs_owned=0
if prepare_lane_worktree windows > "$logs/windows-inputs.log" 2>&1; then
  windows_inputs_owned=1
  if windows_python="$(windows_input_python 2>> "$logs/windows-inputs.log")" &&
     "$windows_python" scripts/prepare-prepush-windows.py --output "$checkouts/windows/prepush-inputs" \
     --source-cache "$checkouts/windows/target/prepush-source-cache" \
     --cache "$root/target/scratch/prepush-offline-cache" --require-snapshot-visible >> "$logs/windows-inputs.log" 2>&1; then
    windows_ready=1
  fi
fi
cat "$logs/windows-inputs.log"
echo "prepush: Windows input preparation wall $((SECONDS - windows_begin))s"
if [[ "$windows_ready" != 1 ]]; then
  names+=(windows)
  requests+=(0)
  pids+=(0)
  results[${#names[@]}-1]=204
  durations[${#names[@]}-1]="$((SECONDS - windows_begin))"
  cp "$logs/windows-inputs.log" "$logs/windows.log"
  echo 'prepush: NOT-RUN windows: offline input preparation failed'
  [[ "$windows_inputs_owned" != 1 ]] || rm -rf -- "$checkouts/windows/prepush-inputs"
fi
# A completion FIFO works with macOS's Bash 3 as well as Linux, unlike wait -n.
mkfifo "$logs/completed.fifo"
exec 3<> "$logs/completed.fifo"
trap 'exec 3>&-; rm -f "$logs/completed.fifo"' EXIT
scheduling=1 in_flight=0 active=0 pending="${#plan_names[@]}"
admitted=()
while [[ "$pending" -gt 0 || "$active" -gt 0 ]]; do
  for p in "${!plan_names[@]}"; do
    [[ ${admitted[$p]:-0} == 0 ]] || continue
    if [[ "${plan_names[$p]}" == windows && "$windows_ready" != 1 ]]; then
      admitted[$p]=1
      pending=$((pending - 1))
      continue
    fi
    cpus="${plan_cpus[$p]}"
    [[ $((in_flight + cpus)) -le "$cpu_budget" ]] || continue
    prepared=0
    [[ "${plan_names[$p]}" != windows ]] || prepared=1
    launch "${plan_names[$p]}" "${plan_platforms[$p]}" "$cpus" "$prepared"
    admitted[$p]=1
    pending=$((pending - 1)) active=$((active + 1)) in_flight=$((in_flight + cpus))
    echo "prepush: requested vCPUs in flight=$in_flight/$cpu_budget"
  done
  if [[ "$active" -gt 0 ]]; then
    IFS= read -r completed <&3
    collect "$completed"
    # A lost connection does not prove that the server released its CPUs.
    if [[ ${results[$completed]} != 201 ]]; then
      in_flight=$((in_flight - requests[completed]))
    fi
    active=$((active - 1))
  elif [[ "$pending" -gt 0 ]]; then
    for p in "${!plan_names[@]}"; do
      [[ ${admitted[$p]:-0} == 0 ]] || continue
      name="${plan_names[$p]}"
      names+=("$name") pids+=(0) requests+=(0)
      results[${#names[@]}-1]=204 durations[${#names[@]}-1]=0
      echo "prepush: NOT-RUN $name: UNKNOWN jobs still hold the remaining vCPU budget" > "$logs/$name.log"
      admitted[$p]=1
    done
    pending=0
  fi
done
summarize
