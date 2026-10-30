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

prepare_inputs_checkout() {
  local name=prepush-inputs destination="$checkouts/prepush-inputs" origin
  if [[ ! -e "$destination" ]]; then
    git clone --quiet --shared "$root" "$destination" || return 1
    printf 'managed by scripts/prepush.sh\n' > "$destination/.git/broca-prepush-managed" || return 1
  fi
  [[ -f "$destination/.git/broca-prepush-managed" ]] || {
    echo "prepush: refusing unmanaged checkout $destination" >&2
    return 1
  }
  origin="$(git -C "$root" config --get remote.origin.url || true)"
  if [[ -n "$origin" ]]; then
    git -C "$destination" remote set-url origin "$origin" || return 1
  fi
  git -C "$destination" fetch --quiet "$root" "$candidate" || return 1
  git -C "$destination" checkout --quiet --detach "$candidate" || return 1
  [[ "$(git -C "$destination" rev-parse HEAD)" == "$candidate" ]] || return 1
  local dirty
  dirty="$(git -C "$destination" status --porcelain)"
  [[ -z "$dirty" || ( "$name" == prepush-inputs && "$dirty" == '?? inputs/' ) ]] || {
    echo "prepush: managed checkout is dirty: $destination" >&2
    return 1
  }
}

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
  git -C "$destination" checkout --quiet --detach "$candidate" || return 1
  [[ "$(git -C "$destination" rev-parse HEAD)" == "$candidate" ]]
}

timestamp_ms() {
  python3 -c 'import time; print(time.time_ns() // 1000000)'
}

launch() {
  local name="$1" platform="$2" cpus="$3"
  local args=(--platform "$platform")
  [[ "$platform" != linux ]] || args+=(--net)
  args+=(--timeout 7200 --cwd "$checkouts/$name" --record-file "$logs/$name.motor.jsonl")
  [[ -z "$cpus" ]] || args+=(--vcpus "$cpus")
  if [[ "$platform" == windows ]]; then
    args+=(--sibling "$checkouts/prepush-inputs" --sibling "$checkouts/subconscious")
  fi
  names+=("$name")
  (
    begin=$SECONDS
    begin_ms="$(timestamp_ms)"
    rc=0
    requested_ms="$begin_ms"
    if ! prepare_lane_worktree "$name"; then
      echo "prepush: NOT-RUN $name: could not prepare its isolated checkout"
      rc=204
    else
      requested_ms="$(timestamp_ms)"
      ck motor run "${args[@]}" -- "bash scripts/prepush.sh --lane $name" || rc=$?
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

launch fast linux 8
collect 0
if [[ ${results[0]} != 0 ]]; then
  echo 'prepush: heavy lanes NOT RUN because fast checks failed'
  summarize
  exit 1
fi
# Each lane has a different checkout: motor serializes jobs from one workspace.
launch linux-operator linux 8
launch linux-portable linux 16
launch linux-unit linux 8
for shard in 1 2 3 4; do launch "linux-integration-$shard" linux 8; done
launch linux-production linux 8
launch linux-msrv linux 8
launch linux-wal linux 8
launch linux-deploy linux 16
launch macos macos ''

# Stage downloads on the caller and send them as sibling inputs; Windows has no network.
windows_begin=$SECONDS
windows_ready=0
if prepare_inputs_checkout > "$logs/windows-inputs.log" 2>&1 &&
   python3 scripts/prepare-prepush-windows.py --output "$checkouts/prepush-inputs/inputs" \
     --siblings "$checkouts" --cache "$root/target/scratch/prepush-offline-cache" >> "$logs/windows-inputs.log" 2>&1; then
  windows_ready=1
fi
cat "$logs/windows-inputs.log"
echo "prepush: Windows input preparation wall $((SECONDS - windows_begin))s"
if [[ "$windows_ready" == 1 ]]; then
  launch windows windows ''
else
  names+=(windows)
  pids+=(0)
  results[${#names[@]}-1]=204
  durations[${#names[@]}-1]="$((SECONDS - windows_begin))"
  cp "$logs/windows-inputs.log" "$logs/windows.log"
  echo 'prepush: NOT-RUN windows: offline input preparation failed'
fi
for i in "${!names[@]}"; do
  [[ "$i" == 0 || "${pids[$i]}" == 0 ]] || collect "$i"
done
summarize
