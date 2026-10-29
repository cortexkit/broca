#!/usr/bin/env bash
# Dispatch builds to motor; never fall back to building on the caller's machine.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
export PYTHONDONTWRITEBYTECODE=1

if [[ $# -gt 0 ]]; then
  if [[ $# == 2 && $1 == --lane ]]; then
    exec bash scripts/lib/prepush-lane.sh "$2"
  fi
  echo 'usage: bash scripts/prepush.sh [--lane fast|linux|windows|macos]' >&2
  exit 2
fi
command -v ck >/dev/null || {
  echo 'prepush: ck motor run is required; no local fallback. See docs/prepush.md for split AFT runon commands.' >&2
  exit 200
}
mkdir -p target/scratch
logs="$(mktemp -d "$root/target/scratch/prepush.XXXXXX")"
names=() pids=() results=() durations=()
started=$SECONDS

launch() {
  local name="$1" platform="$2" cpus="$3"
  local args=(--platform "$platform")
  [[ "$platform" == macos ]] || args+=(--net)
  args+=(--timeout 7200 --cwd "$root")
  [[ -z "$cpus" ]] || args+=(--vcpus "$cpus")
  names+=("$name")
  (
    begin=$SECONDS
    rc=0
    ck motor run "${args[@]}" -- "bash scripts/prepush.sh --lane $name" || rc=$?
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
  local i failed=0
  echo '==> prepush lane summary (wall seconds, including dispatch)'
  for i in "${!names[@]}"; do
    printf '%-12s exit=%-3s %ss\n' "${names[$i]}" "${results[$i]}" "${durations[$i]}"
    if [[ ${results[$i]} != 0 ]]; then
      failed=1
      # Replay the complete failed lane, not a grep that could lose failing test names.
      echo "--- FAILED ${names[$i]}: complete output ---"
      cat "$logs/${names[$i]}.log"
    fi
  done
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
# Different platforms have independent runner workspaces and target caches.
launch linux linux 16
launch windows windows ''
launch macos macos ''
collect 1
collect 2
collect 3
summarize
