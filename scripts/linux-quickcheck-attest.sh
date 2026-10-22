#!/usr/bin/env bash
# Run the crate quickcheck on Linux and record the exact staged tree it verified.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

if [[ "$(uname -s)" != Linux ]]; then
  echo "linux-quickcheck-attest: this wrapper must run on Linux" >&2
  exit 2
fi

output=""
if [[ ${1:-} == --output ]]; then
  [[ $# -ge 3 ]] || { echo "usage: $0 [--output FILE|-] <crate> [crate ...]" >&2; exit 2; }
  output="$2"
  shift 2
fi
[[ $# -gt 0 ]] || { echo "usage: $0 [--output FILE|-] <crate> [crate ...]" >&2; exit 2; }

crates=()
while IFS= read -r crate; do
  [[ -n "$crate" ]] && crates+=("$crate")
done < <(printf '%s\n' "$@" | LC_ALL=C sort -u)
crate_list=$(IFS=,; printf '%s' "${crates[*]}")

if ! git diff --quiet || [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
  echo "linux-quickcheck-attest: stage the intended tree and remove unstaged or untracked inputs first" >&2
  exit 2
fi

tree=$(git write-tree)
echo "linux-quickcheck-attest: checking tree $tree for crates $crate_list"
./scripts/quickcheck.sh "${crates[@]}"

if ! git diff --quiet || [[ -n "$(git ls-files --others --exclude-standard)" ]] \
    || [[ "$(git write-tree)" != "$tree" ]]; then
  echo "linux-quickcheck-attest: tree changed during quickcheck; no attestation written" >&2
  exit 1
fi

record=$(printf 'format=broca-linux-quickcheck-v1\nplatform=linux\ntree=%s\ncrates=%s\nstatus=pass\n' \
  "$tree" "$crate_list")
if [[ -z "$output" ]]; then
  output=$(git rev-parse --git-path broca-linux-quickcheck-attestation)
  [[ "$output" == /* ]] || output="$root/$output"
fi
if [[ "$output" == - ]]; then
  printf '%s' "$record"
else
  output_dir=$(dirname "$output")
  mkdir -p "$output_dir"
  temporary=$(mktemp "$output.tmp.XXXXXX")
  trap 'rm -f "$temporary"' EXIT
  printf '%s' "$record" > "$temporary"
  mv "$temporary" "$output"
  trap - EXIT
  echo "linux-quickcheck-attest: passing attestation written to $output"
  printf '%s' "$record"
fi
