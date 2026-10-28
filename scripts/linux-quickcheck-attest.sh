#!/usr/bin/env bash
# Run the crate quickcheck on Linux and record the exact working tree it verified.
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

# The scratch index and objects must live outside the working tree: `git add -A` below hashes
# everything not ignored, so scratch inside the tree would change the very tree being attested.
# The remote runner does not pass TMPDIR, hence the /tmp fallback rather than a path in the repo.
scratch=$(mktemp -d "${TMPDIR:-/tmp}/broca-linux-attest.XXXXXX")
temporary=""
trap 'rm -rf "$scratch"; [[ -z "$temporary" ]] || rm -f "$temporary"' EXIT
objects="$scratch/objects"
mkdir -p "$objects"
alternate_objects=$(git rev-parse --git-path objects)
[[ "$alternate_objects" == /* ]] || alternate_objects="$root/$alternate_objects"

# The remote build checkout keeps .git read-only. Build the candidate tree with a private index
# and object directory so hashing the working tree never writes into Git's shared metadata.
candidate_tree() {
  local index="$scratch/index"
  rm -f "$index" "$index.lock"
  GIT_INDEX_FILE="$index" GIT_OBJECT_DIRECTORY="$objects" \
    GIT_ALTERNATE_OBJECT_DIRECTORIES="$alternate_objects" git read-tree HEAD
  GIT_INDEX_FILE="$index" GIT_OBJECT_DIRECTORY="$objects" \
    GIT_ALTERNATE_OBJECT_DIRECTORIES="$alternate_objects" git add -A
  GIT_INDEX_FILE="$index" GIT_OBJECT_DIRECTORY="$objects" \
    GIT_ALTERNATE_OBJECT_DIRECTORIES="$alternate_objects" git write-tree
}

tree=$(candidate_tree)
echo "linux-quickcheck-attest: checking tree $tree for crates $crate_list"
# Package selection must not change Cargo's feature graph on the shared build server.
python3 scripts/lib/linux-quickcheck.py "${crates[@]}"

if [[ "$(candidate_tree)" != "$tree" ]]; then
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
  printf '%s' "$record" > "$temporary"
  mv "$temporary" "$output"
  temporary=""
  echo "linux-quickcheck-attest: passing attestation written to $output"
  printf '%s' "$record"
fi
