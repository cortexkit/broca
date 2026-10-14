#!/usr/bin/env bash
# Compile the integration slice once; replay the same archive on each CI shard.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
source scripts/rust-test-gate.sh
trap - EXIT

mode="${1:-}"
archive="${2:-}"
[[ -n "$archive" ]] || { echo 'usage: ci-integration.sh build ARCHIVE | run ARCHIVE K/N' >&2; exit 2; }
case "$mode" in
  build)
    cargo_count="$(count_cargo_integration_tests)"
    nextest_count="$(count_nextest_integration_tests)"
    echo "integration tests selected: cargo-test baseline=$cargo_count, nextest=$nextest_count"
    if [[ "$cargo_count" -eq 0 || "$cargo_count" -ne "$nextest_count" ]]; then
      echo 'integration inventory is empty or differs from independent libtest discovery' >&2
      exit 1
    fi
    cargo nextest archive --locked --workspace "${cargo_feature_args[@]}" --archive-file "$archive"
    ;;
  run)
    partition="${3:-}"
    [[ "$partition" =~ ^[1-9][0-9]*/[1-9][0-9]*$ ]] || { echo 'expected partition K/N' >&2; exit 2; }
    # Hosted jobs check out to the same absolute path. Extract to that checkout,
    # not a temporary target: some tests embed Cargo's binary path at build time.
    base=(cargo nextest run --archive-file "$archive" --workspace-remap "$root" --extract-to "$root" --extract-overwrite -P ci)
    # Run the feature-only clock proof on every shard as well as in its normal
    # partition. A listed test is not evidence that the feature actually works.
    clock_output="$(mktemp)"
    trap 'rm -f "$clock_output"' EXIT
    sentinel='test_clock::test_clock_feature_is_enabled_for_retention_tests'
    "${base[@]}" -E "kind(test) and package(=broca-module) and test(=$sentinel)" --success-output final 2>&1 | tee "$clock_output"
    "$python_bin" - "$clock_output" "$sentinel" <<'PY'
import re
import sys
text = open(sys.argv[1], encoding="utf-8", errors="replace").read()
text = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", text)
pattern = r"^\s*PASS\s+\[[^\]\n]+\]\s+(?:\(\s*\d+/\d+\)\s+)?broca-module::it\s+" + re.escape(sys.argv[2]) + r"\s*$"
if re.search(pattern, text, re.MULTILINE) is None:
    sys.exit("CI shard: test-clock sentinel did not execute successfully")
print("CI shard: executed test-clock sentinel")
PY
    "${base[@]}" -E "$nextest_filter" --partition "hash:$partition" --success-output final
    ;;
  *) echo "unknown mode: $mode" >&2; exit 2 ;;
esac
