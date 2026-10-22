#!/usr/bin/env bash
# Run cheap checks on Linux without sibling daemon, vault or mock-provider rigs.
set -euo pipefail
[[ "$(uname -s)" == Linux ]] || { echo 'linux-preflight: requires Linux; refusing local fallback' >&2; exit 2; }
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
dirty="$(git status --porcelain)" || { echo 'linux-preflight: cannot read snapshot status' >&2; exit 2; }
[[ -z "$dirty" ]] || { echo 'linux-preflight: snapshot must be committed and clean' >&2; exit 2; }
: "${CARGO_TARGET_DIR:?motor must supply its persistent target cache}"
export CARGO_BUILD_JOBS=8 PYTHONDONTWRITEBYTECODE=1
rustc --version
cargo --version
cargo nextest --version
echo "linux-preflight: warm target=$CARGO_TARGET_DIR"
source scripts/portable-test-selection.conf
for name in "${portable_unit_exclusions[@]}"; do echo "unit exclusion (CI/Mac only): $name"; done
printf 'integration selection (fixture families stay in CI/Mac):\n%s\n' "$portable_integration_filter"

features=(
  -F broca-provider/live
  -F broca-provider/test-harness
  -F broca-catalog/live
  -F broca-module-serve/test-clock
)
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets "${features[@]}" -- -D warnings
cargo clippy --locked --workspace --all-targets --target x86_64-pc-windows-gnu "${features[@]}" -- -D warnings

# An accidentally selected fixture test must fail, not return a passing skip.
unset SUBC_CORE_BIN FAKE_AFT_STUB_BIN CK_CLAUSTRUM_BIN CK_AUTH_BIN CK_CLAUSTRUM_CHECKOUT AIMOCK_CMD
export BROCA_REQUIRE_SUBC=1 BROCA_REQUIRE_VAULT=1 BROCA_REQUIRE_AIMOCK=1
export NEXTEST_TEST_THREADS=num-cpus
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
unit_skips=()
for name in "${portable_unit_exclusions[@]}"; do unit_skips+=(--skip "$name"); done
list_rc=0
cargo test --locked --workspace --lib --bins --no-fail-fast "${features[@]}" -- --list > "$scratch/unit-list" 2>&1 || list_rc=$?
if [[ "$list_rc" -ne 0 ]]; then cat "$scratch/unit-list" >&2; exit "$list_rc"; fi
python3 scripts/preflight-test-report.py unit-list "$scratch/unit-list" "${portable_unit_exclusions[@]}"
unit_rc=0
cargo test --locked --workspace --lib --bins --no-fail-fast "${features[@]}" --quiet -- --exact "${unit_skips[@]}" --show-output 2>&1 | tee "$scratch/units" || unit_rc=$?
python3 scripts/preflight-test-report.py units "$scratch/units"
[[ "$unit_rc" -eq 0 ]] || exit "$unit_rc"
cargo nextest list --locked --workspace "${features[@]}" -E 'kind(test) and not (binary(=live) or binary(=live_e2e))' -T json > "$scratch/full.json"
cargo nextest list --locked --workspace "${features[@]}" -E "$portable_integration_filter" -T json > "$scratch/selected.json"
python3 scripts/preflight-test-report.py inventory "$scratch/full.json" "$scratch/selected.json"
integration_rc=0
cargo nextest run --locked --workspace "${features[@]}" -P ci -E "$portable_integration_filter" --success-output final 2>&1 | tee "$scratch/integrations" || integration_rc=$?
python3 scripts/preflight-test-report.py integrations "$scratch/integrations" "$scratch/selected.json"
[[ "$integration_rc" -eq 0 ]] || exit "$integration_rc"
dirty="$(git status --porcelain)" || { echo 'linux-preflight: cannot read snapshot status' >&2; exit 2; }
[[ -z "$dirty" ]] || { git status --short; echo 'linux-preflight: checks changed the snapshot' >&2; exit 2; }
echo 'linux-preflight: all fixture-independent lanes passed; full fixtures/doctests/scripts/Postgres remain in CI/Mac gates'
