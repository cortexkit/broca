#!/usr/bin/env bash
# Targeted verification for a small fixup — the cheap check to run BEFORE the
# full gate, so one-line fixes don't pay a full workspace relink to discover a
# fmt/clippy triviality. It checks selected crates, their listed gate-feature
# suites, and always runs the small source-scan guards for workspace invariants.
#
# Usage: scripts/quickcheck.sh <crate> [crate...]
#    ex: scripts/quickcheck.sh broca-subc broca-module
set -u

cd "$(dirname "$0")/.." || exit 2
export PATH="$HOME/.cargo/bin:$PATH"
# Prevent Python helpers from leaving bytecode in the checkout during a gated commit.
export PYTHONDONTWRITEBYTECODE=1
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"

# The mutation catalogue checker uses tomllib, which requires Python 3.11+.
QUICKCHECK_PYTHON=""
suites_venv="${XDG_DATA_HOME:-$HOME/.local/share}/cortexkit/broca/py-suites"
for candidate in "${BROCA_SUITES_PYTHON:-}" "$suites_venv/bin/python3" \
    python3 python3.14 python3.13 python3.12 python3.11 /opt/homebrew/bin/python3; do
    [[ -n "$candidate" ]] || continue
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c 'import tomllib' 2>/dev/null; then
        QUICKCHECK_PYTHON=$(command -v "$candidate")
        break
    fi
done
if [[ -z "$QUICKCHECK_PYTHON" ]]; then
    echo "quickcheck: Python 3.11+ with tomllib is required; set BROCA_SUITES_PYTHON." >&2
    exit 2
fi

[ $# -ge 1 ] || { echo "usage: scripts/quickcheck.sh <crate> [crate...]" >&2; exit 64; }

PKGS=()
for c in "$@"; do PKGS+=("-p" "$c"); done

# Mirror the full gate's feature set: several broca-provider/broca-catalog
# integration targets (aimock_guard, live suites) only compile with `live`
# enabled, so clippy --all-targets without it fails on imports the gate accepts.
# Cargo rejects -F for packages not selected by -p, so only pass the flags the
# named crates can accept.
FEATURES=()
for c in "$@"; do
  case "$c" in
    broca-provider)
      FEATURES+=("-F" "broca-provider/live" "-F" "broca-provider/test-harness")
      ;;
    broca-catalog) FEATURES+=("-F" "broca-catalog/live") ;;
    broca-core|broca-module|broca-module-serve) FEATURES+=("-F" "$c/test-clock") ;;
  esac
done

set -x
# Keep verification read-only: --locked refuses manifest/lock disagreements rather
# than updating dependencies and letting gated-commit sweep them into the commit.
# Formatting every workspace crate was the largest cost of a warm one-crate
# check. Format only the selected crates here; their clippy, lib/bin tests,
# listed feature suites and doctests remain below. gate.sh and CI still check
# whole-workspace formatting.
python3 scripts/lib/cargo-watchdog.py --step "quickcheck cargo fmt for $*" -- cargo fmt "${PKGS[@]}" -- --check || exit $?
python3 scripts/lib/cargo-watchdog.py --step "quickcheck cargo clippy for $*" -- cargo clippy --locked "${PKGS[@]}" ${FEATURES[@]+"${FEATURES[@]}"} --all-targets -- -D warnings || exit $?
metadata=$(python3 scripts/lib/cargo-watchdog.py --step "quickcheck cargo metadata for $*" --discard-stderr -- cargo metadata --locked --no-deps --format-version 1) || {
    echo "quickcheck: REFUSING -- could not read Cargo target metadata." >&2
    exit 2
}
# `--lib` and `--doc` make cargo refuse the whole command when any selected package has no
# library target (ck-import is binary-only), so add those selectors only when at least one
# named package has a library. `--bins` accepts library-only packages.
lib_count=$($QUICKCHECK_PYTHON -c '
import json, sys
wanted = set(sys.argv[1:])
print(sum(1 for p in json.load(sys.stdin)["packages"]
          if p["name"] in wanted and any("lib" in t["kind"] for t in p["targets"])))
' "$@" <<< "$metadata") || exit 2
if [ -z "$lib_count" ]; then
    echo "quickcheck: REFUSING -- could not read which packages have a library target." >&2
    exit 2
fi
LIB=()
[ "$lib_count" -gt 0 ] && LIB=(--lib)
# --bins IS LOAD-BEARING AND WAS MISSING. This line ran --lib alone while the header four
# lines up claimed "their lib/bin tests", so tests living in src/bin/ were invisible to the
# gate: broca-session's CLI tests are there, and a change to the run_finished event passed
# quickcheck twice -- once for the worker, once for me -- before CI caught it on the release
# commit. A cargo test --lib against a binary-target test does not fail, it reports success
# over a set it never selected, which is indistinguishable from a real pass.
python3 scripts/lib/cargo-watchdog.py --step "quickcheck cargo unit and binary tests for $*" -- cargo test --locked "${PKGS[@]}" ${FEATURES[@]+"${FEATURES[@]}"} ${LIB[@]+"${LIB[@]}"} --bins --quiet || exit $?
# The doc lane is here rather than deferred to the full gate because rustdoc COMPILES an
# indented block under a `///` comment as Rust. Our comment convention is to show the
# tempting wrong form next to the right one, so writing that counter-example on a
# documented item turns it into a doctest that fails to compile -- and `--lib` cannot see
# it. The workspace has zero doctests today, so this costs ~7s of rustdoc per crate and
# catches nothing; it is here for the first `///` counter-example anyone writes.
if [ "$lib_count" -gt 0 ]; then
    python3 scripts/lib/cargo-watchdog.py --step "quickcheck cargo doctests for $*" -- cargo test --locked "${PKGS[@]}" ${FEATURES[@]+"${FEATURES[@]}"} --doc --quiet || exit $?
fi

# Run the fixture-backed feature suites when their crate is among the changes.
# Other integration targets, which may need real rigs, stay in gate.sh.
run_feature_tests() {
    local package target features changed feature selected
    local -a cargo_args feature_args selected_features
    [[ -r scripts/quickcheck-feature-tests.list ]] || {
        echo "quickcheck: REFUSING -- feature-test list is missing or unreadable." >&2
        return 2
    }
    while IFS='|' read -r package target features _; do
        [[ -n "$package" && "$package" != \#* ]] || continue
        selected=0
        for changed in "$@"; do
            if [ "$changed" = "$package" ]; then
                selected=1
                break
            fi
        done
        [ "$selected" -eq 1 ] || continue

        cargo_args=(test --locked -p "$package")
        feature_args=()
        if [ "$features" != "-" ]; then
            IFS=',' read -r -a selected_features <<< "$features"
            for feature in "${selected_features[@]}"; do
                feature_args+=(-F "$feature")
            done
        fi
        cargo_args+=(--test "$target")
        echo "==> quickcheck feature suite: $package::$target ($features)"
        python3 scripts/lib/cargo-watchdog.py --step "quickcheck cargo ${cargo_args[*]}" -- cargo "${cargo_args[@]}" ${feature_args[@]+"${feature_args[@]}"} --quiet || return $?
    done < scripts/quickcheck-feature-tests.list
}
run_feature_tests "$@" || exit $?

# These tests guard source-wide rules and inventoried tables. They stay here instead
# of being tied to PKGS because a change in any crate can violate a workspace rule.
run_guards() {
    local package target filter features feature guards_run=0
    local -a cargo_args feature_args selected_features
    [[ -r scripts/quickcheck-guards.list ]] || {
        echo "quickcheck: REFUSING -- guard list is missing or unreadable." >&2
        return 2
    }
    while IFS='|' read -r package target filter features _; do
        [[ -n "$package" && "$package" != \#* ]] || continue
        if [ "$package" = "python" ]; then
            "$QUICKCHECK_PYTHON" -B "$target" "$filter" || return $?

            guards_run=$((guards_run + 1))
            continue
        fi
        cargo_args=(test --locked -p "$package" --test "$target")
        feature_args=()
        if [ "$features" != "-" ]; then
            IFS=',' read -r -a selected_features <<< "$features"
            for feature in "${selected_features[@]}"; do
                feature_args+=(-F "$feature")
            done
        fi
        if [ "$filter" != "-" ]; then
            cargo_args+=("$filter")
        fi
        echo "==> quickcheck guard: $package::$target $filter"
        python3 scripts/lib/cargo-watchdog.py --step "quickcheck cargo ${cargo_args[*]}" -- cargo "${cargo_args[@]}" ${feature_args[@]+"${feature_args[@]}"} --quiet || return $?
        guards_run=$((guards_run + 1))
    done < scripts/quickcheck-guards.list
    [ "$guards_run" -gt 0 ] || {
        echo "quickcheck: REFUSING -- guard list selected no checks." >&2
        return 2
    }

    # Refuse catalog platform claims that are unsupported by the mutation runners.
    "$QUICKCHECK_PYTHON" scripts/verify-mutation-platforms.py || return $?

    # Exercise the catalogue and fixture-copy tests that reject platform drift.
    "$QUICKCHECK_PYTHON" scripts/verify-mutation-platforms.py --self-test || return $?

    # Exercise the Windows mutation catalogue checks without a native Windows host.
    "$QUICKCHECK_PYTHON" scripts/verify-windows-mutations.py --self-test || return $?

}
guards_started=$SECONDS
run_guards || exit $?
echo "==> quickcheck guards OK ($((SECONDS - guards_started))s)"
set +x
echo "==> quickcheck OK: $*"
