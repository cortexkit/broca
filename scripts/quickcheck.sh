#!/usr/bin/env bash
# Targeted verification for a small fixup — the cheap check to run BEFORE the
# full gate, so one-line fixes don't pay a full workspace relink to discover a
# fmt/clippy triviality. Runs, in order: fmt-check (whole workspace, seconds),
# clippy on the named crates, and their lib/bin tests. Integration tests and
# the cross-workspace suites stay in scripts/gate.sh.
#
# Usage: scripts/quickcheck.sh <crate> [crate...]
#    ex: scripts/quickcheck.sh broca-subc broca-module
set -u

cd "$(dirname "$0")/.."
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"

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
    broca-provider) FEATURES+=("-F" "broca-provider/live") ;;
    broca-catalog) FEATURES+=("-F" "broca-catalog/live") ;;
    broca-core|broca-module|broca-module-serve) FEATURES+=("-F" "$c/test-clock") ;;
  esac
done

set -x
# Keep verification read-only: --locked refuses manifest/lock disagreements rather
# than updating dependencies and letting gated-commit sweep them into the commit.
# Scope formatting to workspace members; dependency source files are not ours to
# reformat, whether Cargo resolved them from a registry, git, or a local checkout.
#
# Building the -p list from cargo metadata rather than hardcoding it means a new crate is
# covered the day it is added, with no second list to drift.
fmt_pkgs=()
while IFS= read -r p; do fmt_pkgs+=(-p "$p"); done < <(
    cargo metadata --no-deps --format-version 1 2>/dev/null |
        python3 -c 'import json,sys; [print(p["name"]) for p in json.load(sys.stdin)["packages"]]'
)
if [ ${#fmt_pkgs[@]} -eq 0 ]; then
    echo "quickcheck: REFUSING -- could not enumerate workspace packages for fmt." >&2
    echo "  A fmt check over an empty package set passes vacuously." >&2
    exit 2
fi
cargo fmt "${fmt_pkgs[@]}" -- --check || exit $?
cargo clippy --locked "${PKGS[@]}" ${FEATURES[@]+"${FEATURES[@]}"} --all-targets -- -D warnings || exit $?
# `--lib` and `--doc` make cargo refuse the whole command when any selected package has no
# library target (ck-import is binary-only), so add those selectors only when at least one
# named package has a library. `--bins` accepts library-only packages.
lib_count=$(cargo metadata --no-deps --format-version 1 2>/dev/null | python3 -c '
import json, sys
wanted = set(sys.argv[1:])
print(sum(1 for p in json.load(sys.stdin)["packages"]
          if p["name"] in wanted and any("lib" in t["kind"] for t in p["targets"])))
' "$@")
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
cargo test --locked "${PKGS[@]}" ${FEATURES[@]+"${FEATURES[@]}"} ${LIB[@]+"${LIB[@]}"} --bins --quiet || exit $?
# The doc lane is here rather than deferred to the full gate because rustdoc COMPILES an
# indented block under a `///` comment as Rust. Our comment convention is to show the
# tempting wrong form next to the right one, so writing that counter-example on a
# documented item turns it into a doctest that fails to compile -- and `--lib` cannot see
# it. The workspace has zero doctests today, so this costs ~7s of rustdoc per crate and
# catches nothing; it is here for the first `///` counter-example anyone writes.
if [ "$lib_count" -gt 0 ]; then
    cargo test --locked "${PKGS[@]}" ${FEATURES[@]+"${FEATURES[@]}"} --doc --quiet || exit $?
fi
set +x
echo "==> quickcheck OK: $*"
