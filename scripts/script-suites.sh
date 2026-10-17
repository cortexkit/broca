#!/usr/bin/env bash
# Run broca's operator script suites: the Python tests that guard release, placement,
# WAL-reading and dependency tooling. CI and the local gate both call this one list, so a
# suite added here runs in both places. Before this file existed the list lived only in
# CI, and a conflict between two dependency checks reached CI after passing the local gate.
#
# Usage: scripts/script-suites.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# Suites can spawn Python themselves, so the variable (inherited) rather than `python3 -B`.
# Bytecode left in scripts/__pycache__ would trip CI's clean-worktree guard.
export PYTHONDONTWRITEBYTECODE=1

# Suites need Python 3.11+ (tomllib) and PyYAML (workflow parsing). macOS's system python3
# is older, and Homebrew's has no PyYAML, so an operator venv is tried first. Take the first
# interpreter that has both, and refuse with the remedy if none does.
PY=""
suites_venv="${XDG_DATA_HOME:-$HOME/.local/share}/cortexkit/broca/py-suites"
for candidate in "${BROCA_SUITES_PYTHON:-}" "$suites_venv/bin/python3" \
    python3 python3.14 python3.13 python3.12 python3.11 /opt/homebrew/bin/python3; do
    [[ -n $candidate ]] || continue
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c 'import tomllib, yaml' 2>/dev/null; then
        PY=$(command -v "$candidate")
        break
    fi
done
if [[ -z $PY ]]; then
    echo "script-suites: no Python 3.11+ with PyYAML found. Create one with:" >&2
    echo "  python3.11+ -m venv $suites_venv && $suites_venv/bin/pip install PyYAML" >&2
    echo "or set BROCA_SUITES_PYTHON to an interpreter that has tomllib and yaml." >&2
    exit 2
fi
# A temporary python3 link to the interpreter chosen above goes first on PATH, so a suite
# that starts "python3" itself runs under that same interpreter.
PY_DIR=$(mktemp -d "${TMPDIR:-/tmp}/broca-suites-python.XXXXXX")
trap 'rm -rf "$PY_DIR"' EXIT
# A wrapper that execs the interpreter, not a symlink: a venv's Python finds its
# site-packages relative to the path it was started from, so a symlink elsewhere drops them.
printf '#!/bin/sh\nexec "%s" "$@"\n' "$PY" > "$PY_DIR/python3"
chmod +x "$PY_DIR/python3"
export PATH="$PY_DIR:$PATH"
echo "script-suites: $(python3 --version) at $PY"

# Check the whole catalogue before running suites or any native mutation replay.
python3 scripts/verify-mutation-platforms.py

suites=(
    scripts/test-check-path-deps.py
    scripts/test-anthropic-policy-deps.py
    scripts/test-stage-release.py
    scripts/test-release.py
    scripts/test-prune-staging.py
    # Stubs codesign, file, stat, ps and lsof, so the placement gate's macOS-only signing
    # checks also run on a Linux runner.
    scripts/test-place-release-gate.py
    scripts/test-place-release-seal.py
    scripts/test-train-push.py
    scripts/test-train-preflight.py
    scripts/test-watch-ci-resolve.py
    scripts/test-archive-read.py
    scripts/test-wal-archive-instruments.py
    scripts/test-wal-retention-readers.py
    scripts/test-step-timing.py
    scripts/tests/test-attempt-telemetry.py
    scripts/test-rekey-harness.py
    # The WAL readers' handling of gated (version-3) frames: decoded when their features
    # are known, reported as newer-writer when not.
    scripts/test-live-runs.py
    scripts/test-find-finalize-leaks.py
    # Every script locates broca's state root by the daemon's rule.
    scripts/test-state-root.py
    # Verifies quickcheck's unconditional guards and feature propagation behavior.
    scripts/test-quickcheck-guards.py
    # The gate's phase order (fmt, clippy, prewarm, tests) and its log, with cargo stubbed.
    scripts/test-gate-phases.py
    scripts/test-ci-pipeline.py
    scripts/test-gated-commit.py
    scripts/test-deploy.py
    scripts/test-turn-harnesses.py
    scripts/test-repro-vanished-bind-root.py
    scripts/test-script-options.py
    # Keep Tokio test clocks dev-only and the live transport stack out of non-live graphs.
    scripts/test-dependency-features.py
    scripts/tests/test_clock_safety.py
    # Vendored Cingulate consent contract: checksums, pins and broca's mapping.
    crates/broca-subc/tests/fixtures/consent/verify.py
)

# A rename must not quietly shrink this to a no-op that keeps passing.
for suite in "${suites[@]}"; do
    [[ -f $suite ]] || { echo "missing suite: $suite" >&2; exit 2; }
done
echo "running ${#suites[@]} operator script suites"
for suite in "${suites[@]}"; do
    echo "--- $suite"
    python3 "$suite"
done
python3 scripts/check-path-deps.py
python3 scripts/live-runs.py --self-test
python3 scripts/audit-export-lane.py --self-test
python3 scripts/verify-windows-mutations.py --self-test
python3 scripts/verify-mutation-platforms.py --self-test
# Nothing under .cortexkit/ may be tracked except release-notes/, skills/, rules/ and WORKER.md.
scripts/check-cortexkit-layout.sh
