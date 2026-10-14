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

# Some suites read TOML with tomllib, which needs Python 3.11. macOS's system python3 is
# older, so take the first interpreter on PATH that has it and refuse if none does.
PY=""
for candidate in python3 python3.14 python3.13 python3.12 python3.11 /opt/homebrew/bin/python3; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c 'import tomllib' 2>/dev/null; then
        PY=$(command -v "$candidate")
        break
    fi
done
if [[ -z $PY ]]; then
    echo "script-suites: no Python 3.11+ (tomllib) found on PATH; install one or put it first" >&2
    exit 2
fi
# A temporary python3 link to the interpreter chosen above goes first on PATH, so a suite
# that starts "python3" itself runs under that same interpreter.
PY_DIR=$(mktemp -d "${TMPDIR:-/tmp}/broca-suites-python.XXXXXX")
trap 'rm -rf "$PY_DIR"' EXIT
ln -s "$PY" "$PY_DIR/python3"
export PATH="$PY_DIR:$PATH"
echo "script-suites: $(python3 --version) at $PY"

suites=(
    scripts/test-check-path-deps.py
    scripts/test-anthropic-policy-deps.py
    scripts/test-stage-release.py
    scripts/test-prune-staging.py
    # Stubs codesign, file, stat, ps and lsof, so the placement gate's macOS-only signing
    # checks also run on a Linux runner.
    scripts/test-place-release-gate.py
    scripts/test-train-push.py
    scripts/test-watch-ci-resolve.py
    scripts/test-archive-read.py
    scripts/test-wal-archive-instruments.py
    scripts/test-wal-retention-readers.py
    scripts/test-step-timing.py
    scripts/test-rekey-harness.py
    # The WAL readers' handling of gated (version-3) frames: decoded when their features
    # are known, reported as newer-writer when not.
    scripts/test-live-runs.py
    scripts/test-find-finalize-leaks.py
    # Every script locates broca's state root by the daemon's rule.
    scripts/test-state-root.py
    # The gate's phase order (fmt, clippy, prewarm, tests) and its log, with cargo stubbed.
    scripts/test-gate-phases.py
    scripts/test-ci-pipeline.py
    scripts/test-gated-commit.py
    scripts/test-turn-harnesses.py
    scripts/test-repro-vanished-bind-root.py
    scripts/test-script-options.py
    # Keep Tokio test clocks dev-only and the live transport stack out of non-live graphs.
    scripts/test-dependency-features.py
    scripts/tests/test_clock_safety.py
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
# Nothing under .cortexkit/ may be tracked except release-notes/, skills/, rules/ and WORKER.md.
scripts/check-cortexkit-layout.sh
