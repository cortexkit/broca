#!/usr/bin/env bash
# Footgun-proof wrapper around rust-test-gate.sh. Fixes the recurring traps:
#   1. PATH: agent/CI shells sometimes lack ~/.cargo/bin → "cargo-nextest is
#      required" after an hour of compiling. Export it unconditionally.
#   2. Exit-code masking: piping the gate through tail/head reads the pipe's
#      exit, and an early fmt failure looks like success. Log to a file, echo
#      the REAL exit code, and print the failure region on non-zero.
#   3. Orphaned cargo/nextest processes from a previous killed run hold the
#      target-dir lock and serialize everything behind them. Detect and report
#      them up front (kill with --kill-orphans; never kill silently).
#   4. macOS Gatekeeper: compiles are NOT assessed (rustc children never trip
#      it), so builds run at cargo's default full width. The assessment tax is
#      first-EXEC of each freshly-linked binary (~1.2-4s cold, ~25ms warm), so
#      the prewarm signs and warm-execs every test executable before timed test
#      phases run. (An earlier CARGO_BUILD_JOBS=2 throttle misattributed that
#      exec tax to parallel compilation and cost ~4x gate wall time.)
#   5. Slow runs on this host are LOAD, not a mechanism -- checked, not assumed.
#      Three interrupted runs in one day stopped at three different points, and
#      the slowest completed run against the fastest inflates every phase
#      together: clippy 1s->112s, lib+bins 19s->403s, integrations 89s->460s.
#      Nothing inside a test binary can slow down clippy, so a retry is the
#      right response here. Keep the discriminator rather than the conclusion:
#      a stable victim, a fixed alternation, or a constant offset means a
#      mechanism worth finding; a uniform spread across unrelated phases means
#      the box. Check before retrying -- the next occurrence arrives
#      mid-release, when nobody wants to look.
#
# Usage: scripts/gate.sh [--kill-orphans] [extra rust-test-gate.sh args...]
set -u

cd "$(dirname "$0")/.."

export PATH="$HOME/.cargo/bin:$PATH"
# The gate builds the workspace under several feature sets, and incremental compilation keeps a
# separate cache for each one, which grew to 122 GB in one target folder on the shared Mac. A
# gate rarely recompiles the same crate twice in a way incremental can reuse, so turn it off.
# Non-incremental compiles can also be served from the shared compile cache, which skips
# incremental crates. An explicit caller setting still wins.
export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"
# Honor an explicit caller override; otherwise let cargo use full width.
[ -n "${CARGO_BUILD_JOBS:-}" ] && export CARGO_BUILD_JOBS

command -v cargo >/dev/null || { echo "FATAL: cargo not found even with ~/.cargo/bin on PATH" >&2; exit 90; }
command -v cargo-nextest >/dev/null || { echo "FATAL: cargo-nextest missing (cargo install cargo-nextest --locked)" >&2; exit 90; }

KILL_ORPHANS=0
if [ "${1:-}" = "--kill-orphans" ]; then KILL_ORPHANS=1; shift; fi

# Orphan check: cargo/nextest processes working THIS repo that we did not just
# start. They hold the build-dir lock and starve the gate. Scope the match to
# processes whose cwd is under this repo — a machine-wide "cargo test" kill once
# took out a PEER project's test run (broca crates all start with broca-, so the
# command line alone can also identify ours, but cwd is the reliable signal).
repo_pids() {
    pgrep -f "cargo (test|nextest)|rust-test-gate" 2>/dev/null | while read -r pid; do
        [ "$pid" = "$$" ] && continue
        cwd=$(lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p')
        # REQUIRE A PATH-COMPONENT BOUNDARY. A bare "$PWD"* is a string prefix, not a
        # directory test, so it also selects siblings whose names merely START with this
        # repo's: broca-tagref and brocatui both sit beside broca and both build against
        # its crates, which makes them the repositories most likely to be compiling at the
        # same moment. With --kill-orphans that is a SIGKILL into another project's test
        # run -- the exact incident this function was written after, narrowed rather than
        # fixed. "$PWD" alone matches the repo root, "$PWD"/* matches anything beneath it,
        # and nothing else qualifies.
        case "$cwd" in "$PWD"|"$PWD"/*) echo "$pid";; esac
    done
}
ORPHAN_PIDS=$(repo_pids)
ORPHANS=""
[ -n "$ORPHAN_PIDS" ] && ORPHANS=$(ps -o pid=,command= -p $(echo "$ORPHAN_PIDS" | tr '\n' ',' | sed 's/,$//') 2>/dev/null)
if [ -n "$ORPHANS" ]; then
    if [ "$KILL_ORPHANS" = "1" ]; then
        echo "==> killing orphaned gate/test processes (this repo only):"
        echo "$ORPHANS"
        echo "$ORPHAN_PIDS" | xargs kill -9 2>/dev/null
        sleep 2
    else
        echo "==> WARNING: existing cargo test/nextest processes may hold the build lock:" >&2
        echo "$ORPHANS" >&2
        echo "==> they will serialize this run; pass --kill-orphans to clear them" >&2
    fi
fi

# Check resolved sources before spending time building test rigs or broca.
python3 scripts/check-path-deps.py || exit 2
python3 scripts/test-check-path-deps.py || exit 2

LOG="/tmp/broca-gate-$(date +%Y%m%d-%H%M%S).log"
echo "==> gate starting; log: $LOG"

# The real-daemon rigs need `ck-subc` and `fake-aft-stub` from the sibling subconscious checkout.
# Without SUBC_CORE_BIN they fall back to ../subconscious/target/debug, but a fleet sweep deletes
# every cargo target/debug under the workspace daily, and a rig that finds nothing either skips
# or, under BROCA_REQUIRE_SUBC=1, fails. So the gate builds both binaries itself into a target dir
# it owns, outside that workspace. The build is incremental, so it costs seconds when the
# sibling hasn't changed, and it tracks the sibling's source instead of whatever was last built.
# An explicit SUBC_CORE_BIN still wins, for a caller testing against a particular build.
SUBC_SIBLING="../subconscious"
if [ -z "${SUBC_CORE_BIN:-}" ] && [ -f "$SUBC_SIBLING/Cargo.toml" ]; then
    SUBC_TARGET="${BROCA_SUBC_TARGET_DIR:-$HOME/.cache/broca-subc-target}"
    echo "==> building the subc rig binaries into $SUBC_TARGET"
    if ! cargo build --manifest-path "$SUBC_SIBLING/Cargo.toml" --target-dir "$SUBC_TARGET" \
        -p subc-core --bins >>"$LOG" 2>&1; then
        echo "gate: REFUSING -- could not build ck-subc and fake-aft-stub from $SUBC_SIBLING (see $LOG)" >&2
        exit 2
    fi
    export SUBC_CORE_BIN="$SUBC_TARGET/debug/ck-subc"
    export FAKE_AFT_STUB_BIN="$SUBC_TARGET/debug/fake-aft-stub"
    for bin in "$SUBC_CORE_BIN" "$FAKE_AFT_STUB_BIN"; do
        [ -x "$bin" ] || { echo "gate: REFUSING -- the subc build did not produce $bin" >&2; exit 2; }
    done
fi

# The real-vault rigs run `ck-claustrum` and seed it with `ck-auth`. Without CK_CLAUSTRUM_BIN they
# take both from ../claustrum/target/release, which holds whatever each was last built from: the two
# can come from different commits, and a ck-auth that migrates the store past the schema ck-claustrum
# knows makes the vault refuse to serve, failing every real-vault test for a reason that is not
# broca's. So the gate builds the pair itself, from one commit, into a target dir it owns. Release,
# like the shipped vault. An explicit CK_CLAUSTRUM_BIN still wins.
CLAUSTRUM_SIBLING="../claustrum"
if [ -z "${CK_CLAUSTRUM_BIN:-}" ] && [ -f "$CLAUSTRUM_SIBLING/Cargo.toml" ]; then
    CLAUSTRUM_TARGET="${BROCA_CLAUSTRUM_TARGET_DIR:-$HOME/.cache/broca-claustrum-target}"
    echo "==> building the vault rig binaries into $CLAUSTRUM_TARGET"
    if ! cargo build --release --manifest-path "$CLAUSTRUM_SIBLING/Cargo.toml" \
        --target-dir "$CLAUSTRUM_TARGET" --bin ck-claustrum --bin ck-auth >>"$LOG" 2>&1; then
        echo "gate: REFUSING -- could not build ck-claustrum and ck-auth from $CLAUSTRUM_SIBLING (see $LOG)" >&2
        exit 2
    fi
    export CK_CLAUSTRUM_BIN="$CLAUSTRUM_TARGET/release/ck-claustrum"
    export CK_AUTH_BIN="$CLAUSTRUM_TARGET/release/ck-auth"
    # The binaries no longer sit inside the checkout, so tests that read claustrum's own
    # source files (its wire contract fixture) are told where the checkout is.
    export CK_CLAUSTRUM_CHECKOUT="$(cd "$CLAUSTRUM_SIBLING" && pwd)"
    for bin in "$CK_CLAUSTRUM_BIN" "$CK_AUTH_BIN"; do
        [ -x "$bin" ] || { echo "gate: REFUSING -- the claustrum build did not produce $bin" >&2; exit 2; }
    done
fi

if [ "$(uname -s)" = Linux ]; then
    # The real-daemon tests need subc/aft and claustrum/auth binaries. Supplying all four
    # makes a local Linux run meaningful too; without them those tests would fail or skip.
    for rig in "${SUBC_CORE_BIN:-}" "${FAKE_AFT_STUB_BIN:-}" "${CK_CLAUSTRUM_BIN:-}" "${CK_AUTH_BIN:-}"; do
        if [ -z "$rig" ] || [ ! -x "$rig" ]; then
            echo "gate: REFUSING -- Linux gate requires executable subc, aft, claustrum and auth rigs" >&2
            exit 2
        fi
    done
    export BROCA_ALLOW_LINUX_GATE=1
fi

# The gate uses the machine's rustc wrapper. If compiles sit at 0% CPU,
# sample them (`sample <pid> 3`) before blaming the cache.

# Formatting is checked here, before the prewarm, and AGAIN inside rust-test-gate.sh. The
# duplicate is deliberate: this copy exists so a formatting defect costs 0.9s instead of the
# full compile-and-Gatekeeper warm-up, and the copy in the gate script exists so a direct
# caller of that script still gets the check. Deleting either one is a real loss, and which
# loss depends on how it was invoked.
#
# Measured, from this repository's own gate logs: three runs died on formatting after paying
# the whole prewarm -- 385s, 827s and 888s, against 0.9s standalone. That is 2,098s spent
# proving nothing across 43 invocations in 30 hours.
# Scoped to our own packages for the reason recorded in scripts/quickcheck.sh: `--all`
# reached into a sibling repo's working tree and failed this gate on code we do not own.
gate_fmt_pkgs=()
while IFS= read -r p; do gate_fmt_pkgs+=(-p "$p"); done < <(
    cargo metadata --no-deps --format-version 1 2>/dev/null |
        python3 -c 'import json,sys; [print(p["name"]) for p in json.load(sys.stdin)["packages"]]'
)
if [ ${#gate_fmt_pkgs[@]} -eq 0 ]; then
    echo "gate: REFUSING -- could not enumerate workspace packages for fmt." >&2
    exit 2
fi
if ! cargo fmt "${gate_fmt_pkgs[@]}" -- --check; then
    echo "==> formatting failed; skipping the prewarm (it cannot affect this result)" >&2
    exit 1
fi

# The operator script suites take under two minutes and need no Rust build, so this gate
# runs them before the Rust phases. CI runs the same list, so a suite that fails in CI
# also fails here first.
if ! ./scripts/script-suites.sh >>"$LOG" 2>&1; then
    echo "==> operator script suites failed; skipping the Rust gate" >&2
    grep -B2 -A12 -m3 -E "FAIL|Error|missing suite|no Python" "$LOG" || tail -30 "$LOG"
    echo "==> full log: $LOG"
    exit 1
fi

# The runner owns the phase order: fmt, clippy, prewarm, tests. A lint failure
# must stop the run before building and warm-executing the test binaries.
./scripts/rust-test-gate.sh --prewarm "$@" >>"$LOG" 2>&1
EXIT=$?

# The Linux preflight's exclusion list must cover every rig-dependent test. The check asks
# nextest which tests the filter selects, so it needs the test binaries the run above just
# built; that is why it runs here and not with the operator script suites. CI runs it in the
# Ubuntu archive job for the same reason.
if [ "$EXIT" -eq 0 ]; then
    echo "==> portable preflight selection check"
    PYTHONDONTWRITEBYTECODE=1 "${BROCA_SUITES_PYTHON:-python3}" scripts/test-portable-test-selection.py >>"$LOG" 2>&1
    EXIT=$?
fi

echo "==> gate exit: $EXIT"
if [ "$EXIT" -ne 0 ]; then
    echo "==> failure region (last error in log):"
    # Show the first compile/test error block, or the tail if none matched.
    grep -n -m1 -E "^error(\[|:)|FAILED|panicked at" "$LOG" >/dev/null 2>&1 \
        && grep -B2 -A12 -m3 -E "^error(\[|:)|FAILED|panicked at" "$LOG" \
        || tail -30 "$LOG"
else
    grep -A12 "phase timing summary" "$LOG" | head -14
fi
echo "==> full log: $LOG"
exit "$EXIT"
