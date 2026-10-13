#!/usr/bin/env bash
set -u

cd "$(dirname "$0")/.." || exit 1

# Build every test binary before any phase with a liveness deadline runs.
# Compile time lands here, visibly, so the test timings/deadlines measure tests
# rather than a cold build consuming the count guard's entire budget.
PREWARM_START=$(date +%s)
cargo test --workspace --lib --bins --tests --no-run "$@" \
    || { echo "==> prewarm build FAILED"; exit 1; }

# Sign and warm-exec every test executable so the one-time Gatekeeper
# assessment happens here, not inside a timed phase. --list is a no-op run
# that still pays (and therefore warms) the assessment.
#
# The count is asserted because this step reports success by doing nothing. If the
# executable paths stop being extractable -- a renamed JSON field, a message-format
# change -- the loop body never runs, every binary meets Gatekeeper inside a timed
# phase instead, and the only symptom is phases that take minutes longer for no
# visible reason. Printing the number turns a silent skip into a readable one.
WARM_BINS="$(mktemp)"
cargo test --workspace --lib --bins --tests --no-run "$@" --message-format=json 2>/dev/null \
    | sed -n 's/.*"executable":"\([^"]*\)".*/\1/p' | sort -u >"$WARM_BINS"
WARM_COUNT=$(wc -l <"$WARM_BINS" | tr -d ' ')
if [ "$WARM_COUNT" -eq 0 ]; then
    echo "==> prewarm FAILED: no test executables to sign" >&2
    echo "    nothing could be extracted from cargo's JSON output; fix that rather than" >&2
    echo "    continuing, or the gate pays the Gatekeeper cost inside its timed phases" >&2
    rm -f "$WARM_BINS"
    exit 1
fi
# This loop is the slowest thing in the gate and used to print nothing while running, so a
# host in the slow first-exec regime looked identical to a hang: two runs died at the wrapper's
# 30-minute cap with the log ending on cargo's last build line, pointing at a package that was
# fine. Measured here on fresh inodes: 0.1s, 22.7s, 46.6s, 52.6s, 90.1s -- and 0.0s on the
# second run of the same binary, which is the tell that it is assessment rather than the
# program. At the high end that is hours across a full rebuild, all of it invisible.
#
# Progress every 10 makes the stall readable and the regime measurable: steady seconds per
# binary is normal, tens of seconds means the host is slow to exec new inodes and the gate is
# not going to finish. Same reason the count above is asserted -- a step that reports success
# by doing nothing cannot be distinguished from a step that did nothing.
WARM_DONE=0
WARM_LOOP_START=$(date +%s)
while read -r bin; do
    [ -x "$bin" ] || continue
    codesign -f -s - "$bin" 2>/dev/null
    "$bin" --list >/dev/null 2>&1 || true
    WARM_DONE=$((WARM_DONE + 1))
    if [ $((WARM_DONE % 10)) -eq 0 ]; then
        echo "    warmed $WARM_DONE/$WARM_COUNT ($(( $(date +%s) - WARM_LOOP_START ))s)"
    fi
done <"$WARM_BINS"
rm -f "$WARM_BINS"
echo "==> prewarm done: warmed $WARM_COUNT executables ($(( $(date +%s) - PREWARM_START ))s)"
