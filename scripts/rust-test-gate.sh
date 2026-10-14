#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

# The local wrapper requests Gatekeeper warming after linting. Direct callers
# (including CI) still run fmt and clippy but do not need that macOS-only step.
prewarm=0
if [[ "${1:-}" == "--prewarm" ]]; then
  prewarm=1
  shift
fi

# Outside CI, the gate runs on the Mac that holds the daemon and vault test binaries. A Linux
# build host has neither, so the real-daemon and vault tests would fail or skip there and the
# result would say nothing about broca. CI builds those binaries itself and sets GITHUB_ACTIONS.
if [[ "$(uname -s)" == Linux && -z "${GITHUB_ACTIONS:-}" && -z "${BROCA_ALLOW_LINUX_GATE:-}" ]]; then
  cat >&2 <<'EOF'
rust-test-gate: refusing to run on a Linux host outside CI.
broca's gate runs on the Mac. Rerun the command locally: do not pass runon, and start it with
an environment assignment (for example BROCA_REQUIRE_SUBC=1 scripts/gate.sh) so it is never
routed to the build server. Set BROCA_ALLOW_LINUX_GATE=1 only on a Linux host that has the
ck-subc, fake-aft-stub, ck-claustrum and ck-auth test binaries.
EOF
  exit 2
fi

python_bin="$(command -v python3 || command -v python || true)"
if [[ -z "$python_bin" ]]; then
  echo "python3 or python is required to count tests for the gate" >&2
  exit 127
fi

if [[ "${BASH_SOURCE[0]}" == "$0" ]] && ! command -v cargo-nextest >/dev/null 2>&1; then
  echo "cargo-nextest is required; install it with: cargo install cargo-nextest --locked" >&2
  exit 127
fi

cargo_feature_args=(
  -F broca-provider/live
  -F broca-catalog/live
  -F broca-module-serve/test-clock
)

# Integration-test binaries that drive real provider APIs and so stay out of the gate. `live` is
# the folded live suite of a crate (tests/live/main.rs); more than one crate may have one.
excluded_live_binaries=(
  live
  live_e2e
)

readonly nextest_filter='kind(test) and not (binary(=live) or binary(=live_e2e))'

nextest_profile="${NEXTEST_PROFILE:-}"
if [[ -z "$nextest_profile" && "${CI:-}" == "true" ]]; then
  nextest_profile="ci"
fi

phase_labels=()
phase_durations=()

record_phase() {
  local label="$1"
  local elapsed="$2"
  phase_labels+=("$label")
  phase_durations+=("$elapsed")
}

run_phase() {
  local label="$1"
  shift
  local started=$SECONDS

  echo "==> $label"
  "$@"
  local elapsed=$((SECONDS - started))
  record_phase "$label" "$elapsed"
  echo "    ok (${elapsed}s)"
}

skip_phase() {
  local label="$1"
  local reason="$2"
  echo "==> $label"
  echo "    skipped (${reason})"
  record_phase "$label" 0
}

print_summary() {
  echo
  echo "==> phase timing summary"
  if [[ ${#phase_labels[@]} -eq 0 ]]; then
    echo "    (no phases completed)"
    return
  fi

  local total=0
  local i
  for i in "${!phase_labels[@]}"; do
    printf '    %-60s %4ss\n' "${phase_labels[$i]}" "${phase_durations[$i]}"
    total=$((total + phase_durations[i]))
  done
  printf '    %-60s %4ss\n' "total" "$total"
}
trap print_summary EXIT

# Hard wall-clock deadline for a command. The counting commands have wedged
# before (a zero-CPU park under machine-wide build load): the compile finishes,
# the process never exits, and the gate silently hangs for hours. A parked
# count is worse than a failed one — kill it and fail loud. The deadline is
# generous (the lib/bins test phase has already built most dependencies by the
# time a count runs) and overridable for slow machines.
count_deadline_secs="${GATE_COUNT_DEADLINE_SECS:-1800}"

# The real-subc e2e arms skip when their sibling binaries are absent, and a skip reports
# itself as a PASS -- the suite prints `ok` and the gate has nothing to notice. That is
# harmless in CI, which sets CI=true and builds the binaries first, and dangerous here:
# a local gate run is exactly where someone decides a change is sound, and it is the one
# place those arms can vanish without a word. Forcing the switch makes a missing binary a
# failure that names itself instead of a silent subtraction from what the gate covers.
export BROCA_REQUIRE_SUBC=1

run_with_deadline() {
  local secs="$1"
  shift
  # A new session gives the command and its descendants a private process group.
  # Killing only cargo's PID leaves rustc/test children alive after the deadline.
  "$python_bin" -c 'import os, sys; os.setsid(); os.execvp(sys.argv[1], sys.argv[1:])' "$@" &
  local pid=$!
  local waited=0
  while kill -0 "$pid" 2>/dev/null; do
    if (( waited >= secs )); then
      kill -KILL -- "-$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      echo "gate: command exceeded its ${secs}s liveness deadline (wedged?): $*" >&2
      return 124
    fi
    sleep 5
    waited=$((waited + 5))
  done
  wait "$pid"
}

count_cargo_integration_tests() {
  local tmp
  tmp="$(mktemp)"
  # This output is PARSED (the count guard), never displayed — force color off so
  # CI's CARGO_TERM_COLOR=always cannot wrap the `Running` lines in ANSI codes and
  # break the target regex.
  if ! run_with_deadline "$count_deadline_secs" env CARGO_TERM_COLOR=never cargo test --workspace "${cargo_feature_args[@]}" --tests -- --list >"$tmp" 2>&1; then
    cat "$tmp" >&2
    rm -f "$tmp"
    return 1
  fi
  # The parse can refuse (see the stale-exclusion check below). A command substitution does not
  # inherit `set -e`, so carry its status out explicitly instead of returning rm's.
  local rc=0
  "$python_bin" - "$tmp" "${excluded_live_binaries[@]}" <<'PY' || rc=$?
import re
import sys

path = sys.argv[1]
excluded = set(sys.argv[2:])
counts = []
targets = []
with open(path, encoding="utf-8", errors="replace") as fh:
    for raw in fh:
        line = raw.rstrip("\n")
        m = re.match(r"^\s*(\d+)\s+tests?,\s+\d+\s+benchmarks?$", line)
        if m:
            counts.append(int(m.group(1)))
            continue
        m = re.match(r"^\s*Running\s+(.+?)\s+\(", line)
        if m:
            targets.append(m.group(1))

if len(counts) != len(targets):
    raise SystemExit(
        f"could not pair cargo test counts ({len(counts)}) with targets ({len(targets)})"
    )

total = 0
seen = set()
for count, target in zip(counts, targets):
    if not (target.startswith("tests/") or target.startswith("tests\\")):
        continue
    parts = re.split(r"[\\/]", target)
    # tests/<name>.rs builds the binary <name>; a multi-file test binary,
    # tests/<name>/main.rs, takes its directory's name instead.
    if len(parts) == 3 and parts[2] == "main.rs":
        binary_name = parts[1]
    else:
        binary_name = re.sub(r"\.rs$", "", parts[-1])
    seen.add(binary_name)
    if binary_name in excluded:
        continue
    total += count

# An exclusion naming a binary that no longer exists excludes nothing, and the tests it was
# written for may now sit in a binary the gate runs. Refuse rather than count past it.
stale = sorted(excluded - seen)
if stale:
    raise SystemExit(
        "excluded live binaries not built by this workspace (rename or remove them in "
        f"scripts/rust-test-gate.sh): {', '.join(stale)}"
    )

print(total)
PY
  rm -f "$tmp"
  return "$rc"
}

count_nextest_integration_tests() {
  local tmp
  local err_tmp
  local cmd
  tmp="$(mktemp)"
  err_tmp="$(mktemp)"
  # Parsed output — color off, same reason as the cargo-test baseline count.
  cmd=(env CARGO_TERM_COLOR=never cargo nextest list --workspace "${cargo_feature_args[@]}" -E "$nextest_filter" -T json)
  if [[ -n "$nextest_profile" ]]; then
    cmd+=(-P "$nextest_profile")
  fi
  if ! run_with_deadline "$count_deadline_secs" "${cmd[@]}" >"$tmp" 2>"$err_tmp"; then
    cat "$err_tmp" >&2
    rm -f "$tmp" "$err_tmp"
    return 1
  fi
  local rc=0
  "$python_bin" - "$tmp" <<'PY' || rc=$?
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)
print(int(data["test-count"]))
PY
  rm -f "$tmp" "$err_tmp"
  if [[ "$rc" -ne 0 ]]; then
    echo "gate: could not count nextest integration tests" >&2
  fi
  return "$rc"
}

count_doc_tests() {
  local tmp
  tmp="$(mktemp)"
  if ! run_with_deadline "$count_deadline_secs" cargo test --workspace "${cargo_feature_args[@]}" --doc -- --list >"$tmp" 2>&1; then
    cat "$tmp" >&2
    rm -f "$tmp"
    return 1
  fi
  local rc=0
  "$python_bin" - "$tmp" <<'PY' || rc=$?
import re
import sys

count = 0
records = 0
with open(sys.argv[1], encoding="utf-8", errors="replace") as fh:
    for raw in fh:
        m = re.match(r"^\s*(\d+)\s+tests?,\s+\d+\s+benchmarks?$", raw.rstrip("\n"))
        if m:
            records += 1
            count += int(m.group(1))
if records == 0:
    sys.exit("gate: could not count doctests: unrecognised cargo test --doc listing")
print(count)
PY
  rm -f "$tmp"
  if [[ "$rc" -ne 0 ]]; then
    echo "gate: could not count doctests" >&2
  fi
  return "$rc"
}

run_integration_phase() {
  local cargo_count
  local nextest_count
  local cmd

  cargo_count="$(count_cargo_integration_tests)" || {
    echo "gate: could not count cargo integration tests" >&2
    return 1
  }
  nextest_count="$(count_nextest_integration_tests)" || return 1

  echo "    integration tests selected: cargo-test baseline=${cargo_count}, nextest=${nextest_count}"
  # Keep independent libtest and nextest discovery: this checks that nextest's
  # filter selects every non-live integration test libtest sees. Deriving both
  # counts from one listing would hide a nextest discovery or selection loss.
  # Both counts are measured here rather than compared against a number written down
  # earlier, so an ordinary change to the suite cannot fail this guard for being merely
  # out of date. The cost is that it cannot see PARTIAL erosion: if some tests stop being
  # built at all, both counts drop together, agree with each other, and pass.
  #
  # Only total disappearance is caught, by the floor below. Catching partial loss would
  # need a committed count to ratchet against, which every legitimate test addition would
  # then have to update -- friction on every change, against a loss that normally shows up
  # as a deleted file in review. Accepted deliberately; revisit if a silent drop ever
  # reaches main.
  if [[ "$cargo_count" -eq 0 ]]; then
    echo "integration test baseline is zero; the gate would be vacuous" >&2
    return 1
  fi
  if [[ "$cargo_count" -ne "$nextest_count" ]]; then
    echo "integration test count mismatch: cargo-test baseline=${cargo_count}, nextest=${nextest_count}" >&2
    return 1
  fi

  cmd=(cargo nextest run --workspace "${cargo_feature_args[@]}" -E "$nextest_filter")
  if [[ -n "$nextest_profile" ]]; then
    cmd+=(-P "$nextest_profile")
  fi

  # SURFACE SKIPS. A test that prints a SKIP notice and returns still PASSES, and nextest hides
  # stdout for passing tests -- so the only trace is a suspiciously short duration nobody reads.
  #
  # `--success-output final` replays that stdout after the run, so the notices become visible
  # without changing any verdict. This deliberately does NOT fail: a legitimately absent sibling
  # binary is a real reason to skip, and turning that into a failure would push people toward
  # deleting the guard rather than reading it.
  #
  # WHAT IS COUNTED HERE IS RUNTIME FIRINGS, NOT SOURCE SITES, and the distinction is the whole
  # reason this comment no longer quotes a number. It used to say "forty-three sites across this
  # workspace" -- a figure counted from the SOURCE TREE, sitting above code that greps the RUN
  # OUTPUT. Two different quantities, and only one of them is what the grep below measures.
  # Reported from outside (issue #11): counting today gives 22, 53 or 61 depending on the
  # pattern, so a bare number in prose was never a fact anyone could check.
  #
  # AND THE RUNTIME COUNT IS ENVIRONMENT-DEPENDENT BY DESIGN. A machine with every sibling binary
  # present skips almost nothing; one without claustrum skips the credential arms legitimately. So
  # a committed expected count would be wrong in one environment or the other, and the first
  # person to hit the wrong one would "fix" it by widening the expectation. The SET printed below
  # is the durable signal: it names WHICH arms declined, so a change is inspectable rather than a
  # number to re-argue.
  local out rc
  out="$(mktemp)"
  rc=0
  "${cmd[@]}" --success-output final 2>&1 | tee "$out" || rc=$?

  # Discovery counts cannot notice tests compiled out of both inventories. Require the
  # feature-only clock test's PASS record, not its name in a listing or echoed command.
  if ! "$python_bin" - "$out" <<'PY_SENTINEL'
import re
import sys

text = open(sys.argv[1], encoding="utf-8", errors="replace").read()
text = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", text)
sentinel = "test_clock::test_clock_feature_is_enabled_for_retention_tests"
passed = re.search(
    # nextest pads its progress counter to the width of the total, e.g. `( 447/1252)`.
    r"^\s*PASS\s+\[[^\]\n]+\]\s+(?:\(\s*\d+/\d+\)\s+)?broca-module::it\s+" + re.escape(sentinel) + r"\s*$",
    text,
    re.MULTILINE,
)
if passed is None:
    sys.exit("gate: test-clock sentinel did not execute successfully: " + sentinel)
print("    executed test-clock sentinel: " + sentinel)
PY_SENTINEL
  then
    # A missing sentinel fails the run, but still reaches skip reporting and capture cleanup.
    [[ "$rc" -ne 0 ]] || rc=1
  fi

  # A CONTROL THAT NEVER RAN IS NOT A CONTROL THAT PASSED.
  #
  # `grep -c` returns 0 for "no skips", for a missing file, for an empty file, and for a run
  # whose output never reached this path -- four states, one number, and three of them are
  # instrument failures wearing the reassuring answer. The skip report exists precisely because
  # a passing test that asserted nothing looks identical to one that did; reporting "0 skips"
  # from an empty capture reproduces that defect one level up.
  #
  # So: prove the capture is readable AND recognisable before believing a zero. The second half
  # is the known-positive control -- if nextest ran at all it emitted a summary line, so its
  # ABSENCE means the pattern or the capture is wrong, not that nothing was skipped.
  local skipped
  if [[ ! -s "$out" ]]; then
    echo
    echo "== skip report could not run: no captured output at ${out}"
    echo "   (this is NOT 'no tests skipped' -- the check itself did not execute)"
    rm -f "$out"
    return "$rc"
  fi
  if ! grep -qE 'Summary \[|test result:|Starting [0-9]+ test' "$out"; then
    echo
    echo "== skip report could not run: captured output holds no recognisable test summary"
    echo "   (the runner changed its output shape, or the capture is of something else;"
    echo "    a zero here would be a pattern failure, not a clean result)"
    rm -f "$out"
    return "$rc"
  fi
  skipped="$(grep -cE '^\s*SKIP [a-z_]+' "$out" || true)"
  if [[ "$skipped" -gt 0 ]]; then
    echo
    echo "== ${skipped} test(s) declined to run and reported ok:"
    grep -hoE 'SKIP [a-z_]+[^\"]*' "$out" | sort -u | sed 's/^/     /'
    echo "   (each is a passing test that asserted nothing; arm its guard to make it fail instead)"
  fi
  rm -f "$out"
  return "$rc"
}

# CI's archive builder reuses the independent inventory parsers, but does not
# run the local, serial gate when importing this file.
if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
  return
fi

run_phase "cargo fmt --all -- --check" \
  cargo fmt --all -- --check

run_phase "cargo clippy --workspace --all-targets (live and test-clock features) -- -D warnings" \
  cargo clippy --workspace --all-targets "${cargo_feature_args[@]}" -- -D warnings

if [[ "$prewarm" -eq 1 ]]; then
  run_phase "prewarm: cargo test --workspace --tests --no-run (live and test-clock features)" \
    bash "$repo_root/scripts/rust-test-prewarm.sh" "${cargo_feature_args[@]}"
fi

run_phase "cargo test --workspace --lib --bins (live and test-clock features) --quiet" \
  cargo test --workspace --lib --bins "${cargo_feature_args[@]}" --quiet

run_phase "cargo nextest run --workspace -E kind(test) (live binaries excluded)" \
  run_integration_phase

doc_count="$(count_doc_tests)"
if [[ "$doc_count" -gt 0 ]]; then
  run_phase "cargo test --workspace --doc (live and test-clock features) --quiet" \
    cargo test --workspace --doc "${cargo_feature_args[@]}" --quiet
else
  skip_phase "cargo test --workspace --doc (live and test-clock features) --quiet" "0 doctests"
fi
