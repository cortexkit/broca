#!/usr/bin/env bash
# Machine-side lanes also work when AFT dispatches them separately with runon.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
export PYTHONDONTWRITEBYTECODE=1
export CARGO_PROFILE_DEV_DEBUG=line-tables-only
export CARGO_TERM_COLOR=always CI=true
lane="${1:-}"
case "$lane" in
  fast|linux-operator|linux|linux-portable|linux-unit|linux-integration-[1234]|linux-production|linux-msrv|linux-wal|linux-deploy|windows|macos) ;;
  *) echo "prepush: unknown lane $lane" >&2; exit 2 ;;
esac
platform="${lane%%-*}"
[[ "$lane" != fast ]] || platform=linux
case "$platform:$(uname -s)" in
  linux:Linux|windows:MINGW*|windows:MSYS*|macos:Darwin) ;;
  *) echo "prepush: invalid lane/platform: $lane/$(uname -s)" >&2; exit 2 ;;
esac
if [[ "$lane" == windows ]]; then
  BROCA_PREPUSH_INPUTS="$(cygpath -m "$root/prepush-inputs")"
  export BROCA_PREPUSH_INPUTS
  command -v powershell.exe >/dev/null || {
    echo 'prepush: NOT-RUN windows: PowerShell is required to verify offline inputs' >&2
    exit 200
  }
  powershell.exe -NoProfile -NonInteractive -File \
    "$(cygpath -w "$root/scripts/lib/verify-prepush-inputs.ps1")" \
    -InputsDirectory "$(cygpath -w "$BROCA_PREPUSH_INPUTS")"
fi
python_exec=python3
[[ "$lane" != windows ]] || python_exec=python
printf 'prepush: runner instance=%s lane=%s\n' "${MOTOR_RUNNER_INSTANCE:-unreported}" "$lane"
mkdir -p target/scratch
log_dir="$(mktemp -d "$root/target/scratch/prepush-$lane.XXXXXX")"
# Connection-file publication rejects writable ancestors, even above a private leaf.
# Keep daemon/test temporary files out of the shared, group-writable log directory.
umask 022
if [[ "$lane" == windows ]]; then
  export RUNNER_OS=Windows
  : "${CARGO_TARGET_DIR:?motor must supply its persistent target cache}"
  # Windows has ACLs, not Unix mode bits. Reuse the same native creator and
  # validator as broca's stores before exporting the test temporary directory.
  scratch="$(cargo run --quiet --locked --offline \
    -p broca-owner-only --example prepush_scratch -- "$lane")"
  scratch="$(cygpath -m "$scratch")"
  CARGO_TARGET_DIR="$(cygpath -m "${CARGO_TARGET_DIR:?motor must supply its persistent target cache}")"
  export CARGO_TARGET_DIR
else
  runtime_root="${HOME:?private runtime requires the runner home}/.broca-prepush-runtime"
  mkdir -p -m 700 "$runtime_root"
  chmod 700 "$runtime_root"
  scratch="$(mktemp -d "$runtime_root/$lane.XXXXXX")"
  chmod 700 "$scratch"
fi
export TMPDIR="$scratch"
export TMP="$scratch" TEMP="$scratch"
labels=() codes=() times=() files=()
last_rc=0
begin=$SECONDS
source scripts/lib/prepush-report.sh

step() {
  local label="$1" start=$SECONDS pid rc=0 file
  shift
  file="$log_dir/${#labels[@]}.log"
  echo "==> $lane: $label"
  printf 'command:' > "$file"
  printf ' %q' "$@" >> "$file"
  printf '\n' >> "$file"
  # Keep errexit active inside functions even when their result is being collected.
  ( set -e; "$@" ) >> "$file" 2>&1 &
  pid=$!
  wait "$pid" || rc=$?
  printf '[step exit=%s wall=%ss]\n' "$rc" "$((SECONDS - start))" >> "$file"
  cat "$file"
  labels+=("$label") codes+=("$rc") times+=("$((SECONDS - start))") files+=("$file")
  last_rc=$rc
}

summary() {
  local i failed=0
  echo "==> $lane summary"
  for i in "${!labels[@]}"; do
    printf '%-45s exit=%-3s %ss\n' "${labels[$i]}" "${codes[$i]}" "${times[$i]}"
    if [[ ${codes[$i]} != 0 ]]; then
      failed=1
      echo "--- FIRST ERRORS ${labels[$i]} ---"
      first_errors "${files[$i]}"
      echo "--- FAILED ${labels[$i]}: complete output ---"
      cat "${files[$i]}"
    fi
  done
  echo "prepush: $lane wall $((SECONDS - begin))s; logs: $log_dir; private runtime: $scratch"
  exit "$failed"
}

clean() {
  local dirty
  dirty="$(git status --porcelain --untracked-files=normal)"
  if [[ "$platform" == windows && "$dirty" == '?? prepush-inputs/' ]]; then
    return 0
  fi
  [[ -z "$dirty" ]] || { printf '%s\n' "$dirty"; git diff; return 1; }
}

features=(-F broca-provider/live -F broca-provider/test-harness -F broca-catalog/live -F broca-module-serve/test-clock)

# nextest 0.9.138 can omit a full-weight test with mixed groups at 8 threads:
# the aimock weight-4 test never starts, while every printed test is PASS.
# Four slots pass the synthetic regression and match hosted CI's 2/4-core range.
# Remove this cap once a fixed nextest passes that same regression at 8 threads.
prepush_nextest_threads=4

checkout_pin() {
  local repo="$1" rev="$2" dest="$3"
  if [[ -d "$dest/.git" && "$(git -C "$dest" rev-parse HEAD)" == "$rev" ]]; then return 0; fi
  git init -q "$dest"
  git -C "$dest" fetch --depth 1 "https://github.com/cortexkit/$repo.git" "$rev"
  git -C "$dest" checkout -q --detach FETCH_HEAD
  [[ "$(git -C "$dest" rev-parse HEAD)" == "$rev" ]]
}

fixtures() {
  source scripts/ci-fixture-revisions.conf
  local subc_source vault_source
  if [[ "$platform" == windows ]]; then
    subc_source="$scratch/subconscious-source"
    vault_source="$scratch/claustrum-source"
    "$python_exec" - "$BROCA_PREPUSH_INPUTS" "$SUBC_REV" "$subc_source" <<'PY'
import hashlib, json, pathlib, subprocess, sys
inputs, revision, destination = pathlib.Path(sys.argv[1]), sys.argv[2], pathlib.Path(sys.argv[3])
entry = json.loads((inputs / 'manifest.json').read_text())['source_bundles']['subconscious']
if entry['revision'] != revision or pathlib.Path(entry['file']).name != entry['file']:
    sys.exit('offline Subc bundle does not match the pinned revision')
bundle = inputs / entry['file']
if hashlib.sha256(bundle.read_bytes()).hexdigest() != entry['sha256']:
    sys.exit('offline Subc bundle checksum mismatch')
subprocess.run(['git', 'init', '-q', str(destination)], check=True)
subprocess.run(['git', '-C', str(destination), 'fetch', str(bundle.resolve()), 'HEAD'], check=True)
subprocess.run(['git', '-C', str(destination), 'checkout', '-q', '--detach', revision], check=True)
PY
    "$python_exec" scripts/prepare-prepush-windows.py --extract-source claustrum \
      --inputs "$BROCA_PREPUSH_INPUTS" --revision "$CLAUSTRUM_REV" --output "$vault_source"
  else
    subc_source="$CARGO_TARGET_DIR/fixture-sources/subconscious-$SUBC_REV"
    vault_source="$CARGO_TARGET_DIR/fixture-sources/claustrum-$CLAUSTRUM_REV"
    checkout_pin subconscious "$SUBC_REV" "$subc_source"
    checkout_pin claustrum "$CLAUSTRUM_REV" "$vault_source"
  fi
  cargo build --locked --manifest-path "$subc_source/Cargo.toml" -p subc-core --bins
  cargo build --locked --manifest-path "$vault_source/Cargo.toml" -p credentials-module --bins
  suffix=''
  [[ "$lane" != windows ]] || suffix=.exe
  mkdir -p "$scratch/package/fixtures/bin" "$scratch/package/fixtures/claustrum/crates/credentials-module/tests/fixtures"
  for name in ck-subc fake-aft-stub ck ck-claustrum ck-auth; do
    cp "$CARGO_TARGET_DIR/debug/$name$suffix" "$scratch/package/fixtures/bin/"
    test -s "$scratch/package/fixtures/bin/$name$suffix"
  done
  if [[ "$platform" != windows ]]; then
    cargo build --locked --manifest-path crates/broca-module-serve/tests/support/fake-surface/Cargo.toml --target-dir "$CARGO_TARGET_DIR/fake-surface-target"
    cp "$CARGO_TARGET_DIR/fake-surface-target/debug/fake-surface" "$scratch/package/fixtures/bin/"
  fi
  cp "$vault_source/crates/credentials-module/tests/fixtures/enrollment_wire_contract.json" \
    "$scratch/package/fixtures/claustrum/crates/credentials-module/tests/fixtures/"
  archive="$scratch/fixtures.tar.gz"
  [[ "$lane" != windows ]] || archive="$(cygpath -u "$archive")"
  tar -czf "$archive" -C "$scratch/package" fixtures
  mkdir -p "$scratch/rigs"
  bash scripts/unpack-fixtures.sh "$scratch/fixtures.tar.gz" "$scratch/rigs"
}

rig_environment() {
  local suffix=''
  [[ "$lane" != windows ]] || suffix=.exe
  export BROCA_REQUIRE_SUBC=1 BROCA_REQUIRE_AIMOCK=1
  export SUBC_CORE_BIN="$scratch/rigs/fixtures/bin/ck-subc$suffix"
  export FAKE_AFT_STUB_BIN="$scratch/rigs/fixtures/bin/fake-aft-stub$suffix"
  export FAKE_SURFACE_BIN="$scratch/rigs/fixtures/bin/fake-surface$suffix"
  export BROCA_REQUIRE_VAULT='' CK_CLAUSTRUM_BIN='' CK_AUTH_BIN='' CK_CLAUSTRUM_CHECKOUT=''
  if [[ "$platform" == linux ]]; then
    export BROCA_REQUIRE_VAULT=1
    export CK_CLAUSTRUM_BIN="$scratch/rigs/fixtures/bin/ck-claustrum"
    export CK_AUTH_BIN="$scratch/rigs/fixtures/bin/ck-auth"
    export CK_CLAUSTRUM_CHECKOUT="$scratch/rigs/fixtures/claustrum"
  fi
  # rust-test-gate guards Linux unless the caller supplies the real rigs.
  export BROCA_ALLOW_LINUX_GATE=1
}

broker() {
  local args=()
  if [[ "$platform" == windows ]]; then
    CK_NATS_SERVER_BIN="$(cygpath -m "$(command -v nats-server)")"
    export CK_NATS_SERVER_BIN
    printf 'CK_NATS_SERVER_BIN=%s\n' "$CK_NATS_SERVER_BIN" > "$scratch/nats.env"
    return
  fi
  RUNNER_TEMP="$scratch" GITHUB_ENV="$scratch/nats.env" python3 scripts/install-ci-nats.py "${args[@]}"
}

aimock() {
  if [[ "$platform" == windows ]]; then
    "$python_exec" - "$root/package-lock.json" "$BROCA_PREPUSH_INPUTS/manifest.json" > "$scratch/npm-archives" <<'PY'
import hashlib, json, pathlib, re, sys
manifest = json.loads(pathlib.Path(sys.argv[2]).read_text())
if hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest() != manifest['package_lock_sha256']:
    sys.exit('offline npm inputs do not match package-lock.json')
for name in manifest['npm_archives']:
    if not re.fullmatch(r'[a-f0-9]{64}\.tgz', name):
        sys.exit(f'unsafe offline npm input name: {name}')
    print(name)
PY
    while IFS= read -r name; do
      npm cache add --offline --cache "$scratch/npm-cache" "$BROCA_PREPUSH_INPUTS/$name"
    done < "$scratch/npm-archives"
    npm ci --offline --cache "$scratch/npm-cache"
  else
    npm ci
  fi
}

lib_doc() {
  source scripts/rust-test-gate.sh
  trap - EXIT
  run_phase 'lib/bin tests' cargo test --locked --workspace --lib --bins --no-fail-fast "${cargo_feature_args[@]}" --quiet
  doc_count="$(count_doc_tests)"
  echo "doctests discovered: $doc_count"
  if [[ "$doc_count" -gt 0 ]]; then
    run_phase doctests cargo test --locked --workspace --doc "${cargo_feature_args[@]}" --quiet
  fi
}

deploy() {
  cargo build --release --locked -p broca-module-serve -p broca-run --bin ck-broca --bin broca-session --bin broca-run
  BROCA_DEPLOY_TEST_BIN="$CARGO_TARGET_DIR/release/ck-broca" BROCA_REQUIRE_LINUX_DEPLOY=1 \
    python3 -B scripts/test-place-release-gate.py LinuxDeployTests.test_real_linux_subc_stage_place_restart_verify -v
}

locked_resolution() {
  cargo metadata --locked --offline --format-version 1 > "$scratch/metadata.json"
  python3 - "$scratch/metadata.json" <<'PY'
import json, sys
with open(sys.argv[1]) as source:
    graph = json.load(source)
assert graph['resolve'] is not None, 'missing locked dependency resolution'
print(f"locked resolution: {len(graph['packages'])} packages, {len(graph['workspace_members'])} workspace members")
PY
}

python_tools() {
  if python3 -c 'import tomllib, yaml' 2>/dev/null; then
    python3 --version
  else
    python3 scripts/lib/prepush-python.py "$scratch/python-modules"
  fi
}

linux_image() {
  if ! command -v file >/dev/null; then
    echo 'prepush: MOTOR Linux build image is missing file(1); the image owner must install the file package before operator or ELF deployment tests can run.' >&2
    return 2
  fi
  command -v cc >/dev/null || {
    echo 'prepush: MOTOR Linux build image needs a C compiler (cc) for the operator ELF fixtures.' >&2
    return 2
  }
  file --version
  cc --version
}

case "$lane" in
  fast)
    export CARGO_BUILD_JOBS=2 RUST_TEST_THREADS=2
    step 'Linux image prerequisites' linux_image
    [[ $last_rc == 0 ]] || summary
    step fmt cargo fmt --all -- --check
    [[ $last_rc == 0 ]] || summary
    step 'pins, manifest counts and golden lists' python3 scripts/check-fast-rust-invariants.py
    [[ $last_rc == 0 ]] || summary
    step 'locked fetch' cargo fetch --locked
    [[ $last_rc == 0 ]] || summary
    step 'locked resolution' locked_resolution
    [[ $last_rc == 0 ]] || summary
    step 'Python script prerequisites' python_tools
    [[ $last_rc == 0 ]] || summary
    if ! python3 -c 'import tomllib, yaml' 2>/dev/null; then
      export PYTHONPATH="$scratch/python-modules${PYTHONPATH:+:$PYTHONPATH}"
    fi
    step 'train preconditions' python3 scripts/check-train-preconditions.py
    step 'path dependencies' python3 scripts/check-path-deps.py
    step 'dependency feature guards' python3 scripts/test-dependency-features.py
    step 'portable selection guard fixture' python3 scripts/test-portable-test-selection.py PortableRigSelectionTests.test_exact_rig_test_exclusion_keeps_guard_test_selected
    step 'clean worktree' clean
    ;;
  linux-operator)
    export CARGO_BUILD_JOBS=2 RUST_TEST_THREADS=2
    step 'Linux image prerequisites' linux_image
    [[ $last_rc == 0 ]] || summary
    step 'locked script dependencies' cargo fetch --locked
    [[ $last_rc == 0 ]] || summary
    step 'Python script prerequisites' python_tools
    [[ $last_rc == 0 ]] || summary
    if ! python3 -c 'import tomllib, yaml' 2>/dev/null; then
      export PYTHONPATH="$scratch/python-modules${PYTHONPATH:+:$PYTHONPATH}"
    fi
    step 'operator scripts, pins and dependency guards' bash scripts/script-suites.sh
    step 'clean worktree' clean
    ;;
  macos)
    step 'store contention' nice -n 10 cargo test --locked -p broca-store-contention-tests --lib
    step 'WAL reader' nice -n 10 cargo test --locked -p broca-wal-reader
    step 'v4 framing' nice -n 10 cargo test --locked -p broca-wal --lib framing::tests::v4
    step 'clean worktree' clean
    ;;
  linux|linux-*|windows)
    : "${CARGO_TARGET_DIR:?motor must supply its persistent target cache}"
    case "$lane" in
      linux-production|linux-msrv) export CARGO_BUILD_JOBS=4 RUST_TEST_THREADS=4 ;;
      linux-wal) export CARGO_BUILD_JOBS=2 RUST_TEST_THREADS=2 ;;
      *) export CARGO_BUILD_JOBS=8 RUST_TEST_THREADS=4 ;;
    esac
    portable=0 unit=0 integration=0 production=0 msrv=0 wal=0 deployment=0 lint=0
    case "$lane" in
      linux) portable=1 unit=1 integration=1 production=1 msrv=1 wal=1 deployment=1 ;;
      linux-portable) portable=1 ;;
      linux-unit) unit=1 ;;
      linux-integration-*) integration=1 ;;
      linux-production) production=1 ;;
      linux-msrv) msrv=1 ;;
      linux-wal) wal=1 ;;
      linux-deploy) deployment=1 ;;
      windows) lint=1 unit=1 integration=1 wal=1 ;;
    esac
    if [[ "$platform" == windows ]]; then
      export CARGO_NET_OFFLINE=true
    fi
    rustc --version
    cargo --version
    cargo nextest --version
    if [[ "$platform" != windows && ( $unit == 1 || $integration == 1 || $production == 1 ) ]]; then
      step 'Node/Python runtimes' bash scripts/lib/prepush-tools.sh "$lane" "$scratch"
      if [[ $last_rc != 0 ]]; then
        echo 'prepush: Rust and fixture lanes NOT RUN because runtime provisioning failed'
        summary
      fi
      IFS= read -r PATH < "$scratch/runtime.path"
      export PATH
    fi
    if [[ $portable == 1 || $integration == 1 ]]; then
      step 'pinned NATS broker' broker
      if [[ $last_rc != 0 ]]; then summary; fi
      # GITHUB_ENV is a file protocol; do not source unquoted Windows paths as shell code.
      IFS= read -r nats_env < "$scratch/nats.env"
      nats_env="${nats_env%$'\r'}"
      export CK_NATS_SERVER_BIN="${nats_env#CK_NATS_SERVER_BIN=}"
    fi
    if [[ $portable == 1 ]]; then
      step 'Linux portable preflight' env NEXTEST_TEST_THREADS="$prepush_nextest_threads" bash scripts/linux-preflight.sh
    fi
    if [[ $lint == 1 ]]; then
      step fmt cargo fmt --all -- --check
      step 'Windows native clippy' cargo clippy --locked --workspace --all-targets "${features[@]}" -- -D warnings
    fi
    if [[ $unit == 1 || $integration == 1 || $production == 1 || $deployment == 1 ]]; then
      step 'pinned fixtures (build, package, unpack)' fixtures
      if [[ $last_rc != 0 ]]; then
        echo 'prepush: fixture-dependent lanes NOT RUN because fixture build failed'
        summary
      fi
      rig_environment
    fi
    if [[ $unit == 1 || $integration == 1 || $production == 1 ]]; then
      step 'aimock install' aimock
      if [[ $last_rc != 0 ]]; then
        echo 'prepush: fixture-dependent lanes NOT RUN because npm ci failed'
        summary
      fi
    fi
    if [[ $integration == 1 ]]; then
      step 'archive and independent inventories' env NEXTEST_PROFILE=ci bash scripts/ci-integration.sh build "$scratch/integration.tar.zst"
      if [[ $last_rc == 0 ]]; then
        if [[ "$platform" == linux ]]; then
          step 'portable selection (built inventory)' python3 scripts/test-portable-test-selection.py
        fi
        for shard in 1 2 3 4; do
          [[ "$lane" != linux-integration-* || "$lane" == "linux-integration-$shard" ]] || continue
          step "integration shard $shard/4" env CK_BUS_TESTKIT_REQUIRE=1 NEXTEST_TEST_THREADS="$prepush_nextest_threads" bash scripts/ci-integration.sh run "$scratch/integration.tar.zst" "$shard/4"
        done
      else
        echo 'prepush: integration shards NOT RUN because archive build failed'
      fi
    fi
    if [[ $unit == 1 ]]; then
      step 'linked dependency sources' "$python_exec" scripts/check-path-deps.py
      step 'linked dependency source tests' "$python_exec" scripts/test-check-path-deps.py
      step 'CI lib/bin/doc' lib_doc
    fi
    if [[ $wal == 1 ]]; then
      step 'WAL reader' cargo test --locked -p broca-wal-reader
      step 'v4 framing' cargo test --locked -p broca-wal --lib framing::tests::v4
    fi
    if [[ $production == 1 ]]; then
      step 'production clippy' cargo clippy --locked -p broca-core -p broca-module -p broca-module-serve --all-targets -- -D warnings
      step 'production lib/bin' cargo test --locked -p broca-core -p broca-module -p broca-module-serve --lib --bins --no-fail-fast
    fi
    if [[ $msrv == 1 ]]; then
      # Motor preinstalls the patch-named compiler in a read-only toolchain home.
      # The floating 1.96 alias would try to sync/install instead of using it.
      step MSRV cargo +1.96.0 check --locked --workspace --all-targets
    fi
    if [[ $deployment == 1 ]]; then
      step 'ELF deployment' deploy
    fi
    step 'clean worktree' clean
    ;;
esac
summary
