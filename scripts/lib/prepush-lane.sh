#!/usr/bin/env bash
# Machine-side lanes also work when AFT dispatches them separately with runon.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
export PYTHONDONTWRITEBYTECODE=1
export CARGO_PROFILE_DEV_DEBUG=line-tables-only
export CARGO_TERM_COLOR=always CI=true
lane="${1:-}"
case "$lane:$(uname -s)" in
  fast:Linux|linux:Linux|windows:MINGW*|windows:MSYS*|macos:Darwin) ;;
  *) echo "prepush: invalid lane/platform: $lane/$(uname -s)" >&2; exit 2 ;;
esac
mkdir -p target/scratch
scratch="$(mktemp -d "$root/target/scratch/prepush-$lane.XXXXXX")"
if [[ "$lane" == windows ]]; then
  export RUNNER_OS=Windows
  scratch="$(cygpath -m "$scratch")"
  CARGO_TARGET_DIR="$(cygpath -m "${CARGO_TARGET_DIR:?motor must supply its persistent target cache}")"
  export CARGO_TARGET_DIR
fi
export TMPDIR="$scratch"
labels=() codes=() times=() files=()
last_rc=0
begin=$SECONDS

step() {
  local label="$1" start=$SECONDS pid rc=0 file
  shift
  file="$scratch/${#labels[@]}.log"
  echo "==> $lane: $label"
  # Keep errexit active inside functions even when their result is being collected.
  ( set -e; "$@" ) > "$file" 2>&1 &
  pid=$!
  wait "$pid" || rc=$?
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
      echo "--- FAILED ${labels[$i]}: complete output ---"
      cat "${files[$i]}"
    fi
  done
  echo "prepush: $lane wall $((SECONDS - begin))s; logs: $scratch"
  exit "$failed"
}

clean() {
  local dirty
  dirty="$(git status --porcelain)"
  [[ -z "$dirty" ]] || { printf '%s\n' "$dirty"; git diff; return 1; }
}

features=(-F broca-provider/live -F broca-provider/test-harness -F broca-catalog/live -F broca-module-serve/test-clock)

checkout_pin() {
  local repo="$1" rev="$2" dest="$3"
  git init -q "$dest"
  git -C "$dest" fetch --depth 1 "https://github.com/cortexkit/$repo.git" "$rev"
  git -C "$dest" checkout -q --detach FETCH_HEAD
  [[ "$(git -C "$dest" rev-parse HEAD)" == "$rev" ]]
}

fixtures() {
  source scripts/ci-fixture-revisions.conf
  checkout_pin subconscious "$SUBC_REV" "$scratch/subconscious"
  checkout_pin claustrum "$CLAUSTRUM_REV" "$scratch/claustrum"
  cargo build --locked --manifest-path "$scratch/subconscious/Cargo.toml" -p subc-core --bins
  cargo build --locked --manifest-path "$scratch/claustrum/Cargo.toml" -p credentials-module --bins
  suffix=''
  [[ "$lane" != windows ]] || suffix=.exe
  mkdir -p "$scratch/package/fixtures/bin" "$scratch/package/fixtures/claustrum/crates/credentials-module/tests/fixtures"
  for name in ck-subc fake-aft-stub ck ck-claustrum ck-auth; do
    cp "$CARGO_TARGET_DIR/debug/$name$suffix" "$scratch/package/fixtures/bin/"
    test -s "$scratch/package/fixtures/bin/$name$suffix"
  done
  if [[ "$lane" != windows ]]; then
    cargo build --locked --manifest-path crates/broca-module-serve/tests/support/fake-surface/Cargo.toml --target-dir "$scratch/fake-surface-target"
    cp "$scratch/fake-surface-target/debug/fake-surface" "$scratch/package/fixtures/bin/"
  fi
  cp "$scratch/claustrum/crates/credentials-module/tests/fixtures/enrollment_wire_contract.json" \
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
  if [[ "$lane" == linux ]]; then
    export BROCA_REQUIRE_VAULT=1
    export CK_CLAUSTRUM_BIN="$scratch/rigs/fixtures/bin/ck-claustrum"
    export CK_AUTH_BIN="$scratch/rigs/fixtures/bin/ck-auth"
    export CK_CLAUSTRUM_CHECKOUT="$scratch/rigs/fixtures/claustrum"
  fi
  # rust-test-gate guards Linux unless the caller supplies the real rigs.
  export BROCA_ALLOW_LINUX_GATE=1
}

broker() {
  RUNNER_TEMP="$scratch" GITHUB_ENV="$scratch/nats.env" python3 scripts/install-ci-nats.py
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
    step 'operator scripts, pins and dependency guards' bash scripts/script-suites.sh
    step 'clean worktree' clean
    ;;
  macos)
    step 'store contention' nice -n 10 cargo test --locked -p broca-store-contention-tests --lib
    step 'WAL reader' nice -n 10 cargo test --locked -p broca-wal-reader
    step 'v4 framing' nice -n 10 cargo test --locked -p broca-wal --lib framing::tests::v4
    step 'clean worktree' clean
    ;;
  linux|windows)
    : "${CARGO_TARGET_DIR:?motor must supply its persistent target cache}"
    rustc --version
    cargo --version
    cargo nextest --version
    step 'Node/Python runtimes' bash scripts/lib/prepush-tools.sh "$lane" "$scratch"
    if [[ $last_rc != 0 ]]; then
      echo 'prepush: Rust and fixture lanes NOT RUN because runtime provisioning failed'
      summary
    fi
    IFS= read -r PATH < "$scratch/runtime.path"
    export PATH
    if [[ "$lane" == linux ]]; then
      step 'Linux portable preflight' bash scripts/linux-preflight.sh
    else
      step fmt cargo fmt --all -- --check
      step 'Windows native clippy' cargo clippy --locked --workspace --all-targets "${features[@]}" -- -D warnings
    fi
    step 'pinned fixtures (build, package, unpack)' fixtures
    if [[ $last_rc != 0 ]]; then
      echo 'prepush: fixture-dependent lanes NOT RUN because fixture build failed'
      summary
    fi
    rig_environment
    step 'aimock install' npm ci
    if [[ $last_rc != 0 ]]; then
      echo 'prepush: fixture-dependent lanes NOT RUN because npm ci failed'
      summary
    fi
    step 'pinned NATS broker' broker
    if [[ $last_rc != 0 ]]; then summary; fi
    # GITHUB_ENV is a file protocol; do not source unquoted Windows paths as shell code.
    IFS= read -r nats_env < "$scratch/nats.env"
    nats_env="${nats_env%$'\r'}"
    export CK_NATS_SERVER_BIN="${nats_env#CK_NATS_SERVER_BIN=}"
    step 'archive and independent inventories' env NEXTEST_PROFILE=ci bash scripts/ci-integration.sh build "$scratch/integration.tar.zst"
    if [[ $last_rc == 0 ]]; then
      if [[ "$lane" == linux ]]; then
        step 'portable selection (built inventory)' python3 scripts/test-portable-test-selection.py
      fi
      for shard in 1 2 3 4; do
        step "integration shard $shard/4" env CK_BUS_TESTKIT_REQUIRE=1 NEXTEST_TEST_THREADS=num-cpus bash scripts/ci-integration.sh run "$scratch/integration.tar.zst" "$shard/4"
      done
    else
      echo 'prepush: integration shards NOT RUN because archive build failed'
    fi
    step 'linked dependency sources' python3 scripts/check-path-deps.py
    step 'linked dependency source tests' python3 scripts/test-check-path-deps.py
    step 'CI lib/bin/doc' lib_doc
    step 'WAL reader' cargo test --locked -p broca-wal-reader
    step 'v4 framing' cargo test --locked -p broca-wal --lib framing::tests::v4
    if [[ "$lane" == linux ]]; then
      step 'production clippy' cargo clippy --locked -p broca-core -p broca-module -p broca-module-serve --all-targets -- -D warnings
      step 'production lib/bin' cargo test --locked -p broca-core -p broca-module -p broca-module-serve --lib --bins --no-fail-fast
      step MSRV cargo +1.96 check --locked --workspace --all-targets
      step 'ELF deployment' deploy
    fi
    step 'clean worktree' clean
    ;;
esac
summary
