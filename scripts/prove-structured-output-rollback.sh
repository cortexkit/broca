#!/usr/bin/env bash
# Prove broca v0.3.212's WAL reader refuses sessions with structured output by feature name
# during rollback, while continuing to read ordinary sessions without modifying either WAL.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/broca-structured-output-rollback.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/released"
if [ -n "${BROCA_RELEASE_ARCHIVE:-}" ]; then
    tar -xf "$BROCA_RELEASE_ARCHIVE" -C "$scratch/released"
else
    git -C "$root" archive v0.3.212 | tar -x -C "$scratch/released"
fi
export RUSTC_WRAPPER= CARGO_BUILD_RUSTC_WRAPPER=
export BROCA_STRUCTURED_OUTPUT_WAL_FIXTURE="$scratch/structured.wal"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}/released-structured-output-reader"
rustc --version
cargo --version
cargo test --manifest-path "$root/Cargo.toml" --locked -p broca-wal --lib \
    record::pinned_bytes_tests::structured_output_records_are_must_understand_and_round_trip -- --exact
mkdir -p "$scratch/released/crates/broca-wal/tests"
cat > "$scratch/released/crates/broca-wal/tests/structured_output_rollback.rs" <<'RUST'
use broca_wal::{replay_file, WalError, WalRecord, DEFAULT_MAX_READ_BYTES};

#[test]
fn released_0212_refuses_structured_output_by_feature_name() {
    let path = std::env::var_os("BROCA_STRUCTURED_OUTPUT_WAL_FIXTURE").unwrap();
    let before = std::fs::read(&path).unwrap();
    match replay_file(std::path::Path::new(&path), DEFAULT_MAX_READ_BYTES) {
        Err(WalError::NewerWriter { missing, .. }) => assert_eq!(missing, ["structured-output/v1"]),
        other => panic!("released 0.3.212 must refuse structured output by name: {other:?}"),
    }
    assert_eq!(std::fs::read(path).unwrap(), before);
}

#[test]
fn released_0212_reads_session_without_structured_output() {
    let mut path = std::env::var_os("BROCA_STRUCTURED_OUTPUT_WAL_FIXTURE").unwrap();
    path.push(".plain");
    let before = std::fs::read(&path).unwrap();
    let records = replay_file(std::path::Path::new(&path), DEFAULT_MAX_READ_BYTES).unwrap();
    assert_eq!(records.len(), 1);
    assert!(matches!(records[0].record, WalRecord::RunStarted { .. }));
    assert_eq!(std::fs::read(path).unwrap(), before);
}
RUST
printf 'released reader: v0.3.212 (git archive; unmodified reader source)\n'
cargo test --manifest-path "$scratch/released/Cargo.toml" --locked -p broca-wal \
    --test structured_output_rollback -- --nocapture
