#!/usr/bin/env bash
# Prove the v0.3.202 WAL reader refuses cancellation frames by name but reads ordinary frames.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/broca-cancel-rollback.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/released"
if [ -n "${BROCA_RELEASE_ARCHIVE:-}" ]; then
    tar -xf "$BROCA_RELEASE_ARCHIVE" -C "$scratch/released"
else
    git -C "$root" archive v0.3.202 | tar -x -C "$scratch/released"
fi
export BROCA_CANCEL_AT_WAL_FIXTURE="$scratch/cancel.wal"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}/released-cancel-reader"
rustc --version
cargo --version
cargo test --manifest-path "$root/Cargo.toml" --locked -p broca-wal --lib \
    record::pinned_bytes_tests::cancellation_records_are_must_understand_and_round_trip -- --exact
mkdir -p "$scratch/released/crates/broca-wal/tests"
cat > "$scratch/released/crates/broca-wal/tests/cancel_at_rollback.rs" <<'RUST'
use broca_wal::{replay_file, WalError, WalRecord, DEFAULT_MAX_READ_BYTES};

#[test]
fn released_0202_refuses_cancel_at_by_feature_name() {
    let path = std::env::var_os("BROCA_CANCEL_AT_WAL_FIXTURE").unwrap();
    let before = std::fs::read(&path).unwrap();
    match replay_file(std::path::Path::new(&path), DEFAULT_MAX_READ_BYTES) {
        Err(WalError::NewerWriter { missing, .. }) => assert_eq!(missing, ["run-cancel-at/v1"]),
        other => panic!("released 0.3.202 must refuse pending cancellation by name: {other:?}"),
    }
    assert_eq!(std::fs::read(path).unwrap(), before);
}

#[test]
fn released_0202_reads_wal_without_cancel_at() {
    let mut path = std::env::var_os("BROCA_CANCEL_AT_WAL_FIXTURE").unwrap();
    path.push(".plain");
    let before = std::fs::read(&path).unwrap();
    let records = replay_file(std::path::Path::new(&path), DEFAULT_MAX_READ_BYTES).unwrap();
    assert_eq!(records.len(), 4);
    assert!(matches!(records[0].record, WalRecord::RunStarted { .. }));
    assert!(matches!(records[3].record, WalRecord::RunFinished { .. }));
    assert_eq!(std::fs::read(path).unwrap(), before);
}
RUST
printf 'released reader: v0.3.202 (git archive)\n'
cargo test --manifest-path "$scratch/released/Cargo.toml" --locked -p broca-wal \
    --test cancel_at_rollback -- --nocapture
