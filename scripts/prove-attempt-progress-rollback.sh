#!/usr/bin/env bash
# Build the released WAL reader, not the worktree's reader, against a newer attempt payload.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/broca-progress-rollback.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
if [ -n "${BROCA_RELEASE_ARCHIVE:-}" ]; then
    tar -xf "$BROCA_RELEASE_ARCHIVE" -C "$scratch"
else
    git -C "$root" archive v0.3.199 | tar -x -C "$scratch"
fi
export BROCA_PROGRESS_FIXTURE="$root/crates/broca-wal/tests/fixtures/model_attempt_with_progress.json"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}/released-progress-reader"
printf 'released reader: v0.3.199 (git archive)\n'
rustc --version
cargo --version
mkdir -p "$scratch/crates/broca-wal/tests"
cat > "$scratch/crates/broca-wal/tests/progress_rollback.rs" <<'RUST'
use broca_wal::{framing::{decode_next, encode_payload, DecodeStep}, WalRecord};

#[test]
fn released_0199_reads_attempt_progress_wal() {
    let payload = std::fs::read(std::env::var("BROCA_PROGRESS_FIXTURE").unwrap()).unwrap();
    let frame = encode_payload(7, 3, &payload).unwrap();
    let path = std::env::temp_dir().join(format!("attempt-progress-{}.wal", std::process::id()));
    std::fs::write(&path, &frame).unwrap();
    let before = std::fs::read(&path).unwrap();
    let DecodeStep::Record { framed, consumed } = decode_next(&before) else {
        panic!("released 0.3.199 refused the progress WAL");
    };
    assert_eq!(consumed, before.len());
    assert_eq!(framed.seq, 7);
    assert!(matches!(framed.record, WalRecord::ModelAttemptFinished { attempt: 0, .. }));
    assert!(framed.record.is_informational());
    assert!(framed.record.required_features().is_empty());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    std::fs::remove_file(path).unwrap();
}
RUST
cargo test --manifest-path "$scratch/Cargo.toml" --locked -p broca-wal --test progress_rollback -- --exact released_0199_reads_attempt_progress_wal
