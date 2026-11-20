//! Fixtures for the WAL compaction records' tests: a scratch lineage id and the record builders
//! the ceiling tests share.

use super::*;
use broca_protocol::RunFinishReason;
use broca_wal::Wal;

pub(super) fn text(role: Role, text: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: text.into() }])
}
pub(super) fn setup_record(ceiling: Option<&str>) -> WalRecord {
    let replacement = vec![text(Role::User, "head")];
    WalRecord::CompactionMessage {
        provider: "provider".into(),
        kind: CompactionMessageKind::Setup,
        compaction_id: "c".into(),
        version: 1,
        range: CompactionRange { from: 0, to: 0 },
        sha256: broca_wal::compaction_replacement_sha256(&replacement).unwrap(),
        replacement: Arc::new(replacement),
        request_id: None,
        cursor: None,
        call_when: None,
        stability: None,
        ceiling: ceiling.map(|json| OpaqueJson::from_json_text(json).unwrap()),
        tag_checkpoint: None,
        ride_wait_ms: None,
        view_features: None,
        view_due: None,
    }
}

fn fixture_lineage_path() -> std::path::PathBuf {
    broca_test_support::scratch_root()
        .join(format!("compaction-lineage-{}.wal", std::process::id()))
}

#[test]
fn compaction_lineage_fixture_uses_private_scratch() {
    let path = fixture_lineage_path();
    assert!(path.starts_with(broca_test_support::scratch_root()));
    assert!(!path.starts_with(std::path::Path::new(env!("CARGO_MANIFEST_DIR"))));
    assert_eq!(fixture_lineage_id(), fixture_lineage_id());
}

fn fixture_lineage_id() -> broca_wal::WalLineageId {
    static LINEAGE: std::sync::OnceLock<broca_wal::WalLineageId> = std::sync::OnceLock::new();
    *LINEAGE.get_or_init(|| {
        // Cargo's target can be a junction, which private WAL creation must refuse.
        let path = fixture_lineage_path();
        let wal = broca_wal::FileWal::open(&path).unwrap();
        let lineage = wal.lineage_id().unwrap();
        drop(wal);
        std::fs::remove_file(path).unwrap();
        lineage
    })
}

#[path = "ceiling_tests.rs"]
mod ceiling_tests;
