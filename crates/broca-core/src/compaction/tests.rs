//! Fixtures for the WAL compaction records' tests: a fixed lineage id and the record builders
//! the ceiling tests share.

use super::*;
use broca_protocol::RunFinishReason;

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

#[path = "ceiling_tests.rs"]
mod ceiling_tests;

pub(super) fn image() -> broca_protocol::MediaRef {
    broca_protocol::MediaRef {
        kind: broca_protocol::MediaKind::Image,
        mime: "image/png".into(),
        byte_size: 68,
        sha256: "a".repeat(64),
        width: Some(1),
        height: Some(1),
    }
}

pub(super) fn altered_media() -> Vec<broca_protocol::MediaRef> {
    let mut variants = vec![image(); 6];
    variants[0].sha256 = "b".repeat(64);
    variants[1].mime = "image/jpeg".into();
    variants[2].byte_size += 1;
    variants[3].width = Some(2);
    variants[4].height = Some(2);
    variants[5].kind = broca_protocol::MediaKind::Document;
    variants[5].width = None;
    variants[5].height = None;
    variants
}
