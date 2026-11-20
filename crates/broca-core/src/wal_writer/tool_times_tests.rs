use super::*;
use broca_protocol::{BatchId, ToolCallId};
use broca_wal::{FileWal, Wal};

fn paths(label: &str) -> (PathBuf, PathBuf) {
    let root = broca_test_support::scratch_root().join(format!(
        "tool-times-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    (root.join("session.wal"), root.join("wal-feature-floor"))
}

fn intent(at: Option<u64>) -> WalRecord {
    WalRecord::ToolDispatchIntent {
        dispatched_at_ms: at,
        batch_id: BatchId("b".into()),
        tool_call_id: ToolCallId("a".into()),
        tool_name: Some("edit".into()),
        args: serde_json::json!({}).into(),
        module_id: None,
        schema_pin: None,
        hooks: None,
        approval_digest: None,
        call_key: None,
    }
}

#[tokio::test]
async fn an_existing_tool_observation_is_not_replaced_at_enqueue() {
    let (path, floor) = paths("preserve");
    let writer = WalHandle::spawn(
        Box::new(FileWal::open(&path).unwrap().with_feature_floor(&floor)),
        1,
    )
    .unwrap();
    writer
        .record(intent(Some(1_800_000_000_123)))
        .into_result()
        .unwrap();
    writer.sync().await.unwrap();
    writer.shutdown().await.unwrap();
    assert!(matches!(
        FileWal::open(&path).unwrap().replay().unwrap()[0].record,
        WalRecord::ToolDispatchIntent {
            dispatched_at_ms: Some(1_800_000_000_123),
            ..
        }
    ));
}

#[tokio::test]
async fn approval_without_a_closing_result_has_no_closing_observation() {
    let (path, floor) = paths("approved");
    let writer = WalHandle::spawn(
        Box::new(FileWal::open(&path).unwrap().with_feature_floor(&floor)),
        1,
    )
    .unwrap();
    writer
        .record(crate::replay::deferred_tests::approval(
            "a",
            "key",
            broca_wal::DeferredDecision::Approved,
        ))
        .into_result()
        .unwrap();
    writer.sync().await.unwrap();
    writer.shutdown().await.unwrap();
    let record = FileWal::open(&path)
        .unwrap()
        .replay()
        .unwrap()
        .remove(0)
        .record;
    assert!(matches!(
        record,
        WalRecord::ApprovalRecorded {
            closed_at_ms: None,
            closing_result: None,
            ..
        }
    ));
    assert!(!record
        .required_features()
        .contains(&broca_wal::TOOL_TIMES_FEATURE));
}
