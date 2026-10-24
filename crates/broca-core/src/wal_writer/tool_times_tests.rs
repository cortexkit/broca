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
async fn crash_cut_preserves_observed_intent_time_and_records_recovery_finish() {
    let (path, floor) = paths("crash-cut");
    let writer = WalHandle::spawn(
        Box::new(FileWal::open(&path).unwrap().with_feature_floor(&floor)),
        1,
    )
    .unwrap();
    for record in crate::replay::deferred_tests::batch(&["a"]) {
        writer.record(record).into_result().unwrap();
    }
    let before = crate::run::epoch_millis().unwrap();
    writer.record(intent(None)).into_result().unwrap();
    writer.sync().await.unwrap();
    writer.shutdown().await.unwrap();

    // The durable prefix ends after the intent: recovery must not dispatch this call again.
    let wal = FileWal::open(&path).unwrap().with_feature_floor(&floor);
    let prefix = wal.replay().unwrap();
    let observed = match &prefix.last().unwrap().record {
        WalRecord::ToolDispatchIntent {
            dispatched_at_ms, ..
        } => dispatched_at_ms.expect("the intent records its observed time"),
        _ => panic!("the crash cut ends at the intent"),
    };
    assert!((before..=crate::run::epoch_millis().unwrap()).contains(&observed));
    let records: Vec<_> = prefix.iter().map(|frame| frame.record.clone()).collect();
    let prior = crate::replay(&records).unwrap();
    assert!(matches!(
        prior.resolution,
        crate::ReplayResolution::Interrupted { .. }
    ));
    let close = crate::close_out_open_batch(&prior, |_| crate::DeferredCloseKind::NotDispatched);
    let writer = WalHandle::spawn(Box::new(wal), 2).unwrap();
    writer.seed_lineage_tags(Some(crate::tags::lineage_tags(&records)));
    for record in close {
        writer.record(record).into_result().unwrap();
    }
    writer.sync().await.unwrap();
    writer.shutdown().await.unwrap();
    let recovered = FileWal::open(&path).unwrap().replay().unwrap();
    assert_eq!(recovered[prefix.len() - 1], prefix[prefix.len() - 1]);
    assert!(recovered.iter().any(|frame| matches!(frame.record,
        WalRecord::ToolResult { finished_at_ms: Some(at), .. } if at >= observed)));
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
async fn declined_deferred_call_retains_its_recorded_closing_time_after_restart() {
    let (path, floor) = paths("declined");
    let writer = WalHandle::spawn(
        Box::new(FileWal::open(&path).unwrap().with_feature_floor(&floor)),
        1,
    )
    .unwrap();
    for record in crate::replay::deferred_tests::batch(&["a"]) {
        writer.record(record).into_result().unwrap();
    }
    writer
        .record(crate::replay::deferred_tests::prepared("a", "b"))
        .into_result()
        .unwrap();
    let before = crate::run::epoch_millis().unwrap();
    writer
        .record(crate::replay::deferred_tests::approval(
            "a",
            "key",
            broca_wal::DeferredDecision::Declined,
        ))
        .into_result()
        .unwrap();
    writer.sync().await.unwrap();
    writer.shutdown().await.unwrap();
    let records = FileWal::open(&path).unwrap().replay().unwrap();
    assert!(records.iter().any(|frame| matches!(
        frame.record,
        WalRecord::ToolCallDeferred {
            deferred_at_ms: Some(_),
            ..
        }
    )));
    let closed_at = match &records.last().unwrap().record {
        WalRecord::ApprovalRecorded {
            closed_at_ms,
            closing_result: Some(result),
            decision: broca_wal::DeferredDecision::Declined,
            ..
        } => {
            assert_eq!(result.tool_call_id, ToolCallId("a".into()));
            closed_at_ms.expect("a declined approval records its observed closing time")
        }
        _ => panic!("the approval closes the declined call"),
    };
    assert!((before..=crate::run::epoch_millis().unwrap()).contains(&closed_at));
    assert_eq!(FileWal::open(&path).unwrap().replay().unwrap(), records);
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
