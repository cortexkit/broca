//! Mode-3 creation times are stored once with the transcript, including tail-only projection.

use broca_core::MaterializedStore;
use broca_protocol::{
    request::{BudgetConfig, BudgetNudge},
    BindIdentity,
};
use broca_wal::{FramedRecord, WalRecord};
use serde_json::json;

fn store(name: &str) -> crate::SqliteStore {
    let root = broca_test_support::scratch_root().join(format!(
        "stored-message-times-{}-{name}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    crate::SqliteStore::open(root.join("run-index.db")).unwrap()
}

fn records(mode3: bool, session: &BindIdentity) -> (Vec<FramedRecord>, usize) {
    let mut start: WalRecord = serde_json::from_str(
        include_str!("../../broca-wal/tests/fixtures/before_message_times.jsonl")
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    if let WalRecord::RunStarted {
        session: stored_session,
        compaction_plan_version,
        config,
        ..
    } = &mut start
    {
        *stored_session = session.clone();
        *compaction_plan_version = mode3.then_some(2);
        config.budget = Some(BudgetConfig {
            max_input_tokens: None,
            nudges: vec![BudgetNudge {
                at_tool_calls: 1,
                text: "budget".into(),
            }],
            finalize_at_tool_calls: 9,
            finalize_text: "finish".into(),
        });
    }
    let prefix = [
        json!({"type":"step_started","step_id":1}),
        json!({"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"a","content":[{"type":"tool_call","tool_call_id":"call","tool_name":"work","input":{}}]},"usage":{},"finish_reason":"tool_calls","retries_used":0}),
        json!({"type":"tool_batch_started","batch_id":"b","step_id":1,"planned_calls":["call"]}),
        json!({"type":"tool_dispatch_intent","batch_id":"b","tool_call_id":"call","tool_name":"work","args":{},"dispatched_at_ms":22}),
        json!({"type":"tool_result","batch_id":"b","finished_at_ms":33,"result":{"tool_call_id":"call","output":{"kind":"text","text":"done"},"is_error":false},"outcome":"completed"}),
        json!({"type":"tool_batch_finished","batch_id":"b"}),
        json!({"type":"turn_finished","step_id":1}),
    ];
    let mut records: Vec<WalRecord> = std::iter::once(start)
        .chain(
            prefix
                .into_iter()
                .map(|value| serde_json::from_value(value).unwrap()),
        )
        .collect();
    let split = records.len();
    records.extend([
        json!({"type":"step_started","step_id":2}),
        json!({"type":"model_attempt_finished","step_id":2,"attempt":0,"outcome":"http_error","ts_ms":77,"request_body_len":0,"request_body_sha256":"a","response_body_len":0,"response_body_sha256":"b"}),
        json!({"type":"budget_fired","run_id":"run-7","kind":"nudge","threshold":1,"ordinal":3,"caller_text":"budget"}),
        json!({"type":"model_step_finished","step_id":2,"assistant_message":{"message_id":"last","content":[{"type":"text","text":"untimed reply"}]},"usage":{},"retries_used":0}),
        json!({"type":"turn_finished","step_id":2}),
    ].into_iter().map(|value| serde_json::from_value(value).unwrap()));
    (
        records
            .into_iter()
            .enumerate()
            .map(|(index, record)| FramedRecord {
                seq: index as u64 + 1,
                fence: 1,
                record,
            })
            .collect(),
        split,
    )
}

fn column(store: &crate::SqliteStore, session: &BindIdentity) -> Vec<Option<u64>> {
    let conn = store.conn.lock().unwrap();
    let mut query = conn
        .prepare("SELECT written_at_ms FROM message WHERE session=?1 ORDER BY ord")
        .unwrap();
    query
        .query_map([crate::SqliteStore::key(session)], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn later(after: usize) -> Vec<FramedRecord> {
    [
        json!({"type":"step_started","step_id":3}),
        json!({"type":"model_step_finished","step_id":3,"written_at_ms":99,"assistant_message":{"message_id":"later","content":[{"type":"text","text":"later timed append"}]},"usage":{},"retries_used":0}),
        json!({"type":"turn_finished","step_id":3}),
    ].into_iter().enumerate().map(|(index, value)| FramedRecord {
        seq: (after + index + 1) as u64, fence: 1, record: serde_json::from_value(value).unwrap(),
    }).collect()
}

#[test]
fn mode3_tail_projection_matches_rebuild_and_keeps_stored_creation_times() {
    let session = BindIdentity::new("/project", "runner", "mode3-times");
    let (mut frames, split) = records(true, &session);
    let incremental = store("incremental");
    let rebuilt = store("rebuilt");
    incremental.project(&session, &frames[..split]).unwrap();
    assert_eq!(
        column(&incremental, &session),
        vec![Some(0), Some(0), Some(33)]
    );
    let checkpoint = incremental
        .read_admission_snapshot(&session)
        .unwrap()
        .unwrap()
        .checkpoint
        .unwrap();
    let seed = checkpoint.head.unwrap().mode3_times.unwrap();
    assert_eq!(seed.last_record_ms, Some(33));
    assert_eq!(seed.last_entry_ms, 33);
    incremental.project(&session, &frames[split..]).unwrap();
    rebuilt.project(&session, &frames).unwrap();
    let before = column(&incremental, &session);
    assert_eq!(before, vec![Some(0), Some(0), Some(33), Some(77), Some(77)]);
    assert_eq!(before, column(&rebuilt, &session));
    let suffix = later(frames.len());
    incremental.project(&session, &suffix).unwrap();
    frames.extend(suffix);
    let full = store("full-after-append");
    full.project(&session, &frames).unwrap();
    let after = column(&incremental, &session);
    assert_eq!(&after[..before.len()], before.as_slice());
    assert_eq!(after.last(), Some(&Some(99)));
    assert_eq!(after, column(&full, &session));
}

#[test]
fn legacy_projection_retains_missing_times_without_backfill() {
    let session = BindIdentity::new("/project", "runner", "legacy-times");
    let (frames, _) = records(false, &session);
    let store = store("legacy");
    store.project(&session, &frames).unwrap();
    assert_eq!(
        column(&store, &session),
        vec![None, None, Some(33), None, None]
    );
    // Keep an existing legacy value even when the available records would derive no time.
    store
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE message SET written_at_ms=123 WHERE session=?1 AND ord=0",
            [crate::SqliteStore::key(&session)],
        )
        .unwrap();
    store.conn.lock().unwrap().execute(
        "UPDATE meta SET head_json=json_remove(head_json,'$.mode3'), admission_checkpoint_json=json_remove(admission_checkpoint_json,'$.checkpoint.head.mode3') WHERE session=?1",
        [crate::SqliteStore::key(&session)],
    ).unwrap();
    assert!(!broca_core::is_mode3_log(
        frames.iter().map(|frame| &frame.record)
    ));
    store.cache_history_mode(&session, false).unwrap();
    let before = column(&store, &session);
    assert_eq!(before, vec![Some(123), None, Some(33), None, None]);
    store.project(&session, &later(frames.len())).unwrap();
    let after = column(&store, &session);
    assert_eq!(&after[..before.len()], before.as_slice());
    assert_eq!(after.last(), Some(&Some(99)));
}
