#![cfg(unix)]

use broca_core::{load_prior_wal_snapshot, FileWalStore, MaterializedStore, Persister, WalStore};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role},
    request::ModelHandle,
    BindIdentity, MessageId, RunId, StepId, Usage,
};
use broca_store::SqliteStore;
use broca_wal::{wal_bytes_read, RunConfig, RunOrigin, WalRecord};

fn run_started(session: &BindIdentity) -> WalRecord {
    WalRecord::RunStarted {
        written_at_ms: None,
        tags: None,
        session_class: None,
        retention: None,
        hooks: None,
        title: Some("historical title".repeat(4096)),
        on_restart: None,
        keep_warm: None,
        marker_order_version: None,
        manifest: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        ts_ms: None,
        run_id: RunId("anchor-run".into()),
        session: session.clone(),
        config: Box::new(RunConfig {
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: ModelHandle::new("stub", "model"),
            tools: vec![].into(),
            tool_choice: Default::default(),
            generation: Default::default(),
            response_format: Default::default(),
            provider_options: serde_json::Value::Null,
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: serde_json::Value::Null,
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: vec![],
            context_management: None,
        }),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text { text: "hi".into() }],
        }]
        .into(),
        origin: RunOrigin::Fresh,
        submission_id: None,
    }
}

#[test]
fn nonadvancing_projection_preserves_anchor_and_tail_read_cost() {
    let root = broca_test_support::scratch_root()
        .join(format!("nonadvancing-anchor-{}", std::process::id()));
    let wal_store = FileWalStore::new(root.clone());
    let session = BindIdentity::new("/anchor-project", "harness", "session");
    let store = SqliteStore::open_in_memory().unwrap();
    let mut wal = wal_store.open(&session).unwrap();
    wal.append(1, &run_started(&session), false).unwrap();
    wal.append(
        1,
        &WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: AssistantMessage {
                message_id: MessageId("reply".into()),
                content: vec![ContentBlock::Text {
                    text: "done".into(),
                }],
            }
            .into(),
            usage: Usage::default(),
            finish_reason: None,
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        },
        false,
    )
    .unwrap();
    let watermark = wal
        .append(1, &WalRecord::TurnFinished { step_id: StepId(1) }, true)
        .unwrap();
    let range = wal_store
        .read_records_range(&session, 0, None, None)
        .unwrap();
    assert_eq!(
        Persister::project_range(&store, &session, &range).unwrap(),
        watermark
    );
    let initial = store.read_admission_snapshot(&session).unwrap().unwrap();
    let anchor = initial.anchor.expect("file projection must mint an anchor");
    assert_eq!(anchor.seq, watermark);

    // An empty projection really is NoChange. A terminal-only projection still writes
    // final-message metadata, even though its materialization boundary cannot advance.
    assert_eq!(
        Persister::project(&store, &session, &[]).unwrap(),
        watermark
    );
    let safe_head = wal
        .append(
            1,
            &WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
            true,
        )
        .unwrap();
    let tail = wal_store
        .read_records_range(&session, watermark, Some(safe_head), Some(&anchor))
        .unwrap();
    assert!(tail.anchor_at(watermark).is_none());
    assert_eq!(
        Persister::project_range(&store, &session, &tail).unwrap(),
        watermark
    );

    let current = store.read_admission_snapshot(&session).unwrap().unwrap();
    // This binary has one test: the process-wide byte counter cannot include another
    // test's reads. Count actual file I/O, not the already-filtered returned records.
    let before = wal_bytes_read();
    let snapshot = load_prior_wal_snapshot(&wal_store, &session, safe_head, Some(&store)).unwrap();
    let bytes = wal_bytes_read() - before;
    let tail_bytes = tail.complete_byte_end.unwrap() - anchor.byte_end;
    eprintln!(
        "nonadvancing projection: watermark={watermark}, anchor={:?}, bytes_read={bytes}, tail_bytes={tail_bytes}",
        current.anchor
    );
    assert_eq!(current.watermark_seq, watermark);
    assert_eq!(
        current.anchor,
        Some(anchor),
        "non-advancing projection must preserve the WAL anchor"
    );
    assert_eq!(snapshot.base_watermark, watermark);
    assert_eq!(snapshot.tail.len(), 1);
    assert_eq!(snapshot.tail[0].seq, safe_head);
    assert_eq!(
        bytes, tail_bytes,
        "snapshot must skip the materialized WAL prefix"
    );

    // The record-only API has no fresh byte anchors either; repeating a terminal
    // catch-up must preserve the same stored anchor rather than treating it as absent.
    assert_eq!(
        Persister::project(&store, &session, &snapshot.tail).unwrap(),
        watermark
    );
    let repeated = store.read_admission_snapshot(&session).unwrap().unwrap();
    assert_eq!(repeated.watermark_seq, watermark);
    assert_eq!(repeated.anchor, Some(anchor));

    // No byte proof for a *new* boundary is different from an unchanged boundary.
    // A record-only advancing projection must not reuse the old byte offset.
    let next_run = wal_store.read_records(&session).unwrap();
    for frame in &next_run[..3] {
        let mut record = frame.record.clone();
        if let WalRecord::RunStarted { run_id, .. } = &mut record {
            *run_id = RunId("next-run".into());
        }
        wal.append(1, &record, true).unwrap();
    }
    let records = wal_store.read_records(&session).unwrap();
    let advanced_watermark = records.last().unwrap().seq;
    assert_eq!(
        Persister::project(&store, &session, &records).unwrap(),
        advanced_watermark
    );
    assert!(store
        .read_admission_snapshot(&session)
        .unwrap()
        .unwrap()
        .anchor
        .is_none());

    let range = wal_store
        .read_records_range(&session, 0, None, None)
        .unwrap();
    Persister::project_range(&store, &session, &range).unwrap();
    assert!(store
        .read_admission_snapshot(&session)
        .unwrap()
        .unwrap()
        .anchor
        .is_some());

    // A replacement lineage can have the same numeric watermark. Resetting from
    // records without byte anchors must not carry an anchor from the previous file.
    let replacement = FileWalStore::new(root.join("replacement"));
    let mut replacement_wal = replacement.open(&session).unwrap();
    for frame in &records {
        replacement_wal.append(1, &frame.record, true).unwrap();
    }
    let replacement_range = replacement
        .read_records_range(&session, 0, None, None)
        .unwrap();
    let mut unanchored = broca_wal::WalRangeRead::without_anchors(records);
    unanchored.lineage_id = replacement_range.lineage_id;
    assert_eq!(
        Persister::project_range(&store, &session, &unanchored).unwrap(),
        advanced_watermark
    );
    assert!(store
        .read_admission_snapshot(&session)
        .unwrap()
        .unwrap()
        .anchor
        .is_none());
    drop(replacement_wal);
    drop(wal);
    std::fs::remove_dir_all(root).unwrap();
}
