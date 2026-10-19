//! Sends with an ordered `delivery` list across crash cuts and a materialized store.
//!
//! For each delivery mode (`tool_result`, `steer`, `idle_start`) a reference session runs a
//! first episode, accepts an ordered send while idle (`DeliveryOrdered` then `Queued`), and runs
//! the episode that delivers it. Then, for every record boundary from the acceptance to the end of
//! that episode, the WAL is cut there, the cut is folded into a SQLite store, and the session
//! resumes from the store prefix and the WAL tail as broca would after a crash: the unfinished
//! episode resumes, or the next one starts, with every still-queued ordered send offered to it.
//!
//! Each cut must deliver the send exactly once (never, if the cut lost its `Queued` record),
//! leave no half-folded order behind, render every resumed request exactly as a replay of the
//! final WAL does, and render byte for byte what the uncut session sent whenever the cut kept the
//! delivery. A store-backed transcript read must equal a full WAL replay.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::delivery_order::OrderedSend;
use broca_core::{
    provider::Provider, read_session_page, replay, AdmissionCheckpointV1, AllowAll, FileWalStore,
    MaterializedStore, Persister, RunControl, RunRequest, StartMode, SteerInbox, StorePageSnapshot,
    StubReply, StubToolPlane, WalHandle, WalStore,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput},
    provider::{NormalizedEvent, ProviderControl},
    request::{ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice},
    BindIdentity, DeliveryMode, DeliveryOrder, FinishReason, GenerationConfig, MessageId, RunId,
    StepId, SubmissionId, ToolCallId, Usage,
};
use broca_store::SqliteStore;
use broca_wal::{FileWal, Wal, WalRecord};
use serde_json::json;
use tokio::sync::mpsc;

const SUB: &str = "sub-note";

/// A provider that answers each step from a script by step number and keeps the prompt of every
/// request it was sent.
struct Recording {
    scripts: Vec<Vec<NormalizedEvent>>,
    prompts: Mutex<Vec<(u64, Vec<Message>)>>,
}

impl Recording {
    fn new(scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        Self {
            scripts,
            prompts: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl Provider for Recording {
    async fn step(
        &self,
        step_id: StepId,
        opts: &broca_protocol::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.prompts
            .lock()
            .unwrap()
            .push((step_id.0, opts.prompt.to_vec()));
        let (tx, rx) = mpsc::channel(16);
        for event in self
            .scripts
            .get((step_id.0 - 1) as usize)
            .cloned()
            .unwrap_or_default()
        {
            tx.send(event).await.unwrap();
        }
        rx
    }
}

fn finished(reason: FinishReason) -> NormalizedEvent {
    NormalizedEvent::Control(ProviderControl::Finished {
        usage: Usage::default(),
        finish_reason: reason,
        retries_used: 0,
    })
}

fn tool_turn(mid: &str, id: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(mid.into()),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId(id.into()),
                    tool_name: "edit".into(),
                    input: json!({ "f": id }),
                })],
            },
        }),
        finished(FinishReason::ToolCalls),
    ]
}

fn text_turn(mid: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(mid.into()),
                content: vec![ContentBlock::Text {
                    text: "done".into(),
                }],
            },
        }),
        finished(FinishReason::Stop),
    ]
}

/// The episode that delivers the send calls two tools, then answers.
fn delivering_scripts() -> Vec<Vec<NormalizedEvent>> {
    vec![
        tool_turn("m1", "t1"),
        tool_turn("m2", "t2"),
        text_turn("m3"),
    ]
}

/// A later episode, for a send a cut left queued, calls tools of its own.
fn later_scripts() -> Vec<Vec<NormalizedEvent>> {
    vec![
        tool_turn("n1", "u1"),
        tool_turn("n2", "u2"),
        text_turn("n3"),
    ]
}

fn text(text: &str) -> Vec<Message> {
    vec![Message::new(
        Role::User,
        vec![ContentBlock::Text { text: text.into() }],
    )]
}

fn request(dir: &std::path::Path, run: &str, input: &str) -> RunRequest {
    RunRequest {
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId(run.into()),
        session: BindIdentity::new(dir, "ordered", "s1"),
        model: ModelHandle::new("stub", "m"),
        input: text(input),
        tools: vec![Tool {
            name: "edit".into(),
            description: "edit".into(),
            input_schema: json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Parallel,
        }],
        policy_allowed: ["edit".to_string()].into_iter().collect(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 5 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    }
}

fn plane() -> StubToolPlane {
    StubToolPlane::new().with_tool("edit", |c| {
        StubReply::Output(ToolOutput::Text {
            text: format!("edited-{}", c.tool_call_id.0),
        })
    })
}

fn wal_path(dir: &std::path::Path) -> std::path::PathBuf {
    FileWalStore::new(dir.join("wal")).wal_path(&request(dir, "r0", "").session)
}

fn frames(dir: &std::path::Path) -> Vec<broca_wal::FramedRecord> {
    FileWal::open(wal_path(dir)).unwrap().replay().unwrap()
}

fn append(dir: &std::path::Path, records: &[WalRecord]) {
    let mut wal = FileWal::open(wal_path(dir))
        .unwrap()
        .with_feature_floor(dir.join("wal-feature-floor"));
    let fence = wal.replay().unwrap().last().map_or(1, |frame| frame.fence);
    for record in records {
        wal.append(fence, record, true).unwrap();
    }
}

fn write_frames(dir: &std::path::Path, frames: &[broca_wal::FramedRecord]) {
    let path = wal_path(dir);
    let _ = std::fs::remove_file(&path);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut wal = FileWal::open(&path)
        .unwrap()
        .with_feature_floor(dir.join("wal-feature-floor"));
    for frame in frames {
        wal.append(frame.fence, &frame.record, true).unwrap();
    }
}

/// Accept the ordered send while the session is idle: its order, then its queued record.
fn accept(dir: &std::path::Path, mode: DeliveryMode) -> [WalRecord; 2] {
    let queued = request(dir, "note", "note");
    [
        WalRecord::DeliveryOrdered {
            submission_id: SubmissionId(SUB.into()),
            order: DeliveryOrder {
                modes: vec![mode],
                dedupe_key: None,
                expires_at_ms: None,
            },
            sent_by: None,
        },
        WalRecord::Queued {
            retention: None,
            ts_ms: None,
            keep_warm: None,
            conversation_key: None,
            submission_id: SubmissionId(SUB.into()),
            request: Box::new(queued.to_submission()),
            request_identity_digest: None,
            principal: None,
            send_id: None,
            title: None,
            delivery: None,
            hooks: None,
        },
    ]
}

/// Run one episode the way the actor would after reading `dir`'s WAL: an unfinished episode
/// resumes, otherwise the next one starts (draining the queued send when its list names
/// `idle_start`), and every other still-queued ordered send is offered to it.
async fn run_next(
    dir: &std::path::Path,
    run: &str,
    scripts: Vec<Vec<NormalizedEvent>>,
    store: Option<Arc<SqliteStore>>,
) -> Ran {
    let records: Vec<WalRecord> = frames(dir).into_iter().map(|frame| frame.record).collect();
    let checkpoint = AdmissionCheckpointV1::from_frames(&frames(dir)).unwrap();
    let state = replay(&records).unwrap();
    let unfinished = state.run_id.is_some()
        && !matches!(
            state.resolution,
            broca_core::ReplayResolution::LineageIdle { .. }
        );
    let drained = checkpoint
        .pending_fifo
        .iter()
        .find(|pending| pending.starts_runs())
        .cloned();
    let first = state.run_id.is_none();
    let (req, mode) = if unfinished {
        let run_id = state.run_id.clone().unwrap();
        (request(dir, &run_id.0, "unused"), StartMode::FreshOrResume)
    } else if let Some(pending) = &drained {
        let mut req = request(dir, run, "note");
        req.submission_id = Some(pending.submission_id.clone());
        (req, StartMode::AppendEpisode)
    } else if first {
        (request(dir, run, "first"), StartMode::FreshOrResume)
    } else {
        (
            request(dir, run, &format!("work {run}")),
            StartMode::AppendEpisode,
        )
    };
    let consumed = req.submission_id.clone();
    let inbox = SteerInbox::new();
    inbox.seed_ordered(
        checkpoint
            .pending_fifo
            .iter()
            .filter(|pending| consumed.as_ref() != Some(&pending.submission_id))
            .filter_map(OrderedSend::from_pending)
            .collect(),
    );
    let wal_store = FileWalStore::new(dir.join("wal"));
    let writer = WalHandle::spawn(wal_store.open(&req.session).unwrap(), 1).unwrap();
    let provider = Recording::new(scripts);
    let plane = plane();
    let (out, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = broca_core::run_loop_on_writer(
        req,
        &writer,
        mode,
        &provider,
        &plane,
        &AllowAll,
        &wal_store,
        store.map(|store| store as Arc<dyn broca_core::SessionStore>),
        RunControl {
            steer: Some(inbox),
            ..Default::default()
        },
        &out,
    )
    .await
    .expect("run ok");
    drop(out);
    collector.await.unwrap();
    writer.shutdown().await.unwrap();
    let ran: Vec<ToolCallId> = ["t1", "t2", "u1", "u2"]
        .into_iter()
        .map(|id| ToolCallId(id.into()))
        .filter(|id| plane.exec_count(id) > 1)
        .collect();
    let prompts = provider.prompts.lock().unwrap().clone();
    Ran {
        reason: summary.reason,
        prompts,
        ran_twice: ran,
        resumed: unfinished,
    }
}

/// What one episode run did.
struct Ran {
    reason: RunFinishReason,
    /// The prompt of every request it sent, by step.
    prompts: Vec<(u64, Vec<Message>)>,
    /// Tools it ran more than once.
    ran_twice: Vec<ToolCallId>,
    /// Whether it continued an unfinished episode rather than starting one.
    resumed: bool,
}

/// The deliveries of the ordered send the WAL records: a `send_delivered` record, or the
/// `run_started` that drained it.
fn deliveries(records: &[WalRecord]) -> usize {
    records
        .iter()
        .filter(|record| match record {
            WalRecord::SendDelivered { submission_id, .. } => submission_id.0 == SUB,
            WalRecord::RunStarted {
                submission_id: Some(id),
                ..
            } => id.0 == SUB,
            _ => false,
        })
        .count()
}

fn message_bytes(messages: &[Message]) -> Vec<u8> {
    serde_json::to_vec(messages).unwrap()
}

/// A store-backed transcript read (materialized prefix plus WAL tail) and a full WAL replay read.
fn reads_agree(dir: &std::path::Path, store: &SqliteStore) {
    let session = request(dir, "r0", "").session;
    let wal_store = FileWalStore::new(dir.join("wal"));
    let snapshot = store
        .read_page_with_watermark(&session, Some(0), 200, broca_core::context::ReadView::Raw)
        .unwrap();
    let tail = match wal_store
        .read_records_range(
            &session,
            snapshot.watermark_seq,
            None,
            snapshot.anchor.as_ref(),
        )
        .unwrap()
        .into_records()
    {
        broca_wal::WalRangeRecords::Complete(records)
        | broca_wal::WalRangeRecords::Partial(records)
        | broca_wal::WalRangeRecords::Unknown(records) => records,
        broca_wal::WalRangeRecords::Absent => Vec::new(),
    };
    let stored = read_session_page(&snapshot, &tail, Some(0), Some(200)).unwrap();
    let replayed = read_session_page(
        &StorePageSnapshot::empty(),
        &wal_store.read_records(&session).unwrap(),
        Some(0),
        Some(200),
    )
    .unwrap();
    let bytes = |page: &broca_core::SessionReadPage| -> Vec<Vec<u8>> {
        page.messages
            .iter()
            .map(|message| serde_json::to_vec(&message.message).unwrap())
            .collect()
    };
    assert!(
        snapshot.watermark_seq > 0,
        "the store folded part of the session"
    );
    assert_eq!(
        bytes(&stored),
        bytes(&replayed),
        "store-backed read equals a full replay"
    );
}

async fn sweep(mode: DeliveryMode) {
    let base = broca_test_support::scratch_root().join(format!(
        "broca-ordered-cuts-{}-{}",
        mode.as_str(),
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);

    // The reference: a first episode, the send accepted while idle, then the episode that
    // delivers it, all with a store folding the session as it runs.
    let reference = base.join("reference");
    std::fs::create_dir_all(&reference).unwrap();
    let store = Arc::new(SqliteStore::open(reference.join("store.db")).unwrap());
    run_next(&reference, "r0", vec![text_turn("m0")], Some(store.clone())).await;
    let accepted_at = frames(&reference).len();
    append(&reference, &accept(&reference, mode));
    let delivering = run_next(&reference, "r1", delivering_scripts(), Some(store.clone())).await;
    assert_eq!(delivering.reason, RunFinishReason::Completed);
    let reference_prompts = delivering.prompts;
    let reference_frames = frames(&reference);
    let reference_records: Vec<WalRecord> = reference_frames
        .iter()
        .map(|frame| frame.record.clone())
        .collect();
    assert_eq!(
        deliveries(&reference_records),
        1,
        "the reference delivers once"
    );
    // The delivery survives the store fold: the materialized transcript reads as a replay does.
    Persister::project(
        store.as_ref(),
        &request(&reference, "r0", "").session,
        &reference_frames,
    )
    .unwrap();
    reads_agree(&reference, &store);
    let reference_by_step: HashMap<u64, Vec<u8>> = reference_prompts
        .iter()
        .map(|(step, prompt)| (*step, message_bytes(prompt)))
        .collect();
    let delivered_at = reference_records
        .iter()
        .position(|record| match record {
            WalRecord::SendDelivered { submission_id, .. } => submission_id.0 == SUB,
            WalRecord::RunStarted {
                submission_id: Some(id),
                ..
            } => id.0 == SUB,
            _ => false,
        })
        .unwrap();

    // How many cuts exercised each case, so the sweep cannot pass by never reaching one.
    let (mut redelivered, mut identical, mut lost) = (0, 0, 0);
    for keep in accepted_at..reference_frames.len() {
        let label = format!("{} cut@{keep}/{}", mode.as_str(), reference_frames.len());
        let dir = base.join(format!("cut-{keep}"));
        std::fs::create_dir_all(&dir).unwrap();
        write_frames(&dir, &reference_frames[..keep]);
        let cut: Vec<WalRecord> = reference_frames[..keep]
            .iter()
            .map(|frame| frame.record.clone())
            .collect();
        let accepted = cut
            .iter()
            .any(|record| matches!(record, WalRecord::Queued { submission_id, .. } if submission_id.0 == SUB));
        let store = Arc::new(SqliteStore::open(dir.join("store.db")).unwrap());
        Persister::project(
            store.as_ref(),
            &request(&dir, "r0", "").session,
            &reference_frames[..keep],
        )
        .unwrap();

        // Resume, then run one more episode if the send is still queued.
        let mut resumed = Vec::new();
        let first = run_next(&dir, "r1x", delivering_scripts(), Some(store.clone())).await;
        assert!(
            first.ran_twice.is_empty(),
            "{label}: no tool ran twice: {:?}",
            first.ran_twice
        );
        let continued = first.resumed;
        resumed.push(first.prompts);
        let still_queued = AdmissionCheckpointV1::from_frames(&frames(&dir))
            .unwrap()
            .pending_fifo
            .iter()
            .any(|pending| pending.submission_id.0 == SUB);
        if still_queued {
            let later = run_next(&dir, "r2", later_scripts(), Some(store.clone())).await;
            resumed.push(later.prompts);
        }

        let final_frames = frames(&dir);
        let final_records: Vec<WalRecord> = final_frames
            .iter()
            .map(|frame| frame.record.clone())
            .collect();
        let delivered_in_cut = deliveries(&cut) == 1;
        redelivered += usize::from(accepted && !delivered_in_cut);
        lost += usize::from(!accepted);
        assert_eq!(
            deliveries(&final_records),
            usize::from(accepted),
            "{label}: delivered exactly once, and never when the cut lost its Queued record"
        );
        let checkpoint = AdmissionCheckpointV1::from_frames(&final_frames).unwrap();
        assert_eq!(
            checkpoint.pending_order, None,
            "{label}: no half-folded order left behind"
        );
        assert!(
            checkpoint
                .pending_fifo
                .iter()
                .all(|pending| pending.submission_id.0 != SUB),
            "{label}: nothing left queued"
        );
        let replayed = replay(&final_records).unwrap().prompt;
        for prompt in resumed.iter().flatten().map(|(_, prompt)| prompt) {
            assert_eq!(
                message_bytes(&replayed[..prompt.len()]),
                message_bytes(prompt),
                "{label}: every resumed request renders as a replay of the final WAL"
            );
        }
        // A cut that kept the delivery and resumed the same episode renders what it first sent.
        if keep > delivered_at && continued {
            identical += usize::from(!resumed[0].is_empty());
            for (step, prompt) in &resumed[0] {
                assert_eq!(
                    Some(&message_bytes(prompt)),
                    reference_by_step.get(step),
                    "{label}: a cut that kept the delivery resends step {step} byte for byte"
                );
            }
        }
    }
    assert!(lost >= 1, "a cut lost the Queued record");
    assert!(
        redelivered >= 1,
        "a cut left the send queued for the resume to deliver"
    );
    assert!(identical >= 1, "a cut kept the delivery and resent a step");
    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test]
async fn a_tool_result_delivery_survives_every_crash_cut_and_the_store_fold() {
    sweep(DeliveryMode::ToolResult).await;
}

#[tokio::test]
async fn a_steer_delivery_survives_every_crash_cut_and_the_store_fold() {
    sweep(DeliveryMode::Steer).await;
}

#[tokio::test]
async fn an_idle_start_delivery_survives_every_crash_cut() {
    sweep(DeliveryMode::IdleStart).await;
}
