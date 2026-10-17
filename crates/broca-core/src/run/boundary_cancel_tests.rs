use super::*;
use crate::{FileWalStore, SteerInbox, StubProvider, StubToolPlane};
use broca_protocol::provider::ProviderControl;
use broca_wal::record::RunCancelAt;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

fn request() -> RunRequest {
    RunRequest::from_submission(serde_json::from_value(serde_json::json!({
        "model":{"provider_module_id":"stub","model_id":"m"},
        "input":[{"role":"user","content":[{"type":"text","text":"work"}]}],
        "policy_allowed":["work"],
        "tools":[{"name":"work","description":"work","input_schema":{"type":"object"},"execution_mode":"parallel"}]
    })).unwrap(), RunId("boundary".into()), BindIdentity::new("/boundary", "runner", "test"),
        broca_protocol::SubmissionId("s".into()))
}

fn step(tool: bool) -> Vec<NormalizedEvent> {
    let content = if tool {
        vec![
            ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("one".into()),
                tool_name: "work".into(),
                input: serde_json::json!({}),
                thought_signature: None,
            }),
            ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("two".into()),
                tool_name: "work".into(),
                input: serde_json::json!({}),
                thought_signature: None,
            }),
        ]
    } else {
        vec![ContentBlock::Text {
            text: "done".into(),
        }]
    };
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: broca_protocol::MessageId("m".into()),
                content,
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: if tool {
                FinishReason::ToolCalls
            } else {
                FinishReason::Stop
            },
            retries_used: 0,
        }),
    ]
}

struct HeldModel {
    entered: Notify,
    release: Notify,
    calls: AtomicUsize,
    tool: bool,
}

#[async_trait::async_trait]
impl Provider for HeldModel {
    async fn step(&self, _: StepId, _: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.entered.notify_one();
            self.release.notified().await;
        }
        let (tx, rx) = mpsc::channel(8);
        for event in step(self.tool && self.calls.load(Ordering::SeqCst) == 1) {
            tx.send(event).await.unwrap();
        }
        rx
    }
    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        crate::render::canonical_request_bytes(opts)
    }
}

async fn model_cancel(tool: bool, immediate: bool) {
    let root = broca_test_support::scratch_root().join(format!(
        "boundary-{tool}-{immediate}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let store = FileWalStore::new(&root);
    let req = request();
    let writer = WalHandle::spawn(store.open(&req.session).unwrap(), 1).unwrap();
    let inbox = SteerInbox::new();
    let token = CancellationToken::new();
    let model = HeldModel {
        entered: Notify::new(),
        release: Notify::new(),
        calls: AtomicUsize::new(0),
        tool,
    };
    let plane = StubToolPlane::new().with_canned_text("work", "done");
    let (out, _rx) = mpsc::channel(256);
    let mut run = Box::pin(run_loop_on_writer(
        req.clone(),
        &writer,
        StartMode::FreshOrResume,
        &model,
        &plane,
        &crate::AllowAll,
        &store,
        None,
        RunControl {
            steer: Some(inbox.clone()),
            cancel: Some(token.clone()),
            ..Default::default()
        },
        &out,
    ));
    let summary = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::select! { biased; result = &mut run => panic!("model was not held: {result:?}"), _ = model.entered.notified() => {} }
        inbox.request_boundary_cancel(&writer, &req.run_id).await.unwrap();
        inbox.request_boundary_cancel(&writer, &req.run_id).await.unwrap();
        writer.sync().await.unwrap();
        let frames = store.read_records(&req.session).unwrap();
        assert_eq!(frames.iter().filter(|f| matches!(f.record, WalRecord::RunCancelRequested { .. })).count(), 1, "duplicate boundary requests write once");
        if immediate { token.cancel(); } else { model.release.notify_one(); }
        // Escalation must finish while the model is still held, not merely at its boundary.
        run.as_mut().await.unwrap()
    }).await.expect("cancellation must terminate without another model step");
    let frames = store.read_records(&req.session).unwrap();
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.record, WalRecord::StepStarted { .. }))
            .count(),
        1,
        "boundary cancel must start no further model step"
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        summary.reason,
        if !tool && !immediate {
            RunFinishReason::Completed
        } else {
            RunFinishReason::Cancelled
        }
    );
    if tool && !immediate {
        assert_eq!(
            frames
                .iter()
                .filter(|f| matches!(f.record, WalRecord::ToolResult { .. }))
                .count(),
            2,
            "finish the entire tool batch"
        );
    }
    drop(run);
    writer.shutdown().await.unwrap();
}

#[tokio::test]
async fn boundary_cancel_finishes_model_and_entire_tool_batch() {
    model_cancel(true, false).await;
}

#[tokio::test]
async fn immediate_cancel_overrides_pending_boundary_cancel() {
    model_cancel(true, true).await;
}

#[tokio::test]
async fn natural_terminal_wins_boundary_cancel_race() {
    model_cancel(false, false).await;
}

#[tokio::test]
async fn paused_boundary_cancel_recovers_without_a_model_step() {
    let root =
        broca_test_support::scratch_root().join(format!("boundary-paused-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let store = FileWalStore::new(&root);
    let req = request();
    let writer = WalHandle::spawn(store.open(&req.session).unwrap(), 1).unwrap();
    let model = StubProvider::new(vec![]);
    let (out, _rx) = mpsc::channel(256);
    let control = RunControl {
        pause_for_auth_at_start: Some(
            serde_json::from_value(serde_json::json!({
                "class":"auth_required", "message":"reauthenticate"
            }))
            .unwrap(),
        ),
        ..Default::default()
    };
    let plane = StubToolPlane::new().with_canned_text("work", "done");
    let first = run_loop_on_writer(
        req.clone(),
        &writer,
        StartMode::FreshOrResume,
        &model,
        &plane,
        &crate::AllowAll,
        &store,
        None,
        control,
        &out,
    )
    .await
    .unwrap();
    assert!(first.paused.is_some());
    writer.record(WalRecord::RunCancelRequested {
        run_id: req.run_id.clone(),
        at: RunCancelAt::StepBoundary,
    });
    writer.sync().await.unwrap();
    let resumed = run_loop_on_writer(
        req,
        &writer,
        StartMode::FreshOrResume,
        &model,
        &plane,
        &crate::AllowAll,
        &store,
        None,
        RunControl::default(),
        &out,
    )
    .await
    .unwrap();
    assert_eq!(resumed.reason, RunFinishReason::Cancelled);
    assert!(model.calls().is_empty());
    writer.shutdown().await.unwrap();
}

struct SyncGate {
    entered: Notify,
    released: std::sync::Mutex<bool>,
    wake: std::sync::Condvar,
}
impl SyncGate {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}
struct ReleaseOnDrop(Arc<SyncGate>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct GatedWal {
    inner: Box<dyn broca_wal::Wal + Send>,
    gate: Arc<SyncGate>,
    cancel_written: bool,
}
impl broca_wal::Wal for GatedWal {
    fn append(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
    ) -> Result<u64, broca_wal::WalError> {
        self.cancel_written |= matches!(record, WalRecord::RunCancelRequested { .. });
        self.inner.append(fence, record, sync)
    }
    fn sync(&self) -> Result<(), broca_wal::WalError> {
        if self.cancel_written {
            self.gate.entered.notify_one();
            let mut released = self.gate.released.lock().unwrap();
            while !*released {
                released = self.gate.wake.wait(released).unwrap();
            }
        }
        self.inner.sync()
    }
    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.inner.replay()
    }
    fn tail_after_seq(
        &self,
        after: u64,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.inner.tail_after_seq(after)
    }
    fn last_seq(&self) -> u64 {
        self.inner.last_seq()
    }
}

#[tokio::test]
async fn boundary_cancel_ack_waits_for_durable_marker() {
    let root =
        broca_test_support::scratch_root().join(format!("boundary-ack-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let store = FileWalStore::new(&root);
    let gate = Arc::new(SyncGate {
        entered: Notify::new(),
        released: std::sync::Mutex::new(false),
        wake: std::sync::Condvar::new(),
    });
    let req = request();
    let writer = WalHandle::spawn(
        Box::new(GatedWal {
            inner: store.open(&req.session).unwrap(),
            gate: gate.clone(),
            cancel_written: false,
        }),
        1,
    )
    .unwrap();
    let inbox = SteerInbox::new();
    // Release the blocked writer before WalHandle's destructor joins its thread on a panic.
    let _release = ReleaseOnDrop(gate.clone());
    let mut ack = Box::pin(inbox.request_boundary_cancel(&writer, &req.run_id));
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::select! {
            biased;
            result = &mut ack => panic!("cancel acknowledged before its sync barrier: {result:?}"),
            _ = gate.entered.notified() => {}
        }
    })
    .await
    .expect("cancellation must reach its sync barrier");
    assert_eq!(
        writer.durable_head().load(Ordering::Acquire),
        0,
        "held sync is not durable"
    );
    gate.release();
    ack.as_mut().await.unwrap();
    assert_eq!(writer.durable_head().load(Ordering::Acquire), 1);
    assert!(matches!(
        store.read_records(&req.session).unwrap()[0].record,
        WalRecord::RunCancelRequested {
            at: RunCancelAt::StepBoundary,
            ..
        }
    ));
    drop(ack);
    writer.shutdown().await.unwrap();
}

#[test]
fn cancellation_replay_and_checkpoint_are_monotonic_and_episode_scoped() {
    let start: WalRecord = serde_json::from_value(serde_json::json!({
        "type":"run_started", "run_id":"r", "session":{"project_root":"/p","harness":"h","session":"s"},
        "config":{"model":{"provider_module_id":"stub","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},
        "input":[], "origin":{"kind":"fresh"}
    })).unwrap();
    let records = vec![
        start.clone(),
        WalRecord::StepStarted { step_id: StepId(1) },
        WalRecord::RunCancelRequested {
            run_id: RunId("other".into()),
            at: RunCancelAt::Now,
        },
        WalRecord::RunCancelRequested {
            run_id: RunId("r".into()),
            at: RunCancelAt::StepBoundary,
        },
        WalRecord::RunCancelRequested {
            run_id: RunId("r".into()),
            at: RunCancelAt::Now,
        },
        WalRecord::RunCancelRequested {
            run_id: RunId("r".into()),
            at: RunCancelAt::StepBoundary,
        },
    ];
    assert_eq!(crate::replay(&records[..3]).unwrap().cancel_at, None);
    assert_eq!(
        crate::replay(&records).unwrap().cancel_at,
        Some(RunCancelAt::Now)
    );
    let frames: Vec<_> = records
        .iter()
        .enumerate()
        .map(|(index, record)| broca_wal::FramedRecord {
            seq: index as u64 + 1,
            fence: 1,
            record: record.clone(),
        })
        .collect();
    let full = crate::AdmissionCheckpointV1::from_frames(&frames).unwrap();
    assert_eq!(
        full.last_episode.as_ref().unwrap().cancel_at,
        Some(RunCancelAt::Now)
    );
    for split in 0..=frames.len() {
        let mut checkpoint = crate::AdmissionCheckpointV1::from_frames(&frames[..split]).unwrap();
        let encoded = serde_json::to_vec(&checkpoint).unwrap();
        checkpoint = serde_json::from_slice(&encoded).unwrap();
        checkpoint.fold_frames(&frames[split..]).unwrap();
        assert_eq!(
            checkpoint, full,
            "checkpoint lost cancellation at split {split}"
        );
    }
    let mut records = records;
    records.push(WalRecord::RunFinished {
        reason: RunFinishReason::Cancelled,
        ts_ms: None,
        usage: Usage::default(),
        error: None,
        retries_used: 0,
    });
    records.push(start);
    assert_eq!(
        crate::replay(&records).unwrap().cancel_at,
        None,
        "cancellation must not leak into a new episode"
    );
}
