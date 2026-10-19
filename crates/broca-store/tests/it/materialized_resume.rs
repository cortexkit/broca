//! The MATERIALIZED-resume exec gate — the store-path equivalent of the
//! resume-execution gate.
//!
//! A context unit test proves `prefix ⊕ tail == full-replay` in isolation. This
//! test proves it END-TO-END through a real resumed run: the persister has ACTUALLY
//! MATERIALIZED part of the run into SQLite before the crash, so resume reads
//! `store-prefix ⊕ WAL-tail` (NOT a full replay) — and the resumed run's
//! provider-SENT bytes must STILL be byte-identical to the no-crash reference. A
//! materialization watermark must not change the rendered request bytes.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Condvar, Mutex,
};

use async_trait::async_trait;
use broca_core::{
    latest_projection_boundary, provider::Provider, read_session_page, replay,
    resume_state_from_store, run_loop, run_loop_with_store, AllowAll, FileLeaseStore, FileWalStore,
    MaterializedStore, Persister, ReplayResolution, RunRequest, SessionReadPage, StorePageSnapshot,
    StubReply, StubToolPlane, WalStore,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput},
    provider::{NormalizedEvent, ProviderControl},
    request::{
        BudgetConfig, BudgetNudge, ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice,
    },
    BindIdentity, FinishReason, GenerationConfig, MessageId, RunId, StepId, ToolCallId, Usage,
};
use broca_store::SqliteStore;
use broca_wal::{DispatchOutcome, FileWal, Wal, WalRecord};
use serde_json::json;
use tokio::sync::{mpsc, Notify};

#[derive(Clone)]
struct RecordingProvider {
    scripts: Arc<Vec<Vec<NormalizedEvent>>>,
    sent: Arc<Mutex<HashMap<usize, Vec<u8>>>>,
    step_delay: Option<std::time::Duration>,
    first_projection: Option<Arc<FirstProjectionGate>>,
}
impl RecordingProvider {
    fn new(scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        Self {
            scripts: Arc::new(scripts),
            sent: Arc::new(Mutex::new(HashMap::new())),
            step_delay: None,
            first_projection: None,
        }
    }

    fn paced(scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        Self {
            scripts: Arc::new(scripts),
            sent: Arc::new(Mutex::new(HashMap::new())),
            step_delay: Some(std::time::Duration::from_millis(25)),
            first_projection: None,
        }
    }
}
#[async_trait]
impl Provider for RecordingProvider {
    async fn step(
        &self,
        step_id: StepId,
        opts: &broca_protocol::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let idx = (step_id.0 - 1) as usize;
        if idx == 1 {
            if let Some(gate) = &self.first_projection {
                // Do not produce the second batch until the first projection is in flight.
                gate.started.notified().await;
            }
        }
        self.sent
            .lock()
            .unwrap()
            .insert(idx, self.render_request(opts));
        let (tx, rx) = mpsc::channel(64);
        let script = self.scripts.get(idx).cloned().unwrap_or_default();
        let step_delay = self.step_delay;
        tokio::spawn(async move {
            if let Some(delay) = step_delay {
                tokio::time::sleep(delay).await;
            }
            for ev in script {
                let _ = tx.send(ev).await;
            }
        });
        rx
    }
}

fn tool_turn(mid: &str, id: &str, f: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(mid.into()),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId(id.into()),
                    tool_name: "edit".into(),
                    input: json!({ "f": f }),
                })],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(100),
                output_tokens: Some(10),
                ..Default::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

fn text_turn(mid: &str, text: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(mid.into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(50),
                output_tokens: Some(4),
                ..Default::default()
            },
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

/// A 3-turn script: tool turn → tool turn → text reply (gives ≥2 committed turns
/// to materialize before the crash).
fn scripts() -> Vec<Vec<NormalizedEvent>> {
    vec![
        tool_turn("m1", "t1", "a"),
        tool_turn("m2", "t2", "b"),
        text_turn("m3", "done"),
    ]
}

fn request(dir: &std::path::Path) -> RunRequest {
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
        run_id: RunId("r".into()),
        session: BindIdentity::new(dir, "m10", "s1"),
        model: ModelHandle::new("stub", "m"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "edit a then b".into(),
            }],
        }],
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

fn hard_stop_request(dir: &std::path::Path, stop_when: StopCondition) -> RunRequest {
    let mut request = request(dir);
    request.stop_when = vec![stop_when];
    request
}

fn append_request(dir: &std::path::Path) -> RunRequest {
    let mut request = request(dir);
    request.run_id = RunId("r-next".into());
    request.input = vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "continue after the hard stop".into(),
        }],
    )];
    request
}

struct LaggingStore {
    inner: Arc<SqliteStore>,
    entered: AtomicBool,
    released: Mutex<bool>,
    wake: Condvar,
}

impl LaggingStore {
    fn new(inner: Arc<SqliteStore>) -> Self {
        Self {
            inner,
            entered: AtomicBool::new(false),
            released: Mutex::new(false),
            wake: Condvar::new(),
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

impl MaterializedStore for LaggingStore {
    fn read_prefix_with_watermark(
        &self,
        session: &BindIdentity,
    ) -> Result<broca_core::StorePrefix, String> {
        self.inner.read_prefix_with_watermark(session)
    }

    fn read_admission_snapshot(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<broca_core::AdmissionStoreSnapshot>, String> {
        self.inner.read_admission_snapshot(session)
    }
}

impl Persister for LaggingStore {
    fn projection_seed(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<broca_core::ProjectionSeed>, String> {
        self.entered.store(true, Ordering::SeqCst);
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.wake.wait(released).unwrap();
        }
        Persister::projection_seed(self.inner.as_ref(), session)
    }

    fn project(
        &self,
        session: &BindIdentity,
        records: &[broca_wal::FramedRecord],
    ) -> Result<u64, String> {
        Persister::project(self.inner.as_ref(), session, records)
    }
}

#[derive(Default)]
struct FirstProjectionGate {
    started: Notify,
    released: Mutex<bool>,
    changed: Condvar,
}

impl FirstProjectionGate {
    fn hold(&self) {
        let mut released = self.released.lock().unwrap();
        self.started.notify_one();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

// Release even when a deadline or assertion fails, so the blocking store write cannot
// strand the Tokio runtime while it shuts down.
struct ProjectionReleaseGuard(Arc<FirstProjectionGate>);

impl Drop for ProjectionReleaseGuard {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct CountingStore {
    inner: Arc<SqliteStore>,
    projection_calls: AtomicUsize,
    projected_watermarks: Mutex<Vec<u64>>,
    first_projection: Option<Arc<FirstProjectionGate>>,
}

impl CountingStore {
    fn new(inner: Arc<SqliteStore>) -> Self {
        Self {
            inner,
            projection_calls: AtomicUsize::new(0),
            projected_watermarks: Mutex::new(Vec::new()),
            first_projection: None,
        }
    }

    fn projection_calls(&self) -> usize {
        self.projection_calls.load(Ordering::SeqCst)
    }

    fn before_projection(&self) {
        if self.projection_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            if let Some(gate) = &self.first_projection {
                gate.hold();
            }
        }
    }
}

impl MaterializedStore for CountingStore {
    fn read_page_with_attribution(
        &self,
        session: &BindIdentity,
        from_ordinal: Option<u64>,
        limit: u64,
        _view: broca_core::ReadView,
    ) -> Result<broca_core::StorePageSnapshot, broca_core::StorePageReadError> {
        self.inner.read_page_with_attribution(
            session,
            from_ordinal,
            limit,
            broca_core::ReadView::Raw,
        )
    }
    fn read_prefix_with_watermark(
        &self,
        session: &BindIdentity,
    ) -> Result<broca_core::StorePrefix, String> {
        self.inner.read_prefix_with_watermark(session)
    }

    fn read_admission_snapshot(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<broca_core::AdmissionStoreSnapshot>, String> {
        self.inner.read_admission_snapshot(session)
    }

    fn read_page_with_watermark(
        &self,
        session: &BindIdentity,
        from_ordinal: Option<u64>,
        limit: u64,
        // Model reads need checkpoint-aware queries; raw queries intentionally remain unchanged.
        _view: broca_core::context::ReadView,
    ) -> Result<broca_core::StorePageSnapshot, String> {
        self.inner.read_page_with_watermark(
            session,
            from_ordinal,
            limit,
            broca_core::context::ReadView::Raw,
        )
    }
}

impl Persister for CountingStore {
    fn projection_seed(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<broca_core::ProjectionSeed>, String> {
        Persister::projection_seed(self.inner.as_ref(), session)
    }

    fn project(
        &self,
        session: &BindIdentity,
        records: &[broca_wal::FramedRecord],
    ) -> Result<u64, String> {
        self.before_projection();
        let watermark = Persister::project(self.inner.as_ref(), session, records)?;
        self.projected_watermarks.lock().unwrap().push(watermark);
        Ok(watermark)
    }

    fn project_range(
        &self,
        session: &BindIdentity,
        range: &broca_wal::WalRangeRead,
    ) -> Result<u64, String> {
        self.before_projection();
        let watermark = Persister::project_range(self.inner.as_ref(), session, range)?;
        self.projected_watermarks.lock().unwrap().push(watermark);
        Ok(watermark)
    }

    fn persist_wal_anchor(
        &self,
        session: &BindIdentity,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<(), String> {
        Persister::persist_wal_anchor(self.inner.as_ref(), session, anchor)
    }
}

fn plane() -> StubToolPlane {
    StubToolPlane::new().with_tool("edit", |c| {
        StubReply::Output(ToolOutput::Text {
            text: format!("edited-{}", c.tool_call_id.0),
        })
    })
}

async fn run_with_store(
    dir: &std::path::Path,
    provider: &RecordingProvider,
    plane: &StubToolPlane,
    store: Arc<SqliteStore>,
) -> RunFinishReason {
    run_request_with_store(dir, request(dir), provider, plane, store).await
}

async fn run_request_with_store<S>(
    dir: &std::path::Path,
    request: RunRequest,
    provider: &RecordingProvider,
    plane: &StubToolPlane,
    store: Arc<S>,
) -> RunFinishReason
where
    S: broca_core::SessionStore + 'static,
{
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop_with_store(
        request,
        provider,
        plane,
        &lease,
        &AllowAll,
        &wal,
        Some(store as Arc<dyn broca_core::SessionStore>),
        &tx,
    )
    .await
    .expect("run ok");
    drop(tx);
    collector.await.unwrap();
    summary.reason
}

async fn run_request_without_store(
    dir: &std::path::Path,
    request: RunRequest,
    provider: &RecordingProvider,
    plane: &StubToolPlane,
) -> RunFinishReason {
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop(request, provider, plane, &lease, &AllowAll, &wal, &tx)
        .await
        .expect("run ok");
    drop(tx);
    collector.await.unwrap();
    summary.reason
}

async fn run_without_store(
    dir: &std::path::Path,
    provider: &RecordingProvider,
    plane: &StubToolPlane,
) -> RunFinishReason {
    run_request_without_store(dir, request(dir), provider, plane).await
}

fn truncate_wal(dir: &std::path::Path, keep: usize) {
    let ws = FileWalStore::new(dir.join("wal"));
    let path = ws.wal_path(&request(dir).session);
    let framed = FileWal::open(&path).unwrap().replay().unwrap();
    let _ = std::fs::remove_file(&path);
    // A tagged session's records are gated frames: raise the feature floor as the store does.
    let mut wal = FileWal::open(&path)
        .unwrap()
        .with_feature_floor(ws.feature_floor_path());
    for f in framed.into_iter().take(keep) {
        wal.append(f.fence, &f.record, true).unwrap();
    }
}

fn wal_len(dir: &std::path::Path) -> usize {
    let ws = FileWalStore::new(dir.join("wal"));
    FileWal::open(ws.wal_path(&request(dir).session))
        .unwrap()
        .replay()
        .unwrap()
        .len()
}

fn wal_frames(dir: &std::path::Path) -> Vec<broca_wal::FramedRecord> {
    let ws = FileWalStore::new(dir.join("wal"));
    FileWal::open(ws.wal_path(&request(dir).session))
        .unwrap()
        .replay()
        .unwrap()
}

fn store_backed_page(
    store: &SqliteStore,
    wal_store: &FileWalStore,
    session: &BindIdentity,
) -> SessionReadPage {
    let snapshot = store
        .read_page_with_watermark(session, Some(0), 100, broca_core::context::ReadView::Raw)
        .expect("read materialized page");
    let tail = wal_store
        .read_records_range(
            session,
            snapshot.watermark_seq,
            None,
            snapshot.anchor.as_ref(),
        )
        .expect("read bounded WAL tail");
    let tail = match tail.into_records() {
        broca_wal::WalRangeRecords::Complete(records)
        | broca_wal::WalRangeRecords::Partial(records)
        | broca_wal::WalRangeRecords::Unknown(records) => records,
        broca_wal::WalRangeRecords::Absent => Vec::new(),
    };
    read_session_page(&snapshot, &tail, Some(0), Some(100))
        .expect("merge materialized page and WAL tail")
}

fn full_replay_page(wal_store: &FileWalStore, session: &BindIdentity) -> SessionReadPage {
    read_session_page(
        &StorePageSnapshot::empty(),
        &wal_store.read_records(session).expect("read complete WAL"),
        Some(0),
        Some(100),
    )
    .expect("read page by full WAL replay")
}

fn page_message_bytes(page: &SessionReadPage) -> Vec<(u64, Vec<u8>)> {
    page.messages
        .iter()
        .map(|message| {
            (
                message.ordinal,
                serde_json::to_vec(&message.message).expect("serialize session.read message"),
            )
        })
        .collect()
}

fn write_wal_frames(dir: &std::path::Path, frames: &[broca_wal::FramedRecord]) {
    let ws = FileWalStore::new(dir.join("wal"));
    let path = ws.wal_path(&request(dir).session);
    let _ = std::fs::remove_file(&path);
    // A tagged session's records are gated frames: raise the feature floor as the store does.
    let mut wal = FileWal::open(&path)
        .unwrap()
        .with_feature_floor(ws.feature_floor_path());
    for frame in frames {
        wal.append(frame.fence, &frame.record, true).unwrap();
    }
}

fn corrupt_first_materialized_message(dir: &std::path::Path) {
    let session = request(dir).session;
    let key = format!(
        "{}\u{1f}{}\u{1f}{}",
        session.project_root.display(),
        session.harness,
        session.session
    );
    let conn = rusqlite::Connection::open(dir.join("store.db")).unwrap();
    conn.execute(
        "UPDATE message SET json = ?2 WHERE session = ?1 AND ord = 0",
        rusqlite::params![key, "{"],
    )
    .unwrap();
}

#[tokio::test]
async fn materialized_resume_is_byte_identical() {
    let base = broca_test_support::scratch_root().join(format!("broca-m10-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);

    // 1. REFERENCE: a clean no-crash run WITH a store (so the persister runs the
    //    whole time). Capture per-step sent bytes.
    let ref_dir = base.join("ref");
    std::fs::create_dir_all(&ref_dir).unwrap();
    let ref_store = Arc::new(SqliteStore::open(ref_dir.join("store.db")).unwrap());
    let ref_provider = RecordingProvider::new(scripts());
    let ref_reason = run_with_store(&ref_dir, &ref_provider, &plane(), ref_store.clone()).await;
    assert_eq!(ref_reason, RunFinishReason::Completed);
    let ref_sent = ref_provider.sent.lock().unwrap().clone();
    let total = wal_len(&ref_dir);

    // 2. For each cut: truncate a completed WAL to the durable crash head, project only
    //    that durable prefix, then resume with the same store. Projecting after the cut avoids
    //    an impossible snapshot whose watermark points beyond the simulated WAL head.
    for keep in 1..total {
        let dir = base.join(format!("cut-{keep}"));
        std::fs::create_dir_all(&dir).unwrap();

        run_without_store(&dir, &RecordingProvider::new(scripts()), &plane()).await;
        truncate_wal(&dir, keep);
        let cut_frames = wal_frames(&dir);
        let store = Arc::new(SqliteStore::open(dir.join("store.db")).unwrap());
        Persister::project(store.as_ref(), &request(&dir).session, &cut_frames)
            .expect("project durable records at the crash cut");

        // RESUME with the SAME store + a fresh recording provider + fresh plane.
        let provider = RecordingProvider::new(scripts());
        let plane = plane();
        let reason = run_with_store(&dir, &provider, &plane, store.clone()).await;
        let label = format!("cut@{keep}/{total}");

        // C7: every step the RESUME re-issued must be byte-identical to the
        // reference — proven through the store-prefix ⊕ tail merge, not full replay.
        let resumed_sent = provider.sent.lock().unwrap().clone();
        for (idx, bytes) in &resumed_sent {
            assert_eq!(
                bytes,
                ref_sent.get(idx).expect("reference sent this step"),
                "{label}: materialized resume re-rendered step {idx} byte-identical (C7)"
            );
        }

        // Exactly-once: no tool ran twice across crash+resume.
        for id in ["t1", "t2"] {
            assert!(
                plane.exec_count(&ToolCallId(id.into())) <= 1,
                "{label}: {id} ran ≤once in resume"
            );
        }

        // The run reaches a terminal state (Completed or Interrupted on an
        // indeterminate cut) — never a divergent/error finish.
        assert!(
            matches!(
                reason,
                RunFinishReason::Completed | RunFinishReason::Interrupted
            ),
            "{label}: terminal finish, got {reason:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test]
async fn lagging_projector_does_not_change_resume_state() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-lagging-projector-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let store = Arc::new(LaggingStore::new(Arc::new(
        SqliteStore::open(dir.join("store.db")).unwrap(),
    )));
    let provider = RecordingProvider::new(scripts());
    let run_dir = dir.clone();
    let run_store = store.clone();
    let run_provider = provider.clone();
    let run = tokio::spawn(async move {
        let lease = FileLeaseStore::new(run_dir.join("lease"));
        let wal = FileWalStore::new(run_dir.join("wal"));
        let (tx, mut rx) = mpsc::channel(512);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let summary = run_loop_with_store(
            request(&run_dir),
            &run_provider,
            &plane(),
            &lease,
            &AllowAll,
            &wal,
            Some(run_store as Arc<dyn broca_core::SessionStore>),
            &tx,
        )
        .await;
        drop(tx);
        collector.await.unwrap();
        summary
    });

    broca_test_support::wait_until("model loop advances with its first projection blocked", || async {
            let sent_turns = provider.sent.lock().unwrap().len();
            let terminal_is_durable = FileWalStore::new(dir.join("wal"))
                .read_records(&request(&dir).session)
                .is_ok_and(|frames| {
                    frames.iter().any(|frame| {
                        matches!(frame.record, broca_wal::WalRecord::RunFinished { .. })
                    })
                });
            if store.entered.load(Ordering::SeqCst) && sent_turns >= 3 && terminal_is_durable {
                return Ok(());
            }
            Err(format!("projector_entered={}, sent_turns={sent_turns}, terminal_durable={terminal_is_durable}", store.entered.load(Ordering::SeqCst)))
    })
    .await;
    let returned_before_projector_release = run.is_finished();

    let session = request(&dir).session;
    let prefix = store
        .read_prefix_with_watermark(&session)
        .expect("lagging store remains readable");
    assert_eq!(prefix.watermark_seq, 0);

    let frames = FileWalStore::new(dir.join("wal"))
        .read_records(&session)
        .expect("read the durable WAL while projection is lagging");
    let records: Vec<_> = frames.iter().map(|frame| frame.record.clone()).collect();
    let full = broca_core::replay(&records).expect("full WAL replay");
    let merged = resume_state_from_store(
        &prefix,
        &records,
        full.run_config.clone(),
        full.run_id.clone(),
        None,
    )
    .expect("empty store prefix plus the longer WAL tail resumes");
    assert_eq!(merged.prompt, full.prompt);
    assert_eq!(merged.run_config, full.run_config);
    assert_eq!(merged.run_id, full.run_id);
    assert_eq!(merged.completed_steps, full.completed_steps);
    assert_eq!(merged.usage, full.usage);
    assert_eq!(merged.resolution, full.resolution);

    store.release();
    assert!(
        !returned_before_projector_release,
        "a durable terminal does not return before the projector is joined"
    );
    let summary =
        broca_test_support::wait_for("projector drain completes after release", run, || {
            format!(
                "projector_entered={}, sent_turns={}",
                store.entered.load(Ordering::SeqCst),
                provider.sent.lock().unwrap().len()
            )
        })
        .await
        .expect("run task joins")
        .expect("WAL-authoritative run succeeds");
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert!(
        store
            .read_prefix_with_watermark(&session)
            .expect("drained projection is readable")
            .watermark_seq
            > 0,
        "shutdown drains the terminal projection"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn projected_no_tool_terminal_restart_is_idle_without_provider_call() {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-no-tool-terminal-restart-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let store_path = dir.join("store.db");
    let seed_store = Arc::new(SqliteStore::open(&store_path).unwrap());
    let seed_provider = RecordingProvider::new(scripts());
    assert_eq!(
        run_with_store(&dir, &seed_provider, &plane(), seed_store.clone()).await,
        RunFinishReason::Completed
    );

    let frames = wal_frames(&dir);
    let terminal_step_seq = frames
        .iter()
        .find_map(|frame| match &frame.record {
            broca_wal::WalRecord::ModelStepFinished {
                assistant_message, ..
            } if assistant_message.tool_calls().next().is_none() => Some(frame.seq),
            _ => None,
        })
        .expect("seed run contains a no-tool terminal step");
    let prefix =
        MaterializedStore::read_prefix_with_watermark(seed_store.as_ref(), &request(&dir).session)
            .expect("projected prefix is readable");
    assert_eq!(prefix.watermark_seq, terminal_step_seq);

    let tail: Vec<broca_wal::WalRecord> = frames
        .iter()
        .filter(|frame| frame.seq > prefix.watermark_seq)
        .map(|frame| frame.record.clone())
        .collect();
    assert!(matches!(
        tail.first(),
        Some(broca_wal::WalRecord::RunFinished { .. })
    ));
    let (run_config, run_id) = frames
        .iter()
        .rev()
        .find_map(|frame| match &frame.record {
            broca_wal::WalRecord::RunStarted { run_id, config, .. } => {
                Some(((**config).clone(), run_id.clone()))
            }
            _ => None,
        })
        .expect("seed run contains RunStarted");
    let resumed = resume_state_from_store(&prefix, &tail, Some(run_config), Some(run_id), None)
        .expect("materialized terminal state resumes");
    assert!(matches!(
        resumed.resolution,
        ReplayResolution::LineageIdle {
            last_reason: RunFinishReason::Completed
        }
    ));

    drop(seed_store);
    let restarted_store = Arc::new(SqliteStore::open(&store_path).unwrap());
    let restarted_provider = RecordingProvider::new(scripts());
    assert_eq!(
        run_with_store(&dir, &restarted_provider, &plane(), restarted_store,).await,
        RunFinishReason::Completed
    );
    assert!(
        restarted_provider.sent.lock().unwrap().is_empty(),
        "an idle terminal lineage must not call the provider after process restart"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn hard_stop_turn_materializes_and_store_merge_matches_full_replay() {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-hard-stop-materialized-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let store = Arc::new(SqliteStore::open(dir.join("store.db")).unwrap());
    let hard_stop = hard_stop_request(&dir, StopCondition::StepCount { n: 1 });
    let session = hard_stop.session.clone();
    let provider = RecordingProvider::new(vec![tool_turn("m-hard-stop", "t-hard-stop", "a")]);
    assert_eq!(
        run_request_with_store(&dir, hard_stop, &provider, &plane(), store.clone()).await,
        RunFinishReason::MaxSteps
    );

    let wal_store = FileWalStore::new(dir.join("wal"));
    let frames = wal_store
        .read_records(&session)
        .expect("read hard-stop WAL");
    let turn_finished_seq = frames
        .iter()
        .find_map(|frame| {
            matches!(frame.record, WalRecord::TurnFinished { .. }).then_some(frame.seq)
        })
        .expect("hard-stop WAL contains the canonical turn boundary");
    assert_eq!(
        latest_projection_boundary(&frames, 0),
        Some(turn_finished_seq)
    );

    let prefix = store
        .read_prefix_with_watermark(&session)
        .expect("read materialized hard-stop prefix");
    assert_eq!(prefix.watermark_seq, turn_finished_seq);
    assert_eq!(prefix.prefix.len(), 3);
    assert_eq!(
        prefix
            .prefix
            .iter()
            .map(|message| message.role)
            .collect::<Vec<_>>(),
        vec![Role::User, Role::Assistant, Role::Tool]
    );

    let snapshot = store
        .read_page_with_watermark(&session, Some(0), 100, broca_core::context::ReadView::Raw)
        .expect("read hard-stop store page");
    assert_eq!(snapshot.watermark_seq, turn_finished_seq);
    assert!(snapshot.prefix_len > 0);
    let tail = wal_store
        .read_records_range(
            &session,
            snapshot.watermark_seq,
            None,
            snapshot.anchor.as_ref(),
        )
        .expect("read hard-stop terminal tail");
    let tail = match tail.into_records() {
        broca_wal::WalRangeRecords::Complete(records)
        | broca_wal::WalRangeRecords::Partial(records)
        | broca_wal::WalRangeRecords::Unknown(records) => records,
        broca_wal::WalRangeRecords::Absent => Vec::new(),
    };
    assert!(matches!(
        tail.first().map(|frame| &frame.record),
        Some(WalRecord::RunFinished { .. })
    ));

    let merged = store_backed_page(store.as_ref(), &wal_store, &session);
    let full = full_replay_page(&wal_store, &session);
    assert_eq!(page_message_bytes(&merged), page_message_bytes(&full));
    assert_eq!(merged.lineage_state, full.lineage_state);
    let full_prompt = replay(
        &frames
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>(),
    )
    .expect("full hard-stop WAL replays")
    .prompt;
    assert_eq!(
        serde_json::to_vec(&prefix.prefix).expect("serialize materialized prefix"),
        serde_json::to_vec(&full_prompt).expect("serialize full-replay prompt")
    );

    // Existing hard-stop WALs omit TurnFinished. They remain readable, but have no safe
    // materialization boundary and therefore retain the full-WAL fallback.
    let legacy_frames: Vec<_> = frames
        .iter()
        .filter(|frame| !matches!(frame.record, WalRecord::TurnFinished { .. }))
        .cloned()
        .collect();
    assert_eq!(latest_projection_boundary(&legacy_frames, 0), None);
    let legacy_store = SqliteStore::open(dir.join("legacy-store.db")).unwrap();
    assert_eq!(
        Persister::project(&legacy_store, &session, &legacy_frames)
            .expect("legacy projection remains readable"),
        0
    );
    let legacy_prefix = legacy_store
        .read_prefix_with_watermark(&session)
        .expect("read legacy empty prefix");
    assert_eq!(legacy_prefix.watermark_seq, 0);
    assert!(legacy_prefix.prefix.is_empty());
    let legacy_snapshot = legacy_store
        .read_page_with_watermark(&session, Some(0), 100, broca_core::context::ReadView::Raw)
        .expect("read legacy page metadata");
    assert_eq!(legacy_snapshot.watermark_seq, 0);
    assert_eq!(legacy_snapshot.prefix_len, 0);
    let legacy_page = read_session_page(&legacy_snapshot, &legacy_frames, Some(0), Some(100))
        .expect("legacy hard-stop WAL replays completely");
    assert_eq!(page_message_bytes(&legacy_page), page_message_bytes(&full));
    assert_eq!(legacy_page.lineage_state, full.lineage_state);

    // Every record-prefix remains replayable. Once ToolBatchFinished commits all synthetic
    // results, replay must classify MaxSteps whether or not TurnFinished is present.
    for cut in 1..=frames.len() {
        let records: Vec<_> = frames[..cut]
            .iter()
            .map(|frame| frame.record.clone())
            .collect();
        let state = replay(&records).unwrap_or_else(|error| {
            panic!("hard-stop crash cut at record {cut} failed replay: {error}")
        });
        if records
            .iter()
            .any(|record| matches!(record, WalRecord::ToolBatchFinished { .. }))
        {
            assert!(matches!(
                state.resolution,
                ReplayResolution::LineageIdle {
                    last_reason: RunFinishReason::MaxSteps
                }
            ));
        }
    }

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn has_tool_call_hard_stop_materializes_interrupted_results() {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-has-tool-call-materialized-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let store = Arc::new(SqliteStore::open(dir.join("store.db")).unwrap());
    let request = hard_stop_request(
        &dir,
        StopCondition::HasToolCall {
            name: Some("edit".into()),
        },
    );
    let session = request.session.clone();
    let provider = RecordingProvider::new(vec![tool_turn("m-has-tool", "t-has-tool", "a")]);
    assert_eq!(
        run_request_with_store(&dir, request, &provider, &plane(), store.clone()).await,
        RunFinishReason::Completed
    );

    let frames = FileWalStore::new(dir.join("wal"))
        .read_records(&session)
        .expect("read HasToolCall WAL");
    let turn_finished_seq = frames
        .iter()
        .find_map(|frame| {
            matches!(frame.record, WalRecord::TurnFinished { .. }).then_some(frame.seq)
        })
        .expect("HasToolCall hard stop contains TurnFinished");
    assert_eq!(
        latest_projection_boundary(&frames, 0),
        Some(turn_finished_seq)
    );
    assert!(frames.iter().any(|frame| matches!(
        frame.record,
        WalRecord::ToolResult {
            outcome: DispatchOutcome::Interrupted,
            ..
        }
    )));
    assert_eq!(
        store
            .read_prefix_with_watermark(&session)
            .expect("read HasToolCall materialization")
            .watermark_seq,
        turn_finished_seq
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn append_after_materialized_hard_stop_matches_full_replay_bytes() {
    let base = broca_test_support::scratch_root().join(format!(
        "broca-hard-stop-append-bytes-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);

    let materialized_dir = base.join("materialized");
    let full_replay_dir = base.join("full-replay");
    std::fs::create_dir_all(&materialized_dir).unwrap();
    std::fs::create_dir_all(&full_replay_dir).unwrap();

    let materialized_store =
        Arc::new(SqliteStore::open(materialized_dir.join("store.db")).unwrap());
    let materialized_seed = RecordingProvider::new(vec![tool_turn("m-seed", "t-seed", "a")]);
    assert_eq!(
        run_request_with_store(
            &materialized_dir,
            hard_stop_request(&materialized_dir, StopCondition::StepCount { n: 1 }),
            &materialized_seed,
            &plane(),
            materialized_store.clone(),
        )
        .await,
        RunFinishReason::MaxSteps
    );

    let full_replay_seed = RecordingProvider::new(vec![tool_turn("m-seed", "t-seed", "a")]);
    assert_eq!(
        run_request_without_store(
            &full_replay_dir,
            hard_stop_request(&full_replay_dir, StopCondition::StepCount { n: 1 }),
            &full_replay_seed,
            &plane(),
        )
        .await,
        RunFinishReason::MaxSteps
    );
    let empty_store = Arc::new(SqliteStore::open(full_replay_dir.join("store.db")).unwrap());

    let materialized_provider = RecordingProvider::new(vec![text_turn("m-next", "done")]);
    let full_replay_provider = RecordingProvider::new(vec![text_turn("m-next", "done")]);
    assert_eq!(
        run_request_with_store(
            &materialized_dir,
            append_request(&materialized_dir),
            &materialized_provider,
            &plane(),
            materialized_store,
        )
        .await,
        RunFinishReason::Completed
    );
    assert_eq!(
        run_request_with_store(
            &full_replay_dir,
            append_request(&full_replay_dir),
            &full_replay_provider,
            &plane(),
            empty_store,
        )
        .await,
        RunFinishReason::Completed
    );
    assert_eq!(
        *materialized_provider.sent.lock().unwrap(),
        *full_replay_provider.sent.lock().unwrap(),
        "store-prefix plus WAL-tail renders the appended request byte-identically to full replay"
    );

    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn corrupt_store_snapshot_falls_back_to_full_replay() {
    let base = broca_test_support::scratch_root()
        .join(format!("broca-m10-corrupt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);

    let seed_dir = base.join("seed");
    std::fs::create_dir_all(&seed_dir).unwrap();
    let seed_reason =
        run_without_store(&seed_dir, &RecordingProvider::new(scripts()), &plane()).await;
    assert_eq!(seed_reason, RunFinishReason::Completed);
    let seed_frames = wal_frames(&seed_dir);
    let cut_len = seed_frames
        .iter()
        .position(|frame| {
            matches!(
                &frame.record,
                broca_wal::WalRecord::TurnFinished { step_id } if step_id.0 == 2
            )
        })
        .expect("seed WAL contains the second turn boundary")
        + 1;
    let cut_frames = seed_frames[..cut_len].to_vec();

    let reference_dir = base.join("reference");
    std::fs::create_dir_all(&reference_dir).unwrap();
    write_wal_frames(&reference_dir, &cut_frames);
    let reference_provider = RecordingProvider::new(scripts());
    let reference_reason = run_without_store(&reference_dir, &reference_provider, &plane()).await;
    assert_eq!(reference_reason, RunFinishReason::Completed);
    let reference_sent = reference_provider.sent.lock().unwrap().clone();
    assert!(
        !reference_sent.is_empty(),
        "the full-replay reference must issue at least one provider call"
    );

    let corrupt_dir = base.join("corrupt");
    std::fs::create_dir_all(&corrupt_dir).unwrap();
    write_wal_frames(&corrupt_dir, &cut_frames);
    let store = Arc::new(SqliteStore::open(corrupt_dir.join("store.db")).unwrap());
    let session = request(&corrupt_dir).session;
    Persister::project(store.as_ref(), &session, &cut_frames)
        .expect("project cut frames into store");
    corrupt_first_materialized_message(&corrupt_dir);
    MaterializedStore::read_prefix_with_watermark(store.as_ref(), &session)
        .expect_err("corrupted message JSON must reject the snapshot");

    let provider = RecordingProvider::new(scripts());
    let plane = plane();
    let reason = run_with_store(&corrupt_dir, &provider, &plane, store.clone()).await;
    assert_eq!(reason, RunFinishReason::Completed);
    let resumed_sent = provider.sent.lock().unwrap().clone();
    assert_eq!(
        resumed_sent, reference_sent,
        "a corrupted store snapshot must fall back to the same full-WAL replay prompt"
    );

    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test]
async fn budget_message_is_materialized_from_frozen_config() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-budget-materialized-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(SqliteStore::open(dir.join("store.db")).unwrap());
    let provider = RecordingProvider::new(scripts());
    let mut budgeted = request(&dir);
    budgeted.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![BudgetNudge {
            at_tool_calls: 2,
            text: "durable budget reminder".into(),
        }],
        finalize_at_tool_calls: 99,
        finalize_text: "finalize".into(),
    });

    let reason = run_request_with_store(&dir, budgeted, &provider, &plane(), store.clone()).await;
    assert_eq!(reason, RunFinishReason::Completed);
    let prefix = store
        .read_prefix_with_watermark(&request(&dir).session)
        .expect("read materialized budget prefix");
    assert!(prefix.prefix.iter().any(|message| {
        message.role == Role::User
            && message.content
                == vec![ContentBlock::Text {
                    text: "durable budget reminder".into(),
                }]
    }));
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn budgeted_tool_loop_projects_once_at_terminal_and_materializes_full_state() {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-budget-projection-count-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(CountingStore::new(Arc::new(
        SqliteStore::open(dir.join("store.db")).unwrap(),
    )));
    let provider = RecordingProvider::paced(scripts());
    let mut budgeted = request(&dir);
    budgeted.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![BudgetNudge {
            at_tool_calls: 2,
            text: "durable budget reminder".into(),
        }],
        finalize_at_tool_calls: 99,
        finalize_text: "finalize".into(),
    });
    let session = budgeted.session.clone();
    let expected_input = budgeted.input.clone();

    let reason = run_request_with_store(&dir, budgeted, &provider, &plane(), store.clone()).await;
    assert_eq!(reason, RunFinishReason::Completed);
    assert_eq!(
        store.projection_calls(),
        1,
        "two budgeted tool batches are coalesced into one terminal projection"
    );

    let prefix = store
        .read_prefix_with_watermark(&session)
        .expect("read fully materialized budget prefix");
    assert_eq!(prefix.completed_steps, 3);
    assert_eq!(prefix.prefix.len(), 7);
    assert!(prefix.prefix.iter().any(|message| {
        message.role == Role::User
            && message.content
                == vec![ContentBlock::Text {
                    text: "durable budget reminder".into(),
                }]
    }));

    let admission = store
        .read_admission_snapshot(&session)
        .expect("read terminal admission snapshot")
        .expect("terminal projection writes admission metadata");
    assert_eq!(admission.watermark_seq, prefix.watermark_seq);
    let episode = admission
        .checkpoint
        .expect("terminal projection writes an admission checkpoint")
        .last_episode
        .expect("checkpoint retains the projected episode");
    assert_eq!(episode.input, expected_input);
    assert!(episode.config.budget.is_some());

    let page = store
        .read_page_with_watermark(&session, Some(0), 100, broca_core::context::ReadView::Raw)
        .expect("read the materialized transcript page");
    let wal = FileWalStore::new(dir.join("wal"));
    let tail = wal
        .read_records_range(&session, page.watermark_seq, None, page.anchor.as_ref())
        .expect("read the terminal WAL tail");
    let assembled = broca_core::read_session_page(
        &page,
        match tail.records() {
            broca_wal::WalRangeRecords::Complete(records)
            | broca_wal::WalRangeRecords::Partial(records)
            | broca_wal::WalRangeRecords::Unknown(records) => records,
            broca_wal::WalRangeRecords::Absent => &[],
        },
        Some(0),
        Some(100),
    )
    .expect("assemble session.read from the terminal store state");
    assert_eq!(assembled.messages.len(), 7);
    assert_eq!(
        assembled.lineage_state.state,
        broca_core::SessionReadStateKind::Completed
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn non_budgeted_tool_loop_keeps_per_batch_projection_schedule() {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-non-budget-projection-count-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let gate = Arc::new(FirstProjectionGate::default());
    let release = ProjectionReleaseGuard(gate.clone());
    let mut store = CountingStore::new(Arc::new(SqliteStore::open(dir.join("store.db")).unwrap()));
    store.first_projection = Some(gate.clone());
    let store = Arc::new(store);
    let mut provider = RecordingProvider::new(scripts());
    provider.first_projection = Some(gate.clone());
    let req = request(&dir);
    let session = req.session.clone();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            if matches!(
                event,
                broca_core::OutputEvent::Control(
                    broca_protocol::caller::ControlUnit::RunFinished { .. }
                )
            ) {
                gate.release();
            }
        }
    });
    // On this single-threaded runtime, finish_run's send fits in the output channel and
    // the run body enqueues the terminal head before the collector can release the write.
    // Thus both the second batch and the terminal arrive while the first write is held.
    let summary = broca_test_support::event_timeout(
        "held projection run completes",
        run_loop_with_store(
            req,
            &provider,
            &plane(),
            &lease,
            &AllowAll,
            &wal,
            Some(store.clone() as Arc<dyn broca_core::SessionStore>),
            &tx,
        ),
        || format!("projection_calls={}", store.projection_calls()),
    )
    .await;
    drop(release);
    drop(tx);
    if summary.is_err() {
        collector.abort();
    } else {
        collector.await.unwrap();
    }
    let reason = summary
        .expect("held projection run completes")
        .expect("run ok")
        .reason;
    assert_eq!(reason, RunFinishReason::Completed);

    let records = wal.read_records(&session).expect("read completed WAL");
    let batch_boundaries: Vec<_> = records
        .iter()
        .filter_map(|frame| {
            matches!(frame.record, WalRecord::TurnFinished { .. }).then_some(frame.seq)
        })
        .collect();
    assert_eq!(
        batch_boundaries.len(),
        2,
        "the run commits two tool batches"
    );
    // Read the expected terminal boundary directly from the WAL, not through the
    // projection planner: no-tool steps become projectable only after RunFinished.
    let terminal_boundary = records
        .iter()
        .find_map(|frame| match &frame.record {
            WalRecord::ModelStepFinished {
                assistant_message, ..
            } if assistant_message.tool_calls().next().is_none() => Some(frame.seq),
            _ => None,
        })
        .expect("the final step has no tools");
    assert!(records
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::RunFinished { .. })));
    let watermarks = store.projected_watermarks.lock().unwrap().clone();
    assert_eq!(
        watermarks.first().copied(),
        Some(batch_boundaries[0]),
        "non-budgeted runs start projecting before the next batch"
    );
    // The watch channel retains the newest safe head, not one item per batch. A
    // projection may therefore cover several boundaries when a store write is slow.
    for boundary in batch_boundaries {
        assert!(
            watermarks.iter().any(|watermark| *watermark >= boundary),
            "tool batch at {boundary} must be covered by a projected watermark: {watermarks:?}"
        );
    }
    assert_eq!(
        watermarks.last().copied(),
        Some(terminal_boundary),
        "terminal projection must drain through the final no-tool step"
    );
    assert_eq!(
        store
            .read_prefix_with_watermark(&session)
            .unwrap()
            .watermark_seq,
        terminal_boundary,
        "the terminal watermark is persisted in SQLite"
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn paused_budgeted_run_projects_once_at_pause() {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-paused-budget-projection-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(CountingStore::new(Arc::new(
        SqliteStore::open(dir.join("store.db")).unwrap(),
    )));
    let provider = RecordingProvider::new(vec![vec![NormalizedEvent::Control(
        ProviderControl::Error {
            error: broca_protocol::ProviderError {
                provider_subcode: None,
                refusal: None,
                class: broca_protocol::ErrorClass::Auth,
                message: "credential expired".into(),
                status: Some(401),
                provider_code: None,
                retry_after_secs: None,
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
                affordable_max_tokens: None,
            },
            retries_used: 0,
        },
    )]]);
    let mut budgeted = request(&dir);
    budgeted.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![],
        finalize_at_tool_calls: 99,
        finalize_text: "finalize".into(),
    });
    let session = budgeted.session.clone();

    let reason = run_request_with_store(&dir, budgeted, &provider, &plane(), store.clone()).await;
    assert_eq!(reason, RunFinishReason::Interrupted);
    assert_eq!(store.projection_calls(), 1);
    let records = FileWalStore::new(dir.join("wal"))
        .read_records(&session)
        .expect("read paused budget WAL");
    assert!(records
        .iter()
        .any(|record| matches!(record.record, broca_wal::WalRecord::RunPaused { .. })));

    let _ = std::fs::remove_dir_all(dir);
}
