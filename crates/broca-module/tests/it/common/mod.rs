//! Shared test rig for the coordinator races: a CONTROLLABLE fake executor + an
//! in-process registry wired to real (temp-dir) lease + WAL stores.
//!
//! The fake executor is the whole point: the Oracle's interleavings (close-while-X-races,
//! double-reopen, send-during-Closing) cannot be driven deterministically against a real
//! LLM loop. The fake lets a test hold a run OPEN until it signals finish, count
//! executions, and make a run finish with a chosen outcome — so the actor + registry
//! concurrency is exercised reproducibly.
//!
//! This module is shared across several test binaries; not every binary uses every helper,
//! so dead-code is expected per-binary (the union is used).
#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use broca_core::{FileLeaseStore, FileWalStore, OutputEvent, RunError, RunRequest, RunSummary};
use broca_module::executor::RunKind;
use broca_module::executor::{RunExecutor, RunJob};
use broca_module::registry::LineageRegistry;
use broca_module::DefaultOpener;
use broca_protocol::{
    caller::{PauseReason, RunFinishReason},
    BindIdentity, Usage,
};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

/// A fake executor whose runs block until released, so a test can hold a run active while
/// other ops race the actor.
#[derive(Default)]
pub struct FakeExecutor {
    /// Total runs started.
    pub started: AtomicUsize,
    /// Total runs finished.
    pub finished: AtomicUsize,
    /// A gate: each started run waits on a receiver the test feeds to release it.
    gate: Mutex<Option<mpsc::Receiver<()>>>,
    /// `true` ⇒ runs finish immediately (no gating); `false` ⇒ runs block on the gate.
    auto_finish: bool,
    /// `true` ⇒ the run PANICS after recording RunStarted (to exercise the run-task
    /// drop-guard: the actor must still learn the run ended via a synthetic terminal).
    panic_after_start: bool,
    /// Optional scripted outcomes for tests that need specific WAL/index transitions.
    scripted: Mutex<VecDeque<RunScript>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunScript {
    FreshCompleted,
    FreshCompletedAfterGate,
    FreshPanicsAfterGate,
    FreshPaused,
    FreshPausedAfterGate,
    ResumeCancelled,
    ResumeCompleted,
    ResumeMaxSteps,
    ResumeInterrupted,
    AdmissionPermanent,
    AdmissionTransient,
}

impl FakeExecutor {
    /// Runs finish immediately — for tests that don't need to hold a run open.
    pub fn auto() -> Arc<Self> {
        Arc::new(Self {
            auto_finish: true,
            ..Default::default()
        })
    }

    /// Runs BLOCK until `release()` is called — for tests that hold a run active while
    /// racing the actor.
    pub fn gated() -> (Arc<Self>, RunGate) {
        let (tx, rx) = mpsc::channel(64);
        let me = Arc::new(Self {
            auto_finish: false,
            gate: Mutex::new(Some(rx)),
            ..Default::default()
        });
        (me, RunGate { tx })
    }

    /// Runs PANIC after recording RunStarted — to prove the run-task drop-guard posts a
    /// synthetic terminal so the actor leaves Active even when a run unwinds.
    pub fn panicking() -> Arc<Self> {
        Arc::new(Self {
            panic_after_start: true,
            ..Default::default()
        })
    }

    /// Run a scripted sequence of outcomes. Used when a test needs a specific durable state
    /// transition (for example Paused → Completed on resume) rather than just "finish".
    pub fn scripted(scripts: Vec<RunScript>) -> Arc<Self> {
        Arc::new(Self {
            scripted: Mutex::new(VecDeque::from(scripts)),
            ..Default::default()
        })
    }

    pub fn scripted_gated(scripts: Vec<RunScript>) -> (Arc<Self>, RunGate) {
        let (tx, rx) = mpsc::channel(64);
        let executor = Arc::new(Self {
            gate: Mutex::new(Some(rx)),
            scripted: Mutex::new(VecDeque::from(scripts)),
            ..Default::default()
        });
        (executor, RunGate { tx })
    }

    pub fn started_count(&self) -> usize {
        self.started.load(Ordering::SeqCst)
    }
    pub fn finished_count(&self) -> usize {
        self.finished.load(Ordering::SeqCst)
    }
}

/// Releases gated runs (one per `release()`).
pub struct RunGate {
    tx: mpsc::Sender<()>,
}

impl RunGate {
    pub async fn release(&self) {
        let _ = self.tx.send(()).await;
    }
}

impl FakeExecutor {
    async fn execute_script(
        &self,
        script: RunScript,
        mut job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        match script {
            RunScript::FreshCompleted => {
                self.write_started(&job).await;
                if let Some(ack) = job.on_admitted.take() {
                    let _ = ack.send(Ok(job.req.run_id.clone()));
                }
                job.writer.record(broca_wal::WalRecord::RunFinished {
                    ts_ms: None,
                    reason: RunFinishReason::Completed,
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                });
                job.writer.sync().await?;
                self.finished.fetch_add(1, Ordering::SeqCst);
                Ok(RunSummary {
                    reason: RunFinishReason::Completed,
                    paused: None,
                    steps: 1,
                    usage: Usage::default(),
                    retries_used: 0,
                    final_step_finish_reason: None,
                })
            }
            RunScript::FreshCompletedAfterGate => {
                self.write_started(&job).await;
                if let Some(ack) = job.on_admitted.take() {
                    let _ = ack.send(Ok(job.req.run_id.clone()));
                }
                if let Some(rx) = self.gate.lock().await.as_mut() {
                    let _ = rx.recv().await;
                }
                job.writer.record(broca_wal::WalRecord::RunFinished {
                    ts_ms: None,
                    reason: RunFinishReason::Completed,
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                });
                job.writer.sync().await?;
                self.finished.fetch_add(1, Ordering::SeqCst);
                Ok(RunSummary {
                    reason: RunFinishReason::Completed,
                    paused: None,
                    steps: 1,
                    usage: Usage::default(),
                    retries_used: 0,
                    final_step_finish_reason: None,
                })
            }
            RunScript::FreshPanicsAfterGate => {
                self.write_started(&job).await;
                if let Some(ack) = job.on_admitted.take() {
                    let _ = ack.send(Ok(job.req.run_id.clone()));
                }
                if let Some(rx) = self.gate.lock().await.as_mut() {
                    let _ = rx.recv().await;
                }
                panic!("fake executor: simulated post-admission run-loop panic");
            }
            RunScript::FreshPaused => {
                self.write_started(&job).await;
                if let Some(ack) = job.on_admitted.take() {
                    let _ = ack.send(Ok(job.req.run_id.clone()));
                }
                job.writer.record(broca_wal::WalRecord::RunPaused {
                    ts_ms: None,
                    run_id: job.req.run_id.clone(),
                    reason: PauseReason::AuthRequired,
                    error: None,
                });
                let _ = job.writer.sync().await;
                self.finished.fetch_add(1, Ordering::SeqCst);
                Ok(RunSummary {
                    reason: RunFinishReason::Interrupted,
                    paused: Some(PauseReason::AuthRequired),
                    steps: 0,
                    usage: Usage::default(),
                    retries_used: 0,
                    final_step_finish_reason: None,
                })
            }
            RunScript::FreshPausedAfterGate => {
                self.write_started(&job).await;
                if let Some(ack) = job.on_admitted.take() {
                    let _ = ack.send(Ok(job.req.run_id.clone()));
                }
                if let Some(rx) = self.gate.lock().await.as_mut() {
                    let _ = rx.recv().await;
                }
                job.writer.record(broca_wal::WalRecord::RunPaused {
                    ts_ms: None,
                    run_id: job.req.run_id.clone(),
                    reason: PauseReason::AuthRequired,
                    error: None,
                });
                let _ = job.writer.sync().await;
                self.finished.fetch_add(1, Ordering::SeqCst);
                Ok(RunSummary {
                    reason: RunFinishReason::Interrupted,
                    paused: Some(PauseReason::AuthRequired),
                    steps: 0,
                    usage: Usage::default(),
                    retries_used: 0,
                    final_step_finish_reason: None,
                })
            }
            RunScript::ResumeCancelled => {
                assert_eq!(job.kind, RunKind::FreshOrResume);
                assert!(
                    job.control
                        .cancel
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancelled),
                    "paused cancel must enter the resume executor with a pre-cancelled token"
                );
                if let Some(ack) = job.on_admitted.take() {
                    let _ = ack.send(Ok(job.req.run_id.clone()));
                }
                job.writer.record(broca_wal::WalRecord::RunFinished {
                    ts_ms: None,
                    reason: RunFinishReason::Cancelled,
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                });
                let _ = job.writer.sync().await;
                self.finished.fetch_add(1, Ordering::SeqCst);
                Ok(RunSummary {
                    reason: RunFinishReason::Cancelled,
                    paused: None,
                    steps: 0,
                    usage: Usage::default(),
                    retries_used: 0,
                    final_step_finish_reason: None,
                })
            }
            RunScript::ResumeCompleted => {
                assert!(
                    !job.control
                        .cancel
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancelled),
                    "auth-resolved resume must not inherit cancellation"
                );
                if let Some(ack) = job.on_admitted.take() {
                    let _ = ack.send(Ok(job.req.run_id.clone()));
                }
                job.writer.record(broca_wal::WalRecord::RunFinished {
                    ts_ms: None,
                    reason: RunFinishReason::Completed,
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                });
                let _ = job.writer.sync().await;
                self.finished.fetch_add(1, Ordering::SeqCst);
                Ok(RunSummary {
                    reason: RunFinishReason::Completed,
                    paused: None,
                    steps: 1,
                    usage: Usage::default(),
                    retries_used: 0,
                    final_step_finish_reason: None,
                })
            }
            RunScript::ResumeMaxSteps => {
                if let Some(ack) = job.on_admitted.take() {
                    let _ = ack.send(Ok(job.req.run_id.clone()));
                }
                job.writer.record(broca_wal::WalRecord::RunFinished {
                    ts_ms: None,
                    reason: RunFinishReason::MaxSteps,
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                });
                let _ = job.writer.sync().await;
                self.finished.fetch_add(1, Ordering::SeqCst);
                Ok(RunSummary {
                    reason: RunFinishReason::MaxSteps,
                    paused: None,
                    steps: 1,
                    usage: Usage::default(),
                    retries_used: 0,
                    final_step_finish_reason: None,
                })
            }
            RunScript::ResumeInterrupted => {
                if let Some(ack) = job.on_admitted.take() {
                    let _ = ack.send(Ok(job.req.run_id.clone()));
                }
                job.writer.record(broca_wal::WalRecord::RunFinished {
                    ts_ms: None,
                    reason: RunFinishReason::Interrupted,
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                });
                let _ = job.writer.sync().await;
                self.finished.fetch_add(1, Ordering::SeqCst);
                Ok(RunSummary {
                    reason: RunFinishReason::Interrupted,
                    paused: None,
                    steps: 1,
                    usage: Usage::default(),
                    retries_used: 0,
                    final_step_finish_reason: None,
                })
            }
            RunScript::AdmissionPermanent => {
                let error = RunError::Config("unknown model in queued request".into());
                if let Some(ack) = job.on_admitted.take() {
                    let _ = ack.send(Err(error.clone()));
                }
                Err(error)
            }
            RunScript::AdmissionTransient => {
                let error = RunError::Transient("provider catalog temporarily unavailable".into());
                if let Some(ack) = job.on_admitted.take() {
                    let _ = ack.send(Err(error.clone()));
                }
                Err(error)
            }
        }
    }

    async fn write_started(&self, job: &RunJob) {
        let _seq = job.writer.record(broca_wal::WalRecord::RunStarted {
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
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
            run_id: job.req.run_id.clone(),
            session: job.req.session.clone(),
            config: Box::new(broca_wal::RunConfig {
                tools: job.req.tools.clone().into(),
                tools_were_omitted: job.control.tools_were_omitted,
                ..min_config()
            }),
            input: job.req.input.clone().into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: job.req.submission_id.clone(),
        });
        let _ = job.writer.sync().await;
    }
}

#[async_trait]
impl RunExecutor for FakeExecutor {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        if let Some(script) = self.scripted.lock().await.pop_front() {
            return self.execute_script(script, job, out).await;
        }
        // Write a RunStarted (so the WAL is non-empty, exercising the durability path).
        // Honor job.req.submission_id like the real loop does — so a run consuming a queued
        // submission writes the consume-link (RunStarted{submission_id}), marking it
        // consumed when the pending queue is derived from the WAL.
        self.write_started(&job).await;
        let _ = out; // fan-out is commit-2

        // Fire admission right after the RunStarted barrier (mirroring the real loop), so
        // start_run unblocks and replies Active. A panic AFTER this models a MID-run panic
        // (the run admitted, then crashed) — the realistic case the drop-guard recovers.
        if let Some(ack) = job.on_admitted {
            let _ = ack.send(Ok(job.req.run_id.clone()));
        }

        if self.panic_after_start {
            // Unwind at this await point, BEFORE the run-driving task's normal finish —
            // the drop-guard must still post a terminal so the actor isn't wedged Active.
            panic!("fake executor: simulated run-loop panic");
        }

        if !self.auto_finish {
            // Block until released (or the gate is drained).
            if let Some(rx) = self.gate.lock().await.as_mut() {
                let _ = rx.recv().await;
            }
        }
        job.writer.record(broca_wal::WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        });
        job.writer.sync().await?;
        self.finished.fetch_add(1, Ordering::SeqCst);
        Ok(RunSummary {
            reason: RunFinishReason::Completed,
            paused: None,
            steps: 1,
            usage: Usage::default(),
            retries_used: 0,
            final_step_finish_reason: None,
        })
    }
}

fn min_config() -> broca_wal::RunConfig {
    broca_wal::RunConfig {
        render_tags: false,
        model: broca_protocol::request::ModelHandle::new("fake", "fake"),
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
        stop_when: Vec::new(),
        context_management: None,
    }
}

/// A test rig: a unique temp dir, a registry wired to real file lease + WAL stores and a
/// fake executor, with a short idle TTL so close races are quick to trigger.
pub struct Rig {
    pub dir: std::path::PathBuf,
    pub registry: LineageRegistry,
    pub exec: Arc<FakeExecutor>,
    /// The durable run index the actors write to (shared via the opener), so a test can
    /// inspect it OR simulate a crash (write a WAL terminal without the index terminal).
    pub run_index: Arc<broca_store::SqliteStore>,
}

impl Rig {
    pub fn new(tag: &str, exec: Arc<FakeExecutor>, idle_ttl: std::time::Duration) -> Self {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-coord-{}-{}-{}",
            std::process::id(),
            tag,
            unique()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        let lease_store = Arc::new(FileLeaseStore::new(dir.join("leases")));
        let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
        // A SQLite run index shared by all actors this rig opens (a real durable index).
        let run_index = Arc::new(
            broca_store::SqliteStore::open(dir.join("store.db")).expect("open run-index store"),
        );
        let charge_basis_resolver: Arc<dyn broca_core::ChargeBasisResolver + Send + Sync> =
            Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1));
        let opener = Arc::new(
            DefaultOpener::new(lease_store, wal_store.clone(), exec.clone())
                .with_idle_ttl(idle_ttl)
                .with_run_index(run_index.clone())
                .with_export_index(run_index.clone())
                .with_charge_basis_resolver(charge_basis_resolver),
        );
        let registry = LineageRegistry::new_with_reader(opener, wal_store, Some(run_index.clone()));
        Rig {
            dir,
            registry,
            exec,
            run_index,
        }
    }

    pub fn key(&self, session: &str) -> BindIdentity {
        BindIdentity::new(self.dir.join("proj"), "runner", session)
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn unique() -> u64 {
    use std::sync::atomic::AtomicU64;
    static N: AtomicU64 = AtomicU64::new(0);
    N.fetch_add(1, Ordering::SeqCst)
}

/// A simple text run request.
pub fn req(key: &BindIdentity, run: &str) -> RunRequest {
    use broca_protocol::content::{ContentBlock, Message, Role};
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
        run_id: broca_protocol::RunId(run.into()),
        session: key.clone(),
        model: broca_protocol::request::ModelHandle::new("fake", "fake"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text { text: "hi".into() }],
        }],
        tools: vec![],
        policy_allowed: Default::default(),
        tool_choice: Default::default(),
        generation: Default::default(),
        cache: None,
        stop_when: vec![],
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
