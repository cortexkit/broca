//! Cancel token + provider watchdog conformance (Phase 2, commit-4).
//!
//! Both controls stop the run COOPERATIVELY at a safe WAL boundary without stranding a tool_use:
//!  - a tripped cancel token records Cancelled at the next turn boundary;
//!  - a provider-call timeout (the watchdog) records Interrupted when a step never terminates;
//!  - a cancel while the run is PAUSED (awaiting re-auth) WINS — it finalizes the paused
//!    run as Cancelled rather than leaving it stuck awaiting auth;
//!  - with neither control set, behavior is byte-identical to a plain run_loop.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use broca_core::{
    replay, run_loop, run_loop_with_control, AllowAll, FileLeaseStore, FileWalStore,
    ReplayResolution, RunControl, RunRequest, StubProvider, StubToolPlane, WalStore,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role},
    error::{ErrorClass, ProviderError},
    provider::{NormalizedEvent, ProviderControl},
    request::{GenerationConfig, ModelHandle, StopCondition, ToolChoice},
    BindIdentity, CallOptions, FinishReason, MessageId, RunId, StepId, Usage,
};
use broca_wal::WalRecord;
use tokio::sync::{mpsc, Notify};
use tokio_util::sync::CancellationToken;

fn req(session: &BindIdentity, prompt: &str) -> RunRequest {
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
        run_id: RunId("r1".into()),
        session: session.clone(),
        model: ModelHandle::new("stub", "m"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: prompt.into(),
            }],
        }],
        tools: vec![],
        policy_allowed: Default::default(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        // Many steps allowed — the cancel, not stop_when, ends these runs.
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 50 }],
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

fn text_step(id: &str, text: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

fn auth_error_step() -> Vec<NormalizedEvent> {
    vec![NormalizedEvent::Control(ProviderControl::Error {
        error: ProviderError {
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Auth,
            message: "expired".into(),
            status: Some(401),
            retry_after_secs: None,
            provider_code: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
        },
        retries_used: 0,
    })]
}

fn dirs(
    tag: &str,
) -> (
    std::path::PathBuf,
    BindIdentity,
    FileLeaseStore,
    FileWalStore,
) {
    let dir =
        broca_test_support::scratch_root().join(format!("broca-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let session = BindIdentity::new(dir.join("proj"), "h", "s");
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    (dir, session, lease, wal)
}

fn records_of(wal: &FileWalStore, session: &BindIdentity) -> Vec<WalRecord> {
    WalStore::read_records(wal, session)
        .unwrap()
        .into_iter()
        .map(|f| f.record)
        .collect()
}

/// A cancel token tripped BEFORE the run starts stops it at the very first turn boundary:
/// Cancelled, zero model steps, no stranded state.
#[tokio::test]
async fn cancel_before_start_stops_at_first_boundary() {
    let (dir, session, lease, wal) = dirs("cancel-pre");
    let provider = StubProvider::new(vec![text_step("m1", "should not run")]);
    let plane = StubToolPlane::new();

    let token = CancellationToken::new();
    token.cancel(); // tripped before the loop
    let control = RunControl {
        cancel: Some(token),
        ..Default::default()
    };

    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let s = run_loop_with_control(
        req(&session, "hi"),
        &provider,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        control,
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();

    assert_eq!(
        s.reason,
        RunFinishReason::Cancelled,
        "a tripped cancel stops the run"
    );
    assert_eq!(s.steps, 0, "no model step ran");
    // The run is finalized (a RunFinished is durable) at a clean boundary.
    let recs = records_of(&wal, &session);
    assert!(
        recs.iter().any(|r| matches!(
            r,
            WalRecord::RunFinished {
                reason: RunFinishReason::Cancelled,
                ..
            }
        )),
        "the cancelled run is finalized as Cancelled at a safe boundary"
    );
    assert!(
        !recs
            .iter()
            .any(|r| matches!(r, WalRecord::ModelStepFinished { .. })),
        "no model step was recorded"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The provider watchdog: a step whose stream never terminates is timed out, and the run
/// stops cooperatively as Interrupted. Nothing durable was recorded for the timed-out
/// step (a later resume re-runs it).
#[tokio::test]
async fn watchdog_times_out_a_hung_step() {
    let (dir, session, lease, wal) = dirs("watchdog");
    // A provider whose step stream NEVER sends a terminal (hangs).
    let provider = HangingProvider::new();
    let plane = StubToolPlane::new();

    let control = RunControl {
        provider_call_timeout: Some(Duration::from_millis(150)),
        ..Default::default()
    };

    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let s = run_loop_with_control(
        req(&session, "hi"),
        &provider,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        control,
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();

    assert_eq!(
        s.reason,
        RunFinishReason::Interrupted,
        "a hung step is timed out and the run stops Interrupted"
    );
    provider.wait_finished().await;
    let recs = records_of(&wal, &session);
    assert!(
        recs.iter().any(|r| matches!(
            r,
            WalRecord::RunFinished {
                reason: RunFinishReason::Interrupted,
                ..
            }
        )),
        "the watchdog seals the run as Interrupted rather than Cancelled"
    );
    assert!(
        !recs
            .iter()
            .any(|r| matches!(r, WalRecord::ModelStepFinished { .. })),
        "the timed-out step recorded nothing durable"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn cancel_during_a_hung_step_aborts_provider_work() {
    let (dir, session, lease, wal) = dirs("cancel-hung-step");
    let provider = HangingProvider::new();
    let plane = StubToolPlane::new();
    let token = CancellationToken::new();
    let control = RunControl {
        cancel: Some(token.clone()),
        ..Default::default()
    };
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    let run = run_loop_with_control(
        req(&session, "hi"),
        &provider,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        control,
        &tx,
    );
    let cancel = async {
        // Generous bound: admission fsyncs run before the step, and a loaded CI
        // runner (Windows especially) can take seconds to reach it. On success the
        // wait returns as soon as the step starts, so the size costs nothing.
        tokio::time::timeout(Duration::from_secs(30), provider.started.notified())
            .await
            .expect("provider step did not start");
        token.cancel();
    };
    let (summary, ()) = tokio::join!(run, cancel);
    let summary = summary.unwrap();
    drop(tx);
    collector.await.unwrap();

    assert_eq!(summary.reason, RunFinishReason::Cancelled);
    provider.wait_finished().await;
    assert!(
        !records_of(&wal, &session)
            .iter()
            .any(|record| matches!(record, WalRecord::ModelStepFinished { .. })),
        "the cancelled step recorded nothing durable"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A cancel while the run is PAUSED (awaiting re-auth) WINS: the paused run is finalized
/// as Cancelled (a durable RunFinished) rather than left resumable.
#[tokio::test]
async fn cancel_while_paused_finalizes_cancelled() {
    let (dir, session, lease, wal) = dirs("cancel-paused");
    let plane = StubToolPlane::new();

    // 1. Pause the run with an auth error.
    let p1 = StubProvider::new(vec![auth_error_step()]);
    {
        let (tx, mut rx) = mpsc::channel(256);
        let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        run_loop(
            req(&session, "hi"),
            &p1,
            &plane,
            &lease,
            &AllowAll,
            &wal,
            &tx,
        )
        .await
        .unwrap();
        drop(tx);
        collector.await.unwrap();
    }
    // It's paused (replay → AuthRequired, no RunFinished yet).
    assert!(matches!(
        replay(&records_of(&wal, &session)).unwrap().resolution,
        ReplayResolution::AuthRequired { .. }
    ));

    // 2. Resume-after-auth BUT with a tripped cancel → cancel wins, finalize Cancelled.
    let token = CancellationToken::new();
    token.cancel();
    let control = RunControl {
        cancel: Some(token),
        ..Default::default()
    };
    let p2 = StubProvider::new(vec![text_step("m2", "should not run")]);
    // run_loop_with_control resumes (is_resume true since the WAL is non-empty); the
    // cancel-while-paused check fires before the auth-resolved continue.
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let s = run_loop_with_control(
        req(&session, "hi"),
        &p2,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        control,
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();

    assert_eq!(s.reason, RunFinishReason::Cancelled);
    // The paused run is now FINALIZED (a RunFinished is durable) — cancel sealed it.
    let recs = records_of(&wal, &session);
    assert!(
        recs.iter().any(|r| matches!(
            r,
            WalRecord::RunFinished {
                reason: RunFinishReason::Cancelled,
                ..
            }
        )),
        "cancel-while-paused finalizes the run as Cancelled, not left paused"
    );
    let st = replay(&recs).unwrap();
    assert!(
        matches!(st.resolution, ReplayResolution::LineageIdle { .. }),
        "after a cancel the run is terminal/idle, no longer AuthRequired"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// With neither control set, run_loop_with_control is byte-identical to a plain
/// run_loop: the run completes normally.
#[tokio::test]
async fn no_controls_is_a_normal_run() {
    let (dir, session, lease, wal) = dirs("no-control");
    let provider = StubProvider::new(vec![text_step("m1", "the answer")]);
    let plane = StubToolPlane::new();

    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let s = run_loop_with_control(
        req(&session, "hi"),
        &provider,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        None,
        RunControl::default(),
        &tx,
    )
    .await
    .unwrap();
    drop(tx);
    collector.await.unwrap();
    assert_eq!(
        s.reason,
        RunFinishReason::Completed,
        "no controls → a normal completed run"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ───────────────────────── a provider whose step hangs ─────────────────────────

use async_trait::async_trait;

/// A provider whose step stream never sends a terminal — used to exercise cancellation.
struct HangingProvider {
    started: Arc<Notify>,
    finished: Arc<AtomicBool>,
}

impl HangingProvider {
    fn new() -> Self {
        Self {
            started: Arc::new(Notify::new()),
            finished: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn wait_finished(&self) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !self.finished.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("provider task remained alive after step cancellation");
    }
}

#[async_trait]
impl broca_core::provider::Provider for HangingProvider {
    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.step_with_cancel(step_id, opts, CancellationToken::new(), None)
            .await
    }

    async fn step_with_cancel(
        &self,
        _step_id: StepId,
        _opts: &CallOptions,
        cancel: CancellationToken,
        _deadline: Option<tokio::time::Instant>,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let (tx, rx) = mpsc::channel(1);
        let started = self.started.clone();
        let finished = self.finished.clone();
        tokio::spawn(async move {
            started.notify_one();
            cancel.cancelled().await;
            drop(tx);
            finished.store(true, Ordering::SeqCst);
        });
        rx
    }
}
