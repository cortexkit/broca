//! A module that is stopping lets a run finish the step it is in, and starts nothing new.
//!
//! These drive the real run loop under the registry's real stop path. The registry first asks
//! every run to stop, waits a grace period for runs to leave on their own, then seals whatever
//! is still running. A run asked to stop finishes the tool call it has in flight, records the
//! result, and ends `Interrupted` before the next model call, so the seal finds nothing to cut.
//! Only a call that outlasts the grace is still cut and left `outcome_unknown`.

use super::tests::text_req;
use super::*;
use crate::opener::DefaultOpener;
use crate::registry::LineageRegistry;
use broca_core::resume::OUTCOME_UNKNOWN_TEXT;
use broca_core::{
    run_loop_on_writer_admitted_with_prior_snapshot, AllowAll, BatchCtx, DispatchResult,
    FileLeaseStore, FileWalStore, StartMode, StubProvider, ToolManifest, ToolPlane, ToolPlaneError,
};
use broca_protocol::content::{AssistantMessage, ContentBlock, ToolCall, ToolOutput};
use broca_protocol::provider::{NormalizedEvent, ProviderControl};
use broca_protocol::request::{ExecutionMode, Tool};
use broca_protocol::{
    BindIdentity, FinishReason, MessageId, RunFinishReason, ToolCallId, ToolResult,
};
use broca_wal::DispatchOutcome;
use std::sync::atomic::AtomicUsize;
use tokio::sync::Notify;

/// The production grace and total, so a run that leaves on its own is seen to leave well
/// before the grace would have run out.
const GRACE: Duration = Duration::from_secs(10);
const TOTAL: Duration = Duration::from_secs(20);

fn step(content: Vec<ContentBlock>, finish: FinishReason) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m".into()),
                content,
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Default::default(),
            finish_reason: finish,
            retries_used: 0,
        }),
    ]
}

fn edit_step() -> Vec<NormalizedEvent> {
    step(
        vec![ContentBlock::ToolCall(ToolCall {
            thought_signature: None,
            tool_call_id: ToolCallId("t1".into()),
            tool_name: "edit".into(),
            input: serde_json::json!({"f": "a"}),
        })],
        FinishReason::ToolCalls,
    )
}

fn text_step(text: &str) -> Vec<NormalizedEvent> {
    step(
        vec![ContentBlock::Text { text: text.into() }],
        FinishReason::Stop,
    )
}

/// How long the tool plane holds each call it is sent.
#[derive(Clone, Copy)]
enum Hold {
    /// Until the test releases it.
    UntilReleased,
    /// Hold the call, then hold the durable batch before the next step-entry boundary.
    UntilReleasedAtBoundary,
    /// Forever: the call outlasts any grace.
    Forever,
}

/// A tool plane whose calls run until released, and that counts the calls it is sent.
struct HoldingPlane {
    hold: Hold,
    entered: Notify,
    release: Notify,
    batch_committed: Notify,
    leave_batch: Notify,
    dispatched: AtomicUsize,
}

#[async_trait::async_trait]
impl ToolPlane for HoldingPlane {
    async fn batch_started(
        &self,
        _ctx: &BatchCtx,
        _calls: &[ToolCall],
        _manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }

    async fn dispatch(&self, _ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        self.dispatched.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        match self.hold {
            Hold::UntilReleased | Hold::UntilReleasedAtBoundary => self.release.notified().await,
            Hold::Forever => std::future::pending().await,
        }
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Text {
                text: "edited".into(),
            },
            is_error: false,
        })
    }

    async fn batch_finished(&self, _ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        // The run loop calls this after syncing the tool results and TurnFinished. Holding
        // here lets a test order the stop against a durable result, not a dispatch estimate.
        self.batch_committed.notify_one();
        if matches!(self.hold, Hold::UntilReleasedAtBoundary) {
            self.leave_batch.notified().await;
        }
        Ok(())
    }
}

struct LoopExecutor {
    provider: StubProvider,
    plane: Arc<HoldingPlane>,
    wal_store: Arc<FileWalStore>,
}

#[async_trait::async_trait]
impl RunExecutor for LoopExecutor {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, broca_core::RunError> {
        let mode = match job.kind {
            RunKind::FreshOrResume => StartMode::FreshOrResume,
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
        };
        run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            job.writer.as_ref(),
            mode,
            &self.provider,
            self.plane.as_ref(),
            &AllowAll,
            self.wal_store.as_ref(),
            None,
            job.control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        )
        .await
    }
}

struct Fixture {
    registry: LineageRegistry,
    key: BindIdentity,
    root: std::path::PathBuf,
    provider: StubProvider,
    plane: Arc<HoldingPlane>,
    /// Whether dropping this fixture removes the state directory. A fixture reopened over
    /// another's state leaves it to the first.
    owns_root: bool,
}

impl Fixture {
    fn new(hold: Hold, scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "broca-graceful-stop-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(root.join("project")).unwrap();
        Self::over(root, hold, scripts, true)
    }

    /// A second process's registry over the same state directory.
    fn reopen(&self, scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        Self::over(self.root.clone(), Hold::UntilReleased, scripts, false)
    }

    fn over(
        root: std::path::PathBuf,
        hold: Hold,
        scripts: Vec<Vec<NormalizedEvent>>,
        owns_root: bool,
    ) -> Self {
        let key = BindIdentity::new(root.join("project"), "runner", "session");
        let wal_store = Arc::new(FileWalStore::new(root.join("wal")));
        let provider = StubProvider::new(scripts);
        let plane = Arc::new(HoldingPlane {
            hold,
            entered: Notify::new(),
            release: Notify::new(),
            batch_committed: Notify::new(),
            leave_batch: Notify::new(),
            dispatched: AtomicUsize::new(0),
        });
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(root.join("leases"))),
            wal_store.clone(),
            Arc::new(LoopExecutor {
                provider: provider.clone(),
                plane: Arc::clone(&plane),
                wal_store,
            }),
        );
        Self {
            registry: LineageRegistry::new(Arc::new(opener)),
            key,
            root,
            provider,
            plane,
            owns_root,
        }
    }

    /// A request whose run may call the `edit` tool.
    fn edit_req(&self, run: &str) -> RunRequest {
        let mut req = text_req(&self.key, run);
        req.tools = vec![Tool {
            name: "edit".into(),
            description: "edit a file".into(),
            input_schema: serde_json::json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Parallel,
            reply: None,
        }];
        req.policy_allowed = ["edit".to_string()].into_iter().collect();
        req
    }

    async fn start_and_wait_for_the_tool_call(&self) {
        assert!(matches!(
            self.registry
                .route_send(
                    &self.key,
                    self.edit_req("graceful-run"),
                    RunKind::FreshOrResume
                )
                .await
                .unwrap(),
            SendOutcome::Active { .. }
        ));
        tokio::time::timeout(Duration::from_secs(10), self.plane.entered.notified())
            .await
            .expect("the run reaches its tool call");
    }

    /// Stop through the registry while releasing the held call once the stop is requested.
    /// Returns how long the stop took.
    async fn stop_releasing_the_call(&self, grace: Duration) -> Duration {
        let started = tokio::time::Instant::now();
        // `join!` polls the stop first, and its first poll requests the stop before it waits;
        // only then is the running call released, so the call finishes after the request.
        let (sealed, ()) = tokio::join!(
            self.registry.stop_and_seal_with_budget(grace, TOTAL),
            async { self.plane.release.notify_one() }
        );
        assert!(
            sealed,
            "{}",
            stop_failure_message(started.elapsed(), &self.registry)
        );
        assert_eq!(self.registry.slot_counts().open, 0);
        started.elapsed()
    }

    fn records(&self) -> Vec<WalRecord> {
        FileWalStore::new(self.root.join("wal"))
            .read_records(&self.key)
            .unwrap()
            .into_iter()
            .map(|f| f.record)
            .collect()
    }
}

fn stop_failure_message(elapsed: Duration, registry: &LineageRegistry) -> String {
    format!(
        "the stop completes: elapsed={elapsed:?}; {}",
        registry
            .shutdown_report()
            .expect("stop records its outcome")
    )
}

#[tokio::test]
async fn stop_deadline_diagnostics_name_the_remaining_session() {
    let f = Fixture::new(Hold::Forever, vec![edit_step()]);
    f.start_and_wait_for_the_tool_call().await;
    // A zero total reaches the real deadline return before the actor can handle the seal.
    let started = tokio::time::Instant::now();
    assert!(
        !f.registry
            .stop_and_seal_with_budget(Duration::ZERO, Duration::ZERO)
            .await
    );
    let message = stop_failure_message(started.elapsed(), &f.registry);
    assert!(
        message.contains("registry total shutdown deadline expired"),
        "{message}"
    );
    assert!(message.contains("elapsed="), "{message}");
    assert!(message.contains("budget=0ns"), "{message}");
    assert!(
        message.contains(&format!("{:?}", f.key.session)),
        "{message}"
    );
    assert!(
        !message.contains(&f.root.to_string_lossy().to_string()),
        "{message}"
    );
    assert!(message.contains("\"open\""), "{message}");
    assert!(message.contains("current_failures=[]"), "{message}");
    assert!(f.registry.stop_and_seal().await);
    println!("{message}");
}

#[tokio::test]
async fn stop_seal_failure_diagnostics_include_the_session_and_error() {
    use std::io::Write;

    let f = Fixture::new(Hold::Forever, vec![edit_step()]);
    f.start_and_wait_for_the_tool_call().await;
    // Damage only this fixture's WAL while the run is held. The real seal must report its
    // failed read/write barrier rather than present it as a registry deadline expiring.
    let path = FileWalStore::new(f.root.join("wal")).wal_path(&f.key);
    std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .unwrap()
        .write_all(b"foreign-write")
        .unwrap();
    let started = tokio::time::Instant::now();
    assert!(
        !f.registry
            .stop_and_seal_with_budget(Duration::ZERO, TOTAL)
            .await
    );
    let message = stop_failure_message(started.elapsed(), &f.registry);
    assert!(
        message.contains("fresh final-sync or writer shutdown failure"),
        "{message}"
    );
    assert!(message.contains("elapsed="), "{message}");
    assert!(message.contains("budget=20s"), "{message}");
    assert!(message.contains("bound=5s"), "{message}");
    assert!(message.contains("remaining_sessions=[]"), "{message}");
    assert!(
        message.contains(&format!("{:?}", f.key.session)),
        "{message}"
    );
    assert!(message.contains("foreign writer"), "{message}");
    println!("{message}");
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if self.owns_root {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

fn terminals(records: &[WalRecord]) -> Vec<RunFinishReason> {
    records
        .iter()
        .filter_map(|r| match r {
            WalRecord::RunFinished { reason, .. } => Some(*reason),
            _ => None,
        })
        .collect()
}

fn steps_started(records: &[WalRecord]) -> usize {
    records
        .iter()
        .filter(|r| matches!(r, WalRecord::StepStarted { .. }))
        .count()
}

fn tool_results(records: &[WalRecord]) -> Vec<(String, DispatchOutcome, String)> {
    records
        .iter()
        .filter_map(|r| match r {
            WalRecord::ToolResult {
                result, outcome, ..
            } => {
                let text = match &result.output {
                    ToolOutput::Text { text } => text.clone(),
                    ToolOutput::Error { message } => message.clone(),
                    other => format!("{other:?}"),
                };
                Some((result.tool_call_id.0.clone(), *outcome, text))
            }
            _ => None,
        })
        .collect()
}

fn run_started_ids(records: &[WalRecord]) -> Vec<String> {
    records
        .iter()
        .filter_map(|r| match r {
            WalRecord::RunStarted { run_id, .. } => Some(run_id.0.clone()),
            _ => None,
        })
        .collect()
}

/// The stop arrives while the run's tool call is running. The call finishes within the grace
/// and its result is recorded `completed`; no further step or model call starts; the run itself
/// ends `Interrupted`, so the stop finishes long before the grace runs out.
#[tokio::test]
async fn a_stop_during_a_tool_call_lets_it_finish_and_starts_no_next_step() {
    let f = Fixture::new(Hold::UntilReleased, vec![edit_step(), text_step("done")]);
    f.start_and_wait_for_the_tool_call().await;

    let elapsed = f.stop_releasing_the_call(GRACE).await;

    assert!(
        elapsed < Duration::from_secs(5),
        "the run left on its own instead of waiting out the {GRACE:?} grace: took {elapsed:?}"
    );
    let records = f.records();
    assert_eq!(f.plane.dispatched.load(Ordering::SeqCst), 1);
    assert_eq!(
        tool_results(&records),
        vec![(
            "t1".to_string(),
            DispatchOutcome::Completed,
            "edited".to_string()
        )],
        "the call in flight finished and its result was recorded; nothing is outcome_unknown"
    );
    assert_eq!(steps_started(&records), 1, "no step starts after the stop");
    assert_eq!(
        f.provider.calls().len(),
        1,
        "no model call starts after the stop"
    );
    assert_eq!(
        terminals(&records),
        vec![RunFinishReason::Interrupted],
        "one Interrupted terminal, never Cancelled"
    );
    assert!(
        !records
            .iter()
            .any(|r| matches!(r, WalRecord::ModelAttemptFinished { .. })),
        "the seal found no attempt in flight to cut"
    );
}

/// A call that is still running when the grace runs out is cut by the seal exactly as before:
/// its result is `outcome_unknown` and the run ends `Interrupted`.
#[tokio::test]
async fn a_tool_call_that_outlasts_the_grace_is_sealed_outcome_unknown() {
    let f = Fixture::new(Hold::Forever, vec![edit_step(), text_step("done")]);
    f.start_and_wait_for_the_tool_call().await;

    f.stop_releasing_the_call(Duration::from_millis(200)).await;

    let records = f.records();
    assert_eq!(
        tool_results(&records),
        vec![(
            "t1".to_string(),
            DispatchOutcome::Unknown,
            OUTCOME_UNKNOWN_TEXT.to_string()
        )]
    );
    assert_eq!(steps_started(&records), 1);
    assert_eq!(terminals(&records), vec![RunFinishReason::Interrupted]);
}

/// A send queued behind the running run does not start when the stop ends that run. It stays
/// durable in the WAL, and the next process's registry drains it on open.
#[tokio::test]
async fn a_queued_send_waits_in_the_wal_for_the_next_process() {
    let f = Fixture::new(Hold::UntilReleased, vec![edit_step(), text_step("done")]);
    f.start_and_wait_for_the_tool_call().await;
    assert!(matches!(
        f.registry
            .route_send(
                &f.key,
                text_req(&f.key, "queued-run"),
                RunKind::FreshOrResume
            )
            .await
            .unwrap(),
        SendOutcome::Pending { .. }
    ));

    f.stop_releasing_the_call(GRACE).await;

    let records = f.records();
    assert_eq!(
        records
            .iter()
            .filter(|r| matches!(r, WalRecord::Queued { .. }))
            .count(),
        1,
        "the queued send is durable"
    );
    assert_eq!(
        run_started_ids(&records),
        vec!["graceful-run"],
        "the queued send did not start when the stopped run ended"
    );
    assert_eq!(terminals(&records), vec![RunFinishReason::Interrupted]);
    assert_eq!(f.provider.calls().len(), 1);

    // The next process: opening the lineage drains the queued send before anything new.
    let next = f.reopen(vec![text_step("queued done"), text_step("later done")]);
    let outcome = next
        .registry
        .route_send(
            &next.key,
            text_req(&next.key, "after-restart"),
            RunKind::FreshOrResume,
        )
        .await
        .unwrap();
    assert!(
        matches!(outcome, SendOutcome::Pending { .. }),
        "the recovered queued send runs first: {outcome:?}"
    );
    broca_test_support::wait_for(
        "queued run completes after reopen",
        async {
            loop {
                let records = next.records();
                if terminals(&records).len() >= 2 {
                    assert_eq!(
                        &run_started_ids(&records)[..2],
                        ["graceful-run", "queued-run"],
                        "the queued send is the next run"
                    );
                    assert_eq!(
                        &terminals(&records)[..2],
                        [RunFinishReason::Interrupted, RunFinishReason::Completed]
                    );
                    break;
                }

                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
        || broca_test_support::record_kinds(next.records()),
    )
    .await;
    assert!(
        next.registry
            .stop_and_seal_with_budget(Duration::from_secs(5), TOTAL)
            .await
    );
}

async fn start_restart_run_and_wait_for_call(f: &Fixture) {
    let mut request = f.edit_req("restart-run");
    request.on_restart = Some(broca_protocol::request::OnRestart::Resume);
    assert!(matches!(
        f.registry
            .route_send(&f.key, request, RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::Active { .. }
    ));
    tokio::time::timeout(Duration::from_secs(10), f.plane.entered.notified())
        .await
        .unwrap();
}

/// Returns false after verifying a grace-expired terminal, which cannot be resumed on boot.
async fn pause_for_restart(f: &Fixture) -> bool {
    start_restart_run_and_wait_for_call(f).await;
    let elapsed = f.stop_releasing_the_call(GRACE).await;
    let records = f.records();
    assert_restart_stop_outcome(&records, elapsed)
}

fn assert_restart_stop_outcome(records: &[WalRecord], elapsed: Duration) -> bool {
    match records.last() {
        // A pause is durable before the actor closes. Slow teardown may put total elapsed
        // past grace without changing that valid outcome, so the WAL takes precedence.
        Some(WalRecord::RunPaused {
            reason: broca_protocol::PauseReason::Restart,
            ..
        }) => true,
        Some(WalRecord::RunFinished {
            reason: RunFinishReason::Interrupted,
            ..
        }) if elapsed >= GRACE => false,
        _ => panic!(
            "expected a restart pause, or Interrupted after grace expiry; {}",
            restart_stop_diagnostics(records, elapsed)
        ),
    }
}

fn restart_stop_diagnostics(records: &[WalRecord], elapsed: Duration) -> String {
    let last_records: Vec<_> = records[records.len().saturating_sub(3)..]
        .iter()
        .map(|record| match record {
            WalRecord::RunFinished { reason, .. } => format!("RunFinished({reason:?})"),
            WalRecord::RunPaused { reason, .. } => format!("RunPaused({reason:?})"),
            other => serde_json::to_value(other).unwrap()["type"]
                .as_str()
                .unwrap()
                .to_owned(),
        })
        .collect();
    format!("elapsed={elapsed:?}; grace={GRACE:?}; last_records={last_records:?}")
}

fn begin_restart_stop(f: &Fixture) -> tokio::task::JoinHandle<(bool, Duration)> {
    let registry = f.registry.clone();
    tokio::spawn(async move {
        let started = tokio::time::Instant::now();
        let sealed = registry.stop_and_seal_with_budget(GRACE, TOTAL).await;
        (sealed, started.elapsed())
    })
}

#[tokio::test]
async fn drain_stops_admission_and_queue_without_sealing_restart_pause() {
    let f = Fixture::new(
        Hold::UntilReleased,
        vec![edit_step(), text_step("must not run")],
    );
    start_restart_run_and_wait_for_call(&f).await;
    assert!(matches!(
        f.registry
            .route_send(
                &f.key,
                text_req(&f.key, "queued-run"),
                RunKind::FreshOrResume
            )
            .await
            .unwrap(),
        SendOutcome::Pending { .. }
    ));
    let seal = f.registry.handle().seal_signal();
    f.registry.begin_stop();
    f.registry.begin_stop(); // Repeated drain notices must not escalate to a seal.
    assert!(
        f.registry
            .route_send(&f.key, text_req(&f.key, "new-run"), RunKind::FreshOrResume)
            .await
            .is_err(),
        "draining refuses new work"
    );
    assert!(!seal.is_cancelled());
    assert_eq!(f.registry.slot_counts().runs_in_flight, 1);
    f.plane.release.notify_one();
    broca_test_support::wait_for(
        "restart pause and cleared in-flight gauge",
        async {
            loop {
                let records = f.records();
                if records.iter().any(|r| {
                    matches!(
                        r,
                        WalRecord::RunPaused {
                            reason: broca_protocol::PauseReason::Restart,
                            ..
                        }
                    )
                }) && f.registry.slot_counts().runs_in_flight == 0
                {
                    assert_eq!(
                        records
                            .iter()
                            .filter(|r| matches!(r, WalRecord::RunStarted { .. }))
                            .count(),
                        1,
                        "durable queued work must wait for the replacement process"
                    );
                    break;
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || {
            format!(
                "in_flight={}, {}",
                f.registry.slot_counts().runs_in_flight,
                broca_test_support::record_kinds(f.records())
            )
        },
    )
    .await;
    assert_eq!(
        f.registry.slot_counts().open,
        1,
        "paused actor remains open for health probes"
    );
    assert!(!seal.is_cancelled(), "only the final stop may seal actors");
    assert!(f.registry.stop_and_seal().await);
    assert!(seal.is_cancelled());
}

async fn finish_restart_stop(
    f: &Fixture,
    stopping: tokio::task::JoinHandle<(bool, Duration)>,
) -> Duration {
    let (sealed, elapsed) = tokio::time::timeout(Duration::from_secs(60), stopping)
        .await
        .expect("the stop future must not hang")
        .unwrap();
    assert!(sealed, "{}", stop_failure_message(elapsed, &f.registry));
    elapsed
}

#[tokio::test]
async fn restart_call_held_past_grace_is_interrupted_not_paused() {
    let f = Fixture::new(Hold::UntilReleased, vec![edit_step()]);
    start_restart_run_and_wait_for_call(&f).await;
    let seal = f.registry.handle().seal_signal();
    let elapsed = finish_restart_stop(&f, begin_restart_stop(&f)).await;
    // Release only after the stop has sealed and aborted the still-held call. Its result
    // cannot reach a clean boundary inside grace, so it must remain outcome_unknown.
    f.plane.release.notify_one();
    assert!(seal.is_cancelled());
    assert!(elapsed >= GRACE);
    let records = f.records();
    assert!(!assert_restart_stop_outcome(&records, elapsed));
    assert!(
        matches!(
            records.last(),
            Some(WalRecord::RunFinished {
                reason: RunFinishReason::Interrupted,
                ..
            })
        ),
        "{}",
        restart_stop_diagnostics(&records, elapsed)
    );
    assert_eq!(tool_results(&records)[0].1, DispatchOutcome::Unknown);
    println!("{}", restart_stop_diagnostics(&records, elapsed));
}

async fn restart_pause_with_ordered_result(stop_first: bool) {
    let f = Fixture::new(Hold::UntilReleasedAtBoundary, vec![edit_step()]);
    start_restart_run_and_wait_for_call(&f).await;
    let stop_requested = f.registry.handle().stopping_signal();
    let seal = f.registry.handle().seal_signal();
    let stopping = if stop_first {
        let stopping = begin_restart_stop(&f);
        tokio::time::timeout(Duration::from_secs(60), stop_requested.cancelled())
            .await
            .expect("the stop request must not hang");
        f.plane.release.notify_one();
        wait_for_batch_commit(&f.plane).await;
        stopping
    } else {
        f.plane.release.notify_one();
        wait_for_batch_commit(&f.plane).await;
        assert!(!stop_requested.is_cancelled());
        let stopping = begin_restart_stop(&f);
        tokio::time::timeout(Duration::from_secs(60), stop_requested.cancelled())
            .await
            .expect("the stop request must not hang");
        stopping
    };
    let committed = f.records();
    assert_eq!(tool_results(&committed)[0].1, DispatchOutcome::Completed);
    assert!(matches!(
        committed.last(),
        Some(WalRecord::TurnFinished { .. })
    ));
    assert!(
        !seal.is_cancelled(),
        "the result is durable before grace expires"
    );
    f.plane.leave_batch.notify_one();
    let elapsed = finish_restart_stop(&f, stopping).await;
    let records = f.records();
    assert!(
        elapsed < GRACE,
        "{}",
        restart_stop_diagnostics(&records, elapsed)
    );
    assert!(assert_restart_stop_outcome(&records, elapsed));
    assert!(
        matches!(
            records.last(),
            Some(WalRecord::RunPaused {
                reason: broca_protocol::PauseReason::Restart,
                ..
            })
        ),
        "{}",
        restart_stop_diagnostics(&records, elapsed)
    );
    assert!(terminals(&records).is_empty());
    println!(
        "stop_first={stop_first}; {}",
        restart_stop_diagnostics(&records, elapsed)
    );
}

async fn wait_for_batch_commit(plane: &HoldingPlane) {
    tokio::time::timeout(Duration::from_secs(60), plane.batch_committed.notified())
        .await
        .expect("the durable tool batch must not hang");
}

#[tokio::test]
async fn restart_stop_before_call_result_pauses_at_the_durable_boundary() {
    restart_pause_with_ordered_result(true).await;
}

#[tokio::test]
async fn restart_stop_after_call_result_pauses_at_the_durable_boundary() {
    restart_pause_with_ordered_result(false).await;
}

#[test]
fn restart_pause_is_valid_even_if_teardown_outlasts_grace() {
    let records = [WalRecord::RunPaused {
        ts_ms: None,
        run_id: RunId("restart-run".into()),
        reason: broca_protocol::PauseReason::Restart,
        error: None,
    }];
    assert!(assert_restart_stop_outcome(
        &records,
        GRACE + Duration::from_secs(1)
    ));
}

#[test]
fn restart_stop_rejects_premature_interruption_and_other_outcomes() {
    let terminal = |reason| WalRecord::RunFinished {
        interrupt_reason: None,
        ts_ms: None,
        reason,
        usage: Default::default(),
        error: None,
        retries_used: 0,
    };
    let other_pause = WalRecord::RunPaused {
        ts_ms: None,
        run_id: RunId("restart-run".into()),
        reason: broca_protocol::PauseReason::AuthRequired,
        error: None,
    };
    for (records, elapsed) in [
        (
            vec![terminal(RunFinishReason::Interrupted)],
            GRACE - Duration::from_secs(1),
        ),
        (vec![terminal(RunFinishReason::Completed)], GRACE),
        (vec![terminal(RunFinishReason::Cancelled)], GRACE),
        (vec![other_pause], GRACE),
        (Vec::new(), GRACE),
    ] {
        assert!(
            std::panic::catch_unwind(|| assert_restart_stop_outcome(&records, elapsed)).is_err(),
            "a third outcome must fail: {}",
            restart_stop_diagnostics(&records, elapsed)
        );
    }
}

#[tokio::test]
async fn restart_pause_queues_plain_send_and_delivers_steer_on_boot() {
    let f = Fixture::new(Hold::UntilReleased, vec![edit_step()]);
    if !pause_for_restart(&f).await {
        return;
    }
    let next = f.reopen(vec![text_step("resumed"), text_step("queued")]);
    next.registry.actor_for(&next.key).await.unwrap();
    let state = next
        .registry
        .route_run_state(&next.key, RunId("restart-run".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.state, RunIndexState::Paused);
    assert_eq!(
        state.pause_reason,
        Some(broca_protocol::PauseReason::Restart)
    );
    let plain = text_req(&next.key, "queued-run");
    assert!(matches!(
        next.registry
            .route_send(&next.key, plain, RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::Pending { .. }
    ));
    let mut steer = text_req(&next.key, "steer-run").to_submission();
    steer.input[0].content = vec![ContentBlock::Text {
        text: "restart steer".into(),
    }];
    let outcome = next
        .registry
        .route_submission_with_delivery(
            &next.key,
            RunId("steer-run".into()),
            steer,
            RunKind::FreshOrResume,
            SendKey::RunId,
            false,
            broca_protocol::Delivery::Steer,
        )
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        SendOutcome::Paused {
            reason: broca_protocol::PauseReason::Restart,
            ..
        }
    ));
    let store = Arc::new(FileWalStore::new(next.root.join("wal")));
    assert_eq!(
        next.registry
            .resume_restart_pauses(store, vec![("restart-run".into(), next.key.clone())])
            .await,
        (1, 0)
    );
    broca_test_support::wait_until("resumed restart and queued run terminals", || async {
        let records = next.records();
        (terminals(&records).len() >= 2)
            .then_some(())
            .ok_or_else(|| broca_test_support::record_kinds(records))
    })
    .await;
    assert_eq!(
        run_started_ids(&next.records()),
        vec!["restart-run", "queued-run"]
    );
    assert!(next.provider.calls()[0].prompt.iter().any(|message| message
        .content
        .iter()
        .any(|block| matches!(block, ContentBlock::Text { text } if text == "restart steer"))));
    assert!(next.registry.stop_and_seal().await);
}

#[tokio::test]
async fn restart_pause_can_be_cancelled_before_boot_resume() {
    let f = Fixture::new(Hold::UntilReleased, vec![edit_step()]);
    if !pause_for_restart(&f).await {
        return;
    }
    let next = f.reopen(vec![text_step("must not run")]);
    next.registry.actor_for(&next.key).await.unwrap();
    next.registry
        .route_cancel(&next.key, RunId("restart-run".into()))
        .await
        .unwrap();
    broca_test_support::wait_until("cancelled restart pause terminal", || async {
        let records = next.records();
        (!terminals(&records).is_empty())
            .then_some(())
            .ok_or_else(|| broca_test_support::record_kinds(records))
    })
    .await;
    assert_eq!(terminals(&next.records()), vec![RunFinishReason::Cancelled]);
    assert!(next.provider.calls().is_empty());
    assert!(next.registry.stop_and_seal().await);
}

#[tokio::test]
async fn restart_boot_unknown_feature_logs_refusal_without_touching_wal_or_blocking_other_resumes()
{
    let f = Fixture::new(Hold::UntilReleased, vec![edit_step()]);
    if !pause_for_restart(&f).await {
        return;
    }
    let next = f.reopen(vec![text_step("resumed")]);
    let store = Arc::new(FileWalStore::new(next.root.join("wal")));
    let unknown = BindIdentity::new(&next.key.project_root, "runner", "unknown-feature-session");
    let unknown_path = store.wal_path(&unknown);
    let mut bytes = std::fs::read(store.wal_path(&next.key)).unwrap();
    bytes.extend(
        broca_wal::framing::encode_with_requires(
            1000,
            1,
            &["future-restart/v99"],
            &WalRecord::RunPaused {
                ts_ms: None,
                run_id: RunId("restart-run".into()),
                reason: broca_protocol::PauseReason::Restart,
                error: None,
            },
        )
        .unwrap(),
    );
    std::fs::write(&unknown_path, &bytes).unwrap();
    let before = std::fs::metadata(&unknown_path).unwrap();
    let logs = broca_test_support::LogCapture::new();
    assert_eq!(
        logs.scope_future(next.registry.resume_restart_pauses(
            store,
            vec![
                ("unknown".into(), unknown),
                ("restart-run".into(), next.key.clone())
            ]
        ))
        .await,
        (1, 0)
    );
    let after = std::fs::metadata(&unknown_path).unwrap();
    assert_eq!(std::fs::read(&unknown_path).unwrap(), bytes);
    assert_eq!(before.len(), after.len());
    assert_eq!(before.modified().unwrap(), after.modified().unwrap());
    let captured = logs.events();
    assert!(
        captured
            .iter()
            .any(|event| event.level == tracing::Level::WARN
                && event
                    .fields
                    .values()
                    .any(|value| value.contains("unknown-feature-session"))
                && event
                    .fields
                    .values()
                    .any(|value| value.contains("future-restart/v99"))),
        "refusal log: {captured:?}"
    );
    assert!(next.registry.stop_and_seal().await);
}

struct RestartRefusingExecutor;
#[async_trait::async_trait]
impl RunExecutor for RestartRefusingExecutor {
    async fn execute(
        &self,
        mut job: RunJob,
        _: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let error = RunError::Config("frozen model refused by catalog".into());
        if let Some(reply) = job.on_admitted.take() {
            let _ = reply.send(Err(error));
        }
        Err(RunError::Config("frozen model refused by catalog".into()))
    }
}

struct RestartAdmissionHeld {
    entered: Notify,
}

#[async_trait::async_trait]
impl RunExecutor for RestartAdmissionHeld {
    async fn execute(
        &self,
        job: RunJob,
        _: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        self.entered.notify_one();
        // Keep the acknowledgement and writer owned exactly as a dependency-admission wait
        // does, but do not block a runtime worker. Aborting this future must drop both.
        std::future::pending::<()>().await;
        drop(job);
        unreachable!("held admission never completes")
    }
}

#[tokio::test]
async fn restart_boot_admission_observes_seal_while_dependency_is_held() {
    let f = Fixture::new(Hold::UntilReleased, vec![edit_step()]);
    assert!(pause_for_restart(&f).await, "setup must durably pause");
    let store = Arc::new(FileWalStore::new(f.root.join("wal")));
    let executor = Arc::new(RestartAdmissionHeld {
        entered: Notify::new(),
    });
    let registry = LineageRegistry::new(Arc::new(DefaultOpener::new(
        Arc::new(FileLeaseStore::new(f.root.join("leases"))),
        store.clone(),
        executor.clone(),
    )));
    let boot_registry = registry.clone();
    let key = f.key.clone();
    let boot = tokio::spawn(async move {
        boot_registry
            .resume_restart_pauses(store, vec![("restart-run".into(), key)])
            .await
    });
    tokio::time::timeout(TOTAL, executor.entered.notified())
        .await
        .unwrap();
    assert_eq!(registry.slot_counts().runs_in_flight, 1);
    let started = tokio::time::Instant::now();
    assert!(
        registry
            .stop_and_seal_with_budget(Duration::ZERO, TOTAL)
            .await,
        "{}",
        stop_failure_message(started.elapsed(), &registry)
    );
    assert_eq!(registry.slot_counts().open, 0);
    let records = f.records();
    assert!(
        matches!(
            records.last(),
            Some(WalRecord::RunFinished {
                reason: RunFinishReason::Interrupted,
                error: None,
                ..
            })
        ),
        "{records:?}"
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::RunPaused { .. }))
            .count(),
        1
    );
    tokio::time::timeout(TOTAL, boot).await.unwrap().unwrap();
}

#[tokio::test]
async fn restart_resume_admission_refusal_seals_interrupted_with_reason() {
    let f = Fixture::new(Hold::UntilReleased, vec![edit_step()]);
    if !pause_for_restart(&f).await {
        return;
    }
    let store = Arc::new(FileWalStore::new(f.root.join("wal")));
    let opener = DefaultOpener::new(
        Arc::new(FileLeaseStore::new(f.root.join("leases"))),
        store.clone(),
        Arc::new(RestartRefusingExecutor),
    );
    let registry = LineageRegistry::new(Arc::new(opener));
    let counts = registry
        .resume_restart_pauses(store, vec![("restart-run".into(), f.key.clone())])
        .await;
    assert_eq!(counts, (0, 1), "records: {:?}", f.records());
    let records = f.records();
    assert!(
        matches!(records.last(), Some(WalRecord::RunFinished { reason: RunFinishReason::Interrupted, error: Some(error), .. }) if error.message.contains("frozen model refused by catalog"))
    );
    assert!(records
        .iter()
        .any(|record| matches!(record, WalRecord::ResumeStarted { .. })));
    assert!(registry.stop_and_seal().await);
}

/// A System One batch call holds no session, so the stop sees it only through the batch gauge.
/// The grace waits for a batch the same as for a run, and no longer: a batch still running when
/// the grace ends does not hold the stop open.
#[tokio::test]
async fn the_stop_grace_waits_for_batches_but_never_past_its_end() {
    let f = Fixture::new(Hold::UntilReleased, vec![edit_step()]);
    let gauge = f.registry.batches_in_flight_gauge();
    gauge.store(1, Ordering::Relaxed);
    assert_eq!(f.registry.slot_counts().batches_in_flight, 1);
    let lowered = gauge.clone();
    let lowering = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        lowered.store(0, Ordering::Relaxed);
    });
    let started = tokio::time::Instant::now();
    assert!(
        f.registry
            .stop_and_seal_with_budget(Duration::from_secs(5), TOTAL)
            .await
    );
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(150),
        "the grace ended before the batch did: {waited:?}"
    );
    assert!(waited < Duration::from_secs(5), "{waited:?}");
    lowering.await.unwrap();

    let f = Fixture::new(Hold::UntilReleased, vec![edit_step()]);
    f.registry
        .batches_in_flight_gauge()
        .store(1, Ordering::Relaxed);
    let started = tokio::time::Instant::now();
    assert!(
        f.registry
            .stop_and_seal_with_budget(Duration::from_millis(200), TOTAL)
            .await
    );
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(200), "{waited:?}");
    assert!(
        waited < Duration::from_secs(2),
        "a batch that outlives the grace held the stop open: {waited:?}"
    );
}
