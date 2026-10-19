//! Coordinator concurrency conformance — the GATED piece's race harness.
//!
//! Drives the Oracle's named interleavings deterministically via the controllable fake
//! executor: double-reopen coalesce, send-while-active queues durably, idle-close releases
//! the lease, close-vs-just-queued aborts, send-during-close re-routes, cancel reaches the
//! active run, and the cross-episode writer handoff does NOT trip the file-integrity belt.

use crate::common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{
    run_loop_on_writer_admitted_with_prior_snapshot, AllowAll, FileLeaseStore, FileWalStore,
    OutputEvent, Provider, RunError, RunIndex, RunIndexRow, RunIndexState, RunSummary, StartMode,
    StubToolPlane, WalStore,
};
use broca_module::actor::{CancelOutcome, LineageStatus, RetractOutcome, SendOutcome};
use broca_module::executor::{RunExecutor, RunJob, RunKind};
use broca_protocol::provider::NormalizedEvent;
use broca_protocol::{CallOptions, StepId};
use common::{req, FakeExecutor, Rig, RunScript};
use tokio::sync::{mpsc, Notify};

const LONG_TTL: Duration = Duration::from_secs(3600); // effectively "never auto-close"
const SHORT_TTL: Duration = Duration::from_millis(100);

struct RendererV2Provider {
    calls: AtomicUsize,
}

#[async_trait]
impl Provider for RendererV2Provider {
    async fn step(&self, _step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let _ = tx
                .send(NormalizedEvent::Control(
                    broca_protocol::provider::ProviderControl::AssistantMessage {
                        message: broca_protocol::AssistantMessage {
                            message_id: broca_protocol::MessageId("after-drift".into()),
                            content: vec![broca_protocol::ContentBlock::Text {
                                text: "continued".into(),
                            }],
                        },
                    },
                ))
                .await;
            let _ = tx
                .send(NormalizedEvent::Control(
                    broca_protocol::provider::ProviderControl::Finished {
                        usage: broca_protocol::Usage::default(),
                        finish_reason: broca_protocol::FinishReason::Stop,
                        retries_used: 0,
                    },
                ))
                .await;
        });
        rx
    }

    fn renderer_version(&self) -> u32 {
        2
    }
}

struct CoreRendererDriftExecutor {
    wal_store: Arc<FileWalStore>,
    store: Arc<broca_store::SqliteStore>,
    provider: Arc<RendererV2Provider>,
}

#[async_trait]
impl RunExecutor for CoreRendererDriftExecutor {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let mode = match job.kind {
            RunKind::FreshOrResume => StartMode::FreshOrResume,
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
        };
        run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            job.writer.as_ref(),
            mode,
            self.provider.as_ref(),
            &StubToolPlane::new(),
            &AllowAll,
            self.wal_store.as_ref(),
            Some(self.store.clone()),
            job.control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        )
        .await
    }
}

struct CancelThenCompleteProvider {
    calls: AtomicUsize,
    first_step_started: Arc<Notify>,
}

impl CancelThenCompleteProvider {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            first_step_started: Arc::new(Notify::new()),
        }
    }

    async fn wait_for_first_step(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.first_step_started.notified())
            .await
            .expect("first provider step did not start");
    }
}

#[async_trait]
impl Provider for CancelThenCompleteProvider {
    async fn step(&self, _step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        panic!("the cancellation test must pass the step cancellation token to the provider")
    }

    async fn step_with_cancel(
        &self,
        _step_id: StepId,
        _opts: &CallOptions,
        cancel: tokio_util::sync::CancellationToken,
        _deadline: Option<tokio::time::Instant>,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let (tx, rx) = mpsc::channel(4);
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            let first_step_started = Arc::clone(&self.first_step_started);
            tokio::spawn(async move {
                first_step_started.notify_one();
                cancel.cancelled().await;
                drop(tx);
            });
        } else {
            tokio::spawn(async move {
                let _ = tx
                    .send(NormalizedEvent::Control(
                        broca_protocol::provider::ProviderControl::AssistantMessage {
                            message: broca_protocol::AssistantMessage {
                                message_id: broca_protocol::MessageId("r2-completed".into()),
                                content: vec![broca_protocol::ContentBlock::Text {
                                    text: "second episode".into(),
                                }],
                            },
                        },
                    ))
                    .await;
                let _ = tx
                    .send(NormalizedEvent::Control(
                        broca_protocol::provider::ProviderControl::Finished {
                            usage: broca_protocol::Usage::default(),
                            finish_reason: broca_protocol::FinishReason::Stop,
                            retries_used: 0,
                        },
                    ))
                    .await;
            });
        }
        rx
    }
}

struct CoreCancellationExecutor {
    wal_store: Arc<FileWalStore>,
    provider: Arc<CancelThenCompleteProvider>,
}

#[async_trait]
impl RunExecutor for CoreCancellationExecutor {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let mode = match job.kind {
            RunKind::FreshOrResume => StartMode::FreshOrResume,
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
        };
        run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            job.writer.as_ref(),
            mode,
            self.provider.as_ref(),
            &StubToolPlane::new(),
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

/// A test index that models an implementation which replaces an Active row wholesale. Recovery
/// must therefore supply the durable digest instead of relying on an index-specific merge.
#[derive(Default)]
struct ReplacingRunIndex {
    rows: Mutex<HashMap<String, RunIndexRow>>,
}

impl RunIndex for ReplacingRunIndex {
    fn index_run_active(&self, row: &RunIndexRow) -> Result<(), String> {
        self.rows
            .lock()
            .map_err(|_| "test run index lock poisoned".to_owned())?
            .insert(row.run_id.clone(), row.clone());
        Ok(())
    }

    fn index_run_terminal(
        &self,
        run_id: &str,
        state: RunIndexState,
        usage: Option<broca_protocol::Usage>,
        final_step_finish_reason: Option<broca_protocol::FinishReason>,
        retries_used: u32,
    ) -> Result<(), String> {
        if let Some(row) = self
            .rows
            .lock()
            .map_err(|_| "test run index lock poisoned".to_owned())?
            .get_mut(run_id)
        {
            row.state = state;
            row.usage = usage;
            row.final_step_finish_reason = final_step_finish_reason;
            row.retries_used = retries_used;
        }
        Ok(())
    }

    fn lookup_run(&self, run_id: &str) -> Result<Option<RunIndexRow>, String> {
        Ok(self
            .rows
            .lock()
            .map_err(|_| "test run index lock poisoned".to_owned())?
            .get(run_id)
            .cloned())
    }
}

/// A run index whose `index_run_active` parks on a std channel until the test releases it.
/// Used to hold the actor inside the post-admission index write so the health gauge can be
/// observed while that write is still outstanding (spawn_blocking runs this on a blocking
/// thread, so the hold must be synchronous).
struct ParkingRunIndex {
    rows: Mutex<HashMap<String, RunIndexRow>>,
    /// Fired once when `index_run_active` is entered (before the hold).
    entered: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    /// When present, `index_run_active` blocks until a token arrives.
    hold: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    /// Return value of `index_run_active` after the hold (if any) completes.
    active_result: Mutex<Result<(), String>>,
}

impl ParkingRunIndex {
    fn holding() -> (
        Arc<Self>,
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let index = Arc::new(Self {
            rows: Mutex::new(HashMap::new()),
            entered: Mutex::new(Some(entered_tx)),
            hold: Mutex::new(Some(release_rx)),
            active_result: Mutex::new(Ok(())),
        });
        (index, entered_rx, release_tx)
    }

    fn failing(error: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            rows: Mutex::new(HashMap::new()),
            entered: Mutex::new(None),
            hold: Mutex::new(None),
            active_result: Mutex::new(Err(error.into())),
        })
    }
}

impl RunIndex for ParkingRunIndex {
    fn index_run_active(&self, row: &RunIndexRow) -> Result<(), String> {
        if let Some(tx) = self
            .entered
            .lock()
            .map_err(|_| "test run index lock poisoned".to_owned())?
            .take()
        {
            let _ = tx.send(());
        }
        if let Some(rx) = self
            .hold
            .lock()
            .map_err(|_| "test run index lock poisoned".to_owned())?
            .take()
        {
            // Park until the test releases — this is the outstanding-write window.
            let _ = rx.recv();
        }
        let result = self
            .active_result
            .lock()
            .map_err(|_| "test run index lock poisoned".to_owned())?
            .clone();
        if result.is_ok() {
            self.rows
                .lock()
                .map_err(|_| "test run index lock poisoned".to_owned())?
                .insert(row.run_id.clone(), row.clone());
        }
        result
    }

    fn index_run_terminal(
        &self,
        run_id: &str,
        state: RunIndexState,
        usage: Option<broca_protocol::Usage>,
        final_step_finish_reason: Option<broca_protocol::FinishReason>,
        retries_used: u32,
    ) -> Result<(), String> {
        if let Some(row) = self
            .rows
            .lock()
            .map_err(|_| "test run index lock poisoned".to_owned())?
            .get_mut(run_id)
        {
            row.state = state;
            row.usage = usage;
            row.final_step_finish_reason = final_step_finish_reason;
            row.retries_used = retries_used;
        }
        Ok(())
    }

    fn lookup_run(&self, run_id: &str) -> Result<Option<RunIndexRow>, String> {
        Ok(self
            .rows
            .lock()
            .map_err(|_| "test run index lock poisoned".to_owned())?
            .get(run_id)
            .cloned())
    }
}

/// Build a registry whose actors write through `run_index`, with a gated fake executor so the
/// run stays mid-flight after admission.
fn registry_with_parking_index(
    tag: &str,
    exec: Arc<FakeExecutor>,
    run_index: Arc<dyn RunIndex>,
) -> (
    broca_module::registry::LineageRegistry,
    broca_protocol::BindIdentity,
    std::path::PathBuf,
) {
    use broca_core::{FileLeaseStore, FileWalStore};

    let dir = broca_test_support::scratch_root().join(format!(
        "broca-coord-{}-{}-{}",
        std::process::id(),
        tag,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let key = broca_protocol::BindIdentity::new(dir.join("proj"), "runner", "s1");
    let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
    let opener = broca_module::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(dir.join("leases"))),
        wal_store,
        exec,
    )
    .with_idle_ttl(LONG_TTL)
    .with_run_index(run_index);
    let registry = broca_module::registry::LineageRegistry::new(Arc::new(opener));
    (registry, key, dir)
}

async fn wait_for_index_state(rig: &Rig, run_id: &str, expected: RunIndexState) {
    broca_test_support::wait_for(
        "wait_for_index_state",
        async {
            loop {
                let state = rig
                    .run_index
                    .lookup_run(run_id)
                    .unwrap()
                    .map(|row| row.state);
                if state == Some(expected.clone()) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
        || {
            format!(
                "index={:?}, expected={expected:?}",
                rig.run_index.lookup_run(run_id).map(|r| r.map(|r| r.state))
            )
        },
    )
    .await
}

async fn wait_for_resolved_state(
    rig: &Rig,
    key: &broca_protocol::BindIdentity,
    run_id: &str,
    expected: RunIndexState,
) {
    broca_test_support::wait_for(
        "wait_for_resolved_state",
        async {
            loop {
                let state = rig
                    .registry
                    .route_run_state(key, broca_protocol::RunId(run_id.into()))
                    .await
                    .unwrap();
                if state.map(|resolved| resolved.state) == Some(expected.clone()) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
        || {
            format!(
                "slot={:?}, started={}, finished={}",
                rig.registry.slot_state(key),
                rig.exec.started_count(),
                rig.exec.finished_count()
            )
        },
    )
    .await
}

async fn wait_for_actor_close(rig: &Rig, key: &broca_protocol::BindIdentity) {
    broca_test_support::wait_for(
        "wait_for_actor_close",
        async {
            loop {
                if rig.registry.slot_state(key).is_none() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
        || {
            format!(
                "slot={:?}, started={}, finished={}",
                rig.registry.slot_state(key),
                rig.exec.started_count(),
                rig.exec.finished_count()
            )
        },
    )
    .await
}

/// The in-flight gauge must flip at durable admission, before the run-index active write
/// returns. A test that only waits for the gauge to become 1 eventually would pass on the
/// old post-index ordering too — the assertion has to land while the write is still held.
#[tokio::test]
async fn runs_in_flight_gauge_is_one_while_index_active_write_is_outstanding() {
    let (exec, gate) = FakeExecutor::gated();
    let (index, entered_rx, release_tx) = ParkingRunIndex::holding();
    let (registry, key, dir) =
        registry_with_parking_index("gauge-during-index", exec.clone(), index);

    let reg = registry.clone();
    let k = key.clone();
    let send = tokio::spawn(async move {
        reg.route_send(&k, req(&k, "r1"), RunKind::FreshOrResume)
            .await
    });

    // Block until the actor has entered the index write (post-admission).
    tokio::task::spawn_blocking(move || {
        entered_rx
            .recv_timeout(broca_test_support::EVENT_HANG_GUARD)
            .expect("index_run_active did not start")
    })
    .await
    .expect("entered wait join");

    assert_eq!(
        registry.slot_counts().runs_in_flight,
        1,
        "gauge must read 1 while the run-index active write is still outstanding — that is the \
         window health.check would otherwise report zero for a live run"
    );

    release_tx.send(()).expect("release index write");
    let outcome = send.await.expect("send task join").expect("route_send");
    assert!(
        matches!(outcome, SendOutcome::Active { .. }),
        "send must complete Active after the index write: {outcome:?}"
    );
    assert_eq!(
        registry.slot_counts().runs_in_flight,
        1,
        "gauge stays 1 for the still-gated run"
    );

    gate.release().await;
    broca_test_support::wait_for(
        "runs_in_flight_gauge_is_one_while_index_active_write_is_outstanding",
        async {
            loop {
                if exec.finished_count() >= 1 && registry.slot_counts().runs_in_flight == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || {
            format!(
                "finished={}, in_flight={}",
                exec.finished_count(),
                registry.slot_counts().runs_in_flight
            )
        },
    )
    .await;
    assert_eq!(
        registry.slot_counts().runs_in_flight,
        0,
        "gauge returns to 0 on terminal"
    );

    drop(registry);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A failed run-index active write is non-fatal: the run stays Active and the gauge stays 1.
#[tokio::test]
async fn failing_index_active_write_still_leaves_run_in_flight() {
    let (exec, gate) = FakeExecutor::gated();
    let index = ParkingRunIndex::failing("simulated index write failure");
    let (registry, key, dir) = registry_with_parking_index("gauge-index-fail", exec.clone(), index);

    let outcome = registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .expect("route_send");
    assert!(
        matches!(outcome, SendOutcome::Active { .. }),
        "a failed index write must not reject admission: {outcome:?}"
    );
    assert_eq!(
        registry.slot_counts().runs_in_flight,
        1,
        "failed index write must leave the gauge at 1 for the live run"
    );

    // Status must still see the live actor as Active (tier-1), even with no index row.
    let state = registry
        .route_run_state(&key, broca_protocol::RunId("r1".into()))
        .await
        .expect("route_run_state");
    assert_eq!(
        state.map(|s| s.state),
        Some(RunIndexState::Active),
        "live run remains Active after a non-fatal index failure"
    );

    gate.release().await;
    broca_test_support::wait_for(
        "failing_index_active_write_still_leaves_run_in_flight",
        async {
            loop {
                if exec.finished_count() >= 1 && registry.slot_counts().runs_in_flight == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || {
            format!(
                "finished={}, in_flight={}",
                exec.finished_count(),
                registry.slot_counts().runs_in_flight
            )
        },
    )
    .await;
    assert_eq!(
        registry.slot_counts().runs_in_flight,
        0,
        "gauge returns to 0 on terminal after a failed index write"
    );

    drop(registry);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A first send on an idle lineage opens the actor and starts the run (active, run_id).
#[tokio::test]
async fn first_send_starts_a_run() {
    let exec = FakeExecutor::auto();
    let rig = Rig::new("first-send", exec.clone(), LONG_TTL);
    let key = rig.key("s1");

    let outcome = rig
        .registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert!(matches!(outcome, SendOutcome::Active { .. }));
    assert_eq!(rig.registry.slot_state(&key), Some("open"));
}

/// DOUBLE-REOPEN COALESCE: many concurrent first-ops on the same closed session admit
/// EXACTLY ONE actor (one lease acquire, one open) — the others coalesce onto it. With a
/// GATED executor the first run stays active, so the coalesce is unambiguous: exactly one
/// run starts (active), the rest queue (Pending) — proving ONE actor handled all 16 ops,
/// not 16 actors each acquiring the lease.
#[tokio::test]
async fn concurrent_first_ops_open_exactly_one_actor() {
    let (exec, _gate) = FakeExecutor::gated();
    let rig = Rig::new("coalesce", exec.clone(), LONG_TTL);
    let key = rig.key("s1");

    // Fire N concurrent sends at the same (absent) session.
    let mut handles = Vec::new();
    for i in 0..16 {
        let reg = rig.registry.clone();
        let k = key.clone();
        handles.push(tokio::spawn(async move {
            reg.route_send(&k, req(&k, &format!("r{i}")), RunKind::FreshOrResume)
                .await
        }));
    }
    let mut active = 0;
    let mut pending = 0;
    for h in handles {
        match h.await.unwrap().unwrap() {
            SendOutcome::Active { .. } => active += 1,
            SendOutcome::Pending { .. } => pending += 1,
            other => panic!("unexpected {other:?}"),
        }
    }
    // The gated first run stays active, so exactly ONE op started a run and the other 15
    // queued — proving a SINGLE actor served all 16 (16 separate actors would each have
    // found their own lineage idle and started 16 runs / acquired the lease 16 times).
    assert_eq!(active, 1, "exactly one send started a run (one actor)");
    assert_eq!(pending, 15, "the other 15 sends queued onto the same actor");
    assert_eq!(
        rig.exec.started_count(),
        1,
        "exactly one run started — one actor, not 16"
    );
}

/// Retrying an active deterministic send identity returns the same run. Reusing that identity
/// with different input is rejected, and completing the run leaves exactly one durable episode.
#[tokio::test]
async fn active_send_retry_is_idempotent() {
    use broca_core::{replay, WalStore};
    use broca_protocol::content::ContentBlock;
    use broca_wal::WalRecord;

    let (exec, gate) = FakeExecutor::gated();
    let rig = Rig::new("active-send-retry", exec.clone(), LONG_TTL);
    let key = rig.key("s1");
    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));

    let active = rig
        .registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert_eq!(
        active,
        SendOutcome::Active {
            run_id: broca_protocol::RunId("r1".into()),
            delivered: None,
        }
    );
    let active_retry = rig
        .registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert_eq!(active_retry, active);

    let mut changed_active = req(&key, "r1");
    changed_active.input[0].content[0] = ContentBlock::Text {
        text: "different active input".into(),
    };
    assert_eq!(
        rig.registry
            .route_send(&key, changed_active, RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::SendIdReuse { differing: "input" }
    );

    let mut changed_active_budget = req(&key, "r1");
    changed_active_budget.budget = Some(broca_protocol::request::BudgetConfig {
        max_input_tokens: None,
        nudges: vec![],
        finalize_at_tool_calls: 8,
        finalize_text: "finish now".into(),
    });
    assert_eq!(
        rig.registry
            .route_send(&key, changed_active_budget, RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::SendIdReuse {
            differing: "budget"
        }
    );

    let mut changed_active_generation = req(&key, "r1");
    changed_active_generation.generation.max_output_tokens = Some(17);
    assert_eq!(
        rig.registry
            .route_send(&key, changed_active_generation, RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::SendIdReuse {
            differing: "generation"
        }
    );

    gate.release().await;
    broca_test_support::wait_for(
        "active_send_retry_is_idempotent",
        async {
            loop {
                if exec.finished_count() == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || {
            format!(
                "started={}, finished={}",
                exec.started_count(),
                exec.finished_count()
            )
        },
    )
    .await;
    assert_eq!(exec.finished_count(), 1);

    let records: Vec<_> = store
        .read_records(&key)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::RunStarted { .. }))
            .count(),
        1,
        "the active retry must not create another episode"
    );
    assert!(
        records
            .iter()
            .all(|record| !matches!(record, WalRecord::Queued { .. })),
        "the active retry must not enter the durable queue"
    );
    assert_eq!(replay(&records).unwrap().run_id.unwrap().0, "r1");
}

/// Retrying a pending deterministic send identity returns the same submission without adding a
/// second queue record. Different input under the same identity is rejected.
#[tokio::test]
async fn pending_send_retry_is_idempotent() {
    use broca_core::WalStore;
    use broca_protocol::content::ContentBlock;
    use broca_wal::WalRecord;

    let (exec, gate) = FakeExecutor::gated();
    let rig = Rig::new("pending-send-retry", exec.clone(), LONG_TTL);
    let key = rig.key("s1");
    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));

    rig.registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    let pending = rig
        .registry
        .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
        .await
        .unwrap();
    let pending_retry = rig
        .registry
        .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert_eq!(pending_retry, pending);

    let mut changed_pending = req(&key, "r2");
    changed_pending.input[0].content[0] = ContentBlock::Text {
        text: "different pending input".into(),
    };
    assert_eq!(
        rig.registry
            .route_send(&key, changed_pending, RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::SendIdReuse { differing: "input" }
    );

    let mut changed_pending_budget = req(&key, "r2");
    changed_pending_budget.budget = Some(broca_protocol::request::BudgetConfig {
        max_input_tokens: None,
        nudges: vec![],
        finalize_at_tool_calls: 8,
        finalize_text: "finish now".into(),
    });
    assert_eq!(
        rig.registry
            .route_send(&key, changed_pending_budget, RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::SendIdReuse {
            differing: "budget"
        }
    );

    let mut changed_pending_generation = req(&key, "r2");
    changed_pending_generation.generation.temperature = Some(0.25);
    assert_eq!(
        rig.registry
            .route_send(&key, changed_pending_generation, RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::SendIdReuse {
            differing: "generation"
        }
    );

    let records: Vec<_> = store
        .read_records(&key)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::Queued { .. }))
            .count(),
        1,
        "the pending retry must not create another queue record"
    );
    assert_eq!(exec.started_count(), 1);

    gate.release().await;
    broca_test_support::wait_for(
        "pending_send_retry_is_idempotent",
        async {
            loop {
                if exec.started_count() >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || {
            format!(
                "started={}, finished={}",
                exec.started_count(),
                exec.finished_count()
            )
        },
    )
    .await;
    gate.release().await;
}

/// A deterministic send identity names one logical submission even when that submission is
/// retracted before it starts. Retrying the identity must not resurrect the durable tombstone.
#[tokio::test]
async fn retracted_pending_send_retry_does_not_queue_the_submission_again() {
    use broca_wal::WalRecord;

    let (exec, gate) = FakeExecutor::gated();
    let rig = Rig::new("retracted-send-retry", exec.clone(), SHORT_TTL);
    let key = rig.key("s1");
    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));

    rig.registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    let submission_id = match rig
        .registry
        .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
        .await
        .unwrap()
    {
        SendOutcome::Pending { submission_id } => submission_id,
        other => panic!("expected pending submission, got {other:?}"),
    };
    assert_eq!(
        rig.registry
            .route_retract(&key, submission_id)
            .await
            .unwrap(),
        RetractOutcome::Retracted
    );

    let retry = rig
        .registry
        .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert_eq!(
        retry,
        SendOutcome::Retracted {
            submission_id: broca_protocol::SubmissionId("sub-r2".into())
        },
        "the retry must report the durable tombstone instead of becoming pending again"
    );

    let records = store.read_records(&key).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record.record, WalRecord::Queued { .. }))
            .count(),
        1,
        "the retry must not append another queue record after the tombstone"
    );

    gate.release().await;
    wait_for_actor_close(&rig, &key).await;
    assert_eq!(
        rig.registry
            .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::Retracted {
            submission_id: broca_protocol::SubmissionId("sub-r2".into())
        },
        "reopening the actor must recover the tombstone before admitting the retry"
    );
    assert_eq!(
        exec.started_count(),
        1,
        "the retracted logical submission must never reach execution"
    );
}

/// The same exhaustive retry check also runs before an idle same-run resume reaches core.
#[tokio::test]
async fn idle_resume_rejects_changed_generation() {
    let exec = FakeExecutor::auto();
    let rig = Rig::new("idle-retry", exec, LONG_TTL);
    let key = rig.key("s1");
    rig.registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    broca_test_support::wait_for(
        "idle_resume_rejects_changed_generation",
        async {
            loop {
                if matches!(
                    rig.registry.route_status(&key).await.unwrap(),
                    LineageStatus::Idle
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || {
            format!(
                "slot={:?}, started={}, finished={}",
                rig.registry.slot_state(&key),
                rig.exec.started_count(),
                rig.exec.finished_count()
            )
        },
    )
    .await;

    let mut changed = req(&key, "r1");
    changed.generation.top_p = Some(0.5);
    assert_eq!(
        rig.registry
            .route_send(&key, changed, RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::SendIdReuse {
            differing: "generation"
        }
    );
}

/// Once a later episode has completed, retrying an older deterministic send still resolves to
/// that original terminal run. It must not append the old prompt again or enter the queue.
#[tokio::test]
async fn delayed_terminal_send_retry_is_a_historical_noop() {
    use broca_core::WalStore;
    use broca_wal::WalRecord;

    let exec = FakeExecutor::auto();
    let rig = Rig::new("historical-terminal-retry", exec.clone(), LONG_TTL);
    let key = rig.key("s1");

    rig.registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    wait_until_idle(&rig, &key).await;
    rig.registry
        .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
        .await
        .unwrap();
    wait_until_idle(&rig, &key).await;
    wait_for_index_state(&rig, "r1", RunIndexState::Completed).await;

    let before = broca_core::FileWalStore::new(rig.dir.join("wal"))
        .read_records(&key)
        .unwrap();
    let outcome = rig
        .registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        SendOutcome::Finished {
            reason: broca_protocol::RunFinishReason::Completed,
            run_id: broca_protocol::RunId("r1".into()),
            delivered: None,
        }
    );
    let after = broca_core::FileWalStore::new(rig.dir.join("wal"))
        .read_records(&key)
        .unwrap();
    assert_eq!(after, before, "terminal retry must perform no WAL write");
    assert_eq!(
        after
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::RunStarted { .. }))
            .count(),
        2,
        "only the original A and B episodes exist"
    );
    assert_eq!(
        exec.started_count(),
        2,
        "the retry never reaches provider or tool execution"
    );
    assert_eq!(
        rig.run_index.lookup_run("r1").unwrap().unwrap().state,
        RunIndexState::Completed,
        "the terminal acknowledgement remains A's original outcome"
    );
}

/// A delayed retry of completed A while B is active is also a no-op and cannot disturb B.
#[tokio::test]
async fn delayed_terminal_send_retry_does_not_queue_behind_active_run() {
    use broca_core::WalStore;
    use broca_wal::WalRecord;

    let (exec, gate) = FakeExecutor::scripted_gated(vec![
        RunScript::FreshCompleted,
        RunScript::FreshCompletedAfterGate,
    ]);
    let rig = Rig::new("historical-retry-during-active", exec.clone(), LONG_TTL);
    let key = rig.key("s1");
    rig.registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    wait_until_idle(&rig, &key).await;
    rig.registry
        .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
        .await
        .unwrap();

    let outcome = rig
        .registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        SendOutcome::Finished {
            reason: broca_protocol::RunFinishReason::Completed,
            run_id: broca_protocol::RunId("r1".into()),
            delivered: None,
        }
    );
    assert_eq!(
        rig.registry.route_status(&key).await.unwrap(),
        LineageStatus::Active {
            run_id: broca_protocol::RunId("r2".into()),
            step: 0,
        },
        "B remains the one active run"
    );
    let records = broca_core::FileWalStore::new(rig.dir.join("wal"))
        .read_records(&key)
        .unwrap();
    assert!(
        records
            .iter()
            .all(|frame| !matches!(frame.record, WalRecord::Queued { .. })),
        "historical A must not be queued behind B"
    );
    assert_eq!(exec.started_count(), 2, "A retry starts no executor work");
    gate.release().await;
}

/// The durable digest rejects a delayed retry whose caller payload changed.
#[tokio::test]
async fn delayed_historical_send_retry_rejects_mutated_payload() {
    use broca_protocol::content::ContentBlock;

    let rig = Rig::new("historical-retry-mutation", FakeExecutor::auto(), LONG_TTL);
    let key = rig.key("s1");
    rig.registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    wait_until_idle(&rig, &key).await;
    rig.registry
        .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
        .await
        .unwrap();
    wait_until_idle(&rig, &key).await;

    let mut changed = req(&key, "r1");
    changed.input[0].content[0] = ContentBlock::Text {
        text: "mutated delayed retry".into(),
    };
    assert_eq!(
        rig.registry
            .route_send(&key, changed, RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::SendIdReuse { differing: "input" }
    );
}

/// Historical idempotency survives actor close because the run index, not the actor cache, owns
/// the durable identity.
#[tokio::test]
async fn delayed_historical_send_retry_survives_actor_reopen() {
    use broca_core::WalStore;

    let exec = FakeExecutor::auto();
    let rig = Rig::new("historical-retry-reopen", exec.clone(), SHORT_TTL);
    let key = rig.key("s1");
    rig.registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    wait_until_idle(&rig, &key).await;
    rig.registry
        .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
        .await
        .unwrap();
    wait_until_idle(&rig, &key).await;
    wait_for_actor_close(&rig, &key).await;

    let before = broca_core::FileWalStore::new(rig.dir.join("wal"))
        .read_records(&key)
        .unwrap();
    assert_eq!(
        rig.registry
            .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::Finished {
            reason: broca_protocol::RunFinishReason::Completed,
            run_id: broca_protocol::RunId("r1".into()),
            delivered: None,
        }
    );
    let after = broca_core::FileWalStore::new(rig.dir.join("wal"))
        .read_records(&key)
        .unwrap();
    assert_eq!(after, before);
    assert_eq!(exec.started_count(), 2);
}

/// Recovery re-drives a Running episode with its real input and preserves the indexed identity,
/// so an exact retry is a no-op while a changed input remains a loud SendIdReuse.
#[tokio::test]
async fn crash_recovery_preserves_send_identity_for_retry() {
    use broca_core::{FileLeaseStore, FileWalStore};
    use broca_protocol::content::ContentBlock;
    use broca_wal::WalRecord;

    let dir = broca_test_support::scratch_root().join(format!(
        "broca-crash-recovery-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let key = broca_protocol::BindIdentity::new(dir.join("proj"), "runner", "s1");
    let original = req(&key, "r1");
    assert!(
        !original.input.is_empty(),
        "the regression request must carry input"
    );
    let digest = broca_core::submission_identity_digest(&original.to_submission());

    let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
    let index = Arc::new(ReplacingRunIndex::default());
    let (executor, gate) = FakeExecutor::scripted_gated(vec![
        RunScript::FreshPanicsAfterGate,
        RunScript::ResumeCompleted,
    ]);
    let opener = broca_module::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(dir.join("leases"))),
        wal_store.clone(),
        executor,
    )
    .with_idle_ttl(SHORT_TTL)
    .with_run_index(index.clone());
    let registry = broca_module::registry::LineageRegistry::new(Arc::new(opener));

    // The first send is admitted, then its run task crashes after writing RunStarted. Closing and
    // reopening the actor leaves that Running episode for recover_durable_work to re-drive.
    assert!(matches!(
        registry
            .route_send(&key, original.clone(), RunKind::FreshOrResume)
            .await
            .expect("start the send before the simulated crash"),
        SendOutcome::Active { .. }
    ));
    gate.release().await;
    broca_test_support::wait_for(
        "crash_recovery_preserves_send_identity_for_retry",
        async {
            loop {
                if registry.slot_state(&key).is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
        || format!("slot={:?}", registry.slot_state(&key)),
    )
    .await;
    assert_eq!(
        registry.slot_state(&key),
        None,
        "the actor must close after the crashed run"
    );
    assert_eq!(
        index
            .lookup_run(&original.run_id.0)
            .unwrap()
            .and_then(|row| row.request_identity_digest),
        Some(digest.clone()),
        "the original admission must persist its request digest"
    );

    let recovered = registry
        .route_send(&key, original.clone(), RunKind::FreshOrResume)
        .await
        .expect("route exact retry after recovery");
    // Recovery re-drives the crashed episode, and that re-driven run may or may not have
    // finished by the time the retry arrives. Both answers are the idempotent one: the retry
    // names the original run and starts nothing new. Pinning only `Active` made this test race
    // the recovered run's completion.
    match &recovered {
        SendOutcome::Active { run_id, .. } => assert_eq!(run_id, &original.run_id),
        SendOutcome::Finished { run_id, reason, .. } => {
            assert_eq!(run_id, &original.run_id);
            assert_eq!(*reason, broca_protocol::RunFinishReason::Completed);
        }
        other => panic!(
            "an identical keyed retry must be idempotent across crash recovery, got {other:?}"
        ),
    }
    assert_eq!(
        index
            .lookup_run(&original.run_id.0)
            .unwrap()
            .and_then(|row| row.request_identity_digest),
        Some(digest),
        "recovery must preserve the durable request digest"
    );

    let records = broca_core::FileWalStore::new(dir.join("wal"))
        .read_records(&key)
        .unwrap();
    let recovered_input = records
        .iter()
        .filter_map(|frame| match &frame.record {
            WalRecord::RunStarted { input, .. } => Some(input.clone()),
            _ => None,
        })
        .next_back();
    assert_eq!(
        recovered_input,
        Some(original.input.clone().into()),
        "recovery must re-drive the durable input, not an empty fabricated request"
    );

    let mut changed = original;
    changed.input[0].content[0] = ContentBlock::Text {
        text: "different after recovery".into(),
    };
    assert_eq!(
        registry
            .route_send(&key, changed, RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::SendIdReuse { differing: "input" }
    );

    broca_test_support::wait_for(
        "crash_recovery_preserves_send_identity_for_retry",
        async {
            loop {
                if registry.slot_state(&key).is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
        || format!("slot={:?}", registry.slot_state(&key)),
    )
    .await;
    drop(registry);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A pre-migration terminal row cannot prove payload equality. It fails loudly instead of
/// silently re-executing or accepting a possibly different request.
#[tokio::test]
async fn historical_send_retry_with_legacy_index_row_is_unverifiable() {
    let exec = FakeExecutor::auto();
    let rig = Rig::new("historical-retry-legacy", exec.clone(), LONG_TTL);
    let key = rig.key("s1");
    rig.run_index
        .index_run_active(&broca_core::RunIndexRow {
            withdrawals_owed: false,
            title: None,
            run_id: "r1".into(),
            session: key.clone(),
            submission_id: None,
            request_identity_digest: None,
            state: RunIndexState::Active,
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
        })
        .unwrap();
    rig.run_index
        .index_run_terminal("r1", RunIndexState::Completed, None, None, 0)
        .unwrap();

    assert_eq!(
        rig.registry
            .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::SendIdReuse {
            differing: "request identity because the legacy run-index row has no digest"
        }
    );
    assert_eq!(exec.started_count(), 0);
}

/// SEND-WHILE-ACTIVE queues durably: a send arriving while a run is active replies Pending
/// (a durable Queued record exists) and does NOT start a second run.
#[tokio::test]
async fn send_while_active_queues_durably() {
    let (exec, gate) = FakeExecutor::gated();
    let rig = Rig::new("queue", exec.clone(), LONG_TTL);
    let key = rig.key("s1");

    // The Active reply follows durable admission and the actor's Active transition.
    // The executor gate then keeps r1 active until the queued record is inspected.
    let o1 = rig
        .registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert!(matches!(o1, SendOutcome::Active { .. }));

    // Second send → Pending (durably queued; the run is still active).
    let o2 = rig
        .registry
        .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
        .await
        .unwrap();
    let SendOutcome::Pending { submission_id } = o2 else {
        panic!("send while r1 is active must queue, got {o2:?}");
    };
    assert_eq!(
        rig.exec.started_count(),
        1,
        "no second run started while active"
    );
    let records = FileWalStore::new(rig.dir.join("wal"))
        .read_records(&key)
        .expect("read durable queue before releasing r1");
    assert!(
        records.iter().any(|frame| matches!(
            &frame.record,
            broca_wal::WalRecord::Queued { submission_id: id, request, .. }
                if id == &submission_id && request.input == req(&key, "r2").input
        )),
        "Pending must follow a durable Queued record carrying r2's input"
    );

    gate.release().await;
    // Releasing the gate only lets r1 begin its terminal sync. The actor must then
    // process the terminal, update its index and admit r2 before drain is observable.
    // Executor entry precedes admission, so wait for actor status, not started_count.
    let mut last_status = None;
    let drained = broca_test_support::event_timeout(
        "send_while_active_queues_durably",
        async {
            loop {
                let status = rig.registry.route_status(&key).await.unwrap();
                let is_r2 =
                    matches!(&status, LineageStatus::Active { run_id, .. } if run_id.0 == "r2");
                last_status = Some(status);
                if is_r2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        },
        || {
            format!(
                "slot={:?}, in_flight={}",
                rig.registry.slot_state(&key),
                rig.registry.slot_counts().runs_in_flight
            )
        },
    )
    .await;
    assert!(
        drained.is_ok(),
        "timed out waiting for queued r2 to become active: {}; last actor status: {last_status:?}",
        if matches!(&last_status, Some(LineageStatus::Active { run_id, .. }) if run_id.0 == "r1") {
            "r1 has not left Active after its gate was released"
        } else {
            "r1 is no longer active, but queued r2 was not admitted"
        }
    );
    assert_eq!(rig.exec.started_count(), 2);

    gate.release().await;
    // Keep the rig alive until the drained run's terminal is handled as well.
    broca_test_support::event_timeout(
        "send_while_active_queues_durably",
        async {
            while rig.registry.route_status(&key).await.unwrap() != LineageStatus::Idle {
                tokio::task::yield_now().await;
            }
        },
        || {
            format!(
                "slot={:?}, in_flight={}",
                rig.registry.slot_state(&key),
                rig.registry.slot_counts().runs_in_flight
            )
        },
    )
    .await
    .expect("drained r2 did not become idle after its gate was released");
}

#[tokio::test]
async fn queued_burst_drains_fifo_with_identical_durable_payloads() {
    let (exec, gate) = FakeExecutor::gated();
    let rig = Rig::new("queue-burst-fifo", exec.clone(), LONG_TTL);
    let key = rig.key("s1");
    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));

    rig.registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .expect("start blocking head run");
    for run_id in ["r2", "r3", "r4"] {
        let mut request = req(&key, run_id);
        request.input[0].content = vec![broca_protocol::content::ContentBlock::Text {
            text: format!("payload-{run_id}"),
        }];
        request.tools = tool_surface(run_id);
        request.policy_allowed = request.tools.iter().map(|tool| tool.name.clone()).collect();
        let outcome = rig
            .registry
            .route_send(&key, request, RunKind::FreshOrResume)
            .await
            .expect("queue burst submission");
        assert!(matches!(outcome, SendOutcome::Pending { .. }));
    }

    // These wait for events, not for speed: each queued run starts only after the previous
    // run's terminal is fsynced on a real file WAL, and one fsync on a busy CI disk has taken
    // over a second. The 60 s bound is a hang guard; the exact counts are still asserted.
    let hang_guard = || std::time::Instant::now() + Duration::from_secs(60);
    for expected_starts in 2..=4 {
        gate.release().await;
        let deadline = hang_guard();
        while exec.started_count() < expected_starts && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(exec.started_count(), expected_starts);
    }
    gate.release().await;
    let deadline = hang_guard();
    while exec.finished_count() < 4 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(exec.finished_count(), 4);

    let records: Vec<_> = store
        .read_records(&key)
        .expect("read drained burst WAL")
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    let queued_payloads: HashMap<_, _> = records
        .iter()
        .filter_map(|record| match record {
            broca_wal::WalRecord::Queued {
                submission_id,
                request,
                ..
            } => Some((
                submission_id.clone(),
                (
                    serde_json::to_vec(&request.input).expect("serialize queued input"),
                    serde_json::to_vec(&request.tools).expect("serialize queued tools"),
                ),
            )),
            _ => None,
        })
        .collect();
    let starts: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            broca_wal::WalRecord::RunStarted {
                run_id,
                input,
                config,
                submission_id,
                ..
            } => Some((run_id.0.as_str(), input, config, submission_id.as_ref())),
            _ => None,
        })
        .collect();
    assert_eq!(
        starts
            .iter()
            .map(|(run_id, ..)| *run_id)
            .collect::<Vec<_>>(),
        ["r1", "r2", "r3", "r4"]
    );
    for (_, input, config, submission_id) in starts.into_iter().skip(1) {
        let submission_id = submission_id.expect("drained run carries consume link");
        let (queued_input, queued_tools) = queued_payloads
            .get(submission_id)
            .expect("consume link names queued payload");
        assert_eq!(
            serde_json::to_vec(input).expect("serialize started input"),
            *queued_input
        );
        assert_eq!(
            serde_json::to_vec(&config.tools).expect("serialize started tools"),
            *queued_tools
        );
    }
    assert!(broca_core::pending_submissions(&records).is_empty());
}

#[tokio::test]
async fn reopened_actor_starts_older_durable_queue_before_a_new_send() {
    let (exec, gate) = FakeExecutor::gated();
    let rig = Rig::new("reopen-queue-first", exec.clone(), LONG_TTL);
    let key = rig.key("s1");
    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));
    let mut wal = store.open(&key).expect("open WAL to seed pending work");
    wal.append(
        1,
        &broca_wal::WalRecord::Queued {
            retention: None,
            ts_ms: None,
            hooks: None,
            title: None,
            keep_warm: None,
            conversation_key: None,
            send_id: None,
            delivery: None,
            submission_id: broca_protocol::SubmissionId("sub-r2".into()),
            request: Box::new(req(&key, "r2").to_submission()),
            request_identity_digest: None,
            principal: None,
        },
        true,
    )
    .expect("seed pending r2");
    drop(wal);

    let outcome = rig
        .registry
        .route_send(&key, req(&key, "r3"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert!(
        matches!(outcome, SendOutcome::Pending { .. }),
        "new work must queue behind the recovered r2"
    );
    assert_eq!(exec.started_count(), 1, "only recovered r2 may be running");

    let records = store.read_records(&key).unwrap();
    let first_started = records.iter().find_map(|framed| match &framed.record {
        broca_wal::WalRecord::RunStarted {
            run_id,
            submission_id,
            ..
        } => Some((
            run_id.0.as_str(),
            submission_id.as_ref().map(|id| id.0.as_str()),
        )),
        _ => None,
    });
    assert_eq!(first_started, Some(("r2", Some("sub-r2"))));
    gate.release().await;
    gate.release().await;
}

#[tokio::test]
async fn live_queue_drain_persists_the_same_permanent_failure_terminal() {
    let (exec, gate) = FakeExecutor::scripted_gated(vec![
        RunScript::FreshCompletedAfterGate,
        RunScript::AdmissionPermanent,
    ]);
    let rig = Rig::new("live-poison-terminal", exec, LONG_TTL);
    let key = rig.key("s1");

    rig.registry
        .route_send(&key, req(&key, "a"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert!(matches!(
        rig.registry
            .route_send(&key, req(&key, "b"), RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::Pending { .. }
    ));
    gate.release().await;
    wait_for_resolved_state(&rig, &key, "b", RunIndexState::Error).await;

    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));
    let records: Vec<_> = store
        .read_records(&key)
        .unwrap()
        .into_iter()
        .map(|record| record.record)
        .collect();
    assert!(records.iter().any(|record| {
        matches!(
            record,
            broca_wal::WalRecord::Retracted {
                submission_id,
                failure: Some(failure),
            } if submission_id.0 == "sub-b" && failure.run_id.0 == "b"
        )
    }));
}

#[tokio::test]
async fn reopened_actor_resolves_permanent_queue_failure_and_serves_commands() {
    use broca_module::fanout::{Delivery, FromCursor};
    use broca_protocol::caller::ControlUnit;

    let exec = FakeExecutor::scripted(vec![
        RunScript::AdmissionPermanent,
        RunScript::FreshCompleted,
    ]);
    let rig = Rig::new("reopen-poison-responsive", exec, LONG_TTL);
    let key = rig.key("s1");
    seed_completed_run_with_queue(&rig, &key, &["b"]);

    // Revert check: the old unconditional startup retry never reached this subscribe command,
    // so this bounded await timed out forever on the permanently invalid queue head.
    let (sink, mut deliveries) = tokio::sync::mpsc::channel(16);
    broca_test_support::event_timeout(
        "reopened_actor_resolves_permanent_queue_failure_and_serves_commands",
        rig.registry.route_subscribe(&key, FromCursor::Start, sink),
        || {
            format!(
                "slot={:?}, in_flight={}",
                rig.registry.slot_state(&key),
                rig.registry.slot_counts().runs_in_flight
            )
        },
    )
    .await
    .expect("startup recovery must not starve the actor command loop")
    .expect("subscribe after recovery");

    let mut saw_failed_start = false;
    let mut saw_failed_error = false;
    broca_test_support::event_timeout(
        "reopened_actor_resolves_permanent_queue_failure_and_serves_commands",
        async {
            loop {
                match deliveries.recv().await {
                    Some(Delivery::Control(event)) => match &event.1 {
                        ControlUnit::RunStarted {
                            run_id,
                            submission_id,
                        } => {
                            saw_failed_start = run_id.0 == "b"
                                && submission_id.as_ref().is_some_and(|id| id.0 == "sub-b");
                        }
                        ControlUnit::Error { error } if saw_failed_start => {
                            saw_failed_error =
                                error.provider_code.as_deref() == Some("queued_admission_failed");
                        }
                        ControlUnit::RunFinished {
                            reason: broca_protocol::caller::RunFinishReason::Error,
                            ..
                        } if saw_failed_start && saw_failed_error => break,
                        _ => {}
                    },
                    Some(Delivery::Detached) => {
                        panic!("subscriber detached during failed terminal")
                    }
                    Some(Delivery::Error(error)) => panic!("subscriber replay failed: {error}"),
                    Some(_) => {}
                    None => panic!("subscriber ended before queued failure terminal"),
                }
            }
        },
        || {
            format!(
                "slot={:?}, in_flight={}",
                rig.registry.slot_state(&key),
                rig.registry.slot_counts().runs_in_flight
            )
        },
    )
    .await
    .expect("queued failure must be subscriber-visible");

    let resolved = rig
        .registry
        .route_run_state(&key, broca_protocol::RunId("b".into()))
        .await
        .unwrap()
        .expect("failed queued run has durable status");
    assert_eq!(resolved.state, RunIndexState::Error);
    assert_eq!(
        rig.registry
            .route_retract(&key, broca_protocol::SubmissionId("sub-b".into()),)
            .await
            .unwrap(),
        RetractOutcome::NotPending
    );
    assert_eq!(
        rig.registry
            .route_cancel(&key, broca_protocol::RunId("b".into()))
            .await
            .unwrap(),
        CancelOutcome::NotActive
    );
    assert!(matches!(
        rig.registry.route_status(&key).await.unwrap(),
        LineageStatus::Idle
    ));

    assert_eq!(
        rig.registry
            .route_send(&key, req(&key, "c"), RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::Active {
            run_id: broca_protocol::RunId("c".into()),
            delivered: None,
        }
    );
    wait_for_resolved_state(&rig, &key, "c", RunIndexState::Completed).await;
}

#[tokio::test]
async fn permanent_poison_head_is_consumed_before_younger_queue_work() {
    let exec = FakeExecutor::scripted(vec![
        RunScript::AdmissionPermanent,
        RunScript::FreshCompleted,
    ]);
    let rig = Rig::new("reopen-poison-fifo", exec.clone(), LONG_TTL);
    let key = rig.key("s1");
    seed_completed_run_with_queue(&rig, &key, &["b", "c"]);

    broca_test_support::event_timeout(
        "permanent_poison_head_is_consumed_before_younger_queue_work",
        rig.registry.route_status(&key),
        || {
            format!(
                "slot={:?}, in_flight={}",
                rig.registry.slot_state(&key),
                rig.registry.slot_counts().runs_in_flight
            )
        },
    )
    .await
    .expect("actor must enter its command loop after draining recovered FIFO")
    .unwrap();
    wait_for_resolved_state(&rig, &key, "b", RunIndexState::Error).await;
    wait_for_resolved_state(&rig, &key, "c", RunIndexState::Completed).await;

    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));
    let records: Vec<_> = store
        .read_records(&key)
        .unwrap()
        .into_iter()
        .map(|record| record.record)
        .collect();
    let failed_at = records
        .iter()
        .position(|record| {
            matches!(
                record,
                broca_wal::WalRecord::Retracted {
                    failure: Some(failure),
                    ..
                } if failure.run_id.0 == "b"
            )
        })
        .expect("poison head has a durable failed tombstone");
    let younger_started_at = records
        .iter()
        .position(|record| {
            matches!(
                record,
                broca_wal::WalRecord::RunStarted {
                    run_id,
                    submission_id: Some(submission_id),
                    ..
                } if run_id.0 == "c" && submission_id.0 == "sub-c"
            )
        })
        .expect("younger queued send starts");
    assert!(failed_at < younger_started_at, "FIFO head resolves first");
    assert!(broca_core::pending_submissions(&records).is_empty());
    assert_eq!(
        exec.started_count(),
        2,
        "one failed attempt plus younger run"
    );
}

#[tokio::test]
async fn transient_queued_admission_retries_without_consuming_the_head() {
    let exec = FakeExecutor::scripted(vec![
        RunScript::AdmissionTransient,
        RunScript::FreshCompleted,
    ]);
    let rig = Rig::new("reopen-transient-queue", exec.clone(), LONG_TTL);
    let key = rig.key("s1");
    seed_completed_run_with_queue(&rig, &key, &["b"]);

    broca_test_support::event_timeout(
        "transient_queued_admission_retries_without_consuming_the_head",
        rig.registry.route_status(&key),
        || {
            format!(
                "slot={:?}, in_flight={}",
                rig.registry.slot_state(&key),
                rig.registry.slot_counts().runs_in_flight
            )
        },
    )
    .await
    .expect("transient startup retry must recover")
    .unwrap();
    wait_for_resolved_state(&rig, &key, "b", RunIndexState::Completed).await;

    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));
    let records: Vec<_> = store
        .read_records(&key)
        .unwrap()
        .into_iter()
        .map(|record| record.record)
        .collect();
    assert!(!records.iter().any(|record| {
        matches!(
            record,
            broca_wal::WalRecord::Retracted {
                failure: Some(_),
                ..
            }
        )
    }));
    assert!(records.iter().any(|record| {
        matches!(
            record,
            broca_wal::WalRecord::RunStarted {
                run_id,
                submission_id: Some(submission_id),
                ..
            } if run_id.0 == "b" && submission_id.0 == "sub-b"
        )
    }));
    assert_eq!(exec.started_count(), 2, "one retry, then one admission");
}

#[tokio::test]
async fn reopened_retry_of_pending_run_is_not_started_twice() {
    let (exec, gate) = FakeExecutor::gated();
    let rig = Rig::new("reopen-pending-retry", exec.clone(), LONG_TTL);
    let key = rig.key("s1");
    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));
    let mut wal = store.open(&key).expect("open WAL to seed pending work");
    wal.append(
        1,
        &broca_wal::WalRecord::Queued {
            retention: None,
            ts_ms: None,
            hooks: None,
            title: None,
            keep_warm: None,
            conversation_key: None,
            send_id: None,
            delivery: None,
            submission_id: broca_protocol::SubmissionId("sub-r2".into()),
            request: Box::new(req(&key, "r2").to_submission()),
            request_identity_digest: None,
            principal: None,
        },
        true,
    )
    .expect("seed pending r2");
    drop(wal);

    assert_eq!(
        rig.registry
            .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::Active {
            run_id: broca_protocol::RunId("r2".into()),
            delivered: None,
        }
    );
    assert_eq!(exec.started_count(), 1);
    let records = store.read_records(&key).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|framed| matches!(&framed.record, broca_wal::WalRecord::RunStarted { .. }))
            .count(),
        1,
        "the retry must share the recovered queue consumption"
    );
    gate.release().await;
}

#[tokio::test]
async fn reopened_actor_redrives_unfinished_run_before_accepting_a_new_send() {
    let exec = FakeExecutor::scripted(vec![RunScript::ResumeCompleted, RunScript::FreshCompleted]);
    let rig = Rig::new("reopen-unfinished-first", exec.clone(), LONG_TTL);
    let key = rig.key("s1");
    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));
    let mut wal = store.open(&key).expect("open WAL to seed unfinished run");
    wal.append(
        1,
        &broca_wal::WalRecord::RunStarted {
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
            run_id: broca_protocol::RunId("r1".into()),
            session: key.clone(),
            config: Box::new(min_run_config()),
            input: req(&key, "r1").input.into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
        true,
    )
    .expect("seed unfinished r1");
    drop(wal);

    let outcome = rig
        .registry
        .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert!(
        !matches!(
            outcome,
            SendOutcome::Active {
                run_id: broca_protocol::RunId(ref id), ..
            } if id == "r1"
        ),
        "the new r2 send must never be acknowledged as r1's recovery"
    );

    broca_test_support::wait_for(
        "reopened_actor_redrives_unfinished_run_before_accepting_a_new_send",
        async {
            loop {
                if exec.started_count() >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
        || {
            format!(
                "started={}, finished={}",
                exec.started_count(),
                exec.finished_count()
            )
        },
    )
    .await;
    assert_eq!(exec.started_count(), 2, "r1 recovery and r2 each run once");
    let records = store.read_records(&key).unwrap();
    assert!(records.iter().any(|framed| {
        matches!(
            &framed.record,
            broca_wal::WalRecord::RunStarted { run_id, .. } if run_id.0 == "r2"
        )
    }));
}

#[tokio::test]
async fn reopened_actor_seals_indeterminate_run_before_draining_new_send() {
    let exec = FakeExecutor::scripted(vec![
        RunScript::ResumeInterrupted,
        RunScript::FreshCompleted,
    ]);
    let rig = Rig::new("reopen-indeterminate-first", exec.clone(), LONG_TTL);
    let key = rig.key("s1");
    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));
    let mut wal = store.open(&key).expect("open WAL to seed ambiguous run");
    let call_id = broca_protocol::ToolCallId("ambiguous".into());
    let batch_id = broca_protocol::BatchId("r1-b1".into());
    let seed = [
        broca_wal::WalRecord::RunStarted {
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
            run_id: broca_protocol::RunId("r1".into()),
            session: key.clone(),
            config: Box::new(min_run_config()),
            input: req(&key, "r1").input.into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
        broca_wal::WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
        broca_wal::WalRecord::ModelStepFinished {
            tags: None,
            hooks: None,
            step_id: broca_protocol::StepId(1),
            assistant_message: broca_protocol::AssistantMessage {
                message_id: broca_protocol::MessageId("m1".into()),
                content: vec![broca_protocol::ContentBlock::ToolCall(
                    broca_protocol::ToolCall {
                        tool_call_id: call_id.clone(),
                        tool_name: "edit".into(),
                        input: serde_json::json!({"path": "file"}),
                        thought_signature: None,
                    },
                )],
            }
            .into(),
            usage: broca_protocol::Usage::default(),
            finish_reason: Some(broca_protocol::FinishReason::ToolCalls),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        },
        broca_wal::WalRecord::ToolBatchStarted {
            batch_id: batch_id.clone(),
            step_id: broca_protocol::StepId(1),
            planned_calls: vec![call_id.clone()],
        },
        broca_wal::WalRecord::ToolDispatchIntent {
            hooks: None,
            batch_id,
            tool_call_id: call_id,
            tool_name: Some("edit".into()),
            args: serde_json::json!({"path": "file"}).into(),
            module_id: None,
            schema_pin: None,
            approval_digest: None,
            call_key: None,
        },
    ];
    for (index, record) in seed.into_iter().enumerate() {
        wal.append(index as u64 + 1, &record, true)
            .expect("seed ambiguous run record");
    }
    drop(wal);

    let outcome = rig
        .registry
        .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert!(
        !matches!(
            outcome,
            SendOutcome::Active {
                run_id: broca_protocol::RunId(ref id), ..
            } if id == "r1"
        ),
        "a new send must not be acknowledged as the ambiguous run's recovery"
    );

    broca_test_support::wait_for(
        "reopened_actor_seals_indeterminate_run_before_draining_new_send",
        async {
            loop {
                if exec.started_count() >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || {
            format!(
                "started={}, finished={}",
                exec.started_count(),
                exec.finished_count()
            )
        },
    )
    .await;
    assert_eq!(exec.started_count(), 2, "recovery seals r1 before r2 runs");
    wait_until_idle(&rig, &key).await;

    let records = store.read_records(&key).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|frame| matches!(
                frame.record,
                broca_wal::WalRecord::RunFinished {
                    reason: broca_protocol::caller::RunFinishReason::Interrupted,
                    ..
                }
            ))
            .count(),
        1,
        "actor-open recovery writes one interrupted seal"
    );
    assert!(records.iter().any(|frame| matches!(
        &frame.record,
        broca_wal::WalRecord::RunStarted { run_id, .. } if run_id.0 == "r2"
    )));
    assert!(records
        .iter()
        .all(|frame| !matches!(frame.record, broca_wal::WalRecord::ToolResult { .. })));
    let replayed = broca_core::replay(
        &records
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>(),
    )
    .expect("sealed lineage with next episode replays");
    assert!(replayed.prompt.iter().all(|message| message
        .content
        .iter()
        .all(|block| !matches!(block, broca_protocol::ContentBlock::ToolCall(_)))));
}

#[tokio::test]
async fn reopened_actor_seals_inferred_natural_end_before_draining_queue() {
    let exec = FakeExecutor::scripted(vec![RunScript::ResumeCompleted, RunScript::FreshCompleted]);
    let rig = Rig::new("reopen-natural-end-first", exec.clone(), LONG_TTL);
    let key = rig.key("s1");
    seed_unsealed_run_with_queue(&rig, &key, UnsealedTerminal::NaturalEnd);
    assert_unsealed_append_would_corrupt(&rig, &key);

    let _ = rig.registry.route_status(&key).await.unwrap();
    wait_until_idle(&rig, &key).await;
    assert_eq!(
        exec.started_count(),
        2,
        "r1 is sealed before the queued r2 starts"
    );

    let frames = repaired_frames(
        &rig,
        &key,
        broca_protocol::caller::RunFinishReason::Completed,
    );
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame.record, broca_wal::WalRecord::ModelStepFinished { .. }))
            .count(),
        1,
        "seal-first recovery must not re-run r1's completed model step"
    );
    let page = rig.registry.route_read(&key, None, Some(20)).await.unwrap();
    let transcript = page
        .messages
        .iter()
        .flat_map(|message| message.message.content.iter())
        .filter_map(|block| match block {
            broca_protocol::ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("|");
    assert!(transcript.contains("r1 answer"));
    assert_eq!(
        page.messages
            .iter()
            .filter(|message| message.message.role == broca_protocol::Role::User)
            .count(),
        2,
        "session.read reconstructs both r1 and queued r2 inputs"
    );
}

#[tokio::test]
async fn reopened_actor_seals_inferred_hard_stop_before_draining_queue() {
    let exec = FakeExecutor::scripted(vec![RunScript::ResumeMaxSteps, RunScript::FreshCompleted]);
    let rig = Rig::new("reopen-hard-stop-first", exec.clone(), LONG_TTL);
    let key = rig.key("s1");
    seed_unsealed_run_with_queue(&rig, &key, UnsealedTerminal::HardStop);
    assert_unsealed_append_would_corrupt(&rig, &key);

    let _ = rig.registry.route_status(&key).await.unwrap();
    wait_until_idle(&rig, &key).await;
    assert_eq!(
        exec.started_count(),
        2,
        "the hard-stopped r1 is sealed before queued r2 starts"
    );
    repaired_frames(
        &rig,
        &key,
        broca_protocol::caller::RunFinishReason::MaxSteps,
    );
}

/// THE CONSUME-LINK (B3): draining a queued submission writes a durable
/// `RunStarted{submission_id}` that marks it CONSUMED, so a fresh replay of the WAL no
/// longer sees it pending — a crash AFTER the consume never re-drains it. This is the
/// durability the pending-set algebra (pending = Queued − Retracted − consumed) depends on.
#[tokio::test]
async fn drained_submission_is_marked_consumed_in_the_durable_wal() {
    use broca_core::{pending_submissions, WalStore};

    let (exec, gate) = FakeExecutor::gated();
    let rig = Rig::new("consume-link", exec.clone(), LONG_TTL);
    let key = rig.key("s1");
    // A separate read-only store over the SAME wal dir, to inspect the durable WAL exactly
    // as a fresh process (a resume) would replay it.
    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));

    // First send → active; second → durably queued (Pending).
    rig.registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    let o2 = rig
        .registry
        .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert!(matches!(o2, SendOutcome::Pending { .. }));

    // While r1 is active, the submission is PENDING in the durable WAL (Queued, no
    // consuming RunStarted yet).
    let recs: Vec<_> = store
        .read_records(&key)
        .unwrap()
        .into_iter()
        .map(|f| f.record)
        .collect();
    assert_eq!(
        pending_submissions(&recs).len(),
        1,
        "the queued submission is pending while the active run hasn't consumed it"
    );

    // Finish r1 → the actor drains the submission into a consuming run (which writes
    // RunStarted{submission_id}).
    gate.release().await;
    // Wait for the drain's consuming run to have started + recorded its RunStarted.
    let mut consumed = false;
    broca_test_support::wait_for(
        "drained_submission_is_marked_consumed_in_the_durable_wal",
        async {
            loop {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let recs: Vec<_> = store
                    .read_records(&key)
                    .unwrap()
                    .into_iter()
                    .map(|f| f.record)
                    .collect();
                if pending_submissions(&recs).is_empty() && rig.exec.started_count() >= 2 {
                    consumed = true;
                    break;
                }
            }
        },
        || {
            format!(
                "started={}, finished={}",
                rig.exec.started_count(),
                rig.exec.finished_count()
            )
        },
    )
    .await;
    gate.release().await; // let the drained run finish too
    assert!(
        consumed,
        "after the drain, a fresh WAL replay must see the submission CONSUMED (a durable \
         RunStarted{{submission_id}} exists) — so a crash after the consume never re-drains it"
    );
}

/// A live subscriber keeps an otherwise idle actor open across repeated idle-TTL ticks and
/// receives a later run on the same stream without reattaching.
#[tokio::test]
async fn live_subscriber_pins_idle_actor_and_receives_later_run() {
    use broca_module::fanout::{Delivery, FromCursor};
    use broca_protocol::caller::ControlUnit;

    let exec = FakeExecutor::auto();
    let rig = Rig::new("subscriber-pins-idle", exec, Duration::from_millis(40));
    let key = rig.key("s1");
    let (sink, mut deliveries) = tokio::sync::mpsc::channel(16);
    rig.registry
        .route_subscribe(&key, FromCursor::Start, sink)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(240)).await;
    assert_eq!(
        rig.registry.slot_state(&key),
        Some("open"),
        "a live subscriber must keep an idle actor open"
    );

    let outcome = rig
        .registry
        .route_send(&key, req(&key, "r-late"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert!(matches!(outcome, SendOutcome::Active { .. }));

    broca_test_support::event_timeout(
        "live_subscriber_pins_idle_actor_and_receives_later_run",
        async {
            let mut saw_later_run = false;
            loop {
                match deliveries.recv().await {
                    Some(Delivery::Control(event)) => match &event.1 {
                        ControlUnit::RunStarted {
                            run_id,
                            submission_id,
                        } => {
                            saw_later_run = run_id.0 == "r-late" && submission_id.is_none();
                        }
                        ControlUnit::RunFinished { .. } if saw_later_run => break,
                        _ => {}
                    },
                    Some(Delivery::Detached) => panic!("pinned subscriber detached unexpectedly"),
                    Some(Delivery::Error(error)) => panic!("subscriber replay failed: {error}"),
                    Some(_) => {}
                    None => panic!("pinned subscriber stream ended before the later run"),
                }
            }
        },
        || {
            format!(
                "slot={:?}, in_flight={}",
                rig.registry.slot_state(&key),
                rig.registry.slot_counts().runs_in_flight
            )
        },
    )
    .await
    .expect("the original subscriber stream must receive the later run");

    drop(deliveries);
    wait_for_actor_close(&rig, &key).await;
}

/// Dropping a subscriber sink without an explicit detach releases its fan-out slot, allowing
/// the otherwise idle actor to close on a later TTL tick.
#[tokio::test]
async fn dropped_subscriber_sink_does_not_pin_idle_actor() {
    use broca_module::fanout::FromCursor;

    let exec = FakeExecutor::auto();
    let rig = Rig::new("dropped-subscriber-sink", exec, Duration::from_millis(40));
    let key = rig.key("s1");
    let (sink, deliveries) = tokio::sync::mpsc::channel(16);
    rig.registry
        .route_subscribe(&key, FromCursor::Start, sink)
        .await
        .unwrap();
    assert_eq!(rig.registry.slot_state(&key), Some("open"));

    drop(deliveries);
    wait_for_actor_close(&rig, &key).await;
}

/// IDLE CLOSE releases the lease: after the idle TTL with no work, the actor closes
/// (registry slot gone) — and a subsequent op RE-OPENS cleanly (the lease was released and
/// is re-acquirable).
#[tokio::test]
async fn idle_close_releases_then_reopens() {
    let exec = FakeExecutor::auto();
    let rig = Rig::new("idle-close", exec.clone(), Duration::from_millis(100));
    let key = rig.key("s1");

    // Open + run (auto-finishes) → goes idle.
    rig.registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    // Past the idle TTL the actor closes. Poll with a generous budget rather than a fixed
    // sleep, so a loaded scheduler (e.g. parallel compilation in CI) slow to run the actor
    // task doesn't false-fail — the property is "it DOES close", not "within 300ms".
    let mut closed = false;
    broca_test_support::wait_for(
        "idle_close_releases_then_reopens",
        async {
            loop {
                tokio::time::sleep(Duration::from_millis(50)).await;
                if rig.registry.slot_state(&key).is_none() {
                    closed = true;
                    break;
                }
            }
        },
        || {
            format!(
                "slot={:?}, started={}, finished={}",
                rig.registry.slot_state(&key),
                rig.exec.started_count(),
                rig.exec.finished_count()
            )
        },
    )
    .await;
    assert!(closed, "actor closed after idle TTL");

    // A new op re-opens cleanly (the lease was released, so re-acquire succeeds).
    let o = rig
        .registry
        .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert!(
        matches!(o, SendOutcome::Active { .. }),
        "re-opened and ran after close"
    );
}

/// CANCEL reaches the active run cooperatively.
#[tokio::test]
async fn cancel_acks_the_active_run() {
    let (exec, _gate) = FakeExecutor::gated();
    let rig = Rig::new("cancel", exec.clone(), LONG_TTL);
    let key = rig.key("s1");

    rig.registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    let ack = rig
        .registry
        .route_cancel(&key, broca_protocol::RunId("r1".into()))
        .await
        .unwrap();
    assert_eq!(ack, CancelOutcome::Ack);

    // A cancel for a different run id → NotActive.
    let other = rig
        .registry
        .route_cancel(&key, broca_protocol::RunId("nope".into()))
        .await
        .unwrap();
    assert_eq!(other, CancelOutcome::NotActive);
}

/// A coordinator cancel must persist the caller's intent before the actor accepts a new episode.
#[tokio::test]
async fn coordinator_cancel_is_durably_cancelled_and_allows_a_new_episode() {
    use broca_protocol::caller::RunFinishReason;
    use broca_wal::WalRecord;

    static NEXT_DIR: AtomicUsize = AtomicUsize::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-coord-cancelled-terminal-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let key = broca_protocol::BindIdentity::new(dir.join("proj"), "runner", "s1");
    let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
    let provider = Arc::new(CancelThenCompleteProvider::new());
    let executor = Arc::new(CoreCancellationExecutor {
        wal_store: wal_store.clone(),
        provider: provider.clone(),
    });
    let charge_basis_resolver: Arc<dyn broca_core::ChargeBasisResolver + Send + Sync> =
        Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1));
    let opener = Arc::new(
        broca_module::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store.clone(),
            executor,
        )
        .with_idle_ttl(LONG_TTL)
        .with_run_index(store.clone())
        .with_charge_basis_resolver(charge_basis_resolver),
    );
    let registry = broca_module::registry::LineageRegistry::new_with_reader(
        opener,
        wal_store.clone(),
        Some(store.clone()),
    );

    assert!(matches!(
        registry
            .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::Active { run_id, .. } if run_id.0 == "r1"
    ));
    provider.wait_for_first_step().await;
    assert_eq!(
        registry
            .route_cancel(&key, broca_protocol::RunId("r1".into()))
            .await
            .unwrap(),
        CancelOutcome::Ack
    );

    broca_test_support::wait_for(
        "coordinator_cancel_is_durably_cancelled_and_allows_a_new_episode",
        async {
            loop {
                if store.lookup_run("r1").unwrap().map(|row| row.state)
                    == Some(RunIndexState::Cancelled)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
        || {
            format!(
                "index={:?}",
                store.lookup_run("r1").map(|r| r.map(|r| r.state))
            )
        },
    )
    .await;
    assert_eq!(
        store.lookup_run("r1").unwrap().map(|row| row.state),
        Some(RunIndexState::Cancelled),
        "the coordinator indexes the caller-cancelled terminal"
    );
    assert_eq!(
        store
            .lookup_run("r1")
            .unwrap()
            .expect("cancelled run remains indexed")
            .usage,
        None,
        "a cancelled provider call without ModelStepFinished has no caller usage"
    );

    let records = wal_store.read_records(&key).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|frame| matches!(
                frame.record,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Cancelled,
                    ..
                }
            ))
            .count(),
        1,
        "the caller cancel writes exactly one Cancelled terminal"
    );
    assert!(matches!(
        broca_core::replay(
            &records
                .iter()
                .map(|frame| frame.record.clone())
                .collect::<Vec<_>>()
        )
        .unwrap()
        .resolution,
        broca_core::ReplayResolution::LineageIdle {
            last_reason: RunFinishReason::Cancelled
        }
    ));

    broca_test_support::wait_for(
        "coordinator_cancel_is_durably_cancelled_and_allows_a_new_episode",
        async {
            loop {
                if matches!(
                    registry.route_status(&key).await.unwrap(),
                    LineageStatus::Idle
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
        || format!("slot={:?}", registry.slot_state(&key)),
    )
    .await;
    assert!(matches!(
        registry.route_status(&key).await.unwrap(),
        LineageStatus::Idle
    ));
    assert!(matches!(
        registry
            .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::Active { run_id, .. } if run_id.0 == "r2"
    ));

    broca_test_support::wait_for(
        "coordinator_cancel_is_durably_cancelled_and_allows_a_new_episode",
        async {
            loop {
                if store.lookup_run("r2").unwrap().map(|row| row.state)
                    == Some(RunIndexState::Completed)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
        || {
            format!(
                "index={:?}",
                store.lookup_run("r2").map(|r| r.map(|r| r.state))
            )
        },
    )
    .await;
    assert_eq!(
        store.lookup_run("r2").unwrap().map(|row| row.state),
        Some(RunIndexState::Completed),
        "the subsequent send starts and finishes a new episode"
    );
    assert_eq!(
        store
            .lookup_run("r2")
            .unwrap()
            .expect("completed run remains indexed")
            .usage,
        Some(broca_protocol::Usage::default()),
        "a completed zero-token model step still has caller usage"
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    let started_run_ids: Vec<_> = wal_store
        .read_records(&key)
        .unwrap()
        .into_iter()
        .filter_map(|frame| match frame.record {
            WalRecord::RunStarted { run_id, .. } => Some(run_id.0),
            _ => None,
        })
        .collect();
    assert_eq!(started_run_ids, vec!["r1", "r2"]);

    drop(registry);
    let _ = std::fs::remove_dir_all(&dir);
}

/// STATUS reflects the run state.
#[tokio::test]
async fn status_reflects_state() {
    let (exec, gate) = FakeExecutor::gated();
    let rig = Rig::new("status", exec.clone(), LONG_TTL);
    let key = rig.key("s1");

    assert!(
        matches!(
            rig.registry.route_status(&key).await.unwrap(),
            LineageStatus::Idle
        ),
        "a fresh lineage is idle"
    );
    rig.registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert!(matches!(
        rig.registry.route_status(&key).await.unwrap(),
        LineageStatus::Active { .. }
    ));
    gate.release().await;
}

/// A send arriving while the session actor closes must re-route through the registry
/// rather than fail on a closed channel. A tiny idle timeout races sends against close:
/// each must resolve to an admitted run, queued submission, or already-finished retry,
/// without hanging, panicking, or losing the command.
#[tokio::test]
async fn send_racing_idle_close_always_reopens_cleanly() {
    for round in 0..40 {
        let exec = FakeExecutor::auto();
        // A tiny TTL so the actor is constantly trying to close, maximizing the chance a
        // send lands during Opening/Closing.
        let rig = Rig::new(
            &format!("race-close-{round}"),
            exec.clone(),
            Duration::from_millis(1),
        );
        let key = rig.key("s1");

        // Open + run, then let it go idle and start closing.
        rig.registry
            .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await; // into the close window

        // Fire a burst of sends right as the actor is closing — each must reopen cleanly.
        let mut handles = Vec::new();
        for i in 0..4 {
            let reg = rig.registry.clone();
            let k = key.clone();
            handles.push(tokio::spawn(async move {
                reg.route_send(&k, req(&k, &format!("r{i}")), RunKind::FreshOrResume)
                    .await
            }));
        }
        for h in handles {
            // The op must resolve (no hang/panic) to a real outcome — never an error from
            // landing on a draining actor.
            let outcome = h
                .await
                .unwrap()
                .expect("send must not error on a close race");
            assert!(
                matches!(
                    outcome,
                    SendOutcome::Active { .. }
                        | SendOutcome::Pending { .. }
                        | SendOutcome::Finished { .. }
                ),
                "round {round}: a send racing close resolved to {outcome:?}"
            );
        }
    }
}

/// A post-admission run-task panic has no durable RunFinished. The current actor generation must
/// close without terminalizing the run index, then recovery must resume that run before draining
/// its already-durable queued successor.
#[tokio::test]
async fn run_task_panic_does_not_wedge_the_actor() {
    use broca_core::ReplayResolution;
    use broca_wal::WalRecord;

    let (exec, gate) = FakeExecutor::scripted_gated(vec![
        RunScript::FreshPanicsAfterGate,
        RunScript::ResumeCompleted,
        RunScript::FreshCompleted,
    ]);
    let rig = Rig::new("panic-recovery-queue", exec.clone(), LONG_TTL);
    let key = rig.key("s1");

    assert!(matches!(
        rig.registry
            .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::Active { .. }
    ));
    assert!(matches!(
        rig.registry
            .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
            .await
            .unwrap(),
        SendOutcome::Pending { .. }
    ));

    gate.release().await;
    wait_for_actor_close(&rig, &key).await;

    assert_eq!(
        rig.run_index.lookup_run("r1").unwrap().unwrap().state,
        RunIndexState::Active,
        "an executor Err must leave the as-admitted index row nonterminal"
    );
    let records: Vec<_> = broca_core::FileWalStore::new(rig.dir.join("wal"))
        .read_records(&key)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    assert!(matches!(
        broca_core::replay(&records).unwrap().resolution,
        ReplayResolution::Resume { .. }
    ));
    assert!(!records
        .iter()
        .any(|record| matches!(record, WalRecord::RunFinished { .. })));

    // route_run_state must open a new actor because the prior generation's slot is gone. Startup
    // recovery resumes r1, durably seals it, and only then drains r2; the old actor stayed open
    // forever and never reached this recovery path.
    let reopened_state = rig
        .registry
        .route_run_state(&key, broca_protocol::RunId("r1".into()))
        .await
        .unwrap()
        .unwrap()
        .state;
    assert_ne!(reopened_state, RunIndexState::Error);
    wait_for_index_state(&rig, "r1", RunIndexState::Completed).await;
    wait_for_index_state(&rig, "r2", RunIndexState::Completed).await;
    assert_eq!(exec.started_count(), 3);

    let started_run_ids: Vec<_> = broca_core::FileWalStore::new(rig.dir.join("wal"))
        .read_records(&key)
        .unwrap()
        .into_iter()
        .filter_map(|frame| match frame.record {
            WalRecord::RunStarted { run_id, .. } => Some(run_id.0),
            _ => None,
        })
        .collect();
    assert_eq!(started_run_ids, vec!["r1", "r2"]);
}

/// CROSS-EPISODE WRITER HANDOFF: two sequential runs (episodes) in ONE session do NOT trip
/// the file-integrity belt. The belt was designed for FOREIGN writers; a legitimate
/// same-session next-episode write (on the SAME session writer) must pass — there is only
/// ONE writer for the session, so the belt never sees a foreign append.
#[tokio::test]
async fn two_sequential_episodes_do_not_trip_integrity_belt() {
    let exec = FakeExecutor::auto();
    let rig = Rig::new("two-episodes", exec.clone(), LONG_TTL);
    let key = rig.key("s1");

    // Episode 1.
    let o1 = rig
        .registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert!(matches!(o1, SendOutcome::Active { .. }));

    // Wait for the first run to finish and the actor to go Idle by polling status (a fixed
    // sleep races a loaded scheduler: if the first run hasn't finished, the second send
    // would QUEUE instead of starting a fresh episode, breaking this test's premise).
    broca_test_support::wait_for(
        "two_sequential_episodes_do_not_trip_integrity_belt",
        async {
            loop {
                if matches!(
                    rig.registry.route_status(&key).await.unwrap(),
                    LineageStatus::Idle
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
        || {
            format!(
                "slot={:?}, started={}, finished={}",
                rig.registry.slot_state(&key),
                rig.exec.started_count(),
                rig.exec.finished_count()
            )
        },
    )
    .await;

    // Episode 2 over the SAME open actor (same session writer). If the writer model were
    // per-run with a re-seed, OR if a second writer were spawned, the integrity belt could
    // false-positive here; with one session writer it just works.
    let o2 = rig
        .registry
        .route_send(&key, req(&key, "r2"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert!(
        matches!(o2, SendOutcome::Active { .. }),
        "second episode started cleanly"
    );
    // Wait for both episodes to have run by polling (a fixed sleep would race a loaded
    // scheduler and could assert before the second episode's executor recorded its finish).
    broca_test_support::wait_for(
        "two_sequential_episodes_do_not_trip_integrity_belt",
        async {
            loop {
                if rig.exec.finished_count() >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
        || {
            format!(
                "started={}, finished={}",
                rig.exec.started_count(),
                rig.exec.finished_count()
            )
        },
    )
    .await;
    assert_eq!(
        rig.exec.started_count(),
        2,
        "both episodes ran on one session writer with no ForeignWrite"
    );
    assert_eq!(rig.exec.finished_count(), 2, "both finished cleanly");
}

/// ADMISSION BARRIER: a cancel/status racing IMMEDIATELY after a send always resolves the
/// run_id — because the actor commits the durable run-index Active row BEFORE replying
/// `active{run_id}`, and the actor's command loop is serialized, so a cancel that arrives
/// after the client has the run_id arrives after the index row is durable.
#[tokio::test]
async fn run_state_resolves_immediately_after_send() {
    let (exec, _gate) = FakeExecutor::gated(); // run stays active
    let rig = Rig::new("admit-race", exec.clone(), LONG_TTL);
    let key = rig.key("s1");

    // Start a run; the reply comes ONLY after admission (index Active durable).
    let outcome = rig
        .registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    let run_id = match outcome {
        SendOutcome::Active { run_id, .. } => run_id,
        other => panic!("expected Active, got {other:?}"),
    };

    // IMMEDIATELY resolve the run_id — must be Active (the index row is durable by now,
    // AND a live actor backs it: tier 1). No race window where the run_id is unresolvable.
    let state = rig.registry.route_run_state(&key, run_id).await.unwrap();
    assert_eq!(
        state.map(|resolved| resolved.state),
        Some(RunIndexState::Active),
        "a run-state lookup immediately after the send resolves the run_id (admission \
         committed the index row before the reply)"
    );
}

/// A paused run writes `Paused` into the durable index, and a run-keyed lookup keeps
/// resolving `Paused` both before and after the idle actor closes.
#[tokio::test]
async fn paused_run_indexes_paused_and_reconciles_after_close() {
    let exec = FakeExecutor::scripted(vec![RunScript::FreshPaused]);
    let rig = Rig::new("paused-index", exec.clone(), SHORT_TTL);
    let key = rig.key("s1");

    let outcome = rig
        .registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    let run_id = match outcome {
        SendOutcome::Active { run_id, .. } => run_id,
        other => panic!("expected Active, got {other:?}"),
    };

    wait_for_index_state(&rig, &run_id.0, RunIndexState::Paused).await;
    wait_for_resolved_state(&rig, &key, &run_id.0, RunIndexState::Paused).await;

    wait_for_actor_close(&rig, &key).await;
    wait_for_resolved_state(&rig, &key, &run_id.0, RunIndexState::Paused).await;
}

#[tokio::test]
async fn settled_paused_cancel_seals_and_unblocks_queued_work() {
    let (exec, gate) = FakeExecutor::scripted_gated(vec![
        RunScript::FreshPausedAfterGate,
        RunScript::AdmissionTransient,
        RunScript::ResumeCancelled,
        RunScript::FreshCompleted,
    ]);
    let rig = Rig::new("paused-cancel-queue", exec, LONG_TTL);
    let key = rig.key("s1");

    let first = rig
        .registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert!(matches!(first, SendOutcome::Active { .. }));
    let queued = rig
        .registry
        .route_send(&key, req(&key, "r2"), RunKind::AppendEpisode)
        .await
        .unwrap();
    assert!(matches!(queued, SendOutcome::Pending { .. }));

    gate.release().await;
    wait_for_resolved_state(&rig, &key, "r1", RunIndexState::Paused).await;
    assert_eq!(
        rig.registry
            .route_cancel(&key, broca_protocol::RunId("r1".into()))
            .await
            .unwrap(),
        CancelOutcome::Ack
    );

    wait_for_resolved_state(&rig, &key, "r1", RunIndexState::Cancelled).await;
    wait_for_resolved_state(&rig, &key, "r2", RunIndexState::Completed).await;
}

#[tokio::test]
async fn paused_cancel_after_actor_reopen_uses_durable_state() {
    let exec = FakeExecutor::scripted(vec![RunScript::FreshPaused, RunScript::ResumeCancelled]);
    let rig = Rig::new("paused-cancel-reopen", exec, SHORT_TTL);
    let key = rig.key("s1");

    rig.registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    wait_for_resolved_state(&rig, &key, "r1", RunIndexState::Paused).await;
    wait_for_actor_close(&rig, &key).await;

    assert_eq!(
        rig.registry
            .route_cancel(&key, broca_protocol::RunId("r1".into()))
            .await
            .unwrap(),
        CancelOutcome::Ack
    );
    wait_for_resolved_state(&rig, &key, "r1", RunIndexState::Cancelled).await;
}

#[tokio::test]
async fn active_cancel_wins_the_pause_completion_race() {
    let (exec, gate) = FakeExecutor::scripted_gated(vec![
        RunScript::FreshPausedAfterGate,
        RunScript::ResumeCancelled,
    ]);
    let rig = Rig::new("paused-cancel-race", exec, LONG_TTL);
    let key = rig.key("s1");

    rig.registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert_eq!(
        rig.registry
            .route_cancel(&key, broca_protocol::RunId("r1".into()))
            .await
            .unwrap(),
        CancelOutcome::Ack
    );
    gate.release().await;

    wait_for_resolved_state(&rig, &key, "r1", RunIndexState::Cancelled).await;
}

/// Session send opens a renderer-drifted resume through the real core loop. Core must seal its
/// own terminal so WAL replay, subscribers, and the run index all agree on Interrupted.
#[tokio::test]
async fn renderer_drift_reaches_subscriber_and_run_status() {
    use broca_module::fanout::{Delivery, FromCursor};
    use broca_protocol::caller::{ControlUnit, RunFinishReason};

    static NEXT_DIR: AtomicUsize = AtomicUsize::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-coord-core-renderer-drift-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let key = broca_protocol::BindIdentity::new(dir.join("proj"), "runner", "s1");
    let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());

    let request = req(&key, "r-drift");
    let mut config = min_run_config();
    config.frozen_render_config = serde_json::json!({"render_config": "stateless"});
    let mut wal = wal_store.open(&key).expect("open renderer-drift WAL");
    wal.append(
        1,
        &broca_wal::WalRecord::RunStarted {
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
            ts_ms: Some(100),
            run_id: request.run_id.clone(),
            session: key.clone(),
            config: Box::new(config),
            input: request.input.clone().into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
        true,
    )
    .expect("seed synced unfinished episode");
    drop(wal);

    let provider = Arc::new(RendererV2Provider {
        calls: AtomicUsize::new(0),
    });
    let executor = Arc::new(CoreRendererDriftExecutor {
        wal_store: wal_store.clone(),
        store: store.clone(),
        provider: provider.clone(),
    });
    let charge_basis_resolver: Arc<dyn broca_core::ChargeBasisResolver + Send + Sync> =
        Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1));
    let opener = Arc::new(
        broca_module::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store.clone(),
            executor,
        )
        .with_idle_ttl(LONG_TTL)
        .with_run_index(store.clone())
        .with_materialized_store(store.clone())
        .with_export_index(store.clone())
        .with_charge_basis_resolver(charge_basis_resolver),
    );
    let registry = broca_module::registry::LineageRegistry::new_with_reader(
        opener,
        wal_store.clone(),
        Some(store.clone()),
    );

    let outcome = registry
        .route_send(&key, req(&key, "r-next"), RunKind::FreshOrResume)
        .await
        .unwrap();
    assert!(
        match &outcome {
            SendOutcome::Pending { .. } => true,
            SendOutcome::Active { run_id, .. } => run_id.0 == "r-next",
            _ => false,
        },
        "renderer-drift recovery send returned {outcome:?}"
    );

    broca_test_support::wait_for(
        "renderer_drift_reaches_subscriber_and_run_status",
        async {
            loop {
                if store.lookup_run("r-drift").unwrap().map(|row| row.state)
                    == Some(RunIndexState::Interrupted)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
        || {
            format!(
                "index={:?}",
                store.lookup_run("r-drift").map(|r| r.map(|r| r.state))
            )
        },
    )
    .await;
    assert_eq!(
        store.lookup_run("r-drift").unwrap().map(|row| row.state),
        Some(RunIndexState::Interrupted)
    );

    let (sink, mut deliveries) = tokio::sync::mpsc::channel(16);
    registry
        .route_subscribe(&key, FromCursor::Start, sink)
        .await
        .unwrap();
    let mut typed_error = false;
    let mut terminal = false;
    for _ in 0..8 {
        let delivery = broca_test_support::event_timeout(
            "renderer_drift_reaches_subscriber_and_run_status",
            deliveries.recv(),
            || {
                format!(
                    "slot={:?}, in_flight={}",
                    registry.slot_state(&key),
                    registry.slot_counts().runs_in_flight
                )
            },
        )
        .await
        .expect("drift projection must not wedge")
        .expect("subscriber remains attached");
        if let Delivery::Control(event) = delivery {
            match &event.1 {
                ControlUnit::Error { error } => {
                    typed_error = error.provider_code.as_deref() == Some("renderer_version_drift")
                        && error.message.contains("renderer version differs");
                }
                ControlUnit::RunFinished { reason, .. } => {
                    terminal = *reason == RunFinishReason::Interrupted;
                    break;
                }
                _ => {}
            }
        }
    }
    assert!(
        typed_error,
        "subscriber receives the typed drift error from the WAL"
    );
    assert!(
        terminal,
        "subscriber receives the Interrupted terminal from the WAL"
    );

    let status = registry
        .route_run_state(&key, broca_protocol::RunId("r-drift".into()))
        .await
        .unwrap()
        .expect("drift run resolves");
    assert_eq!(status.state, RunIndexState::Interrupted);

    let records = wal_store
        .read_records(&key)
        .expect("read core-sealed renderer-drift WAL");
    let drift_terminal_index = records
        .iter()
        .position(|frame| {
            matches!(
                &frame.record,
                broca_wal::WalRecord::RunFinished {
                    reason: RunFinishReason::Interrupted,
                    error: Some(error),
                    ..
                } if error.provider_code.as_deref() == Some("renderer_version_drift")
            )
        })
        .expect("core writes the drift terminal");
    assert!(
        records[..drift_terminal_index]
            .iter()
            .all(|frame| !matches!(frame.record, broca_wal::WalRecord::StepStarted { .. })),
        "the resumed drift exit makes no provider call"
    );
    assert_eq!(
        records
            .iter()
            .filter(|frame| matches!(
                &frame.record,
                broca_wal::WalRecord::RunFinished {
                    reason: RunFinishReason::Interrupted,
                    error: Some(error),
                    ..
                } if error.provider_code.as_deref() == Some("renderer_version_drift")
            ))
            .count(),
        1,
        "core writes exactly one drift terminal"
    );
    let terminal_ts_ms = match &records[drift_terminal_index].record {
        broca_wal::WalRecord::RunFinished { ts_ms, .. } => {
            ts_ms.expect("core terminal carries real wall-clock time")
        }
        _ => unreachable!(),
    };
    let replayed = broca_core::replay(
        &records[..=drift_terminal_index]
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>(),
    )
    .expect("core-sealed drift WAL prefix replays");
    assert!(matches!(
        replayed.resolution,
        broca_core::ReplayResolution::LineageIdle {
            last_reason: RunFinishReason::Interrupted
        }
    ));

    let fact = broca_test_support::event_timeout(
        "renderer_drift_reaches_subscriber_and_run_status",
        async {
            loop {
                let page = store
                    .read_export_page(None, 10)
                    .expect("read renderer-drift usage export");
                if let Some(fact) = page
                    .facts
                    .into_iter()
                    .find(|fact| fact.segment.run_id.0 == "r-drift")
                {
                    return fact;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || {
            format!(
                "slot={:?}, in_flight={}",
                registry.slot_state(&key),
                registry.slot_counts().runs_in_flight
            )
        },
    )
    .await
    .expect("renderer-drift terminal eventually emits an immutable usage fact");
    assert_eq!(
        fact.segment.occurred_at_ms,
        Some(terminal_ts_ms),
        "billing preserves the durable terminal timestamp for period attribution"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// After recovery seals the original deterministic send because the current renderer version no
/// longer matches its frozen version, retrying that send ID is a historical no-op. A new send ID
/// still opens the next episode and reaches the provider.
#[tokio::test]
async fn same_send_id_after_renderer_drift_is_an_idempotent_terminal_noop() {
    use broca_protocol::caller::RunFinishReason;
    use broca_wal::WalRecord;

    static NEXT_DIR: AtomicUsize = AtomicUsize::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-coord-renderer-drift-redrive-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    let key = broca_protocol::BindIdentity::new(dir.join("proj"), "runner", "s1");
    let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
    let original = req(&key, "r-drift");
    let request_identity_digest = broca_core::submission_identity_digest(&original.to_submission());

    store
        .index_run_active(&RunIndexRow {
            withdrawals_owed: false,
            title: None,
            run_id: original.run_id.0.clone(),
            session: key.clone(),
            submission_id: None,
            request_identity_digest: Some(request_identity_digest.clone()),
            state: RunIndexState::Active,
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
        })
        .unwrap();
    let mut config = min_run_config();
    config.frozen_render_config = serde_json::json!({"render_config": "stateless"});
    let mut wal = wal_store.open(&key).expect("open renderer-drift WAL");
    wal.append(
        1,
        &WalRecord::RunStarted {
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
            ts_ms: Some(100),
            run_id: original.run_id.clone(),
            session: key.clone(),
            config: Box::new(config),
            input: original.input.clone().into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
        true,
    )
    .expect("seed synced unfinished episode");
    drop(wal);

    let provider = Arc::new(RendererV2Provider {
        calls: AtomicUsize::new(0),
    });
    let executor = Arc::new(CoreRendererDriftExecutor {
        wal_store: wal_store.clone(),
        store: store.clone(),
        provider: provider.clone(),
    });
    let charge_basis_resolver: Arc<dyn broca_core::ChargeBasisResolver + Send + Sync> =
        Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1));
    let opener = Arc::new(
        broca_module::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store.clone(),
            executor,
        )
        .with_idle_ttl(LONG_TTL)
        .with_run_index(store.clone())
        .with_materialized_store(store.clone())
        .with_export_index(store.clone())
        .with_charge_basis_resolver(charge_basis_resolver),
    );
    let registry = broca_module::registry::LineageRegistry::new_with_reader(
        opener,
        wal_store.clone(),
        Some(store.clone()),
    );

    // Looking up the original run starts recovery of its unfinished durable episode. A renderer
    // version mismatch marks that run Interrupted before any provider call instead of creating a
    // replacement episode.
    let _ = registry
        .route_run_state(&key, original.run_id.clone())
        .await
        .expect("open the lineage and start recovery");
    broca_test_support::wait_for(
        "same_send_id_after_renderer_drift_is_an_idempotent_terminal_noop",
        async {
            loop {
                if store.lookup_run("r-drift").unwrap().map(|row| row.state)
                    == Some(RunIndexState::Interrupted)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
        || {
            format!(
                "index={:?}",
                store.lookup_run("r-drift").map(|r| r.map(|r| r.state))
            )
        },
    )
    .await;
    let drift_row = store
        .lookup_run("r-drift")
        .unwrap()
        .expect("drift run remains indexed");
    assert_eq!(drift_row.state, RunIndexState::Interrupted);
    assert_eq!(
        drift_row.request_identity_digest,
        Some(request_identity_digest),
        "recovery preserves the durable request identity used by historical retries"
    );
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        0,
        "renderer drift seals before provider execution"
    );

    let before_retry = wal_store
        .read_records(&key)
        .expect("read sealed renderer-drift WAL");
    assert_eq!(
        before_retry
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::RunStarted { .. }))
            .count(),
        1
    );
    assert_eq!(
        before_retry
            .iter()
            .filter(|frame| matches!(
                &frame.record,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Interrupted,
                    error: Some(error),
                    ..
                } if error.provider_code.as_deref() == Some("renderer_version_drift")
            ))
            .count(),
        1
    );

    let retry = registry
        .route_send(&key, original, RunKind::FreshOrResume)
        .await
        .expect("route the identical deterministic retry");
    assert_eq!(
        retry,
        SendOutcome::Finished {
            reason: broca_protocol::RunFinishReason::Interrupted,
            run_id: broca_protocol::RunId("r-drift".into()),
            delivered: None,
        },
        "the same send identity resolves to its existing Interrupted run"
    );
    assert_eq!(
        wal_store.read_records(&key).unwrap(),
        before_retry,
        "the historical retry writes no second RunStarted or terminal"
    );
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        0,
        "the historical retry never reaches the provider"
    );

    let fresh = registry
        .route_send(&key, req(&key, "r-next"), RunKind::FreshOrResume)
        .await
        .expect("route a genuinely fresh send identity");
    assert!(
        matches!(
            fresh,
            SendOutcome::Active {
                run_id: broca_protocol::RunId(ref id), ..
            } if id == "r-next"
        ),
        "a fresh send identity opens the next episode"
    );
    broca_test_support::wait_for(
        "same_send_id_after_renderer_drift_is_an_idempotent_terminal_noop",
        async {
            loop {
                if store.lookup_run("r-next").unwrap().map(|row| row.state)
                    == Some(RunIndexState::Completed)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
        || {
            format!(
                "index={:?}",
                store.lookup_run("r-next").map(|r| r.map(|r| r.state))
            )
        },
    )
    .await;
    assert_eq!(
        store.lookup_run("r-next").unwrap().map(|row| row.state),
        Some(RunIndexState::Completed)
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let after_fresh = wal_store.read_records(&key).unwrap();
    assert_eq!(
        after_fresh
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::RunStarted { .. }))
            .count(),
        2,
        "only the genuinely fresh send appends a new episode"
    );
    assert_eq!(
        after_fresh
            .iter()
            .filter(|frame| matches!(
                &frame.record,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Interrupted,
                    error: Some(error),
                    ..
                } if error.provider_code.as_deref() == Some("renderer_version_drift")
            ))
            .count(),
        1,
        "the drift episode keeps exactly one terminal"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A paused run can later resume and complete, replacing the non-terminal `Paused` index row
/// with the final terminal state for the same run_id.
#[tokio::test]
async fn paused_run_resume_updates_index_to_completed() {
    let exec = FakeExecutor::scripted(vec![RunScript::FreshPaused, RunScript::ResumeCompleted]);
    let rig = Rig::new("paused-resume", exec.clone(), LONG_TTL);
    let key = rig.key("s1");

    let first = rig
        .registry
        .route_send(&key, req(&key, "r1"), RunKind::FreshOrResume)
        .await
        .unwrap();
    let run_id = match first {
        SendOutcome::Active { run_id, .. } => run_id,
        other => panic!("expected Active, got {other:?}"),
    };
    wait_for_index_state(&rig, &run_id.0, RunIndexState::Paused).await;

    let resumed = rig
        .registry
        .route_send(&key, req(&key, &run_id.0), RunKind::ResumeAfterAuth)
        .await
        .unwrap();
    let resumed_run_id = match resumed {
        SendOutcome::Active { run_id, .. } => run_id,
        other => panic!("expected Active, got {other:?}"),
    };
    assert_eq!(resumed_run_id, run_id, "resume keeps the same run_id");

    wait_for_index_state(&rig, &run_id.0, RunIndexState::Completed).await;
}

/// THE STALE-ACTIVE RECONCILIATION (the crash-window proof): the run index is a DERIVED
/// projection; a crash between a run's durable RunFinished and the index terminal write
/// leaves the index row Active for a FINISHED run. A run-state lookup with no live actor
/// backing the Active row must NOT trust it — it reconciles against the WAL (the authority)
/// and reports the real terminal.
#[tokio::test]
async fn stale_active_reconciles_to_terminal_from_wal() {
    use broca_protocol::{caller::RunFinishReason, RunId, Usage};
    let exec = FakeExecutor::auto();
    let rig = Rig::new("stale-active", exec.clone(), LONG_TTL);
    let key = rig.key("s1");
    let terminal_usage = Usage {
        input_tokens: Some(13),
        cached_input_tokens: Some(2),
        cache_write_tokens: Some(3),
        output_tokens: Some(5),
        reasoning_tokens: Some(1),
    };

    // Simulate the crash window directly: the index has an ACTIVE row for run "r1", and the
    // WAL has a RunStarted + RunFinished{Completed} for it — but the index terminal write
    // was LOST (the crash). There is NO live actor running "r1".
    rig.run_index
        .index_run_active(&broca_core::RunIndexRow {
            withdrawals_owed: false,
            title: None,
            run_id: "r1".into(),
            session: key.clone(),
            submission_id: None,
            request_identity_digest: None,
            state: RunIndexState::Active, // STALE: the run actually finished (see WAL below)
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
        })
        .unwrap();

    // Write the run's real terminal to the WAL (what a finished run durably recorded). Use a
    // direct WAL handle so we model "the WAL has the terminal, the index does not".
    {
        use broca_core::{LeaseStore, WalHandle, WalStore};
        let lease_store = broca_core::FileLeaseStore::new(rig.dir.join("leases"));
        let wal_store = broca_core::FileWalStore::new(rig.dir.join("wal"));
        let lease = lease_store
            .acquire(&broca_core::lease_key_for(&key))
            .expect("acquire lease to seed the WAL");
        let wal = wal_store.open(&key).expect("open wal");
        let writer = WalHandle::spawn(wal, lease.epoch()).expect("spawn writer");
        writer.record(broca_wal::WalRecord::RunStarted {
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
            run_id: RunId("r1".into()),
            session: key.clone(),
            config: Box::new(min_run_config()),
            input: vec![].into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        });
        writer.record(broca_wal::WalRecord::ModelStepFinished {
            tags: None,
            hooks: None,
            step_id: broca_protocol::StepId(1),
            assistant_message: broca_protocol::AssistantMessage {
                message_id: broca_protocol::MessageId("m1".into()),
                content: vec![broca_protocol::ContentBlock::Text {
                    text: "truncated".into(),
                }],
            }
            .into(),
            usage: Usage::default(),
            finish_reason: Some(broca_protocol::FinishReason::Length),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        });
        writer.record(broca_wal::WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: terminal_usage,
            error: None,
            retries_used: 0,
        });
        writer.sync().await.unwrap();
        let _ = writer.shutdown().await;
        drop(lease); // release so the actor the lookup opens can acquire it
    }

    // The lookup opens an actor (no run active on it), the index says Active, but no live
    // actor backs it → reconcile from the WAL → Completed, NOT the stale Active.
    let state = rig
        .registry
        .route_run_state(&key, broca_protocol::RunId("r1".into()))
        .await
        .unwrap();
    let state = state.expect("WAL run resolves");
    assert_eq!(
        state.state,
        RunIndexState::Completed,
        "a stale-Active index row (no live actor) must reconcile to the WAL's real terminal, \
         not report the run as forever-Active (the projection-staleness hole)"
    );
    assert_eq!(
        state.final_step_finish_reason,
        Some(broca_protocol::FinishReason::Length),
        "WAL reconciliation carries terminal step metadata absent from the run index"
    );
    assert_eq!(state.usage, Some(terminal_usage));

    // Exercise the ordinary terminal-index path as well. A metadata-bearing terminal row is
    // authoritative without replaying the matching WAL episode.
    rig.run_index
        .index_run_terminal(
            "r1",
            RunIndexState::Completed,
            Some(terminal_usage),
            Some(broca_protocol::FinishReason::Length),
            0,
        )
        .unwrap();
    let indexed = rig
        .registry
        .route_run_state(&key, broca_protocol::RunId("r1".into()))
        .await
        .unwrap()
        .expect("indexed run resolves");
    assert_eq!(
        indexed.final_step_finish_reason,
        Some(broca_protocol::FinishReason::Length),
        "terminal-index status preserves the final provider finish reason"
    );
    assert_eq!(
        indexed.usage,
        Some(terminal_usage),
        "terminal-index status preserves the indexed run-total usage"
    );
}

/// If the process crashes after fsyncing the WAL terminal that marks a run Interrupted because its
/// renderer version changed, but before updating the derived index, a run-state lookup repairs the
/// stale Active index row from the WAL authority.
#[tokio::test]
async fn stale_active_reconciles_renderer_drift_terminal_from_wal() {
    use broca_core::{LeaseStore, WalHandle};
    use broca_protocol::{
        caller::RunFinishReason,
        error::{ErrorClass, ProviderError},
        Usage,
    };

    let exec = FakeExecutor::auto();
    let rig = Rig::new("stale-active-renderer-drift", exec.clone(), LONG_TTL);
    let key = rig.key("s1");
    let original = req(&key, "r-drift");
    let request_identity_digest = broca_core::submission_identity_digest(&original.to_submission());
    rig.run_index
        .index_run_active(&RunIndexRow {
            withdrawals_owed: false,
            title: None,
            run_id: original.run_id.0.clone(),
            session: key.clone(),
            submission_id: None,
            request_identity_digest: Some(request_identity_digest),
            state: RunIndexState::Active,
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
        })
        .unwrap();

    let lease_store = FileLeaseStore::new(rig.dir.join("leases"));
    let wal_store = FileWalStore::new(rig.dir.join("wal"));
    let lease = lease_store
        .acquire(&broca_core::lease_key_for(&key))
        .expect("acquire lease to seed the crash cut");
    let writer = WalHandle::spawn(wal_store.open(&key).expect("open WAL"), lease.epoch())
        .expect("spawn writer");
    let mut config = min_run_config();
    config.frozen_render_config = serde_json::json!({"render_config": "stateless"});
    writer.record(broca_wal::WalRecord::RunStarted {
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
        ts_ms: Some(100),
        run_id: original.run_id.clone(),
        session: key.clone(),
        config: Box::new(config),
        input: original.input.into(),
        origin: broca_wal::RunOrigin::Fresh,
        submission_id: None,
    });
    writer.record(broca_wal::WalRecord::RunFinished {
        ts_ms: Some(101),
        reason: RunFinishReason::Interrupted,
        usage: Usage::default(),
        error: Some(ProviderError {
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Permanent,
            message: "resume refused: the provider renderer version differs from the version recorded at run start".into(),
            status: None,
            retry_after_secs: None,
            provider_code: Some("renderer_version_drift".into()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        }),
        retries_used: 0,
    });
    writer.sync().await.expect("fsync the core-sealed terminal");
    let _ = writer.shutdown().await;
    drop(lease);

    assert_eq!(
        rig.registry.slot_state(&key),
        None,
        "no live actor backs the stale Active row"
    );
    let state = rig
        .registry
        .route_run_state(&key, original.run_id)
        .await
        .unwrap()
        .expect("renderer-drift run resolves from the WAL");
    assert_eq!(state.state, RunIndexState::Interrupted);
    assert_eq!(
        state.usage, None,
        "a renderer-drift terminal has no ModelStepFinished, even though its durable usage is zero"
    );
    assert_eq!(
        exec.started_count(),
        0,
        "a durable drift terminal reconciles without re-driving the executor"
    );
}

#[derive(Clone, Copy)]
enum UnsealedTerminal {
    NaturalEnd,
    HardStop,
}

fn seed_unsealed_run_with_queue(
    rig: &Rig,
    key: &broca_protocol::BindIdentity,
    terminal: UnsealedTerminal,
) {
    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));
    let mut wal = store.open(key).expect("open WAL to seed crash cut");
    let call_id = broca_protocol::ToolCallId("t1".into());
    let batch_id = broca_protocol::BatchId("r1-b1".into());
    let assistant_message = match terminal {
        UnsealedTerminal::NaturalEnd => broca_protocol::AssistantMessage {
            message_id: broca_protocol::MessageId("m1".into()),
            content: vec![broca_protocol::ContentBlock::Text {
                text: "r1 answer".into(),
            }],
        },
        UnsealedTerminal::HardStop => broca_protocol::AssistantMessage {
            message_id: broca_protocol::MessageId("m1".into()),
            content: vec![broca_protocol::ContentBlock::ToolCall(
                broca_protocol::ToolCall {
                    tool_call_id: call_id.clone(),
                    tool_name: "edit".into(),
                    input: serde_json::json!({"path": "file"}),
                    thought_signature: None,
                },
            )],
        },
    };
    let mut records = vec![
        broca_wal::WalRecord::RunStarted {
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
            run_id: broca_protocol::RunId("r1".into()),
            session: key.clone(),
            config: Box::new(min_run_config()),
            input: req(key, "r1").input.into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
        broca_wal::WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
        broca_wal::WalRecord::ModelStepFinished {
            tags: None,
            hooks: None,
            step_id: broca_protocol::StepId(1),
            assistant_message: assistant_message.into(),
            usage: broca_protocol::Usage::default(),
            finish_reason: Some(match terminal {
                UnsealedTerminal::NaturalEnd => broca_protocol::FinishReason::Stop,
                UnsealedTerminal::HardStop => broca_protocol::FinishReason::ToolCalls,
            }),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        },
    ];
    if matches!(terminal, UnsealedTerminal::HardStop) {
        records.extend([
            broca_wal::WalRecord::ToolBatchStarted {
                batch_id: batch_id.clone(),
                step_id: broca_protocol::StepId(1),
                planned_calls: vec![call_id.clone()],
            },
            broca_wal::WalRecord::ToolResult {
                tags: None,
                hooks: None,
                batch_id: batch_id.clone(),
                result: broca_protocol::ToolResult {
                    tool_call_id: call_id,
                    output: broca_protocol::ToolOutput::Error {
                        message: "tool not run: max steps reached before dispatch".into(),
                    },
                    is_error: true,
                }
                .into(),
                outcome: broca_wal::DispatchOutcome::MaxSteps,
                reason: None,
                withdraw_elicitation: None,
                provider_held: None,
            },
            broca_wal::WalRecord::ToolBatchFinished { batch_id },
        ]);
    }
    records.push(broca_wal::WalRecord::Queued {
        retention: None,
        ts_ms: None,
        hooks: None,
        title: None,
        keep_warm: None,
        conversation_key: None,
        send_id: None,
        delivery: None,
        submission_id: broca_protocol::SubmissionId("sub-r2".into()),
        request: Box::new(req(key, "r2").to_submission()),
        request_identity_digest: None,
        principal: None,
    });
    for record in records {
        wal.append(1, &record, true).expect("seed crash-cut record");
    }
}

fn assert_unsealed_append_would_corrupt(rig: &Rig, key: &broca_protocol::BindIdentity) {
    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));
    let mut frames = store.read_records(key).expect("read crash cut");
    frames.push(broca_wal::FramedRecord {
        seq: frames.last().map_or(1, |frame| frame.seq + 1),
        fence: 1,
        record: broca_wal::WalRecord::RunStarted {
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
            run_id: broca_protocol::RunId("r2".into()),
            session: key.clone(),
            config: Box::new(min_run_config()),
            input: req(key, "r2").input.into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: Some(broca_protocol::SubmissionId("sub-r2".into())),
        },
    });
    let records = frames
        .iter()
        .map(|frame| frame.record.clone())
        .collect::<Vec<_>>();
    assert!(
        broca_core::replay(&records).is_err(),
        "the former queue-first ordering must be recognized as corrupt"
    );
    assert!(
        broca_core::AdmissionCheckpointV1::from_frames(&frames).is_err(),
        "admission folding must independently reject the former ordering"
    );
}

fn repaired_frames(
    rig: &Rig,
    key: &broca_protocol::BindIdentity,
    expected_seal: broca_protocol::caller::RunFinishReason,
) -> Vec<broca_wal::FramedRecord> {
    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));
    let frames = store.read_records(key).expect("read repaired WAL");
    let seal = frames
        .iter()
        .position(|frame| {
            matches!(
                frame.record,
                broca_wal::WalRecord::RunFinished { reason, .. } if reason == expected_seal
            )
        })
        .expect("r1 receives its durable seal");
    let next_start = frames
        .iter()
        .position(|frame| {
            matches!(
                &frame.record,
                broca_wal::WalRecord::RunStarted { run_id, .. } if run_id.0 == "r2"
            )
        })
        .expect("queued r2 starts");
    assert!(
        seal < next_start,
        "RunFinished(r1) must precede RunStarted(r2)"
    );
    broca_core::AdmissionCheckpointV1::from_frames(&frames)
        .expect("the repaired lineage folds into an admission checkpoint");
    broca_core::replay(
        &frames
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>(),
    )
    .expect("the repaired lineage replays");
    frames
}

fn seed_completed_run_with_queue(
    rig: &Rig,
    key: &broca_protocol::BindIdentity,
    queued_run_ids: &[&str],
) {
    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));
    let mut wal = store.open(key).expect("open WAL to seed restart state");
    let mut seq = 1;
    wal.append(
        seq,
        &broca_wal::WalRecord::RunStarted {
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
            run_id: broca_protocol::RunId("a".into()),
            session: key.clone(),
            config: Box::new(min_run_config()),
            input: req(key, "a").input.into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        },
        true,
    )
    .expect("seed completed run start");
    seq += 1;
    wal.append(
        seq,
        &broca_wal::WalRecord::RunFinished {
            ts_ms: None,
            reason: broca_protocol::caller::RunFinishReason::Completed,
            usage: broca_protocol::Usage::default(),
            error: None,
            retries_used: 0,
        },
        true,
    )
    .expect("seed completed run terminal");
    for run_id in queued_run_ids {
        seq += 1;
        wal.append(
            seq,
            &broca_wal::WalRecord::Queued {
                retention: None,
                ts_ms: None,
                hooks: None,
                title: None,
                keep_warm: None,
                conversation_key: None,
                send_id: None,
                delivery: None,
                submission_id: broca_protocol::SubmissionId(format!("sub-{run_id}")),
                request: Box::new(req(key, run_id).to_submission()),
                request_identity_digest: None,
                principal: None,
            },
            true,
        )
        .expect("seed queued send");
    }
}

fn min_run_config() -> broca_wal::RunConfig {
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

fn tool_surface(label: &str) -> Vec<broca_protocol::request::Tool> {
    vec![broca_protocol::request::Tool {
        name: "edit".into(),
        description: label.into(),
        input_schema: serde_json::json!({"type": "object"}),
        module: Some("aft".into()),
        execution_mode: broca_protocol::request::ExecutionMode::Parallel,
    }]
}

fn submission_with_tools(
    key: &broca_protocol::BindIdentity,
    run_id: &str,
    tools: Option<Vec<broca_protocol::request::Tool>>,
) -> broca_protocol::request::SubmissionRequest {
    let mut request = req(key, run_id).to_submission();
    request.policy_allowed = tools
        .as_ref()
        .map(|surface| surface.iter().map(|tool| tool.name.clone()).collect())
        .unwrap_or_default();
    request.tools = tools;
    request
}

async fn wait_until_idle(rig: &Rig, key: &broca_protocol::BindIdentity) {
    broca_test_support::wait_for(
        "wait_until_idle",
        async {
            loop {
                if rig.registry.route_status(key).await.unwrap() == LineageStatus::Idle {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || {
            format!(
                "slot={:?}, started={}, finished={}",
                rig.registry.slot_state(key),
                rig.exec.started_count(),
                rig.exec.finished_count()
            )
        },
    )
    .await
}

fn frozen_tool_configs(rig: &Rig, key: &broca_protocol::BindIdentity) -> Vec<broca_wal::RunConfig> {
    use broca_core::WalStore;

    broca_core::FileWalStore::new(rig.dir.join("wal"))
        .read_records(key)
        .unwrap()
        .into_iter()
        .filter_map(|frame| match frame.record {
            broca_wal::WalRecord::RunStarted { config, .. } => Some(*config),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn omitted_tools_inherit_the_last_frozen_surface() {
    let rig = Rig::new("tools-inherit", FakeExecutor::auto(), LONG_TTL);
    let key = rig.key("s1");
    let original = tool_surface("original provider surface");

    rig.registry
        .route_submission(
            &key,
            broca_protocol::RunId("r1".into()),
            submission_with_tools(&key, "r1", Some(original.clone())),
            RunKind::FreshOrResume,
            true,
        )
        .await
        .unwrap();
    wait_until_idle(&rig, &key).await;

    for run_id in ["r2", "r3"] {
        // A provider upgrade could produce a different explicit surface here, but omission
        // deliberately keeps inheriting the old frozen bytes until a client re-sends tools.
        let _upgraded_surface = tool_surface("upgraded provider surface");
        rig.registry
            .route_submission(
                &key,
                broca_protocol::RunId(run_id.into()),
                submission_with_tools(&key, run_id, None),
                RunKind::FreshOrResume,
                true,
            )
            .await
            .unwrap();
        wait_until_idle(&rig, &key).await;
    }

    rig.registry
        .route_submission(
            &key,
            broca_protocol::RunId("r4".into()),
            submission_with_tools(&key, "r4", Some(Vec::new())),
            RunKind::FreshOrResume,
            true,
        )
        .await
        .unwrap();
    wait_until_idle(&rig, &key).await;
    rig.registry
        .route_submission(
            &key,
            broca_protocol::RunId("r5".into()),
            submission_with_tools(&key, "r5", None),
            RunKind::FreshOrResume,
            true,
        )
        .await
        .unwrap();
    wait_until_idle(&rig, &key).await;

    let configs = frozen_tool_configs(&rig, &key);
    assert_eq!(configs.len(), 5);
    assert_eq!(configs[0].tools.as_ref(), &original);
    let original_bytes = serde_json::to_vec(&configs[0].tools).unwrap();
    assert_eq!(
        serde_json::to_vec(&configs[1].tools).unwrap(),
        original_bytes
    );
    assert_eq!(
        serde_json::to_vec(&configs[2].tools).unwrap(),
        original_bytes
    );
    assert!(!configs[0].tools_were_omitted);
    assert!(configs[1].tools_were_omitted);
    assert!(configs[2].tools_were_omitted);
    assert!(configs[3].tools.is_empty());
    assert!(!configs[3].tools_were_omitted);
    assert!(configs[4].tools.is_empty());
    assert!(configs[4].tools_were_omitted);
}

#[tokio::test]
async fn fresh_omission_and_queued_omission_resolve_at_episode_start() {
    let fresh = Rig::new("tools-fresh-none", FakeExecutor::auto(), LONG_TTL);
    let fresh_key = fresh.key("s1");
    fresh
        .registry
        .route_submission(
            &fresh_key,
            broca_protocol::RunId("fresh".into()),
            submission_with_tools(&fresh_key, "fresh", None),
            RunKind::FreshOrResume,
            true,
        )
        .await
        .unwrap();
    wait_until_idle(&fresh, &fresh_key).await;
    let fresh_configs = frozen_tool_configs(&fresh, &fresh_key);
    assert!(fresh_configs[0].tools.is_empty());
    assert!(fresh_configs[0].tools_were_omitted);

    let (executor, gate) = FakeExecutor::gated();
    let queued = Rig::new("tools-queued-none", executor.clone(), LONG_TTL);
    let queued_key = queued.key("s1");
    let original = tool_surface("queued inherited surface");
    queued
        .registry
        .route_submission(
            &queued_key,
            broca_protocol::RunId("r1".into()),
            submission_with_tools(&queued_key, "r1", Some(original.clone())),
            RunKind::FreshOrResume,
            true,
        )
        .await
        .unwrap();
    let pending = queued
        .registry
        .route_submission(
            &queued_key,
            broca_protocol::RunId("r2".into()),
            submission_with_tools(&queued_key, "r2", None),
            RunKind::FreshOrResume,
            true,
        )
        .await
        .unwrap();
    assert!(matches!(pending, SendOutcome::Pending { .. }));
    {
        use broca_core::WalStore;
        let records = broca_core::FileWalStore::new(queued.dir.join("wal"))
            .read_records(&queued_key)
            .unwrap();
        let durable_omission = records.into_iter().any(|frame| {
            matches!(
                frame.record,
                broca_wal::WalRecord::Queued { request, .. } if request.tools.is_none()
            )
        });
        assert!(durable_omission);
    }

    gate.release().await;
    broca_test_support::wait_for(
        "fresh_omission_and_queued_omission_resolve_at_episode_start",
        async {
            loop {
                if executor.started_count() >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || {
            format!(
                "started={}, finished={}",
                executor.started_count(),
                executor.finished_count()
            )
        },
    )
    .await;
    gate.release().await;
    broca_test_support::wait_for(
        "fresh_omission_and_queued_omission_resolve_at_episode_start",
        async {
            loop {
                if executor.finished_count() >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || {
            format!(
                "started={}, finished={}",
                executor.started_count(),
                executor.finished_count()
            )
        },
    )
    .await;
    wait_until_idle(&queued, &queued_key).await;
    let queued_configs = frozen_tool_configs(&queued, &queued_key);
    assert_eq!(queued_configs.len(), 2);
    assert_eq!(queued_configs[1].tools.as_ref(), &original);
    assert!(queued_configs[1].tools_were_omitted);
}
