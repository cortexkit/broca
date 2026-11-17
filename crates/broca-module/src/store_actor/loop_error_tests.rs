//! A run whose loop fails after its start, without writing an end, is ended by the store actor
//! with the error, so the session serves again in the same process: its queued sends drain, and
//! a new send starts a run.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use broca_core::provider::{Provider, StubProvider};
use broca_core::{FileLeaseStore, OutputEvent, RunError};
use broca_protocol::request::CallOptions;
use broca_protocol::{BindIdentity, NormalizedEvent, RunId};
use broca_session_loop::LoopError;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Notify};

use super::tests::{request, text_step, Caller};
use crate::actor::{SendKey, SendOutcome};
use crate::executor::{
    RunExecutor, RunKind, StoreRunEnd, StoreRunError, StoreRunJob, StoreRunMode,
};
use crate::registry::LineageRegistry;
use crate::serve::ServeHandler as _;

/// The error the failing run's loop stops with.
const LOOP_FAILED: &str = "the run's request could not be prepared";

/// How long the session's actor stays open with no command: longer than any test here, so one
/// actor serves the whole test, as a live session's actor stays open while its caller keeps
/// subscribing. An actor opened again would recover the failed run on its own.
const IDLE: std::time::Duration = std::time::Duration::from_secs(600);

/// A scripted model, except for a run told to stall: its model call never answers.
struct Stalling {
    script: Arc<StubProvider>,
    stalls: bool,
}

#[async_trait]
impl Provider for Stalling {
    async fn step(
        &self,
        step_id: broca_protocol::StepId,
        opts: &CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        if self.stalls {
            std::future::pending::<()>().await;
        }
        self.script.step(step_id, opts).await
    }
}

/// Runs each store job through the session loop (`broca_session_loop`). The next fresh run, when
/// `fails_next` is set, has its model call stall, and its loop stops with [`LOOP_FAILED`] once
/// `fail` is notified: the run's start and step are written, and no end.
/// When set for a resume, `fails_next` refuses admission instead, before the loop is called.
struct FailingExecutor {
    script: Arc<StubProvider>,
    fails_next: AtomicBool,
    temporarily_refuse_resume: AtomicBool,
    fail: Notify,
    /// When set, the next fresh run's admission is refused because routing couldn't be reached,
    /// before anything of the run is written.
    routing_unreachable_next: AtomicBool,
}

#[async_trait]
impl RunExecutor for FailingExecutor {
    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        let StoreRunJob {
            mut req,
            control,
            mut writer,
            mut parts,
            mode,
            ..
        } = job;
        let fails = self.fails_next.swap(false, Ordering::SeqCst);
        let provider = Stalling {
            script: Arc::clone(&self.script),
            stalls: fails,
        };
        let result = async {
            let tools = broca_core::StubToolPlane::new().with_canned_text("read", "file contents");
            let now = || broca_core::clock::epoch_millis().unwrap_or(0);
            let seams = broca_session_loop::Seams {
                person_steers: None,
                provider: &provider,
                tools: &tools,
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.clone().unwrap_or_default(),
                stop: Default::default(),
                held: None,
                approval: &broca_core::AllowAll,
                hook_plane: control.hook_plane.clone(),
                now_ms: &now,
                session_address: None,
                compaction_plane: control.compaction_plane.as_deref(),
                compaction_session: control.compaction_session.clone(),
                compaction_changes: true,
                keep_warm: None,
                account_reports: None,
                max_same_account_wait: Default::default(),
                auth_selection: None,
                step_progress: control.step_progress.clone(),
                model_call_ready: None,
                pause_for_auth_at_start: None,
                account_continue: None,
            };
            match mode {
                StoreRunMode::Fresh => {
                    if self.routing_unreachable_next.swap(false, Ordering::SeqCst) {
                        return Err(StoreRunError::Admission(RunError::RoutingUnavailable(
                            broca_core::RoutingUnavailable::unreachable(),
                        )));
                    }
                    let admission = broca_session_loop::admit::admission(
                        &mut req,
                        &control,
                        &provider,
                        broca_session_loop::writer::Writer::operations(&writer),
                        Some(1),
                    )
                    .map_err(StoreRunError::Admission)?;
                    let run =
                        broca_session_loop::run(&mut writer, parts.as_mut(), admission, &seams);
                    if fails {
                        tokio::select! {
                            finished = run => finished.map_err(StoreRunError::Loop),
                            _ = self.fail.notified() => Err(StoreRunError::Loop(
                                LoopError::Parts(LOOP_FAILED.into()),
                            )),
                        }
                    } else {
                        run.await.map_err(StoreRunError::Loop)
                    }
                }
                StoreRunMode::Seal(cause) => {
                    broca_session_loop::seal(&mut writer, parts.as_ref(), &seams, cause)
                        .await
                        .map_err(StoreRunError::Loop)
                }
                StoreRunMode::Resume | StoreRunMode::RecheckSignIn | StoreRunMode::ProbeSignIn => {
                    if self.temporarily_refuse_resume.swap(false, Ordering::SeqCst) {
                        return Err(StoreRunError::Admission(RunError::CredentialsUnavailable));
                    }
                    if fails {
                        return Err(StoreRunError::Admission(RunError::Config(
                            "resume admission failed".into(),
                        )));
                    }
                    broca_session_loop::run::resume(&mut writer, parts.as_ref(), &seams)
                        .await
                        .map_err(StoreRunError::Loop)
                }
            }
        }
        .await;
        StoreRunEnd { parts, result }
    }
}

fn scratch_dir() -> std::path::PathBuf {
    use std::sync::atomic::AtomicU64;
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-loop-error-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// A module over a scratch folder whose new sessions start in the new session storage
/// (`broca_session_store`), run by `executor`.
struct Side {
    dir: std::path::PathBuf,
    registry: LineageRegistry,
    key: BindIdentity,
    caller: Caller,
}

impl Drop for Side {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Side {
    async fn new(executor: Arc<FailingExecutor>) -> Self {
        let dir = scratch_dir();
        let key = BindIdentity::new(dir.join("project"), "store-actor", "s1");
        Self::at(dir, key, executor).await
    }

    /// A module over the state root `dir`, whose session is `key`.
    async fn at(
        dir: std::path::PathBuf,
        key: BindIdentity,
        executor: Arc<FailingExecutor>,
    ) -> Self {
        let executor: Arc<dyn RunExecutor> = executor;
        let runtime = crate::store_open::StoreRuntime::open(
            &dir,
            broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
            Arc::clone(&executor),
            IDLE,
        )
        .await
        .unwrap();
        let registry = LineageRegistry::new(Arc::new(
            crate::opener::DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                executor,
            )
            .with_idle_ttl(IDLE)
            .with_store_runtime(Arc::new(runtime)),
        ));
        Self {
            dir,
            registry,
            key,
            caller: Caller::default(),
        }
    }

    async fn send(&self, run_id: &str, text: &str) -> SendOutcome {
        self.send_keyed(run_id, text, SendKey::Minted).await
    }

    async fn send_keyed(&self, run_id: &str, text: &str, key: SendKey) -> SendOutcome {
        let request = self.caller.shape(run_id, &key, request(text));
        self.registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                request,
                RunKind::FreshOrResume,
                key,
                false,
                broca_protocol::Delivery::Queue,
                None,
                None,
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .expect("the session opens")
    }

    async fn run_status(&self, run_id: &str) -> Value {
        match self
            .registry
            .run_status_reply(&self.key, RunId(run_id.into()))
            .await
        {
            Ok(reply) => serde_json::to_value(reply).unwrap(),
            Err(error) => json!({ "refused": format!("{error:?}") }),
        }
    }

    /// Waits until `run.status` says run `run_id` is in `state`.
    async fn until_state(&self, run_id: &str, state: &str) -> Value {
        broca_test_support::wait_until(&format!("run {run_id} is {state}"), || async {
            let status = self.run_status(run_id).await;
            if status["state"] == state {
                Ok(status)
            } else {
                Err(status.to_string())
            }
        })
        .await
    }
}

/// A run whose loop stops with an error after its start, writing no end, ends with that error,
/// permanent and coded `runner_error`, as `run.status` reads it. The send queued behind it then
/// drains, and a new send starts a run rather than being refused.
#[tokio::test]
async fn a_run_whose_loop_fails_after_its_start_ends_with_the_error_and_the_session_serves_again() {
    let executor = Arc::new(FailingExecutor {
        script: Arc::new(StubProvider::new(vec![
            text_step("a1", "queued answer"),
            text_step("a2", "next answer"),
        ])),
        fails_next: AtomicBool::new(true),
        fail: Notify::new(),
        temporarily_refuse_resume: AtomicBool::new(false),
        routing_unreachable_next: AtomicBool::new(false),
    });
    let side = Side::new(Arc::clone(&executor)).await;
    let sent = side.send("r1", "first").await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    let queued = side.send("r2", "queued").await;
    assert!(matches!(queued, SendOutcome::Pending { .. }), "{queued:?}");
    executor.fail.notify_one();

    let failed = side.until_state("r1", "error").await;
    assert_eq!(
        failed["error"],
        json!({
            "class": "permanent",
            "message": format!("{:?}", LoopError::Parts(LOOP_FAILED.into())),
            "provider_code": "runner_error",
        }),
        "the run's end carries the loop's error: {failed}"
    );
    side.until_state("r2", "completed").await;

    let next = side.send("r3", "next").await;
    assert!(
        matches!(
            next,
            SendOutcome::Active { .. } | SendOutcome::Finished { .. }
        ),
        "a new send starts a run: {next:?}"
    );
    side.until_state("r3", "completed").await;
}

/// A send that creates its session and is refused before its run starts (here routing can't
/// be reached for its account) leaves nothing behind: the actor closes at once rather than at
/// its idle timeout, which deletes the session it created, so the address has no session again.
#[tokio::test]
async fn a_refused_creating_send_leaves_no_session() {
    let executor = Arc::new(FailingExecutor {
        script: Arc::new(StubProvider::new(vec![text_step("a1", "never asked")])),
        fails_next: AtomicBool::new(false),
        fail: Notify::new(),
        temporarily_refuse_resume: AtomicBool::new(false),
        routing_unreachable_next: AtomicBool::new(true),
    });
    let side = Side::new(Arc::clone(&executor)).await;
    let sent = side.send("r1", "first").await;
    assert!(
        !matches!(
            sent,
            SendOutcome::Active { .. } | SendOutcome::Pending { .. }
        ),
        "{sent:?}"
    );
    let runtime = side.registry.store_runtime().unwrap();
    broca_test_support::wait_until("the refused send's session is deleted", || async {
        match (
            side.registry.slot_state(&side.key),
            runtime.current_view(&side.key).await,
        ) {
            (None, Ok(None)) => Ok(()),
            (slot, view) => Err(format!(
                "slot {slot:?}, session present: {:?}",
                view.map(|view| view.is_some())
            )),
        }
    })
    .await;
    assert!(
        executor.script.calls().is_empty(),
        "no model request was sent"
    );
}

/// A send ordered to start a run when the session is idle (`idle_start`) is answered `pending`,
/// and its run starts in the background. When routing can't be reached for that run's account,
/// the run ends with the refusal at once: `run.status` reads a transient `routing_unavailable`
/// error and `session.send_status` reads the send finished with an error, without any later send
/// waking the session, and admission is never retried for it.
#[tokio::test]
async fn an_idle_start_send_routing_cant_admit_reads_as_an_error_on_both_status_surfaces() {
    let executor = Arc::new(FailingExecutor {
        script: Arc::new(StubProvider::new(vec![text_step("a1", "never asked")])),
        fails_next: AtomicBool::new(false),
        fail: Notify::new(),
        temporarily_refuse_resume: AtomicBool::new(false),
        routing_unreachable_next: AtomicBool::new(true),
    });
    let side = Side::new(Arc::clone(&executor)).await;
    let key = SendKey::SendId("refused".into());
    let request = side.caller.shape("r1", &key, request("first"));
    let sent = side
        .registry
        .route_submission_with_order(
            &side.key,
            RunId("r1".into()),
            request,
            RunKind::FreshOrResume,
            key,
            false,
            broca_protocol::Delivery::Queue,
            None,
            None,
            None,
            None,
            None,
            false,
            Some(broca_protocol::DeliveryOrder {
                modes: vec![broca_protocol::DeliveryMode::IdleStart],
                dedupe_key: None,
                expires_at_ms: None,
                template: None,
            }),
        )
        .await
        .expect("the session opens");
    assert!(matches!(sent, SendOutcome::Pending { .. }), "{sent:?}");

    let status = side.until_state("r1", "error").await;
    assert_eq!(status["error"]["class"], "transient", "{status}");
    assert_eq!(
        status["error"]["provider_code"], "routing_unavailable",
        "{status}"
    );
    let send_status = serde_json::to_value(
        side.registry
            .send_status(&side.key, RunId("r1".into()), "refused", None)
            .await
            .expect("the send's fate reads"),
    )
    .unwrap();
    assert_eq!(send_status["state"], "finished", "{send_status}");
    assert_eq!(send_status["reason"], "error", "{send_status}");
    assert!(
        !executor.routing_unreachable_next.load(Ordering::SeqCst),
        "the refused run was admitted once"
    );
    assert!(
        executor.script.calls().is_empty(),
        "no model request is sent for a run routing can't admit"
    );
}

#[tokio::test]
async fn a_failed_admission_of_an_existing_auth_pause_records_an_error() {
    let executor = Arc::new(FailingExecutor {
        script: Arc::new(StubProvider::new(vec![
            super::tests::auth_failure(),
            text_step("a2", "next answer"),
        ])),
        fails_next: AtomicBool::new(false),
        fail: Notify::new(),
        temporarily_refuse_resume: AtomicBool::new(false),
        routing_unreachable_next: AtomicBool::new(false),
    });
    let side = Side::new(executor.clone()).await;
    let key = || SendKey::SendId("synthetic-send".into());
    assert!(matches!(
        side.send_keyed("r1", "first", key()).await,
        SendOutcome::Active { .. }
    ));
    side.until_state("r1", "paused").await;
    executor.fails_next.store(true, Ordering::SeqCst);
    side.send_keyed("r1", "first", key()).await;
    let failed = side.until_state("r1", "error").await;
    assert_eq!(failed["error"]["provider_code"], "runner_error");
    assert!(failed["error"]["message"]
        .as_str()
        .unwrap()
        .contains("resume admission failed"));
    side.send("r2", "next").await;
    side.until_state("r2", "completed").await;
}

/// A dependency that is not ready says nothing permanent about the paused run. Keep the
/// pause and its start intact, then continue the same run when the caller retries later.
#[tokio::test]
async fn a_temporary_admission_failure_keeps_the_pause_for_a_later_resume() {
    use broca_session_store::record::Record;
    let executor = Arc::new(FailingExecutor {
        script: Arc::new(StubProvider::new(vec![
            super::tests::auth_failure(),
            text_step("a2", "recovered answer"),
        ])),
        fails_next: AtomicBool::new(false),
        temporarily_refuse_resume: AtomicBool::new(false),
        fail: Notify::new(),
        routing_unreachable_next: AtomicBool::new(false),
    });
    let side = Side::new(executor.clone()).await;
    let key = || SendKey::SendId("temporary-admission-send".into());
    assert!(matches!(
        side.send_keyed("r1", "first", key()).await,
        SendOutcome::Active { .. }
    ));
    let paused = side.until_state("r1", "paused").await;
    let runtime = side.registry.store_runtime().unwrap();
    let before = runtime
        .current_view(&side.key)
        .await
        .unwrap()
        .unwrap()
        .records_from(0)
        .await
        .unwrap();
    executor
        .temporarily_refuse_resume
        .store(true, Ordering::SeqCst);
    assert!(matches!(
        side.send_keyed("r1", "first", key()).await,
        SendOutcome::Active { .. }
    ));
    broca_test_support::wait_until("temporary admission task parks", || async {
        match side.registry.route_status(&side.key).await.unwrap() {
            crate::actor::LineageStatus::Idle => Ok(()),
            other => Err(format!("{other:?}")),
        }
    })
    .await;
    assert_eq!(side.run_status("r1").await, paused);
    let after = runtime
        .current_view(&side.key)
        .await
        .unwrap()
        .unwrap()
        .records_from(0)
        .await
        .unwrap();
    assert_eq!(
        after.len(),
        before.len(),
        "temporary admission writes no end or other record"
    );
    assert!(!after
        .iter()
        .any(|(_, record)| matches!(record, Record::RunEnd(_))));
    assert!(matches!(
        side.send_keyed("r1", "first", key()).await,
        SendOutcome::Active { .. }
    ));
    side.until_state("r1", "completed").await;
    let records = runtime
        .current_view(&side.key)
        .await
        .unwrap()
        .unwrap()
        .records_from(0)
        .await
        .unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|(_, record)| matches!(record, Record::RunStart(_)))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|(_, record)| matches!(record, Record::RunEnd(_)))
            .count(),
        1
    );
    assert!(side.registry.stop_and_seal().await);
}

/// Copies the state root `from` to `to` as a stopped process leaves it: every file but the
/// leases, which name the stopped process.
fn copy_state(from: &std::path::Path, to: &std::path::Path) {
    fn copy(from: &std::path::Path, to: &std::path::Path) {
        if std::fs::metadata(from).unwrap().is_dir() {
            std::fs::create_dir_all(to).unwrap();
            for entry in std::fs::read_dir(from).unwrap() {
                let entry = entry.unwrap();
                copy(&entry.path(), &to.join(entry.file_name()));
            }
        } else {
            std::fs::copy(from, to).unwrap();
        }
    }
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() != "leases" {
            copy(&entry.path(), &to.join(entry.file_name()));
        }
    }
}

/// A run already started, with its caller's cancel recorded for its next boundary and no end:
/// its process stopped while the run's model call ran. The session's next open resumes the run,
/// and here the executor refuses that resume at admission, `temporary`ly or not. The run isn't
/// paused, so either way it ends with the refusal, and only then does the send waiting behind it
/// start its run: a session never starts a run over one that hasn't ended.
async fn a_refused_resume_of_a_started_run_ends_it_before_the_next_run(temporary: bool) {
    use broca_session_store::record::Record;
    let original = Arc::new(FailingExecutor {
        script: Arc::new(StubProvider::new(Vec::new())),
        fails_next: AtomicBool::new(true),
        temporarily_refuse_resume: AtomicBool::new(false),
        fail: Notify::new(),
        routing_unreachable_next: AtomicBool::new(false),
    });
    let mut side = Side::new(Arc::clone(&original)).await;
    let sent = side.send("r1", "first").await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    side.registry
        .route_cancel_at(
            &side.key,
            RunId("r1".into()),
            broca_wal::record::RunCancelAt::StepBoundary,
        )
        .await
        .expect("the cancel is routed");
    let runtime = side.registry.store_runtime().unwrap();
    broca_test_support::wait_until("the cancel is recorded", || async {
        let records = runtime
            .current_view(&side.key)
            .await
            .unwrap()
            .unwrap()
            .records_from(0)
            .await
            .unwrap();
        if records
            .iter()
            .any(|(_, record)| matches!(record, Record::CancelRequest(_)))
        {
            Ok(())
        } else {
            Err("no cancel yet".to_owned())
        }
    })
    .await;
    // The process stops here, with r1's model call still running: its state root as it lies.
    let stopped = scratch_dir();
    copy_state(&side.dir, &stopped);
    original.fail.notify_one();
    side.until_state("r1", "error").await;
    assert!(side.registry.stop_and_seal().await);

    let resumed = Arc::new(FailingExecutor {
        script: Arc::new(StubProvider::new(vec![text_step("a2", "next answer")])),
        fails_next: AtomicBool::new(!temporary),
        temporarily_refuse_resume: AtomicBool::new(temporary),
        fail: Notify::new(),
        routing_unreachable_next: AtomicBool::new(false),
    });
    let mut restarted = Side::at(stopped, side.key.clone(), Arc::clone(&resumed)).await;
    // The same caller sends on: its later sends leave out the tools its first send froze into
    // the session, which a send from a new caller would declare again and be refused for.
    restarted.caller = std::mem::take(&mut side.caller);
    let next = restarted.send("r2", "next").await;
    assert!(
        matches!(
            next,
            SendOutcome::Active { .. } | SendOutcome::Pending { .. }
        ),
        "{next:?}"
    );
    let failed = restarted.until_state("r1", "error").await;
    assert_eq!(failed["error"]["provider_code"], "runner_error", "{failed}");
    if !temporary {
        assert!(
            failed["error"]["message"]
                .as_str()
                .unwrap()
                .contains("resume admission failed"),
            "{failed}"
        );
    }
    restarted.until_state("r2", "completed").await;
    let runtime = restarted.registry.store_runtime().unwrap();
    let records = runtime
        .current_view(&restarted.key)
        .await
        .unwrap()
        .unwrap()
        .records_from(0)
        .await
        .unwrap();
    let mut open: Option<String> = None;
    let mut starts = 0;
    for (seq, record) in &records {
        match record {
            Record::RunStart(start) => {
                assert!(
                    open.is_none(),
                    "run {} starts at seq {seq} while run {open:?} hasn't ended",
                    start.run_id
                );
                open = Some(start.run_id.clone());
                starts += 1;
            }
            Record::RunEnd(end) => {
                assert_eq!(open.as_deref(), Some(end.run_id.as_str()), "seq {seq}");
                open = None;
            }
            _ => {}
        }
    }
    assert_eq!((starts, open), (2, None), "r1 ended, then r2 ran and ended");
    assert!(restarted.registry.stop_and_seal().await);
}

#[tokio::test]
async fn a_permanently_refused_resume_of_a_started_run_ends_it_before_the_next_run() {
    a_refused_resume_of_a_started_run_ends_it_before_the_next_run(false).await;
}

#[tokio::test]
async fn a_temporarily_refused_resume_of_a_started_run_ends_it_before_the_next_run() {
    a_refused_resume_of_a_started_run_ends_it_before_the_next_run(true).await;
}
