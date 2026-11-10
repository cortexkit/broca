//! A run whose loop fails after its start, without writing an end, is ended by the store actor
//! with the error, so the session serves again in the same process: its queued sends drain, and
//! a new send starts a run.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use broca_core::provider::{Provider, StubProvider};
use broca_core::{FileLeaseStore, FileWalStore, OutputEvent, RunError, RunSummary};
use broca_protocol::request::CallOptions;
use broca_protocol::{BindIdentity, NormalizedEvent, RunId};
use broca_session_loop::LoopError;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Notify};

use super::tests::{request, text_step, Caller};
use crate::actor::{SendKey, SendOutcome};
use crate::executor::{
    RunExecutor, RunJob, RunKind, StoreRunEnd, StoreRunError, StoreRunJob, StoreRunMode,
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
struct FailingExecutor {
    script: Arc<StubProvider>,
    fails_next: AtomicBool,
    fail: Notify,
}

#[async_trait]
impl RunExecutor for FailingExecutor {
    async fn execute(
        &self,
        _job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        unreachable!("a store session's runs come through execute_on_store")
    }

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
        let fails = mode == StoreRunMode::Fresh && self.fails_next.swap(false, Ordering::SeqCst);
        let provider = Stalling {
            script: Arc::clone(&self.script),
            stalls: fails,
        };
        let result = async {
            let tools = broca_core::StubToolPlane::new().with_canned_text("read", "file contents");
            let now = || broca_core::clock::epoch_millis().unwrap_or(0);
            let seams = broca_session_loop::Seams {
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
            };
            match mode {
                StoreRunMode::Fresh => {
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
                StoreRunMode::Resume => {
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
        use std::sync::atomic::AtomicU64;
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = broca_test_support::scratch_root().join(format!(
            "store-loop-error-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let executor: Arc<dyn RunExecutor> = executor;
        let wal = Arc::new(FileWalStore::new(dir.join("wal")));
        let runtime = crate::store_open::StoreRuntime::open(
            &dir,
            broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
            Arc::clone(&executor),
            IDLE,
        )
        .await
        .unwrap();
        let registry = LineageRegistry::new_with_reader(
            Arc::new(
                crate::opener::DefaultOpener::new(
                    Arc::new(FileLeaseStore::new(dir.join("leases"))),
                    wal.clone(),
                    executor,
                )
                .with_idle_ttl(IDLE)
                .with_store_runtime(Arc::new(runtime)),
            ),
            wal,
            None,
        )
        .with_store_new_sessions();
        let key = BindIdentity::new(dir.join("project"), "store-actor", "s1");
        Self {
            dir,
            registry,
            key,
            caller: Caller::default(),
        }
    }

    async fn send(&self, run_id: &str, text: &str) -> SendOutcome {
        let key = SendKey::Minted;
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
