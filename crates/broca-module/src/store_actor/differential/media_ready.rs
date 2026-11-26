//! A run on a session kept in the session store renders nothing before its media are ready. Unlike
//! the comparisons beside it, these drive the store side alone: the store actor builds the
//! session's media fold and hands each run its media, and the executor installs the run's media
//! renderer as the module's executor does (`store_media::LazyRenderer`), which refuses every
//! render until the run waited for those media. The scripted model renders each estimate and
//! each request it sends through that renderer, as the transport does.
//!
//! A run renders its request before the model call in two places that these cover: a compacting
//! step's compaction estimate, and the output cap's estimate once an earlier step measured the
//! model. Each waits for the media first; when they can't be had, nothing renders, the step fails
//! with the reason before anything is sent, and the run ends with its end record.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use broca_core::provider::{MediaRequestRenderer, Provider, StubProvider};
use broca_core::{OutputEvent, RunError};
use broca_protocol::request::CallOptions;
use broca_protocol::{NormalizedEvent, ProviderControl, ProviderError, RunId, SubmissionRequest};
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Notify};

use super::controls::{compacting_plan, limited, Compactor};
use super::media::image_render_config;
use crate::actor::{CancelOutcome, LineageCmd, SendKey, SendOutcome, SendPlan};
use crate::executor::{RunExecutor, RunKind, StoreRunEnd, StoreRunError, StoreRunJob};
use crate::store_actor::tests::{
    bind, deployment, idle, request, send_with, text_step, triple, Commands, ADDR,
};
use crate::store_actor::StoreActor;

/// Why a run's renderer can't be built, when the test makes building it fail.
const BUILD_FAILED: &str = "the session's media can't be prepared";

/// What the runs' model renders, shared by every run of a test.
#[derive(Default)]
struct Log {
    /// Each request a run rendered to estimate it: its length, or why the render was refused.
    estimates: Mutex<Vec<Result<usize, String>>>,
    /// Each request a model call rendered to send: its length, or why the render was refused.
    sent: Mutex<Vec<Result<usize, String>>>,
    /// Whether a run started waiting for its media.
    waiting: AtomicBool,
}

impl Log {
    fn estimates(&self) -> Vec<Result<usize, String>> {
        self.estimates.lock().unwrap().clone()
    }

    fn sent(&self) -> Vec<Result<usize, String>> {
        self.sent.lock().unwrap().clone()
    }
}

/// One run's scripted model, rendering through the media renderer its run installed.
struct Rendering {
    script: Arc<StubProvider>,
    renderer: RwLock<Option<Arc<MediaRequestRenderer>>>,
    render_tags: AtomicBool,
    log: Arc<Log>,
}

impl Rendering {
    fn render(&self, opts: &CallOptions, commit: bool) -> Result<Vec<u8>, Box<ProviderError>> {
        let renderer = self
            .renderer
            .read()
            .unwrap()
            .clone()
            .expect("the executor installs the run's media renderer");
        renderer(
            &image_render_config(),
            opts,
            self.render_tags.load(Ordering::Relaxed),
            commit,
            None,
        )
    }
}

fn logged(rendered: &Result<Vec<u8>, Box<ProviderError>>) -> Result<usize, String> {
    rendered
        .as_ref()
        .map(Vec::len)
        .map_err(|error| error.message.clone())
}

#[async_trait]
impl Provider for Rendering {
    fn render_config_blob(&self) -> Value {
        image_render_config()
    }

    fn render_config_blob_for_version(&self, _renderer_version: u32) -> Value {
        image_render_config()
    }

    fn supports_media_renderer(&self) -> bool {
        true
    }

    fn set_media_request_renderer(&self, renderer: Arc<MediaRequestRenderer>) {
        *self.renderer.write().unwrap() = Some(renderer);
    }

    fn defer_media_tags(&self, render_tags: bool) -> bool {
        self.render_tags.store(render_tags, Ordering::Relaxed);
        self.renderer.read().unwrap().is_some()
    }

    /// A cap the loop may lower, so a step after a measured one estimates its request.
    fn output_limits(&self) -> Option<broca_core::output_reservation::OutputLimits> {
        Some(broca_core::output_reservation::OutputLimits::new(4_096, 0))
    }

    /// Renders through the run's media renderer without committing, as the transport sizes a
    /// request.
    fn try_render_request(&self, opts: &CallOptions) -> Result<Vec<u8>, Box<ProviderError>> {
        let rendered = self.render(opts, false);
        self.log.estimates.lock().unwrap().push(logged(&rendered));
        rendered
    }

    /// Renders the request to send, as the transport does; a refused render sends nothing and
    /// fails the step.
    async fn step(
        &self,
        step_id: broca_protocol::StepId,
        opts: &CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let rendered = self.render(opts, true);
        self.log.sent.lock().unwrap().push(logged(&rendered));
        if let Err(error) = rendered {
            let (events, receiver) = mpsc::channel(1);
            let _ = events
                .send(NormalizedEvent::Control(ProviderControl::Error {
                    error: *error,
                    retries_used: 0,
                }))
                .await;
            return receiver;
        }
        self.script.step(step_id, opts).await
    }
}

/// The run's media renderer, as the run waits for it: noting in the log that the wait started.
struct Waiting {
    lazy: Arc<crate::store_media::LazyRenderer>,
    log: Arc<Log>,
}

#[async_trait]
impl broca_session_loop::run::ModelCallReady for Waiting {
    async fn ready(&self) -> Result<(), ProviderError> {
        self.log.waiting.store(true, Ordering::SeqCst);
        self.lazy.ready().await
    }
}

/// Runs each store job through the new loop, installing the run's media renderer as the module's
/// executor does for a run that may carry media, over the session's media fold.
struct ReadyExecutor {
    script: Arc<StubProvider>,
    compactor: Arc<Compactor>,
    /// Whether a run's renderer can be built; when not, building it fails with
    /// [`BUILD_FAILED`], as building a run's renderer over its media can fail.
    builds: Arc<AtomicBool>,
    log: Arc<Log>,
}

impl ReadyExecutor {
    fn new(steps: Vec<Vec<NormalizedEvent>>) -> Self {
        Self {
            script: Arc::new(StubProvider::new(steps)),
            compactor: Arc::default(),
            builds: Arc::new(AtomicBool::new(true)),
            log: Arc::default(),
        }
    }
}

#[async_trait]
impl RunExecutor for ReadyExecutor {
    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::CompactionPlane>> {
        Some(Arc::clone(&self.compactor) as Arc<dyn broca_core::CompactionPlane>)
    }

    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        let StoreRunJob {
            mut req,
            mut control,
            mut writer,
            mut parts,
            mode,
            media,
            ..
        } = job;
        control.admitted_context_limit = control.admitted_context_limit.or(req.context_limit);
        let provider = Rendering {
            script: Arc::clone(&self.script),
            renderer: RwLock::new(None),
            render_tags: AtomicBool::new(false),
            log: Arc::clone(&self.log),
        };
        let result = async {
            let media = media.ok_or_else(|| {
                StoreRunError::Admission(RunError::Log("the run has no media fold".into()))
            })?;
            let builds = self.builds.load(Ordering::SeqCst);
            let lazy = crate::store_media::LazyRenderer::new(
                media,
                Box::new(move |media| {
                    if !builds {
                        return Err(BUILD_FAILED.into());
                    }
                    Ok(media.renderer())
                }),
            );
            provider.set_media_request_renderer(lazy.renderer());
            let ready = Arc::new(Waiting {
                lazy,
                log: Arc::clone(&self.log),
            });
            let tools = broca_core::StubToolPlane::new().with_canned_text("read", "file contents");
            let now = || 1;
            let seams = broca_session_loop::Seams {
                steer_yields: None,
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
                accepting_drain: None,
                model_call_ready: Some(ready),
                pause_for_auth_at_start: None,
                account_continue: None,
            };
            match mode {
                crate::executor::StoreRunMode::Fresh => {
                    let admission = broca_session_loop::admit::admission(
                        &mut req,
                        &control,
                        &provider,
                        broca_session_loop::writer::Writer::operations(&writer),
                        Some(1),
                    )
                    .map_err(StoreRunError::Admission)?;
                    broca_session_loop::run(&mut writer, parts.as_mut(), admission, &seams).await
                }
                crate::executor::StoreRunMode::Seal(cause) => {
                    broca_session_loop::seal(&mut writer, parts.as_ref(), &seams, cause).await
                }
                crate::executor::StoreRunMode::Resume
                | crate::executor::StoreRunMode::RecheckSignIn
                | crate::executor::StoreRunMode::ProbeSignIn => {
                    broca_session_loop::run::resume(&mut writer, parts.as_ref(), &seams).await
                }
            }
            .map_err(StoreRunError::Loop)
        }
        .await;
        StoreRunEnd { parts, result }
    }
}

/// A store actor over a new session in memory running `executor`'s runs, serving on its own task.
/// With `build`, the session's media fold isn't built until it is notified, as a long session's
/// fold takes seconds to build.
async fn actor(
    executor: ReadyExecutor,
    build: Option<Arc<Notify>>,
) -> (
    Commands,
    tokio::task::JoinHandle<Session<&'static MemoryBackend>>,
) {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    executor.compactor.reads_memory(backend);
    let actor = StoreActor::new(
        bind(),
        session,
        Arc::new(executor),
        Box::new(SharedParts::new(Box::new(MemoryParts::default()))),
    )
    .with_readers(Arc::new(move || backend));
    let actor = match build {
        Some(gate) => actor.with_media_build_gate(gate),
        None => actor,
    };
    let (commands, receiver) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receiver));
    (Commands::over(commands, backend), served)
}

/// Sends `request` as `run_id` under a plan naming a compaction provider, so its run compacts.
async fn send_compacting(
    commands: &Commands,
    run_id: &str,
    request: SubmissionRequest,
) -> SendOutcome {
    let plan = compacting_plan();
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Send {
            session_class: None,
            retention: None,
            stop_sequences_named: false,
            conversation_key: None,
            title: None,
            run_id: RunId(run_id.into()),
            request: Box::new(request),
            kind: RunKind::FreshOrResume,
            key: SendKey::Minted,
            skip_materialization: false,
            delivery: broca_protocol::Delivery::Queue,
            plan: Some(Box::new(SendPlan {
                identity: plan.plan_identity.clone(),
                manifest: Some(Arc::clone(&plan)),
                model_check: None,
            })),
            order: None,
            reply,
        })
        .unwrap();
    answer.await.unwrap()
}

/// A send declaring the `read` tool, with the context window the output cap is reserved in.
fn windowed(text: &str) -> SubmissionRequest {
    let mut request = request(text);
    request.context_limit = Some(200_000);
    request
}

/// A model step that answers `text` after an attempt whose body length the provider reported,
/// so the step measures the model for the next request's estimate.
fn measured_step(id: &str, text: &str) -> Vec<NormalizedEvent> {
    let attempt = serde_json::from_value(json!({
        "attempt": 0,
        "request_body_len": 10,
        "request_body_sha256": "0".repeat(64),
        "http_status": 200,
        "response_body_len": 0,
        "response_body_sha256": "0".repeat(64),
        "outcome": "complete",
        "ts_ms": 1,
        "sent_at_ms": 1,
    }))
    .unwrap();
    let mut step = vec![NormalizedEvent::Control(ProviderControl::AttemptFinished {
        attempt,
    })];
    step.extend(text_step(id, text));
    step
}

/// How each run ended, in order: its id, its reason and its error's message.
async fn ends(session: &Session<&'static MemoryBackend>) -> Vec<(String, Value, Value)> {
    session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, record)| match record {
            Record::RunEnd(end) => Some((
                end.run_id,
                json!(end.reason),
                end.error
                    .and_then(|error| error.parse().get("message").cloned())
                    .unwrap_or(Value::Null),
            )),
            _ => None,
        })
        .collect()
}

/// Lets the session's media fold build once dropped. A test that holds the build and fails
/// before letting it through would otherwise leave the build's thread blocked, and the test's
/// runtime waiting on it instead of reporting the failure.
struct Released(Arc<Notify>);

impl Drop for Released {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

/// Waits until a run is waiting for its media.
async fn until_waiting(log: &Log) {
    broca_test_support::wait_until("a run waits for its media", || async {
        if log.waiting.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(format!(
                "renders so far: {:?}, sends so far: {:?}",
                log.estimates(),
                log.sent()
            ))
        }
    })
    .await;
}

/// A compacting run whose media are still being prepared when its step starts waits for them
/// before its step is prepared: nothing is rendered or sent until then. Once the media are
/// ready, the step's request is sent, never sized first, and the run ends with its end.
#[tokio::test]
async fn a_compacting_run_renders_and_sends_only_once_its_media_are_ready() {
    let build = Arc::new(Notify::new());
    let _released = Released(Arc::clone(&build));
    let executor = ReadyExecutor::new(vec![text_step("a1", "hello")]);
    let log = Arc::clone(&executor.log);
    let (commands, served) = actor(executor, Some(Arc::clone(&build))).await;
    let sent = send_compacting(&commands, "r1", limited("first")).await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    until_waiting(&log).await;
    assert_eq!(
        log.estimates(),
        vec![],
        "nothing is rendered before the media"
    );
    assert_eq!(log.sent(), vec![], "nothing is sent before the media");
    build.notify_one();
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(
        ends(&session).await,
        vec![("r1".into(), json!("completed"), Value::Null)]
    );
    assert_eq!(
        log.estimates(),
        vec![],
        "no request is sized before it is sent"
    );
    let sent = log.sent();
    assert!(
        sent.len() == 1 && sent[0].is_ok(),
        "one request was sent: {sent:?}"
    );
}

/// A compacting run whose media can't be had renders nothing: its step fails with the reason
/// before anything is sent, and the run ends with that error.
#[tokio::test]
async fn a_compacting_run_whose_media_cant_be_had_ends_with_the_error_and_sends_nothing() {
    let executor = ReadyExecutor::new(vec![text_step("a1", "never sent")]);
    executor.builds.store(false, Ordering::SeqCst);
    let log = Arc::clone(&executor.log);
    let script = Arc::clone(&executor.script);
    let (commands, served) = actor(executor, None).await;
    let sent = send_compacting(&commands, "r1", limited("first")).await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(
        ends(&session).await,
        vec![("r1".into(), json!("error"), json!(BUILD_FAILED))]
    );
    assert_eq!(log.estimates(), vec![], "nothing was rendered");
    assert_eq!(log.sent(), vec![], "nothing was sent");
    assert!(script.calls().is_empty(), "the model was never called");
}

/// A cancel while a compacting run waits for its media ends the run cancelled, with nothing
/// rendered or sent.
#[tokio::test]
async fn a_cancel_while_a_compacting_run_waits_for_its_media_ends_it_cancelled() {
    let build = Arc::new(Notify::new());
    let _released = Released(Arc::clone(&build));
    let executor = ReadyExecutor::new(vec![text_step("a1", "never sent")]);
    let log = Arc::clone(&executor.log);
    let (commands, served) = actor(executor, Some(Arc::clone(&build))).await;
    let sent = send_compacting(&commands, "r1", limited("first")).await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    until_waiting(&log).await;
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Cancel {
            run_id: RunId("r1".into()),
            at: broca_records::record::RunCancelAt::Now,
            reply,
        })
        .unwrap();
    assert_eq!(answer.await.unwrap(), CancelOutcome::Ack);
    idle(&commands).await;
    // The fold's build is let through only now, so the test's runtime doesn't wait on it.
    build.notify_one();
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(
        ends(&session).await,
        vec![("r1".into(), json!("cancelled"), Value::Null)]
    );
    assert_eq!(log.estimates(), vec![], "nothing was rendered");
    assert_eq!(log.sent(), vec![], "nothing was sent");
}

/// A run after a measured one whose media can't be had doesn't estimate its output cap: its
/// step fails with the reason before anything is sent, and the run ends with that error.
#[tokio::test]
async fn a_later_run_whose_media_cant_be_had_ends_with_the_error_and_sends_nothing() {
    let executor = ReadyExecutor::new(vec![
        measured_step("a1", "hello"),
        text_step("a2", "never sent"),
    ]);
    let log = Arc::clone(&executor.log);
    let builds = Arc::clone(&executor.builds);
    let (commands, served) = actor(executor, None).await;
    let sent = send_with(&commands, "r1", windowed("first"), SendKey::Minted).await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    idle(&commands).await;
    builds.store(false, Ordering::SeqCst);
    let sent = send_with(&commands, "r2", windowed("second"), SendKey::Minted).await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(
        ends(&session).await,
        vec![
            ("r1".into(), json!("completed"), Value::Null),
            ("r2".into(), json!("error"), json!(BUILD_FAILED)),
        ]
    );
    assert_eq!(log.estimates(), vec![], "nothing was estimated");
    assert_eq!(
        log.sent().len(),
        1,
        "only the first run sent: {:?}",
        log.sent()
    );
}
