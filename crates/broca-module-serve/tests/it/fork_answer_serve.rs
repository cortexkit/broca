//! `fork_answer` (llm-runner §9.20) on a head kept in the session store, through the real serve
//! path: each request goes over the wire into `serve_connection`, into the registry and on to
//! the fork manager, which reads the head through a read-only view and keeps each fork in its
//! own session. The head is created by a send the store actor runs over a scripted provider; the
//! fork's model call is the executor's `fork_call`, played here by a script that records every
//! job it is handed.
//!
//! The daemon side is played by the test: it sends the HELLO_ACK and binds each route.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{AllowAll, FileLeaseStore, OutputEvent};
use broca_module::actor::{SendKey, SendOutcome, SendPlan};
use broca_module::executor::{
    ForkCallJob, ForkCallOutcome, ForkCallRefusal, ForkReleaseJob, RunExecutor, RunKind,
    StoreRunEnd, StoreRunError, StoreRunJob, StoreRunMode,
};
use broca_module::{DefaultOpener, LineageRegistry};
use broca_module_serve::{serve_connection, ServeEnd};
use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason};
use broca_protocol::{
    BindIdentity, MessageId, NormalizedEvent, ProviderControl, RunId, SubmissionRequest, Usage,
};
use cortexkit_role_llm_runner::usage::RunUsage;
use serde_json::{json, Value};
use subc_protocol::session::ModuleControlRequest;
use subc_protocol::{
    BindIdentity as SubcBindIdentity, Flags, Frame, FrameType, ModuleHelloAckBody, Principal,
    Priority, RouteTarget, PROTOCOL_VERSION,
};
use subc_transport::{read_frame, write_frame};
use tokio::sync::{mpsc, oneshot};

/// The module the head's plan lets fork the head.
const ASKER: &str = "synthetic-voice";

fn asker() -> Principal {
    Principal::Reserved {
        module_id: ASKER.into(),
    }
}

fn stranger() -> Principal {
    Principal::Reserved {
        module_id: "synthetic-stranger".into(),
    }
}

fn owner() -> Principal {
    Principal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

/// One `fork_call` the executor was handed, as the test reads it back.
#[derive(Clone)]
struct Call {
    fork_id: String,
    ask_id: String,
    attempt: u64,
    admission_id: String,
    run_id: String,
    request: broca_protocol::request::CallOptions,
    base: broca_protocol::request::CallOptions,
    base_digest: String,
}

/// A fork call that waits: the test hears when the call arrives and lets it go.
struct Hold {
    arrived: Option<oneshot::Sender<()>>,
    release: oneshot::Receiver<()>,
}

/// What the fork's model calls answer, in order, and what they were asked.
#[derive(Default)]
struct ForkScript {
    outcomes: Mutex<VecDeque<ForkCallOutcome>>,
    calls: Mutex<Vec<Call>>,
    hold: tokio::sync::Mutex<Option<Hold>>,
    /// Usage each call reports before it answers.
    report: Mutex<Option<RunUsage>>,
    released: Mutex<Vec<String>>,
    /// The next call's own release didn't reach routing (`fork_release_failed`).
    release_fails: std::sync::atomic::AtomicBool,
}

impl ForkScript {
    fn answer(&self, outcome: ForkCallOutcome) {
        self.outcomes.lock().unwrap().push_back(outcome);
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    /// Holds the next call; the returned pair says when it arrived and lets it go.
    async fn hold_next(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (arrived, arrival) = oneshot::channel();
        let (release, released) = oneshot::channel();
        *self.hold.lock().await = Some(Hold {
            arrived: Some(arrived),
            release: released,
        });
        (arrival, release)
    }
}

fn usage(input: u64, cached: u64, output: u64) -> RunUsage {
    serde_json::from_value(json!({
        "steps": 1, "unreported_steps": 0, "input_tokens": input,
        "cached_input_tokens": cached, "output_tokens": output, "by_model": [],
    }))
    .unwrap()
}

/// The provider's account of a call that spent `usage`.
fn spend_of(usage: &RunUsage) -> broca_module::executor::ForkStepSpend {
    broca_module::executor::ForkStepSpend {
        usage: Usage {
            input_tokens: usage.input_tokens,
            cached_input_tokens: usage.cached_input_tokens,
            output_tokens: usage.output_tokens,
            ..Usage::default()
        },
        service_tier_used: None,
        finish_reason: Some(FinishReason::Stop),
    }
}

fn answered(text: &str) -> ForkCallOutcome {
    ForkCallOutcome::Answered {
        answer: text.into(),
        finish: "end_turn".into(),
        usage: usage(17, 3, 11),
    }
}

/// Counts the head's compaction calls; a fork must make none.
#[derive(Default)]
struct CountingCompaction {
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl broca_core::CompactionPlane for CountingCompaction {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: broca_core::compaction_provider::setup::SetupRequest,
    ) -> Result<
        broca_core::compaction_provider::setup::SetupAnswer,
        broca_core::CompactionUnavailable,
    > {
        panic!("a head without a compaction item never sets up compaction")
    }

    async fn call_v2(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: &str,
        _: Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(serde_json::value::to_raw_value(&json!({"answer": "nothing"})).unwrap())
    }

    async fn provider_capabilities(&self, _: &str) -> broca_core::ProviderCapabilities {
        broca_core::ProviderCapabilities::Declared(vec!["compaction-provider/v2".to_owned()])
    }
}

/// A scripted provider whose every step reports the attempt a transport would: one whose request
/// digest is that of the bytes the provider renders, so a fork can check its rebuilt base.
struct Attempting(broca_core::provider::StubProvider);

#[async_trait]
impl broca_core::Provider for Attempting {
    async fn step(
        &self,
        step_id: broca_protocol::StepId,
        opts: &broca_protocol::request::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let digest = broca_core::render::request_digest_hex(&self.render_request(opts));
        let mut inner = self.0.step(step_id, opts).await;
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            // A scripted step that fails is an attempt the provider didn't serve.
            let first = inner.recv().await;
            let failed = matches!(
                first,
                Some(NormalizedEvent::Control(ProviderControl::Error { .. }))
            );
            let (status, outcome) = if failed {
                (500, "http_error")
            } else {
                (200, "complete")
            };
            let attempt = serde_json::from_value(json!({
                "attempt": 0,
                "request_body_len": 10,
                "request_body_sha256": digest,
                "http_status": status,
                "response_body_len": 0,
                "response_body_sha256": "0".repeat(64),
                "outcome": outcome,
                "ts_ms": 1,
                "sent_at_ms": 1,
            }))
            .unwrap();
            if tx
                .send(NormalizedEvent::Control(ProviderControl::AttemptFinished {
                    attempt,
                }))
                .await
                .is_err()
            {
                return;
            }
            if let Some(event) = first {
                if tx.send(event).await.is_err() {
                    return;
                }
            }
            while let Some(event) = inner.recv().await {
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });
        rx
    }
}

/// Runs the head's runs through the new loop over a scripted provider, as the module's executor
/// does, and its forks' calls through [`ForkScript`].
struct StoreLoop {
    provider: Attempting,
    compaction: Arc<CountingCompaction>,
    forks: Arc<ForkScript>,
}

#[async_trait]
impl RunExecutor for StoreLoop {
    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::CompactionPlane>> {
        Some(self.compaction.clone() as Arc<dyn broca_core::CompactionPlane>)
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
        let result = async {
            use broca_session_loop::writer::Writer;
            let tools = broca_core::StubToolPlane::new();
            let now = || 1;
            let seams = broca_session_loop::run::Seams {
                person_steers: None,
                provider: &self.provider,
                tools: &tools,
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.clone().unwrap_or_default(),
                stop: Default::default(),
                held: None,
                approval: &AllowAll,
                hook_plane: None,
                now_ms: &now,
                session_address: None,
                compaction_plane: control.compaction_plane.as_deref(),
                compaction_session: None,
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
                    let admission = broca_session_loop::admit::admission(
                        &mut req,
                        &control,
                        &self.provider,
                        writer.operations(),
                        Some(1),
                    )
                    .map_err(StoreRunError::Admission)?;
                    broca_session_loop::run::run(&mut writer, parts.as_mut(), admission, &seams)
                        .await
                }
                StoreRunMode::Seal(cause) => {
                    broca_session_loop::seal(&mut writer, parts.as_ref(), &seams, cause).await
                }
                _ => broca_session_loop::run::resume(&mut writer, parts.as_ref(), &seams).await,
            }
            .map_err(StoreRunError::Loop)
        }
        .await;
        StoreRunEnd { parts, result }
    }

    async fn fork_call(&self, job: ForkCallJob) -> ForkCallOutcome {
        self.forks.calls.lock().unwrap().push(Call {
            fork_id: job.fork_id.clone(),
            ask_id: job.ask_id.clone(),
            attempt: job.attempt,
            admission_id: job.admission_id.clone(),
            run_id: job.run_id.clone(),
            request: job.request.clone(),
            base: job.base.opts.clone(),
            base_digest: job.base.request_body_sha256.clone(),
        });
        let report = self.forks.report.lock().unwrap().clone();
        if let Some(report) = report {
            if let Some(on_step) = &job.on_step {
                on_step(spend_of(&report));
            }
            if let Some(on_usage) = &job.on_usage {
                on_usage(report);
            }
        }
        let hold = self.forks.hold.lock().await.take();
        if let Some(mut hold) = hold {
            if let Some(arrived) = hold.arrived.take() {
                let _ = arrived.send(());
            }
            // A dropped release is a crash cut: the call never answers.
            if hold.release.await.is_err() {
                std::future::pending::<()>().await;
            }
        }
        let outcome = self
            .forks
            .outcomes
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| {
                ForkCallOutcome::Refused(ForkCallRefusal::Unsupported("no scripted answer".into()))
            });
        // A call the model answered, or failed in, reports what it spent; a refusal made none.
        if let (
            ForkCallOutcome::Answered { usage, .. } | ForkCallOutcome::ModelFailed { usage, .. },
            Some(on_step),
        ) = (&outcome, &job.on_step)
        {
            on_step(spend_of(usage));
        }
        outcome
    }

    async fn fork_release(&self, job: ForkReleaseJob) -> Result<(), String> {
        self.forks.released.lock().unwrap().push(job.admission_id);
        Ok(())
    }

    fn fork_release_failed(&self, _admission_id: &str) -> bool {
        self.forks
            .release_fails
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }
}

fn text_step(id: &str, text: &str) -> Vec<NormalizedEvent> {
    finished_step(id, text, FinishReason::Stop)
}

/// A step whose reply ended for `finish_reason`.
fn finished_step(id: &str, text: &str, finish_reason: FinishReason) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(100),
                output_tokens: Some(20),
                ..Usage::default()
            },
            finish_reason,
            retries_used: 0,
        }),
    ]
}

/// A step the provider fails with a permanent error.
fn failed_step() -> Vec<NormalizedEvent> {
    vec![NormalizedEvent::Control(ProviderControl::Error {
        error: serde_json::from_value(
            json!({"class": "permanent", "message": "the provider refused the request"}),
        )
        .unwrap(),
        retries_used: 0,
    })]
}

fn request(text: &str) -> SubmissionRequest {
    serde_json::from_value(json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": [{"role": "user", "content": [{"type": "text", "text": text}]}],
        "stop_when": [{"type": "step_count", "n": 3}],
    }))
    .unwrap()
}

/// A plan whose `fork_callers` are `callers`.
fn plan(identity: &str, callers: Value) -> SendPlan {
    let plan = json!({ "fork_callers": callers });
    SendPlan {
        identity: identity.into(),
        manifest: Some(Arc::new(manifest(identity, plan))),
        model_check: None,
    }
}

fn manifest(identity: &str, plan: Value) -> broca_records::record::PlanManifest {
    broca_records::record::PlanManifest {
        rich_ui: None,
        plan: broca_records::record::OpaqueJson::from_json_text(plan.to_string()).unwrap(),
        plan_identity: identity.into(),
        composition_digest: "composition".into(),
        tools: Arc::new(Vec::new()),
        system_text: "you are a worker".into(),
        system_digest: "system".into(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![],
    }
}

struct Rig {
    dir: PathBuf,
    registry: Arc<LineageRegistry>,
    key: BindIdentity,
    forks: Arc<ForkScript>,
    compaction: Arc<CountingCompaction>,
    export: Arc<broca_store::SqliteStore>,
    reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    serve: tokio::task::JoinHandle<Result<ServeEnd, broca_module_serve::serve::ServeIoError>>,
    corr: u64,
    /// Replies read while waiting for another request's.
    pending: std::collections::HashMap<u64, Frame>,
    /// What the next call's reply is kept as, as a reader sample.
    sample_next: Option<String>,
    /// Whether dropping the rig removes its state root.
    owns_dir: bool,
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.serve.abort();
        if self.owns_dir {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

fn scratch(tag: &str) -> PathBuf {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-fork-answer-serve-{}-{tag}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    broca_core::media_uploads::create_upload_directories(&dir).unwrap();
    dir
}

impl Rig {
    /// A module on a fresh state root whose new storage creates sessions, the head's runs
    /// scripted by `steps`.
    async fn start(tag: &str, steps: Vec<Vec<NormalizedEvent>>) -> Self {
        Self::open(scratch(tag), steps, Arc::default(), true).await
    }

    /// A module on the state root `dir`, as a restart opens it.
    async fn open(
        dir: PathBuf,
        steps: Vec<Vec<NormalizedEvent>>,
        forks: Arc<ForkScript>,
        owns_dir: bool,
    ) -> Self {
        let compaction = Arc::new(CountingCompaction::default());
        let executor: Arc<dyn RunExecutor> = Arc::new(StoreLoop {
            provider: Attempting(broca_core::provider::StubProvider::new(steps)),
            compaction: Arc::clone(&compaction),
            forks: Arc::clone(&forks),
        });
        // Spend goes to the usage export the module's runs are billed into.
        let export = Arc::new(broca_store::SqliteStore::open(dir.join("export.db")).unwrap());
        let runtime = broca_module::store_open::StoreRuntime::open(
            &dir,
            broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
            executor.clone(),
            Duration::from_millis(100),
        )
        .await
        .unwrap()
        .with_billing(broca_module::store_billing::StoreBilling {
            index: Arc::clone(&export) as Arc<dyn broca_core::ExportIndex>,
            resolver: Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::MeteredApi, 4)),
        });
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            executor,
            std::sync::Arc::new(broca_module::store_open::StoreSlot::loaded(Arc::new(
                runtime,
            ))),
        )
        .with_idle_ttl(Duration::from_millis(100));
        let registry = LineageRegistry::new(Arc::new(opener));
        let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
        broca_store::session_refs::migrate(
            &dir.join("store.db"),
            &broca_store::session_refs::StoredSessions::new(&dir),
        )
        .unwrap();
        registry.set_session_address_inventory(store);
        let registry = Arc::new(registry);
        let key = registry
            .resolve_session_address(&BindIdentity::new("", "runner", "head"), None, None)
            .await
            .unwrap();
        let (client, server) = tokio::io::duplex(256 * 1024);
        let (mut reader, mut writer) = tokio::io::split(client);
        let (server_read, server_write) = tokio::io::split(server);
        let served = registry.clone();
        let serve = tokio::spawn(async move {
            serve_connection(
                server_read,
                server_write,
                "broca",
                served.clone(),
                served,
                broca_module_serve::serve::VaultGauge::not_in_use(),
            )
            .await
        });
        let hello = read_frame(&mut reader).await.unwrap().unwrap();
        let ack = Frame::build(
            FrameType::HelloAck,
            Flags::new(false, Priority::Passive, false),
            0,
            0,
            hello.header.corr,
            serde_json::to_vec(&ModuleHelloAckBody {
                negotiated_ver: PROTOCOL_VERSION,
                subc_ops: vec![],
                subc_capabilities: vec![],
                storage: None,
                machine_id: None,
            })
            .unwrap(),
        )
        .unwrap();
        write_frame(&mut writer, &ack).await.unwrap();
        Self {
            dir,
            registry,
            key,
            forks,
            compaction,
            export,
            reader,
            writer,
            serve,
            corr: 1,
            pending: Default::default(),
            sample_next: None,
            owns_dir,
        }
    }

    /// Sends `text` to the head and waits until its run has ended.
    async fn send(&self, run_id: &str, text: &str, plan: Option<SendPlan>) {
        let outcome = self
            .registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                request(text),
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                broca_protocol::Delivery::Queue,
                plan,
                None,
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .expect("the session opens");
        assert!(
            matches!(
                outcome,
                SendOutcome::Active { .. } | SendOutcome::Pending { .. }
            ),
            "{outcome:?}"
        );
        self.settled(run_id).await;
    }

    async fn settled(&self, run_id: &str) {
        use broca_module::serve::{ServeHandler, SessionReadRequest};
        for _ in 0..2000 {
            let read = self
                .registry
                .read(SessionReadRequest::new(self.key.clone(), None, Some(1)))
                .await
                .expect("the session reads");
            if read.lineage_state.last_run_id.as_deref() == Some(run_id)
                && read.lineage_state.state != "active"
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("run {run_id} never settled");
    }

    /// Starts the head with a plan letting [`ASKER`] fork it, and binds route 1 for the asker,
    /// route 2 for a stranger and route 3 for the head's owner.
    async fn head(tag: &str, steps: Vec<Vec<NormalizedEvent>>) -> Self {
        let mut rig = Self::start(tag, steps).await;
        rig.send("r1", "hello", Some(plan("plan-1", json!([ASKER]))))
            .await;
        rig.bind(1, asker()).await;
        rig.bind(2, stranger()).await;
        rig.bind(3, owner()).await;
        rig
    }

    /// Sends a request without waiting for its reply; returns its correlation id.
    async fn send_request(&mut self, channel: u16, body: Value) -> u64 {
        self.corr += 1;
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            channel,
            u32::from(channel != 0),
            self.corr,
            serde_json::to_vec(&body).unwrap(),
        )
        .unwrap();
        write_frame(&mut self.writer, &frame).await.unwrap();
        self.corr
    }

    /// The reply to the request `corr`, keeping replies to other requests for their callers.
    async fn reply_to(&mut self, corr: u64) -> Frame {
        loop {
            if let Some(frame) = self.pending.remove(&corr) {
                return frame;
            }
            let frame = tokio::time::timeout(Duration::from_secs(20), read_frame(&mut self.reader))
                .await
                .expect("a reply")
                .unwrap()
                .unwrap();
            self.pending.insert(frame.header.corr, frame);
        }
    }

    async fn request(&mut self, channel: u16, body: Value) -> Frame {
        let corr = self.send_request(channel, body).await;
        self.reply_to(corr).await
    }

    async fn bind(&mut self, channel: u16, principal: Principal) {
        let bind = ModuleControlRequest::RouteBind {
            route_channel: channel,
            epoch: 1,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new("runner", "head"),
            principal: Some(principal),
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        };
        let reply = self.request(0, serde_json::to_value(bind).unwrap()).await;
        assert_eq!(
            reply.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&reply.body)
        );
    }

    /// The reply to `method`: its result, or its error body.
    async fn call(&mut self, channel: u16, method: &str, params: Value) -> Result<Value, Value> {
        let reply = self
            .request(channel, json!({"method": method, "params": params}))
            .await;
        if let Some(name) = self.sample_next.take() {
            sample(&name, &reply);
        }
        outcome(&reply)
    }

    /// Names the reply to the next call as a reader sample (see [`Rig::sampled`]).
    fn sample_next(&mut self, name: &str) {
        self.sample_next = Some(name.to_owned());
    }

    async fn prepare(&mut self, channel: u16) -> Result<String, Value> {
        self.call(channel, "session.fork_prepare", json!({}))
            .await
            .map(|reply| reply["fork_id"].as_str().unwrap().to_owned())
    }

    async fn ask(
        &mut self,
        channel: u16,
        fork: &str,
        ask_id: &str,
        question: &str,
    ) -> Result<Value, Value> {
        self.call(
            channel,
            "session.fork_ask",
            json!({"fork_id": fork, "ask_id": ask_id, "question": question}),
        )
        .await
    }

    async fn end(&mut self, channel: u16, fork: &str) -> Result<Value, Value> {
        self.call(channel, "session.fork_end", json!({"fork_id": fork}))
            .await
    }

    /// [`Rig::call`], also writing the reply's wire bytes to `name` in the folder
    /// `BROCA_FORK_ANSWER_SAMPLES` names, when it names one: samples for the replies' readers.
    async fn sampled(
        &mut self,
        name: &str,
        channel: u16,
        method: &str,
        params: Value,
    ) -> Result<Value, Value> {
        let reply = self
            .request(channel, json!({"method": method, "params": params}))
            .await;
        sample(name, &reply);
        outcome(&reply)
    }

    /// The head as its owner reads it: its head and every message.
    async fn head_state(&mut self) -> (Value, Value) {
        let head = self.call(3, "session.head", json!({})).await.unwrap();
        let read = self.call(3, "session.read", json!({})).await.unwrap();
        (head, read)
    }
}

fn sample(name: &str, reply: &Frame) {
    if let Some(dir) = std::env::var_os("BROCA_FORK_ANSWER_SAMPLES") {
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(name), &reply.body).unwrap();
    }
}

/// A reply's result, or its error body.
fn outcome(reply: &Frame) -> Result<Value, Value> {
    let body: Value = serde_json::from_slice(&reply.body).unwrap();
    match reply.header.ty {
        FrameType::Response => Ok(body["result"].clone()),
        FrameType::Error => Err(body),
        other => panic!("unexpected {other:?}"),
    }
}

fn ask_params(fork: &str, ask_id: &str, question: &str) -> Value {
    json!({"method": "session.fork_ask",
        "params": {"fork_id": fork, "ask_id": ask_id, "question": question}})
}

impl Rig {
    /// The usage export's facts of every fork session, in the order they were written.
    fn fork_facts(&self) -> Vec<broca_core::billing::SpendSegment> {
        use broca_core::ExportIndex as _;
        self.export
            .read_page(None, 1000)
            .unwrap()
            .facts
            .into_iter()
            .filter_map(|fact| fact.segment.as_session().cloned())
            .filter(|segment| segment.session.session.contains(":fork:"))
            .collect()
    }
}

fn code(error: &Value) -> &str {
    error["code"].as_str().unwrap()
}

/// The text of every message `opts` sends, in order.
fn texts(opts: &broca_protocol::request::CallOptions) -> Vec<String> {
    opts.prompt
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.to_string()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_fork_answers_replays_and_ends_without_touching_the_head() {
    let mut rig = Rig::head("replay", vec![text_step("a", "head reply")]).await;
    let before = rig.head_state().await;
    let fork = rig
        .sampled(
            "fork_prepare.reply.json",
            1,
            "session.fork_prepare",
            json!({}),
        )
        .await
        .unwrap()["fork_id"]
        .as_str()
        .unwrap()
        .to_owned();
    // Requests are strict, and name the member at fault.
    for (method, params) in [
        ("session.fork_prepare", json!({"unexpected": true})),
        (
            "session.fork_ask",
            json!({"fork_id": fork, "ask_id": "a", "question": "q", "unexpected": true}),
        ),
        (
            "session.fork_end",
            json!({"fork_id": fork, "unexpected": true}),
        ),
    ] {
        let error = rig.call(1, method, params).await.unwrap_err();
        assert_eq!(code(&error), "invalid_params", "{method}");
        assert_eq!(error["detail"]["field"], "unexpected", "{method}");
    }
    // Broca declares `fork_effort`, whose hints are closed: one outside them is refused.
    for effort in [json!("minimal"), json!("max"), json!(2)] {
        let error = rig
            .call(
                1,
                "session.fork_ask",
                json!({"fork_id": fork, "ask_id": "e", "question": "q", "effort": effort}),
            )
            .await
            .unwrap_err();
        assert_eq!(
            (code(&error), error["detail"]["field"].as_str()),
            ("invalid_params", Some("effort")),
            "{effort}"
        );
    }
    // Nor `fork_tools`: a tool list or framing, even an empty list, is an unknown member, so the
    // fork never runs a tool it wasn't built for.
    for (member, value) in [
        ("allowed_tools", json!(["read"])),
        ("allowed_tools", json!([])),
        ("framing", json!("Answer briefly.")),
    ] {
        let mut ask = json!({"fork_id": fork, "ask_id": "t", "question": "q"});
        ask[member] = value.clone();
        let error = rig.call(1, "session.fork_ask", ask).await.unwrap_err();
        assert_eq!(
            (code(&error), error["detail"]["field"].as_str()),
            ("invalid_params", Some(member)),
            "{member}: {value}"
        );
    }
    for (ask_id, question, field) in [
        (String::new(), "q", "ask_id"),
        ("界".repeat(257), "q", "ask_id"),
        ("empty".into(), "", "question"),
    ] {
        let error = rig.ask(1, &fork, &ask_id, question).await.unwrap_err();
        assert_eq!(
            (code(&error), error["detail"]["field"].as_str()),
            ("invalid_params", Some(field))
        );
    }
    assert_ne!(
        rig.prepare(1).await.unwrap(),
        fork,
        "fork ids are never reused"
    );
    // Only a listed caller prepares; the head's owner isn't listed. A fork answers only its
    // preparer, and any other caller can't tell it exists.
    for channel in [2, 3] {
        assert_eq!(
            code(&rig.prepare(channel).await.unwrap_err()),
            "scope_owner_mismatch"
        );
        assert_eq!(
            code(&rig.ask(channel, &fork, "one", "q").await.unwrap_err()),
            "unknown_fork"
        );
        assert_eq!(
            code(&rig.end(channel, &fork).await.unwrap_err()),
            "unknown_fork"
        );
    }
    for unknown in ["fork-00000000000000000000000000000000", "../head"] {
        assert_eq!(
            code(&rig.ask(1, unknown, "one", "q").await.unwrap_err()),
            "unknown_fork"
        );
    }

    rig.sampled(
        "fork_prepare.scope_owner_mismatch.json",
        2,
        "session.fork_prepare",
        json!({}),
    )
    .await
    .unwrap_err();
    rig.sampled(
        "fork_ask.unknown_fork.json",
        2,
        "session.fork_ask",
        json!({"fork_id": fork, "ask_id": "one", "question": "q"}),
    )
    .await
    .unwrap_err();
    rig.sampled(
        "fork_ask.invalid_params.json",
        1,
        "session.fork_ask",
        json!({"fork_id": fork, "ask_id": "", "question": "q"}),
    )
    .await
    .unwrap_err();
    rig.forks.answer(answered("The title changed."));
    let first = rig
        .sampled(
            "fork_ask.reply.json",
            1,
            "session.fork_ask",
            json!({"fork_id": fork, "ask_id": "one", "question": "What changed?"}),
        )
        .await
        .unwrap();
    assert_eq!(
        first,
        json!({"answer": "The title changed.", "finish": "end_turn",
            "usage": serde_json::to_value(usage(17, 3, 11)).unwrap()})
    );
    let calls = rig.forks.calls();
    assert_eq!(calls.len(), 1);
    let call = &calls[0];
    assert_eq!(
        (call.fork_id.as_str(), call.ask_id.as_str(), call.attempt),
        (fork.as_str(), "one", 1)
    );
    assert_eq!(
        call.admission_id,
        broca_module::executor::fork_admission_id(&fork, "one", 1)
    );
    assert_eq!(call.run_id, call.admission_id);
    // The call's spend is in the usage export: one fact, under the attempt's own run and the
    // fork's own session, never the head's.
    let fork_session = broca_module::fork_answer::fork_session(&rig.key, &fork);
    let facts = rig.fork_facts();
    assert_eq!(facts.len(), 1, "{facts:?}");
    assert_eq!(
        (facts[0].run_id.0.as_str(), &facts[0].session),
        (call.run_id.as_str(), &fork_session)
    );
    assert_eq!(
        (facts[0].usage.input_tokens, facts[0].usage.output_tokens),
        (Some(17), Some(11))
    );
    // The request is the head's last request with the question appended.
    let base = serde_json::to_value(&call.base.prompt).unwrap();
    let sent = serde_json::to_value(&call.request.prompt).unwrap();
    assert_eq!(
        sent.as_array().unwrap()[..base.as_array().unwrap().len()],
        base.as_array().unwrap()[..]
    );
    assert_eq!(texts(&call.request).last().unwrap(), "What changed?");
    assert_eq!(call.request.prompt.len(), call.base.prompt.len() + 1);
    assert_eq!(
        serde_json::to_value(&*call.request.tools).unwrap(),
        serde_json::to_value(&*call.base.tools).unwrap()
    );

    // A repeat replays without a model call; another question under the same id is refused.
    assert_eq!(
        rig.ask(1, &fork, "one", "What changed?").await.unwrap(),
        first
    );
    rig.sample_next("fork_ask.ask_id_reuse.json");
    assert_eq!(
        code(
            &rig.ask(1, &fork, "one", "Something else?")
                .await
                .unwrap_err()
        ),
        "ask_id_reuse"
    );
    // The same question with an effort hint is a different ask too.
    let hinted = rig
        .call(
            1,
            "session.fork_ask",
            json!({"fork_id": fork, "ask_id": "one", "question": "What changed?", "effort": "low"}),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&hinted), "ask_id_reuse");
    assert_eq!(rig.forks.calls().len(), 1);

    // A later ask carries the fork's conversation, on the same base.
    rig.forks.answer(answered("lib.rs"));
    rig.ask(1, &fork, "two", "Which file?").await.unwrap();
    let second = rig.forks.calls()[1].clone();
    // A replay and a refused reuse spent nothing; the second call is billed under its own run.
    let facts = rig.fork_facts();
    assert_eq!(
        facts
            .iter()
            .map(|fact| fact.run_id.0.as_str())
            .collect::<Vec<_>>(),
        [call.run_id.as_str(), second.run_id.as_str()]
    );
    assert_eq!(second.base_digest, call.base_digest);
    let asked = texts(&second.request);
    assert_eq!(
        asked[asked.len() - 3..],
        ["What changed?", "The title changed.", "Which file?"]
    );

    // Nothing of the head's moved, and its compaction provider heard nothing.
    assert_eq!(rig.head_state().await, before);
    assert_eq!(
        rig.compaction
            .calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );

    rig.sample_next("fork_end.reply.json");
    let ended = json!({"fork_id": fork, "ended": true});
    assert_eq!(rig.end(1, &fork).await.unwrap(), ended);
    assert_eq!(rig.end(1, &fork).await.unwrap(), ended);
    assert_eq!(
        rig.ask(1, &fork, "one", "What changed?").await.unwrap(),
        first
    );
    rig.sample_next("fork_ask.fork_ended.json");
    assert_eq!(
        code(&rig.ask(1, &fork, "three", "More?").await.unwrap_err()),
        "fork_ended"
    );
    assert_eq!(rig.forks.calls().len(), 2);
}

#[tokio::test]
async fn an_ask_in_flight_makes_the_fork_busy_and_an_end_lets_it_finish() {
    let mut rig = Rig::head("busy", vec![text_step("a", "head reply")]).await;
    let fork = rig.prepare(1).await.unwrap();
    let (arrival, release) = rig.forks.hold_next().await;
    rig.forks.answer(answered("held answer"));
    let held = rig
        .send_request(1, ask_params(&fork, "held", "held question"))
        .await;
    arrival.await.unwrap();
    rig.sample_next("fork_ask.fork_busy.json");
    for ask_id in ["held", "other"] {
        assert_eq!(
            code(&rig.ask(1, &fork, ask_id, "q").await.unwrap_err()),
            "fork_busy"
        );
    }
    assert_eq!(
        rig.end(1, &fork).await.unwrap(),
        json!({"fork_id": fork, "ended": true})
    );
    release.send(()).unwrap();
    let answer = outcome(&rig.reply_to(held).await).unwrap();
    assert_eq!(answer["answer"], "held answer");
    assert_eq!(
        rig.ask(1, &fork, "held", "held question").await.unwrap(),
        answer
    );
    assert_eq!(
        code(&rig.ask(1, &fork, "after", "q").await.unwrap_err()),
        "fork_ended"
    );
}

#[tokio::test]
async fn a_failed_call_stores_model_error_and_a_tool_call_ends_the_fork() {
    let mut rig = Rig::head("failures", vec![text_step("a", "head reply")]).await;
    let fork = rig.prepare(1).await.unwrap();
    rig.forks.answer(ForkCallOutcome::ModelFailed {
        usage: usage(5, 0, 1),
        error: "scripted model failure".into(),
    });
    rig.sample_next("fork_ask.model_error.json");
    let failed = rig.ask(1, &fork, "fail", "failed question").await.unwrap();
    assert_eq!(
        failed,
        json!({"answer": "", "finish": "model_error",
            "usage": serde_json::to_value(usage(5, 0, 1)).unwrap()})
    );
    assert_eq!(
        rig.ask(1, &fork, "fail", "failed question").await.unwrap(),
        failed
    );
    rig.forks.answer(answered("recovered"));
    rig.ask(1, &fork, "next", "fresh question").await.unwrap();
    let asked = texts(&rig.forks.calls()[1].request);
    assert!(
        !asked.iter().any(|text| text == "failed question"),
        "{asked:?}"
    );

    // A reply cut at its output limit is an answer like any other.
    rig.forks.answer(ForkCallOutcome::Answered {
        answer: "cut sho".into(),
        finish: "max_tokens".into(),
        usage: usage(17, 3, 64),
    });
    rig.sample_next("fork_ask.max_tokens.json");
    let cut = rig.ask(1, &fork, "long", "a long answer").await.unwrap();
    assert_eq!(
        (cut["answer"].as_str(), cut["finish"].as_str()),
        (Some("cut sho"), Some("max_tokens"))
    );
    rig.sample_next("fork_ask.replay.max_tokens.json");
    assert_eq!(
        rig.ask(1, &fork, "long", "a long answer").await.unwrap(),
        cut
    );
    rig.forks.answer(ForkCallOutcome::Answered {
        answer: "before tool".into(),
        finish: "tool_call_refused".into(),
        usage: usage(17, 3, 11),
    });
    rig.sample_next("fork_ask.tool_call_refused.json");
    let refused = rig.ask(1, &fork, "tool", "try a tool").await.unwrap();
    assert_eq!(
        (refused["answer"].as_str(), refused["finish"].as_str()),
        (Some("before tool"), Some("tool_call_refused"))
    );
    assert_eq!(
        rig.ask(1, &fork, "tool", "try a tool").await.unwrap(),
        refused
    );
    assert_eq!(
        code(&rig.ask(1, &fork, "new", "new question").await.unwrap_err()),
        "fork_ended"
    );
    assert_eq!(rig.forks.calls().len(), 4);
}

#[tokio::test]
async fn refusals_before_a_model_call_store_nothing_and_each_retry_is_a_new_attempt() {
    let mut rig = Rig::head("refusals", vec![text_step("a", "head reply")]).await;
    let fork = rig.prepare(1).await.unwrap();
    rig.forks
        .answer(ForkCallOutcome::Refused(ForkCallRefusal::AccountHeld {
            routing_code: Some("account_hold".into()),
            mismatch: None,
        }));
    rig.forks
        .answer(ForkCallOutcome::Refused(ForkCallRefusal::AccountHeld {
            routing_code: None,
            mismatch: Some("credential".into()),
        }));
    rig.forks
        .answer(ForkCallOutcome::Refused(ForkCallRefusal::AccountBlocked {
            routing_code: "no_account".into(),
        }));
    rig.forks
        .answer(ForkCallOutcome::Refused(ForkCallRefusal::BaseUnavailable(
            "rendered differently".into(),
        )));
    rig.forks.answer(answered("finally"));
    let expected = [
        ("fork_account_held", json!({"routing_code": "account_hold"})),
        ("fork_account_held", json!({"mismatch": "credential"})),
        (
            "fork_account_blocked",
            json!({"routing_code": "no_account"}),
        ),
        ("fork_base_unavailable", Value::Null),
    ];
    for (index, (expected_code, detail)) in expected.iter().enumerate() {
        // A refusal is never stored, so the same id may come back with another question.
        rig.sample_next(&format!("fork_ask.refused.{index}.{expected_code}.json"));
        let error = rig
            .ask(1, &fork, "retry", &format!("question {index}"))
            .await
            .unwrap_err();
        assert_eq!(code(&error), *expected_code);
        if !detail.is_null() {
            assert_eq!(&error["detail"], detail);
        }
    }
    assert_eq!(
        rig.ask(1, &fork, "retry", "question 4").await.unwrap()["answer"],
        "finally"
    );
    let calls = rig.forks.calls();
    assert_eq!(
        calls.iter().map(|call| call.attempt).collect::<Vec<_>>(),
        [1, 2, 3, 4, 5]
    );
    let admissions: std::collections::BTreeSet<_> =
        calls.iter().map(|call| call.admission_id.clone()).collect();
    assert_eq!(admissions.len(), 5, "every attempt gets its own admission");
    // Only the attempt that reached the model spent anything: the refused ones record none.
    let facts = rig.fork_facts();
    assert_eq!(
        facts
            .iter()
            .map(|fact| fact.run_id.0.as_str())
            .collect::<Vec<_>>(),
        [calls[4].run_id.as_str()]
    );
}

/// The system text the head's plan gives it, first in every request it sends.
const SYSTEM: &str = "you are a worker";

/// The digest the executor checks a fork's base against: its options rendered under the rig's
/// stateless render config.
fn rendered_digest(opts: &broca_protocol::request::CallOptions) -> String {
    broca_core::render::request_digest_hex(&broca_core::render::canonical_request_bytes(opts))
}

#[tokio::test]
async fn a_finished_answer_follows_the_base_and_a_cut_off_one_does_not() {
    let mut rig = Rig::head("answered", vec![text_step("a", "head reply")]).await;
    let fork = rig.prepare(1).await.unwrap();
    rig.forks.answer(answered("one"));
    rig.ask(1, &fork, "one", "What did you say?").await.unwrap();
    let call = rig.forks.calls()[0].clone();
    // The head's reply asked for no tool call, so it ends the base, as an assistant message
    // after the request, and the question follows it.
    assert_eq!(texts(&call.base), [SYSTEM, "hello", "head reply"]);
    assert_eq!(
        call.base.prompt.last().unwrap().role,
        broca_protocol::content::Role::Assistant
    );
    assert_eq!(
        texts(&call.request),
        [SYSTEM, "hello", "head reply", "What did you say?"]
    );
    // The base's digest is that of the bytes with the answer, which the executor checks.
    assert_eq!(call.base_digest, rendered_digest(&call.base));

    let mut cut = Rig::head(
        "cut-off",
        vec![finished_step("a", "a reply cut sh", FinishReason::Length)],
    )
    .await;
    let fork = cut.prepare(1).await.unwrap();
    cut.forks.answer(answered("one"));
    cut.ask(1, &fork, "one", "What did you say?").await.unwrap();
    let call = cut.forks.calls()[0].clone();
    // An answer cut off at the output limit isn't the head's to carry on as it is.
    assert_eq!(texts(&call.base), [SYSTEM, "hello"]);
    assert_eq!(call.base_digest, rendered_digest(&call.base));
}

/// A hint lowers an answer's effort only where the head's model keeps its cache across the
/// change. The rig's head runs with no render config of its own, so the hint is taken and
/// changes nothing: the call carries no answer effort and the reply names none.
#[tokio::test]
async fn an_effort_hint_the_head_cannot_take_leaves_the_answer_as_it_is() {
    let mut rig = Rig::head("hint", vec![text_step("a", "head reply")]).await;
    let fork = rig.prepare(1).await.unwrap();
    rig.forks.answer(answered("one"));
    let reply = rig
        .call(
            1,
            "session.fork_ask",
            json!({"fork_id": fork, "ask_id": "one", "question": "q", "effort": "low"}),
        )
        .await
        .unwrap();
    assert_eq!(reply["answer"], "one");
    assert!(reply.get("effort_applied").is_none(), "{reply}");
    assert_eq!(
        rig.forks.calls()[0].request.render_episodes.answer_effort,
        None
    );
}

#[tokio::test]
async fn a_newer_request_the_provider_did_not_serve_is_not_the_base() {
    let mut rig = Rig::head(
        "unserved",
        vec![text_step("a", "head reply"), failed_step()],
    )
    .await;
    rig.send("r2", "the provider refuses this", None).await;
    let fork = rig.prepare(1).await.unwrap();
    rig.forks.answer(answered("one"));
    rig.ask(1, &fork, "one", "What did you say?").await.unwrap();
    let call = rig.forks.calls()[0].clone();
    // The newest request the provider served is the first, with its answer.
    assert_eq!(texts(&call.base), [SYSTEM, "hello", "head reply"]);
}

#[tokio::test]
async fn the_base_stays_fixed_while_the_head_runs_on() {
    let mut rig = Rig::head(
        "head-moves",
        vec![text_step("a", "head reply"), text_step("b", "second reply")],
    )
    .await;
    let fork = rig.prepare(1).await.unwrap();
    rig.forks.answer(answered("one"));
    rig.ask(1, &fork, "one", "first").await.unwrap();
    rig.send("r2", "the head goes on", None).await;
    rig.forks.answer(answered("two"));
    rig.ask(1, &fork, "two", "second").await.unwrap();
    let calls = rig.forks.calls();
    assert_eq!(calls[1].base_digest, calls[0].base_digest);
    assert_eq!(texts(&calls[1].base), texts(&calls[0].base));
    assert!(!texts(&calls[1].request)
        .iter()
        .any(|text| text == "the head goes on"));
    // A fork prepared now is based on the head's newest request.
    let later = rig.prepare(1).await.unwrap();
    rig.forks.answer(answered("three"));
    rig.ask(1, &later, "one", "now?").await.unwrap();
    let newest = rig.forks.calls()[2].clone();
    assert_ne!(newest.base_digest, calls[0].base_digest);
    assert!(texts(&newest.base)
        .iter()
        .any(|text| text == "the head goes on"));
}

#[tokio::test]
async fn a_caller_dropped_from_the_plan_loses_its_forks_for_good() {
    let mut rig = Rig::head("revoked", vec![text_step("a", "head reply")]).await;
    let fork = rig.prepare(1).await.unwrap();
    rig.forks.answer(answered("stored"));
    rig.ask(1, &fork, "one", "question").await.unwrap();
    let held_fork = rig.prepare(1).await.unwrap();
    let (arrival, release) = rig.forks.hold_next().await;
    rig.forks.answer(answered("held"));
    let held = rig
        .send_request(1, ask_params(&held_fork, "held", "held question"))
        .await;
    arrival.await.unwrap();

    let (registry, key) = (Arc::clone(&rig.registry), rig.key.clone());
    let refresh = |generation, callers: Value| {
        let registry = Arc::clone(&registry);
        let key = key.clone();
        async move {
            let outcome = registry
                .route_session_refresh(
                    &key,
                    generation,
                    "next_run".into(),
                    Box::new(manifest(
                        &format!("plan-{}", generation + 1),
                        json!({ "fork_callers": callers }),
                    )),
                    Vec::new(),
                )
                .await
                .unwrap();
            assert!(
                matches!(
                    outcome,
                    broca_module::session_cmd::SessionRefreshOutcome::Accepted { .. }
                ),
                "{outcome:?}"
            );
        }
    };
    refresh(1, json!([])).await;
    assert_eq!(
        code(&rig.ask(1, &fork, "one", "question").await.unwrap_err()),
        "unknown_fork"
    );
    assert_eq!(
        code(&rig.ask(1, &fork, "two", "new").await.unwrap_err()),
        "unknown_fork"
    );
    assert_eq!(code(&rig.end(1, &fork).await.unwrap_err()), "unknown_fork");
    assert_eq!(
        code(&rig.prepare(1).await.unwrap_err()),
        "scope_owner_mismatch"
    );
    // The ask in flight finishes and is stored, but its caller gets no answer.
    release.send(()).unwrap();
    assert_eq!(
        code(&outcome(&rig.reply_to(held).await).unwrap_err()),
        "unknown_fork"
    );
    // Listing the caller again revives none of its forks.
    refresh(2, json!([ASKER])).await;
    assert_eq!(
        code(&rig.ask(1, &fork, "one", "question").await.unwrap_err()),
        "unknown_fork"
    );
    assert_eq!(
        code(
            &rig.ask(1, &held_fork, "held", "held question")
                .await
                .unwrap_err()
        ),
        "unknown_fork"
    );
    let next = rig.prepare(1).await.unwrap();
    assert!(next != fork && next != held_fork);
    assert_eq!(rig.forks.calls().len(), 2);
}

#[tokio::test]
async fn a_release_routing_never_heard_of_stays_owed_and_is_sent_again() {
    let mut rig = Rig::head("unreleased", vec![text_step("a", "head reply")]).await;
    let fork = rig.prepare(1).await.unwrap();
    rig.forks.answer(answered("an answer"));
    rig.forks
        .release_fails
        .store(true, std::sync::atomic::Ordering::SeqCst);
    rig.ask(1, &fork, "first", "first question").await.unwrap();
    assert!(rig.forks.released.lock().unwrap().is_empty());
    let image = scratch("unreleased-image");
    crash_image(&rig.dir, &image);

    let forks = Arc::new(ForkScript::default());
    let mut restarted = Rig::open(image, vec![], Arc::clone(&forks), true).await;
    restarted.bind(1, asker()).await;
    restarted
        .ask(1, &fork, "first", "first question")
        .await
        .unwrap();
    assert_eq!(
        *forks.released.lock().unwrap(),
        [broca_module::executor::fork_admission_id(&fork, "first", 1)],
        "the owed release is sent again when the fork opens"
    );
}

#[tokio::test]
async fn an_ask_cut_by_a_crash_is_stored_as_a_model_error_with_its_usage_and_released() {
    let mut rig = Rig::head("crash", vec![text_step("a", "head reply")]).await;
    let fork = rig.prepare(1).await.unwrap();
    rig.forks.answer(answered("stored answer"));
    let first = rig
        .ask(1, &fork, "stored", "stored question")
        .await
        .unwrap();
    *rig.forks.report.lock().unwrap() = Some(usage(9, 1, 0));
    let (arrival, _release) = rig.forks.hold_next().await;
    let _cut = rig
        .send_request(1, ask_params(&fork, "cut", "cut question"))
        .await;
    arrival.await.unwrap();
    // Wait until the attempt's usage is durable, then take what is on disk as the crash left it.
    let runtime = rig.registry.store_runtime().unwrap();
    let fork_key = broca_module::fork_answer::fork_session(&rig.key, &fork);
    broca_test_support::wait_until("the cut attempt's usage is recorded", || {
        let runtime = Arc::clone(&runtime);
        let fork_key = fork_key.clone();
        async move {
            let view = runtime
                .current_view(&fork_key)
                .await
                .map_err(|error| format!("{error:?}"))?
                .ok_or("no fork session")?;
            let records = view.records_from(0).await.map_err(|e| format!("{e:?}"))?;
            records
                .iter()
                .any(|(_, record)| {
                    matches!(record, broca_session_store::record::Record::Fork(fork)
                        if fork.event == broca_session_store::record::ForkEvent::AskUsage)
                })
                .then_some(())
                .ok_or_else(|| "no usage yet".to_owned())
        }
    })
    .await;
    let image = scratch("crash-image");
    crash_image(&rig.dir, &image);

    let forks = Arc::new(ForkScript::default());
    let mut restarted = Rig::open(image, vec![], Arc::clone(&forks), true).await;
    restarted.bind(1, asker()).await;
    assert_eq!(
        restarted
            .ask(1, &fork, "stored", "stored question")
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        *forks.released.lock().unwrap(),
        [broca_module::executor::fork_admission_id(&fork, "cut", 1)]
    );
    let recovered = restarted
        .ask(1, &fork, "cut", "cut question")
        .await
        .unwrap();
    assert_eq!(
        recovered,
        json!({"answer": "", "finish": "model_error",
            "usage": serde_json::to_value(usage(9, 1, 0)).unwrap()})
    );
    assert_eq!(
        restarted
            .ask(1, &fork, "cut", "cut question")
            .await
            .unwrap(),
        recovered
    );
    forks.answer(answered("after recovery"));
    restarted
        .ask(1, &fork, "after", "after restart")
        .await
        .unwrap();
    let asked = texts(&forks.calls()[0].request);
    assert!(asked.iter().any(|text| text == "stored question"));
    assert!(asked.iter().any(|text| text == "stored answer"));
    assert!(!asked.iter().any(|text| text == "cut question"));
    assert_eq!(forks.calls()[0].attempt, 1);
    assert_eq!(forks.released.lock().unwrap().len(), 1, "released once");
    // The cut attempt's spend reached the usage export on recovery, with the usage it had
    // reported, once.
    let cut = broca_module::executor::fork_admission_id(&fork, "cut", 1);
    let facts: Vec<_> = restarted
        .fork_facts()
        .into_iter()
        .filter(|fact| fact.run_id.0 == cut)
        .collect();
    assert_eq!(facts.len(), 1, "{facts:?}");
    assert_eq!(facts[0].usage.input_tokens, Some(9));
}

/// Moves the daemon's test clock to `now_ms`, as the conformance kit's clock hook does.
#[cfg(feature = "test-clock")]
fn set_clock(dir: &Path, now_ms: u64) {
    let temp = dir.join("test-clock.tmp");
    std::fs::write(&temp, json!({ "now_ms": now_ms }).to_string()).unwrap();
    std::fs::rename(&temp, dir.join("test-clock")).unwrap();
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn a_fork_expires_idle_on_the_daemon_clock_and_an_ask_in_flight_holds_it() {
    const IDLE: u64 = broca_module::fork_answer::FORK_IDLE_MS;
    let mut rig = Rig::head("expiry", vec![text_step("a", "head reply")]).await;
    let mut now = 2_000_000_000_000;
    set_clock(&rig.dir, now);
    let unused = rig.prepare(1).await.unwrap();
    now += IDLE;
    set_clock(&rig.dir, now);
    assert_eq!(
        code(&rig.ask(1, &unused, "late", "q").await.unwrap_err()),
        "unknown_fork"
    );

    let fork = rig.prepare(1).await.unwrap();
    now += IDLE - 1;
    set_clock(&rig.dir, now);
    rig.forks.answer(answered("first"));
    let first = rig.ask(1, &fork, "one", "first question").await.unwrap();
    // The answer restarted the idle time; a replay doesn't.
    now += IDLE - 1;
    set_clock(&rig.dir, now);
    assert_eq!(
        rig.ask(1, &fork, "one", "first question").await.unwrap(),
        first
    );
    now += 1;
    set_clock(&rig.dir, now);
    assert_eq!(
        code(&rig.ask(1, &fork, "two", "expired").await.unwrap_err()),
        "unknown_fork"
    );
    assert_eq!(
        rig.ask(1, &fork, "one", "first question").await.unwrap(),
        first
    );

    let fork = rig.prepare(1).await.unwrap();
    let (arrival, release) = rig.forks.hold_next().await;
    rig.forks.answer(answered("held"));
    let held = rig
        .send_request(1, ask_params(&fork, "held", "held question"))
        .await;
    arrival.await.unwrap();
    now += IDLE + 1;
    set_clock(&rig.dir, now);
    assert_eq!(
        code(&rig.ask(1, &fork, "other", "q").await.unwrap_err()),
        "fork_busy"
    );
    release.send(()).unwrap();
    assert_eq!(
        outcome(&rig.reply_to(held).await).unwrap()["answer"],
        "held"
    );
    now += IDLE - 1;
    set_clock(&rig.dir, now);
    rig.forks.answer(answered("still open"));
    rig.ask(1, &fork, "after", "still alive?").await.unwrap();
}

/// Copies the state root `from` to a fresh `to`, as a crash leaves it: whatever is on disk now.
fn crash_image(from: &Path, to: &Path) {
    let _ = std::fs::remove_dir_all(to);
    for entry in walk(from) {
        let relative = entry.strip_prefix(from).unwrap();
        let target = to.join(relative);
        if entry.is_dir() {
            std::fs::create_dir_all(&target).unwrap();
        } else {
            std::fs::create_dir_all(target.parent().unwrap()).unwrap();
            match std::fs::copy(&entry, &target) {
                Ok(_) => {}
                // Windows refuses to read a file another handle holds an OS lock on. The files
                // broca keeps locked under its state root (`broca.lock` and the decision-model
                // store's owner lock) hold no data, only the lock, and a crash frees the lock,
                // so the image keeps such a file, empty.
                Err(error) if error.raw_os_error() == Some(ERROR_LOCK_VIOLATION) => {
                    std::fs::File::create(&target).unwrap();
                }
                Err(error) => panic!("copy {}: {error}", entry.display()),
            }
        }
    }
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = vec![dir.to_path_buf()];
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(walk(&path));
        } else {
            out.push(path);
        }
    }
    out
}

/// Windows' `ERROR_LOCK_VIOLATION`: part of the file is locked by another handle.
const ERROR_LOCK_VIOLATION: i32 = 33;
