//! Wire conformance for the serve adapter — drives `serve_connection` over an in-memory
//! duplex with a fake-executor registry (NO live daemon, NO live LLM). Proves:
//!  - HELLO registers the manifest (correct module_id + ManagementSurface shape);
//!  - route.bind → RouteBindAck;
//!  - session.send / run.status dispatch to the coordinator and return Response frames;
//!  - session.subscribe opens a StreamData* → StreamEnd stream on the held corr;
//!  - egress-closed (client drops the read half mid-stream) tears the subscriber down
//!    cleanly — the serve loop ends without panic/hang.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_catalog::live::{
    resolve_chatgpt_pause_provider_with_client, resolve_provider, AuthMode, ResolveOptions,
    WireFamilyId,
};
use broca_core::{
    run_loop_on_writer_admitted, AllowAll, CapBaseline, CapInstallRequest, CapMutation,
    CapOperation, CapRow, CapStore, CapStoreError, CapWatermark, ChargeBasis, FileLeaseStore,
    FileWalStore, MaterializedStore, OutputEvent, Provider, RunError, RunIndex, RunSummary,
    SpendDeltaRequest, StartMode, StubProvider, StubToolPlane, WalStore,
};
use broca_module::executor::{RunExecutor, RunJob, RunKind};
use broca_module::serve::{
    CancelResponse, RetractResponse, RunStatusResponse, SendRequest, SendResponse, ServeError,
    ServeHandler, SessionImportRequest, SessionImportResponse, SessionReadRequest,
    SessionReadResponse, StatusResponse, SubscribeFrom,
};
use broca_module::{DefaultOpener, LineageRegistry};
use broca_module_serve::{serve_connection, serve_connection_with_catalog, ServeEnd};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role},
    error::{ErrorClass, ProviderError},
    provider::{NormalizedEvent, ProviderControl},
    request::{ModelHandle, SubmissionRequest},
    BindIdentity, FinishReason, MessageId, RunId, Usage,
};
use broca_wal::{FramedRecord, Wal, WalRecord};
use subc_protocol::session::{
    HealthStatus, ModuleControlRequest, ModuleControlResponse, MODULE_CONTROL_OP_HEALTH_CHECK,
};
use subc_protocol::{
    BindIdentity as SubcBindIdentity, ErrorBody, Flags, Frame, FrameType, ModuleHelloAckBody,
    ModuleHelloBody, Priority, RouteTarget, PROTOCOL_VERSION,
};
use subc_transport::{read_frame, write_frame};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, Notify};

use crate::common;

// Serialize the only integration test that intentionally drops a stale-route frame with
// health probes that compare process-wide drop counters.
static SERVE_FRAME_DROP_COUNTER_TEST_LOCK: tokio::sync::Mutex<()> =
    tokio::sync::Mutex::const_new(());

// ── A fake executor that writes a RunStarted then emits a control unit + finishes, so the
//    subscribe stream has events to forward. ──

struct FakeExec {
    wal_store: Arc<FileWalStore>,
    started: Arc<AtomicUsize>,
    /// Parks the run between its admission ack and its terminal, so a test can observe a run
    /// that is genuinely IN FLIGHT rather than one that has already ended.
    ///
    /// Without this the executor acks and writes RunFinished microseconds later, so any
    /// assertion about in-flight state is a race against the actor processing the terminal --
    /// and one that passes on a fast platform and fails on a slow one. `Notify` rather than a
    /// channel because a permit sent before the wait is STORED: releasing early cannot lose the
    /// wakeup and hang the run.
    hold: Option<Arc<tokio::sync::Notify>>,
}

/// A tiny real-core executor for the send-id wire test: it runs the actual loop with a
/// one-step stub provider so the second same-send-id request hits the core's same-run-id
/// replay path instead of a fake executor's hand-written outcome.
struct CoreExec {
    wal_store: Arc<FileWalStore>,
    store: Option<Arc<broca_store::SqliteStore>>,
    reply_text: String,
}

/// Exercises the public re-auth path with a mutable credential outcome. The first dead
/// credential attempt is held after admission so the test can queue a later turn behind it.
struct ReauthCoreExec {
    wal_store: Arc<FileWalStore>,
    credential_ready: Arc<AtomicBool>,
    attempts: Arc<AtomicUsize>,
    provider_calls: Arc<Mutex<HashMap<String, usize>>>,
    first_pause_release: Arc<Notify>,
    first_attempt_pauses: bool,
}

struct ResolvedStubProvider {
    resolved: Box<dyn Provider>,
    stub: StubProvider,
}

#[async_trait]
impl Provider for ResolvedStubProvider {
    async fn step(
        &self,
        step_id: broca_protocol::StepId,
        opts: &broca_protocol::request::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.stub.step(step_id, opts).await
    }

    fn render_request(&self, opts: &broca_protocol::request::CallOptions) -> Vec<u8> {
        self.resolved.render_request(opts)
    }

    fn renderer_version(&self) -> u32 {
        self.resolved.renderer_version()
    }

    fn render_config_blob(&self) -> serde_json::Value {
        self.resolved.render_config_blob()
    }
}

fn chatgpt_test_options<'a>(account_id: Option<&'a str>) -> ResolveOptions<'a> {
    ResolveOptions {
        claude_code_persona: None,
        auth_method: Some(broca_catalog::live::AuthMethod::Chatgpt),
        family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
        project_id: None,
        session_id: Some("wire-chatgpt-reauth"),
        account_id,
        cache_policy: None,
        prompt_cache_key: None,
        variant: None,
        service_tier: None,
        secret_refresher: None,
        structured_output: None,
        frozen_account_ref: None,
        clock: None,
    }
}

fn chatgpt_stub_provider(credential_ready: bool) -> ResolvedStubProvider {
    common::install_snapshot();
    let options = chatgpt_test_options(credential_ready.then_some("acct-repaired"));
    let resolution = if credential_ready {
        resolve_provider(
            "openai/gpt-5.3-codex",
            None,
            2048,
            &AuthMode::Secret("repaired-token".into()),
            &options,
        )
    } else {
        resolve_chatgpt_pause_provider_with_client(
            "openai/gpt-5.3-codex",
            Some(2048),
            None,
            &options,
            broca_provider::build_provider_http_client(),
        )
    };
    let resolved = resolution.unwrap_or_else(|error| panic!("ChatGPT test provider: {error}"));
    ResolvedStubProvider {
        resolved: resolved.provider,
        stub: StubProvider::new(vec![one_step_text_reply("reauthenticated")]),
    }
}

fn auth_required_error() -> ProviderError {
    ProviderError {
        provider_details: None,
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::Auth,
        message: "credential needs re-authentication".into(),
        status: None,
        retry_after_secs: None,
        provider_code: Some("needs_reauth".into()),
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}

fn one_step_text_reply(text: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(11),
                output_tokens: Some(4),
                ..Usage::default()
            },
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

#[async_trait]
impl RunExecutor for FakeExec {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        if job.req.cache.is_some() {
            let result = broca_module_serve::resolve_cache_policy(
                broca_provider::spec::WireFamilyId::OpenAiChat,
                job.req.model.provider_module_id.as_str(),
                job.req.cache.as_ref(),
                &broca_module_serve::CacheTtlTable::shipped_defaults(),
                &job.req.session,
            );
            if let Err(message) = result {
                let error = RunError::Config(message);
                if let Some(ack) = job.on_admitted {
                    let _ = ack.send(Err(error.clone()));
                }
                return Err(error);
            }
        }
        self.started.fetch_add(1, Ordering::SeqCst);
        job.writer.record(broca_wal::WalRecord::RunStarted {
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
            run_id: job.req.run_id.clone(),
            session: job.req.session.clone(),
            ts_ms: None,
            config: Box::new(min_config()),
            input: job.req.input.clone().into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: job.req.submission_id.clone(),
        });
        let _ = job.writer.sync().await;
        // Admit (RunStarted durable) so the actor's start_run unblocks + replies Active.
        if let Some(ack) = job.on_admitted {
            let _ = ack.send(Ok(job.req.run_id.clone()));
        }
        // Park here when a test needs the run observably in flight. The ack above is what
        // unblocks the caller's `session.send`, so everything between it and the terminal below
        // is the only window in which runs_in_flight reads non-zero.
        if let Some(hold) = &self.hold {
            hold.notified().await;
        }
        job.writer.record(WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        });
        let _ = job.writer.sync().await;
        let _ = &self.wal_store;
        let _ = out;
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

#[async_trait]
impl RunExecutor for CoreExec {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let mode = match job.kind {
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
            RunKind::FreshOrResume => StartMode::FreshOrResume,
        };
        let provider = StubProvider::new(vec![one_step_text_reply(&self.reply_text)]);
        let plane = StubToolPlane::new();
        let materialized_store = self.store.as_ref().and_then(|store| {
            job.materialized_store(store.clone() as Arc<dyn broca_core::SessionStore>)
        });
        run_loop_on_writer_admitted(
            job.req,
            &job.writer,
            mode,
            &provider,
            &plane,
            &AllowAll,
            self.wal_store.as_ref(),
            materialized_store,
            job.control,
            &out,
            job.on_admitted,
        )
        .await
    }
}

#[async_trait]
impl RunExecutor for ReauthCoreExec {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        let credential_ready = self.credential_ready.load(Ordering::SeqCst);
        let provider = chatgpt_stub_provider(credential_ready);

        // Hold the first needs_reauth attempt after its admission acknowledgement. This mirrors
        // core's admit-then-pause records while keeping the actor Active long enough to enqueue a
        // deterministic later turn.
        if attempt == 0 && !credential_ready && self.first_attempt_pauses {
            let mut config = min_config();
            config.model = job.req.model.clone();
            config.tools = job.req.tools.clone().into();
            config.tool_choice = job.req.tool_choice.clone();
            config.generation = job.req.generation.clone();
            config.renderer_version = provider.renderer_version();
            config.frozen_render_config = provider.render_config_blob();
            config.auth_selection = Some(broca_core::AuthSelection {
                credential_id: "chatgpt:openai".into(),
                account_ref: None,
                resolved_credential_id: None,
                vault_request: None,
                account_decision: None,
            });
            config.tools_were_omitted = job.control.tools_were_omitted;
            config.budget = job.req.budget.clone();
            config.stop_when = job.req.stop_when.clone();
            job.writer.record(WalRecord::RunStarted {
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
                run_id: job.req.run_id.clone(),
                session: job.req.session.clone(),
                ts_ms: None,
                config: Box::new(config),
                input: job.req.input.clone().into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: job.req.submission_id.clone(),
            });
            job.writer.sync().await?;
            if let Some(ack) = job.on_admitted {
                let _ = ack.send(Ok(job.req.run_id.clone()));
            }
            self.first_pause_release.notified().await;
            job.writer.record(WalRecord::RunPaused {
                run_id: job.req.run_id.clone(),
                ts_ms: None,
                reason: broca_protocol::PauseReason::AuthRequired,
                error: Some(auth_required_error()),
            });
            job.writer.sync().await?;
            return Ok(RunSummary {
                reason: RunFinishReason::Interrupted,
                paused: Some(broca_protocol::PauseReason::AuthRequired),
                steps: 0,
                usage: Usage::default(),
                retries_used: 0,
                final_step_finish_reason: None,
            });
        }

        if attempt == 0 && !credential_ready {
            let mut config = min_config();
            config.model = job.req.model.clone();
            config.renderer_version = provider.renderer_version();
            config.frozen_render_config = provider.render_config_blob();
            config.auth_selection = Some(broca_core::AuthSelection {
                credential_id: "chatgpt:openai".into(),
                account_ref: None,
                resolved_credential_id: None,
                vault_request: None,
                account_decision: None,
            });
            job.writer.record(WalRecord::RunStarted {
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
                run_id: job.req.run_id.clone(),
                session: job.req.session.clone(),
                ts_ms: None,
                config: Box::new(config),
                input: job.req.input.clone().into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: job.req.submission_id.clone(),
            });
            job.writer.sync().await?;
            if let Some(ack) = job.on_admitted {
                let _ = ack.send(Ok(job.req.run_id.clone()));
            }
            self.first_pause_release.notified().await;
            job.writer.record(WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            });
            job.writer.sync().await?;
            return Ok(RunSummary {
                reason: RunFinishReason::Completed,
                paused: None,
                steps: 0,
                usage: Usage::default(),
                retries_used: 0,
                final_step_finish_reason: None,
            });
        }

        let pause = (!credential_ready).then(auth_required_error);
        let mode = broca_module_serve::start_mode_after_auth(job.kind, pause.is_none());
        let run_id = job.req.run_id.0.clone();
        let mut control = job.control;
        control.pause_for_auth_at_start = pause;
        control.auth_selection = Some(broca_core::AuthSelection {
            credential_id: "chatgpt:openai".into(),
            account_ref: None,
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        });
        let plane = StubToolPlane::new();
        let result = run_loop_on_writer_admitted(
            job.req,
            &job.writer,
            mode,
            &provider,
            &plane,
            &AllowAll,
            self.wal_store.as_ref(),
            None,
            control,
            &out,
            job.on_admitted,
        )
        .await;
        let calls = provider.stub.calls().len();
        if calls > 0 {
            *self
                .provider_calls
                .lock()
                .unwrap()
                .entry(run_id)
                .or_default() += calls;
        }
        result
    }
}

fn min_config() -> broca_wal::RunConfig {
    broca_wal::RunConfig {
        turn_directives_v1: None,
        render_tags: false,
        computed_tags: None,
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

/// Rig whose executor PARKS each run between its ack and its terminal until `hold` fires.
///
/// Exists because asserting on in-flight state against the default rig is a race: the executor
/// acks and finishes microseconds later, so a probe observes a run only if it wins that window.
/// It won on macOS and Linux for weeks and lost on Windows, which is the failure mode of every
/// assertion that depends on losing a race being unlikely rather than impossible.
fn registry_with_hold(dir: &Path, hold: Arc<tokio::sync::Notify>) -> Arc<LineageRegistry> {
    registry_and_wal_inner(dir, Some(hold)).0
}

fn registry_and_wal(dir: &Path) -> (Arc<LineageRegistry>, Arc<FileWalStore>) {
    registry_and_wal_inner(dir, None)
}

fn registry_and_wal_inner(
    dir: &Path,
    hold: Option<Arc<tokio::sync::Notify>>,
) -> (Arc<LineageRegistry>, Arc<FileWalStore>) {
    let lease = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
    let exec = Arc::new(FakeExec {
        wal_store: wal.clone(),
        started: Arc::new(AtomicUsize::new(0)),
        hold: hold.clone(),
    });
    let opener = DefaultOpener::new(lease, wal.clone(), exec)
        .with_run_index(store.clone())
        .with_materialized_store(store.clone());
    let wal_reader: Arc<dyn broca_core::WalStore> = wal.clone();
    let store_reader: Arc<dyn broca_core::MaterializedStore> = store;
    (
        Arc::new(LineageRegistry::new_with_reader(
            Arc::new(opener),
            wal_reader,
            Some(store_reader),
        )),
        wal,
    )
}

fn registry(dir: &Path) -> Arc<LineageRegistry> {
    registry_and_wal(dir).0
}

type CapRig = (
    Arc<LineageRegistry>,
    Arc<broca_store::CapsStore>,
    Arc<AtomicUsize>,
    Arc<broca_store::SqliteStore>,
    Arc<FileWalStore>,
);

struct LegacyNegativeCapStore {
    row: CapRow,
}

impl CapStore for LegacyNegativeCapStore {
    fn install_cap(&self, _request: &CapInstallRequest) -> Result<CapMutation, CapStoreError> {
        unreachable!("legacy admission fixture is read-only")
    }

    fn apply_spend_delta(
        &self,
        _request: &SpendDeltaRequest,
    ) -> Result<CapMutation, CapStoreError> {
        unreachable!("legacy admission fixture is read-only")
    }

    fn matching_caps(&self, scope_key: &str, project_root: &Path) -> Vec<CapRow> {
        (self.row.active
            && self.row.scope_key == scope_key
            && self
                .row
                .project_root
                .as_deref()
                .is_none_or(|root| root == project_root.to_string_lossy()))
        .then(|| self.row.clone())
        .into_iter()
        .collect()
    }
}

fn registry_with_caps(dir: &Path) -> CapRig {
    let lease = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
    let caps = Arc::new(broca_store::CapsStore::open(dir.join("caps.db")).unwrap());
    let started = Arc::new(AtomicUsize::new(0));
    let exec = Arc::new(FakeExec {
        wal_store: wal.clone(),
        started: started.clone(),
        hold: None,
    });
    let resolver = Arc::new(|_: &str, _: &str| (ChargeBasis::Unknown, 1));
    let opener = DefaultOpener::new(lease, wal.clone(), exec)
        .with_run_index(store.clone())
        .with_materialized_store(store.clone())
        .with_export_index(store.clone())
        .with_charge_basis_resolver(resolver)
        .with_cap_store(caps.clone());
    let wal_reader: Arc<dyn broca_core::WalStore> = wal.clone();
    let store_reader: Arc<dyn broca_core::MaterializedStore> = store.clone();
    let registry = Arc::new(LineageRegistry::new_with_reader_and_caps(
        Arc::new(opener),
        Some(wal_reader),
        Some(store_reader),
        Some(caps.clone()),
    ));
    (registry, caps, started, store, wal)
}

fn registry_with_export(dir: &Path) -> (Arc<LineageRegistry>, Arc<broca_store::SqliteStore>) {
    let lease = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
    let exec = Arc::new(FakeExec {
        wal_store: wal.clone(),
        started: Arc::new(AtomicUsize::new(0)),
        hold: None,
    });
    let resolver = Arc::new(|_: &str, _: &str| (ChargeBasis::Unknown, 1));
    let opener = DefaultOpener::new(lease, wal.clone(), exec)
        .with_run_index(store.clone())
        .with_materialized_store(store.clone())
        .with_export_index(store.clone())
        .with_charge_basis_resolver(resolver);
    let wal_reader: Arc<dyn broca_core::WalStore> = wal;
    let store_reader: Arc<dyn broca_core::MaterializedStore> = store.clone();
    (
        Arc::new(LineageRegistry::new_with_reader(
            Arc::new(opener),
            wal_reader,
            Some(store_reader),
        )),
        store,
    )
}

/// True when the session holds no committed records: its WAL is absent (the usual result of a
/// refusal, which writes nothing) or present and empty. File stores report absence as
/// `Io(NotFound)`, so `read_records(..).unwrap()` would panic on exactly the case these tests
/// are proving. Any other read error still fails the test.
fn no_durable_records(wal: &dyn broca_core::WalStore, session: &BindIdentity) -> bool {
    match wal.read_records(session) {
        Ok(records) => records.is_empty(),
        Err(broca_wal::WalError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(error) => panic!("reading the session WAL failed: {error}"),
    }
}

#[tokio::test]
async fn over_cap_admission_is_typed_and_leaves_no_wal_or_run_index_row() {
    use broca_module::serve::ServeHandler;

    let dir = tmpdir("budget-denied");
    let (registry, caps, started, store, wal) = registry_with_caps(&dir);
    let session = BindIdentity::new(dir.join("proj"), "runner", "budget");
    registry
        .cap_install(CapInstallRequest {
            cap_id: "cap-org-x".into(),
            cap_version: 1,
            op: CapOperation::Install,
            scope_key: "runner".into(),
            project_root: Some(session.project_root.display().to_string()),
            constraint_dim: "spend_dollars".into(),
            period: "calendar_month".into(),
            period_tz: "Europe/Madrid".into(),
            hard_cap_nanos: 10,
            max_staleness_ms: 60_000,
            replaces_cap_id: None,
            baseline: Some(CapBaseline {
                spend_nanos: 10,
                unresolved_count: 0,
                watermark: CapWatermark {
                    source_instance_id: "source-a".into(),
                    source_epoch: 1,
                    consumed_export_seq: 0,
                },
                period_bucket: Some("2026-07".into()),
                as_of_ms: i64::MAX,
            }),
        })
        .await
        .expect("cap install is acknowledged after commit");

    let request = SubmissionRequest {
        directives: None,
        on_restart: None,
        keep_warm: None,
        scope: Default::default(),
        project_id: None,
        principal: None,
        model: ModelHandle::new("fake", "fake"),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "hello".into(),
            }],
        )],
        tools: Some(vec![]),
        policy_allowed: Default::default(),
        tool_choice: Default::default(),
        generation: Default::default(),
        cache: None,
        budget: None,
        auth: None,
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: vec![],
        context_management: None,
        inherit_frozen: Default::default(),
        structured_output: None,
    };
    let error = registry
        .send(broca_module::serve::SendRequest {
            session_class: None,
            retention: None,
            stop_sequences_named: false,
            title: None,
            conversation_key: None,
            delivery: Default::default(),
            session: session.clone(),
            run_id: broca_protocol::RunId("run-denied".into()),
            send_id: None,
            request: Box::new(request),
            skip_materialization: false,
            kind: RunKind::FreshOrResume,
            plan: None,
            order: None,
        })
        .await
        .expect_err("hard cap should deny before execution");
    assert!(matches!(
        error,
        ServeError::BudgetDenied { cap_id, reason }
            if cap_id == "cap-org-x" && reason == "hard_cap_exceeded"
    ));
    assert_eq!(started.load(Ordering::SeqCst), 0);
    assert!(no_durable_records(wal.as_ref(), &session));
    assert!(store.lookup_run("run-denied").unwrap().is_none());
    assert!(caps.row("cap-org-x").unwrap().active);
}

fn unknown_charge_basis(_: &str, _: &str) -> (ChargeBasis, u32) {
    (ChargeBasis::Unknown, 1)
}

fn under_cap_request(
    session: &BindIdentity,
    source_instance_id: String,
    source_epoch: u64,
    consumed_export_seq: u64,
) -> CapInstallRequest {
    CapInstallRequest {
        cap_id: "cap-under-limit".into(),
        cap_version: 1,
        op: CapOperation::Install,
        scope_key: session.harness.clone(),
        project_root: Some(session.project_root.display().to_string()),
        constraint_dim: "spend_dollars".into(),
        period: "calendar_month".into(),
        period_tz: "Europe/Madrid".into(),
        hard_cap_nanos: 100,
        max_staleness_ms: 60_000,
        replaces_cap_id: None,
        baseline: Some(CapBaseline {
            spend_nanos: 0,
            unresolved_count: 0,
            watermark: CapWatermark {
                source_instance_id,
                source_epoch,
                consumed_export_seq,
            },
            period_bucket: Some("2026-07".into()),
            as_of_ms: i64::MAX,
        }),
    }
}

fn chatgpt_send(session: BindIdentity, run_id: &str, prompt: &str) -> SendRequest {
    SendRequest {
        session_class: None,
        retention: None,
        stop_sequences_named: false,
        title: None,
        conversation_key: None,
        delivery: Default::default(),
        session,
        run_id: RunId(run_id.into()),
        send_id: Some(format!("send-{run_id}")),
        request: Box::new(SubmissionRequest {
            directives: None,
            on_restart: None,
            keep_warm: None,
            scope: Default::default(),
            project_id: None,
            principal: None,
            model: ModelHandle::new("openai", "gpt-5.3-codex"),
            input: vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: prompt.into(),
                }],
            )],
            tools: Some(vec![]),
            policy_allowed: Default::default(),
            tool_choice: Default::default(),
            generation: Default::default(),
            cache: None,
            budget: None,
            auth: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: vec![],
            context_management: None,
            inherit_frozen: Default::default(),
            structured_output: None,
        }),
        skip_materialization: false,
        kind: RunKind::FreshOrResume,
        plan: None,
        order: None,
    }
}

fn budget_send(session: BindIdentity, run_id: &str) -> SendRequest {
    SendRequest {
        session_class: None,
        retention: None,
        stop_sequences_named: false,
        title: None,
        conversation_key: None,
        delivery: Default::default(),
        session,
        run_id: broca_protocol::RunId(run_id.into()),
        send_id: None,
        request: Box::new(SubmissionRequest {
            directives: None,
            on_restart: None,
            keep_warm: None,
            scope: Default::default(),
            project_id: None,
            principal: None,
            model: ModelHandle::new("fake", "fake"),
            input: vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "hello".into(),
                }],
            )],
            tools: Some(vec![]),
            policy_allowed: Default::default(),
            tool_choice: Default::default(),
            generation: Default::default(),
            cache: None,
            budget: None,
            auth: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: vec![],
            context_management: None,
            inherit_frozen: Default::default(),
            structured_output: None,
        }),
        skip_materialization: false,
        kind: RunKind::FreshOrResume,
        plan: None,
        order: None,
    }
}

async fn assert_budget_watermark_case(
    name: &str,
    foreign_source: bool,
    epoch_offset: u64,
    consumed_export_seq: u64,
    expected_denial: Option<&str>,
) {
    let dir = tmpdir(name);
    let (registry, _caps, started, store, _wal) = registry_with_caps(&dir);
    let session = BindIdentity::new(dir.join("proj"), "runner", name);
    let source_instance_id = if foreign_source {
        "foreign-export-source".into()
    } else {
        store.source_instance_id().to_string()
    };
    registry
        .cap_install(under_cap_request(
            &session,
            source_instance_id,
            store.source_epoch() + epoch_offset,
            consumed_export_seq,
        ))
        .await
        .unwrap();

    let result = registry
        .send(budget_send(session, &format!("run-{name}")))
        .await;
    match expected_denial {
        Some(expected_reason) => {
            assert!(matches!(
                result,
                Err(ServeError::BudgetDenied { cap_id, reason })
                    if cap_id == "cap-under-limit" && reason == expected_reason
            ));
            assert_eq!(started.load(Ordering::SeqCst), 0);
        }
        None => {
            assert!(matches!(result, Ok(SendResponse::Active { .. })));
            assert_eq!(started.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn budget_admission_validates_the_complete_export_position() {
    assert_budget_watermark_case(
        "budget-foreign-source",
        true,
        0,
        0,
        Some("export_source_mismatch"),
    )
    .await;
    assert_budget_watermark_case(
        "budget-foreign-epoch",
        false,
        1,
        0,
        Some("export_source_mismatch"),
    )
    .await;
    assert_budget_watermark_case(
        "budget-future-sequence",
        false,
        0,
        1,
        Some("export_source_mismatch"),
    )
    .await;
    assert_budget_watermark_case("budget-matching-position", false, 0, 0, None).await;
}

#[tokio::test]
async fn unresolved_decrement_to_zero_allows_admission_at_the_export_head() {
    let dir = tmpdir("budget-unresolved-zero");
    let (registry, caps, started, store, _wal) = registry_with_caps(&dir);
    let session = BindIdentity::new(dir.join("proj"), "runner", "resolved");
    let exported_run = RunId("run-export-head".into());
    let records = vec![
        FramedRecord {
            seq: 1,
            fence: 1,
            record: WalRecord::RunStarted {
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
                run_id: exported_run.clone(),
                session: session.clone(),
                ts_ms: Some(100),
                config: Box::new(min_config()),
                input: vec![].into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
        },
        FramedRecord {
            seq: 2,
            fence: 1,
            record: WalRecord::RunFinished {
                interrupt_reason: None,
                reason: RunFinishReason::Completed,
                ts_ms: Some(200),
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        },
    ];
    store
        .emit_export_run(&records, &exported_run, &unknown_charge_basis)
        .unwrap();
    assert_eq!(store.high_water(), 1);

    let mut cap = under_cap_request(
        &session,
        store.source_instance_id().to_string(),
        store.source_epoch(),
        0,
    );
    cap.baseline.as_mut().unwrap().unresolved_count = 1;
    registry.cap_install(cap).await.unwrap();
    assert!(matches!(
        registry
            .apply_spend_delta(SpendDeltaRequest {
                cap_id: "cap-under-limit".into(),
                cap_version: 1,
                from_watermark: CapWatermark {
                    source_instance_id: store.source_instance_id().to_string(),
                    source_epoch: store.source_epoch(),
                    consumed_export_seq: 0,
                },
                to_watermark: CapWatermark {
                    source_instance_id: store.source_instance_id().to_string(),
                    source_epoch: store.source_epoch(),
                    consumed_export_seq: 1,
                },
                priced_delta_nanos: 0,
                unresolved_delta: -1,
                period_bucket: "2026-07".into(),
            })
            .await
            .unwrap(),
        CapMutation::Applied(_)
    ));
    assert_eq!(caps.row("cap-under-limit").unwrap().unresolved_count, 0);

    let result = registry
        .send(budget_send(session, "run-after-unresolved-zero"))
        .await;
    assert!(matches!(result, Ok(SendResponse::Active { .. })));
    assert_eq!(started.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn legacy_negative_unresolved_row_denies_admission_after_upgrade() {
    let dir = tmpdir("budget-legacy-negative");
    let lease = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
    let started = Arc::new(AtomicUsize::new(0));
    let exec = Arc::new(FakeExec {
        wal_store: wal.clone(),
        started: started.clone(),
        hold: None,
    });
    let session = BindIdentity::new(dir.join("proj"), "runner", "legacy-negative");
    let cap_store: Arc<dyn CapStore> = Arc::new(LegacyNegativeCapStore {
        row: CapRow {
            cap_id: "cap-legacy-negative".into(),
            cap_version: 1,
            scope_key: session.harness.clone(),
            project_root: Some(session.project_root.display().to_string()),
            constraint_dim: "spend_dollars".into(),
            period: "calendar_month".into(),
            period_tz: "Europe/Madrid".into(),
            hard_cap_nanos: 100,
            max_staleness_ms: 60_000,
            replaces_cap_id: None,
            period_bucket: Some("2026-07".into()),
            spend_nanos: 0,
            unresolved_count: -1,
            applied_watermark: CapWatermark {
                source_instance_id: store.source_instance_id().to_string(),
                source_epoch: store.source_epoch(),
                consumed_export_seq: store.high_water(),
            },
            as_of_ms: i64::MAX,
            active: true,
            replaced_by: None,
        },
    });
    let resolver = Arc::new(|_: &str, _: &str| (ChargeBasis::Unknown, 1));
    let opener = DefaultOpener::new(lease, wal.clone(), exec)
        .with_run_index(store.clone())
        .with_materialized_store(store.clone())
        .with_export_index(store.clone())
        .with_charge_basis_resolver(resolver)
        .with_cap_store(cap_store.clone());
    let wal_reader: Arc<dyn WalStore> = wal;
    let store_reader: Arc<dyn MaterializedStore> = store;
    let registry = LineageRegistry::new_with_reader_and_caps(
        Arc::new(opener),
        Some(wal_reader),
        Some(store_reader),
        Some(cap_store),
    );

    let error = registry
        .send(budget_send(session, "run-legacy-negative"))
        .await
        .expect_err("a persisted negative unresolved count must fail closed");
    assert!(matches!(
        error,
        ServeError::BudgetDenied { cap_id, reason }
            if cap_id == "cap-legacy-negative" && reason == "unresolved_spend"
    ));
    assert_eq!(started.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn conflicting_late_cap_replacement_does_not_create_an_admission_blocker() {
    let dir = tmpdir("budget-conflicting-replacement");
    let (registry, caps, started, store, _wal) = registry_with_caps(&dir);
    let session = BindIdentity::new(dir.join("proj"), "runner", "replacement");
    let source_instance_id = store.source_instance_id().to_string();
    let source_epoch = store.source_epoch();
    let high_water = store.high_water();

    let mut cap_a = under_cap_request(
        &session,
        source_instance_id.clone(),
        source_epoch,
        high_water,
    );
    cap_a.cap_id = "cap-a".into();
    registry.cap_install(cap_a).await.unwrap();

    let mut cap_c = under_cap_request(
        &session,
        source_instance_id.clone(),
        source_epoch,
        high_water,
    );
    cap_c.cap_id = "cap-c".into();
    cap_c.replaces_cap_id = Some("cap-a".into());
    registry.cap_install(cap_c).await.unwrap();

    let mut delayed_cap_b =
        under_cap_request(&session, source_instance_id, source_epoch, high_water);
    delayed_cap_b.cap_id = "cap-b".into();
    delayed_cap_b.replaces_cap_id = Some("cap-a".into());
    delayed_cap_b.baseline.as_mut().unwrap().spend_nanos = delayed_cap_b.hard_cap_nanos;
    assert!(matches!(
        registry.cap_install(delayed_cap_b).await,
        Err(ServeError::BaselineRequired)
    ));

    let retired = caps.row("cap-a").unwrap();
    assert_eq!(retired.replaced_by.as_deref(), Some("cap-c"));
    assert!(caps.row("cap-c").unwrap().active);
    assert!(caps.row("cap-b").is_none());

    let result = registry
        .send(budget_send(session, "run-conflicting-replacement"))
        .await;
    assert!(matches!(result, Ok(SendResponse::Active { .. })));
    assert_eq!(started.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn replacing_the_export_database_keeps_surviving_caps_fail_closed() {
    let dir = tmpdir("budget-export-replacement");
    let old_source_instance_id = {
        let (registry, caps, _started, store, wal) = registry_with_caps(&dir);
        let session = BindIdentity::new(dir.join("proj"), "runner", "replacement");
        registry
            .cap_install(under_cap_request(
                &session,
                store.source_instance_id().to_string(),
                store.source_epoch(),
                0,
            ))
            .await
            .unwrap();
        let source_instance_id = store.source_instance_id().to_string();
        drop(registry);
        drop(caps);
        drop(store);
        drop(wal);
        source_instance_id
    };
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(dir.join(format!("store.db{suffix}")));
    }

    let (registry, _caps, started, replacement, _wal) = registry_with_caps(&dir);
    assert_ne!(replacement.source_instance_id(), old_source_instance_id);
    let session = BindIdentity::new(dir.join("proj"), "runner", "replacement");
    let error = registry
        .send(budget_send(session, "run-after-export-replacement"))
        .await
        .expect_err("a cap anchored to the replaced export source must deny admission");
    assert!(matches!(
        error,
        ServeError::BudgetDenied { cap_id, reason }
            if cap_id == "cap-under-limit" && reason == "export_source_mismatch"
    ));
    assert_eq!(started.load(Ordering::SeqCst), 0);
}

struct BlockingSendHandler {
    slow_send_started: Notify,
    release_slow_send: Notify,
    ordered_calls: AtomicUsize,
    first_ordered_send_started: Notify,
    second_ordered_send_started: Notify,
    release_first_ordered_send: Notify,
}

impl BlockingSendHandler {
    fn new() -> Self {
        Self {
            slow_send_started: Notify::new(),
            release_slow_send: Notify::new(),
            ordered_calls: AtomicUsize::new(0),
            first_ordered_send_started: Notify::new(),
            second_ordered_send_started: Notify::new(),
            release_first_ordered_send: Notify::new(),
        }
    }
}

#[async_trait]
impl ServeHandler for BlockingSendHandler {
    async fn send(&self, req: SendRequest) -> Result<SendResponse, ServeError> {
        if req.session.session == "slow" {
            self.slow_send_started.notify_one();
            self.release_slow_send.notified().await;
        } else if req.session.session == "ordered" {
            match self.ordered_calls.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    self.first_ordered_send_started.notify_one();
                    self.release_first_ordered_send.notified().await;
                }
                1 => self.second_ordered_send_started.notify_one(),
                _ => {}
            }
        }
        Ok(SendResponse::Active {
            run_id: req.run_id.0.clone(),
            delivered: None,
        })
    }

    async fn import(
        &self,
        _req: SessionImportRequest,
    ) -> Result<SessionImportResponse, ServeError> {
        Err(ServeError::Import("test handler does not import".into()))
    }

    async fn retract(
        &self,
        _session: &BindIdentity,
        _submission_id: broca_protocol::SubmissionId,
    ) -> Result<RetractResponse, ServeError> {
        Ok(RetractResponse::NotPending)
    }

    async fn subscribe(
        &self,
        _session: &BindIdentity,
        _from: SubscribeFrom,
        _sink: mpsc::Sender<broca_module::Delivery>,
    ) -> Result<(), ServeError> {
        Err(ServeError::Open(
            "test handler does not subscribe".into(),
            None,
        ))
    }

    async fn read(&self, _req: SessionReadRequest) -> Result<SessionReadResponse, ServeError> {
        Err(ServeError::Open("test handler does not read".into(), None))
    }

    async fn cancel(
        &self,
        _session: &BindIdentity,
        _run_id: broca_protocol::RunId,
    ) -> Result<CancelResponse, ServeError> {
        Ok(CancelResponse::NotActive)
    }

    async fn status(&self, _session: &BindIdentity) -> Result<StatusResponse, ServeError> {
        Ok(StatusResponse::Idle)
    }

    async fn run_status(
        &self,
        _session: &BindIdentity,
        _run_id: broca_protocol::RunId,
    ) -> Result<RunStatusResponse, ServeError> {
        Ok(RunStatusResponse::Unknown)
    }
}

// ── A tiny client over the duplex: write request frames, read response frames. ──

async fn write_req<W: AsyncWrite + Unpin>(
    w: &mut W,
    channel: u16,
    corr: u64,
    body: serde_json::Value,
) {
    write_req_epoch(w, channel, u32::from(channel != 0), corr, body).await;
}

async fn write_req_epoch<W: AsyncWrite + Unpin>(
    w: &mut W,
    channel: u16,
    epoch: u32,
    corr: u64,
    body: serde_json::Value,
) {
    let frame = Frame::build(
        FrameType::Request,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        serde_json::to_vec(&body).unwrap(),
    )
    .unwrap();
    write_frame(w, &frame).await.unwrap();
}

async fn read_one<R: AsyncRead + Unpin>(r: &mut R) -> Frame {
    read_frame(r).await.unwrap().expect("a frame")
}

/// A fresh scratch directory for one fixture, never shared with another call.
///
/// The name carries a per-call sequence number, not just the tag, because a helper that
/// several tests use passes the same tag for all of them. With a tag-only name, those tests
/// running in parallel got one directory: each setup deleted it and rebuilt it under the
/// others, so a test failed opening its store, appending to its WAL, or read another test's
/// session markers. The scratch root is already private to this process, so a unique name
/// needs no clean-up of a previous directory before use.
fn tmpdir(tag: &str) -> std::path::PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let call = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-serve-{}-{tag}-{call}", std::process::id()));
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    dir
}

/// Assert the EXACT body a cap mutation returns, member by member.
///
/// A consumer specification is pinned to this shape: ASTRO's validator treats any success
/// body that is not exactly this as malformed, and a malformed success authorizes no
/// outbox retirement -- so an added member does not degrade their delivery, it wedges
/// every cap at the first install. Asserting one field's VALUE (as the assertions above
/// do) cannot see a new member appear, which is precisely the change that would break
/// them silently on my side and loudly on theirs.
///
/// Three properties they depend on, all checked here: the envelope nests the watermark
/// under `result` rather than carrying it at top level, the result is closed at exactly
/// one member, and the watermark is closed at exactly three.
fn assert_cap_mutation_shape(body: &serde_json::Value) {
    let top = body.as_object().expect("response body is an object");
    assert_eq!(
        top.keys().collect::<Vec<_>>(),
        vec!["result"],
        "the response envelope carries exactly one member, `result`"
    );
    let result = top["result"].as_object().expect("result is an object");
    assert_eq!(
        result.keys().collect::<Vec<_>>(),
        vec!["applied_watermark"],
        "a cap mutation result is closed at one member"
    );
    let watermark = result["applied_watermark"]
        .as_object()
        .expect("applied_watermark is an object");
    let mut members: Vec<&String> = watermark.keys().collect();
    members.sort();
    assert_eq!(
        members,
        vec!["seq", "source_epoch", "source_instance_id"],
        "the applied watermark is closed at three members"
    );
    assert!(
        watermark["source_instance_id"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "source_instance_id is a non-empty string"
    );
    assert!(watermark["source_epoch"].is_u64(), "source_epoch is a u64");
    assert!(watermark["seq"].is_u64(), "seq is a u64");
}
#[tokio::test]
async fn cap_mutations_use_total_success_and_baseline_error_wire_contracts() {
    let dir = tmpdir("cap-wire");
    let (registry, _caps, _started, _store, _wal) = registry_with_caps(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let hello_ack = Frame::build(
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
    write_frame(&mut cwriter, &hello_ack).await.unwrap();
    let project_root = dir.join("proj");
    write_req(
        &mut cwriter,
        0,
        10,
        serde_json::to_value(ModuleControlRequest::RouteBind {
            route_channel: 7,
            epoch: 2,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new(project_root.clone(), "runner", "cap-wire"),
            principal: None,
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        })
        .unwrap(),
    )
    .await;
    assert_eq!(read_one(&mut creader).await.header.ty, FrameType::Response);

    write_req_epoch(
        &mut cwriter,
        7,
        2,
        11,
        serde_json::json!({
            "method": "cap.install",
            "params": {
                "cap_id": "cap-wire",
                "cap_version": 4,
                "op": "install",
                "scope_key": "runner",
                "project_root": project_root.display().to_string(),
                "constraint_dim": "spend_dollars",
                "period": "calendar_month",
                "period_tz": "Europe/Madrid",
                "hard_cap_nanos": 50,
                "max_staleness_ms": 60000,
                "replaces_cap_id": null,
                "baseline": {
                    "spend_nanos": 1,
                    "unresolved_count": 0,
                    "watermark": {
                        "source_instance_id": "source-a",
                        "source_epoch": 1,
                        "consumed_export_seq": 40
                    },
                    "period_bucket": "2026-07",
                    "as_of_ms": 9223372036854775807i64
                }
            }
        }),
    )
    .await;
    let installed = read_one(&mut creader).await;
    assert_eq!(installed.header.ty, FrameType::Response);
    let installed: serde_json::Value = serde_json::from_slice(&installed.body).unwrap();
    assert_eq!(installed["result"]["applied_watermark"]["seq"], 40);
    assert_cap_mutation_shape(&installed);

    write_req_epoch(
        &mut cwriter,
        7,
        2,
        12,
        serde_json::json!({
            "method": "spend.delta",
            "params": {
                "cap_id": "cap-wire",
                "cap_version": 4,
                "from_watermark": {"source_instance_id": "source-a", "source_epoch": 1, "consumed_export_seq": 40},
                "to_watermark": {"source_instance_id": "source-a", "source_epoch": 1, "consumed_export_seq": 41},
                "priced_delta_nanos": 0,
                "unresolved_delta": 0,
                "period_bucket": "2026-07"
            }
        }),
    )
    .await;
    let applied = read_one(&mut creader).await;
    assert_eq!(applied.header.ty, FrameType::Response);
    let applied: serde_json::Value = serde_json::from_slice(&applied.body).unwrap();
    assert_eq!(applied["result"]["applied_watermark"]["seq"], 41);
    assert_cap_mutation_shape(&applied);

    write_req_epoch(
        &mut cwriter,
        7,
        2,
        13,
        serde_json::json!({
            "method": "spend.delta",
            "params": {
                "cap_id": "cap-wire",
                "cap_version": 4,
                "from_watermark": {"source_instance_id": "other-source", "source_epoch": 9, "consumed_export_seq": 0},
                "to_watermark": {"source_instance_id": "other-source", "source_epoch": 9, "consumed_export_seq": 1},
                "priced_delta_nanos": 1,
                "unresolved_delta": 0,
                "period_bucket": "2026-07"
            }
        }),
    )
    .await;
    let rejected = read_one(&mut creader).await;
    assert_eq!(rejected.header.ty, FrameType::Error);
    let rejected: ErrorBody = serde_json::from_slice(&rejected.body).unwrap();
    assert_eq!(rejected.code, "baseline_required");

    serve.abort();
    let _ = serve.await;
}

/// Answers `vault.audit` with a fixed result and counts the calls.
struct FixedAuditor {
    calls: std::sync::atomic::AtomicUsize,
}

impl broca_module_serve::vault_audit::VaultAuditor for FixedAuditor {
    fn audit(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = broca_module_serve::wire::VaultAuditResponse>
                + Send
                + '_,
        >,
    > {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async {
            broca_module_serve::wire::VaultAuditResponse::Checked {
                principal: "reserved:broca".into(),
                view: "digest".into(),
                rows: Vec::new(),
                missing: vec![broca_module_serve::wire::VaultAuditMissing {
                    credential_id: "apikey:kimi-for-coding".into(),
                    referenced_by: vec!["apikey:kimi-code-plan-global".into()],
                }],
            }
        })
    }
}

/// `vault.audit` is served by the daemon's auditor, is listed in the manifest as a query, and
/// opens no session.
#[tokio::test]
async fn vault_audit_is_answered_by_the_daemon_auditor_without_opening_a_session() {
    let dir = tmpdir("vault-audit");
    let (reg, _store) = registry_with_export(&dir);
    let auditor = Arc::new(FixedAuditor {
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve_reg = reg.clone();
    let serve_auditor: Arc<dyn broca_module_serve::vault_audit::VaultAuditor> = auditor.clone();
    let serve = tokio::spawn(async move {
        let _ = broca_module_serve::serve_connection_configured(
            sreader,
            swriter,
            "broca",
            serve_reg.clone(),
            serve_reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
            None,
            broca_module_serve::wal_archive::WalArchiveConfig::default(),
            broca_module_serve::consumer_routes::ConsumerRoutes::default(),
            Some(serve_auditor),
            None,
            None,
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let hello_text = String::from_utf8_lossy(&hello.body).to_string();
    assert!(
        hello_text.contains("\"vault.audit\""),
        "the HELLO manifest declares vault.audit"
    );
    let hello_ack = Frame::build(
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
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 2,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(dir.join("proj"), "runner", "audit"),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(bind).unwrap()).await;
    assert_eq!(read_one(&mut creader).await.header.ty, FrameType::Response);

    write_req_epoch(
        &mut cwriter,
        7,
        2,
        11,
        serde_json::json!({"method": "vault.audit"}),
    )
    .await;
    let audit = read_one(&mut creader).await;
    assert_eq!(audit.header.ty, FrameType::Response);
    let body: serde_json::Value = serde_json::from_slice(&audit.body).unwrap();
    assert_eq!(body["result"]["status"], "checked");
    assert_eq!(
        body["result"]["missing"][0]["credential_id"],
        "apikey:kimi-for-coding"
    );
    assert_eq!(auditor.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(reg.slot_counts().open, 0, "vault.audit opens no session");

    // An option the op does not have is refused, not ignored.
    write_req_epoch(
        &mut cwriter,
        7,
        2,
        12,
        serde_json::json!({"method": "vault.audit", "params": {"verbose": true}}),
    )
    .await;
    let refused = read_one(&mut creader).await;
    assert_eq!(refused.header.ty, FrameType::Error);
    assert_eq!(auditor.calls.load(std::sync::atomic::Ordering::SeqCst), 1);

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn retired_usage_export_is_unknown_and_terminal_facts_remain() {
    let dir = tmpdir("usage-export");
    let (reg, store) = registry_with_export(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve_reg = reg.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_reg.clone(),
            serve_reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let hello_ack = Frame::build(
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
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    let session = BindIdentity::new(dir.join("proj"), "runner", "export");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 2,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            session.project_root.clone(),
            session.harness.clone(),
            session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(bind).unwrap()).await;
    assert_eq!(read_one(&mut creader).await.header.ty, FrameType::Response);

    // The retired method follows the ordinary unknown-method refusal.
    write_req_epoch(
        &mut cwriter,
        7,
        2,
        11,
        serde_json::json!({"method": "usage.export", "params": {"limit": 10}}),
    )
    .await;
    let refused = read_one(&mut creader).await;
    assert_eq!(refused.header.ty, FrameType::Error);
    let body: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
    assert_eq!(body["code"], "unknown_method");
    assert_eq!(body["message"], "unknown method 'usage.export'");

    // A broca without the vault still answers vault.audit, and says it could not check,
    // rather than failing the request or answering an empty (clean-looking) audit.
    write_req_epoch(
        &mut cwriter,
        7,
        2,
        15,
        serde_json::json!({"method": "vault.audit", "params": {}}),
    )
    .await;
    let audit = read_one(&mut creader).await;
    assert_eq!(audit.header.ty, FrameType::Response);
    let audit_body: serde_json::Value = serde_json::from_slice(&audit.body).unwrap();
    assert_eq!(audit_body["result"]["status"], "unanswered");
    assert_eq!(reg.slot_counts().open, 0, "vault.audit is daemon-scoped");

    write_req_epoch(
        &mut cwriter,
        7,
        2,
        12,
        serde_json::json!({
            "method": "session.send",
            "params": {"prompt": "hi", "model": {"provider": "fake", "model": "fake"}}
        }),
    )
    .await;
    let send = read_one(&mut creader).await;
    assert_eq!(send.header.ty, FrameType::Response);

    let mut page = None;
    broca_test_support::wait_for(
        "retired_usage_export_is_unknown_and_terminal_facts_remain",
        async {
            loop {
                let candidate = store.read_export_page(None, 10).unwrap();
                if !candidate.facts.is_empty() {
                    page = Some(candidate);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || {
            format!(
                "export_facts={:?}",
                store.read_export_page(None, 10).map(|p| p.facts.len())
            )
        },
    )
    .await;
    let page = page.expect("terminal emission should publish one usage fact");
    assert_eq!(page.facts.len(), 1);
    assert_eq!(page.facts[0].segment.charge_basis, ChargeBasis::Unknown);
    assert_eq!(page.facts[0].segment.session, session);
    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn bind_ack_precedes_route_egress_and_stale_epoch_is_silent() {
    let _drop_counter_guard = SERVE_FRAME_DROP_COUNTER_TEST_LOCK.lock().await;
    let dir = tmpdir("bind-order-epoch");
    let handler = Arc::new(BlockingSendHandler::new());
    let health_registry = registry(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_handler = handler.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_handler,
            health_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let hello_ack = Frame::build(
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
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    let session = BindIdentity::new(dir.join("proj"), "runner", "ordered");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 2,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.project_root, session.harness, session.session),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(bind).unwrap()).await;
    let bind_ack = read_one(&mut creader).await;
    assert_eq!(bind_ack.header.ty, FrameType::Response);
    assert_eq!((bind_ack.header.channel, bind_ack.header.epoch), (0, 0));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), read_one(&mut creader))
            .await
            .is_err(),
        "route.bind must not emit route traffic before or after its ack"
    );

    write_req_epoch(
        &mut cwriter,
        7,
        1,
        11,
        serde_json::json!({
            "method": "session.send",
            "params": { "prompt": "stale", "model": { "provider": "fake", "model": "fake" } }
        }),
    )
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), read_one(&mut creader))
            .await
            .is_err(),
        "a stale route generation must not receive a response"
    );
    assert_eq!(handler.ordered_calls.load(Ordering::SeqCst), 0);

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn separator_collision_bind_is_rejected_before_any_durable_state() {
    let dir = tmpdir("bind-separator-collision");
    let (registry, wal) = registry_and_wal(&dir);
    let colliding = BindIdentity::new(dir.join("proj\u{1f}runner"), "a", "b");
    let invalid = BindIdentity::new(dir.join("proj"), "runner\u{1f}a", "b");
    let wal_path = wal.wal_path(&invalid);
    assert_ne!(colliding, invalid);
    assert_eq!(wal.wal_path(&colliding), wal_path);

    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let hello_ack = Frame::build(
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
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 2,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            invalid.project_root.clone(),
            invalid.harness.clone(),
            invalid.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(bind).unwrap()).await;
    let bind_reply = read_one(&mut creader).await;

    // Keep the test non-vacuous: without the serve-side validation, the bind is acknowledged;
    // this status request then opens the actor and creates the colliding WAL before the failure.
    if bind_reply.header.ty == FrameType::Response {
        write_req_epoch(
            &mut cwriter,
            7,
            2,
            11,
            serde_json::json!({"method": "run.status", "params": {}}),
        )
        .await;
        let _ = read_one(&mut creader).await;
        panic!(
            "ambiguous bind was accepted and opened colliding durable state at {}",
            wal_path.display()
        );
    }

    assert_eq!(bind_reply.header.ty, FrameType::Error);
    let error: ErrorBody = serde_json::from_slice(&bind_reply.body).unwrap();
    assert_eq!(error.code, "invalid_bind_identity");
    assert!(error.message.contains("U+001F"));
    assert!(!wal_path.exists(), "rejected bind must not create a WAL");
    let counts = registry.slot_counts();
    assert_eq!((counts.open, counts.opening, counts.closing), (0, 0, 0));
    let store = broca_store::SqliteStore::open(dir.join("store.db")).unwrap();
    assert!(store.read_admission_snapshot(&invalid).unwrap().is_none());
    let lease_root = dir.join("leases");
    assert!(
        !lease_root.exists() || std::fs::read_dir(&lease_root).unwrap().next().is_none(),
        "rejected bind must not create a lease"
    );

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

async fn wait_for_run_state<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    channel: u16,
    run_id: &str,
    expected: &str,
    first_corr: u64,
) -> serde_json::Value {
    // Bounded by elapsed time, not by a poll count. The run's progress is real file and SQLite
    // work, and on a loaded host a run that needs a second or two can need many times that; a
    // fixed 40 polls failed healthy runs there. The last state seen goes into the failure, so a
    // run that is stuck reads differently from one that is merely slow.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut offset = 0;
    let mut last = serde_json::Value::Null;
    while tokio::time::Instant::now() < deadline {
        write_req(
            writer,
            channel,
            first_corr + offset,
            serde_json::json!({
                "method": "run.status",
                "params": { "run_id": run_id }
            }),
        )
        .await;
        offset += 1;
        let resp = read_one(reader).await;
        assert_eq!(
            resp.header.ty,
            FrameType::Response,
            "run.status reply: {}",
            String::from_utf8_lossy(&resp.body)
        );
        let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        if v["result"]["state"] == expected {
            return v["result"].clone();
        }
        last = v["result"].clone();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("run {run_id} never reached {expected} status within 30 s; last status: {last}");
}

async fn wait_for_completed_run<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    channel: u16,
    run_id: &str,
    first_corr: u64,
) -> serde_json::Value {
    wait_for_run_state(reader, writer, channel, run_id, "completed", first_corr).await
}

/// HELLO + route.bind + session.send with a system prompt writes a RunStarted record whose
/// input leads with Role::System and preserves the exact text bytes.
#[tokio::test]
async fn hello_bind_send_with_system_writes_leading_system_message() {
    let dir = tmpdir("system");
    let (reg, wal) = registry_and_wal(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
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
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new(dir.join("proj"), "runner", "s1");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            session.project_root.clone(),
            session.harness.clone(),
            session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    let system = "  system α\nline δύο 🧪  ";
    write_req(
        &mut cwriter,
        7,
        11,
        serde_json::json!({
            "method": "session.send",
            "params": {
                "prompt": "hi",
                "system": system,
                "model": { "provider": "fake", "model": "fake" }
            }
        }),
    )
    .await;
    let resp = read_one(&mut creader).await;
    assert_eq!(resp.header.ty, FrameType::Response);
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(v["result"]["state"], "active");

    let records = broca_core::WalStore::read_records(wal.as_ref(), &session).unwrap();
    let run_started = records
        .into_iter()
        .find_map(|f| match f.record {
            broca_wal::WalRecord::RunStarted { input, .. } => Some(input),
            _ => None,
        })
        .expect("run started record");

    assert_eq!(
        run_started,
        vec![
            Message::new(
                Role::System,
                vec![ContentBlock::Text {
                    text: system.into(),
                }],
            ),
            Message::new(Role::User, vec![ContentBlock::Text { text: "hi".into() }],),
        ]
        .into()
    );

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// `seed_from` names a future lineage-fork operation, so v1 must reject it instead of silently
/// starting an unrelated fresh episode.
#[tokio::test]
async fn session_send_rejects_reserved_seed_from_before_starting_a_run() {
    let dir = tmpdir("seed-from-rejected");
    let (registry, wal) = registry_and_wal(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
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
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new(dir.join("proj"), "runner", "seed-from");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            session.project_root.clone(),
            session.harness.clone(),
            session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    write_req(
        &mut cwriter,
        7,
        11,
        serde_json::json!({
            "method": "session.send",
            "params": {
                "prompt": "fork from the earlier lineage",
                "model": { "provider": "fake", "model": "fake" },
                "seed_from": {
                    "source_lineage": "runner:source",
                    "at_seq": 42
                }
            }
        }),
    )
    .await;
    let response = read_one(&mut creader).await;
    assert_eq!(response.header.ty, FrameType::Error);
    let error: ErrorBody = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(error.code, "unsupported_seed_from");
    assert!(no_durable_records(wal.as_ref(), &session));

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// An unknown `context_management` value is refused on the wire before anything runs.
///
/// `SendParams` tolerates unknown FIELDS, so this is the one place a too-lenient field VALUE
/// would slip through: reading an unknown value as `"none"` would run the session unmanaged
/// against a window its caller believed was managed.
#[tokio::test]
async fn session_send_refuses_an_unknown_context_management_value_before_starting_a_run() {
    let dir = tmpdir("context-management-unknown");
    let (registry, wal) = registry_and_wal(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
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
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new(dir.join("proj"), "runner", "context-management");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            session.project_root.clone(),
            session.harness.clone(),
            session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    for (corr, value) in [
        (11, serde_json::json!("summarize")),
        (12, serde_json::json!({"reduce": "magic-context"})),
        (13, serde_json::json!({"transform": 7})),
    ] {
        write_req(
            &mut cwriter,
            7,
            corr,
            serde_json::json!({
                "method": "session.send",
                "params": {
                    "prompt": "manage my context",
                    "model": { "provider": "fake", "model": "fake" },
                    "context_management": value,
                }
            }),
        )
        .await;
        let response = read_one(&mut creader).await;
        assert_eq!(
            response.header.ty,
            FrameType::Error,
            "{value} must be refused"
        );
        let error: ErrorBody = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(error.code, "invalid_params", "{value}: {error:?}");
        assert!(
            error.message.contains("context_management must be")
                && error.message.contains(&value.to_string()),
            "the refusal names the field and the value: {error:?}"
        );
    }
    assert!(
        no_durable_records(wal.as_ref(), &session),
        "a refused value writes nothing durable"
    );

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// An unknown `service_tier` value, `"fast"` included, is refused on the wire before anything
/// runs, naming the three accepted tiers. `fast` is OpenAI's marketing name for priority and is
/// deliberately not an alias.
#[tokio::test]
async fn session_send_refuses_an_unknown_service_tier_before_starting_a_run() {
    let dir = tmpdir("service-tier-unknown");
    let (registry, wal) = registry_and_wal(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
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
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new(dir.join("proj"), "runner", "service-tier");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            session.project_root.clone(),
            session.harness.clone(),
            session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    for (corr, value) in [
        (11, serde_json::json!("fast")),
        (12, serde_json::json!("auto")),
        (13, serde_json::json!("PRIORITY")),
    ] {
        write_req(
            &mut cwriter,
            7,
            corr,
            serde_json::json!({
                "method": "session.send",
                "params": {
                    "prompt": "go fast",
                    "model": { "provider": "fake", "model": "fake" },
                    "service_tier": value,
                }
            }),
        )
        .await;
        let response = read_one(&mut creader).await;
        assert_eq!(
            response.header.ty,
            FrameType::Error,
            "{value} must be refused"
        );
        let error: ErrorBody = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(error.code, "invalid_params", "{value}: {error:?}");
        assert!(
            error
                .message
                .contains("service_tier must be \"flex\", \"default\" or \"priority\"",)
                && error.message.contains(&value.to_string()),
            "the refusal names the field and the value: {error:?}"
        );
    }
    assert!(
        no_durable_records(wal.as_ref(), &session),
        "a refused value writes nothing durable"
    );

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// Health reports lifetime projection failures, including failures from earlier
// tests in this binary. Re-execute probes of a healthy process with fresh metrics
// instead of resetting production counters or relaxing their exact assertions.
async fn isolated_health_probe(test: &str) -> bool {
    const CHILD_TEST: &str = "BROCA_ISOLATED_HEALTH_TEST";
    if std::env::var(CHILD_TEST).as_deref() == Ok(test) {
        return true;
    }
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--nocapture"])
            .env(CHILD_TEST, test)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("isolated health probe must finish within 30s")
    .expect("spawn isolated health probe");
    assert!(
        output.status.success() && String::from_utf8_lossy(&output.stdout).contains("1 passed"),
        "isolated health probe {test} failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    false
}

/// A channel-0 health.check returns registry counts and dependency states from memory.
#[tokio::test]
async fn health_check_reports_slot_counts_and_stale_lkg_without_degrading() {
    if !isolated_health_probe(
        "wire::health_check_reports_slot_counts_and_stale_lkg_without_degrading",
    )
    .await
    {
        return;
    }
    let _drop_counter_guard = SERVE_FRAME_DROP_COUNTER_TEST_LOCK.lock().await;
    // The serve binary publishes the table it loaded before serving; this harness has no
    // config, so it publishes the shipped table the same way.
    broca_module_serve::ChargeBasisTable::shipped_defaults().publish_for_health();
    let dir = tmpdir("health");
    // The run is HELD open across the health probe rather than left to finish on its own, so
    // runs_in_flight is observed while a run genuinely is in flight.
    let hold = Arc::new(tokio::sync::Notify::new());
    let reg = registry_with_hold(&dir, hold.clone());
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let resolved_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        - 5_000;
    let catalog_cache =
        common::snapshot_cache(resolved_at_ms, broca_catalog::CatalogSource::LastKnownGood);

    let serve = tokio::spawn(async move {
        let _ = serve_connection_with_catalog(
            sreader,
            swriter,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
            Some(catalog_cache),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
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
    write_frame(&mut cwriter, &ack).await.unwrap();

    // Capture the process-wide values before this rig sends any requests. A sibling test
    // may already have dropped a frame, so this test compares its two health readings rather
    // than assuming a fresh process starts at zero.
    write_req(
        &mut cwriter,
        0,
        9,
        serde_json::to_value(ModuleControlRequest::HealthCheck {}).unwrap(),
    )
    .await;
    let initial_health = read_one(&mut creader).await;
    assert_eq!(initial_health.header.ty, FrameType::Response);
    let initial_health: ModuleControlResponse =
        serde_json::from_slice(&initial_health.body).unwrap();
    let ModuleControlResponse::HealthCheck {
        metrics: Some(initial_metrics),
        ..
    } = initial_health
    else {
        panic!("expected initial health.check metrics");
    };
    let initial_metrics = initial_metrics
        .as_object()
        .expect("initial health.check metrics are a JSON object");
    let unbound_drops_before = initial_metrics["serve_frames_dropped_unbound_channel"]
        .as_u64()
        .expect("initial health.check includes the unbound-channel counter");
    let stale_drops_before = initial_metrics["serve_frames_dropped_stale_epoch"]
        .as_u64()
        .expect("initial health.check includes the stale-epoch counter");

    let session = BindIdentity::new(dir.join("proj"), "runner", "s1");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            session.project_root.clone(),
            session.harness.clone(),
            session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    write_req(
        &mut cwriter,
        7,
        11,
        serde_json::json!({
            "method": "session.send",
            "params": { "prompt": "hi", "model": { "provider": "fake", "model": "fake" } }
        }),
    )
    .await;
    let resp = read_one(&mut creader).await;
    assert_eq!(resp.header.ty, FrameType::Response);

    write_req(
        &mut cwriter,
        0,
        12,
        serde_json::to_value(&ModuleControlRequest::HealthCheck {}).unwrap(),
    )
    .await;
    let resp = read_one(&mut creader).await;
    assert_eq!(resp.header.ty, FrameType::Response);
    let health: ModuleControlResponse = serde_json::from_slice(&resp.body).unwrap();
    let ModuleControlResponse::HealthCheck {
        status,
        detail,
        metrics,
    } = health
    else {
        panic!("expected a health.check response");
    };
    assert_eq!(status, HealthStatus::Ok);
    assert_ne!(status, HealthStatus::Degraded);
    assert_eq!(detail, None);
    let metrics = metrics.expect("health.check includes metrics");
    let metrics = metrics
        .as_object()
        .expect("health.check metrics are a JSON object");
    // EXACT SET, not a contains-check, and the direction it adds is the load-bearing one.
    //
    // A `contains_key` list catches a field being REMOVED and cannot catch one being ADDED
    // unpinned -- which is the failure that actually happens here. The key list gets written
    // when the first field lands; later fields arrive in commits focused on their own logic,
    // and nothing sends the author back to this test. So the fields added LAST are the fields
    // nobody pins, and those are disproportionately the incident-reading ones, because
    // observability fields are added late by nature. Both catalog refusal gauges reached the
    // wire that way and were unasserted until an unrelated review found them.
    //
    // With an exact set, adding a metric fails here by name and forces the author to decide
    // what it should read on a healthy daemon at the moment of adding it.
    let expected: std::collections::BTreeSet<&str> = [
        // Identity of the serving process, for "did the process holding my work restart".
        // Pinned in the exact set like every other key: a health field the fleet reads during
        // an incident must not appear or vanish without a test saying so.
        "launch_id",
        "active_sessions",
        "opening",
        "closing",
        "vault",
        "catalog_age_ms",
        "catalog_source",
        // Runs executing right now, as opposed to open session slots. Shipped in v0.3.27 for
        // SUBC's restart gate and unpinned until the exact-set fence below found it -- the same
        // added-late-so-nobody-pins-it path as the refusal gauges.
        "runs_in_flight",
        // Present at zero for the same reason as the drop counters: absent-versus-zero is
        // the distinction an operator needs, and a field that appears only on failure
        // cannot say "this process has projected cleanly since boot".
        "projection_failures",
        // The current-state counterpart: unlike the lifetime counter, successful projection
        // removes only the recovered session's entry and leaves other sessions behind.
        "projection_behind_sessions",
        // The ck-bus run-event publisher: off by default, so a healthy default daemon reads
        // `enabled: false` with every counter at zero; `enabled` is what separates that from an
        // enabled publisher that has simply had nothing to send.
        "bus_events",
        "compaction_bus",
        // Process-local lifetime operation counts. The isolated operation_counters target proves
        // their zero and non-zero readings; this test keeps their presence in the total key set.
        "stream_chunks",
        "session_reads",
        "actor_opens",
        "management_ops",
        // The liveness signal for the scheduled WAL archive. Present even when the knob
        // is unset: zero with no knob is correct, zero on a CONFIGURED daemon that has
        // been up an hour is a defect -- and on 2026-09-14 there was no way to tell a
        // sleeping timer from one that never spawned.
        "wal_archive_polls",
        // The subscriber fan-out's counters, pinned in the exact set. The fan-out had
        // NO counters before -- not "only failure counters", which is what I first
        // wrote; frames_dropped_* above belong to broca-subc's client socket.
        // display_gaps is the loss signal, kept separate so deliveries cannot rise
        // when the lane drops frames.
        "control_deliveries",
        "display_deliveries",
        "display_gaps",
        // Present even at zero. A drop counter that appears only when non-zero cannot tell a
        // connection losing nothing from a gauge that was never wired, so the healthy reading
        // would be the one indistinguishable from a missing instrument.
        "frames_dropped_stale_epoch",
        "frames_dropped_unmatched_terminal",
        "reverse_requests_refused",
        // The preceding stale-epoch field belongs to broca-subc's client-side outbound routes;
        // these two are the serve loop's process-side inbound frame drops.
        "serve_frames_dropped_unbound_channel",
        "serve_frames_dropped_stale_epoch",
        // BOTH refusal fields, and the pair is the point. The streak is a current-state gauge
        // that resets on the first success; the total survives recovery. Streak zero with a
        // non-zero total is "occurred and recovered", which neither field can say alone.
        //
        // Listed together because a check on one half of a pair is not a check on the other,
        // and these two fail in different vocabularies: a missing total reads as absent, while
        // a total that wrongly resets reads as zero. FUSI hit exactly that -- their fence caught
        // one half of a two-field adoption and was blind to the other.
        "catalog_refusals",
        "catalog_refusals_total",
        // Present rows of the served catalog that could not be used and were left out or kept
        // at their previous model. A healthy catalog reads 0 with a null reason; a bad row
        // shows here rather than as a refusal, because the rest of the catalog still refreshed.
        "catalog_rows_excluded",
        "catalog_rows_excluded_reason",
        // The charge-basis table new facts are resolved against, as the facts carry it, and
        // how many local override rows make it up. A deployment with no override reads
        // "none" and 0; an override swap check compares these instead of scraping logs.
        "charge_basis_overrides",
        "charge_basis_override_rows",
        // Entries providers listed as changes they made to a request's input (for Anthropic,
        // thinking blocks dropped because an earlier part of the conversation changed). Present
        // at zero so a daemon that stopped counting cannot pass for a healthy one.
        "input_transformations",
        // Attempts sent without asking the provider for that report (every Claude Code persona
        // request), so their missing report means "not measured" rather than zero. Present at
        // zero, so the pair can tell "nothing transformed" from "nothing asked".
        "input_transformations_unmeasured_attempts",
        // How each new run's provider account was chosen and how its reports to routing
        // fared. Null, not an empty object, in a process that asks routing nothing (no vault
        // credential source, as here); its counters are read through a real daemon in
        // account_selection_real_daemon.
        "account_selection",
        "stall_watchdog",
        // Retained sessions whose expiry could not be confirmed because their log is unreadable
        // and the index is not provably current, so deletion keeps them. Present at zero.
        "retention_unreadable_sessions",
        "retention_repairs_pending",
        "agent_data_ops",
        // Decision-model batches currently calling a provider. It is one of the gauges the
        // daemon's restart drain waits on, so an idle daemon must read 0 here or every restart
        // would wait out its drain ceiling.
        "system_one_batches_in_flight",
    ]
    .into_iter()
    .collect();
    let actual: std::collections::BTreeSet<&str> = metrics.keys().map(String::as_str).collect();
    assert_eq!(
        actual, expected,
        "health.check metric keys drifted from what this test pins. A NEW key here is not a \
         failure to fix by adding it to the list -- decide what it must read on a healthy \
         daemon and assert that below, because an unasserted metric is one the fleet reads \
         during an incident and nothing defends."
    );
    // This in-process adapter has no daemon bootstrap or watchdog file. The real
    // binary enables the witness during boot; a library-only connection says so.
    assert_eq!(
        metrics["stall_watchdog"],
        serde_json::json!({"enabled": false})
    );
    // Asserted as a NON-EMPTY STRING rather than a literal: the value is per-launch, so
    // pinning it would pin the pid of whichever process ran the test. What must hold is that
    // it is present, non-empty, and carries the two components a caller compares.
    let launch = metrics["launch_id"]
        .as_str()
        .expect("launch_id is a string");
    assert!(
        launch.contains('-') && !launch.starts_with('-') && !launch.ends_with('-'),
        "launch_id must carry both nonce components, got {launch:?}"
    );
    assert_eq!(metrics["active_sessions"], serde_json::json!(1));
    assert_eq!(metrics["opening"], serde_json::json!(0));
    assert_eq!(metrics["closing"], serde_json::json!(0));
    assert_eq!(
        metrics["system_one_batches_in_flight"],
        serde_json::json!(0)
    );
    assert_eq!(metrics["vault"], serde_json::json!("disconnected"));
    assert_eq!(metrics["projection_failures"], serde_json::json!(0));
    assert_eq!(metrics["projection_behind_sessions"], serde_json::json!(0));
    assert_eq!(
        metrics["compaction_bus"],
        serde_json::json!({
            "attached": false,
            "consumed": 0,
            "acked": 0,
            "dropped_unknown_session": 0,
            "last_error": null,
        }),
        "a bus-off daemon has no compaction attachment, deliveries, or bus error"
    );
    assert_eq!(
        metrics["bus_events"],
        serde_json::json!({
            "enabled": false,
            "bus_events_published": 0,
            "bus_outbox_depth": 0,
            "bus_publish_failures": 0,
            "bus_events_oversize": 0,
            "bus_outbox_dropped": 0,
            // The liveness readings an enabled publisher moves. This rig leaves
            // `bus_run_events_enabled` off, so no publisher loop runs and the pass age is null
            // rather than a number that could look healthy.
            "bus_hints_seen": 0,
            "bus_sessions_projected": 0,
            "bus_account_known": false,
            "bus_recovery_pending": 0,
            "bus_recovery_walks": 0,
            "bus_recovery_visits": 0,
            "bus_loop_pass_age_ms": null,
        })
    );
    assert!(metrics["catalog_age_ms"].as_i64().unwrap() >= 5_000);
    assert_eq!(metrics["catalog_source"], serde_json::json!("lkg"));
    assert_eq!(metrics["catalog_refusals"], serde_json::json!(0));
    assert_eq!(metrics["catalog_refusals_total"], serde_json::json!(0));
    assert_eq!(metrics["catalog_rows_excluded"], serde_json::json!(0));
    assert_eq!(
        metrics["catalog_rows_excluded_reason"],
        serde_json::Value::Null
    );
    assert_eq!(metrics["charge_basis_overrides"], serde_json::json!("none"));
    assert_eq!(metrics["charge_basis_override_rows"], serde_json::json!(0));
    // Zero is the healthy reading: this rig's provider reports no transformations, and any
    // other number means some request was not an append-only extension of the one before.
    assert_eq!(metrics["input_transformations"], serde_json::json!(0));
    assert_eq!(
        metrics["input_transformations_unmeasured_attempts"],
        serde_json::json!(0)
    );
    assert_eq!(metrics["account_selection"], serde_json::Value::Null);
    // Zero on a healthy daemon: any other number is a retained session kept past its expiry.
    // The count belongs to this registry's retention service, so other tests cannot raise it.
    assert_eq!(
        metrics["retention_unreadable_sessions"],
        serde_json::json!(0)
    );
    // This registry has no failed retention writes, so no repair obligation is outstanding.
    assert_eq!(metrics["retention_repairs_pending"], serde_json::json!(0));
    assert_eq!(
        metrics["agent_data_ops"],
        serde_json::json!({
            "describe":0,"busy":0,"diverged":0,"prepare":0,"export":0,
            "import":0,"release":0,"activate":0,"abort":0,"status":0
        })
    );
    assert!(metrics.get("agent_data_inventory").is_none());
    // The counter lock keeps the stale-frame sibling out during this interval. Equal readings
    // show that this rig's health probe and valid route request did not drop a frame, while the
    // unsigned values above ensure each metric remains present when its process count is zero.
    assert_eq!(
        metrics["serve_frames_dropped_unbound_channel"].as_u64(),
        Some(unbound_drops_before),
        "this rig did not submit an unbound-channel frame"
    );
    assert_eq!(
        metrics["serve_frames_dropped_stale_epoch"].as_u64(),
        Some(stale_drops_before),
        "this rig did not submit a stale-epoch frame"
    );
    // ONE, not zero: this rig holds a run at the point health is probed. I wrote zero first,
    // assuming the rig drove no runs, and the fence corrected me -- which is the better
    // assertion by luck. A gauge hardcoded to zero would satisfy `== 0` here and would also
    // approve every restart, because SUBC's gate reads this to decide whether a bounce would
    // cut work in flight. Asserting the NON-zero reading is what proves it counts at all.
    assert_eq!(metrics["runs_in_flight"], serde_json::json!(1));

    // Release the parked run so the rig can shut down cleanly.
    hold.notify_one();

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A vanished project root refuses the ops that use it and admits the ops that end a run.
///
/// The second half is the one worth having. A run whose root disappears can only be ended
/// through `run.cancel`/`run.status`, so a check applied to every op would leave it with no
/// exit at all -- turning a directory that went missing into a run nobody can finish.
#[tokio::test]
async fn a_vanished_project_root_refuses_execute_ops_but_still_admits_run_ops() {
    let dir = tmpdir("vanished-root");
    let reg = registry(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
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
    write_frame(&mut cwriter, &ack).await.unwrap();

    let root = dir.join("proj");
    let session = BindIdentity::new(root.clone(), "runner", "vanished");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 2,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            session.project_root.clone(),
            session.harness.clone(),
            session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(bind).unwrap()).await;
    assert_eq!(read_one(&mut creader).await.header.ty, FrameType::Response);

    // CONTROL: the same send succeeds while the root is present, so a later refusal is
    // attributable to the missing directory rather than to anything else in this rig.
    let send = serde_json::json!({
        "method": "session.send",
        "params": {"prompt": "hi", "model": {"provider": "fake", "model": "fake"}}
    });
    write_req_epoch(&mut cwriter, 7, 2, 11, send.clone()).await;
    let admitted = read_one(&mut creader).await;
    assert_eq!(
        admitted.header.ty,
        FrameType::Response,
        "the send must be admitted while the root exists"
    );
    let admitted_body: serde_json::Value = serde_json::from_slice(&admitted.body).unwrap();
    let run_id = admitted_body["result"]["run_id"]
        .as_str()
        .expect("an admitted send returns its run_id")
        .to_string();

    std::fs::remove_dir_all(&root).unwrap();
    assert!(
        !root.exists(),
        "the root must be gone for this test to mean anything"
    );

    // A send USES the root: tools resolve paths beneath it, so this must fail at admission
    // rather than several steps later at the first tool call.
    write_req_epoch(&mut cwriter, 7, 2, 12, send).await;
    let refused = read_one(&mut creader).await;
    assert_eq!(refused.header.ty, FrameType::Error);
    let refused_body: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
    assert_eq!(refused_body["code"], "invalid_project_root");
    assert_eq!(
        refused_body["message"],
        format!("project root does not exist: {}", root.display()),
        "the refusal must name the missing directory so the operator knows what to repair"
    );

    let import = serde_json::json!({
        "method": "session.import",
        "params": {
            "messages": [],
            "import_id": "vanished-import",
            "origin": {"kind": "import", "source_ref": "fixture"}
        }
    });
    write_req_epoch(&mut cwriter, 7, 2, 13, import).await;
    let refused = read_one(&mut creader).await;
    assert_eq!(refused.header.ty, FrameType::Error);
    let refused_body: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
    assert_eq!(refused_body["code"], "invalid_project_root");
    assert_eq!(
        refused_body["message"],
        format!("project root does not exist: {}", root.display()),
        "an import refusal must name the same missing directory"
    );

    // THE HALF THAT MATTERS: every op that reads or ends a run must still be reachable.
    // run.cancel is named explicitly because it is the ONLY exit from a paused run -- a test
    // that checked run.status alone would pass even with the check wrongly applied to cancel,
    // which is precisely the mistake that strands a run forever.
    for (corr, method) in [(14u64, "run.status"), (15, "run.cancel")] {
        write_req_epoch(
            &mut cwriter,
            7,
            2,
            corr,
            serde_json::json!({"method": method, "params": {"run_id": run_id}}),
        )
        .await;
        let reply = read_one(&mut creader).await;
        if reply.header.ty == FrameType::Error {
            let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
            assert_ne!(
                body["code"], "invalid_project_root",
                "{method} must not be refused for a vanished root, or a run in this state has no exit"
            );
        }
    }

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Vault health distinguishes never-connected, connected, broken, and cached-fallback states.
#[tokio::test]
async fn health_check_vault_gauge_flips_on_stamped_outcomes() {
    if !isolated_health_probe("wire::health_check_vault_gauge_flips_on_stamped_outcomes").await {
        return;
    }
    let dir = tmpdir("health-gauge");
    let reg = registry(&dir);
    let gauge = broca_module_serve::serve::VaultGauge::disconnected();
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_gauge = gauge.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(sreader, swriter, "broca", reg.clone(), reg, serve_gauge).await;
    });

    let hello = read_one(&mut creader).await;
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
    write_frame(&mut cwriter, &ack).await.unwrap();

    let probe = || serde_json::to_value(&ModuleControlRequest::HealthCheck {}).unwrap();

    write_req(&mut cwriter, 0, 20, probe()).await;
    let resp = read_one(&mut creader).await;
    let ModuleControlResponse::HealthCheck {
        status,
        detail,
        metrics,
    } = serde_json::from_slice(&resp.body).unwrap()
    else {
        panic!("expected a health.check response");
    };
    assert_eq!(status, HealthStatus::Degraded);
    assert_eq!(
        detail.as_deref(),
        Some("vault: credentials module not catalog-live")
    );
    let metrics = metrics.unwrap();
    assert_eq!(metrics["vault"], serde_json::json!("disconnected"));
    assert!(
        metrics.get("vault_ok_age_s").is_none(),
        "a gauge that has never completed a call must not report an age at all"
    );

    gauge.stamp_ok();
    write_req(&mut cwriter, 0, 21, probe()).await;
    let resp = read_one(&mut creader).await;
    let ModuleControlResponse::HealthCheck {
        status,
        detail,
        metrics,
    } = serde_json::from_slice(&resp.body).unwrap()
    else {
        panic!("expected a health.check response");
    };
    assert_eq!(status, HealthStatus::Ok);
    assert_eq!(detail, None);
    let metrics = metrics.unwrap();
    assert_eq!(metrics["vault"], serde_json::json!("connected"));
    // The healthy reading has to be ageable too. Before a call completes the field is ABSENT
    // rather than zero, so "registered but never dialled" cannot be read as a fresh success.
    assert_eq!(
        metrics["vault_ok_age_s"].as_i64(),
        Some(0),
        "a just-stamped success ages to zero seconds"
    );

    gauge.stamp_error();
    write_req(&mut cwriter, 0, 22, probe()).await;
    let resp = read_one(&mut creader).await;
    let ModuleControlResponse::HealthCheck {
        status,
        detail,
        metrics,
    } = serde_json::from_slice(&resp.body).unwrap()
    else {
        panic!("expected a health.check response");
    };
    assert_eq!(status, HealthStatus::Degraded);
    assert!(
        detail.unwrap().contains("credential vault unreachable"),
        "broken state names the unreachable vault"
    );
    let metrics = metrics.unwrap();
    assert_eq!(metrics["vault"], serde_json::json!("broken"));
    assert!(metrics["last_vault_error_ms"].as_i64().unwrap() > 0);

    gauge.stamp_serving_cached();
    write_req(&mut cwriter, 0, 23, probe()).await;
    let resp = read_one(&mut creader).await;
    let ModuleControlResponse::HealthCheck {
        status,
        detail,
        metrics,
    } = serde_json::from_slice(&resp.body).unwrap()
    else {
        panic!("expected a health.check response");
    };
    assert_eq!(status, HealthStatus::Degraded);
    assert_eq!(
        detail.as_deref(),
        Some("vault unreachable; serving cached credentials")
    );
    assert_eq!(metrics.unwrap()["vault"], serde_json::json!("broken"));

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// HELLO carries the broca manifest with the ManagementSurface shape; route.bind acks;
/// session.send + run.status dispatch and return Response frames.
#[tokio::test]
async fn hello_bind_send_status_roundtrip() {
    let dir = tmpdir("rt");
    let reg = registry(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    // 1. HELLO from the module.
    let hello = read_one(&mut creader).await;
    assert_eq!(hello.header.ty, FrameType::Hello);
    let body: ModuleHelloBody = serde_json::from_slice(&hello.body).unwrap();
    assert_eq!(body.manifest.module_id, "broca");
    assert_eq!(
        body.control_ops,
        Some(vec![MODULE_CONTROL_OP_HEALTH_CHECK.to_string()])
    );
    // Send HELLO_ACK.
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
    write_frame(&mut cwriter, &ack).await.unwrap();

    // 2. route.bind on channel 7 for a session.
    let session = BindIdentity::new(dir.join("proj"), "runner", "s1");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            session.project_root.clone(),
            session.harness.clone(),
            session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let ack = read_one(&mut creader).await;
    assert_eq!(ack.header.ty, FrameType::Response);
    let _: ModuleControlResponse = serde_json::from_slice(&ack.body).unwrap();

    // 3. session.send on the bound route.
    write_req(
        &mut cwriter,
        7,
        11,
        serde_json::json!({
            "method": "session.send",
            "params": { "prompt": "hi", "model": { "provider": "fake", "model": "fake" } }
        }),
    )
    .await;
    let resp = read_one(&mut creader).await;
    assert_eq!(resp.header.ty, FrameType::Response);
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(
        v["result"]["state"], "active",
        "send returns active for an idle lineage"
    );

    // 4. run.status on the bound route.
    write_req(
        &mut cwriter,
        7,
        12,
        serde_json::json!({ "method": "run.status", "params": {} }),
    )
    .await;
    let resp = read_one(&mut creader).await;
    assert_eq!(resp.header.ty, FrameType::Response);
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert!(v["result"]["state"].is_string(), "status returns a state");

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// This test verifies that route requests are spawned instead of being handled directly in
/// the reader loop. If the reader waits for one slow send to finish before reading the next
/// frame, another session can time out, so spawning request tasks keeps the connection
/// draining and prevents one blocked request from delaying other sessions.
#[tokio::test]
async fn slow_request_on_one_session_does_not_block_another_session() {
    let dir = tmpdir("hol");
    let handler = Arc::new(BlockingSendHandler::new());
    let health_registry = registry(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_handler = handler.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_handler,
            health_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
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
    write_frame(&mut cwriter, &ack).await.unwrap();

    let slow_session = BindIdentity::new(dir.join("proj"), "runner", "slow");
    let slow_bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            slow_session.project_root.clone(),
            slow_session.harness.clone(),
            slow_session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(
        &mut cwriter,
        0,
        10,
        serde_json::to_value(&slow_bind).unwrap(),
    )
    .await;
    let _ = read_one(&mut creader).await;

    let fast_session = BindIdentity::new(dir.join("proj"), "runner", "fast");
    let fast_bind = ModuleControlRequest::RouteBind {
        route_channel: 8,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            fast_session.project_root.clone(),
            fast_session.harness.clone(),
            fast_session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(
        &mut cwriter,
        0,
        11,
        serde_json::to_value(&fast_bind).unwrap(),
    )
    .await;
    let _ = read_one(&mut creader).await;

    write_req(
        &mut cwriter,
        7,
        20,
        serde_json::json!({
            "method": "session.send",
            "params": { "prompt": "slow", "model": { "provider": "fake", "model": "fake" } }
        }),
    )
    .await;
    handler.slow_send_started.notified().await;

    write_req(
        &mut cwriter,
        8,
        21,
        serde_json::json!({ "method": "run.status", "params": {} }),
    )
    .await;

    let fast_resp = tokio::time::timeout(Duration::from_millis(250), read_one(&mut creader))
        .await
        .expect("the fast session must not wait for the slow session's send");
    assert_eq!(fast_resp.header.channel, 8);
    assert_eq!(fast_resp.header.corr, 21);
    assert_eq!(fast_resp.header.ty, FrameType::Response);
    let fast_body: serde_json::Value = serde_json::from_slice(&fast_resp.body).unwrap();
    assert_eq!(fast_body["result"]["state"], "idle");

    handler.release_slow_send.notify_one();
    let slow_resp = tokio::time::timeout(Duration::from_secs(2), read_one(&mut creader))
        .await
        .expect("the slow send should finish once the test releases it");
    assert_eq!(slow_resp.header.channel, 7);
    assert_eq!(slow_resp.header.corr, 20);
    assert_eq!(slow_resp.header.ty, FrameType::Response);
    let slow_body: serde_json::Value = serde_json::from_slice(&slow_resp.body).unwrap();
    assert_eq!(slow_body["result"]["state"], "active");

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn pipelined_requests_for_one_session_reach_the_handler_in_wire_order() {
    let dir = tmpdir("same-session-fifo");
    let handler = Arc::new(BlockingSendHandler::new());
    let health_registry = registry(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_handler = handler.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_handler,
            health_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
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
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new(dir.join("proj"), "runner", "ordered");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            session.project_root.clone(),
            session.harness.clone(),
            session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    write_req(
        &mut cwriter,
        7,
        20,
        serde_json::json!({
            "method": "session.send",
            "params": { "prompt": "first", "model": { "provider": "fake", "model": "fake" } }
        }),
    )
    .await;
    handler.first_ordered_send_started.notified().await;

    write_req(
        &mut cwriter,
        7,
        21,
        serde_json::json!({
            "method": "session.send",
            "params": { "prompt": "second", "model": { "provider": "fake", "model": "fake" } }
        }),
    )
    .await;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            handler.second_ordered_send_started.notified()
        )
        .await
        .is_err(),
        "the second request must not reach the actor-bound handler while the first is parked"
    );

    handler.release_first_ordered_send.notify_one();
    let first = tokio::time::timeout(Duration::from_secs(2), read_one(&mut creader))
        .await
        .expect("first request completes after release");
    assert_eq!((first.header.channel, first.header.corr), (7, 20));
    handler.second_ordered_send_started.notified().await;
    let second = tokio::time::timeout(Duration::from_secs(2), read_one(&mut creader))
        .await
        .expect("second request runs after the first");
    assert_eq!((second.header.channel, second.header.corr), (7, 21));

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A one-shot send writes no store projection, then public `session.read` returns its complete
/// transcript through the existing WAL fallback.
#[tokio::test]
async fn read_route_manifest_and_page_roundtrip() {
    let dir = tmpdir("read");
    let lease = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
    let exec = Arc::new(CoreExec {
        wal_store: wal.clone(),
        store: Some(store.clone()),
        reply_text: "one-shot answer".into(),
    });
    let opener = Arc::new(
        DefaultOpener::new(lease, wal.clone(), exec)
            .with_run_index(store.clone())
            .with_materialized_store(store.clone()),
    );
    let wal_reader: Arc<dyn broca_core::WalStore> = wal.clone();
    let store_reader: Arc<dyn broca_core::MaterializedStore> = store.clone();
    let reg = Arc::new(LineageRegistry::new_with_reader(
        opener,
        wal_reader,
        Some(store_reader),
    ));
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let body: ModuleHelloBody = serde_json::from_slice(&hello.body).unwrap();
    let read_op = body
        .manifest
        .provides
        .iter()
        .find_map(|role| match role {
            subc_protocol::manifest::ProviderRole::ManagementSurface { operations, .. } => {
                operations.iter().find(|op| op.name == "session.read")
            }
            _ => None,
        })
        .expect("manifest includes session.read");
    assert_eq!(
        read_op.kind,
        subc_protocol::manifest::ManagementOperationKind::Query
    );
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
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new(dir.join("proj"), "runner", "s1");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            session.project_root.clone(),
            session.harness.clone(),
            session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    write_req(
        &mut cwriter,
        7,
        11,
        serde_json::json!({
            "method": "session.send",
            "params": {
                "prompt": "hi",
                "model": { "provider": "fake", "model": "fake" },
                "one_shot": true
            }
        }),
    )
    .await;
    let resp = read_one(&mut creader).await;
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    let run_id = v["result"]["run_id"].as_str().unwrap().to_string();
    wait_for_completed_run(&mut creader, &mut cwriter, 7, &run_id, 100).await;

    let prefix = store
        .read_prefix_with_watermark(&session)
        .expect("inspect materialized store");
    assert_eq!(prefix.watermark_seq, 0, "one-shot run writes no store row");
    assert!(
        prefix.prefix.is_empty(),
        "one-shot transcript is not stored"
    );

    write_req(
        &mut cwriter,
        7,
        12,
        serde_json::json!({
            "method": "session.read",
            "params": { "from_ordinal": 0, "limit": 10 }
        }),
    )
    .await;
    let resp = read_one(&mut creader).await;
    assert_eq!(resp.header.ty, FrameType::Response);
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    let result: broca_module_serve::wire::ReadResult =
        serde_json::from_value(v["result"].clone()).unwrap();
    assert_eq!(result.messages.len(), 2);
    assert_eq!(result.messages[0].ordinal, 0);
    assert_eq!(result.messages[0].message.role, Role::User);
    assert_eq!(result.messages[1].ordinal, 1);
    assert_eq!(result.messages[1].message.role, Role::Assistant);
    assert_eq!(result.lineage_state.state, "completed");
    assert_eq!(result.lineage_state.reason.as_deref(), Some("completed"));
    assert_eq!(result.tools, None, "tools are opt-in");
    assert_eq!(result.tools_run_id, None);

    // `include_tools` must travel from the wire params to the handler. This send declared no
    // tools, so the episode's frozen list is present and empty -- not absent, which would
    // mean the session has no episode.
    write_req(
        &mut cwriter,
        7,
        13,
        serde_json::json!({
            "method": "session.read",
            "params": { "from_ordinal": 0, "limit": 10, "include_tools": true }
        }),
    )
    .await;
    let resp = read_one(&mut creader).await;
    assert_eq!(resp.header.ty, FrameType::Response);
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    let with_tools: broca_module_serve::wire::ReadResult =
        serde_json::from_value(v["result"].clone()).unwrap();
    assert_eq!(with_tools.tools, Some(Vec::new()));
    assert_eq!(with_tools.tools_run_id.as_deref(), Some(run_id.as_str()));
    assert_eq!(with_tools.messages, result.messages);

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Two omitted-tool `session.send` requests with the same `send_id` route to the same
/// durable episode. Reusing that id with an explicit empty surface is rejected.
#[tokio::test]
async fn same_send_id_same_prompt_reuses_one_episode() {
    let dir = tmpdir("send-id");
    let lease = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let exec = Arc::new(CoreExec {
        wal_store: wal.clone(),
        store: None,
        reply_text: "answer".into(),
    });
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
    let reg = Arc::new(LineageRegistry::new(Arc::new(
        DefaultOpener::new(lease, wal.clone(), exec)
            .with_idle_ttl(Duration::from_millis(50))
            .with_run_index(store),
    )));
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_registry = reg.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
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
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new(dir.join("proj"), "runner", "s1");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            session.project_root.clone(),
            session.harness.clone(),
            session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    let send_body = serde_json::json!({
        "method": "session.send",
        "params": {
            "prompt": "hi",
            "send_id": "logical-send-1",
            "model": { "provider": "fake", "model": "fake" }
        }
    });

    write_req(&mut cwriter, 7, 11, send_body.clone()).await;
    let resp1 = read_one(&mut creader).await;
    assert_eq!(resp1.header.ty, FrameType::Response);
    let v1: serde_json::Value = serde_json::from_slice(&resp1.body).unwrap();
    let run_id = v1["result"]["run_id"]
        .as_str()
        .expect("first send returns a run_id")
        .to_string();
    let terminal = wait_for_completed_run(&mut creader, &mut cwriter, 7, &run_id, 100).await;
    assert_eq!(terminal["final_step_finish_reason"], "stop");
    assert_eq!(terminal["usage"]["input_tokens"], 11);
    assert_eq!(terminal["usage"]["output_tokens"], 4);

    broca_test_support::wait_for(
        "same_send_id_same_prompt_reuses_one_episode",
        async {
            loop {
                if reg.slot_state(&session).is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
        || format!("slot={:?}", reg.slot_state(&session)),
    )
    .await;
    assert_eq!(
        reg.slot_state(&session),
        None,
        "the mismatch must exercise durable comparison after actor recreation"
    );

    let mut explicit_empty = send_body.clone();
    explicit_empty["params"]["tools"] = serde_json::json!([]);
    write_req(&mut cwriter, 7, 12, explicit_empty).await;
    let mismatch = read_one(&mut creader).await;
    assert_eq!(mismatch.header.ty, FrameType::Error);
    let mismatch_body: serde_json::Value = serde_json::from_slice(&mismatch.body).unwrap();
    assert_eq!(mismatch_body["code"], "send_id_reuse");
    assert!(
        mismatch_body["message"]
            .as_str()
            .is_some_and(|message| message.contains("tools")),
        "the recreated actor must preserve the field-specific tools mismatch: {mismatch_body}"
    );

    write_req(&mut cwriter, 7, 13, send_body).await;
    let resp2 = read_one(&mut creader).await;
    assert_eq!(resp2.header.ty, FrameType::Response);
    let v2: serde_json::Value = serde_json::from_slice(&resp2.body).unwrap();
    assert_eq!(
        v2["result"]["run_id"].as_str(),
        Some(run_id.as_str()),
        "the byte-identical retry still resolves the same derived run_id"
    );
    wait_for_completed_run(&mut creader, &mut cwriter, 7, &run_id, 140).await;

    let run_starts = broca_core::WalStore::read_records(wal.as_ref(), &session)
        .unwrap()
        .into_iter()
        .filter(|f| matches!(f.record, broca_wal::WalRecord::RunStarted { .. }))
        .count();
    assert_eq!(
        run_starts, 1,
        "two identical same-send-id sends write one episode"
    );

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn chatgpt_needs_reauth_pauses_and_resumes_original_send() {
    let dir = tmpdir("reauth-public-send");
    let lease = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let credential_ready = Arc::new(AtomicBool::new(false));
    let attempts = Arc::new(AtomicUsize::new(0));
    let provider_calls = Arc::new(Mutex::new(HashMap::new()));
    let first_pause_release = Arc::new(Notify::new());
    let exec = Arc::new(ReauthCoreExec {
        wal_store: wal.clone(),
        credential_ready: credential_ready.clone(),
        attempts,
        provider_calls: provider_calls.clone(),
        first_pause_release: first_pause_release.clone(),
        first_attempt_pauses: true,
    });
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
    let reg = Arc::new(LineageRegistry::new_with_reader(
        Arc::new(
            DefaultOpener::new(lease, wal.clone(), exec)
                .with_idle_ttl(Duration::from_secs(5))
                .with_run_index(store.clone()),
        ),
        wal.clone(),
        Some(store),
    ));
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve_registry = reg.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
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
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new(dir.join("proj"), "runner", "reauth");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            session.project_root.clone(),
            session.harness.clone(),
            session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    assert_eq!(read_one(&mut creader).await.header.ty, FrameType::Response);

    let original_send = serde_json::json!({
        "method": "session.send",
        "params": {
            "prompt": "resume me",
            "send_id": "reauth-original",
            "model": { "provider": "openai", "model": "gpt-5.3-codex" }
        }
    });
    write_req(&mut cwriter, 7, 11, original_send.clone()).await;
    let first = read_one(&mut creader).await;
    assert_eq!(first.header.ty, FrameType::Response);
    let first_body: serde_json::Value = serde_json::from_slice(&first.body).unwrap();
    let original_run_id = first_body["result"]["run_id"]
        .as_str()
        .expect("first send returns the admitted run")
        .to_string();

    let queued_send = serde_json::json!({
        "method": "session.send",
        "params": {
            "prompt": "run after reauth",
            "send_id": "queued-after-reauth",
            "model": { "provider": "openai", "model": "gpt-5.3-codex" }
        }
    });
    write_req(&mut cwriter, 7, 12, queued_send).await;
    let queued = read_one(&mut creader).await;
    assert_eq!(queued.header.ty, FrameType::Response);
    let queued_body: serde_json::Value = serde_json::from_slice(&queued.body).unwrap();
    assert!(queued_body["result"]["submission_id"].is_string());

    first_pause_release.notify_one();
    wait_for_run_state(
        &mut creader,
        &mut cwriter,
        7,
        &original_run_id,
        "paused",
        100,
    )
    .await;
    assert!(wal
        .read_records(&session)
        .unwrap()
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::RunPaused { .. })));

    // A still-dead credential must not consume the pause or call the provider, even with a
    // queued turn waiting behind the original run.
    write_req(&mut cwriter, 7, 13, original_send.clone()).await;
    let still_dead = read_one(&mut creader).await;
    assert_eq!(still_dead.header.ty, FrameType::Response);
    let still_dead_body: serde_json::Value = serde_json::from_slice(&still_dead.body).unwrap();
    assert_eq!(
        still_dead_body["result"]["run_id"].as_str(),
        Some(original_run_id.as_str())
    );
    wait_for_run_state(
        &mut creader,
        &mut cwriter,
        7,
        &original_run_id,
        "paused",
        150,
    )
    .await;
    assert_eq!(
        provider_calls
            .lock()
            .unwrap()
            .get(&original_run_id)
            .copied()
            .unwrap_or(0),
        0
    );

    credential_ready.store(true, Ordering::SeqCst);
    // Polling observes the durable pause even after credential repair. Only the
    // explicit re-send below may resume this episode or write another record.
    let before = serde_json::to_vec(
        &wal.read_records(&session)
            .unwrap()
            .iter()
            .map(|frame| &frame.record)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    write_req(
        &mut cwriter,
        7,
        15,
        serde_json::json!({"method": "send.status", "params": {"send_id": "reauth-original"}}),
    )
    .await;
    let observed = read_one(&mut creader).await;
    assert_eq!(observed.header.ty, FrameType::Response);
    let observed: serde_json::Value = serde_json::from_slice(&observed.body).unwrap();
    assert_eq!(observed["result"]["state"], "paused");
    assert_eq!(observed["result"]["pause_reason"], "auth_required");
    assert_eq!(observed["result"]["run_id"], original_run_id);
    assert_eq!(
        serde_json::to_vec(
            &wal.read_records(&session)
                .unwrap()
                .iter()
                .map(|frame| &frame.record)
                .collect::<Vec<_>>()
        )
        .unwrap(),
        before
    );
    assert!(
        provider_calls.lock().unwrap().is_empty(),
        "send.status must not re-drive a repaired credential"
    );
    write_req(&mut cwriter, 7, 14, original_send).await;
    let resumed = read_one(&mut creader).await;
    assert_eq!(resumed.header.ty, FrameType::Response);
    let resumed_body: serde_json::Value = serde_json::from_slice(&resumed.body).unwrap();
    assert_eq!(
        resumed_body["result"]["run_id"].as_str(),
        Some(original_run_id.as_str()),
        "the repaired credential must continue the original durable episode"
    );
    let terminal =
        wait_for_completed_run(&mut creader, &mut cwriter, 7, &original_run_id, 200).await;
    assert_eq!(terminal["state"], "completed");

    broca_test_support::wait_for(
        "chatgpt_needs_reauth_pauses_and_resumes_original_send",
        async {
            loop {
                let records = wal.read_records(&session).unwrap();
                let record_values = records
                    .iter()
                    .map(|frame| frame.record.clone())
                    .collect::<Vec<_>>();
                let queued_called = {
                    let calls = provider_calls.lock().unwrap();
                    calls
                        .iter()
                        .any(|(run_id, count)| run_id != &original_run_id && *count == 1)
                };
                if broca_core::pending_submissions(&record_values).is_empty() && queued_called {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
        || {
            wal.read_records(&session)
                .map(|frames| broca_test_support::record_kinds(frames.iter().map(|f| &f.record)))
                .unwrap_or_else(|e| format!("WAL read: {e}"))
        },
    )
    .await;

    {
        let calls = provider_calls.lock().unwrap();
        assert_eq!(calls.get(&original_run_id), Some(&1));
        assert_eq!(
            calls.values().sum::<usize>(),
            2,
            "the resumed original and the subsequently drained queued turn each call once"
        );
    }
    let records = wal.read_records(&session).unwrap();
    let record_values = records
        .iter()
        .map(|frame| frame.record.clone())
        .collect::<Vec<_>>();
    assert!(broca_core::pending_submissions(&record_values).is_empty());
    assert_eq!(
        record_values
            .iter()
            .filter(|record| matches!(record, WalRecord::RunStarted { .. }))
            .count(),
        2,
        "continuation keeps one original episode and queue drain appends one later episode"
    );

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn chatgpt_needs_reauth_send_durably_pauses_then_resumes_same_run() {
    let dir = tmpdir("reauth-fresh-chatgpt");
    let lease = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let credential_ready = Arc::new(AtomicBool::new(false));
    let provider_calls = Arc::new(Mutex::new(HashMap::new()));
    let exec = Arc::new(ReauthCoreExec {
        wal_store: wal.clone(),
        credential_ready: credential_ready.clone(),
        // Start after the test executor's special first-attempt handling so the core's
        // actual pause path runs.
        attempts: Arc::new(AtomicUsize::new(1)),
        provider_calls: provider_calls.clone(),
        first_pause_release: Arc::new(Notify::new()),
        first_attempt_pauses: true,
    });
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
    let registry = Arc::new(LineageRegistry::new(Arc::new(
        DefaultOpener::new(lease, wal.clone(), exec)
            .with_idle_ttl(Duration::from_secs(5))
            .with_run_index(store),
    )));
    let session = BindIdentity::new(dir.join("proj"), "runner", "fresh-reauth");
    let request = chatgpt_send(session.clone(), "run-chatgpt-reauth", "resume me");

    let first = registry.send(request.clone()).await.unwrap();
    assert!(matches!(
        first,
        SendResponse::Active { run_id, .. } if run_id == "run-chatgpt-reauth"
    ));
    broca_test_support::wait_for(
        "chatgpt_needs_reauth_send_durably_pauses_then_resumes_same_run",
        async {
            loop {
                if wal.read_records(&session).unwrap().iter().any(|frame| {
                    matches!(
                        &frame.record,
                        WalRecord::RunPaused { run_id, .. } if run_id.0 == "run-chatgpt-reauth"
                    )
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
        || {
            wal.read_records(&session)
                .map(|frames| broca_test_support::record_kinds(frames.iter().map(|f| &f.record)))
                .unwrap_or_else(|e| format!("WAL read: {e}"))
        },
    )
    .await;
    let paused_records = wal.read_records(&session).unwrap();
    assert!(paused_records
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::RunStarted { .. })));
    assert!(paused_records.iter().any(|frame| {
        matches!(
            &frame.record,
            WalRecord::RunPaused { run_id, .. } if run_id.0 == "run-chatgpt-reauth"
        )
    }));
    assert_eq!(
        provider_calls
            .lock()
            .unwrap()
            .get("run-chatgpt-reauth")
            .copied()
            .unwrap_or(0),
        0,
        "pause-only construction must not call the provider"
    );
    broca_test_support::wait_for(
        "chatgpt_needs_reauth_send_durably_pauses_then_resumes_same_run",
        async {
            loop {
                if matches!(
                    registry
                        .run_status(&session, RunId("run-chatgpt-reauth".into()))
                        .await
                        .unwrap(),
                    RunStatusResponse::Paused { .. }
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
        || {
            wal.read_records(&session)
                .map(|frames| broca_test_support::record_kinds(frames.iter().map(|f| &f.record)))
                .unwrap_or_else(|e| format!("WAL read: {e}"))
        },
    )
    .await;
    assert!(matches!(
        registry
            .run_status(&session, RunId("run-chatgpt-reauth".into()))
            .await
            .unwrap(),
        RunStatusResponse::Paused { .. }
    ));

    credential_ready.store(true, Ordering::SeqCst);
    let resumed = registry.send(request).await.unwrap();
    assert!(matches!(
        resumed,
        SendResponse::Active { run_id, .. } if run_id == "run-chatgpt-reauth"
    ));
    broca_test_support::wait_for(
        "chatgpt_needs_reauth_send_durably_pauses_then_resumes_same_run",
        async {
            loop {
                if provider_calls
                    .lock()
                    .unwrap()
                    .get("run-chatgpt-reauth")
                    .copied()
                    == Some(1)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
        || {
            wal.read_records(&session)
                .map(|frames| broca_test_support::record_kinds(frames.iter().map(|f| &f.record)))
                .unwrap_or_else(|e| format!("WAL read: {e}"))
        },
    )
    .await;
    assert_eq!(
        provider_calls
            .lock()
            .unwrap()
            .get("run-chatgpt-reauth")
            .copied(),
        Some(1)
    );
    assert_eq!(
        wal.read_records(&session)
            .unwrap()
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::RunStarted { .. }))
            .count(),
        1,
        "reauth resumes the original episode"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn queued_chatgpt_needs_reauth_pauses_instead_of_being_tombstoned() {
    let dir = tmpdir("reauth-queued-chatgpt");
    let lease = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let credential_ready = Arc::new(AtomicBool::new(false));
    let attempts = Arc::new(AtomicUsize::new(0));
    let provider_calls = Arc::new(Mutex::new(HashMap::new()));
    let first_run_release = Arc::new(Notify::new());
    let exec = Arc::new(ReauthCoreExec {
        wal_store: wal.clone(),
        credential_ready: credential_ready.clone(),
        attempts,
        provider_calls: provider_calls.clone(),
        first_pause_release: first_run_release.clone(),
        first_attempt_pauses: false,
    });
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
    let registry = Arc::new(LineageRegistry::new(Arc::new(
        DefaultOpener::new(lease, wal.clone(), exec)
            .with_idle_ttl(Duration::from_secs(5))
            .with_run_index(store),
    )));
    let session = BindIdentity::new(dir.join("proj"), "runner", "queued-reauth");

    let first = registry
        .send(chatgpt_send(session.clone(), "run-before-reauth", "first"))
        .await
        .unwrap();
    assert!(matches!(first, SendResponse::Active { .. }));

    let queued_request = chatgpt_send(session.clone(), "run-queued-reauth", "queued");
    let queued = registry.send(queued_request.clone()).await.unwrap();
    assert!(matches!(queued, SendResponse::Pending { .. }));
    first_run_release.notify_one();

    // This waits for an event, not a speed: the queued run starts only after the first run's
    // records are fsynced on a real file WAL, and one fsync on a busy CI disk has taken over a
    // second. The bound is a hang guard, so a run that never pauses still fails by name.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let paused = |records: &[FramedRecord]| {
        records.iter().any(|frame| {
            matches!(
                &frame.record,
                WalRecord::RunPaused { run_id, .. } if run_id.0 == "run-queued-reauth"
            )
        })
    };
    let records = loop {
        let records = wal.read_records(&session).unwrap();
        if paused(&records) || std::time::Instant::now() >= deadline {
            break records;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(
        paused(&records),
        "queued run never paused for reauth; records: {:?}",
        records
            .iter()
            .map(|frame| {
                let debug = format!("{:?}", frame.record);
                debug
                    .split([' ', '(', '{'])
                    .next()
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect::<Vec<_>>()
    );
    assert!(!records.iter().any(|frame| {
        matches!(
            &frame.record,
            WalRecord::Retracted {
                failure: Some(failure),
                ..
            } if failure.run_id.0 == "run-queued-reauth"
        )
    }));
    assert_eq!(
        provider_calls
            .lock()
            .unwrap()
            .get("run-queued-reauth")
            .copied()
            .unwrap_or(0),
        0,
        "the queued run pauses before any provider call"
    );
    broca_test_support::wait_for(
        "queued_chatgpt_needs_reauth_pauses_instead_of_being_tombstoned",
        async {
            loop {
                if matches!(
                    registry
                        .run_status(&session, RunId("run-queued-reauth".into()))
                        .await
                        .unwrap(),
                    RunStatusResponse::Paused { .. }
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
        || {
            wal.read_records(&session)
                .map(|frames| broca_test_support::record_kinds(frames.iter().map(|f| &f.record)))
                .unwrap_or_else(|e| format!("WAL read: {e}"))
        },
    )
    .await;

    credential_ready.store(true, Ordering::SeqCst);
    let resumed = registry.send(queued_request).await.unwrap();
    assert!(matches!(
        resumed,
        SendResponse::Active { run_id, .. } if run_id == "run-queued-reauth"
    ));
    broca_test_support::wait_for(
        "queued_chatgpt_needs_reauth_pauses_instead_of_being_tombstoned",
        async {
            loop {
                if provider_calls
                    .lock()
                    .unwrap()
                    .get("run-queued-reauth")
                    .copied()
                    == Some(1)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
        || {
            wal.read_records(&session)
                .map(|frames| broca_test_support::record_kinds(frames.iter().map(|f| &f.record)))
                .unwrap_or_else(|e| format!("WAL read: {e}"))
        },
    )
    .await;
    assert_eq!(
        provider_calls
            .lock()
            .unwrap()
            .get("run-queued-reauth")
            .copied(),
        Some(1)
    );
    let records = wal.read_records(&session).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|frame| matches!(frame.record, WalRecord::RunStarted { .. }))
            .count(),
        2,
        "resume continues the queued run instead of opening a third episode"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// session.subscribe opens a StreamData* → StreamEnd stream; dropping the client mid-stream
/// (egress-closed) tears the subscriber down cleanly (the serve loop ends, no hang/panic).
#[tokio::test]
async fn subscribe_streams_then_egress_close_tears_down_cleanly() {
    let dir = tmpdir("sub");
    let reg = registry(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve = tokio::spawn(async move {
        serve_connection(
            sreader,
            swriter,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await
    });

    // HELLO/ACK.
    let hello = read_one(&mut creader).await;
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
    write_frame(&mut cwriter, &ack).await.unwrap();

    // bind + send a run (so there's a RunStarted + terminal to stream).
    let session = BindIdentity::new(dir.join("proj"), "runner", "s1");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 3,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            session.project_root.clone(),
            session.harness.clone(),
            session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 1, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await; // bind ack

    // Bind a SECOND, dedicated route channel (4) for the subscribe stream — subscribe holds
    // its route's credit until StreamEnd, so it must not share the ops channel.
    let bind4 = ModuleControlRequest::RouteBind {
        route_channel: 4,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            session.project_root.clone(),
            session.harness.clone(),
            session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 5, serde_json::to_value(&bind4).unwrap()).await;
    let _ = read_one(&mut creader).await; // bind4 ack

    write_req(
        &mut cwriter,
        3,
        2,
        serde_json::json!({ "method": "session.send", "params": { "prompt": "hi", "model": { "provider": "fake", "model": "fake" } } }),
    )
    .await;
    let _ = read_one(&mut creader).await; // send response

    // subscribe from start on the DEDICATED route channel (4), corr 99.
    write_req(
        &mut cwriter,
        4,
        99,
        serde_json::json!({ "method": "session.subscribe", "params": { "from": "start" } }),
    )
    .await;

    // Read at least one StreamData frame on (channel=4, corr=99). A session subscription is
    // LINEAGE-scoped: it does NOT emit StreamEnd when the run finishes — it stays open
    // across turns. So we expect StreamData (the RunStarted etc. control units) and the
    // stream stays live (no StreamEnd until unsubscribe/teardown).
    let mut saw_stream_data = false;
    for _ in 0..20 {
        let f = match tokio::time::timeout(Duration::from_secs(2), read_frame(&mut creader)).await {
            Ok(Ok(Some(f))) => f,
            _ => break,
        };
        if f.header.channel == 4 && f.header.corr == 99 && f.header.ty == FrameType::StreamData {
            saw_stream_data = true;
            break;
        }
    }
    assert!(
        saw_stream_data,
        "the subscribe stream emitted at least one StreamData frame (the run's control units)"
    );

    // EGRESS CLOSE: drop the client entirely while the lineage-scoped subscription is still
    // LIVE (its forward task is parked waiting for the next turn's events). The connection
    // teardown must cancel that parked forward task so it drops its egress sender, the
    // writer channel closes, and `serve_connection` ends — WITHOUT hanging. (Before the
    // cancel-on-teardown fix, the parked forward task held the egress open forever.)
    drop(cwriter);
    drop(creader);
    let result = tokio::time::timeout(Duration::from_secs(3), serve).await;
    assert!(
        result.is_ok(),
        "the serve loop must end cleanly after egress close even with a LIVE subscription \
         (the parked forward task must be cancelled on teardown, not leak/hang)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A WAL decode failure during attach is a typed stream Error, never an empty clean StreamEnd.
#[tokio::test]
async fn corrupted_wal_attach_surfaces_stream_error() {
    let body = attach_after_damaging_the_wal("corrupt-sub", |wal_path| {
        let mut bytes = std::fs::read(wal_path).expect("the admitted run created its WAL");
        assert!(!bytes.is_empty());
        bytes[0] ^= 0xff;
        std::fs::write(wal_path, bytes).expect("corrupt the durable WAL header");
    })
    .await;
    assert_eq!(body["code"], "subscribe_replay_failed");
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("WAL replay failed"));
}

/// The control lane refuses a WAL a newer build appended to with a typed stream Error that
/// names NewerWriter, not corruption, and never a clean StreamEnd.
#[tokio::test]
async fn newer_writer_wal_attach_surfaces_a_named_stream_error() {
    let body = attach_after_damaging_the_wal("newer-writer-sub", |wal_path| {
        let mut bytes = std::fs::read(wal_path).expect("the admitted run created its WAL");
        let next = broca_wal::replay_file(wal_path, broca_wal::DEFAULT_MAX_READ_BYTES)
            .unwrap()
            .len() as u64
            + 1;
        bytes.extend(
            broca_wal::framing::encode_with_requires(
                next,
                1,
                &["from-the-future/v1"],
                &broca_wal::WalRecord::StepStarted {
                    step_id: broca_protocol::StepId(next),
                },
            )
            .unwrap(),
        );
        // Replace the file rather than append to it, so a writer still finishing the run
        // keeps writing to the old inode and cannot interleave with this frame.
        let staged = wal_path.with_extension("staged");
        std::fs::write(&staged, bytes).unwrap();
        std::fs::rename(&staged, wal_path).unwrap();
    })
    .await;
    assert_eq!(body["code"], "subscribe_replay_failed");
    let message = body["message"].as_str().unwrap();
    assert!(message.contains("NewerWriter"), "{message}");
    assert!(message.contains("from-the-future/v1"), "{message}");
    assert!(!message.contains("corruption"), "{message}");
}

/// The control lane reads a session whose `RunStarted` froze a scope (a gated frame requiring
/// `scope/v1`): attaching from the start streams the session's control units instead of
/// refusing the WAL.
#[tokio::test]
async fn scoped_wal_attach_streams_control_units() {
    let first = attach_after_rewriting_the_wal("scoped-sub", |wal_path| {
        let frames = broca_wal::replay_file(wal_path, broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
        let mut bytes = Vec::new();
        let mut gated = 0;
        for mut framed in frames {
            if let broca_wal::WalRecord::RunStarted { scope, .. } = &mut framed.record {
                *scope = Some(broca_protocol::SessionScope {
                    owner: broca_protocol::BindPrincipal::Reserved {
                        module_id: "prefrontal-core".into(),
                    },
                    scope_ref: "head-1".into(),
                    scope_epoch: 3,
                    flow_id: None,
                    sender: None,
                    agent_id: None,
                    run_id: None,
                });
                gated += 1;
            }
            bytes.extend(
                broca_wal::framing::encode(framed.seq, framed.fence, &framed.record).unwrap(),
            );
        }
        assert_eq!(gated, 1, "the admitted run wrote one RunStarted");
        let staged = wal_path.with_extension("staged");
        std::fs::write(&staged, bytes).unwrap();
        std::fs::rename(&staged, wal_path).unwrap();
    })
    .await;
    assert_eq!(
        first.header.ty,
        FrameType::StreamData,
        "a scoped WAL must stream, not refuse: {}",
        String::from_utf8_lossy(&first.body)
    );
}

/// The control lane reads a session whose `RunStarted` froze a plan manifest (a gated frame
/// requiring `plan-manifest/v1`): attaching from the start streams the session's control units
/// instead of refusing the WAL.
#[tokio::test]
async fn manifest_wal_attach_streams_control_units() {
    let fixture: broca_wal::WalRecord = serde_json::from_str(
        &include_str!("../../../broca-wal/tests/fixtures/plan_manifest_run_started.json")
            .trim_end()
            .replace("{RUN_ID}", "fixture"),
    )
    .unwrap();
    let broca_wal::WalRecord::RunStarted {
        manifest: frozen, ..
    } = fixture
    else {
        unreachable!()
    };
    let first = attach_after_rewriting_the_wal("manifest-sub", |wal_path| {
        let frames = broca_wal::replay_file(wal_path, broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
        let mut bytes = Vec::new();
        let mut gated = 0;
        for mut framed in frames {
            if let broca_wal::WalRecord::RunStarted { manifest, .. } = &mut framed.record {
                *manifest = frozen.clone();
                gated += 1;
            }
            bytes.extend(
                broca_wal::framing::encode(framed.seq, framed.fence, &framed.record).unwrap(),
            );
        }
        assert_eq!(gated, 1, "the admitted run wrote one RunStarted");
        // The admitted run's input is tagged, so the frame requires runner tags as well.
        let needle =
            br#"{"requires":["plan-manifest/v1","runner-tags/v1"],"record":{"type":"run_started""#;
        assert!(bytes.windows(needle.len()).any(|window| window == needle));
        let staged = wal_path.with_extension("staged");
        std::fs::write(&staged, bytes).unwrap();
        std::fs::rename(&staged, wal_path).unwrap();
    })
    .await;
    assert_eq!(
        first.header.ty,
        FrameType::StreamData,
        "a manifest WAL must stream, not refuse: {}",
        String::from_utf8_lossy(&first.body)
    );
}

/// The control lane reads a session holding a tool dispatch intent that names its module (a
/// gated frame requiring `dispatch-module/v1`): attaching from the start streams the session's
/// control units instead of refusing the WAL.
#[tokio::test]
async fn dispatch_module_wal_attach_streams_control_units() {
    let first = attach_after_rewriting_the_wal("dispatch-module-sub", |wal_path| {
        let frames = broca_wal::replay_file(wal_path, broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
        let last = frames.last().expect("the admitted run wrote frames");
        let (seq, fence) = (last.seq + 1, last.fence);
        let intent: broca_wal::WalRecord = serde_json::from_str(
            r#"{"type":"tool_dispatch_intent","batch_id":"b-x","tool_call_id":"t-x","tool_name":"lookup","args":{},"module_id":"aft"}"#,
        )
        .unwrap();
        let mut bytes = std::fs::read(wal_path).unwrap();
        bytes.extend(broca_wal::framing::encode(seq, fence, &intent).unwrap());
        let needle =
            br#"{"requires":["dispatch-module/v1"],"record":{"type":"tool_dispatch_intent""#;
        assert!(bytes.windows(needle.len()).any(|window| window == needle));
        let staged = wal_path.with_extension("staged");
        std::fs::write(&staged, bytes).unwrap();
        std::fs::rename(&staged, wal_path).unwrap();
    })
    .await;
    assert_eq!(
        first.header.ty,
        FrameType::StreamData,
        "a dispatch-module WAL must stream, not refuse: {}",
        String::from_utf8_lossy(&first.body)
    );
}

/// Admit a run, let `damage` rewrite its WAL, then attach from the start and return the body
/// of the terminal error frame the attach must end with.
async fn attach_after_damaging_the_wal(
    tag: &str,
    damage: impl FnOnce(&std::path::Path),
) -> serde_json::Value {
    let terminal = attach_after_rewriting_the_wal(tag, damage).await;
    assert_eq!(
        terminal.header.ty,
        FrameType::Error,
        "corrupted replay must not be misreported as a clean StreamEnd"
    );
    serde_json::from_slice(&terminal.body).unwrap()
}

/// Admit a run, let `rewrite` replace its WAL, then attach from the start and return the first
/// frame the attach answers with.
async fn attach_after_rewriting_the_wal(
    tag: &str,
    rewrite: impl FnOnce(&std::path::Path),
) -> Frame {
    let dir = tmpdir(tag);
    let (reg, wal) = registry_and_wal(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve = tokio::spawn(async move {
        serve_connection(
            sreader,
            swriter,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await
    });

    let hello = read_one(&mut creader).await;
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
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new(dir.join("proj"), "runner", "s1");
    for (route_channel, corr) in [(3, 1), (4, 2)] {
        let bind = ModuleControlRequest::RouteBind {
            route_channel,
            epoch: 1,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new(
                session.project_root.clone(),
                session.harness.clone(),
                session.session.clone(),
            ),
            principal: None,
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        };
        write_req(&mut cwriter, 0, corr, serde_json::to_value(&bind).unwrap()).await;
        assert_eq!(read_one(&mut creader).await.header.ty, FrameType::Response);
    }

    write_req(
        &mut cwriter,
        3,
        3,
        serde_json::json!({ "method": "session.send", "params": { "prompt": "hi", "model": { "provider": "fake", "model": "fake" } } }),
    )
    .await;
    assert_eq!(read_one(&mut creader).await.header.ty, FrameType::Response);

    rewrite(&wal.wal_path(&session));

    write_req(
        &mut cwriter,
        4,
        99,
        serde_json::json!({ "method": "session.subscribe", "params": { "from": "start" } }),
    )
    .await;

    let terminal = tokio::time::timeout(Duration::from_secs(2), read_one(&mut creader))
        .await
        .expect("corrupted-WAL attach must terminate promptly");
    assert_eq!(terminal.header.channel, 4);
    assert_eq!(terminal.header.corr, 99);

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
    terminal
}

/// The production seal hangs off `serve_connection` RETURNING when the daemon's side of the
/// connection closes (main.rs: `result = &mut serving => { registry.stop_and_seal() ... }`).
/// Every test in this file exercises that property by dropping the client halves at the end
/// and awaiting the serve task -- and a serve loop that RECONNECTED instead of returning would
/// make all of them hang, which reads as a slow runner rather than a regression. Covered
/// everywhere, failure mode named nowhere. This arm is bounded and names the consequence.
///
/// Measured why it matters: the 2026-09-18 daemon cut sent no GOODBYE (the daemon has no
/// signal handler), so EOF was the ONLY trigger the seal had, and one run in flight was sealed
/// correctly because serve returned. A refactor that made serve reconnect on EOF would leave
/// the seal's own tests green (they call stop_and_seal directly) while the production seal
/// never fired.
#[tokio::test]
async fn serve_returns_on_daemon_eof_so_the_seal_can_run() {
    let dir = tmpdir("serve-eof");
    let (registry, _wal) = registry_and_wal(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await
    });

    // Bring the connection fully up first, so EOF lands on a SERVING loop and not on a
    // handshake that would have refused anyway.
    let hello = read_one(&mut creader).await;
    let hello_ack = Frame::build(
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
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    // The daemon dies: both halves of its side go away. No GOODBYE, no route.closing.
    drop(cwriter);
    drop(creader);

    let returned = tokio::time::timeout(Duration::from_secs(5), serve).await;
    assert!(
        returned.is_ok(),
        "serve_connection must RETURN when the daemon's connection closes; it did not within 5s. \
         If it reconnects or waits instead, the production seal (main.rs, the `serving` arm) \
         never runs and every in-flight run is left to WAL recovery on the next open."
    );
    let end = returned
        .unwrap()
        .expect("serve task panicked")
        .expect("a clean EOF ends serve Ok");
    assert_eq!(
        end,
        ServeEnd::Eof,
        "a close with no GOODBYE first must be reported as EOF on the seal line"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A channel-0 GOODBYE is the daemon's planned stop, and serve must return on it promptly and
/// say so: the seal line in main.rs appends `(cause: goodbye)` from this value, which is how a
/// placement tells a planned stop from the connection merely ending. The client halves stay
/// open until serve has returned, so EOF cannot be what ended it.
#[tokio::test]
async fn serve_returns_on_channel0_goodbye_with_cause_goodbye() {
    let dir = tmpdir("serve-goodbye");
    let (registry, _wal) = registry_and_wal(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await
    });

    let hello = read_one(&mut creader).await;
    let hello_ack = Frame::build(
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
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    let goodbye = Frame::build(
        FrameType::Goodbye,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        9,
        Vec::new(),
    )
    .unwrap();
    write_frame(&mut cwriter, &goodbye).await.unwrap();

    let end = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve_connection must return promptly on a channel-0 GOODBYE")
        .expect("serve task panicked")
        .expect("a GOODBYE ends serve Ok");
    assert_eq!(
        end,
        ServeEnd::Goodbye,
        "a channel-0 GOODBYE must be reported as the cause, not EOF"
    );
    drop(cwriter);
    drop(creader);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A daemon cut RESETS the socket rather than closing it, and that has to end serve with `Ok`
/// too, or main exits 1 after a clean seal and the supervisor records a crash.
///
/// Measured on the 2026-09-24 16:25Z cut: broca logged "shutdown seal complete on serve
/// return", then main returned `ServeIoError("frame I/O error: Connection reset by peer (os
/// error 54)")`, exited 1, and `ck module` recorded exit_kind=Crash for a clean stop. The EOF
/// test above cannot see this: a dropped in-memory duplex reads as a clean EOF, never a reset.
/// So this arm uses a REAL TCP pair and closes the daemon's side with SO_LINGER 0, which makes
/// the kernel send RST -- the same thing the cut produced.
#[tokio::test]
async fn serve_ends_ok_when_the_daemon_resets_the_socket() {
    let dir = tmpdir("serve-reset");
    let (registry, _wal) = registry_and_wal(&dir);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let module_side = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (daemon_side, _) = listener.accept().await.unwrap();

    let (sreader, swriter) = tokio::io::split(module_side);
    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await
    });

    let (mut creader, mut cwriter) = tokio::io::split(daemon_side);
    let hello = read_one(&mut creader).await;
    let hello_ack = Frame::build(
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
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    // WAIT FOR THE SERVING LOOP BEFORE RESETTING. A reset discards data the receiver hasn't
    // read yet, and on Windows that includes the HELLO_ACK just written: the first version of
    // this test reset straight after it, so on Windows CI serve failed inside the handshake
    // (os error 10054) instead of in the serving loop this test is about. A channel-0 Ping is
    // answered only by the serving loop, so reading its Pong proves serve consumed HELLO_ACK
    // and is serving, with nothing left unread for the reset to throw away.
    let ping = Frame::build(
        FrameType::Ping,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        7,
        Vec::new(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ping).await.unwrap();
    let pong = read_one(&mut creader).await;
    assert_eq!(
        pong.header.ty,
        FrameType::Pong,
        "the serving loop answers a channel-0 Ping"
    );

    // The daemon dies with the socket still open: linger 0 turns the close into a reset.
    let daemon_side = creader.unsplit(cwriter);
    daemon_side.set_zero_linger().unwrap();
    drop(daemon_side);

    let returned = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve_connection must return when the daemon resets the socket")
        .expect("serve task panicked");
    assert!(
        returned.is_ok(),
        "a reset from the daemon is the daemon going away, not a fault: serve must end Ok so \
         main exits 0 after the seal. Got {returned:?}"
    );
    assert_eq!(
        returned.unwrap(),
        ServeEnd::PeerReset,
        "a reset must be reported as its own cause on the seal line, not as EOF"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn project_id_some_then_none_preserves_wal_address_and_lineage() {
    project_observation_wire(false).await;
}

#[tokio::test]
async fn project_id_import_is_carried_into_run_started() {
    project_observation_wire(true).await;
}

async fn project_observation_wire(import: bool) {
    let dir = tmpdir(if import {
        "project-import"
    } else {
        "project-observation"
    });
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
    let exec = Arc::new(CoreExec {
        wal_store: wal.clone(),
        store: Some(store.clone()),
        reply_text: "answer".into(),
    });
    let opener = Arc::new(
        DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal.clone(),
            exec,
        )
        .with_run_index(store.clone())
        .with_materialized_store(store.clone()),
    );
    let reg = Arc::new(LineageRegistry::new_with_reader(
        opener,
        wal.clone(),
        Some(store),
    ));
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut reader, mut writer) = tokio::io::split(client);
    let (sr, sw) = tokio::io::split(server);
    let serve = tokio::spawn(async move {
        serve_connection(
            sr,
            sw,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await
    });
    let hello = read_one(&mut reader).await;
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
    let session = BindIdentity::new(dir.join("proj"), "runner", "observed");
    let address = broca_core::session_addr(&session);
    let path = wal.wal_path(&session);
    let mut lineage_id = None;
    let mut previous_bytes = Vec::new();
    for (index, project_id) in [Some("pj-wire"), None].into_iter().enumerate() {
        let mut identity =
            SubcBindIdentity::new(session.project_root.clone(), "runner", "observed");
        identity.project_id = project_id.map(str::to_owned);
        write_req(
            &mut writer,
            0,
            10,
            serde_json::to_value(ModuleControlRequest::RouteBind {
                route_channel: 7,
                epoch: 1,
                target: RouteTarget::ManagementSurface {
                    module_id: "broca".into(),
                },
                identity,
                principal: None,
                consumer_capabilities: None,
                admission_facts: None,
                scope: None,
                role_versions: None,
                call_mode: None,
            })
            .unwrap(),
        )
        .await;
        assert_eq!(read_one(&mut reader).await.header.ty, FrameType::Response);
        let request = if !import || index > 0 {
            serde_json::json!({"method":"session.send", "params": {
                "prompt":"hi", "send_id":format!("turn-{index}"),
                "model":{"provider":"fake","model":"fake"}
            }})
        } else {
            serde_json::json!({"method":"session.import", "params": {
                "import_id":"import-observed", "origin":{"kind":"import","source_ref":"fixture:observed"},
                "messages":[{"role":"user","content":[{"type":"text","text":"imported"}]}]
            }})
        };
        // A send that arrives while the previous run is still releasing the actor is queued and
        // answers `pending` with no run id, even though run.status already reports that run
        // completed. Retrying the same send_id writes nothing and answers with the run id once
        // the queued send has started, so retry until it does.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut corr = 11;
        let response = loop {
            write_req(&mut writer, 7, corr, request.clone()).await;
            let response = read_one(&mut reader).await;
            assert_eq!(
                response.header.ty,
                FrameType::Response,
                "{}",
                String::from_utf8_lossy(&response.body)
            );
            let response: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
            if response["result"]["state"] != "pending" {
                break response;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "send turn-{index} stayed queued: {response}"
            );
            corr += 1;
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let run_id = response["result"]["run_id"]
            .as_str()
            .unwrap_or_else(|| panic!("send reply has no run id: {response}"));
        if !import || index > 0 {
            wait_for_completed_run(&mut reader, &mut writer, 7, run_id, 100).await;
        }
        let range =
            broca_wal::read_records_range(&path, broca_wal::DEFAULT_MAX_READ_BYTES, 0, None, None)
                .unwrap();
        assert!(
            range.lineage_id.is_some(),
            "the original triple must address the WAL"
        );
        if index == 0 {
            lineage_id = range.lineage_id;
        }
        assert_eq!(range.lineage_id, lineage_id);
        let broca_wal::WalRangeRecords::Complete(frames) = range.records() else {
            panic!("expected a complete live WAL");
        };
        let episodes: Vec<_> = frames
            .iter()
            .filter_map(|frame| match &frame.record {
                WalRecord::RunStarted {
                    project_id,
                    session: recorded,
                    ..
                } => {
                    assert_eq!(recorded, &session);
                    Some(project_id.as_deref())
                }
                _ => None,
            })
            .collect();
        assert_eq!(episodes, [Some("pj-wire"), None][..=index]);
        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.starts_with(&previous_bytes));
        previous_bytes = bytes;
        assert_eq!(path.file_stem().unwrap().to_str().unwrap(), address);
        assert_eq!(
            std::fs::read_dir(dir.join("wal"))
                .unwrap()
                .filter_map(Result::ok)
                .filter(|e| e.path().extension().is_some_and(|e| e == "wal"))
                .count(),
            1
        );
        write_req(
            &mut writer,
            7,
            200,
            serde_json::json!({"method":"session.read", "params":{}}),
        )
        .await;
        let page = read_one(&mut reader).await;
        assert_eq!(page.header.ty, FrameType::Response);
        let page: serde_json::Value = serde_json::from_slice(&page.body).unwrap();
        assert_eq!(
            page["result"]["lineage_id"],
            lineage_id.unwrap().to_string()
        );
        assert!(!page.to_string().contains("project_id"));
    }
    drop(writer);
    drop(reader);
    tokio::time::timeout(Duration::from_secs(2), serve)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // Detached actor and projector tasks may still hold store handles inside `dir` for a
    // moment after serve returns; Windows refuses to remove a tree with an open file in it
    // where unix unlinks silently. Every other test in this file ignores the result for the
    // same reason. Nothing under test lives past this line.
    let _ = std::fs::remove_dir_all(dir);
}

// ── The bind principal: recorded on RunStarted and Queued through the real serve path ──

/// A stub provider whose step can be held, so a run stays genuinely active (admitted, its
/// `RunStarted` durable) while the test queues other sends behind it.
struct GatedProvider {
    stub: StubProvider,
    gate: Option<Arc<Notify>>,
}

#[async_trait]
impl Provider for GatedProvider {
    async fn step(
        &self,
        step_id: broca_protocol::StepId,
        opts: &broca_protocol::request::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        if let Some(gate) = &self.gate {
            gate.notified().await;
        }
        self.stub.step(step_id, opts).await
    }
}

/// The real core loop, so `RunStarted` is written by core from the request the serve layer
/// built, never by the test. Only the first run it executes waits on `gate`.
struct GatedCoreExec {
    wal_store: Arc<FileWalStore>,
    store: Arc<broca_store::SqliteStore>,
    gate: Arc<Notify>,
    runs: AtomicUsize,
}

#[async_trait]
impl RunExecutor for GatedCoreExec {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let mode = match job.kind {
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
            RunKind::FreshOrResume => StartMode::FreshOrResume,
        };
        let first = self.runs.fetch_add(1, Ordering::SeqCst) == 0;
        let provider = GatedProvider {
            stub: StubProvider::new(vec![one_step_text_reply("answer")]),
            gate: first.then(|| self.gate.clone()),
        };
        let plane = StubToolPlane::new();
        let materialized_store =
            job.materialized_store(self.store.clone() as Arc<dyn broca_core::SessionStore>);
        run_loop_on_writer_admitted(
            job.req,
            &job.writer,
            mode,
            &provider,
            &plane,
            &AllowAll,
            self.wal_store.as_ref(),
            materialized_store,
            job.control,
            &out,
            job.on_admitted,
        )
        .await
    }
}

/// One serve connection over a real registry and core loop, with helpers to bind channels
/// under a principal and to read what reached the WAL.
struct PrincipalRig {
    dir: std::path::PathBuf,
    reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    serve: tokio::task::JoinHandle<Result<ServeEnd, broca_module_serve::serve::ServeIoError>>,
    session: BindIdentity,
    wal_path: std::path::PathBuf,
    gate: Arc<Notify>,
    corr: u64,
}

fn prefrontal_core() -> subc_protocol::Principal {
    subc_protocol::Principal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

fn other_agent() -> subc_protocol::Principal {
    subc_protocol::Principal::Reserved {
        module_id: "other-agent".into(),
    }
}

impl PrincipalRig {
    async fn start(tag: &str) -> Self {
        let dir = tmpdir(tag);
        let wal = Arc::new(FileWalStore::new(dir.join("wal")));
        let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
        let gate = Arc::new(Notify::new());
        let exec = Arc::new(GatedCoreExec {
            wal_store: wal.clone(),
            store: store.clone(),
            gate: gate.clone(),
            runs: AtomicUsize::new(0),
        });
        let opener = Arc::new(
            DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                wal.clone(),
                exec,
            )
            .with_run_index(store.clone())
            .with_materialized_store(store.clone()),
        );
        let reg = Arc::new(LineageRegistry::new_with_reader(
            opener,
            wal.clone(),
            Some(store),
        ));
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (mut reader, mut writer) = tokio::io::split(client);
        let (sr, sw) = tokio::io::split(server);
        let serve = tokio::spawn(async move {
            serve_connection(
                sr,
                sw,
                "broca",
                reg.clone(),
                reg,
                broca_module_serve::serve::VaultGauge::not_in_use(),
            )
            .await
        });
        let hello = read_one(&mut reader).await;
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
        let session = BindIdentity::new(dir.join("proj"), "runner", "bound");
        let wal_path = wal.wal_path(&session);
        Self {
            dir,
            reader,
            writer,
            serve,
            session,
            wal_path,
            gate,
            corr: 1,
        }
    }

    fn next_corr(&mut self) -> u64 {
        self.corr += 1;
        self.corr
    }

    /// Bind `channel` to the rig's session the way the daemon relays it, stamped with
    /// `principal` (or with none, as a daemon that predates principal stamping sends).
    async fn bind(&mut self, channel: u16, principal: Option<subc_protocol::Principal>) {
        let corr = self.next_corr();
        write_req(
            &mut self.writer,
            0,
            corr,
            serde_json::to_value(ModuleControlRequest::RouteBind {
                route_channel: channel,
                epoch: 1,
                target: RouteTarget::ManagementSurface {
                    module_id: "broca".into(),
                },
                identity: SubcBindIdentity::new(
                    self.session.project_root.clone(),
                    "runner",
                    "bound",
                ),
                principal,
                consumer_capabilities: None,
                admission_facts: None,
                scope: None,
                role_versions: None,
                call_mode: None,
            })
            .unwrap(),
        )
        .await;
        let ack = read_one(&mut self.reader).await;
        assert_eq!(
            ack.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&ack.body)
        );
    }

    /// `session.send` on `channel`; returns the response's `result`.
    async fn send(&mut self, channel: u16, send_id: &str) -> serde_json::Value {
        let corr = self.next_corr();
        write_req(
            &mut self.writer,
            channel,
            corr,
            serde_json::json!({"method":"session.send", "params": {
                "prompt":"hi", "send_id":send_id,
                "model":{"provider":"fake","model":"fake"}
            }}),
        )
        .await;
        let response = read_one(&mut self.reader).await;
        assert_eq!(
            response.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        body["result"].clone()
    }

    fn records(&self) -> Vec<WalRecord> {
        let range = broca_wal::read_records_range(
            &self.wal_path,
            broca_wal::DEFAULT_MAX_READ_BYTES,
            0,
            None,
            None,
        )
        .unwrap();
        let broca_wal::WalRangeRecords::Complete(frames) = range.records() else {
            panic!("expected a complete live WAL");
        };
        frames.iter().map(|frame| frame.record.clone()).collect()
    }

    /// Wait until `n` episodes have a durable terminal. Read from the WAL, so a queued send
    /// is waited for without asking the wire about a run that may not have started yet.
    async fn wait_for_finished(&self, n: usize) -> Vec<WalRecord> {
        broca_test_support::wait_for(
            "wait_for_finished",
            async {
                loop {
                    let records = self.records();
                    let finished = records
                        .iter()
                        .filter(|record| matches!(record, WalRecord::RunFinished { .. }))
                        .count();
                    if finished >= n {
                        return records;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
            || broca_test_support::record_kinds(self.records()),
        )
        .await
    }

    async fn finish(self) {
        let Self {
            dir,
            reader,
            writer,
            serve,
            ..
        } = self;
        drop(writer);
        drop(reader);
        tokio::time::timeout(Duration::from_secs(2), serve)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        // Detached actor tasks may keep store handles open for a moment after serve returns,
        // and Windows refuses to remove a directory with an open file, so ignore the result.
        let _ = std::fs::remove_dir_all(dir);
    }
}

fn run_started_principals(records: &[WalRecord]) -> Vec<Option<String>> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::RunStarted { principal, .. } => {
                Some(principal.as_ref().map(|p| p.as_owner_string()))
            }
            _ => None,
        })
        .collect()
}

fn queued_principals(records: &[WalRecord]) -> Vec<Option<String>> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::Queued { principal, .. } => {
                Some(principal.as_ref().map(|p| p.as_owner_string()))
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_send_over_a_reserved_bind_records_that_principal_on_run_started() {
    let mut rig = PrincipalRig::start("principal-reserved").await;
    rig.bind(7, Some(prefrontal_core())).await;
    rig.gate.notify_one();
    let result = rig.send(7, "turn-1").await;
    assert_eq!(result["state"], "active", "{result}");
    let records = rig.wait_for_finished(1).await;
    assert_eq!(
        run_started_principals(&records),
        [Some("reserved:prefrontal-core".to_owned())]
    );
    // The WAL stores the principal as the daemon's structured value; the
    // `reserved:prefrontal-core` string compared above is only its comparison form.
    let bytes = std::fs::read(&rig.wal_path).unwrap();
    let needle = br#""principal":{"kind":"reserved","module_id":"prefrontal-core"}"#;
    assert!(
        bytes.windows(needle.len()).any(|window| window == needle),
        "the WAL must carry the stamped principal verbatim"
    );
    rig.finish().await;
}

/// A bind that carried no principal must record none: no member at all, never a default or an
/// empty value. Absence is what an older daemon sends, and it must never read as a principal.
#[tokio::test]
async fn a_bind_without_a_principal_writes_no_principal_bytes() {
    let mut rig = PrincipalRig::start("principal-absent").await;
    rig.bind(7, None).await;
    rig.gate.notify_one();
    let result = rig.send(7, "turn-1").await;
    assert_eq!(result["state"], "active", "{result}");
    let records = rig.wait_for_finished(1).await;
    assert_eq!(run_started_principals(&records), [None]);
    let bytes = std::fs::read(&rig.wal_path).unwrap();
    let contains = |needle: &[u8]| bytes.windows(needle.len()).any(|window| window == needle);
    // The payloads are plain JSON in the file, so a missing member really is missing bytes.
    assert!(contains(br#""type":"run_started""#));
    assert!(
        !contains(br#""principal":"#),
        "an absent principal must cost no bytes"
    );
    rig.finish().await;
}

/// A send queued behind another caller's run records the principal that QUEUED it when it is
/// drained, not the principal of the run whose end drained it. Channel 9 starts a held run over
/// a bind with no principal; channel 7 queues, then channel 8 queues; releasing the held run
/// drains both in order.
///
/// The held run records no principal because an unscoped session accepts later sends only from
/// the principal of its first send, when that first send recorded one. A first episode without
/// a principal (a pre-attestation daemon's, as here) leaves the session open to any principal,
/// which is what lets two different principals queue into it.
#[tokio::test]
async fn a_drained_queued_send_records_the_principal_that_queued_it() {
    let mut rig = PrincipalRig::start("principal-queued").await;
    rig.bind(9, None).await;
    rig.bind(8, Some(other_agent())).await;
    rig.bind(7, Some(prefrontal_core())).await;

    let held = rig.send(9, "turn-1").await;
    assert_eq!(held["state"], "active", "{held}");
    let queued = rig.send(7, "turn-2").await;
    assert_eq!(queued["state"], "pending", "{queued}");
    let queued_after = rig.send(8, "turn-3").await;
    assert_eq!(queued_after["state"], "pending", "{queued_after}");

    rig.gate.notify_one();
    let records = rig.wait_for_finished(3).await;
    assert_eq!(
        run_started_principals(&records),
        [
            None,
            Some("reserved:prefrontal-core".to_owned()),
            Some("reserved:other-agent".to_owned()),
        ],
        "each drained run must record the principal that queued it"
    );
    assert_eq!(
        queued_principals(&records),
        [
            Some("reserved:prefrontal-core".to_owned()),
            Some("reserved:other-agent".to_owned()),
        ]
    );
    // The second RunStarted consumed the submission channel 7 queued.
    let consumed: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::RunStarted { submission_id, .. } => Some(submission_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(consumed[0], None);
    assert_eq!(
        consumed[1].as_ref().map(|id| id.0.as_str()),
        queued["submission_id"].as_str()
    );
    rig.finish().await;
}

/// The principal is recorded, never part of what makes two sends the same send: the same
/// send id and payload from a second principal is the first send, not a reuse error, and it
/// starts nothing new.
///
/// The first send's bind carries no principal. An unscoped session whose first send recorded
/// a principal accepts later sends only from that principal (the owner check refuses anyone
/// else before the send key is even compared); one whose first send recorded none accepts any
/// principal, which is the case where two principals can present the same send at all.
#[tokio::test]
async fn the_same_send_from_two_principals_is_one_send() {
    let mut rig = PrincipalRig::start("principal-identity").await;
    rig.bind(7, None).await;
    rig.bind(8, Some(subc_protocol::Principal::Direct)).await;
    rig.gate.notify_one();

    let first = rig.send(7, "same-send").await;
    assert_eq!(first["state"], "active", "{first}");
    rig.wait_for_finished(1).await;
    let second = rig.send(8, "same-send").await;
    assert_eq!(second["run_id"], first["run_id"], "{second}");
    // `active` when the actor has not yet folded the durable terminal into its state; either
    // way it is the original run answering, not a refusal and not a new one.
    assert!(
        matches!(second["state"].as_str(), Some("active" | "finished")),
        "{second}"
    );

    let records = rig.wait_for_finished(1).await;
    assert_eq!(
        run_started_principals(&records),
        [None],
        "the repeat must not start a second episode"
    );
    rig.finish().await;
}

/// The pinned refusal bodies are valid subc error bodies: decoded as subc's `ErrorBody` and
/// re-encoded, they keep exactly their bytes, so the refusal can be sent as an ordinary error
/// with a typed detail.
#[test]
fn scope_adoption_refused_golden_bodies_are_subc_error_bodies() {
    let golden = Path::new(env!("CARGO_MANIFEST_DIR")).join("../broca-wire/tests/golden");
    for file in [
        "scope_adoption_refused.no_recorded_principal.json",
        "scope_adoption_refused.principal_mismatch.json",
    ] {
        let bytes = std::fs::read(golden.join(file)).unwrap();
        let body: ErrorBody = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.code, broca_wire::SCOPE_ADOPTION_REFUSED);
        assert!(body.detail.is_some());
        assert_eq!(serde_json::to_vec(&body).unwrap(), bytes, "{file}");
    }
}

/// A served route bound to one session on a fresh registry, for tests that prepare the
/// session's WAL on disk before the first request reaches it. No actor is open for the
/// session, so the first request goes through the lineage opener or the lease-free read.
struct PreparedWalRoute {
    dir: std::path::PathBuf,
    wal_path: std::path::PathBuf,
    reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    serve: tokio::task::JoinHandle<()>,
}

impl PreparedWalRoute {
    /// Seed the session's WAL with two ordinary records, let `damage` rewrite the file, then
    /// serve the registry and bind channel 7 to the session.
    async fn start(tag: &str, damage: impl FnOnce(&Path)) -> Self {
        Self::start_with(tag, damage, None).await
    }

    async fn start_with(
        tag: &str,
        damage: impl FnOnce(&Path),
        reader: Option<Arc<dyn broca_core::WalStore>>,
    ) -> Self {
        Self::start_with_authorization(tag, damage, reader, None, None).await
    }

    async fn start_with_authorization(
        tag: &str,
        damage: impl FnOnce(&Path),
        reader: Option<Arc<dyn broca_core::WalStore>>,
        first_principal: Option<broca_protocol::BindPrincipal>,
        principal: Option<subc_protocol::Principal>,
    ) -> Self {
        let dir = tmpdir(tag);
        let (mut reg, wal) = registry_and_wal(&dir);
        if let Some(reader) = reader {
            let opener = DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                wal.clone(),
                Arc::new(FakeExec {
                    wal_store: wal.clone(),
                    started: Arc::new(AtomicUsize::new(0)),
                    hold: None,
                }),
            );
            reg = Arc::new(LineageRegistry::new_with_reader(
                Arc::new(opener),
                reader,
                None,
            ));
        }
        let session = BindIdentity::new(dir.join("proj"), "runner", "s1");
        {
            let mut file = wal.open(&session).expect("create the session WAL");
            if let Some(principal) = first_principal {
                let ownership_scope = broca_protocol::SessionScope {
                    owner: broca_protocol::BindPrincipal::Reserved {
                        module_id: "prefrontal-core".into(),
                    },
                    scope_ref: "flow-1".into(),
                    scope_epoch: 1,
                    flow_id: Some("flow-1".into()),
                    sender: Some(principal.clone()),
                    agent_id: None,
                    run_id: None,
                };
                file.append(
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
                        scope: Some(ownership_scope),
                        send_id: None,
                        request_identity_digest: None,
                        project_id: None,
                        principal: Some(principal),
                        run_id: RunId("protected".into()),
                        session: session.clone(),
                        ts_ms: None,
                        config: Box::new(min_config()),
                        input: vec![].into(),
                        origin: broca_wal::RunOrigin::Fresh,
                        submission_id: None,
                    },
                    true,
                )
                .unwrap();
                file.append(
                    1,
                    &WalRecord::RunFinished {
                        interrupt_reason: None,
                        reason: RunFinishReason::Completed,
                        ts_ms: None,
                        usage: Usage::default(),
                        error: None,
                        retries_used: 0,
                    },
                    true,
                )
                .unwrap();
            }
            for seq in 1..=2 {
                file.append(
                    1,
                    &WalRecord::StepStarted {
                        step_id: broca_protocol::StepId(seq),
                    },
                    true,
                )
                .expect("append a seed record");
            }
        }
        let wal_path = wal.wal_path(&session);
        damage(&wal_path);

        let (client, server) = tokio::io::duplex(64 * 1024);
        let (mut reader, mut writer) = tokio::io::split(client);
        let (sreader, swriter) = tokio::io::split(server);
        let serve = tokio::spawn(async move {
            let _ = serve_connection(
                sreader,
                swriter,
                "broca",
                reg.clone(),
                reg,
                broca_module_serve::serve::VaultGauge::not_in_use(),
            )
            .await;
        });

        let hello = read_one(&mut reader).await;
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

        let bind_scope = principal.as_ref().map(|_| {
            let mut stamp = subc_protocol::scope::ScopeStamp::new(
                prefrontal_core(),
                "flow-1",
                1,
                subc_protocol::scope::ScopeKind::Head,
            )
            .with_owner_authorized(true);
            stamp.attributes.flow_id = Some("flow-1".into());
            stamp
        });
        let bind = ModuleControlRequest::RouteBind {
            route_channel: 7,
            epoch: 1,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new(
                session.project_root.clone(),
                session.harness.clone(),
                session.session.clone(),
            ),
            principal,
            consumer_capabilities: None,
            admission_facts: None,
            scope: bind_scope,
            role_versions: None,
            call_mode: None,
        };
        write_req(&mut writer, 0, 1, serde_json::to_value(&bind).unwrap()).await;
        assert_eq!(read_one(&mut reader).await.header.ty, FrameType::Response);

        Self {
            dir,
            wal_path,
            reader,
            writer,
            serve,
        }
    }

    /// Send one request on the bound channel and return the Error frame's raw body bytes.
    async fn expect_error(&mut self, corr: u64, request: serde_json::Value) -> Vec<u8> {
        write_req(&mut self.writer, 7, corr, request).await;
        let frame = tokio::time::timeout(Duration::from_secs(5), read_one(&mut self.reader))
            .await
            .expect("the refusal must arrive promptly");
        assert_eq!((frame.header.channel, frame.header.corr), (7, corr));
        assert_eq!(
            frame.header.ty,
            FrameType::Error,
            "expected an error reply, got {}",
            String::from_utf8_lossy(&frame.body)
        );
        frame.body
    }

    async fn finish(self) {
        drop(self.writer);
        drop(self.reader);
        let _ = tokio::time::timeout(Duration::from_secs(2), self.serve).await;
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn read_request() -> serde_json::Value {
    serde_json::json!({ "method": "session.read", "params": { "from_ordinal": 0, "limit": 10 } })
}

fn send_request() -> serde_json::Value {
    serde_json::json!({
        "method": "session.send",
        "params": { "prompt": "hi", "model": { "provider": "fake", "model": "fake" } }
    })
}

fn status_request() -> serde_json::Value {
    serde_json::json!({ "method": "run.status", "params": {} })
}

fn subscribe_request() -> serde_json::Value {
    serde_json::json!({ "method": "session.subscribe", "params": { "from": "start" } })
}

/// Damage the first frame of a WAL that has complete frames after it. That is mid-log
/// corruption, never a torn tail, so no reader or writer may repair it away.
fn corrupt_first_frame(wal_path: &Path) {
    let mut bytes = std::fs::read(wal_path).expect("the seeded WAL exists");
    bytes[0] ^= 0xff;
    std::fs::write(wal_path, bytes).expect("corrupt the WAL's first frame");
}

/// A corrupt WAL answers `session.read`, `session.send`, `run.status` and `session.subscribe`
/// with `open_failed`, byte for byte as it always has. The newer-writer refusal has its own
/// code; corruption must not drift into it, and nothing about these replies may change when
/// it is added.
#[tokio::test]
async fn a_corrupt_wal_still_answers_open_failed_with_unchanged_bytes() {
    let mut route = PreparedWalRoute::start("corrupt-open-failed", corrupt_first_frame).await;
    let before = std::fs::read(&route.wal_path).unwrap();

    // The read, and status on a session with no live actor and a small log, are lease-free
    // and read the WAL directly.
    let reader_refusal = r#"{"code":"open_failed","message":"open failed: wal range-read failed: wal corruption at offset 0: record seq=0 failed its digest — WAL corruption"}"#;
    for (corr, request) in [(2, read_request()), (4, status_request())] {
        let body = route.expect_error(corr, request.clone()).await;
        assert_eq!(
            String::from_utf8(body).unwrap(),
            reader_refusal,
            "{request}"
        );
    }

    // Every actor-backed operation goes through the lineage opener, which opens the WAL for
    // writing and refuses it the same way for each of them.
    let opener_refusal = r#"{"code":"open_failed","message":"open failed: wal write failure: wal corruption at offset 0: record seq=0 failed its digest — WAL corruption"}"#;
    for (corr, request) in [(3, send_request()), (5, subscribe_request())] {
        let body = route.expect_error(corr, request.clone()).await;
        assert_eq!(
            String::from_utf8(body).unwrap(),
            opener_refusal,
            "{request}"
        );
    }

    assert_eq!(
        std::fs::read(&route.wal_path).unwrap(),
        before,
        "a refused request must leave the corrupt WAL exactly as it was"
    );
    route.finish().await;
}

/// Append a complete gated frame that requires a WAL feature no build knows, through the real
/// encoder, as a newer build would. It follows the seeded records, so it is the WAL's tail.
fn append_newer_writer_frame(wal_path: &Path) {
    let next = broca_wal::replay_file(wal_path, broca_wal::DEFAULT_MAX_READ_BYTES)
        .expect("the seeded WAL decodes")
        .len() as u64
        + 1;
    let mut bytes = std::fs::read(wal_path).expect("the seeded WAL exists");
    bytes.extend(
        broca_wal::framing::encode_with_requires(
            next,
            1,
            &["probe-unknown/v1"],
            &WalRecord::StepStarted {
                step_id: broca_protocol::StepId(next),
            },
        )
        .unwrap(),
    );
    std::fs::write(wal_path, bytes).expect("append the gated frame");
}

/// A WAL whose tail a newer build wrote answers every served operation with
/// `wal_newer_writer`, a `newer_writer` detail naming the missing feature, and the message
/// the generic `open_failed` reply used to carry. Nothing is written: the WAL keeps its
/// exact bytes, so the gated frame is neither repaired away nor appended after.
#[tokio::test]
async fn a_newer_writer_wal_answers_wal_newer_writer_on_every_served_path() {
    let mut route = PreparedWalRoute::start("newer-writer-code", append_newer_writer_frame).await;
    let before = std::fs::read(&route.wal_path).unwrap();

    // The read, and status on a session with no live actor and a small log, are lease-free
    // and read the WAL directly.
    let reader_refusal = r#"{"code":"wal_newer_writer","message":"open failed: wal range-read failed: wal written by a newer broca (NewerWriter): the version 3 frame at offset 383 requires features this build does not know: probe-unknown/v1","detail":{"kind":"newer_writer","missing":["probe-unknown/v1"]}}"#;
    for (corr, request) in [(2, read_request()), (4, status_request())] {
        let body = route.expect_error(corr, request.clone()).await;
        assert_eq!(
            String::from_utf8(body).unwrap(),
            reader_refusal,
            "{request}"
        );
    }

    // Every actor-backed operation first opens the session, which opens its WAL for writing.
    let opener_refusal = r#"{"code":"wal_newer_writer","message":"open failed: wal write failure: wal written by a newer broca (NewerWriter): the version 3 frame at offset 383 requires features this build does not know: probe-unknown/v1","detail":{"kind":"newer_writer","missing":["probe-unknown/v1"]}}"#;
    for (corr, request) in [(3, send_request()), (5, subscribe_request())] {
        let body = route.expect_error(corr, request.clone()).await;
        assert_eq!(
            String::from_utf8(body).unwrap(),
            opener_refusal,
            "{request}"
        );
    }

    assert_eq!(
        std::fs::read(&route.wal_path).unwrap(),
        before,
        "a refused request must leave the WAL exactly as the newer build wrote it"
    );
    route.finish().await;
}

// ── The send key: recorded on the WAL record that carries the message ──

/// The real core loop, so `RunStarted` is written by core from the request the serve layer
/// built. When `crash` is set, the first run it executes holds in its provider step until
/// `crash` fires and then dies without a terminal: its `RunStarted` is durable and nothing that
/// would end it is, which is what a process killed mid-run leaves behind.
struct CrashingCoreExec {
    wal_store: Arc<FileWalStore>,
    store: Arc<broca_store::SqliteStore>,
    crash: Option<Arc<Notify>>,
    runs: AtomicUsize,
}

#[async_trait]
impl RunExecutor for CrashingCoreExec {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let mode = match job.kind {
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
            RunKind::FreshOrResume => StartMode::FreshOrResume,
        };
        let crash = self
            .crash
            .as_ref()
            .filter(|_| self.runs.fetch_add(1, Ordering::SeqCst) == 0);
        let provider = GatedProvider {
            stub: StubProvider::new(vec![one_step_text_reply("answer")]),
            // A gate nobody opens: the run that is going to crash never finishes its step.
            gate: crash.map(|_| Arc::new(Notify::new())),
        };
        let plane = StubToolPlane::new();
        let materialized_store =
            job.materialized_store(self.store.clone() as Arc<dyn broca_core::SessionStore>);
        let run = run_loop_on_writer_admitted(
            job.req,
            &job.writer,
            mode,
            &provider,
            &plane,
            &AllowAll,
            self.wal_store.as_ref(),
            materialized_store,
            job.control,
            &out,
            job.on_admitted,
        );
        match crash {
            None => run.await,
            Some(crash) => tokio::select! {
                result = run => result,
                () = crash.notified() => Err(RunError::Wal(
                    "simulated crash: the run died with its RunStarted durable and no terminal"
                        .into(),
                )),
            },
        }
    }
}

/// One process's worth of in-memory state (store handle, registry, actors, one bound
/// connection) over directories that outlive it. `crash` drops all of it and a later `start`
/// on the same directory is the restarted process: it knows only what the WAL, the lease files
/// and the store file hold.
struct SendKeyRig {
    dir: std::path::PathBuf,
    reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    serve: tokio::task::JoinHandle<Result<ServeEnd, broca_module_serve::serve::ServeIoError>>,
    registry: Arc<LineageRegistry>,
    session: BindIdentity,
    wal_path: std::path::PathBuf,
    corr: u64,
}

impl SendKeyRig {
    async fn start(dir: &Path, crash: Option<Arc<Notify>>) -> Self {
        let wal = Arc::new(FileWalStore::new(dir.join("wal")));
        let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
        let exec = Arc::new(CrashingCoreExec {
            wal_store: wal.clone(),
            store: store.clone(),
            crash,
            runs: AtomicUsize::new(0),
        });
        let opener = Arc::new(
            DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                wal.clone(),
                exec,
            )
            .with_idle_ttl(Duration::from_millis(50))
            .with_run_index(store.clone())
            .with_materialized_store(store.clone()),
        );
        let registry = Arc::new(LineageRegistry::new_with_reader(
            opener,
            wal.clone(),
            Some(store),
        ));
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (mut reader, mut writer) = tokio::io::split(client);
        let (sr, sw) = tokio::io::split(server);
        let serve_registry = registry.clone();
        let serve = tokio::spawn(async move {
            serve_connection(
                sr,
                sw,
                "broca",
                serve_registry.clone(),
                serve_registry,
                broca_module_serve::serve::VaultGauge::not_in_use(),
            )
            .await
        });
        let hello = read_one(&mut reader).await;
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
        let session = BindIdentity::new(dir.join("proj"), "runner", "keyed");
        let bind = ModuleControlRequest::RouteBind {
            route_channel: 7,
            epoch: 1,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new(session.project_root.clone(), "runner", "keyed"),
            principal: None,
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        };
        write_req(&mut writer, 0, 1, serde_json::to_value(&bind).unwrap()).await;
        assert_eq!(read_one(&mut reader).await.header.ty, FrameType::Response);
        let wal_path = wal.wal_path(&session);
        Self {
            dir: dir.to_path_buf(),
            reader,
            writer,
            serve,
            registry,
            session,
            wal_path,
            corr: 1,
        }
    }

    /// `session.send` with `send_id` and `prompt`: the reply frame's type and its body (the
    /// `result` of a response, or the whole error body).
    async fn send(&mut self, send_id: &str, prompt: &str) -> (FrameType, serde_json::Value) {
        self.corr += 1;
        write_req(
            &mut self.writer,
            7,
            self.corr,
            serde_json::json!({"method":"session.send", "params": {
                "prompt": prompt, "send_id": send_id,
                "model": {"provider": "fake", "model": "fake"}
            }}),
        )
        .await;
        let reply = read_one(&mut self.reader).await;
        let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
        match reply.header.ty {
            FrameType::Response => (FrameType::Response, body["result"].clone()),
            other => (other, body),
        }
    }

    fn records(&self) -> Vec<WalRecord> {
        let range = broca_wal::read_records_range(
            &self.wal_path,
            broca_wal::DEFAULT_MAX_READ_BYTES,
            0,
            None,
            None,
        )
        .unwrap();
        let broca_wal::WalRangeRecords::Complete(frames) = range.records() else {
            panic!("expected a complete live WAL");
        };
        frames.iter().map(|frame| frame.record.clone()).collect()
    }

    async fn wait_for_finished(&self, n: usize) -> Vec<WalRecord> {
        broca_test_support::wait_for(
            "wait_for_finished",
            async {
                loop {
                    let records = self.records();
                    let finished = records
                        .iter()
                        .filter(|record| matches!(record, WalRecord::RunFinished { .. }))
                        .count();
                    if finished >= n {
                        return records;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
            || broca_test_support::record_kinds(self.records()),
        )
        .await
    }

    /// Drop every piece of in-memory state, once no actor holds the session (so the next
    /// process can take its lease), and return the directory the restart reopens.
    async fn crash(self) -> std::path::PathBuf {
        broca_test_support::wait_for(
            "crash",
            async {
                loop {
                    if self.registry.slot_state(&self.session).is_none() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
            || {
                format!(
                    "slot={:?}; {}",
                    self.registry.slot_state(&self.session),
                    broca_test_support::record_kinds(self.records())
                )
            },
        )
        .await;
        assert_eq!(
            self.registry.slot_state(&self.session),
            None,
            "the session's actor must be gone before the process state is dropped"
        );
        let Self {
            dir,
            reader,
            writer,
            serve,
            registry,
            ..
        } = self;
        drop(writer);
        drop(reader);
        let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
        drop(registry);
        dir
    }

    async fn finish(self) {
        let dir = self.crash().await;
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// Remove one run's row from the store's run index, the way a crash between the WAL sync and
/// the index write (or a lost index) leaves it. Only called while no process has the store open.
fn delete_run_index_row(dir: &Path, run_id: &str) {
    let conn = rusqlite::Connection::open(dir.join("store.db")).unwrap();
    let deleted = conn
        .execute("DELETE FROM run_index WHERE run_id = ?1", [run_id])
        .unwrap();
    assert_eq!(
        deleted, 1,
        "the run index must have held a row for {run_id} for its loss to be tested"
    );
}

/// `(run_id, send_id, has a request identity digest)` for every `RunStarted`, in WAL order.
fn run_started_send_keys(records: &[WalRecord]) -> Vec<(String, Option<String>, bool)> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::RunStarted {
                run_id,
                send_id,
                request_identity_digest,
                ..
            } => Some((
                run_id.0.clone(),
                send_id.clone(),
                request_identity_digest.is_some(),
            )),
            _ => None,
        })
        .collect()
}

/// The run a send reply names: its run id, or for a queued send the run id its `sub-<run_id>`
/// submission id was derived from (a send can queue behind a run whose terminal is durable but
/// not yet processed by the session).
fn run_id_of(result: &serde_json::Value) -> String {
    if let Some(run_id) = result["run_id"].as_str() {
        return run_id.to_owned();
    }
    result["submission_id"]
        .as_str()
        .and_then(|submission_id| submission_id.strip_prefix("sub-"))
        .unwrap_or_else(|| panic!("a reply naming a run: {result}"))
        .to_owned()
}

/// Assert a retry was answered with the run it repeats, as finished, and nothing new.
fn assert_answers_finished(reply: &(FrameType, serde_json::Value), run_id: &str) {
    assert_eq!(reply.0, FrameType::Response, "{}", reply.1);
    assert_eq!(reply.1["state"], "finished", "{}", reply.1);
    assert_eq!(reply.1["run_id"], run_id, "{}", reply.1);
}

/// Assert a retry with a changed payload was refused, naming the field that changed.
fn assert_refused_naming(reply: &(FrameType, serde_json::Value), field: &str) {
    assert_eq!(reply.0, FrameType::Error, "{}", reply.1);
    assert_eq!(reply.1["code"], "send_id_reuse", "{}", reply.1);
    let message = reply.1["message"].as_str().unwrap_or_default();
    assert!(
        message.contains(&format!("different {field}")),
        "the refusal must name the changed field {field}: {}",
        reply.1
    );
}

/// A keyed send whose episode is no longer the latest, retried after a crash that also lost
/// its run-index row. The WAL's `RunStarted` holds the key and the request identity, so the
/// retry answers the finished run; a changed payload is refused by field. Neither writes a
/// record: one key never opens two episodes.
#[tokio::test]
async fn a_keyed_resend_after_a_crash_and_a_lost_run_index_row_answers_from_the_wal() {
    let dir = tmpdir("send-key-lost-index-row");
    let mut rig = SendKeyRig::start(&dir, None).await;
    let first = rig.send("first", "the first send").await;
    assert_eq!(first.0, FrameType::Response, "{}", first.1);
    let first_run = run_id_of(&first.1);
    rig.wait_for_finished(1).await;
    // A later episode, so the first is no longer the one the lineage would resume.
    let second = rig.send("second", "a later send").await;
    let second_run = run_id_of(&second.1);
    let before = rig.wait_for_finished(2).await;
    assert_eq!(
        run_started_send_keys(&before),
        vec![
            (first_run.clone(), Some("first".into()), true),
            (second_run, Some("second".into()), true),
        ],
        "each RunStarted records its send's key and request identity"
    );

    let dir = rig.crash().await;
    delete_run_index_row(&dir, &first_run);
    let mut rig = SendKeyRig::start(&dir, None).await;

    assert_answers_finished(&rig.send("first", "the first send").await, &first_run);
    assert_refused_naming(&rig.send("first", "a different prompt").await, "input");
    assert_eq!(
        rig.records(),
        before,
        "neither retry may write a record: the same key never opens a second episode"
    );
    rig.finish().await;
}

/// A keyed send queued behind a run that then crashed. The `Pending` reply follows the sync of
/// its `Queued` record, which carries the key. After the restart the queue is rebuilt from that
/// record alone, and the episode that drains it records the same key and identity on its
/// `RunStarted`. A later crash that also loses the drained run's index row still answers a
/// retry from the WAL: the same payload gets the finished run, a changed one is refused.
#[tokio::test]
async fn a_queued_keyed_send_drained_after_a_crash_is_deduplicated_from_the_wal() {
    let dir = tmpdir("send-key-queued-crash");
    let crash = Arc::new(Notify::new());
    let mut rig = SendKeyRig::start(&dir, Some(crash.clone())).await;
    let held = rig.send("held", "holds, then dies").await;
    assert_eq!(held.1["state"], "active", "{}", held.1);
    let held_run = run_id_of(&held.1);

    let queued = rig.send("queued", "queued behind the held run").await;
    assert_eq!(queued.0, FrameType::Response, "{}", queued.1);
    assert_eq!(queued.1["state"], "pending", "{}", queued.1);
    let submission_id = queued.1["submission_id"].as_str().unwrap().to_owned();
    let queued_run = submission_id.strip_prefix("sub-").unwrap().to_owned();
    // The reply came after the queued record was made durable, so it is already on disk, and
    // it carries the key the drain will need once memory is gone.
    let queued_digest = rig
        .records()
        .into_iter()
        .find_map(|record| match record {
            WalRecord::Queued {
                submission_id: id,
                send_id,
                request_identity_digest,
                ..
            } if id.0 == submission_id => {
                assert_eq!(send_id.as_deref(), Some("queued"));
                Some(request_identity_digest.expect("a keyed queued send records its identity"))
            }
            _ => None,
        })
        .expect("the Pending reply follows a durable Queued record");

    crash.notify_one();
    let dir = rig.crash().await;
    let mut rig = SendKeyRig::start(&dir, None).await;
    // Opening the session re-drives the crashed run and then drains the queued send; this
    // send only wakes the session and lands behind them.
    let _ = rig.send("after", "sent after the restart").await;
    let records = rig.wait_for_finished(3).await;
    let drained = records
        .iter()
        .find_map(|record| match record {
            WalRecord::RunStarted {
                run_id,
                submission_id,
                send_id,
                request_identity_digest,
                ..
            } if run_id.0 == queued_run => Some((
                submission_id.clone(),
                send_id.clone(),
                request_identity_digest.clone(),
            )),
            _ => None,
        })
        .expect("the queued send was drained after the restart");
    assert_eq!(
        drained,
        (
            Some(broca_protocol::SubmissionId(submission_id.clone())),
            Some("queued".into()),
            Some(queued_digest),
        ),
        "the drained RunStarted records the key and identity its Queued record carried"
    );
    assert_eq!(
        run_started_send_keys(&records)
            .into_iter()
            .map(|(run_id, _, _)| run_id)
            .collect::<Vec<_>>()[..2],
        [held_run, queued_run.clone()],
        "the crashed run keeps its one episode and the queued send opens the next"
    );

    let dir = rig.crash().await;
    delete_run_index_row(&dir, &queued_run);
    let mut rig = SendKeyRig::start(&dir, None).await;
    let before = rig.records();
    assert_answers_finished(
        &rig.send("queued", "queued behind the held run").await,
        &queued_run,
    );
    assert_refused_naming(&rig.send("queued", "a different prompt").await, "input");
    assert_eq!(
        rig.records(),
        before,
        "neither retry may write a record: the same key never opens a second episode"
    );
    rig.finish().await;
}

#[tokio::test]
async fn admission_open_failed_preserves_legacy_message() {
    let mut route = PreparedWalRoute::start("admission-open-failed", |_| {}).await;
    let body = route.expect_error(2, serde_json::json!({"method":"session.send", "params":{"prompt":"hi","delivery":"interrupt"}})).await;
    // An interrupt names no model on a session with no episode, so the one thing missing is
    // the model; the message says exactly that.
    let legacy = r#"{"code":"open_failed","message":"open failed: run resolution failed: model is required: the session has no episode to inherit it from"}"#;
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["detail"]["class"], "permanent");
    let mut without_detail = value;
    without_detail.as_object_mut().unwrap().remove("detail");
    assert_eq!(serde_json::to_string(&without_detail).unwrap(), legacy);
    route.finish().await;
}

struct TemporaryWalReader;

impl broca_core::WalStore for TemporaryWalReader {
    fn open(
        &self,
        _session: &BindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        unreachable!("the refusal uses the lease-free reader")
    }
    fn read_records(
        &self,
        _session: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        Err(broca_wal::WalError::Io(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "temporary WAL read interruption",
        )))
    }
}

#[tokio::test]
async fn permanent_open_failed_cache_refusal_golden() {
    let mut route = PreparedWalRoute::start("permanent-open-failed", |_| {}).await;
    let body = route
        .expect_error(
            2,
            serde_json::json!({
                "method":"session.send", "params": {
                    "prompt":"hi", "model":{"provider":"ollama-cloud","model":"deepseek-v4-flash"},
                    "cache":{"role":"primary"}
                }
            }),
        )
        .await;
    assert_eq!(
        body,
        include_bytes!("../../../broca-wire/tests/golden/open_failed_permanent.json")
    );
    route.finish().await;
}

/// The refusal body is captured from a real session.read over serve_connection, with
/// materialized history remaining after both live and archive WAL sources are absent.
#[tokio::test]
async fn history_unreachable_refusal_golden_from_real_serve() {
    let mut route = PreparedWalRoute::start("history-unreachable", |wal_path| {
        let dir = wal_path
            .parent()
            .and_then(Path::parent)
            .expect("the WAL lives below the rig directory");
        let session = BindIdentity::new(dir.join("proj"), "runner", "s1");
        std::fs::remove_file(wal_path).expect("replace the helper's incidental WAL");
        let wal_store = FileWalStore::new(dir.join("wal"));
        let mut wal = broca_wal::FileWal::open(wal_store.wal_path(&session))
            .expect("create a WAL whose history can be projected");
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
                run_id: RunId("history-unreachable".into()),
                session: session.clone(),
                ts_ms: None,
                config: Box::new(min_config()),
                input: vec![Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "history that must not be truncated".into(),
                    }],
                )]
                .into(),
                origin: broca_wal::RunOrigin::Fresh,
                submission_id: None,
            },
            true,
        )
        .expect("write the session start");
        wal.append(
            1,
            &WalRecord::ModelStepFinished {
                tags: None,
                hooks: None,
                step_id: broca_protocol::StepId(1),
                assistant_message: (AssistantMessage {
                    message_id: MessageId("history-answer".into()),
                    content: vec![ContentBlock::Text {
                        text: "projected answer".into(),
                    }],
                })
                .into(),
                usage: Usage::default(),
                finish_reason: Some(FinishReason::Stop),
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            true,
        )
        .expect("write the completed step");
        wal.append(
            1,
            &WalRecord::RunFinished {
                interrupt_reason: None,
                reason: RunFinishReason::Completed,
                ts_ms: None,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
            true,
        )
        .expect("write the session terminal");
        drop(wal);

        let records = wal_store
            .read_records(&session)
            .expect("read the seeded history");
        let store = broca_store::SqliteStore::open(dir.join("store.db"))
            .expect("open the registry's materialized store");
        store
            .project(&session, &records)
            .expect("materialize the session history");
        assert!(
            store
                .read_prefix_with_watermark(&session)
                .unwrap()
                .watermark_seq
                > 0,
            "the wire refusal requires persisted history"
        );
        std::fs::remove_file(wal_store.wal_path(&session)).expect("remove the live WAL");
    })
    .await;
    let body = route.expect_error(2, read_request()).await;
    let session = BindIdentity::new(route.dir.join("proj"), "runner", "s1");
    let session_addr = broca_core::session_addr(&session);
    let session_ref = broca_core::agent_session_ref(&session);
    let value: serde_json::Value = serde_json::from_slice(&body).expect("error response JSON");
    assert_eq!(value["code"], "history_unreachable");
    assert_eq!(value["detail"]["class"], "permanent");
    assert_eq!(value["detail"]["session_ref"], session_ref);
    assert_eq!(value["detail"].as_object().unwrap().len(), 2);
    assert!(value["message"].as_str().unwrap().contains(&session_addr));
    let normalized = String::from_utf8(body)
        .expect("UTF-8 error response")
        .replace(&session_addr, "@SESSION_ADDR@")
        .replace(&session_ref, "@SESSION_REF@");
    let golden = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden/session_read.history_unreachable.json");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&golden, &normalized).expect("write the served refusal bytes");
    }
    let pinned = std::fs::read(&golden)
        .unwrap_or_else(|error| panic!("read golden {}: {error}", golden.display()));
    assert_eq!(
        normalized.as_bytes(),
        pinned,
        "the real serve refusal bytes pinned in the golden"
    );
    route.finish().await;
}

#[tokio::test]
async fn transient_open_failed_wal_refusal_golden() {
    let mut route = PreparedWalRoute::start_with(
        "transient-open-failed",
        |_| {},
        Some(Arc::new(TemporaryWalReader)),
    )
    .await;
    let body = route.expect_error(2, read_request()).await;
    assert_eq!(
        body,
        include_bytes!("../../../broca-wire/tests/golden/open_failed_transient.json")
    );
    route.finish().await;
}

#[tokio::test]
async fn undecodable_control_request_errors_and_keeps_connection_alive() {
    let dir = tmpdir("invalid-control-request");
    let (registry, _caps, _started, _store, _wal) = registry_with_caps(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await
    });

    let hello = read_one(&mut creader).await;
    let hello_ack = Frame::build(
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
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    let mut invalid_bind = serde_json::to_value(ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(dir.join("proj"), "runner", "invalid-control-request"),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    })
    .unwrap();
    invalid_bind["scope"] = serde_json::json!({
        "owner": {"kind": "reserved", "module_id": "prefrontal-core"},
        "scope_ref": "head-1",
        "scope_epoch": 1,
        "kind": "head",
        "parent": null,
        "parent_state": null,
        "attributes": {"agent_id": "agent-7", "delegates": true, "future_attribute": "value"},
        "owner_authorized": true
    });
    write_req(&mut cwriter, 0, 41, invalid_bind).await;
    let rejected = read_one(&mut creader).await;
    assert_eq!(rejected.header.ty, FrameType::Error);
    assert_eq!(
        rejected.header.corr, 41,
        "error must identify the rejected request"
    );
    let error: ErrorBody = serde_json::from_slice(&rejected.body).unwrap();
    assert_eq!(error.code, "invalid_request");
    assert!(error.message.contains("future_attribute"));

    write_req(
        &mut cwriter,
        0,
        42,
        serde_json::to_value(ModuleControlRequest::HealthCheck {}).unwrap(),
    )
    .await;
    let health = read_one(&mut creader).await;
    assert_eq!(health.header.ty, FrameType::Response);
    assert_eq!(health.header.corr, 42);
    assert!(matches!(
        serde_json::from_slice::<ModuleControlResponse>(&health.body).unwrap(),
        ModuleControlResponse::HealthCheck { .. }
    ));

    drop(cwriter);
    drop(creader);
    let _ = serve.await;
}

#[tokio::test]
async fn session_send_invalid_on_restart_matches_golden_and_writes_nothing() {
    let dir = tmpdir("restart-policy-invalid");
    let (registry, wal) = registry_and_wal(&dir);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
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
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new(dir.join("proj"), "runner", "restart-policy");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            session.project_root.clone(),
            session.harness.clone(),
            session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    for (corr, value) in [
        (11, serde_json::json!("always")),
        (12, serde_json::json!(true)),
        (13, serde_json::json!(null)),
    ] {
        write_req(
            &mut cwriter,
            7,
            corr,
            serde_json::json!({
                "method": "session.send",
                "params": {
                    "prompt": "manage my context",
                    "model": { "provider": "fake", "model": "fake" },
                    "on_restart": value,
                }
            }),
        )
        .await;
        let response = read_one(&mut creader).await;
        assert_eq!(
            response.header.ty,
            FrameType::Error,
            "{value} must be refused"
        );
        let error: ErrorBody = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(error.code, "invalid_params", "{value}: {error:?}");
        assert_eq!(error.detail.as_ref().unwrap()["field"], "on_restart");
        if corr == 11 {
            assert_eq!(
                response.body.as_slice(),
                include_bytes!("../../../broca-wire/tests/golden/invalid_params.on_restart.json")
            );
        }
    }
    assert!(
        no_durable_records(wal.as_ref(), &session),
        "a refused value writes nothing durable"
    );

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn moving_sidecar_serves_reads_and_refuses_sends_with_the_real_wire_golden() {
    let mut route = PreparedWalRoute::start("moving-wire", |_| {}).await;
    let before = std::fs::read(&route.wal_path).unwrap();
    broca_wal::migration::freeze(
        &route.dir,
        &route.wal_path,
        &broca_wal::migration::Freeze {
            cut_id: "cut-a".into(),
            end_offset: before.len() as u64,
            lineage_id: "lineage".into(),
        },
    )
    .unwrap()
    .unwrap();
    let body = route.expect_error(2, send_request()).await;
    assert_eq!(
        String::from_utf8(body).unwrap(),
        r#"{"code":"session_moving","message":"session is moving under cut cut-a","detail":{"cutId":"cut-a"}}"#
    );
    write_req(&mut route.writer, 7, 3, read_request()).await;
    assert_eq!(
        read_one(&mut route.reader).await.header.ty,
        FrameType::Response
    );
    assert_eq!(std::fs::read(&route.wal_path).unwrap(), before);
    route.finish().await;
}

#[tokio::test]
async fn moved_tombstone_refuses_every_read_and_send_with_the_real_wire_golden() {
    let mut route = PreparedWalRoute::start("moved-wire", |_| {}).await;
    let before = std::fs::read(&route.wal_path).unwrap();
    broca_wal::migration::atomic_json(
        &route.dir,
        &route.wal_path.with_extension("moved"),
        &broca_wal::migration::Moved {
            cut_id: "cut-a".into(),
            holder: "remote".into(),
            lineage_id: "lineage".into(),
        },
    )
    .unwrap();
    for (i, request) in [
        send_request(),
        read_request(),
        status_request(),
        serde_json::json!({"method":"session.head","params":{}}),
        serde_json::json!({"method":"run.result","params":{"run_id":"r"}}),
        serde_json::json!({"method":"run.cancel","params":{"run_id":"r"}}),
        serde_json::json!({"method":"session.retract","params":{"submission_id":"q"}}),
    ]
    .into_iter()
    .enumerate()
    {
        let body = route.expect_error(2 + i as u64, request.clone()).await;
        assert_eq!(
            String::from_utf8(body).unwrap(),
            r#"{"code":"session_moved","message":"session moved to remote","detail":{"holder":"remote"}}"#,
            "{request}"
        );
    }
    assert_eq!(std::fs::read(&route.wal_path).unwrap(), before);
    route.finish().await;
}

#[tokio::test]
async fn reader_release_refuses_first_time_activation_fence_without_any_sidecar() {
    let mut route = PreparedWalRoute::start("activation-fence-wire", |_| {}).await;
    let reference = cortexkit_role_llm_runner::session_ref(
        "runner",
        &route.dir.join("proj").to_string_lossy(),
        "s1",
    );
    let authority = route.dir.join("agent-data/cuts/cut-a.json");
    broca_wal::migration::atomic_json(&route.dir, &authority, &serde_json::json!({"step":"imported","refs":[{"ref_kind":"broca-session","ref":reference}],"intent":{"op":"activate","request":{}}})).unwrap();
    let before = std::fs::read(&route.wal_path).unwrap();
    let state_before = std::fs::read(&authority).unwrap();
    let body = route.expect_error(2, send_request()).await;
    assert_eq!(
        String::from_utf8(body).unwrap(),
        r#"{"code":"session_moving","message":"session is moving under cut cut-a","detail":{"cutId":"cut-a"}}"#
    );
    assert_eq!(std::fs::read(&route.wal_path).unwrap(), before);
    assert_eq!(std::fs::read(&authority).unwrap(), state_before);
    route.finish().await;
}

#[tokio::test]
async fn pending_release_fence_outranks_a_tombstone_for_mutations_but_not_reads() {
    let mut route = PreparedWalRoute::start("pending-release-wire", |_| {}).await;
    let reference = cortexkit_role_llm_runner::session_ref(
        "runner",
        &route.dir.join("proj").to_string_lossy(),
        "s1",
    );
    broca_wal::migration::atomic_json(&route.dir, &route.dir.join("agent-data/cuts/cut-a.json"),&serde_json::json!({"step":"exported","refs":[{"ref_kind":"broca-session","ref":reference}],"intent":{"op":"release","request":{}}})).unwrap();
    broca_wal::migration::atomic_json(
        &route.dir,
        &route.wal_path.with_extension("moved"),
        &broca_wal::migration::Moved {
            cut_id: "cut-a".into(),
            holder: "remote".into(),
            lineage_id: "lineage".into(),
        },
    )
    .unwrap();
    let before = std::fs::read(&route.wal_path).unwrap();
    for (i, request) in [
        send_request(),
        serde_json::json!({"method":"session.warm","params":{}}),
        serde_json::json!({"method":"run.cancel","params":{"run_id":"r"}}),
        serde_json::json!({"method":"session.retract","params":{"submission_id":"q"}}),
    ]
    .into_iter()
    .enumerate()
    {
        let body = route.expect_error(i as u64 + 2, request).await;
        assert_eq!(
            String::from_utf8(body).unwrap(),
            r#"{"code":"session_moving","message":"session is moving under cut cut-a","detail":{"cutId":"cut-a"}}"#
        );
    }
    let body = route.expect_error(6, read_request()).await;
    assert_eq!(
        String::from_utf8(body).unwrap(),
        r#"{"code":"session_moved","message":"session moved to remote","detail":{"holder":"remote"}}"#
    );
    assert_eq!(std::fs::read(&route.wal_path).unwrap(), before);
    route.finish().await;
}

#[tokio::test]
async fn authorization_precedes_migration_and_validation_for_moved_sessions() {
    for pending_release in [false, true] {
        let mut route = protected_moved_route("prefrontal-core", pending_release).await;
        let before = std::fs::read(&route.wal_path).unwrap();
        for (i, method) in [
            "session.send",
            "session.warm",
            "session.import",
            "run.cancel",
            "session.retract",
            "session.read",
            "session.head",
            "run.result",
            "run.status",
            "session.subscribe",
        ]
        .into_iter()
        .enumerate()
        {
            let body = route
                .expect_error(
                    i as u64 + 2,
                    serde_json::json!({"method":method,"params":{"unknown":true}}),
                )
                .await;
            let error: ErrorBody = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                error.code,
                if matches!(method, "run.cancel" | "session.retract") {
                    "scope_owner_mismatch"
                } else {
                    "sender_not_owner"
                },
                "{method}, pending release: {pending_release}, {error:?}"
            );
        }
        assert_eq!(std::fs::read(&route.wal_path).unwrap(), before);
        route.finish().await;
    }
}

#[tokio::test]
async fn authorized_moved_sessions_keep_operation_specific_fences() {
    for pending_release in [false, true] {
        let mut route = protected_moved_route("basal", pending_release).await;
        let before = std::fs::read(&route.wal_path).unwrap();
        for (i, method) in [
            "session.send",
            "session.warm",
            "session.import",
            "run.cancel",
            "session.retract",
            "session.read",
            "session.head",
            "run.result",
            "run.status",
            "session.subscribe",
        ]
        .into_iter()
        .enumerate()
        {
            let body = route
                .expect_error(
                    i as u64 + 2,
                    serde_json::json!({"method":method,"params":{"unknown":true}}),
                )
                .await;
            let error: ErrorBody = serde_json::from_slice(&body).unwrap();
            let write = matches!(
                method,
                "session.send"
                    | "session.warm"
                    | "session.import"
                    | "run.cancel"
                    | "session.retract"
            );
            assert_eq!(
                error.code,
                if pending_release && write {
                    "session_moving"
                } else {
                    "session_moved"
                },
                "{method}, pending release: {pending_release}, {error:?}"
            );
        }
        assert_eq!(std::fs::read(&route.wal_path).unwrap(), before);
        route.finish().await;
    }
}

#[tokio::test]
async fn unreadable_moved_ownership_fails_closed_without_repair() {
    let mut route = protected_moved_route("prefrontal-core", true).await;
    corrupt_first_frame(&route.wal_path);
    let before = std::fs::read(&route.wal_path).unwrap();
    for (i, method) in [
        "session.send",
        "run.cancel",
        "session.retract",
        "session.read",
    ]
    .into_iter()
    .enumerate()
    {
        let body = route
            .expect_error(
                i as u64 + 2,
                serde_json::json!({"method":method,"params":{"unknown":true}}),
            )
            .await;
        let error: ErrorBody = serde_json::from_slice(&body).unwrap();
        assert_eq!(error.code, "open_failed", "{method}: {error:?}");
    }
    assert_eq!(std::fs::read(&route.wal_path).unwrap(), before);
    route.finish().await;
}

/// Two moved-session routes alive at once, which is what happens when the tests built on
/// `protected_moved_route` run in parallel inside one test binary. The second route's setup
/// must not touch the first route's scratch directory: when both shared one directory, the
/// second setup deleted the first route's WAL, store and markers and rewrote them, so the
/// first route answered from the second route's pending-release cut (`session_moving`), or
/// its own setup failed opening the store or appending to a WAL that had just been removed.
#[tokio::test]
async fn overlapping_protected_moved_routes_keep_separate_scratch_state() {
    let mut first = protected_moved_route("basal", false).await;
    let before = std::fs::read(&first.wal_path).unwrap();
    // Set up the second route while the first is still serving: the interleaving that
    // parallel tests produce by chance, forced here on every run.
    let second = protected_moved_route("basal", true).await;
    let body = first
        .expect_error(
            2,
            serde_json::json!({"method":"session.send","params":{"unknown":true}}),
        )
        .await;
    let error: ErrorBody = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        error.code, "session_moved",
        "the first route has no pending release, so a write must see only its moved marker: {error:?}"
    );
    assert_eq!(std::fs::read(&first.wal_path).unwrap(), before);
    assert_ne!(first.dir, second.dir);
    second.finish().await;
    first.finish().await;
}

async fn protected_moved_route(first: &str, pending_release: bool) -> PreparedWalRoute {
    let route = PreparedWalRoute::start_with_authorization(
        "protected-moved-wire",
        |_| {},
        None,
        Some(broca_protocol::BindPrincipal::Reserved {
            module_id: first.into(),
        }),
        Some(subc_protocol::Principal::Reserved {
            module_id: "basal".into(),
        }),
    )
    .await;
    if pending_release {
        let reference = cortexkit_role_llm_runner::session_ref(
            "runner",
            &route.dir.join("proj").to_string_lossy(),
            "s1",
        );
        broca_wal::migration::atomic_json(&route.dir, &route.dir.join("agent-data/cuts/cut-a.json"),
            &serde_json::json!({"step":"exported","refs":[{"ref_kind":"broca-session","ref":reference}],"intent":{"op":"release","request":{}}})).unwrap();
    }
    broca_wal::migration::atomic_json(
        &route.dir,
        &route.wal_path.with_extension("moved"),
        &broca_wal::migration::Moved {
            cut_id: "cut-a".into(),
            holder: "remote".into(),
            lineage_id: "lineage".into(),
        },
    )
    .unwrap();
    route
}

/// A tool declared on `session.send` with `reply: {max_ms}`, over the real serve path: every
/// malformed `reply` is refused `invalid_params` naming its member before anything is written;
/// the golden send freezes the deadline into its `RunStarted` tool entry (byte for byte the
/// entry pinned in `run_started.tool_reply.json`, in a frame requiring `tool-reply/v1`); and a
/// later send that omits `tools` inherits the deadline with the rest of the tool.
#[tokio::test]
async fn send_declared_tool_reply_is_validated_frozen_and_inherited_over_the_serve_path() {
    let dir = tmpdir("tool-reply");
    let lease = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
    let exec = Arc::new(CoreExec {
        wal_store: wal.clone(),
        store: Some(store.clone()),
        reply_text: "researched".into(),
    });
    let opener = Arc::new(
        DefaultOpener::new(lease, wal.clone(), exec)
            .with_run_index(store.clone())
            .with_materialized_store(store.clone()),
    );
    let wal_reader: Arc<dyn broca_core::WalStore> = wal.clone();
    let store_reader: Arc<dyn broca_core::MaterializedStore> = store.clone();
    let reg = Arc::new(LineageRegistry::new_with_reader(
        opener,
        wal_reader,
        Some(store_reader),
    ));
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });
    let hello = read_one(&mut creader).await;
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
    write_frame(&mut cwriter, &ack).await.unwrap();
    let session = BindIdentity::new(dir.join("proj"), "runner", "lead");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(
            session.project_root.clone(),
            session.harness.clone(),
            session.session.clone(),
        ),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    let mut golden: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../../../broca-wire/tests/golden/send_params.tool_reply.json"
    ))
    .unwrap();
    golden["model"] = serde_json::json!({ "provider": "fake", "model": "fake" });

    for (corr, reply, field) in [
        (11, serde_json::json!({}), "tools[0].reply.max_ms"),
        (
            12,
            serde_json::json!({"max_ms": 0}),
            "tools[0].reply.max_ms",
        ),
        (
            13,
            serde_json::json!({"max_ms": "1800000"}),
            "tools[0].reply.max_ms",
        ),
        (
            14,
            serde_json::json!({"max_ms": 1000, "grace_ms": 5}),
            "tools[0].reply.grace_ms",
        ),
        (15, serde_json::json!(1_800_000), "tools[0].reply"),
    ] {
        let mut params = golden.clone();
        params["tools"][0]["reply"] = reply.clone();
        write_req(
            &mut cwriter,
            7,
            corr,
            serde_json::json!({"method": "session.send", "params": params}),
        )
        .await;
        let response = read_one(&mut creader).await;
        assert_eq!(
            response.header.ty,
            FrameType::Error,
            "{reply} must be refused"
        );
        let error: ErrorBody = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(error.code, "invalid_params", "{reply}: {error:?}");
        assert_eq!(error.detail.as_ref().unwrap()["field"], field, "{reply}");
        assert!(
            error.message.starts_with(field),
            "{reply}: {}",
            error.message
        );
    }
    assert!(
        no_durable_records(wal.as_ref(), &session),
        "a refused reply writes nothing durable"
    );

    write_req(
        &mut cwriter,
        7,
        20,
        serde_json::json!({"method": "session.send", "params": golden}),
    )
    .await;
    let response = read_one(&mut creader).await;
    assert_eq!(
        response.header.ty,
        FrameType::Response,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    let first: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    let first_run = first["result"]["run_id"].as_str().unwrap().to_string();
    wait_for_completed_run(&mut creader, &mut cwriter, 7, &first_run, 100).await;

    // A later send that omits `tools` inherits the frozen surface, deadline included.
    write_req(
        &mut cwriter,
        7,
        30,
        serde_json::json!({"method": "session.send", "params": {
            "prompt": "Continue.",
            "model": { "provider": "fake", "model": "fake" }
        }}),
    )
    .await;
    let response = read_one(&mut creader).await;
    assert_eq!(response.header.ty, FrameType::Response);
    let second: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    let second_run = second["result"]["run_id"].as_str().unwrap().to_string();
    wait_for_completed_run(&mut creader, &mut cwriter, 7, &second_run, 200).await;

    let pinned: WalRecord = serde_json::from_str(include_str!(
        "../../../broca-wal/tests/golden/run_started.tool_reply.json"
    ))
    .unwrap();
    let WalRecord::RunStarted { config: pinned, .. } = pinned else {
        unreachable!()
    };
    let pinned_entry = serde_json::to_string(&pinned.tools[0]).unwrap();
    let starts: Vec<_> = wal
        .read_records(&session)
        .unwrap()
        .into_iter()
        .filter(|frame| matches!(frame.record, WalRecord::RunStarted { .. }))
        .collect();
    assert_eq!(starts.len(), 2);
    for frame in &starts {
        let WalRecord::RunStarted { config, .. } = &frame.record else {
            unreachable!()
        };
        assert_eq!(config.tools.len(), 1);
        assert_eq!(
            serde_json::to_string(&config.tools[0]).unwrap(),
            pinned_entry
        );
        assert!(frame
            .record
            .required_features()
            .contains(&broca_wal::TOOL_REPLY_FEATURE));
    }
    let raw = std::fs::read_dir(dir.join("wal"))
        .unwrap()
        .flat_map(|entry| walk_files(&entry.unwrap().path()))
        .map(|path| std::fs::read(path).unwrap())
        .find(|bytes| {
            bytes
                .windows(pinned_entry.len())
                .any(|w| w == pinned_entry.as_bytes())
        })
        .expect("the session WAL holds the pinned tool entry verbatim");
    assert!(raw
        .windows(b"tool-reply/v1".len())
        .any(|window| window == b"tool-reply/v1"));

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

fn walk_files(path: &Path) -> Vec<std::path::PathBuf> {
    if path.is_dir() {
        std::fs::read_dir(path)
            .unwrap()
            .flat_map(|entry| walk_files(&entry.unwrap().path()))
            .collect()
    } else {
        vec![path.to_path_buf()]
    }
}
