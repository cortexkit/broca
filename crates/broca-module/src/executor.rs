//! The run-executor seam.
//!
//! The coordinator actor does not call `run_loop` directly — it depends on this
//! [`RunExecutor`] trait. That keeps the actor's concurrency (registry, state machine,
//! close teardown, races) PURE and deterministically testable: the race harness drives a
//! CONTROLLABLE fake executor (signal exactly when a run finishes, hold a run open while a
//! close/subscribe races), which a real LLM loop could never do reproducibly. The real
//! adapter wraps `broca_core::run_loop_under_lease` and is the only place that touches the
//! loop.

use std::sync::Arc;

use async_trait::async_trait;
use broca_core::{OutputEvent, RunControl, RunError, RunId, RunRequest};
use broca_protocol::BindIdentity;
use tokio::sync::mpsc;

#[path = "executor_fork.rs"]
mod fork;
pub use fork::fork_admission_id;
pub(crate) use fork::fork_conversation_key;
pub use fork::{ForkCallJob, ForkCallOutcome, ForkCallRefusal, ForkReleaseJob, ForkStepSpend};

/// Which start mode a run uses (the coordinator decides; the executor obeys).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunKind {
    /// A fresh run, or resume the last (unfinished) episode of an existing lineage — the
    /// executor distinguishes these from the WAL (empty ⇒ fresh, non-empty ⇒ resume).
    FreshOrResume,
    /// Append a NEW episode (turn) to an IDLE lineage (the prior conversation is the
    /// prefix). Only valid over a finished lineage.
    AppendEpisode,
    /// Continue a run that paused awaiting re-auth (after the credential refreshed).
    ResumeAfterAuth,
}

/// Drives ONE agentic run to a terminal under an already-held lease, emitting its
/// OutputStream to `out`. Neither acquires nor releases a lease (the actor owns the lease
/// for the whole session lifetime). Implementors: the real loop adapter, and the test
/// fake.
#[async_trait]
pub trait RunExecutor: Send + Sync + 'static {
    /// Normalize an explicit joining temperature only for compatibility comparison. The outer
    /// `None` means model facts are unavailable and the actor must compare the value literally;
    /// `Some(None)` means this model omits temperature. This never changes the running request.
    fn joining_temperature(
        &self,
        _model: &broca_protocol::request::ModelHandle,
        _frozen_render_config: &serde_json::Value,
        _requested: f64,
    ) -> Option<Option<f64>> {
        None
    }

    /// Resolve a selectable model without admitting a run or changing routing state.
    fn head_selection_wire(
        &self,
        _model: &cortexkit_role_llm_runner::head_selection::PlanModel,
    ) -> Option<String> {
        None
    }

    async fn check_plan_selection(
        &self,
        plan: &serde_json::Value,
    ) -> Result<
        Option<cortexkit_role_llm_runner::head_selection::PlanModel>,
        cortexkit_role_llm_runner::head_selection::PlanModelProblem,
    > {
        cortexkit_role_llm_runner::head_selection::check_plan(
            plan,
            &broca_wire::broca_role_describe(env!("CARGO_PKG_VERSION")).role,
            |model| self.head_selection_wire(model),
        )
    }

    /// The context limit a fresh run of `model` would freeze, resolved as the run resolves it
    /// but without choosing an account, reading a credential or touching a connection: what a
    /// plan refresh that adds a compaction provider is checked against, since a run on the
    /// compaction contract can't run without one. A send may name its own `context_limit`, which
    /// a run prefers, but a refresh carries none and a later send may not, so only the resolved
    /// limit counts here. `Ok(None)` when resolution gives none (an opaque endpoint); `Err` when
    /// it couldn't run, with its cause. A runner that can't resolve models answers `Err`.
    async fn compaction_context_limit(
        &self,
        _model: &cortexkit_role_llm_runner::head_selection::PlanModel,
    ) -> Result<Option<u64>, String> {
        Err("this runner can't resolve a model's context limit".into())
    }

    /// Validate the abstract level before a send can enter the durable queue.
    fn check_service_tier(
        &self,
        _request: &broca_protocol::request::SubmissionRequest,
    ) -> Result<(), String> {
        Ok(())
    }
    /// Validate the effective sign-in method before a routed send can be durably queued.
    fn check_account_route(
        &self,
        _request: &broca_protocol::request::SubmissionRequest,
    ) -> Result<(), String> {
        Ok(())
    }
    /// The email on a cached vault row, never a vault request on a read path.
    fn cached_account_email(
        &self,
        _selection: &broca_records::record::AuthSelection,
    ) -> Option<String> {
        None
    }
    /// Quote current catalog media facts for enqueue and read-only fresh-send preflight.
    /// Actual admission checks again against the provider's frozen render config.
    async fn media_profile(
        &self,
        _request: &broca_protocol::request::SubmissionRequest,
    ) -> Result<Option<Box<broca_provider::media_profile::MediaProfile>>, RunError> {
        Ok(None)
    }
    /// Where a provider-held call waits for its late result (see `crate::held_slots`). Without
    /// it a run that resumes on a held call stays paused, because nothing can deliver to it.
    fn provider_held_lifecycle(
        &self,
    ) -> Option<Arc<dyn broca_core::tool_plane::ProviderHeldLifecycle>> {
        None
    }

    /// The adapter that calls the compaction provider's Setup. Its presence enables
    /// compaction writes; the actor supplies it through `RunControl`.
    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::CompactionPlane>> {
        None
    }

    fn compaction_views(&self) -> Option<Arc<dyn broca_core::CompactionViews>> {
        None
    }

    /// The same session-scoped hook adapter used by the run loop. Input hooks execute in
    /// the actor before a send writes or interrupts anything, never under the steer lock.
    fn hook_plane(&self) -> Option<Arc<dyn broca_core::HookPlane>> {
        None
    }

    /// Runs one run of a session. An executor that runs no sessions (a test's) refuses with
    /// nothing written.
    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        StoreRunEnd {
            parts: job.parts,
            result: Err(StoreRunError::Admission(RunError::Config(
                "this executor runs no sessions".into(),
            ))),
        }
    }

    /// Called by the actor's CLOSE teardown when a session ends (the lineage idles to
    /// close), so an executor holding per-session resources keyed off a SHARED handle can
    /// release them. The real executor uses it to send a route GOODBYE for the session so a
    /// shared subc consumer connection doesn't accumulate stale route channels across
    /// sessions. Default no-op (the in-process fakes hold nothing per session). Best-effort:
    /// a failure here must not block the close.
    async fn on_session_close(&self, _session: &BindIdentity) {}

    /// Retention may delete a session with no actor to run the async close hook. Retire its
    /// cached routes before returning so a later generation cannot inherit their teardown.
    fn on_session_deleted(&self, _session: &BindIdentity) {}

    /// Discard authenticated connection state when a session is deleted or moved.
    fn retire_session_transport(&self, _session: &BindIdentity) {}

    /// Drop only idle connections while a move probes whether a session is busy.
    fn retire_idle_session_transport(&self, _session: &BindIdentity) {}

    /// Discard idle and borrowed connections after the module's shutdown grace period.
    fn retire_all_transports(&self) {}

    /// Called after the actor has durably written an `Interrupted` terminal for a run it
    /// sealed: one stopped by the module's shutdown, or one a crash left in flight. The run
    /// loop did not write that terminal, so an executor that reports each run's end hears of
    /// it only here. Must not block: the shutdown seal runs under a deadline. Default no-op.
    fn on_run_sealed(&self, _sealed: SealedRun) {}

    /// Quote the next cache-refresh request and one uncached request from the in-memory model
    /// catalog, current at decision time. The frozen provider/model selects the rate row;
    /// the frozen account method selects subscription or metered pricing. Missing rates refuse
    /// warming.
    fn warm_costs(
        &self,
        _config: &broca_records::record::RunConfig,
        _prefix_tokens: u64,
        _ttl_ms: u64,
    ) -> Result<broca_core::keep_warm::WarmCosts, String> {
        Err("pricing_unavailable".into())
    }

    /// Price the returned Responses usage in the same units as `warm_costs`.
    /// Drivers without usage pricing retain the pre-send estimate.
    fn responses_warm_cost(
        &self,
        _config: &broca_records::record::RunConfig,
        _usage: broca_protocol::Usage,
    ) -> Result<f64, String> {
        Err("pricing_unavailable".into())
    }

    /// A failed warm consumes only provider-reported usage, not a cache-refresh estimate.
    fn failed_warm_cost(
        &self,
        _config: &broca_records::record::RunConfig,
        _reservation: &broca_records::record::AttemptReservation,
        _envelope: &broca_records::record::AttemptEnvelope,
        _ttl_ms: Option<u64>,
    ) -> Result<f64, String> {
        Err("reported warm usage pricing unavailable".into())
    }

    /// Check quota before an automatic cache-refresh request. An error prevents the send;
    /// the tool-wait keep-warm scheduler logs the returned error code once and stops warming
    /// for the rest of this tool wait.
    async fn warm_quota(
        &self,
        _config: &broca_records::record::RunConfig,
        _headers: &std::collections::BTreeMap<String, String>,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Override expiry-based timing with an interval after the last request was sent,
    /// for providers that do not document whether a cached read extends retention.
    fn warm_after_send_ms(&self, _config: &broca_records::record::RunConfig) -> Option<u64> {
        None
    }

    /// Build the provider from the recorded run config and a freshly read credential, then
    /// re-send the last model request with output suppressed. Requests re-rendered from the
    /// session's write-ahead log must match their recorded body digest before sending.
    /// Provider errors after sending mean a request may have been sent; other errors mean
    /// none was sent. The default executor cannot warm.
    async fn warm(&self, _job: WarmJob) -> Result<broca_core::WarmSent, WarmError> {
        Err(WarmError::Unsupported(
            "this runner cannot warm a prompt cache".into(),
        ))
    }

    /// Make one attempt of a fork's ask (llm-runner `fork_answer`): admit it with routing when
    /// the head's account is routed, check the account, render and check the base, send the
    /// fork's request once, refuse any tool call, and release the admission, as the fields of
    /// [`ForkCallJob`] and the variants of [`ForkCallOutcome`] describe. Never writes the head's
    /// session, takes its lease or waits on its run. The default executor serves no fork.
    async fn fork_call(&self, _job: ForkCallJob) -> ForkCallOutcome {
        ForkCallOutcome::Refused(ForkCallRefusal::Unsupported(
            "this runner cannot answer fork questions".into(),
        ))
    }

    /// Release a fork attempt's routing admission that no [`RunExecutor::fork_call`] released:
    /// one whose attempt was in flight when the module stopped. Sends `route.account_observe`
    /// `run_finished` for it when the head's account is routed; does nothing otherwise. `Err`
    /// means the release may not have reached routing, and the fork manager asks again the
    /// next time it opens the fork.
    async fn fork_release(&self, _job: ForkReleaseJob) -> Result<(), String> {
        Ok(())
    }

    /// Whether the routing admission of the fork attempt `admission_id`, whose
    /// [`RunExecutor::fork_call`] just returned, may not have been released because routing
    /// couldn't be told. The fork manager then records no release, so it asks again with
    /// [`RunExecutor::fork_release`] the next time it opens the fork. Asking takes the answer.
    fn fork_release_failed(&self, _admission_id: &str) -> bool {
        false
    }
}

/// One run of a session, as the store actor hands it to the executor. The actor builds the
/// request and the session's part of the run control; the executor resolves the credential, routing, provider and the model's entry in the model catalog,
/// completes the control, admits the run by `broca_session_loop::admit::admission` and runs it
/// through `writer`.
pub struct StoreRunJob {
    pub req: RunRequest,
    /// Caller-scoped credential selection carried separately from core's provider-agnostic
    /// request. The executor resolves it at admission and freezes only the resulting credential.
    pub auth: Option<broca_protocol::AuthRequest>,
    pub control: RunControl,
    /// The run's writer: a client of the store actor, which owns the session.
    pub writer: broca_session_loop::shared::ActorWriter,
    /// Where the run's setup parts are stored and read back, lent by the actor for the run and
    /// handed back in [`StoreRunEnd`].
    pub parts: Box<dyn broca_session_loop::parts::Parts>,
    pub mode: StoreRunMode,
    /// What the run renders media from (see `store_media`), sent once the session's image index is
    /// ready. `None` for a seal, which calls no model.
    pub media: Option<crate::store_media::StoreRunMediaWait>,
    /// The abstract service tier level the send admitted: routing may choose another, and a fresh
    /// run's admission applies it to the request it freezes.
    pub queued_service_tier: Option<String>,
    /// What warms the prompt cache while the run waits on tool calls, for a run whose keep-warm
    /// is `auto` (`broca_session_loop::run::Seams::keep_warm`). `None` warms nothing.
    pub keep_warm: Option<
        Arc<dyn broca_core::keep_warm_wait::KeepWarmDriver<broca_session_loop::run::Admission>>,
    >,
}

/// What a store run does: admit a new run, or carry on or end the session's unfinished one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreRunMode {
    /// Admit the job's request as a new run.
    Fresh,
    /// Continue the session's unfinished run from its last durable boundary.
    Resume,
    /// Re-check a sign-in pause, allowing one fresh account-continuation question.
    RecheckSignIn,
    /// At actor open, inspect sign-in and ask routing only; never resume the original account.
    ProbeSignIn,
    /// End the session's unfinished run `Interrupted` without calling the model:
    /// `broca_session_loop::seal`.
    Seal(broca_core::seal::SealCause),
}

/// How a store run ended, with the parts store its job lent.
pub struct StoreRunEnd {
    pub parts: Box<dyn broca_session_loop::parts::Parts>,
    pub result: Result<broca_session_loop::run::Finished, StoreRunError>,
}

/// Why a store run ended without a finish the loop recorded.
#[derive(Debug)]
pub enum StoreRunError {
    /// Resolving or admitting the run failed before anything was written.
    Admission(RunError),
    /// The loop stopped; see [`broca_session_loop::run::LoopError`].
    Loop(broca_session_loop::run::LoopError),
}

/// A run the actor sealed `Interrupted`, as [`RunExecutor::on_run_sealed`] receives it.
#[derive(Debug, Clone)]
pub struct SealedRun {
    pub session: BindIdentity,
    pub run_id: RunId,
    /// The sealed episode's frozen auth selection, as its `RunStarted` recorded it.
    pub auth_selection: Option<broca_core::AuthSelection>,
    /// The lineage of the WAL the terminal was written to.
    pub lineage: Option<broca_wal::WalLineageId>,
    /// The WAL sequence number of the durable `RunFinished` record.
    pub terminal_seq: u64,
}

/// Send one warm of `base` through `provider`, which holds the session's frozen config and a
/// freshly read credential. An executor's [`RunExecutor::warm`] ends here.
///
/// A recorded base is rendered by `provider` and sent only when those bytes have exactly the
/// digest its step recorded: any difference would warm a different prefix, so nothing is sent.
pub async fn send_warm(
    provider: &dyn broca_core::Provider,
    base: WarmBase,
) -> Result<broca_core::WarmSent, WarmError> {
    let bytes = prepare_warm_base(provider, base).await?;
    provider.warm(&bytes).await.map_err(warm_failure)
}

/// Render and check the warm prefix before granting the transport a billing hook.
pub async fn send_warm_with_hook(
    provider: &dyn broca_core::Provider,
    base: WarmBase,
    hook: Option<Arc<dyn broca_core::run::AttemptHook>>,
) -> Result<broca_core::WarmSent, WarmError> {
    let bytes = prepare_warm_base(provider, base).await?;
    let result = match hook {
        Some(hook) => provider.warm_with_hook(&bytes, Some(hook)).await,
        None => provider.warm(&bytes).await,
    };
    result.map_err(warm_failure)
}

async fn prepare_warm_base(
    provider: &dyn broca_core::Provider,
    base: WarmBase,
) -> Result<Vec<u8>, WarmError> {
    let base = match base {
        WarmBase::RecordedMedia { request, renderer } => {
            use broca_session_loop::run::ModelCallReady as _;
            renderer
                .ready()
                .await
                .map_err(|error| WarmError::PrefixUnavailable(error.message))?;
            provider.set_media_request_renderer(renderer.renderer());
            WarmBase::Recorded(request)
        }
        base => base,
    };
    let bytes = match base {
        WarmBase::InFlight(sent) => {
            // The in-flight bytes are already the ones the transport sent, for whichever model
            // it sent them to; the warm must be shaped and addressed for that same model, so
            // the provider follows the route the transport recorded with them. Without this a
            // run on its refusal-fallback model would have its warm addressed as the episode's
            // own model.
            provider.set_step_route(sent.route);
            sent.bytes().to_vec()
        }
        WarmBase::Recorded(recorded) => {
            // A step the refusal-fallback model answered was rendered and sent for that model,
            // so the warm must render it, and address the warm, the same way: on the source
            // route its bytes would never match the recorded digest. The route is set for a
            // source step too, so a provider reused after a fallback warm goes back to the
            // episode's own model.
            provider.set_step_route(if recorded.route.is_some() {
                broca_core::refusal_fallback::StepRoute::Fallback
            } else {
                broca_core::refusal_fallback::StepRoute::Source
            });
            let rendered = provider
                .try_render_request(&recorded.opts)
                .map_err(|error| WarmError::PrefixUnavailable(error.message))?;
            let digest = broca_core::render::request_digest_hex(&rendered);
            if digest != recorded.request_body_sha256 {
                return Err(WarmError::PrefixUnavailable(format!(
                    "the request rebuilt from the WAL renders to {digest}, not the {} its step \
                     sent",
                    recorded.request_body_sha256
                )));
            }
            rendered
        }
        WarmBase::RecordedMedia { .. } => unreachable!("media readiness was resolved above"),
    };
    Ok(bytes)
}

fn warm_failure(failure: broca_core::WarmFailure) -> WarmError {
    match failure {
        broca_core::WarmFailure::Unsupported(reason) => WarmError::Unsupported(reason),
        broca_core::WarmFailure::AccountChanged { frozen, served } => {
            WarmError::AccountChanged { frozen, served }
        }
        broca_core::WarmFailure::Provider(error) => WarmError::Provider(error),
    }
}

/// The request a cache warm re-sends.
pub enum WarmBase {
    /// The exact bytes the running run's transport sent for its most recent request.
    InFlight(broca_core::SentRequest),
    /// The most recent request the WAL records, to be rendered again and checked against the
    /// digest its step recorded.
    Recorded(Box<broca_core::RecordedRequest>),
    /// A recorded store request whose media bytes and rendering state must be ready first.
    /// It passes through the same digest check as a recorded request without media.
    RecordedMedia {
        request: Box<broca_core::RecordedRequest>,
        renderer: Arc<crate::store_media::LazyRenderer>,
    },
}

impl WarmBase {
    /// The digest of the request this base stands for.
    pub fn request_body_sha256(&self) -> &str {
        match self {
            WarmBase::InFlight(sent) => &sent.sha256,
            WarmBase::Recorded(recorded) => &recorded.request_body_sha256,
            WarmBase::RecordedMedia { request, .. } => &request.request_body_sha256,
        }
    }
}

/// One cache warm the coordinator asks the executor to send.
pub struct WarmJob {
    pub session: BindIdentity,
    /// The config the request was sent with: the provider is built from it.
    pub config: broca_records::record::RunConfig,
    pub base: WarmBase,
    /// Present only on overhaul roots; the transport must obtain permission before each send.
    pub attempt_hook: Option<Arc<dyn broca_core::run::AttemptHook>>,
}

/// Why an executor did not warm.
#[derive(Debug, Clone, PartialEq)]
pub enum WarmError {
    /// The provider, or a shape the session froze, cannot be warmed. Nothing was sent.
    Unsupported(String),
    /// The recorded request rendered to different bytes than its step sent. Nothing was sent.
    PrefixUnavailable(String),
    /// The credential now belongs to a different account than the session froze. Nothing was
    /// sent.
    AccountChanged {
        frozen: Option<String>,
        served: Option<String>,
    },
    /// The warm was sent and the provider refused it.
    Provider(Box<broca_protocol::error::ProviderError>),
    /// A dependency, such as the credential source, failed before anything was sent.
    Unavailable(String),
}
