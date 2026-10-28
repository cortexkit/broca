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
use broca_core::{OutputEvent, RunControl, RunError, RunId, RunRequest, RunSummary, WalHandle};
use broca_protocol::BindIdentity;
use tokio::sync::{mpsc, oneshot};

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

/// One run the coordinator asks the executor to drive. The actor injects its
/// SESSION-scoped writer (the run appends to it; the actor owns its lifecycle), so the
/// run never spawns or shuts down a writer — a run ends with a `sync()` barrier and the
/// writer lives on for the next episode.
pub struct RunJob {
    pub media: Vec<crate::person_media::StagedMedia>,
    /// Keep the full profile off the actor's nested admission-future stack, including
    /// text-only sends whose optional profile is absent.
    pub media_profile: Option<Box<broca_provider::media_profile::MediaProfile>>,
    /// Routing affinity supplied by the admitting send, including a drained queued send.
    pub conversation_key: Option<String>,
    pub req: RunRequest,
    /// Caller-scoped credential selection carried separately from core's provider-agnostic request.
    /// The executor resolves it at admission and freezes only the resulting credential id.
    pub auth: Option<broca_protocol::AuthRequest>,
    /// The session-scoped writer the run records to (shared, `record()` is `&self`).
    pub writer: Arc<WalHandle>,
    pub kind: RunKind,
    /// Omit the derived session-store projection while retaining the authoritative WAL.
    pub skip_materialization: bool,
    pub control: RunControl,
    /// The checkpoint-plus-tail view used to resolve admission fields. Core validates its
    /// explicit head against the writer before reconstruction.
    pub prior_snapshot: broca_core::PriorWalSnapshot,
    /// The ADMISSION-ACK: the executor fires this exactly once when the run is durably
    /// ADMITTED — `Ok` after `RunStarted` is fsynced (passed into `run_loop_on_writer`), or
    /// `Err` if the executor fails to RESOLVE the run (build the provider) BEFORE writing
    /// any `RunStarted` (so a resolution failure is a typed error, never a ghost run). The
    /// actor awaits it to reply `active{run_id}` only after the run is durable. The Ok value
    /// is the RESOLVED run_id — the original run_id on a resume (the loop continues under
    /// it), or `req.run_id` on a fresh/appended episode. `None` for a fake/test executor
    /// that doesn't model admission.
    pub on_admitted: Option<oneshot::Sender<Result<RunId, RunError>>>,
}

impl RunJob {
    fn protected_input(&self) -> Vec<broca_protocol::Message> {
        self.control
            .steer
            .as_ref()
            .map_or_else(|| self.req.input.clone(), |inbox| inbox.protected_inputs())
    }

    pub fn has_media_input(&self) -> bool {
        broca_provider::person_media::references(&self.protected_input())
            .next()
            .is_some()
    }
    /// Validate against the provider that will actually admit the run, then publish
    /// held bytes before RunStarted can name any reference.
    pub fn admit_media(
        &self,
        provider: &dyn broca_core::Provider,
        uploads: Option<&broca_core::media_uploads::Uploads>,
    ) -> Result<(), RunError> {
        let protected = self.protected_input();
        if broca_provider::person_media::references(&protected)
            .next()
            .is_none()
        {
            return Ok(());
        }
        let frozen = serde_json::from_value::<broca_provider::wire::FrozenRenderConfig>(
            provider.render_config_blob(),
        )
        .ok();
        let model = format!(
            "{}/{}",
            self.req.model.provider_module_id, self.req.model.model_id
        );
        crate::person_media::check(
            frozen.as_ref().and_then(|frozen| frozen.media.as_ref()),
            &model,
            &protected,
        )?;
        crate::person_media::publish(uploads, &self.req.session, &self.media)
    }

    /// Apply this send's projection policy to an available store. Only an explicit one-shot send
    /// sets `skip_materialization`; every omitted, false, or recovery-defaulted policy keeps the
    /// existing materialization behavior.
    pub fn materialized_store(
        &self,
        store: Arc<dyn broca_core::SessionStore>,
    ) -> Option<Arc<dyn broca_core::SessionStore>> {
        (!self.skip_materialization).then_some(store)
    }
}

/// Drives ONE agentic run to a terminal under an already-held lease, emitting its
/// OutputStream to `out`. Neither acquires nor releases a lease (the actor owns the lease
/// for the whole session lifetime). Implementors: the real loop adapter, and the test
/// fake.
#[async_trait]
pub trait RunExecutor: Send + Sync + 'static {
    /// Quote current catalog media facts for enqueue and read-only fresh-send preflight.
    /// Actual admission checks again against the provider's frozen render config.
    async fn media_profile(
        &self,
        _request: &broca_protocol::request::SubmissionRequest,
    ) -> Result<Option<Box<broca_provider::media_profile::MediaProfile>>, RunError> {
        Ok(None)
    }
    /// The scoped question adapter shared by approval waits and withdrawals after a run ends.
    /// Without it, calls waiting for approval close with ordinary not-dispatched results;
    /// no approval-feature WAL records or question operations are written by the actor.
    fn elicitation_plane(&self) -> Option<Arc<dyn broca_core::tool_plane::ElicitationPlane>> {
        None
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

    /// Runs only with the session idle and its single writer held by the actor.
    async fn apply_idle_compaction(
        &self,
        _session: &broca_protocol::BindIdentity,
        _writer: &broca_core::WalHandle,
        _records: Vec<broca_wal::WalRecord>,
        _ready: &broca_core::compaction_provider::notice::ViewReady,
    ) -> Result<bool, String> {
        Ok(false)
    }

    /// The same session-scoped hook adapter used by the run loop. Input hooks execute in
    /// the actor before a send writes or interrupts anything, never under the steer lock.
    fn hook_plane(&self) -> Option<Arc<dyn broca_core::HookPlane>> {
        None
    }

    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError>;

    /// Called by the actor's CLOSE teardown when a session ends (the lineage idles to
    /// close), so an executor holding per-session resources keyed off a SHARED handle can
    /// release them. The real executor uses it to send a route GOODBYE for the session so a
    /// shared subc consumer connection doesn't accumulate stale route channels across
    /// sessions. Default no-op (the in-process fakes hold nothing per session). Best-effort:
    /// a failure here must not block the close.
    async fn on_session_close(&self, _session: &BindIdentity) {}

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
        _config: &broca_wal::RunConfig,
        _prefix_tokens: u64,
        _ttl_ms: u64,
    ) -> Result<broca_core::keep_warm::WarmCosts, String> {
        Err("pricing_unavailable".into())
    }

    /// Price the returned Responses usage in the same units as `warm_costs`.
    /// Drivers without usage pricing retain the pre-send estimate.
    fn responses_warm_cost(
        &self,
        _config: &broca_wal::RunConfig,
        _usage: broca_protocol::Usage,
    ) -> Result<f64, String> {
        Err("pricing_unavailable".into())
    }

    /// Check quota before an automatic cache-refresh request. An error prevents the send;
    /// the tool-wait keep-warm scheduler logs the returned error code once and stops warming
    /// for the rest of this tool wait.
    async fn warm_quota(
        &self,
        _config: &broca_wal::RunConfig,
        _headers: &std::collections::BTreeMap<String, String>,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Override expiry-based timing with an interval after the last request was sent,
    /// for providers that do not document whether a cached read extends retention.
    fn warm_after_send_ms(&self, _config: &broca_wal::RunConfig) -> Option<u64> {
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
            let rendered = provider.render_request(&recorded.opts);
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
    };
    provider
        .warm(&bytes)
        .await
        .map_err(|failure| match failure {
            broca_core::WarmFailure::Unsupported(reason) => WarmError::Unsupported(reason),
            broca_core::WarmFailure::AccountChanged { frozen, served } => {
                WarmError::AccountChanged { frozen, served }
            }
            broca_core::WarmFailure::Provider(error) => WarmError::Provider(Box::new(error)),
        })
}

/// The request a cache warm re-sends.
pub enum WarmBase {
    /// The exact bytes the running run's transport sent for its most recent request.
    InFlight(broca_core::SentRequest),
    /// The most recent request the WAL records, to be rendered again and checked against the
    /// digest its step recorded.
    Recorded(Box<broca_core::RecordedRequest>),
}

impl WarmBase {
    /// The digest of the request this base stands for.
    pub fn request_body_sha256(&self) -> &str {
        match self {
            WarmBase::InFlight(sent) => &sent.sha256,
            WarmBase::Recorded(recorded) => &recorded.request_body_sha256,
        }
    }
}

/// One cache warm the coordinator asks the executor to send.
pub struct WarmJob {
    pub session: BindIdentity,
    /// The config the request was sent with: the provider is built from it.
    pub config: broca_wal::RunConfig,
    pub base: WarmBase,
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

impl WarmError {
    pub(crate) fn variant_name(&self) -> &'static str {
        match self {
            Self::Unsupported(_) => "Unsupported",
            Self::PrefixUnavailable(_) => "PrefixUnavailable",
            Self::AccountChanged { .. } => "AccountChanged",
            Self::Provider(_) => "Provider",
            Self::Unavailable(_) => "Unavailable",
        }
    }
}
