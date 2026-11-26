//! The serve loop: register broca as a subc module, then dispatch inbound
//! session.*/run.* route requests to the `broca_module::serve::ServeHandler`.
//!
//! This file reads and routes inbound module-protocol traffic for a connection. It performs
//! the initial handshake, handles control frames on channel 0, runs each route request in
//! its own bounded task so one slow request does not block others, and passes request
//! handling to `broca_module::serve::ServeHandler`.
//!
//! ## The streaming subscribe route (the novel piece)
//!
//! `session.subscribe` is NOT a request/response op — it is a held-open STREAM. The inbound
//! subscribe Request opens the stream; the adapter attaches a `ServeHandler::subscribe`
//! sink and spawns a task that emits one `StreamData` frame per `Delivery` (control unit /
//! display event) on the route channel + the request's corr, terminated by a `StreamEnd`
//! frame when the run reaches a terminal. The request's CREDIT is held until `StreamEnd`,
//! which is why subscribe MUST be a dedicated route (it blocks other ops on that channel
//! until terminal) — send/retract/cancel/status each get their own route.
//!
//! ## Lane labels, not subc scheduling
//!
//! Control `StreamData` rides `Interactive`, display rides `Background` — but subc does NOT
//! priority-schedule the module→client egress (it is one bounded best-effort FIFO per
//! connection; a full buffer CLOSES the client). So these priorities are LANE LABELS the
//! client reads ("reliable control vs lossy display"), not a subc scheduling input. Because
//! control + display share one route's egress, a display flood can fill the FIFO and close
//! the client route, taking live control down with it — which is fine ONLY because control
//! reliability is durable+replayable (the client resubscribes from its last durable cursor
//! and replays the log projection), never guaranteed-live. The egress-closed → clean
//! subscriber teardown → client-resubscribes path IS the reliability mechanism. (v-next, if
//! display-flood-disrupting-live-control ever measures as a real problem, the fix is
//! splitting display onto its own route; v1 keeps the unified stream + replay-covers-it.)

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::stall::{self, Phase};
use broca_diagnostics::{Receiver as EgressReceiver, Sender as EgressSender};
use broca_module::serve::{
    ServeError, ServeHandler, SessionImportRequest, SessionReadRequest, SubscribeFrom,
};
use broca_module::{Delivery, LineageRegistry};
use broca_protocol::BindIdentity as BrocaBindIdentity;
use broca_wire::encode_live_subscribe_display;
use subc_protocol::session::{
    ModuleControlCommand, ModuleControlRequest, ModuleControlResponse,
    MODULE_CONTROL_OP_HEALTH_CHECK,
};
use subc_protocol::{
    BindIdentity as SubcBindIdentity, ErrorBody, Flags, Frame, FrameType, ModuleHelloAckBody,
    ModuleHelloBody, Priority, PROTOCOL_VERSION,
};
use subc_transport::{read_frame, write_frame, FrameIoError};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::manifest::{
    OP_BASELINE, OP_CANCEL, OP_FLUSH_PREFIX, OP_HEAD, OP_IMPORT, OP_INSTALL_VIEW, OP_MEMORY_REPORT,
    OP_READ, OP_REFRESH, OP_REFRESH_POLICY, OP_RELEASE, OP_REQUEST_DUMP, OP_RESTORE_FROM_BACKUP,
    OP_RETRACT, OP_ROLE_DESCRIBE, OP_RUN_ADMISSION, OP_RUN_EVENT_GET, OP_RUN_RESULT,
    OP_RUN_TOOL_CALLS, OP_SEND, OP_SEND_STATUS, OP_SESSION_DELETE, OP_SESSION_SEND_STATUS,
    OP_STATUS, OP_SUBSCRIBE, OP_TAG_MAP, OP_VAULT_AUDIT, OP_WARM,
};
use crate::wire::{
    CancelParams, FromWireExt, ImportParams, ImportResult, RetractParams, RouteRequest, SendParams,
    SendParamsExt, SendResult, StatusParams, SubscribeParams,
};

const HELLO_CORR: u64 = 1;
const EGRESS_BUFFER: usize = 64;
const MAX_INFLIGHT_ROUTE_REQUESTS: usize = 64;
const DISPATCH_LANE_SWEEP_MIN: usize = 64;
const DROP_LOG_KEY_LIMIT: usize = 256;

/// Start reader-census coverage at daemon startup, before accepting any routes.
pub fn initialize_session_readers() {
    crate::session_readers::initialize();
}

// Serve-side drops are inbound daemon frames rejected by broca's route-binding guard below.
// They are distinct from broca_subc::FRAME_DROPS, which counts frames dropped on the
// client-side outbound routes.
static SERVE_FRAMES_DROPPED_UNBOUND_CHANNEL: AtomicU64 = AtomicU64::new(0);
static SERVE_FRAMES_DROPPED_STALE_EPOCH: AtomicU64 = AtomicU64::new(0);
static MANAGEMENT_OPERATION_COUNTS: OnceLock<Vec<(String, AtomicU64)>> = OnceLock::new();

fn management_operation_counters() -> &'static [(String, AtomicU64)] {
    MANAGEMENT_OPERATION_COUNTS.get_or_init(|| {
        let operations = crate::manifest::management_operations()
            .into_iter()
            .map(|operation| operation.name)
            .chain(
                crate::manifest::observability_surfaces()
                    .into_iter()
                    .map(|surface| surface.name),
            );
        operations
            .map(|operation| (operation, AtomicU64::new(0)))
            .collect()
    })
}

fn count_management_operation(operation: &str) {
    if let Some((_, count)) = management_operation_counters()
        .iter()
        .find(|(name, _)| name == operation)
    {
        count.fetch_add(1, Ordering::Relaxed);
    }
}

fn management_operation_counts() -> serde_json::Value {
    serde_json::Value::Object(
        management_operation_counters()
            .iter()
            .map(|(operation, count)| {
                (
                    operation.clone(),
                    serde_json::json!(count.load(Ordering::Relaxed)),
                )
            })
            .collect(),
    )
}

tokio::task_local! {
    // Successful calls need no owned outcome string, including high-rate run.status polls.
    static REQUEST_OUTCOME: std::cell::RefCell<Option<String>>;
}

fn record_request_outcome(outcome: &str) {
    let _ = REQUEST_OUTCOME.try_with(|slot| *slot.borrow_mut() = Some(outcome.to_owned()));
}

fn caller_module(bind: &BindObservation) -> Option<&str> {
    match bind.principal.as_ref() {
        Some(broca_protocol::BindPrincipal::Reserved { module_id }) => Some(module_id),
        _ => None,
    }
}

fn request_operation(body: &[u8]) -> String {
    #[derive(serde::Deserialize)]
    struct Operation {
        method: String,
    }
    serde_json::from_slice::<Operation>(body)
        .map(|operation| operation.method)
        .unwrap_or_else(|_| "<invalid>".into())
}

async fn log_served_request<F: std::future::Future>(
    operation: &str,
    session: &BrocaBindIdentity,
    caller_module_id: Option<&str>,
    phase: &str,
    future: F,
) -> F::Output {
    let started = tokio::time::Instant::now();
    REQUEST_OUTCOME
        .scope(std::cell::RefCell::new(None), async {
            let (output, actor_opened) =
                broca_module::registry::with_request_actor_opened(catch_panic(future)).await;
            if output.is_err()
                && !TERMINAL_SENT
                    .try_with(|sent| sent.load(Ordering::SeqCst))
                    .unwrap_or(false)
            {
                // reply_on_panic sends the wire code "transient" when no reply was claimed.
                record_request_outcome("transient");
            }
            let elapsed_us = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
            // Compute the session-address hash only when a line will be emitted; log the hash rather than the raw session path or request body.
            if elapsed_us > 2_000_000
                || tracing::enabled!(target: "requests", tracing::Level::DEBUG)
            {
                REQUEST_OUTCOME.with(|outcome| {
                    emit_request_log(
                        operation,
                        &broca_core::session_addr(session),
                        caller_module_id,
                        outcome.borrow().as_deref().unwrap_or("ok"),
                        phase,
                        actor_opened,
                        elapsed_us,
                    );
                });
            }
            output.unwrap_or_else(|()| std::panic::resume_unwind(Box::new(())))
        })
        .await
}

fn emit_request_log(
    operation: &str,
    session: &str,
    caller_module_id: Option<&str>,
    outcome: &str,
    phase: &str,
    actor_opened: bool,
    elapsed_us: u64,
) {
    let elapsed_ms = elapsed_us / 1000;
    if elapsed_us > 2_000_000 {
        tracing::warn!(target: "requests", operation, session, caller_module_id, outcome, phase, actor_opened, elapsed_us, elapsed_ms, "served management request");
    } else {
        tracing::debug!(target: "requests", operation, session, caller_module_id, outcome, phase, actor_opened, elapsed_us, elapsed_ms, "served management request");
    }
}

// Keep the first keys seen rather than evicting them: this bounds log volume even if a peer
// cycles through many channel/epoch pairs, while the counters continue to capture every drop.
static DROP_LOG_KEYS: OnceLock<Mutex<HashSet<(u16, u32)>>> = OnceLock::new();

fn first_drop_for_log(channel: u16, epoch: u32) -> bool {
    let keys = DROP_LOG_KEYS.get_or_init(|| Mutex::new(HashSet::new()));
    let mut keys = match keys.lock() {
        Ok(keys) => keys,
        Err(poisoned) => poisoned.into_inner(),
    };
    if keys.contains(&(channel, epoch)) || keys.len() >= DROP_LOG_KEY_LIMIT {
        return false;
    }
    keys.insert((channel, epoch));
    true
}

/// A probe-cheap gauge of credential-vault reachability. The executor stamps it from vault
/// calls it already makes, so `health.check` remains an in-memory operation.
pub struct VaultGauge {
    /// NotInUse, Connected, Disconnected, or Broken.
    state: std::sync::atomic::AtomicU8,
    /// Unix epoch millis of the last connectivity-class vault error; 0 = never.
    last_error_ms: std::sync::atomic::AtomicI64,
    /// Unix epoch millis of the last COMPLETED vault round-trip; 0 = never.
    ///
    /// Without this the healthy reading is the only one a reader cannot age. A failure
    /// carries `last_error_ms`, so its staleness is visible; `connected` on its own says
    /// what the last call returned and nothing about when, which on an idle module is
    /// indistinguishable from a call made moments ago. That gap is what lets "the vault is
    /// fine" mean "nobody has asked it lately".
    last_ok_ms: std::sync::atomic::AtomicI64,
    /// True when the latest connectivity failure was recovered from the in-memory token cache.
    serving_cached: std::sync::atomic::AtomicBool,
}

impl VaultGauge {
    const NOT_IN_USE: u8 = 0;
    const CONNECTED: u8 = 1;
    const DISCONNECTED: u8 = 2;
    const BROKEN: u8 = 3;

    /// The gauge for a server whose auth source never uses the vault.
    pub fn not_in_use() -> Arc<Self> {
        Self::with_state(Self::NOT_IN_USE)
    }

    /// A configured vault that has not yet completed a successful call.
    pub fn disconnected() -> Arc<Self> {
        Self::with_state(Self::DISCONNECTED)
    }

    fn with_state(state: u8) -> Arc<Self> {
        Arc::new(Self {
            state: std::sync::atomic::AtomicU8::new(state),
            last_error_ms: std::sync::atomic::AtomicI64::new(0),
            last_ok_ms: std::sync::atomic::AtomicI64::new(0),
            serving_cached: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Stamp a successful vault round-trip, including an authoritative credential error.
    ///
    /// THIS GAUGE MEASURES REACHABILITY, NEVER CREDENTIAL HEALTH, and the two are easy to
    /// confuse because only one of them is on the health probe. A `needs_reauth` reply is a
    /// SUCCESS here: the vault was asked and answered authoritatively, which is exactly what a
    /// working vault does when a credential is dead. So a session where every credential has
    /// expired reads `connected` continuously, and it is right to.
    ///
    /// The consequence, worth stating because a reader looking at a green probe will otherwise
    /// draw the wrong conclusion: nothing in broca's telemetry distinguishes "credentials are
    /// fine" from "credentials are dying and the vault is reporting it correctly". That
    /// distinction belongs to the credentials module's own surface, which holds the record
    /// state; inferring it from reachability here would mean guessing at another module's data
    /// from the shape of our round-trips.
    pub fn stamp_ok(&self) {
        self.last_ok_ms
            .store(now_epoch_ms(), std::sync::atomic::Ordering::Relaxed);
        self.serving_cached
            .store(false, std::sync::atomic::Ordering::Relaxed);
        self.state
            .store(Self::CONNECTED, std::sync::atomic::Ordering::Relaxed);
    }

    /// Stamp a connectivity-class failure for which no cached credential was served.
    pub fn stamp_error(&self) {
        self.stamp_broken(false);
    }

    /// Mark the vault reachable only if no call outcome has stamped the gauge yet. The
    /// catalog readiness watch uses this so its registration signal can never overwrite
    /// the stronger truth of an actual vault call's outcome.
    pub fn stamp_ok_if_disconnected(&self) {
        let _ = self.state.compare_exchange(
            Self::DISCONNECTED,
            Self::CONNECTED,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Stamp a connectivity-class failure recovered by an eligible cached credential.
    pub fn stamp_serving_cached(&self) {
        self.stamp_broken(true);
    }

    fn stamp_broken(&self, serving_cached: bool) {
        self.last_error_ms
            .store(now_epoch_ms(), std::sync::atomic::Ordering::Relaxed);
        self.serving_cached
            .store(serving_cached, std::sync::atomic::Ordering::Relaxed);
        self.state
            .store(Self::BROKEN, std::sync::atomic::Ordering::Relaxed);
    }

    fn read(&self) -> VaultReading {
        VaultReading {
            state: self.state.load(std::sync::atomic::Ordering::Relaxed),
            last_error_ms: self
                .last_error_ms
                .load(std::sync::atomic::Ordering::Relaxed),
            last_ok_ms: self.last_ok_ms.load(std::sync::atomic::Ordering::Relaxed),
            serving_cached: self
                .serving_cached
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

/// One consistent-enough snapshot of the gauge for a single health response.
struct VaultReading {
    state: u8,
    last_error_ms: i64,
    /// 0 when no vault call has ever completed, which is NOT the same as a stale success:
    /// the catalog readiness watch can report the module live without any call having been
    /// made, so `connected` with 0 here means "registered, never dialled".
    last_ok_ms: i64,
    serving_cached: bool,
}

fn health_status(details: &[String]) -> subc_protocol::session::HealthStatus {
    if details.is_empty() {
        subc_protocol::session::HealthStatus::Ok
    } else {
        subc_protocol::session::HealthStatus::Degraded
    }
}

fn held_withdrawal_health_detail(withdrawals: &serde_json::Value) -> Option<String> {
    (withdrawals["failed"].as_u64().unwrap_or(0) > 0 || withdrawals.get("error").is_some())
        .then(|| "provider-held tool.withdraw failures need attention".into())
}

fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// A per-PROCESS launch nonce, computed once. Combined with a process-global counter it
/// makes every minted run_id unique across BOTH concurrent connections AND process
/// restarts — a fresh process (a new nonce) can never re-mint a run_id an earlier process
/// used. `{pid}-{start-nanos}`: the delimiter makes the two nonce components unambiguous;
/// the pid disambiguates processes alive at once and the start time disambiguates a
/// restarted pid. The global counter (below) guarantees in-process uniqueness regardless of
/// clock resolution, so the nonce only needs to be per-launch.
fn launch_nonce() -> &'static str {
    use std::sync::OnceLock;
    static NONCE: OnceLock<String> = OnceLock::new();
    NONCE.get_or_init(|| {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("{}-{nanos:x}", std::process::id())
    })
}

/// This process's launch id (the nonce above), for a holder name that must differ between
/// processes, such as the owner System One writes on a batch it holds.
pub fn process_launch_id() -> &'static str {
    launch_nonce()
}

/// Refuse an execute-class op whose bound project root no longer exists.
///
/// Directory-keyed sessions retain the legacy admission guard for `session.send` and
/// `session.import`: both create durable state under a folder-derived identity. The fixed
/// unscoped namespace is not a folder and is exempt from the directory check.
///
/// Every other op — cancel, status, read, subscribe, retract — only reads or ends something
/// that already exists, and must keep working when the root is gone. A paused run's only exit
/// is cancel, so refusing these would leave such a run with no way to end.
///
/// `retract` is permitted even though it WRITES, which looks like an inconsistency worth
/// tidying and is not. The invariant is not "no writes while the root is missing" but "no NEW
/// durable state under a root whose meaning is still provisional": a missing component may
/// later reappear as a symlink elsewhere, which re-points the key. A retract appends to a
/// lineage that already exists at that key and cannot create a second one, so it cannot split
/// a history across two keys. A send or an import can, which is the whole reason those two are
/// refused rather than every write.
///
/// The reported code deliberately matches the one the daemon uses when it refuses a bind for
/// the same reason, so a caller that already handles it sees no change when the refusal moves
/// from bind time to op time.
async fn execute_class_root_refusal(session: &BrocaBindIdentity) -> Option<String> {
    // The unscoped namespace is a storage key, not a directory, so this guard does not apply.
    if session.project_root.as_os_str()
        == std::ffi::OsStr::new(broca_protocol::session::UNSCOPED_SESSION_ROOT)
    {
        return None;
    }
    // `tokio::fs` runs the stat on the blocking pool: a slow or network filesystem must not
    // stall the runtime worker that also carries other sessions' frames and streams.
    if tokio::fs::metadata(&session.project_root).await.is_ok() {
        return None;
    }
    Some(format!(
        "project root does not exist: {}",
        session.project_root.display()
    ))
}

fn to_broca_identity(id: &SubcBindIdentity) -> BrocaBindIdentity {
    BrocaBindIdentity {
        project_root: broca_protocol::session::UNSCOPED_SESSION_ROOT.into(),
        harness: id.harness.clone(),
        session: id.session.clone(),
    }
}

#[path = "serve_accepting_drain.rs"]
mod accepting_drain;
#[path = "serve_hello.rs"]
mod hello;
#[path = "serve_run_ids.rs"]
mod run_ids;
mod send_plan;
#[cfg(test)]
use run_ids::mint_run_id;
use run_ids::run_id_for_import;
pub(crate) use run_ids::run_id_for_send;
use send_plan::{admit_send_plan, approve_scope_error, worker_scope_error};
#[cfg(test)]
#[path = "serve_project_identity_tests.rs"]
mod project_identity_tests;
#[path = "serve_refresh.rs"]
mod serve_refresh;
#[path = "serve_tag_read.rs"]
mod serve_tag_read;
use hello::{expect_hello_ack, send_hello};
pub use hello::{hello_manifest, register_unready, GatedRegistration};

/// What a `route.bind` said about its caller beyond the session identity. Both values are
/// recorded on the `RunStarted` of every send or import made over the route, and neither keys
/// anything: the same session bound with different observations is still one lineage.
#[derive(Debug, Clone, Default, PartialEq)]
struct BindObservation {
    /// Actual route target, independent of a role declaration on a management route.
    management: bool,
    agent_data: bool,
    /// Only llm-runner routes address a session. Other roles serve their own operations
    /// without opening a session or reading its history.
    role: RouteRole,
    /// The producer's registered project id, as the client supplied it (unattested).
    project_id: Option<String>,
    /// The daemon-stamped principal, converted without change. `None` when the bind carried
    /// none, which is what a daemon that predates principal stamping sends.
    principal: Option<broca_protocol::BindPrincipal>,
    /// Render once per bind so repeated status polls never allocate a census key.
    reader_principal: Option<Arc<str>>,
    /// The daemon's scope stamp on the bind, or why there is none. Every send over the route
    /// carries it to admission, which decides the owner check and the scope to freeze.
    scope: broca_protocol::SendScope,
    /// The session folder and project id on the bind's scope stamp, `None` on an unstamped
    /// bind. New scoped pairs take its workdir as their storage root; existing pairs keep
    /// their inventory root. Project id remains attribution only.
    stamped_project: Option<crate::scope_workdir::StampedProject>,
    /// What the consumer declared about reading tool replies, under either version's name for
    /// the reply format.
    consumer_format: crate::llm_tools::ConsumerFormat,
}

/// Preserve the scope identity, including the agent and run identifiers of an agent-run scope
/// and its opener. The scoped bind establishes that the daemon allowed
/// this opener to use the scope, either as its owner or as another module allowed to open
/// routes under it. `owner_authorized` describes the scope owner's authority, not the opener's;
/// that flag alone grants no permission, nor do agent delegation attributes.
fn to_bind_scope(
    stamp: &subc_protocol::scope::ScopeStamp,
    principal: Option<&subc_protocol::Principal>,
) -> broca_protocol::BindScope {
    let flow_id = stamp.attributes.flow_id.clone();
    let run_id = stamp.attributes.run_id.clone();
    let agent_id = run_id
        .as_ref()
        .or(flow_id.as_ref())
        .and(stamp.attributes.agent_id.as_ref())
        .cloned();
    let sender = flow_id
        .as_ref()
        .or(run_id.as_ref())
        .and(principal)
        .map(to_bind_principal);
    broca_protocol::BindScope {
        scope: broca_protocol::SessionScope {
            owner: to_bind_principal(&stamp.owner),
            agent_id_frozen: false,
            scope_ref: stamp.scope_ref.clone(),
            scope_epoch: stamp.scope_epoch,
            flow_id,
            sender,
            agent_id,
            run_id,
        },
        kind: match stamp.kind {
            subc_protocol::scope::ScopeKind::Head => broca_protocol::ScopeKind::Head,
            subc_protocol::scope::ScopeKind::Worker => broca_protocol::ScopeKind::Worker,
            subc_protocol::scope::ScopeKind::Ephemeral => broca_protocol::ScopeKind::Ephemeral,
            _ => broca_protocol::ScopeKind::Other,
        },
        parent: stamp.parent.as_ref().map(|parent| {
            Box::new(broca_protocol::ScopeParent {
                owner: to_bind_principal(&parent.owner),
                scope_ref: parent.scope_ref.clone(),
                scope_epoch: parent.scope_epoch,
            })
        }),
        parent_state: stamp.parent_state.map(|state| match state {
            subc_protocol::scope::ParentState::Linked => broca_protocol::ParentState::Linked,
            subc_protocol::scope::ParentState::Pending => broca_protocol::ParentState::Pending,
            subc_protocol::scope::ParentState::Ended => broca_protocol::ParentState::Ended,
        }),
        owner_authorized: stamp.owner_authorized,
    }
}

/// What a bind says about scopes, given whether the daemon advertises `scopes/v1`. A stamp is
/// kept whatever the advertisement says: only a daemon that admits scoped routes can stamp one.
fn bind_send_scope(
    stamp: Option<&subc_protocol::scope::ScopeStamp>,
    daemon_scopes: bool,
    principal: Option<&subc_protocol::Principal>,
) -> broca_protocol::SendScope {
    match stamp {
        Some(stamp) => broca_protocol::SendScope::Stamped(to_bind_scope(stamp, principal)),
        None if daemon_scopes => broca_protocol::SendScope::Unstamped,
        None => broca_protocol::SendScope::DaemonWithoutScopes,
    }
}

/// Carry the daemon's principal into broca's own type. The match is exhaustive on purpose: a
/// principal kind added to subc fails to compile here until someone decides how to record it.
fn to_bind_principal(principal: &subc_protocol::Principal) -> broca_protocol::BindPrincipal {
    match principal {
        subc_protocol::Principal::Reserved { module_id } => {
            broca_protocol::BindPrincipal::Reserved {
                module_id: module_id.clone(),
            }
        }
        subc_protocol::Principal::Direct => broca_protocol::BindPrincipal::Direct,
        subc_protocol::Principal::Unverified => broca_protocol::BindPrincipal::Unverified,
    }
}

/// Per-route binding state: which session a route channel is bound to, plus a per-channel
/// cancel for a live subscribe stream on that channel (cancelled on the route's GOODBYE so
/// an explicit unsubscribe tears the forward task down promptly).
struct Routes {
    drain: accepting_drain::Drain,
    /// The settings gate, when broca registered through it (`settings::gate`).
    gate: Option<Arc<crate::settings::gate::Gate>>,
    agent_data: Option<Arc<crate::agent_data::AgentDataService>>,
    /// System One, when this build serves it; a classifier bind is refused without it.
    system_one: Option<Arc<crate::system_one::SystemOne>>,
    llm_tools: Option<Arc<crate::llm_tools::LlmTools>>,
    // The bind observations (project id, principal) stay beside the session key rather than
    // inside it: BindIdentity also keys the actor registry, so folding them in would give one
    // session's log a separate actor per distinct observation, all writing the same file.
    by_channel: std::collections::HashMap<u16, (u32, BrocaBindIdentity, BindObservation)>,
    pending_binds: std::collections::HashMap<u16, (u32, CancellationToken)>,
    bind_tasks: JoinSet<BindCompletion>,
    subscribe_cancel: std::collections::HashMap<u16, Arc<SubscriptionSlot>>,
    agent_data_cancel: std::collections::HashMap<u16, CancellationToken>,
    dispatch_lanes: std::collections::HashMap<BrocaBindIdentity, Arc<DispatchLane>>,
    next_dispatch_lane_sweep: usize,
    /// Whether the daemon advertises `scopes/v1` (from `server.describe`, or from this
    /// connection's HELLO_ACK when serve was started without a describe answer). Decides
    /// whether an unstamped bind means "no scope on this route" or "this daemon cannot stamp".
    daemon_scopes: bool,
}

impl Routes {
    fn new(daemon_scopes: bool) -> Self {
        Self {
            drain: accepting_drain::Drain::default(),
            gate: None,
            agent_data: None,
            system_one: None,
            llm_tools: None,
            by_channel: std::collections::HashMap::new(),
            pending_binds: std::collections::HashMap::new(),
            bind_tasks: JoinSet::new(),
            subscribe_cancel: std::collections::HashMap::new(),
            agent_data_cancel: std::collections::HashMap::new(),
            dispatch_lanes: std::collections::HashMap::new(),
            next_dispatch_lane_sweep: DISPATCH_LANE_SWEEP_MIN,
            daemon_scopes,
        }
    }

    fn reserve_dispatch(&mut self, session: &BrocaBindIdentity) -> DispatchTicket {
        self.sweep_dispatch_lanes_if_due();
        self.dispatch_lanes
            .entry(session.clone())
            .or_insert_with(|| Arc::new(DispatchLane::new()))
            .reserve()
    }

    fn route_closed(&mut self, channel: u16, epoch: u32) {
        if self
            .pending_binds
            .get(&channel)
            .is_some_and(|(pending_epoch, _)| *pending_epoch == epoch)
        {
            if let Some((_, cancel)) = self.pending_binds.remove(&channel) {
                cancel.cancel();
            }
        }
        // A pending replacement and the published route can share a channel, but a GOODBYE
        // names only one generation. Closing either must leave the other generation intact.
        if !self
            .by_channel
            .get(&channel)
            .is_some_and(|(published_epoch, _, _)| *published_epoch == epoch)
        {
            return;
        }
        let departed_session = self
            .by_channel
            .remove(&channel)
            .map(|(_, session, _)| session);
        if let Some(cancel) = self.subscribe_cancel.remove(&channel) {
            cancel.route.cancel();
        }
        if let Some(cancel) = self.agent_data_cancel.remove(&channel) {
            cancel.cancel();
        }
        if let Some(session) = departed_session {
            self.evict_dispatch_lane_if_idle(&session);
        }
        self.next_dispatch_lane_sweep = self
            .next_dispatch_lane_sweep
            .min(dispatch_lane_sweep_target(self.dispatch_lanes.len()));
    }

    fn evict_dispatch_lane_if_idle(&mut self, session: &BrocaBindIdentity) {
        if self
            .dispatch_lanes
            .get(session)
            .is_some_and(dispatch_lane_is_idle)
        {
            self.dispatch_lanes.remove(session);
        }
    }

    fn sweep_dispatch_lanes_if_due(&mut self) {
        if self.dispatch_lanes.len() < self.next_dispatch_lane_sweep {
            return;
        }
        self.dispatch_lanes
            .retain(|_, lane| !dispatch_lane_is_idle(lane));
        // The next scan waits for the map to grow to at least twice the retained set (or the
        // small initial floor), amortizing this O(n) backstop across O(n) lane insertions.
        self.next_dispatch_lane_sweep = dispatch_lane_sweep_target(self.dispatch_lanes.len());
    }
}

fn dispatch_lane_is_idle(lane: &Arc<DispatchLane>) -> bool {
    // `Routes` is mutated only by the serve-loop task, so no reservation can race this check.
    // Every outstanding DispatchTicket or DispatchTurn owns an Arc clone; a strong count of one
    // therefore proves the map is the sole owner and eviction cannot split same-session FIFO.
    // A ticket's own Arc also keeps the watch sender alive while `wait` is parked, preserving its
    // "lane lives as long as its tickets" expectation even when unrelated idle lanes are swept.
    Arc::strong_count(lane) == 1
}

fn dispatch_lane_sweep_target(retained: usize) -> usize {
    retained.saturating_mul(2).max(DISPATCH_LANE_SWEEP_MIN)
}

struct DispatchLane {
    next_ticket: AtomicU64,
    turn: watch::Sender<u64>,
}

impl DispatchLane {
    fn new() -> Self {
        let (turn, _) = watch::channel(0);
        Self {
            next_ticket: AtomicU64::new(0),
            turn,
        }
    }

    fn reserve(self: &Arc<Self>) -> DispatchTicket {
        DispatchTicket {
            ticket: self.next_ticket.fetch_add(1, Ordering::Relaxed),
            lane: self.clone(),
        }
    }
}

struct DispatchTicket {
    ticket: u64,
    lane: Arc<DispatchLane>,
}

impl DispatchTicket {
    async fn wait(self) -> DispatchTurn {
        let mut turn = self.lane.turn.subscribe();
        while *turn.borrow_and_update() != self.ticket {
            turn.changed()
                .await
                .expect("the dispatch lane lives as long as its tickets");
        }
        DispatchTurn {
            ticket: self.ticket,
            lane: self.lane,
        }
    }
}

struct DispatchTurn {
    ticket: u64,
    lane: Arc<DispatchLane>,
}

impl Drop for DispatchTurn {
    fn drop(&mut self) {
        self.lane.turn.send_replace(self.ticket + 1);
    }
}

enum PreparedRouteRequest {
    Request(RouteRequest),
    Subscribe {
        params: serde_json::Value,
        slot: Arc<SubscriptionSlot>,
    },
}

struct SubscriptionSlot {
    route: CancellationToken,
    current: std::sync::Mutex<Option<CancellationToken>>,
}

struct BindCompletion {
    address_refusal: Option<broca_store::session_address::SessionAddressError>,
    header: subc_protocol::EnvelopeHeader,
    channel: u16,
    epoch: u32,
    identity: BrocaBindIdentity,
    bind: BindObservation,
    cancel: CancellationToken,
    refusal: Result<Option<broca_core::AdoptionRefusal>, ServeError>,
    /// A `compaction-runner/v1` route's `bind` refusal (its encoded error body). The route is
    /// acknowledged only after the runner accepts the caller as the session's provider.
    role_refusal: Option<Vec<u8>>,
}

impl SubscriptionSlot {
    fn replace(&self) -> CancellationToken {
        let cancel = self.route.child_token();
        if let Some(previous) = self.current.lock().unwrap().replace(cancel.clone()) {
            previous.cancel();
        }
        cancel
    }
}

/// Why a served connection stopped without an error. main.rs appends it to the shutdown seal
/// line, so an operator can tell a planned stop (the daemon said GOODBYE) from the daemon's
/// connection simply ending (EOF or a reset) without guessing from timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServeEnd {
    /// The daemon sent GOODBYE on channel 0: a planned stop.
    Goodbye,
    /// The connection closed cleanly with no GOODBYE first.
    Eof,
    /// The socket was reset or aborted by the peer (see `peer_went_away`). Treated as a normal
    /// end, like EOF, but reported separately because it means the daemon went away abruptly.
    PeerReset,
}

impl ServeEnd {
    /// The cause label for a finished serve: the end's own label on success, `error` when serve
    /// failed. The error itself still propagates to the caller; this only names it for the log.
    pub fn cause_of<E>(result: &Result<ServeEnd, E>) -> &'static str {
        match result {
            Ok(end) => end.label(),
            Err(_) => "error",
        }
    }

    /// The short lowercase label used in log lines.
    pub fn label(self) -> &'static str {
        match self {
            ServeEnd::Goodbye => "goodbye",
            ServeEnd::Eof => "eof",
            ServeEnd::PeerReset => "reset",
        }
    }
}

/// Serve one authenticated subc connection with the default catalog and configuration.
pub async fn serve_connection<R, W, H>(
    read_half: R,
    write_half: W,
    module_id: &str,
    handler: Arc<H>,
    health_registry: Arc<LineageRegistry>,
    vault_gauge: Arc<VaultGauge>,
) -> Result<ServeEnd, ServeIoError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
    H: ServeHandler + 'static,
{
    serve_connection_with_catalog(
        read_half,
        write_half,
        module_id,
        handler,
        health_registry,
        vault_gauge,
        None,
    )
    .await
}

/// Real module serve path with the runtime catalog gauge used by channel-0 health checks.
#[allow(clippy::too_many_arguments)]
pub async fn serve_connection_with_catalog<R, W, H>(
    read_half: R,
    write_half: W,
    module_id: &str,
    handler: Arc<H>,
    health_registry: Arc<LineageRegistry>,
    vault_gauge: Arc<VaultGauge>,
    catalog_cache: Option<Arc<broca_catalog::CatalogCache>>,
) -> Result<ServeEnd, ServeIoError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
    H: ServeHandler + 'static,
{
    serve_connection_configured(
        read_half,
        write_half,
        module_id,
        handler,
        health_registry,
        vault_gauge,
        catalog_cache,
        crate::consumer_routes::ConsumerRoutes::default(),
        None,
        None,
        None,
    )
    .await
}

/// Serve with the process's boot configuration.
///
/// `daemon_capabilities` is the capability list the daemon answered `server.describe` with at
/// startup; `None` falls back to the list in this connection's HELLO_ACK, which the daemon
/// fills from the same source.
///
/// `scope_describer`, when given, is attached to this connection once it has registered, so
/// the tool plane can ask the daemon about a scope (`scope.describe` is answered only on a
/// module's registered connection), and detached when the connection ends.
#[allow(clippy::too_many_arguments)]
pub async fn serve_connection_configured<R, W, H>(
    read_half: R,
    write_half: W,
    module_id: &str,
    handler: Arc<H>,
    health_registry: Arc<LineageRegistry>,
    vault_gauge: Arc<VaultGauge>,
    catalog_cache: Option<Arc<broca_catalog::CatalogCache>>,
    consumer_routes: crate::consumer_routes::ConsumerRoutes,
    vault_auditor: Option<Arc<dyn crate::vault_audit::VaultAuditor>>,
    daemon_capabilities: Option<Vec<String>>,
    scope_describer: Option<Arc<broca_subc::ModuleScopeDescriber>>,
) -> Result<ServeEnd, ServeIoError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
    H: ServeHandler + 'static,
{
    serve_connection_with_agent_data(
        read_half,
        write_half,
        module_id,
        handler,
        health_registry,
        vault_gauge,
        catalog_cache,
        consumer_routes,
        vault_auditor,
        daemon_capabilities,
        scope_describer,
        None,
    )
    .await
}

/// Serve the internal `agent-data` session-WAL move service alongside the management surface.
#[allow(clippy::too_many_arguments)]
pub async fn serve_connection_with_agent_data<R, W, H>(
    read_half: R,
    write_half: W,
    module_id: &str,
    handler: Arc<H>,
    health_registry: Arc<LineageRegistry>,
    vault_gauge: Arc<VaultGauge>,
    catalog_cache: Option<Arc<broca_catalog::CatalogCache>>,
    consumer_routes: crate::consumer_routes::ConsumerRoutes,
    vault_auditor: Option<Arc<dyn crate::vault_audit::VaultAuditor>>,
    daemon_capabilities: Option<Vec<String>>,
    scope_describer: Option<Arc<broca_subc::ModuleScopeDescriber>>,
    agent_data: Option<Arc<crate::agent_data::AgentDataService>>,
) -> Result<ServeEnd, ServeIoError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
    H: ServeHandler + 'static,
{
    serve_connection_with_services(
        read_half,
        write_half,
        module_id,
        handler,
        health_registry,
        vault_gauge,
        catalog_cache,
        consumer_routes,
        vault_auditor,
        daemon_capabilities,
        scope_describer,
        agent_data,
        None,
    )
    .await
}

/// Serve the management surface with every optional service: `agent-data` and System One's
/// `classifier/v1` role, reached by routes that declare it.
#[allow(clippy::too_many_arguments)]
pub async fn serve_connection_with_services<R, W, H>(
    read_half: R,
    write_half: W,
    module_id: &str,
    handler: Arc<H>,
    health_registry: Arc<LineageRegistry>,
    vault_gauge: Arc<VaultGauge>,
    catalog_cache: Option<Arc<broca_catalog::CatalogCache>>,
    consumer_routes: crate::consumer_routes::ConsumerRoutes,
    vault_auditor: Option<Arc<dyn crate::vault_audit::VaultAuditor>>,
    daemon_capabilities: Option<Vec<String>>,
    scope_describer: Option<Arc<broca_subc::ModuleScopeDescriber>>,
    agent_data: Option<Arc<crate::agent_data::AgentDataService>>,
    system_one: Option<Arc<crate::system_one::SystemOne>>,
) -> Result<ServeEnd, ServeIoError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
    H: ServeHandler + 'static,
{
    serve_connection_with_llm_tools(
        read_half,
        write_half,
        module_id,
        handler,
        health_registry,
        vault_gauge,
        catalog_cache,
        consumer_routes,
        vault_auditor,
        daemon_capabilities,
        scope_describer,
        agent_data,
        system_one,
        None,
        None,
    )
    .await
}

/// Add the sessionless model-tool service without changing existing classifier callers.
#[allow(clippy::too_many_arguments)]
pub async fn serve_connection_with_llm_tools<R, W, H>(
    mut read_half: R,
    write_half: W,
    module_id: &str,
    handler: Arc<H>,
    health_registry: Arc<LineageRegistry>,
    vault_gauge: Arc<VaultGauge>,
    catalog_cache: Option<Arc<broca_catalog::CatalogCache>>,
    consumer_routes: crate::consumer_routes::ConsumerRoutes,
    vault_auditor: Option<Arc<dyn crate::vault_audit::VaultAuditor>>,
    daemon_capabilities: Option<Vec<String>>,
    scope_describer: Option<Arc<broca_subc::ModuleScopeDescriber>>,
    agent_data: Option<Arc<crate::agent_data::AgentDataService>>,
    system_one: Option<Arc<crate::system_one::SystemOne>>,
    llm_tools: Option<Arc<crate::llm_tools::LlmTools>>,
    registered: Option<GatedRegistration>,
) -> Result<ServeEnd, ServeIoError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
    H: ServeHandler + 'static,
{
    initialize_session_readers();
    let stall_state = stall::connection_state();
    let (tx, rx) = broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, stall_state.egress.clone());
    let mut writer = tokio::spawn(drain_writer(write_half, rx));
    let reader_lifetime = stall::reader_lifetime(stall_state.clone());

    // A connection-level cancel: when the frame loop ends (EOF / GOODBYE / error), this
    // cancels every live subscribe forward task so they drop their egress sender clones,
    // letting the writer task's channel close and `drain_writer` exit. Without it, a
    // lineage-scoped subscription's parked forward task would keep the egress open forever
    // and hang the connection on teardown.
    let conn_cancel = CancellationToken::new();
    let gate = registered
        .as_ref()
        .map(|registered| registered.gate.clone());
    let frame_loop = stall::READER.scope(
        stall_state,
        run_frame_loop(
            &mut read_half,
            tx.clone(),
            module_id,
            handler,
            health_registry.as_ref(),
            vault_gauge.as_ref(),
            catalog_cache.as_deref(),
            conn_cancel.clone(),
            &consumer_routes,
            vault_auditor,
            daemon_capabilities,
            scope_describer.as_deref(),
            agent_data,
            system_one,
            llm_tools,
            registered,
        ),
    );
    let mut writer_finished = false;
    let loop_result = tokio::select! {
        biased;
        // A reset reported by the write half closes the TCP connection, not one route
        // (RFC 9293 §3.10.7.4). Observe it before a request sees the closed egress queue.
        result = &mut writer => {
            writer_finished = true;
            match result {
                Ok(Ok(())) => Ok(ServeEnd::PeerReset),
                Ok(Err(error)) => Err(error),
                Err(error) => Err(ServeIoError(error.to_string())),
            }
        }
        result = frame_loop => result,
    };
    drop(reader_lifetime);
    // Before the egress queue can close: the describer and the gate hold senders on it, and the
    // gate outlives this connection (broca's claim minter keeps it).
    if let Some(describer) = &scope_describer {
        describer.detach();
    }
    if let Some(gate) = &gate {
        gate.detach();
    }
    conn_cancel.cancel();
    drop(tx);
    let writer_result = if writer_finished {
        Ok(Ok(()))
    } else {
        writer.await
    };

    match (loop_result, writer_result) {
        (Err(e), _) => Err(e),
        (Ok(end), Ok(Ok(()))) => Ok(end),
        (Ok(_), Ok(Err(e))) => Err(e),
        (Ok(_), Err(e)) => Err(ServeIoError(e.to_string())),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_frame_loop<R, H>(
    read_half: &mut R,
    writer: EgressSender<Frame>,
    module_id: &str,
    handler: Arc<H>,
    health_registry: &LineageRegistry,
    vault_gauge: &VaultGauge,
    catalog_cache: Option<&broca_catalog::CatalogCache>,
    conn_cancel: CancellationToken,
    consumer_routes: &crate::consumer_routes::ConsumerRoutes,
    vault_auditor: Option<Arc<dyn crate::vault_audit::VaultAuditor>>,
    daemon_capabilities: Option<Vec<String>>,
    scope_describer: Option<&broca_subc::ModuleScopeDescriber>,
    agent_data: Option<Arc<crate::agent_data::AgentDataService>>,
    system_one: Option<Arc<crate::system_one::SystemOne>>,
    llm_tools: Option<Arc<crate::llm_tools::LlmTools>>,
    registered: Option<GatedRegistration>,
) -> Result<ServeEnd, ServeIoError>
where
    R: AsyncRead + Unpin,
    H: ServeHandler + 'static,
{
    let image_references = system_one
        .as_ref()
        .is_some_and(|system| system.image_references());
    let (ack, gate) = match registered {
        None => {
            send_hello(&writer, module_id, consumer_routes, image_references).await?;
            let _phase = stall::enter(Phase::AwaitFrame).await;
            (expect_hello_ack(read_half).await?, None)
        }
        Some(registered) => (registered.ack, Some(registered.gate)),
    };
    // Registered: from here the daemon answers this connection's `scope.describe`.
    if let Some(describer) = scope_describer {
        describer.attach_observed(writer.clone());
    }
    let daemon_scopes = daemon_capabilities
        .as_deref()
        .unwrap_or(&ack.subc_capabilities)
        .iter()
        .any(|capability| capability == subc_protocol::scope::CAP_SCOPES_V1);

    let mut routes = Routes::new(daemon_scopes);
    routes.drain = accepting_drain::Drain::negotiated(&ack);
    routes.gate = gate;
    routes.agent_data = agent_data;
    routes.system_one = system_one;
    routes.llm_tools = llm_tools;
    let request_slots = Arc::new(Semaphore::new(MAX_INFLIGHT_ROUTE_REQUESTS));
    let mut request_tasks = JoinSet::new();
    if let Some(gate) = routes.gate.clone() {
        // The daemon's answers reach the gate through this loop, so it runs beside it. A refused
        // readiness ends the loop, and broca restarts.
        let provides = hello_manifest(module_id, consumer_routes, image_references).provides;
        let writer = writer.clone();
        request_tasks.spawn(async move { gate.finish_start(writer, provides).await });
    }

    // ONE READ STAYS IN FLIGHT ACROSS SELECT TURNS. `read_frame` is not cancellation-safe: it
    // reads the header and then the body in separate steps, and dropping it part-way loses the
    // bytes it already took. The loop used to start a fresh `read_frame` inside the select on
    // every turn, so whenever a request task finished while a frame was half-read, the partial
    // frame was dropped and the next read began mid-frame. Measured 2026-09-26 00:09:26Z:
    // broca exited 1 with "unsupported envelope version 32", a JSON space read as a header.
    // So the read future is created once, pinned here, polled by reference, and replaced only
    // after it has completed; losing the select race just leaves it pending for the next turn.
    let mut pending_read = std::pin::pin!(read_next_frame(read_half));
    let _ = stall::READER.try_with(|s| s.set_reader(Phase::AwaitFrame, 0, 0));

    let loop_result = loop {
        tokio::select! {
            _ = routes.drain.next_check() => {
                routes.drain.check(health_registry, &writer, routes.llm_tools.as_deref()).await;
            }
            joined = routes.bind_tasks.join_next(), if !routes.bind_tasks.is_empty() => {
                match joined.expect("guarded by !routes.bind_tasks.is_empty()") {
                    Ok(completed) => finish_bind(completed, &writer, &mut routes).await?,
                    // A panic inside the adoption check is caught in `authorize_bind` and
                    // answered as a transient refusal. Reaching here means the task failed
                    // outside it; that is still this bind's failure, not the connection's. Its
                    // pending entry is replaced by a rebind or removed by its GOODBYE.
                    Err(error) => tracing::error!(
                        target: "wire.ingress",
                        "bind authorization task failed; refusing only that bind: {error}"
                    ),
                }
            }
            joined = request_tasks.join_next(), if !request_tasks.is_empty() => {
                match joined.expect("guarded by !request_tasks.is_empty()") {
                    Ok(Ok(())) => {}
                    // An egress failure inside a request task means the connection itself is
                    // unusable, so it still ends the loop.
                    Ok(Err(e)) => break Err(e),
                    Err(e) if e.is_cancelled() => {}
                    // A panic in a request is answered inside its task (`reply_on_panic`).
                    // Reaching here means the task failed outside that guard. It is still
                    // confined to its own request: ending the loop would cut every session's
                    // streams and in-flight sends on this connection for one request's bug.
                    Err(e) => tracing::error!(
                        target: "wire.ingress",
                        "route request task failed; dropping only that request: {e}"
                    ),
                }
            }
            (frame, reader) = &mut pending_read => {
                let frame = match frame {
                    Ok(Some(frame)) => frame,
                    // The peer closed the connection, so an empty read is a normal end-of-stream.
                    Ok(None) => break Ok(ServeEnd::Eof),
                    // THE DAEMON GOING AWAY IS A NORMAL END, whichever way the socket reports it.
                    // A clean close reads as EOF (`Ok(None)` below), but at a daemon cut the
                    // daemon exits and the kernel RESETS the socket instead. Measured 2026-09-24,
                    // 16:25Z cut: "frame I/O error: Connection reset by peer (os error 54)",
                    // returned from here after the seal had already run cleanly, so main exited 1
                    // and the supervisor recorded a clean stop as a crash. Match the I/O error
                    // kind, never the message text. A torn frame (FrameIoError::UnexpectedEof) is
                    // NOT treated this way: that could be a protocol fault, and stays an error.
                    Err(FrameIoError::Io(error)) if peer_went_away(error.kind()) => {
                        break Ok(ServeEnd::PeerReset)
                    }
                    Err(error) => return Err(ServeIoError(error.to_string())),
                };
                stall::frame_context(0, frame.header.channel);
                // Log before route validation: a stale-epoch frame is discarded before its
                // method is decoded, so describe milestones alone cannot show that it arrived.
                tracing::debug!(
                    target: "wire.ingress",
                    channel = frame.header.channel, epoch = frame.header.epoch,
                    corr = frame.header.corr, ty = ?frame.header.ty,
                    "module frame received"
                );
                // A reply to the describer's own request is its alone; every other frame is
                // handled as before.
                let describe_reply = scope_describer.is_some_and(|describer| describer.on_frame(&frame))
                    || routes.gate.as_ref().is_some_and(|gate| gate.on_frame(&frame))
                    || routes.drain.on_reply(&frame, health_registry);
                if !describe_reply && !handle_frame(
                    frame,
                    &writer,
                    &mut routes,
                    &handler,
                    health_registry,
                    vault_gauge,
                                catalog_cache,
                    &conn_cancel,
                    &request_slots,
                    &mut request_tasks,
                    vault_auditor.clone(),
                )
                .await?
                {
                    // handle_frame stops the loop only for a channel-0 GOODBYE.
                    break Ok(ServeEnd::Goodbye);
                }
                pending_read.set(read_next_frame(reader));
                let _ = stall::READER.try_with(|s| s.set_reader(Phase::AwaitFrame, 0, 0));
            }
        }
    };

    conn_cancel.cancel();
    request_tasks.abort_all();
    while request_tasks.join_next().await.is_some() {}
    loop_result
}

/// Read one frame and hand the reader back with it, so the frame loop can keep a single read
/// future alive across select turns and start the next read from the reader it returns.
async fn read_next_frame<R>(reader: &mut R) -> (Result<Option<Frame>, FrameIoError>, &mut R)
where
    R: AsyncRead + Unpin,
{
    let frame = read_frame(reader).await;
    (frame, reader)
}

/// Returns Ok(false) to stop the loop, which it does only for a channel-0 GOODBYE; the frame
/// loop reports that stop as `ServeEnd::Goodbye`.
#[allow(clippy::too_many_arguments)]
async fn handle_frame<H>(
    frame: Frame,
    writer: &EgressSender<Frame>,
    routes: &mut Routes,
    handler: &Arc<H>,
    health_registry: &LineageRegistry,
    vault_gauge: &VaultGauge,
    catalog_cache: Option<&broca_catalog::CatalogCache>,
    conn_cancel: &CancellationToken,
    request_slots: &Arc<Semaphore>,
    request_tasks: &mut JoinSet<Result<(), ServeIoError>>,
    vault_auditor: Option<Arc<dyn crate::vault_audit::VaultAuditor>>,
) -> Result<bool, ServeIoError>
where
    H: ServeHandler + 'static,
{
    let _control = if frame.header.channel == 0 {
        Some(stall::enter(Phase::Control).await)
    } else {
        None
    };
    if frame.header.ty == FrameType::Goodbye
        && routes
            .pending_binds
            .get(&frame.header.channel)
            .is_some_and(|(epoch, _)| *epoch == frame.header.epoch)
    {
        routes.route_closed(frame.header.channel, frame.header.epoch);
        return Ok(true);
    }
    let bound_epoch = routes
        .by_channel
        .get(&frame.header.channel)
        .map(|(epoch, _, _)| *epoch);
    if frame.header.channel != 0 && bound_epoch != Some(frame.header.epoch) {
        // Unknown and stale route generations are dropped before dispatch so they cannot
        // invoke a session, produce an error, or tear down a replacement binding.
        let cause = if bound_epoch.is_some() {
            SERVE_FRAMES_DROPPED_STALE_EPOCH.fetch_add(1, Ordering::Relaxed);
            "stale_epoch"
        } else {
            SERVE_FRAMES_DROPPED_UNBOUND_CHANNEL.fetch_add(1, Ordering::Relaxed);
            "unbound_channel"
        };
        if first_drop_for_log(frame.header.channel, frame.header.epoch) {
            tracing::warn!(
                target: "wire.ingress",
                cause,
                frame_type = ?frame.header.ty,
                channel = frame.header.channel,
                epoch = frame.header.epoch,
                bound_epoch = ?bound_epoch,
                corr = frame.header.corr,
                "dropped module frame without a matching route binding"
            );
        }
        return Ok(true);
    }

    match frame.header.ty {
        FrameType::Ping if frame.header.channel == 0 => {
            let pong = Frame::build_with_version(
                frame.header.ver,
                FrameType::Pong,
                frame.header.flags,
                0,
                0,
                frame.header.corr,
                Vec::new(),
            )
            .map_err(|e| ServeIoError(e.to_string()))?;
            send(writer, pong).await?;
            Ok(true)
        }
        FrameType::Goodbye if frame.header.channel == 0 => Ok(false),
        FrameType::Push if frame.header.channel == 0 => {
            if let Some(gate) = routes.gate.clone().filter(|gate| gate.is_changed(&frame)) {
                request_tasks.spawn(async move { gate.on_changed(&frame.body).await });
                return Ok(true);
            }
            routes.drain.on_push(&frame, health_registry);
            Ok(true)
        }
        FrameType::Goodbye => {
            // A route GOODBYE: drop the channel binding and cancel any live subscribe
            // forward task on it (explicit unsubscribe → prompt teardown).
            routes.route_closed(frame.header.channel, frame.header.epoch);
            Ok(true)
        }
        FrameType::Request if frame.header.channel == 0 => {
            if let Some(answer) = routes.gate.as_ref().and_then(|gate| gate.review(&frame)) {
                send(writer, answer?).await?;
                return Ok(true);
            }
            handle_control_request(
                frame,
                writer,
                routes,
                health_registry,
                vault_gauge,
                catalog_cache,
            )
            .await?;
            Ok(true)
        }
        FrameType::Request => {
            let channel = frame.header.channel;
            let epoch = frame.header.epoch;
            let corr = frame.header.corr;
            let ver = frame.header.ver;
            // The channel/epoch guard above verified this binding, and routes cannot change
            // between that guard and this lookup.
            let Some((_, session, bind)) = routes.by_channel.get(&channel).cloned() else {
                unreachable!("route binding disappeared after the channel/epoch guard")
            };
            if bind.management
                && request_operation(&frame.body) == crate::findings_watch::OP_EVENTS_GET
            {
                count_management_operation(crate::findings_watch::OP_EVENTS_GET);
                match crate::findings_watch::answer(&frame.body) {
                    Ok(reply) => {
                        send_json_response(writer, ver, channel, epoch, corr, &reply).await?
                    }
                    Err(error) => send_error_body(writer, ver, channel, epoch, corr, error).await?,
                }
                return Ok(true);
            }
            if bind.management && request_operation(&frame.body) == crate::diagnostics::OP {
                count_management_operation(crate::diagnostics::OP);
                match crate::management_admission::admit(
                    crate::diagnostics::OP,
                    bind.principal.as_ref(),
                    &bind.scope,
                ) {
                    Ok(crate::management_admission::Admission::Diagnostics) => {}
                    Err(crate::management_admission::Refusal::Diagnostics(error)) => {
                        send_error_body(writer, ver, channel, epoch, corr, error).await?;
                        return Ok(true);
                    }
                    _ => unreachable!("diagnostics scope admission returned a different operation"),
                }
                let params = match crate::diagnostics::request_params(&frame.body) {
                    Ok(params) => params,
                    Err(error) => {
                        send_error_body(writer, ver, channel, epoch, corr, error).await?;
                        return Ok(true);
                    }
                };
                let now = now_epoch_ms();
                let catalog_age = crate::diagnostics::catalog_age(catalog_cache, now);
                let writer = writer.clone();
                let slots = request_slots.clone();
                request_tasks.spawn(reply_on_panic(
                    writer.clone(),
                    (ver, channel, epoch, corr),
                    async move {
                        let _permit = slots
                            .acquire_owned()
                            .await
                            .map_err(|e| ServeIoError(e.to_string()))?;
                        let reply = tokio::task::spawn_blocking(move || {
                            crate::diagnostics::serving_reply(
                                params,
                                bind.principal.as_ref(),
                                &bind.scope,
                                catalog_age,
                                now,
                            )
                        })
                        .await
                        .map_err(|e| ServeIoError(e.to_string()))?;
                        match reply {
                            Ok(reply) => {
                                send_json_response(&writer, ver, channel, epoch, corr, &reply).await
                            }
                            Err(error) => {
                                send_error_body(&writer, ver, channel, epoch, corr, error).await
                            }
                        }
                    },
                ));
                return Ok(true);
            }
            if bind.agent_data {
                let caller = bind
                    .principal
                    .as_ref()
                    .map(|p| p.as_owner_string())
                    .unwrap_or_default();
                let service = routes.agent_data.clone();
                let writer = writer.clone();
                let slots = request_slots.clone();
                let cancel = routes
                    .agent_data_cancel
                    .entry(channel)
                    .or_insert_with(|| conn_cancel.child_token())
                    .clone();
                let read_only = crate::agent_data::request_is_read_only(&frame.body);
                let reply_cancel = cancel.clone();
                request_tasks.spawn(reply_on_panic(writer.clone(), (ver, channel, epoch, corr), async move {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled(), if read_only => Ok(()),
                        result = async move {
                    let operation = async move {
                    let _permit = slots
                        .acquire_owned()
                        .await
                        .map_err(|e| ServeIoError(e.to_string()))?;
                    Ok::<_, ServeIoError>(crate::agent_data::dispatch(service.as_deref(), &caller, &frame.body).await)
                    };
                    let result = if read_only {
                        operation.await?
                    } else {
                        // The connection's JoinSet aborts its waiters on EOF.
                        // A separate task owns mutations but no egress sender,
                        // so EOF drops only this waiter and cannot tear a move.
                        tokio::spawn(operation).await.map_err(|e| ServeIoError(e.to_string()))??
                    };
                    // Mutating calls must finish even after the route closes:
                    // dropping one between door cover and export can strand a
                    // partially applied move. Only its unobserved reply is lost.
                    if reply_cancel.is_cancelled() { return Ok(()); }
                    match result {
                        Ok(reply) => {
                            send_json_response(&writer, ver, channel, epoch, corr, &reply).await
                        }
                        Err(error) => {
                            send_error_bytes(
                                &writer,
                                ver,
                                channel,
                                epoch,
                                corr,
                                serde_json::to_vec(&error)
                                    .map_err(|e| ServeIoError(e.to_string()))?,
                            )
                            .await
                        }
                    }
                        } => result,
                    }
                }));
                return Ok(true);
            }
            if matches!(bind.role, RouteRole::Classifier | RouteRole::Extractor) {
                system_one_route::spawn_request(
                    frame.body,
                    bind.clone(),
                    session,
                    routes.system_one.clone(),
                    writer,
                    request_tasks,
                    ((ver, channel, epoch, corr), routes.drain.sessionless_call()),
                );
                return Ok(true);
            }
            if bind.role == RouteRole::CompactionRunner {
                let handler = handler.clone();
                let writer = writer.clone();
                request_tasks.spawn(reply_on_panic(
                    writer.clone(),
                    (ver, channel, epoch, corr),
                    async move {
                        let operation = request_operation(&frame.body);
                        let caller = caller_module(&bind).map(str::to_owned);
                        let logged_session = session.clone();
                        log_served_request(
                            &operation,
                            &logged_session,
                            caller.clone().as_deref(),
                            "reply",
                            async move {
                                let answer = match broca_core::compaction_v2::protocol::wire::decode(
                                    &frame.body,
                                ) {
                                    Ok((method, params)) => {
                                        let params = serde_json::value::to_raw_value(&params)
                                            .expect("decoded params re-encode");
                                        handler
                                            .compaction_runner(&session, caller, method, params)
                                            .await
                                    }
                                    Err(refusal) => Err(refusal),
                                };
                                match answer {
                                    Ok(body) => {
                                        send_response_bytes(
                                            &writer,
                                            ver,
                                            channel,
                                            epoch,
                                            corr,
                                            body.get().as_bytes().to_vec(),
                                        )
                                        .await
                                    }
                                    Err(refusal) => {
                                        send_error_bytes(
                                            &writer,
                                            ver,
                                            channel,
                                            epoch,
                                            corr,
                                            serde_json::to_vec(&refusal)
                                                .expect("refusals encode"),
                                        )
                                        .await
                                    }
                                }
                            },
                        )
                        .await
                    },
                ));
                return Ok(true);
            }
            if matches!(bind.role, RouteRole::ToolProvider | RouteRole::Permissions) {
                let service = routes.llm_tools.clone();
                let in_flight =
                    (bind.role == RouteRole::ToolProvider).then(|| routes.drain.sessionless_call());
                let writer = writer.clone();
                request_tasks.spawn(reply_on_panic(
                    writer.clone(),
                    (ver, channel, epoch, corr),
                    async move {
                        let _in_flight = in_flight;
                        let operation = request_operation(&frame.body);
                        let caller = caller_module(&bind).map(str::to_owned);
                        log_served_request(
                            &operation,
                            &session,
                            caller.as_deref(),
                            "reply",
                            async move {
                                let answer = match service {
                                    Some(service) => tokio::spawn(async move {
                                        if bind.role == RouteRole::Permissions {
                                            service
                                                .permissions()
                                                .answer(&frame.body, bind.principal.as_ref())
                                                .await
                                        } else {
                                            service
                                                .answer_for_consumer_in_project(
                                                    &frame.body,
                                                    bind.principal.as_ref(),
                                                    &bind.scope,
                                                    bind.consumer_format,
                                                    bind.stamped_project.as_ref().and_then(
                                                        |stamp| stamp.project_id.as_deref(),
                                                    ),
                                                )
                                                .await
                                        }
                                    })
                                    .await
                                    .unwrap_or_else(|_| {
                                        Err(crate::system_one::refusal_body(
                                            &crate::system_one::internal_refusal(
                                                "model tool request failed",
                                            ),
                                        ))
                                    }),
                                    None => {
                                        Err(serde_json::to_vec(&subc_protocol::ErrorBody::new(
                                            "unknown_method",
                                            "model tools are not configured",
                                        ))
                                        .expect("error encodes"))
                                    }
                                };
                                match answer {
                                    Ok(body) => {
                                        send_response_bytes(
                                            &writer,
                                            ver,
                                            channel,
                                            epoch,
                                            corr,
                                            body.get().as_bytes().to_vec(),
                                        )
                                        .await
                                    }
                                    Err(body) => {
                                        send_error_bytes(&writer, ver, channel, epoch, corr, body)
                                            .await
                                    }
                                }
                            },
                        )
                        .await
                    },
                ));
                return Ok(true);
            }
            let req: RouteRequest = match serde_json::from_slice(&frame.body) {
                Ok(r) => r,
                Err(e) => {
                    log_served_request(
                        "<invalid>",
                        &session,
                        caller_module(&bind),
                        "reply",
                        send_error(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            "invalid_request",
                            &format!("request body not decodable: {e}"),
                        ),
                    )
                    .await?;
                    return Ok(true);
                }
            };
            stall::method_context(method_code(&req.method));
            if req.method == OP_ROLE_DESCRIBE {
                tracing::debug!(
                    target: "wire.describe",
                    stage = "received",
                    channel, epoch, corr,
                    session = %session.session,
                    permits = request_slots.available_permits(),
                    tasks = request_tasks.len(),
                    "role.describe progress"
                );
            }
            // Check before stream preparation too: replacing a subscription cancels its old
            // forwarder. Audit requests bypass ordinary dispatch but share this route guard.
            if req.method == OP_SEND {
                if let Err(refusal) = broca_core::scope_admission::require_flow_scope(
                    bind.principal.as_ref(),
                    &bind.scope,
                ) {
                    log_served_request(
                        &req.method,
                        &session,
                        caller_module(&bind),
                        "reply",
                        send_serve_error(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            &ServeError::ScopeRefused(refusal),
                        ),
                    )
                    .await?;
                    return Ok(true);
                }
            }
            if req.method == OP_ROLE_DESCRIBE {
                trace_describe("admitted", channel, epoch, corr);
            }
            if req.method == OP_VAULT_AUDIT {
                spawn_vault_audit_request(
                    req,
                    writer,
                    conn_cancel,
                    request_slots,
                    request_tasks,
                    ver,
                    channel,
                    epoch,
                    corr,
                    vault_auditor,
                    handler.clone(),
                    session,
                    bind,
                )
                .await?;
            } else if req.method == OP_MEMORY_REPORT && bind.management {
                spawn_memory_report_request(
                    req,
                    writer,
                    conn_cancel,
                    request_slots,
                    request_tasks,
                    (ver, channel, epoch, corr),
                    handler.clone(),
                    session,
                    bind,
                )
                .await?;
            } else {
                let prepared = prepare_route_request(req, routes, channel, conn_cancel);
                let dispatch_ticket = routes.reserve_dispatch(&session);
                if matches!(&prepared, PreparedRouteRequest::Request(req) if req.method == OP_ROLE_DESCRIBE)
                {
                    tracing::debug!(
                        target: "wire.describe",
                        stage = "reserved",
                        channel, epoch, corr,
                        ticket = dispatch_ticket.ticket,
                        "role.describe progress"
                    );
                }
                spawn_route_request(
                    prepared,
                    dispatch_ticket,
                    writer,
                    handler,
                    conn_cancel,
                    request_slots,
                    request_tasks,
                    ver,
                    channel,
                    epoch,
                    corr,
                    session,
                    bind,
                )
                .await?;
            }
            Ok(true)
        }
        _ => Ok(true),
    }
}

/// Channel-0 control: route.bind → record the channel→session binding + ack.
async fn handle_control_request(
    frame: Frame,
    writer: &EgressSender<Frame>,
    routes: &mut Routes,
    health_registry: &LineageRegistry,
    vault_gauge: &VaultGauge,
    catalog_cache: Option<&broca_catalog::CatalogCache>,
) -> Result<(), ServeIoError> {
    let request: ModuleControlRequest = match serde_json::from_slice(&frame.body) {
        Ok(request) => request,
        Err(error) => {
            tracing::warn!(
                target: "wire",
                corr = frame.header.corr,
                error = %error,
                "rejected undecodable channel-0 control request"
            );
            return send_error(
                writer,
                frame.header.ver,
                0,
                0,
                frame.header.corr,
                "invalid_request",
                &error.to_string(),
            )
            .await;
        }
    };
    match request {
        ModuleControlRequest::RouteBind {
            route_channel,
            epoch,
            identity,
            principal,
            scope,
            target,
            role_versions,
            consumer_capabilities,
            ..
        } => {
            stall::method_context(1);
            let role = match bind_target_role(
                &target,
                role_versions.as_ref(),
                routes.system_one.is_some(),
                routes
                    .system_one
                    .as_ref()
                    .is_some_and(|service| service.serves_extractor()),
            ) {
                Ok(role) => role,
                Err(message) => {
                    return send_error(
                        writer,
                        frame.header.ver,
                        0,
                        0,
                        frame.header.corr,
                        ROLE_VERSIONS_UNSUPPORTED,
                        &message,
                    )
                    .await;
                }
            };
            // Keep the daemon's principal with the channel so every episode this route starts
            // records who started it. A session may later adopt a scope only if the scope's
            // owner is the principal on every earlier episode, and an episode recorded without
            // one can never satisfy that.
            let principal_observation = principal.as_ref().map(to_bind_principal);
            let bind = BindObservation {
                management: matches!(target, subc_protocol::RouteTarget::ManagementSurface { .. }),
                agent_data: matches!(&target, subc_protocol::RouteTarget::InternalService { service_id, .. }
                    if service_id == broca_wire::agent_data::SERVICE_ID),
                role: if matches!(target, subc_protocol::RouteTarget::ManagementSurface { .. })
                    || (matches!(target, subc_protocol::RouteTarget::ToolProvider { .. })
                        && role == RouteRole::ToolProvider)
                {
                    role
                } else {
                    RouteRole::LlmRunner
                },
                // The 0.34 bind carries no untrusted project claim. Authenticated project
                // authority remains on the separate scope/stamped-project path below.
                project_id: None,
                reader_principal: Some(crate::session_readers::principal_label(
                    principal_observation.as_ref(),
                )),
                principal: principal_observation,
                scope: bind_send_scope(scope.as_ref(), routes.daemon_scopes, principal.as_ref()),
                stamped_project: scope
                    .as_ref()
                    .map(crate::scope_workdir::StampedProject::from_stamp),
                consumer_format: crate::llm_tools::ConsumerFormat::from_capabilities(
                    consumer_capabilities.as_deref(),
                ),
            };
            if !serves_bind_target(&target, bind.role, routes.llm_tools.is_some()) {
                return send_error(
                    writer,
                    frame.header.ver,
                    0,
                    0,
                    frame.header.corr,
                    "invalid_request",
                    "broca does not serve this route target",
                )
                .await;
            }
            let identity = to_broca_identity(&identity);
            // Sessionless roles use the stamped caller and scope, not a session identity.
            if let Err(error) = if bind.agent_data || bind.role.is_sessionless() {
                Ok(())
            } else {
                BrocaBindIdentity::new("", &identity.harness, &identity.session).validate_keyable()
            } {
                crate::session_address_health::record_refusal(
                    &broca_store::session_address::SessionAddressError::Invalid(error.clone()),
                );
                return send_error(
                    writer,
                    frame.header.ver,
                    0,
                    0,
                    frame.header.corr,
                    "invalid_bind_identity",
                    &error,
                )
                .await;
            }
            let cancel = CancellationToken::new();
            if let Some((_, previous)) = routes
                .pending_binds
                .insert(route_channel, (epoch, cancel.clone()))
            {
                previous.cancel();
            }
            let completed = BindCompletion {
                address_refusal: None,
                header: frame.header,
                channel: route_channel,
                epoch,
                identity,
                bind,
                cancel,
                refusal: Ok(None),
                role_refusal: None,
            };
            // Keep the bind unacknowledged until its history authorizes the stamp. The read
            // runs off ingress so a slow session log/store cannot hide health probes or GOODBYE.
            if !completed.bind.agent_data && !completed.bind.role.is_sessionless() {
                let registry = health_registry.clone();
                routes
                    .bind_tasks
                    .spawn(async move { authorize_bind(completed, registry).await });
                return Ok(());
            }
            finish_bind(completed, writer, routes).await
        }
        // Health probes read only in-memory gauges stamped by normal dependency calls. A probe
        // must never add catalog, vault, log, lease, or store IO to an already degraded daemon.
        ModuleControlRequest::HealthCheck {} => {
            stall::method_context(2);
            let counts = health_registry.slot_counts();
            // During a drain the daemon ends early only once these three gauges read idle, and
            // client health probes are refused then, so nothing else can show what it read.
            // Log a reading when it changes, and at probe 1, 2, 4, 8, ..., so a drain held to
            // its ceiling shows both the gauge that kept it busy and whether probes kept
            // arriving at all; a repeated reading alone could not tell those apart.
            if health_registry.stopping_signal().is_cancelled() {
                type Gauges = (usize, usize, usize);
                static LAST_DRAIN_GAUGES: std::sync::Mutex<(u64, Option<Gauges>)> =
                    std::sync::Mutex::new((0, None));
                let reading = (counts.runs_in_flight, counts.opening, counts.closing);
                let mut last = LAST_DRAIN_GAUGES.lock().unwrap_or_else(|p| p.into_inner());
                last.0 += 1;
                let probe = last.0;
                if last.1 != Some(reading) || probe.is_power_of_two() {
                    last.1 = Some(reading);
                    tracing::info!(target: "lifecycle", probe, runs_in_flight = reading.0,
                        opening = reading.1, closing = reading.2,
                        "drain health probe answered with busy gauges");
                }
            }
            let vault = vault_gauge.read();
            let now_ms = now_epoch_ms();
            let mut details = Vec::new();
            let vault_metric = match vault.state {
                VaultGauge::CONNECTED => "connected",
                VaultGauge::BROKEN => {
                    if vault.serving_cached {
                        details.push("vault unreachable; serving cached credentials".to_string());
                    } else {
                        let stale_s = (now_ms - vault.last_error_ms).max(0) / 1000;
                        details.push(format!(
                            "credential vault unreachable (last failure {stale_s}s ago); \
                             runs needing vault auth will fail admission or pause"
                        ));
                    }
                    "broken"
                }
                VaultGauge::DISCONNECTED => {
                    details.push("vault: credentials module not catalog-live".to_string());
                    "disconnected"
                }
                _ => "disconnected",
            };
            let catalog_health = catalog_cache.map(|cache| cache.health(now_ms));
            if let Some(Err(error)) = &catalog_health {
                details.push(error.to_string());
            }
            // Repeated refusals degrade, where ordinary staleness does not.
            //
            // A single missed poll is routine and the served LKG is a complete catalog, so
            // staleness alone leaves status Ok. But the completeness guard compares against the
            // identities this cache HOLDS and a refused refresh does not install, so the
            // comparison basis never advances: once a producer serves content that trips it, the
            // refusal repeats for as long as that content is served. The catalog then freezes at
            // the last accepted document while every reading still looks ordinary.
            //
            // Three is one hour at the 30-minute poll interval -- past a restart or a transient
            // producer fault, short of a working day of silent drift.
            //
            // DEGRADING HERE IS CORRECT EVEN THOUGH THE CONDITION USUALLY CLEARS ITSELF, and the
            // distinction is frequency rather than duration. Fusiform reports that their identity
            // set re-converges on their first changed poll -- including re-writing tombstones a
            // restore dropped -- so a refusal is normally bounded by that gap, which they measured
            // at 62 minutes average and 208 at worst. Three refusals is 90 minutes, so the tail of
            // that distribution degrades during a recovery that is working.
            //
            // That is acceptable BECAUSE THE TRIGGER IS RARE. An alarm trains itself away by
            // firing during ORDINARY operation; this one fires only after a producer restore or a
            // genuinely degraded response, both abnormal, and an operator wants to know either
            // happened. The detail below therefore says what to expect rather than implying a
            // human must intervene.
            //
            // WHAT A CLIMBING COUNT DOES AND DOES NOT TELL YOU. It says the expected self-heal has
            // not happened, which is what an operator needs. It does NOT say why, and reading it as
            // evidence about any one cause is the mistake to avoid: a count past 7 (208 minutes,
            // fusiform's worst observed gap over 30 observations, so not a bound) is equally
            // consistent with fusiform being down, their upstream being unreachable so no CHANGED
            // poll occurs at all, models.dev simply being quiet, or their identity set failing to
            // re-converge. Only the last is a defect in the mechanism this threshold assumes.
            //
            // THE DISCRIMINATOR IS ON THE PRODUCER, not here: `ck models status` lists recent polls
            // with outcomes. Changed polls landing while this count climbs puts the re-convergence
            // claim in trouble; nothing changed means the claim is untested and the upstream is
            // quiet. This gauge observes a refusal continuing, which is a different reading from
            // what occurred upstream of it.
            //
            // Either way the response is to look, not to raise the threshold -- raising it hides
            // the only signal that any of these are happening.
            if let Some(Ok(catalog)) = &catalog_health {
                if catalog.consecutive_refusals >= 3 {
                    details.push(format!(
                        "catalog refresh refused {}x in a row; serving the last accepted catalog. \
                         This normally clears on the producer's next changed poll (measured: 62 \
                         minutes average, 208 worst). Persisting well past that is a real fault{}",
                        catalog.consecutive_refusals,
                        catalog
                            .refusal_reason
                            .as_deref()
                            .map(|reason| format!(": {reason}"))
                            .unwrap_or_default()
                    ));
                }
            }
            let retention_unreadable = health_registry.retention_unreadable_sessions();
            let held_runtime = crate::held_runtime::health_gauges();
            if held_runtime["rebuild_required"] == true {
                details
                    .push("late-results-rebuild-required: intake completeness is unknown".into());
            }
            if let Some(detail) = held_withdrawal_health_detail(&held_runtime["withdrawals"]) {
                details.push(detail);
            }
            if retention_unreadable > 0 {
                details.push(format!("{retention_unreadable} retained logs blocked by unreadability without a current projection"));
            }
            let status = health_status(&details);
            let detail = (!details.is_empty()).then(|| details.join("; "));
            let (stale_epoch_drops, unmatched_terminal_drops) = broca_subc::FRAME_DROPS.counts();
            // EVERY FIGURE BELOW IS THIS PROCESS DESCRIBING ITSELF: in-memory counters and
            // gauges this same process wrote. That is what makes the probe cheap, and it bounds
            // what a green reading can mean -- it says the process believes it is
            // healthy, not that the work landed. The failure it structurally cannot see is one
            // where the process is wrong about its own state, and a peer lost four days to
            // exactly that: their health read green throughout because it asked the process
            // about itself while the durable store held the cause the whole time.
            //
            // So do not add a field here that a caller might read as an assertion about durable
            // state. Anything of that kind has to be derived from the store or the log by a
            // reader that did not write them -- scripts/live-runs.py is that shape deliberately.
            let mut metrics = serde_json::json!({
                    "agent_data_ops": crate::agent_data::health_counts(routes.agent_data.as_deref()),
                    "retention_unreadable_sessions": retention_unreadable,
                    "provider_held": held_runtime,
                    "stall_watchdog": crate::stall::health_gauges(),
                    // The identity of THIS process, so a caller can tell "my work finished while I
                    // was away" from "the process holding my work is gone" -- WITHOUT already
                    // holding a run to ask about, which is why it belongs here rather than on any
                    // run-keyed surface. A caller that lost its own state in the same fault still
                    // gets an answer in one call.
                    //
                    // COMPARE FOR EQUALITY, NEVER FOR ORDER. It is the same per-launch nonce that
                    // makes minted run_ids unique across restarts, reused rather than minted
                    // separately so the two can never disagree about what a restart is -- but its
                    // shape is `{pid}-{start-nanos}`, so "different" means the process changed and
                    // nothing more. It is not a counter and a later launch does not sort after an
                    // earlier one under string comparison.
                    "launch_id": launch_nonce(),
                    "active_sessions": counts.open,
                    // Sessions actually driving a run, as distinct from sessions that exist. An
                    // operator asking "will a restart cut somebody's turn" needs this one:
                    // `active_sessions` counts idle-but-open slots and would answer a different
                    // question with a plausible number.
                    "runs_in_flight": counts.runs_in_flight,
                    // System One batch calls running now. They hold no session, so they are not
                    // runs, but a drain waits for them the same way (within the same grace).
                    "system_one_batches_in_flight": counts.batches_in_flight,
                    "opening": counts.opening,
                    "closing": counts.closing,
                    "vault": vault_metric,
                    // Always present, zero included: a drop counter that appears only once it is
                    // non-zero cannot distinguish a connection losing nothing from a gauge that was
                    // never wired, and "nothing is being lost" is the reading most worth asserting.
                    // Client-side drops on broca_subc's outbound routes; these are not drops
                    // made by the module serve loop below.
                    "frames_dropped_stale_epoch": stale_epoch_drops,
                    "frames_dropped_unmatched_terminal": unmatched_terminal_drops,
                    // Serve-side inbound daemon frames rejected by this process's binding guard.
                    "serve_frames_dropped_unbound_channel": SERVE_FRAMES_DROPPED_UNBOUND_CHANNEL.load(Ordering::Relaxed),
                    "serve_frames_dropped_stale_epoch": SERVE_FRAMES_DROPPED_STALE_EPOCH.load(Ordering::Relaxed),
                    // Distinct scoped sessions whose send's stamped folder matched, differed
                    // from, or was missing beside the resolved storage root. Existing sessions
                    // retain that root; `sessions_differ` is how many would move if
                    // they were keyed by the stamped folder instead.
                    "scoped_session_workdirs": crate::scope_workdir::health(),
                    "session_address_refusals": crate::session_address_health::health(),
                    // Requests a module sent toward broca on a route, each answered with
                    // reverse_request_unhandled because broca fronts no person who could answer.
                    // Non-zero means some module is asking and not getting what it wants.
                    "reverse_requests_refused": broca_subc::FRAME_DROPS.reverse_requests_refused(),
                    // Lifetime operation counts for THIS process. They reset on restart and are not
                    // fleet totals; consumers derive rates by diffing two readings from one launch.
                    // Always emit zero so a quiet process cannot be confused with an unwired counter.
                    "stream_chunks": broca_provider::sse::stream_chunks(),
                    "display_index_resets": broca_provider::display_index_resets(),
                    // Entries providers listed in input_transformations: for Anthropic, signed
                    // thinking blocks dropped because an earlier part of the conversation changed
                    // between requests. Zero is healthy only over measured attempts; anything
                    // else means a render was not append-only, and the WARN log names the
                    // session and step.
                    "input_transformations": broca_core::input_transformations_reported(),
                    // Attempts sent without asking for that report (every Claude Code persona
                    // request has no thinking-binding beta). Their transformations are unknown,
                    // not zero, so a zero above covers only the other attempts.
                    "input_transformations_unmeasured_attempts": broca_core::input_transformations_unmeasured_attempts(),
                    "session_reads": broca_module::registry::session_reads(),
                    "actor_opens": broca_module::registry::actor_opens(),
                    "management_ops": management_operation_counts(),
            "bus_events": crate::bus_events::health(),
            // The subscriber fan-out's own counters. NOT related to frames_dropped_*
            // above, which count inbound frames on broca-subc's client socket -- I
            // first claimed they were this path's failure counters, and they are not.
            // Adjacency on this surface is what made that misreading easy, so the
            // correction lives here too rather than only where the counters are defined.
            //
            // display_gaps is the display lane's LOSS signal and is excluded from
            // display_deliveries: a gap rides in a Delivery::Display envelope, so
            // counting the envelope made deliveries rise on loss.
            "control_deliveries": broca_module::fanout::control_deliveries(),
            "display_deliveries": broca_module::fanout::display_deliveries(),
            "display_gaps": broca_module::fanout::display_gaps(),
            // Which charge-basis table new spend facts are resolved against: "none" (the
            // shipped table unchanged) or the digest of the local override rows, exactly as
            // facts carry it in charge_basis_overrides, plus the row count. A check that an
            // override swap took effect reads these rather than the startup log. Null only in
            // a process that never loaded a table, which the serve binary always does before
            // serving -- null rather than "none" so a missing load cannot read as "no overrides".
            "charge_basis_overrides": crate::billing_routing::published_overrides()
                .map(|published| published.digest.clone()),
            "charge_basis_override_rows": crate::billing_routing::published_overrides()
                .map(|published| published.rows),
            // How each new run's provider account was chosen (asked of routing, routing
            // switched off, refused because routing was unreachable, pinned by the caller...),
            // how the run's reports to routing fared, and `unmapped_credentials`: listed vault
            // credentials without the provider_ids or auth_method metadata account selection
            // needs, which are never routed. Null in a process with no vault credential source,
            // which asks routing nothing.
            "account_selection": crate::account_routing::published_metrics(),
            "system_one": routes.system_one.as_ref().map(|service| service.health()),
            "tool_routes": health_registry.tool_routes_health(),
            // What the private-content check concluded for each llm_classify call and how the
            // calls it found marked ended: released by a standing grant, held behind a card,
            // denied, or refused. This is what tells "cerebellum said unmarked" apart from "a
            // grant let a private call through" when a call went out without a card. Null when
            // this process serves no model-tool route.
            "llm_tools_consent": routes.llm_tools.as_ref().map(|tools| tools.consent_health()),
                });
            metrics["scoped_role_route_refusals"] =
                serde_json::json!(broca_subc::scoped_role_route_refusals());
            metrics["session_readers"] = crate::session_readers::health();
            // Views of closed sessions loaded and not kept because each alone was larger than the
            // view cache's byte cap: each read of such a session while it is closed loads it again.
            metrics["views_over_cap"] =
                serde_json::json!(broca_module::store_reads::views_over_cap());
            // Which deployment key the session store is opened with, and whether it has
            // loaded: while it hasn't, sessions kept there are refused `key_unavailable`.
            metrics["store"] = crate::store_key::health();
            // The daemon's settings broca runs on, and the changed fields that wait for its next
            // start; null when broca read its settings file.
            metrics["settings"] =
                serde_json::json!(routes.gate.as_ref().and_then(|gate| gate.health()));
            metrics["usage_observe"] = crate::usage_observe::published_metrics();
            metrics["account_limit_stale_resets"] =
                serde_json::json!(broca_core::account_limit_stale_resets());
            metrics["tool_keepalive_discovery"] = broca_fetch_plan::keepalive_discovery_health();
            metrics["account_limit_pause_declined"] = broca_core::account_limit_pause_declined();
            metrics["account_limit_hold_cap_unknown"] =
                serde_json::json!(crate::account_routing::account_limit_hold_cap_unknown());
            if let Some(Ok(catalog)) = catalog_health {
                // Staleness is ordinary operation: expose its age and provenance while leaving
                // status Ok. Fusiform reachability is deliberately absent because health never
                // dials it and the served LKG remains a complete catalog.
                metrics["catalog_age_ms"] = serde_json::json!(catalog.age_ms);
                metrics["catalog_source"] = serde_json::json!(catalog.source.as_str());
                // Emitted at zero for the same reason as the counters above: "refreshing
                // cleanly" is a real reading and a field that vanishes when healthy cannot be
                // told from one that was never wired.
                metrics["catalog_refusals"] = serde_json::json!(catalog.consecutive_refusals);
                // The streak above resets on the first success, so it can only be read WHILE the
                // condition holds -- and the conditions it catches are both rare and
                // self-clearing. Without a total, the likely reading is at 09:00 after a 02:00
                // event that healed by 04:00, showing zero with nothing to suggest anything
                // happened. The pair is what discriminates: streak 0 with total non-zero says
                // "occurred and recovered", which is when to go read the producer's poll history.
                metrics["catalog_refusals_total"] = serde_json::json!(catalog.total_refusals);
                // Rows of the served catalog left out (or kept at their previous model) because
                // one of their facts could not be used. A successful refresh with exclusions is
                // not a refusal, so neither counter above moves; this count is where one bad row
                // shows, and a sudden large count means an upstream fault spoiling many rows.
                // Emitted at zero like the counters above; the reason is null when none.
                metrics["catalog_rows_excluded"] = serde_json::json!(catalog.rows_excluded);
                metrics["catalog_rows_excluded_reason"] =
                    serde_json::json!(catalog.rows_excluded_reason);
            }
            if vault.last_error_ms > 0 {
                metrics["last_vault_error_ms"] = serde_json::json!(vault.last_error_ms);
            }
            // Age the healthy reading too, so a reader can tell "reachable now" from "was
            // reachable whenever someone last dialled". Absent rather than zero when no call
            // has completed: a missing field reads as unknown, where a 0 invites arithmetic.
            //
            // Note the opposite convention on the counts above, which are emitted even at zero.
            // The two are not inconsistent -- EMIT ZERO WHEN ZERO IS A STATE, OMIT WHEN ZERO IS
            // A LIE. "No runs in flight" is a real reading and worth asserting, and a count that
            // vanishes at zero cannot distinguish quiet from broken. An age of zero says
            // "succeeded just now", which is the exact opposite of "never succeeded", so the
            // same rule that requires the count forbids the age.
            if vault.last_ok_ms > 0 {
                metrics["vault_ok_age_s"] =
                    serde_json::json!((now_ms - vault.last_ok_ms).max(0) / 1000);
            }
            let body = serde_json::to_vec(&ModuleControlResponse::HealthCheck {
                status,
                detail,
                metrics: Some(metrics),
            })
            .map_err(|e| ServeIoError(e.to_string()))?;
            let response = Frame::build_with_version(
                frame.header.ver,
                FrameType::Response,
                control_flags(),
                0,
                0,
                frame.header.corr,
                body,
            )
            .map_err(|e| ServeIoError(e.to_string()))?;
            send(writer, response).await
        }
    }
}

/// Drive `task` to completion, turning a panic raised while polling it into `Err(())`. The
/// payload is discarded: it can carry arbitrary internal state and is never reported.
async fn catch_panic<F: std::future::Future>(task: F) -> Result<F::Output, ()> {
    let mut task = std::pin::pin!(task);
    std::future::poll_fn(move |cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| task.as_mut().poll(cx))) {
            Ok(poll) => poll.map(Ok),
            Err(_) => std::task::Poll::Ready(Err(())),
        }
    })
    .await
}

tokio::task_local! {
    /// Whether the route request running in this task has claimed its terminal: started to
    /// write its Response or Error, or handed a subscription stream's terminal to the forward
    /// task. Set only by [`mark_terminal_sent`]; read by [`reply_on_panic`].
    static TERMINAL_SENT: Arc<std::sync::atomic::AtomicBool>;
}

/// Record that the current route request has claimed its terminal. Outside a request task
/// (frame-loop control replies, health, binds) there is nothing to record.
fn mark_terminal_sent() {
    let _ = TERMINAL_SENT.try_with(|sent| sent.store(true, Ordering::SeqCst));
}

/// The fixed refusal message used when a route request task or a bind's adoption check panics.
/// A panic payload can carry arbitrary internal state, so it is never reported.
const REQUEST_PANICKED_MESSAGE: &str = "the request failed inside broca before it was answered";

/// Run one route request's task, answering its correlation with a transient refusal if the
/// task panics. Without the answer the caller waits out its whole timeout and cannot tell a
/// crash from a slow reply. The panic stays confined to this request: the connection and every
/// other request on it keep running.
async fn reply_on_panic<F>(
    writer: EgressSender<Frame>,
    (ver, channel, epoch, corr): (u8, u16, u32, u64),
    task: F,
) -> Result<(), ServeIoError>
where
    F: std::future::Future<Output = Result<(), ServeIoError>>,
{
    let terminal_sent = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let task = TERMINAL_SENT.scope(Arc::clone(&terminal_sent), task);
    match catch_panic(task).await {
        Ok(result) => result,
        // A request answers once. Once its terminal has been claimed (written, or delegated to
        // a stream's forward task), a second answer could become an orphan the daemon forwards
        // anyway, possibly onto a newer request that reuses the correlation on this channel.
        Err(_) if terminal_sent.load(Ordering::SeqCst) => {
            tracing::error!(
                target: "wire.ingress",
                channel, epoch, corr,
                "route request task panicked after its reply went out; sending nothing more"
            );
            Ok(())
        }
        Err(_) => {
            tracing::error!(
                target: "wire.ingress",
                channel, epoch, corr,
                "route request task panicked; answering it with a transient refusal"
            );
            send_serve_error(
                &writer,
                ver,
                channel,
                epoch,
                corr,
                &ServeError::Transient(REQUEST_PANICKED_MESSAGE.into()),
            )
            .await
        }
    }
}

async fn authorize_bind(
    mut completed: BindCompletion,
    registry: LineageRegistry,
) -> BindCompletion {
    // A scoped bind that reserves a new session address records its scope's owner and ref with
    // it, so until the session's first send is admitted only that scope can start it.
    let reservation = match &completed.bind.scope {
        broca_protocol::SendScope::Stamped(stamp) => {
            Some(broca_store::session_address::ScopeReservation {
                owner: stamp.scope.owner.clone(),
                scope_ref: stamp.scope.scope_ref.clone(),
            })
        }
        _ => None,
    };
    match registry
        .resolve_session_address(
            &completed.identity,
            completed
                .bind
                .stamped_project
                .as_ref()
                .and_then(|stamp| stamp.workdir.as_deref()),
            reservation.as_ref(),
        )
        .await
    {
        Ok(identity) => completed.identity = identity,
        Err(error) => {
            completed.address_refusal = Some(error);
            return completed;
        }
    }
    if let broca_protocol::SendScope::Stamped(stamp) = &completed.bind.scope {
        let check = registry.bind_adoption_refusal(
            &completed.identity,
            completed.bind.principal.as_ref(),
            stamp,
        );
        // A panic in the check refuses this bind as transient, through the same reply as an
        // unreadable history, instead of leaving the bind unanswered.
        let refusal = match catch_panic(check).await {
            Ok(refusal) => refusal,
            Err(_) => {
                tracing::error!(
                    target: "wire.ingress",
                    channel = completed.channel, epoch = completed.epoch,
                    "bind authorization panicked; refusing the bind as transient"
                );
                Err(ServeError::Transient(REQUEST_PANICKED_MESSAGE.into()))
            }
        };
        completed.refusal = refusal;
    }
    if completed.bind.role == RouteRole::CompactionRunner && matches!(completed.refusal, Ok(None)) {
        // The runner checks that the attested caller is this session's compaction provider
        // before the route is acknowledged; the caller id is the daemon's, never a body field.
        let caller = caller_module(&completed.bind).map(str::to_owned);
        let params = serde_json::value::RawValue::from_string("{}".into())
            .expect("an empty object is valid JSON");
        if let Err(refusal) = registry
            .route_compaction_v2(&completed.identity, caller, "bind".into(), params)
            .await
        {
            completed.role_refusal = Some(serde_json::to_vec(&refusal).expect("refusals encode"));
        }
    }
    completed
}

async fn finish_bind(
    completed: BindCompletion,
    writer: &EgressSender<Frame>,
    routes: &mut Routes,
) -> Result<(), ServeIoError> {
    if completed.cancel.is_cancelled() {
        return Ok(());
    }
    routes.pending_binds.remove(&completed.channel);
    if let Some(error) = completed.address_refusal {
        crate::session_address_health::record_refusal(&error);
        let class = match error {
            broca_store::session_address::SessionAddressError::Ambiguous
            | broca_store::session_address::SessionAddressError::Invalid(_) => "permanent",
            broca_store::session_address::SessionAddressError::Unavailable(_) => "transient",
        };
        let message = format!(
            "cannot resolve session {:?} in harness {:?}: {error}",
            completed.identity.session, completed.identity.harness
        );
        let mut detail = serde_json::json!({"class":class});
        if let broca_store::session_address::SessionAddressError::Unavailable(reason) = &error {
            detail["reason"] = serde_json::json!(reason.reason());
        }
        let body = ErrorBody::new(error.code(), message).with_detail(detail);
        return send_error_body(
            writer,
            completed.header.ver,
            0,
            0,
            completed.header.corr,
            body,
        )
        .await;
    }
    if let Some(body) = completed.role_refusal {
        return send_error_bytes(
            writer,
            completed.header.ver,
            0,
            0,
            completed.header.corr,
            body,
        )
        .await;
    }
    let refusal = match completed.refusal {
        Ok(refusal) => refusal,
        // The adoption check could not read the session's history. Refuse the bind as a
        // transient failure rather than acknowledge a scope the history might forbid.
        Err(error) => {
            return send_serve_error(
                writer,
                completed.header.ver,
                0,
                0,
                completed.header.corr,
                &error,
            )
            .await;
        }
    };
    if let Some(refusal) = refusal {
        let body = serde_json::to_vec(&adoption_refused(&refusal))
            .map_err(|e| ServeIoError(e.to_string()))?;
        return send_error_bytes(
            writer,
            completed.header.ver,
            0,
            0,
            completed.header.corr,
            body,
        )
        .await;
    }
    // The whole bind triple, not only the session: a caller that reads back under the wrong
    // harness or root addresses a different (often empty) log, and only this line shows it.
    tracing::debug!(target: "wire.ingress", stage = "bound", channel = completed.channel,
        epoch = completed.epoch, session = %completed.identity.session,
        harness = %completed.identity.harness, project_root = %completed.identity.project_root.display(),
        "module route bound");
    if let Some(cancel) = routes.agent_data_cancel.remove(&completed.channel) {
        cancel.cancel();
    }
    // Any subscription slot on this channel belongs to the generation being replaced: a stream
    // for the new generation can open only after this publication. The old generation's GOODBYE
    // is ignored once the new one is published, so retiring its stream here is the only point
    // where it ends before the replacement route itself closes.
    if let Some(slot) = routes.subscribe_cancel.remove(&completed.channel) {
        slot.route.cancel();
    }
    routes.by_channel.insert(
        completed.channel,
        (completed.epoch, completed.identity, completed.bind),
    );
    let body = serde_json::to_vec(&ModuleControlResponse::RouteBindAck {})
        .map_err(|e| ServeIoError(e.to_string()))?;
    let response = Frame::build_with_version(
        completed.header.ver,
        FrameType::Response,
        control_flags(),
        0,
        0,
        completed.header.corr,
        body,
    )
    .map_err(|e| ServeIoError(e.to_string()))?;
    send(writer, response).await
}

/// Reserve subscription teardown state on ingress so GOODBYE can cancel a queued request.
/// Leave parameters opaque until task-side authorization; reserving a slot does not replace
/// its active stream or reveal validation errors to an unauthorized caller.
fn prepare_route_request(
    req: RouteRequest,
    routes: &mut Routes,
    channel: u16,
    conn_cancel: &CancellationToken,
) -> PreparedRouteRequest {
    if req.method != OP_SUBSCRIBE {
        return PreparedRouteRequest::Request(req);
    }
    let slot = routes.subscribe_cancel.entry(channel).or_insert_with(|| {
        Arc::new(SubscriptionSlot {
            route: conn_cancel.child_token(),
            current: std::sync::Mutex::new(None),
        })
    });
    PreparedRouteRequest::Subscribe {
        params: req.params,
        slot: slot.clone(),
    }
}

fn subscribe_from(
    params: &serde_json::Value,
) -> Result<
    (
        SubscribeFrom,
        Option<cortexkit_role_llm_runner::display::Limits>,
    ),
    (Option<String>, String),
> {
    if params
        .get("from")
        .and_then(serde_json::Value::as_object)
        .is_some_and(|cursor| {
            cursor.keys().any(|field| {
                !matches!(
                    field.as_str(),
                    "wal_seq" | "sub_index" | "incarnation" | "digest"
                )
            })
        })
    {
        return Err((
            Some("from".into()),
            "subscribe cursor has an unknown member".into(),
        ));
    }
    let params: SubscribeParams = decode_params(params)?;
    let from = match params.from {
        None => SubscribeFrom::Live,
        Some(w) => w
            .to_subscribe_from()
            .map_err(|e| (Some("from".into()), e))?,
    };
    Ok((from, params.limits))
}

/// Dispatch `memory.report` without acquiring an actor lease or touching the bound session: the
/// report is daemon-scoped, and the bound route is only its transport. Only the operator's own
/// connection is answered, since the report names every open session's address.
#[allow(clippy::too_many_arguments)]
async fn spawn_memory_report_request<H: ServeHandler + 'static>(
    req: RouteRequest,
    writer: &EgressSender<Frame>,
    conn_cancel: &CancellationToken,
    request_slots: &Arc<Semaphore>,
    request_tasks: &mut JoinSet<Result<(), ServeIoError>>,
    (ver, channel, epoch, corr): (u8, u16, u32, u64),
    handler: Arc<H>,
    session: BrocaBindIdentity,
    bind: BindObservation,
) -> Result<(), ServeIoError> {
    let request_slots = request_slots.clone();
    let writer = writer.clone();
    let conn_cancel = conn_cancel.clone();
    request_tasks.spawn(reply_on_panic(
        writer.clone(),
        (ver, channel, epoch, corr),
        async move {
            log_served_request(
                OP_MEMORY_REPORT,
                &session,
                caller_module(&bind),
                "reply",
                async {
                    let _permit = request_slots
                        .acquire_owned()
                        .await
                        .map_err(|_| ServeIoError("request concurrency limiter closed".into()))?;
                    // Every request is counted, a refused one too, as every management operation is.
                    count_management_operation(OP_MEMORY_REPORT);
                    if !matches!(bind.principal, Some(broca_protocol::BindPrincipal::Direct)) {
                        return send_error(
                            &writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            "operator_required",
                            "memory.report is for the operator only",
                        )
                        .await;
                    }
                    if !req.params.is_null() {
                        if let Err(error) =
                            serde_json::from_value::<crate::wire::MemoryReportParams>(req.params)
                        {
                            return send_error(
                                &writer,
                                ver,
                                channel,
                                epoch,
                                corr,
                                "invalid_params",
                                &error.to_string(),
                            )
                            .await;
                        }
                    }
                    let report = tokio::select! {
                        _ = conn_cancel.cancelled() => return Ok(()),
                        report = handler.memory_report() => report,
                    };
                    match report {
                        Some(report) => {
                            send_json_response(&writer, ver, channel, epoch, corr, &report).await
                        }
                        None => {
                            send_error(
                                &writer,
                                ver,
                                channel,
                                epoch,
                                corr,
                                "unavailable",
                                "this broca serves no sessions to report",
                            )
                            .await
                        }
                    }
                },
            )
            .await
        },
    ));
    Ok(())
}

/// Dispatch `vault.audit` without acquiring an actor lease or touching the bound session.
/// The configured `VaultAuditor` performs the vault call; this decodes params and writes the answer.
#[allow(clippy::too_many_arguments)]
async fn spawn_vault_audit_request<H: ServeHandler + 'static>(
    req: RouteRequest,
    writer: &EgressSender<Frame>,
    conn_cancel: &CancellationToken,
    request_slots: &Arc<Semaphore>,
    request_tasks: &mut JoinSet<Result<(), ServeIoError>>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    vault_auditor: Option<Arc<dyn crate::vault_audit::VaultAuditor>>,
    handler: Arc<H>,
    session: BrocaBindIdentity,
    bind: BindObservation,
) -> Result<(), ServeIoError> {
    let request_slots = request_slots.clone();
    let writer = writer.clone();
    let conn_cancel = conn_cancel.clone();
    request_tasks.spawn(reply_on_panic(writer.clone(), (ver, channel, epoch, corr), async move {
        log_served_request(OP_VAULT_AUDIT, &session, caller_module(&bind), "reply", async {
        let _permit = request_slots
            .acquire_owned()
            .await
            .map_err(|_| ServeIoError("request concurrency limiter closed".into()))?;
        let authorization = tokio::select! {
            _ = conn_cancel.cancelled() => return Ok(()),
            result = authorize_route_request(handler.as_ref(), &session, &bind, &req.method, &req.params) => result,
        };
        if let Err(error) = authorization {
            return send_serve_error(&writer, ver, channel, epoch, corr, &error).await;
        }
        count_management_operation(OP_VAULT_AUDIT);
        let request = async move {
            tokio::select! {
                _ = conn_cancel.cancelled() => Ok(()),
                result = async move {
                    if !req.params.is_null() {
                        if let Err(error) =
                            serde_json::from_value::<crate::wire::VaultAuditParams>(req.params)
                        {
                            return send_error(
                                &writer,
                                ver,
                                channel,
                                epoch,
                                corr,
                                "invalid_params",
                                &error.to_string(),
                            )
                            .await;
                        }
                    }
                    // A daemon without the vault auth source still answers, and says it could not
                    // check: the caller must be able to tell that apart from a clean audit.
                    let answer = match vault_auditor {
                        Some(auditor) => auditor.audit().await,
                        None => crate::wire::VaultAuditResponse::Unanswered {
                            reason: "this broca is not configured with the credential vault".into(),
                        },
                    };
                    send_json_response(&writer, ver, channel, epoch, corr, &answer).await
                } => result,
            }
        };
        request.await
        }).await
    }));
    Ok(())
}

/// Whether `method` is one of the `fork_answer` operations (llm-runner §9.20).
fn is_fork_operation(method: &str) -> bool {
    matches!(
        method,
        cortexkit_role_llm_runner::ops::SESSION_FORK_PREPARE
            | cortexkit_role_llm_runner::ops::SESSION_FORK_ASK
            | cortexkit_role_llm_runner::ops::SESSION_FORK_END
    )
}

#[allow(clippy::too_many_arguments)]
async fn spawn_route_request<H>(
    prepared: PreparedRouteRequest,
    dispatch_ticket: DispatchTicket,
    writer: &EgressSender<Frame>,
    handler: &Arc<H>,
    conn_cancel: &CancellationToken,
    request_slots: &Arc<Semaphore>,
    request_tasks: &mut JoinSet<Result<(), ServeIoError>>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    session: BrocaBindIdentity,
    bind: BindObservation,
) -> Result<(), ServeIoError>
where
    H: ServeHandler + 'static,
{
    let request_slots = request_slots.clone();
    let writer = writer.clone();
    let handler = handler.clone();
    let conn_cancel = conn_cancel.clone();
    let describe =
        matches!(&prepared, PreparedRouteRequest::Request(req) if req.method == OP_ROLE_DESCRIBE);
    let fork_operation =
        matches!(&prepared, PreparedRouteRequest::Request(req) if is_fork_operation(&req.method));

    request_tasks.spawn(reply_on_panic(writer.clone(), (ver, channel, epoch, corr), async move {
        let method = match &prepared {
            PreparedRouteRequest::Request(req) => req.method.as_str(),
            PreparedRouteRequest::Subscribe { .. } => OP_SUBSCRIBE,
        };
        let operation = if method == OP_STATUS {
            std::borrow::Cow::Borrowed(OP_STATUS)
        } else {
            std::borrow::Cow::Owned(method.to_owned())
        };
        let phase = if method == OP_SUBSCRIBE { "open" } else { "reply" };
        log_served_request(&operation, &session, caller_module(&bind), phase, async {
        // Authorization can read durable history, and permits can be held by slow calls.
        // Neither wait belongs on the frame reader: it must still answer PING and GOODBYE.
        let turn = dispatch_ticket.wait().await;
        // A fork operation reads its head without waiting on it and may hold a model call for
        // seconds: it gives the session's turn back once it has started, so the head's own
        // requests, and the fork's other operations, are never queued behind it.
        let _turn = (!fork_operation).then_some(turn);
        let _permit = request_slots.acquire_owned().await
            .map_err(|_| ServeIoError("request concurrency limiter closed".into()))?;
        if describe {
            trace_describe("permit_acquired", channel, epoch, corr);
        }
        let method = match &prepared {
            PreparedRouteRequest::Request(req) => req.method.as_str(),
            PreparedRouteRequest::Subscribe { .. } => OP_SUBSCRIBE,
        };
        let params = match &prepared {
            PreparedRouteRequest::Request(req) => &req.params,
            PreparedRouteRequest::Subscribe { params, .. } => params,
        };
        // Only the operator's own connection reads a session's tag map or its request bodies, or
        // deletes a session, refused before anything reads the session. The refused request is
        // counted, as every management operation's is.
        if matches!(method, OP_TAG_MAP | OP_REQUEST_DUMP | OP_SESSION_DELETE)
            && !matches!(bind.principal, Some(broca_protocol::BindPrincipal::Direct))
        {
            count_management_operation(method);
            return send_error(
                &writer,
                ver,
                channel,
                epoch,
                corr,
                "operator_required",
                &format!("{method} is for the operator only"),
            )
            .await;
        }
        if let Err(error) = authorize_route_request(handler.as_ref(), &session, &bind, method, params).await {
            return send_serve_error(&writer, ver, channel, epoch, corr, &error).await;
        }
        let write = matches!(
            method,
            OP_SEND | OP_WARM | OP_IMPORT | OP_INSTALL_VIEW | OP_CANCEL | OP_RETRACT | OP_REFRESH | OP_REFRESH_POLICY | OP_FLUSH_PREFIX | OP_RELEASE
        );
        let write = write || matches!(method, "session.upload_begin" | "session.upload_append" | "session.upload_commit");
        let session_read = matches!(method, OP_READ | OP_HEAD | OP_RUN_RESULT | OP_RUN_TOOL_CALLS | OP_RUN_EVENT_GET | OP_STATUS | OP_RUN_ADMISSION | OP_SUBSCRIBE | OP_BASELINE | OP_TAG_MAP | OP_REQUEST_DUMP | broca_wire::plan_view::OP_SESSION_PLAN_VIEW | "session.upload_status" | cortexkit_role_llm_runner::ops::SESSION_HISTORY | cortexkit_role_llm_runner::ops::SESSION_OUTPUT_REFS | cortexkit_role_llm_runner::ops::SESSION_OUTPUT_REF_READ)
            || method == cortexkit_role_llm_runner::ops::SESSION_MEDIA_READ;
        if write || session_read {
            if let Err(error) = handler.check_migration(&session, write).await {
                return send_serve_error(&writer, ver, channel, epoch, corr, &error).await;
            }
        }
        count_management_operation(method);
        if let Some(op) = crate::session_readers::operation(method) {
            crate::session_readers::record(
                bind.reader_principal.as_ref(), op,
                handler.cached_session_owner(&session, bind.principal.as_ref()),
            );
        }
        if describe {
            trace_describe("task_started", channel, epoch, corr);
        }
        // The connection keeps reading while requests run, but actor-bound work for one session
        // takes its turn in wire-arrival order. Separate sessions use separate lanes and remain
        // concurrent; replies may still arrive out of order because correlation ids disambiguate them.
        let request = async {
            match prepared {
                PreparedRouteRequest::Request(req) => {
                    if describe {
                        trace_describe("turn_acquired", channel, epoch, corr);
                    }
                    tokio::select! {
                        _ = conn_cancel.cancelled() => Ok(()),
                        result = execute_route_request(req, &writer, ver, channel, epoch, corr, &session, &bind, &handler) => result,
                    }
                }
                PreparedRouteRequest::Subscribe { params, slot } => {
                    let (from, limits) = match subscribe_from(&params) {
                        Ok(from) => from,
                        Err((field, message)) => {
                            return send_invalid_params(
                                &writer, ver, channel, epoch, corr, field, &message,
                            ).await;
                        }
                    };
                    // Only an authorized, well-formed replacement may stop the previous stream.
                    let cancel = slot.replace();
                    tokio::select! {
                        _ = conn_cancel.cancelled() => Ok(()),
                        _ = cancel.cancelled() => Ok(()),
                        result = start_subscribe_stream(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            session.clone(),
                            from,
                            limits,
                            handler,
                            cancel.clone(),
                            caller_module(&bind).map(str::to_owned),
                            read_access(&session, &bind, OP_SUBSCRIBE, &params),
                        ) => result,
                    }
                }
            }
        };
        request.await
        }).await
    }));
    Ok(())
}

async fn authorize_route_request<H: ServeHandler>(
    handler: &H,
    session: &BrocaBindIdentity,
    bind: &BindObservation,
    method: &str,
    params: &serde_json::Value,
) -> Result<(), ServeError> {
    if broca_module::reader_admission::governs(method) {
        handler
            .authorize_session_read(session, &read_access(session, bind, method, params))
            .await
    } else if method == OP_RETRACT {
        handler
            .authorize_session_retract(
                session,
                bind.principal.as_ref(),
                &bind.scope,
                params
                    .get("submission_id")
                    .and_then(serde_json::Value::as_str),
            )
            .await
    } else if method == OP_CANCEL {
        let at = match params.get("at") {
            None => Some(broca_records::record::RunCancelAt::Now),
            Some(at) => match at.as_str() {
                Some("now") => Some(broca_records::record::RunCancelAt::Now),
                Some("step_boundary") => Some(broca_records::record::RunCancelAt::StepBoundary),
                _ => None,
            },
        };
        handler
            .authorize_session_cancel(session, bind.principal.as_ref(), &bind.scope, at)
            .await
    } else {
        handler
            .authorize_session(session, bind.principal.as_ref())
            .await
    }
}

fn read_access(
    session: &BrocaBindIdentity,
    bind: &BindObservation,
    method: &str,
    params: &serde_json::Value,
) -> broca_module::reader_admission::ReadAccess {
    let send = matches!(method, OP_SEND_STATUS | OP_SESSION_SEND_STATUS)
        .then(|| params.get("send_id").and_then(serde_json::Value::as_str))
        .flatten()
        .map(|id| (run_id_for_send(session, Some(id)), id.to_owned()));
    broca_module::reader_admission::ReadAccess {
        principal: bind.principal.clone(),
        op: method.into(),
        send,
    }
}

/// Handle an authorized request on an already bound route channel by checking the request method and
/// calling the matching handler for that method.
///
/// The reader constructs request tasks on the main thread, whose Windows stack is only 1 MiB.
/// Heap-own the method dispatch state before the cancellation, logging and panic wrappers
/// embed it in the spawned task; copying those nested futures inline overflowed that stack.
#[allow(clippy::too_many_arguments)]
fn execute_route_request<'a, H>(
    req: RouteRequest,
    writer: &'a EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    session: &'a BrocaBindIdentity,
    bind: &'a BindObservation,
    handler: &'a Arc<H>,
) -> std::pin::Pin<Box<impl std::future::Future<Output = Result<(), ServeIoError>> + Send + 'a>>
where
    H: ServeHandler + 'static,
{
    Box::pin(execute_route_request_inner(
        req, writer, ver, channel, epoch, corr, session, bind, handler,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn execute_route_request_inner<H>(
    req: RouteRequest,
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    session: &BrocaBindIdentity,
    bind: &BindObservation,
    handler: &Arc<H>,
) -> Result<(), ServeIoError>
where
    H: ServeHandler + 'static,
{
    if req.method == OP_SEND {
        if let Err(refusal) =
            broca_core::scope_admission::require_flow_scope(bind.principal.as_ref(), &bind.scope)
        {
            return send_serve_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                &ServeError::ScopeRefused(refusal),
            )
            .await;
        }
    }
    if matches!(
        req.method.as_str(),
        OP_SEND | cortexkit_role_llm_runner::ops::SESSION_REFRESH
    ) {
        if let Some(plan) = req
            .params
            .get("plan")
            .and_then(serde_json::Value::as_object)
        {
            if let Err(field) =
                cortexkit_role_llm_runner::attribution::check_plan(plan, &[] as &[&str])
            {
                return send_invalid_params(writer, ver, channel, epoch, corr, Some(field), "sender_attribution is not declared; the plan's attribution member is unsupported").await;
            }
        }
    }
    match req.method.as_str() {
        cortexkit_role_llm_runner::ops::SESSION_MEDIA_READ => {
            match handler.media_read(session, req.params).await {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        "session.upload_begin"
        | "session.upload_append"
        | "session.upload_commit"
        | "session.upload_status" => {
            match handler
                .upload(
                    session,
                    bind.principal.as_ref(),
                    &bind.scope,
                    &req.method,
                    req.params,
                )
                .await
            {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_SEND_STATUS | OP_SESSION_SEND_STATUS => {
            let params: broca_wire::SendStatusRequest =
                match serde_json::from_value(req.params.clone()) {
                    Ok(params) => params,
                    Err(error) => {
                        return send_invalid_params(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            broca_wire::failing_field::<broca_wire::SendStatusRequest>(
                                &req.params,
                                &error,
                            ),
                            &error.to_string(),
                        )
                        .await
                    }
                };
            let run_id = run_id_for_send(session, Some(&params.send_id));
            match handler
                .send_status(session, run_id, &params.send_id, bind.principal.as_ref())
                .await
            {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_SEND => {
            if req
                .params
                .get("seed_from")
                .is_some_and(|seed_from| !seed_from.is_null())
            {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "unsupported_seed_from",
                    "seed_from is reserved for lineage forks and is not supported in v1",
                )
                .await;
            }
            if let Err(message) = broca_wire::check_upload_references(&req.params) {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("content".into()),
                    &message,
                )
                .await;
            }
            // Surface what this send carries that broca does not act on: an ignored key, or a
            // variant that is recorded but not yet applied. Warn only; never refuse here.
            crate::send_audit::warn_send_notices(
                crate::send_audit::global_send_auditor(),
                &req.params,
                &session.harness,
            );
            if let Err((field, message)) = broca_wire::check_tool_replies(&req.params) {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some(field),
                    &message,
                )
                .await;
            }
            if let Err(field) = broca_wire::check_run_parameters(&req.params) {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some(field.clone()),
                    &format!("invalid {field}"),
                )
                .await;
            }
            let mut send_params = req.params.clone();
            let service_tier_level = if broca_protocol::service_tier_enabled() {
                match cortexkit_role_llm_runner::service_tier::parse_request(
                    req.params.get("service_tier"),
                ) {
                    Ok(level) => {
                        if let Some(object) = send_params.as_object_mut() {
                            object.remove("service_tier");
                        }
                        level.map(|level| level.as_str().to_owned())
                    }
                    Err(problem) => {
                        return send_error_body(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            ErrorBody::new(
                                "invalid_params",
                                format!("unsupported service_tier {}", req.params["service_tier"]),
                            )
                            .with_detail(problem.detail()),
                        )
                        .await;
                    }
                }
            } else {
                None
            };
            if let Some(body) = crate::cache_routing::send_cache_refusal(&req.params) {
                let error = ErrorBody::new(body.code, body.message).with_detail(body.detail);
                return send_error_body(writer, ver, channel, epoch, corr, error).await;
            }
            let mut params: SendParams = match serde_json::from_value(send_params) {
                Ok(p) => p,
                Err(e) => {
                    let field = broca_wire::send_budget_refusal_field(&req.params)
                        .or_else(|| broca_wire::send_structured_output_refusal_field(&req.params))
                        .or_else(|| broca_wire::failing_field::<SendParams>(&req.params, &e));
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        field,
                        &e.to_string(),
                    )
                    .await;
                }
            };
            if params
                .auth
                .as_ref()
                .is_some_and(crate::wire::auth_pin_hint_conflict)
            {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("auth.account".into()),
                    crate::wire::AUTH_PIN_HINT_CONFLICT,
                )
                .await;
            }
            if params.template.is_some()
                && (req.params.get("prompt").is_some() || req.params.get("prompt_blocks").is_some())
            {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("template".into()),
                    "a template send must not also carry prompt or prompt_blocks",
                )
                .await;
            }
            let order = match params.delivery_order() {
                Ok(order) => order,
                Err((field, message)) => {
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        Some(field.into()),
                        &message,
                    )
                    .await;
                }
            };
            if let Err(field) = params.check_prompt_for(true) {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some(field.into()),
                    "prompt must contain text or media; a template send must omit prompt",
                )
                .await;
            }
            let describe = broca_wire::broca_role_describe_with_writers(
                env!("CARGO_PKG_VERSION"),
                handler.compaction_supported(),
                crate::RETENTION_WRITER_ENABLED,
                handler.hook_plane_available(),
            )
            .role;
            if let Some(refusal) = delivery_list_refusal(params.delivery.as_ref(), &describe) {
                return send_error_body(writer, ver, channel, epoch, corr, refusal).await;
            }
            // broca keeps a send's client_message_id only in the send's identity digest, which
            // exists for a send that names its `send_id` (an unkeyed send's run id never
            // repeats, so broca records no identity for it). An id on an unkeyed send would
            // therefore be dropped; it is refused instead, naming the field.
            if params.client_message_id.is_some() && params.send_id.is_none() {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("client_message_id".into()),
                    "client_message_id requires send_id: broca stores it with the send's identity",
                )
                .await;
            }
            let tools = req.params.get("tools").is_some_and(|tools| {
                !tools.is_null() && tools.as_array().is_none_or(|items| !items.is_empty())
            });
            let planned_tools = req
                .params
                .pointer("/plan/tool_items")
                .is_some_and(|items| items.as_array().is_none_or(|items| !items.is_empty()));
            if let Some(field) = broca_core::scope_admission::model_only_scope_tool_field(
                &bind.scope,
                tools,
                planned_tools,
            ) {
                let flow = matches!(&bind.scope, broca_protocol::SendScope::Stamped(stamp) if stamp.scope.flow_id.is_some());
                let message = match (flow, field) {
                    (true, "tools") => "flow sends cannot carry tools",
                    (true, _) => "flow fetch plans cannot carry plan.tool_items",
                    (false, "tools") => "agent-run sends cannot carry tools",
                    (false, _) => "agent-run fetch plans cannot carry plan.tool_items",
                };
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some(field.into()),
                    message,
                )
                .await;
            }
            if let Some(plan) = req
                .params
                .get("plan")
                .and_then(serde_json::Value::as_object)
            {
                let supported = match handler
                    .plan_readers_supported(session, params.session_class.as_deref())
                {
                    Ok(supported) => supported,
                    Err(error) => {
                        return send_serve_error(writer, ver, channel, epoch, corr, &error).await
                    }
                };
                if let Err(error) = broca_module::reader_admission::check_plan(plan, supported) {
                    return send_serve_error(writer, ver, channel, epoch, corr, &error).await;
                }
            }
            use broca_module::executor::CreatingPlanSelectionError;
            let plan_selection =
                match send_plan::creating_plan_selection(&**handler, session, &req.params).await {
                    Ok(selection) => selection,
                    Err(CreatingPlanSelectionError::Invalid(problem)) => {
                        let body = send_plan::invalid_plan_selection(problem);
                        return send_error_body(writer, ver, channel, epoch, corr, body).await;
                    }
                    Err(CreatingPlanSelectionError::CredentialsUnavailable) => {
                        let error = ServeError::CredentialsUnavailable;
                        return send_serve_error(writer, ver, channel, epoch, corr, &error).await;
                    }
                };
            if params.model.is_none() {
                if let Some(selection) = &plan_selection {
                    params.model = Some(crate::wire::ModelParams {
                        provider: selection.provider.clone(),
                        model: selection.model.clone(),
                        variant: selection.variant.clone(),
                    });
                }
            }
            // Deliveries that can join an episode take omitted runner parameters from its
            // frozen config, so they render the session's own request prefix. Admission resolves
            // them against its history snapshot; this preflight only classifies log presence.
            // Deliveries that can only start a new run take defaults and must name a model.
            let mut inherit_frozen: std::collections::BTreeSet<
                broca_protocol::request::InheritedParam,
            > = if match params.delivery.as_ref() {
                Some(broca_wire::SendDelivery::Mode(_)) => matches!(
                    params.legacy_delivery(),
                    broca_protocol::Delivery::Steer | broca_protocol::Delivery::Interrupt
                ),
                // Only modes that can join an episode need its frozen settings. Starting or
                // queueing a new run takes defaults even when expressed as an ordered list.
                Some(broca_wire::SendDelivery::Order(modes)) => modes.iter().any(|mode| {
                    matches!(
                        mode,
                        broca_protocol::DeliveryMode::Steer
                            | broca_protocol::DeliveryMode::ToolResult
                            | broca_protocol::DeliveryMode::TurnSystem
                    )
                }),
                None => false,
            } {
                broca_protocol::request::InheritedParam::ALL
                    .iter()
                    .filter(|(_, member)| {
                        req.params
                            .get(*member)
                            .is_none_or(serde_json::Value::is_null)
                    })
                    .map(|(param, _)| *param)
                    .collect()
            } else {
                Default::default()
            };
            let session_written = if inherit_frozen.is_empty() && params.model.is_some() {
                false
            } else {
                // The presence probe can stat the live session log and read archive containers,
                // so it runs on the blocking pool rather than on this request's runtime worker.
                let probe_handler = Arc::clone(handler);
                let probe_session = session.clone();
                let probed = broca_diagnostics::spawn_blocking(
                    broca_diagnostics::JobKind::WalRead,
                    move || probe_handler.session_written(&probe_session),
                )
                .await
                .unwrap_or_else(|error| {
                    Err(ServeError::Open(
                        format!("session presence probe failed: {error}; retry the send"),
                        Some(broca_protocol::ErrorClass::Transient),
                    ))
                });
                match probed {
                    Ok(written) => written,
                    Err(error) => {
                        return send_serve_error(writer, ver, channel, epoch, corr, &error).await
                    }
                }
            };
            // Keep caller omission distinct from the value resolved from the creating plan.
            let model_inherited = req
                .params
                .get("model")
                .is_none_or(serde_json::Value::is_null);
            if params.model.is_none() {
                let may_inherit = session_written;
                if !may_inherit {
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        Some("model".into()),
                        "model is required unless plan.model selects it or the session has a recorded selection",
                    )
                    .await;
                }
                // An empty placeholder, replaced with the session's frozen model before the send
                // is recorded as a queued send or admitted (`InheritedParam::Model`), so it is
                // never rendered and never written to the log.
                params.model = Some(crate::wire::ModelParams {
                    provider: String::new(),
                    model: String::new(),
                    variant: None,
                });
                inherit_frozen.insert(broca_protocol::request::InheritedParam::Model);
            }
            // A session never written has no frozen values, so its first send keeps the
            // defaults for everything it omits.
            let inherit_frozen = if !session_written {
                if model_inherited && plan_selection.is_some() {
                    std::collections::BTreeSet::from([
                        broca_protocol::request::InheritedParam::Model,
                    ])
                } else {
                    Default::default()
                }
            } else {
                inherit_frozen
            };
            if let Some(detail) = execute_class_root_refusal(session).await {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_project_root",
                    &detail,
                )
                .await;
            }
            // A send carrying a fetch plan is checked before it reaches the session and, for a
            // session with no episode yet, every provider the plan names is fetched now. A
            // refusal here is answered before anything is written to the session's log. The
            // session's actor checks the plan again against the session's history as it admits
            // the send, in case an episode was written in between.
            if !handler.hook_plane_available() && handler.frozen_plan_requires_hooks(session).await
            {
                return send_serve_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    &ServeError::PlanRefused(broca_fetch_plan::Refusal::InvalidParams {
                        detail: serde_json::json!({"field":"plan.step_transform_items"}),
                    }),
                )
                .await;
            }
            if !matches!(&bind.scope, broca_protocol::SendScope::Stamped(_))
                && handler.frozen_plan_requires_approval(session).await
            {
                return send_error_body(writer, ver, channel, epoch, corr, approve_scope_error())
                    .await;
            }
            let plan = match req.params.get("plan").filter(|plan| !plan.is_null()) {
                None => None,
                Some(plan) => {
                    let scope = match &bind.scope {
                        broca_protocol::SendScope::Stamped(stamp) => Some(&stamp.scope),
                        _ => None,
                    };
                    match admit_send_plan(
                        handler.as_ref(),
                        session,
                        &req.params,
                        plan,
                        scope,
                        // An inherited model is known only to the session's actor, which checks
                        // the plan's models against it; the placeholder must never be compared.
                        if model_inherited && plan_selection.is_none() {
                            None
                        } else {
                            params.model.as_ref()
                        },
                        |senders, owner| {
                            if params.urgent_yield {
                                broca_module::serve::check_urgent_yield(
                                    owner,
                                    bind.principal.as_ref(),
                                    params.legacy_delivery(),
                                    order.as_ref(),
                                    senders,
                                )
                            } else {
                                Ok(())
                            }
                        },
                    )
                    .await
                    {
                        Ok(plan) => Some(plan),
                        Err(refusal) => {
                            return send_error_body(
                                writer,
                                ver,
                                channel,
                                epoch,
                                corr,
                                refusal.wire_body(),
                            )
                            .await
                        }
                    }
                }
            };
            // A storage policy is decoded and checked before anything reaches the session. A send
            // carries it or the older `retention` seconds, never both.
            let retention_policy = match req
                .params
                .get("retention_policy")
                .filter(|policy| !policy.is_null())
            {
                None => None,
                Some(_) if params.retention.is_some() => {
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        Some("retention_policy".into()),
                        "a send carries retention_policy or retention, never both",
                    )
                    .await;
                }
                Some(policy) => {
                    match cortexkit_role_llm_runner::session_retention::RetentionPolicy::from_value(
                        policy,
                    ) {
                        Ok(policy) => Some(policy),
                        Err(field) => {
                            return send_invalid_params(
                                writer,
                                ver,
                                channel,
                                epoch,
                                corr,
                                Some(field),
                                "the send's retention policy is not valid",
                            )
                            .await;
                        }
                    }
                }
            };
            let run_id = run_id_for_send(session, params.send_id.as_deref());
            let own_run_id = run_id.clone();
            let bound_session = session.clone();
            let mut send_req = match params.into_send_request(session.clone(), run_id) {
                Ok(request) => request,
                Err(broca_core::RunError::Config(message)) => {
                    return send_error(writer, ver, channel, epoch, corr, "config", &message).await;
                }
                Err(error) => unreachable!("send parameter validation returned {error}"),
            };
            send_req.request.project_id = bind.project_id.clone();
            send_req.request.retention_policy = retention_policy;
            send_req.request.message_marks = ["speaks_for", "notice", "keep"]
                .into_iter()
                .filter_map(|field| {
                    req.params
                        .get(field)
                        .map(|value| (field.into(), value.clone()))
                })
                .collect();
            send_req.request.inherit_frozen = inherit_frozen;
            if broca_protocol::service_tier_enabled() {
                send_req.request.service_tier_level = service_tier_level;
                send_req
                    .request
                    .inherit_frozen
                    .retain(|param| *param != broca_protocol::request::InheritedParam::ServiceTier);
            }
            send_req.stop_sequences_named =
                req.params.pointer("/generation/stop_sequences").is_some();
            send_req.request.principal = bind.principal.clone();
            send_req.request.scope = bind.scope.clone();
            send_req.plan = plan;
            // Measured here, after every serve-side refusal, so only sends that reach the
            // session are counted. Compare against the resolved storage identity.
            if matches!(&bind.scope, broca_protocol::SendScope::Stamped(_)) {
                crate::scope_workdir::observe_scoped_send(
                    session,
                    bind.stamped_project.as_ref(),
                    bind.project_id.as_deref(),
                );
            }
            let refusal_session_ref = send_req.session.session.clone();
            let refusal_send_id = send_req.send_id.clone();
            let refusal_delivery_mode = send_req.delivery.as_str();
            match handler.send(send_req).await {
                Ok(resp) => {
                    let (result, delivered) = match resp {
                        broca_module::SendResponse::Finished {
                            run_id,
                            reason,
                            delivered,
                        } => (
                            SendResult::Finished {
                                run_id,
                                reason: reason.into(),
                            },
                            delivered,
                        ),
                        broca_module::SendResponse::Paused {
                            run_id,
                            pause_reason,
                            delivered,
                        } => (
                            SendResult::Paused {
                                run_id,
                                pause_reason: pause_reason.into(),
                            },
                            delivered,
                        ),
                        broca_module::SendResponse::Active { run_id, delivered } => {
                            (SendResult::Active { run_id }, delivered)
                        }
                        broca_module::SendResponse::Pending { submission_id } => {
                            (SendResult::Pending { submission_id }, None)
                        }
                    };
                    // The reply to the send that admitted the session (its first episode), and to
                    // every retry of that send, carries the session's baseline (see
                    // `session.baseline`); any other send's reply carries none. A send admitted
                    // the session when the first episode runs under the send's own run id, so a
                    // steer that joined that episode (and answers its run id) did not. The
                    // baseline comes from what the session's actor already holds, so producing
                    // it reads nothing.
                    let baseline = match &result {
                        SendResult::Active { run_id } | SendResult::Finished { run_id, .. }
                            if *run_id == own_run_id.0 =>
                        {
                            handler.admission_baseline(
                                &bound_session,
                                &broca_protocol::RunId(run_id.clone()),
                            )
                        }
                        _ => None,
                    };
                    let reply = broca_wire::SendReplyWire {
                        result,
                        baseline,
                        delivered,
                    };
                    send_json_response(writer, ver, channel, epoch, corr, &reply).await
                }
                Err(error) => {
                    log_refused_send(
                        &refusal_session_ref,
                        refusal_send_id.as_deref(),
                        refusal_delivery_mode,
                        &error,
                    );
                    if let ServeError::Upload(upload) = &error {
                        return send_error_body(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            upload_error_body(upload, true),
                        )
                        .await;
                    }
                    send_serve_error(writer, ver, channel, epoch, corr, &error).await
                }
            }
        }
        OP_IMPORT if req.params.get("source").is_some() => {
            // A head's history is read from a file exported by another host into a session
            // created by the session store. An import carrying its messages inline is handled
            // by the inline import below.
            let params: broca_wire::head_import::FileImportParams =
                match serde_json::from_value(req.params) {
                    Ok(params) => params,
                    Err(error) => {
                        return send_error(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            "invalid_params",
                            &error.to_string(),
                        )
                        .await
                    }
                };
            let Some(principal) = bind.principal.clone() else {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "principal_required",
                    "an import needs the daemon's attested principal on its route",
                )
                .await;
            };
            // An import's retention is checked against the same limits as a send's. A repeat of
            // the import is matched against its receipt, which compares the retention too.
            if let Err(detail) =
                broca_module::retention::validate_retention(params.retention, None, false)
            {
                let error = ServeError::RetentionInvalid(detail);
                return send_serve_error(writer, ver, channel, epoch, corr, &error).await;
            }
            // The roots belong to the session store's runtime, which opens at the module's start
            // or, when that open failed, once the deployment key loads.
            let Some(roots) = handler.import_roots() else {
                let error = ServeError::Transient(
                    "the session store hasn't opened yet; retry once it has".into(),
                );
                return send_serve_error(writer, ver, channel, epoch, corr, &error).await;
            };
            let command = broca_module::store_import::StoreCommand::Import(Box::new(
                broca_module::store_import::ImportCommand {
                    params,
                    principal,
                    scope: match &bind.scope {
                        broca_protocol::SendScope::Stamped(stamped) => Some(stamped.scope.clone()),
                        _ => None,
                    },
                    roots,
                    // Wall-clock time: it stamps the seeded run's records, and retention counts
                    // the session's deadline from it.
                    now_ms: broca_core::clock::epoch_millis().unwrap_or(0),
                },
            ));
            send_store_outcome(
                handler.as_ref(),
                session,
                command,
                writer,
                ver,
                channel,
                epoch,
                corr,
            )
            .await
        }
        OP_RESTORE_FROM_BACKUP => {
            // Only the operator's own connection restores: restore rewrites a session's head and
            // cuts its log.
            if !matches!(bind.principal, Some(broca_protocol::BindPrincipal::Direct)) {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "operator_required",
                    "session.restore_from_backup is for the operator only",
                )
                .await;
            }
            let all = match restore_all(&req.params) {
                Ok(all) => all,
                Err(message) => {
                    return send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "invalid_params",
                        &message,
                    )
                    .await
                }
            };
            match handler.restore_from_backup(session, all).await {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(refusal) => {
                    let mut body = ErrorBody::new(refusal.code, refusal.message);
                    if let Some(detail) = refusal.detail {
                        body = body.with_detail(detail);
                    }
                    send_error_body(writer, ver, channel, epoch, corr, body).await
                }
            }
        }
        OP_TAG_MAP => {
            // Checked again here so no path reaches the read without the operator's own
            // connection.
            if !matches!(bind.principal, Some(broca_protocol::BindPrincipal::Direct)) {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "operator_required",
                    "session.tag_map is for the operator only",
                )
                .await;
            }
            let params: broca_wire::tag_map::TagMapParams = if req.params.is_null() {
                Default::default()
            } else {
                match serde_json::from_value(req.params) {
                    Ok(params) => params,
                    Err(error) => {
                        return send_error(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            "invalid_params",
                            &error.to_string(),
                        )
                        .await
                    }
                }
            };
            match handler.tag_map(session, &params).await {
                Ok(Some(reply)) => {
                    send_json_response(writer, ver, channel, epoch, corr, &reply).await
                }
                Ok(None) => {
                    send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        NO_SESSION_STORE,
                        "session.tag_map is served only by a handler with the session store",
                    )
                    .await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_REQUEST_DUMP => {
            // Checked again here so no path reaches the rebuild without the operator's own
            // connection.
            if !matches!(bind.principal, Some(broca_protocol::BindPrincipal::Direct)) {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "operator_required",
                    "session.request_dump is for the operator only",
                )
                .await;
            }
            let params: broca_wire::request_dump::RequestDumpParams =
                match serde_json::from_value(req.params) {
                    Ok(params) => params,
                    Err(error) => {
                        return send_error(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            "invalid_params",
                            &error.to_string(),
                        )
                        .await
                    }
                };
            match handler.request_dump(session, &params.run_id).await {
                Ok(Some(reply)) => {
                    send_json_response(writer, ver, channel, epoch, corr, &reply).await
                }
                Ok(None) => {
                    send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        NO_SESSION_STORE,
                        "session.request_dump is served only by a handler with the session store",
                    )
                    .await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_SESSION_DELETE => {
            // Checked again here so no path reaches the deletion without the operator's own
            // connection.
            if !matches!(bind.principal, Some(broca_protocol::BindPrincipal::Direct)) {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "operator_required",
                    "session.delete is for the operator only",
                )
                .await;
            }
            let paused_run = match serde_json::from_value::<broca_wire::session_delete::DeleteParams>(
                req.params,
            ) {
                Ok(params) => params.paused_run,
                Err(error) => {
                    let message = error.to_string();
                    let code = "invalid_params";
                    return send_error(writer, ver, channel, epoch, corr, code, &message).await;
                }
            };
            match handler.delete_session(session, paused_run).await {
                Ok(Some(reply)) => {
                    send_json_response(writer, ver, channel, epoch, corr, &reply).await
                }
                Ok(None) => {
                    send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        NO_SESSION_STORE,
                        "session.delete is served only by a handler with the session store",
                    )
                    .await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_INSTALL_VIEW => {
            let params: broca_wire::head_import::InstallViewParams =
                match serde_json::from_value(req.params) {
                    Ok(params) => params,
                    Err(error) => {
                        return send_error(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            "invalid_params",
                            &error.to_string(),
                        )
                        .await
                    }
                };
            let Some(principal) = bind.principal.clone() else {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "principal_required",
                    "installing a view needs the daemon's attested principal on its route",
                )
                .await;
            };
            let command = broca_module::store_import::StoreCommand::InstallView(Box::new(
                broca_module::store_import::InstallViewCommand {
                    view: params.view,
                    principal,
                },
            ));
            send_store_outcome(
                handler.as_ref(),
                session,
                command,
                writer,
                ver,
                channel,
                epoch,
                corr,
            )
            .await
        }
        OP_IMPORT => {
            let params: ImportParams = match serde_json::from_value(req.params) {
                Ok(params) => params,
                Err(error) => {
                    return send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "invalid_params",
                        &error.to_string(),
                    )
                    .await
                }
            };
            if let Some(detail) = execute_class_root_refusal(session).await {
                return send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "invalid_project_root",
                    &detail,
                )
                .await;
            }
            let run_id = run_id_for_import(session, &params.import_id);
            let import_req = SessionImportRequest {
                project_id: bind.project_id.clone(),
                principal: bind.principal.clone(),
                scope: bind.scope.clone(),
                session: session.clone(),
                run_id,
                messages: params.messages,
                source_ref: params.origin.source_ref,
            };
            match handler.import(import_req).await {
                Ok(response) => {
                    let result: ImportResult = response;
                    send_json_response(writer, ver, channel, epoch, corr, &result).await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_RETRACT => {
            let params: RetractParams = match serde_json::from_value(req.params) {
                Ok(p) => p,
                Err(e) => {
                    return send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "invalid_params",
                        &e.to_string(),
                    )
                    .await
                }
            };
            let sid = broca_protocol::SubmissionId(params.submission_id);
            match handler.retract(session, sid).await {
                Ok(resp) => send_json_response(writer, ver, channel, epoch, corr, &resp).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
            }
        }
        OP_CANCEL => {
            if req
                .params
                .get("at")
                .is_some_and(|at| !matches!(at.as_str(), Some("now" | "step_boundary")))
            {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("at".into()),
                    "run.cancel.at must be now or step_boundary",
                )
                .await;
            }
            let params: CancelParams = match serde_json::from_value(req.params) {
                Ok(p) => p,
                Err(e) => {
                    return send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "invalid_params",
                        &e.to_string(),
                    )
                    .await
                }
            };
            let at = match params.at.as_deref() {
                None | Some("now") => broca_records::record::RunCancelAt::Now,
                Some("step_boundary") => broca_records::record::RunCancelAt::StepBoundary,
                _ => {
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        Some("at".into()),
                        "run.cancel.at is not a supported cancellation point",
                    )
                    .await
                }
            };
            match handler
                .cancel_from(
                    session,
                    broca_protocol::RunId(params.run_id),
                    at,
                    bind.principal.as_ref(),
                    &bind.scope,
                )
                .await
            {
                Ok(resp) => send_json_response(writer, ver, channel, epoch, corr, &resp).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
            }
        }
        OP_RUN_ADMISSION => {
            let params: broca_wire::RunAdmissionParams =
                match serde_json::from_value(req.params.clone()) {
                    Ok(params) => params,
                    Err(error) => {
                        return send_invalid_params(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            Some("run_id".into()),
                            &error.to_string(),
                        )
                        .await
                    }
                };
            match handler
                .run_admission(session, broca_protocol::RunId(params.run_id))
                .await
            {
                Ok(Some(response)) => {
                    send_json_response(writer, ver, channel, epoch, corr, &response).await
                }
                Ok(None) => {
                    send_json_response(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        &broca_wire::RunStatusResponse::Unknown,
                    )
                    .await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_STATUS => {
            // run.status with an OPTIONAL run_id: absent → session status; present → the
            // run-keyed three-tier status (the durable run index's wire surface).
            let params: StatusParams = if req.params.is_null() {
                StatusParams::default()
            } else {
                match serde_json::from_value(req.params) {
                    Ok(p) => p,
                    Err(e) => {
                        return send_error(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            "invalid_params",
                            &e.to_string(),
                        )
                        .await
                    }
                }
            };
            match params.run_id {
                None => match handler.status(session).await {
                    Ok(resp) => send_json_response(writer, ver, channel, epoch, corr, &resp).await,
                    Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
                },
                Some(run_id) => {
                    match handler
                        .run_status_reply(session, broca_protocol::RunId(run_id))
                        .await
                    {
                        Ok(resp) => {
                            send_json_response(writer, ver, channel, epoch, corr, &resp).await
                        }
                        Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
                    }
                }
            }
        }
        OP_RUN_EVENT_GET => {
            let params: broca_wire::RunEventGetParams = match decode_params(&req.params) {
                Ok(params) => params,
                Err((field, message)) => {
                    return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                        .await
                }
            };
            if params.event_key.len() != 64
                || !params
                    .event_key
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("event_key".into()),
                    "run.event.get.event_key must be a 64-character lowercase hex key",
                )
                .await;
            }
            match handler.run_event_get(session, &params.event_key).await {
                Ok(response) => {
                    send_json_response(writer, ver, channel, epoch, corr, &response).await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        cortexkit_role_llm_runner::ops::SESSION_HISTORY => {
            let request = match decode_params::<cortexkit_role_llm_runner::history::HistoryRequest>(
                &req.params,
            ) {
                Ok(request) => request,
                Err((field, message)) => {
                    return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                        .await
                }
            };
            match broca_module::history::read(handler.as_ref(), session, &request).await {
                Ok(page) => send_json_response(writer, ver, channel, epoch, corr, &page).await,
                Err(broca_module::history::HistoryError::Serve(error)) => {
                    send_serve_error(writer, ver, channel, epoch, corr, &error).await
                }
                Err(broca_module::history::HistoryError::Unrepresentable { ordinal, field }) => {
                    let body = serde_json::json!({
                        "code":"history_unrepresentable", "class":"permanent",
                        "message":format!("session.history cannot represent message {ordinal}: no retained {field}; use session.read for raw recovery"),
                        "detail":{"ordinal":ordinal,"field":field},
                    });
                    let bytes = serde_json::to_vec(&body)
                        .map_err(|error| ServeIoError(error.to_string()))?;
                    send_error_bytes(writer, ver, channel, epoch, corr, bytes).await
                }
                Err(broca_module::history::HistoryError::Refused { code, message }) => {
                    send_error(writer, ver, channel, epoch, corr, code, &message).await
                }
            }
        }
        broca_wire::plan_view::OP_SESSION_PLAN_VIEW => {
            if let Err((field, message)) =
                decode_params::<broca_wire::plan_view::PlanViewParams>(&req.params)
            {
                return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                    .await;
            }
            match handler.plan_view(session).await {
                Ok(Some(reply)) => {
                    send_json_response(writer, ver, channel, epoch, corr, &reply).await
                }
                Ok(None) => {
                    send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        NO_SESSION_STORE,
                        "session.plan_view is served only by a handler with the session store",
                    )
                    .await
                }
                Err(broca_module::store_plan_view::Error::NoPlan) => {
                    send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "no_plan",
                        "the session has no plan manifest",
                    )
                    .await
                }
                Err(broca_module::store_plan_view::Error::TooLarge) => {
                    send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "too_large",
                        "the plan view exceeds the transport frame limit",
                    )
                    .await
                }
                Err(broca_module::store_plan_view::Error::Serve(error)) => {
                    send_serve_error(writer, ver, channel, epoch, corr, &error).await
                }
            }
        }
        OP_READ => {
            if req.params.get("tag").is_some() {
                let params = &req.params;
                return serve_tag_read::tag_read(
                    writer, ver, channel, epoch, corr, session, bind, params, handler,
                )
                .await;
            }
            let params: broca_wire::ReadParams = match decode_params(&req.params) {
                Ok(p) => p,
                Err((field, message)) => {
                    return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                        .await
                }
            };
            let media = params.media.unwrap_or_default();
            // Tool media references ride only on a media_refs read.
            if params.tool_media.is_some()
                && media != cortexkit_role_llm_runner::read::MediaMode::MediaRefs
            {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("tool_media".into()),
                    "tool_media is valid only with media: \"media_refs\"",
                )
                .await;
            }
            if params.after_mid.is_some() {
                let refusal = if params.history_version.is_none() {
                    Some((
                        "history_version",
                        "after_mid needs the history_version it was read under",
                    ))
                } else if params.from_ordinal.is_some() {
                    Some((
                        "from_ordinal",
                        "after_mid and from_ordinal are two cursors; send one",
                    ))
                } else {
                    None
                };
                if let Some((field, message)) = refusal {
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        Some(field.into()),
                        message,
                    )
                    .await;
                }
            }
            let read_req = SessionReadRequest {
                late_after: params.late_after,
                session: session.clone(),
                from_ordinal: params.from_ordinal,
                limit: params.limit,
                include_tools: params.include_tools,
                include_rich_ui: params.include_rich_ui,
                include_display_facts: params.include_display_facts,
                after_mid: params.after_mid,
                history_version: params.history_version,
                lineage_id: None,
                max_bytes: params.max_bytes,
                media,
                tool_media: params.tool_media.is_some(),
            };
            match handler.read(read_req).await {
                Ok(resp) => send_json_response(writer, ver, channel, epoch, corr, &resp).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
            }
        }
        OP_ROLE_DESCRIBE => {
            trace_describe("authorized", channel, epoch, corr);
            if let Err((field, message)) = decode_params::<RoleDescribeParams>(&req.params) {
                return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                    .await;
            }
            let mut describe = broca_wire::broca_role_describe_with_writers(
                env!("CARGO_PKG_VERSION"),
                handler.compaction_supported(),
                crate::RETENTION_WRITER_ENABLED,
                handler.hook_plane_available(),
            );
            describe.session_class.backup_classes = handler.backup_classes();
            trace_describe("response_ready", channel, epoch, corr);
            let sent = send_json_response(writer, ver, channel, epoch, corr, &describe).await;
            if sent.is_ok() {
                trace_describe("response_queued", channel, epoch, corr);
            }
            sent
        }
        OP_HEAD => {
            if let Err((field, message)) = decode_params::<broca_wire::HeadParams>(&req.params) {
                return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                    .await;
            }
            match handler
                .head_for(session, bind.principal.as_ref(), &bind.scope)
                .await
            {
                Ok(head) => send_json_response(writer, ver, channel, epoch, corr, &head).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
            }
        }
        OP_RUN_RESULT => {
            let params: broca_wire::RunResultParams =
                match serde_json::from_value(req.params.clone()) {
                    Ok(p) => p,
                    Err(e) => {
                        let field = broca_wire::failing_field::<broca_wire::RunResultParams>(
                            &req.params,
                            &e,
                        );
                        return send_invalid_params(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            field,
                            &e.to_string(),
                        )
                        .await;
                    }
                };
            match handler
                .run_result(session, broca_protocol::RunId(params.run_id))
                .await
            {
                Ok(result) => send_json_response(writer, ver, channel, epoch, corr, &result).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
            }
        }
        OP_RUN_TOOL_CALLS => {
            let request: cortexkit_role_llm_runner::tool_calls::ToolCallsRequest =
                match serde_json::from_value(req.params.clone()) {
                    Ok(request) => request,
                    Err(error) => {
                        let field = broca_wire::failing_field::<
                            cortexkit_role_llm_runner::tool_calls::ToolCallsRequest,
                        >(&req.params, &error);
                        return send_invalid_params(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            field,
                            &error.to_string(),
                        )
                        .await;
                    }
                };
            if let Err(field) = request.check() {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some(field.into()),
                    "run.tool_calls.limit must be greater than zero",
                )
                .await;
            }
            match handler.run_tool_calls(session, request).await {
                Ok(page) => send_json_response(writer, ver, channel, epoch, corr, &page).await,
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_BASELINE => {
            if let Err((field, message)) = decode_params::<broca_wire::BaselineParams>(&req.params)
            {
                return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                    .await;
            }
            match handler
                .baseline(session, bind.principal.as_ref(), &bind.scope)
                .await
            {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
            }
        }
        OP_REFRESH => {
            let advertised = handler
                .plan_admitter()
                .is_some_and(|admitter| admitter.admits_output_refs());
            let request = match serve_refresh::decode(&req.params, advertised) {
                Ok(request) => request,
                Err((field, message)) => {
                    return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                        .await;
                }
            };
            match handler
                .session_refresh(
                    session,
                    bind.principal.as_ref(),
                    &bind.scope,
                    request.plan,
                    request.policy,
                    request.generation,
                    request.refetch.unwrap_or_default(),
                    request.system_note,
                )
                .await
            {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => {
                    send_session_change_error(writer, ver, channel, epoch, corr, error).await
                }
            }
        }
        OP_REFRESH_POLICY => {
            let request: cortexkit_role_llm_runner::session_change::RefreshPolicyRequest =
                match serde_json::from_value(req.params.clone()) {
                    Ok(request) => request,
                    Err(error) => {
                        let field = broca_wire::failing_field::<
                            cortexkit_role_llm_runner::session_change::RefreshPolicyRequest,
                        >(&req.params, &error);
                        return send_invalid_params(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            field,
                            &error.to_string(),
                        )
                        .await;
                    }
                };
            if let Some((field, message)) = serve_refresh::policy_refusal(&request.policy) {
                let field = Some(field.into());
                return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                    .await;
            }
            // `send_id` is required: a missing or empty key is `invalid_params`, not a reuse.
            if request.send_id.is_empty() {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("send_id".into()),
                    "send_id is required",
                )
                .await;
            }
            match handler
                .session_refresh_policy(
                    session,
                    bind.principal.as_ref(),
                    &bind.scope,
                    request.generation,
                    request.policy,
                    request.send_id,
                )
                .await
            {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => {
                    send_session_change_error(writer, ver, channel, epoch, corr, error).await
                }
            }
        }
        OP_FLUSH_PREFIX => {
            let request: cortexkit_role_llm_runner::session_change::FlushPrefixRequest =
                match serde_json::from_value(req.params.clone()) {
                    Ok(request) => request,
                    Err(error) => {
                        let field = broca_wire::failing_field::<
                            cortexkit_role_llm_runner::session_change::FlushPrefixRequest,
                        >(&req.params, &error);
                        return send_invalid_params(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            field,
                            &error.to_string(),
                        )
                        .await;
                    }
                };
            if request.send_id.is_empty() {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("send_id".into()),
                    "send_id is required",
                )
                .await;
            }
            match handler
                .session_flush_prefix(
                    session,
                    bind.principal.as_ref(),
                    &bind.scope,
                    request.send_id,
                )
                .await
            {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => {
                    send_session_change_error(writer, ver, channel, epoch, corr, error).await
                }
            }
        }
        OP_RELEASE => {
            if let Err((field, message)) = decode_params::<
                cortexkit_role_llm_runner::session_retention::ReleaseRequest,
            >(&req.params)
            {
                return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                    .await;
            }
            match handler
                .session_release(session, bind.principal.as_ref())
                .await
            {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => {
                    send_session_change_error(writer, ver, channel, epoch, corr, error).await
                }
            }
        }
        OP_WARM => {
            let params = match decode_params::<broca_wire::WarmParams>(&req.params) {
                Ok(params) => params,
                Err((field, message)) => {
                    return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                        .await;
                }
            };
            match handler.warm(session, params.lineage_id).await {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
            }
        }
        cortexkit_role_llm_runner::ops::SESSION_FORK_PREPARE => {
            type Request = cortexkit_role_llm_runner::fork_answer::ForkPrepareRequest;
            if let Err((field, message)) = decode_params::<Request>(&req.params) {
                return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                    .await;
            }
            let started = std::time::Instant::now();
            let result = handler.fork_prepare(session, bind.principal.as_ref()).await;
            let fork_id = result.as_ref().ok().map(|reply| reply.fork_id.as_str());
            log_fork_op("prepare", session, fork_id, None, None, &result, started);
            match result {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => send_fork_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        cortexkit_role_llm_runner::ops::SESSION_FORK_ASK => {
            type Request = cortexkit_role_llm_runner::fork_answer::ForkAskRequest;
            let request: Request = match serde_json::from_value(req.params.clone()) {
                Ok(request) => request,
                Err(error) => {
                    let field = broca_wire::failing_field::<Request>(&req.params, &error);
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        field,
                        &error.to_string(),
                    )
                    .await;
                }
            };
            // Broca declares `fork_effort` but not `fork_tools`: a tool list or framing is unknown.
            let undeclared =
                request.check_effort(&[cortexkit_role_llm_runner::capabilities::FORK_EFFORT]);
            if let Err(field) = undeclared.and_then(|()| request.check_tools::<&str>(&[])) {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some(field.to_owned()),
                    "this runner takes no tools or framing on session.fork_ask",
                )
                .await;
            }
            let started = std::time::Instant::now();
            let (fork_id, ask_id) = (request.fork_id.clone(), request.ask_id.clone());
            let result = handler
                .fork_ask(session, bind.principal.as_ref(), request)
                .await;
            // How the answer finished and its length, never its text.
            let answered = result.as_ref().ok().map(|reply| {
                format!(
                    "finish={:?} answer_chars={}",
                    reply.finish,
                    reply.answer.chars().count()
                )
            });
            log_fork_op(
                "ask",
                session,
                Some(&fork_id),
                Some(&ask_id),
                answered.as_deref(),
                &result,
                started,
            );
            match result {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => send_fork_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        cortexkit_role_llm_runner::ops::SESSION_FORK_END => {
            type Request = cortexkit_role_llm_runner::fork_answer::ForkEndRequest;
            let request: Request = match serde_json::from_value(req.params.clone()) {
                Ok(request) => request,
                Err(error) => {
                    let field = broca_wire::failing_field::<Request>(&req.params, &error);
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        field,
                        &error.to_string(),
                    )
                    .await;
                }
            };
            let started = std::time::Instant::now();
            let fork_id = request.fork_id.clone();
            let result = handler
                .fork_end(session, bind.principal.as_ref(), request)
                .await;
            log_fork_op("end", session, Some(&fork_id), None, None, &result, started);
            match result {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(error) => send_fork_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        cortexkit_role_llm_runner::ops::SESSION_OUTPUT_REFS => {
            type Request = cortexkit_role_llm_runner::output_refs::ListRequest;
            let request: Request = match serde_json::from_value(req.params.clone()) {
                Ok(request) => request,
                Err(error) => {
                    let field = broca_wire::failing_field::<Request>(&req.params, &error);
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        field,
                        &error.to_string(),
                    )
                    .await;
                }
            };
            match handler.output_refs(session, &request).await {
                Ok(Ok(page)) => send_json_response(writer, ver, channel, epoch, corr, &page).await,
                Ok(Err(refusal)) => {
                    send_output_ref_refusal(writer, ver, channel, epoch, corr, refusal).await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        cortexkit_role_llm_runner::ops::SESSION_OUTPUT_REF_READ => {
            type Request = cortexkit_role_llm_runner::output_refs::ReadRequest;
            let request: Request = match serde_json::from_value(req.params.clone()) {
                Ok(request) => request,
                Err(error) => {
                    let field = broca_wire::failing_field::<Request>(&req.params, &error);
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        field,
                        &error.to_string(),
                    )
                    .await;
                }
            };
            match handler.output_ref_read(session, &request).await {
                Ok(Ok(reply)) => {
                    send_json_response(writer, ver, channel, epoch, corr, &reply).await
                }
                Ok(Err(refusal)) => {
                    send_output_ref_refusal(writer, ver, channel, epoch, corr, refusal).await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_SUBSCRIBE => unreachable!("prepare_route_request handles session.subscribe"),
        // Broca declares no `send_promote`; the contract answers its op `unsupported` on such a
        // runner (a known op it doesn't serve), not `unknown_method`.
        cortexkit_role_llm_runner::ops::SESSION_PROMOTE => {
            send_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                cortexkit_role_llm_runner::errors::UNSUPPORTED,
                "this runner does not serve session.promote (send_promote undeclared)",
            )
            .await
        }
        other => {
            send_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                "unknown_method",
                &format!("unknown method '{other}'"),
            )
            .await
        }
    }
}

/// Open the subscribe STREAM: attach a ServeHandler sink, then spawn a task that forwards
/// each Delivery as a StreamData frame on (channel, corr), terminated by StreamEnd. The
/// request's credit is held by this corr until StreamEnd. If the egress closes (the client
/// is gone / subc closed the route), the sink the subscriber drains to closes → the
/// subscriber task detaches cleanly (no panic/leak).
#[allow(clippy::too_many_arguments)]
async fn start_subscribe_stream<H>(
    writer: EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    session: BrocaBindIdentity,
    from: SubscribeFrom,
    limits: Option<cortexkit_role_llm_runner::display::Limits>,
    handler: Arc<H>,
    cancel: CancellationToken,
    caller_module_id: Option<String>,
    read_access: broca_module::reader_admission::ReadAccess,
) -> Result<(), ServeIoError>
where
    H: ServeHandler + 'static,
{
    // The Delivery sink the coordinator's subscriber task drains to. Bounded so a slow
    // egress backpressures the FORWARD task here (not the coordinator's fanout, which has
    // its own per-subscriber backpressure + detach-on-lag).
    let (sink, mut deliveries) = mpsc::channel::<Delivery>(EGRESS_BUFFER);
    let subscribe_result = handler.subscribe(&session, from, sink).await;
    if let Err(e) = subscribe_result {
        return send_serve_error(&writer, ver, channel, epoch, corr, &e).await;
    }
    if cancel.is_cancelled() {
        return Ok(());
    }
    // From here the forward task owns this request's terminal (its StreamEnd or stream Error),
    // so the request task must not answer it again.
    mark_terminal_sent();

    // Forward task: Delivery → StreamData (control=Interactive, display=Background lane
    // labels) until the stream ends, then StreamEnd. A session subscription is
    // LINEAGE-scoped — it spans turns and does NOT end when one run finishes, so the task
    // parks on `deliveries.recv()` between runs. It ends on: a `Detached` delivery (the
    // control stream gapped — the client resubscribes); the sink closing (the coordinator
    // tore the subscriber down); a failed egress send (client gone); or `cancel` (this
    // route's GOODBYE, or the connection teardown). `cancel` is what prevents a parked task
    // from holding the egress open forever and hanging the connection on teardown.
    use tracing::instrument::WithSubscriber;
    tokio::spawn(
        async move {
            log_served_request(
            OP_SUBSCRIBE,
            &session,
            caller_module_id.as_deref(),
            "close",
            async {
                let mut next_seq = cortexkit_role_llm_runner::live::FIRST_SEQ;
                let mut message_identity: Option<(bool, Option<String>)> = None;
                let mut first_delivery = true;
                loop {
                    let delivery = tokio::select! {
                        _ = cancel.cancelled() => { record_request_outcome("cancelled"); break },
                        d = deliveries.recv() => match d {
                            Some(d) => d,
                            None => break, // the coordinator's subscriber task ended (sink closed)
                        },
                    };
                    // A policy change between the open check and fanout attachment may not
                    // reach this subscriber. Check before its first unit, then at control
                    // boundaries and on policy changes, not on every display delta.
                    let check = matches!(delivery, Delivery::Control(_) | Delivery::AdmissionChanged)
                        || first_delivery && matches!(delivery, Delivery::Display(_));
                    first_delivery = false;
                    let admission = if check {
                        handler.authorize_session_read(&session, &read_access).await
                    } else {
                        Ok(())
                    };
                    if let Err(error) = admission {
                        if matches!(error, ServeError::ReadAccessRefused | ServeError::ScopeRefused(_) | ServeError::ScopeFreezeBusy { .. }) {
                            let _ = send_subscribe_scope_error(&writer, (ver, channel, epoch, corr), next_seq, &error).await;
                        } else {
                            let _ = send_subscribe_error_body(&writer, (ver, channel, epoch, corr), next_seq,
                                ErrorBody::new(refused_send_code(&error), error.to_string())).await;
                        }
                        return;
                    }
                    if matches!(delivery, Delivery::AdmissionChanged) {
                        continue;
                    }
                    if matches!(delivery, Delivery::CursorExpired) {
                        let _ = send_subscribe_error_body(&writer, (ver, channel, epoch, corr), next_seq,
                            ErrorBody::new("cursor_expired", "the subscribe cursor is not held in this session log")).await;
                        return;
                    }
                    if let Delivery::Expired { expired_at_ms } = &delivery {
                        let error = ServeError::Expired {
                            expired_at_ms: *expired_at_ms,
                        };
                        let body =
                            ErrorBody::new(broca_module::retention::EXPIRED, error.to_string())
                                .with_detail(
                                    serde_json::to_value(
                                        broca_module::retention::ExpiredDetail::new(*expired_at_ms),
                                    )
                                    .expect("expiry details serialize"),
                                );
                        let _ = send_subscribe_error_body(
                            &writer,
                            (ver, channel, epoch, corr),
                            next_seq,
                            body,
                        )
                        .await;
                        return;
                    }
                    if let Delivery::AccessEnded(refusal) = &delivery {
                        // Read access ended with a change the session accepted: the stream ends
                        // with the refusal a new subscribe would get now.
                        let error = ServeError::ScopeRefused((**refusal).clone());
                        let _ = send_subscribe_scope_error(
                            &writer,
                            (ver, channel, epoch, corr),
                            next_seq,
                            &error,
                        )
                        .await;
                        return;
                    }
                    if let Delivery::Error(message) = &delivery {
                        // A replay failure is a typed stream terminal, not an empty successful stream.
                        // `Error` is already terminal in the existing subc vocabulary, so do not append
                        // a clean `StreamEnd` that could overwrite its meaning for a client.
                        let _ = send_subscribe_error_body(
                            &writer,
                            (ver, channel, epoch, corr),
                            next_seq,
                            ErrorBody::new("subscribe_replay_failed", message),
                        )
                        .await;
                        return;
                    }
                    let mut delivery = delivery;
                    // Projection and provider display keep native stored positions. Resolve the
                    // public vocabulary only when a message exists, including subscriptions
                    // opened before the first send, and use it for both replay and live lanes.
                    if message_identity.is_none() && delivery_has_mid(&delivery) {
                        match tokio::select! {
                            _ = cancel.cancelled() => break,
                            identity = handler.message_identity(&session) => identity,
                        } {
                            Ok(identity) => message_identity = Some(identity),
                            Err(error) => {
                                let _ = send_subscribe_error_body(&writer, (ver, channel, epoch, corr), next_seq,
                                    ErrorBody::new("subscribe_replay_failed", error.to_string())).await;
                                return;
                            }
                        }
                    }
                    if let Some((mode3, lineage)) = &message_identity {
                        if let Err(error) = public_delivery_mids(&mut delivery, *mode3, lineage.as_deref()) {
                            let _ = send_subscribe_error_body(&writer, (ver, channel, epoch, corr), next_seq,
                                ErrorBody::new("subscribe_replay_failed", error)).await;
                            return;
                        }
                    }
                    let delivery = limit_live_arguments(delivery, limits.as_ref());
                    let (priority, body) = match encode_delivery(&delivery, next_seq) {
                        EncodedDelivery::Data(priority, body) => (priority, body),
                        // A Detached marker: end the stream cleanly; the client resubscribes.
                        EncodedDelivery::Detached => break,
                        EncodedDelivery::Unencodable(error) => {
                            let _ = send_undeliverable_unit(
                                &writer,
                                (ver, channel, epoch, corr),
                                &delivery,
                                next_seq,
                                "unencodable",
                                format!("subscribe unit could not be encoded: {error}"),
                                None,
                            )
                            .await;
                            return;
                        }
                    };
                    // Subscribe frames share SUBC_MAX_FRAME_BYTES with unary replies. An oversized frame
                    // could invalidate sibling requests on this one module connection, and a clean
                    // StreamEnd here would send the client back to the same cursor forever.
                    let frame_bytes = body.len().saturating_add(subc_protocol::HEADER_LEN);
                    if frame_bytes > broca_wire::SUBC_MAX_FRAME_BYTES as usize {
                        let _ = send_undeliverable_unit(
                            &writer,
                            (ver, channel, epoch, corr),
                            &delivery,
                            next_seq,
                            "too_large",
                            "subscribe unit exceeds the transport frame limit".into(),
                            Some(frame_bytes),
                        )
                        .await;
                        return;
                    }
                    let frame = match Frame::build_with_version(
                        ver,
                        FrameType::StreamData,
                        Flags::new(false, priority, false),
                        channel,
                        epoch,
                        corr,
                        body,
                    ) {
                        Ok(f) => f,
                        Err(error) => {
                            let _ = send_undeliverable_unit(
                                &writer,
                                (ver, channel, epoch, corr),
                                &delivery,
                                next_seq,
                                "unencodable",
                                format!("subscribe frame could not be built: {error}"),
                                None,
                            )
                            .await;
                            return;
                        }
                    };
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        res = writer.send(frame) => {
                            if res.is_err() {
                                return; // egress closed → client gone; stop (the sink drop tears down the sub)
                            }
                        }
                    }
                    next_seq = next_seq
                        .checked_add(1)
                        .expect("subscription sequence exhausted");
                }
                // Terminal: emit StreamEnd (LAST flag) to release the held credit + close the
                // stream. Best-effort — on egress close this just no-ops.
                if let Ok(end) = Frame::build_with_version(
                    ver,
                    FrameType::StreamEnd,
                    Flags::new(false, Priority::Interactive, true),
                    channel,
                    epoch,
                    corr,
                    serde_json::to_vec(&SubscribeEndMetadata {
                        seq: next_seq,
                        event: cortexkit_role_llm_runner::live::LiveEvent::DisplayReset,
                    })
                    .expect("subscription end metadata serializes"),
                ) {
                    tokio::select! {
                        _ = cancel.cancelled() => {}
                        _ = writer.send(end) => {}
                    }
                }
            },
        )
        .await
        }
        .with_current_subscriber(),
    );
    Ok(())
}

fn delivery_has_mid(delivery: &Delivery) -> bool {
    use cortexkit_role_llm_runner::live::LiveEvent;
    match delivery {
        Delivery::Control(event) => {
            matches!(
                &event.1,
                broca_protocol::caller::ControlUnit::MessageBoundary { .. }
            ) || matches!(
                &event.2,
                Some(
                    LiveEvent::MessageStarted(_)
                        | LiveEvent::MessageFinished(_)
                        | LiveEvent::ToolStarted(_)
                        | LiveEvent::ToolFinished(_)
                        | LiveEvent::NoticeDelivered(_)
                )
            )
        }
        Delivery::Display(event) => match event.as_ref() {
            broca_protocol::display::DisplayEvent::TextDelta { mid, .. }
            | broca_protocol::display::DisplayEvent::ReasoningDelta { mid, .. }
            | broca_protocol::display::DisplayEvent::ToolInputDelta { mid, .. } => mid.is_some(),
            _ => false,
        },
        _ => false,
    }
}

/// Translate native control/display coordinates at the wire boundary, never in stored messages
/// or provider requests. Legacy subscriptions retain their original bytes.
fn public_delivery_mids(
    delivery: &mut Delivery,
    mode3: bool,
    lineage: Option<&str>,
) -> Result<(), String> {
    use cortexkit_role_llm_runner::live::LiveEvent;
    if !mode3 {
        return Ok(());
    }
    let translate = |mid: &mut String| -> Result<(), String> {
        if let Some(position) = broca_core::position_of_mid(mid, false, None) {
            *mid = broca_core::message_mid(true, lineage, position)?;
        } else if broca_core::position_of_mid(mid, true, lineage).is_none() {
            return Err("subscription message id does not name this log".into());
        }
        Ok(())
    };
    match delivery {
        Delivery::Control(event) => {
            let (_, unit, live) = Arc::make_mut(event);
            if let broca_protocol::caller::ControlUnit::MessageBoundary { mid } = unit {
                translate(mid)?;
            }
            match live {
                Some(LiveEvent::MessageStarted(event) | LiveEvent::MessageFinished(event)) => {
                    translate(&mut event.mid)?
                }
                Some(LiveEvent::ToolStarted(event)) => translate(&mut event.mid)?,
                Some(LiveEvent::ToolFinished(event)) => translate(&mut event.mid)?,
                Some(LiveEvent::NoticeDelivered(event)) => translate(&mut event.mid)?,
                _ => {}
            }
        }
        Delivery::Display(event) => match Arc::make_mut(event) {
            broca_protocol::display::DisplayEvent::TextDelta { mid: Some(mid), .. }
            | broca_protocol::display::DisplayEvent::ReasoningDelta { mid: Some(mid), .. }
            | broca_protocol::display::DisplayEvent::ToolInputDelta { mid: Some(mid), .. } => {
                translate(mid)?
            }
            _ => {}
        },
        _ => {}
    }
    Ok(())
}

/// What one Delivery becomes on a subscribe stream.
enum EncodedDelivery {
    /// A data frame body and its lane priority.
    Data(Priority, Vec<u8>),
    /// The clean resubscribe boundary. Terminal deliveries (`Error`, `Expired`) are handled by
    /// the forward task before encoding and also land here.
    Detached,
    /// A unit that cannot be serialized. It must end the stream with a typed error: a clean end
    /// would send the client back to the same unit forever.
    Unencodable(serde_json::Error),
}

fn limit_live_arguments(
    delivery: Delivery,
    limits: Option<&cortexkit_role_llm_runner::display::Limits>,
) -> Delivery {
    if let (Delivery::Control(frame), Some(limits)) = (&delivery, limits) {
        if let Some(cortexkit_role_llm_runner::live::LiveEvent::ToolStarted(event)) = &frame.2 {
            let arguments: serde_json::Value =
                serde_json::from_str(&event.arguments).expect("projected arguments are JSON");
            let mut limited = cortexkit_role_llm_runner::live::ToolStarted::new(
                &event.mid,
                &event.tool_call_id,
                &event.name,
                &arguments,
                Some(limits),
                event.started_at,
            );
            // `new` only re-cuts the arguments; the call identity the projector already
            // attached must survive the cut, or a limited stream loses what lets a client
            // join this event to its display row.
            limited.call_key.clone_from(&event.call_key);
            limited.schema_pin.clone_from(&event.schema_pin);
            return Delivery::Control(Arc::new((
                frame.0,
                frame.1.clone(),
                Some(cortexkit_role_llm_runner::live::LiveEvent::ToolStarted(
                    limited,
                )),
            )));
        }
    }
    delivery
}

/// Mint the control delivery's opaque resume cursor from its durable source evidence.
pub async fn control_cursor_for_delivery<H: ServeHandler + ?Sized>(
    _handler: &H,
    _session: &BrocaBindIdentity,
    delivery: &Delivery,
) -> Result<Option<broca_wire::CursorWire>, ServeError> {
    let Delivery::Control(event) = delivery else {
        return Ok(None);
    };
    // Historical logs without retained cursor identity still stream; position-only cursors cannot resume.
    Ok(Some(event.0.into()))
}

fn encode_delivery(delivery: &Delivery, seq: u64) -> EncodedDelivery {
    let (priority, encoded) = match delivery {
        Delivery::Control(event) => {
            let (cursor, unit, live_event) = event.as_ref();
            (
                Priority::Interactive,
                broca_wire::encode_live_subscribe_control_with_cursor(
                    (*cursor).into(),
                    unit,
                    seq,
                    live_event.as_ref(),
                ),
            )
        }
        Delivery::Display(event) => (
            Priority::Background,
            encode_live_subscribe_display(event.as_ref(), seq),
        ),
        Delivery::CursorExpired
        | Delivery::Expired { .. }
        | Delivery::Error(_)
        | Delivery::AccessEnded(_)
        | Delivery::AdmissionChanged
        | Delivery::Detached => return EncodedDelivery::Detached,
    };
    match encoded {
        Ok(body) => EncodedDelivery::Data(priority, body),
        Err(error) => EncodedDelivery::Unencodable(error),
    }
}

/// The stream terminal for a unit this connection cannot deliver.
const SUBSCRIBE_UNIT_UNDELIVERABLE: &str = "subscribe_unit_undeliverable";

/// End a subscribe stream with a typed error naming the unit that cannot be delivered: its
/// control cursor (null for a display event), why, and for an oversized unit its frame size and
/// the limit. Like the other stream terminals, no StreamEnd follows it.
async fn send_undeliverable_unit(
    writer: &EgressSender<Frame>,
    (ver, channel, epoch, corr): (u8, u16, u32, u64),
    delivery: &Delivery,
    seq: u64,
    reason: &str,
    message: String,
    frame_bytes: Option<usize>,
) -> Result<(), ServeIoError> {
    let cursor = match delivery {
        Delivery::Control(event) => {
            let (cursor, ..) = event.as_ref();
            serde_json::json!({"wal_seq": cursor.wal_seq, "sub_index": cursor.sub_index})
        }
        _ => serde_json::Value::Null,
    };
    let mut detail = serde_json::json!({"reason": reason, "cursor": cursor});
    if let Some(frame_bytes) = frame_bytes {
        detail["frame_bytes"] = frame_bytes.into();
        detail["limit"] = broca_wire::SUBC_MAX_FRAME_BYTES.into();
    }
    tracing::warn!(
        target: "wire.egress",
        channel, epoch, corr, reason, %detail,
        "ending subscribe stream on an undeliverable unit: {message}"
    );
    let body = ErrorBody::new(SUBSCRIBE_UNIT_UNDELIVERABLE, message).with_detail(detail);
    send_subscribe_error_body(writer, (ver, channel, epoch, corr), seq, body).await
}

#[derive(serde::Serialize)]
struct SubscribeEndMetadata {
    seq: u64,
    event: cortexkit_role_llm_runner::live::LiveEvent,
}

async fn send_subscribe_error_body(
    writer: &EgressSender<Frame>,
    (ver, channel, epoch, corr): (u8, u16, u32, u64),
    seq: u64,
    body: ErrorBody,
) -> Result<(), ServeIoError> {
    record_request_outcome(&body.code);
    #[derive(serde::Serialize)]
    struct LiveErrorBody {
        #[serde(flatten)]
        body: ErrorBody,
        seq: u64,
        event: cortexkit_role_llm_runner::live::LiveEvent,
    }
    let body = serde_json::to_vec(&LiveErrorBody {
        body,
        seq,
        event: cortexkit_role_llm_runner::live::LiveEvent::DisplayReset,
    })
    .map_err(|error| ServeIoError(error.to_string()))?;
    send_error_bytes(writer, ver, channel, epoch, corr, body).await
}

#[derive(serde::Serialize)]
struct ResultEnvelope<'a, T: serde::Serialize> {
    result: &'a T,
}

async fn send_json_response<T: serde::Serialize>(
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    result: &T,
) -> Result<(), ServeIoError> {
    let envelope = ResultEnvelope { result };
    let body = serde_json::to_vec(&envelope).map_err(|e| ServeIoError(e.to_string()))?;
    let frame_bytes = body.len().saturating_add(subc_protocol::HEADER_LEN);
    if frame_bytes > broca_wire::SUBC_MAX_FRAME_BYTES as usize {
        // No bytes from this reply have reached the wire, so a small correlated Error
        // can fail this call without invalidating any sibling request on the connection.
        return send_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "response_too_large",
            "response exceeds the transport frame limit",
        )
        .await;
    }
    let frame = Frame::build_with_version(
        ver,
        FrameType::Response,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|e| ServeIoError(e.to_string()))?;
    send(writer, frame).await
}

async fn send_response_bytes(
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    body: Vec<u8>,
) -> Result<(), ServeIoError> {
    let frame_bytes = body.len().saturating_add(subc_protocol::HEADER_LEN);
    if frame_bytes > broca_wire::SUBC_MAX_FRAME_BYTES as usize {
        // No bytes from this reply have reached the wire, so a small correlated Error
        // can fail this call without invalidating any sibling request on the connection.
        return send_error(
            writer,
            ver,
            channel,
            epoch,
            corr,
            "response_too_large",
            "response exceeds the transport frame limit",
        )
        .await;
    }
    let frame = Frame::build_with_version(
        ver,
        FrameType::Response,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|e| ServeIoError(e.to_string()))?;
    send(writer, frame).await
}

const HISTORY_REWRITING_REFUSAL: (&str, broca_protocol::ErrorClass) = (
    broca_core::served_history::HISTORY_REWRITING,
    broca_protocol::ErrorClass::Transient,
);
const CONTEXT_CEILING_REFUSAL: (&str, broca_protocol::ErrorClass) = (
    cortexkit_role_llm_runner::errors::CONTEXT_CEILING,
    broca_protocol::ErrorClass::ContextOverflow,
);

fn refused_send_code(error: &ServeError) -> String {
    match error {
        // Answered on the wire as the contract's `transient` code (see `admission_refusal`).
        ServeError::CredentialsUnavailable => "transient".into(),
        ServeError::CredentialAmbiguous { .. } => "credential_ambiguous".into(),
        ServeError::Upload(error) => error.code.into(),
        ServeError::ContextCeiling(_) => CONTEXT_CEILING_REFUSAL.0.into(),
        ServeError::UnknownSend => cortexkit_role_llm_runner::errors::UNKNOWN_SEND.into(),
        ServeError::UnknownSession => cortexkit_role_llm_runner::errors::UNKNOWN_SESSION.into(),
        ServeError::SessionMoving { .. } => "session_moving".into(),
        ServeError::SessionMoved { .. } => {
            cortexkit_role_llm_runner::errors::SESSION_HELD_ELSEWHERE.into()
        }
        ServeError::Expired { .. } => broca_module::retention::EXPIRED.into(),
        ServeError::RetentionInvalid(_) | ServeError::InvalidParams { .. } => {
            broca_wire::INVALID_PARAMS.into()
        }
        ServeError::RetentionRefused(refusal) => refusal.code().into(),
        ServeError::VariantUnsupported(_) => "variant_unsupported".into(),
        ServeError::ToolAliasInvalid(_) => "tool_alias_invalid".into(),
        ServeError::ServiceTierInvalid { .. } => "invalid_params".into(),
        ServeError::SessionHeld(_) => "session_held".into(),
        ServeError::Open(_, _) => "open_failed".into(),
        ServeError::ReplyTooLarge(_) => broca_wire::REPLY_TOO_LARGE.into(),
        ServeError::SendRetracted { .. } => "send_retracted".into(),
        ServeError::SendEnded { .. } => "send_ended".into(),
        ServeError::DeliveryRefused { code, .. } => (*code).into(),
        ServeError::TagReadRefused { code, .. } => (*code).into(),
        ServeError::NotServedOnNewStorage { .. } => NOT_SERVED_ON_NEW_STORAGE.into(),
        ServeError::MediaTooLarge(_) => cortexkit_role_llm_runner::errors::MEDIA_TOO_LARGE.into(),
        ServeError::TemplatePartGone { .. } => {
            cortexkit_role_llm_runner::errors::TEMPLATE_PART_GONE.into()
        }
        ServeError::SendIdReuse { .. } => "send_id_reuse".into(),
        ServeError::Transient(_) | ServeError::TagsAwaitSetup => "transient".into(),
        // Answered on the wire as the contract's `transient` code (see `admission_refusal`).
        ServeError::ModuleStopping(_) => "transient".into(),
        ServeError::Import(_) => "import_rejected".into(),
        ServeError::ImportIdReuse => "import_id_reuse".into(),
        ServeError::ImportBusy => "import_busy".into(),
        ServeError::SessionBusy(_) => "session_busy".into(),
        ServeError::HistoryRewriting => HISTORY_REWRITING_REFUSAL.0.into(),
        ServeError::HistoryChanged => broca_wire::HISTORY_CHANGED.into(),
        ServeError::CursorExpired => "cursor_expired".into(),
        ServeError::UnknownMid(_) => broca_wire::UNKNOWN_MID.into(),
        ServeError::UnknownRun(_) => broca_wire::UNKNOWN_RUN.into(),
        ServeError::ScopeRefused(_)
        | ServeError::ScopeFreezeBusy { .. }
        | ServeError::ReadAccessRefused => scope_error_bytes(error)
            .ok()
            .and_then(|body| serde_json::from_slice::<serde_json::Value>(&body).ok())
            .and_then(|body| body.get("code")?.as_str().map(str::to_owned))
            .unwrap_or_else(|| "scope_refused".into()),
        ServeError::PlanRefused(refusal) => refusal.code().into(),
        ServeError::AccountSelectionRefused(refusal) => refusal.routing_code.clone(),
        ServeError::RoutingUnavailable(_) => broca_wire::ROUTING_UNAVAILABLE.into(),
        ServeError::WarmRefused(refusal) => refusal.code.clone(),
        ServeError::RunPaused(_) => "run_paused".into(),
        ServeError::PreUserUnavailable { .. } => broca_module::serve::PRE_USER_UNAVAILABLE.into(),
    }
}

fn shared_class_name(class: broca_protocol::ErrorClass) -> &'static str {
    match class.shared_class() {
        broca_protocol::ErrorClass::Transient => "transient",
        broca_protocol::ErrorClass::Permanent => "permanent",
        broca_protocol::ErrorClass::Auth => "auth_required",
        broca_protocol::ErrorClass::ContextOverflow => "context_overflow",
        broca_protocol::ErrorClass::ProviderUnavailable => unreachable!(
            "shared_class maps provider-local unavailable to the shared transient class"
        ),
    }
}

fn refused_send_class(error: &ServeError) -> &'static str {
    match error {
        ServeError::HistoryRewriting => shared_class_name(HISTORY_REWRITING_REFUSAL.1),
        ServeError::ContextCeiling(_) => shared_class_name(CONTEXT_CEILING_REFUSAL.1),
        ServeError::Upload(error) => shared_class_name(error.error_class()),
        ServeError::Open(_, Some(class)) => shared_class_name(*class),
        ServeError::AccountSelectionRefused(refusal) => shared_class_name(refusal.class),
        ServeError::RoutingUnavailable(_)
        | ServeError::CredentialsUnavailable
        | ServeError::Transient(_)
        | ServeError::ModuleStopping(_)
        | ServeError::TagsAwaitSetup
        | ServeError::SessionMoving { .. }
        | ServeError::SessionMoved { .. }
        | ServeError::SessionHeld(_)
        | ServeError::ScopeFreezeBusy { .. }
        | ServeError::ImportBusy => "transient",
        _ => "permanent",
    }
}

fn log_refused_send(
    session_ref: &str,
    send_id: Option<&str>,
    delivery_mode: &str,
    error: &ServeError,
) {
    let refusal_code = refused_send_code(error);
    let class = refused_send_class(error);
    tracing::info!(
        target: "session.send",
        session_ref,
        send_id = send_id.unwrap_or(""),
        delivery_mode,
        refusal_code = %refusal_code,
        class,
        "session.send refused"
    );
}

fn upload_error_body(error: &broca_core::media_uploads::UploadError, for_send: bool) -> ErrorBody {
    let body = broca_module::serve::admission_refusal::upload_refusal(error, for_send);
    ErrorBody::new(body.code, body.message).with_detail(body.detail.expect("upload detail"))
}

/// Whether `session.restore_from_backup`'s params ask for every session kept in the new
/// storage (`all: true`) or only the route's own (none, or `all: false`). Anything else is
/// refused, by message.
fn restore_all(params: &serde_json::Value) -> Result<bool, String> {
    match params.get("all") {
        None | Some(serde_json::Value::Null) => Ok(false),
        Some(serde_json::Value::Bool(all)) => Ok(*all),
        Some(other) => Err(format!("all must be true or false when given, not {other}")),
    }
}

/// The refusal of an op that has no implementation yet for a session kept in the session store.
const NOT_SERVED_ON_NEW_STORAGE: &str = "not_served_on_new_storage";

/// The refusal of an op only the session store serves, from a handler that has no session
/// store. Only test handlers lack one.
const NO_SESSION_STORE: &str = "no_session_store";

/// Routes a command only the session store serves, and sends its answer: the reply, or its
/// refusal under the refusal's own code.
#[allow(clippy::too_many_arguments)]
async fn send_store_outcome<H: ServeHandler + ?Sized>(
    handler: &H,
    session: &BrocaBindIdentity,
    command: broca_module::store_import::StoreCommand,
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
) -> Result<(), ServeIoError> {
    use broca_module::store_import::StoreOutcome;
    match handler.store_command(session, command).await {
        Ok(StoreOutcome::Imported(reply)) => {
            send_json_response(writer, ver, channel, epoch, corr, &reply).await
        }
        Ok(StoreOutcome::ViewInstalled(reply)) => {
            send_json_response(writer, ver, channel, epoch, corr, &reply).await
        }
        Ok(StoreOutcome::Refused(refusal)) => {
            send_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                refusal.code,
                &refusal.message,
            )
            .await
        }
        Ok(StoreOutcome::ScopeRefused(refusal)) => {
            let error = ServeError::ScopeRefused(refusal);
            send_serve_error(writer, ver, channel, epoch, corr, &error).await
        }
        Ok(StoreOutcome::Transient(message)) => {
            let error = ServeError::Transient(message);
            send_serve_error(writer, ver, channel, epoch, corr, &error).await
        }
        // The registry routes a command again when its session closes under it, so it never
        // hands this back.
        Ok(StoreOutcome::Reopen) => unreachable!("route_store_command reopens the session itself"),
        // Only `session.apply_view` is answered so, and it travels on the compaction-runner
        // route, whose answers that route sends.
        Ok(StoreOutcome::ViewApplied(_)) => {
            unreachable!("session.apply_view isn't routed as a store command here")
        }
        Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
    }
}

async fn send_serve_error(
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    err: &ServeError,
) -> Result<(), ServeIoError> {
    if let Some(refusal) = broca_module::serve::admission_refusal::admission_refusal(err) {
        let body = serde_json::to_vec(&refusal).map_err(|error| ServeIoError(error.to_string()))?;
        return send_error_bytes(writer, ver, channel, epoch, corr, body).await;
    }
    if let ServeError::ReplyTooLarge(detail) = err {
        #[derive(serde::Serialize)]
        struct ReplyTooLargeErrorBody<'a> {
            code: &'static str,
            message: &'static str,
            detail: &'a broca_module::serve::ReplyTooLargeDetail,
        }

        let body = serde_json::to_vec(&ReplyTooLargeErrorBody {
            code: broca_wire::REPLY_TOO_LARGE,
            message: "the session.read reply exceeds the transport frame limit",
            detail,
        })
        .map_err(|e| ServeIoError(e.to_string()))?;
        return send_error_bytes(writer, ver, channel, epoch, corr, body).await;
    }
    let code = match err {
        ServeError::Upload(_)
        | ServeError::CredentialsUnavailable
        | ServeError::CredentialAmbiguous { .. }
        | ServeError::Open(_, _)
        | ServeError::VariantUnsupported(_)
        | ServeError::ToolAliasInvalid(_)
        | ServeError::DeliveryRefused { .. }
        | ServeError::NotServedOnNewStorage { .. }
        | ServeError::MediaTooLarge(_)
        | ServeError::SendIdReuse { .. }
        | ServeError::Transient(_)
        | ServeError::ModuleStopping(_)
        | ServeError::InvalidParams { .. }
        | ServeError::AccountSelectionRefused(_)
        | ServeError::RoutingUnavailable(_) => unreachable!("admission refusal returned above"),
        ServeError::ReplyTooLarge(_) => unreachable!("reply_too_large returned above"),
        ServeError::SessionMoving { cut_id } => {
            let body = ErrorBody::new("session_moving", err.to_string())
                .with_detail(serde_json::json!({"cutId":cut_id}));
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::SessionMoved { holder } => {
            // `holder` is the receiving computer's box id (the release's `holderBox`), left out
            // when the release named none; the backup author id is not an address.
            let mut detail =
                cortexkit_role_llm_runner::session_retention::SessionHeldElsewhereDetail::new();
            detail.holder = holder.clone();
            let body = ErrorBody::new(
                cortexkit_role_llm_runner::errors::SESSION_HELD_ELSEWHERE,
                err.to_string(),
            )
            .with_detail(serde_json::to_value(detail).map_err(|e| ServeIoError(e.to_string()))?);
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::Expired { expired_at_ms } => {
            let body = ErrorBody::new(broca_module::retention::EXPIRED, err.to_string())
                .with_detail(
                    serde_json::to_value(broca_module::retention::ExpiredDetail::new(
                        *expired_at_ms,
                    ))
                    .map_err(|e| ServeIoError(e.to_string()))?,
                );
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::RetentionInvalid(detail) => {
            let body = ErrorBody::new(broca_wire::INVALID_PARAMS, err.to_string()).with_detail(
                serde_json::to_value(detail).map_err(|e| ServeIoError(e.to_string()))?,
            );
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::RetentionRefused(refusal) => {
            let body =
                ErrorBody::new(refusal.code(), err.to_string()).with_detail(refusal.detail());
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::SessionHeld(_) => "session_held",
        ServeError::ServiceTierInvalid { .. } => "invalid_params",
        ServeError::SendRetracted { .. } => "send_retracted",
        ServeError::SendEnded {
            submission_id,
            outcome,
        } => {
            let reply = broca_wire::ordered_send_ended_reply(submission_id, outcome);
            return send_json_response(writer, ver, channel, epoch, corr, &reply).await;
        }
        ServeError::TemplatePartGone { slot } => {
            let body = ErrorBody::new(
                cortexkit_role_llm_runner::errors::TEMPLATE_PART_GONE,
                err.to_string(),
            )
            .with_detail(serde_json::json!({ "slot": slot }));
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::TagsAwaitSetup => "transient",
        ServeError::ContextCeiling(detail) => {
            let body = ErrorBody::new(CONTEXT_CEILING_REFUSAL.0, err.to_string()).with_detail(
                serde_json::to_value(detail).map_err(|e| ServeIoError(e.to_string()))?,
            );
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::Import(_) => "import_rejected",
        ServeError::ImportIdReuse => "import_id_reuse",
        ServeError::ImportBusy => "import_busy",
        ServeError::SessionBusy(_) => "session_busy",
        ServeError::HistoryRewriting => HISTORY_REWRITING_REFUSAL.0,
        ServeError::HistoryChanged => broca_wire::HISTORY_CHANGED,
        ServeError::CursorExpired => "cursor_expired",
        ServeError::UnknownMid(_) => broca_wire::UNKNOWN_MID,
        ServeError::UnknownSend => cortexkit_role_llm_runner::errors::UNKNOWN_SEND,
        ServeError::UnknownSession => cortexkit_role_llm_runner::errors::UNKNOWN_SESSION,
        ServeError::UnknownRun(_) => broca_wire::UNKNOWN_RUN,
        ServeError::TagReadRefused { code, .. } => code,
        ServeError::ScopeRefused(_)
        | ServeError::ScopeFreezeBusy { .. }
        | ServeError::ReadAccessRefused => {
            // Callers branch on these bodies, so shared wire serializers produce them directly.
            let body = scope_error_bytes(err)?;
            return send_error_bytes(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::PlanRefused(refusal) => {
            let body =
                ErrorBody::new(refusal.code(), refusal.message()).with_detail(refusal.detail());
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::PreUserUnavailable { provider } => {
            let body = ErrorBody::new(broca_module::serve::PRE_USER_UNAVAILABLE, err.to_string())
                .with_detail(serde_json::json!({"provider": provider}));
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::WarmRefused(refusal) => {
            // Sent exactly as the broca-wire type serializes: the bytes its golden files pin.
            let body = serde_json::to_vec(refusal).map_err(|e| ServeIoError(e.to_string()))?;
            return send_error_bytes(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::RunPaused(refusal) => {
            // Sent exactly as the broca-wire type serializes: the bytes its golden file pins.
            let body = serde_json::to_vec(&broca_wire::RunPaused::new(
                refusal.run_id.clone(),
                refusal.reason,
            ))
            .map_err(|e| ServeIoError(e.to_string()))?;
            return send_error_bytes(writer, ver, channel, epoch, corr, body).await;
        }
    };
    let mut body = ErrorBody::new(code, err.to_string());
    if matches!(err, ServeError::HistoryRewriting) {
        body = body.with_detail(serde_json::json!({"class": HISTORY_REWRITING_REFUSAL.1}));
    }
    if let ServeError::ServiceTierInvalid { reason, .. } = err {
        body = body.with_detail(serde_json::json!({"field":"service_tier", "reason":reason}));
    }
    send_error_body(writer, ver, channel, epoch, corr, body).await
}

/// The bind refusal for an unsupported role version or multiple supported roles on one route.
/// Each route has only one operation namespace, including one `role.describe`.
pub const ROLE_VERSIONS_UNSUPPORTED: &str = "role_versions_unsupported";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum RouteRole {
    #[default]
    LlmRunner,
    Classifier,
    Extractor,
    ToolProvider,
    Permissions,
    /// A session's compaction provider reading entries and pushing views. Session-bound like
    /// llm-runner, but with its own operation namespace and caller check.
    CompactionRunner,
}

/// Adding model tools admits only the matching tool-provider role, not arbitrary new route
/// targets. Internal agent-data and management routes retain their existing target rules.
fn serves_bind_target(target: &subc_protocol::RouteTarget, role: RouteRole, tools: bool) -> bool {
    match target {
        subc_protocol::RouteTarget::ManagementSurface { .. } => true,
        subc_protocol::RouteTarget::InternalService { service_id, .. } => {
            service_id == broca_wire::agent_data::SERVICE_ID
        }
        subc_protocol::RouteTarget::ToolProvider { .. } => role == RouteRole::ToolProvider && tools,
    }
}

impl RouteRole {
    fn is_sessionless(self) -> bool {
        !matches!(self, Self::LlmRunner | Self::CompactionRunner)
    }
}

/// Tool-provider targets identify their role without a declaration. A declaration may pin
/// that role's version, but cannot switch the target to another operation namespace.
fn bind_target_role(
    target: &subc_protocol::RouteTarget,
    role_versions: Option<&std::collections::BTreeMap<String, String>>,
    serves_classifier: bool,
    serves_extractor: bool,
) -> Result<RouteRole, String> {
    if !matches!(target, subc_protocol::RouteTarget::ToolProvider { .. }) {
        return bind_role(role_versions, serves_classifier, serves_extractor);
    }
    if let Some(roles) = role_versions {
        if let Some(other) = roles.keys().find(|role| role.as_str() != "tool-provider") {
            return Err(format!(
                "a route serves one role; the tool-provider target conflicts with declared role {other}"
            ));
        }
    }
    bind_role(role_versions, serves_classifier, serves_extractor)?;
    Ok(RouteRole::ToolProvider)
}

/// Select a supported role, retaining llm-runner for legacy or unrelated declarations.
fn bind_role(
    role_versions: Option<&std::collections::BTreeMap<String, String>>,
    serves_classifier: bool,
    serves_extractor: bool,
) -> Result<RouteRole, String> {
    let Some(roles) = role_versions else {
        return Ok(RouteRole::LlmRunner);
    };
    let supported = [
        ("classifier", "v1", RouteRole::Classifier),
        (
            cortexkit_role_extractor::ROLE,
            cortexkit_role_extractor::VERSION,
            RouteRole::Extractor,
        ),
        ("llm-runner", "v1", RouteRole::LlmRunner),
        ("tool-provider", "v1", RouteRole::ToolProvider),
        ("permissions", "v1", RouteRole::Permissions),
        ("compaction-runner", "v1", RouteRole::CompactionRunner),
    ];
    let mut declared_roles = supported
        .into_iter()
        .filter(|(role, _, _)| roles.contains_key(*role));
    let Some((role, version, selected)) = declared_roles.next() else {
        return Ok(RouteRole::LlmRunner);
    };
    let declared = &roles[role];
    if declared != version {
        return Err(format!(
            "this build serves {role} {version} only; the route declares {role} {declared}"
        ));
    }
    if let Some((other, _, _)) = declared_roles.next() {
        return Err(format!(
            "a route serves one role; this one declares both {role} and {other}"
        ));
    }
    if (selected == RouteRole::Classifier && !serves_classifier)
        || (selected == RouteRole::Extractor && !serves_extractor)
    {
        return Err(format!("this build does not serve {role} {version}"));
    }
    Ok(selected)
}

#[cfg(test)]
#[path = "serve_role_selection_tests.rs"]
mod role_selection_tests;

#[path = "system_one/route.rs"]
mod system_one_route;

/// `role.describe` params: none.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleDescribeParams {}

/// Decode a strict request's `params`, a JSON `null` meaning the empty request. A refusal names
/// the member at fault when it can be told.
fn decode_params<T: serde::de::DeserializeOwned + Default>(
    params: &serde_json::Value,
) -> Result<T, (Option<String>, String)> {
    if params.is_null() {
        return Ok(T::default());
    }
    serde_json::from_value(params.clone())
        .map_err(|e| (broca_wire::failing_field::<T>(params, &e), e.to_string()))
}

/// An `invalid_params` refusal, naming the member at fault in `detail.field` when known.
async fn send_invalid_params(
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    field: Option<String>,
    message: &str,
) -> Result<(), ServeIoError> {
    let mut body = ErrorBody::new(broca_wire::INVALID_PARAMS, message);
    if let Some(field) = field {
        body = body.with_detail(
            serde_json::to_value(broca_wire::FieldDetail { field })
                .map_err(|e| ServeIoError(e.to_string()))?,
        );
    }
    send_error_body(writer, ver, channel, epoch, corr, body).await
}

/// Answer a `session.refresh`, `session.refresh_policy` or `session.flush_prefix` refusal.
/// Every refusal writes nothing. Owner mismatches reuse the session-baseline owner check
/// so a non-owner gets `scope_owner_mismatch` on all three ops.
async fn send_session_change_error(
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    error: broca_module::serve::SessionChangeError,
) -> Result<(), ServeIoError> {
    use broca_module::serve::SessionChangeError;
    match error {
        SessionChangeError::OwnerMismatch(mismatch) => {
            let serve = ServeError::ScopeRefused(broca_core::ScopeRefusal::OwnerMismatch(mismatch));
            send_serve_error(writer, ver, channel, epoch, corr, &serve).await
        }
        SessionChangeError::InvalidParams { field, message } => {
            send_invalid_params(writer, ver, channel, epoch, corr, Some(field), &message).await
        }
        SessionChangeError::SendIdReuse { field, message } => {
            let body = ErrorBody::new(cortexkit_role_llm_runner::errors::SEND_ID_REUSE, message)
                .with_detail(serde_json::json!({"field": field}));
            send_error_body(writer, ver, channel, epoch, corr, body).await
        }
        SessionChangeError::RungUnsupported { surface, rung } => {
            let detail = cortexkit_role_llm_runner::session_change::RungUnsupportedDetail::new(
                surface.clone(),
                rung.clone(),
            );
            let body = ErrorBody::new(
                cortexkit_role_llm_runner::errors::RUNG_UNSUPPORTED,
                format!("this session does not support {rung} for {surface}"),
            )
            .with_detail(serde_json::to_value(detail).map_err(|e| ServeIoError(e.to_string()))?);
            send_error_body(writer, ver, channel, epoch, corr, body).await
        }
        SessionChangeError::GenerationStale { accepted } => {
            let detail =
                cortexkit_role_llm_runner::session_change::GenerationStaleDetail::new(accepted);
            let body = ErrorBody::new(
                cortexkit_role_llm_runner::errors::GENERATION_STALE,
                format!("generation is at or below the accepted generation {accepted}: send a higher generation, or repeat the accepted plan and policy exactly"),
            )
            .with_detail(
                serde_json::to_value(detail).map_err(|e| ServeIoError(e.to_string()))?,
            );
            send_error_body(writer, ver, channel, epoch, corr, body).await
        }
        SessionChangeError::PendingSuperseded {
            generation,
            pending,
        } => {
            let detail = cortexkit_role_llm_runner::session_change::PendingSupersededDetail::new(
                generation, pending,
            );
            let body = ErrorBody::new(
                cortexkit_role_llm_runner::errors::PENDING_SUPERSEDED,
                format!("generation {generation} is no longer pending; generation {pending} is"),
            )
            .with_detail(serde_json::to_value(detail).map_err(|e| ServeIoError(e.to_string()))?);
            send_error_body(writer, ver, channel, epoch, corr, body).await
        }
        SessionChangeError::AlreadyApplied {
            generation,
            applied_by,
        } => {
            let detail = cortexkit_role_llm_runner::session_change::AlreadyAppliedDetail::new(
                generation,
                applied_by.clone(),
            );
            let body = ErrorBody::new(
                cortexkit_role_llm_runner::errors::ALREADY_APPLIED,
                format!("generation {generation} was applied by {applied_by}"),
            )
            .with_detail(serde_json::to_value(detail).map_err(|e| ServeIoError(e.to_string()))?);
            send_error_body(writer, ver, channel, epoch, corr, body).await
        }
        SessionChangeError::FetchRefused(refusal) => {
            let body =
                ErrorBody::new(refusal.code(), refusal.message()).with_detail(refusal.detail());
            send_error_body(writer, ver, channel, epoch, corr, body).await
        }
        SessionChangeError::CompactionNotAddable(refusal) => {
            let body = ErrorBody::new(
                cortexkit_role_llm_runner::errors::INVALID_PARAMS,
                refusal.message(),
            )
            .with_detail(broca_module::serve::compaction_not_addable_detail(refusal));
            send_error_body(writer, ver, channel, epoch, corr, body).await
        }
        SessionChangeError::Transient(message) => {
            let serve = ServeError::Transient(message);
            send_serve_error(writer, ver, channel, epoch, corr, &serve).await
        }
        SessionChangeError::Unsupported(op) => {
            send_error_body(
                writer,
                ver,
                channel,
                epoch,
                corr,
                ErrorBody::new("open_failed", format!("{op} is unsupported")),
            )
            .await
        }
    }
}

fn delivery_list_refusal(
    delivery: Option<&broca_wire::SendDelivery>,
    describe: &cortexkit_role_llm_runner::describe::RoleDescribe,
) -> Option<ErrorBody> {
    let broca_wire::SendDelivery::Order(modes) = delivery? else {
        return None;
    };
    if modes.iter().any(|mode| mode.is_declared_by(describe)) {
        return None;
    }
    Some(
        ErrorBody::new(
            broca_wire::DELIVERY_UNSUPPORTED,
            "none of the requested delivery modes are declared",
        )
        .with_detail(serde_json::json!({"delivery":modes})),
    )
}

/// An output-reference refusal (llm-runner §9.13) under its error code.
async fn send_output_ref_refusal(
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    refusal: broca_module::output_refs::OutputRefRefusal,
) -> Result<(), ServeIoError> {
    use broca_module::output_refs::OutputRefRefusal;
    match refusal {
        OutputRefRefusal::UnknownRef(message) => {
            send_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                cortexkit_role_llm_runner::errors::UNKNOWN_REF,
                &message,
            )
            .await
        }
        OutputRefRefusal::InvalidParams { field, message } => {
            send_invalid_params(
                writer,
                ver,
                channel,
                epoch,
                corr,
                Some(field.into()),
                &message,
            )
            .await
        }
        OutputRefRefusal::ReplyTooLarge(message) => {
            send_error(
                writer,
                ver,
                channel,
                epoch,
                corr,
                cortexkit_role_llm_runner::errors::REPLY_TOO_LARGE,
                &message,
            )
            .await
        }
    }
}

/// A fork operation's refusal (llm-runner `fork_answer`) under its code and detail.
/// One line per fork operation, with its outcome: `ok` or the refusal code its caller got. A
/// fork's caller is the only other party that sees the outcome, so without this line a failed
/// ask leaves nothing to trace. Never the question or the answer.
fn log_fork_op<T>(
    op: &str,
    session: &broca_module::LineageKey,
    fork_id: Option<&str>,
    ask_id: Option<&str>,
    answered: Option<&str>,
    result: &Result<T, broca_module::fork_answer::ForkError>,
    started: std::time::Instant,
) {
    let elapsed_ms = started.elapsed().as_millis() as u64;
    match result {
        Ok(_) => tracing::info!(target: "fork", op, session = %session.session, fork_id, ask_id,
            outcome = "ok", answered, elapsed_ms, "fork operation"),
        Err(error) => tracing::warn!(target: "fork", op, session = %session.session, fork_id,
            ask_id, outcome = error.code(), message = %error.message(), elapsed_ms,
            "fork operation refused"),
    }
}

async fn send_fork_error(
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    error: &broca_module::fork_answer::ForkError,
) -> Result<(), ServeIoError> {
    let mut body = ErrorBody::new(error.code(), error.message());
    if let Some(detail) = error.detail() {
        body = body.with_detail(detail);
    }
    send_error_body(writer, ver, channel, epoch, corr, body).await
}

async fn send_error(
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    code: &str,
    message: &str,
) -> Result<(), ServeIoError> {
    send_error_body(
        writer,
        ver,
        channel,
        epoch,
        corr,
        ErrorBody::new(code, message),
    )
    .await
}

async fn send_error_body(
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    body: ErrorBody,
) -> Result<(), ServeIoError> {
    let body = serde_json::to_vec(&body).map_err(|e| ServeIoError(e.to_string()))?;
    send_error_bytes(writer, ver, channel, epoch, corr, body).await
}

/// Ends a subscribe stream with a scope refusal's wire body, carrying the stream's next seq and
/// a display reset as every subscribe stream error does.
async fn send_subscribe_scope_error(
    writer: &EgressSender<Frame>,
    (ver, channel, epoch, corr): (u8, u16, u32, u64),
    seq: u64,
    error: &ServeError,
) -> Result<(), ServeIoError> {
    let mut body: serde_json::Map<String, serde_json::Value> =
        serde_json::from_slice(&scope_error_bytes(error)?)
            .map_err(|error| ServeIoError(error.to_string()))?;
    if let Some(code) = body.get("code").and_then(serde_json::Value::as_str) {
        record_request_outcome(code);
    }
    body.insert("seq".into(), seq.into());
    body.insert(
        "event".into(),
        serde_json::to_value(cortexkit_role_llm_runner::live::LiveEvent::DisplayReset)
            .map_err(|error| ServeIoError(error.to_string()))?,
    );
    let body = serde_json::to_vec(&body).map_err(|error| ServeIoError(error.to_string()))?;
    send_error_bytes(writer, ver, channel, epoch, corr, body).await
}

/// The wire body of a scope or reader-policy refusal.
fn scope_error_bytes(err: &ServeError) -> Result<Vec<u8>, ServeIoError> {
    let encoded = match err {
        ServeError::ReadAccessRefused => serde_json::to_vec(&ErrorBody::new(
            "scope_owner_mismatch",
            "the session's read policy does not admit this caller",
        )),
        ServeError::ScopeRefused(broca_core::ScopeRefusal::FlowScopeRequired) => {
            serde_json::to_vec(&broca_wire::FlowPrincipalRefusal::new(
                true,
                &format!("reserved:{}", broca_core::scope_admission::FLOW_ONLY_MODULE),
            ))
        }
        ServeError::ScopeRefused(broca_core::ScopeRefusal::SenderNotOwner) => {
            serde_json::to_vec(&broca_wire::FlowPrincipalRefusal::new(
                false,
                &format!("reserved:{}", broca_core::scope_admission::FLOW_ONLY_MODULE),
            ))
        }
        ServeError::ScopeRefused(broca_core::ScopeRefusal::AdoptionRefused(refusal)) => {
            serde_json::to_vec(&adoption_refused(refusal))
        }
        ServeError::ScopeRefused(broca_core::ScopeRefusal::OwnerMismatch(mismatch)) => {
            let mut body = match &**mismatch {
                // Same code and detail as a first-principal refusal; only the message differs.
                broca_core::OwnerMismatch::Reserved {
                    owner,
                    presented_principal,
                } => broca_wire::ScopeOwnerMismatch::reserved(
                    owner.as_owner_string(),
                    presented_principal
                        .as_ref()
                        .map(broca_protocol::BindPrincipal::as_owner_string),
                ),
                _ => broca_wire::ScopeOwnerMismatch::new(owner_mismatch_detail(mismatch)),
            };
            if let broca_core::OwnerMismatch::OwnerOperation { first, mismatch } = &**mismatch {
                let first = first.as_owner_string();
                let accepted = match &**mismatch {
                    broca_core::OwnerMismatch::Scoped { frozen, .. }
                        if !broca_protocol::same_owner_string(
                            &frozen.owner.as_owner_string(),
                            &first,
                        ) =>
                    {
                        format!("'{first}' or '{}'", frozen.owner.as_owner_string())
                    }
                    _ => format!("'{first}'"),
                };
                body.message = format!("owner operations require {accepted} on a plain or unscoped route; flow- and run-stamped routes are senders, never owners");
            }
            serde_json::to_vec(&body)
        }
        ServeError::ScopeRefused(broca_core::ScopeRefusal::FlowMismatch { field }) => {
            let field = match field {
                broca_core::scope_admission::FlowMismatchField::Sender => {
                    broca_wire::FlowMismatchField::Sender
                }
                broca_core::scope_admission::FlowMismatchField::FlowId => {
                    broca_wire::FlowMismatchField::FlowId
                }
            };
            serde_json::to_vec(&broca_wire::ScopeFlowMismatch::new(field))
        }
        ServeError::ScopeRefused(broca_core::ScopeRefusal::RunMismatch { field }) => {
            let field = match field {
                broca_core::scope_admission::RunMismatchField::RunId => {
                    broca_wire::RunMismatchField::RunId
                }
                broca_core::scope_admission::RunMismatchField::AgentId => {
                    broca_wire::RunMismatchField::AgentId
                }
            };
            serde_json::to_vec(&broca_wire::ScopeRunMismatch::new(field))
        }
        ServeError::ScopeRefused(broca_core::ScopeRefusal::AgentRunScopesUnsupported { scope }) => {
            serde_json::to_vec(&broca_wire::AgentRunScopesUnsupported::new(
                (**scope).clone(),
            ))
        }
        ServeError::ScopeRefused(broca_core::ScopeRefusal::Unsupported { scope }) => {
            serde_json::to_vec(&ErrorBody::new(
                broca_wire::SCOPE_UNSUPPORTED,
                format!(
                    "this session is scoped to '{}' (ref '{}') and the subc daemon does not \
                     advertise scopes/v1, so no route to it can carry the owner's stamp",
                    scope.owner.as_owner_string(),
                    scope.scope_ref
                ),
            ))
        }
        // The run-start guard and the plan check refuse the same thing, so they share one body.
        ServeError::ScopeRefused(broca_core::ScopeRefusal::ScopeRequired { .. }) => {
            serde_json::to_vec(&worker_scope_error())
        }
        ServeError::ScopeFreezeBusy { reason, run_id } => {
            let detail = match (reason, run_id) {
                (broca_module::actor::ScopeFreezeBusyReason::RunActive, Some(run_id)) => {
                    broca_wire::ScopeFreezeBusyDetail::RunActive {
                        run_id: run_id.clone(),
                    }
                }
                _ => broca_wire::ScopeFreezeBusyDetail::QueueNonempty,
            };
            serde_json::to_vec(&broca_wire::ScopeFreezeBusy::new(detail))
        }
        other => unreachable!("not a scope refusal: {other}"),
    };
    encoded.map_err(|e| ServeIoError(e.to_string()))
}

fn adoption_refused(refusal: &broca_core::AdoptionRefusal) -> broca_wire::ScopeAdoptionRefused {
    broca_wire::ScopeAdoptionRefused::new(match refusal {
        broca_core::AdoptionRefusal::FlowScope => broca_wire::ScopeAdoptionRefusedDetail::FlowScope,
        broca_core::AdoptionRefusal::NoRecordedPrincipal => {
            broca_wire::ScopeAdoptionRefusedDetail::NoRecordedPrincipal
        }
        broca_core::AdoptionRefusal::PrincipalMismatch { recorded } => {
            broca_wire::ScopeAdoptionRefusedDetail::principal_mismatch(recorded)
        }
    })
}

fn owner_mismatch_detail(
    mismatch: &broca_core::OwnerMismatch,
) -> broca_wire::ScopeOwnerMismatchDetail {
    let spell = |principal: &Option<broca_protocol::BindPrincipal>| {
        principal
            .as_ref()
            .map(broca_protocol::BindPrincipal::as_owner_string)
    };
    match mismatch {
        broca_core::OwnerMismatch::OwnerOperation { mismatch, .. } => {
            owner_mismatch_detail(mismatch)
        }
        broca_core::OwnerMismatch::Scoped {
            frozen: scope,
            presented_principal,
            ..
        }
        | broca_core::OwnerMismatch::NotTheOwner {
            stamped: scope,
            presented_principal,
        } if scope.run_id.is_some() => broca_wire::ScopeOwnerMismatchDetail::AgentRunSender {
            owner: scope.owner.as_owner_string(),
            scope_ref: scope.scope_ref.clone(),
            scope_epoch: scope.scope_epoch,
            presented_principal: spell(presented_principal),
        },
        broca_core::OwnerMismatch::Scoped {
            frozen,
            presented_principal,
            presented_scope,
        } => broca_wire::ScopeOwnerMismatchDetail::Scoped {
            owner: frozen.owner.as_owner_string(),
            scope_ref: frozen.scope_ref.clone(),
            scope_epoch: frozen.scope_epoch,
            presented_principal: spell(presented_principal),
            presented_scope: presented_scope
                .as_ref()
                .map(|scope| broca_wire::PresentedScope {
                    owner: scope.owner.as_owner_string(),
                    scope_ref: scope.scope_ref.clone(),
                    scope_epoch: scope.scope_epoch,
                }),
        },
        broca_core::OwnerMismatch::NotTheOwner {
            stamped,
            presented_principal,
        } => broca_wire::ScopeOwnerMismatchDetail::NotTheOwner {
            owner: stamped.owner.as_owner_string(),
            scope_ref: stamped.scope_ref.clone(),
            scope_epoch: stamped.scope_epoch,
            presented_principal: spell(presented_principal),
        },
        broca_core::OwnerMismatch::FirstPrincipal {
            first,
            presented_principal,
        }
        | broca_core::OwnerMismatch::Reserved {
            owner: first,
            presented_principal,
        } => broca_wire::ScopeOwnerMismatchDetail::FirstPrincipal {
            first: first.as_owner_string(),
            presented_principal: spell(presented_principal),
        },
    }
}

async fn send_error_bytes(
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    mut body: Vec<u8>,
) -> Result<(), ServeIoError> {
    if body.len().saturating_add(subc_protocol::HEADER_LEN)
        > broca_wire::SUBC_MAX_FRAME_BYTES as usize
    {
        body = serde_json::to_vec(&ErrorBody::new(
            "response_too_large",
            "error response exceeds the transport frame limit",
        ))
        .map_err(|error| ServeIoError(error.to_string()))?;
    }
    if let Ok(error) = serde_json::from_slice::<ErrorBody>(&body) {
        record_request_outcome(&error.code);
    }
    let frame = Frame::build_with_version(
        ver,
        FrameType::Error,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        body,
    )
    .map_err(|e| ServeIoError(e.to_string()))?;
    send(writer, frame).await
}

async fn send(writer: &EgressSender<Frame>, frame: Frame) -> Result<(), ServeIoError> {
    let _phase = stall::enter(Phase::EgressSend).await;
    // Every Response and Error a request writes passes here, so this is where the request's
    // terminal is recorded. It is recorded before the frame is handed off: once ownership
    // leaves this task the frame may reach the daemon.
    if matches!(frame.header.ty, FrameType::Response | FrameType::Error) {
        mark_terminal_sent();
    }
    writer.send(frame).await.map_err(|_| {
        record_request_outcome("egress_closed");
        ServeIoError("egress channel closed".into())
    })
}

// Codes are deliberately a closed diagnostic vocabulary, never hashes or copies of
// caller-provided method text. Unknown/new methods still carry their route channel.
fn method_code(method: &str) -> u8 {
    match method {
        OP_SEND => 3,
        OP_READ => 4,
        OP_SUBSCRIBE => 5,
        OP_STATUS => 6,
        OP_CANCEL => 7,
        OP_RETRACT => 8,
        OP_ROLE_DESCRIBE => 9,
        OP_VAULT_AUDIT => 10,
        OP_IMPORT => 11,
        OP_HEAD => 12,
        OP_BASELINE => 13,
        // 14 and 15 named the removed spend-cap operations (`cap.install`, `spend.delta`).
        // They are not reused, so a code in an older stall report keeps its meaning.
        OP_RUN_RESULT => 16,
        OP_WARM => 17,
        OP_RUN_TOOL_CALLS => 19,
        OP_INSTALL_VIEW => 20,
        OP_MEMORY_REPORT => 21,
        _ => 255,
    }
}

// Correlated milestones distinguish a request that never reached serve from one parked at
// admission, dispatch, or egress. Debug-only, and no prompt or response bodies are recorded.
fn trace_describe(stage: &str, channel: u16, epoch: u32, corr: u64) {
    tracing::debug!(target: "wire.describe", stage, channel, epoch, corr, "role.describe progress");
}

fn trace_egress(stage: &str, header: &subc_protocol::EnvelopeHeader) {
    tracing::debug!(
        target: "wire.egress",
        stage,
        channel = header.channel,
        epoch = header.epoch,
        corr = header.corr,
        ty = ?header.ty,
        "module egress progress"
    );
}

fn control_flags() -> Flags {
    Flags::new(false, Priority::Passive, false)
}

/// Drains the egress channel to the write half, flushing on quiescence.
/// Whether an I/O error means the daemon's end of the connection is gone, which ends serve
/// normally rather than as a fault.
///
/// A daemon cut does not close the socket cleanly: the daemon exits and the kernel RESETS it.
/// Measured 2026-09-24: macOS reported "Connection reset by peer (os error 54)" on the read
/// half, and Windows CI reported "An existing connection was forcibly closed by the remote
/// host. (os error 10054)" on the WRITE half, because a frame was still being written when
/// the reset landed. So the rule has to hold on both halves, or whichever half meets the reset
/// first turns a clean stop into exit 1 and a recorded crash. Both OS errors map to
/// ConnectionReset. A torn frame (FrameIoError::UnexpectedEof) is not listed: that could be a
/// protocol fault and stays an error.
pub(crate) fn peer_went_away(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
    )
}

async fn drain_writer<W>(write_half: W, mut rx: EgressReceiver<Frame>) -> Result<(), ServeIoError>
where
    W: AsyncWrite + Unpin,
{
    struct ClearOperation(Arc<broca_diagnostics::Egress>);
    impl Drop for ClearOperation {
        fn drop(&mut self) {
            self.0.operation.store(0, Ordering::Relaxed);
        }
    }
    let _operation_lifetime = ClearOperation(rx.state.clone());
    // A write the daemon can no longer receive ends the writer quietly: see peer_went_away.
    let written = |result: Result<(), FrameIoError>| match result {
        Ok(()) => Ok(true),
        Err(FrameIoError::Io(error)) if peer_went_away(error.kind()) => Ok(false),
        Err(error) => Err(ServeIoError(error.to_string())),
    };
    let flushed = |result: std::io::Result<()>| match result {
        Ok(()) => Ok(true),
        Err(error) if peer_went_away(error.kind()) => Ok(false),
        Err(error) => Err(ServeIoError(error.to_string())),
    };
    let mut writer = BufWriter::new(write_half);
    while let Some(frame) = rx.recv().await {
        rx.state
            .operation
            .store(broca_diagnostics::now_ms() << 1, Ordering::Relaxed);
        trace_egress("write_started", &frame.header);
        if !written(write_frame(&mut writer, &frame).await)? {
            return Ok(());
        }
        rx.state.written.fetch_add(1, Ordering::Relaxed);
        trace_egress("buffered", &frame.header);
        let mut last_header = frame.header;
        while let Ok(frame) = rx.try_recv() {
            rx.state
                .operation
                .store(broca_diagnostics::now_ms() << 1, Ordering::Relaxed);
            trace_egress("write_started", &frame.header);
            if !written(write_frame(&mut writer, &frame).await)? {
                return Ok(());
            }
            rx.state.written.fetch_add(1, Ordering::Relaxed);
            trace_egress("buffered", &frame.header);
            last_header = frame.header;
        }
        // A flush covers every preceding buffered frame in this batch, not only its last one.
        rx.state
            .operation
            .store((broca_diagnostics::now_ms() << 1) | 1, Ordering::Relaxed);
        trace_egress("flush_started", &last_header);
        if !flushed(writer.flush().await)? {
            return Ok(());
        }
        trace_egress("flushed", &last_header);
        rx.state
            .flushed
            .store(rx.state.written.load(Ordering::Relaxed), Ordering::Relaxed);
        rx.state.operation.store(0, Ordering::Relaxed);
    }
    rx.state
        .operation
        .store((broca_diagnostics::now_ms() << 1) | 1, Ordering::Relaxed);
    flushed(writer.flush().await)?;
    rx.state.operation.store(0, Ordering::Relaxed);
    Ok(())
}

/// An IO/wire error in the serve loop.
#[derive(Debug)]
pub struct ServeIoError(pub String);

impl std::fmt::Display for ServeIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "serve io error: {}", self.0)
    }
}

impl std::error::Error for ServeIoError {}

#[cfg(test)]
#[path = "queued_admission_tests.rs"]
mod queued_admission_tests;

#[cfg(test)]
#[path = "serve_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "serve_open_failed_wire_classes.rs"]
mod open_failed_wire_classes;

#[cfg(test)]
#[path = "serve_agent_run_writer_tests.rs"]
mod agent_run_writer_tests;

#[cfg(test)]
#[path = "serve_agent_data_refusal_vectors.rs"]
mod agent_data_refusal_vectors;

#[cfg(test)]
#[path = "serve_delivery_list_tests.rs"]
mod delivery_list_tests;

#[cfg(test)]
#[path = "serve_classifier_tests.rs"]
mod classifier_route_tests;

#[cfg(test)]
#[path = "serve_flow_compat_tests.rs"]
mod flow_management_compat_tests;

#[cfg(test)]
#[path = "serve_withdrawal_health_tests.rs"]
mod withdrawal_health_tests;

#[cfg(test)]
#[path = "serve_run_id_tests.rs"]
mod run_id_tests;

#[cfg(test)]
#[path = "serve_test_openers.rs"]
mod test_openers;
