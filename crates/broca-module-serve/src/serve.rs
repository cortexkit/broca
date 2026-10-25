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
//! and replays the WAL projection), never guaranteed-live. The egress-closed → clean
//! subscriber teardown → client-resubscribes path IS the reliability mechanism. (v-next, if
//! display-flood-disrupting-live-control ever measures as a real problem, the fix is
//! splitting display onto its own route; v1 keeps the unified stream + replay-covers-it.)

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use crate::stall::{self, Phase};
use broca_diagnostics::{Receiver as EgressReceiver, Sender as EgressSender};
use broca_module::serve::{
    ServeError, ServeHandler, SessionImportRequest, SessionReadRequest, SubscribeFrom,
};
use broca_module::{Delivery, LineageRegistry};
use broca_protocol::BindIdentity as BrocaBindIdentity;
use broca_wire::{encode_live_subscribe_control, encode_live_subscribe_display};
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
    OP_BASELINE, OP_CANCEL, OP_HEAD, OP_IMPORT, OP_READ, OP_RESOLVE, OP_RETRACT, OP_ROLE_DESCRIBE,
    OP_RUN_EVENT_GET, OP_RUN_RESULT, OP_RUN_TOOL_CALLS, OP_SEND, OP_SEND_STATUS, OP_STATUS,
    OP_SUBSCRIBE, OP_VAULT_AUDIT, OP_WARM,
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

async fn log_served_request<F: std::future::Future>(
    operation: String,
    session: String,
    future: F,
) -> F::Output {
    let started = Instant::now();
    let (output, actor_opened) = broca_module::registry::with_request_actor_opened(future).await;
    emit_request_log(
        &operation,
        &session,
        actor_opened,
        started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
    );
    output
}

fn emit_request_log(operation: &str, session: &str, actor_opened: bool, elapsed_us: u64) {
    tracing::debug!(target: "requests", operation, session, actor_opened, elapsed_us, "served management request");
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

#[derive(Debug, PartialEq, Eq)]
struct ProjectionHealthReport {
    failures: u64,
    behind_sessions: u64,
    detail: Option<String>,
}

fn projection_health_report(failures: u64, behind_sessions: u64) -> ProjectionHealthReport {
    let detail = (behind_sessions > 0).then(|| {
        format!(
            "store projection behind on {behind_sessions} session(s); their reads replay the WAL tail until a projection succeeds"
        )
    });
    ProjectionHealthReport {
        failures,
        behind_sessions,
        detail,
    }
}

fn health_status(details: &[String]) -> subc_protocol::session::HealthStatus {
    if details.is_empty() {
        subc_protocol::session::HealthStatus::Ok
    } else {
        subc_protocol::session::HealthStatus::Degraded
    }
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

/// Mint a globally-unique run_id for a fresh send. `run-<session>-<launch-nonce>-<seq>`:
/// unique across connections (process-global seq) and restarts (per-launch nonce), so a
/// new run NEVER collides with an old finished run's id — which would otherwise let the
/// durable run index clobber a terminal row, or `run.cancel` target the wrong run. A
/// RESUME reuses its OWN original run_id (the coordinator reads it from the WAL); only a
/// FRESH send mints here, so re-indexing the same id on resume stays correct.
fn mint_run_id(session: &str) -> broca_protocol::RunId {
    use std::sync::atomic::AtomicU64;
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::SeqCst);
    broca_protocol::RunId(format!("run-{session}-{}-{seq}", launch_nonce()))
}

/// The stable session-scope bytes used when deriving a deterministic run id from a client
/// `send_id`. The trailing unit separator keeps `(session, send_id)` pairs unambiguous when
/// the caller bytes are appended.
fn send_id_scope(session: &BrocaBindIdentity) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}",
        session.project_root.display(),
        session.harness,
        session.session
    )
}

/// Refuse an execute-class op whose bound project root no longer exists.
///
/// Two ops need the directory to be real. `session.send` USES it: tools dispatched to the
/// tool module resolve paths beneath it, so admitting a send into a vanished root produces a
/// run that dies at its first tool call, several steps from the cause. `session.import`
/// CREATES durable state under that name, and seeding a session under a root that does not
/// exist is how an unreachable session gets manufactured in the first place.
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

/// Resolve the run id for one `session.send`. A fresh send with no client id gets a unique
/// server-minted id; a send with `send_id` gets a deterministic session-scoped id so a retry
/// reuses the same durable episode identity.
fn run_id_for_send(session: &BrocaBindIdentity, send_id: Option<&str>) -> broca_protocol::RunId {
    match send_id {
        None => mint_run_id(&session.session),
        Some(send_id) => {
            let mut seed = send_id_scope(session);
            seed.push_str(send_id);
            let digest = broca_provider::hash::fnv1a128(&seed);
            broca_protocol::RunId(format!("run-sid-{}", broca_provider::hash::base62(digest)))
        }
    }
}

/// Map a subc route bind identity to the broca session key. The triples are
/// field-identical; this ONE mapping is the adapter boundary (like the SubcToolPlane's
/// identity mapping on the consumer side).
fn run_id_for_import(session: &BrocaBindIdentity, import_id: &str) -> broca_protocol::RunId {
    let mut seed = send_id_scope(session);
    seed.push_str("import:");
    seed.push_str(import_id);
    let digest = broca_provider::hash::fnv1a128(&seed);
    broca_protocol::RunId(format!("run-iid-{}", broca_provider::hash::base62(digest)))
}

fn to_broca_identity(id: &SubcBindIdentity) -> BrocaBindIdentity {
    BrocaBindIdentity {
        project_root: id.project_root.clone(),
        harness: id.harness.clone(),
        session: id.session.clone(),
    }
}

/// What a `route.bind` said about its caller beyond the session identity. Both values are
/// recorded on the `RunStarted` of every send or import made over the route, and neither keys
/// anything: the same session bound with different observations is still one lineage.
#[derive(Debug, Clone, Default, PartialEq)]
struct BindObservation {
    agent_data: bool,
    /// Only llm-runner routes address a session. Other roles serve their own operations
    /// without opening a session or reading its history.
    role: RouteRole,
    /// The producer's registered project id, as the client supplied it (unattested).
    project_id: Option<String>,
    /// The daemon-stamped principal, converted without change. `None` when the bind carried
    /// none, which is what a daemon that predates principal stamping sends.
    principal: Option<broca_protocol::BindPrincipal>,
    /// The daemon's scope stamp on the bind, or why there is none. Every send over the route
    /// carries it to admission, which decides the owner check and the scope to freeze.
    scope: broca_protocol::SendScope,
}

/// Preserve the scope identity, including the agent and run identifiers of an agent-run scope
/// and its opener. The scoped bind establishes that the daemon allowed
/// this opener to use the scope, either as its owner or as another module allowed to open
/// routes under it. `owner_authorized` describes the scope owner's authority, not the opener's;
/// broca grants no permission based on that flag or on agent delegation attributes.
fn to_bind_scope(
    stamp: &subc_protocol::scope::ScopeStamp,
    principal: Option<&subc_protocol::Principal>,
) -> broca_protocol::BindScope {
    let flow_id = stamp.attributes.flow_id.clone();
    let run_id = stamp.attributes.run_id.clone();
    let agent_id = run_id
        .as_ref()
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
            scope_ref: stamp.scope_ref.clone(),
            scope_epoch: stamp.scope_epoch,
            flow_id,
            sender,
            agent_id,
            run_id,
        },
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
    agent_data: Option<Arc<crate::agent_data::AgentDataService>>,
    /// System One, when this build serves it; a classifier bind is refused without it.
    system_one: Option<Arc<crate::system_one::SystemOne>>,
    llm_tools: Option<Arc<crate::llm_tools::LlmTools>>,
    // The bind observations (project id, principal) stay beside the session key rather than
    // inside it: BindIdentity also keys the actor registry, so folding them in would give one
    // session WAL a separate actor per distinct observation, all writing the same file.
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
    header: subc_protocol::EnvelopeHeader,
    channel: u16,
    epoch: u32,
    identity: BrocaBindIdentity,
    bind: BindObservation,
    cancel: CancellationToken,
    refusal: Result<Option<broca_core::AdoptionRefusal>, ServeError>,
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

/// Serve one authenticated subc connection with the default catalog and archive configuration.
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
        crate::wal_archive::WalArchiveConfig::default(),
        crate::consumer_routes::ConsumerRoutes::default(),
        None,
        None,
        None,
    )
    .await
}

/// Serve with the same boot configuration that controls the archive timer.
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
    archive: crate::wal_archive::WalArchiveConfig,
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
        archive,
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
    archive: crate::wal_archive::WalArchiveConfig,
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
        archive,
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
    archive: crate::wal_archive::WalArchiveConfig,
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
        archive,
        consumer_routes,
        vault_auditor,
        daemon_capabilities,
        scope_describer,
        agent_data,
        system_one,
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
    archive: crate::wal_archive::WalArchiveConfig,
    consumer_routes: crate::consumer_routes::ConsumerRoutes,
    vault_auditor: Option<Arc<dyn crate::vault_audit::VaultAuditor>>,
    daemon_capabilities: Option<Vec<String>>,
    scope_describer: Option<Arc<broca_subc::ModuleScopeDescriber>>,
    agent_data: Option<Arc<crate::agent_data::AgentDataService>>,
    system_one: Option<Arc<crate::system_one::SystemOne>>,
    llm_tools: Option<Arc<crate::llm_tools::LlmTools>>,
) -> Result<ServeEnd, ServeIoError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
    H: ServeHandler + 'static,
{
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
            archive,
            &consumer_routes,
            vault_auditor,
            daemon_capabilities,
            scope_describer.as_deref(),
            agent_data,
            system_one,
            llm_tools,
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
    // Before the egress queue can close: the describer holds a sender on it.
    if let Some(describer) = &scope_describer {
        describer.detach();
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
    archive: crate::wal_archive::WalArchiveConfig,
    consumer_routes: &crate::consumer_routes::ConsumerRoutes,
    vault_auditor: Option<Arc<dyn crate::vault_audit::VaultAuditor>>,
    daemon_capabilities: Option<Vec<String>>,
    scope_describer: Option<&broca_subc::ModuleScopeDescriber>,
    agent_data: Option<Arc<crate::agent_data::AgentDataService>>,
    system_one: Option<Arc<crate::system_one::SystemOne>>,
    llm_tools: Option<Arc<crate::llm_tools::LlmTools>>,
) -> Result<ServeEnd, ServeIoError>
where
    R: AsyncRead + Unpin,
    H: ServeHandler + 'static,
{
    send_hello(&writer, module_id, archive, consumer_routes).await?;
    let ack = {
        let _phase = stall::enter(Phase::AwaitFrame).await;
        expect_hello_ack(read_half).await?
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
    routes.agent_data = agent_data;
    routes.system_one = system_one;
    routes.llm_tools = llm_tools;
    let request_slots = Arc::new(Semaphore::new(MAX_INFLIGHT_ROUTE_REQUESTS));
    let mut request_tasks = JoinSet::new();

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
                let describe_reply = scope_describer.is_some_and(|describer| describer.on_frame(&frame));
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
            match serde_json::from_slice::<ModuleControlCommand>(&frame.body) {
                Ok(ModuleControlCommand::Draining {
                    reason,
                    deadline_ms,
                }) => {
                    // Keep serving health probes and current tool replies. Starting the seal
                    // timer here would spend the post-GOODBYE grace inside the drain window.
                    health_registry.begin_stop();
                    tracing::info!(target: "lifecycle", ?reason, deadline_ms,
                        "module draining; admission stopped, waiting for step boundaries");
                }
                Err(error) => {
                    tracing::warn!(target: "wire", %error,
                        "ignored unknown or malformed channel-0 control push");
                }
            }
            Ok(true)
        }
        FrameType::Goodbye => {
            // A route GOODBYE: drop the channel binding and cancel any live subscribe
            // forward task on it (explicit unsubscribe → prompt teardown).
            routes.route_closed(frame.header.channel, frame.header.epoch);
            Ok(true)
        }
        FrameType::Request if frame.header.channel == 0 => {
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
            if bind.role == RouteRole::Classifier {
                spawn_classifier_request(
                    frame.body,
                    bind.clone(),
                    routes.system_one.clone(),
                    writer,
                    request_tasks,
                    (ver, channel, epoch, corr),
                );
                return Ok(true);
            }
            if matches!(bind.role, RouteRole::ToolProvider | RouteRole::Permissions) {
                if bind.role == RouteRole::ToolProvider {
                    let service = routes.llm_tools.clone();
                    let writer = writer.clone();
                    request_tasks.spawn(reply_on_panic(
                        writer.clone(),
                        (ver, channel, epoch, corr),
                        async move {
                            let answer = match service {
                                Some(service) => tokio::spawn(async move {
                                    service
                                        .answer(&frame.body, bind.principal.as_ref(), &bind.scope)
                                        .await
                                })
                                .await
                                .unwrap_or_else(|_| {
                                    Err(crate::system_one::refusal_body(
                                        &crate::system_one::internal_refusal(
                                            "model tool request failed",
                                        ),
                                    ))
                                }),
                                None => Err(serde_json::to_vec(&subc_protocol::ErrorBody::new(
                                    "unknown_method",
                                    "model tools are not configured",
                                ))
                                .expect("error encodes")),
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
                                    send_error_bytes(&writer, ver, channel, epoch, corr, body).await
                                }
                            }
                        },
                    ));
                    return Ok(true);
                }
                send_error(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "unknown_method",
                    "this role's operations are not implemented in this build",
                )
                .await?;
                return Ok(true);
            }
            let req: RouteRequest = match serde_json::from_slice(&frame.body) {
                Ok(r) => r,
                Err(e) => {
                    send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "invalid_request",
                        &format!("request body not decodable: {e}"),
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
                    send_serve_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        &ServeError::ScopeRefused(refusal),
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
            ..
        } => {
            stall::method_context(1);
            let role = match bind_target_role(
                &target,
                role_versions.as_ref(),
                routes.system_one.is_some(),
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
            let bind = BindObservation {
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
                project_id: identity.project_id.clone(),
                principal: principal.as_ref().map(to_bind_principal),
                scope: bind_send_scope(scope.as_ref(), routes.daemon_scopes, principal.as_ref()),
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
                identity.validate_keyable()
            } {
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
                header: frame.header,
                channel: route_channel,
                epoch,
                identity,
                bind,
                cancel,
                refusal: Ok(None),
            };
            // Keep the bind unacknowledged until its history authorizes the stamp. The read
            // runs off ingress so a slow WAL/store cannot hide health probes or GOODBYE.
            if !completed.bind.agent_data
                && !completed.bind.role.is_sessionless()
                && matches!(&completed.bind.scope, broca_protocol::SendScope::Stamped(_))
            {
                let registry = health_registry.clone();
                routes
                    .bind_tasks
                    .spawn(async move { authorize_bind(completed, registry).await });
                return Ok(());
            }
            finish_bind(completed, writer, routes).await
        }
        // Health probes read only in-memory gauges stamped by normal dependency calls. A probe
        // must never add catalog, vault, WAL, lease, or store IO to an already degraded daemon.
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
            // Failure attempts remain visible in the counter after recovery; only unresolved
            // sessions degrade health because their reads still replay the WAL tail.
            let projection_health = projection_health_report(
                broca_core::run::projection_failures(),
                broca_core::run::projection_behind_sessions(),
            );
            if let Some(detail) = &projection_health.detail {
                details.push(detail.clone());
            }
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
            if retention_unreadable > 0 {
                details.push(format!("{retention_unreadable} retained logs blocked by unreadability without a current projection"));
            }
            let status = health_status(&details);
            let detail = (!details.is_empty()).then(|| details.join("; "));
            let (stale_epoch_drops, unmatched_terminal_drops) = broca_subc::FRAME_DROPS.counts();
            // EVERY FIGURE BELOW IS THIS PROCESS DESCRIBING ITSELF: in-memory counters and
            // gauges this same process wrote. That is what makes the probe cheap, and it bounds what
            // a green reading can mean -- it says the process believes it is
            // healthy, not that the work landed. The failure it structurally cannot see is one
            // where the process is wrong about its own state, and a peer lost four days to
            // exactly that: their health read green throughout because it asked the process
            // about itself while the durable store held the cause the whole time.
            //
            // So do not add a field here that a caller might read as an assertion about durable
            // state. Anything of that kind has to be derived from the store or the WAL by a
            // reader that did not write them -- scripts/live-runs.py and
            // scripts/audit-export-lane.py are that shape deliberately.
            let mut metrics = serde_json::json!({
                    "agent_data_ops": crate::agent_data::health_counts(routes.agent_data.as_deref()),
                    "retention_unreadable_sessions": retention_unreadable,
                    "retention_repairs_pending": health_registry.retention_repairs_pending(),
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
                    // Requests a module sent toward broca on a route, each answered with
                    // reverse_request_unhandled because broca fronts no person who could answer.
                    // Non-zero means some module is asking and not getting what it wants.
                    "reverse_requests_refused": broca_subc::FRAME_DROPS.reverse_requests_refused(),
                    // Lifetime attempts this process saw fail; recovery clears only the separate
                    // behind-session gauge, never this historical counter.
                    "projection_failures": projection_health.failures,
                    "projection_behind_sessions": projection_health.behind_sessions,
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
            "wal_archive_polls": crate::wal_archive::wal_archive_polls(),
            "bus_events": crate::bus_events::health(),
            "compaction_bus": crate::compaction_bus::health(),
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
            // switched off or unreachable, pinned by the caller...) and how the run's reports to
            // routing fared. Null in a process with no vault credential source, which asks
            // routing nothing.
            "account_selection": crate::account_routing::published_metrics(),
            "system_one": routes.system_one.as_ref().map(|service| service.health()),
                });
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
    // harness or root addresses a different (often empty) WAL, and only this line shows it.
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
    let params: SubscribeParams = decode_params(params)?;
    let from = match params.from {
        None => SubscribeFrom::Live,
        Some(w) => w
            .to_subscribe_from()
            .map_err(|e| (Some("from".into()), e))?,
    };
    Ok((from, params.limits))
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
        let _permit = request_slots
            .acquire_owned()
            .await
            .map_err(|_| ServeIoError("request concurrency limiter closed".into()))?;
        let authorization = tokio::select! {
            _ = conn_cancel.cancelled() => return Ok(()),
            result = authorize_route_request(handler.as_ref(), &session, &bind, &req.method) => result,
        };
        if let Err(error) = authorization {
            return send_serve_error(&writer, ver, channel, epoch, corr, &error).await;
        }
        count_management_operation(OP_VAULT_AUDIT);
        let log_context = tracing::enabled!(target: "requests", tracing::Level::DEBUG)
            .then(|| (req.method.clone(), session.session.clone()));
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
        match log_context {
            Some((operation, session)) => log_served_request(operation, session, request).await,
            None => request.await,
        }
    }));
    Ok(())
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

    request_tasks.spawn(reply_on_panic(writer.clone(), (ver, channel, epoch, corr), async move {
        // Authorization can read durable history, and permits can be held by slow calls.
        // Neither wait belongs on the frame reader: it must still answer PING and GOODBYE.
        let _turn = dispatch_ticket.wait().await;
        let _permit = request_slots.acquire_owned().await
            .map_err(|_| ServeIoError("request concurrency limiter closed".into()))?;
        if describe {
            trace_describe("permit_acquired", channel, epoch, corr);
        }
        let method = match &prepared {
            PreparedRouteRequest::Request(req) => req.method.as_str(),
            PreparedRouteRequest::Subscribe { .. } => OP_SUBSCRIBE,
        };
        if let Err(error) = authorize_route_request(handler.as_ref(), &session, &bind, method).await {
            return send_serve_error(&writer, ver, channel, epoch, corr, &error).await;
        }
        let write = matches!(method, OP_SEND | OP_WARM | OP_IMPORT | OP_CANCEL | OP_RETRACT);
        let write = write || matches!(method, "session.upload_begin" | "session.upload_append" | "session.upload_commit");
        let session_read = matches!(method, OP_READ | OP_RESOLVE | OP_HEAD | OP_RUN_RESULT | OP_RUN_TOOL_CALLS | OP_RUN_EVENT_GET | OP_STATUS | OP_SUBSCRIBE | OP_BASELINE | "session.upload_status" | cortexkit_role_llm_runner::ops::SESSION_HISTORY | cortexkit_role_llm_runner::ops::SESSION_OUTPUT_REFS | cortexkit_role_llm_runner::ops::SESSION_OUTPUT_REF_READ);
        if write || session_read {
            if let Err(error) = handler.check_migration(&session, write).await {
                return send_serve_error(&writer, ver, channel, epoch, corr, &error).await;
            }
        }
        count_management_operation(method);
        let log_context = (method != OP_SUBSCRIBE
            && tracing::enabled!(target: "requests", tracing::Level::DEBUG))
            .then(|| (method.to_owned(), session.session.clone()));
        if describe {
            trace_describe("task_started", channel, epoch, corr);
        }
        // The connection keeps reading while requests run, but actor-bound work for one session
        // takes its turn in wire-arrival order. Separate sessions use separate lanes and remain
        // concurrent; replies may still arrive out of order because correlation ids disambiguate them.
        let request = async move {
            match prepared {
                PreparedRouteRequest::Request(req) => {
                    if describe {
                        trace_describe("turn_acquired", channel, epoch, corr);
                    }
                    tokio::select! {
                        _ = conn_cancel.cancelled() => Ok(()),
                        result = execute_route_request(req, &writer, ver, channel, epoch, corr, session, bind, &handler) => result,
                    }
                }
                PreparedRouteRequest::Subscribe { params, slot } => {
                    let (from, limits) = match subscribe_from(&params) {
                        Ok(from) => from,
                        Err((field, message)) => {
                            let refusal = send_invalid_params(
                                &writer, ver, channel, epoch, corr, field, &message,
                            );
                            return if tracing::enabled!(
                                target: "requests",
                                tracing::Level::DEBUG
                            ) {
                                log_served_request(
                                    OP_SUBSCRIBE.to_string(),
                                    session.session.clone(),
                                    refusal,
                                )
                                .await
                            } else {
                                refusal.await
                            };
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
                            session,
                            from,
                            limits,
                            handler,
                            cancel.clone(),
                        ) => result,
                    }
                }
            }
        };
        match log_context {
            Some((operation, session)) => log_served_request(operation, session, request).await,
            None => request.await,
        }
    }));
    Ok(())
}

async fn authorize_route_request<H: ServeHandler>(
    handler: &H,
    session: &BrocaBindIdentity,
    bind: &BindObservation,
    method: &str,
) -> Result<(), ServeError> {
    if matches!(method, OP_CANCEL | OP_RETRACT) {
        handler
            .authorize_session_owner(session, bind.principal.as_ref(), &bind.scope)
            .await
    } else {
        handler
            .authorize_session(session, bind.principal.as_ref())
            .await
    }
}

/// Handle an authorized request on an already bound route channel by checking the request method and
/// calling the matching handler for that method.
#[allow(clippy::too_many_arguments)]
async fn execute_route_request<H>(
    req: RouteRequest,
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    session: BrocaBindIdentity,
    bind: BindObservation,
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
    match req.method.as_str() {
        "session.upload_begin"
        | "session.upload_append"
        | "session.upload_commit"
        | "session.upload_status" => {
            match handler
                .upload(
                    &session,
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
        OP_SEND_STATUS => {
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
            let run_id = run_id_for_send(&session, Some(&params.send_id));
            match handler
                .send_status(&session, run_id, &params.send_id, bind.principal.as_ref())
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
            let mut params: SendParams = match serde_json::from_value(req.params.clone()) {
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
            if let Err((field, message)) = params.delivery_order() {
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
            if params
                .cache
                .as_ref()
                .is_some_and(|cache| cache.mode == Some(broca_protocol::request::CacheMode::None))
                && params.keep_warm == Some(broca_protocol::request::KeepWarm::Auto)
            {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("keep_warm".into()),
                    "cache.mode \"none\" cannot be combined with keep_warm \"auto\"",
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
            // Deliveries that can join an episode take omitted runner parameters from its
            // frozen config, so they render the session's own request prefix. Admission resolves
            // them against its history snapshot; this preflight only classifies WAL presence.
            // Deliveries that can only start a new run take defaults and must name a model.
            let inherit_frozen: std::collections::BTreeSet<
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
            let session_written = if inherit_frozen.is_empty() {
                false
            } else {
                // The presence probe can stat the live WAL and read archive containers, so it
                // runs on the blocking pool rather than on this request's runtime worker.
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
            // Captured before the placeholder below replaces the absent model.
            let model_inherited = params.model.is_none();
            if params.model.is_none() {
                let may_inherit = inherit_frozen
                    .contains(&broca_protocol::request::InheritedParam::Model)
                    && session_written;
                if !may_inherit {
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        Some("model".into()),
                        "model is required unless a joining delivery can inherit it from an existing episode",
                    )
                    .await;
                }
                // An empty placeholder, replaced with the session's frozen model before the send
                // is recorded as a queued send or admitted (`InheritedParam::Model`), so it is
                // never rendered and never written to the WAL.
                params.model = Some(crate::wire::ModelParams {
                    provider: String::new(),
                    model: String::new(),
                    variant: None,
                });
            }
            // A session never written has no frozen values, so its first send keeps the
            // defaults for everything it omits.
            let inherit_frozen = if !session_written {
                Default::default()
            } else {
                inherit_frozen
            };
            if let Some(detail) = execute_class_root_refusal(&session).await {
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
            // refusal here is answered before anything is written to the session's WAL. The
            // session's actor checks the plan again against the session's history as it admits
            // the send, in case an episode was written in between.
            if !handler.hook_plane_available() && handler.frozen_plan_requires_hooks(&session).await
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
                && handler.frozen_plan_requires_approval(&session).await
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
                        &session,
                        &req.params,
                        plan,
                        scope,
                        // An inherited model is known only to the session's actor, which checks
                        // the plan's models against it; the placeholder must never be compared.
                        if model_inherited {
                            None
                        } else {
                            params.model.as_ref()
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
            let run_id = run_id_for_send(&session, params.send_id.as_deref());
            let own_run_id = run_id.clone();
            let bound_session = session.clone();
            let mut send_req = match params.into_send_request(session, run_id) {
                Ok(request) => request,
                Err(broca_core::RunError::Config(message)) => {
                    return send_error(writer, ver, channel, epoch, corr, "config", &message).await;
                }
                Err(error) => unreachable!("send parameter validation returned {error}"),
            };
            send_req.request.project_id = bind.project_id;
            send_req.request.inherit_frozen = inherit_frozen;
            send_req.stop_sequences_named =
                req.params.pointer("/generation/stop_sequences").is_some();
            send_req.request.principal = bind.principal;
            send_req.request.scope = bind.scope;
            send_req.plan = plan;
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
                    send_serve_error(writer, ver, channel, epoch, corr, &error).await
                }
            }
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
            if let Some(detail) = execute_class_root_refusal(&session).await {
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
            let run_id = run_id_for_import(&session, &params.import_id);
            let import_req = SessionImportRequest {
                project_id: bind.project_id,
                principal: bind.principal,
                session,
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
            match handler.retract(&session, sid).await {
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
                None | Some("now") => broca_wal::record::RunCancelAt::Now,
                Some("step_boundary") => broca_wal::record::RunCancelAt::StepBoundary,
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
                .cancel_at(&session, broca_protocol::RunId(params.run_id), at)
                .await
            {
                Ok(resp) => send_json_response(writer, ver, channel, epoch, corr, &resp).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
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
                None => match handler.status(&session).await {
                    Ok(resp) => send_json_response(writer, ver, channel, epoch, corr, &resp).await,
                    Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
                },
                Some(run_id) => {
                    match handler
                        .run_status_reply(&session, broca_protocol::RunId(run_id))
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
            match handler.run_event_get(&session, &params.event_key).await {
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
            match broca_module::history::read(handler.as_ref(), &session, &request).await {
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
        OP_READ => {
            if !handler.compaction_supported() {
                if let Err((field, message)) = decode_params::<broca_wire::ReadParams>(&req.params)
                {
                    return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                        .await;
                }
            }
            let params: broca_wire::WriterReadParams = match decode_params(&req.params) {
                Ok(p) => p,
                Err((field, message)) => {
                    return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                        .await
                }
            };
            if params.view == Some(broca_wire::ReadView::Model) && params.read.after_mid.is_some() {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("view".into()),
                    "model view cannot use after_mid",
                )
                .await;
            }
            let model_view = params.view == Some(broca_wire::ReadView::Model);
            // Descriptors are offered on raw transcript reads only, refused before any read
            // work.
            let media = params.read.media.unwrap_or_default();
            if model_view && media == cortexkit_role_llm_runner::read::MediaMode::Descriptors {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some("media".into()),
                    "descriptors are not offered on a model-view read",
                )
                .await;
            }
            let params = params.read;
            if params.after_mid.is_some() {
                // A read after a message is defined only within the lineage it came from, and
                // `after_mid` is a cursor in its own right, so it cannot be combined with
                // `from_ordinal`.
                let refusal = if params.lineage_id.is_none() {
                    Some((
                        "lineage_id",
                        "after_mid needs the lineage_id it was read under",
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
                session,
                from_ordinal: params.from_ordinal,
                limit: params.limit,
                include_tools: params.include_tools,
                after_mid: params.after_mid,
                lineage_id: params.lineage_id,
                max_bytes: params.max_bytes,
                media,
            };
            if model_view {
                return match handler.read_model(read_req).await {
                    Ok(page) => send_json_response(writer, ver, channel, epoch, corr, &page).await,
                    Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
                };
            }
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
        OP_RESOLVE => {
            let params: broca_wire::ResolveParams = match serde_json::from_value(req.params.clone())
            {
                Ok(params) => params,
                Err(error) => {
                    let field =
                        broca_wire::failing_field::<broca_wire::ResolveParams>(&req.params, &error);
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
            if let Err(field) = params.number() {
                return send_invalid_params(
                    writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    Some(field.into()),
                    "tag must have the form §N§ for a positive integer N",
                )
                .await;
            }
            match handler.resolve(&session, &params).await {
                Ok(Ok(reply)) => {
                    send_json_response(writer, ver, channel, epoch, corr, &reply).await
                }
                Ok(Err((code, message))) => {
                    if code == broca_wire::INVALID_PARAMS {
                        return send_invalid_params(
                            writer,
                            ver,
                            channel,
                            epoch,
                            corr,
                            Some(broca_wire::RANGE_FIELD.into()),
                            &message,
                        )
                        .await;
                    }
                    send_error(writer, ver, channel, epoch, corr, code, &message).await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_HEAD => {
            if let Err((field, message)) = decode_params::<broca_wire::HeadParams>(&req.params) {
                return send_invalid_params(writer, ver, channel, epoch, corr, field, &message)
                    .await;
            }
            match handler
                .head_for(&session, bind.principal.as_ref(), &bind.scope)
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
                .run_result(&session, broca_protocol::RunId(params.run_id))
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
            match handler.run_tool_calls(&session, request).await {
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
                .baseline(&session, bind.principal.as_ref(), &bind.scope)
                .await
            {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
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
            match handler.warm(&session, params.lineage_id).await {
                Ok(reply) => send_json_response(writer, ver, channel, epoch, corr, &reply).await,
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
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
            match handler.output_refs(&session, &request).await {
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
            match handler.output_ref_read(&session, &request).await {
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
) -> Result<(), ServeIoError>
where
    H: ServeHandler + 'static,
{
    // The Delivery sink the coordinator's subscriber task drains to. Bounded so a slow
    // egress backpressures the FORWARD task here (not the coordinator's fanout, which has
    // its own per-subscriber backpressure + detach-on-lag).
    let (sink, mut deliveries) = mpsc::channel::<Delivery>(EGRESS_BUFFER);
    let subscribe = handler.subscribe(&session, from, sink);
    let subscribe_result = if tracing::enabled!(target: "requests", tracing::Level::DEBUG) {
        log_served_request(OP_SUBSCRIBE.to_string(), session.session.clone(), subscribe).await
    } else {
        subscribe.await
    };
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
    tokio::spawn(async move {
        let mut next_seq = cortexkit_role_llm_runner::live::FIRST_SEQ;
        loop {
            let delivery = tokio::select! {
                _ = cancel.cancelled() => break,
                d = deliveries.recv() => match d {
                    Some(d) => d,
                    None => break, // the coordinator's subscriber task ended (sink closed)
                },
            };
            if let Delivery::Expired { expired_at_ms } = &delivery {
                let error = ServeError::Expired {
                    expired_at_ms: *expired_at_ms,
                };
                let body = ErrorBody::new(broca_module::retention::EXPIRED, error.to_string())
                    .with_detail(
                        serde_json::to_value(broca_module::retention::ExpiredDetail::new(
                            *expired_at_ms,
                        ))
                        .expect("expiry details serialize"),
                    );
                let _ =
                    send_subscribe_error_body(&writer, (ver, channel, epoch, corr), next_seq, body)
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
            // The same transport bound unary replies honour. An oversized frame on the wire
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
    });
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
        if let cortexkit_role_llm_runner::live::LiveEvent::ToolStarted(event) = &frame.2 {
            let arguments: serde_json::Value =
                serde_json::from_str(&event.arguments).expect("projected arguments are JSON");
            let limited = cortexkit_role_llm_runner::live::ToolStarted::new(
                &event.mid,
                &event.tool_call_id,
                &event.name,
                &arguments,
                Some(limits),
                event.started_at,
            );
            return Delivery::Control(Arc::new((
                frame.0,
                frame.1.clone(),
                cortexkit_role_llm_runner::live::LiveEvent::ToolStarted(limited),
            )));
        }
    }
    delivery
}

fn encode_delivery(delivery: &Delivery, seq: u64) -> EncodedDelivery {
    let (priority, encoded) = match delivery {
        Delivery::Control(event) => {
            let (cursor, unit, live_event) = event.as_ref();
            (
                Priority::Interactive,
                encode_live_subscribe_control(
                    cursor.wal_seq,
                    cursor.sub_index,
                    unit,
                    seq,
                    live_event,
                ),
            )
        }
        Delivery::Display(event) => (
            Priority::Background,
            encode_live_subscribe_display(event.as_ref(), seq),
        ),
        Delivery::Expired { .. } | Delivery::Error(_) | Delivery::Detached => {
            return EncodedDelivery::Detached
        }
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

async fn send_hello(
    writer: &EgressSender<Frame>,
    module_id: &str,
    archive: crate::wal_archive::WalArchiveConfig,
    consumer_routes: &crate::consumer_routes::ConsumerRoutes,
) -> Result<(), ServeIoError> {
    let body = serde_json::to_vec(&ModuleHelloBody {
        manifest: crate::manifest::manifest_configured(module_id, archive, consumer_routes),
        protocol_ver: PROTOCOL_VERSION,
        control_ops: Some(vec![MODULE_CONTROL_OP_HEALTH_CHECK.to_string()]),
        launch_nonce: hello_launch_nonce()?,
    })
    .map_err(|e| ServeIoError(e.to_string()))?;
    let frame = Frame::build(FrameType::Hello, control_flags(), 0, 0, HELLO_CORR, body)
        .map_err(|e| ServeIoError(e.to_string()))?;
    send(writer, frame).await
}

/// The launch nonce HELLO presents, from the process's one nonce accessor.
///
/// `main` has already made the first read, so this returns the cached value and never touches
/// the descriptor again. A named descriptor that could not be read fails HELLO with the
/// accessor's own message; it never falls back to the environment copy, because that would hide
/// a broken handoff and let a process that inherited only the variable register as broca.
fn hello_launch_nonce() -> Result<Option<String>, ServeIoError> {
    match subc_os::launch_nonce() {
        Ok(nonce) => Ok(nonce.map(|nonce| nonce.value().to_string())),
        Err(error) => Err(ServeIoError(format!("launch nonce: {error}"))),
    }
}

async fn expect_hello_ack<R>(reader: &mut R) -> Result<ModuleHelloAckBody, ServeIoError>
where
    R: AsyncRead + Unpin,
{
    let frame = read_frame(reader)
        .await
        .map_err(|e| ServeIoError(e.to_string()))?
        .ok_or_else(|| ServeIoError("connection closed before HELLO_ACK".into()))?;
    match frame.header.ty {
        FrameType::HelloAck => {
            serde_json::from_slice(&frame.body).map_err(|e| ServeIoError(e.to_string()))
        }
        FrameType::Error => {
            let body: ErrorBody =
                serde_json::from_slice(&frame.body).map_err(|e| ServeIoError(e.to_string()))?;
            Err(ServeIoError(format!(
                "subc rejected HELLO: {} — {}",
                body.code, body.message
            )))
        }
        ty => Err(ServeIoError(format!(
            "unexpected frame {ty:?} awaiting HELLO_ACK"
        ))),
    }
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

fn refused_send_code(error: &ServeError) -> String {
    match error {
        ServeError::Upload(error) => error.code.into(),
        ServeError::ContextCeiling(_) => cortexkit_role_llm_runner::errors::CONTEXT_CEILING.into(),
        ServeError::UnknownSend => cortexkit_role_llm_runner::errors::UNKNOWN_SEND.into(),
        ServeError::SessionMoving { .. } => "session_moving".into(),
        ServeError::SessionMoved { .. } => "session_moved".into(),
        ServeError::Expired { .. } => broca_module::retention::EXPIRED.into(),
        ServeError::RetentionInvalid(_) | ServeError::InvalidParams { .. } => {
            broca_wire::INVALID_PARAMS.into()
        }
        ServeError::VariantUnsupported(_) => "variant_unsupported".into(),
        ServeError::SessionHeld(_) => "session_held".into(),
        ServeError::Open(_, _) => "open_failed".into(),
        ServeError::HistoryUnreachable { .. } => broca_wire::HISTORY_UNREACHABLE.into(),
        ServeError::ReplyTooLarge(_) => broca_wire::REPLY_TOO_LARGE.into(),
        ServeError::RetainedSessionTooLarge { .. } => "retained_session_too_large".into(),
        ServeError::SessionTooLarge { .. } => "session_too_large".into(),
        ServeError::WalNewerWriter { .. } => broca_wire::WAL_NEWER_WRITER.into(),
        ServeError::SendRetracted { .. } => "send_retracted".into(),
        ServeError::SendEnded { .. } => "send_ended".into(),
        ServeError::DeliveryRefused { code, .. } => (*code).into(),
        ServeError::TemplatePartGone { .. } => {
            cortexkit_role_llm_runner::errors::TEMPLATE_PART_GONE.into()
        }
        ServeError::SendIdReuse { .. } => "send_id_reuse".into(),
        ServeError::Transient(_) | ServeError::TagsAwaitSetup => "transient".into(),
        ServeError::Import(_) => "import_rejected".into(),
        ServeError::ImportIdReuse => "import_id_reuse".into(),
        ServeError::ImportBusy => "import_busy".into(),
        ServeError::LineageChanged { .. } => broca_wire::LINEAGE_CHANGED.into(),
        ServeError::UnknownMid(_) => broca_wire::UNKNOWN_MID.into(),
        ServeError::UnknownRun(_) => broca_wire::UNKNOWN_RUN.into(),
        ServeError::ScopeRefused(_) | ServeError::ScopeFreezeBusy { .. } => {
            scope_error_bytes(error)
                .ok()
                .and_then(|body| serde_json::from_slice::<serde_json::Value>(&body).ok())
                .and_then(|body| body.get("code")?.as_str().map(str::to_owned))
                .unwrap_or_else(|| "scope_refused".into())
        }
        ServeError::PlanRefused(refusal) => refusal.code().into(),
        ServeError::AccountSelectionRefused(refusal) => refusal.routing_code.clone(),
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
        ServeError::Open(_, Some(class)) => shared_class_name(*class),
        ServeError::AccountSelectionRefused(refusal) => shared_class_name(refusal.class),
        ServeError::Transient(_)
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

async fn send_serve_error(
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    err: &ServeError,
) -> Result<(), ServeIoError> {
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
        ServeError::Upload(error) => {
            let mut detail = serde_json::json!({});
            if let Some(field) = error.field {
                detail["field"] = field.into();
            }
            if let Some(received) = error.received {
                detail["received"] = received.into();
            }
            if matches!(
                error.code,
                "upload_staging_full" | "upload_session_full" | "upload_store_failed"
            ) {
                detail["class"] = "transient".into();
            }
            let body = ErrorBody::new(error.code, error.to_string()).with_detail(detail);
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::ReplyTooLarge(_) => unreachable!("reply_too_large returned above"),
        ServeError::SessionMoving { cut_id } => {
            let body = ErrorBody::new("session_moving", err.to_string())
                .with_detail(serde_json::json!({"cutId":cut_id}));
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::SessionMoved { holder } => {
            let body = ErrorBody::new("session_moved", err.to_string())
                .with_detail(serde_json::json!({"holder":holder}));
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::SessionTooLarge { max_bytes } => {
            let body = ErrorBody::new("session_too_large", err.to_string())
                .with_detail(serde_json::json!({"class": "permanent", "max_bytes": max_bytes}));
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::RetainedSessionTooLarge { max_bytes } => {
            let body = ErrorBody::new("retained_session_too_large", err.to_string())
                .with_detail(serde_json::json!({"class": "permanent", "max_bytes": max_bytes}));
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::HistoryUnreachable { session_ref, .. } => {
            let body = ErrorBody::new(broca_wire::HISTORY_UNREACHABLE, err.to_string())
                .with_detail(serde_json::json!({
                    "class": "permanent",
                    "session_ref": session_ref,
                }));
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
        ServeError::SessionHeld(_) => "session_held",
        ServeError::Open(_, _) => "open_failed",
        ServeError::WalNewerWriter { .. } => broca_wire::WAL_NEWER_WRITER,
        ServeError::VariantUnsupported(_) => "variant_unsupported",
        ServeError::SendRetracted { .. } => "send_retracted",
        ServeError::SendEnded {
            submission_id,
            outcome,
        } => {
            let reply = broca_wire::ordered_send_ended_reply(submission_id, outcome);
            return send_json_response(writer, ver, channel, epoch, corr, &reply).await;
        }
        ServeError::DeliveryRefused { code, .. } => {
            let body = ErrorBody::new(*code, err.to_string())
                .with_detail(serde_json::json!({"field": "delivery"}));
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::TemplatePartGone { slot } => {
            let body = ErrorBody::new(
                cortexkit_role_llm_runner::errors::TEMPLATE_PART_GONE,
                err.to_string(),
            )
            .with_detail(serde_json::json!({ "slot": slot }));
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::SendIdReuse { .. } => "send_id_reuse",
        ServeError::Transient(_) | ServeError::TagsAwaitSetup => "transient",
        ServeError::ContextCeiling(detail) => {
            let body = ErrorBody::new(
                cortexkit_role_llm_runner::errors::CONTEXT_CEILING,
                err.to_string(),
            )
            .with_detail(serde_json::to_value(detail).map_err(|e| ServeIoError(e.to_string()))?);
            return send_error_body(writer, ver, channel, epoch, corr, body).await;
        }
        ServeError::Import(_) => "import_rejected",
        ServeError::ImportIdReuse => "import_id_reuse",
        ServeError::ImportBusy => "import_busy",
        ServeError::InvalidParams { .. } => broca_wire::INVALID_PARAMS,
        ServeError::LineageChanged { .. } => broca_wire::LINEAGE_CHANGED,
        ServeError::UnknownMid(_) => broca_wire::UNKNOWN_MID,
        ServeError::UnknownSend => cortexkit_role_llm_runner::errors::UNKNOWN_SEND,
        ServeError::UnknownRun(_) => broca_wire::UNKNOWN_RUN,
        ServeError::ScopeRefused(_) | ServeError::ScopeFreezeBusy { .. } => {
            // Callers branch on these bodies, so they are sent exactly as the broca-wire types
            // serialize, the bytes their golden files pin.
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
        ServeError::AccountSelectionRefused(refusal) => {
            // Sent exactly as the broca-wire type serializes: the bytes its golden files pin.
            let body = serde_json::to_vec(&broca_wire::AccountSelectionRefused::new(
                refusal.routing_code.clone(),
                refusal.class,
                refusal.retry_after_s,
                refusal.decision_id.clone(),
            ))
            .map_err(|e| ServeIoError(e.to_string()))?;
            return send_error_bytes(writer, ver, channel, epoch, corr, body).await;
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
    if let ServeError::Open(_, Some(class)) = err {
        body = body.with_detail(serde_json::json!({"class": class.shared_class()}));
    }
    let field = match err {
        ServeError::InvalidParams { field, .. } => Some(field.clone()),
        ServeError::SendIdReuse { differing } => send_field_of(differing).map(String::from),
        _ => None,
    };
    if let Some(field) = field {
        body = body.with_detail(
            serde_json::to_value(broca_wire::FieldDetail { field })
                .map_err(|e| ServeIoError(e.to_string()))?,
        );
    }
    if let ServeError::VariantUnsupported(refusal) = err {
        body = body.with_detail(serde_json::json!({
            "model": refusal.model,
            "variant": refusal.variant,
            "defined": refusal.defined,
        }));
    }
    if let ServeError::WalNewerWriter { missing, .. } = err {
        // Serialized from `broca_wire::WalNewerWriterDetail`, whose bytes the golden file
        // `broca-wire/tests/golden/wal_newer_writer.json` pins, so a change to the detail's
        // wire shape fails that test instead of reaching callers unannounced.
        let detail = broca_wire::WalNewerWriterDetail::NewerWriter {
            missing: missing.clone(),
        };
        body = body
            .with_detail(serde_json::to_value(&detail).map_err(|e| ServeIoError(e.to_string()))?);
    }
    send_error_body(writer, ver, channel, epoch, corr, body).await
}

enum SendPlanRefusal {
    Plan(broca_fetch_plan::Refusal),
    ApproveRequiresScope,
    WorkerPresetRequiresScope,
}

impl From<broca_fetch_plan::Refusal> for SendPlanRefusal {
    fn from(refusal: broca_fetch_plan::Refusal) -> Self {
        Self::Plan(refusal)
    }
}

impl SendPlanRefusal {
    fn wire_body(&self) -> ErrorBody {
        match self {
            Self::Plan(refusal) => {
                ErrorBody::new(refusal.code(), refusal.message()).with_detail(refusal.detail())
            }
            Self::ApproveRequiresScope => approve_scope_error(),
            Self::WorkerPresetRequiresScope => worker_scope_error(),
        }
    }
}

fn worker_scope_error() -> ErrorBody {
    ErrorBody::new(
        "scope_required",
        "a plan with a worker tool preset requires a daemon-attested session scope",
    )
    .with_detail(serde_json::json!({
        "field": "plan.tool_items",
        "preset": broca_fetch_plan::CONFINED_TOOL_PRESET,
    }))
}

fn approve_scope_error() -> ErrorBody {
    ErrorBody::new(
        "approve_requires_scope",
        "an approve subscription requires a daemon-attested session scope",
    )
    .with_detail(serde_json::json!({"field": "plan.step_transform_items", "phase": "approve"}))
}

/// Check the send's plan before fetching its providers or opening a session actor.
/// Later plans are only compared with the plan digest frozen by the session's first run.
async fn admit_send_plan<H: ServeHandler + ?Sized>(
    handler: &H,
    session: &broca_module::LineageKey,
    params: &serde_json::Value,
    plan: &serde_json::Value,
    scope: Option<&broca_protocol::SessionScope>,
    model: Option<&crate::wire::ModelParams>,
) -> Result<broca_module::actor::SendPlan, SendPlanRefusal> {
    // A plan item's `output_refs` member is refused unless the admitter can freeze sessions
    // that opt into output references (llm-runner §9.13: a runner without the group refuses it).
    let output_refs = if handler
        .plan_admitter()
        .is_some_and(|admitter| admitter.admits_output_refs())
    {
        broca_fetch_plan::OutputRefsSupport::Supported
    } else {
        broca_fetch_plan::OutputRefsSupport::Unsupported
    };
    if let Some(params) = params.as_object() {
        broca_fetch_plan::check_send_members_with(params, output_refs)?;
    }
    let support = if handler.compaction_supported() {
        broca_fetch_plan::CompactionSupport::Supported
    } else {
        broca_fetch_plan::CompactionSupport::Unsupported
    };
    let parsed = broca_fetch_plan::parse_plan_with(plan, support, output_refs)?;
    // Filing an approval question and executing its tool call must use the scope
    // identity supplied by the daemon on the inbound route, never a caller's payload.
    // Check before fetching providers or opening an actor, so refusal writes no WAL.
    if scope.is_none()
        && parsed.step_items.iter().any(|item| {
            item.subscriptions.iter().any(|subscription| {
                subscription.phase
                    == Some(cortexkit_role_step_transform_provider::subscription::Phase::Approve)
            })
        })
    {
        return Err(SendPlanRefusal::ApproveRequiresScope);
    }
    // A worker's tools are confined by the scope on its route. Without one, a provider would
    // run them under head defaults, so refuse before any fetch or WAL write instead of trusting
    // the caller never to send that combination.
    if scope.is_none()
        && parsed
            .tool_items
            .iter()
            .any(|item| item.preset.as_deref() == Some(broca_fetch_plan::CONFINED_TOOL_PRESET))
    {
        return Err(SendPlanRefusal::WorkerPresetRequiresScope);
    }
    // A reader build must not admit a plan it cannot execute, even when its WAL decoder
    // understands hook records. Refuse before fetching providers or allocating a lineage.
    if !parsed.step_items.is_empty() && !handler.hook_plane_available() {
        return Err(broca_fetch_plan::Refusal::InvalidParams {
            detail: serde_json::json!({"field": "plan.step_transform_items"}),
        }
        .into());
    }
    // Even an unchanged frozen plan can carry persona text for a different model on a later send.
    // A send that left its model to the session is checked by the session's actor instead, once
    // the inherited model is resolved; the plan travels with the send for that.
    let model_check = match model {
        Some(model) => {
            broca_fetch_plan::admit::check_plan_model(
                &parsed,
                &model.provider,
                &model.model,
                model.variant.as_deref(),
            )?;
            None
        }
        None => Some(std::sync::Arc::new(parsed.clone())),
    };
    match handler.plan_state(session).await {
        broca_module::serve::PlanState::Admitted { frozen } => {
            broca_fetch_plan::check_later_plan(frozen.as_deref(), &parsed.identity)?;
            Ok(broca_module::actor::SendPlan {
                identity: parsed.identity,
                manifest: None,
                model_check,
            })
        }
        broca_module::serve::PlanState::NotAdmitted | broca_module::serve::PlanState::Unknown => {
            let Some(admitter) = handler.plan_admitter() else {
                return Err(broca_fetch_plan::Refusal::InvalidParams {
                    detail: serde_json::json!({"field": "plan"}),
                }
                .into());
            };
            let admitted = admitter.admit(&parsed, session, scope).await?;
            Ok(broca_module::actor::SendPlan {
                identity: parsed.identity,
                manifest: Some(std::sync::Arc::new(admitted.manifest)),
                model_check,
            })
        }
    }
}

/// The `session.send` member a send-identity difference is about, as a caller spells it: the
/// identity compares the converted request, whose `input` is the send's `prompt`, and whose
/// `policy_allowed` is derived from `tools`. `None` when the difference names no single member.
fn send_field_of(differing: &str) -> Option<&'static str> {
    Some(match differing {
        "input" => "prompt",
        "policy_allowed" | "tools" => "tools",
        "delivery" => "delivery",
        "conversation_key" => "conversation_key",
        "model" => "model",
        "variant" => "model.variant",
        "tool_choice" => "tool_choice",
        "generation" => "generation",
        "cache" => "cache",
        "budget" => "budget",
        "auth" => "auth",
        "context_limit" => "context_limit",
        "service_tier" => "service_tier",
        "stop_when" => "stop_when",
        "context_management" => "context_management",
        "client_message_id" => "client_message_id",
        _ => return None,
    })
}

/// The bind refusal for an unsupported role version or multiple supported roles on one route.
/// Each route has only one operation namespace, including one `role.describe`.
pub const ROLE_VERSIONS_UNSUPPORTED: &str = "role_versions_unsupported";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum RouteRole {
    #[default]
    LlmRunner,
    Classifier,
    ToolProvider,
    Permissions,
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
        self != Self::LlmRunner
    }
}

/// Tool-provider targets identify their role without a declaration. A declaration may pin
/// that role's version, but cannot switch the target to another operation namespace.
fn bind_target_role(
    target: &subc_protocol::RouteTarget,
    role_versions: Option<&std::collections::BTreeMap<String, String>>,
    serves_classifier: bool,
) -> Result<RouteRole, String> {
    if !matches!(target, subc_protocol::RouteTarget::ToolProvider { .. }) {
        return bind_role(role_versions, serves_classifier);
    }
    if let Some(roles) = role_versions {
        if let Some(other) = roles.keys().find(|role| role.as_str() != "tool-provider") {
            return Err(format!(
                "a route serves one role; the tool-provider target conflicts with declared role {other}"
            ));
        }
    }
    bind_role(role_versions, serves_classifier)?;
    Ok(RouteRole::ToolProvider)
}

/// Select a supported role, retaining llm-runner for legacy or unrelated declarations.
fn bind_role(
    role_versions: Option<&std::collections::BTreeMap<String, String>>,
    serves_classifier: bool,
) -> Result<RouteRole, String> {
    let Some(roles) = role_versions else {
        return Ok(RouteRole::LlmRunner);
    };
    let supported = [
        ("classifier", "v1", RouteRole::Classifier),
        ("llm-runner", "v1", RouteRole::LlmRunner),
        ("tool-provider", "v1", RouteRole::ToolProvider),
        ("permissions", "v1", RouteRole::Permissions),
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
    if selected == RouteRole::Classifier && !serves_classifier {
        return Err(format!("this build does not serve {role} {version}"));
    }
    Ok(selected)
}

#[cfg(test)]
mod role_selection_tests {
    use super::{bind_role, RouteRole};

    #[test]
    fn each_supported_role_selects_its_own_dispatch() {
        assert_eq!(bind_role(None, true), Ok(RouteRole::LlmRunner));
        for (name, expected) in [
            ("llm-runner", RouteRole::LlmRunner),
            ("classifier", RouteRole::Classifier),
            ("tool-provider", RouteRole::ToolProvider),
            ("permissions", RouteRole::Permissions),
        ] {
            let roles = [(name.into(), "v1".into())].into_iter().collect();
            assert_eq!(bind_role(Some(&roles), true), Ok(expected), "{name}");
        }
    }
}

/// Answer one request on a classifier route, on tasks of its own: never the frame loop, and
/// none of the connection's request slots, so a slow provider cannot hold up any session's
/// work. The answer runs on a separate task from the one that writes the reply, so a panic
/// while answering becomes this request's `internal_error` instead of a failed request task
/// (which would end the connection).
fn spawn_classifier_request(
    body: Vec<u8>,
    bind: BindObservation,
    service: Option<Arc<crate::system_one::SystemOne>>,
    writer: &EgressSender<Frame>,
    request_tasks: &mut JoinSet<Result<(), ServeIoError>>,
    (ver, channel, epoch, corr): (u8, u16, u32, u64),
) {
    let writer = writer.clone();
    request_tasks.spawn(async move {
        let answered = match service {
            Some(service) => {
                tokio::spawn(async move {
                    service
                        .answer(&body, bind.principal.as_ref(), &bind.scope)
                        .await
                })
                .await
            }
            None => Ok(Err(serde_json::to_vec(&subc_protocol::ErrorBody::new(
                "unknown_method",
                "this build serves no classifier/v1 role",
            ))
            .unwrap_or_default())),
        };
        match answered {
            Ok(Ok(result)) => send_json_response(&writer, ver, channel, epoch, corr, &result).await,
            Ok(Err(body)) => send_error_bytes(&writer, ver, channel, epoch, corr, body).await,
            Err(error) => {
                tracing::error!(target: "system_one", %error,
                    "answering a classifier request failed; it is answered internal_error");
                let refusal = crate::system_one::internal_refusal(
                    "serving this request failed inside broca; send it again",
                );
                send_error_bytes(
                    &writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    crate::system_one::refusal_body(&refusal),
                )
                .await
            }
        }
    });
}

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

/// The wire body of a scope refusal, serialized from its broca-wire type.
fn scope_error_bytes(err: &ServeError) -> Result<Vec<u8>, ServeIoError> {
    let encoded = match err {
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
            let mut body = broca_wire::ScopeOwnerMismatch::new(owner_mismatch_detail(mismatch));
            if let broca_core::OwnerMismatch::OwnerOperation { first, mismatch } = &**mismatch {
                let first = first.as_owner_string();
                let accepted = match &**mismatch {
                    broca_core::OwnerMismatch::Scoped { frozen, .. }
                        if frozen.owner.as_owner_string() != first =>
                    {
                        format!("'{first}' or '{}'", frozen.owner.as_owner_string())
                    }
                    _ => format!("'{first}'"),
                };
                body.message = format!("this session accepts cancellation and retraction only from {accepted}, on any route and at any scope epoch");
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
    writer
        .send(frame)
        .await
        .map_err(|_| ServeIoError("egress channel closed".into()))
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
        OP_RESOLVE => 18,
        OP_RUN_TOOL_CALLS => 19,
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
fn peer_went_away(kind: std::io::ErrorKind) -> bool {
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
mod tests {
    use super::*;

    #[test]
    fn tool_bind_target_does_not_widen_management_or_internal_service_admission() {
        use subc_protocol::RouteTarget;
        let module_id = "broca".to_owned();
        let management = RouteTarget::ManagementSurface {
            module_id: module_id.clone(),
        };
        let tools = RouteTarget::ToolProvider {
            module_id: module_id.clone(),
        };
        let agent_data = RouteTarget::InternalService {
            module_id: module_id.clone(),
            service_id: broca_wire::agent_data::SERVICE_ID.into(),
        };
        let unrelated = RouteTarget::InternalService {
            module_id,
            service_id: "unrelated-service".into(),
        };
        for role in [
            RouteRole::LlmRunner,
            RouteRole::Classifier,
            RouteRole::ToolProvider,
            RouteRole::Permissions,
        ] {
            for enabled in [false, true] {
                assert!(serves_bind_target(&management, role, enabled));
                assert!(serves_bind_target(&agent_data, role, enabled));
                assert!(!serves_bind_target(&unrelated, role, enabled));
                assert_eq!(
                    serves_bind_target(&tools, role, enabled),
                    role == RouteRole::ToolProvider && enabled
                );
            }
        }
    }
    use broca_module::serve::{
        CancelResponse, RetractResponse, RunStatusResponse, SendRequest, SendResponse,
        SessionReadResponse, StatusResponse,
    };

    #[derive(Clone)]
    struct RequestLogCapture(Arc<Mutex<Vec<CapturedRequestLog>>>);

    #[derive(Clone, Debug)]
    struct CapturedRequestLog {
        target: &'static str,
        level: tracing::Level,
        fields: std::collections::BTreeMap<String, String>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for RequestLogCapture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            #[derive(Default)]
            struct Fields(std::collections::BTreeMap<String, String>);

            impl tracing::field::Visit for Fields {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    self.0
                        .insert(field.name().to_string(), format!("{value:?}"));
                }

                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    self.0.insert(field.name().to_string(), value.to_string());
                }

                fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                    self.0.insert(field.name().to_string(), value.to_string());
                }

                fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                    self.0.insert(field.name().to_string(), value.to_string());
                }
            }

            let mut fields = Fields::default();
            event.record(&mut fields);
            self.0.lock().unwrap().push(CapturedRequestLog {
                target: event.metadata().target(),
                level: *event.metadata().level(),
                fields: fields.0,
            });
        }
    }

    fn request_logs_for_filter(filter: &str) -> Vec<CapturedRequestLog> {
        use tracing_subscriber::prelude::*;

        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(filter))
            .with(RequestLogCapture(events.clone()));
        tracing::subscriber::with_default(subscriber, || {
            emit_request_log("session.read", "bind-session-id", true, 17);
        });
        let captured = events.lock().unwrap().clone();
        captured
    }

    #[test]
    fn request_log_is_debug_only_and_carries_the_bound_session() {
        assert!(request_logs_for_filter("info").is_empty());

        let logs = request_logs_for_filter("info,requests=debug");
        assert_eq!(logs.len(), 1);
        let log = &logs[0];
        assert_eq!(log.target, "requests");
        assert_eq!(log.level, tracing::Level::DEBUG);
        assert_eq!(log.fields["operation"], "session.read");
        assert_eq!(log.fields["session"], "bind-session-id");
        assert_eq!(log.fields["actor_opened"], "true");
        assert_eq!(log.fields["elapsed_us"], "17");
        assert_eq!(log.fields["message"], "served management request");
    }

    #[test]
    fn refused_session_send_logs_identity_code_and_class_without_error_text() {
        use tracing_subscriber::prelude::*;

        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new("info,session.send=info"))
            .with(RequestLogCapture(events.clone()));
        let error = ServeError::DeliveryRefused {
            code: "idle_start_not_granted",
            message: "private prompt and argument content".into(),
        };

        tracing::subscriber::with_default(subscriber, || {
            log_refused_send("session-ref-7", Some("send-9"), "steer", &error);
        });

        let logs = events.lock().unwrap().clone();
        assert_eq!(logs.len(), 1, "{logs:?}");
        let log = &logs[0];
        assert_eq!(log.target, "session.send");
        assert_eq!(log.level, tracing::Level::INFO);
        assert_eq!(log.fields["session_ref"], "session-ref-7");
        assert_eq!(log.fields["send_id"], "send-9");
        assert_eq!(log.fields["delivery_mode"], "steer");
        assert_eq!(log.fields["refusal_code"], "idle_start_not_granted");
        assert_eq!(log.fields["class"], "permanent");
        assert_eq!(log.fields["message"], "session.send refused");
        assert!(!format!("{:?}", log.fields).contains("private prompt and argument content"));
    }

    #[test]
    fn projection_health_degrades_only_for_sessions_still_behind() {
        let recovered = projection_health_report(1, 0);
        assert_eq!(recovered.failures, 1);
        assert_eq!(recovered.behind_sessions, 0);
        assert_eq!(recovered.detail, None);
        assert_eq!(health_status(&[]), subc_protocol::session::HealthStatus::Ok);

        let lagging = projection_health_report(2, 1);
        let detail = lagging.detail.expect("a behind session degrades health");
        assert_eq!(
            health_status(std::slice::from_ref(&detail)),
            subc_protocol::session::HealthStatus::Degraded
        );
        assert_eq!(
            detail,
            "store projection behind on 1 session(s); their reads replay the WAL tail until a projection succeeds"
        );
        assert!(
            !detail.contains("  "),
            "health detail contains an accidental run of spaces: {detail:?}"
        );
    }

    #[tokio::test]
    async fn watchdog_reader_phase_gates_name_stalls() {
        use crate::stall::{tests::Witness, Gate, State};
        use std::time::Duration;
        // Authorization and permit waits run in request tasks, not reader phases.
        for (phase, expected_name) in [
            (Phase::EgressSend, "egress_send"),
            (Phase::Control, "channel_0_control"),
        ] {
            let mut state = State::new();
            state.gate = Some(Gate {
                phase,
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            });
            let witness = Witness::new(state);
            let (writer, _rx) = broca_diagnostics::channel(1, witness.state.egress.clone());
            let mut routes = Routes::new(true);
            let channel = 0;
            let body = serde_json::to_vec(&ModuleControlRequest::HealthCheck {}).unwrap();
            let frame = Frame::build(
                FrameType::Request,
                control_flags(),
                channel,
                if channel == 0 { 0 } else { 1 },
                1,
                body,
            )
            .unwrap();
            let state = witness.state.clone();
            let task = tokio::spawn(stall::READER.scope(state, async move {
                stall::frame_context(0, channel);
                let handler = Arc::new(GatedStatusHandler {
                    gate_auth: false,
                    entered: tokio::sync::Notify::new(),
                    release: tokio::sync::Notify::new(),
                });
                let registry = LineageRegistry::new(Arc::new(RefusingOpener));
                handle_frame(
                    frame,
                    &writer,
                    &mut routes,
                    &handler,
                    &registry,
                    &VaultGauge::not_in_use(),
                    None,
                    &CancellationToken::new(),
                    &Arc::new(Semaphore::new(64)),
                    &mut JoinSet::new(),
                    None,
                )
                .await
            }));
            tokio::time::timeout(
                Duration::from_secs(2),
                witness.state.gate.as_ref().unwrap().entered.notified(),
            )
            .await
            .unwrap_or_else(|_| panic!("reader did not reach {phase:?} gate"));
            let snapshot = witness.snapshot().await;
            task.abort();
            let _ = task.await;
            assert_eq!(snapshot["kind"], "reader_phase", "{phase:?}");
            assert_eq!(snapshot["reader"]["phase_code"], phase as u8);
            assert_eq!(snapshot["reader"]["phase"], expected_name);
            assert_eq!(snapshot["reader"]["channel"], channel);
        }
    }

    #[tokio::test]
    async fn watchdog_stalled_egress_write_and_flush_are_named() {
        use crate::stall::{tests::Witness, State};
        struct HeldFlush;
        impl AsyncWrite for HeldFlush {
            fn poll_write(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
                bytes: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                std::task::Poll::Ready(Ok(bytes.len()))
            }
            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Pending
            }
            fn poll_shutdown(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
        }
        for flush in [false, true] {
            let witness = Witness::new(State::new());
            let (tx, rx) = broca_diagnostics::channel(64, witness.state.egress.clone());
            let (socket, _held_peer) = tokio::io::duplex(32);
            let task = if flush {
                tokio::spawn(drain_writer(HeldFlush, rx))
            } else {
                tokio::spawn(drain_writer(socket, rx))
            };
            let frame = Frame::build(
                FrameType::Response,
                control_flags(),
                7,
                1,
                1,
                vec![b'x'; 16_384],
            )
            .unwrap();
            tx.send(frame).await.unwrap();
            let snapshot = witness.snapshot().await;
            task.abort();
            let _ = task.await;
            assert_eq!(snapshot["kind"], "egress_write");
            assert_eq!(
                snapshot["egress"]["operation"],
                if flush { "flush" } else { "write" }
            );
            assert_eq!(snapshot["egress"]["sent"], 1);
            assert_eq!(snapshot["egress"]["written"], if flush { 1 } else { 0 });
            assert_eq!(snapshot["egress"]["occupancy"], if flush { 0 } else { 1 });
            assert_eq!(
                witness.state.egress.operation.load(Ordering::Relaxed),
                0,
                "cancelled writer clears active operation"
            );
        }
    }

    /// A write half whose every write fails with one chosen I/O error kind.
    struct FailingWriter(std::io::ErrorKind);

    impl tokio::io::AsyncWrite for FailingWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            _: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Err(std::io::Error::from(self.0)))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::from(self.0)))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// `body_len` picks which call meets the failure. drain_writer wraps the socket in an 8 KiB
    /// BufWriter, so a small frame is only buffered and the failure surfaces at the FLUSH, while
    /// a frame larger than the buffer is written straight through and fails at the WRITE. Both
    /// sites have their own peer-gone arm, so each needs a case: the first version of this test
    /// used only a small frame, and deleting the write-site arm left it green.
    ///
    /// The frame is a route-channel response, a type that may carry a body: the frame writer
    /// refuses a body on a header-only frame such as GOODBYE before writing a byte, so a large
    /// GOODBYE would test that refusal instead of the write failure.
    async fn drain_one_frame_into(
        kind: std::io::ErrorKind,
        body_len: usize,
    ) -> Result<(), ServeIoError> {
        let (tx, rx) = broca_diagnostics::channel(1, Arc::default());
        let frame = Frame::build(
            FrameType::Response,
            Flags::new(false, Priority::Passive, false),
            1,
            0,
            1,
            vec![b'x'; body_len],
        )
        .unwrap();
        tx.send(frame).await.unwrap();
        drop(tx);
        drain_writer(FailingWriter(kind), rx).await
    }

    /// The Windows half of the daemon-cut fix. On Windows CI the reset reached the WRITE half
    /// first (os error 10054), which the TCP test in tests/it/wire.rs cannot exercise on macOS,
    /// where the read half meets it first. So the writer's rule is pinned directly: a write the
    /// daemon can no longer receive ends the writer Ok, and any other write failure is still an
    /// error. The control arm is what stops "return Ok on every error" from passing.
    #[tokio::test]
    async fn the_writer_ends_ok_when_the_daemon_is_gone_and_errs_otherwise() {
        for (site, body_len) in [("flush", 0), ("write", 64 * 1024)] {
            for kind in [
                std::io::ErrorKind::ConnectionReset,
                std::io::ErrorKind::ConnectionAborted,
                std::io::ErrorKind::BrokenPipe,
            ] {
                assert!(
                    drain_one_frame_into(kind, body_len).await.is_ok(),
                    "{kind:?} at the {site} means the daemon is gone; the writer must end Ok"
                );
            }
            assert!(
                drain_one_frame_into(std::io::ErrorKind::PermissionDenied, body_len)
                    .await
                    .is_err(),
                "a {site} failure that is not the daemon going away must stay an error"
            );
        }
    }

    #[test]
    fn result_envelope_preserves_legacy_json_shape() {
        use broca_protocol::content::{ContentBlock, Message, Role};
        use broca_wire::{CursorWire, SessionReadLineageState, SessionReadMessage};

        let response = SessionReadResponse {
            next_late_after: None,
            deferred_calls: None,
            late_approvals: None,
            open_batch: None,
            // Absent deliberately: this test asserts the envelope still serialises to the
            // LEGACY json shape, so the new field must be skipped rather than emitted.
            lineage_id: None,
            messages: vec![
                SessionReadMessage {
                    written_at: None,
                    delivered_as: None,
                    run: None,
                    ordinal: 0,
                    mid: "m0".into(),
                    message: Message::new(
                        Role::User,
                        vec![ContentBlock::Text {
                            text: "hello, 世界 🧪".into(),
                        }],
                    ),
                    tool_calls: Vec::new(),
                    tags: Vec::new(),
                    described: None,
                    digest: None,
                    part_digests: None,
                },
                SessionReadMessage {
                    written_at: None,
                    delivered_as: None,
                    run: None,
                    ordinal: 1,
                    mid: "m1".into(),
                    message: Message::new(
                        Role::Assistant,
                        vec![ContentBlock::Text {
                            text: "response".into(),
                        }],
                    ),
                    tool_calls: Vec::new(),
                    tags: Vec::new(),
                    described: None,
                    digest: None,
                    part_digests: None,
                },
            ],
            next_from_ordinal: Some(2),
            head: Some(CursorWire {
                wal_seq: 7,
                sub_index: 1,
            }),
            lineage_state: SessionReadLineageState {
                last_run_id: Some("run-1".into()),
                state: "idle".into(),
                reason: Some("completed".into()),
                error: None,
                usage: None,
            },
            // Absent: this models a read that did not set `include_tools`, so the legacy shape
            // must hold with the tool fields skipped.
            tools: None,
            tools_run_id: None,
        };

        let direct = serde_json::to_vec(&ResultEnvelope { result: &response }).unwrap();
        let legacy = serde_json::json!({ "result": &response });
        let direct: serde_json::Value = serde_json::from_slice(&direct).unwrap();
        assert_eq!(direct, legacy);
        let envelope = direct.as_object().unwrap();
        assert_eq!(envelope.len(), 1);
        assert!(envelope.contains_key("result"));
    }

    #[test]
    fn minted_run_ids_are_unique_and_carry_the_launch_nonce() {
        let a = mint_run_id("s1");
        let b = mint_run_id("s1");
        // Distinct sends never collide (the process-global counter).
        assert_ne!(
            a.0, b.0,
            "two sends in the same session mint distinct run_ids"
        );
        // The id carries the per-launch nonce, so a restarted process (a new nonce) can
        // never re-mint an id an earlier process used (the cross-restart collision that
        // let the run index clobber an old terminal / cancel target the wrong run).
        let nonce = launch_nonce();
        assert!(
            a.0.contains(nonce) && b.0.contains(nonce),
            "minted ids embed the launch nonce: {} / {}",
            a.0,
            b.0
        );
        assert!(a.0.starts_with("run-s1-"), "id is session-scoped: {}", a.0);
    }

    #[test]
    fn completed_session_dispatch_lane_is_evicted_when_its_route_closes() {
        let session = BrocaBindIdentity::new("/tmp/project-a", "runner", "s1");
        let mut routes = Routes::new(false);
        routes
            .by_channel
            .insert(7, (1, session.clone(), BindObservation::default()));

        let ticket = routes.reserve_dispatch(&session);
        assert!(routes.dispatch_lanes.contains_key(&session));
        drop(ticket);

        routes.route_closed(7, 1);
        assert!(!routes.dispatch_lanes.contains_key(&session));
        assert!(routes.dispatch_lanes.is_empty());
    }

    #[test]
    fn dispatch_lane_sweep_reclaims_a_completed_request_that_outlived_its_route() {
        let departed = BrocaBindIdentity::new("/tmp/project-a", "runner", "departed");
        let mut routes = Routes::new(false);
        routes
            .by_channel
            .insert(7, (1, departed.clone(), BindObservation::default()));
        let ticket = routes.reserve_dispatch(&departed);

        routes.route_closed(7, 1);
        assert!(routes.dispatch_lanes.contains_key(&departed));
        drop(ticket);

        for index in 0..DISPATCH_LANE_SWEEP_MIN {
            let session =
                BrocaBindIdentity::new("/tmp/project-a", "runner", format!("replacement-{index}"));
            drop(routes.reserve_dispatch(&session));
        }

        assert!(!routes.dispatch_lanes.contains_key(&departed));
        assert!(routes.dispatch_lanes.len() < DISPATCH_LANE_SWEEP_MIN);
    }

    #[tokio::test]
    async fn route_reopen_keeps_fifo_while_the_departing_lane_has_an_outstanding_ticket() {
        let session = BrocaBindIdentity::new("/tmp/project-a", "runner", "s1");
        let mut routes = Routes::new(false);
        routes
            .by_channel
            .insert(7, (1, session.clone(), BindObservation::default()));
        let first_turn = routes.reserve_dispatch(&session).wait().await;

        routes.route_closed(7, 1);
        assert!(routes.dispatch_lanes.contains_key(&session));
        routes
            .by_channel
            .insert(8, (2, session.clone(), BindObservation::default()));
        let second_ticket = routes.reserve_dispatch(&session);

        let mut second_wait = Box::pin(second_ticket.wait());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), second_wait.as_mut())
                .await
                .is_err(),
            "the reopened route bypassed the departing route's in-flight request"
        );

        drop(first_turn);
        let _second_turn = tokio::time::timeout(std::time::Duration::from_secs(1), second_wait)
            .await
            .expect("the reopened route advances after the first request releases its turn");
    }

    /// A control unit whose encoded frame exceeds the transport limit is never put on the wire:
    /// the stream ends with a typed error naming the unit's cursor, the frame size and the
    /// limit, and no clean StreamEnd that would send the client back to the same cursor. The
    /// served bytes are pinned by `tests/golden/subscribe_unit_undeliverable.too_large.json`.
    #[tokio::test]
    async fn an_oversized_subscribe_unit_ends_the_stream_with_a_typed_error() {
        let registry = Arc::new(LineageRegistry::new(Arc::new(RefusingOpener)));
        let mut ingress = BindIngress::new(registry);
        ingress.bind(1, "published", false).await;
        next_written(&mut ingress.written).await;
        let sink = ingress.subscribe(1, 10).await;
        // 1 MiB of control characters: JSON escapes each as six bytes.
        let unit = broca_protocol::caller::ControlUnit::ToolResult {
            result: broca_protocol::ToolResult {
                tool_call_id: broca_protocol::ToolCallId("call-1".into()),
                output: broca_protocol::ToolOutput::Text {
                    text: "\u{1}".repeat(1 << 20),
                },
                is_error: false,
            },
        };
        sink.send(Delivery::Control(Arc::new((
            broca_core::Cursor {
                wal_seq: 42,
                sub_index: 1,
            },
            unit,
            cortexkit_role_llm_runner::live::LiveEvent::DisplayReset,
        ))))
        .await
        .unwrap();
        let frame = next_written(&mut ingress.written).await;
        assert_eq!(
            (frame.header.ty, frame.header.epoch, frame.header.corr),
            (FrameType::Error, 1, 10),
            "an oversized unit must end the stream with a typed error"
        );
        assert_eq!(
            String::from_utf8(frame.body.to_vec()).unwrap(),
            include_str!("../tests/golden/subscribe_unit_undeliverable.too_large.json").trim_end()
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            ingress.written.try_recv().is_err(),
            "no StreamEnd or data frame follows the typed terminal"
        );
    }

    #[tokio::test]
    async fn transient_admission_error_uses_transient_wire_code() {
        let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
        send_serve_error(
            &tx,
            PROTOCOL_VERSION,
            7,
            3,
            11,
            &ServeError::Transient("credentials service unreachable".into()),
        )
        .await
        .unwrap();

        let frame = rx.recv().await.expect("error frame");
        assert_eq!(frame.header.ty, FrameType::Error);
        let error: ErrorBody = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(error.code, "transient");
        assert!(error.message.contains("credentials service unreachable"));
    }

    #[tokio::test]
    async fn variant_unsupported_error_body_pins_wire_bytes() {
        let refusal = broca_core::run::VariantRefusal {
            model: "anthropic/claude-opus-5".into(),
            variant: "max".into(),
            defined: vec!["none".into(), "low".into(), "medium".into(), "high".into()],
            reason: "model 'anthropic/claude-opus-5' does not define variant 'max'; its variants are [none, low, medium, high]".into(),
        };
        assert_eq!(
            broca_core::RunError::VariantUnsupported(refusal.clone()).admission_error_class(),
            broca_protocol::error::ErrorClass::Permanent
        );
        let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
        send_serve_error(
            &tx,
            PROTOCOL_VERSION,
            7,
            3,
            11,
            &ServeError::VariantUnsupported(refusal),
        )
        .await
        .unwrap();
        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.ty, FrameType::Error);
        assert_eq!(frame.body.as_slice(), br#"{"code":"variant_unsupported","message":"open failed: run resolution failed: model 'anthropic/claude-opus-5' does not define variant 'max'; its variants are [none, low, medium, high]","detail":{"defined":["none","low","medium","high"],"model":"anthropic/claude-opus-5","variant":"max"}}"#);
    }

    /// The served newer-writer refusal is byte for byte `broca-wire/tests/golden/
    /// wal_newer_writer.json`, and its error class is permanent (a caller must not retry it).
    /// The message is rendered from a real `WalError::NewerWriter`, so a change to that
    /// error's wording is caught here too.
    #[tokio::test]
    async fn wal_newer_writer_error_body_is_the_pinned_golden() {
        let missing = vec!["probe-unknown/v1".to_string()];
        assert_eq!(
            broca_core::RunError::NewerWriter {
                missing: missing.clone()
            }
            .admission_error_class(),
            broca_protocol::error::ErrorClass::Permanent
        );
        let wal_error = broca_wal::WalError::NewerWriter {
            offset: 69,
            ver: 3,
            missing: missing.clone(),
        };
        let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
        send_serve_error(
            &tx,
            PROTOCOL_VERSION,
            7,
            3,
            11,
            &ServeError::WalNewerWriter {
                missing,
                reason: format!("wal range-read failed: {wal_error}"),
            },
        )
        .await
        .unwrap();
        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.ty, FrameType::Error);
        let golden = std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../broca-wire/tests/golden/wal_newer_writer.json"),
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(frame.body).unwrap(),
            String::from_utf8(golden).unwrap()
        );
    }

    #[tokio::test]
    async fn retained_session_size_error_body_is_the_pinned_golden() {
        assert_eq!(
            broca_core::RunError::RetainedSessionTooLarge { max_bytes: 4096 }
                .admission_error_class(),
            broca_protocol::ErrorClass::Permanent
        );
        let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
        send_serve_error(
            &tx,
            PROTOCOL_VERSION,
            7,
            3,
            11,
            &ServeError::RetainedSessionTooLarge { max_bytes: 4096 },
        )
        .await
        .unwrap();
        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.ty, FrameType::Error);
        assert_eq!(
            std::str::from_utf8(&frame.body).unwrap(),
            include_str!("../tests/golden/retained_session_too_large.json").trim_end()
        );
    }

    #[tokio::test]
    async fn reply_too_large_detail_bytes_match_published_vectors() {
        let vector = cortexkit_role_llm_runner::vectors::FILES
            .iter()
            .find_map(|(name, contents)| (*name == "reply-too-large.json").then_some(*contents))
            .expect("the role crate publishes reply-too-large.json");
        let vectors: serde_json::Value = serde_json::from_str(vector).unwrap();
        #[derive(serde::Serialize)]
        struct ExpectedError<'a> {
            code: &'a str,
            message: &'a str,
            detail: &'a cortexkit_role_llm_runner::errors::ReplyTooLargeDetail,
        }

        for case in vectors["refusals"].as_array().unwrap() {
            let refusal = &case["refusal"];
            let detail: cortexkit_role_llm_runner::errors::ReplyTooLargeDetail =
                serde_json::from_value(refusal["detail"].clone()).unwrap();
            assert_eq!(
                serde_json::to_value(&detail).unwrap(),
                refusal["detail"],
                "{}",
                case["name"]
            );
            let expected = serde_json::to_vec(&ExpectedError {
                code: refusal["code"].as_str().unwrap(),
                message: refusal["message"].as_str().unwrap(),
                detail: &detail,
            })
            .unwrap();
            let (writer, mut rx) = broca_diagnostics::channel(1, Arc::default());
            send_serve_error(
                &writer,
                PROTOCOL_VERSION,
                7,
                3,
                11,
                &ServeError::ReplyTooLarge(detail),
            )
            .await
            .unwrap();
            let frame = rx.recv().await.unwrap();
            assert_eq!(frame.header.ty, FrameType::Error);
            assert_eq!(frame.body, expected, "{}", case["name"]);
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap(),
                *refusal,
                "{}",
                case["name"]
            );
            if case["name"] == "model-view atomic group exceeds the transport frame limit" {
                let route_frame = model_group_reply_too_large_vector_through_route(refusal).await;
                assert_eq!(route_frame.header.ty, FrameType::Error);
                assert_eq!(route_frame.body, expected, "{}", case["name"]);
            }
        }
    }

    #[tokio::test]
    async fn source_less_model_metadata_and_run_result_use_generic_frame_limit_error() {
        let oversized = "x".repeat(broca_wire::SUBC_MAX_FRAME_BYTES as usize);
        let model_page = broca_wire::ModelPage {
            messages: Vec::new(),
            lineage_id: Some(oversized.clone()),
            next_from_ordinal: None,
            head: None,
            compaction_id: None,
            version: None,
        };
        let run_result = broca_wire::RunResultResponse {
            auth_selection: None,
            directives: None,
            context_ceiling: None,
            usage: Default::default(),
            detail: None,
            run_id: "run".into(),
            state: "active".into(),
            reason: Some(oversized),
            error: None,
            final_message: None,
            structured_output: None,
        };
        let expected =
            br#"{"code":"response_too_large","message":"response exceeds the transport frame limit"}"#;
        let (writer, mut rx) = broca_diagnostics::channel(2, Arc::default());
        send_json_response(&writer, PROTOCOL_VERSION, 7, 3, 11, &model_page)
            .await
            .unwrap();
        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.ty, FrameType::Error);
        assert_eq!(frame.body, expected);
        send_json_response(&writer, PROTOCOL_VERSION, 7, 3, 12, &run_result)
            .await
            .unwrap();
        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.ty, FrameType::Error);
        assert_eq!(frame.body, expected);
    }

    async fn model_group_reply_too_large_vector_through_route(
        refusal: &serde_json::Value,
    ) -> Frame {
        let source_json = refusal["detail"]["subject"]["source"].clone();
        let compaction_id = source_json["compaction_id"].as_str().unwrap().to_owned();
        let version = source_json["version"].as_u64().unwrap();
        let source: cortexkit_role_llm_runner::read::EntrySource =
            serde_json::from_value(source_json).unwrap();
        let page = broca_wire::ModelPage {
            messages: vec![broca_wire::ModelEntry {
                source: source.clone(),
                message: broca_protocol::Message::new(broca_protocol::Role::Assistant, Vec::new()),
                tags: Vec::new(),
            }],
            lineage_id: None,
            next_from_ordinal: None,
            head: None,
            compaction_id: Some(compaction_id),
            version: Some(version),
        };
        let detail = broca_module::serve::ReplyTooLargeDetail::new(
            refusal["detail"]["bytes"].as_u64().unwrap(),
            refusal["detail"]["limit"].as_u64().unwrap(),
            broca_module::serve::ReplyTooLargeSubject::ModelGroup {
                source: page.messages[0].source.clone(),
            },
        );
        let (sink_tx, _) = mpsc::channel(1);
        let handler = Arc::new(MockSubscribeHandler {
            sink_tx,
            refuse_session: std::sync::atomic::AtomicBool::new(false),
            model_reply_too_large: Some(detail.clone()),
        });
        let (writer, mut rx) = broca_diagnostics::channel(1, Arc::default());
        execute_route_request(
            crate::wire::RouteRequest {
                method: OP_READ.into(),
                params: serde_json::json!({"view":"model"}),
            },
            &writer,
            PROTOCOL_VERSION,
            7,
            3,
            11,
            BrocaBindIdentity::new("/tmp/vector-project", "runner", "session"),
            BindObservation::default(),
            &handler,
        )
        .await
        .unwrap();
        rx.recv().await.unwrap()
    }

    #[tokio::test]
    async fn ended_ordered_send_retries_are_successful_terminal_replies() {
        let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
        for outcome in ["superseded", "expired"] {
            send_serve_error(
                &tx,
                PROTOCOL_VERSION,
                7,
                3,
                11,
                &ServeError::SendEnded {
                    submission_id: "sub-note".into(),
                    outcome,
                },
            )
            .await
            .unwrap();
            let frame = rx.recv().await.unwrap();
            assert_eq!(frame.header.ty, FrameType::Response, "{outcome}");
            let value: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
            assert_eq!(
                value,
                serde_json::json!({"result":{"state":outcome,"submission_id":"sub-note"}})
            );
        }
    }

    #[tokio::test]
    async fn plan_model_mismatch_bytes_are_pinned() {
        use sha2::{Digest, Sha256};
        let refusal = broca_fetch_plan::Refusal::PlanModelMismatch {
            provider: "prefrontal-core".into(),
            plan_model: serde_json::json!({"provider": "fake", "id": "other"}),
            send_model: serde_json::json!({"provider": "fake", "id": "fake"}),
        };
        let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
        send_serve_error(
            &tx,
            PROTOCOL_VERSION,
            7,
            3,
            11,
            &ServeError::PlanRefused(refusal),
        )
        .await
        .unwrap();
        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.ty, FrameType::Error);
        let golden = std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../broca-wire/tests/golden/plan_model_mismatch.json"),
        )
        .unwrap();
        assert_eq!(frame.body, golden);
        assert_eq!(
            format!("{:x}", Sha256::digest(&golden)),
            "9f77fab81eaf92677ced37b818a08d2feefa8dbe873b2f82fd0e365a223376bd",
        );
    }

    #[tokio::test]
    async fn pre_user_unavailable_wire_names_only_the_refusing_provider() {
        let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
        send_serve_error(
            &tx,
            PROTOCOL_VERSION,
            7,
            3,
            11,
            &ServeError::PreUserUnavailable {
                provider: "magic-context".into(),
            },
        )
        .await
        .unwrap();
        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.ty, FrameType::Error);
        let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(body["code"], "pre_user_unavailable");
        assert_eq!(
            body["detail"],
            serde_json::json!({"provider": "magic-context"})
        );
        assert!(body.get("retryable").is_none());
    }

    #[tokio::test]
    async fn retracted_send_error_uses_retracted_wire_code() {
        let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
        send_serve_error(
            &tx,
            PROTOCOL_VERSION,
            7,
            3,
            11,
            &ServeError::SendRetracted {
                submission_id: "sub-r2".into(),
            },
        )
        .await
        .unwrap();

        let frame = rx.recv().await.expect("error frame");
        assert_eq!(frame.header.ty, FrameType::Error);
        let error: ErrorBody = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(error.code, "send_retracted");
        assert!(error.message.contains("sub-r2"));
    }

    #[test]
    fn send_id_derived_run_ids_are_deterministic_and_session_scoped() {
        let session_a = BrocaBindIdentity::new("/tmp/project-a", "runner", "s1");
        let session_b = BrocaBindIdentity::new("/tmp/project-b", "runner", "s1");

        let a1 = run_id_for_send(&session_a, Some("logical-send"));
        let a2 = run_id_for_send(&session_a, Some("logical-send"));
        let b1 = run_id_for_send(&session_b, Some("logical-send"));

        assert_eq!(
            a1.0, a2.0,
            "the same session + send_id reuses the same run_id"
        );
        assert_ne!(
            a1.0, b1.0,
            "the same send_id on another session hashes differently"
        );
        assert!(
            a1.0.starts_with("run-sid-"),
            "derived ids keep the send-id prefix: {}",
            a1.0
        );
        let digest = a1.0.strip_prefix("run-sid-").unwrap();
        assert!(
            digest.len() > 16 && digest.chars().all(|ch| ch.is_ascii_alphanumeric()),
            "send ids use the wider FNV-1a-128 base62 digest: {digest}"
        );
    }

    #[test]
    fn import_ids_are_deterministic_session_scoped_and_separate_from_send_ids() {
        let session_a = BrocaBindIdentity::new("/tmp/project-a", "runner", "s1");
        let session_b = BrocaBindIdentity::new("/tmp/project-b", "runner", "s1");
        let a1 = run_id_for_import(&session_a, "logical-import");
        let a2 = run_id_for_import(&session_a, "logical-import");
        let b = run_id_for_import(&session_b, "logical-import");
        assert_eq!(a1, a2);
        assert_ne!(a1, b);
        assert_ne!(a1, run_id_for_send(&session_a, Some("logical-import")));
        assert!(a1.0.starts_with("run-iid-"));
    }

    /// A transcript served two messages per page, so `session.resolve` has to page.
    struct PagedTranscript {
        messages: Vec<broca_protocol::Message>,
        tag_location: Option<broca_core::TagLocation>,
    }

    #[async_trait::async_trait]
    impl ServeHandler for PagedTranscript {
        async fn tag_location(
            &self,
            _session: &BrocaBindIdentity,
            _tag: u64,
        ) -> Result<Option<broca_core::TagLocation>, ServeError> {
            Ok(self.tag_location)
        }
        async fn send(&self, _req: SendRequest) -> Result<SendResponse, ServeError> {
            unimplemented!()
        }
        async fn retract(
            &self,
            _session: &BrocaBindIdentity,
            _submission_id: broca_protocol::SubmissionId,
        ) -> Result<RetractResponse, ServeError> {
            unimplemented!()
        }
        async fn subscribe(
            &self,
            _session: &BrocaBindIdentity,
            _from: SubscribeFrom,
            _sink: mpsc::Sender<Delivery>,
        ) -> Result<(), ServeError> {
            unimplemented!()
        }
        async fn read(&self, req: SessionReadRequest) -> Result<SessionReadResponse, ServeError> {
            assert!(
                req.from_ordinal.is_some(),
                "resolve pages from the oldest message"
            );
            let from = req.from_ordinal.unwrap() as usize;
            let end = (from + 2).min(self.messages.len());
            let messages = (from..end)
                .map(|ordinal| broca_wire::SessionReadMessage {
                    written_at: None,
                    delivered_as: None,
                    ordinal: ordinal as u64,
                    mid: format!("m{ordinal}"),
                    tags: broca_wire::tag_rows(ordinal as u64, &self.messages[ordinal]),
                    message: self.messages[ordinal].clone(),
                    run: None,
                    tool_calls: Vec::new(),
                    described: None,
                    digest: None,
                    part_digests: None,
                })
                .collect();
            Ok(SessionReadResponse {
                next_late_after: None,
                deferred_calls: None,
                late_approvals: None,
                open_batch: None,
                lineage_id: Some("lineage".into()),
                messages,
                next_from_ordinal: (end < self.messages.len()).then_some(end as u64),
                head: None,
                lineage_state: broca_wire::SessionReadLineageState {
                    last_run_id: None,
                    state: "idle".into(),
                    reason: None,
                    error: None,
                    usage: None,
                },
                tools: None,
                tools_run_id: None,
            })
        }
        async fn cancel(
            &self,
            _session: &BrocaBindIdentity,
            _run_id: broca_protocol::RunId,
        ) -> Result<CancelResponse, ServeError> {
            unimplemented!()
        }
        async fn status(&self, _session: &BrocaBindIdentity) -> Result<StatusResponse, ServeError> {
            unimplemented!()
        }
        async fn run_status(
            &self,
            _session: &BrocaBindIdentity,
            _run_id: broca_protocol::RunId,
        ) -> Result<RunStatusResponse, ServeError> {
            unimplemented!()
        }
    }

    fn tagged_text(
        role: broca_protocol::Role,
        texts: &[&str],
        tags: &[u64],
    ) -> broca_protocol::Message {
        let mut message = broca_protocol::Message::new(
            role,
            texts
                .iter()
                .map(|text| broca_protocol::ContentBlock::Text {
                    text: (*text).into(),
                })
                .collect(),
        );
        message.set_tags(Some(Arc::new(broca_protocol::tags::PartTags::all(
            tags.iter().copied(),
        ))));
        message
    }

    #[tokio::test]
    async fn resolve_answers_a_parts_stored_text_whole_or_by_character_range() {
        use broca_protocol::Role;
        let handler = PagedTranscript {
            tag_location: None,
            messages: vec![
                tagged_text(Role::User, &["first"], &[1]),
                tagged_text(Role::Assistant, &["answer"], &[2]),
                broca_protocol::Message::new(Role::User, vec![]),
                tagged_text(Role::User, &["héllo, 世界", "tail"], &[5, 6]),
            ],
        };
        let session = BrocaBindIdentity::new("/tmp/project", "runner", "s1");
        let resolve = |tag, range| {
            let handler = &handler;
            let session = session.clone();
            async move {
                handler
                    .resolve(&session, &{
                        let mut params =
                            broca_wire::ResolveParams::new(broca_protocol::tags::tag_mark(tag));
                        params.range = range;
                        params
                    })
                    .await
            }
        };

        let whole = resolve(5, None).await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(&whole).unwrap(),
            serde_json::json!({"tag":"§5§","ordinal":3,"mid":"m3","part":"m3#0","content":"héllo, 世界"})
        );
        let second_part = resolve(6, None).await.unwrap().unwrap();
        assert_eq!(second_part.part, "m3#1");
        assert_eq!(second_part.content, "tail");

        // A range counts characters, not bytes.
        let range = broca_wire::CharRange::new(1, 9);
        let ranged = resolve(5, Some(range)).await.unwrap().unwrap();
        assert_eq!(ranged.content, "éllo, 世界");
        assert_eq!(ranged.range, Some(range));
        let empty = broca_wire::CharRange::new(9, 9);
        assert_eq!(resolve(5, Some(empty)).await.unwrap().unwrap().content, "");

        for bad in [
            broca_wire::CharRange::new(0, 10),
            broca_wire::CharRange::new(4, 3),
        ] {
            let refusal = resolve(5, Some(bad)).await.unwrap().unwrap_err();
            assert_eq!(refusal.0, broca_wire::INVALID_PARAMS, "{bad:?}");
        }
        let unknown = resolve(4, None).await.unwrap().unwrap_err();
        assert_eq!(unknown.0, broca_wire::UNKNOWN_TAG);
    }

    #[tokio::test]
    async fn resolve_uses_current_lineage_locations_and_refuses_left_tags() {
        let handler = PagedTranscript {
            messages: vec![tagged_text(
                broca_protocol::Role::User,
                &["carried", "new"],
                &[2, 5],
            )],
            // A lookup made before the read repaired an old lineage's projection.
            tag_location: Some(broca_core::TagLocation {
                found: Some((3, 0)),
                indexed_through: 4,
            }),
        };
        let session = BrocaBindIdentity::new("/tmp/project", "runner", "current-lineage");
        let carried = handler
            .resolve(&session, &broca_wire::ResolveParams::new("§2§"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(carried.content, "carried");
        assert_eq!(carried.ordinal, 0);
        assert_eq!(carried.mid, "m0");
        let absent = handler
            .resolve(&session, &broca_wire::ResolveParams::new("§3§"))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(absent.0, broca_wire::UNKNOWN_TAG);
    }

    #[test]
    fn resolve_params_are_strict() {
        assert!(serde_json::from_value::<broca_wire::ResolveParams>(
            serde_json::json!({"tag":"§1§","range":{"start":0,"end":1}})
        )
        .is_ok());
        for bad in [
            serde_json::json!({}),
            serde_json::json!({"tag":"§1§","origin":"owner"}),
            serde_json::json!({"tag":"§1§","range":{"start":0}}),
            serde_json::json!({"tag":1}),
            serde_json::json!({"tag":-1}),
        ] {
            assert!(
                serde_json::from_value::<broca_wire::ResolveParams>(bad.clone()).is_err(),
                "{bad}"
            );
        }
    }

    struct MockSubscribeHandler {
        sink_tx: mpsc::Sender<mpsc::Sender<Delivery>>,
        refuse_session: std::sync::atomic::AtomicBool,
        model_reply_too_large: Option<broca_module::serve::ReplyTooLargeDetail>,
    }

    #[async_trait::async_trait]
    impl ServeHandler for MockSubscribeHandler {
        fn compaction_supported(&self) -> bool {
            self.model_reply_too_large.is_some()
        }

        async fn authorize_session(
            &self,
            _session: &BrocaBindIdentity,
            _principal: Option<&broca_protocol::BindPrincipal>,
        ) -> Result<(), ServeError> {
            if self.refuse_session.load(Ordering::Relaxed) {
                Err(ServeError::ScopeRefused(
                    broca_core::ScopeRefusal::SenderNotOwner,
                ))
            } else {
                Ok(())
            }
        }

        async fn send(&self, _req: SendRequest) -> Result<SendResponse, ServeError> {
            unimplemented!()
        }
        async fn retract(
            &self,
            _session: &BrocaBindIdentity,
            _submission_id: broca_protocol::SubmissionId,
        ) -> Result<RetractResponse, ServeError> {
            unimplemented!()
        }
        async fn subscribe(
            &self,
            _session: &BrocaBindIdentity,
            _from: SubscribeFrom,
            sink: mpsc::Sender<Delivery>,
        ) -> Result<(), ServeError> {
            let _ = self.sink_tx.send(sink).await;
            Ok(())
        }
        async fn read(&self, _req: SessionReadRequest) -> Result<SessionReadResponse, ServeError> {
            unimplemented!()
        }
        async fn read_model(
            &self,
            _req: SessionReadRequest,
        ) -> Result<broca_wire::ModelPage, ServeError> {
            Err(ServeError::ReplyTooLarge(
                self.model_reply_too_large
                    .clone()
                    .expect("model-page test supplies a reply-too-large detail"),
            ))
        }
        async fn cancel(
            &self,
            _session: &BrocaBindIdentity,
            _run_id: broca_protocol::RunId,
        ) -> Result<CancelResponse, ServeError> {
            unimplemented!()
        }
        async fn status(&self, _session: &BrocaBindIdentity) -> Result<StatusResponse, ServeError> {
            unimplemented!()
        }
        async fn run_status(
            &self,
            _session: &BrocaBindIdentity,
            _run_id: broca_protocol::RunId,
        ) -> Result<RunStatusResponse, ServeError> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn subscribe_transport_terminals_keep_stream_sequence_and_event() {
        for terminal in [
            Delivery::Detached,
            Delivery::Error("broken replay".into()),
            Delivery::Expired {
                expired_at_ms: 1234,
            },
        ] {
            let detached = matches!(terminal, Delivery::Detached);
            let (sink_tx, mut sink_rx) = mpsc::channel(1);
            let handler = Arc::new(MockSubscribeHandler {
                sink_tx,
                refuse_session: std::sync::atomic::AtomicBool::new(false),
                model_reply_too_large: None,
            });
            let (writer, mut written) = broca_diagnostics::channel::<Frame>(8, Arc::default());
            start_subscribe_stream(
                writer,
                1,
                1,
                1,
                1,
                BrocaBindIdentity::new("/tmp/project-a", "runner", "terminal-sequence"),
                SubscribeFrom::Start,
                None,
                handler,
                CancellationToken::new(),
            )
            .await
            .unwrap();
            let sink = sink_rx.recv().await.unwrap();
            sink.send(Delivery::Display(Arc::new(
                broca_protocol::DisplayEvent::DisplayReset,
            )))
            .await
            .unwrap();
            sink.send(terminal).await.unwrap();
            let first = tokio::time::timeout(std::time::Duration::from_secs(2), written.recv())
                .await
                .unwrap()
                .unwrap();
            let end = tokio::time::timeout(std::time::Duration::from_secs(2), written.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(first.header.ty, FrameType::StreamData);
            assert_eq!(
                end.header.ty,
                if detached {
                    FrameType::StreamEnd
                } else {
                    FrameType::Error
                }
            );
            for (frame, seq) in [(first, 1_u64), (end, 2)] {
                let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
                assert_eq!(body["seq"], seq);
                assert_eq!(body["event"]["type"], "display_reset");
            }
        }
    }

    #[tokio::test]
    async fn test_subscribe_forward_task_cancel_while_blocked() {
        use broca_protocol::display::DisplayEvent;
        use std::time::Duration;

        let (sink_tx, mut sink_rx) = mpsc::channel(1);
        let handler = Arc::new(MockSubscribeHandler {
            sink_tx,
            refuse_session: std::sync::atomic::AtomicBool::new(false),
            model_reply_too_large: None,
        });

        // Bounded channel of size 1 for the writer.
        let (writer_tx, _writer_rx) = broca_diagnostics::channel::<Frame>(1, Arc::default());

        let cancel = CancellationToken::new();
        let session = BrocaBindIdentity::new("/tmp/project-a", "runner", "s1");

        let start_res = start_subscribe_stream(
            writer_tx,
            1, // ver
            1, // channel
            1, // epoch
            1, // corr
            session,
            SubscribeFrom::Start,
            None,
            handler,
            cancel.clone(),
        )
        .await;
        assert!(start_res.is_ok());

        let delivery_sink = sink_rx.recv().await.unwrap();

        // Send first delivery to fill the writer channel buffer.
        let delivery1 = Delivery::Display(Arc::new(DisplayEvent::DisplayReset));
        delivery_sink.send(delivery1).await.unwrap();

        // Send second delivery. The forward task will receive it and block on writer.send(frame).await.
        let delivery2 = Delivery::Display(Arc::new(DisplayEvent::DisplayReset));
        delivery_sink.send(delivery2).await.unwrap();

        // Give the forward task a moment to block on writer.send.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Cancel the stream.
        cancel.cancel();

        // The forward task should unblock and exit, dropping the deliveries receiver.
        // We verify this by checking that sending a third delivery returns an error.
        let delivery3 = Delivery::Display(Arc::new(DisplayEvent::DisplayReset));

        let mut success = false;
        for _ in 0..20 {
            if delivery_sink.send(delivery3.clone()).await.is_err() {
                success = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            success,
            "forward task did not exit after cancel while blocked on send"
        );
    }

    /// A handler whose session status parks until the test releases it, so the test decides
    /// exactly when a route request task finishes.
    struct GatedStatusHandler {
        gate_auth: bool,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl ServeHandler for GatedStatusHandler {
        async fn authorize_session(
            &self,
            _session: &BrocaBindIdentity,
            _principal: Option<&broca_protocol::BindPrincipal>,
        ) -> Result<(), ServeError> {
            if self.gate_auth {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(())
        }

        async fn send(&self, _req: SendRequest) -> Result<SendResponse, ServeError> {
            unimplemented!()
        }
        async fn retract(
            &self,
            _session: &BrocaBindIdentity,
            _submission_id: broca_protocol::SubmissionId,
        ) -> Result<RetractResponse, ServeError> {
            unimplemented!()
        }
        async fn subscribe(
            &self,
            _session: &BrocaBindIdentity,
            _from: SubscribeFrom,
            _sink: mpsc::Sender<Delivery>,
        ) -> Result<(), ServeError> {
            unimplemented!()
        }
        async fn read(&self, _req: SessionReadRequest) -> Result<SessionReadResponse, ServeError> {
            unimplemented!()
        }
        async fn cancel(
            &self,
            _session: &BrocaBindIdentity,
            _run_id: broca_protocol::RunId,
        ) -> Result<CancelResponse, ServeError> {
            unimplemented!()
        }
        async fn status(&self, session: &BrocaBindIdentity) -> Result<StatusResponse, ServeError> {
            if session.session == "panicking-status" {
                panic!("a request handler bug");
            }
            if session.session == "oversized-error-response" {
                return Err(ServeError::Transient(
                    "x".repeat(broca_wire::SUBC_MAX_FRAME_BYTES as usize),
                ));
            }
            if session.session == "oversized-response" {
                return Ok(StatusResponse::Active {
                    run_id: "x".repeat(broca_wire::SUBC_MAX_FRAME_BYTES as usize),
                    step: 1,
                });
            }
            self.entered.notify_one();
            self.release.notified().await;
            Err(ServeError::Transient("released by the test".into()))
        }
        async fn run_status(
            &self,
            _session: &BrocaBindIdentity,
            _run_id: broca_protocol::RunId,
        ) -> Result<RunStatusResponse, ServeError> {
            unimplemented!()
        }
    }

    struct RefusingOpener;

    #[async_trait::async_trait]
    impl broca_module::registry::LineageOpener for RefusingOpener {
        async fn open(
            &self,
            _key: &BrocaBindIdentity,
            _gen: u64,
            _registry: broca_module::registry::RegistryHandle,
        ) -> Result<broca_module::registry::ActorHandle, broca_module::registry::OpenError>
        {
            Err(broca_module::registry::OpenError::Open(
                "this test opens no lineage".into(),
            ))
        }
    }

    async fn next_written(rx: &mut EgressReceiver<Frame>) -> Frame {
        tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("the serve loop wrote nothing within 5s")
            .expect("the serve loop's writer channel closed")
    }

    struct HeldBindRead {
        entered: tokio::sync::Notify,
        /// Holds the first history read until the test releases it. A bind may read again
        /// after that (a store without range proofs is then decoded whole), and those later
        /// reads return at once.
        release: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
        finished: tokio::sync::Notify,
        records: Vec<broca_wal::FramedRecord>,
    }

    impl broca_core::WalStore for HeldBindRead {
        fn open(
            &self,
            _: &BrocaBindIdentity,
        ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
            panic!("bind ownership must not open an actor")
        }
        fn read_records(
            &self,
            _: &BrocaBindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            let gate = self.release.lock().unwrap().take();
            if let Some(gate) = gate {
                self.entered.notify_one();
                gate.recv().unwrap();
                self.finished.notify_one();
            }
            Ok(self.records.clone())
        }
    }

    struct ReleaseBindRead(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for ReleaseBindRead {
        fn drop(&mut self) {
            if let Some(release) = self.0.take() {
                let _ = release.send(());
            }
        }
    }

    struct BindIngress {
        routes: Routes,
        registry: Arc<LineageRegistry>,
        handler: Arc<MockSubscribeHandler>,
        writer: EgressSender<Frame>,
        written: EgressReceiver<Frame>,
        sinks: mpsc::Receiver<mpsc::Sender<Delivery>>,
        requests: JoinSet<Result<(), ServeIoError>>,
        cancel: CancellationToken,
    }

    impl BindIngress {
        fn new(registry: Arc<LineageRegistry>) -> Self {
            let (writer, written) = broca_diagnostics::channel(16, Arc::default());
            let (sink_tx, sinks) = mpsc::channel(4);
            Self {
                routes: Routes::new(true),
                registry,
                handler: Arc::new(MockSubscribeHandler {
                    sink_tx,
                    refuse_session: std::sync::atomic::AtomicBool::new(false),
                    model_reply_too_large: None,
                }),
                writer,
                written,
                sinks,
                requests: JoinSet::new(),
                cancel: CancellationToken::new(),
            }
        }

        async fn frame(&mut self, frame: Frame) {
            handle_frame(
                frame,
                &self.writer,
                &mut self.routes,
                &self.handler,
                &self.registry,
                &VaultGauge::not_in_use(),
                None,
                &self.cancel,
                &Arc::new(Semaphore::new(4)),
                &mut self.requests,
                None,
            )
            .await
            .unwrap();
        }

        async fn bind(&mut self, epoch: u32, session: &str, stamped: bool) {
            let owner = subc_protocol::Principal::Reserved {
                module_id: "agent".into(),
            };
            let bind = ModuleControlRequest::RouteBind {
                route_channel: 7,
                epoch,
                target: subc_protocol::RouteTarget::ManagementSurface {
                    module_id: "broca".into(),
                },
                identity: SubcBindIdentity::new("/tmp/project-a", "runner", session),
                principal: Some(owner.clone()),
                consumer_capabilities: None,
                admission_facts: None,
                scope: stamped.then(|| {
                    subc_protocol::scope::ScopeStamp::new(
                        owner,
                        "head",
                        1,
                        subc_protocol::scope::ScopeKind::Head,
                    )
                    .with_owner_authorized(true)
                }),
                role_versions: None,
                call_mode: None,
            };
            self.frame(
                Frame::build(
                    FrameType::Request,
                    control_flags(),
                    0,
                    0,
                    u64::from(epoch),
                    serde_json::to_vec(&bind).unwrap(),
                )
                .unwrap(),
            )
            .await;
        }

        async fn complete_bind(&mut self) {
            let completed = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                self.routes.bind_tasks.join_next(),
            )
            .await
            .expect("bind authorization did not finish")
            .unwrap()
            .unwrap();
            finish_bind(completed, &self.writer, &mut self.routes)
                .await
                .unwrap();
        }

        async fn goodbye(&mut self, epoch: u32) {
            self.frame(
                Frame::build(FrameType::Goodbye, control_flags(), 7, epoch, 0, vec![]).unwrap(),
            )
            .await;
        }

        async fn subscribe(&mut self, epoch: u32, corr: u64) -> mpsc::Sender<Delivery> {
            self.frame(
                Frame::build(
                    FrameType::Request,
                    control_flags(),
                    7,
                    epoch,
                    corr,
                    serde_json::to_vec(&RouteRequest {
                        method: OP_SUBSCRIBE.into(),
                        params: serde_json::json!({}),
                    })
                    .unwrap(),
                )
                .unwrap(),
            )
            .await;
            let sink = tokio::time::timeout(std::time::Duration::from_secs(5), self.sinks.recv())
                .await
                .expect("subscription did not reach the handler")
                .unwrap();
            self.assert_delivery(&sink, epoch, corr).await;
            sink
        }

        async fn assert_delivery(&mut self, sink: &mpsc::Sender<Delivery>, epoch: u32, corr: u64) {
            sink.send(Delivery::Display(Arc::new(
                broca_protocol::display::DisplayEvent::DisplayReset,
            )))
            .await
            .expect("the original subscription was canceled");
            let delivered = next_written(&mut self.written).await;
            assert_eq!(
                (
                    delivered.header.ty,
                    delivered.header.epoch,
                    delivered.header.corr
                ),
                (FrameType::StreamData, epoch, corr)
            );
        }
    }

    impl Drop for BindIngress {
        fn drop(&mut self) {
            self.cancel.cancel();
        }
    }

    fn held_bind_registry() -> (BindIngress, Arc<HeldBindRead>, ReleaseBindRead) {
        held_bind_registry_with_records(vec![])
    }

    fn held_bind_registry_with_records(
        records: Vec<broca_wal::FramedRecord>,
    ) -> (BindIngress, Arc<HeldBindRead>, ReleaseBindRead) {
        let (release, read_gate) = std::sync::mpsc::channel();
        let store = Arc::new(HeldBindRead {
            entered: tokio::sync::Notify::new(),
            release: std::sync::Mutex::new(Some(read_gate)),
            finished: tokio::sync::Notify::new(),
            records,
        });
        let registry = Arc::new(LineageRegistry::new_with_reader(
            Arc::new(RefusingOpener),
            store.clone(),
            None,
        ));
        (
            BindIngress::new(registry),
            store,
            ReleaseBindRead(Some(release)),
        )
    }

    #[tokio::test]
    async fn pending_generation_goodbye_preserves_published_route_and_stream() {
        let (mut ingress, store, release) = held_bind_registry();
        ingress.bind(1, "published", false).await;
        assert_eq!(next_written(&mut ingress.written).await.header.corr, 1);
        let sink = ingress.subscribe(1, 10).await;
        ingress.bind(2, "pending", true).await;
        store.entered.notified().await;
        ingress.goodbye(2).await;
        assert!(!ingress.routes.pending_binds.contains_key(&7));
        assert_eq!(ingress.routes.by_channel[&7].0, 1);
        ingress.assert_delivery(&sink, 1, 10).await;
        drop(release);
        ingress.complete_bind().await;
        assert_eq!(ingress.routes.by_channel[&7].0, 1);
        assert!(
            ingress.written.try_recv().is_err(),
            "canceled bind acknowledged"
        );
    }

    #[tokio::test]
    async fn published_generation_goodbye_preserves_pending_bind() {
        let (mut ingress, store, release) = held_bind_registry();
        ingress.bind(1, "published", false).await;
        next_written(&mut ingress.written).await;
        let _sink = ingress.subscribe(1, 10).await;
        let slot = ingress.routes.subscribe_cancel[&7].clone();
        ingress.bind(2, "pending", true).await;
        store.entered.notified().await;
        let pending = ingress.routes.pending_binds[&7].1.clone();
        ingress.goodbye(1).await;
        assert!(slot.route.is_cancelled());
        assert!(!ingress.routes.by_channel.contains_key(&7));
        assert_eq!(ingress.routes.pending_binds[&7].0, 2);
        assert!(!pending.is_cancelled());
        drop(release);
        ingress.complete_bind().await;
        assert_eq!(ingress.routes.by_channel[&7].0, 2);
        let mut ack = next_written(&mut ingress.written).await;
        if ack.header.corr == 10 {
            assert_eq!((ack.header.ty, ack.header.epoch), (FrameType::StreamEnd, 1));
            ack = next_written(&mut ingress.written).await;
        }
        assert_eq!((ack.header.ty, ack.header.corr), (FrameType::Response, 2));
    }

    #[tokio::test]
    async fn accepted_replacement_makes_old_generation_goodbye_stale() {
        let (mut ingress, store, release) = held_bind_registry();
        ingress.bind(1, "published", false).await;
        next_written(&mut ingress.written).await;
        ingress.bind(2, "replacement", true).await;
        store.entered.notified().await;
        drop(release);
        ingress.complete_bind().await;
        assert_eq!(next_written(&mut ingress.written).await.header.corr, 2);
        let sink = ingress.subscribe(2, 20).await;
        ingress.goodbye(1).await;
        assert_eq!(ingress.routes.by_channel[&7].0, 2);
        ingress.assert_delivery(&sink, 2, 20).await;
    }

    /// Publishing a replacement generation ends the stream the replaced generation opened, whose
    /// own GOODBYE is ignored from then on, while a stream opened under the replacement lives on.
    #[tokio::test]
    async fn accepted_replacement_retires_the_old_generation_stream() {
        let (mut ingress, store, release) = held_bind_registry();
        ingress.bind(1, "published", false).await;
        next_written(&mut ingress.written).await;
        let old = ingress.subscribe(1, 10).await;
        ingress.bind(2, "replacement", true).await;
        store.entered.notified().await;
        drop(release);
        ingress.complete_bind().await;
        // A cancelled stream's StreamEnd is best-effort and may precede the acknowledgement.
        let ack = loop {
            let frame = next_written(&mut ingress.written).await;
            if (frame.header.ty, frame.header.epoch) != (FrameType::StreamEnd, 1) {
                break frame;
            }
        };
        assert_eq!((ack.header.ty, ack.header.corr), (FrameType::Response, 2));
        // The forwarder drops its delivery receiver only when it ends.
        tokio::time::timeout(std::time::Duration::from_secs(5), old.closed())
            .await
            .expect("the replaced generation's stream must end when the replacement is published");
        while let Ok(frame) = ingress.written.try_recv() {
            assert_eq!(
                (frame.header.ty, frame.header.epoch, frame.header.corr),
                (FrameType::StreamEnd, 1, 10)
            );
        }
        let sink = ingress.subscribe(2, 20).await;
        ingress.goodbye(1).await;
        assert_eq!(ingress.routes.by_channel[&7].0, 2);
        ingress.assert_delivery(&sink, 2, 20).await;
    }

    #[tokio::test]
    async fn refused_replacement_goodbye_preserves_published_route_and_stream() {
        let session = BrocaBindIdentity::new("/tmp/project-a", "runner", "published");
        let record = broca_wal::WalRecord::RunStarted {
            written_at_ms: None,
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
            run_id: broca_protocol::RunId("legacy".into()),
            session,
            config: Box::new(broca_wal::RunConfig {
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
                frozen_render_config: serde_json::json!({"test": true}),
                auth_selection: None,
                tools_were_omitted: false,
                budget: None,
                context_limit: None,
                work_class: None,
                variant: None,
                service_tier: None,
                stop_when: vec![],
                context_management: None,
            }),
            input: vec![].into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        };
        let (mut ingress, store, release) =
            held_bind_registry_with_records(vec![broca_wal::FramedRecord {
                seq: 1,
                fence: 1,
                record,
            }]);
        ingress.bind(1, "published", false).await;
        next_written(&mut ingress.written).await;
        let sink = ingress.subscribe(1, 10).await;
        ingress.bind(2, "published", true).await;
        store.entered.notified().await;
        drop(release);
        ingress.complete_bind().await;
        let refused = next_written(&mut ingress.written).await;
        assert_eq!(
            (refused.header.ty, refused.header.corr),
            (FrameType::Error, 2)
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&refused.body).unwrap()["code"],
            "scope_adoption_refused"
        );
        ingress.goodbye(2).await;
        assert_eq!(ingress.routes.by_channel[&7].0, 1);
        ingress.assert_delivery(&sink, 1, 10).await;
    }

    /// A WAL store whose every record read fails, so no ownership source can answer.
    struct UnreadableBindHistory;

    impl broca_core::WalStore for UnreadableBindHistory {
        fn open(
            &self,
            _: &BrocaBindIdentity,
        ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
            panic!("bind ownership must not open an actor")
        }
        fn read_records(
            &self,
            _: &BrocaBindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
            Err(broca_wal::WalError::Io(std::io::Error::other(
                "injected unreadable history",
            )))
        }
    }

    #[tokio::test]
    async fn stamped_bind_with_unreadable_history_is_refused_not_published() {
        let registry = Arc::new(LineageRegistry::new_with_reader(
            Arc::new(RefusingOpener),
            Arc::new(UnreadableBindHistory),
            None,
        ));
        let mut ingress = BindIngress::new(registry);
        ingress.bind(1, "unreadable", true).await;
        ingress.complete_bind().await;
        let refused = next_written(&mut ingress.written).await;
        assert_eq!(
            (refused.header.ty, refused.header.corr),
            (FrameType::Error, 1)
        );
        let body = serde_json::from_slice::<serde_json::Value>(&refused.body).unwrap();
        assert_eq!(body["code"], "open_failed", "{body}");
        assert_eq!(body["detail"]["class"], "transient", "{body}");
        assert!(
            !ingress.routes.by_channel.contains_key(&7),
            "a bind whose adoption check could not run must not be published"
        );
    }

    #[tokio::test]
    async fn request_during_stamped_bind_authorization_is_unbound() {
        let (mut ingress, store, release) = held_bind_registry();
        ingress.bind(2, "pending", true).await;
        store.entered.notified().await;
        ingress
            .frame(
                Frame::build(
                    FrameType::Request,
                    control_flags(),
                    7,
                    2,
                    20,
                    serde_json::to_vec(&RouteRequest {
                        method: OP_SUBSCRIBE.into(),
                        params: serde_json::json!({}),
                    })
                    .unwrap(),
                )
                .unwrap(),
            )
            .await;
        assert!(ingress.routes.by_channel.is_empty());
        assert_eq!(ingress.routes.pending_binds[&7].0, 2);
        assert!(ingress.routes.subscribe_cancel.is_empty());
        assert!(
            ingress.requests.is_empty(),
            "unbound request dispatched to a handler task"
        );
        assert!(ingress.sinks.try_recv().is_err());
        assert!(ingress.written.try_recv().is_err());
        drop(release);
        ingress.complete_bind().await;
        assert_eq!(next_written(&mut ingress.written).await.header.corr, 2);
        assert!(
            ingress.requests.is_empty(),
            "unbound request was deferred until ack"
        );
        assert!(ingress.sinks.try_recv().is_err());
    }

    #[tokio::test]
    async fn superseded_bind_late_authorization_preserves_replacement() {
        let (mut ingress, store, release) = held_bind_registry();
        ingress.bind(1, "superseded", true).await;
        store.entered.notified().await;
        ingress.bind(2, "replacement", false).await;
        assert_eq!(next_written(&mut ingress.written).await.header.corr, 2);
        let sink = ingress.subscribe(2, 20).await;
        drop(release);
        ingress.complete_bind().await;
        assert_eq!(ingress.routes.by_channel[&7].0, 2);
        assert_eq!(ingress.routes.by_channel[&7].1.session, "replacement");
        assert!(
            ingress.written.try_recv().is_err(),
            "superseded bind acknowledged"
        );
        ingress.assert_delivery(&sink, 2, 20).await;
    }

    #[tokio::test]
    async fn unauthorized_subscribe_reports_authorization_before_invalid_params() {
        let registry = Arc::new(LineageRegistry::new(Arc::new(RefusingOpener)));
        let mut ingress = BindIngress::new(registry);
        ingress.bind(1, "published", false).await;
        next_written(&mut ingress.written).await;
        ingress
            .handler
            .refuse_session
            .store(true, Ordering::Relaxed);
        for (corr, params) in [
            (20, serde_json::json!({"unknown": true})),
            (21, serde_json::json!({"from": "not-an-attach-point"})),
        ] {
            ingress
                .frame(
                    Frame::build(
                        FrameType::Request,
                        control_flags(),
                        7,
                        1,
                        corr,
                        serde_json::to_vec(&RouteRequest {
                            method: OP_SUBSCRIBE.into(),
                            params,
                        })
                        .unwrap(),
                    )
                    .unwrap(),
                )
                .await;
            let refused = next_written(&mut ingress.written).await;
            assert_eq!(
                (refused.header.ty, refused.header.corr),
                (FrameType::Error, corr)
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&refused.body).unwrap()["code"],
                "sender_not_owner"
            );
        }
        assert!(ingress.sinks.try_recv().is_err());
    }

    #[tokio::test]
    async fn unauthorized_replacement_subscribe_preserves_original_stream_correlation() {
        let registry = Arc::new(LineageRegistry::new(Arc::new(RefusingOpener)));
        let mut ingress = BindIngress::new(registry);
        ingress.bind(1, "published", false).await;
        next_written(&mut ingress.written).await;
        let sink = ingress.subscribe(1, 10).await;
        let original = ingress.routes.subscribe_cancel[&7]
            .current
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        ingress
            .handler
            .refuse_session
            .store(true, Ordering::Relaxed);
        ingress
            .frame(
                Frame::build(
                    FrameType::Request,
                    control_flags(),
                    7,
                    1,
                    20,
                    serde_json::to_vec(&RouteRequest {
                        method: OP_SUBSCRIBE.into(),
                        params: serde_json::json!({}),
                    })
                    .unwrap(),
                )
                .unwrap(),
            )
            .await;
        let refused = next_written(&mut ingress.written).await;
        assert_eq!(
            (refused.header.ty, refused.header.corr),
            (FrameType::Error, 20)
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&refused.body).unwrap()["code"],
            "sender_not_owner"
        );
        assert!(!original.is_cancelled());
        assert!(
            ingress.sinks.try_recv().is_err(),
            "unauthorized replacement reached subscribe"
        );
        ingress.assert_delivery(&sink, 1, 10).await;
    }

    #[tokio::test]
    async fn authorized_invalid_subscribe_preserves_original_stream_correlation() {
        let registry = Arc::new(LineageRegistry::new(Arc::new(RefusingOpener)));
        let mut ingress = BindIngress::new(registry);
        ingress.bind(1, "published", false).await;
        next_written(&mut ingress.written).await;
        let sink = ingress.subscribe(1, 10).await;
        ingress
            .frame(
                Frame::build(
                    FrameType::Request,
                    control_flags(),
                    7,
                    1,
                    20,
                    serde_json::to_vec(&RouteRequest {
                        method: OP_SUBSCRIBE.into(),
                        params: serde_json::json!({"unknown": true}),
                    })
                    .unwrap(),
                )
                .unwrap(),
            )
            .await;
        let invalid = next_written(&mut ingress.written).await;
        assert_eq!(
            (invalid.header.ty, invalid.header.corr),
            (FrameType::Error, 20)
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&invalid.body).unwrap()["code"],
            "invalid_params"
        );
        assert!(ingress.sinks.try_recv().is_err());
        ingress.assert_delivery(&sink, 1, 10).await;
    }

    #[tokio::test]
    async fn reader_answers_ping_and_goodbye_while_bind_ownership_read_is_held() {
        let (release, read_gate) = std::sync::mpsc::channel();
        let release = ReleaseBindRead(Some(release));
        let store = Arc::new(HeldBindRead {
            entered: tokio::sync::Notify::new(),
            release: std::sync::Mutex::new(Some(read_gate)),
            finished: tokio::sync::Notify::new(),
            records: vec![],
        });
        let registry = Arc::new(LineageRegistry::new_with_reader(
            Arc::new(RefusingOpener),
            store.clone(),
            None,
        ));
        let (mut daemon, module) = tokio::io::duplex(64 * 1024);
        let (reader, writer) = tokio::io::split(module);
        let serve = tokio::spawn(serve_connection(
            reader,
            writer,
            "broca",
            registry.clone(),
            registry,
            VaultGauge::not_in_use(),
        ));
        bind_test_route(&mut daemon, "slow-bind").await;
        let owner = subc_protocol::Principal::Reserved {
            module_id: "agent".into(),
        };
        let bind = ModuleControlRequest::RouteBind {
            route_channel: 7,
            epoch: 2,
            target: subc_protocol::RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new("/tmp/project-a", "runner", "slow-bind"),
            principal: Some(owner.clone()),
            consumer_capabilities: None,
            admission_facts: None,
            scope: Some(
                subc_protocol::scope::ScopeStamp::new(
                    owner,
                    "head",
                    1,
                    subc_protocol::scope::ScopeKind::Head,
                )
                .with_owner_authorized(true),
            ),
            role_versions: None,
            call_mode: None,
        };
        write_frame(
            &mut daemon,
            &Frame::build(
                FrameType::Request,
                control_flags(),
                0,
                0,
                3,
                serde_json::to_vec(&bind).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        store.entered.notified().await;
        write_frame(
            &mut daemon,
            &Frame::build(FrameType::Ping, control_flags(), 0, 0, 4, vec![]).unwrap(),
        )
        .await
        .unwrap();
        let pong = tokio::time::timeout(std::time::Duration::from_secs(5), read_frame(&mut daemon))
            .await
            .expect("held bind read blocked health ingress")
            .unwrap()
            .unwrap();
        assert_eq!((pong.header.ty, pong.header.corr), (FrameType::Pong, 4));
        write_frame(
            &mut daemon,
            &Frame::build(FrameType::Goodbye, control_flags(), 0, 0, 5, vec![]).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(serve.await.unwrap().unwrap(), ServeEnd::Goodbye);
        drop(release);
        store.finished.notified().await;
    }

    #[tokio::test]
    async fn pending_bind_goodbye_cannot_publish_a_late_ack() {
        let mut routes = Routes::new(true);
        let cancel = CancellationToken::new();
        routes.pending_binds.insert(7, (1, cancel.clone()));
        let goodbye = Frame::build(FrameType::Goodbye, control_flags(), 7, 1, 0, vec![]).unwrap();
        let (writer, mut written) = broca_diagnostics::channel::<Frame>(4, Arc::default());
        let registry = Arc::new(LineageRegistry::new(Arc::new(RefusingOpener)));
        handle_frame(
            goodbye,
            &writer,
            &mut routes,
            &registry,
            &registry,
            &VaultGauge::not_in_use(),
            None,
            &CancellationToken::new(),
            &Arc::new(Semaphore::new(1)),
            &mut JoinSet::new(),
            None,
        )
        .await
        .unwrap();
        let header = Frame::build(FrameType::Request, control_flags(), 0, 0, 2, vec![])
            .unwrap()
            .header;
        finish_bind(
            BindCompletion {
                header,
                channel: 7,
                epoch: 1,
                identity: BrocaBindIdentity::new("/tmp/project-a", "runner", "late-bind"),
                bind: BindObservation {
                    agent_data: false,
                    role: RouteRole::LlmRunner,
                    project_id: None,
                    principal: None,
                    scope: broca_protocol::SendScope::Unstamped,
                },
                cancel,
                refusal: Ok(None),
            },
            &writer,
            &mut routes,
        )
        .await
        .unwrap();
        assert!(routes.by_channel.is_empty());
        assert!(
            written.try_recv().is_err(),
            "a canceled bind must not publish an ack"
        );
    }

    struct ResetWrite<W> {
        inner: W,
        reset: Arc<std::sync::atomic::AtomicBool>,
        observed: Arc<tokio::sync::Notify>,
    }

    impl<W: AsyncWrite + Unpin> AsyncWrite for ResetWrite<W> {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            if self.reset.load(Ordering::SeqCst) {
                self.observed.notify_one();
                return std::task::Poll::Ready(Err(std::io::ErrorKind::ConnectionReset.into()));
            }
            std::pin::Pin::new(&mut self.inner).poll_write(cx, bytes)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    async fn bind_test_route(daemon: &mut tokio::io::DuplexStream, session: &str) {
        let hello = read_frame(daemon).await.unwrap().unwrap();
        write_frame(
            daemon,
            &Frame::build(
                FrameType::HelloAck,
                control_flags(),
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
            .unwrap(),
        )
        .await
        .unwrap();
        let bind = ModuleControlRequest::RouteBind {
            route_channel: 7,
            epoch: 1,
            target: subc_protocol::RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new("/tmp/project-a", "runner", session),
            principal: None,
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        };
        write_frame(
            daemon,
            &Frame::build(
                FrameType::Request,
                control_flags(),
                0,
                0,
                2,
                serde_json::to_vec(&bind).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            read_frame(daemon).await.unwrap().unwrap().header.ty,
            FrameType::Response
        );
    }

    async fn test_request(daemon: &mut tokio::io::DuplexStream, method: &str, corr: u64) {
        write_frame(
            daemon,
            &Frame::build(
                FrameType::Request,
                control_flags(),
                7,
                1,
                corr,
                serde_json::to_vec(&RouteRequest {
                    method: method.into(),
                    params: serde_json::Value::Null,
                })
                .unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn oversized_tool_response_is_call_local() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (writer, mut written) =
                broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, Arc::default());
            let large = serde_json::to_vec(&serde_json::json!({
                "answers":{"q":{"type":"noul","noul":0.5,"provider_extra":{"text":"x".repeat(broca_wire::SUBC_MAX_FRAME_BYTES as usize)}}},
                "usage":{"input_tokens":0},"cost_usd":0,
            })).unwrap();
            let result: cortexkit_llm_tools::classify::Result = serde_json::from_slice(&large).unwrap();
            cortexkit_llm_tools::classify::validate_result(&result).unwrap();
            send_response_bytes(
                &writer,
                PROTOCOL_VERSION,
                1,
                1,
                1,
                large,
            )
            .await
            .unwrap();
            send_response_bytes(&writer, PROTOCOL_VERSION, 1, 1, 2, b"{}".to_vec())
                .await
                .unwrap();
            let refused = next_written(&mut written).await;
            assert_eq!(refused.header.ty, FrameType::Error);
            assert_eq!(refused.header.corr, 1);
            let error: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
            assert_eq!(error["code"], "response_too_large");
            let sibling = next_written(&mut written).await;
            assert_eq!(sibling.header.ty, FrameType::Response);
            assert_eq!(sibling.header.corr, 2);
            assert_eq!(sibling.body, b"{}");
        })
        .await
        .expect("the tool reply writer did not finish within its bound");
    }

    #[test]
    fn reply_frame_size_uses_the_real_header_length_and_result_envelope() {
        let result = serde_json::json!({"answer":"ok"});
        let envelope = serde_json::to_vec(&serde_json::json!({"result":result})).unwrap();
        assert_eq!(
            broca_wire::serialized_result_frame_bytes(&result),
            envelope.len() as u64 + subc_protocol::HEADER_LEN as u64
        );
        assert_eq!(
            broca_wire::SUBC_FRAME_HEADER_BYTES,
            subc_protocol::HEADER_LEN as u64
        );
        assert_eq!(broca_wire::SUBC_MAX_FRAME_BYTES, 4_194_304);
    }

    #[tokio::test]
    async fn oversized_response_fails_only_its_call_and_sibling_completes() {
        for session in ["oversized-response", "oversized-error-response"] {
            let (mut daemon, module) = tokio::io::duplex(64 * 1024);
            let (reader, writer) = tokio::io::split(module);
            let handler = Arc::new(GatedStatusHandler {
                gate_auth: false,
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            });
            let serve = tokio::spawn(serve_connection(
                reader,
                writer,
                "broca",
                handler,
                Arc::new(LineageRegistry::new(Arc::new(RefusingOpener))),
                VaultGauge::not_in_use(),
            ));
            bind_test_route(&mut daemon, session).await;
            test_request(&mut daemon, OP_STATUS, 3).await;
            test_request(&mut daemon, OP_ROLE_DESCRIBE, 4).await;
            let error = read_frame(&mut daemon)
                .await
                .unwrap()
                .expect("oversized call lost the connection");
            assert_eq!((error.header.ty, error.header.corr), (FrameType::Error, 3));
            assert_eq!(
                serde_json::from_slice::<ErrorBody>(&error.body)
                    .unwrap()
                    .code,
                "response_too_large"
            );
            let sibling = read_frame(&mut daemon)
                .await
                .unwrap()
                .expect("sibling lost the connection");
            assert_eq!(
                (sibling.header.ty, sibling.header.corr),
                (FrameType::Response, 4)
            );
            write_frame(
                &mut daemon,
                &Frame::build(FrameType::Goodbye, control_flags(), 0, 0, 5, vec![]).unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(serve.await.unwrap().unwrap(), ServeEnd::Goodbye);
        }
    }

    #[tokio::test]
    async fn write_half_reset_is_classified_even_with_a_request_in_flight() {
        let (mut daemon, module) = tokio::io::duplex(64 * 1024);
        let (reader, writer) = tokio::io::split(module);
        let reset = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = Arc::new(tokio::sync::Notify::new());
        let handler = Arc::new(GatedStatusHandler {
            gate_auth: false,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let serve = tokio::spawn(serve_connection(
            reader,
            ResetWrite {
                inner: writer,
                reset: reset.clone(),
                observed: observed.clone(),
            },
            "broca",
            handler.clone(),
            Arc::new(LineageRegistry::new(Arc::new(RefusingOpener))),
            VaultGauge::not_in_use(),
        ));
        bind_test_route(&mut daemon, "reset-response").await;
        test_request(&mut daemon, OP_STATUS, 3).await;
        handler.entered.notified().await;
        reset.store(true, Ordering::SeqCst);
        write_frame(
            &mut daemon,
            &Frame::build(FrameType::Ping, control_flags(), 0, 0, 4, vec![]).unwrap(),
        )
        .await
        .unwrap();
        observed.notified().await;
        handler.release.notify_one();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), serve)
            .await
            .expect("write reset left the reader waiting on an unusable connection")
            .unwrap();
        assert_eq!(result.unwrap(), ServeEnd::PeerReset);
    }

    #[tokio::test]
    async fn reader_answers_health_ping_during_authorization_and_permit_waits() {
        let (mut daemon, mut module_read) = tokio::io::duplex(64 * 1024);
        let (writer, mut written) =
            broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, Arc::default());
        let handler = Arc::new(GatedStatusHandler {
            gate_auth: true,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let loop_handler = handler.clone();
        let serve = tokio::spawn(async move {
            let registry = LineageRegistry::new(Arc::new(RefusingOpener));
            run_frame_loop(
                &mut module_read,
                writer,
                "broca",
                loop_handler,
                &registry,
                &VaultGauge::not_in_use(),
                None,
                CancellationToken::new(),
                crate::wal_archive::WalArchiveConfig::default(),
                &crate::consumer_routes::ConsumerRoutes::default(),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
        });
        let hello = next_written(&mut written).await;
        write_frame(
            &mut daemon,
            &Frame::build(
                FrameType::HelloAck,
                control_flags(),
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
            .unwrap(),
        )
        .await
        .unwrap();
        // Separate sessions occupy all permits without depending on task scheduling order.
        for index in 0..=MAX_INFLIGHT_ROUTE_REQUESTS {
            let channel = index as u16 + 1;
            let bind = ModuleControlRequest::RouteBind {
                route_channel: channel,
                epoch: 1,
                target: subc_protocol::RouteTarget::ManagementSurface {
                    module_id: "broca".into(),
                },
                identity: SubcBindIdentity::new("/tmp/project-a", "runner", format!("s{index}")),
                principal: None,
                consumer_capabilities: None,
                admission_facts: None,
                scope: None,
                role_versions: None,
                call_mode: None,
            };
            write_frame(
                &mut daemon,
                &Frame::build(
                    FrameType::Request,
                    control_flags(),
                    0,
                    0,
                    2,
                    serde_json::to_vec(&bind).unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(
                next_written(&mut written).await.header.ty,
                FrameType::Response
            );
            write_frame(
                &mut daemon,
                &Frame::build(
                    FrameType::Request,
                    control_flags(),
                    channel,
                    1,
                    3,
                    serde_json::to_vec(&RouteRequest {
                        method: OP_STATUS.into(),
                        params: serde_json::Value::Null,
                    })
                    .unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
            if index < MAX_INFLIGHT_ROUTE_REQUESTS {
                handler.entered.notified().await;
            }
            // The last request has no permit; earlier requests are inside authorization.
            write_frame(
                &mut daemon,
                &Frame::build(FrameType::Ping, control_flags(), 0, 0, 4, vec![]).unwrap(),
            )
            .await
            .unwrap();
            let pong = next_written(&mut written).await;
            assert_eq!((pong.header.ty, pong.header.corr), (FrameType::Pong, 4));
        }
        write_frame(
            &mut daemon,
            &Frame::build(FrameType::Goodbye, control_flags(), 0, 0, 5, vec![]).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(serve.await.unwrap().unwrap(), ServeEnd::Goodbye);
    }

    /// A handler whose WAL presence probe blocks its thread until the test releases it, and
    /// records whether the release arrived in time.
    struct BlockingProbeHandler {
        entered: tokio::sync::Notify,
        release: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
        released: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl ServeHandler for BlockingProbeHandler {
        fn session_written(&self, _session: &BrocaBindIdentity) -> Result<bool, ServeError> {
            self.entered.notify_one();
            let release = self.release.lock().unwrap().take().unwrap();
            let released = release
                .recv_timeout(std::time::Duration::from_secs(2))
                .is_ok();
            self.released.store(released, Ordering::SeqCst);
            Ok(false)
        }
        async fn send(&self, _req: SendRequest) -> Result<SendResponse, ServeError> {
            unimplemented!()
        }
        async fn retract(
            &self,
            _session: &BrocaBindIdentity,
            _submission_id: broca_protocol::SubmissionId,
        ) -> Result<RetractResponse, ServeError> {
            unimplemented!()
        }
        async fn subscribe(
            &self,
            _session: &BrocaBindIdentity,
            _from: SubscribeFrom,
            _sink: mpsc::Sender<Delivery>,
        ) -> Result<(), ServeError> {
            unimplemented!()
        }
        async fn read(&self, _req: SessionReadRequest) -> Result<SessionReadResponse, ServeError> {
            unimplemented!()
        }
        async fn cancel(
            &self,
            _session: &BrocaBindIdentity,
            _run_id: broca_protocol::RunId,
        ) -> Result<CancelResponse, ServeError> {
            unimplemented!()
        }
        async fn status(&self, _session: &BrocaBindIdentity) -> Result<StatusResponse, ServeError> {
            unimplemented!()
        }
        async fn run_status(
            &self,
            _session: &BrocaBindIdentity,
            _run_id: broca_protocol::RunId,
        ) -> Result<RunStatusResponse, ServeError> {
            unimplemented!()
        }
    }

    /// Start a frame loop serving `handler`, complete its handshake, and bind route channel 7
    /// to `session`. Returns the daemon end of the socket, the loop's egress, and the loop.
    async fn bound_frame_loop<H: ServeHandler + 'static>(
        handler: Arc<H>,
        session: &str,
    ) -> (
        tokio::io::DuplexStream,
        EgressReceiver<Frame>,
        tokio::task::JoinHandle<Result<ServeEnd, ServeIoError>>,
    ) {
        let (mut daemon, mut module_read) = tokio::io::duplex(64 * 1024);
        let (writer, mut written) =
            broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, Arc::default());
        let serve = tokio::spawn(async move {
            let registry = LineageRegistry::new(Arc::new(RefusingOpener));
            run_frame_loop(
                &mut module_read,
                writer,
                "broca",
                handler,
                &registry,
                &VaultGauge::not_in_use(),
                None,
                CancellationToken::new(),
                crate::wal_archive::WalArchiveConfig::default(),
                &crate::consumer_routes::ConsumerRoutes::default(),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
        });
        let hello = next_written(&mut written).await;
        write_frame(
            &mut daemon,
            &Frame::build(
                FrameType::HelloAck,
                control_flags(),
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
            .unwrap(),
        )
        .await
        .unwrap();
        let bind = ModuleControlRequest::RouteBind {
            route_channel: 7,
            epoch: 1,
            target: subc_protocol::RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new("/tmp", "runner", session),
            principal: None,
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        };
        write_frame(
            &mut daemon,
            &Frame::build(
                FrameType::Request,
                control_flags(),
                0,
                0,
                2,
                serde_json::to_vec(&bind).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            next_written(&mut written).await.header.ty,
            FrameType::Response
        );
        (daemon, written, serve)
    }

    /// The WAL presence probe on the send path runs off the runtime worker. On this
    /// single-threaded runtime a probe run inline would block the only worker, so the task that
    /// releases it could never run.
    #[tokio::test]
    async fn the_send_path_wal_probe_does_not_block_the_runtime_worker() {
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let handler = Arc::new(BlockingProbeHandler {
            entered: tokio::sync::Notify::new(),
            release: std::sync::Mutex::new(Some(release_rx)),
            released: std::sync::atomic::AtomicBool::new(false),
        });
        let releaser = handler.clone();
        tokio::spawn(async move {
            releaser.entered.notified().await;
            let _ = release_tx.send(());
        });
        let (mut daemon, mut written, serve) = bound_frame_loop(handler.clone(), "probe").await;
        write_frame(
            &mut daemon,
            &Frame::build(
                FrameType::Request,
                control_flags(),
                7,
                1,
                3,
                serde_json::to_vec(&RouteRequest {
                    method: OP_SEND.into(),
                    params: serde_json::json!({"prompt": "steer me", "delivery": "steer"}),
                })
                .unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        // The session was never written, so the model-less steer is refused for its model.
        let reply = next_written(&mut written).await;
        assert_eq!((reply.header.ty, reply.header.corr), (FrameType::Error, 3));
        assert!(
            handler.released.load(Ordering::SeqCst),
            "the WAL presence probe must not run on the runtime worker"
        );
        serve.abort();
    }

    /// A request that has already sent its reply and then panics is not answered again: the
    /// daemon sees exactly one terminal frame for the correlation, never an orphan Error that
    /// could land on a newer request reusing it.
    #[tokio::test]
    async fn a_request_that_panics_after_replying_sends_one_terminal() {
        let (writer, mut written) =
            broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, Arc::default());
        let task_writer = writer.clone();
        let result = reply_on_panic(writer, (PROTOCOL_VERSION, 7, 1, 3), async move {
            send_json_response(
                &task_writer,
                PROTOCOL_VERSION,
                7,
                1,
                3,
                &serde_json::json!({"ok": true}),
            )
            .await?;
            if true {
                panic!("a bug after the reply");
            }
            Ok(())
        })
        .await;
        assert!(result.is_ok());
        let reply = next_written(&mut written).await;
        assert_eq!(
            (reply.header.ty, reply.header.corr),
            (FrameType::Response, 3)
        );
        assert!(
            written.try_recv().is_err(),
            "a request answers once: no second terminal after its reply"
        );
    }

    /// A panic inside one request's task is confined to that request: the request is answered
    /// on its own correlation with a transient refusal carrying a fixed message, and the
    /// connection keeps answering frames and ends normally on GOODBYE.
    #[tokio::test]
    async fn a_panicking_request_does_not_end_the_connection() {
        let (mut daemon, mut module_read) = tokio::io::duplex(64 * 1024);
        let (writer, mut written) =
            broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, Arc::default());
        let handler = Arc::new(GatedStatusHandler {
            gate_auth: false,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let serve = tokio::spawn(async move {
            let registry = LineageRegistry::new(Arc::new(RefusingOpener));
            run_frame_loop(
                &mut module_read,
                writer,
                "broca",
                handler,
                &registry,
                &VaultGauge::not_in_use(),
                None,
                CancellationToken::new(),
                crate::wal_archive::WalArchiveConfig::default(),
                &crate::consumer_routes::ConsumerRoutes::default(),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
        });
        let hello = next_written(&mut written).await;
        write_frame(
            &mut daemon,
            &Frame::build(
                FrameType::HelloAck,
                control_flags(),
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
            .unwrap(),
        )
        .await
        .unwrap();
        let bind = ModuleControlRequest::RouteBind {
            route_channel: 7,
            epoch: 1,
            target: subc_protocol::RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new("/tmp/project-a", "runner", "panicking-status"),
            principal: None,
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        };
        write_frame(
            &mut daemon,
            &Frame::build(
                FrameType::Request,
                control_flags(),
                0,
                0,
                2,
                serde_json::to_vec(&bind).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            next_written(&mut written).await.header.ty,
            FrameType::Response
        );
        test_request(&mut daemon, OP_STATUS, 3).await;
        write_frame(
            &mut daemon,
            &Frame::build(FrameType::Ping, control_flags(), 0, 0, 4, vec![]).unwrap(),
        )
        .await
        .unwrap();
        // The refusal and the PONG may arrive in either order: collect both and tell them
        // apart by correlation.
        let mut frames = Vec::new();
        while frames.len() < 2 {
            match tokio::time::timeout(std::time::Duration::from_secs(5), written.recv()).await {
                Ok(Some(frame)) => frames.push(frame),
                _ => break,
            }
        }
        let refusal = frames
            .iter()
            .find(|frame| frame.header.corr == 3)
            .expect("the panicked request must be answered on its own correlation");
        assert_eq!(
            (refusal.header.ty, refusal.header.channel),
            (FrameType::Error, 7)
        );
        let pong = frames
            .iter()
            .find(|frame| frame.header.corr == 4)
            .expect("the connection still answers PING");
        let body: ErrorBody = serde_json::from_slice(&refusal.body).unwrap();
        assert_eq!(body.code, "transient");
        assert_eq!(
            body.message,
            ServeError::Transient(REQUEST_PANICKED_MESSAGE.into()).to_string()
        );
        assert!(
            !body.message.contains("a request handler bug"),
            "the panic payload must not reach the caller"
        );
        assert_eq!((pong.header.ty, pong.header.corr), (FrameType::Pong, 4));
        write_frame(
            &mut daemon,
            &Frame::build(FrameType::Goodbye, control_flags(), 0, 0, 5, vec![]).unwrap(),
        )
        .await
        .unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), serve)
            .await
            .expect("the serve loop did not end on GOODBYE")
            .unwrap();
        assert_eq!(
            result.expect("a panicking request must not end the connection with an error"),
            ServeEnd::Goodbye
        );
    }

    /// The tool plane's `scope.describe` goes out on this registered module connection, only
    /// once it has registered, and the daemon's reply is routed back to the describer instead
    /// of the ordinary frame handling. Before registration the describer answers "can't tell".
    #[tokio::test]
    async fn a_scope_describe_rides_the_registered_module_connection() {
        let (mut daemon, mut module_read) = tokio::io::duplex(64 * 1024);
        let (writer_tx, mut written) =
            broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, Arc::default());
        let describer = Arc::new(broca_subc::ModuleScopeDescriber::new());
        let owner = subc_protocol::Principal::Reserved {
            module_id: "prefrontal".into(),
        };
        assert!(
            broca_subc::ScopeDescriber::describe(describer.as_ref(), &owner, "head")
                .await
                .is_err(),
            "no describe before the connection registers"
        );
        let loop_describer = Arc::clone(&describer);
        let handler = Arc::new(GatedStatusHandler {
            gate_auth: false,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let serve = tokio::spawn(async move {
            let registry = LineageRegistry::new(Arc::new(RefusingOpener));
            let consumer_routes = crate::consumer_routes::ConsumerRoutes::default();
            run_frame_loop(
                &mut module_read,
                writer_tx,
                "broca",
                handler,
                &registry,
                &VaultGauge::not_in_use(),
                None,
                CancellationToken::new(),
                crate::wal_archive::WalArchiveConfig::default(),
                &consumer_routes,
                None,
                None,
                Some(loop_describer.as_ref()),
                None,
                None,
                None,
            )
            .await
        });

        let hello = next_written(&mut written).await;
        let ack = Frame::build(
            FrameType::HelloAck,
            control_flags(),
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
        write_frame(&mut daemon, &ack).await.unwrap();

        let asking = {
            let describer = Arc::clone(&describer);
            tokio::spawn(async move {
                broca_subc::ScopeDescriber::describe(describer.as_ref(), &owner, "head").await
            })
        };
        let request = next_written(&mut written).await;
        assert_eq!(request.header.ty, FrameType::Request);
        assert_eq!(request.header.channel, 0);
        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["op"], "scope.describe");
        assert_eq!(body["ref"], "head");

        let reply = Frame::build(
            FrameType::Response,
            control_flags(),
            0,
            0,
            request.header.corr,
            serde_json::to_vec(&serde_json::json!({
                "op": "scope.describe",
                "status": "ended",
                "scope_epoch": 4,
                "daemon_incarnation": "d1",
                "owner_synced": true,
                "owner_configured": true,
            }))
            .unwrap(),
        )
        .unwrap();
        write_frame(&mut daemon, &reply).await.unwrap();
        let described = tokio::time::timeout(std::time::Duration::from_secs(5), asking)
            .await
            .expect("the reply reached the describer")
            .unwrap()
            .expect("a described scope");
        assert_eq!(described.status, subc_protocol::scope::ScopeStatus::Ended);
        assert_eq!(described.scope_epoch, Some(4));

        drop(daemon);
        let _ = serve.await;
    }

    /// A request task finishing while the loop is part-way through reading a frame must not
    /// lose the bytes already read.
    ///
    /// Measured 2026-09-26 00:09:26Z (v0.3.132): broca exited 1 with `unsupported envelope
    /// version 32`, a space from a JSON body read where a frame header should start. The loop
    /// selected between `join_next` and a fresh `read_frame` each turn; `read_frame` is not
    /// cancellation-safe, so when `join_next` won while a frame was half-read, the half that
    /// had been read was dropped with the future and the next read began mid-frame. This test
    /// holds a frame half-written, lets a request task finish, then writes the rest.
    #[tokio::test]
    async fn a_request_task_finishing_mid_frame_does_not_desync_the_reader() {
        use tokio::io::AsyncWriteExt as _;

        let (mut daemon, mut module_read) = tokio::io::duplex(64 * 1024);
        let (writer_tx, mut written) =
            broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, Arc::default());
        let handler = Arc::new(GatedStatusHandler {
            gate_auth: false,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let loop_handler = handler.clone();
        let serve = tokio::spawn(async move {
            let registry = LineageRegistry::new(Arc::new(RefusingOpener));
            let consumer_routes = crate::consumer_routes::ConsumerRoutes::default();
            run_frame_loop(
                &mut module_read,
                writer_tx,
                "broca",
                loop_handler,
                &registry,
                &VaultGauge::not_in_use(),
                None,
                CancellationToken::new(),
                crate::wal_archive::WalArchiveConfig::default(),
                &consumer_routes,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
        });

        let hello = next_written(&mut written).await;
        let ack = Frame::build(
            FrameType::HelloAck,
            control_flags(),
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
        write_frame(&mut daemon, &ack).await.unwrap();

        let bind = ModuleControlRequest::RouteBind {
            route_channel: 7,
            epoch: 1,
            target: subc_protocol::RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new(
                std::path::PathBuf::from("/tmp/project-a"),
                "runner",
                "s1",
            ),
            principal: None,
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        };
        let bind = Frame::build(
            FrameType::Request,
            control_flags(),
            0,
            0,
            2,
            serde_json::to_vec(&bind).unwrap(),
        )
        .unwrap();
        write_frame(&mut daemon, &bind).await.unwrap();
        assert_eq!(
            next_written(&mut written).await.header.ty,
            FrameType::Response
        );

        // One route request, parked in the handler: this is the task that will finish mid-frame.
        let status = Frame::build(
            FrameType::Request,
            control_flags(),
            7,
            1,
            3,
            serde_json::to_vec(&RouteRequest {
                method: OP_STATUS.into(),
                params: serde_json::Value::Null,
            })
            .unwrap(),
        )
        .unwrap();
        write_frame(&mut daemon, &status).await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handler.entered.notified(),
        )
        .await
        .expect("the status request never reached the handler");

        // A health probe whose JSON body opens with spaces, so a reader restarted inside the
        // body sees version 32, the byte the production crash reported. Write the header and
        // the first few body bytes only, and give the loop time to take them into its read.
        let mut probe = vec![b' '; 256];
        probe.extend(serde_json::to_vec(&ModuleControlRequest::HealthCheck {}).unwrap());
        let probe = Frame::build(FrameType::Request, control_flags(), 0, 0, 4, probe).unwrap();
        let mut encoded = Vec::new();
        write_frame(&mut encoded, &probe).await.unwrap();
        let split = subc_protocol::HEADER_LEN + 10;
        daemon.write_all(&encoded[..split]).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // The request task finishes now, while the probe is half-read.
        handler.release.notify_one();
        let reply = next_written(&mut written).await;
        assert_eq!((reply.header.channel, reply.header.corr), (7, 3));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        daemon.write_all(&encoded[split..]).await.unwrap();
        let answer = tokio::time::timeout(std::time::Duration::from_secs(5), written.recv()).await;
        drop(daemon);
        let loop_result = tokio::time::timeout(std::time::Duration::from_secs(5), serve)
            .await
            .expect("the serve loop did not end at EOF")
            .unwrap();
        let answer = answer.ok().flatten().unwrap_or_else(|| {
            panic!(
                "the half-read probe was lost when a request task finished mid-frame; \
                 the loop returned {loop_result:?}"
            )
        });
        assert_eq!(
            (answer.header.ty, answer.header.channel, answer.header.corr),
            (FrameType::Response, 0, 4)
        );
        assert!(
            loop_result.is_ok(),
            "EOF after the probe is a normal end, got {loop_result:?}"
        );
    }

    #[tokio::test]
    async fn unbound_and_stale_module_frames_are_counted_without_replies() {
        let (mut daemon, mut module_read) = tokio::io::duplex(64 * 1024);
        let (writer_tx, mut written) =
            broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, Arc::default());
        let handler = Arc::new(GatedStatusHandler {
            gate_auth: false,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let loop_handler = handler.clone();
        let serve = tokio::spawn(async move {
            let registry = LineageRegistry::new(Arc::new(RefusingOpener));
            let consumer_routes = crate::consumer_routes::ConsumerRoutes::default();
            run_frame_loop(
                &mut module_read,
                writer_tx,
                "broca",
                loop_handler,
                &registry,
                &VaultGauge::not_in_use(),
                None,
                CancellationToken::new(),
                crate::wal_archive::WalArchiveConfig::default(),
                &consumer_routes,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
        });

        let hello = next_written(&mut written).await;
        let ack = Frame::build(
            FrameType::HelloAck,
            control_flags(),
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
        write_frame(&mut daemon, &ack).await.unwrap();

        // A new connection reports both serve-loop counters before this test submits any drops.
        let health = Frame::build(
            FrameType::Request,
            control_flags(),
            0,
            0,
            2,
            serde_json::to_vec(&ModuleControlRequest::HealthCheck {}).unwrap(),
        )
        .unwrap();
        write_frame(&mut daemon, &health).await.unwrap();
        let health = next_written(&mut written).await;
        assert_eq!(
            (health.header.ty, health.header.channel, health.header.corr),
            (FrameType::Response, 0, 2)
        );
        let health: serde_json::Value = serde_json::from_slice(&health.body).unwrap();
        assert_eq!(
            health["metrics"]["retention_repairs_pending"].as_u64(),
            Some(0),
            "health.check includes the retention repair backlog, zero included"
        );
        assert!(
            health["metrics"]["serve_frames_dropped_unbound_channel"]
                .as_u64()
                .is_some(),
            "health.check includes the unbound-channel counter, zero included"
        );
        assert!(
            health["metrics"]["serve_frames_dropped_stale_epoch"]
                .as_u64()
                .is_some(),
            "health.check includes the stale-epoch counter, zero included"
        );

        let bind = ModuleControlRequest::RouteBind {
            route_channel: 7,
            epoch: 1,
            target: subc_protocol::RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new(
                std::path::PathBuf::from("/tmp/project-a"),
                "runner",
                "s1",
            ),
            principal: None,
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        };
        let bind = Frame::build(
            FrameType::Request,
            control_flags(),
            0,
            0,
            3,
            serde_json::to_vec(&bind).unwrap(),
        )
        .unwrap();
        write_frame(&mut daemon, &bind).await.unwrap();
        let bind_ack = next_written(&mut written).await;
        assert_eq!(
            (
                bind_ack.header.ty,
                bind_ack.header.channel,
                bind_ack.header.corr
            ),
            (FrameType::Response, 0, 3)
        );

        let unbound_before = SERVE_FRAMES_DROPPED_UNBOUND_CHANNEL.load(Ordering::Relaxed);
        let stale_before = SERVE_FRAMES_DROPPED_STALE_EPOCH.load(Ordering::Relaxed);
        let status_body = serde_json::to_vec(&RouteRequest {
            method: OP_STATUS.into(),
            params: serde_json::Value::Null,
        })
        .unwrap();
        let unbound = Frame::build(
            FrameType::Request,
            control_flags(),
            11,
            1,
            4,
            status_body.clone(),
        )
        .unwrap();
        let stale = Frame::build(
            FrameType::Request,
            control_flags(),
            7,
            2,
            5,
            status_body.clone(),
        )
        .unwrap();
        let correctly_bound =
            Frame::build(FrameType::Request, control_flags(), 7, 1, 6, status_body).unwrap();
        write_frame(&mut daemon, &unbound).await.unwrap();
        write_frame(&mut daemon, &stale).await.unwrap();
        write_frame(&mut daemon, &correctly_bound).await.unwrap();

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handler.entered.notified(),
        )
        .await
        .expect("the correct-epoch request did not reach its bound route");
        // The counters are process-wide and other tests in this binary run in parallel, so
        // another test's drop can land between the two reads. Each counter must still have
        // risen by at least this test's one drop of its own cause: counting a drop under the
        // wrong cause, or not at all, leaves one of them unmoved.
        assert!(
            SERVE_FRAMES_DROPPED_UNBOUND_CHANNEL.load(Ordering::Relaxed) > unbound_before,
            "the unbound-channel request was not counted as an unbound-channel drop"
        );
        assert!(
            SERVE_FRAMES_DROPPED_STALE_EPOCH.load(Ordering::Relaxed) > stale_before,
            "the stale-epoch request was not counted as a stale-epoch drop"
        );

        handler.release.notify_one();
        let response = next_written(&mut written).await;
        assert_eq!(
            (response.header.ty, response.header.channel, response.header.corr),
            (FrameType::Error, 7, 6),
            "the two dropped requests must not produce replies, and the bound route must still answer"
        );
        drop(daemon);
        let loop_result = tokio::time::timeout(std::time::Duration::from_secs(5), serve)
            .await
            .expect("the serve loop did not end at EOF")
            .unwrap();
        assert!(loop_result.is_ok(), "EOF is a normal end: {loop_result:?}");
        assert!(
            written.try_recv().is_err(),
            "a dropped request got an extra reply"
        );
    }
}

#[cfg(test)]
mod open_failed_wire_classes {
    use super::*;

    #[tokio::test]
    async fn open_failed_serializes_each_shared_class() {
        for (class, spelling) in [
            (broca_protocol::ErrorClass::Transient, "transient"),
            (broca_protocol::ErrorClass::Permanent, "permanent"),
            (broca_protocol::ErrorClass::Auth, "auth_required"),
            (
                broca_protocol::ErrorClass::ContextOverflow,
                "context_overflow",
            ),
        ] {
            let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
            send_serve_error(
                &tx,
                1,
                7,
                1,
                2,
                &ServeError::Open("known cause".into(), Some(class)),
            )
            .await
            .unwrap();
            let frame = rx.recv().await.unwrap();
            let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
            assert_eq!(body["detail"]["class"], spelling);
            assert_eq!(body["message"], "open failed: known cause");
        }
    }
}

#[cfg(test)]
mod agent_run_writer_tests {
    use super::*;

    #[test]
    fn run_scope_deadline_does_not_change_frozen_scope_bytes() {
        let stamp = subc_protocol::scope::ScopeStamp::new(
            subc_protocol::Principal::Reserved {
                module_id: "prefrontal-core".into(),
            },
            "parent-run",
            1,
            subc_protocol::scope::ScopeKind::Head,
        )
        .with_attributes(
            subc_protocol::scope::ScopeAttributes::new()
                .with_agent_id(Some("agent-1".into()))
                .with_run_id(Some("parent-run".into())),
        )
        .with_owner_authorized(true);
        let principal = subc_protocol::Principal::Reserved {
            module_id: "basal".into(),
        };
        let freeze = |stamp: &subc_protocol::scope::ScopeStamp| {
            let bind = to_bind_scope(stamp, Some(&principal));
            let broca_core::ScopeDecision::Freeze(scope) = broca_core::decide_scope(
                &broca_core::SessionOwnershipV1::default(),
                Some(&to_bind_principal(&principal)),
                &broca_protocol::SendScope::Stamped(bind),
            )
            .unwrap() else {
                panic!("the first send must freeze the scope")
            };
            serde_json::to_vec(&scope).unwrap()
        };
        let expected = br#"{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"parent-run","scope_epoch":1,"sender":{"kind":"reserved","module_id":"basal"},"agent_id":"agent-1","run_id":"parent-run"}"#;
        assert_eq!(freeze(&stamp), expected);
        for deadline in [0, 1, u64::MAX] {
            assert_eq!(
                freeze(&stamp.clone().with_expires_at_ms(Some(deadline))),
                expected
            );
        }
    }

    #[test]
    fn agent_run_bind_identity_reaches_writer_admission() {
        let stamp: subc_protocol::scope::ScopeStamp = serde_json::from_value(serde_json::json!({
            "owner": {"kind": "reserved", "module_id": "prefrontal-core"},
            "ref": "parent-run", "scope_epoch": 1,
            "kind": "head", "owner_authorized": true,
            "attributes": {"agent_id": "agent-1", "run_id": "parent-run"}
        }))
        .unwrap();
        let principal = subc_protocol::Principal::Reserved {
            module_id: "basal".into(),
        };
        let bind = to_bind_scope(&stamp, Some(&principal));
        assert_eq!(bind.scope.run_id.as_deref(), Some("parent-run"));
        assert_eq!(bind.scope.agent_id.as_deref(), Some("agent-1"));
        assert_eq!(
            bind.scope
                .sender
                .as_ref()
                .map(broca_protocol::BindPrincipal::as_owner_string),
            Some("reserved:basal".into())
        );
        broca_core::scope_admission::require_flow_scope(
            bind.scope.sender.as_ref(),
            &broca_protocol::SendScope::Stamped(bind.clone()),
        )
        .unwrap();
        assert_eq!(
            broca_core::decide_scope(
                &broca_core::SessionOwnershipV1::default(),
                bind.scope.sender.as_ref(),
                &broca_protocol::SendScope::Stamped(bind.clone()),
            ),
            Ok(broca_core::ScopeDecision::Freeze(bind.scope))
        );
    }
}

#[cfg(test)]
mod agent_data_refusal_vectors {
    use super::*;

    // broca owns no host-managed transcripts or activation undo images. Their
    // generic refusal cases must still survive this serve ERROR-frame writer,
    // without inventing broca operations that would produce them.
    #[tokio::test]
    async fn contract_refusal_vectors_use_the_serve_error_frame_writer() {
        use broca_wire::agent_data::{
            errors::{driver_action_for, may_refuse, DriverAction, ErrorBody as ContractError},
            Op,
        };
        let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
        for name in ["refusals.json", "digest-mismatch.json"] {
            let file: serde_json::Value =
                serde_json::from_str(cortexkit_role_agent_data::vectors::file(name).unwrap())
                    .unwrap();
            for (index, case) in file["refusals"].as_array().unwrap().iter().enumerate() {
                let error: ContractError = serde_json::from_value(case["body"].clone()).unwrap();
                send_error_bytes(
                    &tx,
                    PROTOCOL_VERSION,
                    7,
                    3,
                    index as u64,
                    serde_json::to_vec(&error).unwrap(),
                )
                .await
                .unwrap();
                let frame = rx.recv().await.unwrap();
                assert_eq!(frame.header.ty, FrameType::Error);
                assert_eq!(frame.header.channel, 7);
                assert_eq!(frame.header.epoch, 3);
                assert_eq!(frame.header.corr, index as u64);
                let decoded: ContractError = serde_json::from_slice(&frame.body).unwrap();
                assert_eq!(
                    decoded.code.as_str(),
                    case["body"]["code"],
                    "{}",
                    case["name"]
                );
                assert_eq!(decoded.message, case["body"]["message"], "{}", case["name"]);
                assert_eq!(
                    serde_json::to_value(&decoded).unwrap()["detail"],
                    case["body"]["detail"],
                    "{}",
                    case["name"]
                );
                let action = match case["op"].as_str() {
                    Some(op) => {
                        let op = Op::from_method(&format!("agent_data.{op}")).unwrap();
                        assert!(may_refuse(op, &decoded.code));
                        if let Some(detail) = decoded.digest_mismatch_detail().unwrap() {
                            assert_eq!(
                                detail.reason.as_deref(),
                                case["body"]["detail"]["reason"].as_str()
                            );
                        }
                        driver_action_for(op, &decoded.code)
                    }
                    None => decoded.code.driver_action(),
                };
                let expected = match case["driver"].as_str().unwrap() {
                    "abort" => DriverAction::Abort,
                    "retry_within_step" => DriverAction::RetryWithinStep,
                    "retry_later" => DriverAction::RetryLater,
                    "retry_after_operator" => DriverAction::RetryAfterOperator,
                    "retry" => DriverAction::Retry,
                    "terminal_for_step" => DriverAction::TerminalForStep,
                    other => panic!("unknown driver-action vector {other}"),
                };
                assert_eq!(action, expected, "{}", case["name"]);
                assert_eq!(action.retryable(), case["retryable"], "{}", case["name"]);
                assert_eq!(decoded.code.is_known(), case["named"], "{}", case["name"]);
            }
        }
    }
}

#[cfg(test)]
mod delivery_list_tests {
    use super::*;

    fn describe_with_capabilities(
        capabilities: &[String],
    ) -> cortexkit_role_llm_runner::describe::RoleDescribe {
        cortexkit_role_llm_runner::describe::RoleDescribe::new(Vec::new(), "test")
            .with_capabilities(capabilities.to_vec())
    }

    #[test]
    fn undeclared_delivery_lists_match_published_refusal_vectors() {
        let raw = cortexkit_role_llm_runner::vectors::FILES
            .iter()
            .find_map(|(name, raw)| (*name == "delivery-order.json").then_some(*raw))
            .unwrap();
        let vectors: serde_json::Value = serde_json::from_str(raw).unwrap();
        let cases = vectors["undeclared_mode_lists"].as_array().unwrap();
        assert!(!cases.is_empty());
        for case in cases {
            let params: SendParams = serde_json::from_value(case["request"].clone()).unwrap();
            let capabilities: Vec<String> =
                serde_json::from_value(case["declares"].clone()).unwrap();
            let describe = describe_with_capabilities(&capabilities);
            let refusal = serde_json::to_value(
                delivery_list_refusal(params.delivery.as_ref(), &describe)
                    .expect("a wholly undeclared list must be refused"),
            )
            .unwrap();
            assert_eq!(refusal["code"], case["refusal"]["code"]);
            assert_eq!(refusal["detail"], case["refusal"]["detail"]);
        }
    }

    #[test]
    fn only_undeclared_delivery_modes_are_refused() {
        let params: SendParams = serde_json::from_value(
            serde_json::json!({"prompt":"reminder", "delivery":["turn_system"]}),
        )
        .unwrap();
        let before = serde_json::to_value(&params.delivery).unwrap();
        let describe = broca_wire::broca_role_describe("test").role;
        let refusal = serde_json::to_value(
            delivery_list_refusal(params.delivery.as_ref(), &describe)
                .expect("turn_system has no landing path"),
        )
        .unwrap();
        assert_eq!(refusal["code"], broca_wire::DELIVERY_UNSUPPORTED);
        assert_eq!(refusal["detail"]["delivery"], before);
    }

    #[test]
    fn one_declared_mode_keeps_the_original_delivery_list_accepted() {
        let params: SendParams = serde_json::from_value(
            serde_json::json!({"prompt":"reminder", "delivery":["turn_system","idle_start"]}),
        )
        .unwrap();
        let before = serde_json::to_value(&params.delivery).unwrap();
        let describe = broca_wire::broca_role_describe("test").role;
        assert!(delivery_list_refusal(params.delivery.as_ref(), &describe).is_none());
        assert_eq!(serde_json::to_value(&params.delivery).unwrap(), before);
        let none_declared = describe.with_delivery_order_modes(Vec::new());
        let refusal = serde_json::to_value(
            delivery_list_refusal(params.delivery.as_ref(), &none_declared).unwrap(),
        )
        .unwrap();
        assert_eq!(refusal["detail"]["delivery"], before);
    }
}

#[cfg(test)]
#[path = "serve_classifier_tests.rs"]
mod classifier_route_tests;
