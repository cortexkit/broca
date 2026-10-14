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

use crate::stall::{self, Phase};
use broca_diagnostics::{Receiver as EgressReceiver, Sender as EgressSender};
use broca_module::serve::{
    ServeError, ServeHandler, SessionImportRequest, SessionReadRequest, SubscribeFrom,
};
use broca_module::{Delivery, LineageRegistry};
use broca_protocol::BindIdentity as BrocaBindIdentity;
use broca_wire::{encode_subscribe_control, encode_subscribe_display};
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
    OP_BASELINE, OP_CANCEL, OP_CAP_INSTALL, OP_HEAD, OP_IMPORT, OP_READ, OP_RETRACT,
    OP_ROLE_DESCRIBE, OP_RUN_RESULT, OP_SEND, OP_SPEND_DELTA, OP_STATUS, OP_SUBSCRIBE,
    OP_VAULT_AUDIT, OP_WARM,
};
use crate::wire::{
    CancelParams, CapInstallParams, CapMutationResult, FromWireExt, ImportParams, ImportResult,
    RetractParams, RouteRequest, SendParams, SendParamsExt, SendResult, SpendDeltaParams,
    StatusParams, SubscribeParams,
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
fn execute_class_root_refusal(session: &BrocaBindIdentity) -> Option<String> {
    if session.project_root.exists() {
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
    // The bind observations (project id, principal) stay beside the session key rather than
    // inside it: BindIdentity also keys the actor registry, so folding them in would give one
    // session WAL a separate actor per distinct observation, all writing the same file.
    by_channel: std::collections::HashMap<u16, (u32, BrocaBindIdentity, BindObservation)>,
    subscribe_cancel: std::collections::HashMap<u16, CancellationToken>,
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
            by_channel: std::collections::HashMap::new(),
            subscribe_cancel: std::collections::HashMap::new(),
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

    fn route_closed(&mut self, channel: u16) {
        let departed_session = self
            .by_channel
            .remove(&channel)
            .map(|(_, session, _)| session);
        if let Some(cancel) = self.subscribe_cancel.remove(&channel) {
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
        from: SubscribeFrom,
        cancel: CancellationToken,
    },
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
) -> Result<ServeEnd, ServeIoError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
    H: ServeHandler + 'static,
{
    let stall_state = stall::connection_state();
    let (tx, rx) = broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, stall_state.egress.clone());
    let writer = tokio::spawn(drain_writer(write_half, rx));
    let reader_lifetime = stall::reader_lifetime(stall_state.clone());

    // A connection-level cancel: when the frame loop ends (EOF / GOODBYE / error), this
    // cancels every live subscribe forward task so they drop their egress sender clones,
    // letting the writer task's channel close and `drain_writer` exit. Without it, a
    // lineage-scoped subscription's parked forward task would keep the egress open forever
    // and hang the connection on teardown.
    let conn_cancel = CancellationToken::new();
    let loop_result = stall::READER
        .scope(
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
            ),
        )
        .await;
    drop(reader_lifetime);
    // Before the egress queue can close: the describer holds a sender on it.
    if let Some(describer) = &scope_describer {
        describer.detach();
    }
    conn_cancel.cancel();
    drop(tx);
    let writer_result = writer.await;

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
            joined = request_tasks.join_next(), if !request_tasks.is_empty() => {
                match joined.expect("guarded by !request_tasks.is_empty()") {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => break Err(e),
                    Err(e) if e.is_cancelled() => {}
                    Err(e) => break Err(ServeIoError(format!("route request task failed: {e}"))),
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
            routes.route_closed(frame.header.channel);
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
            let authorization = {
                let write = matches!(
                    req.method.as_str(),
                    OP_SEND | OP_WARM | OP_IMPORT | OP_CANCEL | OP_RETRACT
                );
                let session_read = matches!(
                    req.method.as_str(),
                    OP_READ | OP_HEAD | OP_RUN_RESULT | OP_STATUS | OP_SUBSCRIBE | OP_BASELINE
                );
                if write || session_read {
                    if let Err(error) = handler.check_migration(&session, write).await {
                        send_serve_error(writer, ver, channel, epoch, corr, &error).await?;
                        return Ok(true);
                    }
                }
                let _phase = stall::enter(Phase::FlowAuthorization).await;
                authorize_route_request(handler.as_ref(), &session, &bind, &req.method).await
            };
            if let Err(error) = authorization {
                send_serve_error(writer, ver, channel, epoch, corr, &error).await?;
                return Ok(true);
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
                )
                .await?;
            } else {
                let Some(prepared) = prepare_route_request(
                    req,
                    writer,
                    routes,
                    channel,
                    epoch,
                    corr,
                    ver,
                    conn_cancel,
                )
                .await?
                else {
                    return Ok(true);
                };
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
            ..
        } => {
            stall::method_context(1);
            // Keep the daemon's principal with the channel so every episode this route starts
            // records who started it. A session may later adopt a scope only if the scope's
            // owner is the principal on every earlier episode, and an episode recorded without
            // one can never satisfy that.
            let bind = BindObservation {
                project_id: identity.project_id.clone(),
                principal: principal.as_ref().map(to_bind_principal),
                scope: bind_send_scope(scope.as_ref(), routes.daemon_scopes, principal.as_ref()),
            };
            let identity = to_broca_identity(&identity);
            if let Err(error) = identity.validate_keyable() {
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
            // A stamped bind to a session recorded without a scope whose history refuses the
            // stamp is refused here too, with the same typed body the send would get, so the
            // opener learns it at route open rather than on its first send.
            if let broca_protocol::SendScope::Stamped(stamp) = &bind.scope {
                let refusal = {
                    let _phase = stall::enter(Phase::BindOwnership).await;
                    health_registry
                        .bind_adoption_refusal(&identity, bind.principal.as_ref(), stamp)
                        .await
                };
                if let Some(refusal) = refusal {
                    let body = serde_json::to_vec(&adoption_refused(&refusal))
                        .map_err(|e| ServeIoError(e.to_string()))?;
                    return send_error_bytes(
                        writer,
                        frame.header.ver,
                        0,
                        0,
                        frame.header.corr,
                        body,
                    )
                    .await;
                }
            }
            tracing::debug!(
                target: "wire.ingress",
                stage = "bound",
                channel = route_channel, epoch,
                session = %identity.session,
                "module route bound"
            );
            routes
                .by_channel
                .insert(route_channel, (epoch, identity, bind));
            let body = serde_json::to_vec(&ModuleControlResponse::RouteBindAck {})
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
        // Health probes read only in-memory gauges stamped by normal dependency calls. A probe
        // must never add catalog, vault, WAL, lease, or store IO to an already degraded daemon.
        ModuleControlRequest::HealthCheck {} => {
            stall::method_context(2);
            let counts = health_registry.slot_counts();
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
            // A count names the event and not the state it leaves, so an operator reading
            // `projection_failures: 3` learns that something failed and not what to do about
            // it. The consequence is what makes it actionable: the store's watermark stayed
            // put, so affected sessions re-read a WAL tail that grows every turn and their
            // transcript pages get slower until the next successful projection. That is the
            // per-page latency we chased from the reader's side before connecting it to this
            // counter. Degraded rather than Ok because reads really are degrading, even
            // though every run completed correctly.
            let projection_failures = broca_core::run::projection_failures();
            if projection_failures > 0 {
                details.push(format!(
                    "store projection failed {projection_failures}x since boot;                      affected sessions re-read their full WAL tail per page until one succeeds"
                ));
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
            let status = if details.is_empty() {
                subc_protocol::session::HealthStatus::Ok
            } else {
                subc_protocol::session::HealthStatus::Degraded
            };
            let detail = (!details.is_empty()).then(|| details.join("; "));
            let (stale_epoch_drops, unmatched_terminal_drops) = broca_subc::FRAME_DROPS.counts();
            // EVERY FIGURE BELOW IS THIS PROCESS DESCRIBING ITSELF: in-memory counters and
            // gauges this same process wrote. That is what makes the probe cheap and lock-free,
            // and it bounds what a green reading can mean -- it says the process believes it is
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
                    // Attempts this process saw fail, not a claim that the store is current: a
                    // fresh process reads zero with an arbitrarily stale store behind it. Non-zero
                    // means some session's reads are merging a growing WAL tail and getting slower
                    // every turn, which previously surfaced only as one line in the shared log.
                    "projection_failures": projection_failures,
                    // Lifetime operation counts for THIS process. They reset on restart and are not
                    // fleet totals; consumers derive rates by diffing two readings from one launch.
                    // Always emit zero so a quiet process cannot be confused with an unwired counter.
                    "stream_chunks": broca_provider::sse::stream_chunks(),
                    // Entries providers listed in input_transformations: for Anthropic, signed
                    // thinking blocks dropped because an earlier part of the conversation changed
                    // between requests. Zero is healthy; anything else means a render was not
                    // append-only, and the WARN log names the session and step.
                    "input_transformations": broca_core::input_transformations_reported(),
                    "session_reads": broca_module::registry::session_reads(),
                    "actor_opens": broca_module::registry::actor_opens(),
            "wal_archive_polls": crate::wal_archive::wal_archive_polls(),
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

/// Before spawning a route request, verify the state needed to manage that route. For
/// `subscribe`, only a valid request should be allowed to replace the active stream on this
/// channel, so a malformed retry cannot accidentally cancel the stream that is already
/// running.
#[allow(clippy::too_many_arguments)]
async fn prepare_route_request(
    req: RouteRequest,
    writer: &EgressSender<Frame>,
    routes: &mut Routes,
    channel: u16,
    epoch: u32,
    corr: u64,
    ver: u8,
    conn_cancel: &CancellationToken,
) -> Result<Option<PreparedRouteRequest>, ServeIoError> {
    if req.method != OP_SUBSCRIBE {
        return Ok(Some(PreparedRouteRequest::Request(req)));
    }

    let params: SubscribeParams = match decode_params(&req.params) {
        Ok(p) => p,
        Err((field, message)) => {
            send_invalid_params(writer, ver, channel, epoch, corr, field, &message).await?;
            return Ok(None);
        }
    };
    let from = match params.from {
        None => SubscribeFrom::Live,
        Some(w) => match w.to_subscribe_from() {
            Ok(f) => f,
            Err(e) => {
                send_invalid_params(writer, ver, channel, epoch, corr, Some("from".into()), &e)
                    .await?;
                return Ok(None);
            }
        },
    };

    if let Some(prev) = routes.subscribe_cancel.remove(&channel) {
        prev.cancel();
    }
    let cancel = conn_cancel.child_token();
    routes.subscribe_cancel.insert(channel, cancel.clone());
    Ok(Some(PreparedRouteRequest::Subscribe { from, cancel }))
}

/// Dispatch `vault.audit` without acquiring an actor lease or touching the bound session.
/// The configured `VaultAuditor` performs the vault call; this decodes params and writes the answer.
#[allow(clippy::too_many_arguments)]
async fn spawn_vault_audit_request(
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
) -> Result<(), ServeIoError> {
    let permit = {
        let _phase = stall::enter(Phase::RequestPermit).await;
        request_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ServeIoError("request concurrency limiter closed".into()))?
    };
    let writer = writer.clone();
    let conn_cancel = conn_cancel.clone();
    request_tasks.spawn(async move {
        let _permit = permit;
        let work = async move {
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
        };
        tokio::select! {
            _ = conn_cancel.cancelled() => Ok(()),
            result = work => result,
        }
    });
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
    let permit = {
        let _phase = stall::enter(Phase::RequestPermit).await;
        request_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ServeIoError("request concurrency limiter closed".into()))?
    };
    let writer = writer.clone();
    let handler = handler.clone();
    let conn_cancel = conn_cancel.clone();
    let describe =
        matches!(&prepared, PreparedRouteRequest::Request(req) if req.method == OP_ROLE_DESCRIBE);
    if describe {
        trace_describe("permit_acquired", channel, epoch, corr);
    }

    request_tasks.spawn(async move {
        let _permit = permit;
        if describe {
            trace_describe("task_started", channel, epoch, corr);
        }
        // The connection keeps reading while requests run, but actor-bound work for one session
        // takes its turn in wire-arrival order. Separate sessions use separate lanes and remain
        // concurrent; replies may still arrive out of order because correlation ids disambiguate them.
        match prepared {
            PreparedRouteRequest::Request(req) => {
                let _turn = dispatch_ticket.wait().await;
                if describe {
                    trace_describe("turn_acquired", channel, epoch, corr);
                }
                tokio::select! {
                    _ = conn_cancel.cancelled() => Ok(()),
                    result = execute_route_request(req, &writer, ver, channel, epoch, corr, session, bind, &handler) => result,
                }
            }
            PreparedRouteRequest::Subscribe { from, cancel } => {
                let _turn = dispatch_ticket.wait().await;
                if let Err(error) = handler.authorize_session(&session, bind.principal.as_ref()).await {
                    return send_serve_error(&writer, ver, channel, epoch, corr, &error).await;
                }
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
                        handler,
                        cancel.clone(),
                    ) => result,
                }
            }
        }
    });
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

/// Handle a request on an already bound route channel by checking the request method and
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
    if let Err(error) =
        authorize_route_request(handler.as_ref(), &session, &bind, &req.method).await
    {
        return send_serve_error(writer, ver, channel, epoch, corr, &error).await;
    }
    match req.method.as_str() {
        "compaction.ready" => {
            let request: broca_wire::CompactionReady =
                match serde_json::from_value(req.params.clone()) {
                    Ok(request) => request,
                    Err(error) => {
                        let field = broca_wire::failing_field::<broca_wire::CompactionReady>(
                            &req.params,
                            &error,
                        );
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
            let caller = match bind.principal.as_ref() {
                Some(broca_protocol::BindPrincipal::Reserved { module_id }) => module_id.as_str(),
                _ => "",
            };
            match handler.compaction_ready(&session, caller, request).await {
                Ok(Ok(reply)) => {
                    send_json_response(writer, ver, channel, epoch, corr, &reply).await
                }
                Ok(Err(code)) => send_error(writer, ver, channel, epoch, corr, code, code).await,
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_CAP_INSTALL => {
            let params: CapInstallParams = match serde_json::from_value(req.params) {
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
                    .await;
                }
            };
            match handler.cap_install(params.into_core()).await {
                Ok(broca_core::CapMutation::Applied(watermark)) => {
                    let response = CapMutationResult {
                        applied_watermark: watermark,
                    };
                    send_json_response(writer, ver, channel, epoch, corr, &response).await
                }
                Ok(broca_core::CapMutation::BaselineRequired) => {
                    send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "baseline_required",
                        "baseline_required",
                    )
                    .await
                }
                Err(error) => send_serve_error(writer, ver, channel, epoch, corr, &error).await,
            }
        }
        OP_SPEND_DELTA => {
            let params: SpendDeltaParams = match serde_json::from_value(req.params) {
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
                    .await;
                }
            };
            match handler.spend_delta(params.into_core()).await {
                Ok(broca_core::CapMutation::Applied(watermark)) => {
                    let response = CapMutationResult {
                        applied_watermark: watermark,
                    };
                    send_json_response(writer, ver, channel, epoch, corr, &response).await
                }
                Ok(broca_core::CapMutation::BaselineRequired) => {
                    send_error(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        "baseline_required",
                        "baseline_required",
                    )
                    .await
                }
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
            // Surface what this send carries that broca does not act on: an ignored key, or a
            // variant that is recorded but not yet applied. Warn only; never refuse here.
            crate::send_audit::warn_send_notices(
                crate::send_audit::global_send_auditor(),
                &req.params,
                &session.harness,
            );
            let mut params: SendParams = match serde_json::from_value(req.params.clone()) {
                Ok(p) => p,
                Err(e) => {
                    let field = broca_wire::failing_field::<SendParams>(&req.params, &e);
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
            // A steer or interrupt into a session that already has an episode takes every
            // runner parameter it omits from the session's latest frozen config, so its turn
            // renders the session's own request prefix. Admission fills them in, from the
            // admission view it reads anyway, so nothing is read here beyond whether the session
            // was ever written. Any other send takes the defaults for what it omits, and must
            // name a model.
            let inherit_frozen: std::collections::BTreeSet<
                broca_protocol::request::InheritedParam,
            > = match params.delivery {
                Some(broca_protocol::Delivery::Steer | broca_protocol::Delivery::Interrupt) => {
                    broca_protocol::request::InheritedParam::ALL
                        .iter()
                        .filter(|(_, member)| {
                            req.params
                                .get(*member)
                                .is_none_or(serde_json::Value::is_null)
                        })
                        .map(|(param, _)| *param)
                        .collect()
                }
                _ => Default::default(),
            };
            if params.model.is_none() {
                let may_inherit = inherit_frozen
                    .contains(&broca_protocol::request::InheritedParam::Model)
                    && handler.session_written(&session);
                if !may_inherit {
                    return send_invalid_params(
                        writer,
                        ver,
                        channel,
                        epoch,
                        corr,
                        Some("model".into()),
                        "model is required, except on a steer or interrupt into a session that has one",
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
            let inherit_frozen = if inherit_frozen.is_empty() || !handler.session_written(&session)
            {
                Default::default()
            } else {
                inherit_frozen
            };
            if let Some(detail) = execute_class_root_refusal(&session) {
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
                        params.model.as_ref().expect("model was resolved"),
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
            match handler.send(send_req).await {
                Ok(resp) => {
                    let (result, delivered) = match resp {
                        broca_module::SendResponse::Finished {
                            run_id,
                            reason,
                            delivered,
                        } => (SendResult::Finished { run_id, reason }, delivered),
                        broca_module::SendResponse::Paused {
                            run_id,
                            pause_reason,
                        } => (
                            SendResult::Paused {
                                run_id,
                                pause_reason,
                            },
                            None,
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
                Err(e) => send_serve_error(writer, ver, channel, epoch, corr, &e).await,
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
            if let Some(detail) = execute_class_root_refusal(&session) {
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
            match handler
                .cancel(&session, broca_protocol::RunId(params.run_id))
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
                        .run_status(&session, broca_protocol::RunId(run_id))
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
            let describe = broca_wire::broca_role_describe_with_writers(
                env!("CARGO_PKG_VERSION"),
                handler.compaction_supported(),
                crate::RETENTION_WRITER_ENABLED,
                handler.hook_plane_available(),
            );
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
            match handler.head(&session).await {
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
    if let Err(e) = handler.subscribe(&session, from, sink).await {
        return send_serve_error(&writer, ver, channel, epoch, corr, &e).await;
    }
    if cancel.is_cancelled() {
        return Ok(());
    }

    // Forward task: Delivery → StreamData (control=Interactive, display=Background lane
    // labels) until the stream ends, then StreamEnd. A session subscription is
    // LINEAGE-scoped — it spans turns and does NOT end when one run finishes, so the task
    // parks on `deliveries.recv()` between runs. It ends on: a `Detached` delivery (the
    // control stream gapped — the client resubscribes); the sink closing (the coordinator
    // tore the subscriber down); a failed egress send (client gone); or `cancel` (this
    // route's GOODBYE, or the connection teardown). `cancel` is what prevents a parked task
    // from holding the egress open forever and hanging the connection on teardown.
    tokio::spawn(async move {
        loop {
            let delivery = tokio::select! {
                _ = cancel.cancelled() => break,
                d = deliveries.recv() => match d {
                    Some(d) => d,
                    None => break, // the coordinator's subscriber task ended (sink closed)
                },
            };
            if let Delivery::Expired { expired_at_ms } = &delivery {
                let _ = send_serve_error(
                    &writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    &ServeError::Expired {
                        expired_at_ms: *expired_at_ms,
                    },
                )
                .await;
                return;
            }
            if let Delivery::Error(message) = &delivery {
                // A replay failure is a typed stream terminal, not an empty successful stream.
                // `Error` is already terminal in the existing subc vocabulary, so do not append
                // a clean `StreamEnd` that could overwrite its meaning for a client.
                let _ = send_error(
                    &writer,
                    ver,
                    channel,
                    epoch,
                    corr,
                    "subscribe_replay_failed",
                    message,
                )
                .await;
                return;
            }
            let (priority, body) = match encode_delivery(&delivery) {
                Some(v) => v,
                None => break, // a Detached marker: end the stream (client resubscribes)
            };
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
                Err(_) => break,
            };
            tokio::select! {
                _ = cancel.cancelled() => break,
                res = writer.send(frame) => {
                    if res.is_err() {
                        return; // egress closed → client gone; stop (the sink drop tears down the sub)
                    }
                }
            }
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
            Vec::new(),
        ) {
            tokio::select! {
                _ = cancel.cancelled() => {}
                _ = writer.send(end) => {}
            }
        }
    });
    Ok(())
}

/// Encode one data Delivery to (lane priority, body bytes). Terminal deliveries have no data
/// body; the forward task handles `Error` before calling this and treats `Detached` as a clean
/// resubscribe boundary.
fn encode_delivery(delivery: &Delivery) -> Option<(Priority, Vec<u8>)> {
    match delivery {
        Delivery::Control(event) => {
            let (cursor, unit) = event.as_ref();
            encode_subscribe_control(cursor.wal_seq, cursor.sub_index, unit)
                .ok()
                .map(|body| (Priority::Interactive, body))
        }
        Delivery::Display(event) => encode_subscribe_display(event.as_ref())
            .ok()
            .map(|body| (Priority::Background, body)),
        Delivery::Expired { .. } | Delivery::Error(_) | Delivery::Detached => None,
    }
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

async fn send_serve_error(
    writer: &EgressSender<Frame>,
    ver: u8,
    channel: u16,
    epoch: u32,
    corr: u64,
    err: &ServeError,
) -> Result<(), ServeIoError> {
    let code = match err {
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
        ServeError::SendIdReuse { .. } => "send_id_reuse",
        ServeError::Transient(_) => "transient",
        ServeError::BaselineRequired => "baseline_required",
        ServeError::CapUnavailable(_) => "cap_unavailable",
        ServeError::BudgetDenied { .. } => "budget_denied",
        ServeError::Import(_) => "import_rejected",
        ServeError::ImportIdReuse => "import_id_reuse",
        ServeError::ImportBusy => "import_busy",
        ServeError::InvalidParams { .. } => broca_wire::INVALID_PARAMS,
        ServeError::LineageChanged { .. } => broca_wire::LINEAGE_CHANGED,
        ServeError::UnknownMid(_) => broca_wire::UNKNOWN_MID,
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
        }
    }
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
    model: &crate::wire::ModelParams,
) -> Result<broca_module::actor::SendPlan, SendPlanRefusal> {
    if let Some(params) = params.as_object() {
        broca_fetch_plan::check_send_members(params)?;
    }
    let support = if handler.compaction_supported() {
        broca_fetch_plan::CompactionSupport::Supported
    } else {
        broca_fetch_plan::CompactionSupport::Unsupported
    };
    let parsed = broca_fetch_plan::parse_plan(plan, support)?;
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
    // A reader build must not admit a plan it cannot execute, even when its WAL decoder
    // understands hook records. Refuse before fetching providers or allocating a lineage.
    if !parsed.step_items.is_empty() && !handler.hook_plane_available() {
        return Err(broca_fetch_plan::Refusal::InvalidParams {
            detail: serde_json::json!({"field": "plan.step_transform_items"}),
        }
        .into());
    }
    // Even an unchanged frozen plan can carry persona text for a different model on a later send.
    broca_fetch_plan::admit::check_plan_model(
        &parsed,
        &model.provider,
        &model.model,
        model.variant.as_deref(),
    )?;
    match handler.plan_state(session).await {
        broca_module::serve::PlanState::Admitted { frozen } => {
            broca_fetch_plan::check_later_plan(frozen.as_deref(), &parsed.identity)?;
            Ok(broca_module::actor::SendPlan {
                identity: parsed.identity,
                manifest: None,
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
        _ => return None,
    })
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
    body: Vec<u8>,
) -> Result<(), ServeIoError> {
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
        OP_CAP_INSTALL => 14,
        OP_SPEND_DELTA => 15,
        OP_RUN_RESULT => 16,
        OP_WARM => 17,
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
    use broca_module::serve::{
        CancelResponse, RetractResponse, RunStatusResponse, SendRequest, SendResponse,
        SessionReadResponse, StatusResponse,
    };

    #[tokio::test]
    async fn watchdog_reader_phase_gates_name_stalls() {
        use crate::stall::{tests::Witness, Gate, State};
        use std::time::Duration;
        for (phase, expected_name) in [
            (Phase::FlowAuthorization, "flow_authorization"),
            (Phase::BindOwnership, "bind_ownership"),
            (Phase::RequestPermit, "request_permit"),
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
            let session = BrocaBindIdentity::new(
                std::path::PathBuf::from("/tmp/stall-project"),
                "runner",
                "stall-session",
            );
            let mut routes = Routes::new(true);
            routes.by_channel.insert(
                7,
                (
                    1,
                    session.clone(),
                    BindObservation {
                        project_id: None,
                        principal: None,
                        scope: broca_protocol::SendScope::Unstamped,
                    },
                ),
            );
            let (channel, body) = if phase == Phase::BindOwnership {
                let owner = subc_protocol::Principal::Reserved {
                    module_id: "prefrontal".into(),
                };
                let bind = ModuleControlRequest::RouteBind {
                    route_channel: 7,
                    epoch: 1,
                    target: subc_protocol::RouteTarget::ManagementSurface {
                        module_id: "broca".into(),
                    },
                    identity: SubcBindIdentity::new(
                        std::path::PathBuf::from("/tmp/stall-project"),
                        "runner",
                        "stall-session",
                    ),
                    principal: Some(owner.clone()),
                    consumer_capabilities: None,
                    admission_facts: None,
                    role_versions: None,
                    call_mode: None,
                    scope: Some(
                        subc_protocol::scope::ScopeStamp::new(
                            owner,
                            "head",
                            1,
                            subc_protocol::scope::ScopeKind::Head,
                        )
                        .with_owner_authorized(true),
                    ),
                };
                (0, serde_json::to_vec(&bind).unwrap())
            } else if matches!(phase, Phase::Control | Phase::EgressSend) {
                (
                    0,
                    serde_json::to_vec(&ModuleControlRequest::HealthCheck {}).unwrap(),
                )
            } else {
                (
                    7,
                    serde_json::to_vec(&serde_json::json!({"method": OP_STATUS, "params": {}}))
                        .unwrap(),
                )
            };
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
                },
                SessionReadMessage {
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

        routes.route_closed(7);
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

        routes.route_closed(7);
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

        routes.route_closed(7);
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

    struct MockSubscribeHandler {
        sink_tx: mpsc::Sender<mpsc::Sender<Delivery>>,
    }

    #[async_trait::async_trait]
    impl ServeHandler for MockSubscribeHandler {
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
    async fn test_subscribe_forward_task_cancel_while_blocked() {
        use broca_protocol::display::DisplayEvent;
        use std::time::Duration;

        let (sink_tx, mut sink_rx) = mpsc::channel(1);
        let handler = Arc::new(MockSubscribeHandler { sink_tx });

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
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl ServeHandler for GatedStatusHandler {
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
