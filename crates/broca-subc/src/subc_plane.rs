//! `SubcToolPlane` — the real tool plane (2nd [`ToolPlane`] impl). Dispatches the
//! loop's tool calls runner→subc→AFT over an authenticated consumer connection,
//! with ZERO loop changes (the loop only sees the trait).
//!
//! - `batch_started`: ensure a route is OPEN for the batch's session (cached +
//!   reused across batches — route.open is per (provider, session), not per
//!   batch).
//! - `dispatch`: FENCE by `tool_call_id` (a repeated id returns the prior
//!   result), else send the tool call on the session's route channel and map the
//!   CallToolResult → `ToolResult`. Bounded by FLOW CONTROL (below).
//! - `batch_finished`: evict the batch-local routing and fence entries; the session route stays
//!   open for reuse.
//!
//! FLOW CONTROL: subc enforces a per-route-channel credit window sized by the
//! provider's declared `Concurrency` (Serial=1, ModuleManaged=32,
//! StatelessParallel=1024); concurrent in-flight ABOVE the window gets an ERROR
//! frame (no queueing). The loop fires a batch concurrently, so the plane bounds
//! per-route in-flight to the window via a per-route semaphore (permit held from
//! send to terminal). A Serial provider therefore serializes instead of erroring.

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{
    tool_plane::{BatchCtx, DispatchResult, ToolPlane, ToolPlaneError},
    ToolManifest,
};
use broca_protocol::{
    BatchId, BindIdentity, RunId, SessionScope, ToolCall, ToolCallId, ToolOutput, ToolResult,
};
use futures::future::join_all;
use subc_control::CatalogEntry;
use subc_protocol::{
    error_codes::{
        is_retryable_route_open, SCOPE_CHANGED, SCOPE_NOT_LIVE, SCOPE_NOT_SYNCED, SCOPE_UNSUPPORTED,
    },
    manifest::{Concurrency, ProviderRole},
    scope::{ParentState, ScopeSelector, ScopeStamp, ScopeStatus, CAP_SCOPES_V1},
    Principal,
};
use tokio::sync::Semaphore;
use tokio::time::{sleep, Instant};
use tokio_util::sync::CancellationToken;

use crate::connection::{
    rejection_code, retry_route_open, retry_route_open_with_scope_hold,
    route_failure_is_known_dead, ConnError, RouteHandle, RouteOpenError, RouteOpenRetry, ScopeHold,
    SubcConnection, ROUTE_OPEN_RETRY_DEADLINE, ROUTE_OPEN_RETRY_INTERVAL,
};
use crate::scope_describe::{DescribedScope, ScopeDescriber};
use crate::tool_deadline::tool_reply_deadline;

/// How long a dispatch that was refused `module_reloading` waits for the module to come back.
///
/// THE DEFAULT 3s DOES NOT COVER A RESTART, and I shipped a fix that assumed it did. Measured
/// from the daemon's own logs: 16 AFT module_reloading episodes, 13 of them longer than 3s,
/// clustered at 15-30s -- which is the module's 30s drain window. With the 3s patience the
/// retry mostly ended in `NotSent`, so the model still got an error, just three seconds later.
///
/// 45s covers the 30s drain plus a respawn and warm-up, and stays well inside the 120s
/// default execution deadline a tool call runs under (some calls get longer; see `tool_deadline`). A restart that takes longer still surfaces as
/// `NotSent` -- a bounded wait, not an unbounded one, is the point. Session close still aborts
/// the wait immediately.
const RELOAD_REOPEN_PATIENCE: RouteOpenPatience = RouteOpenPatience {
    deadline: Duration::from_secs(45),
    // Backs off to 2s rather than polling every 100ms: at the flat interval a 45s wait is
    // ~450 route.open calls per waiting dispatch, the storm the default path's cap exists to
    // prevent.
    max_interval: Duration::from_secs(2),
};

/// How long a scoped route open holds while the daemon answers `scope_not_synced` (the scope's
/// owner has not synced since the daemon started) or `scope_changed` (the scope record changed
/// while the open was being committed). Both mean "not yet", never "no": nothing was sent, so
/// waiting is safe. Past this bound, or the caller's own deadline if that is sooner, the call
/// is refused `scope_unverifiable`, still with nothing dispatched.
///
/// PLACEHOLDER: 45 s is not a measurement. Replace it with the measured time a scope owner takes
/// to re-sync its scopes after a subc daemon restart, from the daemon's restart to the owner's
/// first accepted `scope.sync`.
pub const SCOPE_SYNC_HOLD: Duration = Duration::from_secs(45);

/// The widest wait between attempts while holding on `scope_not_synced`. At the ordinary
/// 100 ms interval a 45 s hold would be hundreds of route.open calls per waiting call; doubling
/// up to 2 s keeps it to a few dozen while still noticing a sync within two seconds.
const SCOPE_SYNC_HOLD_MAX_INTERVAL: Duration = Duration::from_secs(2);

/// Broca's refusal for a scoped call whose scope it could not confirm: the daemon still said
/// "not yet" when the hold ran out, the daemon could not be asked whether it admits scoped
/// routes, or the frozen owner is not a principal this build can name. Nothing was sent.
pub const SCOPE_UNVERIFIABLE: &str = "scope_unverifiable";

/// How many times a scoped open is retried when its stamp changes between the describes that
/// bracket it. A scope being rebound that often, that fast, cannot be pinned down; the call is
/// then refused `scope_unverifiable`.
const MAX_STAMP_BRACKET_ATTEMPTS: usize = 3;

/// `scope.describe` says the scope ended, or it answers for a different `scope_epoch` than the
/// route was opened under (a newer session reused the ref, so ours ended).
pub const SCOPE_ENDED: &str = "scope_ended";
/// `scope.describe` says the scope is not live and its owner has synced since the daemon started,
/// so the owner's full set no longer holds it: the scope is gone.
pub const SCOPE_NOT_LIVE_REASON: &str = SCOPE_NOT_LIVE;
/// `scope.describe` says the scope is live but its parent link has ended.
pub const SCOPE_PARENT_ENDED: &str = "scope_parent_ended";
/// The scope is live, but a route opened now would be stamped with a different agent (or a
/// different epoch) than the route the call was composed under, so the call is not re-sent.
pub const SCOPE_AGENT_CHANGED: &str = "scope_agent_changed";

/// Whether a scope reason means the scope, or its parent, is gone. When a route closes with
/// nothing in flight for one of these, the rest of the run's calls stop; any other close of a
/// live scope is ordinary route loss and the next call re-opens.
fn drain_ends_run(reason: &str) -> bool {
    matches!(
        reason,
        SCOPE_ENDED | SCOPE_NOT_LIVE_REASON | SCOPE_PARENT_ENDED
    )
}

/// What a scoped route's close means, read from a `scope.describe` answer as current state.
#[derive(Debug, Clone)]
enum ScopeReading {
    /// The scope or its parent is gone; the reason names which ([`drain_ends_run`] holds).
    Gone(&'static str),
    /// The scope is live; this is the stamp a route opened now would carry.
    Live(ScopeStamp),
    /// The owner has not synced since the daemon started: wait, as for `scope_not_synced` on
    /// open.
    NotSynced,
    /// The answer cannot settle it (no answer, or an owner that will never sync).
    Unverifiable(String),
}

/// Read one `scope.describe` answer for a scope a route was opened under at `opened_epoch`.
fn read_describe(opened_epoch: u64, answer: &DescribedScope) -> ScopeReading {
    match answer.status {
        ScopeStatus::Ended => ScopeReading::Gone(SCOPE_ENDED),
        ScopeStatus::NotLive if answer.owner_synced => ScopeReading::Gone(SCOPE_NOT_LIVE_REASON),
        ScopeStatus::NotLive if !answer.owner_configured => ScopeReading::Unverifiable(
            "the scope's owner is not a configured module, so it will never sync".into(),
        ),
        ScopeStatus::NotLive => ScopeReading::NotSynced,
        ScopeStatus::Live => {
            let Some(stamp) = &answer.stamp else {
                return ScopeReading::Unverifiable(
                    "a live scope was described with no stamp".into(),
                );
            };
            let epoch_moved = stamp.scope_epoch != opened_epoch
                || answer
                    .scope_epoch
                    .is_some_and(|epoch| epoch != opened_epoch);
            if epoch_moved {
                ScopeReading::Gone(SCOPE_ENDED)
            } else if stamp.parent_state == Some(ParentState::Ended) {
                ScopeReading::Gone(SCOPE_PARENT_ENDED)
            } else {
                ScopeReading::Live(stamp.clone())
            }
        }
    }
}

/// Whether a call composed under a route stamped `original` may be sent on a route stamped
/// `current`: the same `scope_epoch` and the same agent. Anything else the stamp carries (a
/// narrowed delegation, a parent link now confirmed) is for the provider to judge afresh.
fn may_resend_under(original: &ScopeStamp, current: &ScopeStamp) -> bool {
    original.scope_epoch == current.scope_epoch && same_agent(original, current)
}

/// Whether two stamps name the same agent. THE ONE PLACE broca reads a stamp's agent, and only
/// to compare two values for equality, to notice that a scope was rebound to another agent
/// under a route a call was composed for. Broca never logs, records, forwards or acts on the
/// value, and never reads the delegation flag. `scope_stamp_source.rs` holds the workspace to
/// this, allowing exactly this function.
fn same_agent(a: &ScopeStamp, b: &ScopeStamp) -> bool {
    a.attributes.agent_id == b.attributes.agent_id
}

/// How long a route open keeps retrying a temporary refusal, and how widely it spaces the
/// attempts. The default is exactly the behaviour before this type existed.
#[derive(Clone, Copy, Debug)]
struct RouteOpenPatience {
    deadline: Duration,
    max_interval: Duration,
}

const DEFAULT_ROUTE_OPEN_PATIENCE: RouteOpenPatience = RouteOpenPatience {
    deadline: ROUTE_OPEN_RETRY_DEADLINE,
    max_interval: ROUTE_OPEN_RETRY_INTERVAL,
};

/// subc's per-route credit window for a declared concurrency class.
fn window_for(c: &Concurrency) -> usize {
    match c {
        Concurrency::Serial => 1,
        Concurrency::ModuleManaged => 32,
        Concurrency::StatelessParallel => 1024,
    }
}

struct RouteState {
    handle: RouteHandle,
    /// Bounds concurrent in-flight on this route to the provider's window.
    sem: Arc<Semaphore>,
    /// For a route opened under a scope: the stamp the daemon bound it with, as `scope.describe`
    /// reported it identically just before and just after the open. `None` for an unscoped
    /// route.
    stamp: Option<ScopeStamp>,
}

enum RouteEnsureError {
    Connection(ConnError),
    Plane(ToolPlaneError),
}

impl RouteEnsureError {
    fn into_plane(self) -> ToolPlaneError {
        match self {
            Self::Connection(error) => conn_to_plane(error),
            Self::Plane(error) => error,
        }
    }
}

/// Use the raw bind fields as the route/fence identity. A formatted path string is not a sound
/// key because `PathBuf::display()` is lossy for non-UTF-8 paths and a separator-delimited
/// string can collide when one field contains the separator.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SessionKey {
    project_root: PathBuf,
    harness: String,
    session: String,
}

/// A scope's identity as a route key: the owner spelled as [`BindPrincipal::as_owner_string`]
/// does, the ref, and the epoch. `None` for an unscoped route.
///
/// [`BindPrincipal::as_owner_string`]: broca_protocol::BindPrincipal::as_owner_string
type ScopeKey = Option<(String, String, u64)>;

fn scope_key(scope: Option<&SessionScope>) -> ScopeKey {
    scope.map(|scope| {
        (
            scope.owner.as_owner_string(),
            scope.scope_ref.clone(),
            scope.scope_epoch,
        )
    })
}

/// A subc route is scoped by session identity, the scope it was opened under, and target
/// module. The scope is part of the key so a route opened under one scope, or none, is never
/// reused for a run admitted under another. The durability fence deliberately stays keyed only
/// by `(session, tool_call_id)` so a caller re-driving the same batch cannot duplicate a tool
/// call through another module route.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RouteKey {
    session: SessionKey,
    scope: ScopeKey,
    module_id: String,
}

/// What one run has learned that holds for the rest of its calls.
#[derive(Clone, Debug)]
enum RunScopeLatch {
    /// The daemon refused a route under the run's scope as `scope_not_live`: the rest of the
    /// run's calls are refused at once, without opening anything. A later run is judged afresh.
    Refused { code: String, detail: String },
    /// A scope drain that ends the run's calls (see [`RUN_ENDING_DRAINS`]) closed its route.
    Drained { reason: String },
}

impl RunScopeLatch {
    fn into_plane_error(self) -> ToolPlaneError {
        match self {
            RunScopeLatch::Refused { code, detail } => {
                ToolPlaneError::ScopeRefused { code, detail }
            }
            RunScopeLatch::Drained { reason } => ToolPlaneError::ScopeDrained { reason },
        }
    }
}

type RunKey = (SessionKey, RunId);

/// Per module of one scoped batch: the route it started with and that route's stamp.
type BatchComposedRoutes = HashMap<String, (RouteHandle, ScopeStamp)>;

#[derive(Clone, Debug, PartialEq, Eq)]
enum ToolRoute {
    Module(String),
    Failure(String),
}

impl ToolRoute {
    fn module(&self) -> Option<&str> {
        match self {
            ToolRoute::Module(module) => Some(module.as_str()),
            ToolRoute::Failure(_) => None,
        }
    }
}

/// Batch-local routes are grouped under the complete session and batch identity. Keeping calls
/// in an inner map lets batch start and finish replace or remove one batch without scanning routes
/// owned by unrelated sessions.
type BatchRouteKey = (SessionKey, BatchId);
type BatchRouteMap = HashMap<BatchRouteKey, HashMap<ToolCallId, ToolRoute>>;

/// The at-most-once fence key. Keyed by the SESSION as well as the tool_call_id, because a
/// tool_call_id is only unique WITHIN a run — two different sessions (or two runs) can mint
/// the same provider id, so keying by the id alone would cross-session false-dedup (return
/// one session's result for another's call). The session scopes it.
type FenceKey = (SessionKey, ToolCallId);

/// The at-most-once fence: a repeated `(session, tool_call_id)` returns the prior result,
/// EVICTED at batch terminal so it stays bounded to live batches (the at-most-once
/// guarantee only needs to hold within a batch's life — a resume re-dispatches an
/// un-recorded call within the same batch; the WAL is the cross-batch/cross-run durability,
/// not this in-memory map). Extracted so the keying (collision-safety) and the eviction
/// (bounded growth) are unit-testable without a live connection.
#[derive(Default)]
struct Fence {
    /// The deduped results, keyed by `(session, tool_call_id)`.
    results: HashMap<FenceKey, ToolResult>,
    /// The keys each in-flight batch inserted, so `evict_batch` removes exactly that
    /// batch's entries instead of letting the fence grow unbounded over many batches.
    by_batch: HashMap<BatchId, Vec<FenceKey>>,
}

impl Fence {
    /// The prior result for this key, if any (the at-most-once hit).
    fn get(&self, key: &FenceKey) -> Option<ToolResult> {
        self.results.get(key).cloned()
    }

    /// Record a result under its batch (so `evict_batch` can later drop it).
    fn insert(&mut self, batch_id: &BatchId, key: FenceKey, result: ToolResult) {
        self.results.insert(key.clone(), result);
        self.by_batch.entry(batch_id.clone()).or_default().push(key);
    }

    /// Evict every entry the batch inserted (called at `batch_finished`).
    fn evict_batch(&mut self, batch_id: &BatchId) {
        for key in self.by_batch.remove(batch_id).unwrap_or_default() {
            self.results.remove(&key);
        }
    }

    /// Total live fence entries (for the eviction test).
    #[cfg(test)]
    fn len(&self) -> usize {
        self.results.len()
    }
}

#[async_trait]
trait ToolPlaneConnection: Send + Sync {
    async fn catalog_list(&self) -> Result<Vec<CatalogEntry>, ConnError>;

    async fn route_open(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        scope: Option<&ScopeSelector>,
    ) -> Result<RouteHandle, ConnError>;

    async fn route_close(&self, route: &RouteHandle) -> Result<(), ConnError>;

    async fn tool_call(
        &self,
        route: &RouteHandle,
        name: &str,
        arguments: &serde_json::Value,
        tool_call_id: Option<&str>,
        deadline: Duration,
        preset: Option<&str>,
    ) -> Result<serde_json::Value, ConnError>;

    async fn tool_call_approved(
        &self,
        _route: &RouteHandle,
        _call: &ToolCall,
        _deadline: Duration,
        _preset: Option<&str>,
        _args: &ApprovedArgs,
    ) -> Result<serde_json::Value, crate::connection::ApprovedCallError> {
        Err(ConnError::Protocol("approved_dispatch_unavailable".into()).into())
    }

    /// The capability names the daemon advertises (`server.describe`).
    async fn server_capabilities(&self) -> Result<Vec<String>, ConnError>;

    /// Whether `route` is still open (no GOODBYE has closed it and its connection is current).
    fn route_is_open(&self, route: &RouteHandle) -> bool;
}

#[async_trait]
impl ToolPlaneConnection for SubcConnection {
    async fn catalog_list(&self) -> Result<Vec<CatalogEntry>, ConnError> {
        SubcConnection::catalog_list(self).await
    }

    async fn route_open(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        scope: Option<&ScopeSelector>,
    ) -> Result<RouteHandle, ConnError> {
        // One attempt: the plane's retry runs above this seam (`route_open_for_lifecycle`),
        // where its tests can script each attempt.
        SubcConnection::route_open_single_attempt(self, module_id, identity, scope).await
    }

    async fn tool_call_approved(
        &self,
        route: &RouteHandle,
        call: &ToolCall,
        deadline: Duration,
        preset: Option<&str>,
        args: &ApprovedArgs,
    ) -> Result<serde_json::Value, crate::connection::ApprovedCallError> {
        self.tool_call_raw_guarded(
            route,
            &call.tool_name,
            &args.bytes,
            Some(&call.tool_call_id.0),
            deadline,
            preset,
            Some(&args.key),
            None,
            &args.signals,
        )
        .await
    }

    async fn server_capabilities(&self) -> Result<Vec<String>, ConnError> {
        SubcConnection::server_capabilities(self).await
    }

    fn route_is_open(&self, route: &RouteHandle) -> bool {
        SubcConnection::route_is_open(self, route)
    }

    async fn route_close(&self, route: &RouteHandle) -> Result<(), ConnError> {
        SubcConnection::route_close(self, route).await
    }

    async fn tool_call(
        &self,
        route: &RouteHandle,
        name: &str,
        arguments: &serde_json::Value,
        tool_call_id: Option<&str>,
        deadline: Duration,
        preset: Option<&str>,
    ) -> Result<serde_json::Value, ConnError> {
        SubcConnection::tool_call_with_preset(
            self,
            route,
            name,
            arguments,
            tool_call_id,
            deadline,
            preset,
        )
        .await
    }
}

#[derive(Clone)]
struct ApprovedArgs {
    bytes: String,
    key: String,
    signals: broca_core::tool_plane::DispatchSignals,
}

enum SendAttempt {
    NotSent { reason: String },
    Reply(Result<serde_json::Value, ConnError>),
}

/// The real subc tool plane. A caller can hint a tool's target module, otherwise the
/// plane resolves a catalog-unique provider and finally falls back to the configured
/// default module (e.g. "aft").
///
/// SCALE NOTE (routes map growth): the served daemon builds ONE plane for the whole process
/// and shares it across every session, so the `routes` map spans sessions and is not reclaimed
/// by dropping it -- the process outlives every session in it. What bounds the map is the
/// explicit `close_session_routes` call on the session-close path, which removes that session's
/// entries by key. Treat that call as load-bearing rather than as tidy-up: without it the map
/// grows for the life of the daemon, and the shared consumer connection leaks a route channel
/// per closed session.
///
/// A single-session caller (a CLI that runs one session and exits) does get reclamation for
/// free from the drop, which is why the two shapes can share this type. Dead routes are
/// invalidated separately in both cases, so a stale channel is never reused.
///
/// Idle-based eviction is deliberately absent. Route lifetime is already bounded by session
/// close, and an idle sweeper could only add the risk of evicting a route belonging to a live
/// session that is merely between tool batches.
pub struct SubcToolPlane {
    conn: Arc<dyn ToolPlaneConnection>,
    default_module_id: Option<String>,
    routes: Mutex<HashMap<RouteKey, RouteState>>,
    batch_routes: Mutex<BatchRouteMap>,
    batch_presets: Mutex<HashMap<BatchRouteKey, ToolManifest>>,
    approved_args: Mutex<HashMap<BatchRouteKey, HashMap<ToolCallId, ApprovedArgs>>>,
    #[cfg(debug_assertions)]
    reply_fallback_ms: u64,
    /// Lives across batches for the plane's lifetime, but is reused only as the source snapshot
    /// for per-batch routing; resolved routes remain frozen and separately evicted at batch
    /// terminal. Module respawns preserve module IDs, so those routes remain meaningful while a
    /// provider is restarted. A missing provider triggers one fresh resolution fetch, and any
    /// learned-dead route clears this snapshot, covering the catalog changes that can affect a
    /// later batch without polling per batch.
    catalog_memo: Mutex<Option<Arc<Vec<CatalogEntry>>>>,
    /// The at-most-once fence (keyed by (session, tool_call_id), evicted at batch terminal).
    fence: Mutex<Fence>,
    /// Per-PROVIDER concurrency window, resolved from that provider's catalog entry at
    /// route-open (NOT a single global memo — different providers declare different
    /// windows). Cached per module_id so a second route to the same provider doesn't
    /// re-fetch the catalog.
    windows: Mutex<HashMap<String, usize>>,
    /// Per-session lifecycle cancel tokens: `close_session_routes` cancels (so in-flight
    /// `warm_bind` / late `route.open` completions never cache a route for a dead session)
    /// and REMOVES the entry, so a later session reopen (same `BindIdentity`, new actor
    /// generation) mints a fresh uncancelled token via `or_insert_with`.
    session_cancel: Mutex<HashMap<SessionKey, CancellationToken>>,
    /// For each scoped batch in progress, the route each of its modules had when the batch
    /// started, with that route's stamp: the identity the batch's calls were composed under. A
    /// close of one of these routes catches the batch's calls in flight, and the stamp decides
    /// whether a call not yet sent may be re-sent on a fresh route.
    batch_handles: Mutex<HashMap<BatchRouteKey, BatchComposedRoutes>>,
    /// What each scoped run has learned that holds for the rest of its calls. Dropped with the
    /// session's routes at session close.
    run_latches: Mutex<HashMap<RunKey, RunScopeLatch>>,
    /// How long to wait for a scope's owner to sync before the scope counts as unverifiable:
    /// [`SCOPE_SYNC_HOLD`], except where a test shortens it.
    scope_sync_hold: Duration,
    /// Reads a scope's current state (`scope.describe`) over broca's module connection, once the
    /// serve loop has registered one; see [`Self::set_scope_describer`].
    describer: Mutex<Option<Arc<dyn ScopeDescriber>>>,
}

impl SubcToolPlane {
    async fn send_call(
        &self,
        ctx: &BatchCtx,
        module: &str,
        route: &RouteHandle,
        call: &ToolCall,
        deadline: Duration,
    ) -> SendAttempt {
        let args = self
            .approved_args
            .lock()
            .unwrap()
            .get(&Self::batch_route_key(ctx))
            .and_then(|calls| calls.get(&call.tool_call_id))
            .cloned();
        let preset = self.frozen_preset(ctx, module);
        if let Some(args) = args {
            // This is after every route, permit and stale-route recovery wait.
            // Only approved calls carry run signals; ordinary dispatch is unchanged.
            if args.signals.interrupted() {
                return SendAttempt::NotSent {
                    reason: "approved_dispatch_stopped_before_send".into(),
                };
            }
            match self
                .conn
                .tool_call_approved(route, call, deadline, preset.as_deref(), &args)
                .await
            {
                Ok(reply) => SendAttempt::Reply(Ok(reply)),
                Err(crate::connection::ApprovedCallError::Connection(error)) => {
                    SendAttempt::Reply(Err(error))
                }
                Err(crate::connection::ApprovedCallError::NotSent { reason }) => {
                    SendAttempt::NotSent {
                        reason: reason.into(),
                    }
                }
            }
        } else {
            SendAttempt::Reply(
                self.conn
                    .tool_call(
                        route,
                        &call.tool_name,
                        &call.input,
                        Some(&call.tool_call_id.0),
                        deadline,
                        preset.as_deref(),
                    )
                    .await,
            )
        }
    }

    pub fn new(conn: SubcConnection, module_id: impl Into<String>) -> Self {
        Self::with_connection(Arc::new(conn), module_id)
    }

    fn with_connection(conn: Arc<dyn ToolPlaneConnection>, module_id: impl Into<String>) -> Self {
        let module_id = module_id.into();
        let default_module_id = (!module_id.trim().is_empty()).then_some(module_id);
        Self {
            conn,
            default_module_id,
            routes: Mutex::new(HashMap::new()),
            batch_routes: Mutex::new(HashMap::new()),
            batch_presets: Mutex::new(HashMap::new()),
            approved_args: Mutex::new(HashMap::new()),
            #[cfg(debug_assertions)]
            reply_fallback_ms: 120_000,
            catalog_memo: Mutex::new(None),
            fence: Mutex::new(Fence::default()),
            windows: Mutex::new(HashMap::new()),
            session_cancel: Mutex::new(HashMap::new()),
            batch_handles: Mutex::new(HashMap::new()),
            run_latches: Mutex::new(HashMap::new()),
            scope_sync_hold: SCOPE_SYNC_HOLD,
            describer: Mutex::new(None),
        }
    }

    /// Install what reads a scope's current state. `scope.describe` is answered only on a
    /// connection holding a module registration, so this is broca's module connection, which
    /// exists only once the serve loop has registered. Until then every scoped route open, and
    /// every question about a scoped route's close, answers "can't tell" (`scope_unverifiable`).
    pub fn set_scope_describer(&self, describer: Arc<dyn ScopeDescriber>) {
        *self.describer.lock().unwrap() = Some(describer);
    }

    /// Whether the route cached for `bind` under `scope` to `module_id` is still open; `None`
    /// when none is cached. Lets a test wait until the connection has seen the daemon close it.
    #[doc(hidden)]
    pub fn test_cached_route_is_open(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        module_id: &str,
    ) -> Option<bool> {
        let key = Self::route_key(bind, scope, module_id);
        let handle = self.routes.lock().unwrap().get(&key)?.handle;
        Some(self.conn.route_is_open(&handle))
    }

    /// Shorten the scope hold, so a test can run a hold past its bound in seconds.
    #[doc(hidden)]
    pub fn with_scope_sync_hold_for_test(mut self, hold: Duration) -> Self {
        self.scope_sync_hold = hold;
        self
    }

    /// Capture the lifecycle token for the actor generation that currently owns `bind`.
    /// Detached work must capture this before it is spawned so a late first poll cannot attach
    /// itself to a newer generation after the original session closes.
    pub fn capture_session_lifecycle_token(&self, bind: &BindIdentity) -> CancellationToken {
        let session = Self::session_key(bind);
        self.session_cancel
            .lock()
            .unwrap()
            .entry(session)
            .or_default()
            .clone()
    }

    #[doc(hidden)]
    pub fn test_mint_lifecycle_token_uncancelled(&self, bind: &BindIdentity) -> bool {
        !self.capture_session_lifecycle_token(bind).is_cancelled()
    }

    fn frozen_preset(&self, ctx: &BatchCtx, module_id: &str) -> Option<String> {
        self.batch_presets
            .lock()
            .unwrap()
            .get(&Self::batch_route_key(ctx))
            .and_then(|manifest| manifest.provider_preset(module_id).map(str::to_owned))
    }

    /// Override only this runner plane's wait in the test process, not the remote daemon.
    /// The setter and its storage are compiled out of release builds.
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn with_reply_fallback_for_test(mut self, fallback_ms: u64) -> Self {
        self.reply_fallback_ms = fallback_ms;
        self
    }

    fn reply_fallback_ms(&self) -> u64 {
        #[cfg(debug_assertions)]
        {
            self.reply_fallback_ms
        }
        #[cfg(not(debug_assertions))]
        {
            120_000
        }
    }

    fn reply_deadline(&self, ctx: &BatchCtx, module_id: &str, call: &ToolCall) -> Duration {
        let manifests = self.batch_presets.lock().unwrap();
        let reply = manifests
            .get(&Self::batch_route_key(ctx))
            .and_then(|manifest| manifest.provider_reply(module_id, &call.tool_name));
        if let Some(reply) = reply {
            return Duration::from_millis(
                cortexkit_role_tool_provider::catalog::reply_deadline_ms(
                    Some(reply),
                    &call.input,
                    self.reply_fallback_ms(),
                ),
            );
        }
        let legacy = tool_reply_deadline(module_id, &call.tool_name, &call.input);
        #[cfg(debug_assertions)]
        if legacy == crate::connection::TOOL_CALL_TIMEOUT {
            return Duration::from_millis(self.reply_fallback_ms());
        }
        legacy
    }

    /// The catalog providers + tools (so a caller can build the manifest from
    /// what's actually registered).
    pub async fn catalog(&self) -> Result<Vec<CatalogEntry>, ToolPlaneError> {
        self.conn.catalog_list().await.map_err(conn_to_plane)
    }

    async fn catalog_snapshot(&self) -> Result<Arc<Vec<CatalogEntry>>, ToolPlaneError> {
        if let Some(catalog) = self.catalog_memo.lock().unwrap().as_ref().cloned() {
            return Ok(catalog);
        }

        let fetched = Arc::new(self.conn.catalog_list().await.map_err(conn_to_plane)?);
        let mut memo = self.catalog_memo.lock().unwrap();
        Ok(Arc::clone(memo.get_or_insert(fetched)))
    }

    async fn refresh_catalog_snapshot(&self) -> Result<Arc<Vec<CatalogEntry>>, ToolPlaneError> {
        self.clear_catalog_memo();
        let fetched = Arc::new(self.conn.catalog_list().await.map_err(conn_to_plane)?);
        *self.catalog_memo.lock().unwrap() = Some(Arc::clone(&fetched));
        Ok(fetched)
    }

    fn clear_catalog_memo(&self) {
        self.catalog_memo.lock().unwrap().take();
    }

    /// Open (and cache) the route for `bind` ahead of the run, retrying until
    /// `timeout` elapses. The first `route.open` triggers AFT's `configure`, which
    /// cold-starts its index/callgraph (and can load a semantic model) — on a large
    /// project root that can exceed subc's internal per-bind timeout, surfacing as a
    /// rejected `route.open`. Warming the bind here (with retry) absorbs that cold
    /// start BEFORE the timed model loop, so a slow first-configure never looks like
    /// a loop stall. On a warm/tiny root this returns on the first try in
    /// milliseconds.
    pub async fn warm_bind(
        &self,
        bind: &BindIdentity,
        timeout: Duration,
    ) -> Result<(), ToolPlaneError> {
        let lifecycle = self.capture_session_lifecycle_token(bind);
        self.warm_bind_for_lifecycle(bind, None, timeout, lifecycle)
            .await
    }

    /// Warm a route on behalf of the generation represented by `lifecycle`, under `scope`: the
    /// scope the run the warm serves was admitted under, `None` for an unscoped run. The warmed
    /// route is cached under that scope, so only a run admitted under it uses the route.
    ///
    /// Callers that detach the returned work must capture the token before spawning. Reusing this
    /// exact token for both checks prevents an old task from minting a new generation after close.
    pub async fn warm_bind_for_lifecycle(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        timeout: Duration,
        lifecycle: CancellationToken,
    ) -> Result<(), ToolPlaneError> {
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;
        loop {
            if lifecycle.is_cancelled() {
                return Err(ToolPlaneError::Unavailable(
                    "session closed during warm_bind".into(),
                ));
            }
            attempt += 1;
            let e = match self
                .ensure_default_route_for_lifecycle(bind, scope, &lifecycle, Some(deadline))
                .await
            {
                Ok(_) => return Ok(()),
                // A scope refusal is already final: the route open has already held as long as
                // the scope hold allows, so retrying here would only hold again.
                Err(RouteEnsureError::Plane(error @ ToolPlaneError::ScopeRefused { .. })) => {
                    return Err(error)
                }
                Err(RouteEnsureError::Connection(error)) => {
                    if matches!(
                        &error,
                        ConnError::Rejected(message)
                            if !is_retryable_route_open(rejection_code(message))
                    ) {
                        return Err(conn_to_plane(error));
                    }
                    conn_to_plane(error)
                }
                Err(error) => error.into_plane(),
            };
            if Instant::now() >= deadline {
                return Err(ToolPlaneError::Unavailable(format!(
                    "route.bind did not succeed within {:?} ({attempt} attempts): {e}",
                    timeout
                )));
            }
            // Exponential backoff, capped. A flat retry interval turns a
            // DETERMINISTIC rejection (e.g. the tool module refusing the bind
            // identity) into a route.open storm against the daemon for the whole
            // timeout window; backing off caps a 30s window at ~8 attempts while
            // still retrying fast enough to absorb the transient case this warm
            // exists for (the tool module's slow first-configure).
            tokio::select! {
                _ = lifecycle.cancelled() => {
                    return Err(ToolPlaneError::Unavailable(
                        "session closed during warm_bind".into(),
                    ));
                }
                _ = sleep(warm_bind_backoff(attempt)) => {}
            }
        }
    }

    /// Build the loop's tool set from what the target module ACTUALLY registers in
    /// subc's catalog — so a harness discovers its tools from the live manifest
    /// rather than hand-authoring schemas that can drift from it.
    ///
    /// Two deliberate mappings, because the subc manifest and the loop's `Tool`
    /// describe different axes:
    ///  - the subc manifest `Tool` carries an OPTIONAL human `description`; it is
    ///    taken verbatim when the module declares one and left empty otherwise. A
    ///    caller that needs richer prompt descriptions still supplies its own tool
    ///    list (the caller list is authoritative over catalog discovery).
    ///  - subc's `execution_mode` is the SIDE-EFFECT class (pure/mutating/
    ///    unfenceable, used for durability fencing), NOT the loop's batch-scheduling
    ///    class (`ExecutionMode::Parallel`/`Sequential`). They don't translate, so
    ///    every catalog tool maps to the loop's default `Parallel` scheduling; the
    ///    side-effect class lives on the subc/AFT side, not in the `RunRequest`.
    pub async fn tools_from_catalog(
        &self,
    ) -> Result<Vec<broca_protocol::request::Tool>, ToolPlaneError> {
        use broca_protocol::request::{ExecutionMode, Tool};
        let module_id = self.default_module_id.as_deref().ok_or_else(|| {
            ToolPlaneError::Unavailable("no default tool module configured".to_string())
        })?;
        let catalog = self.conn.catalog_list().await.map_err(conn_to_plane)?;
        let entry = catalog
            .iter()
            .find(|e| e.module_id == module_id)
            .ok_or_else(|| {
                ToolPlaneError::Unavailable(format!(
                    "module '{}' not found in subc catalog",
                    module_id
                ))
            })?;
        let tools = entry
            .roles
            .iter()
            .filter_map(|r| match r {
                ProviderRole::ToolProvider { tools, .. } => Some(tools),
                _ => None,
            })
            .flatten()
            .map(|t| Tool {
                name: t.name.clone(),
                // The subc manifest now carries an optional human description; take it
                // when the module declares one (modules that don't still yield empty —
                // a caller-supplied tool list remains the authoritative prompt surface).
                description: t.description.clone().unwrap_or_default(),
                input_schema: t.schema.clone(),
                module: Some(module_id.to_owned()),
                execution_mode: ExecutionMode::Parallel,
            })
            .collect();
        Ok(tools)
    }

    fn session_key(bind: &BindIdentity) -> SessionKey {
        SessionKey {
            project_root: bind.project_root.clone(),
            harness: bind.harness.clone(),
            session: bind.session.clone(),
        }
    }

    fn route_key(bind: &BindIdentity, scope: Option<&SessionScope>, module_id: &str) -> RouteKey {
        RouteKey {
            session: Self::session_key(bind),
            scope: scope_key(scope),
            module_id: module_id.to_string(),
        }
    }

    fn batch_route_key(ctx: &BatchCtx) -> BatchRouteKey {
        (Self::session_key(&ctx.bind), ctx.batch_id.clone())
    }

    fn run_key(ctx: &BatchCtx) -> RunKey {
        (Self::session_key(&ctx.bind), ctx.run_id.clone())
    }

    fn run_latch(&self, ctx: &BatchCtx) -> Option<RunScopeLatch> {
        self.run_latches
            .lock()
            .unwrap()
            .get(&Self::run_key(ctx))
            .cloned()
    }

    /// Remember what holds for the rest of `ctx`'s run. The first latch wins: once a run's
    /// calls are refused or ended, a later observation does not change the answer.
    fn latch_run(&self, ctx: &BatchCtx, latch: RunScopeLatch) {
        self.run_latches
            .lock()
            .unwrap()
            .entry(Self::run_key(ctx))
            .or_insert(latch);
    }

    /// Latch a route-open refusal that holds for the rest of the run (`scope_not_live`).
    fn latch_open_refusal(&self, ctx: &BatchCtx, error: &ToolPlaneError) {
        if let ToolPlaneError::ScopeRefused { code, detail } = error {
            if code == SCOPE_NOT_LIVE {
                self.latch_run(
                    ctx,
                    RunScopeLatch::Refused {
                        code: code.clone(),
                        detail: detail.clone(),
                    },
                );
            }
        }
    }

    /// Forget `handle`, `ctx`'s route to `module_id`, if it is still the cached one. The daemon
    /// has closed it (or is draining it), so no GOODBYE is sent.
    fn forget_route(&self, ctx: &BatchCtx, module_id: &str, handle: RouteHandle) {
        let key = Self::route_key(&ctx.bind, ctx.scope.as_ref(), module_id);
        let mut routes = self.routes.lock().unwrap();
        if routes.get(&key).is_some_and(|route| route.handle == handle) {
            routes.remove(&key);
        }
    }

    /// Called at batch start, when no call of `ctx`'s run is in flight. If a route the session
    /// had cached under the run's scope has closed since the last batch, ask the daemon for the
    /// scope's current state (`scope.describe`); the close itself says nothing about why.
    /// - The scope or its parent is gone: the rest of the run's calls stop. The reason is
    ///   returned and latched for the run.
    /// - The scope is live, or its state cannot be told: ordinary route loss. The closed routes
    ///   are forgotten, the batch re-opens, and that open checks the scope again.
    async fn gone_since_last_batch(&self, ctx: &BatchCtx) -> Option<String> {
        let scope = ctx.scope.as_ref()?;
        let session = Self::session_key(&ctx.bind);
        let key_scope = scope_key(Some(scope));
        let cached: Vec<(String, RouteHandle)> = self
            .routes
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.session == session && key.scope == key_scope)
            .map(|(key, route)| (key.module_id.clone(), route.handle))
            .collect();
        let closed: Vec<(String, RouteHandle)> = cached
            .into_iter()
            .filter(|(_, handle)| !self.conn.route_is_open(handle))
            .collect();
        if closed.is_empty() {
            return None;
        }
        for (module_id, handle) in &closed {
            self.forget_route(ctx, module_id, *handle);
        }
        let lifecycle = self.capture_session_lifecycle_token(&ctx.bind);
        match self.read_scope(scope, &lifecycle, true).await {
            ScopeReading::Gone(reason) => {
                self.latch_gone(ctx, reason);
                Some(reason.to_owned())
            }
            ScopeReading::Live(_) | ScopeReading::NotSynced | ScopeReading::Unverifiable(_) => None,
        }
    }

    /// Latch `ctx`'s run to stop at its next batch when `reason` says the scope or its parent is
    /// gone.
    fn latch_gone(&self, ctx: &BatchCtx, reason: &str) {
        if drain_ends_run(reason) {
            self.latch_run(
                ctx,
                RunScopeLatch::Drained {
                    reason: reason.to_owned(),
                },
            );
        }
    }

    /// The route to `module_id` recorded when `ctx`'s batch started, with the stamp the batch's
    /// calls were composed under. Only a scoped batch records its routes, so an unscoped batch
    /// always gets `None`.
    fn batch_route(&self, ctx: &BatchCtx, module_id: &str) -> Option<(RouteHandle, ScopeStamp)> {
        self.batch_handles
            .lock()
            .unwrap()
            .get(&Self::batch_route_key(ctx))
            .and_then(|routes| routes.get(module_id).cloned())
    }

    /// The stamp recorded for the cached route `handle` of `bind` under `scope` to `module_id`.
    fn cached_stamp(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        module_id: &str,
        handle: RouteHandle,
    ) -> Option<ScopeStamp> {
        let key = Self::route_key(bind, scope, module_id);
        self.routes
            .lock()
            .unwrap()
            .get(&key)
            .filter(|route| route.handle == handle)
            .and_then(|route| route.stamp.clone())
    }

    /// Ask the daemon, over broca's module connection, what state `scope` is in now, read
    /// against the epoch it was frozen at. With `hold`, an owner that has not synced since the
    /// daemon started is waited for, with backoff, inside [`SCOPE_SYNC_HOLD`], as a
    /// `scope_not_synced` open is; past the hold, or without `hold`, that is "can't tell".
    /// A session close ends the wait at once.
    async fn read_scope(
        &self,
        scope: &SessionScope,
        lifecycle: &CancellationToken,
        hold: bool,
    ) -> ScopeReading {
        let Some(describer) = self.describer.lock().unwrap().clone() else {
            return ScopeReading::Unverifiable(
                "broca has no module connection to ask the daemon about the scope".into(),
            );
        };
        let owner = match owner_principal(scope) {
            Ok(owner) => owner,
            Err(detail) => return ScopeReading::Unverifiable(detail),
        };
        let deadline = Instant::now() + self.scope_sync_hold;
        let mut attempt = 0u32;
        loop {
            let reading = match describer.describe(&owner, &scope.scope_ref).await {
                Ok(answer) => read_describe(scope.scope_epoch, &answer),
                Err(error) => ScopeReading::Unverifiable(format!("scope.describe failed: {error}")),
            };
            if !matches!(reading, ScopeReading::NotSynced) {
                return reading;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !hold || remaining.is_zero() {
                return ScopeReading::Unverifiable(
                    "the scope's owner has not synced since the daemon started".into(),
                );
            }
            attempt += 1;
            let wait = ROUTE_OPEN_RETRY_INTERVAL
                .saturating_mul(1 << attempt.min(5))
                .min(SCOPE_SYNC_HOLD_MAX_INTERVAL)
                .min(remaining);
            tokio::select! {
                _ = lifecycle.cancelled() => {
                    return ScopeReading::Unverifiable("the session closed during the hold".into());
                }
                _ = sleep(wait) => {}
            }
        }
    }

    /// What to do with a call in flight but not yet sent whose route, `closed`, is gone or
    /// unusable (closed by the daemon, refused as stale, or draining for a module reload).
    ///
    /// The scope's CURRENT state decides, read with `scope.describe` when the route closes. The
    /// daemon's `route.closed` push is not used: it names a module and a reason but not the
    /// route, so it cannot say which of several routes to one module closed for which reason.
    /// - the scope or its parent is gone: `route_drained` naming what describe said, never
    ///   re-dispatched, and the run stops at its next batch;
    /// - it cannot be told: `route_drained` as `scope_unverifiable`, never re-sent, because a
    ///   re-send needs a positive match;
    /// - the scope is live: re-open, and send on the new route only if its stamp has the same
    ///   `scope_epoch` and the same agent as `composed_under`, the stamp of the route the call
    ///   was composed under. Otherwise `route_drained` as `scope_agent_changed`, or as the
    ///   refusal the re-open met.
    #[allow(clippy::too_many_arguments)]
    async fn resend_or_drain(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        module_id: &str,
        closed: RouteHandle,
        composed_under: Option<ScopeStamp>,
        patience: RouteOpenPatience,
        fence_key: FenceKey,
    ) -> DispatchResult {
        let Some(scope) = ctx.scope.as_ref() else {
            unreachable!("only a scoped call is resolved by describing its scope");
        };
        self.remove_batch_call_route(ctx, &call.tool_call_id);
        self.forget_route(ctx, module_id, closed);
        let drained = |reason: &str| DispatchResult::ScopeDrained {
            reason: reason.to_owned(),
            sent: false,
        };
        let lifecycle = self.capture_session_lifecycle_token(&ctx.bind);
        match self.read_scope(scope, &lifecycle, true).await {
            ScopeReading::Gone(reason) => {
                self.latch_gone(ctx, reason);
                return drained(reason);
            }
            ScopeReading::Unverifiable(_) | ScopeReading::NotSynced => {
                return drained(SCOPE_UNVERIFIABLE)
            }
            ScopeReading::Live(_) => {}
        }
        let Some(composed_under) = composed_under else {
            return drained(SCOPE_UNVERIFIABLE);
        };
        let (fresh, sem) = match self
            .ensure_route_for_lifecycle_inner(
                &ctx.bind,
                Some(scope),
                module_id,
                &lifecycle,
                None,
                patience,
            )
            .await
            .map_err(RouteEnsureError::into_plane)
        {
            Ok(route) => route,
            Err(error @ ToolPlaneError::ScopeRefused { .. }) => {
                self.latch_open_refusal(ctx, &error);
                let ToolPlaneError::ScopeRefused { code, .. } = error else {
                    unreachable!()
                };
                self.latch_gone(ctx, &code);
                return drained(&code);
            }
            Err(error) => {
                return DispatchResult::NotSent {
                    reason: format!("route re-open after the route closed failed: {error}"),
                }
            }
        };
        let fresh_stamp = self.cached_stamp(&ctx.bind, Some(scope), module_id, fresh);
        if !fresh_stamp
            .as_ref()
            .is_some_and(|current| may_resend_under(&composed_under, current))
        {
            return drained(SCOPE_AGENT_CHANGED);
        }
        let _permit = match sem.acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                return DispatchResult::NotSent {
                    reason: "route semaphore closed".into(),
                }
            }
        };
        let reply_deadline = self.reply_deadline(ctx, module_id, call);
        let outcome = match self
            .send_call(ctx, module_id, &fresh, call, reply_deadline)
            .await
        {
            SendAttempt::NotSent { reason } => return DispatchResult::NotSent { reason },
            SendAttempt::Reply(reply) => reply,
        };
        self.finish_dispatch(
            ctx,
            call,
            module_id,
            fence_key,
            fresh,
            Some(composed_under),
            outcome,
        )
        .await
    }

    /// Turn a sent call's reply into its dispatch result. A definite answer is also recorded in
    /// the at-most-once fence, so a repeat of the same call id returns it without re-sending.
    #[allow(clippy::too_many_arguments)]
    async fn finish_dispatch(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        module_id: &str,
        fence_key: FenceKey,
        sent_on: RouteHandle,
        composed_under: Option<ScopeStamp>,
        outcome: Result<serde_json::Value, ConnError>,
    ) -> DispatchResult {
        let scope = ctx.scope.as_ref();
        match outcome {
            Ok(raw) => {
                let result = map_call_tool_result(&call.tool_call_id, &raw);
                // Build the fence copy before taking the process-wide guard. Tool payloads can be
                // large, while the critical section only needs to update the bounded maps.
                let fenced_result = result.clone();
                {
                    let mut fence = self.fence.lock().unwrap();
                    fence.insert(&ctx.batch_id, fence_key, fenced_result);
                }
                DispatchResult::Completed(result)
            }
            Err(ConnError::Rejected(msg)) => {
                if crate::connection::classify_tool_route_refusal(&msg)
                    == crate::connection::ToolRouteRefusal::OutcomeUnknown
                {
                    self.remove_batch_call_route(ctx, &call.tool_call_id);
                    let _ = self.invalidate_route_for_module(&ctx.bind, scope, module_id);
                    return DispatchResult::SentButOutcomeUnknown { reason: msg };
                }
                if crate::connection::classify_tool_route_refusal(&msg)
                    == crate::connection::ToolRouteRefusal::Reopen
                {
                    // The transparent retry also lost its route. Keep the model-visible error,
                    // but discard the known-dead replacement so the next call opens afresh.
                    self.remove_batch_call_route(ctx, &call.tool_call_id);
                    let _ = self.invalidate_route_for_module(&ctx.bind, scope, module_id);
                }
                // A provider that received the call and refuses it with `scope_not_synced` (Plexus
                // does this on its bot-write legs until the owner has synced) lands here like any
                // other refusal: recorded as this call's error result, and NOT re-sent. The
                // provider already has the request, so re-sending it safely needs the durable
                // cross-party call identity (`call_key`) that lets the provider resume the same
                // request instead of starting a second one; broca does not send one yet. Only a
                // route.open refusal, where nothing was sent, is held and retried. A test
                // (`a_provider_scope_not_synced_refusal_is_recorded_and_never_resent`) pins this.
                let result = ToolResult {
                    tool_call_id: call.tool_call_id.clone(),
                    output: ToolOutput::Error {
                        message: format!("tool/route rejected: {msg}"),
                    },
                    is_error: true,
                };
                self.fence
                    .lock()
                    .unwrap()
                    .insert(&ctx.batch_id, fence_key, result.clone());
                DispatchResult::Completed(result)
            }
            Err(ConnError::StaleRouteHandle(_)) => {
                // This can only be the replacement route after the first stale-route retry.
                // It is already absent from the connection's live epoch map, so discard it.
                self.remove_batch_call_route(ctx, &call.tool_call_id);
                let _ = self.invalidate_route_for_module(&ctx.bind, scope, module_id);
                DispatchResult::NotSent {
                    reason: "route handle became stale before send".into(),
                }
            }
            Err(
                e @ (ConnError::Transport(_)
                | ConnError::Protocol(_)
                | ConnError::StreamLagged { .. }),
            ) => {
                // The channel may be reset/closed — INVALIDATE the cached route so the next
                // dispatch re-opens a fresh one, instead of dispatching to a stale dead
                // channel. (The outcome is still ambiguous → SentButOutcomeUnknown stops the
                // run for review; the invalidation is so a future dispatch isn't poisoned.)
                self.remove_batch_call_route(ctx, &call.tool_call_id);
                // The request was sent, and its scoped route closed while it was in flight. The
                // outcome is unknown whatever the scope's state. Asking the daemon about the
                // scope only names why, for the record, and stops the run's later batches when
                // the scope or its parent is gone.
                if let Some(drained) = self
                    .sent_call_route_closed(ctx, module_id, sent_on, composed_under.as_ref())
                    .await
                {
                    return drained;
                }
                let known_dead = route_failure_is_known_dead(&e);
                if let Some(handle) = self.invalidate_route_for_module(&ctx.bind, scope, module_id)
                {
                    if !known_dead {
                        // A timeout/protocol failure can leave the daemon route alive. Close it
                        // off-task so cleanup never delays the ambiguous dispatch outcome.
                        self.close_route_best_effort(handle);
                    }
                }
                DispatchResult::SentButOutcomeUnknown {
                    reason: e.to_string(),
                }
            }
        }
    }

    /// For a scoped call that was sent when its route closed under it: the result naming why,
    /// from the scope's current state. `None` when the route is still open (a timeout, not a
    /// close) or the call is unscoped, leaving the ordinary unknown-outcome handling.
    async fn sent_call_route_closed(
        &self,
        ctx: &BatchCtx,
        module_id: &str,
        sent_on: RouteHandle,
        composed_under: Option<&ScopeStamp>,
    ) -> Option<DispatchResult> {
        let scope = ctx.scope.as_ref()?;
        if self.conn.route_is_open(&sent_on) {
            return None;
        }
        let lifecycle = self.capture_session_lifecycle_token(&ctx.bind);
        // Do not wait for an unsynced owner here: the call's outcome is already decided, and
        // waiting would only delay reporting it.
        let reason = match self.read_scope(scope, &lifecycle, false).await {
            ScopeReading::Gone(reason) => {
                self.latch_gone(ctx, reason);
                reason
            }
            ScopeReading::Live(current)
                if composed_under.is_some_and(|original| !may_resend_under(original, &current)) =>
            {
                SCOPE_AGENT_CHANGED
            }
            ScopeReading::Live(_) | ScopeReading::NotSynced | ScopeReading::Unverifiable(_) => {
                return None
            }
        };
        self.forget_route(ctx, module_id, sent_on);
        Some(DispatchResult::ScopeDrained {
            reason: reason.to_owned(),
            sent: true,
        })
    }

    /// The selector a scoped route opens under, after checking the daemon admits scoped routes
    /// at all. A daemon without `scopes/v1` would drop the field and open an unscoped route,
    /// so the call is refused instead and nothing is opened.
    async fn scoped_selector(
        &self,
        scope: &SessionScope,
    ) -> Result<ScopeSelector, RouteEnsureError> {
        match self.conn.server_capabilities().await {
            Ok(capabilities) if capabilities.iter().any(|c| c == CAP_SCOPES_V1) => {}
            Ok(_) => {
                return Err(RouteEnsureError::Plane(ToolPlaneError::ScopeRefused {
                    code: SCOPE_UNSUPPORTED.into(),
                    detail: format!(
                        "the subc daemon does not advertise {CAP_SCOPES_V1}, so no route is \
                         opened for this scoped session (an unscoped one would drop its scope)"
                    ),
                }))
            }
            Err(error) => {
                return Err(RouteEnsureError::Plane(ToolPlaneError::ScopeRefused {
                    code: SCOPE_UNVERIFIABLE.into(),
                    detail: format!(
                        "could not ask the subc daemon whether it admits scoped routes: {error}"
                    ),
                }))
            }
        }
        let owner = owner_principal(scope).map_err(|detail| {
            RouteEnsureError::Plane(ToolPlaneError::ScopeRefused {
                code: SCOPE_UNVERIFIABLE.into(),
                detail,
            })
        })?;
        Ok(ScopeSelector {
            owner,
            scope_ref: scope.scope_ref.clone(),
            // Always the frozen epoch, so a call can never be carried into a newer session that
            // reused the ref.
            scope_epoch: Some(scope.scope_epoch),
        })
    }

    fn remove_batch_call_route(&self, ctx: &BatchCtx, tool_call_id: &ToolCallId) {
        let key = Self::batch_route_key(ctx);
        let mut batches = self.batch_routes.lock().unwrap();
        let remove_batch = batches.get_mut(&key).is_some_and(|routes| {
            routes.remove(tool_call_id);
            routes.is_empty()
        });
        if remove_batch {
            batches.remove(&key);
        }
    }

    fn remove_session_module_batch_routes(&self, bind: &BindIdentity, module_id: &str) {
        let session = Self::session_key(bind);
        let mut batches = self.batch_routes.lock().unwrap();
        batches.retain(|(owner, _), routes| {
            if owner == &session {
                routes.retain(|_, route| route.module() != Some(module_id));
            }
            !routes.is_empty()
        });
    }

    fn route_is_current(&self, key: &RouteKey, handle: RouteHandle, sem: &Arc<Semaphore>) -> bool {
        let routes = self.routes.lock().unwrap();
        route_matches(&routes, key, handle, sem)
    }

    /// Resolve the window for a SPECIFIC provider (`module_id`) from its catalog entry,
    /// cached per provider so a second route to the same provider doesn't re-fetch the
    /// catalog. Resolved per-route from that route's provider — NOT a single global memo
    /// (different providers declare different windows; a global memo would apply the first
    /// provider's window to every route). Defaults to Serial(1) if the provider/role can't
    /// be read — the safe choice (never allow more concurrent in-flight calls than the
    /// provider declares).
    async fn resolve_window(&self, module_id: &str) -> Result<usize, ToolPlaneError> {
        if let Some(w) = self.windows.lock().unwrap().get(module_id).copied() {
            return Ok(w);
        }
        let catalog = self.conn.catalog_list().await.map_err(conn_to_plane)?;
        let window = catalog
            .iter()
            .find(|e| e.module_id == module_id)
            .and_then(provider_concurrency)
            .map(|c| window_for(&c))
            .unwrap_or(1);
        self.windows
            .lock()
            .unwrap()
            .insert(module_id.to_string(), window);
        Ok(window)
    }

    /// Ensure a route + its flow-control semaphore exist for `bind`; return the
    /// route channel and a shared semaphore handle.
    async fn ensure_route(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        module_id: &str,
    ) -> Result<(RouteHandle, Arc<Semaphore>), ToolPlaneError> {
        let lifecycle = self.capture_session_lifecycle_token(bind);
        self.ensure_route_for_lifecycle_inner(
            bind,
            scope,
            module_id,
            &lifecycle,
            None,
            DEFAULT_ROUTE_OPEN_PATIENCE,
        )
        .await
        .map_err(RouteEnsureError::into_plane)
    }

    /// Open a route to `module_id` for `bind`, under `scope` when the run was admitted under
    /// one. This is the only place a tool route is opened, so it is where a scoped run's route
    /// is guaranteed to carry its scope: a scoped open that cannot carry it is refused, never
    /// sent unscoped.
    async fn route_open_for_lifecycle(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        module_id: &str,
        lifecycle: &CancellationToken,
        retry_deadline: Option<Instant>,
        patience: RouteOpenPatience,
    ) -> Result<(RouteHandle, Option<ScopeStamp>), RouteEnsureError> {
        // One computed deadline keeps the caller budget authoritative while retaining a short
        // per-iteration cap. Using either bound alone would either overshoot a short warm timeout
        // or let a long timeout turn the flat retry interval into a route.open storm before the
        // outer exponential backoff can resume.
        let cap = Instant::now() + patience.deadline;
        let deadline = retry_deadline.map_or(cap, |caller| caller.min(cap));
        // The retry loop is the connection layer's, shared with every other route-open
        // consumer; this plane supplies only the policy. Under DEFAULT_ROUTE_OPEN_PATIENCE the
        // maximum wait equals the initial 100ms wait, so the interval never grows.
        let retry = RouteOpenRetry {
            deadline,
            max_interval: patience.max_interval,
        };
        let cancelled = || {
            RouteEnsureError::Plane(ToolPlaneError::Unavailable(
                "session closed during route open".into(),
            ))
        };
        let Some(scope) = scope else {
            return retry_route_open(retry, Some(lifecycle), || {
                self.conn.route_open(module_id, bind, None)
            })
            .await
            .map(|handle| (handle, None))
            .map_err(|error| match error {
                RouteOpenError::Conn(error) => RouteEnsureError::Connection(error),
                RouteOpenError::Cancelled => cancelled(),
            });
        };
        let selector = self.scoped_selector(scope).await?;
        // The hold is bounded by the scope hold and by the caller's own deadline, whichever
        // comes first. Inside a run there is no caller deadline, so the hold alone bounds it; a
        // session close ends it at once through `lifecycle`.
        let hold_cap = Instant::now() + self.scope_sync_hold;
        let hold = ScopeHold {
            deadline: retry_deadline.map_or(hold_cap, |caller| caller.min(hold_cap)),
            max_interval: SCOPE_SYNC_HOLD_MAX_INTERVAL,
        };
        let refused = |code: &str, detail: String| {
            RouteEnsureError::Plane(ToolPlaneError::ScopeRefused {
                code: code.to_owned(),
                detail,
            })
        };
        // The route's stamp is learned by bracketing the open with two describes: the reply to
        // route.open carries no stamp, and a single describe after it could see a stamp the
        // route was not bound under. Only a pair that agrees on epoch and agent is recorded; a
        // pair that disagrees means the route may have been bound under either, so it is
        // closed and opened again.
        for _ in 0..MAX_STAMP_BRACKET_ATTEMPTS {
            let before = match self.read_scope(scope, lifecycle, true).await {
                ScopeReading::Live(stamp) => stamp,
                ScopeReading::Gone(reason) => {
                    return Err(refused(
                        reason,
                        format!("scope.describe says the scope is gone ({reason})"),
                    ))
                }
                ScopeReading::NotSynced => {
                    return Err(refused(
                        SCOPE_UNVERIFIABLE,
                        "the scope's owner has not synced since the daemon started".into(),
                    ))
                }
                ScopeReading::Unverifiable(detail) => {
                    return Err(refused(
                        SCOPE_UNVERIFIABLE,
                        format!("could not confirm the scope before the open: {detail}"),
                    ))
                }
            };
            let handle =
                retry_route_open_with_scope_hold(retry, Some(hold), Some(lifecycle), || {
                    self.conn.route_open(module_id, bind, Some(&selector))
                })
                .await
                .map_err(|error| match error {
                    RouteOpenError::Conn(ConnError::Rejected(message)) => {
                        scoped_open_refusal(message, self.scope_sync_hold)
                    }
                    RouteOpenError::Conn(error) => RouteEnsureError::Connection(error),
                    RouteOpenError::Cancelled => cancelled(),
                })?;
            match self.read_scope(scope, lifecycle, true).await {
                ScopeReading::Live(after) if may_resend_under(&before, &after) => {
                    return Ok((handle, Some(after)));
                }
                ScopeReading::Live(_) => {
                    // The scope's stamp changed while the route was opening, so the route may
                    // be bound under either the earlier or the later stamp. Close it and retry.
                    let _ = self.conn.route_close(&handle).await;
                }
                ScopeReading::Gone(reason) => {
                    let _ = self.conn.route_close(&handle).await;
                    return Err(refused(
                        reason,
                        format!("scope.describe says the scope is gone ({reason})"),
                    ));
                }
                ScopeReading::NotSynced => {
                    let _ = self.conn.route_close(&handle).await;
                    return Err(refused(
                        SCOPE_UNVERIFIABLE,
                        "the scope's owner has not synced since the daemon started".into(),
                    ));
                }
                ScopeReading::Unverifiable(detail) => {
                    let _ = self.conn.route_close(&handle).await;
                    return Err(refused(
                        SCOPE_UNVERIFIABLE,
                        format!("could not confirm the scope after the open: {detail}"),
                    ));
                }
            }
        }
        Err(refused(
            SCOPE_UNVERIFIABLE,
            format!(
                "the scope's stamp changed during each of {MAX_STAMP_BRACKET_ATTEMPTS} opens, so \
                 no route's stamp could be confirmed"
            ),
        ))
    }

    async fn ensure_route_for_lifecycle_inner(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        module_id: &str,
        lifecycle: &CancellationToken,
        retry_deadline: Option<Instant>,
        patience: RouteOpenPatience,
    ) -> Result<(RouteHandle, Arc<Semaphore>), RouteEnsureError> {
        let key = Self::route_key(bind, scope, module_id);
        if lifecycle.is_cancelled() {
            return Err(RouteEnsureError::Plane(ToolPlaneError::Unavailable(
                "session closed before route open".into(),
            )));
        }
        // Deliberately no in-flight placeholder: two concurrent misses on one key both open
        // a route, and `cache_route_state` keeps one and closes the loser. The wasteful case
        // is the detached warm bind racing the first tool dispatch, and dispatch cannot reach
        // here until a whole model response has completed -- measured across 152 tool-bearing
        // production runs, the fastest first response was 3.3s and the median 103s, against a
        // route open that finishes far sooner. A singleflight would coalesce a window this
        // deployment does not enter, at the cost of an in-flight state that has to be unwound
        // correctly on failure, lifecycle cancellation, and epoch replacement.
        if let Some(r) = self.routes.lock().unwrap().get(&key) {
            return Ok((r.handle, Arc::clone(&r.sem)));
        }
        // Resolve the window from THIS route's provider at open time (per-provider, cached
        // per module_id) — not a single global memo.
        let window = self
            .resolve_window(module_id)
            .await
            .map_err(RouteEnsureError::Plane)?;
        let (handle, stamp) = self
            .route_open_for_lifecycle(bind, scope, module_id, lifecycle, retry_deadline, patience)
            .await?;
        let sem = Arc::new(Semaphore::new(window));
        let cached = {
            let mut routes = self.routes.lock().unwrap();
            cache_route_state(&mut routes, key.clone(), handle, Arc::clone(&sem), stamp)
        };

        // If another task won the race to cache the session route, close the redundant daemon
        // channel we just opened. The lock is already released here, so the best-effort close
        // does not block unrelated route lookups.
        if let Some(loser_handle) = cached.loser_handle {
            let _ = self.conn.route_close(&loser_handle).await;
        }

        if lifecycle.is_cancelled() {
            if let Some(loser) = cached.loser_handle {
                let _ = self.conn.route_close(&loser).await;
            } else {
                self.routes.lock().unwrap().remove(&key);
                let _ = self.conn.route_close(&cached.handle).await;
            }
            return Err(RouteEnsureError::Plane(ToolPlaneError::Unavailable(
                "session closed during route open".into(),
            )));
        }

        Ok((cached.handle, cached.sem))
    }

    async fn ensure_default_route_for_lifecycle(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        lifecycle: &CancellationToken,
        retry_deadline: Option<Instant>,
    ) -> Result<(RouteHandle, Arc<Semaphore>), RouteEnsureError> {
        let module_id = self.default_module_id.as_deref().ok_or_else(|| {
            RouteEnsureError::Plane(ToolPlaneError::Unavailable(
                "no default tool module configured".to_string(),
            ))
        })?;
        self.ensure_route_for_lifecycle_inner(
            bind,
            scope,
            module_id,
            lifecycle,
            retry_deadline,
            DEFAULT_ROUTE_OPEN_PATIENCE,
        )
        .await
    }

    /// Forget the cached route for `bind` so the NEXT dispatch re-opens a fresh channel.
    /// Called when a route is learned-dead — a transport/protocol dispatch failure (the
    /// channel may be reset/closed) or a GOODBYE for the route. WITHOUT this, the plane
    /// would keep dispatching to a stale dead channel forever. (Public so a GOODBYE handler
    /// can invalidate explicitly; the dispatch path invalidates on a transport error.)
    pub fn invalidate_route(&self, bind: &BindIdentity) {
        if let Some(module_id) = self.default_module_id.as_deref() {
            // This entry point is for an externally observed route teardown, so the daemon has
            // already released the channel and no second GOODBYE should be sent. Frozen decisions
            // targeting that dead module are removed from only this session's inner batch maps.
            self.remove_session_module_batch_routes(bind, module_id);
            // Drop the session's routes to this module under every scope: the teardown names
            // the session and module, not a scope.
            let session = Self::session_key(bind);
            self.routes
                .lock()
                .unwrap()
                .retain(|key, _| !(key.session == session && key.module_id == module_id));
        }
        self.clear_catalog_memo();
    }

    fn invalidate_route_for_module(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        module_id: &str,
    ) -> Option<RouteHandle> {
        let key = Self::route_key(bind, scope, module_id);
        let removed = self
            .routes
            .lock()
            .unwrap()
            .remove(&key)
            .map(|route| route.handle);
        // A dead route usually means a module restarted or disappeared. The dispatch or external
        // invalidation path removes affected batch-local decisions separately; the next batch must
        // discover any changed providers rather than inheriting this catalog snapshot.
        self.clear_catalog_memo();
        removed
    }

    fn close_route_best_effort(&self, handle: RouteHandle) {
        let conn = Arc::clone(&self.conn);
        tokio::spawn(async move {
            let _ = conn.route_close(&handle).await;
        });
    }

    /// The cached route channel for `bind`, or `None` if no route is cached (so a test can
    /// observe a route being opened, invalidated, and re-opened).
    pub fn cached_route_channel(&self, bind: &BindIdentity) -> Option<u16> {
        let module_id = self.default_module_id.as_deref()?;
        self.cached_route_channel_for_module(bind, module_id)
    }

    /// The cached unscoped route channel for `bind` to `module_id`.
    pub fn cached_route_channel_for_module(
        &self,
        bind: &BindIdentity,
        module_id: &str,
    ) -> Option<u16> {
        self.cached_route_channel_for_scope(bind, None, module_id)
    }

    /// The cached route channel for `bind` to `module_id` opened under `scope` (`None` for an
    /// unscoped route), or `None` if no such route is cached.
    pub fn cached_route_channel_for_scope(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        module_id: &str,
    ) -> Option<u16> {
        let key = Self::route_key(bind, scope, module_id);
        self.routes
            .lock()
            .unwrap()
            .get(&key)
            .map(|r| r.handle.channel)
    }

    /// Close all of a session's module routes: send route GOODBYE for each cached channel so
    /// the daemon FREES the channels, then drop the local entries. Call this when a SESSION ENDS
    /// (the coordinator's actor closes)
    /// so a long-lived SHARED consumer connection (one plane serving many sessions over one
    /// connection) doesn't accumulate stale channels across sessions. Unlike a per-session
    /// plane (where dropping the plane reclaims its routes), a SHARED plane is long-lived, so
    /// the channel must be explicitly freed. Best-effort: a GOODBYE send failure just leaves
    /// the channel until the connection closes (the local entry is dropped regardless, so a
    /// future dispatch re-opens fresh).
    pub async fn close_session_routes(&self, bind: &BindIdentity) {
        let session = Self::session_key(bind);
        if let Some(token) = self.session_cancel.lock().unwrap().remove(&session) {
            token.cancel();
        }
        self.approved_args
            .lock()
            .unwrap()
            .retain(|(owner, _), _| owner != &session);
        self.batch_presets
            .lock()
            .unwrap()
            .retain(|(owner, _), _| owner != &session);
        self.batch_routes
            .lock()
            .unwrap()
            .retain(|(owner, _), _| owner != &session);
        self.batch_handles
            .lock()
            .unwrap()
            .retain(|(owner, _), _| owner != &session);
        self.run_latches
            .lock()
            .unwrap()
            .retain(|(owner, _), _| owner != &session);
        let channels = {
            let mut routes = self.routes.lock().unwrap();
            let keys: Vec<RouteKey> = routes
                .keys()
                .filter(|key| key.session == session)
                .cloned()
                .collect();
            keys.into_iter()
                .filter_map(|key| routes.remove(&key).map(|route| route.handle))
                .collect::<Vec<_>>()
        };
        for handle in channels {
            let _ = self.conn.route_close(&handle).await;
        }
    }
}

#[async_trait]
impl ToolPlane for SubcToolPlane {
    async fn batch_started(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        if ctx.scope.is_some() {
            // No call of this batch is in flight yet (its intents are written after this
            // returns), so what the run already learned, or a drain that closed its routes since
            // the last batch, decides the whole batch before anything opens.
            if let Some(latch) = self.run_latch(ctx) {
                return Err(latch.into_plane_error());
            }
            if let Some(reason) = self.gone_since_last_batch(ctx).await {
                return Err(ToolPlaneError::ScopeDrained { reason });
            }
        }
        let mut catalog = self.catalog_snapshot().await?;
        let mut resolved = resolve_batch_routes(
            calls,
            manifest,
            catalog.as_ref(),
            self.default_module_id.as_deref(),
        );
        if resolved.values().any(|route| route.module().is_none()) {
            // A cached snapshot must never turn a call that would resolve against today's
            // catalog into a failure. A provider can appear after the snapshot was captured,
            // and ambiguity can disappear when one provider unregisters, so every resolution
            // failure gets exactly one fresh catalog fetch before its result is frozen.
            catalog = self.refresh_catalog_snapshot().await?;
            resolved = resolve_batch_routes(
                calls,
                manifest,
                catalog.as_ref(),
                self.default_module_id.as_deref(),
            );
        }
        let modules = resolved
            .values()
            .filter_map(ToolRoute::module)
            .map(ToOwned::to_owned)
            .collect::<BTreeSet<_>>();

        self.batch_presets
            .lock()
            .unwrap()
            .insert(Self::batch_route_key(ctx), manifest.clone());
        self.batch_routes
            .lock()
            .unwrap()
            .insert(Self::batch_route_key(ctx), resolved);

        let route_results = join_all(
            modules
                .iter()
                .map(|module_id| self.ensure_route(&ctx.bind, ctx.scope.as_ref(), module_id)),
        )
        .await;
        // join_all retains input order. Check every result in the same BTreeSet order as the
        // former serial loop so completion timing cannot change which module error is returned.
        let mut handles = HashMap::new();
        for (module_id, result) in modules.iter().zip(route_results) {
            match result {
                Ok((handle, _)) => {
                    let stamp = self.cached_stamp(&ctx.bind, ctx.scope.as_ref(), module_id, handle);
                    if let Some(stamp) = stamp {
                        handles.insert(module_id.clone(), (handle, stamp));
                    }
                }
                Err(error) => {
                    self.latch_open_refusal(ctx, &error);
                    return Err(error);
                }
            }
        }
        if ctx.scope.is_some() {
            // From here the batch's calls are in flight, composed under these routes' stamps: a
            // close of one of them before a call is sent is resolved by `resend_or_drain`.
            self.batch_handles
                .lock()
                .unwrap()
                .insert(Self::batch_route_key(ctx), handles);
        }
        Ok(())
    }

    fn dispatch_target(&self, ctx: &BatchCtx, tool_call_id: &ToolCallId) -> Option<String> {
        // The same lookup `dispatch` makes, so the module recorded on the intent is the one the
        // call is then sent to.
        let resolved = self
            .batch_routes
            .lock()
            .unwrap()
            .get(&Self::batch_route_key(ctx))
            .and_then(|routes| routes.get(tool_call_id))
            .cloned();
        match resolved {
            Some(route) => route.module().map(ToOwned::to_owned),
            None => self.default_module_id.clone(),
        }
    }

    async fn dispatch(&self, ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        // Fence: a repeated (session, tool_call_id) returns the prior result (at-most-once).
        // Keyed by session so the same provider id minted by two sessions can't false-dedup.
        let session_key = Self::session_key(&ctx.bind);
        let fence_key: FenceKey = (session_key.clone(), call.tool_call_id.clone());
        if let Some(prior) = self.fence.lock().unwrap().get(&fence_key) {
            return DispatchResult::Completed(prior);
        }

        let route = self
            .batch_routes
            .lock()
            .unwrap()
            .get(&Self::batch_route_key(ctx))
            .and_then(|routes| routes.get(&call.tool_call_id))
            .cloned()
            .unwrap_or_else(|| match self.default_module_id.clone() {
                Some(module_id) => ToolRoute::Module(module_id),
                None => ToolRoute::Failure(format!(
                    "tool '{}' could not be resolved: no caller module hint, no catalog provider, and no default module configured",
                    call.tool_name
                )),
            });
        let module_id = match route {
            ToolRoute::Module(module_id) => module_id,
            ToolRoute::Failure(message) => {
                let result = route_failure_result(call, message);
                self.fence
                    .lock()
                    .unwrap()
                    .insert(&ctx.batch_id, fence_key, result.clone());
                return DispatchResult::Completed(result);
            }
        };
        let scope = ctx.scope.as_ref();
        let route_key = Self::route_key(&ctx.bind, scope, &module_id);

        // For a scoped call: the stamp of the route its batch started with, which the call was
        // composed under. `None` for an unscoped call.
        let mut composed_under = None;
        if scope.is_some() {
            match self.run_latch(ctx) {
                Some(RunScopeLatch::Refused { code, detail }) => {
                    return DispatchResult::NotSent {
                        reason: ToolPlaneError::ScopeRefused { code, detail }.to_string(),
                    }
                }
                // The run's scope, or its parent, is gone and this call was not yet sent.
                Some(RunScopeLatch::Drained { reason }) => {
                    return DispatchResult::ScopeDrained {
                        reason,
                        sent: false,
                    }
                }
                None => {}
            }
            if let Some((handle, stamp)) = self.batch_route(ctx, &module_id) {
                // This call's intent is already durable, so it is in flight. If the route its
                // batch started with has closed, it was caught before send.
                if !self.conn.route_is_open(&handle) {
                    return self
                        .resend_or_drain(
                            ctx,
                            call,
                            &module_id,
                            handle,
                            Some(stamp),
                            DEFAULT_ROUTE_OPEN_PATIENCE,
                            fence_key,
                        )
                        .await;
                }
                composed_under = Some(stamp);
            }
        }

        // route.open / semaphore failures are BEFORE any tool send → NotSent (the
        // call never left the process; safe to (re-)dispatch).
        let (route_handle, sem) = match self.ensure_route(&ctx.bind, scope, &module_id).await {
            Ok(rs) => rs,
            Err(e) => {
                self.latch_open_refusal(ctx, &e);
                return DispatchResult::NotSent {
                    reason: e.to_string(),
                };
            }
        };
        if scope.is_some() && composed_under.is_none() {
            composed_under = self.cached_stamp(&ctx.bind, scope, &module_id, route_handle);
        }
        // Flow control: hold a route permit from send to terminal so concurrent
        // in-flight never exceeds the provider's window (Serial → serializes).
        let _stale_permit = match Arc::clone(&sem).acquire_owned().await {
            Ok(p) => p,
            Err(_) => {
                return DispatchResult::NotSent {
                    reason: "route semaphore closed".into(),
                }
            }
        };

        // Another task may have invalidated/re-opened the session route while this task was
        // waiting for flow-control credit. This check runs strictly before the send, so it is
        // safe to report NotSent: we only avoid a stale dispatch when the captured route is no
        // longer the live cached route, and we never reclassify an already-sent request.
        if !self.route_is_current(&route_key, route_handle, &sem) {
            return DispatchResult::NotSent {
                reason: "cached route changed before send".into(),
            };
        }

        // A signal before writer enqueue returns typed NotSent below. Once the
        // guarded push succeeds, reply failures are handled as sent calls:
        //  - an `unknown_channel` rejection is provably pre-send: the daemon's router
        //    freed the channel (the target module was drain-restarted) and refused the
        //    request at routing — it never reached the tool. Evict the stale route and
        //    retry ONCE against a fresh one, so a routine tool-module restart costs one
        //    transparent retry instead of failing every dispatch until this process is
        //    bounced;
        //  - a `module_reloading` rejection occurs while the module is still draining for
        //    restart. The protocol guarantees this rejection happens before the request is
        //    sent to the tool, so re-dispatching cannot execute it twice (see also
        //    `rejection_is_module_reloading`). Without this retry, an ordinary tool-module
        //    restart returns a `tool/route rejected` error to the model. Re-opening waits
        //    for the drain with RELOAD_REOPEN_PATIENCE rather than the default 3s: that
        //    default ends most real restarts in NotSent, returning the same error three
        //    seconds later;
        //  - provider route_not_bound / route_reclaimed refusals share the stale-route retry;
        //    outcome_unknown_root_reclaimed instead interrupts and evicts without a retry;
        //  - any other typed Error frame (Rejected) is a DEFINITE negative response — a
        //    graceful is_error result, terminal, never re-dispatched;
        //  - a transport/protocol failure (timeout / reset / closed mid-call) means
        //    the request may have reached the executor with no result coming back,
        //    so the outcome is ambiguous → SentButOutcomeUnknown (stops the run).
        //
        // That last case is why the reply deadline is sized per call rather than fixed: a
        // deadline shorter than the work turns a healthy call into an ambiguous one.
        let reply_deadline = self.reply_deadline(ctx, &module_id, call);
        let first = match self
            .send_call(ctx, &module_id, &route_handle, call, reply_deadline)
            .await
        {
            SendAttempt::NotSent { reason } => return DispatchResult::NotSent { reason },
            SendAttempt::Reply(reply) => reply,
        };
        let route_is_known_stale = matches!(&first, Err(ConnError::StaleRouteHandle(_)))
            || matches!(&first, Err(ConnError::Rejected(msg)) if crate::connection::classify_tool_route_refusal(msg) == crate::connection::ToolRouteRefusal::Reopen);
        let endpoint_is_reloading = matches!(
            &first,
            Err(ConnError::Rejected(msg)) if crate::rejection_is_module_reloading(msg)
        );
        // A scoped call caught before send is never re-sent blindly. The scope's current state
        // decides, and so does whether a fresh route would carry the same epoch and agent as
        // the route the call was composed under (see `resend_or_drain`).
        if (route_is_known_stale || endpoint_is_reloading) && scope.is_some() {
            let patience = if endpoint_is_reloading {
                RELOAD_REOPEN_PATIENCE
            } else {
                DEFAULT_ROUTE_OPEN_PATIENCE
            };
            return self
                .resend_or_drain(
                    ctx,
                    call,
                    &module_id,
                    route_handle,
                    composed_under,
                    patience,
                    fence_key,
                )
                .await;
        }
        let mut sent_on = route_handle;
        let outcome = if route_is_known_stale || endpoint_is_reloading {
            self.remove_batch_call_route(ctx, &call.tool_call_id);
            // FORGOTTEN, NOT CLOSED, in both cases. For module_reloading the daemon still holds
            // the route, and closing it looks tidier -- but it is not safe. A sibling call in
            // the same batch may still be in flight on it (the drain is waiting for exactly
            // that call), and a GOODBYE would cut its reply off, turning a restart into an
            // ambiguous outcome that stops the run. And eviction here is by key, so if a sibling
            // already re-opened, this would close the FRESH route instead. The drain removes the
            // old binding on its own; leaving it costs nothing but a dead entry until then.
            let _ = self.invalidate_route_for_module(&ctx.bind, scope, &module_id);
            let reopened = if endpoint_is_reloading {
                let lifecycle = self.capture_session_lifecycle_token(&ctx.bind);
                self.ensure_route_for_lifecycle_inner(
                    &ctx.bind,
                    scope,
                    &module_id,
                    &lifecycle,
                    None,
                    RELOAD_REOPEN_PATIENCE,
                )
                .await
                .map_err(RouteEnsureError::into_plane)
            } else {
                self.ensure_route(&ctx.bind, scope, &module_id).await
            };
            match reopened {
                Ok((fresh_handle, fresh_sem)) => {
                    sent_on = fresh_handle;
                    let _fresh_permit = match Arc::clone(&fresh_sem).acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => {
                            return DispatchResult::NotSent {
                                reason: "route semaphore closed".into(),
                            };
                        }
                    };
                    match self
                        .send_call(ctx, &module_id, &fresh_handle, call, reply_deadline)
                        .await
                    {
                        SendAttempt::NotSent { reason } => {
                            return DispatchResult::NotSent { reason }
                        }
                        SendAttempt::Reply(reply) => reply,
                    }
                }
                Err(e) => {
                    self.latch_open_refusal(ctx, &e);
                    return DispatchResult::NotSent {
                        reason: format!("route re-open after stale handle failed: {e}"),
                    };
                }
            }
        } else {
            first
        };
        self.finish_dispatch(
            ctx,
            call,
            &module_id,
            fence_key,
            sent_on,
            composed_under,
            outcome,
        )
        .await
    }

    async fn dispatch_approved(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        args_jcs: &str,
        call_key: &str,
        _approval_digest: &str,
        signals: &broca_core::tool_plane::DispatchSignals,
    ) -> DispatchResult {
        if serde_json::from_str::<serde_json::Value>(args_jcs).is_err() {
            return DispatchResult::NotSent {
                reason: "approval_args_invalid".into(),
            };
        }
        self.approved_args
            .lock()
            .unwrap()
            .entry(Self::batch_route_key(ctx))
            .or_default()
            .insert(
                call.tool_call_id.clone(),
                ApprovedArgs {
                    bytes: args_jcs.into(),
                    key: call_key.into(),
                    signals: signals.clone(),
                },
            );
        self.dispatch(ctx, call).await
    }

    async fn recheck_approval_scope(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        module: Option<&str>,
    ) -> Result<(), ToolPlaneError> {
        if !self
            .conn
            .server_capabilities()
            .await
            .map_err(conn_to_plane)?
            .iter()
            .any(|name| name == CAP_SCOPES_V1)
        {
            return Err(ToolPlaneError::ScopeRefused {
                code: "scope_unsupported".into(),
                detail: "a fresh approval route requires scopes/v1".into(),
            });
        }
        let scope = ctx
            .scope
            .as_ref()
            .ok_or_else(|| ToolPlaneError::ScopeRefused {
                code: "approve_requires_scope".into(),
                detail: "approval requires a frozen scope".into(),
            })?;
        let owner = owner_principal(scope).map_err(|detail| ToolPlaneError::ScopeRefused {
            code: "scope_unverifiable".into(),
            detail,
        })?;
        let module = module
            .map(str::to_owned)
            .or_else(|| self.dispatch_target(ctx, &call.tool_call_id))
            .ok_or_else(|| ToolPlaneError::Unavailable("approval_target_unavailable".into()))?;
        let route = self
            .conn
            .route_open(
                &module,
                &ctx.bind,
                Some(&ScopeSelector {
                    owner,
                    scope_ref: scope.scope_ref.clone(),
                    scope_epoch: Some(scope.scope_epoch),
                }),
            )
            .await
            .map_err(|error| {
                let code = match &error {
                    ConnError::Rejected(message) => rejection_code(message).to_owned(),
                    _ => SCOPE_UNVERIFIABLE.into(),
                };
                ToolPlaneError::ScopeRefused {
                    code,
                    detail: error.to_string(),
                }
            })?;
        let _ = self.conn.route_close(&route).await;
        Ok(())
    }

    async fn batch_finished(&self, ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        // Evict this batch's fence entries — the at-most-once guarantee only needs to hold
        // within a batch's life (the WAL is the cross-batch durability), so without eviction
        // the fence would grow unbounded over thousands of batches.
        self.fence.lock().unwrap().evict_batch(&ctx.batch_id);
        self.approved_args
            .lock()
            .unwrap()
            .remove(&Self::batch_route_key(ctx));
        self.batch_presets
            .lock()
            .unwrap()
            .remove(&Self::batch_route_key(ctx));
        self.batch_routes
            .lock()
            .unwrap()
            .remove(&Self::batch_route_key(ctx));
        self.batch_handles
            .lock()
            .unwrap()
            .remove(&Self::batch_route_key(ctx));
        Ok(())
    }
}

struct CachedRoute {
    handle: RouteHandle,
    sem: Arc<Semaphore>,
    loser_handle: Option<RouteHandle>,
}

fn cache_route_state(
    routes: &mut HashMap<RouteKey, RouteState>,
    key: RouteKey,
    handle: RouteHandle,
    sem: Arc<Semaphore>,
    stamp: Option<ScopeStamp>,
) -> CachedRoute {
    match routes.entry(key) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(RouteState {
                handle,
                sem: Arc::clone(&sem),
                stamp,
            });
            CachedRoute {
                handle,
                sem,
                loser_handle: None,
            }
        }
        std::collections::hash_map::Entry::Occupied(entry) => {
            let existing = entry.get();
            CachedRoute {
                handle: existing.handle,
                sem: Arc::clone(&existing.sem),
                loser_handle: (existing.handle != handle).then_some(handle),
            }
        }
    }
}

fn route_matches(
    routes: &HashMap<RouteKey, RouteState>,
    key: &RouteKey,
    handle: RouteHandle,
    sem: &Arc<Semaphore>,
) -> bool {
    routes
        .get(key)
        .map(|route| route.handle == handle && Arc::ptr_eq(&route.sem, sem))
        .unwrap_or(false)
}

/// Extract a tool-provider role's declared concurrency from a catalog entry.
fn provider_concurrency(entry: &CatalogEntry) -> Option<Concurrency> {
    entry.roles.iter().find_map(|r| match r {
        ProviderRole::ToolProvider { concurrency, .. } => Some(concurrency.clone()),
        _ => None,
    })
}

fn resolve_batch_routes(
    calls: &[ToolCall],
    manifest: &ToolManifest,
    catalog: &[CatalogEntry],
    default_module: Option<&str>,
) -> HashMap<ToolCallId, ToolRoute> {
    let mut routes = HashMap::new();
    for call in calls {
        // The first occurrence owns the route, matching the core duplicate-call plan that
        // dispatches only the first occurrence. Later duplicates never dispatch, so they
        // must not replace the surviving call's route or trigger needless resolution.
        routes.entry(call.tool_call_id.clone()).or_insert_with(|| {
            resolve_tool_module(&call.tool_name, manifest, catalog, default_module)
        });
    }
    routes
}

fn resolve_tool_module(
    tool_name: &str,
    manifest: &ToolManifest,
    catalog: &[CatalogEntry],
    default_module: Option<&str>,
) -> ToolRoute {
    if let Some(module_id) = manifest
        .get(tool_name)
        .and_then(|tool| tool.module.as_deref())
        .map(str::trim)
        .filter(|module_id| !module_id.is_empty())
    {
        return ToolRoute::Module(module_id.to_string());
    }

    let mut candidates = catalog
        .iter()
        .filter(|entry| catalog_entry_has_tool(entry, tool_name))
        .map(|entry| entry.module_id.clone())
        .collect::<Vec<_>>();
    candidates.sort();
    candidates.dedup();

    match candidates.as_slice() {
        [module_id] => ToolRoute::Module(module_id.clone()),
        [] => match default_module.map(str::trim).filter(|module_id| !module_id.is_empty()) {
            Some(module_id) => ToolRoute::Module(module_id.to_string()),
            None => ToolRoute::Failure(format!(
                "tool '{tool_name}' could not be resolved: no caller module hint, no catalog provider, and no default module configured"
            )),
        },
        _ => ToolRoute::Failure(format!(
            "tool '{tool_name}' is registered by multiple modules without a caller module hint: {}",
            candidates.join(", ")
        )),
    }
}

fn catalog_entry_has_tool(entry: &CatalogEntry, tool_name: &str) -> bool {
    entry.roles.iter().any(|role| {
        matches!(role, ProviderRole::ToolProvider { tools, .. } if tools.iter().any(|tool| tool.name == tool_name))
    })
}

fn route_failure_result(call: &ToolCall, message: String) -> ToolResult {
    ToolResult {
        tool_call_id: call.tool_call_id.clone(),
        output: ToolOutput::Error { message },
        is_error: true,
    }
}

/// Convert the frozen scope's owner to subc's `Principal`. `BindPrincipal` serializes to exactly
/// the JSON of subc's `Principal`, so a round trip through JSON converts it; a principal kind
/// this build does not know cannot be named on the wire, and is an error.
pub(crate) fn owner_principal(scope: &SessionScope) -> Result<Principal, String> {
    serde_json::to_value(&scope.owner)
        .and_then(serde_json::from_value::<Principal>)
        .map_err(|error| {
            format!(
                "the frozen scope owner {} is not a principal this build can name: {error}",
                scope.owner.as_owner_string()
            )
        })
}

/// Classify a scoped `route.open` refusal that the retry loop returned.
///
/// The two retryable scope refusals only come back here once the hold has run out, so they
/// become broca's `scope_unverifiable`. Every other `scope_*` code is terminal and is passed on
/// by its own name; it was never retried or held. Anything else is an ordinary refusal.
fn scoped_open_refusal(message: String, hold: Duration) -> RouteEnsureError {
    let code = rejection_code(&message);
    if matches!(code, SCOPE_NOT_SYNCED | SCOPE_CHANGED) {
        return RouteEnsureError::Plane(ToolPlaneError::ScopeRefused {
            code: SCOPE_UNVERIFIABLE.into(),
            detail: format!(
                "the daemon still answered {code} when the scope hold (at most {hold:?}) ran \
                 out; nothing was dispatched ({message})"
            ),
        });
    }
    if code.starts_with("scope_") {
        return RouteEnsureError::Plane(ToolPlaneError::ScopeRefused {
            code: code.to_owned(),
            detail: message,
        });
    }
    RouteEnsureError::Connection(ConnError::Rejected(message))
}

/// A subc-LEVEL failure (typed Error frame / transport) is a PLANE failure → the
/// loop turns it into a graceful is_error result. A tool EXECUTION error rides
/// inside a successful CallToolResult (`isError: true`) instead.
fn conn_to_plane(e: ConnError) -> ToolPlaneError {
    ToolPlaneError::Unavailable(e.to_string())
}

/// The warm-bind retry delay for `attempt` (1-based): 250ms doubling to a 5s cap.
/// Sequence: 250ms, 500ms, 1s, 2s, 4s, 5s, 5s, … — ~8 attempts in a 30s window.
fn warm_bind_backoff(attempt: u32) -> Duration {
    const BASE_MS: u64 = 250;
    const CAP_MS: u64 = 5_000;
    let exp = attempt.saturating_sub(1).min(10);
    Duration::from_millis((BASE_MS << exp).min(CAP_MS))
}

/// Map an MCP `CallToolResult` to the canonical `ToolResult` without placing raw media in
/// the model prompt. Text blocks retain their byte-exact concatenation; non-text blocks become
/// compact source-order placeholders. A result with no content can use `structuredContent` as
/// JSON, while execution errors remain textual errors. A reply carrying none of `content`,
/// `structuredContent` or `isError` is not CallToolResult-shaped and maps to an error quoting
/// the (bounded) raw reply.
fn map_call_tool_result(id: &ToolCallId, raw: &serde_json::Value) -> ToolResult {
    let is_error = raw
        .get("isError")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let content = raw.get("content").and_then(serde_json::Value::as_array);
    let text = content
        .map(|blocks| {
            if blocks.iter().all(|block| {
                block
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
            }) {
                return blocks
                    .iter()
                    .filter_map(|block| block.get("text").and_then(serde_json::Value::as_str))
                    .collect::<Vec<_>>()
                    .join("");
            }
            blocks
                .iter()
                .map(|block| {
                    block
                        .get("text")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                        .unwrap_or_else(|| non_text_tool_content_placeholder(block))
                })
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default();

    // A reply with none of the three CallToolResult fields is not a result at all (a live
    // case was a tool answering `{"result": {...}}`). Mapping it to an empty success would
    // tell the model the tool ran and returned nothing, so it could not tell a failed call
    // from a legitimately empty one. Surface it as an error that quotes what came back.
    // `content: []` is deliberately not caught here: a tool may legitimately return nothing.
    if raw.get("content").is_none()
        && raw.get("structuredContent").is_none()
        && raw.get("isError").is_none()
    {
        return ToolResult {
            tool_call_id: id.clone(),
            output: ToolOutput::Error {
                message: malformed_tool_reply_message(raw),
            },
            is_error: true,
        };
    }

    let output = if is_error {
        ToolOutput::Error { message: text }
    } else if content.is_none_or(Vec::is_empty) {
        match raw.get("structuredContent") {
            Some(value) => ToolOutput::Json {
                value: value.clone(),
            },
            None => ToolOutput::Text { text },
        }
    } else {
        ToolOutput::Text { text }
    };
    ToolResult {
        tool_call_id: id.clone(),
        output,
        is_error,
    }
}

/// Upper bound, in bytes, on the raw reply quoted back to the model when a tool reply is not
/// CallToolResult-shaped. Enough to show what the tool actually sent without letting one
/// oversized malformed reply flood the prompt.
const MALFORMED_TOOL_REPLY_QUOTE_MAX_BYTES: usize = 2048;

fn malformed_tool_reply_message(raw: &serde_json::Value) -> String {
    let compact = serde_json::to_string(raw).unwrap_or_default();
    let quoted = if compact.len() > MALFORMED_TOOL_REPLY_QUOTE_MAX_BYTES {
        // Cut on a char boundary so the quote stays valid UTF-8.
        let mut end = MALFORMED_TOOL_REPLY_QUOTE_MAX_BYTES;
        while !compact.is_char_boundary(end) {
            end -= 1;
        }
        format!(
            "{}… [truncated: {} of {} bytes shown]",
            &compact[..end],
            end,
            compact.len()
        )
    } else {
        compact
    };
    format!(
        "tool reply carried none of `content`, `structuredContent` or `isError`, so it is not a valid tool result; raw reply: {quoted}"
    )
}

fn non_text_tool_content_placeholder(block: &serde_json::Value) -> String {
    let content_type = block
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let mime_type = block
        .get("mimeType")
        .and_then(serde_json::Value::as_str)
        .map(|value| format!(" mimeType={value}"))
        .unwrap_or_default();
    let data_size = block
        .get("data")
        .and_then(serde_json::Value::as_str)
        .map(|value| format!(" data≈{} bytes", value.len()))
        .unwrap_or_default();
    format!("[non-text tool content omitted: type={content_type}{mime_type}{data_size}]")
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};

    use broca_protocol::request::{CallOptions, Tool};
    use broca_provider::{
        families::{openai_chat_frozen_config, OpenAiChat},
        wire::WireFamily,
    };
    use tokio::sync::Notify;

    use super::*;

    #[derive(Clone)]
    struct RouteOpenGate {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl RouteOpenGate {
        fn new() -> Self {
            Self {
                entered: Arc::new(Notify::new()),
                release: Arc::new(Notify::new()),
            }
        }
    }

    #[derive(Clone, Default)]
    struct RouteOpenScript {
        delay: Duration,
        error: Option<ScriptedRouteOpenError>,
        gate: Option<RouteOpenGate>,
    }

    #[derive(Clone)]
    enum ScriptedRouteOpenError {
        Rejected(String),
        Transport(String),
    }

    impl RouteOpenScript {
        fn rejected(message: impl Into<String>) -> Self {
            Self {
                error: Some(ScriptedRouteOpenError::Rejected(message.into())),
                ..Self::default()
            }
        }

        fn transport(message: impl Into<String>) -> Self {
            Self {
                error: Some(ScriptedRouteOpenError::Transport(message.into())),
                ..Self::default()
            }
        }
    }

    struct RouteOpenActivity<'a>(&'a AtomicUsize);

    impl Drop for RouteOpenActivity<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    struct ScriptedToolPlaneConnection {
        catalogs: Mutex<VecDeque<Vec<CatalogEntry>>>,
        catalog_calls: AtomicUsize,
        next_channel: AtomicU16,
        route_open_scripts: Mutex<HashMap<String, VecDeque<RouteOpenScript>>>,
        route_open_calls: AtomicUsize,
        active_route_opens: AtomicUsize,
        max_concurrent_route_opens: AtomicUsize,
        route_open_completions: Mutex<Vec<String>>,
        tool_replies: Mutex<VecDeque<Result<serde_json::Value, ConnError>>>,
        tool_calls: AtomicUsize,
        /// Every id the plane put on the wire, in dispatch order. RECORDED rather than
        /// ignored so a test can assert the retry reuses the SAME id: broca re-sends only
        /// when the daemon proved the first attempt never arrived, so a module seeing one
        /// id twice means it executed twice, and that is a defect rather than a retry.
        dispatched_ids: Mutex<Vec<Option<String>>>,
        closed_routes: Mutex<Vec<RouteHandle>>,
        /// Every route.open attempt, in order: the session it was for and the scope it carried.
        open_attempts: Mutex<Vec<(String, Option<ScopeSelector>)>>,
        /// What `server.describe` advertises; `scopes/v1` unless a test removes it.
        capabilities: Mutex<Vec<String>>,
        /// Routes the daemon has closed, by channel, as the connection learns from a GOODBYE.
        closed: Mutex<std::collections::HashSet<u16>>,
        /// When set, the next tool call's route closes while the call is in flight: the
        /// request counts as sent and its route closes under it.
        close_during_call: Mutex<bool>,
        dispatched_presets: Mutex<Vec<Option<String>>>,
        dispatched_deadlines: Mutex<Vec<Duration>>,
        dispatched_arguments: Mutex<Vec<serde_json::Value>>,
    }

    impl ScriptedToolPlaneConnection {
        /// Close `route` from the daemon's side, as its GOODBYE would.
        fn close(&self, route: RouteHandle) {
            self.closed.lock().unwrap().insert(route.channel);
        }

        fn opened_scopes(&self) -> Vec<(String, Option<ScopeSelector>)> {
            self.open_attempts.lock().unwrap().clone()
        }

        fn new(
            catalogs: Vec<Vec<CatalogEntry>>,
            tool_replies: Vec<Result<serde_json::Value, ConnError>>,
        ) -> Arc<Self> {
            Arc::new(Self {
                catalogs: Mutex::new(catalogs.into()),
                catalog_calls: AtomicUsize::new(0),
                next_channel: AtomicU16::new(1),
                route_open_scripts: Mutex::new(HashMap::new()),
                route_open_calls: AtomicUsize::new(0),
                active_route_opens: AtomicUsize::new(0),
                max_concurrent_route_opens: AtomicUsize::new(0),
                route_open_completions: Mutex::new(Vec::new()),
                tool_replies: Mutex::new(tool_replies.into()),
                tool_calls: AtomicUsize::new(0),
                dispatched_ids: Mutex::new(Vec::new()),
                closed_routes: Mutex::new(Vec::new()),
                open_attempts: Mutex::new(Vec::new()),
                capabilities: Mutex::new(vec![CAP_SCOPES_V1.to_owned()]),
                closed: Mutex::new(Default::default()),
                close_during_call: Mutex::new(false),
                dispatched_presets: Mutex::new(Vec::new()),
                dispatched_deadlines: Mutex::new(Vec::new()),
                dispatched_arguments: Mutex::new(Vec::new()),
            })
        }

        fn script_route_open(&self, module_id: &str, delay: Duration, error: Option<&str>) {
            self.route_open_scripts.lock().unwrap().insert(
                module_id.to_string(),
                VecDeque::from([RouteOpenScript {
                    delay,
                    error: error
                        .map(|message| ScriptedRouteOpenError::Transport(message.to_owned())),
                    gate: None,
                }]),
            );
        }

        fn script_route_open_sequence(&self, module_id: &str, scripts: Vec<RouteOpenScript>) {
            self.route_open_scripts
                .lock()
                .unwrap()
                .insert(module_id.to_string(), scripts.into());
        }

        fn gate_route_open(&self, module_id: &str) -> RouteOpenGate {
            let gate = RouteOpenGate::new();
            self.route_open_scripts.lock().unwrap().insert(
                module_id.to_string(),
                VecDeque::from([RouteOpenScript {
                    gate: Some(gate.clone()),
                    ..RouteOpenScript::default()
                }]),
            );
            gate
        }

        fn enter_route_open(&self) -> RouteOpenActivity<'_> {
            let active = self.active_route_opens.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_concurrent_route_opens
                .fetch_max(active, Ordering::SeqCst);
            RouteOpenActivity(&self.active_route_opens)
        }
    }

    #[async_trait]
    impl ToolPlaneConnection for ScriptedToolPlaneConnection {
        async fn catalog_list(&self) -> Result<Vec<CatalogEntry>, ConnError> {
            self.catalog_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self
                .catalogs
                .lock()
                .unwrap()
                .pop_front()
                .expect("test must script every catalog.list response"))
        }

        async fn route_open(
            &self,
            module_id: &str,
            identity: &BindIdentity,
            scope: Option<&ScopeSelector>,
        ) -> Result<RouteHandle, ConnError> {
            self.route_open_calls.fetch_add(1, Ordering::SeqCst);
            self.open_attempts
                .lock()
                .unwrap()
                .push((identity.session.clone(), scope.cloned()));
            let _activity = self.enter_route_open();
            let script = {
                let mut scripts = self.route_open_scripts.lock().unwrap();
                let scripts = scripts.get_mut(module_id);
                scripts
                    .and_then(|scripts| {
                        if scripts.len() > 1 {
                            scripts.pop_front()
                        } else {
                            scripts.front().cloned()
                        }
                    })
                    .unwrap_or_default()
            };
            if !script.delay.is_zero() {
                tokio::time::sleep(script.delay).await;
            }
            if let Some(gate) = script.gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            self.route_open_completions
                .lock()
                .unwrap()
                .push(module_id.to_string());
            if let Some(error) = script.error {
                return Err(match error {
                    ScriptedRouteOpenError::Rejected(message) => ConnError::Rejected(message),
                    ScriptedRouteOpenError::Transport(message) => ConnError::Transport(message),
                });
            }
            Ok(RouteHandle::synthetic(
                self.next_channel.fetch_add(1, Ordering::SeqCst),
            ))
        }

        async fn route_close(&self, route: &RouteHandle) -> Result<(), ConnError> {
            self.closed_routes.lock().unwrap().push(*route);
            Ok(())
        }

        async fn tool_call(
            &self,
            route: &RouteHandle,
            _name: &str,
            arguments: &serde_json::Value,
            tool_call_id: Option<&str>,
            deadline: Duration,
            preset: Option<&str>,
        ) -> Result<serde_json::Value, ConnError> {
            // A route the connection already saw closed is refused before anything is written,
            // as the real connection does for a handle missing from its live epoch map.
            if self.closed.lock().unwrap().contains(&route.channel) {
                return Err(ConnError::StaleRouteHandle(*route));
            }
            self.dispatched_presets
                .lock()
                .unwrap()
                .push(preset.map(str::to_owned));
            self.tool_calls.fetch_add(1, Ordering::SeqCst);
            self.dispatched_deadlines.lock().unwrap().push(deadline);
            self.dispatched_arguments
                .lock()
                .unwrap()
                .push(arguments.clone());
            self.dispatched_ids
                .lock()
                .unwrap()
                .push(tool_call_id.map(str::to_string));
            if std::mem::take(&mut *self.close_during_call.lock().unwrap()) {
                self.close(*route);
                return Err(ConnError::Transport("route gone (GOODBYE)".into()));
            }
            self.tool_replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("test must script every tool-call response")
        }

        async fn server_capabilities(&self) -> Result<Vec<String>, ConnError> {
            Ok(self.capabilities.lock().unwrap().clone())
        }

        async fn tool_call_approved(
            &self,
            route: &RouteHandle,
            call: &ToolCall,
            deadline: Duration,
            preset: Option<&str>,
            args: &ApprovedArgs,
        ) -> Result<serde_json::Value, crate::connection::ApprovedCallError> {
            let arguments = serde_json::from_str(&args.bytes).expect("approved fixture arguments");
            self.tool_call(
                route,
                &call.tool_name,
                &arguments,
                Some(&call.tool_call_id.0),
                deadline,
                preset,
            )
            .await
            .map_err(Into::into)
        }

        fn route_is_open(&self, route: &RouteHandle) -> bool {
            !self.closed.lock().unwrap().contains(&route.channel)
        }
    }

    #[tokio::test]
    async fn approved_cancel_or_stop_during_ensure_route_sends_nothing() {
        for cancel in [true, false] {
            let conn = ScriptedToolPlaneConnection::new(
                vec![vec![catalog_entry("aft", &["edit"])]],
                vec![successful_tool_reply("must not send")],
            );
            let gate = conn.gate_route_open("aft");
            let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
            let ctx = batch_ctx("approved-open");
            let call = call("edit");
            let token = CancellationToken::new();
            let signals = broca_core::tool_plane::DispatchSignals::new(
                if cancel { Some(&token) } else { None },
                if cancel { None } else { Some(&token) },
            );
            let dispatch =
                plane.dispatch_approved(&ctx, &call, "{}", "call-key", "digest", &signals);
            let interrupt = async {
                gate.entered.notified().await;
                token.cancel();
                gate.release.notify_one();
            };
            let result = tokio::time::timeout(Duration::from_secs(2), async {
                tokio::join!(dispatch, interrupt)
            })
            .await;
            gate.release.notify_one();
            let (result, ()) = result.expect("held approved route must finish");
            assert_eq!(
                conn.tool_calls.load(Ordering::SeqCst),
                0,
                "cancelled approved route-open wait must make zero physical sends"
            );
            assert!(
                matches!(result, DispatchResult::NotSent { ref reason } if reason == "approved_dispatch_stopped_before_send")
            );
            assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn approved_cancel_or_stop_during_permit_wait_sends_nothing() {
        use futures::FutureExt;
        for cancel in [true, false] {
            let conn = ScriptedToolPlaneConnection::new(
                vec![vec![catalog_entry("aft", &["edit"])]],
                vec![successful_tool_reply("must not send")],
            );
            let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
            let ctx = batch_ctx("approved-permit");
            let call = call("edit");
            let (_, sem) = plane.ensure_route(&ctx.bind, None, "aft").await.unwrap();
            let permit = sem.clone().acquire_owned().await.unwrap();
            let token = CancellationToken::new();
            let signals = broca_core::tool_plane::DispatchSignals::new(
                if cancel { Some(&token) } else { None },
                if cancel { None } else { Some(&token) },
            );
            let mut dispatch = Box::pin(
                plane.dispatch_approved(&ctx, &call, "{}", "call-key", "digest", &signals),
            );
            assert!(
                dispatch.as_mut().now_or_never().is_none(),
                "cached route is ready and the only wait is flow-control credit"
            );
            assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
            assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0);
            token.cancel();
            drop(permit);
            let result = tokio::time::timeout(Duration::from_secs(2), dispatch)
                .await
                .unwrap();
            assert_eq!(
                conn.tool_calls.load(Ordering::SeqCst),
                0,
                "cancelled approved permit wait must make zero physical sends"
            );
            assert!(
                matches!(result, DispatchResult::NotSent { ref reason } if reason == "approved_dispatch_stopped_before_send")
            );
            assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn uncancelled_approved_dispatch_still_physically_sends_once() {
        let conn = ScriptedToolPlaneConnection::new(
            vec![vec![catalog_entry("aft", &["edit"])]],
            vec![successful_tool_reply("sent")],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        assert!(matches!(
            plane
                .dispatch_approved(
                    &batch_ctx("approved-live"),
                    &call("edit"),
                    "{}",
                    "key",
                    "digest",
                    &broca_core::tool_plane::DispatchSignals::default()
                )
                .await,
            DispatchResult::Completed(_)
        ));
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn completed_approved_fence_still_wins_over_later_cancel_signal() {
        let conn = ScriptedToolPlaneConnection::new(
            vec![vec![catalog_entry("aft", &["edit"])]],
            vec![successful_tool_reply("prior")],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        let ctx = batch_ctx("approved-fence");
        let call = call("edit");
        assert!(matches!(
            plane
                .dispatch_approved(
                    &ctx,
                    &call,
                    "{}",
                    "key",
                    "digest",
                    &broca_core::tool_plane::DispatchSignals::default()
                )
                .await,
            DispatchResult::Completed(_)
        ));
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(matches!(
            plane
                .dispatch_approved(
                    &ctx,
                    &call,
                    "{}",
                    "key",
                    "digest",
                    &broca_core::tool_plane::DispatchSignals::new(Some(&cancel), None)
                )
                .await,
            DispatchResult::Completed(_)
        ));
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
    }

    fn batch_ctx(batch_id: &str) -> BatchCtx {
        batch_ctx_for(batch_id, "/tmp/catalog-memo", "session")
    }

    fn batch_ctx_for(batch_id: &str, project_root: &str, session: &str) -> BatchCtx {
        BatchCtx {
            batch_id: BatchId(batch_id.into()),
            bind: BindIdentity::new(project_root, "test", session),
            run_id: broca_protocol::RunId("r1".into()),
            scope: None,
        }
    }

    fn successful_tool_reply(text: &str) -> Result<serde_json::Value, ConnError> {
        Ok(serde_json::json!({
            "content": [{ "type": "text", "text": text }],
            "isError": false
        }))
    }

    async fn wait_for_closed_routes(conn: &ScriptedToolPlaneConnection, count: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if conn.closed_routes.lock().unwrap().len() == count {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("best-effort route close should run promptly");
    }

    fn assert_completed_success(outcome: DispatchResult) {
        assert!(
            matches!(outcome, DispatchResult::Completed(result) if !result.is_error),
            "scripted tool call should complete successfully"
        );
    }

    #[test]
    fn window_mapping() {
        assert_eq!(window_for(&Concurrency::Serial), 1);
        assert_eq!(window_for(&Concurrency::ModuleManaged), 32);
        assert_eq!(window_for(&Concurrency::StatelessParallel), 1024);
    }

    #[test]
    fn warm_bind_backoff_doubles_to_a_cap() {
        let ms: Vec<u64> = (1..=8)
            .map(|a| warm_bind_backoff(a).as_millis() as u64)
            .collect();
        assert_eq!(ms, vec![250, 500, 1000, 2000, 4000, 5000, 5000, 5000]);
        // A pathological attempt count must not overflow the shift.
        assert_eq!(warm_bind_backoff(u32::MAX).as_millis() as u64, 5000);
        // The whole 30s warm window fits within ~10 attempts — a deterministic
        // rejection can no longer produce a route.open storm (the old flat 250ms
        // interval allowed ~120 attempts in the same window).
        let total: u64 = (1..=10)
            .map(|a| warm_bind_backoff(a).as_millis() as u64)
            .sum();
        assert!(
            total >= 30_000,
            "10 backoff intervals span the 30s window: {total}ms"
        );
    }

    #[tokio::test]
    async fn mid_run_module_warming_then_success_retries_route_open() {
        let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
        conn.script_route_open_sequence(
            "aft",
            vec![
                RouteOpenScript::rejected("module_warming — warming"),
                RouteOpenScript::default(),
            ],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        plane.windows.lock().unwrap().insert("aft".into(), 1);
        let bind = BindIdentity::new("/tmp/route-open-retry", "test", "session");
        let lifecycle = CancellationToken::new();
        tokio::time::pause();
        let started = Instant::now();
        let task = tokio::spawn(async move {
            plane
                .ensure_route_for_lifecycle_inner(
                    &bind,
                    None,
                    "aft",
                    &lifecycle,
                    None,
                    DEFAULT_ROUTE_OPEN_PATIENCE,
                )
                .await
                .map_err(RouteEnsureError::into_plane)
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        let result = task.await.unwrap();

        assert!(result.is_ok());
        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 2);
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    #[tokio::test]
    async fn mid_run_module_warming_forever_stops_at_retry_deadline() {
        let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
        conn.script_route_open_sequence(
            "aft",
            vec![RouteOpenScript::rejected("module_warming — warming")],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        plane.windows.lock().unwrap().insert("aft".into(), 1);
        let bind = BindIdentity::new("/tmp/route-open-retry-deadline", "test", "session");
        let lifecycle = CancellationToken::new();
        tokio::time::pause();
        let started = Instant::now();
        let task = tokio::spawn(async move {
            plane
                .ensure_route_for_lifecycle_inner(
                    &bind,
                    None,
                    "aft",
                    &lifecycle,
                    None,
                    DEFAULT_ROUTE_OPEN_PATIENCE,
                )
                .await
                .map_err(RouteEnsureError::into_plane)
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(3)).await;
        let result = task.await.unwrap();

        assert!(matches!(result, Err(ToolPlaneError::Unavailable(_))));
        assert!(conn.route_open_calls.load(Ordering::SeqCst) > 1);
        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_secs(2));
        assert!(elapsed < Duration::from_secs(4));
    }

    #[tokio::test]
    async fn warm_bind_respects_its_own_timeout_under_persistent_warming() {
        let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
        conn.script_route_open_sequence(
            "aft",
            vec![RouteOpenScript::rejected("module_warming — warming")],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        plane.windows.lock().unwrap().insert("aft".into(), 1);
        let bind = BindIdentity::new("/tmp/warm-short-timeout", "test", "session");
        let lifecycle = CancellationToken::new();

        tokio::time::pause();
        let started = Instant::now();
        let task = tokio::spawn(async move {
            plane
                .warm_bind_for_lifecycle(&bind, None, Duration::from_millis(500), lifecycle)
                .await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(600)).await;
        tokio::task::yield_now().await;

        assert!(task.is_finished(), "warm bind exceeded its caller deadline");
        let result = task.await.unwrap();
        assert!(matches!(result, Err(ToolPlaneError::Unavailable(_))));
        assert!(conn.route_open_calls.load(Ordering::SeqCst) > 1);
        assert!(started.elapsed() >= Duration::from_millis(500));
        assert!(started.elapsed() <= Duration::from_millis(600));
    }

    #[tokio::test]
    async fn warm_bind_caps_route_open_attempts_per_outer_iteration() {
        let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
        conn.script_route_open_sequence(
            "aft",
            vec![RouteOpenScript::rejected("module_warming — warming")],
        );
        let plane = Arc::new(SubcToolPlane::with_connection(conn.clone(), "aft"));
        plane.windows.lock().unwrap().insert("aft".into(), 1);
        let bind = BindIdentity::new("/tmp/warm-cap", "test", "session");
        let lifecycle = CancellationToken::new();

        tokio::time::pause();
        let task = {
            let plane = Arc::clone(&plane);
            let bind = bind.clone();
            let lifecycle = lifecycle.clone();
            tokio::spawn(async move {
                plane
                    .warm_bind_for_lifecycle(&bind, None, Duration::from_secs(30), lifecycle)
                    .await
            })
        };
        tokio::task::yield_now().await;
        for _ in 0..200 {
            tokio::time::advance(Duration::from_millis(100)).await;
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
        }

        // MEASURED, both designs, 20s of virtual time against a 30s caller budget:
        //   min(caller, cap)  -> 80 opens    the inner loop returns at the 3s cap, so the outer
        //                                    loop's exponential backoff sleeps between bursts
        //   caller alone      -> 101 opens   one uninterrupted burst at the flat retry interval
        // The bound sits between those, not adjacent to either: at `< 100` the failing design
        // clears by one open, so any drift in either direction makes this pass vacuously. The
        // separation is only ~21 because a window this much longer than the cap lets the capped
        // design accumulate several bursts -- worth knowing before widening the window.
        let opens = conn.route_open_calls.load(Ordering::SeqCst);
        assert!(opens > 1, "persistent warming must enter the retry arm");
        assert!(
            opens < 90,
            "one outer warm iteration must cap route.open attempts, got {opens}"
        );

        lifecycle.cancel();
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(task.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn mid_run_module_removed_is_not_retried() {
        let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
        conn.script_route_open_sequence(
            "aft",
            vec![RouteOpenScript::rejected("module_removed — gone")],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        plane.windows.lock().unwrap().insert("aft".into(), 1);
        let bind = BindIdentity::new("/tmp/route-open-terminal", "test", "session");
        let lifecycle = CancellationToken::new();
        let started = Instant::now();

        let result = plane
            .ensure_route_for_lifecycle_inner(
                &bind,
                None,
                "aft",
                &lifecycle,
                None,
                DEFAULT_ROUTE_OPEN_PATIENCE,
            )
            .await
            .map_err(RouteEnsureError::into_plane);

        assert!(matches!(result, Err(ToolPlaneError::Unavailable(_))));
        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn mid_run_unknown_route_open_code_is_not_retried() {
        let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
        conn.script_route_open_sequence(
            "aft",
            vec![RouteOpenScript::rejected("some_future_code — future")],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        plane.windows.lock().unwrap().insert("aft".into(), 1);
        let bind = BindIdentity::new("/tmp/route-open-unknown", "test", "session");
        let lifecycle = CancellationToken::new();
        let started = Instant::now();

        let result = plane
            .ensure_route_for_lifecycle_inner(
                &bind,
                None,
                "aft",
                &lifecycle,
                None,
                DEFAULT_ROUTE_OPEN_PATIENCE,
            )
            .await
            .map_err(RouteEnsureError::into_plane);

        assert!(matches!(result, Err(ToolPlaneError::Unavailable(_))));
        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn mid_run_transport_route_open_error_is_not_retried() {
        let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
        conn.script_route_open_sequence("aft", vec![RouteOpenScript::transport("socket lost")]);
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        plane.windows.lock().unwrap().insert("aft".into(), 1);
        let bind = BindIdentity::new("/tmp/route-open-transport", "test", "session");
        let lifecycle = CancellationToken::new();
        let started = Instant::now();

        let result = plane
            .ensure_route_for_lifecycle_inner(
                &bind,
                None,
                "aft",
                &lifecycle,
                None,
                DEFAULT_ROUTE_OPEN_PATIENCE,
            )
            .await
            .map_err(RouteEnsureError::into_plane);

        assert!(matches!(result, Err(ToolPlaneError::Unavailable(_))));
        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn warm_module_removed_stops_after_one_attempt() {
        let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
        conn.script_route_open_sequence(
            "aft",
            vec![RouteOpenScript::rejected("module_removed — gone")],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        plane.windows.lock().unwrap().insert("aft".into(), 1);
        let bind = BindIdentity::new("/tmp/warm-terminal", "test", "session");
        let lifecycle = CancellationToken::new();
        let started = Instant::now();

        let result = plane
            .warm_bind_for_lifecycle(&bind, None, Duration::from_millis(500), lifecycle)
            .await;

        assert!(matches!(result, Err(ToolPlaneError::Unavailable(_))));
        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn warm_transport_route_open_error_still_retries_to_timeout() {
        let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
        conn.script_route_open_sequence("aft", vec![RouteOpenScript::transport("socket lost")]);
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        plane.windows.lock().unwrap().insert("aft".into(), 1);
        let bind = BindIdentity::new("/tmp/warm-transport", "test", "session");
        let lifecycle = CancellationToken::new();
        let started = Instant::now();

        let result = plane
            .warm_bind_for_lifecycle(&bind, None, Duration::from_millis(300), lifecycle)
            .await;

        assert!(matches!(result, Err(ToolPlaneError::Unavailable(_))));
        assert!(conn.route_open_calls.load(Ordering::SeqCst) > 1);
        assert!(started.elapsed() >= Duration::from_millis(300));
    }

    #[tokio::test]
    async fn captured_lifecycle_rejects_a_delayed_first_poll_after_close() {
        let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
        let plane = Arc::new(SubcToolPlane::with_connection(conn.clone(), "aft"));
        let bind = BindIdentity::new("/tmp/delayed-warm", "test", "session");
        let lifecycle = plane.capture_session_lifecycle_token(&bind);
        let release_first_poll = Arc::new(Notify::new());
        let warm = {
            let plane = Arc::clone(&plane);
            let bind = bind.clone();
            let release_first_poll = Arc::clone(&release_first_poll);
            tokio::spawn(async move {
                release_first_poll.notified().await;
                plane
                    .warm_bind_for_lifecycle(&bind, None, Duration::from_secs(30), lifecycle)
                    .await
            })
        };

        plane.close_session_routes(&bind).await;
        release_first_poll.notify_one();

        let error = warm.await.unwrap().unwrap_err();
        assert!(matches!(error, ToolPlaneError::Unavailable(_)));
        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 0);
        assert_eq!(plane.cached_route_channel_for_module(&bind, "aft"), None);
        assert!(conn.closed_routes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn captured_lifecycle_closes_a_route_opened_during_close() {
        let conn = ScriptedToolPlaneConnection::new(vec![Vec::new()], Vec::new());
        let gate = conn.gate_route_open("aft");
        let plane = Arc::new(SubcToolPlane::with_connection(conn.clone(), "aft"));
        let bind = BindIdentity::new("/tmp/inflight-warm", "test", "session");
        let lifecycle = plane.capture_session_lifecycle_token(&bind);
        let warm = {
            let plane = Arc::clone(&plane);
            let bind = bind.clone();
            tokio::spawn(async move {
                plane
                    .warm_bind_for_lifecycle(&bind, None, Duration::from_secs(30), lifecycle)
                    .await
            })
        };

        gate.entered.notified().await;
        plane.close_session_routes(&bind).await;
        gate.release.notify_one();

        let error = warm.await.unwrap().unwrap_err();
        assert!(matches!(error, ToolPlaneError::Unavailable(_)));
        assert_eq!(plane.cached_route_channel_for_module(&bind, "aft"), None);
        assert_eq!(conn.closed_routes.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn reopened_session_warms_with_a_fresh_lifecycle() {
        let conn = ScriptedToolPlaneConnection::new(vec![Vec::new()], Vec::new());
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        let bind = BindIdentity::new("/tmp/reopened-warm", "test", "session");
        let closed_lifecycle = plane.capture_session_lifecycle_token(&bind);

        plane.close_session_routes(&bind).await;
        assert!(closed_lifecycle.is_cancelled());

        let reopened_lifecycle = plane.capture_session_lifecycle_token(&bind);
        assert!(!reopened_lifecycle.is_cancelled());
        plane
            .warm_bind_for_lifecycle(&bind, None, Duration::from_secs(30), reopened_lifecycle)
            .await
            .unwrap();

        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
        assert!(plane
            .cached_route_channel_for_module(&bind, "aft")
            .is_some());
        plane.close_session_routes(&bind).await;
        assert_eq!(conn.closed_routes.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cold_multi_module_batch_opens_routes_concurrently() {
        let conn = ScriptedToolPlaneConnection::new(vec![Vec::new()], Vec::new());
        let plane = SubcToolPlane::with_connection(conn.clone(), "");
        let module_ids = ["module-a", "module-b", "module-c"];
        for module_id in module_ids {
            plane.windows.lock().unwrap().insert(module_id.into(), 1);
            conn.script_route_open(module_id, Duration::from_millis(25), None);
        }
        let calls = [call("tool-a"), call("tool-b"), call("tool-c")];
        let tools = [
            request_tool("tool-a", Some("module-a")),
            request_tool("tool-b", Some("module-b")),
            request_tool("tool-c", Some("module-c")),
        ];

        plane
            .batch_started(
                &batch_ctx("batch-overlap"),
                &calls,
                &ToolManifest::from_tools(tools.to_vec()),
            )
            .await
            .unwrap();

        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            conn.max_concurrent_route_opens.load(Ordering::SeqCst),
            3,
            "all three cold route opens must overlap; the old serial loop observed one"
        );
    }

    #[tokio::test]
    async fn concurrent_route_setup_returns_first_btreeset_error() {
        let conn = ScriptedToolPlaneConnection::new(vec![Vec::new()], Vec::new());
        let plane = SubcToolPlane::with_connection(conn.clone(), "");
        for module_id in ["module-a", "module-b"] {
            plane.windows.lock().unwrap().insert(module_id.into(), 1);
        }
        conn.script_route_open(
            "module-a",
            Duration::from_millis(40),
            Some("module-a failed after module-b"),
        );
        conn.script_route_open(
            "module-b",
            Duration::from_millis(1),
            Some("module-b failed first"),
        );
        let calls = [call("tool-a"), call("tool-b")];
        let tools = [
            request_tool("tool-a", Some("module-a")),
            request_tool("tool-b", Some("module-b")),
        ];

        let error = plane
            .batch_started(
                &batch_ctx("batch-errors"),
                &calls,
                &ToolManifest::from_tools(tools.to_vec()),
            )
            .await
            .unwrap_err();

        assert_eq!(
            conn.route_open_completions.lock().unwrap().as_slice(),
            &["module-b", "module-a"],
            "module-b must fail by timing before the BTreeSet-first module-a"
        );
        assert_eq!(
            error.to_string(),
            "tool plane unavailable (route bind or transport failed): \
             subc transport: module-a failed after module-b"
        );
    }

    #[tokio::test]
    async fn warm_single_module_batch_reuses_the_cached_route() {
        let conn = ScriptedToolPlaneConnection::new(
            vec![Vec::new()],
            vec![
                successful_tool_reply("first"),
                successful_tool_reply("second"),
            ],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "");
        plane.windows.lock().unwrap().insert("module-a".into(), 1);
        let tool_call = call("tool-a");
        let tools = [request_tool("tool-a", Some("module-a"))];

        let first = batch_ctx("batch-cold");
        plane
            .batch_started(
                &first,
                std::slice::from_ref(&tool_call),
                &ToolManifest::from_tools(tools.to_vec()),
            )
            .await
            .unwrap();
        let route = plane
            .cached_route_channel_for_module(&first.bind, "module-a")
            .expect("the cold batch caches its route");
        assert_completed_success(plane.dispatch(&first, &tool_call).await);
        plane.batch_finished(&first).await.unwrap();

        let second = batch_ctx("batch-warm");
        plane
            .batch_started(
                &second,
                std::slice::from_ref(&tool_call),
                &ToolManifest::from_tools(tools.to_vec()),
            )
            .await
            .unwrap();
        assert_completed_success(plane.dispatch(&second, &tool_call).await);
        plane.batch_finished(&second).await.unwrap();

        assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(conn.max_concurrent_route_opens.load(Ordering::SeqCst), 1);
        assert_eq!(
            plane.cached_route_channel_for_module(&second.bind, "module-a"),
            Some(route)
        );
    }

    fn legacy_map_call_tool_result(id: &ToolCallId, raw: &serde_json::Value) -> ToolResult {
        let is_error = raw
            .get("isError")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let text = raw
            .get("content")
            .and_then(serde_json::Value::as_array)
            .map(|blocks| {
                blocks
                    .iter()
                    .filter_map(|block| block.get("text").and_then(serde_json::Value::as_str))
                    .collect::<Vec<_>>()
                    .join("")
            })
            .unwrap_or_default();
        let output = if is_error {
            ToolOutput::Error { message: text }
        } else {
            ToolOutput::Text { text }
        };
        ToolResult {
            tool_call_id: id.clone(),
            output,
            is_error,
        }
    }

    #[test]
    fn image_only_tool_result_maps_to_a_compact_placeholder() {
        let id = ToolCallId("t1".into());
        let raw = serde_json::json!({
            "content": [{
                "type": "image",
                "mimeType": "image/png",
                "data": "aGVsbG8="
            }],
            "isError": false
        });

        assert!(matches!(
            legacy_map_call_tool_result(&id, &raw).output,
            ToolOutput::Text { text } if text.is_empty()
        ));
        let result = map_call_tool_result(&id, &raw);
        assert!(!result.is_error);
        assert_eq!(
            result.output,
            ToolOutput::Text {
                text: "[non-text tool content omitted: type=image mimeType=image/png data≈8 bytes]"
                    .into()
            }
        );
    }

    #[test]
    fn mixed_tool_content_preserves_source_order_with_placeholders() {
        let raw = serde_json::json!({
            "content": [
                { "type": "text", "text": "before" },
                { "type": "image", "mimeType": "image/jpeg", "data": "abcd" },
                { "type": "text", "text": "after" }
            ],
            "isError": false
        });

        let result = map_call_tool_result(&ToolCallId("t1".into()), &raw);
        assert_eq!(
            result.output,
            ToolOutput::Text {
                text: "before[non-text tool content omitted: type=image mimeType=image/jpeg data≈4 bytes]after"
                    .into()
            }
        );
    }

    #[test]
    fn structured_content_only_maps_to_json() {
        let structured = serde_json::json!({"temperature": 12, "unit": "C"});
        let raw = serde_json::json!({
            "structuredContent": structured.clone(),
            "isError": false
        });

        let result = map_call_tool_result(&ToolCallId("t1".into()), &raw);
        assert!(!result.is_error);
        assert_eq!(result.output, ToolOutput::Json { value: structured });
    }

    #[test]
    fn text_only_tool_result_is_byte_identical_to_legacy_mapping() {
        let id = ToolCallId("t1".into());
        let raw = serde_json::json!({
            "content": [
                { "type": "text", "text": "12:" },
                { "type": "text", "text": "00" }
            ],
            "structuredContent": {"ignored": true},
            "isError": false
        });

        let legacy = serde_json::to_vec(&legacy_map_call_tool_result(&id, &raw)).unwrap();
        let mapped = serde_json::to_vec(&map_call_tool_result(&id, &raw)).unwrap();
        assert_eq!(mapped, legacy);
    }

    #[test]
    fn error_with_non_text_content_keeps_error_and_placeholder() {
        let raw = serde_json::json!({
            "content": [{ "type": "audio", "mimeType": "audio/wav", "data": "123456" }],
            "isError": true
        });

        let result = map_call_tool_result(&ToolCallId("t1".into()), &raw);
        assert!(result.is_error);
        assert_eq!(
            result.output,
            ToolOutput::Error {
                message:
                    "[non-text tool content omitted: type=audio mimeType=audio/wav data≈6 bytes]"
                        .into()
            }
        );
    }

    #[test]
    fn reply_without_content_structured_content_or_is_error_is_an_error_quoting_raw_reply() {
        let raw = serde_json::json!({
            "result": { "hits": ["a", "b"] }
        });

        let result = map_call_tool_result(&ToolCallId("t1".into()), &raw);
        assert!(result.is_error);
        let ToolOutput::Error { message } = result.output else {
            panic!("expected an error output, got {:?}", result.output);
        };
        assert!(
            message.contains("none of `content`, `structuredContent` or `isError`"),
            "{message}"
        );
        assert!(
            message.ends_with(r#"raw reply: {"result":{"hits":["a","b"]}}"#),
            "{message}"
        );
    }

    #[test]
    fn empty_content_array_stays_an_empty_success() {
        let raw = serde_json::json!({ "content": [] });

        let result = map_call_tool_result(&ToolCallId("t1".into()), &raw);
        assert!(!result.is_error);
        assert_eq!(
            result.output,
            ToolOutput::Text {
                text: String::new()
            }
        );
    }

    #[test]
    fn structured_content_alone_without_is_error_still_maps_to_json() {
        let structured = serde_json::json!({"answer": 42});
        let raw = serde_json::json!({ "structuredContent": structured.clone() });

        let result = map_call_tool_result(&ToolCallId("t1".into()), &raw);
        assert!(!result.is_error);
        assert_eq!(result.output, ToolOutput::Json { value: structured });
    }

    #[test]
    fn malformed_reply_quote_is_truncated_to_the_bound() {
        // Multi-byte characters make the byte bound land mid-character, so this also
        // proves the cut backs off to a char boundary instead of panicking.
        let raw =
            serde_json::json!({ "result": "é".repeat(10 * MALFORMED_TOOL_REPLY_QUOTE_MAX_BYTES) });
        let full_len = serde_json::to_string(&raw).unwrap().len();

        let result = map_call_tool_result(&ToolCallId("t1".into()), &raw);
        assert!(result.is_error);
        let ToolOutput::Error { message } = result.output else {
            panic!("expected an error output");
        };
        let quoted = message.split_once("raw reply: ").unwrap().1;
        let (body, marker) = quoted.split_once('…').unwrap();
        assert!(body.len() <= MALFORMED_TOOL_REPLY_QUOTE_MAX_BYTES);
        assert!(body.len() > MALFORMED_TOOL_REPLY_QUOTE_MAX_BYTES - 4);
        assert!(body.starts_with(r#"{"result":"éé"#));
        assert_eq!(
            marker,
            format!(" [truncated: {} of {full_len} bytes shown]", body.len())
        );
    }

    #[test]
    fn maps_error_call_tool_result() {
        let raw = serde_json::json!({
            "content": [{ "type": "text", "text": "boom" }],
            "isError": true
        });
        let r = map_call_tool_result(&ToolCallId("t1".into()), &raw);
        assert!(r.is_error);
        assert!(matches!(r.output, ToolOutput::Error { message } if message == "boom"));
    }

    fn result(text: &str) -> ToolResult {
        ToolResult {
            tool_call_id: ToolCallId("ignored".into()),
            output: ToolOutput::Text { text: text.into() },
            is_error: false,
        }
    }

    fn session_key(project_root: &str, harness: &str, session: &str) -> SessionKey {
        SessionKey {
            project_root: PathBuf::from(project_root),
            harness: harness.into(),
            session: session.into(),
        }
    }

    fn route_key(project_root: &str, harness: &str, session: &str, module_id: &str) -> RouteKey {
        RouteKey {
            session: session_key(project_root, harness, session),
            scope: None,
            module_id: module_id.into(),
        }
    }

    #[tokio::test]
    async fn dispatch_uses_the_routed_providers_frozen_preset() {
        let conn = ScriptedToolPlaneConnection::new(
            (0..3)
                .map(|_| {
                    vec![
                        catalog_entry("aft", &["read"]),
                        catalog_entry("other", &["look"]),
                    ]
                })
                .collect(),
            (0..3)
                .map(|_| Ok(serde_json::json!({"content": []})))
                .collect(),
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        let mut frozen = broca_wal::PlanManifest {
            declarations: Vec::new(),
            plan: broca_wal::OpaqueJson::from_json_text(
                serde_json::json!({"tool_items": [
                    {"provider": "other", "preset": "head"}, {"provider": "aft", "preset": "worker"}
                ]})
                .to_string(),
            )
            .unwrap(),
            plan_identity: String::new(),
            composition_digest: String::new(),
            role_versions: Default::default(),
            fetches: Vec::new(),
            tools: Arc::new(Vec::new()),
            system_text: String::new(),
            system_digest: String::new(),
            join_version: 1,
        };
        let tools = vec![
            request_tool("read", Some("aft")),
            request_tool("look", Some("other")),
        ];
        let manifest = ToolManifest::from_tools(tools.clone())
            .with_plan_manifest(&frozen)
            .unwrap();
        // Mutating a later plan value cannot change the already constructed manifest.
        frozen.plan = broca_wal::OpaqueJson::from_json_text(r#"{"tool_items": []}"#).unwrap();
        let ctx = batch_ctx_for("preset", "project", "planned");
        for name in ["read", "look"] {
            let call = call(name);
            plane
                .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
                .await
                .unwrap();
            assert!(matches!(
                plane.dispatch(&ctx, &call).await,
                DispatchResult::Completed(_)
            ));
            plane.batch_finished(&ctx).await.unwrap();
        }
        let call = call("read");
        plane
            .batch_started(
                &ctx,
                std::slice::from_ref(&call),
                &ToolManifest::from_tools(tools),
            )
            .await
            .unwrap();
        plane.dispatch(&ctx, &call).await;
        plane.batch_finished(&ctx).await.unwrap();
        assert_eq!(
            *conn.dispatched_presets.lock().unwrap(),
            [Some("worker".into()), Some("head".into()), None]
        );
        assert!(plane.batch_presets.lock().unwrap().is_empty());
    }

    fn request_tool(name: &str, module: Option<&str>) -> Tool {
        Tool {
            name: name.into(),
            description: String::new(),
            input_schema: serde_json::json!({"type": "object"}),
            module: module.map(ToOwned::to_owned),
            execution_mode: broca_protocol::request::ExecutionMode::Parallel,
        }
    }

    fn catalog_entry(module_id: &str, tool_names: &[&str]) -> CatalogEntry {
        // Routing uses module id and tool names, not build versions or capabilities. The fixture
        // represents a ready module, rather than the daemon's draining/reloading state.
        CatalogEntry::new(
            module_id,
            vec![ProviderRole::ToolProvider {
                tools: tool_names
                    .iter()
                    .map(|name| subc_protocol::manifest::Tool {
                        name: (*name).into(),
                        description: None,
                        execution_mode: subc_protocol::manifest::ExecutionMode::Pure,
                        schema: serde_json::json!({"type": "object"}),
                    })
                    .collect(),
                identity_scope: Vec::new(),
                concurrency: Concurrency::Serial,
                emits_push: false,
                sub_supervises: true,
            }],
            Vec::new(),
        )
        .with_ready(true)
    }

    fn call(tool_name: &str) -> ToolCall {
        ToolCall {
            thought_signature: None,
            tool_call_id: ToolCallId(format!("call-{tool_name}")),
            tool_name: tool_name.into(),
            input: serde_json::json!({}),
        }
    }

    fn assert_failure_result_names_tool(route: ToolRoute, tool_name: &str) {
        let ToolRoute::Failure(message) = route else {
            panic!("expected route failure for {tool_name}");
        };
        let result = route_failure_result(&call(tool_name), message);
        assert!(
            result.is_error,
            "routing failures are model-visible tool errors"
        );
        assert!(
            matches!(result.output, ToolOutput::Error { message } if message.contains(tool_name)),
            "routing failure message should name the unresolved tool"
        );
    }

    #[tokio::test]
    async fn consecutive_batches_share_one_catalog_snapshot() {
        let catalog = vec![catalog_entry("module-m", &["echo"])];
        let conn = ScriptedToolPlaneConnection::new(
            vec![catalog.clone(), catalog],
            vec![
                successful_tool_reply("first"),
                successful_tool_reply("second"),
            ],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "");
        plane.windows.lock().unwrap().insert("module-m".into(), 1);
        let tool_call = call("echo");
        let tools = [request_tool("echo", None)];

        let first = batch_ctx("batch-1");
        plane
            .batch_started(
                &first,
                std::slice::from_ref(&tool_call),
                &ToolManifest::from_tools(tools.to_vec()),
            )
            .await
            .unwrap();
        assert_completed_success(plane.dispatch(&first, &tool_call).await);
        plane.batch_finished(&first).await.unwrap();

        let second = batch_ctx("batch-2");
        plane
            .batch_started(
                &second,
                std::slice::from_ref(&tool_call),
                &ToolManifest::from_tools(tools.to_vec()),
            )
            .await
            .unwrap();
        assert_completed_success(plane.dispatch(&second, &tool_call).await);
        plane.batch_finished(&second).await.unwrap();

        assert_eq!(
            conn.catalog_calls.load(Ordering::SeqCst),
            1,
            "the second batch must reuse the plane's decoded catalog snapshot"
        );
    }

    #[tokio::test]
    async fn overlapping_sessions_keep_batch_routes_isolated_at_terminal() {
        let catalog = vec![
            catalog_entry("module-a", &["echo"]),
            catalog_entry("module-b", &["echo"]),
        ];
        let conn = ScriptedToolPlaneConnection::new(
            vec![catalog],
            vec![
                successful_tool_reply("from-a"),
                successful_tool_reply("from-b"),
            ],
        );
        let plane = SubcToolPlane::with_connection(conn, "");
        plane.windows.lock().unwrap().insert("module-a".into(), 1);
        plane.windows.lock().unwrap().insert("module-b".into(), 1);
        let tool_call = call("echo");
        let ctx_a = batch_ctx_for("shared-batch", "/tmp/a", "session-a");
        let ctx_b = batch_ctx_for("shared-batch", "/tmp/b", "session-b");

        plane
            .batch_started(
                &ctx_a,
                std::slice::from_ref(&tool_call),
                &ToolManifest::from_tools(vec![request_tool("echo", Some("module-a"))]),
            )
            .await
            .unwrap();
        plane
            .batch_started(
                &ctx_b,
                std::slice::from_ref(&tool_call),
                &ToolManifest::from_tools(vec![request_tool("echo", Some("module-b"))]),
            )
            .await
            .unwrap();

        {
            let batches = plane.batch_routes.lock().unwrap();
            assert_eq!(batches.len(), 2);
            assert_eq!(
                batches
                    .get(&SubcToolPlane::batch_route_key(&ctx_a))
                    .and_then(|routes| routes.get(&tool_call.tool_call_id)),
                Some(&ToolRoute::Module("module-a".into()))
            );
            assert_eq!(
                batches
                    .get(&SubcToolPlane::batch_route_key(&ctx_b))
                    .and_then(|routes| routes.get(&tool_call.tool_call_id)),
                Some(&ToolRoute::Module("module-b".into()))
            );
        }
        // The module each batch's intent records is the one that batch then dispatches to.
        assert_eq!(
            plane
                .dispatch_target(&ctx_a, &tool_call.tool_call_id)
                .as_deref(),
            Some("module-a")
        );
        assert_eq!(
            plane
                .dispatch_target(&ctx_b, &tool_call.tool_call_id)
                .as_deref(),
            Some("module-b")
        );

        assert_completed_success(plane.dispatch(&ctx_a, &tool_call).await);
        plane.batch_finished(&ctx_a).await.unwrap();
        assert!(plane
            .batch_routes
            .lock()
            .unwrap()
            .contains_key(&SubcToolPlane::batch_route_key(&ctx_b)));
        assert_completed_success(plane.dispatch(&ctx_b, &tool_call).await);
        plane.batch_finished(&ctx_b).await.unwrap();
        assert!(plane.batch_routes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dispatch_invalidation_removes_only_the_targeted_call_route() {
        let catalog = vec![catalog_entry("module-m", &["echo-a", "echo-b"])];
        let conn = ScriptedToolPlaneConnection::new(
            vec![catalog],
            vec![Err(ConnError::Transport("connection reset".into()))],
        );
        let plane = SubcToolPlane::with_connection(conn, "");
        plane.windows.lock().unwrap().insert("module-m".into(), 2);
        let first = call("echo-a");
        let second = call("echo-b");
        let calls = [first.clone(), second.clone()];
        let tools = [
            request_tool("echo-a", Some("module-m")),
            request_tool("echo-b", Some("module-m")),
        ];
        let ctx = batch_ctx("batch-invalidation-scope");
        plane
            .batch_started(&ctx, &calls, &ToolManifest::from_tools(tools.to_vec()))
            .await
            .unwrap();

        assert!(matches!(
            plane.dispatch(&ctx, &first).await,
            DispatchResult::SentButOutcomeUnknown { .. }
        ));

        let batches = plane.batch_routes.lock().unwrap();
        let routes = batches
            .get(&SubcToolPlane::batch_route_key(&ctx))
            .expect("the other call keeps the batch alive");
        assert!(!routes.contains_key(&first.tool_call_id));
        assert_eq!(
            routes.get(&second.tool_call_id),
            Some(&ToolRoute::Module("module-m".into()))
        );
    }

    #[tokio::test]
    async fn close_session_removes_all_of_its_batches_only() {
        let conn = ScriptedToolPlaneConnection::new(Vec::new(), Vec::new());
        let plane = SubcToolPlane::with_connection(conn, "");
        let ctx_a1 = batch_ctx_for("batch-1", "/tmp/a", "session-a");
        let ctx_a2 = batch_ctx_for("batch-2", "/tmp/a", "session-a");
        let ctx_b = batch_ctx_for("batch-1", "/tmp/b", "session-b");
        let route = HashMap::from([(
            ToolCallId("call".into()),
            ToolRoute::Module("module-m".into()),
        )]);
        {
            let mut batches = plane.batch_routes.lock().unwrap();
            batches.insert(SubcToolPlane::batch_route_key(&ctx_a1), route.clone());
            batches.insert(SubcToolPlane::batch_route_key(&ctx_a2), route.clone());
            batches.insert(SubcToolPlane::batch_route_key(&ctx_b), route);
        }

        plane.close_session_routes(&ctx_a1.bind).await;

        let batches = plane.batch_routes.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert!(batches.contains_key(&SubcToolPlane::batch_route_key(&ctx_b)));
    }

    #[tokio::test]
    async fn timed_out_dispatch_closes_the_possibly_live_route() {
        let catalog = vec![catalog_entry("module-m", &["echo"])];
        let conn = ScriptedToolPlaneConnection::new(
            vec![catalog],
            vec![Err(ConnError::Transport("timed out after 120s".into()))],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "");
        plane.windows.lock().unwrap().insert("module-m".into(), 1);
        let tool_call = call("echo");
        let tools = [request_tool("echo", None)];
        let ctx = batch_ctx("batch-timeout");
        plane
            .batch_started(
                &ctx,
                std::slice::from_ref(&tool_call),
                &ToolManifest::from_tools(tools.to_vec()),
            )
            .await
            .unwrap();

        assert!(matches!(
            plane.dispatch(&ctx, &tool_call).await,
            DispatchResult::SentButOutcomeUnknown { .. }
        ));
        wait_for_closed_routes(&conn, 1).await;

        assert_eq!(
            conn.closed_routes.lock().unwrap().as_slice(),
            &[RouteHandle::synthetic(1)],
            "revert check: dropping the cache entry without the fix emits no route GOODBYE"
        );
    }

    #[tokio::test]
    async fn route_gone_dispatch_does_not_send_a_second_goodbye() {
        let catalog = vec![catalog_entry("module-m", &["echo"])];
        let conn = ScriptedToolPlaneConnection::new(
            vec![catalog],
            vec![Err(ConnError::Transport(
                "route gone (module endpoint dropped)".into(),
            ))],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "");
        plane.windows.lock().unwrap().insert("module-m".into(), 1);
        let tool_call = call("echo");
        let tools = [request_tool("echo", None)];
        let ctx = batch_ctx("batch-route-gone");
        plane
            .batch_started(
                &ctx,
                std::slice::from_ref(&tool_call),
                &ToolManifest::from_tools(tools.to_vec()),
            )
            .await
            .unwrap();

        assert!(matches!(
            plane.dispatch(&ctx, &tool_call).await,
            DispatchResult::SentButOutcomeUnknown { .. }
        ));
        tokio::task::yield_now().await;

        assert!(
            conn.closed_routes.lock().unwrap().is_empty(),
            "the daemon already freed a route that produced route-gone GOODBYE"
        );
    }

    #[tokio::test]
    async fn dispatch_failure_clears_catalog_snapshot_for_next_batch() {
        let catalog = vec![catalog_entry("module-m", &["echo"])];
        let conn = ScriptedToolPlaneConnection::new(
            vec![catalog.clone(), catalog],
            vec![Err(ConnError::Transport("connection reset".into()))],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "");
        plane.windows.lock().unwrap().insert("module-m".into(), 1);
        let tool_call = call("echo");
        let tools = [request_tool("echo", None)];

        let first = batch_ctx("batch-failure");
        plane
            .batch_started(
                &first,
                std::slice::from_ref(&tool_call),
                &ToolManifest::from_tools(tools.to_vec()),
            )
            .await
            .unwrap();
        assert!(matches!(
            plane.dispatch(&first, &tool_call).await,
            DispatchResult::SentButOutcomeUnknown { .. }
        ));
        assert_eq!(
            plane.cached_route_channel_for_module(&first.bind, "module-m"),
            None
        );
        plane.batch_finished(&first).await.unwrap();

        let second = batch_ctx("batch-after-failure");
        plane
            .batch_started(
                &second,
                std::slice::from_ref(&tool_call),
                &ToolManifest::from_tools(tools.to_vec()),
            )
            .await
            .unwrap();
        assert_eq!(
            conn.catalog_calls.load(Ordering::SeqCst),
            2,
            "a learned-dead route must force the following batch to fetch a fresh catalog"
        );
    }

    /// THE PROVIDER ID REACHES THE WIRE.
    ///
    /// Until 2026-09-03 the envelope was `{name, arguments}` with no id at all, so every module
    /// downstream either synthesized an identity or refused the call -- one of them silently for
    /// weeks, which is the failure this pins.
    ///
    /// SCOPED TO ONE DISPATCH ON PURPOSE. My first version also asserted that a transparent
    /// retry reuses the id, and it HUNG the suite: driving an `unknown_channel` rejection evicts
    /// the route and opens a fresh one, which lands in warm_bind's unbounded backoff waiting for
    /// a session lifecycle cancel a unit test never delivers. The retry property is real and
    /// worth stating, but the honest place for it is the spine test with a live daemon
    /// (tool_fault_spine.rs), not a stub that has to fake the machinery it depends on.
    #[tokio::test]
    async fn dispatch_puts_the_provider_id_on_the_wire() {
        let conn = ScriptedToolPlaneConnection::new(
            vec![Vec::new(), Vec::new(), Vec::new()],
            vec![Ok(serde_json::json!({"content": [], "isError": false}))],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");

        let ctx = batch_ctx("batch-id-on-wire");
        let tool_call = call("read");
        plane
            .batch_started(
                &ctx,
                std::slice::from_ref(&tool_call),
                &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
            )
            .await
            .unwrap();
        let _ = plane.dispatch(&ctx, &tool_call).await;

        let ids = conn.dispatched_ids.lock().unwrap().clone();
        assert_eq!(ids.len(), 1, "one dispatch, one wire call");
        assert_eq!(
            ids[0].as_deref(),
            Some(tool_call.tool_call_id.0.as_str()),
            "the provider id must reach the wire rather than being dropped at the envelope"
        );
    }

    /// A TOOL-MODULE RESTART MID-DISPATCH IS ONE TRANSPARENT RETRY, NOT A MODEL-VISIBLE ERROR.
    ///
    /// Measured live before the fix: an acceptance turn dispatched while AFT was restarting
    /// and the model received `tool/route rejected: module_reloading -- module endpoint for
    /// route channel 1 is reloading`. That code is pre-send by subc-protocol's documented
    /// contract, so the plane may re-dispatch it -- once, with the SAME id.
    ///
    /// Three properties, each carrying weight:
    ///  - the call completes, so the model never sees the restart;
    ///  - the wire saw the same id twice, which is the evidence the retry is a re-send of a
    ///    call the module never received rather than a second call;
    ///  - the evicted route is CLOSED, because unlike a freed `unknown_channel` it is still
    ///    bound on a draining endpoint and forgetting it silently would leak it.
    ///
    /// Bounded by a timeout because an earlier retry test here HUNG: re-opening after an
    /// eviction can wait on a lifecycle cancel a unit test never delivers. If this ever
    /// hangs, it fails in five seconds and says why instead of stalling the suite.
    #[tokio::test]
    async fn module_reloading_mid_dispatch_is_one_transparent_retry() {
        let conn = ScriptedToolPlaneConnection::new(
            vec![Vec::new(), Vec::new(), Vec::new()],
            vec![
                Err(ConnError::Rejected(
                    "module_reloading — module endpoint for route channel 1 is reloading".into(),
                )),
                successful_tool_reply("served after the restart"),
            ],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        let ctx = batch_ctx("batch-reloading");
        let tool_call = call("read");
        plane
            .batch_started(
                &ctx,
                std::slice::from_ref(&tool_call),
                &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
            )
            .await
            .unwrap();

        let outcome =
            tokio::time::timeout(Duration::from_secs(5), plane.dispatch(&ctx, &tool_call))
                .await
                .expect("the retry must re-open without waiting on a lifecycle cancel");
        assert_completed_success(outcome);

        let ids = conn.dispatched_ids.lock().unwrap().clone();
        assert_eq!(ids.len(), 2, "one refused attempt, one retry: {ids:?}");
        assert_eq!(
            ids[0], ids[1],
            "the retry must re-send the SAME id, not mint a second call"
        );

        // NOT CLOSED, deliberately. The route is still bound on the draining endpoint, and a
        // GOODBYE there could cut off a sibling call the drain is waiting for -- or, since the
        // eviction is by key, close a FRESH route a sibling already re-opened. The drain
        // removes the old binding itself.
        tokio::task::yield_now().await;
        assert!(
            conn.closed_routes.lock().unwrap().is_empty(),
            "a reloading route must be forgotten, not closed: a close can cut off an in-flight sibling"
        );
    }

    /// A RESTART LONGER THAN THE DEFAULT 3s STILL RETRIES THROUGH.
    ///
    /// This is the arm the first version of the fix did not have, and why that version was
    /// wrong: its scripted re-open succeeded instantly, so it never met a real drain. Measured
    /// from the daemon's logs, 13 of 16 AFT module_reloading episodes lasted longer than 3s,
    /// clustered at 15-30s. With the default patience every one of those ended in NotSent --
    /// the model still got an error, three seconds later.
    ///
    /// Here the re-open is refused module_reloading for over 7s of (paused) time before the
    /// module comes back. The call must complete, not report NotSent.
    #[tokio::test(start_paused = true)]
    async fn a_reload_longer_than_the_default_patience_still_completes() {
        let conn = ScriptedToolPlaneConnection::new(
            vec![Vec::new(), Vec::new(), Vec::new()],
            vec![
                Err(ConnError::Rejected(
                    "module_reloading \u{2014} module endpoint for route channel 1 is reloading"
                        .into(),
                )),
                successful_tool_reply("served after a long drain"),
            ],
        );
        // The first open (the dispatch's own) succeeds; the re-open after the refusal is
        // refused eight times, then the module is back.
        let mut sequence = vec![RouteOpenScript::default()];
        sequence.extend((0..8).map(|_| {
            RouteOpenScript::rejected("module_reloading \u{2014} module_id 'aft' is reloading")
        }));
        sequence.push(RouteOpenScript::default());
        conn.script_route_open_sequence("aft", sequence);

        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        let ctx = batch_ctx("batch-long-reload");
        let tool_call = call("read");
        plane
            .batch_started(
                &ctx,
                std::slice::from_ref(&tool_call),
                &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
            )
            .await
            .unwrap();

        let started = Instant::now();
        let outcome = plane.dispatch(&ctx, &tool_call).await;
        let waited = started.elapsed();

        assert_completed_success(outcome);
        assert!(
            waited > ROUTE_OPEN_RETRY_DEADLINE,
            "the re-open must have outlasted the default patience to prove anything: {waited:?}"
        );
        assert!(
            waited < RELOAD_REOPEN_PATIENCE.deadline,
            "and still be bounded: {waited:?}"
        );
    }

    /// THE CONTROL: a rejection that is NOT pre-send is still a definite answer.
    ///
    /// Without this arm, a change that retried EVERY rejection would pass the one above -- and
    /// that change is the dangerous one, because a rejection after delivery re-sent is a
    /// non-idempotent tool running twice.
    #[tokio::test]
    async fn a_rejection_that_is_not_pre_send_is_never_redispatched() {
        let conn = ScriptedToolPlaneConnection::new(
            vec![Vec::new(), Vec::new(), Vec::new()],
            vec![Err(ConnError::Rejected(
                "capability_forbidden — not permitted".into(),
            ))],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
        let ctx = batch_ctx("batch-forbidden");
        let tool_call = call("read");
        plane
            .batch_started(
                &ctx,
                std::slice::from_ref(&tool_call),
                &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
            )
            .await
            .unwrap();

        match plane.dispatch(&ctx, &tool_call).await {
            DispatchResult::Completed(result) => assert!(result.is_error),
            other => panic!("expected a model-visible rejection, got {other:?}"),
        }
        assert_eq!(
            conn.tool_calls.load(Ordering::SeqCst),
            1,
            "a definite answer must be delivered, never re-sent"
        );
        assert!(conn.closed_routes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unresolved_tool_refetches_stale_catalog_once_and_succeeds() {
        let stale = vec![catalog_entry("older-module", &["older-tool"])];
        let fresh = vec![catalog_entry("module-m", &["new-tool"])];
        let conn = ScriptedToolPlaneConnection::new(
            vec![stale, fresh],
            vec![successful_tool_reply("resolved after refresh")],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "");
        plane.windows.lock().unwrap().insert("module-m".into(), 1);

        let priming = batch_ctx("batch-prime-stale-catalog");
        plane
            .batch_started(&priming, &[], &ToolManifest::from_tools(Vec::new()))
            .await
            .unwrap();
        plane.batch_finished(&priming).await.unwrap();
        assert_eq!(conn.catalog_calls.load(Ordering::SeqCst), 1);

        let target = batch_ctx("batch-new-module");
        let tool_call = call("new-tool");
        plane
            .batch_started(
                &target,
                std::slice::from_ref(&tool_call),
                &ToolManifest::from_tools(vec![request_tool("new-tool", None)]),
            )
            .await
            .unwrap();
        assert_completed_success(plane.dispatch(&target, &tool_call).await);
        assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            conn.catalog_calls.load(Ordering::SeqCst),
            2,
            "resolution failure must refetch once before becoming model-visible"
        );
    }

    #[test]
    fn duplicate_call_id_keeps_first_calls_route() {
        let duplicate_id = ToolCallId("duplicate".into());
        let mut first = call("first-tool");
        first.tool_call_id = duplicate_id.clone();
        let mut duplicate = call("second-tool");
        duplicate.tool_call_id = duplicate_id.clone();

        let routes = resolve_batch_routes(
            &[first, duplicate],
            &ToolManifest::from_tools(vec![
                request_tool("first-tool", None),
                request_tool("second-tool", None),
            ]),
            &[
                catalog_entry("first-module", &["first-tool"]),
                catalog_entry("second-module", &["second-tool"]),
            ],
            None,
        );

        assert_eq!(routes.len(), 1);
        assert_eq!(
            routes.get(&duplicate_id),
            Some(&ToolRoute::Module("first-module".into())),
            "the first call is the one core dispatches; a collecting last-wins map would route it to second-module",
        );
    }

    #[tokio::test]
    async fn catalog_discovery_keeps_selected_module_when_tool_names_collide() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": { "path": { "type": "string" } }
        });
        let mut selected = catalog_entry("selected-module", &["shared"]);
        let ProviderRole::ToolProvider { tools, .. } = &mut selected.roles[0] else {
            panic!("selected catalog entry must provide tools");
        };
        tools[0].schema = schema.clone();
        let catalog = vec![selected, catalog_entry("colliding-module", &["shared"])];
        let conn = ScriptedToolPlaneConnection::new(
            vec![catalog.clone(), catalog.clone(), catalog],
            vec![successful_tool_reply("selected")],
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), "selected-module");

        let discovered = plane.tools_from_catalog().await.unwrap();
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].module.as_deref(), Some("selected-module"));

        let mut hinted = CallOptions::new(Vec::new());
        hinted.tools = discovered.clone().into();
        let mut hintless = hinted.clone();
        let mut hintless_tools = hintless.tools.as_ref().clone();
        hintless_tools[0].module = None;
        hintless.tools = hintless_tools.into();
        let frozen = openai_chat_frozen_config("test-model");
        let hinted_wire = OpenAiChat.render(&frozen, &hinted);
        assert_eq!(
            hinted_wire,
            OpenAiChat.render(&frozen, &hintless),
            "routing metadata must not change provider request bytes"
        );
        let body: serde_json::Value = serde_json::from_slice(&hinted_wire).unwrap();
        assert_eq!(
            body.pointer("/tools/0/function/name"),
            Some(&serde_json::json!("shared"))
        );
        assert_eq!(body.pointer("/tools/0/function/parameters"), Some(&schema));

        let manifest = ToolManifest::from_tools(discovered);
        let ctx = batch_ctx("catalog-selected-module");
        let call = call("shared");
        plane
            .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
            .await
            .unwrap();
        assert_completed_success(plane.dispatch(&ctx, &call).await);
        assert_eq!(
            conn.route_open_completions.lock().unwrap().as_slice(),
            ["selected-module"]
        );
    }

    #[test]
    fn resolves_caller_hint_before_catalog() {
        let route = resolve_tool_module(
            "shared",
            &ToolManifest::from_tools(vec![request_tool("shared", Some("hinted"))]),
            &[catalog_entry("catalog", &["shared"])],
            Some("default"),
        );
        assert_eq!(route, ToolRoute::Module("hinted".into()));
    }

    #[test]
    fn resolves_catalog_unique_provider() {
        let route = resolve_tool_module(
            "unique",
            &ToolManifest::from_tools(vec![request_tool("unique", None)]),
            &[catalog_entry("catalog", &["unique"])],
            Some("default"),
        );
        assert_eq!(route, ToolRoute::Module("catalog".into()));
    }

    #[test]
    fn ambiguous_without_hint_fails_the_call() {
        let route = resolve_tool_module(
            "shared",
            &ToolManifest::from_tools(vec![request_tool("shared", None)]),
            &[
                catalog_entry("module-a", &["shared"]),
                catalog_entry("module-b", &["shared"]),
            ],
            Some("default"),
        );
        assert_failure_result_names_tool(route, "shared");
    }

    #[test]
    fn default_module_fallback_preserves_single_module_behavior() {
        let route = resolve_tool_module(
            "unknown_to_catalog",
            &ToolManifest::from_tools(vec![request_tool("unknown_to_catalog", None)]),
            &[],
            Some("aft"),
        );
        assert_eq!(route, ToolRoute::Module("aft".into()));
    }

    #[test]
    fn unknown_everywhere_without_default_fails_the_call() {
        let route = resolve_tool_module(
            "missing",
            &ToolManifest::from_tools(vec![request_tool("missing", None)]),
            &[],
            None,
        );
        assert_failure_result_names_tool(route, "missing");
    }

    /// COLLISION-SAFETY: the SAME tool_call_id under TWO different sessions must NOT
    /// false-dedup — the fence keys on (session, tool_call_id), so each session's call sees
    /// only its own result. Keying by the id alone (the old bug) would return session A's
    /// result for session B's identically-named call.
    #[test]
    fn fence_does_not_cross_session_dedup() {
        let mut fence = Fence::default();
        let id = ToolCallId("toolu_same".into());
        let batch = BatchId("b1".into());
        // Session A dispatches the call and gets result "A".
        fence.insert(
            &batch,
            (session_key("/tmp/a", "runner", "sessionA"), id.clone()),
            result("A"),
        );

        // Session B's identically-named call is NOT a fence hit (different session key).
        assert_eq!(
            fence.get(&(session_key("/tmp/b", "runner", "sessionB"), id.clone())),
            None,
            "the same tool_call_id under a DIFFERENT session must not be a fence hit"
        );
        // Session A's repeat IS a hit (same session + id) and returns the exact fenced result.
        let hit = fence.get(&(session_key("/tmp/a", "runner", "sessionA"), id));
        assert_eq!(
            hit,
            Some(result("A")),
            "session A's repeat call returns its original result"
        );
    }

    /// EVICTION: a batch's fence entries are gone after its terminal — bounded growth.
    /// Without eviction the fence grows unbounded over thousands of batches.
    #[test]
    fn fence_evicts_a_batchs_entries_on_terminal() {
        let mut fence = Fence::default();
        let b1 = BatchId("b1".into());
        let b2 = BatchId("b2".into());
        let session = session_key("/tmp/s", "runner", "session");
        fence.insert(
            &b1,
            (session.clone(), ToolCallId("c1".into())),
            result("r1"),
        );
        fence.insert(
            &b1,
            (session.clone(), ToolCallId("c2".into())),
            result("r2"),
        );
        fence.insert(
            &b2,
            (session.clone(), ToolCallId("c3".into())),
            result("r3"),
        );
        assert_eq!(fence.len(), 3);

        // Evicting b1 drops ONLY b1's two entries; b2's survives.
        fence.evict_batch(&b1);
        assert_eq!(fence.len(), 1, "only b1's entries evicted");
        assert!(fence
            .get(&(session.clone(), ToolCallId("c3".into())))
            .is_some());
        assert!(fence
            .get(&(session.clone(), ToolCallId("c1".into())))
            .is_none());

        fence.evict_batch(&b2);
        assert_eq!(
            fence.len(),
            0,
            "the fence is empty after all batches terminate"
        );
    }

    #[test]
    fn structured_session_key_prevents_separator_collisions() {
        let bind_a = BindIdentity::new(PathBuf::from("/tmp/proj\u{1f}runner"), "a", "b");
        let bind_b = BindIdentity::new(PathBuf::from("/tmp/proj"), "runner\u{1f}a", "b");

        let legacy_a = format!(
            "{}\u{1f}{}\u{1f}{}",
            bind_a.project_root.display(),
            bind_a.harness,
            bind_a.session
        );
        let legacy_b = format!(
            "{}\u{1f}{}\u{1f}{}",
            bind_b.project_root.display(),
            bind_b.harness,
            bind_b.session
        );
        assert_eq!(legacy_a, legacy_b, "the old string key would collide");
        assert_ne!(
            SubcToolPlane::session_key(&bind_a),
            SubcToolPlane::session_key(&bind_b),
            "the structured key keeps distinct binds distinct"
        );
    }

    #[test]
    fn cache_route_state_keeps_first_route_and_reports_the_loser() {
        let key = route_key("/tmp/proj", "runner", "s1", "aft");
        let first_sem = Arc::new(Semaphore::new(1));
        let second_sem = Arc::new(Semaphore::new(2));
        let mut routes = HashMap::new();

        let first_handle = RouteHandle::synthetic(11);
        let second_handle = RouteHandle::synthetic(12);
        let first = cache_route_state(
            &mut routes,
            key.clone(),
            first_handle,
            Arc::clone(&first_sem),
            None,
        );
        assert_eq!(first.handle, first_handle);
        assert!(first.loser_handle.is_none());
        assert!(Arc::ptr_eq(&first.sem, &first_sem));

        let second = cache_route_state(
            &mut routes,
            key.clone(),
            second_handle,
            Arc::clone(&second_sem),
            None,
        );
        assert_eq!(
            second.handle, first_handle,
            "the first cached route stays authoritative"
        );
        assert_eq!(
            second.loser_handle,
            Some(second_handle),
            "the redundant daemon route must be closed"
        );
        assert!(Arc::ptr_eq(&second.sem, &first_sem));
        assert_eq!(routes.get(&key).unwrap().handle, first_handle);
    }

    #[test]
    fn route_match_rejects_a_replaced_route_even_if_the_channel_number_is_reused() {
        let key = route_key("/tmp/proj", "runner", "s1", "aft");
        let old_sem = Arc::new(Semaphore::new(1));
        let new_sem = Arc::new(Semaphore::new(1));
        let mut routes = HashMap::new();

        let old_handle = RouteHandle::synthetic_epoch(11, 1);
        let new_handle = RouteHandle::synthetic_epoch(11, 2);
        routes.insert(
            key.clone(),
            RouteState {
                handle: old_handle,
                sem: Arc::clone(&old_sem),
                stamp: None,
            },
        );
        routes.insert(
            key.clone(),
            RouteState {
                handle: new_handle,
                sem: Arc::clone(&new_sem),
                stamp: None,
            },
        );

        assert!(
            !route_matches(&routes, &key, old_handle, &old_sem),
            "a waiting task must not send on a route that was invalidated and replaced"
        );
        assert!(route_matches(&routes, &key, new_handle, &new_sem));
    }

    /// A tool plane connection whose route open is pre-granted but whose TOOL CALLS go through
    /// the real `SubcConnection` request path, so the reply deadline under test is the one the
    /// production connection actually applies, not a re-implementation of it.
    struct InProcessWireConnection {
        conn: SubcConnection,
        route: RouteHandle,
    }

    #[tokio::test]
    async fn reply_deadline_vectors_reach_real_dispatch_selection() {
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../../broca-fetch-plan/tests/fixtures/tool-provider-v1/reply-deadline.json"
        ))
        .unwrap();
        for case in vectors["deadlines"].as_array().unwrap() {
            let reply = if case["reply"].is_null() {
                serde_json::Value::Null
            } else {
                serde_json::json!({"read": case["reply"]})
            };
            let tool = serde_json::json!({"name":"read", "description":"probe", "input_schema": vectors["input_schema"], "module":"aft", "execution_mode":"parallel"});
            let frozen: broca_wal::PlanManifest = serde_json::from_value(serde_json::json!({
                "plan": r#"{"tool_items":[{"provider":"aft","preset":"worker"}]}"#,
                "plan_identity":"", "composition_digest":"", "fetches":[{
                    "provider":"aft", "catalog_digest":"", "tools":[tool], "reply":reply
                }], "tools":[tool], "system_text":"", "system_digest":"", "join_version":1, "role_versions":{}
            })).unwrap();
            // Decode the same bytes a resumed run reads before building its dispatch manifest.
            let manifest = ToolManifest::from_tools(frozen.tools.as_ref().clone())
                .with_plan_manifest(&frozen)
                .unwrap();
            let conn = ScriptedToolPlaneConnection::new(
                vec![vec![]],
                vec![Ok(serde_json::json!({"content":[], "isError":false}))],
            );
            let fallback = case
                .get("runner_fallback_ms")
                .unwrap_or(&vectors["runner_fallback_ms"])
                .as_u64()
                .unwrap();
            let plane = SubcToolPlane::with_connection(conn.clone(), "aft")
                .with_reply_fallback_for_test(fallback);
            plane.windows.lock().unwrap().insert("aft".into(), 1);
            let call = ToolCall {
                tool_call_id: ToolCallId("deadline-vector".into()),
                tool_name: "read".into(),
                input: case["arguments"].clone(),
                thought_signature: None,
            };
            let ctx = batch_ctx("deadline-vector");
            plane
                .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
                .await
                .unwrap();
            assert!(matches!(
                plane.dispatch(&ctx, &call).await,
                DispatchResult::Completed(_)
            ));
            assert_eq!(
                *conn.dispatched_deadlines.lock().unwrap(),
                [Duration::from_millis(case["expected_ms"].as_u64().unwrap())],
                "{}",
                case["name"]
            );
            assert_eq!(
                *conn.dispatched_arguments.lock().unwrap(),
                [case["arguments"].clone()]
            );
            plane.batch_finished(&ctx).await.unwrap();
        }
    }

    #[async_trait]
    impl ToolPlaneConnection for InProcessWireConnection {
        async fn catalog_list(&self) -> Result<Vec<CatalogEntry>, ConnError> {
            Ok(Vec::new())
        }

        async fn route_open(
            &self,
            _module_id: &str,
            _identity: &BindIdentity,
            _scope: Option<&ScopeSelector>,
        ) -> Result<RouteHandle, ConnError> {
            Ok(self.route)
        }

        async fn server_capabilities(&self) -> Result<Vec<String>, ConnError> {
            Ok(Vec::new())
        }

        fn route_is_open(&self, _route: &RouteHandle) -> bool {
            true
        }

        async fn route_close(&self, _route: &RouteHandle) -> Result<(), ConnError> {
            Ok(())
        }

        async fn tool_call(
            &self,
            route: &RouteHandle,
            name: &str,
            arguments: &serde_json::Value,
            tool_call_id: Option<&str>,
            deadline: Duration,
            preset: Option<&str>,
        ) -> Result<serde_json::Value, ConnError> {
            <SubcConnection as ToolPlaneConnection>::tool_call(
                &self.conn,
                route,
                name,
                arguments,
                tool_call_id,
                deadline,
                preset,
            )
            .await
        }
    }

    /// Dispatch one AFT call whose fake module answers `reply_after` after receiving it, and
    /// report the outcome with the (virtual) time it took.
    async fn dispatch_with_slow_aft_reply(
        tool_name: &str,
        input: serde_json::Value,
        reply_after: Duration,
    ) -> (DispatchResult, Duration) {
        let (conn, route, mut module) = SubcConnection::in_process_for_test(7, 1);
        let plane = SubcToolPlane::with_connection(
            Arc::new(InProcessWireConnection { conn, route }),
            crate::tool_deadline::AFT_MODULE_ID,
        );
        plane
            .windows
            .lock()
            .unwrap()
            .insert(crate::tool_deadline::AFT_MODULE_ID.into(), 1);
        let call = ToolCall {
            thought_signature: None,
            tool_call_id: ToolCallId("call-slow".into()),
            tool_name: tool_name.into(),
            input,
        };
        let ctx = batch_ctx("batch-slow");
        let fake_module = tokio::spawn(async move {
            let request = module.next_request().await;
            sleep(reply_after).await;
            module.reply(
                &request,
                serde_json::to_vec(&serde_json::json!({
                    "content": [{ "type": "text", "text": "tests passed" }],
                    "isError": false
                }))
                .unwrap(),
            );
        });
        let started = Instant::now();
        let outcome = plane.dispatch(&ctx, &call).await;
        let elapsed = started.elapsed();
        fake_module.abort();
        (outcome, elapsed)
    }

    /// PAUSED TIME PROVES THE WIRING, NOT A WALL CLOCK: tokio auto-advances the clock whenever
    /// every task is idle, so this shows that the deadline `dispatch` hands the real connection
    /// outlasts a reply arriving after 150 s. Whether AFT really answers in time is AFT's
    /// contract, pinned on their side.
    ///
    /// Completed is what lets the run go on; SentButOutcomeUnknown is what the loop turns into
    /// an Interrupted run, the failure this guards.
    #[tokio::test(start_paused = true)]
    async fn a_waiting_aft_bash_reply_after_120s_completes_the_call() {
        let (outcome, elapsed) = dispatch_with_slow_aft_reply(
            "bash",
            serde_json::json!({"command": "cargo test", "wait": true}),
            Duration::from_secs(150),
        )
        .await;
        assert!(
            matches!(&outcome, DispatchResult::Completed(result) if !result.is_error),
            "a waiting bash call must outlast a 150s reply; got {outcome:?}"
        );
        assert!(elapsed >= Duration::from_secs(150), "elapsed {elapsed:?}");
    }

    /// The control: the same slow reply to a bash call that did NOT ask to hold its reply is
    /// still cut at the 120 s default, so the long deadline is not simply applied everywhere.
    #[tokio::test(start_paused = true)]
    async fn a_non_waiting_bash_reply_after_120s_still_times_out() {
        let (outcome, elapsed) = dispatch_with_slow_aft_reply(
            "bash",
            serde_json::json!({"command": "cargo test"}),
            Duration::from_secs(150),
        )
        .await;
        assert!(
            matches!(&outcome, DispatchResult::SentButOutcomeUnknown { reason } if reason.contains("timed out after 120s")),
            "a non-waiting call keeps the 120s deadline; got {outcome:?}"
        );
        assert!(
            elapsed >= crate::connection::TOOL_CALL_TIMEOUT && elapsed < Duration::from_secs(150),
            "elapsed {elapsed:?}"
        );
    }

    /// The outbound half of the scope carrier: a run admitted under a scope opens its provider
    /// routes under exactly that scope, holds only on the daemon's two retryable scope
    /// refusals, and resolves a call whose route closed by the scope's current state
    /// (`scope.describe`) and by what the call had already done.
    mod scope_carrier {
        use super::*;
        use crate::scope_describe::DescribedScope;
        use subc_protocol::scope::{ScopeAttributes, ScopeKind};

        const OWNER: &str = "prefrontal";

        fn scope(scope_ref: &str, epoch: u64) -> SessionScope {
            SessionScope {
                owner: broca_protocol::BindPrincipal::Reserved {
                    module_id: OWNER.into(),
                },
                scope_ref: scope_ref.into(),
                scope_epoch: epoch,
                flow_id: None,
                sender: None,
                agent_id: None,
                run_id: None,
            }
        }

        fn selector(scope_ref: &str, epoch: u64) -> ScopeSelector {
            ScopeSelector {
                owner: Principal::Reserved {
                    module_id: OWNER.into(),
                },
                scope_ref: scope_ref.into(),
                scope_epoch: Some(epoch),
            }
        }

        /// The stamp attributes named `name` in the fixture file. They live in a JSON fixture
        /// because `scope_stamp_source.rs` allows exactly one place in broca's source files
        /// to name the agent attribute, and that place is not a test.
        fn attributes(name: &str) -> ScopeAttributes {
            let all: HashMap<String, ScopeAttributes> =
                serde_json::from_str(include_str!("../tests/fixtures/scope_attributes.json"))
                    .unwrap();
            all.get(name).cloned().expect("a fixture attribute set")
        }

        fn stamp(scope_ref: &str, epoch: u64, attrs: &str) -> ScopeStamp {
            ScopeStamp::new(
                Principal::Reserved {
                    module_id: OWNER.into(),
                },
                scope_ref,
                epoch,
                ScopeKind::Head,
            )
            .with_attributes(attributes(attrs))
            .with_owner_authorized(true)
        }

        fn live(scope_ref: &str, epoch: u64, attrs: &str) -> Result<DescribedScope, String> {
            Ok(DescribedScope {
                status: ScopeStatus::Live,
                scope_epoch: Some(epoch),
                owner_synced: true,
                owner_configured: true,
                stamp: Some(stamp(scope_ref, epoch, attrs)),
            })
        }

        fn parent_ended(scope_ref: &str, epoch: u64) -> Result<DescribedScope, String> {
            let mut answer = live(scope_ref, epoch, "none").unwrap();
            answer.stamp.as_mut().unwrap().parent_state = Some(ParentState::Ended);
            Ok(answer)
        }

        fn not_live(owner_synced: bool) -> Result<DescribedScope, String> {
            Ok(DescribedScope {
                status: ScopeStatus::NotLive,
                scope_epoch: None,
                owner_synced,
                owner_configured: true,
                stamp: None,
            })
        }

        fn ended(epoch: u64) -> Result<DescribedScope, String> {
            Ok(DescribedScope {
                status: ScopeStatus::Ended,
                scope_epoch: Some(epoch),
                owner_synced: true,
                owner_configured: true,
                stamp: None,
            })
        }

        /// `scope.describe` as a script per ref: answers in order, the last one repeating. A
        /// ref with no script is described live at the epoch its scope was frozen at.
        #[derive(Default)]
        struct ScriptedDescriber {
            scripts: Mutex<HashMap<String, VecDeque<Result<DescribedScope, String>>>>,
            epochs: Mutex<HashMap<String, u64>>,
            calls: AtomicUsize,
        }

        impl ScriptedDescriber {
            fn script(&self, scope_ref: &str, answers: Vec<Result<DescribedScope, String>>) {
                self.scripts
                    .lock()
                    .unwrap()
                    .insert(scope_ref.into(), answers.into());
            }
        }

        #[async_trait]
        impl ScopeDescriber for ScriptedDescriber {
            async fn describe(
                &self,
                _owner: &Principal,
                scope_ref: &str,
            ) -> Result<DescribedScope, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let mut scripts = self.scripts.lock().unwrap();
                match scripts.get_mut(scope_ref) {
                    Some(answers) if answers.len() > 1 => answers.pop_front().unwrap(),
                    Some(answers) => answers.front().cloned().unwrap(),
                    None => {
                        let epoch = self.epochs.lock().unwrap().get(scope_ref).copied();
                        live(scope_ref, epoch.unwrap_or(1), "none")
                    }
                }
            }
        }

        fn ctx(batch: &str, session: &str, run: &str, scope: Option<SessionScope>) -> BatchCtx {
            BatchCtx {
                batch_id: BatchId(batch.into()),
                bind: BindIdentity::new("/tmp/scope-carrier", "test", session),
                run_id: RunId(run.into()),
                scope,
            }
        }

        /// A connection with enough empty catalog answers for any test here, a plane over it
        /// whose `aft` window is already known, and a scripted describer installed on it.
        fn rig(
            tool_replies: Vec<Result<serde_json::Value, ConnError>>,
        ) -> (
            Arc<ScriptedToolPlaneConnection>,
            SubcToolPlane,
            Arc<ScriptedDescriber>,
        ) {
            let conn = ScriptedToolPlaneConnection::new(vec![Vec::new(); 16], tool_replies);
            let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
            plane.windows.lock().unwrap().insert("aft".into(), 4);
            let describer = Arc::new(ScriptedDescriber::default());
            for (scope_ref, epoch) in [("head-a", 3), ("head-b", 9)] {
                describer
                    .epochs
                    .lock()
                    .unwrap()
                    .insert(scope_ref.into(), epoch);
            }
            plane.set_scope_describer(describer.clone());
            (conn, plane, describer)
        }

        fn manifest() -> ToolManifest {
            ToolManifest::from_tools(vec![request_tool("read", Some("aft"))])
        }

        fn route_of(plane: &SubcToolPlane, ctx: &BatchCtx) -> RouteHandle {
            let channel = plane
                .cached_route_channel_for_scope(&ctx.bind, ctx.scope.as_ref(), "aft")
                .expect("the batch opened a route");
            RouteHandle::synthetic(channel)
        }

        /// Start a scoped batch of one `read` call under `head` epoch 1, as run `r`.
        async fn started(plane: &SubcToolPlane) -> (BatchCtx, ToolCall) {
            let scoped = ctx("r-b1", "s", "r", Some(scope("head", 1)));
            let read = call("read");
            plane
                .batch_started(&scoped, std::slice::from_ref(&read), &manifest())
                .await
                .expect("the scoped batch starts");
            (scoped, read)
        }

        fn assert_drained(outcome: DispatchResult, reason: &str, sent: bool) {
            match outcome {
                DispatchResult::ScopeDrained {
                    reason: named,
                    sent: was_sent,
                } => {
                    assert_eq!(named, reason);
                    assert_eq!(was_sent, sent, "sent");
                }
                other => panic!("expected ScopeDrained({reason}, sent: {sent}), got {other:?}"),
            }
        }

        /// Two sessions under two scopes, a third unscoped, and an unscoped run of the first
        /// session: each route opens under the scope of the run it serves and no other, and no
        /// run reuses a route opened under a scope it was not admitted under.
        #[tokio::test]
        async fn no_route_is_opened_or_reused_under_a_scope_the_run_was_not_admitted_under() {
            let (conn, plane, _) = rig((0..4).map(|_| successful_tool_reply("ok")).collect());
            let a = ctx("a-b1", "session-a", "run-a", Some(scope("head-a", 3)));
            let b = ctx("b-b1", "session-b", "run-b", Some(scope("head-b", 9)));
            let c = ctx("c-b1", "session-c", "run-c", None);
            // Session A again, in a run that was not admitted under A's scope.
            let a_unscoped = ctx("a-b2", "session-a", "run-a2", None);
            let read = call("read");
            for ctx in [&a, &b, &c, &a_unscoped] {
                plane
                    .batch_started(ctx, std::slice::from_ref(&read), &manifest())
                    .await
                    .unwrap();
                assert_completed_success(plane.dispatch(ctx, &read).await);
            }

            assert_eq!(
                conn.opened_scopes(),
                vec![
                    ("session-a".to_owned(), Some(selector("head-a", 3))),
                    ("session-b".to_owned(), Some(selector("head-b", 9))),
                    ("session-c".to_owned(), None),
                    ("session-a".to_owned(), None),
                ],
                "every route opens under the scope of the run it serves, with the frozen epoch"
            );
            assert_eq!(
                plane.cached_route_channel_for_scope(&b.bind, a.scope.as_ref(), "aft"),
                None,
                "no route of session B exists under session A's scope"
            );
            assert_ne!(
                plane.cached_route_channel_for_scope(&a.bind, a.scope.as_ref(), "aft"),
                plane.cached_route_channel_for_scope(&a.bind, None, "aft"),
                "A's unscoped run did not reuse the route A's scoped run opened"
            );
        }

        /// A warm bind for a scoped run opens under that scope and caches the route only there.
        #[tokio::test]
        async fn a_scoped_warm_bind_opens_and_caches_under_its_scope_only() {
            let (conn, plane, _) = rig(Vec::new());
            let bind = BindIdentity::new("/tmp/scope-carrier", "test", "warm");
            let head = scope("head-w", 1);
            plane
                .warm_bind_for_lifecycle(
                    &bind,
                    Some(&head),
                    Duration::from_secs(5),
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            assert_eq!(
                conn.opened_scopes(),
                vec![("warm".to_owned(), Some(selector("head-w", 1)))]
            );
            assert!(plane
                .cached_route_channel_for_scope(&bind, Some(&head), "aft")
                .is_some());
            assert_eq!(plane.cached_route_channel_for_module(&bind, "aft"), None);
        }

        /// On a daemon without `scopes/v1` a scoped call is refused `scope_unsupported` and no
        /// route opens, through batch start, dispatch and the warm bind alike. An unscoped
        /// session on the same daemon is unaffected.
        #[tokio::test]
        async fn a_scoped_session_on_a_daemon_without_scopes_v1_never_opens_a_route() {
            let (conn, plane, _) = rig(vec![successful_tool_reply("ok")]);
            conn.capabilities.lock().unwrap().clear();
            let scoped = ctx("s-b1", "scoped", "run-1", Some(scope("head", 1)));
            let read = call("read");

            let refused = plane
                .batch_started(&scoped, std::slice::from_ref(&read), &manifest())
                .await
                .expect_err("a scoped batch cannot start");
            assert!(
                matches!(&refused, ToolPlaneError::ScopeRefused { code, .. } if code == "scope_unsupported"),
                "{refused:?}"
            );
            match plane.dispatch(&scoped, &read).await {
                DispatchResult::NotSent { reason } => {
                    assert!(reason.starts_with("scope_unsupported: "), "{reason}")
                }
                other => panic!("expected scope_unsupported, got {other:?}"),
            }
            let warm = plane
                .warm_bind_for_lifecycle(
                    &scoped.bind,
                    scoped.scope.as_ref(),
                    Duration::from_secs(5),
                    CancellationToken::new(),
                )
                .await
                .expect_err("a scoped warm bind cannot open");
            assert!(
                matches!(&warm, ToolPlaneError::ScopeRefused { code, .. } if code == "scope_unsupported")
            );
            assert_eq!(
                conn.route_open_calls.load(Ordering::SeqCst),
                0,
                "nothing was opened, scoped or not"
            );
            assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0);

            let unscoped = ctx("u-b1", "unscoped", "run-2", None);
            plane
                .batch_started(&unscoped, std::slice::from_ref(&read), &manifest())
                .await
                .unwrap();
            assert_completed_success(plane.dispatch(&unscoped, &read).await);
            assert_eq!(conn.opened_scopes(), vec![("unscoped".to_owned(), None)]);
        }

        /// Without a module connection to describe the scope over, a scoped open cannot learn
        /// its route's stamp, so nothing opens and the call is `scope_unverifiable`.
        #[tokio::test]
        async fn a_scoped_open_without_a_describer_opens_nothing() {
            let conn = ScriptedToolPlaneConnection::new(vec![Vec::new(); 4], Vec::new());
            let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
            plane.windows.lock().unwrap().insert("aft".into(), 1);
            let scoped = ctx("b1", "s", "r", Some(scope("head", 1)));
            let refused = plane
                .batch_started(&scoped, &[call("read")], &manifest())
                .await
                .expect_err("no describer");
            assert!(
                matches!(&refused, ToolPlaneError::ScopeRefused { code, .. } if code == SCOPE_UNVERIFIABLE),
                "{refused:?}"
            );
            assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 0);
        }

        /// `scope_not_synced` on route.open holds with backoff and then opens.
        #[tokio::test(start_paused = true)]
        async fn scope_not_synced_holds_with_backoff_until_the_owner_syncs() {
            let (conn, plane, _) = rig(Vec::new());
            conn.script_route_open_sequence(
                "aft",
                vec![
                    RouteOpenScript::rejected("scope_not_synced — owner has not synced"),
                    RouteOpenScript::rejected("scope_not_synced — owner has not synced"),
                    RouteOpenScript::default(),
                ],
            );
            let started_at = Instant::now();
            started(&plane).await;
            assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 3);
            // Two waits, 100 ms then 200 ms, before the third attempt: a backoff, not a busy
            // loop.
            assert!(started_at.elapsed() >= Duration::from_millis(300));
        }

        /// `scope_changed` on route.open re-opens at once, with no wait.
        #[tokio::test(start_paused = true)]
        async fn scope_changed_reopens_at_once() {
            let (conn, plane, _) = rig(Vec::new());
            conn.script_route_open_sequence(
                "aft",
                vec![
                    RouteOpenScript::rejected("scope_changed — record changed before commit"),
                    RouteOpenScript::default(),
                ],
            );
            let started_at = Instant::now();
            started(&plane).await;
            assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 2);
            assert_eq!(
                started_at.elapsed(),
                Duration::ZERO,
                "no wait before the re-open"
            );
        }

        /// A route.open hold that runs past `SCOPE_SYNC_HOLD` gives `scope_unverifiable`, at the
        /// bound, and nothing is dispatched.
        #[tokio::test(start_paused = true)]
        async fn a_hold_past_the_bound_is_scope_unverifiable_and_dispatches_nothing() {
            let (conn, plane, _) = rig(vec![successful_tool_reply("never sent")]);
            conn.script_route_open_sequence(
                "aft",
                vec![RouteOpenScript::rejected(
                    "scope_not_synced — owner has not synced",
                )],
            );
            let scoped = ctx("b1", "s", "r", Some(scope("head", 1)));
            let read = call("read");

            let started_at = Instant::now();
            let refused = plane
                .batch_started(&scoped, std::slice::from_ref(&read), &manifest())
                .await
                .expect_err("the hold runs out");
            let held = started_at.elapsed();
            assert!(
                matches!(&refused, ToolPlaneError::ScopeRefused { code, .. } if code == SCOPE_UNVERIFIABLE),
                "{refused:?}"
            );
            assert!(
                held >= SCOPE_SYNC_HOLD && held < SCOPE_SYNC_HOLD + Duration::from_secs(3),
                "held {held:?}"
            );

            match plane.dispatch(&scoped, &read).await {
                DispatchResult::NotSent { reason } => {
                    assert!(reason.starts_with("scope_unverifiable: "), "{reason}")
                }
                other => panic!("expected scope_unverifiable, got {other:?}"),
            }
            assert_eq!(
                conn.tool_calls.load(Ordering::SeqCst),
                0,
                "nothing dispatched"
            );
        }

        /// Every route.open scope refusal outside `scope_not_synced` and `scope_changed` is
        /// terminal: answered by its own name after exactly one attempt, never held.
        #[tokio::test(start_paused = true)]
        async fn a_terminal_scope_refusal_is_never_retried_or_held() {
            for code in [
                "scope_ended",
                "scope_parent_ended",
                "scope_carrier_removed",
                "scope_epoch_ended",
                "scope_kind_changed",
                "scope_not_live",
                "scope_owner_mismatch",
                "scope_not_carrier",
                "scope_epoch_required",
            ] {
                let (conn, plane, _) = rig(Vec::new());
                conn.script_route_open_sequence(
                    "aft",
                    vec![RouteOpenScript::rejected(format!("{code} — refused"))],
                );
                let scoped = ctx("b1", "s", "r", Some(scope("head", 1)));
                let started_at = Instant::now();
                let refused = plane
                    .batch_started(&scoped, &[call("read")], &manifest())
                    .await
                    .expect_err("a terminal refusal");
                assert!(
                    matches!(&refused, ToolPlaneError::ScopeRefused { code: named, .. } if named == code),
                    "{code}: {refused:?}"
                );
                assert_eq!(
                    conn.route_open_calls.load(Ordering::SeqCst),
                    1,
                    "{code} was retried"
                );
                assert_eq!(started_at.elapsed(), Duration::ZERO, "{code} was held");
            }
        }

        /// `scope_not_live` refuses the rest of the current run's calls at once, opening nothing
        /// more; a later run of the same session is judged afresh and admitted.
        #[tokio::test]
        async fn scope_not_live_refuses_the_rest_of_the_run_and_a_later_run_is_admitted() {
            let (conn, plane, _) = rig(vec![successful_tool_reply("ok")]);
            conn.script_route_open_sequence(
                "aft",
                vec![
                    RouteOpenScript::rejected("scope_not_live — not in the owner's set"),
                    RouteOpenScript::default(),
                ],
            );
            let read = call("read");
            let first = ctx("r1-b1", "s", "r1", Some(scope("head", 1)));
            assert!(plane
                .batch_started(&first, std::slice::from_ref(&read), &manifest())
                .await
                .is_err());
            let later_batch = ctx("r1-b2", "s", "r1", Some(scope("head", 1)));
            let refused = plane
                .batch_started(&later_batch, std::slice::from_ref(&read), &manifest())
                .await
                .expect_err("the rest of the run is refused");
            assert!(
                matches!(&refused, ToolPlaneError::ScopeRefused { code, .. } if code == "scope_not_live")
            );
            match plane.dispatch(&later_batch, &read).await {
                DispatchResult::NotSent { reason } => {
                    assert!(reason.starts_with("scope_not_live: "), "{reason}")
                }
                other => panic!("expected scope_not_live, got {other:?}"),
            }
            assert_eq!(
                conn.route_open_calls.load(Ordering::SeqCst),
                1,
                "the refused run opened nothing more"
            );

            let next_run = ctx("r2-b1", "s", "r2", Some(scope("head", 1)));
            plane
                .batch_started(&next_run, std::slice::from_ref(&read), &manifest())
                .await
                .expect("a later run is admitted normally");
            assert_completed_success(plane.dispatch(&next_run, &read).await);
        }

        /// The open is bracketed by two describes. When they disagree on the agent, the route
        /// may have been bound under either stamp: it is closed and opened again, and only the
        /// agreeing pair becomes the route's stamp, the one a later re-send is compared to.
        #[tokio::test]
        async fn a_rebind_between_the_bracketing_describes_is_not_recorded_as_the_routes_stamp() {
            let (conn, plane, describer) = rig(vec![successful_tool_reply("re-sent")]);
            describer.script(
                "head",
                vec![
                    live("head", 1, "agent-a"),
                    live("head", 1, "agent-b"),
                    live("head", 1, "agent-b"),
                    live("head", 1, "agent-b"),
                ],
            );
            let (scoped, read) = started(&plane).await;
            assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 2);
            assert_eq!(
                conn.closed_routes.lock().unwrap().len(),
                1,
                "the route bound during the rebind was closed"
            );

            // The recorded stamp is agent-b's: after a close with the scope still on agent-b,
            // the call is re-sent. Had agent-a's been recorded, it would be drained instead.
            conn.close(route_of(&plane, &scoped));
            assert_completed_success(plane.dispatch(&scoped, &read).await);
            assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
        }

        /// A call not yet sent whose route closed while the scope, or its parent, is gone:
        /// `route_drained` naming what describe said, never re-dispatched, and the run's next
        /// batch stops. A describe that answers for a different epoch counts as ended.
        #[tokio::test]
        async fn an_unsent_call_whose_scope_is_gone_is_route_drained_and_the_run_stops() {
            for (answer, reason) in [
                (ended(1), "scope_ended"),
                (not_live(true), "scope_not_live"),
                (live("head", 2, "none"), "scope_ended"),
                (parent_ended("head", 1), "scope_parent_ended"),
            ] {
                // One reply is scripted so a plane that wrongly re-dispatched would complete the
                // call instead of failing on a missing script.
                let (conn, plane, describer) = rig(vec![successful_tool_reply("re-dispatched")]);
                describer.script(
                    "head",
                    vec![live("head", 1, "none"), live("head", 1, "none"), answer],
                );
                let (scoped, read) = started(&plane).await;
                conn.close(route_of(&plane, &scoped));

                assert_drained(plane.dispatch(&scoped, &read).await, reason, false);
                assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0, "{reason}: sent");
                assert_eq!(
                    conn.route_open_calls.load(Ordering::SeqCst),
                    1,
                    "{reason}: a fresh route was opened for the drained call"
                );
                plane.batch_finished(&scoped).await.unwrap();

                let next = ctx("r-b2", "s", "r", Some(scope("head", 1)));
                let stopped = plane
                    .batch_started(&next, &[call("read")], &manifest())
                    .await;
                assert!(
                    matches!(&stopped, Err(ToolPlaneError::ScopeDrained { reason: named }) if named == reason),
                    "{reason}: the run's next batch must stop, got {stopped:?}"
                );
            }
        }

        /// A describe answering for another epoch than the route was opened under means the
        /// scope we were admitted under ended (a newer session reused the ref).
        #[test]
        fn a_describe_for_a_different_epoch_counts_as_ended() {
            for answer in [live("head", 2, "none").unwrap(), {
                let mut answer = live("head", 1, "none").unwrap();
                answer.scope_epoch = Some(2);
                answer
            }] {
                assert!(
                    matches!(read_describe(1, &answer), ScopeReading::Gone(SCOPE_ENDED)),
                    "{answer:?}"
                );
            }
            assert!(matches!(
                read_describe(1, &live("head", 1, "none").unwrap()),
                ScopeReading::Live(_)
            ));
        }

        /// A delegation narrowing leaves the scope live under the same epoch and agent, so the
        /// call not yet sent is re-sent on a fresh route under the current stamp.
        #[tokio::test]
        async fn a_delegation_narrowing_resends_the_unsent_call_on_a_fresh_route() {
            let (conn, plane, describer) = rig(vec![successful_tool_reply("sent once")]);
            describer.script(
                "head",
                vec![
                    live("head", 1, "agent-a-delegating"),
                    live("head", 1, "agent-a-delegating"),
                    live("head", 1, "agent-a"),
                ],
            );
            let (scoped, read) = started(&plane).await;
            conn.close(route_of(&plane, &scoped));

            assert_completed_success(plane.dispatch(&scoped, &read).await);
            assert_eq!(
                conn.tool_calls.load(Ordering::SeqCst),
                1,
                "sent exactly once"
            );
            assert_eq!(
                conn.route_open_calls.load(Ordering::SeqCst),
                2,
                "on a fresh route"
            );
        }

        /// A rebind to another agent leaves the scope live, but the call was composed for the old
        /// agent: `route_drained` as `scope_agent_changed`, and nothing is dispatched.
        #[tokio::test]
        async fn a_rebound_agent_drains_the_unsent_call_and_dispatches_nothing() {
            let (conn, plane, describer) = rig(vec![successful_tool_reply("must not be sent")]);
            describer.script(
                "head",
                vec![
                    live("head", 1, "agent-a"),
                    live("head", 1, "agent-a"),
                    live("head", 1, "agent-b"),
                ],
            );
            let (scoped, read) = started(&plane).await;
            conn.close(route_of(&plane, &scoped));

            assert_drained(
                plane.dispatch(&scoped, &read).await,
                SCOPE_AGENT_CHANGED,
                false,
            );
            assert_eq!(
                conn.tool_calls.load(Ordering::SeqCst),
                0,
                "nothing dispatched"
            );
        }

        /// When the daemon describes the scope as not live because its owner has not synced
        /// since the daemon started, the unsent call waits, as a `scope_not_synced` open does.
        /// It is re-sent once the owner syncs; if the hold runs out first it is drained as
        /// `scope_unverifiable` and never sent.
        #[tokio::test(start_paused = true)]
        async fn an_unsynced_owner_holds_the_unsent_call_inside_the_scope_sync_hold() {
            let (conn, plane, describer) = rig(vec![successful_tool_reply("after the sync")]);
            describer.script(
                "head",
                vec![
                    live("head", 1, "agent-a"),
                    live("head", 1, "agent-a"),
                    not_live(false),
                    not_live(false),
                    live("head", 1, "agent-a"),
                ],
            );
            let (scoped, read) = started(&plane).await;
            conn.close(route_of(&plane, &scoped));
            let started_at = Instant::now();
            assert_completed_success(plane.dispatch(&scoped, &read).await);
            assert!(started_at.elapsed() > Duration::ZERO, "the call was held");
            assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);

            let (conn, plane, describer) = rig(vec![successful_tool_reply("must not be sent")]);
            describer.script(
                "head",
                vec![
                    live("head", 1, "agent-a"),
                    live("head", 1, "agent-a"),
                    not_live(false),
                ],
            );
            let (scoped, read) = started(&plane).await;
            conn.close(route_of(&plane, &scoped));
            let started_at = Instant::now();
            assert_drained(
                plane.dispatch(&scoped, &read).await,
                SCOPE_UNVERIFIABLE,
                false,
            );
            assert!(started_at.elapsed() >= SCOPE_SYNC_HOLD, "held to the bound");
            assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0);
        }

        /// When asking the daemon about the scope fails, nothing shows that a fresh route would
        /// carry the same epoch and agent, so the unsent call is drained as `scope_unverifiable`
        /// and never re-sent.
        #[tokio::test]
        async fn an_unanswered_describe_drains_the_unsent_call() {
            let (conn, plane, describer) = rig(vec![successful_tool_reply("must not be sent")]);
            describer.script(
                "head",
                vec![
                    live("head", 1, "agent-a"),
                    live("head", 1, "agent-a"),
                    Err("module connection closed".into()),
                ],
            );
            let (scoped, read) = started(&plane).await;
            conn.close(route_of(&plane, &scoped));
            assert_drained(
                plane.dispatch(&scoped, &read).await,
                SCOPE_UNVERIFIABLE,
                false,
            );
            assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 0);
        }

        /// A call already sent when its route closed: the outcome is unknown whatever the
        /// scope's state. Describe names why when the scope or parent is gone or the agent
        /// changed; a live, unchanged scope leaves the ordinary unknown outcome.
        #[tokio::test]
        async fn a_call_sent_when_its_route_closed_is_outcome_unknown() {
            for (answer, reason) in [
                (ended(1), Some("scope_ended")),
                (parent_ended("head", 1), Some("scope_parent_ended")),
                (live("head", 1, "agent-b"), Some(SCOPE_AGENT_CHANGED)),
                (live("head", 1, "agent-a"), None),
            ] {
                let (conn, plane, describer) = rig(Vec::new());
                describer.script(
                    "head",
                    vec![
                        live("head", 1, "agent-a"),
                        live("head", 1, "agent-a"),
                        answer,
                    ],
                );
                let (scoped, read) = started(&plane).await;
                *conn.close_during_call.lock().unwrap() = true;

                let outcome = plane.dispatch(&scoped, &read).await;
                match reason {
                    Some(reason) => assert_drained(outcome, reason, true),
                    None => assert!(
                        matches!(outcome, DispatchResult::SentButOutcomeUnknown { .. }),
                        "{outcome:?}"
                    ),
                }
                assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
            }
        }

        /// With nothing in flight, a closed route whose scope or parent is gone stops the rest
        /// of the run at its next batch, before anything opens. A live scope is ordinary route
        /// loss: the next batch re-opens. A later run is judged afresh.
        #[tokio::test]
        async fn a_close_with_nothing_in_flight_ends_the_run_only_when_the_scope_is_gone() {
            for (answer, reason) in [
                (ended(1), Some("scope_ended")),
                (parent_ended("head", 1), Some("scope_parent_ended")),
                (live("head", 1, "agent-b"), None),
            ] {
                let (conn, plane, describer) =
                    rig((0..3).map(|_| successful_tool_reply("ok")).collect());
                describer.script(
                    "head",
                    vec![
                        live("head", 1, "agent-a"),
                        live("head", 1, "agent-a"),
                        answer.clone(),
                    ],
                );
                let (first, read) = started(&plane).await;
                assert_completed_success(plane.dispatch(&first, &read).await);
                plane.batch_finished(&first).await.unwrap();
                conn.close(route_of(&plane, &first));

                let next = ctx("r-b2", "s", "r", Some(scope("head", 1)));
                let started_next = plane
                    .batch_started(&next, std::slice::from_ref(&read), &manifest())
                    .await;
                let Some(reason) = reason else {
                    started_next.expect("a live scope is ordinary route loss");
                    assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 2);
                    continue;
                };
                match started_next {
                    Err(ToolPlaneError::ScopeDrained { reason: named }) => {
                        assert_eq!(named, reason)
                    }
                    other => panic!("{reason}: expected the run to end, got {other:?}"),
                }
                assert_eq!(
                    conn.route_open_calls.load(Ordering::SeqCst),
                    1,
                    "{reason}: the ended run opened nothing more"
                );

                describer.script("head", vec![live("head", 1, "agent-a")]);
                let later_run = ctx("r2-b1", "s", "r2", Some(scope("head", 1)));
                plane
                    .batch_started(&later_run, std::slice::from_ref(&read), &manifest())
                    .await
                    .expect("a later run is judged afresh");
            }
        }

        /// A provider that already received the call and refuses it `scope_not_synced` gets the
        /// refusal recorded as the call's error result, and the call is not sent again. The
        /// provider holds the first request, and broca sends no durable call identity that
        /// would let it recognise a second send as the same request.
        #[tokio::test(start_paused = true)]
        async fn a_provider_scope_not_synced_refusal_is_recorded_and_never_resent() {
            let (conn, plane, _) = rig(vec![
                Err(ConnError::Rejected(
                    "scope_not_synced — the scope's owner has not synced".into(),
                )),
                successful_tool_reply("a second send"),
            ]);
            let (scoped, read) = started(&plane).await;
            match plane.dispatch(&scoped, &read).await {
                DispatchResult::Completed(result) => {
                    assert!(result.is_error);
                    let ToolOutput::Error { message } = &result.output else {
                        panic!("expected an error result, got {result:?}");
                    };
                    assert!(message.contains("scope_not_synced"), "{message}");
                }
                other => panic!("expected the refusal as the call's result, got {other:?}"),
            }
            assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1, "never re-sent");
            assert_eq!(conn.route_open_calls.load(Ordering::SeqCst), 1);
        }
    }
}

#[cfg(test)]
mod approval_digest_contract_tests {
    use super::*;

    #[tokio::test]
    async fn approved_connection_omits_approval_digest_and_preserves_saved_bytes() {
        let (conn, route, mut module) = SubcConnection::in_process_for_test(5, 3);
        let saved = r#"{"n":1e+30,"a":2}"#;
        let call = ToolCall {
            tool_call_id: broca_protocol::ToolCallId("approved".into()),
            tool_name: "edit".into(),
            input: serde_json::from_str(saved).unwrap(),
            thought_signature: None,
        };
        let args = ApprovedArgs {
            bytes: saved.into(),
            key: "lineage:7".into(),
            signals: Default::default(),
        };
        let (result, body) = tokio::join!(
            ToolPlaneConnection::tool_call_approved(
                &conn,
                &route,
                &call,
                Duration::from_secs(1),
                Some("head"),
                &args
            ),
            async {
                let request = module.next_request().await;
                let body = request.body.clone();
                assert!(module.reply(&request, br#"{"content":[]}"#.to_vec()));
                body
            }
        );
        result.unwrap();
        assert!(String::from_utf8(body.clone())
            .unwrap()
            .contains(&format!("\"arguments\":{saved}")));
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["call_key"], "lineage:7");
        assert_eq!(value["preset"], "head");
        assert!(value.get("approval_digest").is_none(), "consent execution must omit approval_digest rather than sending an empty or legacy digest");
    }
}

#[cfg(test)]
mod approved_queue_contract_tests {
    use super::*;
    use futures::FutureExt;

    #[tokio::test]
    async fn approved_closed_writer_queue_is_not_sent() {
        let (conn, route, mut module) = SubcConnection::in_process_for_test(5, 3);
        module.close_writer_for_test();
        let plane = SubcToolPlane::new(conn, "aft");
        let ctx = BatchCtx {
            batch_id: broca_protocol::BatchId("closed-queue".into()),
            bind: BindIdentity::new("/project", "test", "closed-queue"),
            run_id: broca_protocol::RunId("run".into()),
            scope: None,
        };
        plane.routes.lock().unwrap().insert(
            SubcToolPlane::route_key(&ctx.bind, None, "aft"),
            RouteState {
                handle: route,
                sem: Arc::new(Semaphore::new(1)),
                stamp: None,
            },
        );
        let call = ToolCall {
            tool_call_id: broca_protocol::ToolCallId("approved".into()),
            tool_name: "edit".into(),
            input: serde_json::json!({}),
            thought_signature: None,
        };
        let result = plane
            .dispatch_approved(&ctx, &call, "{}", "call-key", "digest", &Default::default())
            .await;
        assert!(
            matches!(result, DispatchResult::NotSent { ref reason } if reason == "writer task closed before enqueue"),
            "a failed reservation never commits a send: {result:?}"
        );
        assert!(module.take_written_for_test().is_empty());
        assert_eq!(module.pending_count_for_test(), 0);
    }

    #[tokio::test]
    async fn approved_cancel_and_stop_while_writer_queue_is_full_are_not_sent() {
        for cancel in [true, false] {
            let (conn, route, mut module) = SubcConnection::in_process_full_writer_for_test(5, 3);
            let plane = SubcToolPlane::new(conn, "aft");
            let ctx = BatchCtx {
                batch_id: broca_protocol::BatchId("queue".into()),
                bind: BindIdentity::new("/project", "test", "queue"),
                run_id: broca_protocol::RunId("run".into()),
                scope: None,
            };
            plane.routes.lock().unwrap().insert(
                SubcToolPlane::route_key(&ctx.bind, None, "aft"),
                RouteState {
                    handle: route,
                    sem: Arc::new(Semaphore::new(1)),
                    stamp: None,
                },
            );
            let call = ToolCall {
                tool_call_id: broca_protocol::ToolCallId("approved".into()),
                tool_name: "edit".into(),
                input: serde_json::json!({}),
                thought_signature: None,
            };
            let token = CancellationToken::new();
            let signals = broca_core::tool_plane::DispatchSignals::new(
                if cancel { Some(&token) } else { None },
                if cancel { None } else { Some(&token) },
            );
            let mut dispatch = Box::pin(
                plane.dispatch_approved(&ctx, &call, "{}", "call-key", "digest", &signals),
            );
            assert!(dispatch.as_mut().now_or_never().is_none());
            assert_eq!(
                module.pending_count_for_test(),
                1,
                "the real tool request must already be waiting for writer capacity"
            );
            token.cancel();
            let result = tokio::time::timeout(Duration::from_millis(250), dispatch)
                .await
                .expect("cancelled approved enqueue must not wait for writer capacity");
            assert!(
                matches!(result, DispatchResult::NotSent { .. }),
                "pre-enqueue interruption is typed not-sent, never unknown: {result:?}"
            );
            let frames = module.take_written_for_test();
            assert_eq!(frames.len(), 256);
            assert!(
                frames
                    .iter()
                    .all(|frame| frame.header.ty == subc_protocol::FrameType::Ping),
                "no tool request or provider cancel may be enqueued before the send boundary"
            );
            assert_eq!(module.pending_count_for_test(), 0);
            tokio::task::yield_now().await;
            assert!(module.take_written_for_test().is_empty());
        }
    }
}
