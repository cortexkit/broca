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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{
    tool_plane::{BatchCtx, DispatchResult, ToolOpenGuard, ToolPlane, ToolPlaneError},
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
    retry_route_open, retry_route_open_waiting_out_outage, retry_route_open_with_scope_hold,
    route_failure_is_known_dead, ConnError, GateTarget, ModuleGate, OutageWait, RouteHandle,
    RouteOpenError, RouteOpenRetry, RouteShape, ScopeHold, SubcConnection,
    ROUTE_OPEN_RETRY_DEADLINE, ROUTE_OPEN_RETRY_INTERVAL, ROUTE_OPEN_RETRY_MAX_INTERVAL,
};
use crate::scope_describe::{DescribedScope, ScopeDescriber};
use crate::tool_deadline::tool_reply_deadline;

use crate::route_close::{CloseTarget, RouteCloser};

mod run_transport;

#[cfg(all(feature = "test-route-close", not(debug_assertions)))]
compile_error!("test-route-close must never be enabled in a shipping build");

/// How long a TOOL-call dispatch route open keeps retrying a module-availability
/// refusal (`target_unavailable`, `module_reloading`, `module_warming`, `module_timeout`)
/// before the call is refused as module-unavailable: 90 s from the first refusal, through
/// the same per-target gate every other waiter shares, so all waiting calls ride out one
/// short provider-module restart together instead of each failing the turn after 3 s.
///
/// Tool dispatch only: vault, catalog, transform and routing opens keep the 3 s default
/// patience. The wait, an open attempt in flight included, still ends at once on run
/// cancel/stop or at the run's step deadline when that comes first (see [`ToolOpenGuard`]
/// and [`OutageWait`]).
const TOOL_DISPATCH_PATIENCE: RouteOpenPatience = RouteOpenPatience {
    deadline: Duration::from_secs(90),
    // Backs off to 2s rather than polling every 100ms: at the flat interval a 90s wait is
    // hundreds of route.open calls per waiting dispatch, the storm the default path's cap
    // exists to prevent. While the module is down the gate does the waiting anyway.
    max_interval: Duration::from_secs(2),
    waits_out_outage: true,
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
    Live(Box<ScopeStamp>),
    /// The owner has not synced since the daemon started: wait, as for `scope_not_synced` on
    /// open.
    NotSynced,
    /// The answer cannot settle it (no answer, or an owner that will never sync).
    Unverifiable(String),
}

/// `scope.describe` for `scope_ref` under `owner`, or under the owner's other id when `owner`
/// is a renamed module ([`broca_protocol::RENAMED_MODULES`]) and the scope is not live under the
/// id the session froze. A renamed owner re-syncs its scopes under its new id, so the scope's
/// twin (same `ref` and `scope_epoch`) is live there while the old id is not even a configured
/// module any more; a route open finds the twin the same way
/// (`SubcConnection::route_open_wire_target`). The twin's answer is used unless it is not live
/// either while the frozen owner is still configured, which leaves the frozen owner's answer.
async fn describe_either_owner(
    describer: &dyn ScopeDescriber,
    owner: &Principal,
    scope_ref: &str,
) -> Result<DescribedScope, String> {
    let answer = describer.describe(owner, scope_ref).await?;
    if answer.status != ScopeStatus::NotLive {
        return Ok(answer);
    }
    let Principal::Reserved { module_id } = owner else {
        return Ok(answer);
    };
    let Some(other) = broca_protocol::other_module_name(module_id) else {
        return Ok(answer);
    };
    let twin = Principal::Reserved {
        module_id: other.to_owned(),
    };
    match describer.describe(&twin, scope_ref).await {
        Ok(twin_answer)
            if twin_answer.status != ScopeStatus::NotLive || !answer.owner_configured =>
        {
            Ok(twin_answer)
        }
        _ => Ok(answer),
    }
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
                ScopeReading::Live(Box::new(stamp.clone()))
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
/// attempts. The default matches [`RouteOpenRetry::default_patience`].
#[derive(Clone, Copy, Debug)]
struct RouteOpenPatience {
    deadline: Duration,
    max_interval: Duration,
    /// Measure `deadline` from the first refusal and race attempts in flight against cancel
    /// and the caller's deadline ([`OutageWait`]), as a tool-call dispatch open does. Every
    /// other open keeps a fixed budget from its start.
    waits_out_outage: bool,
}

const DEFAULT_ROUTE_OPEN_PATIENCE: RouteOpenPatience = RouteOpenPatience {
    deadline: ROUTE_OPEN_RETRY_DEADLINE,
    max_interval: ROUTE_OPEN_RETRY_MAX_INTERVAL,
    waits_out_outage: false,
};

/// subc's per-route credit window for a declared concurrency class.
fn window_for(c: &Concurrency) -> usize {
    match c {
        Concurrency::Serial => 1,
        Concurrency::ModuleManaged => 32,
        Concurrency::StatelessParallel => 1024,
    }
}

#[cfg(test)]
struct RouteCacheGate {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

struct RouteState {
    handle: RouteHandle,
    /// Bounds concurrent in-flight on this route to the provider's window.
    sem: Arc<Semaphore>,
    /// For a route opened under a scope: the stamp the daemon bound it with, as `scope.describe`
    /// reported it identically just before and just after the open. `None` for an unscoped
    /// route.
    stamp: Option<ScopeStamp>,
    last_used: Instant,
}

enum RouteEnsureError {
    Connection(ConnError),
    Plane(ToolPlaneError),
}

struct PlaneCloseTarget(std::sync::Weak<dyn ToolPlaneConnection>);
impl CloseTarget for PlaneCloseTarget {
    fn attempt(
        &self,
        handle: RouteHandle,
    ) -> futures::future::BoxFuture<'static, Result<(), ConnError>> {
        let conn = self.0.upgrade();
        Box::pin(async move {
            match conn {
                Some(conn) => conn.route_close(&handle).await,
                None => Err(ConnError::StaleRouteHandle(handle)),
            }
        })
    }
    fn abandon(&self, _handle: RouteHandle) {}
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

/// A subc route is scoped by session identity, target module, scope and full wire shape.
/// The scope is part of the key so a route opened under one scope, or none, is never
/// reused for a run admitted under another. The durability fence deliberately stays keyed only
/// by `(session, tool_call_id)` so a caller re-driving the same batch cannot duplicate a tool
/// call through another module route.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RouteKey {
    session: SessionKey,
    scope: ScopeKey,
    module_id: String,
    shape: RouteShape,
    held_reply_v1: bool,
}

impl RouteKey {
    fn with_handle(mut self, handle: RouteHandle) -> Self {
        self.shape = RouteShape {
            tool_result_v1: handle.reads_tool_result,
            call_keepalive: handle.call_keepalive,
            tool_provider_v1: handle.tool_provider_v1,
            session_identity: handle.session_identity,
        };
        self.held_reply_v1 = handle.reads_held_reply;
        self
    }

    fn same_target(&self, other: &Self) -> bool {
        self.session == other.session
            && self.scope == other.scope
            && self.module_id == other.module_id
    }
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

    fn evict_session(&mut self, session: &SessionKey) {
        self.results.retain(|(owner, _), _| owner != session);
        self.by_batch.retain(|_, keys| {
            keys.retain(|(owner, _)| owner != session);
            !keys.is_empty()
        });
    }

    /// Total live fence entries (for the eviction test).
    #[cfg(test)]
    fn len(&self) -> usize {
        self.results.len()
    }
}

#[async_trait]
trait ToolPlaneConnection: Send + Sync {
    fn shared_route_closer(&self) -> Option<Arc<RouteCloser>> {
        None
    }
    fn consumer_module_id(&self) -> Option<&str> {
        None
    }
    /// Whether the call `call_key` was handed back for the person's steer (see
    /// [`SubcConnection::took_person_yield`]). A connection that sends no yields never was.
    fn took_person_yield(&self, _call_key: &str) -> bool {
        false
    }

    async fn route_open_dispatch(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        scope: Option<&ScopeSelector>,
        _shape: RouteShape,
        _provider_held_installed: bool,
        _aft_harness: Option<Arc<crate::provider_catalog::AftHarness>>,
    ) -> Result<RouteHandle, ConnError> {
        self.route_open(module_id, identity, scope).await
    }
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
        _deadline: crate::connection::ToolReplyDeadline,
        _preset: Option<&str>,
        _args: &ApprovedArgs,
        _schema_pin: Option<&str>,
    ) -> Result<serde_json::Value, crate::connection::ApprovedCallError> {
        Err(crate::connection::ApprovedCallError::BeforePush(
            ConnError::Protocol("approved_dispatch_unavailable".into()).into(),
        ))
    }

    /// An ordinary (not approved) call's send. `signals` are the run's cancel and stop: a
    /// call they interrupted before its request is enqueued is never sent, and is reported
    /// before-push (not sent).
    #[allow(clippy::too_many_arguments)]
    async fn tool_call_staged(
        &self,
        route: &RouteHandle,
        call: &ToolCall,
        deadline: crate::connection::ToolReplyDeadline,
        preset: Option<&str>,
        _call_key: Option<&str>,
        schema_pin: Option<&str>,
        signals: &broca_core::tool_plane::DispatchSignals,
    ) -> Result<serde_json::Value, crate::connection::ApprovedCallError> {
        if signals.interrupted() {
            return Err(crate::connection::ApprovedCallError::BeforePush(
                crate::connection::ApprovedSetupError::Interrupted,
            ));
        }
        if schema_pin.is_some() {
            return Err(crate::connection::ApprovedCallError::BeforePush(
                ConnError::Protocol("pinned_dispatch_unavailable".into()).into(),
            ));
        }
        self.tool_call(
            route,
            &call.tool_name,
            &(call.input).parse(),
            Some(&call.tool_call_id.0),
            deadline.duration,
            preset,
        )
        .await
        .map_err(crate::connection::ApprovedCallError::AfterPush)
    }

    /// The capability names the daemon advertises (`server.describe`).
    async fn server_capabilities(&self) -> Result<Vec<String>, ConnError>;

    /// Whether `route` is still open (no GOODBYE has closed it and its connection is current).
    fn route_is_open(&self, route: &RouteHandle) -> bool;

    /// The availability gate of `module_id`'s tool routes, shared by every tool route open to
    /// that module on this connection (see [`ModuleGate`]). Test fakes have none unless a test
    /// supplies one.
    fn module_gate(&self, _module_id: &str) -> Option<Arc<ModuleGate>> {
        None
    }
}

#[async_trait]
impl ToolPlaneConnection for SubcConnection {
    fn shared_route_closer(&self) -> Option<Arc<RouteCloser>> {
        Some(self.route_closer())
    }
    fn consumer_module_id(&self) -> Option<&str> {
        SubcConnection::consumer_module_id(self)
    }
    fn took_person_yield(&self, call_key: &str) -> bool {
        SubcConnection::took_person_yield(self, call_key)
    }

    async fn route_open_dispatch(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        scope: Option<&ScopeSelector>,
        shape: RouteShape,
        provider_held_installed: bool,
        aft_harness: Option<Arc<crate::provider_catalog::AftHarness>>,
    ) -> Result<RouteHandle, ConnError> {
        self.route_open_dispatch_single_attempt(
            module_id,
            identity,
            scope,
            shape,
            provider_held_installed,
            aft_harness,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    async fn tool_call_staged(
        &self,
        route: &RouteHandle,
        call: &ToolCall,
        deadline: crate::connection::ToolReplyDeadline,
        preset: Option<&str>,
        call_key: Option<&str>,
        schema_pin: Option<&str>,
        signals: &broca_core::tool_plane::DispatchSignals,
    ) -> Result<serde_json::Value, crate::connection::ApprovedCallError> {
        let args = serde_json::to_string(&call.input).map_err(|error| {
            crate::connection::ApprovedCallError::BeforePush(
                ConnError::Protocol(error.to_string()).into(),
            )
        })?;
        // The run's signals guard the enqueue itself: a cancel or stop observed before the
        // request enters the writer queue leaves it unsent, and one after it never turns the
        // enqueued request into an unsent one.
        self.tool_call_raw_yieldable_guarded(
            route,
            &call.tool_name,
            &args,
            Some(&call.tool_call_id.0),
            deadline.duration,
            deadline.declared,
            preset,
            call_key,
            schema_pin,
            signals,
            deadline.yield_provider.as_deref(),
        )
        .await
    }

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
        deadline: crate::connection::ToolReplyDeadline,
        preset: Option<&str>,
        args: &ApprovedArgs,
        schema_pin: Option<&str>,
    ) -> Result<serde_json::Value, crate::connection::ApprovedCallError> {
        self.tool_call_raw_yieldable_guarded(
            route,
            &call.tool_name,
            &args.bytes,
            Some(&call.tool_call_id.0),
            deadline.duration,
            deadline.declared,
            preset,
            Some(&args.key),
            schema_pin,
            &args.signals,
            deadline.yield_provider.as_deref(),
        )
        .await
    }

    async fn server_capabilities(&self) -> Result<Vec<String>, ConnError> {
        SubcConnection::server_capabilities(self).await
    }

    fn route_is_open(&self, route: &RouteHandle) -> bool {
        SubcConnection::route_is_open(self, route)
    }

    fn module_gate(&self, module_id: &str) -> Option<Arc<ModuleGate>> {
        // The plane opens only tool routes.
        Some(SubcConnection::module_gate(
            self,
            &GateTarget::tool_provider(module_id),
        ))
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
impl SendAttempt {
    fn approved_reply(
        reply: Result<serde_json::Value, crate::connection::ApprovedCallError>,
    ) -> Self {
        match reply {
            Ok(reply) => Self::Reply(Ok(reply)),
            Err(crate::connection::ApprovedCallError::AfterPush(error)) => Self::Reply(Err(error)),
            // Closed or replaced route handles keep the reopen-and-retry path:
            // their request never entered a queue. Every other BeforePush error
            // is not-sent by construction, regardless of its cause.
            Err(crate::connection::ApprovedCallError::BeforePush(
                crate::connection::ApprovedSetupError::Connection(
                    error @ ConnError::StaleRouteHandle(_),
                ),
            )) => Self::Reply(Err(error)),
            Err(crate::connection::ApprovedCallError::BeforePush(error)) => Self::NotSent {
                reason: error.to_string(),
            },
        }
    }
}

type RegisteredKeys = HashMap<BatchRouteKey, HashMap<ToolCallId, (String, String)>>;

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
    #[cfg(test)]
    after_cache_gate: Mutex<Option<RouteCacheGate>>,
    conn: Arc<dyn ToolPlaneConnection>,
    closer: Arc<RouteCloser>,
    owns_closer: bool,
    default_module_id: Option<String>,
    /// Private routing for a single Direct carrier; never stored in the durable conversation identity.
    direct_carrier_scope: Option<ScopeSelector>,
    /// Standalone callers select one provider's preset without changing daemon plan admission.
    direct_tool_item: Option<broca_fetch_plan::plan::Item>,
    direct_schema_pins: Mutex<BTreeMap<String, broca_core::tool_plane::DispatchSchemaPin>>,
    routes: Mutex<HashMap<RouteKey, RouteState>>,
    batch_routes: Mutex<BatchRouteMap>,
    batch_presets: Mutex<HashMap<BatchRouteKey, ToolManifest>>,
    approved_args: Mutex<HashMap<BatchRouteKey, HashMap<ToolCallId, ApprovedArgs>>>,
    provider_held: Option<ProviderHeldSupport>,
    /// The frozen tool manifest of each batch in progress. It is the only source of a
    /// provider's declarations: what its result format and `late_results` capability were when
    /// the session was admitted.
    batch_manifests: Mutex<HashMap<BatchRouteKey, broca_core::capability::ToolManifest>>,
    registered_keys: Mutex<RegisteredKeys>,
    inline_keys: Mutex<HashSet<String>>,
    /// Call keys handed back for the person's steer in batches still open. Their provider still
    /// owes each call its own terminal, which reaches the session as a notice, so the batch's end
    /// keeps the call's late-results registration; the registration sweep removes it once that
    /// notice is in the session.
    yielded_keys: Mutex<HashSet<String>>,
    #[cfg(debug_assertions)]
    reply_fallback_ms: u64,
    /// Lives across batches for the plane's lifetime, but is reused only as the source snapshot
    /// for per-batch routing; resolved routes remain frozen and separately evicted at batch
    /// terminal. Module respawns preserve module IDs, so those routes remain meaningful while a
    /// provider is restarted. A missing provider triggers one fresh resolution fetch, and any
    /// learned-dead route clears this snapshot, covering the catalog changes that can affect a
    /// later batch without polling per batch.
    catalog_memo: Mutex<Option<Arc<crate::provider_catalog::ProviderCatalog>>>,
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
    /// Calls re-opening a route after their first request was refused before it reached the
    /// tool, by route key, each as the token that fires when that call has finished. A call
    /// that was waiting for credit on the same route, and finds it replaced once it gets the
    /// credit, sends only after these have finished: on a serial route, batch order holds.
    resend_turns: Mutex<HashMap<RouteKey, Vec<CancellationToken>>>,
}

impl Drop for SubcToolPlane {
    fn drop(&mut self) {
        if self.owns_closer {
            self.closer.stop();
        }
    }
}

/// What the batch's frozen plan declared about one provider: result formats, keepalives,
/// and whether it may settle a call after its reply. Read from the run's frozen manifest
/// and transport override, never from a live catalog at dispatch or caller-declared tools,
/// so provider changes cannot alter how a resumed run reads a reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FrozenContract {
    pub call_keepalive: bool,
    pub content_v1: bool,
    pub tool_result_v1: bool,
    pub late_results: bool,
    /// The plan fetch answered this provider's catalog. The daemon manifest independently
    /// decides whether the route declares `tool-provider: v1`, including unplanned sessions.
    pub fetched: bool,
    /// A compaction or hook route at this provider already binds the session's own identity.
    pub session_identity: bool,
}

impl FrozenContract {
    /// How this provider's tool route is opened.
    fn route_shape(&self) -> RouteShape {
        RouteShape {
            tool_result_v1: self.tool_result_v1,
            call_keepalive: self.call_keepalive,
            tool_provider_v1: self.fetched,
            session_identity: self.session_identity,
        }
    }
}

struct ProviderHeldSupport {
    intake: Arc<dyn broca_core::tool_plane::LateResultsIntake>,
    lifecycle: Arc<dyn broca_core::tool_plane::ProviderHeldLifecycle>,
}

impl SubcToolPlane {
    /// Install the intake and the waiting lifecycle. The declarations a batch routes and
    /// decodes by come from that batch's frozen plan manifest (`ToolManifest`), so installing
    /// this pair is all a tool plane needs: it never consults a live catalog.
    pub fn with_provider_held_support(
        mut self,
        intake: Arc<dyn broca_core::tool_plane::LateResultsIntake>,
        lifecycle: Arc<dyn broca_core::tool_plane::ProviderHeldLifecycle>,
    ) -> Self {
        self.provider_held = Some(ProviderHeldSupport { intake, lifecycle });
        self
    }

    /// Whether this plane's tool routes declare `held-reply/v1` (with `tool-result/v1`). Only then
    /// does a plain call carry its key to a `late_results` provider, and only then is any call
    /// registered with the intake: without the declaration no provider may hold a call, so a key
    /// or a registration would change the wire and the store for nothing.
    fn held_reply_declared(&self) -> bool {
        self.provider_held.is_some()
    }

    fn frozen_contract(&self, ctx: &BatchCtx, module: &str) -> FrozenContract {
        let manifests = self.batch_manifests.lock().unwrap();
        let Some(manifest) = manifests.get(&Self::batch_route_key(ctx)) else {
            return FrozenContract::default();
        };
        let declarations = manifest.provider_declarations(module);
        FrozenContract {
            fetched: manifest.provider_fetched(module),
            session_identity: manifest.uses_session_identity(module),
            call_keepalive: declarations
                .and_then(|declarations| declarations.call_keepalive)
                .unwrap_or(false),
            late_results: declarations
                .and_then(|declarations| declarations.late_results)
                .unwrap_or(false),
            content_v1: declarations
                .and_then(|declarations| declarations.result_formats.as_ref())
                .is_some_and(|formats| {
                    formats
                        .values()
                        .any(|format| matches!(format.0.as_str(), "content/v1" | "tool-result/v1"))
                }),
            tool_result_v1: declarations
                .and_then(|declarations| declarations.result_formats.as_ref())
                .is_some_and(|formats| formats.values().any(|format| format.0 == "tool-result/v1")),
        }
    }

    /// Whether `tool` itself opted into the `content/v1` result body in the batch's plan.
    fn frozen_tool_is_content_v1(&self, ctx: &BatchCtx, module: &str, tool: &str) -> bool {
        let manifests = self.batch_manifests.lock().unwrap();
        let Some(manifest) = manifests.get(&Self::batch_route_key(ctx)) else {
            return false;
        };
        manifest
            .provider_declarations(module)
            .and_then(|declarations| declarations.result_formats.as_ref())
            .and_then(|formats| formats.get(tool))
            .is_some_and(|format| {
                format.0 == "tool-result/v1"
                    || (self.held_reply_declared() && format.0 == "content/v1")
            })
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_call(
        &self,
        ctx: &BatchCtx,
        module: &str,
        route: &RouteHandle,
        call: &ToolCall,
        deadline: Duration,
        key: Option<broca_core::tool_plane::DispatchKey<'_>>,
        signals: &broca_core::tool_plane::DispatchSignals,
    ) -> SendAttempt {
        let args = self
            .approved_args
            .lock()
            .unwrap()
            .get(&Self::batch_route_key(ctx))
            .and_then(|calls| calls.get(&call.tool_call_id))
            .cloned();
        let preset = self.frozen_preset(ctx, module);
        let schema_pin = self
            .dispatch_schema_pin(ctx, &call.tool_call_id, &call.tool_name)
            .and_then(|pin| {
                cortexkit_role_tool_provider::call::SchemaPin::new(
                    &call.tool_name,
                    pin.schema_digest,
                    pin.semantics,
                )
                .encode()
                .ok()
            });
        let late_results = self.frozen_contract(ctx, module).late_results;
        let plain_key = key.filter(|_| {
            self.held_reply_declared() && late_results && self.conn.consumer_module_id().is_some()
        });
        let sent_key = args
            .as_ref()
            .map(|args| args.key.as_str())
            .or(plain_key.map(|key| key.call_key));
        let yieldable = sent_key.is_some()
            && late_results
            && self.held_reply_declared()
            && self
                .batch_manifests
                .lock()
                .unwrap()
                .get(&Self::batch_route_key(ctx))
                .and_then(|manifest| manifest.provider_declarations(module))
                .is_some_and(|declarations| {
                    declarations
                        .yield_tools
                        .as_ref()
                        .is_some_and(|tools| tools.contains(&call.tool_name))
                        && declarations
                            .result_formats
                            .as_ref()
                            .and_then(|formats| formats.get(&call.tool_name))
                            .is_some_and(|format| format.0 == "tool-result/v1")
                });
        let deadline = crate::connection::ToolReplyDeadline {
            duration: deadline,
            declared: self.declared_reply(ctx, module, call).is_some(),
            yield_provider: yieldable.then(|| module.to_owned()),
        };
        if self.held_reply_declared()
            && late_results
            && sent_key.is_some_and(|key| {
                self.registered_keys
                    .lock()
                    .unwrap()
                    .get(&Self::batch_route_key(ctx))
                    .and_then(|keys| keys.get(&call.tool_call_id))
                    .is_none_or(|(registered_module, registered_key)| {
                        registered_module != module || registered_key != key
                    })
            })
        {
            return SendAttempt::NotSent {
                reason: "late_results_registration_missing".into(),
            };
        }
        if let Some(args) = args {
            // This is after every route, permit and stale-route recovery wait. An approved
            // call stopped here keeps the existing `approved_dispatch_stopped_before_send`
            // reason.
            if args.signals.interrupted() {
                return SendAttempt::NotSent {
                    reason: "approved_dispatch_stopped_before_send".into(),
                };
            }
            SendAttempt::approved_reply(
                self.conn
                    .tool_call_approved(
                        route,
                        call,
                        deadline,
                        preset.as_deref(),
                        &args,
                        schema_pin.as_deref(),
                    )
                    .await,
            )
        } else {
            // After every route-open, permit and stale-route wait: a run cancelled or stopped
            // during any of them sends nothing. The enqueue below checks the same signals
            // once more at the point the request is committed to the writer queue.
            if signals.interrupted() {
                return SendAttempt::NotSent {
                    reason: "dispatch_stopped_before_send".into(),
                };
            }
            let reply = self
                .conn
                .tool_call_staged(
                    route,
                    call,
                    deadline,
                    preset.as_deref(),
                    plain_key.map(|key| key.call_key),
                    schema_pin.as_deref(),
                    signals,
                )
                .await;
            if matches!(
                reply,
                Err(crate::connection::ApprovedCallError::BeforePush(
                    crate::connection::ApprovedSetupError::Interrupted
                ))
            ) {
                return SendAttempt::NotSent {
                    reason: "dispatch_stopped_before_send".into(),
                };
            }
            SendAttempt::approved_reply(reply)
        }
    }

    pub fn new(conn: SubcConnection, module_id: impl Into<String>) -> Self {
        Self::with_connection(Arc::new(conn), module_id)
    }

    /// Route a standalone Direct caller's tool calls under an owner's carrier grant.
    /// The selector (including its private proof) lives only in this plane. Constructing a
    /// separate plane keeps its cached routes from mixing with ordinary unscoped routes.
    pub fn new_with_direct_carrier_scope(
        conn: SubcConnection,
        module_id: impl Into<String>,
        selector: ScopeSelector,
    ) -> Self {
        let mut plane = Self::new(conn, module_id);
        plane.direct_carrier_scope = Some(selector);
        plane
    }

    /// Select a standalone caller's default provider preset. Other providers keep their own
    /// frozen plan presets; the selection never becomes a process-wide default.
    pub fn with_direct_tool_preset(mut self, preset: impl Into<String>) -> Self {
        self.direct_schema_pins.get_mut().unwrap().clear();
        self.direct_tool_item =
            self.default_module_id
                .as_ref()
                .map(|provider| broca_fetch_plan::plan::Item {
                    provider: provider.clone(),
                    preset: Some(preset.into()),
                    params: Default::default(),
                    optional: false,
                    output_refs: false,
                });
        self
    }

    fn with_connection(conn: Arc<dyn ToolPlaneConnection>, module_id: impl Into<String>) -> Self {
        std::sync::LazyLock::force(&crate::tool_result_diagnostics::TOOL_RESULT_JSON_DUMPS);
        let module_id = module_id.into();
        let default_module_id = (!module_id.trim().is_empty()).then_some(module_id);
        let shared = conn.shared_route_closer();
        let owns_closer = shared.is_none();
        let closer = shared
            .unwrap_or_else(|| RouteCloser::new(Arc::new(PlaneCloseTarget(Arc::downgrade(&conn)))));
        Self {
            #[cfg(test)]
            after_cache_gate: Mutex::new(None),
            closer,
            owns_closer,
            conn,
            default_module_id,
            direct_carrier_scope: None,
            direct_tool_item: None,
            direct_schema_pins: Mutex::new(BTreeMap::new()),
            routes: Mutex::new(HashMap::new()),
            batch_routes: Mutex::new(HashMap::new()),
            batch_presets: Mutex::new(HashMap::new()),
            approved_args: Mutex::new(HashMap::new()),
            provider_held: None,
            batch_manifests: Mutex::new(HashMap::new()),
            registered_keys: Mutex::new(HashMap::new()),
            inline_keys: Mutex::new(HashSet::new()),
            yielded_keys: Mutex::new(HashSet::new()),
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
            resend_turns: Mutex::new(HashMap::new()),
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
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn test_cached_route_is_open(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        module_id: &str,
    ) -> Option<bool> {
        let key = Self::route_key(bind, scope, module_id);
        let routes = self.routes.lock().unwrap();
        let handle = routes
            .iter()
            .filter(|(candidate, _)| candidate.same_target(&key))
            .max_by_key(|(_, route)| route.last_used)?
            .1
            .handle;
        Some(self.conn.route_is_open(&handle))
    }

    /// Shorten the scope hold, so a test can run a hold past its bound in seconds.
    #[cfg(debug_assertions)]
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

    #[cfg(debug_assertions)]
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
            .or_else(|| {
                self.direct_tool_item
                    .as_ref()
                    .filter(|item| item.provider == module_id)
                    .and_then(|item| item.preset.clone())
            })
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

    fn declared_reply(
        &self,
        ctx: &BatchCtx,
        module_id: &str,
        call: &ToolCall,
    ) -> Option<cortexkit_role_tool_provider::catalog::Reply> {
        let manifests = self.batch_presets.lock().unwrap();
        // A deadline the frozen fetch plan's provider declared wins; otherwise the one the caller
        // declared on the tool when it sent it. Both go through the role's deadline arithmetic,
        // which never lowers the runner's fallback and is never lowered by it.
        manifests
            .get(&Self::batch_route_key(ctx))
            .and_then(|manifest| {
                manifest
                    .provider_reply(module_id, &call.tool_name)
                    .cloned()
                    .or_else(|| manifest.declared_reply(&call.tool_name))
            })
    }

    fn reply_deadline(&self, ctx: &BatchCtx, module_id: &str, call: &ToolCall) -> Duration {
        let reply = self.declared_reply(ctx, module_id, call);
        let keepalive = self.frozen_contract(ctx, module_id).call_keepalive;
        if reply.is_some() || keepalive {
            return Duration::from_millis(
                cortexkit_role_tool_provider::catalog::reply_deadline_ms_with_keepalive(
                    reply.as_ref(),
                    &(call.input).parse(),
                    self.reply_fallback_ms(),
                    keepalive,
                ),
            );
        }
        let legacy = tool_reply_deadline(module_id, &call.tool_name, &(call.input).parse());
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

    async fn catalog_snapshot(
        &self,
    ) -> Result<Arc<crate::provider_catalog::ProviderCatalog>, ToolPlaneError> {
        if let Some(catalog) = self.catalog_memo.lock().unwrap().as_ref().cloned() {
            return Ok(catalog);
        }

        let fetched: Arc<crate::provider_catalog::ProviderCatalog> = Arc::new(
            self.conn
                .catalog_list()
                .await
                .map_err(conn_to_plane)?
                .into(),
        );
        let mut memo = self.catalog_memo.lock().unwrap();
        Ok(Arc::clone(memo.get_or_insert(fetched)))
    }

    async fn refresh_catalog_snapshot(
        &self,
    ) -> Result<Arc<crate::provider_catalog::ProviderCatalog>, ToolPlaneError> {
        self.clear_catalog_memo();
        let fetched: Arc<crate::provider_catalog::ProviderCatalog> = Arc::new(
            self.conn
                .catalog_list()
                .await
                .map_err(conn_to_plane)?
                .into(),
        );
        *self.catalog_memo.lock().unwrap() = Some(Arc::clone(&fetched));
        Ok(fetched)
    }

    fn aft_harness(&self) -> Option<Arc<crate::provider_catalog::AftHarness>> {
        self.catalog_memo
            .lock()
            .unwrap()
            .as_ref()
            .map(|catalog| Arc::clone(&catalog.aft_harness))
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
                            if !is_retryable_route_open(message.code())
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

    /// Discover a standalone preset's tools on its tool route. Without a preset, preserve
    /// registration-based discovery, which does not require a provider catalog RPC.
    pub async fn tools_from_catalog_for_bind(
        &self,
        bind: &BindIdentity,
        bind_timeout: Duration,
    ) -> Result<Vec<broca_protocol::request::Tool>, ToolPlaneError> {
        let Some(item) = &self.direct_tool_item else {
            return self.tools_from_catalog().await;
        };
        self.warm_bind(bind, bind_timeout).await?;
        let (route, _) = self
            .ensure_route(bind, None, &item.provider, &ToolOpenGuard::default())
            .await?;
        let request = broca_fetch_plan::admit::tool_catalog_request(Some(item));
        let arguments = serde_json::to_value(request)
            .map_err(|error| ToolPlaneError::Unavailable(error.to_string()))?;
        let answer = self
            .conn
            .tool_call(
                &route,
                broca_fetch_plan::CATALOG_OP,
                &arguments,
                None,
                broca_fetch_plan::ADMISSION_DEADLINE,
                item.preset.as_deref(),
            )
            .await
            .map_err(conn_to_plane)?;
        let catalog = broca_fetch_plan::admit::decode_tool_catalog(&item.provider, answer)
            .map_err(|error| {
                ToolPlaneError::Unavailable(format!(
                    "read {} tool.catalog: {error:?}",
                    item.provider
                ))
            })?;
        *self.direct_schema_pins.lock().unwrap() = catalog.schema_pins;
        Ok(catalog.tools)
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
        let entry = crate::provider_catalog::find_module(&catalog, module_id).ok_or_else(|| {
            ToolPlaneError::Unavailable(format!("module '{}' not found in subc catalog", module_id))
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
                reply: None,
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
            shape: RouteShape::default(),
            held_reply_v1: false,
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
        let key = Self::route_key(&ctx.bind, ctx.scope.as_ref(), module_id).with_handle(handle);
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
    async fn gone_since_last_batch(&self, ctx: &BatchCtx, guard: &ToolOpenGuard) -> Option<String> {
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
        // Nothing of the batch is sent yet, so run cancel and stop end the hold as well as the
        // session close; the batch's opens then see the interrupted run.
        let lifecycle = self.capture_session_lifecycle_token(&ctx.bind);
        let wait = WaitCancel::link(&lifecycle, guard);
        match self
            .read_scope(scope, wait.token(), guard.deadline, true)
            .await
        {
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
        let key = Self::route_key(bind, scope, module_id).with_handle(handle);
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
    /// The hold also ends at `until` when that comes first. `cancel` ends the wait between
    /// describes at once (a describe already in flight finishes or reaches its own timeout
    /// first): the caller passes the session close, linked with the run's cancel and stop when
    /// the reading is for a call not yet sent, so a stopped run never sits out the hold. A hold
    /// that ends early reads as "can't tell"; callers that hold a run guard check it to report
    /// the call as stopped before send.
    async fn read_scope(
        &self,
        scope: &SessionScope,
        cancel: &CancellationToken,
        until: Option<Instant>,
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
        let hold_end = Instant::now() + self.scope_sync_hold;
        let deadline = until.map_or(hold_end, |until| until.min(hold_end));
        let mut attempt = 0u32;
        loop {
            let reading = match describe_either_owner(&*describer, &owner, &scope.scope_ref).await {
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
                _ = cancel.cancelled() => {
                    return ScopeReading::Unverifiable("the wait ended during the hold".into());
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
    ///
    /// `earlier` are the calls re-opening on the same route that this call must not send
    /// ahead of (see [`SubcToolPlane::begin_resend_turn`]); the send waits until they finish.
    #[allow(clippy::too_many_arguments)]
    async fn resend_or_drain(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        module_id: &str,
        closed: RouteHandle,
        composed_under: Option<ScopeStamp>,
        patience: RouteOpenPatience,
        guard: &ToolOpenGuard,
        fence_key: FenceKey,
        key: Option<broca_core::tool_plane::DispatchKey<'_>>,
        earlier: &[CancellationToken],
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
        // The call is not sent yet: run cancel and stop end the hold too, not only the session
        // close, and a run they stopped reports the call as stopped before send.
        let reading = {
            let wait = WaitCancel::link(&lifecycle, guard);
            self.read_scope(scope, wait.token(), guard.deadline, true)
                .await
        };
        match reading {
            ScopeReading::Gone(reason) => {
                self.latch_gone(ctx, reason);
                return drained(reason);
            }
            ScopeReading::Unverifiable(_) | ScopeReading::NotSynced if guard.interrupted() => {
                return self.stopped_before_send(ctx, call);
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
            .ensure_result_route_for_lifecycle_inner(
                &ctx.bind,
                Some(scope),
                module_id,
                &lifecycle,
                guard.deadline,
                guard,
                patience,
                self.frozen_contract(ctx, module_id).route_shape(),
            )
            .await
            .map_err(|error| map_tool_open_refusal(error, module_id))
        {
            Ok(route) => route,
            // The run was cancelled or stopped during the re-open: never sent.
            Err(_) if guard.interrupted() => return self.stopped_before_send(ctx, call),
            Err(error @ ToolPlaneError::ScopeRefused { .. }) => {
                self.latch_open_refusal(ctx, &error);
                let ToolPlaneError::ScopeRefused { code, .. } = error else {
                    unreachable!()
                };
                self.latch_gone(ctx, &code);
                return drained(&code);
            }
            Err(ToolPlaneError::Permanent { code, detail }) => {
                return self.permanent_refusal_result(
                    ctx,
                    call,
                    &code,
                    detail.as_ref(),
                    &fence_key,
                );
            }
            Err(ToolPlaneError::ModuleUnavailable {
                module,
                code,
                detail,
            }) => {
                return self
                    .module_unavailable_result(ctx, call, &module, &code, &detail, &fence_key);
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
            self.forget_route(ctx, module_id, fresh);
            return drained(SCOPE_AGENT_CHANGED);
        }
        if guard.interrupted() {
            return self.stopped_before_send(ctx, call);
        }
        if !Self::wait_for_resend_turns(earlier, guard).await {
            return self.stopped_before_send(ctx, call);
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
            .send_call(
                ctx,
                module_id,
                &fresh,
                call,
                reply_deadline,
                key,
                &guard.signals(),
            )
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
            key,
            _permit,
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
        key: Option<broca_core::tool_plane::DispatchKey<'_>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> DispatchResult {
        let scope = ctx.scope.as_ref();
        // Taken whatever the outcome, so a yield accepted for a call that then failed is forgotten.
        let person_yield = key.is_some_and(|key| self.conn.took_person_yield(key.call_key));
        match outcome {
            Ok(raw) => {
                let content_v1 = self.frozen_tool_is_content_v1(ctx, module_id, &call.tool_name);
                let late_results = self.frozen_contract(ctx, module_id).late_results;
                let has_held = raw.get("held").is_some();
                let decoded = (sent_on.reads_tool_result && content_v1).then(|| {
                    serde_json::from_value::<cortexkit_role_tool_provider::result::ToolResult>(
                        raw.clone(),
                    )
                    .map_err(|error| error.to_string())
                    .and_then(|mut reply| {
                        if reply.held().is_some() {
                            cortexkit_role_llm_runner::tool_result_limit::enforce(&mut reply);
                        }
                        if reply.held().is_some()
                            || cortexkit_role_tool_provider::result::model_visible_bytes(&reply)
                                <= cortexkit_role_tool_provider::result::MAX_MODEL_VISIBLE_BYTES
                        {
                            cortexkit_role_tool_provider::result::validate(&reply)
                                .map_err(|error| error.to_string())?;
                        }
                        Ok(reply)
                    })
                });
                if has_held {
                    let held = decoded
                        .as_ref()
                        .and_then(|reply| reply.as_ref().ok())
                        .filter(|_| sent_on.reads_held_reply)
                        .and_then(|reply| reply.held());
                    let eligible_key = key.filter(|key| {
                        self.registered_keys
                            .lock()
                            .unwrap()
                            .get(&Self::batch_route_key(ctx))
                            .and_then(|keys| keys.get(&call.tool_call_id))
                            .is_some_and(|(registered_module, registered_key)| {
                                broca_protocol::same_module(registered_module, module_id)
                                    && registered_key == key.call_key
                            })
                    });
                    // The person's steer had this call handed back (llm-runner `call_yield`): a
                    // held reply answering that yield closes the call now, with its interim
                    // content as the result. The call is never held and never pauses the run;
                    // its registration stays, so the provider's later terminal for it reaches
                    // the session as a notice.
                    if person_yield
                        && late_results
                        && self.provider_held.is_some()
                        && eligible_key.is_some()
                        && held.is_some_and(|held| held.yielded == Some(true))
                    {
                        let mut interim = decoded
                            .as_ref()
                            .and_then(|reply| reply.as_ref().ok())
                            .cloned()
                            .expect("a held reply was decoded");
                        interim.held = None;
                        let result =
                            crate::content_v1::map_content_v1_result(&call.tool_call_id, &interim)
                                .unwrap_or_else(|error| {
                                    route_failure_result(
                                        call,
                                        format!("malformed handed-back tool reply: {error:?}"),
                                    )
                                });
                        self.fence
                            .lock()
                            .unwrap()
                            .insert(&ctx.batch_id, fence_key, result.clone());
                        if let Some(key) = eligible_key {
                            self.yielded_keys
                                .lock()
                                .unwrap()
                                .insert(key.call_key.to_owned());
                        }
                        return DispatchResult::Yielded {
                            result,
                            carrier: module_id.into(),
                        };
                    }
                    if let (Some(support), Some(held), Some(key), Some(_)) = (
                        self.provider_held.as_ref(),
                        held,
                        eligible_key.filter(|_| late_results),
                        self.conn.consumer_module_id(),
                    ) {
                        if let Some(recorder) = key.held_recorder {
                            use sha2::Digest;
                            let reply = broca_core::tool_plane::ProviderHeldReply {
                                tool_call_id: call.tool_call_id.clone(),
                                call_key: key.call_key.into(),
                                module: module_id.into(),
                                reason: held.reason.clone(),
                                detail_digest: held.detail.as_ref().map(|detail| {
                                    format!(
                                        "{:x}",
                                        sha2::Sha256::digest(
                                            serde_json::to_vec(detail)
                                                .expect("JSON object serializes")
                                        )
                                    )
                                }),
                            };
                            if let Err(error) = recorder.sync_held(&reply).await {
                                return DispatchResult::SentButOutcomeUnknown {
                                    reason: format!("provider-held record not durable: {error}"),
                                };
                            }
                            drop(permit);
                            let terminal = support.lifecycle.wait(ctx, call, &reply).await;
                            if let DispatchResult::Completed(result) = &terminal {
                                self.fence.lock().unwrap().insert(
                                    &ctx.batch_id,
                                    fence_key,
                                    result.clone(),
                                );
                            }
                            return terminal;
                        }
                    }
                }
                if let Some(key) = key {
                    // Accepted holds return above. Only inline answers can retire a watch at
                    // the batch's durability barrier without inspecting the WAL again.
                    let registered = self
                        .registered_keys
                        .lock()
                        .unwrap()
                        .get(&Self::batch_route_key(ctx))
                        .and_then(|keys| keys.get(&call.tool_call_id))
                        .is_some_and(|(module, registered_key)| {
                            broca_protocol::same_module(module, module_id)
                                && registered_key == key.call_key
                        });
                    if registered {
                        self.inline_keys
                            .lock()
                            .unwrap()
                            .insert(key.call_key.to_owned());
                    }
                }
                let (result, captures) = if has_held {
                    // Never quote the body: even a malformed held reply contains interim text,
                    // which must not be mistaken for a final answer in a later prompt.
                    (
                        route_failure_result(
                            call,
                            "malformed provider-held reply: prerequisites not met".into(),
                        ),
                        Vec::new(),
                    )
                } else if sent_on.reads_tool_result && content_v1 {
                    let mut result = decoded
                        .as_ref()
                        .expect("negotiated result was decoded")
                        .as_ref()
                        .map_err(Clone::clone)
                        .and_then(|reply| {
                            crate::content_v1::map_content_v1_result(&call.tool_call_id, reply)
                                .map_err(|error| error.to_string())
                        })
                        .unwrap_or_else(|reason| {
                            route_failure_result(
                                call,
                                format!("malformed tool-result/v1 tool reply: {reason}"),
                            )
                        });
                    let captures = if ctx.media_capture.is_some()
                        && decoded.as_ref().is_some_and(|reply| {
                            reply.as_ref().is_ok_and(|reply| {
                                cortexkit_role_tool_provider::result::model_visible_bytes(reply)
                                    <= cortexkit_role_tool_provider::result::MAX_MODEL_VISIBLE_BYTES
                            })
                        }) {
                        tool_capture::content_v1_captures(&mut result, &raw)
                    } else {
                        Vec::new()
                    };
                    (result, captures)
                } else if let Some(error) =
                    cortexkit_role_llm_runner::tool_result_limit::legacy_size_error(&raw)
                {
                    (
                        crate::content_v1::size_error_result(&call.tool_call_id, error),
                        Vec::new(),
                    )
                } else if ctx.media_capture.is_some() {
                    map_call_tool_result(&call.tool_call_id, &raw)
                } else {
                    (
                        map_legacy_call_tool_result(&call.tool_call_id, &raw),
                        Vec::new(),
                    )
                };
                let result = tool_capture::capture(ctx, result, captures).await;
                crate::tool_result_diagnostics::observe_inline_result(
                    ctx,
                    call,
                    module_id,
                    key.map(|key| key.call_key),
                    &result,
                );
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
                    let _ = self.invalidate_route_for_module(&ctx.bind, scope, module_id, sent_on);
                    return DispatchResult::SentButOutcomeUnknown {
                        reason: msg.to_string(),
                    };
                }
                if crate::connection::classify_tool_route_refusal(&msg)
                    == crate::connection::ToolRouteRefusal::Reopen
                {
                    // The transparent retry also lost its route. Keep the model-visible error,
                    // but discard the known-dead replacement so the next call opens afresh.
                    self.remove_batch_call_route(ctx, &call.tool_call_id);
                    let _ = self.invalidate_route_for_module(&ctx.bind, scope, module_id, sent_on);
                }
                if let Some(output) =
                    crate::content_v1::oversized_refusal(msg.code(), msg.message())
                {
                    let result = ToolResult {
                        tool_call_id: call.tool_call_id.clone(),
                        output,
                        is_error: true,
                    };
                    self.fence
                        .lock()
                        .unwrap()
                        .insert(&ctx.batch_id, fence_key, result.clone());
                    return DispatchResult::Completed(result);
                }
                // `module_reloading` here answers the one re-send after a re-open: the module
                // began draining again. The protocol refuses it before the request reaches the
                // tool, so nothing was sent, and the call is reported exactly like a module that
                // stayed down through the re-open wait. Not re-sent again: one re-send per call.
                // The route is forgotten, not closed, for the reason the first refusal's is.
                if msg.code() == subc_protocol::error_codes::MODULE_RELOADING {
                    self.remove_batch_call_route(ctx, &call.tool_call_id);
                    let _ = self.invalidate_route_for_module(&ctx.bind, scope, module_id, sent_on);
                    return self.module_unavailable_result(
                        ctx,
                        call,
                        module_id,
                        msg.code(),
                        &broca_protocol::bound_module_unavailable_detail(&msg.detail()),
                        &fence_key,
                    );
                }
                // A provider that received the call and refuses it with `scope_not_synced` (Plexus
                // does this on its bot-write legs until the owner has synced) lands here like any
                // other refusal: recorded as this call's error result, and NOT re-sent. The
                // provider already has the request. A durable call key correlates late delivery;
                // it does not authorize a retry after a provider received the call. Only a
                // route.open refusal, where nothing was sent, is held and retried. A test
                // (`a_provider_scope_not_synced_refusal_is_recorded_and_never_resent`) pins this.
                let output =
                    if msg.code() == cortexkit_role_tool_provider::errors::TOOL_ROLE_NOT_DECLARED {
                        broca_core::tool_plane::permanent_refusal_output(
                            msg.code(),
                            msg.provider_detail(),
                        )
                    } else {
                        ToolOutput::error(format!("tool/route rejected: {msg}"))
                    };
                let result = ToolResult {
                    tool_call_id: call.tool_call_id.clone(),
                    output,
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
                let _ = self.invalidate_route_for_module(&ctx.bind, scope, module_id, sent_on);
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
                if let Some(handle) =
                    self.invalidate_route_for_module(&ctx.bind, scope, module_id, sent_on)
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
        let reason = match self.read_scope(scope, &lifecycle, None, false).await {
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

    async fn direct_carrier_selector(&self) -> Result<Option<ScopeSelector>, RouteEnsureError> {
        let Some(selector) = &self.direct_carrier_scope else {
            return Ok(None);
        };
        let refused = |code: &str, detail: &str| {
            RouteEnsureError::Plane(ToolPlaneError::ScopeRefused {
                code: code.into(),
                detail: detail.into(),
            })
        };
        if self.conn.consumer_module_id().is_some() {
            return Err(refused(
                SCOPE_UNSUPPORTED,
                "a private carrier selector requires a Direct connection",
            ));
        }
        if selector.bind_proof.is_none() {
            return Err(refused(
                subc_protocol::error_codes::SCOPE_BIND_PROOF_REQUIRED,
                "a Direct carrier must supply its private bind proof",
            ));
        }
        let capabilities = self.conn.server_capabilities().await.map_err(|_| {
            refused(
                SCOPE_UNVERIFIABLE,
                "could not read the daemon's scope capabilities",
            )
        })?;
        if !capabilities.iter().any(|cap| cap == CAP_SCOPES_V1)
            || !capabilities
                .iter()
                .any(|cap| cap == subc_protocol::manifest::CAP_SCOPE_BIND_PROOF_V1)
        {
            return Err(refused(
                SCOPE_UNSUPPORTED,
                "the daemon must advertise scopes/v1 and scope-bind-proof/v1 for a Direct carrier",
            ));
        }
        Ok(Some(selector.clone()))
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
        frozen_scope_selector(scope).map_err(|detail| {
            RouteEnsureError::Plane(ToolPlaneError::ScopeRefused {
                code: SCOPE_UNVERIFIABLE.into(),
                detail,
            })
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

    /// Record that a call on `key` is about to give its credit back and re-open, because its
    /// first request was refused before it reached the tool. The turn ends when the returned
    /// value is dropped: when that call has finished, sent or not.
    fn begin_resend_turn(&self, key: &RouteKey) -> ResendTurn<'_> {
        let done = CancellationToken::new();
        let mut turns = self.resend_turns.lock().unwrap();
        let entry = turns.entry(key.clone()).or_default();
        entry.retain(|turn| !turn.is_cancelled());
        entry.push(done.clone());
        ResendTurn {
            turns: &self.resend_turns,
            key: key.clone(),
            done,
        }
    }

    /// The re-opening calls on `key` that have not finished yet.
    fn resend_turns_in_progress(&self, key: &RouteKey) -> Vec<CancellationToken> {
        self.resend_turns
            .lock()
            .unwrap()
            .get(key)
            .map(|turns| {
                turns
                    .iter()
                    .filter(|turn| !turn.is_cancelled())
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Wait until every call in `earlier` has finished. False when the run was cancelled or
    /// stopped first.
    async fn wait_for_resend_turns(earlier: &[CancellationToken], guard: &ToolOpenGuard) -> bool {
        for turn in earlier {
            tokio::select! {
                biased;
                () = guard.cancelled() => return false,
                () = turn.cancelled() => {}
            }
        }
        !guard.interrupted()
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
        let catalog: Arc<crate::provider_catalog::ProviderCatalog> = Arc::new(
            self.conn
                .catalog_list()
                .await
                .map_err(conn_to_plane)?
                .into(),
        );
        self.catalog_memo
            .lock()
            .unwrap()
            .get_or_insert_with(|| Arc::clone(&catalog));
        let window = crate::provider_catalog::find_module(&catalog, module_id)
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
    /// route channel and a shared semaphore handle. This is the tool-dispatch open: a
    /// module-availability refusal is retried for [`TOOL_DISPATCH_PATIENCE`], and one that
    /// outlasts it becomes [`ToolPlaneError::ModuleUnavailable`] (proven undelivered).
    /// `guard` ends the wait early on run cancel/stop, and `guard.deadline` caps it at the
    /// step deadline that comes earlier.
    async fn ensure_route(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        module_id: &str,
        guard: &ToolOpenGuard,
    ) -> Result<(RouteHandle, Arc<Semaphore>), ToolPlaneError> {
        let lifecycle = self.capture_session_lifecycle_token(bind);
        self.ensure_route_for_lifecycle_inner(
            bind,
            scope,
            module_id,
            &lifecycle,
            guard.deadline,
            guard,
            TOOL_DISPATCH_PATIENCE,
        )
        .await
        .map_err(|error| map_tool_open_refusal(error, module_id))
    }

    async fn ensure_batch_route(
        &self,
        ctx: &BatchCtx,
        module_id: &str,
        guard: &ToolOpenGuard,
    ) -> Result<(RouteHandle, Arc<Semaphore>), ToolPlaneError> {
        let shape = self.frozen_contract(ctx, module_id).route_shape();
        if shape == RouteShape::default() {
            return self
                .ensure_route(&ctx.bind, ctx.scope.as_ref(), module_id, guard)
                .await;
        }
        let lifecycle = self.capture_session_lifecycle_token(&ctx.bind);
        self.ensure_result_route_for_lifecycle_inner(
            &ctx.bind,
            ctx.scope.as_ref(),
            module_id,
            &lifecycle,
            guard.deadline,
            guard,
            TOOL_DISPATCH_PATIENCE,
            shape,
        )
        .await
        .map_err(|error| map_tool_open_refusal(error, module_id))
    }

    /// Open a route to `module_id` for `bind`, under `scope` when the run was admitted under
    /// one. This is the only place a tool route is opened, so it is where a scoped run's route
    /// is guaranteed to carry its scope: a scoped open that cannot carry it is refused, never
    /// sent unscoped.
    #[allow(clippy::too_many_arguments)]
    async fn route_open_for_lifecycle(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        module_id: &str,
        lifecycle: &CancellationToken,
        retry_deadline: Option<Instant>,
        guard: &ToolOpenGuard,
        patience: RouteOpenPatience,
        shape: RouteShape,
    ) -> Result<(RouteHandle, Option<ScopeStamp>), RouteEnsureError> {
        // One computed deadline keeps the caller budget authoritative while retaining a short
        // per-iteration cap. Using either bound alone would either overshoot a short warm timeout
        // or let a long timeout keep a module that is down under retry long after the outer
        // exponential backoff should have taken over.
        let cap = Instant::now() + patience.deadline;
        let deadline = retry_deadline.map_or(cap, |caller| caller.min(cap));
        // The retry loop is the connection layer's, shared with every other route-open
        // consumer; this plane supplies only the policy. Every attempt also passes the module's
        // availability gate, so the opens of all sessions and scopes to one module that is down
        // make one probe per backoff tick between them.
        let retry = || RouteOpenRetry {
            budget: deadline.saturating_duration_since(Instant::now()),
            max_interval: patience.max_interval,
        };
        // A tool-call dispatch open waits out an outage from its first refusal, ending at the
        // caller's absolute deadline at the latest (`retry_deadline`: the run's step deadline).
        let outage = patience.waits_out_outage.then_some(OutageWait {
            patience: patience.deadline,
            max_interval: patience.max_interval,
            deadline: retry_deadline,
        });
        let gate_for = || self.conn.module_gate(module_id);
        // Run cancel/stop ends the wait at once too, not just the session close: link the
        // session lifecycle to the run's guard, and report which side fired.
        let wait = WaitCancel::link(lifecycle, guard);
        let cancel = wait.token();
        let cancelled = || {
            RouteEnsureError::Plane(ToolPlaneError::Unavailable(if guard.interrupted() {
                "run cancelled or stopped during route open".into()
            } else {
                "session closed during route open".into()
            }))
        };
        let Some(scope) = scope else {
            let selector = self.direct_carrier_selector().await?;
            let attempt = || {
                self.conn.route_open_dispatch(
                    module_id,
                    bind,
                    selector.as_ref(),
                    shape,
                    self.held_reply_declared(),
                    self.aft_harness(),
                )
            };
            return match outage {
                Some(outage) => {
                    retry_route_open_waiting_out_outage(
                        outage,
                        None,
                        Some(cancel),
                        gate_for,
                        attempt,
                    )
                    .await
                }
                None => retry_route_open(retry(), Some(cancel), gate_for, attempt).await,
            }
            .map(|handle| (handle, None))
            .map_err(|error| match error {
                // A refusal may quote a request. Keep its code, but never expose a private proof
                // through diagnostic bodies returned by the daemon or transport.
                RouteOpenError::Conn(error) if selector.is_some() => {
                    RouteEnsureError::Plane(ToolPlaneError::ScopeRefused {
                        code: error.code().unwrap_or(SCOPE_UNVERIFIABLE).into(),
                        detail: "the Direct carrier route could not be opened".into(),
                    })
                }
                RouteOpenError::Conn(error) => RouteEnsureError::Connection(error),
                RouteOpenError::Cancelled => cancelled(),
            });
        };
        if self.direct_carrier_scope.is_some() {
            return Err(RouteEnsureError::Plane(ToolPlaneError::ScopeRefused {
                code: SCOPE_UNSUPPORTED.into(),
                detail: "a private Direct carrier selector cannot replace a frozen session scope"
                    .into(),
            }));
        }
        let selector = self.scoped_selector(scope).await?;
        // The hold is bounded by the scope hold and by the caller's own deadline, whichever
        // comes first. Inside a run there is no caller deadline, so the hold alone bounds it; a
        // session close, run cancel or stop ends it at once through the linked `cancel`. The
        // same token ends the scope-sync waits of the describes that bracket the open; a
        // describe already in flight finishes or times out first.
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
            let before = match self.read_scope(scope, cancel, guard.deadline, true).await {
                ScopeReading::Live(stamp) => stamp,
                ScopeReading::Gone(reason) => {
                    return Err(refused(
                        reason,
                        format!("scope.describe says the scope is gone ({reason})"),
                    ))
                }
                // The run was cancelled or stopped while the describe held: nothing is open yet.
                ScopeReading::NotSynced | ScopeReading::Unverifiable(_) if guard.interrupted() => {
                    return Err(cancelled())
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
            let attempt = || {
                self.conn.route_open_dispatch(
                    module_id,
                    bind,
                    Some(&selector),
                    shape,
                    self.held_reply_declared(),
                    self.aft_harness(),
                )
            };
            let opened = match outage {
                Some(outage) => {
                    retry_route_open_waiting_out_outage(
                        outage,
                        Some(hold),
                        Some(cancel),
                        gate_for,
                        attempt,
                    )
                    .await
                }
                None => {
                    retry_route_open_with_scope_hold(
                        retry(),
                        Some(hold),
                        Some(cancel),
                        gate_for,
                        attempt,
                    )
                    .await
                }
            };
            let handle = opened.map_err(|error| match error {
                RouteOpenError::Conn(ConnError::Rejected(message)) => {
                    scoped_open_refusal(message, self.scope_sync_hold)
                }
                RouteOpenError::Conn(error) => RouteEnsureError::Connection(error),
                RouteOpenError::Cancelled => cancelled(),
            })?;
            match self.read_scope(scope, cancel, guard.deadline, true).await {
                ScopeReading::Live(after) if may_resend_under(&before, &after) => {
                    return Ok((handle, Some(*after)));
                }
                // The run was cancelled or stopped while the describe held: the route opened
                // but carries nothing, so close it.
                ScopeReading::NotSynced | ScopeReading::Unverifiable(_) if guard.interrupted() => {
                    let _ = self.closer.close(handle).await;
                    return Err(cancelled());
                }
                ScopeReading::Live(_) => {
                    // The scope's stamp changed while the route was opening, so the route may
                    // be bound under either the earlier or the later stamp. Close it and retry.
                    let _ = self.closer.close(handle).await;
                }
                ScopeReading::Gone(reason) => {
                    let _ = self.closer.close(handle).await;
                    return Err(refused(
                        reason,
                        format!("scope.describe says the scope is gone ({reason})"),
                    ));
                }
                ScopeReading::NotSynced => {
                    let _ = self.closer.close(handle).await;
                    return Err(refused(
                        SCOPE_UNVERIFIABLE,
                        "the scope's owner has not synced since the daemon started".into(),
                    ));
                }
                ScopeReading::Unverifiable(detail) => {
                    let _ = self.closer.close(handle).await;
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

    #[allow(clippy::too_many_arguments)]
    async fn ensure_route_for_lifecycle_inner(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        module_id: &str,
        lifecycle: &CancellationToken,
        retry_deadline: Option<Instant>,
        guard: &ToolOpenGuard,
        patience: RouteOpenPatience,
    ) -> Result<(RouteHandle, Arc<Semaphore>), RouteEnsureError> {
        self.ensure_result_route_for_lifecycle_inner(
            bind,
            scope,
            module_id,
            lifecycle,
            retry_deadline,
            guard,
            patience,
            RouteShape::default(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn ensure_result_route_for_lifecycle_inner(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        module_id: &str,
        lifecycle: &CancellationToken,
        retry_deadline: Option<Instant>,
        guard: &ToolOpenGuard,
        patience: RouteOpenPatience,
        mut shape: RouteShape,
    ) -> Result<(RouteHandle, Arc<Semaphore>), RouteEnsureError> {
        let mut key = Self::route_key(bind, scope, module_id);
        if lifecycle.is_cancelled() {
            return Err(RouteEnsureError::Plane(ToolPlaneError::Unavailable(
                "session closed before route open".into(),
            )));
        }
        // A cold standalone route needs both concurrency and role metadata. Keep the
        // manifest read for its window so declaring the role does not add another query.
        let cold_catalog = self.catalog_memo.lock().unwrap().is_none();
        let cold_window = !self.windows.lock().unwrap().contains_key(module_id);
        if cold_catalog && cold_window {
            self.resolve_window(module_id)
                .await
                .map_err(RouteEnsureError::Plane)?;
        }
        let catalog = self
            .catalog_snapshot()
            .await
            .map_err(RouteEnsureError::Plane)?;
        let policy = crate::connection::ToolProviderRoutePolicy::for_provider(
            &catalog,
            module_id,
            shape.session_identity,
        );
        shape.tool_provider_v1 = policy.declares_role;
        key.shape = RouteShape {
            tool_result_v1: shape.tool_result_v1 || self.held_reply_declared(),
            ..shape
        };
        key.held_reply_v1 = self.held_reply_declared();
        // Deliberately no in-flight placeholder: two concurrent misses on one key both open
        // a route, and `cache_route_state` keeps one and closes the loser.
        //
        // A per-route single-flight was tried here and removed. When a module is DOWN, every
        // tool call in flight misses at once, and with each running its own retry loop one
        // broca connection sent about 80 route.open a second to a module stopped for a minute
        // (other clients: about 5). Sharing one open per key looked like the fix, but handing
        // one route to several calls meant one call's cleanup (a scope bracket that did not
        // confirm, a cancelled session) could close the route another call had just cached,
        // and the shared outcome had to survive until every waiter had read it. The storm is
        // fixed below this point instead: every open to a module passes that module's
        // availability gate (`ModuleGate`), which lets one probe through per backoff tick while
        // the module is down, whatever route each waiter wants. When the module returns, each
        // waiting call makes one open of its own: a bounded burst, after which this cache keeps
        // one route per key as before.
        {
            let mut routes = self.routes.lock().unwrap();
            if let Some(route) = routes.get_mut(&key) {
                route.last_used = Instant::now();
                return Ok((route.handle, Arc::clone(&route.sem)));
            }
        }
        // A route may be opened before the session has a plan. Keep different role, identity
        // and reply-format declarations in separate cache entries, even when opens race.
        // Resolve the window from THIS route's provider at open time (per-provider, cached
        // per module_id) — not a single global memo.
        let window = self
            .resolve_window(module_id)
            .await
            .map_err(RouteEnsureError::Plane)?;
        let (handle, stamp) = self
            .route_open_for_lifecycle(
                bind,
                scope,
                module_id,
                lifecycle,
                retry_deadline,
                guard,
                patience,
                shape,
            )
            .await?;
        let sem = Arc::new(Semaphore::new(window));
        let cached = {
            let mut routes = self.routes.lock().unwrap();
            if lifecycle.is_cancelled() {
                None
            } else {
                Some(cache_route_state(
                    &mut routes,
                    key.clone(),
                    handle,
                    Arc::clone(&sem),
                    stamp,
                ))
            }
        };

        let Some(cached) = cached else {
            self.closer.close(handle).await;
            return Err(RouteEnsureError::Plane(ToolPlaneError::Unavailable(
                "session closed during route open".into(),
            )));
        };

        // If another task won the race to cache the session route, close the redundant daemon
        // channel we just opened. The lock is already released here, so the best-effort close
        // does not block unrelated route lookups.
        if let Some(loser_handle) = cached.loser_handle {
            let _ = self.closer.close(loser_handle).await;
        }

        #[cfg(test)]
        {
            let gate = self.after_cache_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
        }

        if lifecycle.is_cancelled() {
            // Close only our own open. An old generation must not evict a fresh generation's
            // route under the same key after the actor has reopened.
            let removed = self.take_route_if_owned(&key, handle);
            if removed.is_some() {
                self.closer.close(handle).await;
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
            &ToolOpenGuard::default(),
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
            run_transport::invalidate(module_id);
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
        refused: RouteHandle,
    ) -> Option<RouteHandle> {
        run_transport::invalidate(module_id);
        let key = Self::route_key(bind, scope, module_id);
        let removed = self
            .take_route_if_owned(&key, refused)
            .map(|route| route.handle);
        // A dead route usually means a module restarted or disappeared. The dispatch or external
        // invalidation path removes affected batch-local decisions separately; the next batch must
        // discover any changed providers rather than inheriting this catalog snapshot.
        self.clear_catalog_memo();
        removed
    }

    fn close_route_best_effort(&self, handle: RouteHandle) {
        let closer = Arc::clone(&self.closer);
        tokio::spawn(async move {
            closer.close(handle).await;
        });
    }

    fn take_route_if_owned(&self, key: &RouteKey, handle: RouteHandle) -> Option<RouteState> {
        let key = key.clone().with_handle(handle);
        let mut routes = self.routes.lock().unwrap();
        if routes.get(&key).is_some_and(|route| route.handle == handle) {
            routes.remove(&key)
        } else {
            None
        }
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
            .iter()
            .filter(|(candidate, _)| candidate.same_target(&key))
            .max_by_key(|(_, route)| route.last_used)
            .map(|(_, route)| route.handle.channel)
    }

    /// Close all of a session's module routes: send route GOODBYE for each cached channel so
    /// the daemon FREES the channels, then drop the local entries. Call this when a SESSION ENDS
    /// (the coordinator's actor closes)
    /// so a long-lived SHARED consumer connection (one plane serving many sessions over one
    /// connection) doesn't accumulate stale channels across sessions. Unlike a per-session
    /// plane (where dropping the plane reclaims its routes), a SHARED plane is long-lived, so
    /// the channel must be explicitly freed. Failed Goodbyes are retried separately; a later
    /// dispatch opens a fresh route and is never retried by the teardown queue.
    ///
    /// This API is not a dispatch barrier by itself. The actor must stop and join its run
    /// and dispatch futures before calling it; that ordering prevents old work from acquiring
    /// a new lifecycle or committing a request during the asynchronous wire cleanup.
    pub fn close_session_routes(
        &self,
        bind: &BindIdentity,
    ) -> futures::future::BoxFuture<'static, ()> {
        let session = Self::session_key(bind);
        let mut lifecycles = self.session_cancel.lock().unwrap();
        let channels = {
            let mut routes = self.routes.lock().unwrap();
            if let Some(token) = lifecycles.remove(&session) {
                token.cancel();
            }
            let keys: Vec<RouteKey> = routes
                .keys()
                .filter(|key| key.session == session)
                .cloned()
                .collect();
            keys.into_iter()
                .filter_map(|key| {
                    routes
                        .remove(&key)
                        .map(|route| (key.module_id, route.handle))
                })
                .collect::<Vec<_>>()
        };
        self.fence.lock().unwrap().evict_session(&session);
        self.approved_args
            .lock()
            .unwrap()
            .retain(|(owner, _), _| owner != &session);
        self.batch_manifests
            .lock()
            .unwrap()
            .retain(|(owner, _), _| owner != &session);
        self.registered_keys
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
        drop(lifecycles);
        let count = channels.len();
        if count == 0 {
            return Box::pin(async {});
        }
        let closer = Arc::clone(&self.closer);
        let address = broca_core::session_addr(bind);
        // The actor's bounded close may stop awaiting us. Teardown and its summary must
        // survive that timeout rather than abandon the remaining provider channels.
        let task = tokio::spawn(async move {
            let modules: BTreeSet<_> = channels.iter().map(|(module, _)| module.clone()).collect();
            let results =
                join_all(channels.into_iter().map(|(_, handle)| closer.close(handle))).await;
            let failures = results.iter().filter(|closed| !**closed).count();
            tracing::info!(target: "broca.tool_routes", session = %address,
                provider_modules = ?modules, routes_closed = count - failures,
                goodbye_failures = failures, "session tool routes closed");
        });
        Box::pin(async move {
            let _ = task.await;
        })
    }

    /// Aggregate-only diagnostics: no session identity or tool content crosses the health surface.
    pub fn tool_routes_health(&self, open_sessions: &[BindIdentity]) -> serde_json::Value {
        let open: std::collections::HashSet<_> =
            open_sessions.iter().map(Self::session_key).collect();
        let routes = self.routes.lock().unwrap();
        let mut providers = std::collections::BTreeMap::<&str, usize>::new();
        let mut closed = 0;
        let mut oldest = None::<u64>;
        for (key, route) in routes.iter() {
            *providers.entry(&key.module_id).or_default() += 1;
            closed += usize::from(!open.contains(&key.session));
            let age = route.last_used.elapsed().as_secs();
            oldest = Some(oldest.map_or(age, |previous| previous.max(age)));
        }
        serde_json::json!({
            "open_session_actors": open.len(),
            "cached_routes": routes.len(),
            "cached_routes_per_provider": providers,
            "oldest_route_idle_secs": oldest,
            "routes_for_closed_sessions": closed,
            "goodbye_failures_since_boot": self.closer.failures(),
            "pending_goodbyes": self.closer.pending(),
            "goodbye_retry_overflows_since_boot": self.closer.overflows(),
            "goodbye_giveups_since_boot": self.closer.giveups(),
        })
    }
}

impl SubcToolPlane {
    async fn batch_started_inner(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
        guard: &ToolOpenGuard,
    ) -> Result<(), ToolPlaneError> {
        if ctx.scope.is_some() {
            // No call of this batch is in flight yet (its intents are written after this
            // returns), so what the run already learned, or a drain that closed its routes since
            // the last batch, decides the whole batch before anything opens.
            if let Some(latch) = self.run_latch(ctx) {
                return Err(latch.into_plane_error());
            }
            if let Some(reason) = self.gone_since_last_batch(ctx, guard).await {
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

        self.batch_manifests
            .lock()
            .unwrap()
            .insert(Self::batch_route_key(ctx), manifest.clone());

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
                .map(|module_id| self.ensure_batch_route(ctx, module_id, guard)),
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

    fn dispatch_target_inner(&self, ctx: &BatchCtx, tool_call_id: &ToolCallId) -> Option<String> {
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

    /// A terminal result for a call proven undelivered because its module stayed down for
    /// the whole tool-dispatch patience. The message stays the stable unavailable string
    /// while the module, the daemon's code and its detail ride in sibling fields, so the
    /// model can retry rather than give up. Fenced like any other Completed result, so a
    /// repeated id returns it without re-sending.
    fn module_unavailable_result(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        module_id: &str,
        code: &str,
        detail: &str,
        fence_key: &FenceKey,
    ) -> DispatchResult {
        let result = ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::module_unavailable(module_id, code, detail),
            is_error: true,
        };
        self.fence
            .lock()
            .unwrap()
            .insert(&ctx.batch_id, fence_key.clone(), result.clone());
        DispatchResult::Completed(result)
    }

    fn permanent_refusal_result(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        code: &str,
        detail: Option<&serde_json::Value>,
        fence_key: &FenceKey,
    ) -> DispatchResult {
        let result = ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: broca_core::tool_plane::permanent_refusal_output(code, detail),
            is_error: true,
        };
        self.fence
            .lock()
            .unwrap()
            .insert(&ctx.batch_id, fence_key.clone(), result.clone());
        DispatchResult::Completed(result)
    }

    /// The not-sent result for a call whose run was cancelled or stopped before its request
    /// was enqueued. An approved call keeps the reason its records have always carried.
    fn stopped_before_send(&self, ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        let approved = self
            .approved_args
            .lock()
            .unwrap()
            .get(&Self::batch_route_key(ctx))
            .is_some_and(|calls| calls.contains_key(&call.tool_call_id));
        DispatchResult::NotSent {
            reason: if approved {
                "approved_dispatch_stopped_before_send"
            } else {
                "dispatch_stopped_before_send"
            }
            .into(),
        }
    }

    async fn dispatch_inner(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        key: Option<broca_core::tool_plane::DispatchKey<'_>>,
        guard: &ToolOpenGuard,
    ) -> DispatchResult {
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
        // The call's own execution budget once sent. It does not bound the route-open wait in
        // front of the send: that ends at the guard's absolute deadline (the run's step
        // deadline), the tool-dispatch patience, or run cancel/stop, whichever comes first.
        let reply_deadline = self.reply_deadline(ctx, &module_id, call);
        // The same run signals guard the physical enqueue after every open and permit wait.
        let signals = guard.signals();

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
                            TOOL_DISPATCH_PATIENCE,
                            guard,
                            fence_key,
                            key,
                            &[],
                        )
                        .await;
                }
                composed_under = Some(stamp);
            }
        }

        // route.open / semaphore failures are BEFORE any tool send → NotSent (the
        // call never left the process; safe to (re-)dispatch). A module that stays down
        // for the whole tool-dispatch patience instead ends the call with the attributed
        // unavailable result (still never sent).
        let (route_handle, sem) = match self.ensure_batch_route(ctx, &module_id, guard).await {
            Ok(rs) => rs,
            // The run was cancelled or stopped during the open: never sent.
            Err(_) if guard.interrupted() => return self.stopped_before_send(ctx, call),
            Err(ToolPlaneError::Permanent { code, detail }) => {
                return self.permanent_refusal_result(
                    ctx,
                    call,
                    &code,
                    detail.as_ref(),
                    &fence_key,
                );
            }
            Err(ToolPlaneError::ModuleUnavailable {
                module,
                code,
                detail,
            }) => {
                return self
                    .module_unavailable_result(ctx, call, &module, &code, &detail, &fence_key);
            }
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
        // The open may have waited out an outage; a run cancelled or stopped meanwhile sends
        // nothing, and does not queue for flow-control credit either.
        if guard.interrupted() {
            return self.stopped_before_send(ctx, call);
        }
        // Flow control: hold a route permit from send to terminal so concurrent
        // in-flight never exceeds the provider's window (Serial → serializes).
        let first_permit = match Arc::clone(&sem).acquire_owned().await {
            Ok(p) => p,
            Err(_) => {
                return DispatchResult::NotSent {
                    reason: "route semaphore closed".into(),
                }
            }
        };

        // Another task may have invalidated/re-opened the session route while this task was
        // waiting for flow-control credit. This check runs strictly before the send, so the
        // call has sent nothing: it re-opens once, as its first send, rather than fail the
        // turn. That is what a sibling meets when the call ahead of it on a serial route was
        // refused for a module restart and gave its credit back to re-open.
        if !self.route_is_current(&route_key, route_handle, &sem) {
            // The calls re-opening on this route now, which this call must not send ahead of.
            let earlier = self.resend_turns_in_progress(&route_key);
            drop(first_permit);
            if scope.is_some() {
                // A scoped call keeps the scope check of any re-open: a fresh route must carry
                // the epoch and agent the call was composed under.
                return self
                    .resend_or_drain(
                        ctx,
                        call,
                        &module_id,
                        route_handle,
                        composed_under,
                        TOOL_DISPATCH_PATIENCE,
                        guard,
                        fence_key,
                        key,
                        &earlier,
                    )
                    .await;
            }
            return self
                .send_on_replaced_route(ctx, call, &module_id, fence_key, key, guard, &earlier)
                .await;
        }
        if scope.is_some() {
            let current = self.cached_stamp(&ctx.bind, scope, &module_id, route_handle);
            let compatible = composed_under
                .as_ref()
                .zip(current.as_ref())
                .is_some_and(|(original, actual)| may_resend_under(original, actual));
            if !compatible {
                self.forget_route(ctx, &module_id, route_handle);
                return DispatchResult::ScopeDrained {
                    reason: if composed_under.is_some() && current.is_some() {
                        SCOPE_AGENT_CHANGED.into()
                    } else {
                        SCOPE_UNVERIFIABLE.into()
                    },
                    sent: false,
                };
            }
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
        //    sent to the tool, so re-dispatching cannot execute it twice. Without this
        //    retry, an ordinary tool-module restart returns a `tool/route rejected` error to
        //    the model. Re-opening waits out the drain with the tool-dispatch patience; a
        //    second `module_reloading` on the one re-send is reported as the module being
        //    unavailable, still never sent (see `finish_dispatch`);
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
        let first = match self
            .send_call(
                ctx,
                &module_id,
                &route_handle,
                call,
                reply_deadline,
                key,
                &signals,
            )
            .await
        {
            SendAttempt::NotSent { reason } => return DispatchResult::NotSent { reason },
            SendAttempt::Reply(reply) => reply,
        };
        let route_is_known_stale = matches!(&first, Err(ConnError::StaleRouteHandle(_)))
            || matches!(&first, Err(ConnError::Rejected(msg)) if crate::connection::classify_tool_route_refusal(msg) == crate::connection::ToolRouteRefusal::Reopen);
        let endpoint_is_reloading = first.as_ref().err().and_then(ConnError::code)
            == Some(subc_protocol::error_codes::MODULE_RELOADING);
        if !(route_is_known_stale || endpoint_is_reloading) {
            return self
                .finish_dispatch(
                    ctx,
                    call,
                    &module_id,
                    fence_key,
                    route_handle,
                    composed_under,
                    first,
                    key,
                    first_permit,
                )
                .await;
        }
        // The first request was refused before it reached the tool, so it holds no window
        // credit any more. Give the permit back before re-opening: the re-open may wait out a
        // module restart, and on a serial route a sibling must not wait behind that. The
        // sibling then re-opens alongside this call, but sends only after this call has
        // finished (the resend turn), so a serial route keeps batch order.
        let _resend_turn = self.begin_resend_turn(&route_key);
        drop(first_permit);
        // A scoped call caught before send is never re-sent blindly. The scope's current state
        // decides, and so does whether a fresh route would carry the same epoch and agent as
        // the route the call was composed under (see `resend_or_drain`).
        if scope.is_some() {
            return self
                .resend_or_drain(
                    ctx,
                    call,
                    &module_id,
                    route_handle,
                    composed_under,
                    TOOL_DISPATCH_PATIENCE,
                    guard,
                    fence_key,
                    key,
                    &[],
                )
                .await;
        }
        self.remove_batch_call_route(ctx, &call.tool_call_id);
        // FORGOTTEN, NOT CLOSED, in both cases. For module_reloading the daemon still holds
        // the route, and closing it looks tidier -- but it is not safe. A sibling call in
        // the same batch may still be in flight on it (the drain is waiting for exactly
        // that call), and a GOODBYE would cut its reply off, turning a restart into an
        // ambiguous outcome that stops the run. Evict only the refused handle: a sibling may
        // already have cached its replacement. The drain owns the old binding's teardown.
        let _ = self.invalidate_route_for_module(&ctx.bind, scope, &module_id, route_handle);
        let reopened = if endpoint_is_reloading {
            let lifecycle = self.capture_session_lifecycle_token(&ctx.bind);
            self.ensure_result_route_for_lifecycle_inner(
                &ctx.bind,
                scope,
                &module_id,
                &lifecycle,
                guard.deadline,
                guard,
                TOOL_DISPATCH_PATIENCE,
                self.frozen_contract(ctx, &module_id).route_shape(),
            )
            .await
            .map_err(|error| map_tool_open_refusal(error, &module_id))
        } else {
            self.ensure_batch_route(ctx, &module_id, guard).await
        };
        let (fresh_handle, fresh_sem) = match reopened {
            Ok(reopened) => reopened,
            // The run was cancelled or stopped during the re-open: never sent.
            Err(_) if guard.interrupted() => return self.stopped_before_send(ctx, call),
            // A module that stays down for the whole tool-dispatch patience ends the call
            // with the attributed unavailable result, proven never sent, not a reopen error.
            Err(ToolPlaneError::Permanent { code, detail }) => {
                return self.permanent_refusal_result(
                    ctx,
                    call,
                    &code,
                    detail.as_ref(),
                    &fence_key,
                );
            }
            Err(ToolPlaneError::ModuleUnavailable {
                module,
                code,
                detail,
            }) => {
                return self
                    .module_unavailable_result(ctx, call, &module, &code, &detail, &fence_key);
            }
            Err(error) => {
                self.latch_open_refusal(ctx, &error);
                return DispatchResult::NotSent {
                    reason: format!("route re-open after stale handle failed: {error}"),
                };
            }
        };
        if guard.interrupted() {
            return self.stopped_before_send(ctx, call);
        }
        let fresh_permit = match Arc::clone(&fresh_sem).acquire_owned().await {
            Ok(p) => p,
            Err(_) => {
                return DispatchResult::NotSent {
                    reason: "route semaphore closed".into(),
                };
            }
        };
        let outcome = match self
            .send_call(
                ctx,
                &module_id,
                &fresh_handle,
                call,
                reply_deadline,
                key,
                &signals,
            )
            .await
        {
            SendAttempt::NotSent { reason } => return DispatchResult::NotSent { reason },
            SendAttempt::Reply(reply) => reply,
        };
        // The re-sent request holds the fresh route's credit until its terminal.
        self.finish_dispatch(
            ctx,
            call,
            &module_id,
            fence_key,
            fresh_handle,
            composed_under,
            outcome,
            key,
            fresh_permit,
        )
        .await
    }

    /// An unscoped call that waited for credit on a route and found it replaced once it got
    /// the credit: nothing was sent, so it opens the route again once, through the same
    /// tool-dispatch open (the module's gate, `guard`, the outage patience), and sends there
    /// as its first send. It sends only after `earlier`, the calls re-opening on the same route
    /// ahead of it, have finished. A module still down after the patience ends it with the
    /// attributed outage result, never sent.
    #[allow(clippy::too_many_arguments)]
    async fn send_on_replaced_route(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        module_id: &str,
        fence_key: FenceKey,
        key: Option<broca_core::tool_plane::DispatchKey<'_>>,
        guard: &ToolOpenGuard,
        earlier: &[CancellationToken],
    ) -> DispatchResult {
        let route_key = Self::route_key(&ctx.bind, None, module_id);
        let (fresh_handle, fresh_sem) = match self.ensure_batch_route(ctx, module_id, guard).await {
            Ok(route) => route,
            Err(_) if guard.interrupted() => return self.stopped_before_send(ctx, call),
            Err(ToolPlaneError::Permanent { code, detail }) => {
                return self.permanent_refusal_result(
                    ctx,
                    call,
                    &code,
                    detail.as_ref(),
                    &fence_key,
                );
            }
            Err(ToolPlaneError::ModuleUnavailable {
                module,
                code,
                detail,
            }) => {
                return self
                    .module_unavailable_result(ctx, call, &module, &code, &detail, &fence_key);
            }
            Err(error) => {
                self.latch_open_refusal(ctx, &error);
                return DispatchResult::NotSent {
                    reason: format!("route re-open after the cached route changed failed: {error}"),
                };
            }
        };
        if !Self::wait_for_resend_turns(earlier, guard).await {
            return self.stopped_before_send(ctx, call);
        }
        let fresh_permit = match Arc::clone(&fresh_sem).acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                return DispatchResult::NotSent {
                    reason: "route semaphore closed".into(),
                }
            }
        };
        // One re-open only: a route replaced again while this call waited is not chased.
        if !self.route_is_current(&route_key, fresh_handle, &fresh_sem) {
            return DispatchResult::NotSent {
                reason: "cached route changed before send".into(),
            };
        }
        let reply_deadline = self.reply_deadline(ctx, module_id, call);
        let outcome = match self
            .send_call(
                ctx,
                module_id,
                &fresh_handle,
                call,
                reply_deadline,
                key,
                &guard.signals(),
            )
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
            fresh_handle,
            None,
            outcome,
            key,
            fresh_permit,
        )
        .await
    }

    async fn dispatch_approved_inner(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        args_jcs: &str,
        key: broca_core::tool_plane::DispatchKey<'_>,
        signals: &broca_core::tool_plane::DispatchSignals,
    ) -> DispatchResult {
        // Preserve a completed result even if the route or approved input has
        // since become unavailable. Only a new call can be classified NotSent.
        let fence_key = (Self::session_key(&ctx.bind), call.tool_call_id.clone());
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
            .or_else(|| self.default_module_id.clone().map(ToolRoute::Module));
        if !matches!(route, Some(ToolRoute::Module(_))) {
            return DispatchResult::NotSent {
                reason: "route_unresolved".into(),
            };
        }
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
                    key: key.call_key.into(),
                    signals: signals.clone(),
                },
            );
        self.dispatch_inner(ctx, call, Some(key), &ToolOpenGuard::from(signals))
            .await
    }

    async fn recheck_approval_scope_inner(
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
        let selector =
            frozen_scope_selector(scope).map_err(|detail| ToolPlaneError::ScopeRefused {
                code: "scope_unverifiable".into(),
                detail,
            })?;
        let module = module
            .map(str::to_owned)
            .or_else(|| self.dispatch_target(ctx, &call.tool_call_id))
            .ok_or_else(|| ToolPlaneError::Unavailable("approval_target_unavailable".into()))?;
        let lifecycle = self.capture_session_lifecycle_token(&ctx.bind);
        let route = retry_route_open_with_scope_hold(
            RouteOpenRetry::within(
                DEFAULT_ROUTE_OPEN_PATIENCE.deadline,
                DEFAULT_ROUTE_OPEN_PATIENCE.max_interval,
            ),
            Some(ScopeHold {
                deadline: Instant::now() + self.scope_sync_hold,
                max_interval: SCOPE_SYNC_HOLD_MAX_INTERVAL,
            }),
            Some(&lifecycle),
            || self.conn.module_gate(&module),
            || {
                self.conn.route_open_dispatch(
                    &module,
                    &ctx.bind,
                    Some(&selector),
                    self.frozen_contract(ctx, &module).route_shape(),
                    self.held_reply_declared(),
                    self.aft_harness(),
                )
            },
        )
        .await
        .map_err(|error| {
            let code = match &error {
                RouteOpenError::Conn(ConnError::Rejected(message)) => message.code().to_owned(),
                _ => SCOPE_UNVERIFIABLE.into(),
            };
            ToolPlaneError::ScopeRefused {
                code,
                detail: format!("approval scope recheck failed: {error:?}"),
            }
        })?;
        let _ = self.closer.close(route).await;
        Ok(())
    }

    async fn batch_finished_inner(&self, ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        // The run invokes this after syncing terminals and the batch commit. Inline answers
        // need the same registration retirement as held answers; the provider may publish both.
        let registered: Vec<_> = self
            .registered_keys
            .lock()
            .unwrap()
            .get(&Self::batch_route_key(ctx))
            .map(|calls| calls.values().cloned().collect())
            .unwrap_or_default();
        let (inline, held): (Vec<_>, Vec<_>) = {
            let mut keys = self.inline_keys.lock().unwrap();
            registered
                .into_iter()
                .partition(|(_, key)| keys.remove(key))
        };
        // A handed-back call's terminal is still owed by its provider: keep its registration.
        let held: Vec<_> = {
            let mut yielded = self.yielded_keys.lock().unwrap();
            held.into_iter()
                .filter(|(_, key)| !yielded.remove(key))
                .collect()
        };
        let prune = if let Some(support) = &self.provider_held {
            match support.intake.prune_inline(&inline).await {
                Ok(()) => support.intake.prune(&held).await,
                Err(error) => Err(error),
            }
            .map_err(ToolPlaneError::Unavailable)
        } else {
            Ok(())
        };
        // Evict this batch's fence entries — the at-most-once guarantee only needs to hold
        // within a batch's life (the WAL is the cross-batch durability), so without eviction
        // the fence would grow unbounded over thousands of batches.
        self.fence.lock().unwrap().evict_batch(&ctx.batch_id);
        self.batch_manifests
            .lock()
            .unwrap()
            .remove(&Self::batch_route_key(ctx));
        self.registered_keys
            .lock()
            .unwrap()
            .remove(&Self::batch_route_key(ctx));
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
        prune
    }
}

#[async_trait]
impl ToolPlane for SubcToolPlane {
    async fn run_transport(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        manifest: &broca_core::tool_plane::PlanManifest,
    ) -> BTreeMap<String, broca_core::tool_plane::FrozenProviderDeclarations> {
        self.refresh_run_transport(bind, scope, manifest).await
    }

    async fn batch_started(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        self.batch_started_inner(ctx, calls, manifest, &ToolOpenGuard::default())
            .await
    }
    async fn batch_started_guarded(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
        guard: &ToolOpenGuard,
    ) -> Result<(), ToolPlaneError> {
        self.batch_started_inner(ctx, calls, manifest, guard).await
    }
    fn dispatch_target(&self, ctx: &BatchCtx, id: &ToolCallId) -> Option<String> {
        self.dispatch_target_inner(ctx, id)
    }
    fn dispatch_schema_pin(
        &self,
        ctx: &BatchCtx,
        id: &ToolCallId,
        tool: &str,
    ) -> Option<broca_core::tool_plane::DispatchSchemaPin> {
        let module = self.dispatch_target_inner(ctx, id)?;
        // Use the admitted plan or standalone discovery snapshot, never a catalog fetched
        // during dispatch: a later schema must not change the identity of a composed call.
        self.batch_manifests
            .lock()
            .unwrap()
            .get(&Self::batch_route_key(ctx))
            .and_then(|manifest| manifest.provider_declarations(&module))
            .and_then(|declarations| declarations.schema_pins.as_ref())
            .and_then(|pins| pins.get(tool))
            .cloned()
            .or_else(|| {
                self.direct_tool_item
                    .as_ref()
                    .filter(|item| item.provider == module)?;
                self.direct_schema_pins.lock().unwrap().get(tool).cloned()
            })
    }
    async fn dispatch(&self, ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        self.dispatch_inner(ctx, call, None, &ToolOpenGuard::default())
            .await
    }
    async fn dispatch_keyed(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        key: broca_core::tool_plane::DispatchKey<'_>,
    ) -> DispatchResult {
        self.dispatch_inner(ctx, call, Some(key), &ToolOpenGuard::default())
            .await
    }
    async fn dispatch_guarded(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        key: Option<broca_core::tool_plane::DispatchKey<'_>>,
        guard: &ToolOpenGuard,
    ) -> DispatchResult {
        self.dispatch_inner(ctx, call, key, guard).await
    }
    async fn dispatch_approved(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        args: &str,
        key: &str,
        signals: &broca_core::tool_plane::DispatchSignals,
    ) -> DispatchResult {
        self.dispatch_approved_inner(
            ctx,
            call,
            args,
            broca_core::tool_plane::DispatchKey {
                call_key: key,
                held_recorder: None,
            },
            signals,
        )
        .await
    }
    async fn dispatch_approved_keyed(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        args: &str,
        key: broca_core::tool_plane::DispatchKey<'_>,
        signals: &broca_core::tool_plane::DispatchSignals,
    ) -> DispatchResult {
        self.dispatch_approved_inner(ctx, call, args, key, signals)
            .await
    }
    async fn recheck_approval_scope(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        module: Option<&str>,
    ) -> Result<(), ToolPlaneError> {
        self.recheck_approval_scope_inner(ctx, call, module).await
    }
    async fn batch_finished(&self, ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        self.batch_finished_inner(ctx).await
    }
    async fn register_dispatch_group(
        &self,
        ctx: &BatchCtx,
        calls: &[broca_core::tool_plane::DispatchRegistration],
    ) -> Result<(), ToolPlaneError> {
        let Some(support) = self.provider_held.as_ref() else {
            return Ok(());
        };
        let mut registrations = Vec::new();
        let mut keys = Vec::new();
        for call in calls {
            let Some(module) = self.dispatch_target(ctx, &call.tool_call_id) else {
                continue;
            };
            let late_results = self.frozen_contract(ctx, &module).late_results;
            if !late_results || (!call.approved && self.conn.consumer_module_id().is_none()) {
                continue;
            }
            let Some((lineage, _)) = call.call_key.rsplit_once(':') else {
                return Err(ToolPlaneError::Unavailable(
                    "dispatch call key has no lineage".into(),
                ));
            };
            registrations.push(broca_core::tool_plane::LateResultRegistration {
                lineage: lineage.into(),
                call_key: call.call_key.clone(),
                module: module.clone(),
                bind: ctx.bind.clone(),
                scope: ctx.scope.clone(),
            });
            keys.push((call.tool_call_id.clone(), (module, call.call_key.clone())));
        }
        if !registrations.is_empty() {
            support
                .intake
                .register_group(&registrations)
                .await
                .map_err(ToolPlaneError::Unavailable)?;
            self.registered_keys
                .lock()
                .unwrap()
                .entry(Self::batch_route_key(ctx))
                .or_default()
                .extend(keys);
        }
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
    match routes.entry(key.with_handle(handle)) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(RouteState {
                handle,
                sem: Arc::clone(&sem),
                stamp,
                last_used: Instant::now(),
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
    let key = key.clone().with_handle(handle);
    routes
        .get(&key)
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
        // A frozen tool names the id its module had when the run froze it; a renamed module is
        // reached under the id the catalog lists now.
        return ToolRoute::Module(crate::provider_catalog::live_module_id(catalog, module_id));
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
        output: ToolOutput::error(message),
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

/// Always select the frozen epoch: reusing a scope ref must not carry a call into a new session.
pub(crate) fn frozen_scope_selector(scope: &SessionScope) -> Result<ScopeSelector, String> {
    Ok(ScopeSelector::new(
        owner_principal(scope)?,
        scope.scope_ref.clone(),
        Some(scope.scope_epoch),
    ))
}

/// Classify a scoped `route.open` refusal that the retry loop returned.
///
/// The two retryable scope refusals only come back here once the hold has run out, so they
/// become broca's `scope_unverifiable`. Every other `scope_*` code is terminal and is passed on
/// by its own name; it was never retried or held. Anything else is an ordinary refusal.
fn scoped_open_refusal(
    message: crate::connection::RejectedMessage,
    hold: Duration,
) -> RouteEnsureError {
    let code = message.code();
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
            detail: message.to_string(),
        });
    }
    RouteEnsureError::Connection(ConnError::Rejected(message))
}

/// A subc-LEVEL failure (typed Error frame / transport) is a PLANE failure → the
/// loop turns it into a graceful is_error result. A tool EXECUTION error rides
/// inside a successful CallToolResult (`isError: true`) instead.
fn conn_to_plane(e: ConnError) -> ToolPlaneError {
    match e {
        ConnError::Rejected(refusal)
            if refusal.code() == cortexkit_role_tool_provider::errors::TOOL_ROLE_NOT_DECLARED =>
        {
            ToolPlaneError::Permanent {
                code: refusal.code().to_owned(),
                detail: refusal.provider_detail().cloned(),
            }
        }
        e => ToolPlaneError::Unavailable(e.to_string()),
    }
}

/// Preserve every tool-dispatch route-open refusal, including terminal codes the protocol
/// does not know. Refusal attribution is independent of retryability: no tool request was
/// sent, and the model needs the module, code and bounded detail to understand why.
/// Scoped refusals have already taken their separate scope-specific path.
fn map_tool_open_refusal(error: RouteEnsureError, module_id: &str) -> ToolPlaneError {
    match error {
        RouteEnsureError::Connection(ConnError::Rejected(message))
            if message.code() == cortexkit_role_tool_provider::errors::TOOL_ROLE_NOT_DECLARED =>
        {
            conn_to_plane(ConnError::Rejected(message))
        }
        RouteEnsureError::Connection(ConnError::Rejected(message)) => {
            ToolPlaneError::ModuleUnavailable {
                module: module_id.to_owned(),
                code: message.code().to_owned(),
                detail: broca_protocol::bound_module_unavailable_detail(&message.detail()),
            }
        }
        error => error.into_plane(),
    }
}

/// One call's turn in [`SubcToolPlane::begin_resend_turn`]: while it lives, a call that
/// finds the same route replaced waits for it before sending. Dropping it ends the turn and
/// forgets it.
struct ResendTurn<'a> {
    turns: &'a Mutex<HashMap<RouteKey, Vec<CancellationToken>>>,
    key: RouteKey,
    done: CancellationToken,
}

impl Drop for ResendTurn<'_> {
    fn drop(&mut self) {
        self.done.cancel();
        let mut turns = self.turns.lock().unwrap();
        if let Some(entry) = turns.get_mut(&self.key) {
            entry.retain(|turn| !turn.is_cancelled());
            if entry.is_empty() {
                turns.remove(&self.key);
            }
        }
    }
}

/// Links the session lifecycle to the run's early-exit guard for one route-open wait: the
/// returned token fires when the session closes OR the run is cancelled/stopped. The
/// watcher task is aborted when the link drops (when the open finishes), so no task
/// outlives the wait it paces.
struct WaitCancel {
    token: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl WaitCancel {
    fn link(lifecycle: &CancellationToken, guard: &ToolOpenGuard) -> Self {
        let token = CancellationToken::new();
        let linked = token.clone();
        let lifecycle = lifecycle.clone();
        let cancel = guard.cancel_token();
        let stop = guard.stop_token();
        let task = tokio::spawn(async move {
            async fn wait(token: Option<CancellationToken>) {
                match token {
                    Some(token) => token.cancelled().await,
                    None => std::future::pending().await,
                }
            }
            tokio::select! {
                _ = lifecycle.cancelled() => {}
                _ = wait(cancel) => {}
                _ = wait(stop) => {}
            }
            linked.cancel();
        });
        Self { token, task }
    }

    fn token(&self) -> &CancellationToken {
        &self.token
    }
}

impl Drop for WaitCancel {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The warm-bind retry delay for `attempt` (1-based): 250ms doubling to a 5s cap.
/// Sequence: 250ms, 500ms, 1s, 2s, 4s, 5s, 5s, … — ~8 attempts in a 30s window.
fn warm_bind_backoff(attempt: u32) -> Duration {
    const BASE_MS: u64 = 250;
    const CAP_MS: u64 = 5_000;
    let exp = attempt.saturating_sub(1).min(10);
    Duration::from_millis((BASE_MS << exp).min(CAP_MS))
}

mod tool_capture;

/// Classify without storage I/O; dispatch captures the returned blobs before fencing.
fn map_call_tool_result(
    id: &ToolCallId,
    raw: &serde_json::Value,
) -> (ToolResult, Vec<tool_capture::PendingCapture>) {
    tool_capture::classify(id, raw)
}

// Keep non-media replies unchanged, including structuredContent and malformed replies.
pub(crate) fn map_legacy_call_tool_result(id: &ToolCallId, raw: &serde_json::Value) -> ToolResult {
    map_text_call_tool_result(id, raw, legacy_non_text_tool_content_placeholder)
}

fn map_text_call_tool_result(
    id: &ToolCallId,
    raw: &serde_json::Value,
    placeholder: fn(&serde_json::Value) -> String,
) -> ToolResult {
    let is_error = raw
        .get("isError")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let content = raw.get("content").and_then(serde_json::Value::as_array);
    let text = content
        .map(|blocks| {
            if blocks.iter().all(|block| {
                tool_capture::blob_mime(block).is_none()
                    && block
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
            let mut notices = Vec::new();
            let mut text = blocks
                .iter()
                .filter_map(|block| {
                    if let Some(mime) = tool_capture::blob_mime(block) {
                        notices.push(crate::content_v1::dropped_media_notice(mime));
                        return None;
                    }
                    Some(
                        block
                            .get("text")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned)
                            .unwrap_or_else(|| placeholder(block)),
                    )
                })
                .collect::<Vec<_>>()
                .join("");
            for notice in notices {
                if !text.is_empty() && !text.ends_with('\n') {
                    text.push('\n');
                }
                text.push_str(&notice);
            }
            text
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
            output: ToolOutput::error(malformed_tool_reply_message(raw)),
            is_error: true,
        };
    }

    let output = if is_error {
        ToolOutput::error(text)
    } else if content.is_none_or(Vec::is_empty) {
        match raw.get("structuredContent") {
            Some(value) => ToolOutput::Json {
                value: value.clone().into(),
            },
            None => ToolOutput::Text { text: text.into() },
        }
    } else {
        ToolOutput::Text { text: text.into() }
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
    if let Some(media) = tool_capture::supplied_reference(block) {
        return broca_protocol::content::media_not_shown_marker(
            &media,
            broca_protocol::MediaMarkerReason::Provenance,
        );
    }
    legacy_non_text_tool_content_placeholder(block)
}

fn legacy_non_text_tool_content_placeholder(block: &serde_json::Value) -> String {
    broca_protocol::content::non_text_tool_content_placeholder(
        block
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown"),
        block.get("mimeType").and_then(serde_json::Value::as_str),
        block
            .get("data")
            .and_then(serde_json::Value::as_str)
            .map(str::len),
    )
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod held_dispatch_tests;

#[cfg(test)]
mod approval_digest_contract_tests;

#[cfg(test)]
mod approved_queue_contract_tests;

/// The tool plane's route opens against a module that is down: the opens of every session to
/// that module are paced by the module's availability gate.
#[cfg(test)]
mod module_gate_tests;
