//! An authenticated, multiplexed subc CONSUMER connection (the runner's tool
//! plane leg). Mirrors `subc-probe` (authenticate_client, NO HELLO — that is
//! provider-only), but DEMULTIPLEXES by `(channel, corr)` so the loop can
//! dispatch a batch CONCURRENTLY on one route channel (the probe is sequential
//! with a fixed corr; the loop is not).
//!
//! A reader task fans each inbound frame to the waiter registered for its
//! `(channel, corr)`; a writer task serializes egress. Each in-flight call holds
//! a unique corr, so parallel tool calls on the same route channel never collide.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Duration, Instant},
};

use broca_protocol::BindIdentity;
use subc_control::{CatalogEntry, ClientControlRequest, ClientControlResponse};
use subc_protocol::{
    tool_call::ToolCallRequest, BindIdentity as SubcBindIdentity, Flags, Frame, FrameType,
    Priority, RouteTarget,
};
use subc_transport::{authenticate_client, connection_file, read_frame, write_frame};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::{mpsc, oneshot},
    time,
};
use tokio_util::sync::CancellationToken;

/// The error code broca answers a reverse request with: a `Request` a module sends toward the
/// consumer on a route channel. Copied from the subc Rust consumer SDK
/// (`subc-client-rs/src/consumer.rs`, `REVERSE_REQUEST_UNHANDLED`), which keeps it private
/// rather than exporting it from `subc_protocol::error_codes`, so a module sees the same code
/// from broca as from any SDK consumer that declared no handler. Broca declares none because it
/// fronts no person who could answer (for example an MCP elicitation).
pub const REVERSE_REQUEST_UNHANDLED: &str = "reverse_request_unhandled";

const AUTH_DEADLINE: Duration = Duration::from_secs(2);
const LIVENESS_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const PROBE_EGRESS_TIMEOUT: Duration = Duration::from_secs(2);
/// A route bind may keep the daemon's connection loop busy for roughly twelve seconds from
/// registration. A retained route.open tombstone keeps that original bind window visible to probe
/// scheduling, then stops blocking without giving cleanup state an unbounded lifetime as traffic.
///
/// THIS VALUE IS A MIRROR OF THE DAEMON'S BIND-RELAY TIMEOUT, not a tunable of ours. It must stay
/// at or above it. Our own wait for a route.open reply, [`ROUTE_OPEN_ATTEMPT_TIMEOUT`], is derived
/// from it and outlasts it, so a caller that waits the attempt out never leaves a bind still
/// relaying. The tombstone matters for a caller that gives up EARLIER: a dropped future, an outer
/// timeout, a cancelled task. The daemon may still be relaying that open, and probing inside the
/// window queues a Ping behind live work, no pong arrives, and a HEALTHY socket is convicted --
/// tearing down the credential and tool planes together, which is strictly worse than
/// the dead-socket case this recovery exists to fix.
///
/// Pinned by `bind_relay_grace_covers_the_daemon_relay_window` because the behavioural tests
/// INJECT their own grace to compress timeouts, so they prove the mechanism and cannot see this
/// number drift.
const ROUTE_OPEN_BIND_RELAY_GRACE: Duration = Duration::from_secs(12);
/// How long past the daemon's bind-relay bound a route.open reply may still take to reach us: the
/// daemon's own bookkeeping after the module answers `route.bind`, the reply's trip through its
/// connection-scoped FIFO, and scheduler delay on a loaded host. Three seconds is a quarter of the
/// relay bound, generous for a local socket, and short enough that a daemon which never answers
/// is still reported within a quarter-minute.
const ROUTE_OPEN_REPLY_MARGIN: Duration = Duration::from_secs(3);
/// The deadline for ONE `route.open` attempt, from queueing the request to its reply: the
/// daemon's bind-relay bound
/// ([`ROUTE_OPEN_BIND_RELAY_GRACE`], 12s) plus [`ROUTE_OPEN_REPLY_MARGIN`] (3s), so 15s.
///
/// A route.open is not a fast control RPC. The daemon relays it to the target module as
/// `route.bind` and may legitimately wait up to its relay bound for the answer, so a reply at 11s
/// is a SUCCESSFUL open. Under the 10s [`RESPONSE_TIMEOUT`] it was reported to our caller as a
/// failure while the daemon went on to commit the route, which the late-open tombstone then had to
/// close again.
///
/// Every route open in this crate goes through one attempt function (`route_open_target`), so
/// this one value governs all of them: the ordinary retry loop ([`retry_route_open`]), the tool
/// plane's opens and its reload reopens, the catalog poller and the vault.
///
/// This bounds an attempt already in flight. It is independent of a caller's retry deadline
/// ([`RouteOpenRetry::budget`], 3s by default), which only decides whether ANOTHER attempt may
/// start; see [`retry_route_open`].
pub const ROUTE_OPEN_ATTEMPT_TIMEOUT: Duration =
    ROUTE_OPEN_BIND_RELAY_GRACE.saturating_add(ROUTE_OPEN_REPLY_MARGIN);
const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(5);
/// Defensive per-connection bound for outstanding RPC and subscribe waiters.
const MAX_PENDING_REQUESTS: usize = 4096;
/// A normal subscriber drains continuously, so 4096 frames absorbs large scheduler and renderer
/// stalls while still bounding memory when a consumer stops polling altogether.
const SUBSCRIBE_STREAM_BUFFER: usize = 4096;
/// The standard per-frame response deadline for request/response calls. Public so callers
/// of the deadline-parameterized [`SubcConnection::route_unary`] can pass the default.
///
/// This is a liveness budget for fast local control RPCs, not a provider work budget.
/// Credential refresh can perform an upstream exchange and uses an explicit longer wait.
/// A handler that does real work needs its own deadline instead — see
/// [`TOOL_CALL_TIMEOUT`] for dispatch, and [`ROUTE_OPEN_ATTEMPT_TIMEOUT`] for route opens (the
/// daemon's bind relay can outlast this value).
///
/// The hazard is a NEW caller rather than the current ones. `route_request` takes no deadline,
/// so a slow route RPC added later inherits ten seconds silently and fails as a transport error
/// that looks like an unreachable module. If you are adding one, reach for `route_unary` with an
/// explicit deadline: a wait that names itself at the call site cannot be inherited by accident.
pub const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

/// The DEFAULT per-dispatch deadline for TOOL CALLS, deliberately execution-class rather than
/// the control-RPC fast-fail above. It is not the only one: a call that asks its tool to hold
/// the reply until a long command ends (AFT `bash` with `wait`) gets a deadline sized to that
/// command, computed per call by [`crate::tool_deadline::tool_reply_deadline`] -- the one place
/// that decides, and the one to read for the full rule. Tool work is sized to the work, not to a liveness probe:
/// AFT search/read legitimately runs tens of seconds under load, and the cost asymmetry is
/// stark — a spurious timeout on a mutating call mints an ambiguous outcome that interrupts
/// the whole run under the indeterminate policy, while waiting for a true answer costs
/// nothing (the daemon data plane is timeout-free; a late terminal frame releases the route
/// credit regardless, so a generous deadline leaks nothing). Sized for WORK time: the
/// caller acquires the per-route flow-control permit before this deadline starts counting.
pub const TOOL_CALL_TIMEOUT: Duration = Duration::from_secs(120);

const ROUTE_GONE_MESSAGE: &str = "route gone (module endpoint dropped)";

pub type ConnResult<T> = Result<T, ConnError>;

/// One immediate module health result returned by the subc supervisor.
#[derive(Debug, Clone, PartialEq)]
pub struct ModuleHealthSnapshot {
    pub module_id: String,
    pub status: subc_protocol::session::HealthStatus,
    pub detail: Option<String>,
    pub metrics: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedMessage {
    code: Option<String>,
    message: String,
    text: String,
}

impl RejectedMessage {
    fn structured(code: String, message: String) -> Self {
        let text = format!("{code} — {message}");
        Self {
            code: Some(code),
            message,
            text,
        }
    }

    pub(crate) fn code(&self) -> &str {
        self.code.as_deref().unwrap_or("")
    }
}

impl From<String> for RejectedMessage {
    fn from(text: String) -> Self {
        Self {
            code: None,
            message: text.clone(),
            text,
        }
    }
}

impl From<&str> for RejectedMessage {
    fn from(text: &str) -> Self {
        Self::from(text.to_owned())
    }
}

impl std::ops::Deref for RejectedMessage {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.text
    }
}

impl AsRef<str> for RejectedMessage {
    fn as_ref(&self) -> &str {
        &self.text
    }
}

impl std::fmt::Display for RejectedMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

#[derive(Debug)]
pub enum ConnError {
    /// subc returned a typed Error frame (route reject, tool/route failure).
    Rejected(RejectedMessage),
    /// Transport / protocol failure on the consumer connection.
    Transport(String),
    /// A control/route reply didn't match the expected shape.
    Protocol(String),
    /// A subscribe consumer stopped draining long enough to exhaust its bounded frame queue.
    StreamLagged { buffered: usize },
    /// The route belongs to another connection or has already been replaced.
    StaleRouteHandle(RouteHandle),
}

impl std::fmt::Display for ConnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnError::Rejected(m) => write!(f, "subc rejected: {m}"),
            ConnError::Transport(m) => write!(f, "subc transport: {m}"),
            ConnError::Protocol(m) => write!(f, "subc protocol: {m}"),
            ConnError::StreamLagged { buffered } => write!(
                f,
                "subscribe stream lagged: {buffered} buffered frame limit exceeded; resubscribe from your last durable cursor"
            ),
            ConnError::StaleRouteHandle(handle) => write!(
                f,
                "stale subc route handle (channel {}, epoch {})",
                handle.channel, handle.epoch
            ),
        }
    }
}

impl ConnError {
    /// The daemon's structured refusal code, never inferred from its diagnostic message.
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Rejected(message) => message.code.as_deref(),
            _ => None,
        }
    }
}

impl std::error::Error for ConnError {}

static NEXT_CONNECTION_TOKEN: AtomicU64 = AtomicU64::new(1);

/// Opaque identity of one live socket. It is never serialized on the wire.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ConnToken(u64);

#[derive(Debug)]
pub(crate) enum ApprovedCallError {
    BeforePush(ApprovedSetupError),
    AfterPush(ConnError),
}

#[derive(Debug)]
pub(crate) enum ApprovedSetupError {
    Interrupted,
    Connection(ConnError),
}
impl From<ConnError> for ApprovedSetupError {
    fn from(error: ConnError) -> Self {
        Self::Connection(error)
    }
}
impl std::fmt::Display for ApprovedSetupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Interrupted => f.write_str("approved_dispatch_stopped_before_enqueue"),
            Self::Connection(error) => write!(f, "{error}"),
        }
    }
}

/// A route identity that cannot be reused on another connection or route generation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RouteHandle {
    pub channel: u16,
    pub epoch: u32,
    conn: ConnToken,
    pub(crate) reads_held_reply: bool,
}

impl RouteHandle {
    fn new(channel: u16, epoch: u32, conn: ConnToken) -> Self {
        Self {
            channel,
            epoch,
            conn,
            reads_held_reply: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn synthetic(channel: u16) -> Self {
        Self::synthetic_epoch(channel, 1)
    }

    #[cfg(test)]
    pub(crate) fn synthetic_epoch(channel: u16, epoch: u32) -> Self {
        Self::new(channel, epoch, ConnToken(0))
    }
}

type PendingKey = (u16, u32, u64);

#[derive(Clone)]
enum PendingSender {
    Unary(mpsc::UnboundedSender<Frame>),
    Stream {
        sender: mpsc::Sender<Frame>,
        lagged: Arc<AtomicBool>,
    },
}

#[derive(Debug)]
enum PendingSendError {
    Closed,
    StreamFull,
}

impl PendingSender {
    fn send(&self, frame: Frame) -> Result<(), PendingSendError> {
        match self {
            Self::Unary(sender) => sender.send(frame).map_err(|_| PendingSendError::Closed),
            Self::Stream { sender, lagged } => match sender.try_send(frame) {
                Ok(()) => Ok(()),
                Err(mpsc::error::TrySendError::Full(_)) => {
                    lagged.store(true, Ordering::Release);
                    Err(PendingSendError::StreamFull)
                }
                Err(mpsc::error::TrySendError::Closed(_)) => Err(PendingSendError::Closed),
            },
        }
    }
}

enum PendingEntry {
    Live {
        sender: PendingSender,
        route_open: bool,
    },
    LateRouteOpen {
        blocks_probe_until: Instant,
    },
}

type PendingMap = Arc<Mutex<HashMap<PendingKey, PendingEntry>>>;
type RouteEpochs = Arc<Mutex<HashMap<u16, u32>>>;

/// Counts of frames every reader in this process dropped, for a health probe that would
/// otherwise report a silent loss as a quiet connection.
///
/// PROCESS-GLOBAL rather than per-connection, because that is the scope the reading is used
/// at: an operator asks whether this module is losing frames, not which of its sockets lost
/// them. It also keeps the gauge off the connection's constructor, which a dozen call sites
/// and a trait object would otherwise have to thread it through for a number none of them
/// read.
///
/// The drops themselves are correct: a frame for a route generation this client no longer
/// holds must not be delivered, and a terminal whose waiter has gone has nowhere to go. What
/// is wrong is their invisibility -- "delivered zero frames" and "the peer sent nothing" are
/// distinct states with one output, and an investigation that cannot tell them apart starts at
/// the wrong component.
///
/// Counts are emitted even at zero. A counter that appears only once it is non-zero cannot
/// distinguish a quiet connection from a gauge that was never wired, so the reading that
/// matters most -- nothing is being lost -- would be the one indistinguishable from a missing
/// instrument.
#[derive(Debug, Default)]
pub struct FrameDropGauge {
    stale_epoch: std::sync::atomic::AtomicU64,
    unmatched_terminal: std::sync::atomic::AtomicU64,
    reverse_requests_refused: std::sync::atomic::AtomicU64,
}

/// The process-wide frame-drop counts read by `health.check`.
pub static FRAME_DROPS: FrameDropGauge = FrameDropGauge::new();

impl FrameDropGauge {
    /// A zeroed gauge, usable in a static.
    pub const fn new() -> Self {
        Self {
            stale_epoch: std::sync::atomic::AtomicU64::new(0),
            unmatched_terminal: std::sync::atomic::AtomicU64::new(0),
            reverse_requests_refused: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// A frame arrived for a (channel, epoch) this client no longer holds.
    pub fn stamp_stale_epoch(&self) {
        self.stale_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// A terminal frame arrived whose (channel, epoch, corr) matches no pending waiter.
    pub fn stamp_unmatched_terminal(&self) {
        self.unmatched_terminal
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// A module sent a request toward broca on a route channel and broca answered it with
    /// [`REVERSE_REQUEST_UNHANDLED`]. Not a lost frame (the module gets its answer), but kept
    /// beside the drops because both are inbound traffic broca did not act on.
    pub fn stamp_reverse_request_refused(&self) {
        self.reverse_requests_refused
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Reverse requests refused since process start.
    pub fn reverse_requests_refused(&self) -> u64 {
        self.reverse_requests_refused
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// `(stale_epoch, unmatched_terminal)` since process start.
    pub fn counts(&self) -> (u64, u64) {
        (
            self.stale_epoch.load(std::sync::atomic::Ordering::Relaxed),
            self.unmatched_terminal
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }
}

/// RAII registration for one waiter in the pending map. Ordinary request exits remove the
/// waiter. A route.open that was sent retains a cleanup tombstone if its caller is cancelled,
/// because the daemon can still commit that route and return its handle later.
struct PendingRegistration {
    pending: PendingMap,
    key: PendingKey,
    active: bool,
    retain_on_drop: bool,
    probe_observer: Option<PendingProbeObserver>,
}

struct PendingProbeObserver {
    shared: Weak<ConnectionShared>,
    conn: ConnToken,
    route_open_registered_at: Instant,
    bind_relay_grace: Duration,
}

impl PendingProbeObserver {
    fn tombstone_deadline(&self) -> Instant {
        self.route_open_registered_at + self.bind_relay_grace
    }

    fn pending_changed(&self) {
        pending_registration_changed(&self.shared, self.conn);
    }
}

impl PendingRegistration {
    fn new(
        pending: PendingMap,
        key: PendingKey,
        route_open: bool,
        probe_observer: Option<PendingProbeObserver>,
    ) -> ConnResult<(Self, mpsc::UnboundedReceiver<Frame>)> {
        let (sender, rx) = mpsc::unbounded_channel();
        insert_pending(&pending, key, PendingSender::Unary(sender), route_open)?;
        Ok((
            Self {
                pending,
                key,
                active: true,
                retain_on_drop: false,
                probe_observer,
            },
            rx,
        ))
    }

    fn new_stream(
        pending: PendingMap,
        key: PendingKey,
    ) -> ConnResult<(Self, mpsc::Receiver<Frame>, Arc<AtomicBool>)> {
        let (sender, rx) = mpsc::channel(SUBSCRIBE_STREAM_BUFFER);
        let lagged = Arc::new(AtomicBool::new(false));
        insert_pending(
            &pending,
            key,
            PendingSender::Stream {
                sender,
                lagged: Arc::clone(&lagged),
            },
            false,
        )?;
        Ok((
            Self {
                pending,
                key,
                active: true,
                retain_on_drop: false,
                probe_observer: None,
            },
            rx,
            lagged,
        ))
    }

    fn arm_late_route_open_cleanup(&mut self) {
        self.retain_on_drop = true;
    }

    fn retain_late_route_open(&mut self) -> bool {
        let blocks_probe_until = self
            .probe_observer
            .as_ref()
            .map(PendingProbeObserver::tombstone_deadline)
            .unwrap_or_else(|| Instant::now() + ROUTE_OPEN_BIND_RELAY_GRACE);
        let retained = {
            let mut waiters = self.pending.lock().unwrap();
            if matches!(
                waiters.get(&self.key),
                Some(PendingEntry::Live {
                    route_open: true,
                    ..
                })
            ) {
                waiters.insert(self.key, PendingEntry::LateRouteOpen { blocks_probe_until });
                self.active = false;
                true
            } else {
                false
            }
        };
        if retained {
            if let Some(observer) = &self.probe_observer {
                observer.pending_changed();
            }
        }
        retained
    }
}

fn insert_pending(
    pending: &PendingMap,
    key: PendingKey,
    sender: PendingSender,
    route_open: bool,
) -> ConnResult<()> {
    let mut waiters = pending.lock().unwrap();
    if waiters.len() >= MAX_PENDING_REQUESTS {
        return Err(ConnError::Transport(format!(
            "connection has reached the {MAX_PENDING_REQUESTS} in-flight request limit"
        )));
    }
    waiters.insert(key, PendingEntry::Live { sender, route_open });
    Ok(())
}

impl Drop for PendingRegistration {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let blocks_probe_until = self
            .probe_observer
            .as_ref()
            .map(PendingProbeObserver::tombstone_deadline)
            .unwrap_or_else(|| Instant::now() + ROUTE_OPEN_BIND_RELAY_GRACE);
        {
            let mut waiters = self.pending.lock().unwrap();
            if self.retain_on_drop
                && matches!(
                    waiters.get(&self.key),
                    Some(PendingEntry::Live {
                        route_open: true,
                        ..
                    })
                )
            {
                waiters.insert(self.key, PendingEntry::LateRouteOpen { blocks_probe_until });
            } else {
                waiters.remove(&self.key);
            }
        }
        if let Some(observer) = &self.probe_observer {
            observer.pending_changed();
        }
    }
}

type WriteConfirmations = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Instant, String>>>>>;

struct ConnectionRuntime {
    capabilities: tokio::sync::OnceCell<Vec<String>>,
    write_tx: mpsc::Sender<Frame>,
    pending: PendingMap,
    route_epochs: RouteEpochs,
    write_confirmations: WriteConfirmations,
    conn: ConnToken,
    shutdown: CancellationToken,
}

struct GenerationState {
    runtime: Arc<ConnectionRuntime>,
    last_inbound: Instant,
    control_watermark: u64,
    probe_in_flight: bool,
    probe_deferred: bool,
    deferred_probe_wake: Option<Instant>,
    active: bool,
}

impl GenerationState {
    fn new(runtime: Arc<ConnectionRuntime>) -> Self {
        Self {
            runtime,
            last_inbound: Instant::now(),
            control_watermark: 0,
            probe_in_flight: false,
            probe_deferred: false,
            deferred_probe_wake: None,
            active: true,
        }
    }
}

struct ConnectionShared {
    connection_file_path: PathBuf,
    current: Mutex<GenerationState>,
    corr: AtomicU64,
    reconnect_tx: mpsc::UnboundedSender<ConnToken>,
    probe_timeout: Duration,
    route_open_bind_relay_grace: Duration,
    /// The consumer identity every route open on this connection presents. Fixed when the
    /// connection is made so a caller can ask, before sending anything, whether its routes
    /// will be attested; see [`SubcConnection::consumer_module_id`].
    consumer_identity: Option<subc_control::ConsumerIdentity>,
    #[cfg(test)]
    reconnect_attempts: AtomicU64,
    #[cfg(test)]
    probe_windows_started: AtomicU64,
}

impl Drop for ConnectionShared {
    fn drop(&mut self) {
        if let Ok(state) = self.current.get_mut() {
            let _ = deactivate_locked(state);
        }
    }
}

/// A live consumer connection to a subc daemon. Every request snapshots one immutable socket
/// generation; recovery swaps the shared generation pointer without moving an in-flight request.
pub struct SubcConnection {
    shared: Arc<ConnectionShared>,
    /// Each target module's availability gate; every route open on this connection passes
    /// its module's gate (see [`ModuleGate`]).
    module_gates: ModuleGates,
}

impl SubcConnection {
    /// Read the connection file, connect, and authenticate as a CLIENT (no HELLO).
    ///
    /// Route opens present the consumer identity the daemon gave this process when it spawned
    /// it (see [`supervised_consumer_identity`]), read once, here.
    pub async fn connect(connection_file_path: &Path) -> ConnResult<Self> {
        Self::connect_with_consumer_identity(connection_file_path, supervised_consumer_identity())
            .await
    }

    /// As [`Self::connect`], presenting `consumer_identity` on every route open instead of
    /// reading it from the environment. For callers that must not depend on the process
    /// environment, chiefly tests: the environment is process-global, so setting it for one
    /// test changes it for every other test in the same binary.
    pub async fn connect_with_consumer_identity(
        connection_file_path: &Path,
        consumer_identity: Option<subc_control::ConsumerIdentity>,
    ) -> ConnResult<Self> {
        Self::connect_with_timeouts_inner(
            connection_file_path,
            LIVENESS_PROBE_TIMEOUT,
            ROUTE_OPEN_BIND_RELAY_GRACE,
            consumer_identity,
        )
        .await
    }

    /// The module id of the consumer identity this connection presents on route opens, or
    /// `None` when it presents none. With an identity, the daemon attests each route as the
    /// `Reserved` principal of that module; without one, every route arrives as `Direct`, and a
    /// target that authorizes by principal (the credential vault's grants) cannot authorize it.
    pub fn consumer_module_id(&self) -> Option<&str> {
        self.shared
            .consumer_identity
            .as_ref()
            .map(|identity| identity.module_id.as_str())
    }

    #[cfg(test)]
    pub(crate) async fn connect_with_probe_timeout(
        connection_file_path: &Path,
        probe_timeout: Duration,
    ) -> ConnResult<Self> {
        Self::connect_with_timeouts_inner(
            connection_file_path,
            probe_timeout,
            ROUTE_OPEN_BIND_RELAY_GRACE,
            supervised_consumer_identity(),
        )
        .await
    }

    #[cfg(test)]
    async fn connect_with_timeouts(
        connection_file_path: &Path,
        probe_timeout: Duration,
        route_open_bind_relay_grace: Duration,
    ) -> ConnResult<Self> {
        Self::connect_with_timeouts_inner(
            connection_file_path,
            probe_timeout,
            route_open_bind_relay_grace,
            supervised_consumer_identity(),
        )
        .await
    }

    async fn connect_with_timeouts_inner(
        connection_file_path: &Path,
        probe_timeout: Duration,
        route_open_bind_relay_grace: Duration,
        consumer_identity: Option<subc_control::ConsumerIdentity>,
    ) -> ConnResult<Self> {
        let (read_half, write_half) = open_authenticated_socket(connection_file_path).await?;
        let (runtime, write_rx) = new_runtime();
        let (reconnect_tx, reconnect_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(ConnectionShared {
            connection_file_path: connection_file_path.to_path_buf(),
            current: Mutex::new(GenerationState::new(Arc::clone(&runtime))),
            corr: AtomicU64::new(1),
            reconnect_tx,
            probe_timeout,
            route_open_bind_relay_grace,
            consumer_identity,
            #[cfg(test)]
            reconnect_attempts: AtomicU64::new(0),
            #[cfg(test)]
            probe_windows_started: AtomicU64::new(0),
        });
        spawn_generation_tasks(
            &shared,
            Arc::clone(&runtime),
            read_half,
            write_half,
            write_rx,
        );
        let supervisor = tokio::spawn(reconnect_supervisor(Arc::downgrade(&shared), reconnect_rx));
        tokio::spawn(async move {
            if let Err(error) = supervisor.await {
                tracing::error!(target: "transport", %error, "subc reconnect supervisor terminated");
            }
        });
        Ok(Self {
            shared,
            module_gates: ModuleGates::default(),
        })
    }

    fn next_corr(&self) -> u64 {
        self.shared.corr.fetch_add(1, Ordering::Relaxed)
    }

    /// Register channel-0 work and snapshot its socket generation in one critical section.
    fn register_control(
        &self,
        key: PendingKey,
        route_open: bool,
    ) -> ConnResult<(
        Arc<ConnectionRuntime>,
        PendingRegistration,
        mpsc::UnboundedReceiver<Frame>,
    )> {
        let mut state = self.shared.current.lock().unwrap();
        if !state.active {
            return Err(ConnError::Transport("connection is recovering".into()));
        }
        let runtime = Arc::clone(&state.runtime);
        let observer = PendingProbeObserver {
            shared: Arc::downgrade(&self.shared),
            conn: runtime.conn,
            route_open_registered_at: Instant::now(),
            bind_relay_grace: self.shared.route_open_bind_relay_grace,
        };
        let (registration, rx) = PendingRegistration::new(
            Arc::clone(&runtime.pending),
            key,
            route_open,
            Some(observer),
        )?;
        state.control_watermark = state.control_watermark.wrapping_add(1);
        Ok((runtime, registration, rx))
    }

    /// Validate a route and install its waiter under the generation lock. Once this returns,
    /// the request can only enter the captured runtime's writer queue, never a replacement's.
    fn register_route(
        &self,
        handle: &RouteHandle,
        key: PendingKey,
    ) -> ConnResult<(
        Arc<ConnectionRuntime>,
        PendingRegistration,
        mpsc::UnboundedReceiver<Frame>,
    )> {
        let state = self.shared.current.lock().unwrap();
        let runtime = Arc::clone(&state.runtime);
        let routes = runtime.route_epochs.lock().unwrap();
        if !state.active
            || handle.conn != runtime.conn
            || routes.get(&handle.channel) != Some(&handle.epoch)
        {
            return Err(ConnError::StaleRouteHandle(*handle));
        }
        let (registration, rx) =
            PendingRegistration::new(Arc::clone(&runtime.pending), key, false, None)?;
        drop(routes);
        Ok((runtime, registration, rx))
    }

    fn snapshot_route(&self, handle: &RouteHandle) -> ConnResult<Arc<ConnectionRuntime>> {
        let state = self.shared.current.lock().unwrap();
        let runtime = Arc::clone(&state.runtime);
        if !state.active
            || handle.conn != runtime.conn
            || runtime.route_epochs.lock().unwrap().get(&handle.channel) != Some(&handle.epoch)
        {
            Err(ConnError::StaleRouteHandle(*handle))
        } else {
            Ok(runtime)
        }
    }

    async fn send(&self, runtime: &ConnectionRuntime, frame: Frame) -> ConnResult<()> {
        runtime
            .write_tx
            .send(frame)
            .await
            .map_err(|_| ConnError::Transport("writer task closed".into()))
    }

    async fn enqueue_approved(
        &self,
        runtime: &ConnectionRuntime,
        frame: Frame,
        signals: &broca_core::tool_plane::DispatchSignals,
    ) -> Result<(), ApprovedSetupError> {
        let permit = tokio::select! {
            biased;
            _ = signals.cancelled() => return Err(ApprovedSetupError::Interrupted),
            permit = runtime.write_tx.reserve() => permit.map_err(|_| ConnError::Transport("writer task closed before enqueue".into()))?,
        };
        Self::push_approved(permit, frame, signals)
    }

    fn push_approved(
        permit: mpsc::Permit<'_, Frame>,
        frame: Frame,
        signals: &broca_core::tool_plane::DispatchSignals,
    ) -> Result<(), ApprovedSetupError> {
        // A clear final signal observation below is the linearization point: the
        // call is committed to send. Cancellation after that observation, even
        // from another thread before permit.send executes, is treated as after
        // the send. No await separates the observation from the synchronous push.
        if signals.interrupted() {
            return Err(ApprovedSetupError::Interrupted);
        }
        permit.send(frame);
        Ok(())
    }

    fn spawn_liveness_probe(&self, runtime: &Arc<ConnectionRuntime>) {
        request_liveness_probe(&self.shared, runtime);
    }

    /// Channel-0 control RPC: send Request, await the terminal Response/Error.
    async fn control_rpc(&self, body: Vec<u8>) -> ConnResult<Frame> {
        self.control_rpc_deadline(body, RESPONSE_TIMEOUT).await
    }

    async fn control_rpc_deadline(&self, body: Vec<u8>, deadline: Duration) -> ConnResult<Frame> {
        let corr = self.next_corr();
        let (runtime, _registration, mut rx) = self.register_control((0, 0, corr), false)?;
        let frame = Frame::build(FrameType::Request, control_flags(), 0, 0, corr, body)
            .map_err(|e| ConnError::Transport(e.to_string()))?;
        self.send(&runtime, frame).await?;
        self.await_terminal_deadline(&runtime, &mut rx, deadline)
            .await
    }

    async fn route_open_rpc(&self, body: Vec<u8>) -> ConnResult<(Frame, ConnToken)> {
        self.route_open_rpc_deadline(body, ROUTE_OPEN_ATTEMPT_TIMEOUT)
            .await
    }

    async fn route_open_rpc_deadline(
        &self,
        body: Vec<u8>,
        deadline: Duration,
    ) -> ConnResult<(Frame, ConnToken)> {
        // One deadline covers the whole attempt, queueing the request for the writer as well as
        // waiting for the reply. The writer queue is bounded, and a socket that stops draining
        // would otherwise hold the attempt in `send` with no clock running at all. That attempt
        // may be the one probe a module's gate lets through (see `ModuleGate`), and every other
        // open to that module waits on it.
        let attempt_deadline = time::Instant::now() + deadline;
        let corr = self.next_corr();
        let (runtime, mut registration, mut rx) = self.register_control((0, 0, corr), true)?;
        let frame = Frame::build(FrameType::Request, control_flags(), 0, 0, corr, body)
            .map_err(|e| ConnError::Transport(e.to_string()))?;
        match time::timeout_at(attempt_deadline, self.send(&runtime, frame)).await {
            Ok(sent) => sent?,
            Err(_) => {
                // The frame never entered the queue, so nothing reached the daemon and there
                // is no route to clean up later: drop the waiter (not armed for late cleanup,
                // so this removes it), then let the liveness probe judge the stalled socket.
                drop(registration);
                self.spawn_liveness_probe(&runtime);
                return Err(ConnError::Transport(format!(
                    "route.open could not be queued for sending within {deadline:?} \
                     (ROUTE_OPEN_ATTEMPT_TIMEOUT); nothing was sent"
                )));
            }
        }
        registration.arm_late_route_open_cleanup();
        let remaining = attempt_deadline.saturating_duration_since(time::Instant::now());
        let result = self
            .await_terminal_deadline(&runtime, &mut rx, remaining)
            .await;
        if matches!(&result, Err(ConnError::Transport(message)) if message.starts_with("timed out after"))
        {
            // A successful late route.open committed a daemon route. Keep its identity so the
            // reader can close that route rather than orphaning it after the caller timed out.
            if !registration.retain_late_route_open() {
                // The reader won the boundary race and removed the registration before the
                // timeout path could tombstone it. Its frame is already queued locally, so
                // consume that terminal instead of reporting a timeout and losing the route.
                if let Ok(frame) = rx.try_recv() {
                    return Ok((frame, runtime.conn));
                }
            }
            // Name the deadline that expired: a route.open gives up on a different clock from
            // an ordinary control RPC, and "timed out" alone reads as the 10s one.
            return Err(ConnError::Transport(format!(
                "route.open got no reply within {deadline:?} (ROUTE_OPEN_ATTEMPT_TIMEOUT)"
            )));
        }
        result.map(|frame| (frame, runtime.conn))
    }

    /// The daemon's fixed capability list, read once per authenticated socket generation.
    /// Direct clients do not receive HELLO_ACK, so they learn the same list from
    /// `server.describe`. Concurrent readers share the request; failures are not cached.
    pub async fn server_capabilities(&self) -> ConnResult<Vec<String>> {
        let runtime = {
            let state = self.shared.current.lock().unwrap();
            if !state.active {
                return Err(ConnError::Transport("connection is recovering".into()));
            }
            Arc::clone(&state.runtime)
        };
        runtime
            .capabilities
            .get_or_try_init(|| async {
                let corr = self.next_corr();
                let (registered, _registration, mut rx) =
                    self.register_control((0, 0, corr), false)?;
                if registered.conn != runtime.conn {
                    return Err(ConnError::Transport(
                        "connection changed during capability discovery".into(),
                    ));
                }
                let body = serde_json::to_vec(&ClientControlRequest::ServerDescribe {})
                    .map_err(|e| ConnError::Transport(e.to_string()))?;
                let frame = Frame::build(FrameType::Request, control_flags(), 0, 0, corr, body)
                    .map_err(|e| ConnError::Transport(e.to_string()))?;
                self.send(&runtime, frame).await?;
                let frame = self
                    .await_terminal_deadline(&runtime, &mut rx, RESPONSE_TIMEOUT)
                    .await?;
                match serde_json::from_slice::<ClientControlResponse>(&frame.body) {
                    Ok(ClientControlResponse::ServerDescribe { capabilities, .. }) => {
                        Ok(capabilities)
                    }
                    Ok(other) => Err(ConnError::Protocol(format!(
                        "unexpected server.describe reply: {other:?}"
                    ))),
                    Err(e) => Err(ConnError::Protocol(format!("decode server.describe: {e}"))),
                }
            })
            .await
            .cloned()
    }

    /// `catalog.list` — registered providers + their tools.
    pub async fn catalog_list(&self) -> ConnResult<Vec<CatalogEntry>> {
        let body = serde_json::to_vec(&ClientControlRequest::CatalogList { module_id: None })
            .map_err(|e| ConnError::Transport(e.to_string()))?;
        let frame = self.control_rpc(body).await?;
        match serde_json::from_slice::<ClientControlResponse>(&frame.body) {
            Ok(ClientControlResponse::CatalogList { modules, .. }) => Ok(modules),
            Ok(other) => Err(ConnError::Protocol(format!(
                "unexpected catalog.list reply: {other:?}"
            ))),
            Err(e) => Err(ConnError::Protocol(format!("decode catalog.list: {e}"))),
        }
    }

    /// Ask the supervisor to run one module `health.check` probe and return its in-memory
    /// dependency report.
    pub async fn supervisor_health_probe(
        &self,
        module_id: &str,
    ) -> ConnResult<ModuleHealthSnapshot> {
        let body = serde_json::to_vec(&ClientControlRequest::SupervisorHealthProbe {
            module_id: module_id.to_string(),
        })
        .map_err(|error| ConnError::Transport(error.to_string()))?;
        let frame = self.control_rpc(body).await?;
        match serde_json::from_slice::<ClientControlResponse>(&frame.body) {
            Ok(ClientControlResponse::SupervisorHealthProbe {
                module_id,
                status,
                detail,
                metrics,
            }) => Ok(ModuleHealthSnapshot {
                module_id,
                status,
                detail,
                metrics,
            }),
            Ok(other) => Err(ConnError::Protocol(format!(
                "unexpected supervisor.health_probe reply: {other:?}"
            ))),
            Err(error) => Err(ConnError::Protocol(format!(
                "decode supervisor.health_probe: {error}"
            ))),
        }
    }

    /// The supervisor's own record of one module, for failure diagnostics: every module's
    /// `supervisor.list` entry (state, spawn generation, last exit, restart budget), the
    /// module's retained stderr (`supervisor.stderr_tail`) and its terminal exit history
    /// (`supervisor.terminals`). This is the daemon's account of why a module reads `failed`,
    /// and it is there even when the module wrote no log of its own; a `spawn_generation` of 0
    /// says no process of it ever started. Each part holds the raw reply or the error text, so a
    /// daemon that lacks one op still answers the others.
    pub async fn supervisor_module_report(&self, module_id: &str) -> serde_json::Value {
        let requests = [
            ("supervisor.list", ClientControlRequest::SupervisorList {}),
            (
                "supervisor.stderr_tail",
                ClientControlRequest::SupervisorStderrTail {
                    module_id: module_id.to_string(),
                    max_lines: Some(200),
                    max_bytes: None,
                },
            ),
            (
                "supervisor.terminals",
                ClientControlRequest::SupervisorTerminals {
                    module_id: module_id.to_string(),
                },
            ),
        ];
        let mut report = serde_json::Map::new();
        for (op, request) in requests {
            let answer = match serde_json::to_vec(&request) {
                Ok(body) => match self.control_rpc(body).await {
                    Ok(frame) => serde_json::from_slice(&frame.body).unwrap_or_else(|error| {
                        serde_json::Value::String(format!("undecodable reply: {error}"))
                    }),
                    Err(error) => serde_json::Value::String(error.to_string()),
                },
                Err(error) => serde_json::Value::String(format!("encode {op}: {error}")),
            };
            report.insert(op.to_string(), answer);
        }
        serde_json::Value::Object(report)
    }

    /// `route.open` to a tool provider. Subsequent tool calls use the returned handle.
    ///
    /// Retries a temporary refusal under [`RouteOpenRetry::default_patience`]; use
    /// [`route_open_within`](Self::route_open_within) to choose the budget.
    pub async fn route_open(
        &self,
        module_id: &str,
        identity: &BindIdentity,
    ) -> ConnResult<RouteHandle> {
        self.route_open_within(module_id, identity, RouteOpenRetry::default_patience())
            .await
    }

    /// `route.open` to a tool provider, retrying a temporary refusal until `retry` runs out.
    pub async fn route_open_within(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        retry: RouteOpenRetry,
    ) -> ConnResult<RouteHandle> {
        self.retry_open_through_gate(GateTarget::tool_provider(module_id), retry, || {
            self.route_open_single_attempt(module_id, identity, None)
        })
        .await
    }

    /// ONE `route.open` attempt to a tool provider, with no retry of a temporary refusal.
    ///
    /// Crate-visible for exactly one reason: the tool plane's connection trait is the seam its
    /// unit tests fake, and they script individual attempts beneath the shared retry loop
    /// ([`retry_route_open`]), so that seam has to carry one attempt. Every other consumer
    /// must use a retrying form; a refusal during a module restart is otherwise a hard error.
    pub(crate) async fn route_open_single_attempt(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        scope: Option<&subc_protocol::scope::ScopeSelector>,
    ) -> ConnResult<RouteHandle> {
        self.route_open_target(
            RouteTarget::ToolProvider {
                module_id: module_id.to_string(),
            },
            identity,
            scope.cloned(),
            None,
        )
        .await
    }

    pub(crate) async fn route_open_dispatch_single_attempt(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        scope: Option<&subc_protocol::scope::ScopeSelector>,
        provider_held_installed: bool,
    ) -> ConnResult<RouteHandle> {
        let capabilities = provider_held_installed.then(|| {
            vec![
                cortexkit_role_tool_provider::CONTENT_V1_CAPABILITY.into(),
                cortexkit_role_tool_provider::HELD_REPLY_V1.into(),
            ]
        });
        self.route_open_target_with_capabilities(
            RouteTarget::ToolProvider {
                module_id: module_id.into(),
            },
            identity,
            scope.cloned(),
            None,
            capabilities,
        )
        .await
    }

    /// ONE `route.open` attempt to a tool provider that declares `tool-provider: v1`, asking
    /// the module to serve its v1 role surface on the route: `tool.catalog` with its system
    /// text, and its declarations. Only fetch-plan admission opens such routes, and its own
    /// deadline bounds the retries, so this makes one attempt.
    pub async fn route_open_tool_provider_v1(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        scope: Option<subc_protocol::scope::ScopeSelector>,
    ) -> ConnResult<RouteHandle> {
        let mut role_versions = std::collections::BTreeMap::new();
        role_versions.insert("tool-provider".to_owned(), "v1".to_owned());
        // One attempt, so it does not wait at the module's gate: while the module is known to
        // be unavailable this answers with the module's latest refusal at once, unless it is
        // this open's turn to probe. Fetch-plan admission retries a refusal with its own
        // backoff, and those retries are then paced by the gate instead of each reaching the
        // daemon.
        let gate = self.module_gate(&GateTarget::tool_provider(module_id));
        let ticket = match gate.admit(None, None).await {
            GateAdmission::Admitted(ticket) => Some(ticket),
            GateAdmission::Refused(refusal) => return Err(ConnError::Rejected(refusal)),
            GateAdmission::Cancelled => {
                return Err(ConnError::Transport("route open cancelled".into()))
            }
            // The connection is between generations; the open fails at once like any other
            // open made during a reconnect.
            GateAdmission::Retired => None,
        };
        let outcome = self
            .route_open_target(
                RouteTarget::ToolProvider {
                    module_id: module_id.to_string(),
                },
                identity,
                scope,
                Some(role_versions),
            )
            .await;
        if let Some(ticket) = ticket {
            ticket.report(&outcome);
        }
        outcome
    }

    /// Native integration probes need to declare the same paired result formats as a held
    /// tool-plane caller. This opener is absent from release builds and grants no identity.
    #[cfg(debug_assertions)]
    pub async fn route_open_tool_provider_held_v1_for_tests(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        scope: Option<subc_protocol::scope::ScopeSelector>,
    ) -> ConnResult<RouteHandle> {
        self.route_open_target_with_capabilities(
            RouteTarget::ToolProvider {
                module_id: module_id.into(),
            },
            identity,
            scope,
            Some(std::collections::BTreeMap::from([(
                "tool-provider".into(),
                "v1".into(),
            )])),
            Some(vec![
                cortexkit_role_tool_provider::CONTENT_V1_CAPABILITY.into(),
                cortexkit_role_tool_provider::HELD_REPLY_V1.into(),
            ]),
        )
        .await
    }

    /// Open an unscoped module-level role using the same bind translation and retry machinery
    /// as step-transform hooks. The caller bounds the entire route-open future.
    pub(crate) async fn route_open_role_within(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        role: &str,
        version: &str,
        retry: RouteOpenRetry,
    ) -> ConnResult<RouteHandle> {
        self.retry_open_through_gate(GateTarget::tool_provider(module_id), retry, || {
            self.route_open_target(
                RouteTarget::ToolProvider {
                    module_id: module_id.to_string(),
                },
                identity,
                None,
                Some(std::collections::BTreeMap::from([(
                    role.to_owned(),
                    version.to_owned(),
                )])),
            )
        })
        .await
    }

    /// The availability gate of one route target of a module, on the current socket generation.
    pub(crate) fn module_gate(&self, target: &GateTarget) -> Arc<ModuleGate> {
        self.module_gates.get(&self.shared, target)
    }

    /// The retry loop of every retrying open form on this type: [`retry_route_open`], with each
    /// attempt passing the gate of `target`. Each caller owns the route it gets; only the gate
    /// is shared.
    async fn retry_open_through_gate<F, Fut>(
        &self,
        target: GateTarget,
        retry: RouteOpenRetry,
        attempt: F,
    ) -> ConnResult<RouteHandle>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = ConnResult<RouteHandle>>,
    {
        retry_route_open(retry, None, || Some(self.module_gate(&target)), attempt)
            .await
            .map_err(RouteOpenError::into_conn)
    }

    /// Whether `route` is still open: the connection that opened it is the live one and the
    /// daemon has not closed the route (no GOODBYE for it has arrived). A caller that cached the
    /// handle asks this to learn, without sending anything, that the route has gone.
    pub fn route_is_open(&self, route: &RouteHandle) -> bool {
        let runtime = {
            let state = self.shared.current.lock().unwrap();
            Arc::clone(&state.runtime)
        };
        runtime.conn == route.conn
            && runtime.route_epochs.lock().unwrap().get(&route.channel) == Some(&route.epoch)
    }

    /// `route.open` to a MANAGEMENT SURFACE → the route channel for this (module, session).
    /// Subsequent session.*/run.* requests (and a subscribe stream) ride this channel. This
    /// is the CLIENT leg of the management surface (the inverse of the module's serve loop):
    /// a consumer drives a module's session lifecycle over this route.
    ///
    /// Retries a temporary refusal under [`RouteOpenRetry::default_patience`]; use
    /// [`route_open_management_within`](Self::route_open_management_within) to choose the
    /// budget.
    pub async fn route_open_management(
        &self,
        module_id: &str,
        identity: &BindIdentity,
    ) -> ConnResult<RouteHandle> {
        self.route_open_management_within(module_id, identity, RouteOpenRetry::default_patience())
            .await
    }

    /// `route.open` to a management surface, retrying a temporary refusal until `retry` runs
    /// out.
    pub async fn route_open_management_within(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        retry: RouteOpenRetry,
    ) -> ConnResult<RouteHandle> {
        self.retry_open_through_gate(GateTarget::management_surface(module_id), retry, || {
            self.route_open_target(
                RouteTarget::ManagementSurface {
                    module_id: module_id.to_string(),
                },
                identity,
                None,
                None,
            )
        })
        .await
    }

    /// `route.open` to a module's management surface declaring the `classifier/v1` role
    /// ([`CLASSIFIER_ROLE_VERSIONS`]), so the module answers that route as a classifier:
    /// `role.describe` is the classifier's and `classify.run` is served, and nothing else. This
    /// is the one place the route shape of a `classifier/v1` caller is written down; a test
    /// rig opening a classifier route as an outside caller would goes through it too.
    pub async fn route_open_classifier_within(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        retry: RouteOpenRetry,
    ) -> ConnResult<RouteHandle> {
        self.route_open_management_roles_within(
            module_id,
            identity,
            classifier_role_versions(),
            retry,
        )
        .await
    }

    /// Open the classifier role under a daemon-held work scope. The selector is resolved by
    /// the daemon, not trusted as caller-supplied attribution by the classifier.
    pub async fn route_open_classifier_scoped_within(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        scope: subc_protocol::scope::ScopeSelector,
        retry: RouteOpenRetry,
    ) -> ConnResult<RouteHandle> {
        self.retry_open_through_gate(GateTarget::management_surface(module_id), retry, || {
            self.route_open_target(
                RouteTarget::ManagementSurface {
                    module_id: module_id.to_owned(),
                },
                identity,
                Some(scope.clone()),
                Some(classifier_role_versions()),
            )
        })
        .await
    }

    /// `route.open` to a management surface declaring `role_versions`, retrying a temporary
    /// refusal until `retry` runs out. A module that cannot serve the declared roles refuses
    /// the bind, and the open fails with its refusal.
    pub async fn route_open_management_roles_within(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        role_versions: std::collections::BTreeMap<String, String>,
        retry: RouteOpenRetry,
    ) -> ConnResult<RouteHandle> {
        self.retry_open_through_gate(GateTarget::management_surface(module_id), retry, || {
            self.route_open_target(
                RouteTarget::ManagementSurface {
                    module_id: module_id.to_string(),
                },
                identity,
                None,
                Some(role_versions.clone()),
            )
        })
        .await
    }

    /// `route.open` to a management surface under a scope the daemon holds, retrying a
    /// temporary refusal until `retry` runs out. The daemon admits the open only for the scope's
    /// owner or a listed carrier, and stamps the scope on the target's `route.bind`.
    ///
    /// Used by scoped management clients (including prefrontal-core elicitation) and by
    /// real-daemon tests driving broca's management surface. Production callers must first
    /// check `scopes/v1`, since an older daemon would silently ignore the scope selector.
    pub async fn route_open_management_scoped_within(
        &self,
        module_id: &str,
        identity: &BindIdentity,
        scope: subc_protocol::scope::ScopeSelector,
        retry: RouteOpenRetry,
    ) -> ConnResult<RouteHandle> {
        self.retry_open_through_gate(GateTarget::management_surface(module_id), retry, || {
            self.route_open_target(
                RouteTarget::ManagementSurface {
                    module_id: module_id.to_string(),
                },
                identity,
                Some(scope.clone()),
                None,
            )
        })
        .await
    }

    /// Open a named module's internal service, not its management or tool surface. The daemon
    /// validates this connection's consumer identity and stamps the caller principal on the bind.
    /// No scope selector is sent, so the target sees the calling module's own principal.
    pub async fn route_open_internal_service_within(
        &self,
        module_id: &str,
        service_id: &str,
        identity: &BindIdentity,
        retry: RouteOpenRetry,
    ) -> ConnResult<RouteHandle> {
        let gate_target = GateTarget::InternalService {
            module_id: module_id.to_owned(),
            service_id: service_id.to_owned(),
        };
        self.retry_open_through_gate(gate_target, retry, || {
            self.route_open_target(
                RouteTarget::InternalService {
                    module_id: module_id.to_string(),
                    service_id: service_id.to_string(),
                },
                identity,
                None,
                None,
            )
        })
        .await
    }

    /// One `route.open` RPC. Private so that no consumer can reach a single attempt by
    /// accident: every public form goes through [`retry_route_open`].
    async fn route_open_target(
        &self,
        target: RouteTarget,
        identity: &BindIdentity,
        scope: Option<subc_protocol::scope::ScopeSelector>,
        role_versions: Option<std::collections::BTreeMap<String, String>>,
    ) -> ConnResult<RouteHandle> {
        self.route_open_target_with_capabilities(target, identity, scope, role_versions, None)
            .await
    }

    async fn route_open_target_with_capabilities(
        &self,
        target: RouteTarget,
        identity: &BindIdentity,
        scope: Option<subc_protocol::scope::ScopeSelector>,
        role_versions: Option<std::collections::BTreeMap<String, String>>,
        consumer_capabilities: Option<Vec<String>>,
    ) -> ConnResult<RouteHandle> {
        let reads_held_reply = cortexkit_role_tool_provider::consumer_reads_held_reply_v1(
            consumer_capabilities.as_deref(),
        );
        let req = ClientControlRequest::RouteOpen {
            identity: to_subc_bind(identity, &target, role_versions.as_ref()),
            target,
            consumer_identity: self.shared.consumer_identity.clone(),
            // Result-format capabilities are installed as a pair by the tool plane. Role,
            // plan-fetch and management callers keep their previous undeclared format.
            consumer_capabilities,
            // Admission facts come from a configured carrier module; the runner's
            // consumer legs carry none.
            admission_facts: None,
            // Scoped tool and management clients first check the daemon advertises `scopes/v1`:
            // an older daemon drops the field and would open an unscoped route instead.
            scope,
            // Role routes declare their surface. A route that dispatches the model's tool
            // calls declares none, so what a provider serves on it is unchanged.
            role_versions,
        };
        let body = serde_json::to_vec(&req).map_err(|e| ConnError::Transport(e.to_string()))?;
        let (frame, conn) = self.route_open_rpc(body).await?;
        match frame.header.ty {
            FrameType::Response => {
                match serde_json::from_slice::<ClientControlResponse>(&frame.body) {
                    Ok(ClientControlResponse::RouteOpen {
                        route_channel,
                        route_epoch,
                    }) => {
                        let mut handle = RouteHandle::new(route_channel, route_epoch, conn);
                        handle.reads_held_reply = reads_held_reply;
                        Ok(handle)
                    }
                    Ok(other) => Err(ConnError::Protocol(format!(
                        "unexpected route.open reply: {other:?}"
                    ))),
                    Err(e) => Err(ConnError::Protocol(format!("decode route.open: {e}"))),
                }
            }
            FrameType::Error => Err(ConnError::Rejected(decode_error_body(&frame.body))),
            ty => Err(ConnError::Protocol(format!(
                "unexpected route.open frame {ty:?}"
            ))),
        }
    }

    /// Send a JSON request body on a bound route channel and return the terminal Response body
    /// without decoding it. Typed clients use this path to deserialize the response exactly once.
    pub async fn route_request_raw(
        &self,
        route: &RouteHandle,
        body: Vec<u8>,
    ) -> ConnResult<Vec<u8>> {
        self.route_request_terminal(route, body)
            .await?
            .map_err(|error| ConnError::Rejected(decode_error_body(&error)))
    }

    /// A unary request whose provider may perform bounded upstream work, such as token refresh.
    /// The explicit reply budget does not change pending-registration cleanup on cancellation.
    pub async fn route_request_raw_with_deadline(
        &self,
        route: &RouteHandle,
        body: Vec<u8>,
        deadline: Duration,
    ) -> ConnResult<Vec<u8>> {
        self.route_request_terminal_with_deadline(route, body, deadline)
            .await?
            .map_err(|error| ConnError::Rejected(decode_error_body(&error)))
    }

    /// [`Self::route_request_raw`], keeping a typed refusal's `ERROR` body whole: `Ok(Ok(body))`
    /// for a Response, `Ok(Err(body))` for an Error frame. A caller that branches on an error's
    /// `detail` reads it here; `Rejected` keeps only its code and message.
    pub async fn route_request_terminal(
        &self,
        route: &RouteHandle,
        body: Vec<u8>,
    ) -> ConnResult<Result<Vec<u8>, Vec<u8>>> {
        self.route_request_terminal_with_deadline(route, body, RESPONSE_TIMEOUT)
            .await
    }

    /// [`Self::route_request_terminal`] with the caller's own reply budget, for a request whose
    /// provider may legitimately take longer than the default (a batch of model calls).
    pub async fn route_request_terminal_with_deadline(
        &self,
        route: &RouteHandle,
        body: Vec<u8>,
        deadline: Duration,
    ) -> ConnResult<Result<Vec<u8>, Vec<u8>>> {
        let corr = self.next_corr();
        let (runtime, _registration, mut rx) =
            self.register_route(route, (route.channel, route.epoch, corr))?;
        tracing::debug!(
            target: "subc.wire",
            stage = "registered",
            conn = ?runtime.conn,
            channel = route.channel, epoch = route.epoch, corr,
            "route unary progress"
        );
        let frame = Frame::build(
            FrameType::Request,
            data_flags(),
            route.channel,
            route.epoch,
            corr,
            body,
        )
        .map_err(|e| ConnError::Transport(e.to_string()))?;
        async {
            self.send(&runtime, frame).await?;
            tracing::debug!(
                target: "subc.wire",
                stage = "queued",
                conn = ?runtime.conn,
                channel = route.channel, epoch = route.epoch, corr,
                "route unary progress"
            );
            let terminal = self
                .await_terminal_deadline(&runtime, &mut rx, deadline)
                .await?;
            match terminal.header.ty {
                FrameType::Response => Ok(Ok(terminal.body)),
                FrameType::Error => Ok(Err(terminal.body)),
                ty => Err(ConnError::Protocol(format!(
                    "unexpected route response frame {ty:?}"
                ))),
            }
        }
        .await
    }

    /// Send a JSON request body on a bound route channel and await its single terminal
    /// Response/Error. The management-surface analogue of `tool_call` (which is tool-shaped):
    /// this is the generic `{method, params}` request/response the session.* command ops
    /// (send/status/cancel/retract) ride. Returns the decoded Response JSON, or `Rejected`
    /// for a typed Error frame.
    pub async fn route_request(
        &self,
        route: &RouteHandle,
        body: Vec<u8>,
    ) -> ConnResult<serde_json::Value> {
        let body = self.route_request_raw(route, body).await?;
        serde_json::from_slice(&body)
            .map_err(|e| ConnError::Protocol(format!("decode route response: {e}")))
    }

    /// Open a SUBSCRIBE stream on a bound route channel: send the subscribe request body,
    /// then return a [`SubscribeStream`] that yields each `StreamData` frame until the
    /// `StreamEnd` terminates it. Unlike `route_request` (one terminal), a subscribe holds
    /// the channel and streams many frames on one corr (the serve side emits one StreamData
    /// per projected control unit). The caller drains the stream; dropping it deregisters.
    pub async fn subscribe(
        &self,
        route: &RouteHandle,
        body: Vec<u8>,
    ) -> ConnResult<SubscribeStream> {
        let corr = self.next_corr();
        let (runtime, registration, rx, lagged) = {
            let state = self.shared.current.lock().unwrap();
            let runtime = Arc::clone(&state.runtime);
            let routes = runtime.route_epochs.lock().unwrap();
            if !state.active
                || route.conn != runtime.conn
                || routes.get(&route.channel) != Some(&route.epoch)
            {
                return Err(ConnError::StaleRouteHandle(*route));
            }
            let (registration, rx, lagged) = PendingRegistration::new_stream(
                Arc::clone(&runtime.pending),
                (route.channel, route.epoch, corr),
            )?;
            drop(routes);
            (runtime, registration, rx, lagged)
        };
        let frame = Frame::build(
            FrameType::Request,
            data_flags(),
            route.channel,
            route.epoch,
            corr,
            body,
        )
        .map_err(|e| ConnError::Transport(e.to_string()))?;
        self.send(&runtime, frame).await?;
        Ok(SubscribeStream {
            rx: Some(rx),
            lagged,
            registration: Some(registration),
            done: false,
            write_tx: runtime.write_tx.clone(),
            route_epochs: Arc::clone(&runtime.route_epochs),
            route: *route,
            corr,
        })
    }

    /// Close a route channel: send a `Goodbye` frame on it so the daemon FREES the channel
    /// (subc's router releases the route on a client GOODBYE). Used when a session's routes
    /// are no longer needed (the session ended) so a long-lived SHARED consumer connection
    /// doesn't accumulate stale channels across sessions. Best-effort: a failure to send
    /// just means the channel lingers until the connection closes (not a correctness issue).
    pub async fn route_close(&self, route: &RouteHandle) -> ConnResult<()> {
        let runtime = self.snapshot_route(route)?;
        let corr = self.next_corr();
        let frame = Frame::build(
            FrameType::Goodbye,
            data_flags(),
            route.channel,
            route.epoch,
            corr,
            Vec::new(),
        )
        .map_err(|e| ConnError::Transport(e.to_string()))?;
        // Local invalidation must wake callers even when egress cannot accept the GOODBYE.
        finish_route(&runtime.pending, &runtime.route_epochs, &frame);
        self.send(&runtime, frame).await
    }

    /// Send ONE unary request on `route_channel` with caller-encoded JSON bytes and await the
    /// terminal response (interim Push frames drained). Unlike [`tool_call`], which wraps the
    /// args in the `{name, arguments}` tool envelope, this sends `body` AS-IS — for a route
    /// whose handler expects a different top-level shape. Returns the raw response body bytes,
    /// or a typed Error → `Rejected`.
    ///
    /// The `deadline` bounds silence per received frame (it resets on interim Push frames),
    /// not total wall time. It is caller-supplied so a route whose handler legitimately runs
    /// longer than the standard [`RESPONSE_TIMEOUT`] can say so. Ordinary callers pass
    /// `RESPONSE_TIMEOUT`.
    ///
    /// [`tool_call`]: SubcConnection::tool_call
    pub async fn route_unary_bytes(
        &self,
        route: &RouteHandle,
        body: Vec<u8>,
        deadline: Duration,
    ) -> ConnResult<Vec<u8>> {
        let corr = self.next_corr();
        let (runtime, _registration, mut rx) =
            self.register_route(route, (route.channel, route.epoch, corr))?;
        let frame = Frame::build(
            FrameType::Request,
            data_flags(),
            route.channel,
            route.epoch,
            corr,
            body,
        )
        .map_err(|e| ConnError::Transport(e.to_string()))?;
        async {
            self.send(&runtime, frame).await?;
            let terminal = self
                .await_terminal_deadline(&runtime, &mut rx, deadline)
                .await?;
            match terminal.header.ty {
                FrameType::Response => Ok(terminal.body),
                FrameType::Error => Err(ConnError::Rejected(decode_error_body(&terminal.body))),
                ty => Err(ConnError::Protocol(format!(
                    "unexpected unary response frame {ty:?}"
                ))),
            }
        }
        .await
    }

    /// Like [`Self::route_unary_bytes`], except that an Error frame ending the request comes back
    /// as its decoded `ErrorBody` (code, message and detail) instead of a `Rejected` string, for
    /// a caller that acts on a refusal's detail.
    pub async fn route_unary_reply(
        &self,
        route: &RouteHandle,
        body: Vec<u8>,
        deadline: Duration,
    ) -> ConnResult<Result<Vec<u8>, subc_protocol::ErrorBody>> {
        let corr = self.next_corr();
        let (runtime, _registration, mut rx) =
            self.register_route(route, (route.channel, route.epoch, corr))?;
        let frame = Frame::build(
            FrameType::Request,
            data_flags(),
            route.channel,
            route.epoch,
            corr,
            body,
        )
        .map_err(|e| ConnError::Transport(e.to_string()))?;
        self.send(&runtime, frame).await?;
        let terminal = self
            .await_terminal_deadline(&runtime, &mut rx, deadline)
            .await?;
        match terminal.header.ty {
            FrameType::Response => Ok(Ok(terminal.body)),
            FrameType::Error => serde_json::from_slice::<subc_protocol::ErrorBody>(&terminal.body)
                .map(Err)
                .map_err(|_| ConnError::Rejected(decode_error_body(&terminal.body))),
            ty => Err(ConnError::Protocol(format!(
                "unexpected unary response frame {ty:?}"
            ))),
        }
    }

    /// Dispatch ONE tool call on `route_channel` and await the terminal
    /// CallToolResult (interim Push progress frames are drained/ignored). Returns
    /// the raw `{content, isError}` JSON, or a typed Error → `Rejected`.
    ///
    /// `tool_call_id` carries the PROVIDER-MINTED id for this call, and it is an explicit
    /// parameter rather than an optional builder field so every call site has to state which of
    /// two very different things it is doing. Until 2026-09-03 there was no id on the wire at
    /// all: the envelope was `{name, arguments}`, so every module downstream either synthesized
    /// an identity or refused, and one had been silently synthesizing for weeks while looking
    /// like a working component.
    ///
    /// `None` says THE PRODUCER HAS NO PROVIDER ID FOR THIS CALL -- never that the call has no
    /// identity. The catalog poller is the honest `None`: it borrows this envelope shape to reach
    /// a module operation, and no model ever asked for it.
    pub async fn tool_call(
        &self,
        route: &RouteHandle,
        name: &str,
        arguments: &serde_json::Value,
        tool_call_id: Option<&str>,
    ) -> ConnResult<serde_json::Value> {
        self.tool_call_with_deadline(route, name, arguments, tool_call_id, TOOL_CALL_TIMEOUT)
            .await
    }

    /// [`Self::tool_call`] with the terminal-reply deadline chosen by the caller. The tool plane
    /// passes the per-call value from [`crate::tool_deadline::tool_reply_deadline`]; like every
    /// deadline here it bounds silence between frames, resetting on interim Push frames.
    pub async fn tool_call_with_deadline(
        &self,
        route: &RouteHandle,
        name: &str,
        arguments: &serde_json::Value,
        tool_call_id: Option<&str>,
        deadline: Duration,
    ) -> ConnResult<serde_json::Value> {
        self.tool_call_with_preset(route, name, arguments, tool_call_id, deadline, None)
            .await
    }

    /// Dispatch with a provider preset frozen at plan admission. Legacy callers pass none.
    #[allow(clippy::too_many_arguments)]
    pub async fn tool_call_with_preset(
        &self,
        route: &RouteHandle,
        name: &str,
        arguments: &serde_json::Value,
        tool_call_id: Option<&str>,
        deadline: Duration,
        preset: Option<&str>,
    ) -> ConnResult<serde_json::Value> {
        // The SHARED type, not a local struct. A per-consumer body is how the fleet ended up with
        // three spellings of this envelope and one consumer carrying a `progress_token` nobody
        // else knew existed; emitting the canonical type is what keeps the next field from
        // needing a fourth.
        let body = serde_json::to_vec(&ToolCallRequest {
            name: name.to_string(),
            arguments: arguments.clone(),
            tool_call_id: tool_call_id.map(str::to_string),
            progress_token: None,
            // This unkeyed API has no intake registration or provider-declaration context.
            // Eligible dispatch keys go through the tool plane's guarded staged path instead.
            call_key: None,
            // Not sent yet, for the same reason: the pin's content is defined by the
            // tool-provider role, and broca sends one only after it freezes each tool's pin at
            // admission and the receiving provider checks it.
            schema_pin: None,
            // broca dispatches its own model's calls; it relays nobody else's, so there is no
            // relayed origin to attribute.
            origin: None,
            preset: preset.map(str::to_owned),
            output_refs: None,
        })
        .map_err(|e| ConnError::Transport(e.to_string()))?;
        self.send_tool_call_body(route, deadline, name, body).await
    }

    /// Preserve the exact argument bytes rather than parsing and reserializing them.
    #[allow(clippy::too_many_arguments)]
    pub async fn tool_call_raw_with_preset(
        &self,
        route: &RouteHandle,
        name: &str,
        args_jcs: &str,
        tool_call_id: Option<&str>,
        deadline: Duration,
        preset: Option<&str>,
        call_key: Option<&str>,
    ) -> ConnResult<serde_json::Value> {
        let body = Self::raw_tool_call_body(name, args_jcs, tool_call_id, preset, call_key)?;
        self.send_tool_call_body(route, deadline, name, body).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn tool_call_raw_guarded(
        &self,
        route: &RouteHandle,
        name: &str,
        args_jcs: &str,
        tool_call_id: Option<&str>,
        deadline: Duration,
        preset: Option<&str>,
        call_key: Option<&str>,
        signals: &broca_core::tool_plane::DispatchSignals,
    ) -> Result<serde_json::Value, ApprovedCallError> {
        // All fallible setup belongs to one pre-push stage. There is no fallible
        // work after enqueue inside this block, so its errors can never mean sent.
        let before_push = async {
            let body = Self::raw_tool_call_body(name, args_jcs, tool_call_id, preset, call_key)?;
            let corr = self.next_corr();
            let (runtime, registration, rx) =
                self.register_route(route, (route.channel, route.epoch, corr))?;
            let frame = Frame::build(
                FrameType::Request,
                data_flags(),
                route.channel,
                route.epoch,
                corr,
                body,
            )
            .map_err(|error| ConnError::Transport(error.to_string()))?;
            self.enqueue_approved(&runtime, frame, signals).await?;
            log_tool_dispatch(route.channel, corr, name);
            Ok::<_, ApprovedSetupError>((runtime, registration, rx, corr))
        };
        let (runtime, _registration, mut rx, corr) =
            before_push.await.map_err(ApprovedCallError::BeforePush)?;
        self.receive_tool_call(runtime, route, corr, &mut rx, deadline)
            .await
            .map_err(ApprovedCallError::AfterPush)
    }

    fn raw_tool_call_body(
        name: &str,
        args_jcs: &str,
        tool_call_id: Option<&str>,
        preset: Option<&str>,
        call_key: Option<&str>,
    ) -> ConnResult<Vec<u8>> {
        serde_json::from_str::<&serde_json::value::RawValue>(args_jcs)
            .map_err(|error| ConnError::Protocol(format!("invalid approved arguments: {error}")))?;
        let envelope = ToolCallRequest {
            name: name.to_owned(),
            arguments: serde_json::Value::Null,
            tool_call_id: tool_call_id.map(str::to_owned),
            progress_token: None,
            call_key: call_key.map(str::to_owned),
            schema_pin: None,
            origin: None,
            preset: preset.map(str::to_owned),
            output_refs: None,
        };
        let body = serde_json::to_string(&envelope)
            .map_err(|error| ConnError::Protocol(error.to_string()))?;
        let body = body.replacen(
            "\"arguments\":null",
            &format!("\"arguments\":{args_jcs}"),
            1,
        );
        Ok(body.into_bytes())
    }

    async fn send_tool_call_body(
        &self,
        route: &RouteHandle,
        deadline: Duration,
        operation: &str,
        body: Vec<u8>,
    ) -> ConnResult<serde_json::Value> {
        let corr = self.next_corr();
        let (runtime, _registration, mut rx) =
            self.register_route(route, (route.channel, route.epoch, corr))?;
        let frame = Frame::build(
            FrameType::Request,
            data_flags(),
            route.channel,
            route.epoch,
            corr,
            body,
        )
        .map_err(|error| ConnError::Transport(error.to_string()))?;
        self.send(&runtime, frame).await?;
        log_tool_dispatch(route.channel, corr, operation);
        self.receive_tool_call(runtime, route, corr, &mut rx, deadline)
            .await
    }

    async fn receive_tool_call(
        &self,
        runtime: Arc<ConnectionRuntime>,
        route: &RouteHandle,
        corr: u64,
        rx: &mut mpsc::UnboundedReceiver<Frame>,
        deadline: Duration,
    ) -> ConnResult<serde_json::Value> {
        // Arm abandonment only after enqueue. A request canceled while reserving
        // writer capacity has no provider work to cancel.
        let mut abandon = CancelOnAbandon::arm(&runtime, route, corr);
        let terminal = self.await_terminal_deadline(&runtime, rx, deadline).await;
        if terminal.is_ok() {
            abandon.disarm();
        }
        let terminal = terminal?;
        match terminal.header.ty {
            FrameType::Response => serde_json::from_slice::<serde_json::Value>(&terminal.body)
                .map_err(|error| ConnError::Protocol(format!("decode tool result: {error}"))),
            FrameType::Error => Err(ConnError::Rejected(decode_error_body(&terminal.body))),
            ty => Err(ConnError::Protocol(format!(
                "unexpected tool-call frame {ty:?}"
            ))),
        }
    }

    async fn await_terminal_deadline(
        &self,
        runtime: &Arc<ConnectionRuntime>,
        rx: &mut mpsc::UnboundedReceiver<Frame>,
        deadline: Duration,
    ) -> ConnResult<Frame> {
        loop {
            match time::timeout(deadline, rx.recv()).await {
                Ok(Some(frame)) => match frame.header.ty {
                    FrameType::Push => continue,
                    FrameType::Goodbye => {
                        return Err(ConnError::Transport(ROUTE_GONE_MESSAGE.into()))
                    }
                    FrameType::Response | FrameType::Error => return Ok(frame),
                    // Only a Response or an Error ends one of our calls. Anything else carrying
                    // this corr (a Request from the module's side, a stray stream frame) is a
                    // protocol violation, and taking it as the answer would report the call as
                    // completed with the wrong frame. The reader already answers a module's
                    // Request without routing it here; this arm keeps that true by type rather
                    // than by the reader's cooperation.
                    ty => {
                        return Err(ConnError::Protocol(format!(
                            "frame {ty:?} arrived where a Response or Error terminal was expected"
                        )))
                    }
                },
                Ok(None) => return Err(ConnError::Transport("connection closed mid-call".into())),
                Err(_) => {
                    self.spawn_liveness_probe(runtime);
                    return Err(ConnError::Transport(format!(
                        "timed out after {deadline:?}"
                    )));
                }
            }
        }
    }
}

/// A live subscribe stream on a route handle: yields each `StreamData` frame's body until
/// the `StreamEnd` frame terminates it. The serve side emits one `StreamData` per projected
/// control unit (and a final `StreamEnd`); this is the client drain. Dropping the stream
/// deregisters the local waiter AND sends a best-effort route GOODBYE so the daemon frees
/// the route slot + the server tears down its parked forward task — otherwise a long-lived
/// client that subscribes/unsubscribes many times on one connection would accumulate stale
/// server-side routes until the whole connection drops.
pub struct SubscribeStream {
    rx: Option<mpsc::Receiver<Frame>>,
    lagged: Arc<AtomicBool>,
    registration: Option<PendingRegistration>,
    done: bool,
    write_tx: mpsc::Sender<Frame>,
    route_epochs: RouteEpochs,
    route: RouteHandle,
    corr: u64,
}

impl SubscribeStream {
    /// The next stream event: `Some(body)` for a `StreamData` frame's JSON body, `None` when
    /// the stream ends cleanly (a `StreamEnd` frame or the connection closes). A route-gone
    /// daemon `Goodbye` is an error. If the caller stops polling long enough to fill the
    /// bounded queue, this returns [`ConnError::StreamLagged`] and the caller must resubscribe
    /// from its last durable cursor. After `None`, the stream is finished.
    ///
    /// NO inter-event timeout — UNLIKE a request/response (`await_terminal`, which has a
    /// response SLA), a subscribe stream can idle ARBITRARILY long between events: a
    /// subscriber parked on a QUIET session (the long-lived observer this surface exists
    /// for) waits indefinitely between one turn's `RunFinished` and the next turn's
    /// `RunStarted`. Imposing a per-event timeout would conflate "the stream broke" with "the
    /// session is legitimately idle" and spuriously error a healthy parked subscriber.
    /// Liveness is the CONNECTION's job: if the connection closes, `recv()` yields `None`
    /// and the caller sees the stream end without inventing a timeout error.
    pub async fn next(&mut self) -> ConnResult<Option<Vec<u8>>> {
        if self.done {
            return Ok(None);
        }
        loop {
            if self.lagged.load(Ordering::Acquire) {
                return Err(self.finish_lagged());
            }
            let received = self
                .rx
                .as_mut()
                .expect("an unfinished subscribe stream retains its receiver")
                .recv()
                .await;
            if self.lagged.load(Ordering::Acquire) {
                return Err(self.finish_lagged());
            }
            match received {
                Some(frame) => match frame.header.ty {
                    FrameType::StreamData => return Ok(Some(frame.body)),
                    FrameType::StreamEnd => {
                        self.done = true;
                        let _ = self.registration.take();
                        return Ok(None);
                    }
                    FrameType::Push => continue, // interim progress — ignore
                    FrameType::Error => {
                        self.done = true;
                        let _ = self.registration.take();
                        return Err(ConnError::Rejected(decode_error_body(&frame.body)));
                    }
                    // A daemon-initiated GOODBYE means the served module's endpoint dropped
                    // and the route is gone. Surface it as a transport error so callers can
                    // distinguish a broken route from a clean stream end.
                    FrameType::Goodbye => {
                        self.done = true;
                        let _ = self.registration.take();
                        return Err(ConnError::Transport(ROUTE_GONE_MESSAGE.into()));
                    }
                    ty => {
                        return Err(ConnError::Protocol(format!(
                            "unexpected subscribe-stream frame {ty:?}"
                        )))
                    }
                },
                None => {
                    self.done = true;
                    let _ = self.registration.take();
                    return Ok(None); // connection closed → stream ended
                }
            }
        }
    }

    fn finish_lagged(&mut self) -> ConnError {
        self.done = true;
        let _ = self.registration.take();
        let _ = self.rx.take();
        ConnError::StreamLagged {
            buffered: SUBSCRIBE_STREAM_BUFFER,
        }
    }
}

impl Drop for SubscribeStream {
    fn drop(&mut self) {
        let _ = self.registration.take();
        // Best-effort route GOODBYE so the daemon frees this subscribe route and the
        // server-side subscriber task can tear down. Use synchronous try_send because Drop
        // cannot be async; if the local writer queue is full or closed the route just lingers
        // until the connection drops — a bounded resource cost, never a correctness issue.
        // Skip sending GOODBYE once the stream already ended, because a terminal frame already
        // released the route server-side or a route-gone GOODBYE already made it unreachable.
        if !self.done {
            if let Ok(frame) = Frame::build(
                FrameType::Goodbye,
                data_flags(),
                self.route.channel,
                self.route.epoch,
                self.corr,
                Vec::new(),
            ) {
                let _ = self.write_tx.try_send(frame);
            }
        }
        let mut routes = self.route_epochs.lock().unwrap();
        if routes.get(&self.route.channel) == Some(&self.route.epoch) {
            routes.remove(&self.route.channel);
        }
    }
}

/// Sends a best-effort data-plane `Cancel` for one sent request if the caller gives up on it
/// before its terminal arrives.
///
/// Without this, a tool call that outlived broca's reply deadline (or whose run was aborted)
/// kept running at the provider with nobody waiting for its answer. One such call, a search
/// stuck on pathological input, ran on for minutes and held a lock that made every later route
/// open on its project time out. The daemon forwards the `Cancel` to the module, which can then
/// stop the work.
///
/// Fire-and-forget: nothing waits for the provider's `cancelled` reply. That reply, or a
/// Response that crossed the `Cancel` on the wire, finds no waiter and is dropped by the reader
/// like any late terminal. The `Cancel` resends nothing, so the call is still sent at most once,
/// and it says nothing about whether the tool ran, so callers record the outcome exactly as
/// they would without it.
struct CancelOnAbandon {
    write_tx: mpsc::Sender<Frame>,
    route_epochs: RouteEpochs,
    route: RouteHandle,
    corr: u64,
    armed: bool,
}

impl CancelOnAbandon {
    /// Arm the guard for a request already handed to `runtime`'s writer.
    fn arm(runtime: &ConnectionRuntime, route: &RouteHandle, corr: u64) -> Self {
        Self {
            write_tx: runtime.write_tx.clone(),
            route_epochs: Arc::clone(&runtime.route_epochs),
            route: *route,
            corr,
            armed: true,
        }
    }

    /// The provider answered; there is nothing left to cancel.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnAbandon {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // A route the daemon already closed (or that this client closed) has no call left to
        // cancel, and the daemon would only discard the frame.
        let route_open =
            self.route_epochs.lock().unwrap().get(&self.route.channel) == Some(&self.route.epoch);
        if !route_open {
            return;
        }
        // `try_send`, not `send`: Drop cannot await. If the writer queue is full or closed the
        // Cancel is lost and the provider finishes the call unobserved, which is what happened
        // before this guard existed.
        if let Ok(frame) = Frame::build(
            FrameType::Cancel,
            data_flags(),
            self.route.channel,
            self.route.epoch,
            self.corr,
            Vec::new(),
        ) {
            let _ = self.write_tx.try_send(frame);
        }
    }
}

async fn open_authenticated_socket(
    connection_file_path: &Path,
) -> ConnResult<(OwnedReadHalf, OwnedWriteHalf)> {
    let conn = connection_file::read(connection_file_path)
        .map_err(|e| ConnError::Transport(format!("read connection file: {e}")))?;
    let mut stream = crate::tcp_connect::connect_endpoints(
        conn.endpoints
            .iter()
            .map(|endpoint| (endpoint.host.as_str(), endpoint.port)),
    )
    .await
    .map_err(|e| ConnError::Transport(format!("connect: {e}")))?;
    stream
        .set_nodelay(true)
        .map_err(|e| ConnError::Transport(e.to_string()))?;
    authenticate_client(&mut stream, &conn, AUTH_DEADLINE)
        .await
        .map_err(|e| ConnError::Transport(format!("authenticate: {e}")))?;
    Ok(stream.into_split())
}

fn new_runtime() -> (Arc<ConnectionRuntime>, mpsc::Receiver<Frame>) {
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let route_epochs: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
    let (write_tx, write_rx) = mpsc::channel(256);
    (
        Arc::new(ConnectionRuntime {
            write_tx,
            pending,
            route_epochs,
            write_confirmations: Arc::new(Mutex::new(HashMap::new())),
            conn: ConnToken(NEXT_CONNECTION_TOKEN.fetch_add(1, Ordering::Relaxed)),
            shutdown: CancellationToken::new(),
            capabilities: tokio::sync::OnceCell::new(),
        }),
        write_rx,
    )
}

#[cfg(test)]
#[derive(Default)]
struct ReaderTestGate {
    frame_read: tokio::sync::Notify,
    proceed: tokio::sync::Notify,
}

#[derive(Clone)]
struct ReaderGeneration {
    shared: Weak<ConnectionShared>,
    conn: ConnToken,
    shutdown: CancellationToken,
    #[cfg(test)]
    test_gate: Option<Arc<ReaderTestGate>>,
}

fn spawn_generation_tasks(
    shared: &Arc<ConnectionShared>,
    runtime: Arc<ConnectionRuntime>,
    read_half: OwnedReadHalf,
    write_half: OwnedWriteHalf,
    write_rx: mpsc::Receiver<Frame>,
) {
    let generation = ReaderGeneration {
        shared: Arc::downgrade(shared),
        conn: runtime.conn,
        shutdown: runtime.shutdown.clone(),
        #[cfg(test)]
        test_gate: None,
    };
    tokio::spawn(reader_task(
        read_half,
        Arc::clone(&runtime.pending),
        Arc::clone(&runtime.route_epochs),
        runtime.write_tx.clone(),
        Some(generation),
    ));
    tokio::spawn(writer_task(
        write_half,
        write_rx,
        Arc::downgrade(shared),
        runtime.conn,
        runtime.shutdown.clone(),
        Arc::clone(&runtime.write_confirmations),
    ));
}

enum ChannelZeroProbeBlocker {
    Live,
    TombstoneUntil(Instant),
}

/// Distinguishes live channel-0 traffic from retained route.open cleanup state. A live request
/// suppresses both probe writes and conviction because the daemon's connection-scoped FIFO cannot
/// answer a Ping while servicing that request. This is load-bearing for route.open: its bind relay
/// can park the loop longer than the ordinary response deadline, and probing behind that healthy
/// work would falsely convict every route on the socket.
///
/// A retained route.open tombstone serves a different purpose: it lets the reader close a route
/// the daemon committed after the caller left. Treating that cleanup identity as live forever also
/// suppresses recovery forever on a dead socket. Tombstones therefore block only through the
/// bounded bind-relay remainder; after that deadline they remain available for cleanup but are no
/// longer evidence that the daemon is busy.
fn channel_zero_probe_blocker(
    runtime: &ConnectionRuntime,
    now: Instant,
) -> Option<ChannelZeroProbeBlocker> {
    let waiters = runtime.pending.lock().unwrap();
    let mut latest_tombstone = None;
    for (key, entry) in waiters.iter() {
        if key.0 != 0 {
            continue;
        }
        match entry {
            PendingEntry::Live { .. } => return Some(ChannelZeroProbeBlocker::Live),
            PendingEntry::LateRouteOpen { blocks_probe_until } if *blocks_probe_until > now => {
                latest_tombstone = Some(
                    latest_tombstone.map_or(*blocks_probe_until, |current: Instant| {
                        current.max(*blocks_probe_until)
                    }),
                );
            }
            PendingEntry::LateRouteOpen { .. } => {}
        }
    }
    latest_tombstone.map(ChannelZeroProbeBlocker::TombstoneUntil)
}

enum ProbeRequestAction {
    None,
    WakeAt(Instant),
    Start {
        runtime: Arc<ConnectionRuntime>,
        corr: u64,
    },
}

/// Start a probe now or remember it until channel-zero traffic stops blocking it. A timeout is a
/// durable request for a health decision: live control work postpones that decision instead of
/// discarding it, while a tombstone supplies the bounded wake-up time.
fn request_liveness_probe(shared: &Arc<ConnectionShared>, runtime: &Arc<ConnectionRuntime>) {
    let action = {
        let mut state = shared.current.lock().unwrap();
        if !state.active || state.runtime.conn != runtime.conn {
            ProbeRequestAction::None
        } else if state.probe_in_flight {
            if channel_zero_probe_blocker(runtime, Instant::now()).is_some() {
                // Channel-zero activity will acquit the current probe. Remember this timeout so
                // that acquittal cannot consume the only health decision for the newer silence.
                state.probe_deferred = true;
            }
            ProbeRequestAction::None
        } else {
            match channel_zero_probe_blocker(runtime, Instant::now()) {
                Some(ChannelZeroProbeBlocker::Live) => {
                    state.probe_deferred = true;
                    state.deferred_probe_wake = None;
                    ProbeRequestAction::None
                }
                Some(ChannelZeroProbeBlocker::TombstoneUntil(deadline)) => {
                    state.probe_deferred = true;
                    if state.deferred_probe_wake == Some(deadline) {
                        ProbeRequestAction::None
                    } else {
                        state.deferred_probe_wake = Some(deadline);
                        ProbeRequestAction::WakeAt(deadline)
                    }
                }
                None => {
                    state.probe_deferred = false;
                    state.deferred_probe_wake = None;
                    state.probe_in_flight = true;
                    ProbeRequestAction::Start {
                        runtime: Arc::clone(runtime),
                        corr: shared.corr.fetch_add(1, Ordering::Relaxed),
                    }
                }
            }
        }
    };

    match action {
        ProbeRequestAction::None => {}
        ProbeRequestAction::WakeAt(deadline) => {
            let owner = Arc::downgrade(shared);
            let conn = runtime.conn;
            tokio::spawn(async move {
                time::sleep_until(time::Instant::from_std(deadline)).await;
                let Some(owner) = owner.upgrade() else {
                    return;
                };
                let runtime = {
                    let mut state = owner.current.lock().unwrap();
                    if !state.active
                        || state.runtime.conn != conn
                        || !state.probe_deferred
                        || state.deferred_probe_wake != Some(deadline)
                    {
                        return;
                    }
                    state.deferred_probe_wake = None;
                    Arc::clone(&state.runtime)
                };
                request_liveness_probe(&owner, &runtime);
            });
        }
        ProbeRequestAction::Start { runtime, corr } => {
            let owner = Arc::downgrade(shared);
            tokio::spawn(async move { run_liveness_probe(owner, runtime, corr).await });
        }
    }
}

fn pending_registration_changed(shared: &Weak<ConnectionShared>, conn: ConnToken) {
    let Some(owner) = shared.upgrade() else {
        return;
    };
    let runtime = {
        let state = owner.current.lock().unwrap();
        if !state.active
            || state.runtime.conn != conn
            || !state.probe_deferred
            || state.probe_in_flight
        {
            return;
        }
        Arc::clone(&state.runtime)
    };
    request_liveness_probe(&owner, &runtime);
}

fn deactivate_locked(state: &mut GenerationState) -> bool {
    if !state.active {
        return false;
    }
    state.active = false;
    state.probe_in_flight = false;
    state.probe_deferred = false;
    state.deferred_probe_wake = None;
    state.runtime.shutdown.cancel();
    state.runtime.route_epochs.lock().unwrap().clear();
    state.runtime.pending.lock().unwrap().clear();
    for (_, confirmation) in state.runtime.write_confirmations.lock().unwrap().drain() {
        let _ = confirmation.send(Err("connection generation closed".into()));
    }
    true
}

fn generation_failed(shared: &Weak<ConnectionShared>, conn: ConnToken) {
    let Some(shared) = shared.upgrade() else {
        return;
    };
    let should_reconnect = {
        let mut state = shared.current.lock().unwrap();
        state.runtime.conn == conn && deactivate_locked(&mut state)
    };
    if should_reconnect {
        let _ = shared.reconnect_tx.send(conn);
    }
}

fn reconnect_backoff(failures: u32) -> Duration {
    let shift = failures.saturating_sub(1).min(6);
    RECONNECT_INITIAL_BACKOFF
        .saturating_mul(1u32 << shift)
        .min(RECONNECT_MAX_BACKOFF)
}

async fn reconnect_supervisor(
    shared: Weak<ConnectionShared>,
    mut reconnect_rx: mpsc::UnboundedReceiver<ConnToken>,
) {
    while let Some(failed_conn) = reconnect_rx.recv().await {
        let mut failures = 0u32;
        loop {
            let Some(owner) = shared.upgrade() else {
                return;
            };
            let still_needed = {
                let state = owner.current.lock().unwrap();
                state.runtime.conn == failed_conn && !state.active
            };
            if !still_needed {
                break;
            }

            #[cfg(test)]
            owner.reconnect_attempts.fetch_add(1, Ordering::Relaxed);
            match open_authenticated_socket(&owner.connection_file_path).await {
                Ok((read_half, write_half)) => {
                    let (runtime, write_rx) = new_runtime();
                    let installed = {
                        let mut state = owner.current.lock().unwrap();
                        if state.runtime.conn == failed_conn && !state.active {
                            *state = GenerationState::new(Arc::clone(&runtime));
                            true
                        } else {
                            false
                        }
                    };
                    if installed {
                        // Pairs with the conviction line: together they bound the outage, so a
                        // reader can tell "recovered in 200ms" from "convicted and never came
                        // back". A conviction with no matching line here is the shape worth
                        // investigating.
                        tracing::warn!(
                            target: "transport",
                            "reconnected to subc after {} failed attempt(s)",
                            failures
                        );
                        spawn_generation_tasks(&owner, runtime, read_half, write_half, write_rx);
                    } else {
                        runtime.shutdown.cancel();
                    }
                    break;
                }
                Err(_) => {
                    failures = failures.saturating_add(1);
                    drop(owner);
                    time::sleep(reconnect_backoff(failures)).await;
                }
            }
        }
    }
}

fn clear_probe_if_current(shared: &ConnectionShared, conn: ConnToken) {
    let mut state = shared.current.lock().unwrap();
    if state.runtime.conn == conn {
        state.probe_in_flight = false;
    }
}

async fn run_liveness_probe(
    shared: Weak<ConnectionShared>,
    runtime: Arc<ConnectionRuntime>,
    corr: u64,
) {
    let ping = match Frame::build(FrameType::Ping, control_flags(), 0, 0, corr, Vec::new()) {
        Ok(frame) => frame,
        Err(_) => {
            if let Some(owner) = shared.upgrade() {
                clear_probe_if_current(&owner, runtime.conn);
            }
            return;
        }
    };
    let (confirmation_tx, confirmation_rx) = oneshot::channel();
    runtime
        .write_confirmations
        .lock()
        .unwrap()
        .insert(corr, confirmation_tx);
    // Egress has its own budget: a blocked writer cannot defer conviction indefinitely.
    // The silence window below still begins only after the writer confirms the Ping.
    let egress = time::timeout(PROBE_EGRESS_TIMEOUT, async {
        runtime.write_tx.send(ping).await.map_err(|_| ())?;
        confirmation_rx.await.map_err(|_| ())?.map_err(|_| ())
    })
    .await;
    let probe_start = match egress {
        Ok(Ok(sent_at)) => sent_at,
        _ => {
            runtime.write_confirmations.lock().unwrap().remove(&corr);
            generation_failed(&shared, runtime.conn);
            return;
        }
    };

    let Some(owner) = shared.upgrade() else {
        return;
    };
    let (control_watermark, probe_timeout, deferred) = {
        let mut state = owner.current.lock().unwrap();
        if state.runtime.conn != runtime.conn || !state.active || !state.probe_in_flight {
            return;
        }
        if channel_zero_probe_blocker(&runtime, Instant::now()).is_some() {
            state.probe_in_flight = false;
            state.probe_deferred = true;
            (0, owner.probe_timeout, true)
        } else {
            (state.control_watermark, owner.probe_timeout, false)
        }
    };
    if deferred {
        request_liveness_probe(&owner, &runtime);
        return;
    }
    #[cfg(test)]
    owner.probe_windows_started.fetch_add(1, Ordering::Release);
    drop(owner);
    time::sleep(probe_timeout).await;

    let Some(owner) = shared.upgrade() else {
        return;
    };
    let (convicted, retry_deferred) = {
        let mut state = owner.current.lock().unwrap();
        if state.runtime.conn != runtime.conn || !state.active || !state.probe_in_flight {
            (false, false)
        } else if state.control_watermark != control_watermark
            || channel_zero_probe_blocker(&runtime, Instant::now()).is_some()
            || state.last_inbound >= probe_start
        {
            state.probe_in_flight = false;
            let retry_deferred = state.probe_deferred;
            if !retry_deferred {
                state.deferred_probe_wake = None;
            }
            (false, retry_deferred)
        } else {
            (deactivate_locked(&mut state), false)
        }
    };
    if convicted {
        // THE ONLY OBSERVABLE THIS SUBSYSTEM EMITS, and deliberately the rarest event in it.
        //
        // Conviction means a probe went unanswered for the full window while nothing else
        // arrived: the socket is declared dead and every route on it is about to be torn down
        // and reopened. Silent recovery is indistinguishable from never having recovered, and
        // that gap is not academic -- I once reported "no probe or conviction events in the
        // daemon log" as evidence the path had not fired, when this module emitted NOTHING and
        // the absence was guaranteed by construction rather than by behaviour.
        //
        // The healthy cases stay silent on purpose. A probe is sent on every idle window, so
        // logging sends or survivals would bury this line in noise on a quiet daemon.
        tracing::warn!(
            target: "transport",
            "liveness probe unanswered; convicting the subc socket and reconnecting"
        );
        let _ = owner.reconnect_tx.send(runtime.conn);
    } else if retry_deferred {
        request_liveness_probe(&owner, &runtime);
    }
}

async fn reader_task<R>(
    mut read_half: R,
    pending: PendingMap,
    route_epochs: RouteEpochs,
    write_tx: mpsc::Sender<Frame>,
    generation: Option<ReaderGeneration>,
) where
    R: AsyncRead + Unpin,
{
    loop {
        let frame = if let Some(binding) = &generation {
            // Dropping `read_frame` part-way loses the bytes it already read (it is not
            // cancellation-safe), and that is only harmless here because the other arm is
            // shutdown: when it wins, the loop breaks and this reader never reads again. Do not
            // add an arm that lets the loop continue after it wins; the module serve loop did
            // exactly that and desynchronized its stream (see `run_frame_loop` in
            // broca-module-serve), which is why it keeps one read future alive across turns.
            tokio::select! {
                biased;
                _ = binding.shutdown.cancelled() => None,
                result = read_frame(&mut read_half) => result.ok().flatten(),
            }
        } else {
            read_frame(&mut read_half).await.ok().flatten()
        };
        let Some(frame) = frame else {
            break;
        };
        #[cfg(test)]
        if let Some(gate) = generation
            .as_ref()
            .and_then(|binding| binding.test_gate.as_ref())
        {
            gate.frame_read.notify_one();
            gate.proceed.notified().await;
        }
        if let Some(binding) = &generation {
            let Some(shared) = binding.shared.upgrade() else {
                break;
            };
            let mut state = shared.current.lock().unwrap();
            if state.runtime.conn != binding.conn || !state.active {
                break;
            }
            state.last_inbound = Instant::now();
        }

        tracing::debug!(
            target: "subc.wire",
            stage = "received",
            conn = ?generation.as_ref().map(|binding| binding.conn),
            channel = frame.header.channel, epoch = frame.header.epoch,
            corr = frame.header.corr, ty = ?frame.header.ty,
            "consumer ingress progress"
        );

        let channel = frame.header.channel;
        let epoch = frame.header.epoch;
        if channel != 0 && route_epochs.lock().unwrap().get(&channel) != Some(&epoch) {
            // A frame from an unknown or dead route generation must have no dispatch or
            // lifecycle effect, including waking a waiter that reused its correlation id.
            FRAME_DROPS.stamp_stale_epoch();
            tracing::debug!(target: "subc.wire", stage = "stale_epoch", channel, epoch, corr = frame.header.corr, "consumer ingress dropped");
            continue;
        }

        let frame_ty = frame.header.ty;
        if frame_ty == FrameType::Request && channel != 0 {
            // A Request on a route channel is a NEW call from the module, never part of one of
            // ours. Corrs are not namespaced by direction: each side allocates its own on a
            // channel, so this corr may legally equal a call broca has in flight. Routing it by
            // (channel, epoch, corr) would hand it to that call as its terminal, so it never
            // reaches the pending map. Answer it at once, as the subc SDK does for a consumer with
            // no handler, so the module is not left waiting out its own deadline.
            FRAME_DROPS.stamp_reverse_request_refused();
            let Ok(reply) = reverse_request_refusal(&frame) else {
                continue;
            };
            // Control replies are best-effort; a full egress queue must not stop ingress.
            let refusal = write_tx.try_send(reply);
            if matches!(refusal, Err(mpsc::error::TrySendError::Closed(_))) {
                break;
            }
            continue;
        }
        let key = (channel, epoch, frame.header.corr);
        let pending_kind = {
            let mut waiters = pending.lock().unwrap();
            let take_route_open = channel == 0
                && matches!(frame.header.ty, FrameType::Response | FrameType::Error)
                && matches!(
                    waiters.get(&key),
                    Some(PendingEntry::Live {
                        route_open: true,
                        ..
                    })
                );
            if take_route_open {
                match waiters.remove(&key) {
                    Some(PendingEntry::Live { sender, .. }) => Some((Some(sender), true, false)),
                    _ => unreachable!("route.open entry was inspected under the same lock"),
                }
            } else {
                match waiters.get(&key) {
                    Some(PendingEntry::Live { sender, route_open }) => {
                        Some((Some(sender.clone()), *route_open, false))
                    }
                    Some(PendingEntry::LateRouteOpen { .. }) => Some((None, true, true)),
                    None => None,
                }
            }
        };

        let mut opened_route = None;
        if channel == 0 && frame.header.ty == FrameType::Response {
            if let Some((_, true, late)) = &pending_kind {
                if let Ok(ClientControlResponse::RouteOpen {
                    route_channel,
                    route_epoch,
                }) = serde_json::from_slice::<ClientControlResponse>(&frame.body)
                {
                    // Publish the handle before waking route_open. Socket ordering then makes
                    // an immediate reverse frame on the new route visible to ingress validation.
                    route_epochs
                        .lock()
                        .unwrap()
                        .insert(route_channel, route_epoch);
                    opened_route = Some((route_channel, route_epoch));
                    if *late {
                        pending.lock().unwrap().remove(&key);
                        let goodbye = Frame::build(
                            FrameType::Goodbye,
                            data_flags(),
                            route_channel,
                            route_epoch,
                            0,
                            Vec::new(),
                        );
                        let Ok(goodbye) = goodbye else {
                            break;
                        };
                        let late_cleanup = write_tx.try_send(goodbye);
                        if matches!(late_cleanup, Err(mpsc::error::TrySendError::Closed(_))) {
                            break;
                        }
                        let mut routes = route_epochs.lock().unwrap();
                        if routes.get(&route_channel) == Some(&route_epoch) {
                            routes.remove(&route_channel);
                        }
                        continue;
                    }
                }
            }
        }

        if channel == 0
            && frame.header.ty == FrameType::Error
            && matches!(&pending_kind, Some((_, _, true)))
        {
            pending.lock().unwrap().remove(&key);
            continue;
        }

        if frame.header.ty == FrameType::Goodbye && channel != 0 {
            finish_route(&pending, &route_epochs, &frame);
            continue;
        }

        let had_waiter = pending_kind.is_some();
        tracing::debug!(target: "subc.wire", stage = "dispatch", channel, epoch, corr = key.2, had_waiter, "consumer ingress progress");
        if let Some((Some(sender), route_open, _)) = pending_kind {
            match sender.send(frame) {
                Err(PendingSendError::StreamFull) => {
                    detach_lagged_stream(&pending, &route_epochs, &write_tx, key)
                }
                Err(PendingSendError::Closed) if route_open => {
                    if let Some((route_channel, route_epoch)) = opened_route {
                        let goodbye = Frame::build(
                            FrameType::Goodbye,
                            data_flags(),
                            route_channel,
                            route_epoch,
                            0,
                            Vec::new(),
                        );
                        let Ok(goodbye) = goodbye else {
                            break;
                        };
                        let abandoned_cleanup = write_tx.try_send(goodbye);
                        if matches!(abandoned_cleanup, Err(mpsc::error::TrySendError::Closed(_))) {
                            break;
                        }
                        let mut routes = route_epochs.lock().unwrap();
                        if routes.get(&route_channel) == Some(&route_epoch) {
                            routes.remove(&route_channel);
                        }
                    }
                }
                Ok(()) | Err(PendingSendError::Closed) => {}
            }
        }
        // No waiter (e.g. unsolicited route-status push) is intentionally dropped. A TERMINAL
        // with no waiter is the one worth counting: nobody is waiting for it any more, which
        // means the reply arrived after its caller timed out or was cancelled. Discarding it is
        // right; the count is what tells an operator those late replies are happening at all.
        if !had_waiter && matches!(frame_ty, FrameType::Response | FrameType::Error) {
            FRAME_DROPS.stamp_unmatched_terminal();
        }
    }

    // A generation may finish after its replacement is installed. Only the matching current
    // generation can trigger teardown; otherwise these maps belong solely to the stale runtime.
    if let Some(binding) = generation {
        generation_failed(&binding.shared, binding.conn);
    } else {
        route_epochs.lock().unwrap().clear();
        pending.lock().unwrap().clear();
    }
}

/// Hold the epoch lock through fanout so registration cannot slip between validation and close.
fn finish_route(pending: &PendingMap, route_epochs: &RouteEpochs, goodbye: &Frame) {
    let channel = goodbye.header.channel;
    let epoch = goodbye.header.epoch;
    let mut routes = route_epochs.lock().unwrap();
    if routes.get(&channel) != Some(&epoch) {
        return;
    }
    routes.remove(&channel);
    pending.lock().unwrap().retain(|&(ch, ep, _), entry| {
        if ch != channel || ep != epoch {
            return true;
        }
        if let PendingEntry::Live { sender, .. } = entry {
            let _ = sender.send(goodbye.clone());
        }
        false
    });
}

fn detach_lagged_stream(
    pending: &PendingMap,
    route_epochs: &RouteEpochs,
    write_tx: &mpsc::Sender<Frame>,
    key: PendingKey,
) {
    pending.lock().unwrap().remove(&key);
    let (channel, epoch, corr) = key;
    let mut routes = route_epochs.lock().unwrap();
    if routes.get(&channel) == Some(&epoch) {
        routes.remove(&channel);
    }
    drop(routes);

    if let Ok(goodbye) = Frame::build(
        FrameType::Goodbye,
        data_flags(),
        channel,
        epoch,
        corr,
        Vec::new(),
    ) {
        // Ingress must remain non-blocking even if egress is congested. The route is already
        // invalidated locally; connection teardown is the fallback if this GOODBYE cannot queue.
        let _ = write_tx.try_send(goodbye);
    }
}

async fn writer_task<W: AsyncWrite + Unpin>(
    mut write_half: W,
    mut rx: mpsc::Receiver<Frame>,
    shared: Weak<ConnectionShared>,
    conn: ConnToken,
    shutdown: CancellationToken,
    write_confirmations: WriteConfirmations,
) {
    loop {
        let frame = tokio::select! {
            biased;
            _ = shutdown.cancelled() => None,
            frame = rx.recv() => frame,
        };
        let Some(frame) = frame else {
            break;
        };
        tracing::debug!(
            target: "subc.wire",
            stage = "write_started",
            conn = ?conn,
            channel = frame.header.channel, epoch = frame.header.epoch,
            corr = frame.header.corr, ty = ?frame.header.ty,
            "consumer egress progress"
        );
        // A partial frame may be discarded only when this socket generation is retiring.
        let result = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            result = write_frame(&mut write_half, &frame) => result,
        };
        tracing::debug!(
            target: "subc.wire",
            stage = "write_finished",
            conn = ?conn,
            channel = frame.header.channel, epoch = frame.header.epoch,
            corr = frame.header.corr, ok = result.is_ok(),
            "consumer egress progress"
        );
        if frame.header.ty == FrameType::Ping {
            if let Some(confirmation) = write_confirmations
                .lock()
                .unwrap()
                .remove(&frame.header.corr)
            {
                let receipt = result
                    .as_ref()
                    .map(|_| Instant::now())
                    .map_err(ToString::to_string);
                let _ = confirmation.send(receipt);
            }
        }
        if result.is_err() {
            generation_failed(&shared, conn);
            break;
        }
    }
}

// Put request identity on the opt-in wire target; tool arguments are deliberately not logged.
fn log_tool_dispatch(route_channel: u16, correlation_id: u64, operation: &str) {
    tracing::debug!(
        target: "subc.wire",
        stage = "tool_dispatch",
        route_channel,
        correlation_id,
        operation,
        "tool call dispatched"
    );
}

/// Channel-0 control-plane flags: PASSIVE priority, non-binary (JSON body) —
/// matches subc's module-side control convention for route.open/catalog.list.
fn control_flags() -> Flags {
    Flags::new(false, Priority::Passive, false)
}

/// Data-plane (tool-call) flags: INTERACTIVE priority, non-binary (JSON body) —
/// matches the subc-mcp gateway's route-channel request flags.
fn data_flags() -> Flags {
    Flags::new(false, Priority::Interactive, false)
}

/// The `Error` reply to a reverse request: same version, channel, epoch and corr, and the same
/// body the subc SDK sends when no handler is registered for the request's method family (the
/// part of `method` before the first `/`).
fn reverse_request_refusal(request: &Frame) -> Result<Frame, subc_protocol::FrameBuildError> {
    let family = serde_json::from_slice::<serde_json::Value>(&request.body)
        .ok()
        .and_then(|body| {
            body.get("method")?
                .as_str()?
                .split('/')
                .next()
                .filter(|family| !family.is_empty())
                .map(str::to_string)
        });
    let message = family.map_or_else(
        || "reverse request has no valid method".to_string(),
        |family| format!("no reverse-request handler is registered for {family}"),
    );
    let body = serde_json::to_vec(&subc_protocol::ErrorBody::new(
        REVERSE_REQUEST_UNHANDLED,
        message,
    ))
    .unwrap_or_default();
    Frame::build_with_version(
        request.header.ver,
        FrameType::Error,
        data_flags(),
        request.header.channel,
        request.header.epoch,
        request.header.corr,
        body,
    )
}

pub(crate) fn decode_error_body(body: &[u8]) -> RejectedMessage {
    match serde_json::from_slice::<subc_protocol::ErrorBody>(body) {
        Ok(error) => RejectedMessage::structured(error.code, error.message),
        Err(_) => String::from_utf8_lossy(body).into_owned().into(),
    }
}

pub(crate) fn rejection_code(rejection: &str) -> &str {
    rejection
        .split_once(" — ")
        .map_or(rejection, |(code, _)| code)
}

/// The default route-open retry budget. Covers the module-swap refusal window without making a
/// caller wait through a daemon bounce; transport failures belong to the reconnect path, not
/// to this budget.
pub const ROUTE_OPEN_RETRY_DEADLINE: Duration = Duration::from_secs(3);

/// The first backoff envelope between route-open attempts; later envelopes double up to the
/// policy's cap, and each wait is a random part of its envelope (see [`RouteOpenRetry`]).
pub const ROUTE_OPEN_RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// The widest backoff envelope under [`RouteOpenRetry::default_patience`]: once the doubling
/// reaches it, a refused open waits at most this long before trying again.
pub const ROUTE_OPEN_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(5);

/// How long a route open keeps retrying a temporary refusal, and how widely it spaces the
/// attempts.
///
/// A refusal is retried only while its code is one of the daemon's temporary route-open
/// refusals (`module_warming`, `module_reloading`, `target_unavailable`, `module_timeout`).
/// The waits follow an envelope that starts at [`ROUTE_OPEN_RETRY_INTERVAL`] and doubles up to
/// `max_interval`; each wait is drawn uniformly from zero to its envelope ("full jitter"), so
/// callers refused together do not all come back together. No wait extends past the open's
/// deadline. Each open starts a fresh `budget`, so keeping this policy does not consume the
/// patience of future opens. Once that deadline has passed, the next refusal is returned
/// unchanged; a terminal refusal is returned unchanged at once.
#[derive(Clone, Copy, Debug)]
pub struct RouteOpenRetry {
    /// How long to keep retrying, measured from the start of each open. A zero budget still
    /// makes one attempt. It does NOT cut short an attempt already in flight: each attempt
    /// waits up to [`ROUTE_OPEN_ATTEMPT_TIMEOUT`] for its reply regardless of this budget.
    pub budget: Duration,
    /// The upper bound for the doubling backoff envelope between attempts.
    pub max_interval: Duration,
}

impl RouteOpenRetry {
    /// Retry for `budget` per open, with the backoff envelope capped at `max_interval`.
    pub fn within(budget: Duration, max_interval: Duration) -> Self {
        Self {
            budget,
            max_interval,
        }
    }

    /// [`ROUTE_OPEN_RETRY_DEADLINE`] per open, with an envelope that starts at
    /// [`ROUTE_OPEN_RETRY_INTERVAL`] and doubles up to [`ROUTE_OPEN_RETRY_MAX_INTERVAL`].
    ///
    /// The envelope grows, rather than staying at a flat 100 ms, because a module that is down
    /// for longer than a swap turned the flat interval into a storm: during a one-minute module
    /// stop, one broca connection sent about 80 `route.open` a second to the stopped module,
    /// against about 5 a second from other clients. A swap that ends within the first few
    /// hundred milliseconds is still noticed almost as quickly as before.
    pub fn default_patience() -> Self {
        Self::within(ROUTE_OPEN_RETRY_DEADLINE, ROUTE_OPEN_RETRY_MAX_INTERVAL)
    }
}

/// A uniformly random wait between zero and `envelope` ("full jitter"), so that callers refused
/// at the same moment spread their next attempts out instead of arriving together.
pub(crate) fn full_jitter(envelope: Duration) -> Duration {
    use std::hash::BuildHasher;
    static DRAWS: AtomicU64 = AtomicU64::new(0);
    // Each `RandomState` is seeded with fresh random keys, so hashing a counter with one gives
    // an unpredictable value without pulling in a random-number crate for this one use.
    let draw = std::collections::hash_map::RandomState::new()
        .hash_one(DRAWS.fetch_add(1, Ordering::Relaxed));
    // The top 53 bits make an exact f64 fraction in [0, 1).
    let fraction = (draw >> 11) as f64 / (1u64 << 53) as f64;
    envelope.mul_f64(fraction)
}

/// Why [`retry_route_open`] gave up.
#[derive(Debug)]
pub(crate) enum RouteOpenError {
    /// The last attempt's error: a terminal refusal, a temporary refusal that outlasted the
    /// deadline, or a transport or protocol failure (never retried here).
    Conn(ConnError),
    /// The caller's cancellation token fired while waiting between attempts.
    Cancelled,
}

impl RouteOpenError {
    /// For callers that passed no cancellation token, where `Cancelled` cannot occur.
    fn into_conn(self) -> ConnError {
        match self {
            RouteOpenError::Conn(error) => error,
            RouteOpenError::Cancelled => ConnError::Transport("route open cancelled".into()),
        }
    }
}

/// THE route-open retry loop: every consumer's `route.open` goes through here, so a module
/// restart's temporary refusals are absorbed in one place instead of by each consumer that
/// remembers to. `attempt` performs one `route.open`; see [`RouteOpenRetry`] for the policy.
///
/// Only refusals are retried. A transport failure is returned at once: reconnecting is the
/// connection's own job, and retrying on top of it would stack two backoffs.
///
/// `cancel` is checked only while waiting between attempts, so an attempt already in flight
/// completes and its outcome is returned.
///
/// Two clocks meet here. `retry.budget` bounds STARTING a new attempt; each attempt, once
/// started, is bounded by its own reply deadline, [`ROUTE_OPEN_ATTEMPT_TIMEOUT`], inside
/// `attempt`. So the longest this can take is the retry deadline plus one attempt timeout. A
/// wait at the module's gate ends at the retry deadline too, without an attempt, so it does
/// not stretch that bound.
///
/// `gate_for` returns the target module's availability gate (see [`ModuleGate`]), or `None`
/// for an open that is not gated. It is asked before every attempt, and again when a gate it
/// gave retires, because after a reconnect the module's gate is a new one. While the module
/// is known to be down, the wait happens at the gate, shared with every other open to that
/// module.
pub(crate) async fn retry_route_open<F, Fut, G>(
    retry: RouteOpenRetry,
    cancel: Option<&CancellationToken>,
    gate_for: G,
    attempt: F,
) -> Result<RouteHandle, RouteOpenError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ConnResult<RouteHandle>>,
    G: Fn() -> Option<Arc<ModuleGate>>,
{
    retry_route_open_with_scope_hold(retry, None, cancel, gate_for, attempt).await
}

/// The `gate_for` of an open that passes no module gate.
#[cfg(test)]
fn no_gate() -> Option<Arc<ModuleGate>> {
    None
}

/// How long a scoped route open holds on the daemon's two retryable scope refusals, and how
/// widely it spaces the attempts. Separate from [`RouteOpenRetry`] because the scope hold is
/// far longer than the module-swap window the ordinary budget covers, and a scoped open still
/// gives up on a warming module within that ordinary budget.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ScopeHold {
    /// No retry of a scope refusal starts after this instant.
    pub deadline: time::Instant,
    /// The upper bound for the doubling wait after `scope_not_synced`.
    pub max_interval: Duration,
}

/// [`retry_route_open`], plus the scope hold a scoped open needs. With `scope_hold`:
/// - `scope_not_synced` (the scope's owner has not synced since the daemon started) is retried
///   with its own doubling backoff until `scope_hold.deadline`;
/// - `scope_changed` (the scope record changed between admission and commit, and nothing was
///   sent) re-opens at once, against the current record. A second `scope_changed` in a row
///   waits like `scope_not_synced` instead, so a record that keeps changing cannot turn the
///   hold into a route.open storm;
/// - every other retryable code keeps the ordinary `retry` budget.
///
/// Past its deadline the last scope refusal is returned unchanged, for the caller to report
/// as `scope_unverifiable`. Without `scope_hold` the scope codes use `retry` like any other.
pub(crate) async fn retry_route_open_with_scope_hold<F, Fut, G>(
    retry: RouteOpenRetry,
    scope_hold: Option<ScopeHold>,
    cancel: Option<&CancellationToken>,
    gate_for: G,
    attempt: F,
) -> Result<RouteHandle, RouteOpenError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ConnResult<RouteHandle>>,
    G: Fn() -> Option<Arc<ModuleGate>>,
{
    retry_route_open_jittered(retry, scope_hold, cancel, gate_for, &full_jitter, attempt).await
}

/// The retry loop itself, drawing each backoff wait with `jitter` ([`full_jitter`] outside
/// tests, so that a test can make the schedule exact).
async fn retry_route_open_jittered<F, Fut, G>(
    retry: RouteOpenRetry,
    scope_hold: Option<ScopeHold>,
    cancel: Option<&CancellationToken>,
    gate_for: G,
    jitter: &(dyn Fn(Duration) -> Duration + Sync),
    mut attempt: F,
) -> Result<RouteHandle, RouteOpenError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ConnResult<RouteHandle>>,
    G: Fn() -> Option<Arc<ModuleGate>>,
{
    use subc_protocol::error_codes::{SCOPE_CHANGED, SCOPE_NOT_SYNCED};
    let retry_deadline = time::Instant::now() + retry.budget;
    let mut envelope = ROUTE_OPEN_RETRY_INTERVAL.min(retry.max_interval);
    let mut scope_interval =
        ROUTE_OPEN_RETRY_INTERVAL.min(scope_hold.map_or(retry.max_interval, |h| h.max_interval));
    let mut previous_was_scope_changed = false;
    loop {
        let mut gate = gate_for();
        let ticket = loop {
            let Some(current) = gate.clone() else {
                break None;
            };
            match current.admit(Some(retry_deadline), cancel).await {
                GateAdmission::Admitted(ticket) => break Some(ticket),
                GateAdmission::Refused(refusal) => {
                    return Err(RouteOpenError::Conn(ConnError::Rejected(refusal)))
                }
                GateAdmission::Cancelled => return Err(RouteOpenError::Cancelled),
                // The connection generation that gate paced has ended. Pass the current
                // generation's gate instead; while the connection is still reconnecting there is
                // no new gate yet, and the open goes ahead ungated and fails at once like any
                // open made during a reconnect.
                GateAdmission::Retired => {
                    gate = gate_for().filter(|fresh| !fresh.is_retired());
                }
            }
        };
        // Deliberately not wrapped in a timeout against `retry_deadline`: an attempt that
        // started before the deadline runs to its own reply deadline, because the daemon may
        // still be relaying its bind and abandoning it would report a successful open as failed.
        let outcome = attempt().await;
        if let Some(ticket) = ticket {
            ticket.report(&outcome);
        }
        match outcome {
            Ok(handle) => return Ok(handle),
            Err(ConnError::Rejected(message))
                if subc_protocol::error_codes::is_retryable_route_open(message.code()) =>
            {
                let code = message.code();
                let held = scope_hold.filter(|_| matches!(code, SCOPE_NOT_SYNCED | SCOPE_CHANGED));
                let deadline = held.map_or(retry_deadline, |hold| hold.deadline);
                let remaining = deadline.saturating_duration_since(time::Instant::now());
                if remaining.is_zero() {
                    return Err(RouteOpenError::Conn(ConnError::Rejected(message)));
                }
                let changed = code == SCOPE_CHANGED;
                let at_once = held.is_some() && changed && !previous_was_scope_changed;
                previous_was_scope_changed = changed;
                if at_once {
                    continue;
                }
                // While the module's gate is closed, the gate does the waiting for every open
                // to the module; a wait of this caller's own on top would only delay noticing
                // that the module is back.
                if held.is_none()
                    && is_module_availability_refusal(code)
                    && gate.as_deref().is_some_and(ModuleGate::is_closed)
                {
                    continue;
                }
                let wait = match held {
                    Some(_) => scope_interval,
                    None => jitter(envelope).min(envelope),
                };
                match cancel {
                    Some(token) => {
                        tokio::select! {
                            _ = token.cancelled() => return Err(RouteOpenError::Cancelled),
                            _ = time::sleep(wait.min(remaining)) => {}
                        }
                    }
                    None => time::sleep(wait.min(remaining)).await,
                }
                match held {
                    Some(hold) => scope_interval = (scope_interval * 2).min(hold.max_interval),
                    None => envelope = (envelope * 2).min(retry.max_interval),
                }
            }
            Err(error) => return Err(RouteOpenError::Conn(error)),
        }
    }
}

async fn cancelled_or_never(cancel: Option<&CancellationToken>) {
    match cancel {
        Some(token) => token.cancelled().await,
        None => std::future::pending().await,
    }
}

/// Whether a route.open refusal says the target MODULE is unavailable (stopped, starting,
/// restarting, or not answering), as opposed to anything about the route asked for. These are
/// the refusals that are the same for every caller of that module, so one caller's refusal is
/// news for all of them; see [`ModuleGate`].
fn is_module_availability_refusal(code: &str) -> bool {
    use subc_protocol::error_codes::{
        MODULE_RELOADING, MODULE_TIMEOUT, MODULE_WARMING, TARGET_UNAVAILABLE,
    };
    matches!(
        code,
        TARGET_UNAVAILABLE | MODULE_WARMING | MODULE_RELOADING | MODULE_TIMEOUT
    )
}

/// What one open's outcome says about its target module.
enum ModuleEvidence<'a> {
    /// The daemon refused the open because the module is unavailable.
    Unavailable(&'a RejectedMessage),
    /// A scope refusal: about the caller's scope, not the module.
    ScopeOnly,
    /// A route, or a definite answer from the daemon that is not about availability.
    Answered,
    /// A transport or protocol failure, including broca's own attempt timeout (which surfaces
    /// as a transport error): it does not show whether the module is up.
    Inconclusive,
}

fn module_evidence(outcome: &ConnResult<RouteHandle>) -> ModuleEvidence<'_> {
    match outcome {
        Ok(_) => ModuleEvidence::Answered,
        Err(ConnError::Rejected(message)) if is_module_availability_refusal(message.code()) => {
            ModuleEvidence::Unavailable(message)
        }
        Err(ConnError::Rejected(message)) if message.code().starts_with("scope_") => {
            ModuleEvidence::ScopeOnly
        }
        Err(ConnError::Rejected(_)) => ModuleEvidence::Answered,
        Err(_) => ModuleEvidence::Inconclusive,
    }
}

/// One target module's availability for one kind of route ([`GateTarget`]), shared by every
/// route open to that target on one connection generation, whatever session or scope each
/// open is for.
///
/// Without it, every open retries on its own schedule, so many routes to one stopped module
/// add up to a storm: in one measured case about 200 refused opens a second from broca, spread
/// across 52 worker scopes, each a different route with its own retry loop. So once an open to
/// the module is refused for availability ([`is_module_availability_refusal`]), the gate
/// CLOSES: every open to the module waits here, and one probe per backoff tick (an envelope
/// from [`ROUTE_OPEN_RETRY_INTERVAL`] doubling to [`ROUTE_OPEN_RETRY_MAX_INTERVAL`], with full
/// jitter) is let through, as an ordinary open by one of the waiting callers. The gate OPENS
/// again only on evidence that the module is up: a route, or a definite answer from the daemon
/// that is not an availability refusal. Each waiting caller then makes its own open, one
/// bounded burst. A transport or protocol failure (including broca's own attempt timeout) says
/// nothing either way, and a scope refusal (`scope_*`) is about one caller's scope, so neither
/// opens or closes the gate. A waiting caller still leaves at its own deadline or cancellation.
///
/// Every close and every reopen starts a new period (`epoch`), and every probe turn has its
/// own number. So an availability refusal from an open admitted in an earlier period cannot
/// close a reopened gate, and a probe that fails or is abandoned changes the pacing only while
/// its own turn is still the one running. (A route or a definite answer reopens the gate
/// whatever period it came from: either one shows that the module answered.)
pub(crate) struct ModuleGate {
    state: tokio::sync::watch::Sender<GateState>,
    jitter: Box<dyn Fn(Duration) -> Duration + Send + Sync>,
    /// Fires when the connection generation this gate belongs to ends. Its waiters are then
    /// released at once (see [`GateAdmission::Retired`]) instead of sleeping on a gate nothing
    /// will probe any more.
    retired: CancellationToken,
}

struct GateState {
    /// Bumped on every close and every reopen.
    epoch: u64,
    /// `None` while the module is presumed available.
    closed: Option<GateClosed>,
    /// How many probe turns have been handed out; the next turn's number.
    probes_issued: u64,
}

struct GateClosed {
    /// The availability refusal that closed the gate, or the latest one since. A caller that
    /// cannot wait for the next probe is given this: the gate's latest evidence that the module
    /// was unavailable.
    refusal: RejectedMessage,
    /// When the next probe may go through.
    next_probe_at: time::Instant,
    /// The envelope the wait after the next failed probe is drawn from.
    envelope: Duration,
    /// The number of the probe turn running now, if one is.
    probe: Option<u64>,
}

impl GateClosed {
    /// The running probe learned nothing that opens the gate: schedule the next one, further
    /// out.
    fn probe_failed(&mut self, now: time::Instant, jitter: impl Fn(Duration) -> Duration) {
        self.probe = None;
        self.next_probe_at = now + jitter(self.envelope);
        self.envelope = (self.envelope * 2).min(ROUTE_OPEN_RETRY_MAX_INTERVAL);
    }
}

pub(crate) enum GateAdmission {
    /// Make the open, and report its outcome on the ticket.
    Admitted(GateTicket),
    /// The caller cannot wait for a probe (its deadline has passed, or it asked not to wait):
    /// here is the gate's latest availability refusal, to be treated as this open's outcome.
    Refused(RejectedMessage),
    Cancelled,
    /// The connection generation this gate belongs to has ended, so it paces nothing any
    /// more: look up the module's gate on the current generation and pass that one instead.
    Retired,
}

/// Permission to make one open through a [`ModuleGate`], tied to the gate period it was given
/// in and, for a probe, to its own probe turn. A probe ticket dropped without a report (the
/// open was abandoned) hands its turn on to the next waiting caller.
pub(crate) struct GateTicket {
    gate: Arc<ModuleGate>,
    epoch: u64,
    probe: Option<u64>,
    reported: bool,
}

impl GateTicket {
    pub(crate) fn report(mut self, outcome: &ConnResult<RouteHandle>) {
        self.reported = true;
        self.gate.observe(outcome, self.epoch, self.probe);
    }
}

impl Drop for GateTicket {
    fn drop(&mut self) {
        let Some(probe) = self.probe.filter(|_| !self.reported) else {
            return;
        };
        self.gate
            .state
            .send_if_modified(|state| match state.closed.as_mut() {
                Some(closed) if closed.probe == Some(probe) => {
                    closed.probe = None;
                    true
                }
                _ => false,
            });
    }
}

impl ModuleGate {
    pub(crate) fn new(retired: CancellationToken) -> Self {
        Self::with_jitter(full_jitter, retired)
    }

    /// Draw each probe wait with `jitter` instead of [`full_jitter`], so a test can make the
    /// probe schedule exact.
    pub(crate) fn with_jitter(
        jitter: impl Fn(Duration) -> Duration + Send + Sync + 'static,
        retired: CancellationToken,
    ) -> Self {
        let (state, _) = tokio::sync::watch::channel(GateState {
            epoch: 0,
            closed: None,
            probes_issued: 0,
        });
        Self {
            state,
            jitter: Box::new(jitter),
            retired,
        }
    }

    /// Whether the gate is closed and still pacing opens (a retired gate paces nothing).
    pub(crate) fn is_closed(&self) -> bool {
        !self.is_retired() && self.state.borrow().closed.is_some()
    }

    /// Whether the connection generation this gate belongs to has ended.
    pub(crate) fn is_retired(&self) -> bool {
        self.retired.is_cancelled()
    }

    /// Wait for permission to make one open to this module. Open gate: at once. Closed gate:
    /// when this caller gets the next probe turn, when the gate opens, or, at `wait_until`
    /// (`None` meaning do not wait at all), with the module's latest refusal instead. A gate
    /// whose generation has ended answers [`GateAdmission::Retired`] at once, even mid-wait.
    pub(crate) async fn admit(
        self: &Arc<Self>,
        wait_until: Option<time::Instant>,
        cancel: Option<&CancellationToken>,
    ) -> GateAdmission {
        let mut rx = self.state.subscribe();
        // The refusal this caller was told while it waited, once it has waited at all. A caller
        // that has waited must not be admitted after its deadline, even when the gate opens or
        // a probe turn comes up at the same moment the deadline does: that would start an
        // attempt its budget no longer covers. A caller that has not waited (a zero budget
        // included) still gets its first attempt.
        let mut waited: Option<RejectedMessage> = None;
        loop {
            enum Step {
                Go(u64),
                TryProbe,
                /// Wait for a change, or until the given probe time when no probe is running.
                Wait(Option<time::Instant>, RejectedMessage),
            }
            if let (Some(refusal), Some(until)) = (&waited, wait_until) {
                if time::Instant::now() >= until {
                    return GateAdmission::Refused(refusal.clone());
                }
            }
            if self.is_retired() {
                return GateAdmission::Retired;
            }
            let now = time::Instant::now();
            let step = {
                let state = rx.borrow_and_update();
                match &state.closed {
                    None => Step::Go(state.epoch),
                    Some(closed) if closed.probe.is_none() && now >= closed.next_probe_at => {
                        Step::TryProbe
                    }
                    Some(closed) => Step::Wait(
                        closed.probe.is_none().then_some(closed.next_probe_at),
                        closed.refusal.clone(),
                    ),
                }
            };
            match step {
                Step::Go(epoch) => return GateAdmission::Admitted(self.ticket(epoch, None)),
                Step::TryProbe => {
                    let mut claimed = None;
                    self.state
                        .send_if_modified(|state| match state.closed.as_mut() {
                            Some(closed)
                                if closed.probe.is_none() && now >= closed.next_probe_at =>
                            {
                                let turn = state.probes_issued;
                                state.probes_issued += 1;
                                closed.probe = Some(turn);
                                claimed = Some((state.epoch, turn));
                                true
                            }
                            _ => false,
                        });
                    if let Some((epoch, turn)) = claimed {
                        return GateAdmission::Admitted(self.ticket(epoch, Some(turn)));
                    }
                }
                Step::Wait(next_probe_at, refusal) => {
                    let until = match wait_until {
                        Some(until) if until > now => until,
                        _ => return GateAdmission::Refused(refusal),
                    };
                    let probe_turn = async {
                        match next_probe_at {
                            Some(at) => time::sleep_until(at).await,
                            None => std::future::pending().await,
                        }
                    };
                    waited = Some(refusal);
                    tokio::select! {
                        _ = cancelled_or_never(cancel) => return GateAdmission::Cancelled,
                        _ = rx.changed() => {}
                        _ = probe_turn => {}
                        _ = self.retired.cancelled() => {}
                        _ = time::sleep_until(until) => {}
                    }
                }
            }
        }
    }

    fn ticket(self: &Arc<Self>, epoch: u64, probe: Option<u64>) -> GateTicket {
        GateTicket {
            gate: Arc::clone(self),
            epoch,
            probe,
            reported: false,
        }
    }

    /// Learn from one open's outcome. `epoch` is the gate period the open was admitted in, and
    /// `probe` its probe turn if it was one.
    fn observe(&self, outcome: &ConnResult<RouteHandle>, epoch: u64, probe: Option<u64>) {
        let now = time::Instant::now();
        let jitter = |envelope: Duration| (self.jitter)(envelope).min(envelope);
        let evidence = module_evidence(outcome);
        self.state.send_if_modified(|state| {
            let Some(closed) = state.closed.as_mut() else {
                // Open gate: only an availability refusal from an open admitted in THIS open
                // period closes it. One admitted before the last reopen is older news than the
                // evidence that reopened it.
                let ModuleEvidence::Unavailable(refusal) = evidence else {
                    return false;
                };
                if epoch != state.epoch {
                    return false;
                }
                state.epoch += 1;
                state.closed = Some(GateClosed {
                    refusal: refusal.clone(),
                    next_probe_at: now + jitter(ROUTE_OPEN_RETRY_INTERVAL),
                    envelope: (ROUTE_OPEN_RETRY_INTERVAL * 2).min(ROUTE_OPEN_RETRY_MAX_INTERVAL),
                    probe: None,
                });
                return true;
            };
            let own_turn = probe.is_some() && closed.probe == probe;
            match evidence {
                ModuleEvidence::Answered => {
                    state.epoch += 1;
                    state.closed = None;
                }
                ModuleEvidence::Unavailable(refusal) => {
                    closed.refusal = refusal.clone();
                    if own_turn {
                        closed.probe_failed(now, jitter);
                    }
                }
                ModuleEvidence::Inconclusive if own_turn => closed.probe_failed(now, jitter),
                // Not about the module: the probe gives up its turn until the next tick.
                ModuleEvidence::ScopeOnly if own_turn => {
                    closed.probe = None;
                    closed.next_probe_at = now + jitter(closed.envelope);
                }
                ModuleEvidence::Inconclusive | ModuleEvidence::ScopeOnly => return false,
            }
            true
        });
    }
}

/// What one [`ModuleGate`] paces: one route target of one module, as the daemon tells targets
/// apart when it checks that the module provides what a `route.open` asks for (the target kind,
/// and for an internal service its service id; role versions are not part of that check).
///
/// Not the module alone, because not every availability refusal is about the whole module. The
/// daemon answers `target_unavailable` (its reason `role_not_provided`) for a target the module
/// does not provide, such as an internal service id it never registered, while the module is
/// live and serving its other routes. Keyed by module alone, that refusal would close the gate
/// on every other route to the module. Keyed by target, a module that is down still paces all
/// its routes of one kind together: every tool route to a stopped module, whatever session or
/// scope it is for, shares one gate, which is where the storms were.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum GateTarget {
    ToolProvider(String),
    ManagementSurface(String),
    InternalService {
        module_id: String,
        service_id: String,
    },
}

impl GateTarget {
    pub(crate) fn tool_provider(module_id: &str) -> Self {
        Self::ToolProvider(module_id.to_owned())
    }

    pub(crate) fn management_surface(module_id: &str) -> Self {
        Self::ManagementSurface(module_id.to_owned())
    }
}

/// The availability gates of one connection, one per route target and socket generation.
#[derive(Default)]
struct ModuleGates {
    gates: Mutex<HashMap<(u64, GateTarget), Arc<ModuleGate>>>,
}

impl ModuleGates {
    /// `target`'s gate on the connection's current generation.
    fn get(&self, shared: &ConnectionShared, target: &GateTarget) -> Arc<ModuleGate> {
        let mut gates = self.gates.lock().unwrap();
        // Read the generation while holding the gates lock: a caller holding an older reading
        // could otherwise evict the current generation's gates below.
        let (generation, retired) = {
            let state = shared.current.lock().unwrap();
            (state.runtime.conn.0, state.runtime.shutdown.clone())
        };
        // Earlier generations' gates are let go here; their waiters were already released when
        // that generation's shutdown token fired.
        gates.retain(|(gate_generation, _), _| *gate_generation == generation);
        Arc::clone(
            gates
                .entry((generation, target.clone()))
                .or_insert_with(|| Arc::new(ModuleGate::new(retired))),
        )
    }
}

/// Whether a daemon rejection names the router's freed-route code. Parsed `ErrorBody` refusals
/// compare their structured code; only unstructured legacy text falls back to the old wire format.
pub fn rejection_is_unknown_channel(rejection: &RejectedMessage) -> bool {
    if rejection
        .code
        .as_deref()
        .is_some_and(|code| code == "unknown_channel")
    {
        true
    } else if rejection.code.is_none() {
        rejection_code(&rejection.text) == "unknown_channel"
    } else {
        false
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ToolRouteRefusal {
    Reopen,
    OutcomeUnknown,
    Other,
}

pub(crate) fn classify_tool_route_refusal(rejection: &RejectedMessage) -> ToolRouteRefusal {
    // Retrying provider route_not_bound and route_reclaimed refusals is safe only because
    // the provider guarantees they happen before dispatch. An in-flight reclaimed call
    // may have applied side effects and must never be re-sent; unfamiliar codes stay errors.
    match rejection.code() {
        "unknown_channel" | "route_not_bound" | "route_reclaimed" => ToolRouteRefusal::Reopen,
        "outcome_unknown_root_reclaimed" => ToolRouteRefusal::OutcomeUnknown,
        _ => ToolRouteRefusal::Other,
    }
}

/// Whether a route failure proves the daemon has already released that route generation.
/// Callers may discard these handles; other failures can leave a live daemon route that must
/// receive a best-effort GOODBYE before its local handle is forgotten.
pub(crate) fn route_failure_is_known_dead(error: &ConnError) -> bool {
    match error {
        ConnError::Rejected(message) => rejection_is_unknown_channel(message),
        ConnError::Transport(message) => message == ROUTE_GONE_MESSAGE,
        ConnError::StaleRouteHandle(_) => true,
        ConnError::Protocol(_) | ConnError::StreamLagged { .. } => false,
    }
}

/// THE single broca→subc identity conversion at the adapter boundary (guardrail:
/// one place so it can't drift). `project_root` passes through RAW — subc
/// canonicalizes it via cortexkit-paths at route.open, so the runner must NOT
/// pre-canonicalize (that would risk a double/mismatched canonicalization).
/// The daemon-spawned consumer identity for route.open: the module id the daemon sets on every
/// module it launches (`SUBC_MODULE_ID`) and the launch nonce it hands over with it.
/// When both are present the route is attributed to the module as a Reserved principal
/// (e.g. `reserved:broca`) instead of the anonymous Direct principal — the honest
/// stamp for a supervised process, and what lets a target module's policy distinguish
/// the runner from an arbitrary connection-key holder. Outside daemon supervision
/// (tests, ad-hoc CLI runs) neither is present and the route stays Direct, which every
/// current policy treats identically.
///
/// THE NONCE COMES FROM `subc_os::launch_nonce()`, never from the environment directly. The
/// daemon hands it over as an inherited pipe that can be read only once, and the module's
/// `main` has already read it; the accessor returns that cached value for every connection the
/// process makes, for its whole life. A second, independent read would find the descriptor
/// closed, or worse, find some unrelated socket reusing its number.
///
/// When the accessor reports an error (the variable naming the descriptor was inherited without
/// the pipe, as in a process a module spawned), the route presents no identity and stays Direct.
/// It never falls back to the environment copy of the nonce, which would let that process act
/// as the module.
pub fn supervised_consumer_identity() -> Option<subc_control::ConsumerIdentity> {
    let module_id = std::env::var(subc_protocol::SUBC_MODULE_ID_ENV)
        .ok()
        .filter(|v| !v.is_empty())?;
    let launch_nonce = match subc_os::launch_nonce() {
        Ok(nonce) => nonce?.value().to_string(),
        Err(error) => {
            // Warn, not debug: a process with a module id but no readable nonce is abnormal
            // (broca's own main refuses to start in that state), and it loses its identity.
            tracing::warn!(
                "route opens present no consumer identity: launch nonce unavailable: {error}"
            );
            return None;
        }
    };
    Some(subc_control::ConsumerIdentity {
        module_id,
        launch_nonce,
    })
}

/// The harness name broca presents on tool-provider routes.
///
/// Tool modules accept only a fixed set of harness values at bind (opencode, pi, runner, mcp:*,
/// fed:*) and reject anything else, so every `RouteTarget::ToolProvider` bind that does not
/// declare the compaction-provider role presents `runner`.
///
/// Broca's own `BindIdentity.harness` cannot simply be that value: it is an opaque key component,
/// joined into WAL, lease, store and idempotency keys, so it partitions durable state and broca
/// validates nothing about it beyond rejecting the key separator. Forwarding it verbatim would make
/// each caller's private key choice a bind-compatibility question with a module it never named.
///
/// Management and internal routes carry the caller's own harness instead (see `to_subc_bind`):
/// the daemon does not constrain the field there, claustrum discards the bind identity, and
/// magic-context keys nothing on it for `ck-import`'s management route.
///
/// Compaction-provider routes carry the caller's own harness too (see
/// [`declares_compaction_provider`]).
const TOOL_MODULE_HARNESS: &str = "runner";

/// The `role_versions` a caller declares on `route.open` to reach a module's `classifier/v1`
/// surface: role `classifier`, version `v1`. A module serving several roles on one management
/// surface tells them apart by this declaration, which the daemon forwards on the bind.
pub const CLASSIFIER_ROLE_VERSIONS: (&str, &str) = ("classifier", "v1");

/// [`CLASSIFIER_ROLE_VERSIONS`] as the map `route.open` carries.
pub fn classifier_role_versions() -> std::collections::BTreeMap<String, String> {
    std::collections::BTreeMap::from([(
        CLASSIFIER_ROLE_VERSIONS.0.to_owned(),
        CLASSIFIER_ROLE_VERSIONS.1.to_owned(),
    )])
}

/// Whether a route declares the compaction-provider role. Such a route carries the caller's own
/// bind identity (`project_root`, `harness` and `session`) rather than the tool-module `runner`
/// harness; its Setup body names the same harness and session.
///
/// A compaction provider calls back about the conversation (`session.read`, sends, reminders)
/// with the conversation's identity. When the route said `runner` while Setup said the real
/// harness, one session showed two identities on the wire, and a provider that took the route's
/// harness read a lineage nobody had ever written: an empty page with no lineage id. The daemon
/// treats the bind harness as a free string, and compaction providers recognise the runner by
/// its daemon-attested principal rather than the harness, so nothing requires the translation
/// on these routes.
fn declares_compaction_provider(
    role_versions: Option<&std::collections::BTreeMap<String, String>>,
) -> bool {
    role_versions.is_some_and(|roles| roles.contains_key(broca_core::compaction_provider::ROLE))
}

fn to_subc_bind(
    identity: &BindIdentity,
    target: &RouteTarget,
    role_versions: Option<&std::collections::BTreeMap<String, String>>,
) -> SubcBindIdentity {
    // EXHAUSTIVE ON PURPOSE. A new RouteTarget variant must be a compile error here rather
    // than defaulting either way: defaulting to translate silently widens a tool-module
    // workaround onto a surface that never asked for it (which is the defect this fixes), and
    // defaulting to pass-through silently hands a closed enum a value it rejects at bind.
    let harness = match target {
        // A compaction provider keys the conversation on the session's real identity, and its
        // Setup body reports that identity, so the route presents it unchanged.
        RouteTarget::ToolProvider { .. } if declares_compaction_provider(role_versions) => {
            identity.harness.as_str()
        }
        // Tool modules run a closed enum and reject anything else at bind.
        RouteTarget::ToolProvider { .. } => TOOL_MODULE_HARNESS,
        // Management and internal surfaces do not constrain the field -- subc-protocol carries
        // a free String -- so the caller's own value travels. Verified with both consumers
        // before scoping: claustrum's RouteBind destructures `{ route_channel, epoch,
        // principal, .. }` and discards the identity; magic-context stores it but reads it in
        // exactly two places, both on the MCP facade path that ck-import never invokes (mc,
        // 2026-09-20: "re-keys nothing on our side and is safe to scope").
        //
        // THIS ARM RE-KEYED BROCA'S OWN MANAGEMENT SESSIONS AND I DID NOT CHECK FOR IT.
        // I asked whether claustrum and mc key on harness and never asked whether BROCA does
        // -- and broca is the consumer that keys its WAL FILENAME on it, via
        // session_addr = FNV-1a(project_root U+241F harness U+241F session). Before this
        // change every management bind arrived as `runner`, so an unchanged `broca-session`
        // invocation now addresses a DIFFERENT lineage than it did on v0.3.105. The old data
        // is intact under the old address; it is simply no longer what that command names.
        //
        // Measured before deciding not to migrate: 79 distinct runner-keyed sessions, of which
        // 3 have ever carried more than one run and 42 are `acceptance-<epoch>-<pid>` probe
        // names that are unique per invocation by construction. The reopenable population is
        // operator probes, not conversations, so a migration would add a collision-checked
        // rewrite of live identity state to recover throwaway sessions. Recovery for a
        // specific one is `--harness runner`, which needs no code.
        //
        // The rule the omission broke: WHEN A CHANGE MOVES A KEY COMPONENT, ENUMERATE THE
        // CONSUMERS THAT KEY ON IT INCLUDING YOUR OWN. Checking the far side of a wire and
        // skipping the near side is the same blindness as a grep whose control passed for the
        // wrong subject.
        RouteTarget::ManagementSurface { .. } | RouteTarget::InternalService { .. } => {
            identity.harness.as_str()
        }
    };
    // TRANSLATE, never forward. Presenting the constant here rather than asking callers to
    // revert their spelling is the only fix that does not RE-KEY SESSIONS: harness is part
    // of broca's durable key, so changing an inbound bind strands that lineage under a new
    // WAL name with nothing recording the move. Measured when this was chosen: 395 live
    // sessions would have been stranded by the caller-side revert.
    //
    // `project_id` is None here BY CONSTRUCTION, not by default. subc's contract on the field
    // is that a producer sends it on every bind of a session or on none, because alternating
    // silently forks the consumer's lineage. Broca's own `BindIdentity` does not yet carry the
    // inbound id, so nothing is persisted with the session to forward consistently -- and a
    // value that is only sometimes available must be sent never. When the inbound id is
    // carried and persisted (the session-identity re-key), this becomes "forward what was
    // persisted with the session, else None"; it must not become "forward whatever arrived".
    SubcBindIdentity::new(
        identity.project_root.clone(),
        harness,
        identity.session.clone(),
    )
}

/// The far end of an in-process connection with no socket, for tests OUTSIDE this module that
/// need the real request/terminal path (framing, pending waiters, reply deadlines) behind a
/// fake module. The test reads what the connection wrote and answers it through the same
/// waiter the reader task would have fed.
#[cfg(test)]
pub(crate) struct InProcessModuleEnd {
    write_rx: mpsc::Receiver<Frame>,
    pending: PendingMap,
}

#[cfg(test)]
impl SubcConnection {
    /// A connection whose only live route is `(channel, epoch)`, plus the module end of it.
    pub(crate) fn in_process_for_test(
        channel: u16,
        epoch: u32,
    ) -> (SubcConnection, RouteHandle, InProcessModuleEnd) {
        Self::in_process_with_capacity_for_test(channel, epoch, 16)
    }

    pub(crate) fn in_process_full_writer_for_test(
        channel: u16,
        epoch: u32,
    ) -> (SubcConnection, RouteHandle, InProcessModuleEnd) {
        let (conn, route, module) = Self::in_process_with_capacity_for_test(channel, epoch, 256);
        let runtime = conn.snapshot_route(&route).unwrap();
        for _ in 0..256 {
            runtime
                .write_tx
                .try_send(Frame::build(FrameType::Ping, control_flags(), 0, 0, 0, vec![]).unwrap())
                .unwrap();
        }
        (conn, route, module)
    }

    fn in_process_with_capacity_for_test(
        channel: u16,
        epoch: u32,
        capacity: usize,
    ) -> (SubcConnection, RouteHandle, InProcessModuleEnd) {
        let (write_tx, write_rx) = mpsc::channel(capacity);
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let conn_token = ConnToken(NEXT_CONNECTION_TOKEN.fetch_add(1, Ordering::Relaxed));
        let runtime = Arc::new(ConnectionRuntime {
            write_tx,
            pending: Arc::clone(&pending),
            route_epochs: Arc::new(Mutex::new(HashMap::from([(channel, epoch)]))),
            write_confirmations: Arc::new(Mutex::new(HashMap::new())),
            conn: conn_token,
            shutdown: CancellationToken::new(),
            capabilities: tokio::sync::OnceCell::new(),
        });
        let (reconnect_tx, _reconnect_rx) = mpsc::unbounded_channel();
        let conn = SubcConnection {
            shared: Arc::new(ConnectionShared {
                connection_file_path: PathBuf::new(),
                current: Mutex::new(GenerationState::new(runtime)),
                corr: AtomicU64::new(1),
                reconnect_tx,
                probe_timeout: LIVENESS_PROBE_TIMEOUT,
                route_open_bind_relay_grace: ROUTE_OPEN_BIND_RELAY_GRACE,
                consumer_identity: None,
                reconnect_attempts: AtomicU64::new(0),
                probe_windows_started: AtomicU64::new(0),
            }),
            module_gates: ModuleGates::default(),
        };
        (
            conn,
            RouteHandle::new(channel, epoch, conn_token),
            InProcessModuleEnd { write_rx, pending },
        )
    }
}

#[cfg(test)]
impl InProcessModuleEnd {
    pub(crate) fn fill_pending_for_test(&self) {
        for corr in 0..MAX_PENDING_REQUESTS as u64 {
            let (sender, _rx) = mpsc::unbounded_channel();
            self.pending.lock().unwrap().insert(
                (9, 1, corr),
                PendingEntry::Live {
                    sender: PendingSender::Unary(sender),
                    route_open: false,
                },
            );
        }
    }

    pub(crate) fn close_writer_for_test(&mut self) {
        self.write_rx.close();
    }

    pub(crate) fn pending_count_for_test(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    pub(crate) fn take_written_for_test(&mut self) -> Vec<Frame> {
        let mut frames = Vec::new();
        while let Ok(frame) = self.write_rx.try_recv() {
            frames.push(frame);
        }
        frames
    }

    /// The next data-plane Request the connection sent. Other frames (a liveness Ping after a
    /// timeout, a route Goodbye) are skipped.
    pub(crate) async fn next_request(&mut self) -> Frame {
        loop {
            let frame = self
                .write_rx
                .recv()
                .await
                .expect("the connection keeps its writer open");
            if frame.header.ty == FrameType::Request {
                return frame;
            }
        }
    }

    /// Answer `request` with a Response carrying `body`. False when nobody is waiting any more
    /// (the caller already gave up), which is how a late reply looks on a real socket.
    pub(crate) fn reply(&self, request: &Frame, body: Vec<u8>) -> bool {
        self.terminal(request, FrameType::Response, body)
    }

    pub(crate) fn refuse(&self, request: &Frame, code: &str) -> bool {
        self.terminal(
            request,
            FrameType::Error,
            serde_json::to_vec(&serde_json::json!({"code": code, "message": "scripted refusal"}))
                .unwrap(),
        )
    }

    fn terminal(&self, request: &Frame, ty: FrameType, body: Vec<u8>) -> bool {
        let key = (
            request.header.channel,
            request.header.epoch,
            request.header.corr,
        );
        let sender = match self.pending.lock().unwrap().get(&key) {
            Some(PendingEntry::Live { sender, .. }) => sender.clone(),
            _ => return false,
        };
        let response = Frame::build(
            ty,
            data_flags(),
            request.header.channel,
            request.header.epoch,
            request.header.corr,
            body,
        )
        .expect("a test response frame builds");
        sender.send(response).is_ok()
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn dispatch_capabilities_are_installed_together_and_role_routes_stay_undeclared() {
        for installed in [false, true] {
            let (conn, mut write_rx, pending) = route_open_test_connection();
            let identity = broca_protocol::BindIdentity {
                project_root: "/project".into(),
                harness: "broca".into(),
                session: "session".into(),
            };
            let open =
                conn.route_open_dispatch_single_attempt("provider", &identity, None, installed);
            tokio::pin!(open);
            let request = tokio::select! {
                result = &mut open => panic!("premature result: {result:?}"),
                request = write_rx.recv() => request.unwrap(),
            };
            let raw: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            if installed {
                assert_eq!(
                    raw["consumer_capabilities"],
                    serde_json::json!(["content/v1", "held-reply/v1"])
                );
            } else {
                assert_eq!(
                    request.body,
                    include_str!("../tests/fixtures/held_dispatch_route_legacy.json")
                        .trim_end()
                        .as_bytes()
                );
            }
            answer_route_open(&pending, &request, 7);
            assert_eq!(open.await.unwrap().reads_held_reply, installed);
        }
        let (conn, mut write_rx, pending) = route_open_test_connection();
        let identity = broca_protocol::BindIdentity {
            project_root: "/project".into(),
            harness: "broca".into(),
            session: "session".into(),
        };
        let open = conn.route_open_tool_provider_v1("provider", &identity, None);
        tokio::pin!(open);
        let request = tokio::select! {
            result = &mut open => panic!("premature role result: {result:?}"),
            request = write_rx.recv() => request.unwrap(),
        };
        let raw: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert!(raw.get("consumer_capabilities").is_none());
        answer_route_open(&pending, &request, 8);
        assert!(!open.await.unwrap().reads_held_reply);
    }
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use tokio::net::TcpStream;

    async fn read_capabilities_concurrently(
        conn: &SubcConnection,
        module: &mut InProcessModuleEnd,
        capabilities: &[&str],
    ) {
        tokio::time::timeout(Duration::from_secs(2), async {
            let reads = async { tokio::join!(conn.server_capabilities(), conn.server_capabilities(), conn.server_capabilities()) };
            tokio::pin!(reads);
            let mut requests = 0;
            let results = loop {
                tokio::select! {
                    results = &mut reads => break results,
                    request = module.next_request() => {
                        requests += 1;
                        let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                        assert_eq!(body["op"], "server.describe");
                        let reply = ClientControlResponse::ServerDescribe {
                            protocol_ver: 1, subc_ops: vec![],
                            capabilities: capabilities.iter().map(|item| (*item).into()).collect(),
                            connected_clients: 1, counters: None,
                            build_git_sha: None, build_lock_digest: None,
                            capability_requirements: vec![], machine_id: None, config_path: None,
                        };
                        assert!(module.reply(&request, serde_json::to_vec(&reply).unwrap()));
                    }
                }
            };
            let expected: Vec<String> = capabilities.iter().map(|item| (*item).into()).collect();
            assert_eq!(results.0.unwrap(), expected);
            assert_eq!(results.1.unwrap(), expected);
            assert_eq!(results.2.unwrap(), expected);
            assert_eq!(requests, 1, "capability discovery must be single-flight");
            assert_eq!(conn.server_capabilities().await.unwrap(), expected);
            assert!(module.write_rx.try_recv().is_err(), "cached reads must not send RPCs");
        }).await.expect("capability readers must terminate");
    }

    #[tokio::test]
    async fn capabilities_are_single_flight_and_reset_on_reconnect() {
        let (conn, _, mut module) = SubcConnection::in_process_for_test(1, 1);
        read_capabilities_concurrently(&conn, &mut module, &["scopes/v1"]).await;
        let (replacement, write_rx) = new_runtime();
        let mut new_module = InProcessModuleEnd {
            write_rx,
            pending: replacement.pending.clone(),
        };
        *conn.shared.current.lock().unwrap() = GenerationState::new(replacement);
        read_capabilities_concurrently(&conn, &mut new_module, &[]).await;
    }

    #[tokio::test]
    async fn failed_capability_discovery_is_not_cached() {
        let (conn, _, mut module) = SubcConnection::in_process_for_test(1, 1);
        let (failed, ()) = tokio::join!(conn.server_capabilities(), async {
            let request = module.next_request().await;
            assert!(module.reply(&request, b"invalid describe".to_vec()));
        });
        assert!(matches!(failed, Err(ConnError::Protocol(_))));
        read_capabilities_concurrently(&conn, &mut module, &["scopes/v1"]).await;
    }

    /// The behavioural grace tests inject their own window to compress timeouts, so they prove
    /// the mechanism and are blind to the shipped value.
    ///
    /// The relations, not the literals, are what matter. The grace must cover the daemon's
    /// relay window, so a caller that abandons a route.open early cannot get a healthy socket
    /// convicted. And one route.open attempt must wait STRICTLY LONGER than that window, so a
    /// bind the daemon completes near its bound is reported as the success it is. The second
    /// relation used to be the opposite (the attempt waited only the 10s RESPONSE_TIMEOUT), which
    /// turned an 11s bind into a failed open.
    #[test]
    fn bind_relay_grace_covers_the_daemon_relay_window() {
        const DAEMON_BIND_RELAY_WINDOW: Duration = Duration::from_secs(12);

        assert!(
            ROUTE_OPEN_BIND_RELAY_GRACE >= DAEMON_BIND_RELAY_WINDOW,
            "grace {ROUTE_OPEN_BIND_RELAY_GRACE:?} is shorter than the daemon's relay window \
             {DAEMON_BIND_RELAY_WINDOW:?}; a route.open abandoned while the daemon is still \
             relaying would convict a healthy socket"
        );
        assert!(
            ROUTE_OPEN_ATTEMPT_TIMEOUT > DAEMON_BIND_RELAY_WINDOW,
            "a route.open attempt waits {ROUTE_OPEN_ATTEMPT_TIMEOUT:?}, not longer than the \
             daemon's relay window {DAEMON_BIND_RELAY_WINDOW:?}; a bind that completes inside \
             the window would be reported as a failed open"
        );
    }

    /// A connection with no socket whose route.open replies the test delivers by hand, at a
    /// time it chooses. Returns the connection, the queue of frames it wrote, and its pending
    /// map (where a reply is delivered).
    fn route_open_test_connection() -> (SubcConnection, mpsc::Receiver<Frame>, PendingMap) {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
        let (write_tx, write_rx) = mpsc::channel(64);
        let mut conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
        // The shipped grace, not the compressed one other tests use: these tests are about the
        // production clocks.
        Arc::get_mut(&mut conn.shared)
            .expect("the connection is not shared yet")
            .route_open_bind_relay_grace = ROUTE_OPEN_BIND_RELAY_GRACE;
        (conn, write_rx, pending)
    }

    /// Answer a route.open `request` the way the daemon does once its bind relay completes.
    /// Panics when nobody is waiting any more, which is how an abandoned open looks.
    fn answer_route_open(pending: &PendingMap, request: &Frame, channel: u16) {
        // Read the entry and RELEASE the lock before any panic below: panicking while holding it
        // poisons the map, and the connection's own drop then panics too and aborts the whole
        // test binary instead of failing this one test.
        let entry = match pending.lock().unwrap().get(&(0, 0, request.header.corr)) {
            Some(PendingEntry::Live { sender, .. }) => Ok(sender.clone()),
            Some(PendingEntry::LateRouteOpen { .. }) => Err(
                "the caller had already given up on this route.open; the daemon's answer would \
                 only be cleaned up",
            ),
            None => Err("no route.open is waiting for this request"),
        };
        let sender = entry.unwrap_or_else(|reason| panic!("{reason}"));
        let body = serde_json::to_vec(&ClientControlResponse::RouteOpen {
            route_channel: channel,
            route_epoch: 1,
        })
        .unwrap();
        let response = Frame::build(
            FrameType::Response,
            control_flags(),
            0,
            0,
            request.header.corr,
            body,
        )
        .unwrap();
        sender.send(response).unwrap();
    }

    async fn next_route_open_request(write_rx: &mut mpsc::Receiver<Frame>) -> Frame {
        loop {
            let frame = write_rx.recv().await.expect("the writer queue stays open");
            if frame.header.channel == 0 && frame.header.ty == FrameType::Request {
                return frame;
            }
        }
    }

    /// A BIND THE DAEMON COMPLETES AT 11s IS A SUCCESSFUL OPEN, on both route targets. The
    /// daemon relays route.open to the module for up to 12s, so 11s is inside its contract; the
    /// 10s control-RPC deadline used to report this open as a failure while the daemon committed
    /// the route anyway. Paused time: the eleven seconds are virtual.
    #[tokio::test(start_paused = true)]
    async fn a_route_open_whose_bind_answers_at_eleven_seconds_succeeds() {
        const BIND_ANSWERS_AFTER: Duration = Duration::from_secs(11);
        let identity = BindIdentity::new("/tmp/slow-bind", "runner", "session");

        for management in [false, true] {
            let (conn, mut write_rx, pending) = route_open_test_connection();
            let conn = Arc::new(conn);
            let open = {
                let conn = Arc::clone(&conn);
                let identity = identity.clone();
                tokio::spawn(async move {
                    if management {
                        conn.route_open_management("credentials", &identity).await
                    } else {
                        conn.route_open("aft", &identity).await
                    }
                })
            };
            let request = next_route_open_request(&mut write_rx).await;
            time::sleep(BIND_ANSWERS_AFTER).await;
            answer_route_open(&pending, &request, 9);

            let handle = open.await.unwrap().unwrap_or_else(|error| {
                panic!("an 11s bind must open (management={management}): {error}")
            });
            assert_eq!((handle.channel, handle.epoch), (9, 1));
        }
    }

    /// A route.open the daemon never answers fails AT the per-attempt deadline, not before it
    /// and not later, and the error names that deadline.
    #[tokio::test(start_paused = true)]
    async fn a_route_open_that_never_answers_fails_at_the_attempt_deadline() {
        let (conn, mut write_rx, _pending) = route_open_test_connection();
        let identity = BindIdentity::new("/tmp/silent-bind", "runner", "session");
        let started = time::Instant::now();
        let open = tokio::spawn(async move {
            let result = conn.route_open("aft", &identity).await;
            (result, started.elapsed())
        });
        let _request = next_route_open_request(&mut write_rx).await;

        let (result, elapsed) = open.await.unwrap();
        let message = match result {
            Err(ConnError::Transport(message)) => message,
            other => panic!("a silent daemon must fail as a transport timeout, got {other:?}"),
        };
        assert!(
            message.contains("ROUTE_OPEN_ATTEMPT_TIMEOUT")
                && message.contains(&format!("{ROUTE_OPEN_ATTEMPT_TIMEOUT:?}")),
            "the error must name the route-open deadline: {message}"
        );
        assert!(
            elapsed >= ROUTE_OPEN_ATTEMPT_TIMEOUT
                && elapsed < ROUTE_OPEN_ATTEMPT_TIMEOUT + Duration::from_millis(100),
            "the open must fail at {ROUTE_OPEN_ATTEMPT_TIMEOUT:?}, failed after {elapsed:?}"
        );
    }

    fn target_unavailable() -> ConnResult<RouteHandle> {
        Err(ConnError::Rejected(RejectedMessage::structured(
            subc_protocol::error_codes::TARGET_UNAVAILABLE.into(),
            "supervised_not_registered".into(),
        )))
    }

    /// A refusal for one target of a module closes only that target's gate. The daemon answers
    /// `target_unavailable` for an internal service the module does not provide while the
    /// module goes on serving its tool and management routes, so those must stay open.
    #[tokio::test]
    async fn an_availability_refusal_for_one_target_leaves_the_modules_other_targets_open() {
        let (conn, _write_rx, _pending) = route_open_test_connection();
        let service = GateTarget::InternalService {
            module_id: "aft".into(),
            service_id: "not-registered".into(),
        };
        conn.module_gate(&service)
            .observe(&target_unavailable(), 0, None);
        assert!(conn.module_gate(&service).is_closed());
        assert!(!conn
            .module_gate(&GateTarget::tool_provider("aft"))
            .is_closed());
        assert!(!conn
            .module_gate(&GateTarget::management_surface("aft"))
            .is_closed());
    }

    /// A writer queue that never drains cannot hold a route.open attempt past its deadline. The
    /// attempt holding a module gate's only probe turn gives up within
    /// ROUTE_OPEN_ATTEMPT_TIMEOUT, having sent nothing and left no waiter behind, so the gate
    /// lets a later probe through, and that one opens once the writer drains again.
    #[tokio::test(start_paused = true)]
    async fn a_writer_queue_that_never_drains_releases_the_probe_turn_within_the_attempt_bound() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
        let (write_tx, mut write_rx) = mpsc::channel(2);
        // Fill the writer queue; nothing reads it until the first attempt has given up.
        for corr in 0..2 {
            write_tx
                .try_send(
                    Frame::build(
                        FrameType::Ping,
                        control_flags(),
                        0,
                        0,
                        1_000 + corr,
                        Vec::new(),
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
        let identity = BindIdentity::new("/tmp/stalled-writer", "runner", "session");
        // The module is known to be down: the next open through its gate is the probe.
        conn.module_gate(&GateTarget::tool_provider("aft"))
            .observe(&target_unavailable(), 0, None);
        let retry = RouteOpenRetry::within(Duration::from_secs(60), ROUTE_OPEN_RETRY_MAX_INTERVAL);
        let start = time::Instant::now();
        let probe = async {
            let result = conn.route_open_within("aft", &identity, retry).await;
            let failed_at = start.elapsed();
            assert!(
                pending.lock().unwrap().is_empty(),
                "an attempt that never queued its request leaves no waiter behind"
            );
            // The writer drains again from here on; answer the next probe's route.open.
            while write_rx.try_recv().is_ok() {}
            let request = next_route_open_request(&mut write_rx).await;
            answer_route_open(&pending, &request, 9);
            (result, failed_at)
        };
        let later = async {
            time::sleep(Duration::from_secs(1)).await;
            let result = conn.route_open_within("aft", &identity, retry).await;
            (result, start.elapsed())
        };
        // Bounded, so a regression that leaves the attempt stuck fails here instead of hanging.
        let ((first, failed_at), (later, opened_at)) =
            time::timeout(Duration::from_secs(120), async {
                tokio::join!(probe, later)
            })
            .await
            .expect("the stalled attempt must give up and a later probe must open");
        match first {
            Err(ConnError::Transport(message)) => {
                assert!(message.contains("nothing was sent"), "{message}")
            }
            other => panic!("a stalled writer must fail the attempt locally, got {other:?}"),
        }
        // The probe turn came up within the first 100 ms envelope, and the attempt then gave up
        // at its own deadline.
        assert!(
            failed_at >= ROUTE_OPEN_ATTEMPT_TIMEOUT
                && failed_at <= ROUTE_OPEN_ATTEMPT_TIMEOUT + ROUTE_OPEN_RETRY_INTERVAL,
            "the stalled attempt must give up within its bound, gave up after {failed_at:?}"
        );
        assert_eq!(later.expect("the later probe opens").channel, 9);
        // A failed probe schedules the next one within the following 200 ms envelope.
        assert!(
            opened_at >= failed_at && opened_at <= failed_at + 2 * ROUTE_OPEN_RETRY_INTERVAL,
            "the later probe goes at the next tick, went at {opened_at:?}"
        );
    }

    /// The connection generation ends while a caller waits at a module's closed gate, before
    /// any replacement generation exists: the caller is released at once and its open fails
    /// like any open made during a reconnect, instead of sleeping on a gate nothing will probe
    /// any more. This goes through the connection's own gate lookup and generation shutdown.
    #[tokio::test(start_paused = true)]
    async fn a_generation_ending_before_its_replacement_releases_gate_waiters_at_once() {
        let (conn, mut write_rx, _pending) = route_open_test_connection();
        let identity = BindIdentity::new("/tmp/ending-generation", "runner", "session");
        let gate = conn.module_gate(&GateTarget::tool_provider("aft"));
        gate.observe(&target_unavailable(), 0, None);
        time::sleep(ROUTE_OPEN_RETRY_INTERVAL).await;
        // Someone else's probe is running, so the waiter below has no turn coming.
        let probe = match gate.admit(None, None).await {
            GateAdmission::Admitted(ticket) => ticket,
            _ => panic!("the probe turn was due"),
        };
        let start = time::Instant::now();
        let waiter = async {
            let retry =
                RouteOpenRetry::within(Duration::from_secs(30), ROUTE_OPEN_RETRY_MAX_INTERVAL);
            let result = conn.route_open_within("aft", &identity, retry).await;
            (result, start.elapsed())
        };
        let end_generation = async {
            time::sleep(Duration::from_secs(1)).await;
            let mut state = conn.shared.current.lock().unwrap();
            assert!(deactivate_locked(&mut state));
        };
        let ((result, failed_at), ()) = tokio::join!(waiter, end_generation);
        match result {
            Err(ConnError::Transport(message)) => {
                assert!(message.contains("recovering"), "{message}")
            }
            other => panic!("the released waiter fails like any open mid-reconnect, got {other:?}"),
        }
        assert_eq!(
            failed_at,
            Duration::from_secs(1),
            "released the moment the generation ended"
        );
        assert!(write_rx.try_recv().is_err(), "nothing was sent");
        assert!(conn
            .module_gate(&GateTarget::tool_provider("aft"))
            .is_retired());
        drop(probe);
    }

    #[test]
    fn route_open_retryable_codes_match_only_the_four_temporary_refusals() {
        use subc_protocol::error_codes::{is_retryable_route_open, *};

        for code in [
            MODULE_RELOADING,
            MODULE_WARMING,
            TARGET_UNAVAILABLE,
            MODULE_TIMEOUT,
        ] {
            assert!(is_retryable_route_open(code), "{code} should retry");
        }
        // UNKNOWN_MODULE IS TERMINAL, by subc-protocol 0.24.0's decision. It used to retry;
        // upstream narrowed it to mean only "no module of this id is registered", with a
        // module that exists but is not yet up reported as module_warming or
        // target_unavailable instead. So a route.open racing a target module's restart still
        // retries -- through those codes -- while a genuine typo in a module id now fails in
        // one round trip rather than spinning for the full 3-second window first.
        //
        // This arm is here because broca does not own the predicate (it is imported, not
        // mirrored), so a change to it lands with no diff in this repository. Asserting the
        // contract we depend on is what turns the next upstream move into a red test rather
        // than a quiet change in how long a failed tool dispatch takes to report.
        assert!(
            !is_retryable_route_open(UNKNOWN_MODULE),
            "unknown_module is terminal"
        );
        assert!(!is_retryable_route_open(MODULE_REMOVED));
        assert!(!is_retryable_route_open("some_future_code"));

        // AN UNDECODABLE BODY MUST BE TERMINAL, and this is the arm a future "robustness"
        // edit would break. `decode_error_body` falls back to the raw bytes when the body is
        // not an ErrorBody, so there is no " — " to split and `rejection_code` returns the
        // whole string -- which matches no code, so the route open fails fast.
        //
        // Pinned because the tempting change is a fallback arm treating an unrecognised
        // rejection as retryable "in case it is transient". That turns a malformed reply from
        // a permanent refusal into a retry loop against a daemon that is answering correctly.
        for raw in [
            "internal error: connection reset",
            "",
            "unknown_channel: 10",
        ] {
            assert!(
                !is_retryable_route_open(rejection_code(raw)),
                "an undecodable rejection ({raw:?}) must be terminal, not retried"
            );
        }
    }

    #[test]
    fn route_rejection_code_extracts_the_prefix() {
        assert_eq!(
            rejection_code("module_warming — still starting"),
            "module_warming"
        );
        assert_eq!(rejection_code("module_warming"), "module_warming");
        assert_eq!(rejection_code(""), "");
    }

    #[test]
    fn unstructured_rejections_never_supply_a_refusal_code() {
        let message = decode_error_body("module_reloading — diagnostic text".as_bytes());
        assert_eq!(message.code(), "");
        assert_eq!(ConnError::Rejected(message).code(), None);
    }

    #[test]
    fn unknown_channel_rejection_uses_the_structured_code() {
        let decode = |code: &str, message: &str| {
            let body = serde_json::to_vec(&subc_protocol::ErrorBody::new(code, message)).unwrap();
            decode_error_body(&body)
        };
        let unknown = decode("unknown_channel", "unknown_channel — route gone");
        assert_eq!(unknown.code(), "unknown_channel");
        assert!(rejection_is_unknown_channel(&unknown));

        let misleading = decode("module_warming", "unknown_channel — route gone");
        assert_eq!(misleading.code(), "module_warming");
        assert!(!rejection_is_unknown_channel(&misleading));

        // Only a refusal without a structured ErrorBody code uses the legacy text fallback.
        let legacy: RejectedMessage = "unknown_channel — route gone".into();
        assert!(rejection_is_unknown_channel(&legacy));
        assert!(!rejection_is_unknown_channel(&"unknown_channel: 10".into()));
        assert!(!rejection_is_unknown_channel(
            &"unknown_channels — route gone".into()
        ));
    }

    use subc_transport::{
        authenticate_server,
        connection_file::{ConnectionInfo, Endpoint, SCHEMA_VERSION},
    };
    use tokio::{
        net::TcpListener,
        task::{JoinHandle, JoinSet},
    };

    /// Tool modules run a closed harness enum; broca's own harness is an opaque durable key
    /// component it validates nothing about. Forwarding it verbatim made a caller's private key
    /// choice break tool binds in a module they never named, and the runs went exploration-blind
    /// while broca reported nothing wrong.
    ///
    /// The alternative fix -- asking callers to change their bind -- would RE-KEY their sessions,
    /// because harness is joined into the WAL, lease and store keys. That is why the translation
    /// lives here and must not be "simplified" back to a clone.
    #[test]
    fn tool_module_bind_presents_the_contract_harness_not_the_callers_key_component() {
        for caller_harness in ["broca", "runner", "alfonso", "anything-a-caller-picks"] {
            let identity = BindIdentity {
                project_root: std::path::PathBuf::from("/tmp/p"),
                harness: caller_harness.to_string(),
                session: "s1".to_string(),
            };
            let outbound = to_subc_bind(
                &identity,
                &RouteTarget::ToolProvider {
                    module_id: "aft".to_string(),
                },
                None,
            );

            assert_eq!(
                outbound.harness, TOOL_MODULE_HARNESS,
                "a {caller_harness:?} bind must still attach as {TOOL_MODULE_HARNESS:?}"
            );
            // The other two fields are NOT translated: they carry the caller's real identity and
            // the tool module scopes its work by them.
            assert_eq!(outbound.session, identity.session);
            assert_eq!(outbound.project_root, identity.project_root);
        }
    }

    /// The translation above is a TOOL-MODULE workaround and was applied to every route target,
    /// so broca's own CLIs could not express their harness: `--harness broca` produced an export
    /// fact reading `runner` (measured 2026-09-20 on the deployed daemon).
    ///
    /// Both management consumers were checked before scoping. claustrum's `RouteBind`
    /// destructures `{ route_channel, epoch, principal, .. }` and discards the identity; mc
    /// stores it and reads it only on the MCP facade path, and answered "re-keys nothing on our
    /// side and is safe to scope".
    #[test]
    fn a_management_bind_carries_the_callers_own_harness_rather_than_the_tool_constant() {
        for caller_harness in ["broca", "ck-app", "alfonso", "anything-a-caller-picks"] {
            let identity = BindIdentity {
                project_root: std::path::PathBuf::from("/tmp/p"),
                harness: caller_harness.to_string(),
                session: "s1".to_string(),
            };

            for target in [
                RouteTarget::ManagementSurface {
                    module_id: "broca".to_string(),
                },
                RouteTarget::InternalService {
                    module_id: "m".to_string(),
                    service_id: "s".to_string(),
                },
            ] {
                let outbound = to_subc_bind(&identity, &target, None);
                assert_eq!(
                    outbound.harness, caller_harness,
                    "a management/internal bind must carry {caller_harness:?}, not the tool constant"
                );
            }
        }

        // NON-VACUITY: the two arms must actually differ, or this test passes under a build that
        // translates nothing and the tool test passes under one that translates everything only
        // because its callers happen to pass "runner".
        let identity = BindIdentity {
            project_root: std::path::PathBuf::from("/tmp/p"),
            harness: "ck-app".to_string(),
            session: "s1".to_string(),
        };
        let tool = to_subc_bind(
            &identity,
            &RouteTarget::ToolProvider {
                module_id: "aft".to_string(),
            },
            None,
        );
        let mgmt = to_subc_bind(
            &identity,
            &RouteTarget::ManagementSurface {
                module_id: "broca".to_string(),
            },
            None,
        );
        assert_ne!(
            tool.harness, mgmt.harness,
            "the whole point of the scoping is that these two disagree"
        );
    }

    /// A route that declares the compaction-provider role binds the session's real identity,
    /// the one its Setup body reports. A provider calls back about the conversation under that
    /// identity, and a `runner` bind beside a Setup naming the real harness gave one session
    /// two identities on the wire: the provider's read-back found an empty, never-written
    /// lineage. Other role routes (step-transform hooks) and plain tool routes keep `runner`,
    /// because tool modules key per-session state on it.
    #[test]
    fn a_compaction_provider_route_binds_the_sessions_real_identity() {
        use std::collections::BTreeMap;
        let role = |name: &str| BTreeMap::from([(name.to_owned(), "v1".to_owned())]);
        let target = RouteTarget::ToolProvider {
            module_id: "magic-context".to_string(),
        };
        for caller_harness in ["broca", "conformance", "anything-a-caller-picks"] {
            let identity = BindIdentity {
                project_root: std::path::PathBuf::from("/tmp/p"),
                harness: caller_harness.to_string(),
                session: "s1".to_string(),
            };
            let compaction = role(broca_core::compaction_provider::ROLE);
            let outbound = to_subc_bind(&identity, &target, Some(&compaction));
            assert_eq!(outbound.harness, caller_harness);
            assert_eq!(outbound.session, identity.session);
            assert_eq!(outbound.project_root, identity.project_root);

            for other in [
                Some(role("step-transform-provider")),
                Some(role("tool-provider")),
                None,
            ] {
                assert_eq!(
                    to_subc_bind(&identity, &target, other.as_ref()).harness,
                    TOOL_MODULE_HARNESS,
                    "{other:?} must keep the tool-module harness"
                );
            }
        }
    }

    fn frame(ty: FrameType, channel: u16, epoch: u32, corr: u64) -> Frame {
        Frame::build(ty, data_flags(), channel, epoch, corr, br#"{}"#.to_vec()).unwrap()
    }

    fn goodbye_frame(channel: u16, epoch: u32) -> Frame {
        Frame::build(
            FrameType::Goodbye,
            control_flags(),
            channel,
            epoch,
            0,
            Vec::new(),
        )
        .unwrap()
    }

    fn test_connection(
        write_tx: mpsc::Sender<Frame>,
        pending: PendingMap,
        route_epochs: RouteEpochs,
        conn: ConnToken,
        corr: u64,
    ) -> SubcConnection {
        let runtime = Arc::new(ConnectionRuntime {
            write_tx,
            pending,
            route_epochs,
            write_confirmations: Arc::new(Mutex::new(HashMap::new())),
            conn,
            shutdown: CancellationToken::new(),
            capabilities: tokio::sync::OnceCell::new(),
        });
        let (reconnect_tx, _reconnect_rx) = mpsc::unbounded_channel();
        SubcConnection {
            shared: Arc::new(ConnectionShared {
                connection_file_path: PathBuf::new(),
                current: Mutex::new(GenerationState::new(runtime)),
                corr: AtomicU64::new(corr),
                reconnect_tx,
                probe_timeout: Duration::from_millis(25),
                route_open_bind_relay_grace: Duration::from_millis(50),
                consumer_identity: None,
                reconnect_attempts: AtomicU64::new(0),
                probe_windows_started: AtomicU64::new(0),
            }),
            module_gates: ModuleGates::default(),
        }
    }

    async fn wait_for_probe_window(conn: &SubcConnection) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while conn.shared.probe_windows_started.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the confirmed Ping must open its conviction window");
    }

    #[derive(Clone, Copy)]
    enum ControlStubReply {
        Blackhole,
        SlowRouteOpen(Duration),
    }

    struct ControlTcpStub {
        connection_file: PathBuf,
        accepts: Arc<AtomicUsize>,
        requests: Arc<AtomicUsize>,
        pings: Arc<AtomicUsize>,
        errors: Arc<Mutex<Vec<String>>>,
        server: JoinHandle<()>,
    }

    impl ControlTcpStub {
        async fn start(reply: ControlStubReply) -> Self {
            static NEXT_STUB: AtomicUsize = AtomicUsize::new(1);

            let listener = TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("bind control TCP stub");
            let port = listener.local_addr().expect("control stub address").port();
            let key = vec![0x5c; 32];
            let daemon_id = [0xc5; 16];
            let connection_file = broca_test_support::scratch_root().join(format!(
                "broca-control-timeout-stub-{}-{}.json",
                std::process::id(),
                NEXT_STUB.fetch_add(1, Ordering::Relaxed)
            ));
            subc_transport::connection_file::write_atomic(
                &connection_file,
                &ConnectionInfo {
                    schema: SCHEMA_VERSION,
                    wire_version: None,
                    endpoints: vec![Endpoint {
                        host: "127.0.0.1".into(),
                        port,
                    }],
                    key: key.clone(),
                    daemon_id,
                    pid: std::process::id(),
                    daemon_ver: "control-timeout-stub".into(),
                },
            )
            .expect("write control stub connection file");

            let accepts = Arc::new(AtomicUsize::new(0));
            let requests = Arc::new(AtomicUsize::new(0));
            let pings = Arc::new(AtomicUsize::new(0));
            let errors = Arc::new(Mutex::new(Vec::new()));
            let server = {
                let accepts = Arc::clone(&accepts);
                let requests = Arc::clone(&requests);
                let pings = Arc::clone(&pings);
                let errors = Arc::clone(&errors);
                tokio::spawn(async move {
                    let mut handlers = JoinSet::new();
                    loop {
                        let (stream, _) = match listener.accept().await {
                            Ok(accepted) => accepted,
                            Err(error) => {
                                errors.lock().unwrap().push(error.to_string());
                                return;
                            }
                        };
                        accepts.fetch_add(1, Ordering::SeqCst);
                        let key = key.clone();
                        let requests = Arc::clone(&requests);
                        let pings = Arc::clone(&pings);
                        let errors = Arc::clone(&errors);
                        handlers.spawn(async move {
                            if let Err(error) = serve_control_stub_connection(
                                stream, daemon_id, key, reply, requests, pings,
                            )
                            .await
                            {
                                errors.lock().unwrap().push(error);
                            }
                        });
                    }
                })
            };

            Self {
                connection_file,
                accepts,
                requests,
                pings,
                errors,
                server,
            }
        }

        fn accept_count(&self) -> usize {
            self.accepts.load(Ordering::SeqCst)
        }

        fn request_count(&self) -> usize {
            self.requests.load(Ordering::SeqCst)
        }

        fn ping_count(&self) -> usize {
            self.pings.load(Ordering::SeqCst)
        }

        fn assert_healthy(&self) {
            assert!(
                self.errors.lock().unwrap().is_empty(),
                "control TCP stub failed: {:?}",
                self.errors.lock().unwrap().as_slice()
            );
        }
    }

    impl Drop for ControlTcpStub {
        fn drop(&mut self) {
            self.server.abort();
            let _ = std::fs::remove_file(&self.connection_file);
        }
    }

    async fn serve_control_stub_connection(
        mut stream: TcpStream,
        daemon_id: [u8; 16],
        key: Vec<u8>,
        reply: ControlStubReply,
        requests: Arc<AtomicUsize>,
        pings: Arc<AtomicUsize>,
    ) -> Result<(), String> {
        authenticate_server(
            &mut stream,
            &key,
            &daemon_id,
            "control-timeout-stub",
            broca_test_support::STAND_IN_AUTH_DEADLINE,
        )
        .await
        .map_err(|error| format!("authenticate control stub client: {error}"))?;

        while let Some(frame) = read_frame(&mut stream)
            .await
            .map_err(|error| format!("read control stub frame: {error}"))?
        {
            match (frame.header.channel, frame.header.ty) {
                (0, FrameType::Request) => {
                    requests.fetch_add(1, Ordering::SeqCst);
                    if let ControlStubReply::SlowRouteOpen(delay) = reply {
                        time::sleep(delay).await;
                        let body = serde_json::to_vec(&ClientControlResponse::RouteOpen {
                            route_channel: 9,
                            route_epoch: 1,
                        })
                        .map_err(|error| format!("encode route.open response: {error}"))?;
                        let response = Frame::build(
                            FrameType::Response,
                            control_flags(),
                            0,
                            0,
                            frame.header.corr,
                            body,
                        )
                        .map_err(|error| format!("build route.open response: {error}"))?;
                        write_frame(&mut stream, &response)
                            .await
                            .map_err(|error| format!("write route.open response: {error}"))?;
                    }
                }
                (0, FrameType::Ping) => {
                    pings.fetch_add(1, Ordering::SeqCst);
                    if matches!(reply, ControlStubReply::SlowRouteOpen(_)) {
                        let pong = Frame::build(
                            FrameType::Pong,
                            frame.header.flags,
                            0,
                            0,
                            frame.header.corr,
                            Vec::new(),
                        )
                        .map_err(|error| format!("build control stub Pong: {error}"))?;
                        write_frame(&mut stream, &pong)
                            .await
                            .map_err(|error| format!("write control stub Pong: {error}"))?;
                    }
                }
                (_, FrameType::Goodbye) => {}
                (_, other) => return Err(format!("unexpected control stub frame: {other:?}")),
            }
        }
        Ok(())
    }

    async fn connect_control_stub(
        stub: &ControlTcpStub,
        probe_timeout: Duration,
        bind_relay_grace: Duration,
    ) -> SubcConnection {
        SubcConnection::connect_with_timeouts(
            &stub.connection_file,
            probe_timeout,
            bind_relay_grace,
        )
        .await
        .expect("connect to control TCP stub")
    }

    #[tokio::test]
    async fn consumer_connect_tries_second_endpoint() {
        let stub = ControlTcpStub::start(ControlStubReply::Blackhole).await;
        let first = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let mut info = connection_file::read(&stub.connection_file).unwrap();
        info.endpoints.insert(
            0,
            Endpoint {
                host: "127.0.0.1".into(),
                port: first.local_addr().unwrap().port(),
            },
        );
        drop(first);
        connection_file::write_atomic(&stub.connection_file, &info).unwrap();
        let conn = SubcConnection::connect(&stub.connection_file)
            .await
            .unwrap();
        assert!(conn.shared.current.lock().unwrap().active);
        assert_eq!(stub.accept_count(), 1);
    }

    async fn wait_for_accept_count(stub: &ControlTcpStub, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while stub.accept_count() < expected {
                time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("TCP accept count did not reach {expected}"));
    }

    fn stream_data_frame(channel: u16, epoch: u32, corr: u64, sequence: u32) -> Frame {
        Frame::build(
            FrameType::StreamData,
            data_flags(),
            channel,
            epoch,
            corr,
            sequence.to_be_bytes().to_vec(),
        )
        .unwrap()
    }

    fn test_subscribe_stream(
        pending: &PendingMap,
        routes: &RouteEpochs,
        write_tx: &mpsc::Sender<Frame>,
        route: RouteHandle,
        corr: u64,
    ) -> SubscribeStream {
        let (registration, rx, lagged) = PendingRegistration::new_stream(
            Arc::clone(pending),
            (route.channel, route.epoch, corr),
        )
        .unwrap();
        SubscribeStream {
            rx: Some(rx),
            lagged,
            registration: Some(registration),
            done: false,
            write_tx: write_tx.clone(),
            route_epochs: Arc::clone(routes),
            route,
            corr,
        }
    }

    fn encoded_body(
        name: &str,
        arguments: &serde_json::Value,
        id: Option<&str>,
    ) -> serde_json::Value {
        let bytes = serde_json::to_vec(&ToolCallRequest {
            name: name.to_string(),
            arguments: arguments.clone(),
            tool_call_id: id.map(str::to_string),
            progress_token: None,
            call_key: None,
            schema_pin: None,
            origin: None,
            preset: None,
            output_refs: None,
        })
        .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// WITHOUT an id the wire bytes are byte-identical to the pre-2026-09-03 envelope.
    ///
    /// This is what made the migration to the shared `ToolCallRequest` safe to ship without a
    /// coordinated window: every existing module reader sees exactly what it saw before, so
    /// the change is inert until someone reads the new field. The two-key length check also
    /// pins `call_key` as absent, not null, on the unkeyed API.
    #[test]
    fn tool_call_body_without_an_id_preserves_the_legacy_shape() {
        let name = "apply_patch_雪";
        let arguments = serde_json::json!({
            "changes": {
                "lines": ["naïve café", "雪", "🦀"],
                "metadata": { "attempt": 2, "enabled": true }
            }
        });
        let legacy = serde_json::json!({ "name": name, "arguments": &arguments });
        let direct = encoded_body(name, &arguments, None);

        assert_eq!(direct, legacy);
        let envelope = direct.as_object().unwrap();
        assert_eq!(envelope.len(), 2);
        assert!(envelope.contains_key("name"));
        assert!(envelope.contains_key("arguments"));
    }

    /// WITH an id the encoded body carries it, under the canonical snake spelling.
    ///
    /// WHAT THIS DOES NOT COVER, stated because I proved it rather than assumed it: this builds
    /// the `ToolCallRequest` itself, so mutating `tool_call`'s body construction to
    /// `tool_call_id: None` leaves it GREEN. It pins the serialization contract -- snake
    /// spelling, id present, progress_token absent rather than null -- and says nothing about
    /// whether the production encode path uses its parameter.
    ///
    /// The seam above it IS covered: subc_plane's `dispatch_puts_the_provider_id_on_the_wire`
    /// reddens when the plane passes `None`. So the two ends are pinned and the middle -- the
    /// four lines inside `tool_call` -- is not, because reaching it needs a live route and a
    /// driven socket. That gap closes with the live-turn wire capture on the deployed binary,
    /// which is the proof owed to SUBC and ALF anyway; a unit test that faked the transport
    /// would assert less than the real dispatch does.
    ///
    /// Paired with the test above ON PURPOSE. A suite that only ever encoded one optional shape
    /// could not tell a working emit from a field that is silently skipped -- precisely the
    /// state broca shipped in for months, with every module downstream either synthesizing an
    /// identity or refusing the call.
    #[test]
    fn tool_call_body_carries_the_provider_id_when_one_is_supplied() {
        let arguments = serde_json::json!({"filePath": "Cargo.toml"});
        let direct = encoded_body("read", &arguments, Some("toolu_01ABC"));
        let envelope = direct.as_object().unwrap();

        assert_eq!(
            envelope
                .get("tool_call_id")
                .and_then(serde_json::Value::as_str),
            Some("toolu_01ABC"),
            "the provider id must reach the wire under the canonical snake spelling"
        );
        assert_eq!(
            envelope.len(),
            3,
            "progress_token must stay absent, not null"
        );
        assert!(!envelope.contains_key("progress_token"));
    }

    #[tokio::test]
    async fn tool_call_envelope_sends_the_frozen_preset_or_omits_it() {
        for preset in [Some("worker"), None] {
            let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
            let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
            let (write_tx, mut write_rx) = mpsc::channel(1);
            let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
            let route = RouteHandle::new(9, 1, ConnToken(1));
            let task = tokio::spawn(async move {
                conn.tool_call_with_preset(
                    &route,
                    "read",
                    &serde_json::json!({}),
                    Some("call"),
                    Duration::from_secs(1),
                    preset,
                )
                .await
            });
            let request = write_rx.recv().await.unwrap();
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(
                body.get("preset").and_then(serde_json::Value::as_str),
                preset
            );
            assert_eq!(
                body.as_object().unwrap().contains_key("preset"),
                preset.is_some()
            );
            let sender = match pending.lock().unwrap().get(&(9, 1, request.header.corr)) {
                Some(PendingEntry::Live { sender, .. }) => sender.clone(),
                _ => panic!("call registered its reply waiter"),
            };
            sender
                .send(
                    Frame::build(
                        FrameType::Response,
                        data_flags(),
                        9,
                        1,
                        request.header.corr,
                        br#"{"content":[]}"#.to_vec(),
                    )
                    .unwrap(),
                )
                .unwrap();
            task.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn tool_dispatch_log_identifies_route_request_and_operation_without_arguments() {
        let (conn, route, mut module) = SubcConnection::in_process_for_test(5, 3);
        let capture = broca_test_support::LogCapture::new();
        let signals = broca_core::tool_plane::DispatchSignals::default();
        let dispatch = capture.scope_future(conn.tool_call_raw_guarded(
            &route,
            "lookup",
            r#"{"secret":"argument content"}"#,
            Some("provider-call"),
            Duration::from_secs(1),
            None,
            None,
            &signals,
        ));
        let receive = async {
            let request = module.next_request().await;
            assert!(module.reply(&request, br#"{"content":[]}"#.to_vec()));
            request
        };
        let (result, request) = tokio::join!(dispatch, receive);
        assert!(result.is_ok(), "{result:?}");

        let logs: Vec<_> = capture
            .events()
            .into_iter()
            .filter(|event| {
                event.target == "subc.wire"
                    && event
                        .fields
                        .get("stage")
                        .is_some_and(|stage| stage == "tool_dispatch")
            })
            .collect();
        assert_eq!(logs.len(), 1, "{logs:?}");
        let event = &logs[0];
        assert_eq!(event.level, tracing::Level::DEBUG);
        assert_eq!(event.fields["stage"], "tool_dispatch");
        assert_eq!(event.fields["route_channel"], "5");
        assert_eq!(
            event.fields["correlation_id"],
            request.header.corr.to_string()
        );
        assert_eq!(event.fields["operation"], "lookup");
        let fields = format!("{:?}", event.fields);
        assert!(!fields.contains("secret"));
        assert!(!fields.contains("argument content"));
    }

    #[tokio::test]
    async fn route_request_raw_returns_response_body_unchanged() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
        let (write_tx, mut write_rx) = mpsc::channel(1);
        let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
        let route = RouteHandle::new(9, 1, ConnToken(1));

        let task = tokio::spawn(async move {
            conn.route_request_raw(&route, br#"{"method":"run.status","params":{}}"#.to_vec())
                .await
        });
        let request = tokio::time::timeout(Duration::from_secs(1), write_rx.recv())
            .await
            .expect("route request should be queued")
            .expect("writer channel should remain open");
        let response_body = br#"{"result":{"z":1,"a":2}}"#.to_vec();
        let response_tx = {
            let waiters = pending.lock().unwrap();
            match waiters.get(&(9, 1, request.header.corr)) {
                Some(PendingEntry::Live { sender, .. }) => sender.clone(),
                _ => panic!("route request should register a live response waiter"),
            }
        };
        let response = Frame::build(
            FrameType::Response,
            data_flags(),
            9,
            1,
            request.header.corr,
            response_body.clone(),
        )
        .unwrap();
        response_tx.send(response).unwrap();

        assert_eq!(task.await.unwrap().unwrap(), response_body);
    }

    #[tokio::test]
    async fn route_unary_bytes_preserves_request_and_response_buffers() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
        let (write_tx, mut write_rx) = mpsc::channel(1);
        let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
        let route = RouteHandle::new(9, 1, ConnToken(1));
        let body = br#"{"z":1,"a":2}"#.to_vec();
        let expected_body = body.clone();

        let task = tokio::spawn(async move {
            conn.route_unary_bytes(&route, body, Duration::from_secs(1))
                .await
        });
        let request = tokio::time::timeout(Duration::from_secs(1), write_rx.recv())
            .await
            .expect("unary request should be queued")
            .expect("writer channel should remain open");
        assert_eq!(request.body, expected_body);

        let response_tx = {
            let waiters = pending.lock().unwrap();
            match waiters.get(&(9, 1, request.header.corr)) {
                Some(PendingEntry::Live { sender, .. }) => sender.clone(),
                _ => panic!("unary request should register a live response waiter"),
            }
        };
        let response_body = br#"{"ck_messages":[],"diagnostic":{"z":1,"a":2}}"#.to_vec();
        let response = Frame::build(
            FrameType::Response,
            data_flags(),
            9,
            1,
            request.header.corr,
            response_body.clone(),
        )
        .unwrap();
        response_tx.send(response).unwrap();

        assert_eq!(task.await.unwrap().unwrap(), response_body);
    }

    #[tokio::test]
    async fn reader_task_closes_pending_subscribe_streams_on_connection_close() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
        let route = RouteHandle::new(7, 2, ConnToken(1));
        let (write_tx, _write_rx) = mpsc::channel(4);
        let mut stream = test_subscribe_stream(&pending, &routes, &write_tx, route, 11);

        let (client, server) = tokio::io::duplex(1024);
        let reader = tokio::spawn(reader_task(
            server,
            Arc::clone(&pending),
            Arc::clone(&routes),
            write_tx,
            None,
        ));
        drop(client);

        let next = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("stream should observe connection teardown instead of hanging")
            .expect("connection close should end the stream cleanly");
        assert_eq!(next, None);
        reader.await.unwrap();
        assert!(pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn superseded_reader_cannot_stamp_replacement_inbound_watermark() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
        let (write_tx, _write_rx) = mpsc::channel(4);
        let conn = test_connection(
            write_tx.clone(),
            Arc::clone(&pending),
            Arc::clone(&routes),
            ConnToken(1),
            1,
        );
        let old_runtime = {
            let state = conn.shared.current.lock().unwrap();
            Arc::clone(&state.runtime)
        };
        let (mut client, server) = tokio::io::duplex(1024);
        let reader = tokio::spawn(reader_task(
            server,
            pending,
            routes,
            write_tx,
            Some(ReaderGeneration {
                shared: Arc::downgrade(&conn.shared),
                conn: old_runtime.conn,
                shutdown: old_runtime.shutdown.clone(),
                test_gate: None,
            }),
        ));

        let (replacement_write_tx, _replacement_write_rx) = mpsc::channel(4);
        let replacement = Arc::new(ConnectionRuntime {
            write_tx: replacement_write_tx,
            pending: Arc::new(Mutex::new(HashMap::new())),
            route_epochs: Arc::new(Mutex::new(HashMap::new())),
            write_confirmations: Arc::new(Mutex::new(HashMap::new())),
            conn: ConnToken(2),
            shutdown: CancellationToken::new(),
            capabilities: tokio::sync::OnceCell::new(),
        });
        let anchor = Instant::now() - Duration::from_secs(60);
        let mut replacement_state = GenerationState::new(replacement);
        replacement_state.last_inbound = anchor;
        *conn.shared.current.lock().unwrap() = replacement_state;

        write_frame(&mut client, &frame(FrameType::Response, 0, 0, 9))
            .await
            .unwrap();
        drop(client);
        tokio::time::timeout(Duration::from_secs(1), reader)
            .await
            .expect("the superseded reader must stop on its next buffered frame")
            .unwrap();

        let state = conn.shared.current.lock().unwrap();
        assert_eq!(state.runtime.conn, ConnToken(2));
        assert_eq!(
            state.last_inbound, anchor,
            "an old socket's buffered frame cannot exonerate the current generation"
        );
    }

    #[tokio::test]
    async fn inactive_current_reader_cannot_stamp_inbound_watermark() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
        let (write_tx, _write_rx) = mpsc::channel(4);
        let conn = test_connection(
            write_tx.clone(),
            Arc::clone(&pending),
            Arc::clone(&routes),
            ConnToken(1),
            1,
        );
        let runtime = {
            let state = conn.shared.current.lock().unwrap();
            Arc::clone(&state.runtime)
        };
        let anchor = Instant::now() - Duration::from_secs(60);
        conn.shared.current.lock().unwrap().last_inbound = anchor;
        let gate = Arc::new(ReaderTestGate::default());
        let (mut client, server) = tokio::io::duplex(1024);
        let reader = tokio::spawn(reader_task(
            server,
            pending,
            routes,
            write_tx,
            Some(ReaderGeneration {
                shared: Arc::downgrade(&conn.shared),
                conn: runtime.conn,
                shutdown: runtime.shutdown.clone(),
                test_gate: Some(Arc::clone(&gate)),
            }),
        ));

        let frame_read = gate.frame_read.notified();
        write_frame(&mut client, &frame(FrameType::Response, 0, 0, 9))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), frame_read)
            .await
            .expect("the reader must reach the post-read generation check");
        {
            let mut state = conn.shared.current.lock().unwrap();
            assert!(deactivate_locked(&mut state));
        }
        gate.proceed.notify_one();
        drop(client);
        tokio::time::timeout(Duration::from_secs(1), reader)
            .await
            .expect("the inactive reader must stop before stamping inbound")
            .unwrap();

        let state = conn.shared.current.lock().unwrap();
        assert!(!state.active);
        assert_eq!(
            state.last_inbound, anchor,
            "a frame read before teardown cannot exonerate an inactive generation"
        );
    }

    #[tokio::test]
    async fn lagged_subscribe_detaches_without_blocking_unrelated_rpc() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2), (9, 1)])));
        let stream_route = RouteHandle::new(7, 2, ConnToken(1));
        let rpc_route = RouteHandle::new(9, 1, ConnToken(1));
        let stream_corr = 11;
        let (write_tx, mut write_rx) = mpsc::channel(4);
        let conn = test_connection(
            write_tx.clone(),
            Arc::clone(&pending),
            Arc::clone(&routes),
            ConnToken(1),
            20,
        );
        let mut stream =
            test_subscribe_stream(&pending, &routes, &write_tx, stream_route, stream_corr);

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let reader = tokio::spawn(reader_task(
            server,
            Arc::clone(&pending),
            Arc::clone(&routes),
            write_tx,
            None,
        ));
        let rpc = tokio::spawn(async move { conn.route_request(&rpc_route, b"{}".to_vec()).await });
        let request = tokio::time::timeout(Duration::from_secs(1), write_rx.recv())
            .await
            .expect("the unrelated RPC should send its request")
            .expect("the local writer should remain open");

        tokio::time::timeout(Duration::from_secs(5), async {
            for sequence in 0..=SUBSCRIBE_STREAM_BUFFER as u32 {
                write_frame(&mut client, &stream_data_frame(7, 2, stream_corr, sequence))
                    .await
                    .unwrap();
            }
            write_frame(
                &mut client,
                &frame(FrameType::Response, 9, 1, request.header.corr),
            )
            .await
            .unwrap();
        })
        .await
        .expect("a full subscribe queue must not block socket ingress");

        let response = tokio::time::timeout(Duration::from_secs(1), rpc)
            .await
            .expect("the unrelated RPC should complete while the subscriber is wedged")
            .unwrap()
            .unwrap();
        assert_eq!(response, serde_json::json!({}));

        let error = stream
            .next()
            .await
            .expect_err("overflow must detach the subscriber instead of draining stale frames");
        assert!(matches!(
            &error,
            ConnError::StreamLagged { buffered } if *buffered == SUBSCRIBE_STREAM_BUFFER
        ));
        assert!(error
            .to_string()
            .contains("resubscribe from your last durable cursor"));

        let goodbye = tokio::time::timeout(Duration::from_secs(1), write_rx.recv())
            .await
            .expect("overflow should queue a best-effort route close")
            .expect("the local writer should remain open");
        assert_eq!(goodbye.header.ty, FrameType::Goodbye);
        assert_eq!((goodbye.header.channel, goodbye.header.epoch), (7, 2));
        assert!(!pending.lock().unwrap().contains_key(&(7, 2, stream_corr)));
        assert!(!routes.lock().unwrap().contains_key(&7));

        drop(client);
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn subscribe_buffer_preserves_every_frame_below_the_limit() {
        const FRAME_COUNT: u32 = 128;

        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
        let route = RouteHandle::new(7, 2, ConnToken(1));
        let stream_corr = 11;
        let (write_tx, _write_rx) = mpsc::channel(4);
        let mut stream = test_subscribe_stream(&pending, &routes, &write_tx, route, stream_corr);
        let (mut client, server) = tokio::io::duplex(16 * 1024);
        let reader = tokio::spawn(reader_task(
            server,
            Arc::clone(&pending),
            Arc::clone(&routes),
            write_tx,
            None,
        ));

        for sequence in 0..FRAME_COUNT {
            write_frame(&mut client, &stream_data_frame(7, 2, stream_corr, sequence))
                .await
                .unwrap();
        }
        write_frame(&mut client, &frame(FrameType::StreamEnd, 7, 2, stream_corr))
            .await
            .unwrap();

        for expected in 0..FRAME_COUNT {
            let body = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("a buffered frame should be available")
                .unwrap()
                .expect("the stream should not end before all data frames are drained");
            assert_eq!(body, expected.to_be_bytes());
        }
        assert_eq!(stream.next().await.unwrap(), None);
        assert!(pending.lock().unwrap().is_empty());

        drop(stream);
        assert!(!routes.lock().unwrap().contains_key(&7));
        drop(client);
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn subscribe_stream_turns_a_route_gone_goodbye_into_an_error() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
        let route = RouteHandle::new(7, 2, ConnToken(1));
        let (write_tx, _write_rx) = mpsc::channel(4);
        let mut stream = test_subscribe_stream(&pending, &routes, &write_tx, route, 11);

        let (mut client, server) = tokio::io::duplex(1024);
        let reader = tokio::spawn(reader_task(
            server,
            Arc::clone(&pending),
            Arc::clone(&routes),
            write_tx,
            None,
        ));
        write_frame(&mut client, &goodbye_frame(7, 2))
            .await
            .unwrap();

        let next = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("stream should observe route teardown instead of hanging");
        assert!(
            matches!(next, Err(ConnError::Transport(message)) if message.contains("route gone"))
        );
        drop(client);
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn local_route_close_wakes_sibling_calls_only_in_its_epoch() {
        use futures::FutureExt;
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2), (8, 3)])));
        let (write_tx, mut write_rx) = mpsc::channel(1);
        write_tx.try_send(frame(FrameType::Push, 0, 0, 0)).unwrap();
        let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
        let route = RouteHandle::new(7, 2, ConnToken(1));
        let (_, _first, mut first_rx) = conn.register_route(&route, (7, 2, 5)).unwrap();
        let (_, _sibling, mut sibling_rx) = conn.register_route(&route, (7, 2, 6)).unwrap();
        let (_old, mut old_rx) =
            PendingRegistration::new(Arc::clone(&pending), (7, 1, 5), false, None).unwrap();
        let (_, _other, mut other_rx) = conn
            .register_route(&RouteHandle::new(8, 3, ConnToken(1)), (8, 3, 5))
            .unwrap();

        let close = conn.route_close(&route);
        tokio::pin!(close);
        assert!(close.as_mut().now_or_never().is_none(), "egress is full");
        for rx in [&mut first_rx, &mut sibling_rx] {
            let terminal = rx
                .try_recv()
                .expect("local close must fan out before enqueue");
            assert_eq!(terminal.header.ty, FrameType::Goodbye);
            assert_eq!((terminal.header.channel, terminal.header.epoch), (7, 2));
        }
        assert!(matches!(
            old_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            other_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(!conn.route_is_open(&route));
        assert!(conn.register_route(&route, (7, 2, 7)).is_err());
        write_rx.recv().await.unwrap();
        close.await.unwrap();
        assert_eq!(write_rx.recv().await.unwrap().header.ty, FrameType::Goodbye);
    }

    #[tokio::test]
    async fn route_gone_goodbye_fans_out_only_within_its_epoch() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
        let (_registration, mut rx) =
            PendingRegistration::new(Arc::clone(&pending), (7, 2, 5), false, None).unwrap();
        let (write_tx, _write_rx) = mpsc::channel(4);
        let (mut client, server) = tokio::io::duplex(1024);
        let reader = tokio::spawn(reader_task(
            server,
            Arc::clone(&pending),
            Arc::clone(&routes),
            write_tx,
            None,
        ));
        write_frame(&mut client, &goodbye_frame(7, 2))
            .await
            .unwrap();

        let received = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("the corr=0 GOODBYE must reach a waiter in the same route generation")
            .expect("a frame, not a channel close");
        assert_eq!(received.header.ty, FrameType::Goodbye);
        drop(client);
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn stale_epoch_ingress_does_not_settle_a_live_waiter() {
        // Deltas, not absolutes: the gauge is process-global by design, so a parallel test
        // dropping its own frames would break an equality on the total. `>= 1` still fails if
        // the stamp is missing, which is the property under test.
        let (stale_before, _) = FRAME_DROPS.counts();
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
        let (_registration, mut rx) =
            PendingRegistration::new(Arc::clone(&pending), (7, 2, 11), false, None).unwrap();
        let (write_tx, _write_rx) = mpsc::channel(4);
        let (mut client, server) = tokio::io::duplex(1024);
        let reader = tokio::spawn(reader_task(
            server,
            Arc::clone(&pending),
            Arc::clone(&routes),
            write_tx,
            None,
        ));

        write_frame(&mut client, &frame(FrameType::Response, 7, 1, 11))
            .await
            .unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(50), rx.recv())
            .await
            .is_err());

        write_frame(&mut client, &frame(FrameType::Response, 7, 2, 11))
            .await
            .unwrap();
        let received = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("the current epoch response should settle the waiter")
            .expect("a response frame");
        assert_eq!(received.header.epoch, 2);
        drop(client);
        reader.await.unwrap();

        let (stale_after, _) = FRAME_DROPS.counts();
        assert!(
            stale_after > stale_before,
            "the dropped stale-epoch frame must be counted; without it a client losing every \
             frame to a dead route generation reports exactly like an idle one"
        );
    }

    /// A terminal whose waiter has already gone is counted, not merely discarded.
    ///
    /// This is the drop that means a reply outlived its request: the caller timed out, or was
    /// cancelled, and the answer arrived afterwards. Silently discarding it is correct -- there
    /// is nobody to give it to -- but a client where this happens constantly looks identical to
    /// one where it never happens, and the difference is exactly what an investigation needs.
    #[tokio::test]
    async fn an_unmatched_terminal_is_counted() {
        let (_, unmatched_before) = FRAME_DROPS.counts();
        // Route 7 epoch 2 is live, so the frame passes the epoch gate and reaches the waiter
        // lookup -- the path under test. An empty pending map is a waiter that has already gone.
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
        let (write_tx, _write_rx) = mpsc::channel(4);
        let (mut client, server) = tokio::io::duplex(1024);
        let reader = tokio::spawn(reader_task(
            server,
            Arc::clone(&pending),
            Arc::clone(&routes),
            write_tx,
            None,
        ));

        write_frame(&mut client, &frame(FrameType::Response, 7, 2, 99))
            .await
            .unwrap();
        drop(client);
        reader.await.unwrap();

        let (_, unmatched_after) = FRAME_DROPS.counts();
        assert!(
            unmatched_after > unmatched_before,
            "a terminal with no waiter must be counted; uncounted, a reply that outlived its \
             request is indistinguishable from a peer that never answered"
        );
    }

    fn reverse_request(channel: u16, epoch: u32, corr: u64, method: &str) -> Frame {
        Frame::build(
            FrameType::Request,
            data_flags(),
            channel,
            epoch,
            corr,
            serde_json::to_vec(&serde_json::json!({ "method": method, "params": {} })).unwrap(),
        )
        .unwrap()
    }

    async fn next_written(write_rx: &mut mpsc::Receiver<Frame>, what: &str) -> Frame {
        tokio::time::timeout(Duration::from_secs(1), write_rx.recv())
            .await
            .unwrap_or_else(|_| panic!("{what} should be written"))
            .expect("the writer channel should remain open")
    }

    fn assert_refusal(reply: &Frame, channel: u16, epoch: u32, corr: u64, family: &str) {
        assert_eq!(reply.header.ty, FrameType::Error);
        assert_eq!(
            (reply.header.channel, reply.header.epoch, reply.header.corr),
            (channel, epoch, corr),
            "the refusal must answer on the request's own channel, epoch and corr"
        );
        let body: subc_protocol::ErrorBody = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(body.code, REVERSE_REQUEST_UNHANDLED);
        assert_eq!(
            body.message,
            format!("no reverse-request handler is registered for {family}")
        );
    }

    /// A module's request on an open route gets the SDK's refusal instead of silence, so the
    /// module is not left waiting out its own deadline for an answer broca will never send.
    #[tokio::test]
    async fn a_reverse_request_on_an_open_route_is_refused_with_reverse_request_unhandled() {
        let refused_before = FRAME_DROPS.reverse_requests_refused();
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
        let (write_tx, mut write_rx) = mpsc::channel(4);
        let (mut client, server) = tokio::io::duplex(1024);
        let reader = tokio::spawn(reader_task(
            server,
            Arc::clone(&pending),
            Arc::clone(&routes),
            write_tx,
            None,
        ));

        write_frame(
            &mut client,
            &reverse_request(7, 2, 42, "elicitation/create"),
        )
        .await
        .unwrap();
        let reply = next_written(&mut write_rx, "the refusal").await;
        assert_refusal(&reply, 7, 2, 42, "elicitation");

        drop(client);
        reader.await.unwrap();
        // A delta, because the gauge is process-global and other tests run in parallel.
        assert!(
            FRAME_DROPS.reverse_requests_refused() > refused_before,
            "a refused reverse request must be counted so an operator can see modules asking"
        );
    }

    /// Corrs are allocated independently in each direction, so a module may legally send a
    /// request whose corr equals a call broca has in flight on the same route. The frame type
    /// is what tells them apart: the request is refused, and the call still completes with its
    /// own Response rather than taking the request as its terminal.
    #[tokio::test]
    async fn a_reverse_request_sharing_a_pending_calls_corr_does_not_complete_that_call() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
        let (write_tx, mut write_rx) = mpsc::channel(8);
        let conn = test_connection(
            write_tx.clone(),
            Arc::clone(&pending),
            Arc::clone(&routes),
            ConnToken(1),
            1,
        );
        let route = RouteHandle::new(9, 1, ConnToken(1));
        let (mut client, server) = tokio::io::duplex(4096);
        let reader = tokio::spawn(reader_task(
            server,
            Arc::clone(&pending),
            Arc::clone(&routes),
            write_tx,
            None,
        ));

        let call = tokio::spawn(async move {
            conn.tool_call_with_deadline(
                &route,
                "read",
                &serde_json::json!({}),
                None,
                Duration::from_secs(5),
            )
            .await
        });
        let request = next_written(&mut write_rx, "the tool call").await;
        assert_eq!(request.header.ty, FrameType::Request);
        let corr = request.header.corr;

        write_frame(
            &mut client,
            &reverse_request(9, 1, corr, "sampling/createMessage"),
        )
        .await
        .unwrap();
        let reply = next_written(&mut write_rx, "the refusal").await;
        assert_refusal(&reply, 9, 1, corr, "sampling");
        assert!(
            !call.is_finished(),
            "the module's request must not be taken as the pending call's terminal"
        );

        let result = Frame::build(
            FrameType::Response,
            data_flags(),
            9,
            1,
            corr,
            br#"{"content":[],"isError":false}"#.to_vec(),
        )
        .unwrap();
        write_frame(&mut client, &result).await.unwrap();
        let value = tokio::time::timeout(Duration::from_secs(1), call)
            .await
            .expect("the call should complete on its real Response")
            .unwrap()
            .expect("the call should succeed");
        assert_eq!(value, serde_json::json!({"content": [], "isError": false}));

        drop(client);
        reader.await.unwrap();
    }

    /// Only a Response or an Error ends a call. Anything else delivered to a waiter is reported
    /// as a protocol violation instead of being handed back as the call's result.
    #[tokio::test]
    async fn a_non_terminal_frame_type_is_a_protocol_violation_not_a_terminal() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
        let (write_tx, mut write_rx) = mpsc::channel(1);
        let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
        let route = RouteHandle::new(9, 1, ConnToken(1));

        let task = tokio::spawn(async move {
            conn.route_unary_bytes(&route, b"{}".to_vec(), Duration::from_secs(1))
                .await
        });
        let request = next_written(&mut write_rx, "the unary request").await;
        let sender = {
            let waiters = pending.lock().unwrap();
            match waiters.get(&(9, 1, request.header.corr)) {
                Some(PendingEntry::Live { sender, .. }) => sender.clone(),
                _ => panic!("the unary request should register a live waiter"),
            }
        };
        sender
            .send(frame(FrameType::Request, 9, 1, request.header.corr))
            .unwrap();

        let error = task
            .await
            .unwrap()
            .expect_err("a Request is not a terminal");
        assert!(
            matches!(&error, ConnError::Protocol(message)
                if message.contains("Request") && message.contains("Response or Error")),
            "got {error:?}"
        );
    }

    #[tokio::test]
    async fn late_route_open_response_queues_epoch_stamped_goodbye() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::from([(
            (0, 0, 5),
            PendingEntry::LateRouteOpen {
                blocks_probe_until: Instant::now(),
            },
        )])));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
        let (write_tx, mut write_rx) = mpsc::channel(4);
        let (mut client, server) = tokio::io::duplex(1024);
        let reader = tokio::spawn(reader_task(
            server,
            Arc::clone(&pending),
            Arc::clone(&routes),
            write_tx,
            None,
        ));
        let body = serde_json::to_vec(&ClientControlResponse::RouteOpen {
            route_channel: 9,
            route_epoch: 4,
        })
        .unwrap();
        let response = Frame::build(FrameType::Response, control_flags(), 0, 0, 5, body).unwrap();
        write_frame(&mut client, &response).await.unwrap();

        let goodbye = tokio::time::timeout(Duration::from_secs(1), write_rx.recv())
            .await
            .expect("late route.open must queue cleanup")
            .expect("a GOODBYE frame");
        assert_eq!(goodbye.header.ty, FrameType::Goodbye);
        assert_eq!((goodbye.header.channel, goodbye.header.epoch), (9, 4));
        assert!(pending.lock().unwrap().is_empty());
        assert!(routes.lock().unwrap().is_empty());
        drop(client);
        reader.await.unwrap();
    }

    async fn congested_egress_does_not_stop_ingress(cleanup: u8) {
        let pending: PendingMap = Arc::default();
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(7, 2)])));
        let (_live, mut reply_rx) =
            PendingRegistration::new(Arc::clone(&pending), (0, 0, 2), false, None).unwrap();
        let (mut abandoned, open_rx) =
            PendingRegistration::new(Arc::clone(&pending), (0, 0, 1), true, None).unwrap();
        drop(open_rx);
        if cleanup == 1 {
            abandoned.arm_late_route_open_cleanup();
            drop(abandoned);
        }
        let trigger = if cleanup == 0 {
            reverse_request(7, 2, 1, "unhandled")
        } else {
            Frame::build(
                FrameType::Response,
                control_flags(),
                0,
                0,
                1,
                serde_json::to_vec(&ClientControlResponse::RouteOpen {
                    route_channel: 9,
                    route_epoch: 4,
                })
                .unwrap(),
            )
            .unwrap()
        };
        let (write_tx, mut write_rx) = mpsc::channel(1);
        write_tx.try_send(frame(FrameType::Ping, 0, 0, 99)).unwrap();
        let (mut client, server) = tokio::io::duplex(4096);
        let reader = tokio::spawn(reader_task(
            server,
            Arc::clone(&pending),
            Arc::clone(&routes),
            write_tx,
            None,
        ));
        write_frame(&mut client, &trigger).await.unwrap();
        write_frame(&mut client, &frame(FrameType::Response, 0, 0, 2))
            .await
            .unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(1), reply_rx.recv())
            .await
            .expect("ingress must process a reply while egress remains full")
            .unwrap();
        assert_eq!(reply.header.corr, 2);
        assert_eq!(
            write_rx.try_recv().unwrap().header.corr,
            99,
            "the full queue was not drained to permit ingress"
        );
        assert!(
            !routes.lock().unwrap().contains_key(&9),
            "abandoned opens must still be invalidated when cleanup cannot enqueue"
        );
        drop(client);
        reader.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn full_egress_reverse_refusal_does_not_block_replies() {
        congested_egress_does_not_stop_ingress(0).await;
    }

    #[tokio::test(start_paused = true)]
    async fn full_egress_late_open_cleanup_does_not_block_replies() {
        congested_egress_does_not_stop_ingress(1).await;
    }

    #[tokio::test(start_paused = true)]
    async fn full_egress_abandoned_open_cleanup_does_not_block_replies() {
        congested_egress_does_not_stop_ingress(2).await;
    }

    #[tokio::test]
    async fn foreign_connection_handle_fails_without_emitting_a_frame() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
        let (write_tx, mut write_rx) = mpsc::channel(1);
        let conn = test_connection(write_tx, pending, routes, ConnToken(2), 1);
        let foreign = RouteHandle::new(9, 1, ConnToken(1));

        let error = conn
            .route_request(&foreign, br#"{}"#.to_vec())
            .await
            .unwrap_err();
        assert!(matches!(error, ConnError::StaleRouteHandle(handle) if handle == foreign));
        assert!(write_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn pending_waiter_limit_returns_a_typed_transport_error() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        for corr in 0..MAX_PENDING_REQUESTS as u64 {
            let (sender, _rx) = mpsc::unbounded_channel();
            pending.lock().unwrap().insert(
                (9, 1, corr),
                PendingEntry::Live {
                    sender: PendingSender::Unary(sender),
                    route_open: false,
                },
            );
        }
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
        let (write_tx, _write_rx) = mpsc::channel(1);
        let conn = test_connection(
            write_tx,
            Arc::clone(&pending),
            routes,
            ConnToken(1),
            MAX_PENDING_REQUESTS as u64,
        );
        let route = RouteHandle::new(9, 1, ConnToken(1));

        let error = conn
            .route_request(&route, br#"{}"#.to_vec())
            .await
            .expect_err("the 4097th in-flight request must be rejected");
        assert!(matches!(error, ConnError::Transport(message) if message.contains("4096")));
        assert_eq!(pending.lock().unwrap().len(), MAX_PENDING_REQUESTS);
    }

    #[tokio::test]
    async fn cancelled_request_deregisters_its_pending_waiter() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
        let (write_tx, _write_rx) = mpsc::channel(1);
        write_tx
            .send(frame(FrameType::Request, 99, 1, 1))
            .await
            .unwrap();
        let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
        let route = RouteHandle::new(9, 1, ConnToken(1));

        let task = tokio::spawn(async move {
            let _ = conn.route_request(&route, br#"{}"#.to_vec()).await;
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !pending.lock().unwrap().contains_key(&(9, 1, 1)) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("request should register its waiter before cancellation");

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), async {
            while !pending.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelling the request should drop its pending waiter");
    }

    #[tokio::test]
    async fn timed_out_control_blackhole_replaces_the_tcp_connection() {
        let stub = ControlTcpStub::start(ControlStubReply::Blackhole).await;
        let conn =
            connect_control_stub(&stub, Duration::from_millis(20), Duration::from_millis(70)).await;
        assert_eq!(stub.accept_count(), 1, "the test must begin on one socket");

        let _error = conn
            .control_rpc_deadline(Vec::new(), Duration::from_millis(40))
            .await
            .expect_err("the control-only stub intentionally withholds every response");
        wait_for_accept_count(&stub, 2).await;

        assert_eq!(
            stub.accept_count(),
            2,
            "a channel-zero timeout must replace the convicted TCP generation"
        );
        assert_eq!(
            stub.request_count(),
            1,
            "the first control RPC must be sent"
        );
        assert!(
            stub.ping_count() >= 1,
            "the deferred probe must send a Ping"
        );
        stub.assert_healthy();
    }

    #[tokio::test]
    async fn retained_route_open_tombstone_stops_suppressing_after_grace() {
        let stub = ControlTcpStub::start(ControlStubReply::Blackhole).await;
        let conn =
            connect_control_stub(&stub, Duration::from_millis(20), Duration::from_millis(70)).await;
        assert_eq!(stub.accept_count(), 1, "the test must begin on one socket");

        let _error = conn
            .route_open_rpc_deadline(Vec::new(), Duration::from_millis(40))
            .await
            .expect_err("the dead daemon leaves a retained route.open tombstone");
        wait_for_accept_count(&stub, 2).await;

        assert_eq!(
            stub.accept_count(),
            2,
            "an expired cleanup tombstone must not preserve a dead TCP generation"
        );
        assert_eq!(stub.request_count(), 1, "the route.open must be sent once");
        assert!(
            stub.ping_count() >= 1,
            "grace expiry must release the probe"
        );
        stub.assert_healthy();
    }

    #[tokio::test]
    async fn slow_live_route_open_is_not_convicted_during_bind_relay_grace() {
        // These compressed deadlines preserve the ordering of a caller that gives up on a
        // route.open before the daemon's relay window closes: the caller's wait ends, the daemon
        // is still relaying that same bind, and its healthy response arrives after a no-grace
        // probe window would already have convicted the socket.
        let stub =
            ControlTcpStub::start(ControlStubReply::SlowRouteOpen(Duration::from_millis(140)))
                .await;
        let conn =
            connect_control_stub(&stub, Duration::from_millis(20), Duration::from_millis(180))
                .await;
        assert_eq!(stub.accept_count(), 1, "the test must begin on one socket");

        let _error = conn
            .route_open_rpc_deadline(Vec::new(), Duration::from_millis(80))
            .await
            .expect_err("the bind relay deliberately outlives the caller deadline");
        time::sleep(Duration::from_millis(250)).await;

        assert_eq!(
            stub.accept_count(),
            1,
            "late healthy route.open work must not convict the shared TCP generation"
        );
        assert_eq!(
            stub.request_count(),
            1,
            "the route.open must reach the daemon"
        );
        assert!(
            stub.ping_count() >= 1,
            "the deferred health decision must eventually probe the live socket"
        );
        stub.assert_healthy();
    }

    #[tokio::test]
    async fn channel_zero_pending_request_defers_probe_until_registration_drops() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
        let (write_tx, mut write_rx) = mpsc::channel(4);
        let conn = test_connection(write_tx, pending, routes, ConnToken(1), 1);
        let runtime = {
            let state = conn.shared.current.lock().unwrap();
            Arc::clone(&state.runtime)
        };
        let (_runtime, registration, response_rx) = conn
            .register_control((0, 0, 41), false)
            .expect("install an in-flight channel-zero request");

        conn.spawn_liveness_probe(&runtime);
        assert!(
            tokio::time::timeout(Duration::from_millis(60), write_rx.recv())
                .await
                .is_err(),
            "a probe must not enter the writer queue while channel zero is pending"
        );
        {
            let state = conn.shared.current.lock().unwrap();
            assert!(!state.probe_in_flight);
            assert!(state.probe_deferred, "the blocked probe must be remembered");
        }

        drop(response_rx);
        drop(registration);
        let ping = tokio::time::timeout(Duration::from_secs(1), write_rx.recv())
            .await
            .expect("draining the last live blocker must release the probe")
            .expect("the deferred probe must queue a Ping");
        assert_eq!(ping.header.ty, FrameType::Ping);
    }

    #[tokio::test]
    async fn liveness_probe_is_single_flight_within_a_generation() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
        let (write_tx, mut write_rx) = mpsc::channel(4);
        let conn = test_connection(write_tx, pending, routes, ConnToken(1), 1);
        let runtime = {
            let state = conn.shared.current.lock().unwrap();
            Arc::clone(&state.runtime)
        };

        conn.spawn_liveness_probe(&runtime);
        conn.spawn_liveness_probe(&runtime);
        let ping = write_rx
            .recv()
            .await
            .expect("the first probe must queue a Ping");
        assert_eq!(ping.header.ty, FrameType::Ping);
        assert!(
            tokio::time::timeout(Duration::from_millis(60), write_rx.recv())
                .await
                .is_err(),
            "a second timeout must share the generation's in-flight probe"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn probe_timeout_starts_only_after_confirmed_write() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
        let (write_tx, mut write_rx) = mpsc::channel(4);
        let conn = test_connection(write_tx, pending, routes, ConnToken(1), 1);
        let runtime = {
            let state = conn.shared.current.lock().unwrap();
            Arc::clone(&state.runtime)
        };

        conn.shared.current.lock().unwrap().probe_in_flight = true;
        let probe = tokio::spawn(run_liveness_probe(
            Arc::downgrade(&conn.shared),
            Arc::clone(&runtime),
            1,
        ));
        let ping = write_rx.recv().await.expect("probe must queue its Ping");
        assert_eq!(ping.header.ty, FrameType::Ping);
        tokio::time::advance(Duration::from_millis(60)).await;
        assert!(
            conn.shared.current.lock().unwrap().active,
            "queue insertion alone must not start conviction"
        );

        runtime
            .write_confirmations
            .lock()
            .unwrap()
            .remove(&ping.header.corr)
            .expect("the Ping must await a writer receipt")
            .send(Ok(Instant::now()))
            .unwrap();
        wait_for_probe_window(&conn).await;
        tokio::time::advance(conn.shared.probe_timeout).await;
        probe.await.unwrap();
        assert!(
            !conn.shared.current.lock().unwrap().active,
            "silence after a confirmed Ping write must convict the generation"
        );
    }

    struct BlockedWriter(Arc<tokio::sync::Notify>);

    impl AsyncWrite for BlockedWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            _: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            self.0.notify_one();
            std::task::Poll::Pending
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_ping_write_expires_and_reconnects() {
        let (write_tx, write_rx) = mpsc::channel(1);
        let mut conn = test_connection(write_tx, Arc::default(), Arc::default(), ConnToken(1), 1);
        let (reconnect_tx, mut reconnect_rx) = mpsc::unbounded_channel();
        Arc::get_mut(&mut conn.shared).unwrap().reconnect_tx = reconnect_tx;
        let runtime = Arc::clone(&conn.shared.current.lock().unwrap().runtime);
        let blocked = Arc::new(tokio::sync::Notify::new());
        let writer = tokio::spawn(writer_task(
            BlockedWriter(Arc::clone(&blocked)),
            write_rx,
            Arc::downgrade(&conn.shared),
            runtime.conn,
            runtime.shutdown.clone(),
            Arc::clone(&runtime.write_confirmations),
        ));
        let probe = tokio::spawn(run_liveness_probe(
            Arc::downgrade(&conn.shared),
            Arc::clone(&runtime),
            1,
        ));
        blocked.notified().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::timeout(Duration::from_secs(1), probe)
            .await
            .expect("egress deadline must finish the probe")
            .unwrap();
        assert!(!conn.shared.current.lock().unwrap().active);
        assert_eq!(
            reconnect_rx
                .try_recv()
                .expect("conviction must request reconnect"),
            runtime.conn
        );
        assert!(runtime.shutdown.is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), writer)
            .await
            .expect("generation shutdown must interrupt the blocked write")
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_ping_enqueue_expires() {
        use futures::FutureExt;
        let (write_tx, _write_rx) = mpsc::channel(1);
        write_tx.try_send(frame(FrameType::Push, 0, 0, 0)).unwrap();
        let conn = test_connection(write_tx, Arc::default(), Arc::default(), ConnToken(1), 1);
        let runtime = Arc::clone(&conn.shared.current.lock().unwrap().runtime);
        let probe = run_liveness_probe(Arc::downgrade(&conn.shared), Arc::clone(&runtime), 1);
        tokio::pin!(probe);
        assert!(probe.as_mut().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(
            probe.now_or_never().is_some(),
            "queue send must share the egress budget"
        );
        assert!(!conn.shared.current.lock().unwrap().active);
        assert!(runtime.write_confirmations.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn channel_zero_activity_during_probe_window_prevents_conviction() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
        let (write_tx, mut write_rx) = mpsc::channel(4);
        let conn = test_connection(write_tx, pending, routes, ConnToken(1), 1);
        let runtime = {
            let state = conn.shared.current.lock().unwrap();
            Arc::clone(&state.runtime)
        };

        conn.spawn_liveness_probe(&runtime);
        let ping = write_rx.recv().await.expect("probe must queue its Ping");
        runtime
            .write_confirmations
            .lock()
            .unwrap()
            .remove(&ping.header.corr)
            .unwrap()
            .send(Ok(Instant::now()))
            .unwrap();
        wait_for_probe_window(&conn).await;
        let (_runtime, registration, response_rx) = conn
            .register_control((0, 0, 42), false)
            .expect("channel-zero work may appear during the probe window");
        drop(response_rx);
        drop(registration);
        tokio::time::sleep(Duration::from_millis(60)).await;

        assert!(
            conn.shared.current.lock().unwrap().active,
            "even completed channel-zero activity in the window must acquit the socket"
        );
    }

    #[tokio::test]
    async fn stale_probe_cannot_convict_replacement_generation() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::new()));
        let (write_tx, mut write_rx) = mpsc::channel(4);
        let conn = test_connection(write_tx, pending, routes, ConnToken(1), 1);
        let old_runtime = {
            let state = conn.shared.current.lock().unwrap();
            Arc::clone(&state.runtime)
        };

        conn.spawn_liveness_probe(&old_runtime);
        let ping = write_rx.recv().await.expect("probe must queue its Ping");
        old_runtime
            .write_confirmations
            .lock()
            .unwrap()
            .remove(&ping.header.corr)
            .unwrap()
            .send(Ok(Instant::now()))
            .unwrap();
        wait_for_probe_window(&conn).await;

        let (new_write_tx, mut new_write_rx) = mpsc::channel(4);
        let replacement = Arc::new(ConnectionRuntime {
            write_tx: new_write_tx,
            pending: Arc::new(Mutex::new(HashMap::new())),
            route_epochs: Arc::new(Mutex::new(HashMap::from([(17, 3)]))),
            write_confirmations: Arc::new(Mutex::new(HashMap::new())),
            conn: ConnToken(2),
            shutdown: CancellationToken::new(),
            capabilities: tokio::sync::OnceCell::new(),
        });
        let mut replacement_state = GenerationState::new(Arc::clone(&replacement));
        replacement_state.last_inbound = Instant::now() - Duration::from_secs(1);
        replacement_state.probe_in_flight = true;
        *conn.shared.current.lock().unwrap() = replacement_state;
        tokio::time::sleep(Duration::from_millis(60)).await;

        let state = conn.shared.current.lock().unwrap();
        assert_eq!(state.runtime.conn, ConnToken(2));
        assert!(state.active);
        assert_eq!(
            state.runtime.route_epochs.lock().unwrap().get(&17),
            Some(&3)
        );
        drop(state);
        assert!(new_write_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn in_flight_tool_call_is_not_resent_across_generation_swap() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
        let (old_write_tx, mut old_write_rx) = mpsc::channel(4);
        let conn = Arc::new(test_connection(
            old_write_tx,
            pending,
            routes,
            ConnToken(1),
            1,
        ));
        let route = RouteHandle::new(9, 1, ConnToken(1));
        let call = {
            let conn = Arc::clone(&conn);
            tokio::spawn(async move {
                conn.tool_call(&route, "mutate", &serde_json::json!({"value": 1}), None)
                    .await
            })
        };
        let first = old_write_rx
            .recv()
            .await
            .expect("the tool dispatch must enter generation one");
        assert_eq!(first.header.ty, FrameType::Request);

        let (new_write_tx, mut new_write_rx) = mpsc::channel(4);
        let replacement = Arc::new(ConnectionRuntime {
            write_tx: new_write_tx,
            pending: Arc::new(Mutex::new(HashMap::new())),
            route_epochs: Arc::new(Mutex::new(HashMap::new())),
            write_confirmations: Arc::new(Mutex::new(HashMap::new())),
            conn: ConnToken(2),
            shutdown: CancellationToken::new(),
            capabilities: tokio::sync::OnceCell::new(),
        });
        {
            let mut state = conn.shared.current.lock().unwrap();
            assert!(deactivate_locked(&mut state));
            *state = GenerationState::new(replacement);
        }

        let error = call
            .await
            .unwrap()
            .expect_err("the old generation's pending result is ambiguous");
        assert!(
            matches!(error, ConnError::Transport(message) if message == "connection closed mid-call")
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(60), new_write_rx.recv())
                .await
                .is_err(),
            "an in-flight tool request must never be replayed on the replacement socket"
        );
    }

    /// VIRTUAL TIME, because this counts attempts inside a fixed window and that window used
    /// to be wall-clock. On a shared box the supervisor task competes with every other seat's
    /// build for CPU, so a starved task completes FEWER backoff attempts and the assertion
    /// fails for a reason that has nothing to do with backoff: measured 5 failures in 6 runs
    /// with six concurrent cargo processes, every one reporting `got 1`.
    ///
    /// A timing test that is correct only when the machine is idle trains its readers to
    /// re-run rather than to read it, which is how a real regression in the reconnect path
    /// would arrive unnoticed. Paused time makes the count a function of the BACKOFF SCHEDULE
    /// alone: tokio auto-advances the clock once every task is idle, so the window below
    /// advances over the supervisor's own 100ms/200ms/400ms and over nothing else.
    ///
    /// WHAT A REAL SPIN LOOKS LIKE HERE, measured by removing the backoff: the test HANGS
    /// rather than reporting a large count. Paused time only advances when every task is
    /// idle, and a zero-backoff supervisor is never idle, so the clock stops. That is still
    /// a failure and still catches the defect -- but it arrives as a CI timeout rather than
    /// as `got 847`, so a reader who sees this test hang should suspect the backoff itself
    /// before suspecting the harness.
    #[tokio::test(start_paused = true)]
    async fn unreachable_reconnect_attempts_back_off_instead_of_spinning() {
        let (write_tx, _write_rx) = mpsc::channel(1);
        let runtime = Arc::new(ConnectionRuntime {
            write_tx,
            pending: Arc::new(Mutex::new(HashMap::new())),
            route_epochs: Arc::new(Mutex::new(HashMap::new())),
            write_confirmations: Arc::new(Mutex::new(HashMap::new())),
            conn: ConnToken(77),
            shutdown: CancellationToken::new(),
            capabilities: tokio::sync::OnceCell::new(),
        });
        let (reconnect_tx, reconnect_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(ConnectionShared {
            connection_file_path: broca_test_support::scratch_root().join(format!(
                "broca-missing-connection-file-{}",
                std::process::id()
            )),
            current: Mutex::new(GenerationState::new(runtime)),
            corr: AtomicU64::new(1),
            reconnect_tx,
            probe_timeout: Duration::from_millis(25),
            route_open_bind_relay_grace: Duration::from_millis(50),
            consumer_identity: None,
            reconnect_attempts: AtomicU64::new(0),
            probe_windows_started: AtomicU64::new(0),
        });
        {
            let mut state = shared.current.lock().unwrap();
            assert!(deactivate_locked(&mut state));
        }
        let supervisor = tokio::spawn(reconnect_supervisor(Arc::downgrade(&shared), reconnect_rx));
        shared.reconnect_tx.send(ConnToken(77)).unwrap();
        tokio::time::sleep(Duration::from_millis(550)).await;

        let attempts = shared.reconnect_attempts.load(Ordering::Relaxed);
        assert!(
            (2..=4).contains(&attempts),
            "exponential backoff should permit only a few attempts, got {attempts}"
        );
        supervisor.abort();
    }

    #[test]
    fn reconnect_backoff_doubles_to_a_cap() {
        assert_eq!(reconnect_backoff(1), Duration::from_millis(100));
        assert_eq!(reconnect_backoff(2), Duration::from_millis(200));
        assert_eq!(reconnect_backoff(3), Duration::from_millis(400));
        assert_eq!(reconnect_backoff(64), RECONNECT_MAX_BACKOFF);
    }

    /// Under a scope hold, the two scope refusals are held to the hold's deadline even when the
    /// ordinary budget is shorter, while a module refusal keeps the ordinary budget.
    #[tokio::test(start_paused = true)]
    async fn the_scope_hold_governs_only_the_scope_refusals() {
        let hold = ScopeHold {
            deadline: time::Instant::now() + Duration::from_secs(10),
            max_interval: Duration::from_secs(2),
        };
        for (code, expected_at_least) in [
            ("scope_not_synced", Duration::from_secs(10)),
            ("module_warming", Duration::ZERO),
        ] {
            let attempts = AtomicUsize::new(0);
            let started = time::Instant::now();
            let result = retry_route_open_with_scope_hold(
                RouteOpenRetry::within(Duration::from_millis(300), Duration::from_millis(100)),
                Some(ScopeHold {
                    deadline: time::Instant::now() + Duration::from_secs(10),
                    ..hold
                }),
                None,
                no_gate,
                || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        Err(ConnError::Rejected(RejectedMessage::structured(
                            code.into(),
                            "not yet".into(),
                        )))
                    }
                },
            )
            .await;
            let elapsed = started.elapsed();
            assert!(matches!(
                result,
                Err(RouteOpenError::Conn(ConnError::Rejected(_)))
            ));
            assert!(elapsed >= expected_at_least, "{code}: {elapsed:?}");
            if code == "module_warming" {
                assert!(elapsed < Duration::from_secs(1), "{code}: {elapsed:?}");
            } else {
                assert!(elapsed < Duration::from_secs(11), "{code}: {elapsed:?}");
            }
            assert!(attempts.load(Ordering::SeqCst) > 1, "{code} was retried");
        }
    }

    /// Every Cancel frame the connection wrote within `quiet` of the last frame it wrote.
    /// Other frames (a liveness Ping after a timeout, the Request itself) are skipped.
    async fn written_cancels(write_rx: &mut mpsc::Receiver<Frame>, quiet: Duration) -> Vec<Frame> {
        let mut cancels = Vec::new();
        while let Ok(Some(frame)) = tokio::time::timeout(quiet, write_rx.recv()).await {
            if frame.header.ty == FrameType::Cancel {
                cancels.push(frame);
            }
        }
        cancels
    }

    fn assert_cancel_for(cancel: &Frame, channel: u16, epoch: u32, corr: u64) {
        assert_eq!(
            (
                cancel.header.channel,
                cancel.header.epoch,
                cancel.header.corr
            ),
            (channel, epoch, corr),
            "the Cancel must name the abandoned call's route generation and corr"
        );
        assert!(
            cancel.body.is_empty(),
            "a data-plane Cancel carries no body"
        );
    }

    /// A tool call whose reply deadline expires tells the provider to stop. Without the
    /// Cancel the provider keeps running the call with nobody waiting for it, which once held
    /// a project lock for minutes after broca had given up.
    ///
    /// The provider's late Response is then dropped by the real reader as an unmatched
    /// terminal, and the recorded error is unchanged: still the timeout.
    #[tokio::test]
    async fn a_tool_call_past_its_deadline_sends_one_cancel_for_its_corr() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
        let (write_tx, mut write_rx) = mpsc::channel(16);
        let conn = test_connection(
            write_tx.clone(),
            Arc::clone(&pending),
            Arc::clone(&routes),
            ConnToken(1),
            1,
        );
        let route = RouteHandle::new(9, 1, ConnToken(1));
        let (mut client, server) = tokio::io::duplex(4096);
        let reader = tokio::spawn(reader_task(
            server,
            Arc::clone(&pending),
            Arc::clone(&routes),
            write_tx,
            None,
        ));

        let call = tokio::spawn(async move {
            conn.tool_call_with_deadline(
                &route,
                "grep",
                &serde_json::json!({}),
                None,
                Duration::from_millis(50),
            )
            .await
        });
        let request = next_written(&mut write_rx, "the tool call").await;
        assert_eq!(request.header.ty, FrameType::Request);
        let corr = request.header.corr;

        let error = tokio::time::timeout(Duration::from_secs(1), call)
            .await
            .expect("the call gives up at its deadline")
            .unwrap()
            .expect_err("no terminal arrived");
        assert!(
            matches!(&error, ConnError::Transport(message) if message.starts_with("timed out after")),
            "the caller still sees the timeout, not anything the Cancel implies: {error:?}"
        );

        let cancels = written_cancels(&mut write_rx, Duration::from_millis(100)).await;
        assert_eq!(
            cancels.len(),
            1,
            "exactly one Cancel follows the expired deadline"
        );
        assert_cancel_for(&cancels[0], 9, 1, corr);

        // The timeout also starts a liveness probe, and with no writer task here to confirm its
        // Ping the probe retires this test generation, which forgets the route. Put the route
        // back so the late reply is judged by the waiter lookup rather than the epoch gate.
        routes.lock().unwrap().insert(9, 1);
        let (_, unmatched_before) = FRAME_DROPS.counts();
        let late = Frame::build(
            FrameType::Response,
            data_flags(),
            9,
            1,
            corr,
            br#"{"content":[],"isError":false}"#.to_vec(),
        )
        .unwrap();
        write_frame(&mut client, &late).await.unwrap();
        drop(client);
        reader.await.unwrap();
        let (_, unmatched_after) = FRAME_DROPS.counts();
        assert!(
            unmatched_after > unmatched_before,
            "a Response that crossed the Cancel is dropped and counted as an unmatched terminal"
        );
    }

    /// Dropping a sent call (its run task aborted, its caller cancelled) also tells the
    /// provider to stop, and does so exactly once.
    #[tokio::test]
    async fn dropping_a_sent_tool_call_sends_exactly_one_cancel() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
        let (write_tx, mut write_rx) = mpsc::channel(16);
        let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
        let route = RouteHandle::new(9, 1, ConnToken(1));

        let call = tokio::spawn(async move {
            conn.tool_call_with_deadline(
                &route,
                "grep",
                &serde_json::json!({}),
                None,
                Duration::from_secs(60),
            )
            .await
        });
        let request = next_written(&mut write_rx, "the tool call").await;
        assert_eq!(request.header.ty, FrameType::Request);
        let corr = request.header.corr;

        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());

        let cancels = written_cancels(&mut write_rx, Duration::from_millis(100)).await;
        assert_eq!(cancels.len(), 1, "exactly one Cancel follows the drop");
        assert_cancel_for(&cancels[0], 9, 1, corr);
        assert!(
            pending.lock().unwrap().is_empty(),
            "the dropped call's waiter is gone, so a late reply finds nobody"
        );
    }

    /// A call the provider answered has nothing left to cancel, whether the answer was a
    /// Response or an Error.
    #[tokio::test]
    async fn a_tool_call_answered_before_its_deadline_sends_no_cancel() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
        let (write_tx, mut write_rx) = mpsc::channel(16);
        let conn = Arc::new(test_connection(
            write_tx.clone(),
            Arc::clone(&pending),
            Arc::clone(&routes),
            ConnToken(1),
            1,
        ));
        let route = RouteHandle::new(9, 1, ConnToken(1));
        let (mut client, server) = tokio::io::duplex(4096);
        let _reader = tokio::spawn(reader_task(
            server,
            Arc::clone(&pending),
            Arc::clone(&routes),
            write_tx,
            None,
        ));

        for (terminal_ty, body) in [
            (
                FrameType::Response,
                br#"{"content":[],"isError":false}"#.to_vec(),
            ),
            (
                FrameType::Error,
                br#"{"code":"tool_failed","message":"no"}"#.to_vec(),
            ),
        ] {
            let call = {
                let conn = Arc::clone(&conn);
                tokio::spawn(async move {
                    conn.tool_call_with_deadline(
                        &route,
                        "grep",
                        &serde_json::json!({}),
                        None,
                        Duration::from_secs(5),
                    )
                    .await
                })
            };
            let request = next_written(&mut write_rx, "the tool call").await;
            assert_eq!(request.header.ty, FrameType::Request);
            let terminal =
                Frame::build(terminal_ty, data_flags(), 9, 1, request.header.corr, body).unwrap();
            write_frame(&mut client, &terminal).await.unwrap();
            let result = tokio::time::timeout(Duration::from_secs(1), call)
                .await
                .expect("the call completes on its terminal")
                .unwrap();
            assert_eq!(result.is_ok(), terminal_ty == FrameType::Response);

            let cancels = written_cancels(&mut write_rx, Duration::from_millis(100)).await;
            assert!(
                cancels.is_empty(),
                "a call answered with {terminal_ty:?} must not be cancelled: {cancels:?}"
            );
        }
    }

    /// A call dropped while its Request is still waiting for room in the writer queue was never
    /// sent, so the provider has no work to stop and must not be sent a Cancel for a corr it
    /// has never seen.
    #[tokio::test]
    async fn a_tool_call_dropped_before_its_request_is_sent_sends_no_cancel() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let routes: RouteEpochs = Arc::new(Mutex::new(HashMap::from([(9, 1)])));
        // A one-slot queue already holding a frame: the call's send blocks until it is dropped.
        let (write_tx, mut write_rx) = mpsc::channel(1);
        write_tx
            .send(frame(FrameType::Push, 9, 1, 0))
            .await
            .unwrap();
        let conn = test_connection(write_tx, Arc::clone(&pending), routes, ConnToken(1), 1);
        let route = RouteHandle::new(9, 1, ConnToken(1));

        let call = tokio::spawn(async move {
            conn.tool_call_with_deadline(
                &route,
                "grep",
                &serde_json::json!({}),
                None,
                Duration::from_secs(60),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while pending.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the call registers its waiter before blocking on the full queue");
        // Give the call a moment to reach the blocked send.
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Free the queue's slot, then abort before the call's task runs again. The slot is
        // free when the call is dropped, so a Cancel wrongly enqueued for this unsent call would
        // land in it and be seen below; with the slot still full it would fail silently and
        // this test could not tell the difference. (The test runtime is single-threaded, so
        // the call cannot be polled between this receive and the abort.)
        let filler = write_rx
            .try_recv()
            .expect("the frame that filled the queue");
        assert_eq!(filler.header.ty, FrameType::Push);
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        let mut later = Vec::new();
        while let Ok(Some(frame)) =
            tokio::time::timeout(Duration::from_millis(100), write_rx.recv()).await
        {
            later.push(frame.header.ty);
        }
        assert!(
            later.is_empty(),
            "neither the Request nor a Cancel may be written for a call that was never sent: \
             {later:?}"
        );
    }
}

#[cfg(test)]
mod approval_raw_tests {
    use super::*;

    #[tokio::test]
    async fn raw_envelope_has_parity_and_preserves_saved_argument_bytes() {
        let (conn, route, mut module) = SubcConnection::in_process_for_test(5, 3);
        let arguments = serde_json::json!({"a":1,"b":"x"});
        let (result, ordinary) = tokio::join!(
            conn.tool_call_with_preset(
                &route,
                "edit",
                &arguments,
                Some("c"),
                Duration::from_secs(1),
                Some("head")
            ),
            async {
                let request = module.next_request().await;
                let body = request.body.clone();
                assert!(module.reply(&request, br#"{"content":[]}"#.to_vec()));
                body
            }
        );
        result.unwrap();
        let argument_bytes = arguments.to_string();
        let (result, raw) = tokio::join!(
            conn.tool_call_raw_with_preset(
                &route,
                "edit",
                &argument_bytes,
                Some("c"),
                Duration::from_secs(1),
                Some("head"),
                None
            ),
            async {
                let request = module.next_request().await;
                let body = request.body.clone();
                assert!(module.reply(&request, br#"{"content":[]}"#.to_vec()));
                body
            }
        );
        result.unwrap();
        assert_eq!(
            ordinary, raw,
            "the raw carrier changes no other envelope field"
        );

        let saved = r#"{"n":1e+30,"a":2}"#;
        let parsed: serde_json::Value = serde_json::from_str(saved).unwrap();
        assert_ne!(parsed.to_string(), saved);
        let (result, ()) = tokio::join!(
            conn.tool_call_raw_with_preset(
                &route,
                "edit",
                saved,
                Some("c"),
                Duration::from_secs(1),
                Some("head"),
                Some("lineage:7"),
            ),
            async {
                let request = module.next_request().await;
                let body = String::from_utf8(request.body.clone()).unwrap();
                assert!(
                    body.contains(&format!("\"arguments\":{saved}")),
                    "provider receives saved bytes verbatim"
                );
                let value: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                assert_eq!(value["preset"], "head");
                assert_eq!(value["call_key"], "lineage:7");
                assert!(value.get("approval_digest").is_none());
                assert!(module.reply(&request, br#"{"content":[]}"#.to_vec()));
            }
        );
        result.unwrap();
    }

    #[tokio::test]
    async fn malformed_saved_arguments_fail_closed_before_a_frame_is_sent() {
        let (conn, route, mut module) = SubcConnection::in_process_for_test(5, 3);
        for saved in ["{", "{} {}", "null,42"] {
            assert!(matches!(
                conn.tool_call_raw_with_preset(
                    &route,
                    "edit",
                    saved,
                    Some("c"),
                    Duration::from_secs(1),
                    None,
                    None,
                )
                .await,
                Err(ConnError::Protocol(_))
            ));
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(10), module.next_request())
                .await
                .is_err()
        );
    }
}

#[cfg(test)]
mod approved_enqueue_tests {
    use super::*;

    #[tokio::test]
    async fn approved_push_rechecks_cancel_and_stop_after_capacity_is_reserved() {
        for cancel in [true, false] {
            let (tx, mut rx) = mpsc::channel(1);
            let token = CancellationToken::new();
            let signals = broca_core::tool_plane::DispatchSignals::new(
                if cancel { Some(&token) } else { None },
                if cancel { None } else { Some(&token) },
            );
            let permit = tx.reserve().await.unwrap();
            token.cancel();
            let frame =
                Frame::build(FrameType::Request, data_flags(), 5, 3, 1, br#"{}"#.to_vec()).unwrap();
            assert!(matches!(
                SubcConnection::push_approved(permit, frame, &signals),
                Err(ApprovedSetupError::Interrupted)
            ));
            assert!(rx.try_recv().is_err());
        }
    }
}

/// Route opens to a module that is down: how one open's attempts are spaced, and how the
/// module's availability gate paces the opens of every route to it. Paused clock throughout,
/// so every wait is virtual and every count exact.
#[cfg(test)]
mod module_gate_tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use subc_protocol::error_codes::TARGET_UNAVAILABLE;

    fn refused(code: &str) -> ConnError {
        ConnError::Rejected(RejectedMessage::structured(code.into(), "not now".into()))
    }

    /// No jitter: each wait is its whole envelope, so the schedule is exact.
    fn whole_envelope(envelope: Duration) -> Duration {
        envelope
    }

    fn patient(budget: Duration) -> RouteOpenRetry {
        RouteOpenRetry::within(budget, ROUTE_OPEN_RETRY_MAX_INTERVAL)
    }

    /// A gate closed by one availability refusal just now; its first probe turn is 100 ms out.
    fn closed_gate(retired: CancellationToken) -> Arc<ModuleGate> {
        let gate = Arc::new(ModuleGate::with_jitter(whole_envelope, retired));
        gate.observe(&Err(refused(TARGET_UNAVAILABLE)), 0, None);
        assert!(gate.is_closed());
        gate
    }

    async fn admitted(gate: &Arc<ModuleGate>) -> GateTicket {
        match gate.admit(None, None).await {
            GateAdmission::Admitted(ticket) => ticket,
            _ => panic!("the gate must admit this open"),
        }
    }

    #[test]
    fn the_default_patience_backs_off_to_five_seconds_within_three() {
        let retry = RouteOpenRetry::default_patience();
        assert_eq!(retry.budget, Duration::from_secs(3));
        assert_eq!(retry.max_interval, Duration::from_secs(5));
    }

    /// Full jitter stays inside its envelope and actually varies: a wait never exceeds the
    /// envelope, and draws spread over it rather than sitting at one value.
    #[test]
    fn full_jitter_draws_spread_over_the_envelope_and_never_exceed_it() {
        let envelope = Duration::from_secs(5);
        let draws: Vec<Duration> = (0..1000).map(|_| full_jitter(envelope)).collect();
        assert!(draws.iter().all(|wait| *wait <= envelope));
        assert!(draws.iter().any(|wait| *wait < Duration::from_secs(1)));
        assert!(draws.iter().any(|wait| *wait > Duration::from_secs(4)));
    }

    /// One open against a module that stays down: the waits between attempts double from
    /// 100 ms and then hold at 5 s. They never shrink, except the last, which is cut short so
    /// that no wait runs past the open's deadline.
    #[tokio::test(start_paused = true)]
    async fn the_wait_between_refused_attempts_doubles_to_five_seconds() {
        let start = time::Instant::now();
        let started_at = Mutex::new(Vec::new());
        let result = retry_route_open_jittered(
            patient(Duration::from_secs(30)),
            None,
            None,
            no_gate,
            &whole_envelope,
            || {
                started_at.lock().unwrap().push(start.elapsed());
                async { Err(refused(TARGET_UNAVAILABLE)) }
            },
        )
        .await;
        assert!(matches!(
            result,
            Err(RouteOpenError::Conn(ConnError::Rejected(_)))
        ));
        let started_at = started_at.into_inner().unwrap();
        let waits: Vec<Duration> = started_at
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .collect();
        let ms = Duration::from_millis;
        assert_eq!(
            waits[..7],
            [
                ms(100),
                ms(200),
                ms(400),
                ms(800),
                ms(1600),
                ms(3200),
                ms(5000)
            ],
            "the waits must double from 100 ms up to the 5 s cap: {waits:?}"
        );
        assert!(waits
            .iter()
            .all(|wait| *wait <= ROUTE_OPEN_RETRY_MAX_INTERVAL));
        let (_, before_last) = waits.split_last().unwrap();
        assert!(
            before_last.windows(2).all(|pair| pair[1] >= pair[0]),
            "{waits:?}"
        );
        assert_eq!(
            *started_at.last().unwrap(),
            Duration::from_secs(30),
            "the last attempt is made at the deadline, not after it"
        );
    }

    /// Only evidence that the module is up opens the gate: a route, or a definite answer from
    /// the daemon that is not about availability. A transport or protocol failure (broca's own
    /// attempt timeout is a transport failure) says nothing either way, and a scope refusal is
    /// about one caller's scope, so none of those opens it, and a scope refusal never closes it.
    #[tokio::test(start_paused = true)]
    async fn only_evidence_that_the_module_is_up_opens_the_gate() {
        let gate = ModuleGate::with_jitter(whole_envelope, CancellationToken::new());
        gate.observe(&Err(refused("scope_not_synced")), 0, None);
        assert!(!gate.is_closed(), "a scope refusal must not close the gate");
        gate.observe(&Err(refused(TARGET_UNAVAILABLE)), 0, None);
        assert!(gate.is_closed());
        for inconclusive in [
            ConnError::Transport("route.open got no reply within 15s".into()),
            ConnError::Protocol("unexpected route.open frame".into()),
            refused("scope_changed"),
        ] {
            gate.observe(&Err(inconclusive), 1, None);
            assert!(
                gate.is_closed(),
                "only evidence that the module is up opens the gate"
            );
        }
        gate.observe(&Err(refused("unknown_module")), 1, None);
        assert!(
            !gate.is_closed(),
            "a definite answer means the daemon reached the module"
        );
        gate.observe(&Err(refused("module_warming")), 2, None);
        assert!(gate.is_closed());
        gate.observe(&Ok(RouteHandle::synthetic(1)), 3, None);
        assert!(!gate.is_closed());
    }

    /// A probe that fails with a transport error leaves the gate closed, and the next probe
    /// tick tests the module again.
    #[tokio::test(start_paused = true)]
    async fn a_transport_error_keeps_the_gate_closed_and_the_next_tick_probes_again() {
        let gate = closed_gate(CancellationToken::new());
        let start = time::Instant::now();
        // The first caller is the probe at 100 ms, and the daemon never answers it.
        let ticket = {
            time::sleep(Duration::from_millis(100)).await;
            admitted(&gate).await
        };
        ticket.report(&Err(ConnError::Transport(
            "route.open got no reply within 15s (ROUTE_OPEN_ATTEMPT_TIMEOUT)".into(),
        )));
        assert!(
            gate.is_closed(),
            "a transport error does not show the module is up"
        );
        let calls = AtomicUsize::new(0);
        let result = retry_route_open(
            patient(Duration::from_secs(30)),
            None,
            || Some(Arc::clone(&gate)),
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(RouteHandle::synthetic(7)) }
            },
        )
        .await;
        assert_eq!(result.unwrap().channel, 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            start.elapsed(),
            Duration::from_millis(300),
            "the next probe goes at the next tick (100 ms + a 200 ms envelope)"
        );
        assert!(!gate.is_closed());
    }

    /// A caller waiting at a closed gate leaves at its own deadline with the module's refusal,
    /// without an attempt of its own reaching the daemon.
    #[tokio::test(start_paused = true)]
    async fn a_caller_waiting_at_a_closed_gate_leaves_at_its_own_deadline() {
        let gate = closed_gate(CancellationToken::new());
        time::sleep(Duration::from_secs(1)).await;
        // Someone else's probe is running and never ends during this test.
        let probe = admitted(&gate).await;
        let calls = AtomicUsize::new(0);
        let start = time::Instant::now();
        let result = retry_route_open(
            patient(Duration::from_secs(2)),
            None,
            || Some(Arc::clone(&gate)),
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(RouteHandle::synthetic(1)) }
            },
        )
        .await;
        assert_eq!(start.elapsed(), Duration::from_secs(2));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            result,
            Err(RouteOpenError::Conn(ConnError::Rejected(message))) if message.code() == TARGET_UNAVAILABLE
        ));
        drop(probe);
    }

    /// The caller holding the probe turn is dropped in the middle of its attempt (an outer
    /// timeout): the turn passes at once to a caller still waiting, which probes and opens.
    #[tokio::test(start_paused = true)]
    async fn a_probe_holder_dropped_mid_attempt_hands_the_turn_on() {
        let gate = closed_gate(CancellationToken::new());
        let start = time::Instant::now();
        let gate_for = || Some(Arc::clone(&gate));
        // Takes the probe turn at 100 ms; its open never answers, and it is given up at 1 s.
        let abandoned = time::timeout(
            Duration::from_secs(1),
            retry_route_open(patient(Duration::from_secs(30)), None, gate_for, || {
                std::future::pending::<ConnResult<RouteHandle>>()
            }),
        );
        let calls = AtomicUsize::new(0);
        let waiting = async {
            // Arrives while the probe is running, so it waits for the turn.
            time::sleep(Duration::from_millis(150)).await;
            let result = retry_route_open(patient(Duration::from_secs(30)), None, gate_for, || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(RouteHandle::synthetic(7)) }
            })
            .await;
            (result, start.elapsed())
        };
        let (abandoned, (result, opened_at)) = tokio::join!(abandoned, waiting);
        assert!(abandoned.is_err(), "the probe holder was given up");
        assert_eq!(result.unwrap().channel, 7);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            opened_at,
            Duration::from_secs(1),
            "the waiting caller takes the turn the moment the holder is dropped"
        );
    }

    /// A probe from an earlier closed period that is dropped late releases only its own turn:
    /// the current period's probe keeps running alone, so a newcomer is not given a second one.
    #[tokio::test(start_paused = true)]
    async fn a_stale_probe_drop_does_not_let_two_probes_run() {
        let gate = closed_gate(CancellationToken::new());
        time::sleep(Duration::from_millis(100)).await;
        let stale = admitted(&gate).await;
        assert!(stale.probe.is_some());
        // Another open reports a route (the gate reopens), then one admitted in the new open
        // period is refused for availability (the gate closes again).
        gate.observe(&Ok(RouteHandle::synthetic(1)), 1, None);
        assert!(!gate.is_closed());
        let ordinary = admitted(&gate).await;
        ordinary.report(&Err(refused(TARGET_UNAVAILABLE)));
        assert!(gate.is_closed());
        time::sleep(Duration::from_millis(100)).await;
        let current = admitted(&gate).await;
        assert!(current.probe.is_some());
        drop(stale);
        assert!(
            matches!(gate.admit(None, None).await, GateAdmission::Refused(_)),
            "the current probe is still running, so no second probe may start"
        );
        drop(current);
    }

    /// An availability refusal from an open admitted before the gate's last reopen is older
    /// news than what reopened it, so it does not close the gate again.
    #[tokio::test(start_paused = true)]
    async fn a_stale_refusal_does_not_close_a_reopened_gate() {
        let gate = Arc::new(ModuleGate::with_jitter(
            whole_envelope,
            CancellationToken::new(),
        ));
        let early = admitted(&gate).await;
        gate.observe(&Err(refused(TARGET_UNAVAILABLE)), 0, None);
        gate.observe(&Ok(RouteHandle::synthetic(1)), 1, None);
        assert!(!gate.is_closed());
        early.report(&Err(refused(TARGET_UNAVAILABLE)));
        assert!(
            !gate.is_closed(),
            "a refusal older than the reopen must not close it"
        );
    }

    /// A reconnect while callers wait at a closed gate: the old generation's gate retires, and
    /// its waiters move to the new generation's gate instead of sleeping to their deadline and
    /// returning the old generation's refusal. Here the new generation's gate is itself closed
    /// with another caller's probe running, so the moved waiter must make no attempt until that
    /// gate admits it.
    #[tokio::test(start_paused = true)]
    async fn a_reconnect_lets_waiters_on_the_old_gate_open_on_the_new_one() {
        let start = time::Instant::now();
        let old_generation = CancellationToken::new();
        let old_gate = closed_gate(old_generation.clone());
        let new_gate = closed_gate(CancellationToken::new());
        let current = Mutex::new(Arc::clone(&old_gate));
        time::sleep(Duration::from_millis(100)).await;
        // A probe is running on each gate, so neither gives the waiter a turn of its own.
        let old_probe = admitted(&old_gate).await;
        let new_probe = admitted(&new_gate).await;
        let attempts_at = Mutex::new(Vec::new());
        let waiter = retry_route_open(
            patient(Duration::from_secs(30)),
            None,
            || Some(Arc::clone(&current.lock().unwrap())),
            || {
                attempts_at.lock().unwrap().push(start.elapsed());
                async { Ok(RouteHandle::synthetic(9)) }
            },
        );
        let reconnect = async {
            time::sleep(Duration::from_millis(900)).await;
            // The replacement generation's gate is published, then the old generation ends.
            *current.lock().unwrap() = Arc::clone(&new_gate);
            old_generation.cancel();
            time::sleep(Duration::from_secs(1)).await;
            // The new generation's probe finds the module up, which opens its gate.
            new_probe.report(&Ok(RouteHandle::synthetic(1)));
        };
        let (result, ()) = tokio::join!(
            async {
                let result = waiter.await;
                (result, start.elapsed())
            },
            reconnect
        );
        let (result, opened_at) = result;
        assert_eq!(
            result
                .expect("the waiter opens on the new generation")
                .channel,
            9
        );
        assert_eq!(
            attempts_at.into_inner().unwrap(),
            [Duration::from_secs(2)],
            "one attempt, made only once the new generation's gate admitted it"
        );
        assert_eq!(opened_at, Duration::from_secs(2));
        drop(old_probe);
    }

    /// A waiter whose deadline passes while the gate is closed makes no attempt, even when the
    /// gate has opened by the time it next runs: its budget is spent.
    #[tokio::test(start_paused = true)]
    async fn a_waiter_past_its_deadline_makes_no_attempt_when_the_gate_reopens() {
        let gate = closed_gate(CancellationToken::new());
        time::sleep(Duration::from_millis(100)).await;
        // Someone else's probe is running, so the waiter has no turn coming.
        let probe = admitted(&gate).await;
        let calls = AtomicUsize::new(0);
        let mut waiter = std::pin::pin!(retry_route_open(
            patient(Duration::from_secs(1)),
            None,
            || Some(Arc::clone(&gate)),
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(RouteHandle::synthetic(1)) }
            },
        ));
        assert!(
            futures::poll!(&mut waiter).is_pending(),
            "the waiter waits at the gate"
        );
        // The deadline passes and then the gate opens, all before the waiter runs again.
        time::advance(Duration::from_secs(2)).await;
        probe.report(&Ok(RouteHandle::synthetic(2)));
        assert!(!gate.is_closed());
        let result = waiter.await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "no attempt after the deadline"
        );
        assert!(matches!(
            result,
            Err(RouteOpenError::Conn(ConnError::Rejected(message))) if message.code() == TARGET_UNAVAILABLE
        ));
    }

    /// Fifty opens for fifty DIFFERENT routes to one module stopped for ten seconds, arriving
    /// one every 10 ms: the module sees the first open and then one probe per backoff tick in
    /// total, not one per route, and every route opens once the module is back.
    #[tokio::test(start_paused = true)]
    async fn fifty_routes_to_a_stopped_module_make_one_probe_per_tick_between_them() {
        let start = time::Instant::now();
        let up_at = start + Duration::from_secs(10);
        let started_at = Mutex::new(Vec::new());
        let attempt = || {
            let now = time::Instant::now();
            started_at.lock().unwrap().push(now);
            async move {
                time::sleep(Duration::from_millis(5)).await;
                if now >= up_at {
                    Ok(RouteHandle::synthetic(7))
                } else {
                    Err(refused(TARGET_UNAVAILABLE))
                }
            }
        };
        let gate = Arc::new(ModuleGate::with_jitter(
            whole_envelope,
            CancellationToken::new(),
        ));
        let gate_for = || Some(Arc::clone(&gate));
        let results = futures::future::join_all((0..50u64).map(|n| async move {
            time::sleep(Duration::from_millis(10 * n)).await;
            retry_route_open(patient(Duration::from_secs(30)), None, gate_for, attempt).await
        }))
        .await;
        let started_at = started_at.into_inner().unwrap();
        let while_down = started_at.iter().filter(|at| **at < up_at).count();
        let after_return = started_at.iter().filter(|at| **at >= up_at).count();
        // The first open, then probes at 0.1, 0.3, 0.7, 1.5, 3.1 and 6.3 s.
        assert!(
            while_down <= 8,
            "while the module is down the gate lets one probe through per tick in total, \
             got {while_down} opens"
        );
        assert_eq!(
            after_return, 50,
            "each route makes one open of its own on return"
        );
        for result in results {
            assert_eq!(
                result
                    .expect("every route opens once the module is back")
                    .channel,
                7
            );
        }
        assert!(!gate.is_closed());
    }
}
