//! ck-bus JWT custody and the real JetStream claim-check adapter.
//!
//! ck-bus keeps exactly one current user key per module: every `ckbus.credential` call mints
//! the next key and retires the one before it. So the process holds one [`CredentialHolder`],
//! made once at boot, and every NATS connection it opens authenticates through it. Two
//! holders in one process would retire each other's keys on every issue.

use crate::bus_events::{now_ms, BusConnection, BusConnector};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use broca_protocol::BindIdentity;
use broca_store::bus_events::{BusPublisher, Publication};
use broca_subc::{connection::RouteOpenRetry, SubcConnection};
use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_nats::{ConnectConfig, NatsConnection, NatsStream};
use cortexkit_bus_trait::{DurableOwner, PublishReceipt, Stream, StreamCursor};
use serde::Deserialize;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

/// ck-bus's answer when the named key is no longer the module's current one.
pub const CODE_SUPERSEDED: &str = "ckbus_credential_superseded";
/// ck-bus's renewal answer for a key it has revoked.
pub const CODE_REVOKED: &str = "ckbus_credential_revoked";
/// ck-bus's answer when the supervisor shows no live generation for this module, which
/// means this process is being replaced.
pub const CODE_GENERATION_NOT_LIVE: &str = "ckbus_generation_not_live";

/// Renew a credential this long before its JWT expires.
const RENEW_MARGIN_MS: u64 = 300_000;
/// How soon to try a renewal again after one failed while the JWT is still valid.
const RENEW_RETRY_MS: u64 = 30_000;
/// First and longest wait between failed `ckbus.credential` issues.
const ISSUE_BACKOFF_FIRST: Duration = Duration::from_secs(1);
const ISSUE_BACKOFF_CAP: Duration = Duration::from_secs(60);

// Credential bodies deliberately have no Debug implementation and are never logged.
#[derive(Clone, Deserialize)]
struct Credential {
    jwt: String,
    exp: u64,
    acct: String,
    inbox_prefix: String,
    server_url: String,
    credential_public: String,
}

impl Credential {
    fn refresh_at_ms(&self) -> u64 {
        self.exp
            .saturating_mul(1000)
            .saturating_sub(RENEW_MARGIN_MS)
    }
    fn expired(&self) -> bool {
        self.exp.saturating_mul(1000) <= now_ms()
    }
}

/// Why a ck-bus credential call failed. A refusal keeps ck-bus's own code and message, so
/// the log names the cause and the holder can tell a retired key from an outage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CkbusError {
    /// ck-bus answered with a typed refusal.
    Refused {
        method: &'static str,
        code: String,
        message: String,
    },
    /// No answer arrived in time.
    TimedOut { method: &'static str },
    /// The `ckbus` route could not be opened or the request could not be carried.
    RouteUnavailable {
        method: &'static str,
        detail: String,
    },
    /// ck-bus answered, but not in the documented shape.
    Malformed {
        method: &'static str,
        what: &'static str,
    },
    /// ck-bus reported this process's generation is not live; the holder makes no further
    /// ck-bus calls in this process.
    Stopped,
    /// A recent issue failed, and the next is not due yet.
    Backoff,
}

impl CkbusError {
    fn code(&self) -> Option<&str> {
        match self {
            Self::Refused { code, .. } => Some(code),
            _ => None,
        }
    }
    /// The named key is retired: every connection must move to a freshly issued one.
    fn key_retired(&self) -> bool {
        matches!(self.code(), Some(CODE_SUPERSEDED | CODE_REVOKED))
    }
    fn generation_not_live(&self) -> bool {
        self.code() == Some(CODE_GENERATION_NOT_LIVE)
    }
    fn method(&self) -> Option<&'static str> {
        match self {
            Self::Refused { method, .. }
            | Self::TimedOut { method }
            | Self::RouteUnavailable { method, .. }
            | Self::Malformed { method, .. } => Some(method),
            Self::Stopped | Self::Backoff => None,
        }
    }
    /// What distinguishes one cause from another for logging: the refusal code, not its
    /// message, which may carry a changing detail.
    fn cause(&self) -> String {
        match self {
            Self::Refused { code, .. } => format!("refused:{code}"),
            Self::TimedOut { .. } => "timed_out".into(),
            Self::RouteUnavailable { .. } => "route_unavailable".into(),
            Self::Malformed { what, .. } => format!("malformed:{what}"),
            Self::Stopped => "stopped".into(),
            Self::Backoff => "backoff".into(),
        }
    }
}

impl std::fmt::Display for CkbusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused {
                method,
                code,
                message,
            } => write!(f, "{method} refused with {code}: {message}"),
            Self::TimedOut { method } => write!(f, "{method} timed out"),
            Self::RouteUnavailable { method, detail } => {
                write!(f, "{method}: ckbus route unavailable: {detail}")
            }
            Self::Malformed { method, what } => write!(f, "{method}: malformed reply: {what}"),
            Self::Stopped => {
                f.write_str("ck-bus credentials stopped: this process's generation is not live")
            }
            Self::Backoff => f.write_str("ck-bus credential issue is backing off after a failure"),
        }
    }
}

impl std::error::Error for CkbusError {}

struct Credentials {
    conn: Arc<SubcConnection>,
    bind: BindIdentity,
}
#[async_trait]
trait CredentialRoute: Send + Sync {
    async fn call(
        &self,
        method: &'static str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, CkbusError>;
}
impl Credentials {
    fn new(conn: Arc<SubcConnection>) -> Self {
        Self {
            conn,
            bind: BindIdentity::new("/", "broca", "bus-events"),
        }
    }
}
#[async_trait]
impl CredentialRoute for Credentials {
    async fn call(
        &self,
        method: &'static str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, CkbusError> {
        let route = self
            .conn
            .route_open_management_within(
                "ckbus",
                &self.bind,
                RouteOpenRetry {
                    budget: Duration::from_secs(5),
                    max_interval: Duration::from_millis(250),
                },
            )
            .await
            .map_err(|error| CkbusError::RouteUnavailable {
                method,
                detail: error.to_string(),
            })?;
        let request = serde_json::to_vec(&serde_json::json!({"method":method,"params":params}))
            .map_err(|_| CkbusError::Malformed {
                method,
                what: "request could not be encoded",
            })?;
        let answer = tokio::time::timeout(
            Duration::from_secs(5),
            self.conn.route_request_terminal(&route, request),
        )
        .await;
        let _ = self.conn.route_close(&route).await;
        let body = match answer {
            Err(_) => return Err(CkbusError::TimedOut { method }),
            Ok(Err(error)) => {
                return Err(CkbusError::RouteUnavailable {
                    method,
                    detail: error.to_string(),
                })
            }
            Ok(Ok(Err(refusal))) => return Err(refusal_of(method, &refusal)),
            Ok(Ok(Ok(body))) => body,
        };
        let value: serde_json::Value =
            serde_json::from_slice(&body).map_err(|_| CkbusError::Malformed {
                method,
                what: "response is not JSON",
            })?;
        value.get("result").cloned().ok_or(CkbusError::Malformed {
            method,
            what: "response has no result",
        })
    }
}

/// A typed `ERROR` frame body as a refusal, keeping its code and message.
fn refusal_of(method: &'static str, body: &[u8]) -> CkbusError {
    match serde_json::from_slice::<subc_protocol::ErrorBody>(body) {
        Ok(error) => CkbusError::Refused {
            method,
            code: error.code,
            message: error.message,
        },
        Err(_) => CkbusError::Refused {
            method,
            code: "unstructured".into(),
            message: String::from_utf8_lossy(body).into_owned(),
        },
    }
}

async fn issue(route: &dyn CredentialRoute) -> Result<Credential, CkbusError> {
    const METHOD: &str = "ckbus.credential";
    serde_json::from_value(route.call(METHOD, serde_json::json!({})).await?).map_err(|_| {
        CkbusError::Malformed {
            method: METHOD,
            what: "credential fields missing or mistyped",
        }
    })
}
async fn renew(
    route: &dyn CredentialRoute,
    credential: &Credential,
) -> Result<Credential, CkbusError> {
    const METHOD: &str = "ckbus.credential_renew";
    let value = route
        .call(
            METHOD,
            serde_json::json!({"credential_public":credential.credential_public}),
        )
        .await?;
    if value["credential_public"].as_str() != Some(&credential.credential_public) {
        return Err(CkbusError::Malformed {
            method: METHOD,
            what: "renewal changed the credential public key",
        });
    }
    let (Some(jwt), Some(exp)) = (value["jwt"].as_str(), value["exp"].as_u64()) else {
        return Err(CkbusError::Malformed {
            method: METHOD,
            what: "renewal has no jwt or exp",
        });
    };
    let mut renewed = credential.clone();
    renewed.jwt = jwt.into();
    renewed.exp = exp;
    Ok(renewed)
}
async fn nonce_signature(
    route: &dyn CredentialRoute,
    public: &str,
    nonce: &[u8],
) -> Result<Vec<u8>, CkbusError> {
    const METHOD: &str = "ckbus.nonce_sign";
    let result = route
        .call(
            METHOD,
            serde_json::json!({"credential_public":public,"nonce_b64":STANDARD.encode(nonce)}),
        )
        .await?;
    let signature =
        result
            .get("signature_b64")
            .and_then(|v| v.as_str())
            .ok_or(CkbusError::Malformed {
                method: METHOD,
                what: "signature_b64 absent",
            })?;
    STANDARD
        .decode(signature)
        .map_err(|_| CkbusError::Malformed {
            method: METHOD,
            what: "signature_b64 is not base64",
        })
}

/// Logs each ck-bus failure once per distinct cause and method, so a connection retrying
/// every few seconds leaves one WARN line rather than one per attempt. A success on a method
/// clears its entry, so the same cause returning later is logged again.
#[derive(Default)]
struct WarnOnce(std::sync::Mutex<HashMap<&'static str, String>>);
impl WarnOnce {
    /// Returns whether this failure was logged.
    fn failed(&self, error: &CkbusError) -> bool {
        let Some(method) = error.method() else {
            return false;
        };
        let cause = error.cause();
        let mut seen = self.0.lock().unwrap();
        if seen.get(method) == Some(&cause) {
            return false;
        }
        seen.insert(method, cause);
        drop(seen);
        tracing::warn!(target: "bus", method, code = error.code().unwrap_or(""), %error, "ck-bus credential call failed");
        true
    }
    fn succeeded(&self, method: &'static str) {
        self.0.lock().unwrap().remove(method);
    }
}

struct HolderState {
    current: Option<Credential>,
    /// Set by `ckbus_generation_not_live`: this process is stale, so it stops calling ck-bus.
    stopped: bool,
    /// No issue is attempted before this, after a failed one.
    issue_after: Option<tokio::time::Instant>,
    issue_backoff: Duration,
    /// The retired key the latest re-issue replaced, so its WARN is written once.
    reissued_for: Option<String>,
}

/// The process's one ck-bus credential: issued on first use, renewed before it expires, and
/// re-issued once, for every connection at once, when ck-bus says its key is retired.
///
/// Every ck-bus call that reads or replaces the credential runs under one lock, so concurrent
/// callers that find the same key retired cause one `ckbus.credential` issue between them:
/// the first issues, and the rest find the key already replaced.
pub struct CredentialHolder {
    route: Arc<dyn CredentialRoute>,
    state: Mutex<HolderState>,
    warnings: WarnOnce,
}

impl CredentialHolder {
    /// Create the process's holder. Call it once at boot and share the result with every
    /// connection: ck-bus revokes the previous key whenever any holder issues a new one.
    pub fn new(conn: Arc<SubcConnection>) -> Arc<Self> {
        Self::with_route(Arc::new(Credentials::new(conn)))
    }

    fn with_route(route: Arc<dyn CredentialRoute>) -> Arc<Self> {
        Arc::new(Self {
            route,
            state: Mutex::new(HolderState {
                current: None,
                stopped: false,
                issue_after: None,
                issue_backoff: ISSUE_BACKOFF_FIRST,
                reissued_for: None,
            }),
            warnings: WarnOnce::default(),
        })
    }

    /// The live credential: issued if there is none, renewed when it is near expiry.
    async fn current(&self) -> Result<Credential, CkbusError> {
        let mut state = self.state.lock().await;
        if state.stopped {
            return Err(CkbusError::Stopped);
        }
        let Some(credential) = state.current.clone() else {
            return self.issue_locked(&mut state).await;
        };
        if credential.refresh_at_ms() > now_ms() {
            return Ok(credential);
        }
        match renew(self.route.as_ref(), &credential).await {
            Ok(renewed) => {
                self.warnings.succeeded("ckbus.credential_renew");
                state.current = Some(renewed.clone());
                Ok(renewed)
            }
            Err(error) => {
                self.warnings.failed(&error);
                if error.generation_not_live() {
                    self.stop(&mut state);
                    Err(error)
                } else if error.key_retired() {
                    self.retire_locked(&mut state, &credential.credential_public, &error)
                        .await
                } else if credential.expired() {
                    state.current = None;
                    self.issue_locked(&mut state).await
                } else {
                    // An outage, not a retired key: the JWT still works, and the next caller
                    // past the refresh point tries the renewal again.
                    Ok(credential)
                }
            }
        }
    }

    /// Replace `stale_public`, which ck-bus reported retired with `cause`, unless another
    /// caller already has. Every connection then moves to the one fresh key.
    async fn refetch(
        &self,
        stale_public: &str,
        cause: &CkbusError,
    ) -> Result<Credential, CkbusError> {
        let mut state = self.state.lock().await;
        if state.stopped {
            return Err(CkbusError::Stopped);
        }
        self.retire_locked(&mut state, stale_public, cause).await
    }

    async fn retire_locked(
        &self,
        state: &mut HolderState,
        stale_public: &str,
        cause: &CkbusError,
    ) -> Result<Credential, CkbusError> {
        if let Some(current) = state
            .current
            .as_ref()
            .filter(|c| c.credential_public != stale_public)
        {
            return Ok(current.clone());
        }
        state.current = None;
        if state.reissued_for.as_deref() != Some(stale_public) {
            state.reissued_for = Some(stale_public.to_owned());
            tracing::warn!(target: "bus", method = cause.method().unwrap_or(""), code = cause.code().unwrap_or(""), "ck-bus retired this process's credential; re-issuing one for every connection");
        }
        self.issue_locked(state).await
    }

    async fn issue_locked(&self, state: &mut HolderState) -> Result<Credential, CkbusError> {
        if state
            .issue_after
            .is_some_and(|at| tokio::time::Instant::now() < at)
        {
            return Err(CkbusError::Backoff);
        }
        match issue(self.route.as_ref()).await {
            Ok(credential) => {
                self.warnings.succeeded("ckbus.credential");
                state.issue_after = None;
                state.issue_backoff = ISSUE_BACKOFF_FIRST;
                state.current = Some(credential.clone());
                Ok(credential)
            }
            Err(error) => {
                self.warnings.failed(&error);
                if error.generation_not_live() {
                    self.stop(state);
                } else {
                    state.issue_after = Some(tokio::time::Instant::now() + state.issue_backoff);
                    state.issue_backoff = (state.issue_backoff * 2).min(ISSUE_BACKOFF_CAP);
                }
                Err(error)
            }
        }
    }

    fn stop(&self, state: &mut HolderState) {
        if !state.stopped {
            state.stopped = true;
            state.current = None;
            tracing::warn!(target: "bus", "ck-bus reports this process's generation is not live; no credential is issued or renewed again in this process");
        }
    }

    /// Answer a broker's connect nonce: the current JWT and the nonce signed with its key.
    /// A key ck-bus reports retired is replaced, once for every connection, and the nonce is
    /// signed again with the replacement, so a connection the broker kicked reconnects
    /// naming the current key.
    async fn authenticate(&self, nonce: &[u8]) -> Result<(Credential, Vec<u8>), CkbusError> {
        let credential = self.current().await?;
        let error = match nonce_signature(self.route.as_ref(), &credential.credential_public, nonce)
            .await
        {
            Ok(signature) => {
                self.warnings.succeeded("ckbus.nonce_sign");
                return Ok((credential, signature));
            }
            Err(error) => error,
        };
        self.warnings.failed(&error);
        if error.generation_not_live() {
            self.stop(&mut *self.state.lock().await);
            return Err(error);
        }
        if !error.key_retired() {
            return Err(error);
        }
        let fresh = self.refetch(&credential.credential_public, &error).await?;
        match nonce_signature(self.route.as_ref(), &fresh.credential_public, nonce).await {
            Ok(signature) => {
                self.warnings.succeeded("ckbus.nonce_sign");
                Ok((fresh, signature))
            }
            Err(error) => {
                self.warnings.failed(&error);
                Err(error)
            }
        }
    }
}

pub struct NatsPublisher {
    pub stream: NatsStream,
    pub account: AccountNames,
    pub connection: NatsConnection,
}
#[async_trait]
impl BusPublisher for NatsPublisher {
    async fn publish(&self, event: &Publication) -> Result<PublishReceipt, String> {
        self.stream
            .publish(
                &event.subject,
                &event.event_key,
                event.digest(),
                event.headers.clone(),
            )
            .await
            .map_err(|_| "ckbus publish failed".into())
    }
}
impl NatsPublisher {
    /// Connect using `credential`'s inbox prefix. Every handshake, including each reconnect
    /// async-nats makes on its own, asks `holder` for the JWT and nonce signature, so a
    /// reconnect presents the current key and JWT rather than the ones this connection
    /// started with.
    async fn connect(
        holder: Arc<CredentialHolder>,
        credential: &Credential,
    ) -> Result<Self, String> {
        let config = ConnectConfig::new(&credential.credential_public)
            .map_err(|_| "invalid ckbus credential public key")?;
        if config.inbox_prefix() != credential.inbox_prefix {
            return Err("ckbus inbox prefix does not match credential".into());
        }
        let options = async_nats::ConnectOptions::with_auth_callback(move |nonce| {
            let holder = holder.clone();
            // async-nats requires a Sync future; a JoinHandle wraps the Send-only subc RPC future.
            let handshake = tokio::spawn(async move { holder.authenticate(&nonce).await });
            async move {
                let (credential, signature) = handshake
                    .await
                    .map_err(async_nats::AuthError::new)?
                    .map_err(async_nats::AuthError::new)?;
                let mut auth = async_nats::Auth::new();
                auth.jwt = Some(credential.jwt);
                auth.signature = Some(signature);
                Ok(auth)
            }
        })
        .connection_timeout(Duration::from_secs(5));
        let account =
            AccountNames::derive(&credential.acct).map_err(|_| "invalid ckbus account")?;
        let conn = NatsConnection::connect(&credential.server_url, options, config)
            .await
            .map_err(|_| "ckbus connect failed")?;
        Ok(Self {
            stream: conn.stream(account.streams().event.clone()),
            account,
            connection: conn,
        })
    }
}

/// One live broker connection, bound to the key whose inbox prefix it subscribes under.
#[async_trait]
trait Link: Send + Sync {
    fn credential_public(&self) -> &str;
    async fn publish(&self, event: &Publication) -> Result<PublishReceipt, String>;
    async fn attach_compaction(&self) -> Result<Box<dyn StreamCursor>, String>;
}

/// Opens [`Link`]s; a seam so tests can stand in for the broker.
#[async_trait]
trait Dialer: Send + Sync {
    async fn dial(
        &self,
        holder: &Arc<CredentialHolder>,
        credential: &Credential,
    ) -> Result<Box<dyn Link>, String>;
}

struct NatsLink {
    publisher: NatsPublisher,
    credential_public: String,
}
#[async_trait]
impl Link for NatsLink {
    fn credential_public(&self) -> &str {
        &self.credential_public
    }
    async fn publish(&self, event: &Publication) -> Result<PublishReceipt, String> {
        self.publisher.publish(event).await
    }
    async fn attach_compaction(&self) -> Result<Box<dyn StreamCursor>, String> {
        attach_compaction(&self.publisher.connection, &self.publisher.account).await
    }
}

struct NatsDialer;
#[async_trait]
impl Dialer for NatsDialer {
    async fn dial(
        &self,
        holder: &Arc<CredentialHolder>,
        credential: &Credential,
    ) -> Result<Box<dyn Link>, String> {
        Ok(Box::new(NatsLink {
            publisher: NatsPublisher::connect(holder.clone(), credential).await?,
            credential_public: credential.credential_public.clone(),
        }))
    }
}

/// Account discovery is independent of connecting to NATS, so broker outages still queue events.
pub struct NatsConnector {
    holder: Arc<CredentialHolder>,
    dialer: Arc<dyn Dialer>,
    publisher: Mutex<Option<Arc<LazyPublisher>>>,
}
impl NatsConnector {
    /// `holder` is the process's one credential holder; see [`CredentialHolder::new`].
    pub fn new(holder: Arc<CredentialHolder>) -> Self {
        Self {
            holder,
            dialer: Arc::new(NatsDialer),
            publisher: Mutex::new(None),
        }
    }

    /// The one shared publisher, made by `connect` when there is none yet.
    async fn shared_publisher(&self) -> Result<Arc<LazyPublisher>, String> {
        if let Some(publisher) = self.publisher.lock().await.clone() {
            return Ok(publisher);
        }
        self.connect().await?;
        self.publisher
            .lock()
            .await
            .clone()
            .ok_or_else(|| "ckbus publisher absent".into())
    }
}
#[async_trait]
impl BusConnector for NatsConnector {
    async fn connect(&self) -> Result<BusConnection, String> {
        let credential = self.holder.current().await.map_err(|e| e.to_string())?;
        let account =
            AccountNames::derive(&credential.acct).map_err(|_| "invalid ckbus account")?;
        let now = now_ms();
        // A renewal that failed during an outage leaves a JWT that still works for a while;
        // come back soon to renew it rather than at a refresh point already passed.
        let refresh_at_ms = match credential.refresh_at_ms() {
            at if at > now => at,
            _ => now + RENEW_RETRY_MS,
        };
        let mut shared = self.publisher.lock().await;
        let publisher = match shared.as_ref().filter(|p| p.account == account) {
            Some(publisher) => publisher.clone(),
            None => Arc::new(LazyPublisher {
                holder: self.holder.clone(),
                dialer: self.dialer.clone(),
                account: account.clone(),
                state: Mutex::new(LazyState {
                    link: None,
                    last_public: None,
                }),
            }),
        };
        *shared = Some(publisher.clone());
        Ok(BusConnection {
            publisher,
            account,
            refresh_at_ms,
        })
    }

    async fn compaction_consumer(&self) -> Result<Box<dyn StreamCursor>, String> {
        let publisher = self.shared_publisher().await?;
        let mut state = publisher.state.lock().await;
        publisher.ensure_connected(&mut state).await?;
        let link = state.link.as_ref().ok_or("ckbus connection absent")?;
        let result = link.attach_compaction().await;
        if result.is_err() {
            state.link = None;
        }
        result
    }
}

/// Bootstrap owns topology. Attaching only reads configuration and never creates or updates it.
pub async fn attach_compaction(
    connection: &NatsConnection,
    account: &AccountNames,
) -> Result<Box<dyn StreamCursor>, String> {
    let spec = cortexkit_bus_naming::event_consumer_specs(account)
        .map_err(|e| e.to_string())?
        .into_iter()
        .find(|spec| spec.consumer.durable == "m_broca")
        .ok_or("m_broca consumer spec absent")?;
    let consumer = async_nats::jetstream::new(connection.client())
        .get_consumer_from_stream::<async_nats::jetstream::consumer::pull::Config, _, _>(
            &spec.consumer.durable,
            &spec.consumer.stream,
        )
        .await
        .map_err(|_| "m_broca consumer unavailable")?;
    let config = &consumer.cached_info().config;
    if config.durable_name.as_deref() != Some("m_broca")
        || config.deliver_subject.is_some()
        || config.ack_policy != async_nats::jetstream::consumer::AckPolicy::Explicit
        || config.deliver_policy != async_nats::jetstream::consumer::DeliverPolicy::All
        || config.ack_wait != spec.ack_wait
        || config.max_ack_pending != spec.max_ack_pending
        || config.filter_subjects != spec.consumer.filter_subjects
        || !config.filter_subject.is_empty()
    {
        return Err("m_broca consumer configuration does not match ModuleEventConsumerSpec".into());
    }
    let cursor = connection
        .stream(spec.consumer.stream)
        .consumer(DurableOwner::Module("broca"))
        .await
        .map_err(|_| "m_broca attach failed")?;
    Ok(Box::new(cursor))
}

/// The connection every publish and the compaction consumer share, opened on first use.
struct LazyPublisher {
    holder: Arc<CredentialHolder>,
    dialer: Arc<dyn Dialer>,
    account: AccountNames,
    state: Mutex<LazyState>,
}
struct LazyState {
    link: Option<Box<dyn Link>>,
    /// The key of the last link that came up, to tell a re-issued key's first connection.
    last_public: Option<String>,
}
/// Links opened by one `ensure_connected` before it gives up: the first, and one more after a
/// handshake replaced a retired key, plus one spare for a key replaced again meanwhile.
const MAX_DIALS: u32 = 3;
impl LazyPublisher {
    /// Leave `state` holding a link on the holder's current key. A link on any other key is
    /// replaced: its inbox prefix belongs to a key ck-bus has retired. The handshake can
    /// itself replace a retired key, which leaves the new link on the old key's inbox prefix,
    /// so the check repeats after each dial.
    async fn ensure_connected(&self, state: &mut LazyState) -> Result<(), String> {
        let mut dials = 0;
        loop {
            let credential = self.holder.current().await.map_err(|e| e.to_string())?;
            if credential.acct != self.account.account() {
                return Err("ckbus account changed during this process".into());
            }
            match &state.link {
                Some(link) if link.credential_public() == credential.credential_public => {
                    if state
                        .last_public
                        .as_ref()
                        .is_some_and(|last| *last != credential.credential_public)
                    {
                        tracing::warn!(target: "bus", "ck-bus connection is up on the re-issued credential");
                    }
                    state.last_public = Some(credential.credential_public);
                    return Ok(());
                }
                Some(_) => state.link = None,
                None => {}
            }
            if dials == MAX_DIALS {
                return Err("ck-bus credential kept changing while connecting".into());
            }
            dials += 1;
            state.link = Some(self.dialer.dial(&self.holder, &credential).await?);
        }
    }
}
#[async_trait]
impl BusPublisher for LazyPublisher {
    async fn publish(&self, event: &Publication) -> Result<PublishReceipt, String> {
        let mut state = self.state.lock().await;
        self.ensure_connected(&mut state).await?;
        let link = state.link.as_ref().ok_or("ckbus connection absent")?;
        let result = link.publish(event).await;
        if result.is_err() {
            // The next publish dials again on whatever key the holder then has. It never
            // issues a credential by itself: only ck-bus reporting the key retired does.
            state.link = None;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_store::{bus_events::DrainResult, SqliteStore};
    use serde_json::json;
    use std::{collections::VecDeque, sync::Mutex as StdMutex};
    type ExpectedCall = (
        &'static str,
        serde_json::Value,
        Result<serde_json::Value, CkbusError>,
    );
    struct FakeRoute(StdMutex<VecDeque<ExpectedCall>>);
    #[async_trait]
    impl CredentialRoute for FakeRoute {
        async fn call(
            &self,
            method: &'static str,
            params: serde_json::Value,
        ) -> Result<serde_json::Value, CkbusError> {
            let (expected, body, result) = self
                .0
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected credential request");
            assert_eq!(method, expected);
            assert_eq!(params, body);
            result
        }
    }
    fn refused(method: &'static str, code: &str) -> CkbusError {
        CkbusError::Refused {
            method,
            code: code.into(),
            message: format!("fixture refusal {code}"),
        }
    }
    #[tokio::test]
    async fn bus_credentials_issue_sign_and_renew_documented_ckbus_shapes() {
        let issued = json!({"jwt":"fixture-initial-jwt","exp":1900000900u64,"acct":"box_test","account_public":"A_fixture","inbox_prefix":"_INBOX.U_fixture","server_url":"nats://127.0.0.1:4222","credential_public":"U_fixture","user_jwt_id":"j1","spawn_generation":"g1","credential_epoch":1});
        let renewed = json!({"jwt":"fixture-renewed-jwt","exp":1900001800u64,"user_jwt_id":"j2","credential_public":"U_fixture","spawn_generation":"g1","credential_epoch":1});
        let route = FakeRoute(StdMutex::new(VecDeque::from([
            ("ckbus.credential", json!({}), Ok(issued)),
            (
                "ckbus.nonce_sign",
                json!({"nonce_b64":STANDARD.encode(b"fixture nonce"),"credential_public":"U_fixture"}),
                Ok(json!({"signature_b64":STANDARD.encode([7u8;64])})),
            ),
            (
                "ckbus.credential_renew",
                json!({"credential_public":"U_fixture"}),
                Ok(renewed),
            ),
        ])));
        let credential = issue(&route).await.unwrap();
        assert_eq!(credential.acct, "box_test");
        assert_eq!(
            nonce_signature(&route, &credential.credential_public, b"fixture nonce")
                .await
                .unwrap(),
            [7u8; 64]
        );
        let credential = renew(&route, &credential).await.unwrap();
        assert_eq!(credential.jwt, "fixture-renewed-jwt");
        assert_eq!(credential.exp, 1900001800);
        assert_eq!(credential.credential_public, "U_fixture");
        assert!(route.0.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn bus_credentials_refusal_and_bad_renewal_do_not_replace_live_credential() {
        let route = FakeRoute(StdMutex::new(VecDeque::from([
            (
                "ckbus.credential",
                json!({}),
                Err(refused("ckbus.credential", "ckbus_not_ready")),
            ),
            (
                "ckbus.credential_renew",
                json!({"credential_public":"U_fixture"}),
                Ok(json!({"jwt":"bad","exp":42,"credential_public":"other"})),
            ),
            (
                "ckbus.nonce_sign",
                json!({"nonce_b64":STANDARD.encode(b"n"),"credential_public":"U_fixture"}),
                Ok(json!({"signature_b64":"!"})),
            ),
        ])));
        assert!(issue(&route).await.is_err());
        let credential = Credential {
            jwt: "original".into(),
            exp: 10,
            acct: "box_test".into(),
            inbox_prefix: "_INBOX.U_fixture".into(),
            server_url: "nats://127.0.0.1:4222".into(),
            credential_public: "U_fixture".into(),
        };
        assert!(renew(&route, &credential).await.is_err());
        assert_eq!(credential.jwt, "original");
        assert_eq!(credential.exp, 10);
        assert!(nonce_signature(&route, "U_fixture", b"n").await.is_err());
    }

    /// A refusal reaches the caller and the log with ck-bus's own code and message, decoded
    /// from the route's `ERROR` frame body, not as one fixed string for every cause.
    #[tokio::test]
    async fn a_refused_nonce_sign_keeps_its_refusal_code() {
        let decoded = refusal_of(
            "ckbus.nonce_sign",
            br#"{"code":"ckbus_credential_superseded","message":"U_old is not the current key"}"#,
        );
        assert_eq!(
            decoded,
            CkbusError::Refused {
                method: "ckbus.nonce_sign",
                code: CODE_SUPERSEDED.into(),
                message: "U_old is not the current key".into(),
            }
        );
        let route = FakeRoute(StdMutex::new(VecDeque::from([(
            "ckbus.nonce_sign",
            json!({"nonce_b64":STANDARD.encode(b"n"),"credential_public":"U_old"}),
            Err(decoded),
        )])));
        let error = nonce_signature(&route, "U_old", b"n").await.unwrap_err();
        assert_eq!(error.code(), Some(CODE_SUPERSEDED));
        assert!(error.key_retired());
        assert_eq!(
            error.to_string(),
            "ckbus.nonce_sign refused with ckbus_credential_superseded: U_old is not the current key"
        );
        // A body that is not a typed error still arrives as a refusal, with its text kept.
        assert_eq!(
            refusal_of("ckbus.credential", b"daemon said no").to_string(),
            "ckbus.credential refused with unstructured: daemon said no"
        );
    }

    /// ck-bus as it keeps credentials: one current key per module, each issue minting the
    /// next key and retiring the one before it. Renewing or signing with a retired key is
    /// refused with the codes ck-bus uses.
    #[derive(Default)]
    struct CkBus(StdMutex<CkBusState>);
    #[derive(Default)]
    struct CkBusState {
        minted: u32,
        current: Option<String>,
        /// `ckbus.credential` calls broca made.
        issued: u32,
    }
    impl CkBus {
        fn mint(state: &mut CkBusState) -> String {
            state.minted += 1;
            let key = format!("U_KEY{}", state.minted);
            state.current = Some(key.clone());
            key
        }
        /// Something other than broca's holder issues a credential, retiring broca's key.
        fn issue_elsewhere(&self) {
            Self::mint(&mut self.0.lock().unwrap());
        }
        fn issued(&self) -> u32 {
            self.0.lock().unwrap().issued
        }
        fn current(&self) -> Option<String> {
            self.0.lock().unwrap().current.clone()
        }
    }
    #[async_trait]
    impl CredentialRoute for CkBus {
        async fn call(
            &self,
            method: &'static str,
            params: serde_json::Value,
        ) -> Result<serde_json::Value, CkbusError> {
            // Every real call crosses the subc route, so concurrent callers interleave here.
            tokio::task::yield_now().await;
            let mut state = self.0.lock().unwrap();
            let named = params["credential_public"].as_str().map(str::to_owned);
            let current = named.is_some() && named == state.current;
            let exp = now_ms() / 1000 + 3600;
            match method {
                "ckbus.credential" => {
                    state.issued += 1;
                    let key = Self::mint(&mut state);
                    Ok(
                        json!({"jwt":format!("jwt-{key}"),"exp":exp,"acct":"box_test",
                        "inbox_prefix":format!("_INBOX.{key}"),"server_url":"nats://127.0.0.1:1",
                        "credential_public":key}),
                    )
                }
                "ckbus.credential_renew" if current => Ok(
                    json!({"jwt":format!("jwt-renewed-{}", named.clone().unwrap()),"exp":exp,"credential_public":named}),
                ),
                "ckbus.credential_renew" => Err(refused(method, CODE_REVOKED)),
                "ckbus.nonce_sign" if current => {
                    Ok(json!({"signature_b64":STANDARD.encode(named.unwrap())}))
                }
                "ckbus.nonce_sign" => Err(refused(method, CODE_SUPERSEDED)),
                other => panic!("unexpected ck-bus method {other}"),
            }
        }
    }

    /// A broker stand-in. A link authenticates through the holder, as the real handshake
    /// does, and publishes only while the key it authenticated with is ck-bus's current one
    /// and its inbox prefix belongs to that key: the real broker kicks a connection whose
    /// key was revoked, and refuses subscriptions outside a user's own inbox.
    struct Broker {
        ckbus: Arc<CkBus>,
        published: StdMutex<Vec<String>>,
        /// The key each handshake authenticated with, in order.
        auths: StdMutex<Vec<String>>,
    }
    impl Broker {
        fn new(ckbus: Arc<CkBus>) -> Arc<Self> {
            Arc::new(Self {
                ckbus,
                published: StdMutex::new(vec![]),
                auths: StdMutex::new(vec![]),
            })
        }
    }
    struct FakeLink {
        broker: Arc<Broker>,
        inbox_key: String,
        auth_key: String,
    }
    #[async_trait]
    impl Link for FakeLink {
        fn credential_public(&self) -> &str {
            &self.inbox_key
        }
        async fn publish(&self, event: &Publication) -> Result<PublishReceipt, String> {
            if self.broker.ckbus.current().as_deref() != Some(self.auth_key.as_str()) {
                return Err("connection kicked: user authentication revoked".into());
            }
            if self.inbox_key != self.auth_key {
                return Err("permissions violation for the inbox prefix".into());
            }
            let mut published = self.broker.published.lock().unwrap();
            published.push(event.event_key.clone());
            Ok(PublishReceipt::new(published.len() as u64, false))
        }
        async fn attach_compaction(&self) -> Result<Box<dyn StreamCursor>, String> {
            Err("the fake broker has no compaction stream".into())
        }
    }
    #[async_trait]
    impl Dialer for Arc<Broker> {
        async fn dial(
            &self,
            holder: &Arc<CredentialHolder>,
            credential: &Credential,
        ) -> Result<Box<dyn Link>, String> {
            let (auth, signature) = holder
                .authenticate(b"broker nonce")
                .await
                .map_err(|e| e.to_string())?;
            assert_eq!(signature, auth.credential_public.as_bytes());
            self.auths
                .lock()
                .unwrap()
                .push(auth.credential_public.clone());
            Ok(Box::new(FakeLink {
                broker: self.clone(),
                inbox_key: credential.credential_public.clone(),
                auth_key: auth.credential_public,
            }))
        }
    }

    /// Queue one run's start and end for `session` in the outbox.
    fn queue_run(store: &SqliteStore, session: &BindIdentity, run_id: &str, seq: u64) {
        use crate::bus_events::tests::{run_finished, run_started};
        let records = [
            broca_wal::FramedRecord {
                seq,
                fence: 1,
                record: run_started(session, run_id, now_ms()),
            },
            broca_wal::FramedRecord {
                seq: seq + 1,
                fence: 1,
                record: run_finished(now_ms()),
            },
        ];
        store
            .project_bus_events(session, "epoch", &records, now_ms())
            .unwrap();
    }

    /// Drain until the outbox is empty, allowing for publish failures; returns how many
    /// publications succeeded, or `None` when the outbox never emptied.
    async fn drain(store: &SqliteStore, publisher: &dyn BusPublisher) -> Option<usize> {
        let mut published = 0;
        for _ in 0..20 {
            match store.drain_bus_event(publisher, now_ms()).await.unwrap() {
                DrainResult::Published => published += 1,
                DrainResult::Empty => return Some(published),
                DrainResult::Retry => {}
            }
        }
        None
    }

    /// When ck-bus retires the process's key and the broker kicks its connection, the next
    /// handshake finds the nonce signing refused, re-issues once, reconnects on the new key,
    /// and the events queued meanwhile all drain. Later publishes issue nothing more.
    #[tokio::test]
    async fn a_retired_key_is_reissued_once_and_the_queued_outbox_drains() {
        let ckbus = Arc::new(CkBus::default());
        let broker = Broker::new(ckbus.clone());
        let connector = NatsConnector {
            holder: CredentialHolder::with_route(ckbus.clone()),
            dialer: Arc::new(broker.clone()),
            publisher: Mutex::new(None),
        };
        let store = SqliteStore::open_in_memory().unwrap();
        store.enable_bus_events("box_test").unwrap();
        let session = BindIdentity::new("/test", "test", "reissue");
        let bus = connector.connect().await.unwrap();
        queue_run(&store, &session, "before", 1);
        assert_eq!(drain(&store, bus.publisher.as_ref()).await, Some(2));
        assert_eq!(ckbus.issued(), 1);

        ckbus.issue_elsewhere();
        queue_run(&store, &session, "during", 3);
        queue_run(&store, &session, "after", 5);
        assert_eq!(
            drain(&store, bus.publisher.as_ref()).await,
            Some(4),
            "the events queued after the key was retired never drained"
        );
        assert!(store.pending_bus_event().unwrap().is_none());
        assert_eq!(
            ckbus.issued(),
            2,
            "exactly one re-issue serves the reconnect"
        );
        assert_eq!(broker.published.lock().unwrap().len(), 6);

        // The next refresh and publish reuse the re-issued key.
        let bus = connector.connect().await.unwrap();
        queue_run(&store, &session, "later", 7);
        assert_eq!(drain(&store, bus.publisher.as_ref()).await, Some(2));
        assert_eq!(ckbus.issued(), 2);
    }

    /// Two connections whose handshakes both find the key retired share one re-issue: both
    /// end up on the same new key, and neither retires the other's by issuing again.
    #[tokio::test]
    async fn two_connections_finding_the_key_retired_share_one_reissue() {
        let ckbus = Arc::new(CkBus::default());
        let broker = Broker::new(ckbus.clone());
        let holder = CredentialHolder::with_route(ckbus.clone());
        let first = holder.current().await.unwrap();
        ckbus.issue_elsewhere();
        let (a, b) = tokio::join!(broker.dial(&holder, &first), broker.dial(&holder, &first));
        assert!(
            a.is_ok() && b.is_ok(),
            "a handshake failed after the re-issue"
        );
        let current = ckbus.current().unwrap();
        assert_ne!(current, first.credential_public);
        assert_eq!(
            ckbus.issued(),
            2,
            "both connections were served by one re-issue"
        );
        assert_eq!(
            *broker.auths.lock().unwrap(),
            [current.clone(), current.clone()],
            "both handshakes must name the one current key"
        );
        // Each connection, rebuilt on the current key's inbox prefix, publishes, and nothing
        // issues again.
        let rebuilt = holder.current().await.unwrap();
        assert_eq!(rebuilt.credential_public, current);
        for _ in 0..2 {
            let link = broker.dial(&holder, &rebuilt).await.unwrap();
            let _receipt = link.publish(&publication()).await.unwrap();
        }
        assert_eq!(ckbus.issued(), 2, "no further issue");
    }

    fn publication() -> Publication {
        Publication {
            event_key: "fixture".into(),
            subject: "ck.box_test.event.broca.run.started.v1".into(),
            headers: Default::default(),
            body: vec![],
            digest: None,
        }
    }

    /// A renewal ck-bus refuses as revoked replaces the key instead of failing every
    /// connection until the JWT expires.
    #[tokio::test]
    async fn a_renewal_refused_as_revoked_reissues() {
        let ckbus = Arc::new(CkBus::default());
        let holder = CredentialHolder::with_route(ckbus.clone());
        let first = holder.current().await.unwrap();
        holder.state.lock().await.current.as_mut().unwrap().exp = now_ms() / 1000 + 60;
        ckbus.issue_elsewhere();
        let renewed = holder.current().await.unwrap();
        assert_ne!(renewed.credential_public, first.credential_public);
        assert_eq!(Some(renewed.credential_public), ckbus.current());
        assert_eq!(ckbus.issued(), 2);
    }

    /// `ckbus_generation_not_live` means this process is being replaced: the holder issues
    /// nothing in answer and makes no further ck-bus calls.
    #[tokio::test]
    async fn generation_not_live_stops_without_reissuing() {
        let exp = now_ms() / 1000 + 3600;
        let route = Arc::new(FakeRoute(StdMutex::new(VecDeque::from([
            (
                "ckbus.credential",
                json!({}),
                Ok(
                    json!({"jwt":"fixture","exp":exp,"acct":"box_test","inbox_prefix":"_INBOX.U_fixture","server_url":"nats://127.0.0.1:1","credential_public":"U_fixture"}),
                ),
            ),
            (
                "ckbus.nonce_sign",
                json!({"nonce_b64":STANDARD.encode(b"n"),"credential_public":"U_fixture"}),
                Err(refused("ckbus.nonce_sign", CODE_GENERATION_NOT_LIVE)),
            ),
        ]))));
        let holder = CredentialHolder::with_route(route.clone());
        let error = holder.authenticate(b"n").await.err().unwrap();
        assert_eq!(error.code(), Some(CODE_GENERATION_NOT_LIVE));
        // FakeRoute panics on any further call, so these prove no ck-bus request is made.
        assert_eq!(holder.current().await.err().unwrap(), CkbusError::Stopped);
        assert_eq!(
            holder.authenticate(b"n").await.err().unwrap(),
            CkbusError::Stopped
        );
        assert!(route.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn bus_account_discovery_and_renewal_do_not_wait_for_broker_connection() {
        let exp = now_ms() / 1000 + 900;
        let route = Arc::new(FakeRoute(StdMutex::new(VecDeque::from([
            (
                "ckbus.credential",
                json!({}),
                Ok(
                    json!({"jwt":"fixture","exp":exp,"acct":"box_test","inbox_prefix":"_INBOX.U_fixture","server_url":"nats://127.0.0.1:1","credential_public":"U_fixture"}),
                ),
            ),
            (
                "ckbus.credential_renew",
                json!({"credential_public":"U_fixture"}),
                Ok(json!({"jwt":"renewed","exp":exp,"credential_public":"U_fixture"})),
            ),
        ]))));
        let connector = NatsConnector {
            holder: CredentialHolder::with_route(route.clone()),
            dialer: Arc::new(NatsDialer),
            publisher: Mutex::new(None),
        };
        let connection = connector.connect().await.unwrap();
        assert_eq!(connection.account.account(), "box_test");
        connector
            .holder
            .state
            .lock()
            .await
            .current
            .as_mut()
            .unwrap()
            .exp = now_ms() / 1000 + 60;
        let renewed = connector.connect().await.unwrap();
        assert!(renewed.refresh_at_ms > now_ms());
        assert!(route.0.lock().unwrap().is_empty());
    }

    /// ck-bus minting real user credentials from a private broker's trust chain, with one
    /// current key as ck-bus keeps it. Keys are minted up front because the trust chain
    /// belongs to the broker the test owns.
    struct RealCkBus {
        acct: String,
        server_url: String,
        /// Keys not yet issued, each with its user JWT.
        unissued: StdMutex<VecDeque<(Arc<nkeys::KeyPair>, String)>>,
        current: StdMutex<Option<Arc<nkeys::KeyPair>>>,
        issued: std::sync::atomic::AtomicU32,
    }
    impl RealCkBus {
        fn mint(&self) -> (Arc<nkeys::KeyPair>, String) {
            let (key, jwt) = self.unissued.lock().unwrap().pop_front().unwrap();
            *self.current.lock().unwrap() = Some(key.clone());
            (key, jwt)
        }
    }
    #[async_trait]
    impl CredentialRoute for RealCkBus {
        async fn call(
            &self,
            method: &'static str,
            params: serde_json::Value,
        ) -> Result<serde_json::Value, CkbusError> {
            match method {
                "ckbus.credential" => {
                    self.issued
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let (key, jwt) = self.mint();
                    let public = key.public_key();
                    Ok(
                        json!({"jwt":jwt,"exp":now_ms() / 1000 + 3600,"acct":self.acct,
                        "inbox_prefix":ConnectConfig::new(public.clone()).unwrap().inbox_prefix(),
                        "server_url":self.server_url,"credential_public":public}),
                    )
                }
                "ckbus.nonce_sign" => {
                    let current = self.current.lock().unwrap().clone().unwrap();
                    if params["credential_public"].as_str() != Some(&current.public_key()) {
                        return Err(refused(method, CODE_SUPERSEDED));
                    }
                    let nonce = STANDARD
                        .decode(params["nonce_b64"].as_str().unwrap())
                        .unwrap();
                    Ok(json!({"signature_b64":STANDARD.encode(current.sign(&nonce).unwrap())}))
                }
                other => panic!("unexpected ck-bus method {other}"),
            }
        }
    }

    fn account_jwt(signer: &nkeys::KeyPair, subject: &str, nats: serde_json::Value) -> String {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        use sha2::{Digest, Sha256};
        let iat = cortexkit_bus_testkit::trust::unix_now() - 120;
        let mut claims =
            json!({"jti":"","iat":iat,"iss":signer.public_key(),"sub":subject,"nats":nats});
        claims["jti"] = format!("{:X}", Sha256::digest(claims.to_string())).into();
        let input = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(br#"{"alg":"ed25519-nkey","typ":"JWT"}"#),
            URL_SAFE_NO_PAD.encode(claims.to_string())
        );
        format!(
            "{input}.{}",
            URL_SAFE_NO_PAD.encode(signer.sign(input.as_bytes()).unwrap())
        )
    }

    /// The real handshake against a private broker. ck-bus retired the process's key before
    /// the connection opened, so the broker's nonce cannot be signed with it: the holder
    /// re-issues once, the connection authenticates on the new key, and the queued outbox
    /// drains through JetStream.
    #[tokio::test]
    async fn real_broker_handshake_reissues_a_retired_key_and_the_outbox_drains() {
        use cortexkit_bus_testkit::{
            trust::{box_root_id, signer_root_id, unix_now},
            BusOptions, BusServer,
        };
        let dir = broca_test_support::scratch_root().join(format!(
            "bus-reissue-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let bus = match BusServer::start(BusOptions {
            directory: Some(dir.join("broker")),
            ..Default::default()
        })
        .await
        {
            Ok(bus) => bus,
            Err(error) => {
                cortexkit_bus_testkit::skip_or_fail(error);
                return;
            }
        };
        // The testkit's default account has no JetStream quota; provision a disposable one.
        let account = nkeys::KeyPair::new_account();
        let claims = json!({"type":"account","version":2,"signing_keys":[bus.trust.box_root_public()],"limits":{"subs":-1,"data":-1,"payload":-1,"imports":-1,"exports":-1,"wildcards":true,"conn":-1,"leaf":-1,"mem_storage":-1,"disk_storage":-1,"streams":-1,"consumer":-1},"default_permissions":{"pub":{},"sub":{}}});
        std::fs::write(
            bus.dir
                .join("jwt")
                .join(format!("{}.jwt", account.public_key())),
            account_jwt(
                &bus.trust.signer.root(&signer_root_id()).pair,
                &account.public_key(),
                claims,
            ),
        )
        .unwrap();
        let user_jwt = |user: &nkeys::KeyPair| {
            bus.trust
                .user_jwt(&box_root_id(), &account.public_key(), user, unix_now() - 60)
        };
        let names = AccountNames::derive("box_broca_test").unwrap();
        // Provision the event stream over a connection of its own.
        let admin = Arc::new(nkeys::KeyPair::new_user());
        let admin_options = async_nats::ConnectOptions::with_jwt(user_jwt(&admin), {
            let admin = admin.clone();
            move |nonce| {
                let admin = admin.clone();
                async move { admin.sign(&nonce).map_err(async_nats::AuthError::new) }
            }
        });
        let admin_conn = NatsConnection::connect(
            &bus.url,
            admin_options,
            ConnectConfig::new(admin.public_key()).unwrap(),
        )
        .await
        .unwrap();
        async_nats::jetstream::new(admin_conn.client())
            .create_stream(async_nats::jetstream::stream::Config {
                name: names.streams().event.clone(),
                subjects: vec![names.event_binding()],
                duplicate_window: Duration::from_secs(120),
                ..Default::default()
            })
            .await
            .unwrap();
        let ckbus = Arc::new(RealCkBus {
            acct: names.account().to_owned(),
            server_url: bus.url.clone(),
            unissued: StdMutex::new(
                (0..3)
                    .map(|_| {
                        let user = Arc::new(nkeys::KeyPair::new_user());
                        let jwt = user_jwt(&user);
                        (user, jwt)
                    })
                    .collect(),
            ),
            current: StdMutex::new(None),
            issued: Default::default(),
        });
        let connector = NatsConnector::new(CredentialHolder::with_route(ckbus.clone()));
        let connection = connector.connect().await.unwrap();
        let first = connector.holder.current().await.unwrap().credential_public;
        // Another issue retires the key the holder has, before any connection is open.
        ckbus.mint();
        let store = SqliteStore::open_in_memory().unwrap();
        store.enable_bus_events(names.account()).unwrap();
        let session = BindIdentity::new("/test", "test", "real-reissue");
        queue_run(&store, &session, "queued", 1);
        assert_eq!(
            drain(&store, connection.publisher.as_ref()).await,
            Some(2),
            "the queued events never reached JetStream"
        );
        let current = connector.holder.current().await.unwrap().credential_public;
        assert_ne!(current, first);
        assert_eq!(
            Some(current),
            ckbus
                .current
                .lock()
                .unwrap()
                .as_ref()
                .map(|k| k.public_key())
        );
        assert_eq!(
            ckbus.issued.load(std::sync::atomic::Ordering::Relaxed),
            2,
            "one re-issue serves the connection"
        );
        let info = async_nats::jetstream::new(admin_conn.client())
            .get_stream(names.streams().event.clone())
            .await
            .unwrap()
            .info()
            .await
            .unwrap()
            .state
            .messages;
        assert_eq!(info, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
