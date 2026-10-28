//! Account selection through the routing module: which provider account a new run uses, and
//! the reports that keep routing's view of each account current.
//!
//! # Admission
//!
//! A fresh run whose credential method is a subscription account (`oauth`, `chatgpt`) and whose
//! provider has more than one candidate account in broca's latest vault listing asks routing
//! (`route.account_for`) which account to use, before `RunStarted` is written. A candidate is
//! a listed credential whose vault metadata names this provider in `provider_ids` and this
//! method as its `auth_method`; a credential id is an opaque record name and is never parsed
//! for either. A listed credential missing either field is unmapped: it is never offered, is
//! warned about once while it stays unmapped, and is counted in `health.check`. The answer,
//! or the reason there was none, is frozen on the run's auth selection as an
//! [`broca_wal::AccountDecision`], so a resume never asks again. What happens on each answer:
//!
//! * an account: the run reads and uses exactly that credential;
//! * no usable answer (routing unreachable, no decision within [`ADMISSION_BUDGET`], or a pick
//!   of a credential broca did not offer): the send is refused `routing_unavailable`, class
//!   transient, and nothing is written. broca never picks an account on routing's behalf;
//! * `auth_required`: the run admits on the static pick and pauses for a fresh login, the same
//!   path a credential the vault reports as needing re-login takes;
//! * any other refusal: the send is refused with routing's code and detail. Account limits use
//!   `account_limit_exhausted`; other codes use `account_selection_refused`, classified by
//!   [`classify_refusal`]. An account refusal writes no run records; the session's lineage
//!   record may still be initialized.
//!
//! A caller that names an account keeps it and routing is not asked: routing's
//! `requested_account` is a preference it may overrule, and a caller's pin must win.
//!
//! # Reports
//!
//! Every run that has an admission id (every decision path broca writes) is reported
//! through `route.account_observe`: `admission_started` once `RunStarted` is durable, one report
//! per provider attempt once its record is durable, and `run_finished` once the terminal is.
//! Reports are sent once, in order, from a task of their own: they never block a run and are
//! never retried. `run_finished` also ends the run's lease in routing, so broca never sends
//! `route.account_release`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use broca_core::{AccountDecision, AccountSelectionRefusal, RoutingUnavailable};
use broca_protocol::{BindIdentity, ErrorClass, ModelAttempt, RunFinishReason};
use broca_subc::vault::{ScopedCredential, ScopedList};
use broca_wal::WalLineageId;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::auth_routing::{AuthMethod, CredentialId};

/// The routing module broca asks when `broca.jsonc` names none.
pub const DEFAULT_ROUTING_MODULE: &str = "prefrontal-routing";
/// Choose the account for one admission.
pub const ACCOUNT_FOR_OP: &str = "route.account_for";
/// Report admission, attempt and run-end evidence for an admission.
pub const ACCOUNT_OBSERVE_OP: &str = "route.account_observe";
/// Routing's refusal while account selection is switched off in its policy.
pub const SELECTION_DISABLED: &str = "account_selection_disabled";
/// Routing's refusal when every remaining candidate needs a fresh login.
pub const AUTH_REQUIRED: &str = "auth_required";
/// The refusal broca records when the vault does not serve the credential routing chose.
pub const ACCOUNT_NOT_AUTHORIZED: &str = "account_not_authorized";

/// The most an admission waits for routing, listing broca's candidates included when no
/// listing is cached yet.
///
/// Routing's `route.account_for` is one local store transaction around a pure allocator, so a
/// healthy answer takes milliseconds and this budget only matters when routing is slow or gone.
/// A send routing does not answer within it is refused `routing_unavailable` (transient), so
/// the caller retries rather than run on an account routing never chose. The bound it has to
/// fit is the caller's: a `session.send` reply waits for the admission barrier, callers give
/// that reply 10 s, and the vault read the admission makes next may itself take up to its 5 s
/// readiness budget. 1.5 s keeps the worst case under the caller's deadline with 3.5 s to
/// spare, while being long enough that a briefly busy routing module still answers. A longer
/// budget would also hold the session's actor longer: it serves no other command of the session
/// until the admission answers. A route to routing that is still opening when the budget runs
/// out keeps opening in the background, so the next admission finds it ready.
pub const ADMISSION_BUDGET: Duration = Duration::from_millis(1500);
/// The most one report waits for routing's reply. Reports are sent from a task of their own,
/// so this bounds how far a stuck routing module can delay the NEXT report, never a run.
pub const OBSERVE_BUDGET: Duration = Duration::from_secs(5);
/// How often a pending attempt report checks whether its record has become durable.
const DURABILITY_POLL: Duration = Duration::from_millis(20);
/// How long an attempt report waits for its record to become durable before giving up. An
/// attempt record rides the next WAL barrier, which a running step reaches within its own
/// step; a record still not durable after this long belongs to a writer that has stopped.
const DURABILITY_MAX_WAIT: Duration = Duration::from_secs(300);
/// A cached vault listing older than this is refreshed in the background.
pub const LISTING_MAX_AGE: Duration = Duration::from_secs(60);
/// The `caller.runner` routing records with every request. Audit metadata only: routing
/// authorizes by the route's attested principal.
const RUNNER: &str = "broca";

// ------------------------------------------------------------------------------------------
// Configuration
// ------------------------------------------------------------------------------------------

/// The `account_selection` section of `broca.jsonc`.
///
/// Which account serves a send is routing's decision alone, so there is no switch that
/// bypasses routing: the former `legacy_static` key is refused as unknown by the strict loader
/// below, so a leftover setting fails the load instead of silently doing nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSelectionConfig {
    /// The routing module's id.
    pub routing_module: String,
}

impl Default for AccountSelectionConfig {
    fn default() -> Self {
        Self {
            routing_module: DEFAULT_ROUTING_MODULE.to_string(),
        }
    }
}

// Strict, like the vault section beside it: an unknown key (a misspelling, or the removed
// `legacy_static`) is refused rather than ignored.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AccountSelectionSection {
    #[serde(default)]
    routing_module: Option<String>,
}

#[derive(Deserialize)]
struct BrocaConfigAccountView {
    #[serde(default)]
    account_selection: Option<AccountSelectionSection>,
}

impl AccountSelectionConfig {
    /// Read from the config-home `broca.jsonc` (or `$BROCA_CONFIG_PATH`). A missing file, or no
    /// config home, means the defaults.
    pub fn load_default_path() -> Result<Self, String> {
        match crate::config_home::config_file(crate::cache_routing::CONFIG_PATH_ENV, "broca.jsonc")
        {
            crate::config_home::ConfigHome::Resolved { path, .. } => Self::load(&path),
            crate::config_home::ConfigHome::Unresolvable => Ok(Self::default()),
        }
    }

    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default())
            }
            Err(error) => return Err(format!("read broca config {}: {error}", path.display())),
        };
        Self::parse(&text)
            .map_err(|error| format!("parse broca config {}: {error}", path.display()))
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let stripped = crate::cache_routing::strip_trailing_commas(
            &crate::cache_routing::strip_jsonc_comments(text),
        );
        let view: BrocaConfigAccountView = serde_json::from_str(&stripped)
            .map_err(|error| format!("account_selection: {error}"))?;
        let Some(section) = view.account_selection else {
            return Ok(Self::default());
        };
        let routing_module = match section.routing_module {
            Some(module) if module.trim().is_empty() => {
                return Err("account_selection.routing_module must not be empty".into())
            }
            Some(module) => module,
            None => DEFAULT_ROUTING_MODULE.to_string(),
        };
        Ok(Self { routing_module })
    }
}

// ------------------------------------------------------------------------------------------
// Counters
// ------------------------------------------------------------------------------------------

/// Lifetime counts for one routing client, reported by `health.check`. They reset on restart.
#[derive(Debug, Default)]
pub struct Counters {
    counts: Mutex<BTreeMap<String, u64>>,
}

/// The counters `health.check` always reports, zero included, so a quiet process reads
/// differently from one that never wired them.
const REPORTED_COUNTERS: &[&str] = &[
    "routed",
    "disabled",
    "unsupported",
    "unreachable",
    "no_decision_within_budget",
    "invalid_pick",
    "auth_required",
    "refused",
    "pinned",
    "single_candidate",
    "candidates_unknown",
    "scoped_not_found",
    "scoped_not_found_still_listed",
    "observe_sent",
    "observe_disabled",
    "observe_failed",
];

impl Counters {
    pub fn bump(&self, name: &str) {
        *self
            .counts
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_default() += 1;
    }

    pub fn get(&self, name: &str) -> u64 {
        self.counts
            .lock()
            .unwrap()
            .get(name)
            .copied()
            .unwrap_or_default()
    }

    /// Every count, the always-reported ones at zero when nothing happened.
    pub fn snapshot(&self) -> Value {
        let counts = self.counts.lock().unwrap();
        let mut out = Map::new();
        for name in REPORTED_COUNTERS {
            out.insert(
                (*name).to_string(),
                json!(counts.get(*name).copied().unwrap_or(0)),
            );
        }
        for (name, count) in counts.iter() {
            out.insert(name.clone(), json!(count));
        }
        Value::Object(out)
    }
}

static PUBLISHED: OnceLock<Arc<Counters>> = OnceLock::new();
static PUBLISHED_UNMAPPED: OnceLock<Arc<UnmappedCredentials>> = OnceLock::new();

/// Make `counters` the ones `health.check` reports. The serve binary calls this once, for its
/// one routing client.
pub fn publish_counters(counters: Arc<Counters>) {
    let _ = PUBLISHED.set(counters);
}

/// Make `unmapped` the unmapped-credential record `health.check` reports. The serve binary
/// calls this once, for the listing cache its admissions read.
pub fn publish_unmapped(unmapped: Arc<UnmappedCredentials>) {
    let _ = PUBLISHED_UNMAPPED.set(unmapped);
}

/// The published counters as `health.check` reports them, with `unmapped_credentials`: how
/// many credentials in broca's latest vault listing lack the `provider_ids` or `auth_method`
/// metadata account selection needs, and so are never offered to routing. `null` when this
/// process asks no routing module (no vault credential source).
pub fn published_metrics() -> Value {
    let Some(counters) = PUBLISHED.get() else {
        return Value::Null;
    };
    let mut metrics = counters.snapshot();
    metrics["unmapped_credentials"] = json!(PUBLISHED_UNMAPPED
        .get()
        .map_or(0, |unmapped| unmapped.count()));
    metrics
}

// ------------------------------------------------------------------------------------------
// Transport
// ------------------------------------------------------------------------------------------

/// What one request to the routing module got back: `Ok(Ok(body))` is a Response body,
/// `Ok(Err(body))` an Error frame's body, and `Err(cause)` means no reply arrived.
pub type RoutingReply = Result<Result<Vec<u8>, Vec<u8>>, String>;

/// One request to the routing module. `Ok(Ok(body))` is a Response body, `Ok(Err(body))` an
/// Error frame's body, and `Err(cause)` means no reply arrived.
#[async_trait::async_trait]
pub trait RoutingTransport: Send + Sync {
    async fn request(&self, body: Vec<u8>) -> RoutingReply;
}

enum RouteSlot {
    Empty,
    Opening(tokio::sync::watch::Receiver<Option<Result<broca_subc::RouteHandle, String>>>),
    Ready(broca_subc::RouteHandle),
}

/// The routing module's management route over the shared subc connection. The route is opened
/// once, in a task of its own, so a caller that stops waiting (its admission budget ran out)
/// does not abandon the open: the next caller finds the route ready. A request that fails on
/// the connection forgets the route, and the next request opens a fresh one.
pub struct SubcRoutingTransport {
    conn: Arc<broca_subc::SubcConnection>,
    module_id: String,
    identity: BindIdentity,
    route: Arc<Mutex<RouteSlot>>,
}

impl SubcRoutingTransport {
    pub fn new(
        conn: Arc<broca_subc::SubcConnection>,
        module_id: String,
        identity: BindIdentity,
    ) -> Self {
        Self {
            conn,
            module_id,
            identity,
            route: Arc::new(Mutex::new(RouteSlot::Empty)),
        }
    }

    async fn route(&self) -> Result<broca_subc::RouteHandle, String> {
        let mut opening = {
            let mut slot = self.route.lock().unwrap();
            match &*slot {
                RouteSlot::Ready(route) => return Ok(*route),
                RouteSlot::Opening(rx) => rx.clone(),
                RouteSlot::Empty => {
                    let (tx, rx) = tokio::sync::watch::channel(None);
                    *slot = RouteSlot::Opening(rx.clone());
                    let conn = Arc::clone(&self.conn);
                    let module_id = self.module_id.clone();
                    let identity = self.identity.clone();
                    let route = Arc::clone(&self.route);
                    tokio::spawn(async move {
                        let opened = conn
                            .route_open_management(&module_id, &identity)
                            .await
                            .map_err(|error| {
                                format!("route.open to routing module '{module_id}': {error}")
                            });
                        *route.lock().unwrap() = match &opened {
                            Ok(handle) => RouteSlot::Ready(*handle),
                            Err(_) => RouteSlot::Empty,
                        };
                        let _ = tx.send(Some(opened));
                    });
                    rx
                }
            }
        };
        let opened = opening
            .wait_for(Option::is_some)
            .await
            .map_err(|_| "the routing route open was abandoned".to_string())?;
        opened.clone().expect("waited for a result")
    }

    fn forget(&self, failed: broca_subc::RouteHandle) {
        let mut slot = self.route.lock().unwrap();
        if matches!(&*slot, RouteSlot::Ready(route) if *route == failed) {
            *slot = RouteSlot::Empty;
        }
    }
}

#[async_trait::async_trait]
impl RoutingTransport for SubcRoutingTransport {
    async fn request(&self, body: Vec<u8>) -> RoutingReply {
        let route = self.route().await?;
        match self.conn.route_request_terminal(&route, body).await {
            Ok(reply) => Ok(reply),
            Err(error) => {
                self.forget(route);
                Err(format!("routing module '{}': {error}", self.module_id))
            }
        }
    }
}

// ------------------------------------------------------------------------------------------
// route.account_for
// ------------------------------------------------------------------------------------------

/// What `route.account_for` answered, as broca acts on it.
#[derive(Debug, Clone, PartialEq)]
pub enum AccountForOutcome {
    /// Routing chose this account.
    Chosen {
        account_id: String,
        credential_id: String,
        decision_id: Option<String>,
    },
    /// `auth_required`: every remaining candidate needs a fresh login.
    AuthRequired { decision_id: Option<String> },
    /// Only `account_selection_disabled` enables the configured static credential fallback.
    SelectionDisabled { decision_id: Option<String> },
    /// Any refusal broca passes on to the caller.
    Refused(AccountSelectionRefusal),
    /// Routing did not return a decision before the admission budget expired.
    NoDecisionWithinBudget,
    /// No usable answer: a failed route or a reply broca cannot read.
    Unreachable(String),
}

impl AccountForOutcome {
    /// A short label for logs, without account or credential ids.
    pub fn kind(&self) -> &str {
        match self {
            AccountForOutcome::Chosen { .. } => "chosen",
            AccountForOutcome::AuthRequired { .. } => "auth_required",
            AccountForOutcome::SelectionDisabled { .. } => SELECTION_DISABLED,
            AccountForOutcome::Refused(refusal) => &refusal.routing_code,
            AccountForOutcome::NoDecisionWithinBudget => "no_decision_within_budget",
            AccountForOutcome::Unreachable(_) => "unreachable",
        }
    }
}

/// Known refusal classes are pinned by name; a new routing code is a transient refusal, not
/// evidence of a failed transport. Login pauses and the module off-switch are handled before
/// refusal classification.
pub fn classify_refusal(code: &str) -> ErrorClass {
    match code {
        "account_hold"
        | "no_eligible_account"
        | "account_pool_exhausted"
        | "account_pool_reserve_limited"
        | "account_quota_unknown"
        | "account_selection_unavailable"
        | "pin_generation_conflict"
        | "store_error"
        | "internal_error" => ErrorClass::Transient,
        AUTH_REQUIRED => ErrorClass::Auth,
        "admission_conflict"
        | "invalid_request"
        | "unknown_decision"
        | "unscoped_caller"
        | "account_selection_unsupported"
        | ACCOUNT_NOT_AUTHORIZED => ErrorClass::Permanent,
        _ => ErrorClass::Transient,
    }
}

#[derive(Deserialize)]
struct ErrorBodyView {
    code: String,
    #[serde(default)]
    detail: Option<Value>,
}

/// Read one `route.account_for` reply.
pub fn classify_account_for_reply(reply: Result<Vec<u8>, Vec<u8>>) -> AccountForOutcome {
    match reply {
        Ok(body) => {
            let parsed: Value = match serde_json::from_slice(&body) {
                Ok(parsed) => parsed,
                Err(error) => {
                    return AccountForOutcome::Unreachable(format!("reply is not JSON: {error}"))
                }
            };
            let result = &parsed["result"];
            let account_id = result["account"]["account_id"].as_str();
            let credential_id = result["account"]["credential_id"].as_str();
            match (account_id, credential_id) {
                (Some(account_id), Some(credential_id))
                    if !account_id.is_empty() && !credential_id.is_empty() =>
                {
                    AccountForOutcome::Chosen {
                        account_id: account_id.to_string(),
                        credential_id: credential_id.to_string(),
                        decision_id: result["reason"]["decision_id"].as_str().map(str::to_string),
                    }
                }
                _ => AccountForOutcome::Unreachable(
                    "reply names no account_id and credential_id".to_string(),
                ),
            }
        }
        Err(body) => {
            let error: ErrorBodyView = match serde_json::from_slice(&body) {
                Ok(error) => error,
                Err(decode) => {
                    return AccountForOutcome::Unreachable(format!(
                        "error reply is not an error body: {decode}"
                    ))
                }
            };
            let detail = error.detail.unwrap_or(Value::Null);
            let decision_id = detail["decision_id"].as_str().map(str::to_string);
            match error.code.as_str() {
                AUTH_REQUIRED => AccountForOutcome::AuthRequired { decision_id },
                SELECTION_DISABLED => AccountForOutcome::SelectionDisabled { decision_id },
                code => {
                    let resets_at_ms = if code == "account_hold" {
                        detail["until_ms"].as_u64()
                    } else {
                        detail["reset_at_ms"].as_u64()
                    };
                    let reset_wait = resets_at_ms.map(|reset| {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        reset.saturating_sub(now).div_ceil(1000)
                    });
                    let retry_after_s = if code == "account_hold" && resets_at_ms.is_some() {
                        reset_wait
                    } else {
                        detail["retry_after_s"].as_u64().or(reset_wait)
                    };
                    AccountForOutcome::Refused(AccountSelectionRefusal {
                        routing_code: code.to_string(),
                        retry_after_s,
                        resets_at_ms,
                        routing_detail: (!detail.is_null()).then(|| Box::new(detail)),
                        decision_id,
                        class: classify_refusal(code),
                    })
                }
            }
        }
    }
}

/// The identities of one provider's candidate accounts in broca's vault listing, with the
/// vault's digest of that listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidates {
    pub view_digest: String,
    pub accounts: Vec<Candidate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub account_id: String,
    pub credential_id: String,
}

impl Candidates {
    /// The rows of `listing` that are credentials of `method` for `provider` and name a
    /// provider account, sorted by credential id so identical listings give identical requests.
    ///
    /// Provider and method come only from the vault's metadata on each row: `provider_ids`
    /// must name `provider` and `auth_method` must be `method`. The credential id is an opaque
    /// record name and is never read for either, so `oauth:openai:x` whose metadata says
    /// anthropic/oauth is an anthropic candidate. A row missing either field is unmapped and
    /// never a candidate (see [`unmapped_field`]).
    pub fn from_listing(listing: &ScopedList, method: AuthMethod, provider: &str) -> Self {
        let mut accounts: Vec<Candidate> = listing
            .credentials
            .iter()
            .filter(|row| {
                unmapped_field(row).is_none()
                    && row.auth_method.as_deref() == Some(method.as_token())
                    && row.provider_ids.iter().any(|id| id == provider)
            })
            .filter_map(|row| {
                row.account_id.as_ref().map(|account_id| Candidate {
                    account_id: account_id.clone(),
                    credential_id: row.id.clone(),
                })
            })
            .collect();
        accounts.sort_by(|a, b| a.credential_id.cmp(&b.credential_id));
        Self {
            view_digest: listing.view.clone(),
            accounts,
        }
    }

    /// Whether routing's pick is one of these candidates, account and credential together.
    pub fn offers(&self, account_id: &str, credential_id: &str) -> bool {
        self.accounts.iter().any(|candidate| {
            candidate.credential_id == credential_id && candidate.account_id == account_id
        })
    }

    /// Distinct provider accounts; one account may have several credential records.
    pub fn distinct_accounts(&self) -> usize {
        self.accounts
            .iter()
            .map(|candidate| candidate.account_id.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len()
    }
}

/// Which account-selection metadata a listed credential lacks, or `None` when it has both.
/// `auth_method` is derived by the vault for every LLM credential; `provider_ids` is set by the
/// operator. A credential without either cannot be matched to a provider and method without
/// guessing from its id, so it is not routed at all.
pub fn unmapped_field(row: &ScopedCredential) -> Option<&'static str> {
    match (row.provider_ids.is_empty(), row.auth_method.is_none()) {
        (false, false) => None,
        (true, false) => Some("provider_ids"),
        (false, true) => Some("auth_method"),
        (true, true) => Some("provider_ids and auth_method"),
    }
}

/// The credentials of the latest vault listing that are unmapped (see [`unmapped_field`]),
/// with the field each lacks. Each id is warned about once when it becomes unmapped, never per
/// admission; one that gains its metadata leaves the set and is warned about again only if it
/// loses it again.
#[derive(Debug, Default)]
pub struct UnmappedCredentials {
    ids: Mutex<BTreeMap<String, &'static str>>,
}

impl UnmappedCredentials {
    /// Replace the record with what `listing` shows, warning once for each id newly unmapped.
    pub fn observe(&self, listing: &ScopedList) {
        let current: BTreeMap<String, &'static str> = listing
            .credentials
            .iter()
            .filter_map(|row| unmapped_field(row).map(|missing| (row.id.clone(), missing)))
            .collect();
        let mut known = self.ids.lock().unwrap();
        for (id, missing) in &current {
            if !known.contains_key(id) {
                tracing::warn!(
                    target: "accounts",
                    "vault credential {id} has no {missing} metadata, so it is never offered \
                     for account selection until the vault lists it with both"
                );
            }
        }
        *known = current;
    }

    /// How many credentials of the latest listing are unmapped.
    pub fn count(&self) -> usize {
        self.ids.lock().unwrap().len()
    }
}

/// Whether credentials of `method` belong to provider subscription accounts (OAuth logins, as
/// opposed to API keys), the kind routing chooses between.
pub fn selects_accounts(method: AuthMethod) -> bool {
    matches!(method, AuthMethod::Oauth | AuthMethod::Chatgpt)
}

/// The admission id a run is known by in routing. It is the run id, which is itself derived
/// from the session and the caller's `send_id` when the send carries one, so a genuine retry of
/// the same send (after a crash between asking routing and writing `RunStarted`, say) presents
/// the same admission and routing replays the decision it already made. A send with no
/// `send_id` gets a run id minted for that send alone, so its admission id is unique too.
pub fn admission_id(run_id: &str) -> String {
    run_id.to_string()
}

/// The `conversation_key` routing keeps account affinity under: one per broca session, spelled
/// with the session digest that also names the session's WAL file rather than with the session's
/// project path, so the path is never sent to routing.
pub fn conversation_key(session: &BindIdentity) -> String {
    format!(
        "broca-session:{}",
        broca_core::wal_writer::session_addr(session)
    )
}

/// One admission's request to routing.
pub struct AccountForRequest<'a> {
    pub provider: &'a str,
    pub model: &'a str,
    pub conversation_key: &'a str,
    pub admission_id: &'a str,
    /// The serialized size of the episode's new input. Marked estimated: it does not count the
    /// history a later episode renders before it.
    pub initial_input_bytes: u64,
    pub candidates: &'a Candidates,
}

/// The `route.account_for` params, in routing's field names.
pub fn account_for_params(request: &AccountForRequest<'_>) -> Value {
    json!({
        "provider": request.provider,
        "model": request.model,
        "affinity_model": request.model,
        "conversation_key": request.conversation_key,
        "caller": { "runner": RUNNER },
        "admission_id": request.admission_id,
        "mode": "admit",
        "initial_input_bytes": request.initial_input_bytes,
        "initial_input_estimated": true,
        "candidates": {
            "view_digest": request.candidates.view_digest,
            "accounts": request
                .candidates
                .accounts
                .iter()
                .map(|candidate| json!({
                    "account_id": candidate.account_id,
                    "credential_id": candidate.credential_id,
                }))
                .collect::<Vec<_>>(),
        },
    })
}

// ------------------------------------------------------------------------------------------
// The client
// ------------------------------------------------------------------------------------------

/// broca's client of the routing module.
pub struct RoutingClient {
    transport: Arc<dyn RoutingTransport>,
    counters: Arc<Counters>,
    config: AccountSelectionConfig,
}

impl RoutingClient {
    pub fn new(transport: Arc<dyn RoutingTransport>, config: AccountSelectionConfig) -> Self {
        Self {
            transport,
            counters: Arc::new(Counters::default()),
            config,
        }
    }

    pub fn counters(&self) -> Arc<Counters> {
        Arc::clone(&self.counters)
    }

    pub fn config(&self) -> &AccountSelectionConfig {
        &self.config
    }

    /// Ask routing for an account, giving up at `deadline`.
    pub async fn account_for(
        &self,
        params: Value,
        deadline: tokio::time::Instant,
    ) -> AccountForOutcome {
        let body = match serde_json::to_vec(&json!({ "method": ACCOUNT_FOR_OP, "params": params }))
        {
            Ok(body) => body,
            Err(error) => {
                return AccountForOutcome::Unreachable(format!("encode request: {error}"))
            }
        };
        let started = std::time::Instant::now();
        let outcome = match tokio::time::timeout_at(deadline, self.transport.request(body)).await {
            Err(_) => AccountForOutcome::NoDecisionWithinBudget,
            Ok(Err(cause)) => AccountForOutcome::Unreachable(cause),
            Ok(Ok(reply)) => classify_account_for_reply(reply),
        };
        // Routing records no time of its own for this call, and every send waits on it, so the
        // round trip as broca sees it is logged per call: it decides whether the call can stay
        // ahead of the rest of admission or has to run beside it.
        tracing::info!(
            target: "accounts",
            "route.account_for round trip {} us ({})",
            started.elapsed().as_micros(),
            outcome.kind()
        );
        outcome
    }

    /// Read one frozen account's quota over the same transport as account selection. Each warm
    /// decision gets a fresh deadline with the same time budget used to select an account when
    /// accepting a send and freezing its configuration.
    pub async fn account_status(
        &self,
        provider: &str,
        account_id: &str,
        deadline: tokio::time::Instant,
    ) -> crate::keep_warm_quota::StatusOutcome {
        use crate::keep_warm_quota::{classify_status_transport, StatusOutcome, ACCOUNT_STATUS_OP};
        let body = serde_json::to_vec(&json!({ "method": ACCOUNT_STATUS_OP, "params": { "provider": provider, "account_id": account_id } })).expect("string-only account status query encodes");
        match tokio::time::timeout_at(deadline, self.transport.request(body)).await {
            Ok(reply) => classify_status_transport(reply),
            Err(_) => StatusOutcome::Unavailable {
                cause: format!(
                    "no reply within the {} ms routing budget",
                    ADMISSION_BUDGET.as_millis()
                ),
            },
        }
    }

    /// Send one report and wait at most [`OBSERVE_BUDGET`] for routing to take it. Never
    /// retried: a lost report only costs routing that one observation, and routing expires on
    /// its own the lease of a run whose `run_finished` never arrived.
    pub async fn observe(&self, params: Value) {
        let body =
            match serde_json::to_vec(&json!({ "method": ACCOUNT_OBSERVE_OP, "params": params })) {
                Ok(body) => body,
                Err(_) => {
                    self.counters.bump("observe_failed");
                    return;
                }
            };
        let kind = params["kind"].as_str().unwrap_or("?").to_string();
        match tokio::time::timeout(OBSERVE_BUDGET, self.transport.request(body)).await {
            Ok(Ok(Ok(_))) => self.counters.bump("observe_sent"),
            Ok(Ok(Err(error)))
                if serde_json::from_slice::<ErrorBodyView>(&error)
                    .is_ok_and(|error| error.code == SELECTION_DISABLED) =>
            {
                // Expected while routing's account selection is switched off: it refuses every
                // account operation, yet still records the report for the decisions it computes
                // without acting on them.
                self.counters.bump("observe_disabled")
            }
            Ok(Ok(Err(error))) => {
                self.counters.bump("observe_failed");
                tracing::warn!(
                    target: "accounts",
                    "routing refused a {kind} account report: {}",
                    String::from_utf8_lossy(&error)
                );
            }
            Ok(Err(cause)) => {
                self.counters.bump("observe_failed");
                tracing::warn!(target: "accounts", "a {kind} account report was not delivered: {cause}");
            }
            Err(_) => {
                self.counters.bump("observe_failed");
                tracing::warn!(
                    target: "accounts",
                    "a {kind} account report got no reply within {} s",
                    OBSERVE_BUDGET.as_secs()
                );
            }
        }
    }
}

/// How a fresh run's account was chosen, before its credential is read.
#[derive(Debug, Clone, PartialEq)]
pub enum AccountChoice {
    /// Routing has nothing to say about this run: no vault, or a credential method that is not
    /// a subscription account. No decision is recorded.
    NotApplicable,
    /// Use the static pick; record `decision`.
    Static(AccountDecision),
    /// Use the credential routing chose; record `decision`.
    Routed {
        account_id: String,
        /// The vault's own credential id, verbatim: read as is, frozen as is, never parsed.
        credential_id: String,
        /// How the credential authenticates, from the vault metadata it was offered under (its
        /// `auth_method` matched this run's method). Frozen with the selection so a resume
        /// takes the method from here rather than from the id.
        method: AuthMethod,
        decision: AccountDecision,
        /// The digest of the listing the candidates were sent from.
        view_digest: String,
    },
    /// Admit on the static pick and pause for a fresh login; record `decision`.
    AuthRequired(AccountDecision),
}

impl AccountChoice {
    pub fn decision(&self) -> Option<&AccountDecision> {
        match self {
            AccountChoice::NotApplicable => None,
            AccountChoice::Static(decision)
            | AccountChoice::AuthRequired(decision)
            | AccountChoice::Routed { decision, .. } => Some(decision),
        }
    }
}

/// Everything [`choose_account`] needs about one fresh run.
pub struct ChoiceInputs<'a> {
    /// Caller affinity for this admission; an explicit key always wins.
    pub conversation_key: Option<&'a str>,
    /// Affinity frozen by the latest episode in the durable admission snapshot.
    pub inherited_conversation_key: Option<&'a str>,
    /// The credential chosen without routing: the caller's pinned method and account, else the
    /// provider's configured default method.
    pub static_credential: &'a CredentialId,
    /// True when the caller named an account.
    pub caller_named_account: bool,
    pub model: &'a str,
    pub session: &'a BindIdentity,
    pub send_id: Option<&'a str>,
    pub run_id: &'a str,
    pub initial_input_bytes: u64,
}

fn decision(
    path: &str,
    admission_id: Option<String>,
    decision_id: Option<String>,
) -> AccountDecision {
    AccountDecision {
        conversation_key: None,
        conversation_key_source: None,
        path: path.to_string(),
        admission_id,
        decision_id,
    }
}

/// Why a fresh send gets no account: routing's own refusal, or no usable answer from routing.
/// Either way the send is refused before anything is written.
#[derive(Debug, Clone, PartialEq)]
pub enum ChoiceRefusal {
    /// Routing refused; answered as `account_selection_refused`.
    Refused(AccountSelectionRefusal),
    /// Routing gave no usable answer; answered as `routing_unavailable`, class transient.
    Unavailable(RoutingUnavailable),
}

impl From<ChoiceRefusal> for broca_core::RunError {
    fn from(refusal: ChoiceRefusal) -> Self {
        match refusal {
            ChoiceRefusal::Refused(refusal) => {
                broca_core::RunError::AccountSelectionRefused(refusal)
            }
            ChoiceRefusal::Unavailable(unavailable) => {
                broca_core::RunError::RoutingUnavailable(unavailable)
            }
        }
    }
}

/// Decide how a fresh run's account is chosen, asking routing when there is a choice to make.
/// `listing` yields broca's latest vault listing; it is awaited under the same `deadline` as
/// routing's answer. `Err` refuses the send and nothing is written: routing's refusal is
/// answered as `account_selection_refused`, and no usable answer from routing (unreachable, no
/// decision by `deadline`, or a pick broca did not offer) as `routing_unavailable`.
pub async fn choose_account<L>(
    client: &RoutingClient,
    inputs: ChoiceInputs<'_>,
    listing: L,
    deadline: tokio::time::Instant,
) -> Result<AccountChoice, ChoiceRefusal>
where
    L: std::future::Future<Output = Option<Arc<ScopedList>>>,
{
    let derived = conversation_key(inputs.session);
    let conversation = inputs
        .conversation_key
        .or(inputs.inherited_conversation_key)
        .unwrap_or(&derived);
    let decision = |path: &str, admission_id, decision_id| {
        let mut result = decision(path, admission_id, decision_id);
        result.conversation_key = Some(conversation.to_owned());
        result.conversation_key_source = Some(if inputs.conversation_key.is_some() {
            broca_wal::ConversationKeySource::Caller
        } else if inputs.inherited_conversation_key.is_some() {
            broca_wal::ConversationKeySource::Inherited
        } else {
            broca_wal::ConversationKeySource::Session
        });
        result
    };
    let method = inputs.static_credential.method;
    let provider = inputs.static_credential.provider.as_str();
    if !selects_accounts(method) {
        return Ok(AccountChoice::NotApplicable);
    }
    let admission = admission_id(inputs.run_id);
    if inputs.caller_named_account {
        client.counters.bump("pinned");
        return Ok(AccountChoice::Static(decision(
            broca_wal::ACCOUNT_DECISION_PINNED,
            Some(admission),
            None,
        )));
    }
    let Ok(Some(listing)) = tokio::time::timeout_at(deadline, listing).await else {
        client.counters.bump("candidates_unknown");
        tracing::warn!(
            target: "accounts",
            "no vault listing within the admission budget; run {} takes the static {} account",
            inputs.run_id,
            inputs.static_credential.to_id_string()
        );
        return Ok(AccountChoice::Static(decision(
            broca_wal::ACCOUNT_DECISION_CANDIDATES_UNKNOWN,
            Some(admission),
            None,
        )));
    };
    let candidates = Candidates::from_listing(&listing, method, provider);
    if candidates.distinct_accounts() <= 1 {
        client.counters.bump("single_candidate");
        return Ok(AccountChoice::Static(decision(
            broca_wal::ACCOUNT_DECISION_SINGLE_CANDIDATE,
            Some(admission),
            None,
        )));
    }
    let params = account_for_params(&AccountForRequest {
        provider,
        model: inputs.model,
        conversation_key: conversation,
        admission_id: &admission,
        initial_input_bytes: inputs.initial_input_bytes,
        candidates: &candidates,
    });
    match client.account_for(params, deadline).await {
        AccountForOutcome::Chosen {
            account_id,
            credential_id,
            decision_id,
        } => {
            // Routing returns the vault credential id verbatim, and it must be one broca offered:
            // those, and only those, are credentials whose vault metadata names this run's
            // provider and method, so the frozen wire family and the token agree. Anything else
            // is a defect in routing, and the send is refused rather than run on it.
            if !candidates.offers(&account_id, &credential_id) {
                client.counters.bump("invalid_pick");
                tracing::error!(
                    target: "accounts",
                    "routing picked credential {credential_id} (account {account_id}), which is \
                     not one of the {} candidates broca offered for {provider} {}; run {} is \
                     refused routing_unavailable",
                    candidates.accounts.len(),
                    method.as_token(),
                    inputs.run_id
                );
                return Err(ChoiceRefusal::Unavailable(
                    RoutingUnavailable::invalid_pick(),
                ));
            }
            client.counters.bump("routed");
            Ok(AccountChoice::Routed {
                account_id,
                credential_id,
                method,
                decision: decision(
                    broca_wal::ACCOUNT_DECISION_ROUTED,
                    Some(admission),
                    decision_id,
                ),
                view_digest: candidates.view_digest,
            })
        }
        AccountForOutcome::Unreachable(cause) => {
            client.counters.bump("unreachable");
            tracing::warn!(
                target: "accounts",
                "routing gave no account for run {} ({cause}); the send is refused \
                 routing_unavailable",
                inputs.run_id
            );
            Err(ChoiceRefusal::Unavailable(RoutingUnavailable::unreachable()))
        }
        AccountForOutcome::NoDecisionWithinBudget => {
            client.counters.bump("no_decision_within_budget");
            let budget_ms = ADMISSION_BUDGET.as_millis() as u64;
            tracing::warn!(
                target: "routing",
                session = %broca_core::session_addr(inputs.session),
                send_id = inputs.send_id.unwrap_or("<none>"),
                provider = %provider,
                auth_method = method.as_token(),
                "routing did not provide an account decision within the {budget_ms} ms admission budget; run {} is refused routing_unavailable",
                inputs.run_id
            );
            Err(ChoiceRefusal::Unavailable(
                RoutingUnavailable::no_decision_within_budget(budget_ms),
            ))
        }
        AccountForOutcome::AuthRequired { decision_id } => {
            client.counters.bump("auth_required");
            Ok(AccountChoice::AuthRequired(decision(
                broca_wal::ACCOUNT_DECISION_AUTH_REQUIRED,
                Some(admission),
                decision_id,
            )))
        }
        AccountForOutcome::SelectionDisabled { decision_id } => {
            client.counters.bump("disabled");
            tracing::info!(
                target: "routing",
                session = %broca_core::session_addr(inputs.session),
                send_id = inputs.send_id.unwrap_or("<none>"),
                provider = %provider,
                auth_method = method.as_token(),
                routing_code = SELECTION_DISABLED,
                decision_id = decision_id.as_deref().unwrap_or("<none>"),
                "routing account selection is switched off; using the static credential"
            );
            Ok(AccountChoice::Static(decision(
                broca_wal::ACCOUNT_DECISION_DISABLED,
                Some(admission),
                decision_id,
            )))
        }
        AccountForOutcome::Refused(refusal) => {
            client.counters.bump("refused");
            client
                .counters
                .bump(&format!("refused.{}", refusal.routing_code));
            tracing::warn!(
                target: "routing",
                session = %broca_core::session_addr(inputs.session),
                send_id = inputs.send_id.unwrap_or("<none>"),
                provider = %provider,
                auth_method = method.as_token(),
                routing_code = %refusal.routing_code,
                class = ?refusal.class,
                retry_after_s = ?refusal.retry_after_s,
                resets_at_ms = ?refusal.resets_at_ms,
                decision_id = refusal.decision_id.as_deref().unwrap_or("<none>"),
                "routing refused account selection"
            );
            if matches!(
                refusal.routing_code.as_str(),
                "admission_conflict" | "invalid_request"
            ) {
                // `admission_conflict` and `invalid_request` both mean broca sent a request
                // routing could not accept: a defect in broca, not a state of the account pool.
                tracing::error!(
                    target: "accounts",
                    "routing refused run {}'s account request as {} (decision {:?})",
                    inputs.run_id,
                    refusal.routing_code,
                    refusal.decision_id
                );
            }
            Err(ChoiceRefusal::Refused(refusal))
        }
    }
}

// ------------------------------------------------------------------------------------------
// Reports
// ------------------------------------------------------------------------------------------

/// A report's `event_id`: the WAL record it reports, as its log's lineage id and sequence
/// number. `None` for a log with no lineage id, which cannot name its records stably.
pub fn event_id(lineage: Option<WalLineageId>, wal_seq: u64) -> Option<String> {
    lineage.map(|lineage| format!("{lineage}:{wal_seq}"))
}

fn report(kind: &str, event_id: &str, admission_id: &str) -> Map<String, Value> {
    let mut body = Map::new();
    body.insert("event_id".into(), json!(event_id));
    body.insert("caller".into(), json!({ "runner": RUNNER }));
    body.insert("kind".into(), json!(kind));
    body.insert("admission_id".into(), json!(admission_id));
    body
}

/// `admission_started`: the run's `RunStarted` is durable.
pub fn admission_started_body(event_id: &str, admission_id: &str, run_id: &str) -> Value {
    let mut body = report("admission_started", event_id, admission_id);
    body.insert("run_id".into(), json!(run_id));
    Value::Object(body)
}

/// Whether an attempt ended before any status line arrived.
fn attempt_reached_no_upstream(attempt: &ModelAttempt) -> bool {
    attempt.http_status.is_none()
}

/// One provider attempt's report. An attempt that ended in an account-limit refusal is
/// reported as `account_limit_exhausted` with the reset and window fields it recorded; every
/// other attempt is `attempt`. The rate-limit headers travel exactly as received, under their
/// lowercase names, whenever the response carried any.
pub fn attempt_body(
    event_id: &str,
    admission_id: &str,
    attempt: &ModelAttempt,
    view_digest: Option<&str>,
) -> Value {
    let observation = &attempt.account_observation;
    let headers = &observation.ratelimit_headers;
    if let Some(limit) = &observation.limit {
        let mut body = report("account_limit_exhausted", event_id, admission_id);
        if let Some(window) = &limit.limit_window {
            body.insert("limit_window".into(), json!(window));
        }
        if let Some(resets_at_ms) = limit.resets_at_ms {
            body.insert("resets_at_ms".into(), json!(resets_at_ms));
        }
        if let Some(source) = &limit.resets_at_source {
            body.insert("resets_at_source".into(), json!(source));
        }
        body.insert("observed_at_ms".into(), json!(attempt.ts_ms));
        if !headers.is_empty() {
            body.insert("upstream_origin".into(), json!("upstream"));
            body.insert("headers".into(), json!(headers));
        }
        return Value::Object(body);
    }
    let mut body = report("attempt", event_id, admission_id);
    body.insert("upstream_origin".into(), json!("upstream"));
    if attempt_reached_no_upstream(attempt) {
        body.insert("terminal_reason".into(), json!("transport_failure"));
        return Value::Object(body);
    }
    body.insert("observed_at_ms".into(), json!(attempt.ts_ms));
    if let Some(view_digest) = view_digest {
        body.insert("view_digest".into(), json!(view_digest));
    }
    if !headers.is_empty() {
        body.insert("headers".into(), json!(headers));
    }
    Value::Object(body)
}

/// `run_finished`: the run's terminal is durable. Carries the latest rate-limit headers the
/// run saw, when it saw any. Routing ends the run's lease on it.
pub fn run_finished_body(
    event_id: &str,
    admission_id: &str,
    run_id: &str,
    reason: RunFinishReason,
    latest_headers: Option<&BTreeMap<String, String>>,
) -> Value {
    let mut body = report("run_finished", event_id, admission_id);
    body.insert("run_id".into(), json!(run_id));
    body.insert(
        "terminal_reason".into(),
        serde_json::to_value(reason).unwrap_or(Value::Null),
    );
    body.insert("upstream_origin".into(), json!("upstream"));
    if let Some(headers) = latest_headers.filter(|headers| !headers.is_empty()) {
        body.insert("headers".into(), json!(headers));
    }
    Value::Object(body)
}

/// `scoped_not_found`: the vault did not serve the credential routing chose. Routing excludes
/// that account for this admission only.
pub fn scoped_not_found_body(
    event_id: &str,
    admission_id: &str,
    credential_id: &str,
    view_digest: &str,
) -> Value {
    let mut body = report("scoped_not_found", event_id, admission_id);
    body.insert("credential_id".into(), json!(credential_id));
    body.insert("view_digest".into(), json!(view_digest));
    Value::Object(body)
}

enum ReportEvent {
    Started {
        lineage: Option<WalLineageId>,
        wal_seq: u64,
    },
    Attempt {
        lineage: Option<WalLineageId>,
        wal_seq: u64,
        attempt: Box<ModelAttempt>,
    },
    Finished {
        lineage: Option<WalLineageId>,
        wal_seq: u64,
        reason: RunFinishReason,
    },
}

/// One run's reports. The run loop's hooks queue events here; a task of its own sends them, in
/// order, each only once the record it names is durable.
pub struct RunReporter {
    tx: tokio::sync::mpsc::UnboundedSender<ReportEvent>,
}

impl RunReporter {
    /// Start reporting run `run_id`, known to routing as `admission_id`. `durable_head` is the
    /// run's WAL writer's durable-sequence gauge. `view_digest` is the listing digest the
    /// run's credential was chosen from, when this process chose it.
    pub fn start(
        client: Arc<RoutingClient>,
        admission_id: String,
        run_id: String,
        view_digest: Option<String>,
        durable_head: Arc<AtomicU64>,
    ) -> Arc<Self> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ReportEvent>();
        tokio::spawn(async move {
            let mut latest_headers: Option<BTreeMap<String, String>> = None;
            while let Some(event) = rx.recv().await {
                match event {
                    ReportEvent::Started { lineage, wal_seq } => {
                        let Some(id) = event_id_or_count(&client, lineage, wal_seq) else {
                            continue;
                        };
                        client
                            .observe(admission_started_body(&id, &admission_id, &run_id))
                            .await;
                    }
                    ReportEvent::Attempt {
                        lineage,
                        wal_seq,
                        attempt,
                    } => {
                        if !attempt.account_observation.ratelimit_headers.is_empty() {
                            latest_headers =
                                Some(attempt.account_observation.ratelimit_headers.clone());
                        }
                        if !wait_durable(&durable_head, wal_seq).await {
                            client.counters.bump("observe_failed");
                            tracing::warn!(
                                target: "accounts",
                                "run {run_id}'s attempt record {wal_seq} never became durable; \
                                 its account report is dropped"
                            );
                            continue;
                        }
                        let Some(id) = event_id_or_count(&client, lineage, wal_seq) else {
                            continue;
                        };
                        client
                            .observe(attempt_body(
                                &id,
                                &admission_id,
                                &attempt,
                                view_digest.as_deref(),
                            ))
                            .await;
                    }
                    ReportEvent::Finished {
                        lineage,
                        wal_seq,
                        reason,
                    } => {
                        if let Some(id) = event_id_or_count(&client, lineage, wal_seq) {
                            client
                                .observe(run_finished_body(
                                    &id,
                                    &admission_id,
                                    &run_id,
                                    reason,
                                    latest_headers.as_ref(),
                                ))
                                .await;
                        }
                        break;
                    }
                }
            }
        });
        Arc::new(Self { tx })
    }

    /// The run's terminal, at `wal_seq`, is durable. Sent after every report already queued.
    pub fn finish(&self, lineage: Option<WalLineageId>, wal_seq: u64, reason: RunFinishReason) {
        let _ = self.tx.send(ReportEvent::Finished {
            lineage,
            wal_seq,
            reason,
        });
    }
}

impl broca_core::AccountReporter for RunReporter {
    fn run_started(&self, lineage: Option<WalLineageId>, wal_seq: u64) {
        let _ = self.tx.send(ReportEvent::Started { lineage, wal_seq });
    }

    fn attempt_recorded(
        &self,
        lineage: Option<WalLineageId>,
        wal_seq: u64,
        attempt: &ModelAttempt,
    ) {
        let _ = self.tx.send(ReportEvent::Attempt {
            lineage,
            wal_seq,
            attempt: Box::new(attempt.clone()),
        });
    }
}

fn event_id_or_count(
    client: &RoutingClient,
    lineage: Option<WalLineageId>,
    wal_seq: u64,
) -> Option<String> {
    let id = event_id(lineage, wal_seq);
    if id.is_none() {
        client.counters.bump("observe_failed");
        tracing::warn!(
            target: "accounts",
            "WAL record {wal_seq} has no lineage id to name it by; its account report is dropped"
        );
    }
    id
}

/// Wait until the writer's durable head reaches `wal_seq`. False when it did not within
/// [`DURABILITY_MAX_WAIT`].
async fn wait_durable(durable_head: &AtomicU64, wal_seq: u64) -> bool {
    let give_up = tokio::time::Instant::now() + DURABILITY_MAX_WAIT;
    while durable_head.load(Ordering::Acquire) < wal_seq {
        if tokio::time::Instant::now() >= give_up {
            return false;
        }
        tokio::time::sleep(DURABILITY_POLL).await;
    }
    true
}

/// Report the `Interrupted` terminal the module wrote for a run it sealed (a shutdown, or a
/// run a crash left in flight). Does nothing for a run with no admission id.
pub fn report_sealed(client: Arc<RoutingClient>, sealed: &broca_module::SealedRun) {
    let Some(admission_id) = sealed
        .auth_selection
        .as_ref()
        .and_then(|selection| selection.account_decision.as_ref())
        .and_then(|decision| decision.admission_id.clone())
    else {
        return;
    };
    let Some(id) = event_id_or_count(&client, sealed.lineage, sealed.terminal_seq) else {
        return;
    };
    let body = run_finished_body(
        &id,
        &admission_id,
        &sealed.run_id.0,
        RunFinishReason::Interrupted,
        None,
    );
    tokio::spawn(async move { client.observe(body).await });
}

/// The vault did not serve the credential routing chose: tell routing (which excludes that
/// account for this admission only), then re-list. When the fresh listing still names the
/// credential, the vault refused a credential it lists: raise the alarm, once per occurrence.
/// Runs in a task of its own; the admission is refused without waiting for it.
pub fn report_scoped_not_found<F>(
    client: Arc<RoutingClient>,
    admission_id: String,
    credential_id: String,
    view_digest: String,
    relist: F,
) where
    F: std::future::Future<Output = Option<Arc<ScopedList>>> + Send + 'static,
{
    client.counters.bump("scoped_not_found");
    tokio::spawn(async move {
        // No WAL record names a refused admission, so the event is named by the admission and
        // the moment; each occurrence is a distinct report.
        let id = format!(
            "{admission_id}:scoped_not_found:{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_millis())
                .unwrap_or_default()
        );
        client
            .observe(scoped_not_found_body(
                &id,
                &admission_id,
                &credential_id,
                &view_digest,
            ))
            .await;
        if let Some(listing) = relist.await {
            if listing
                .credentials
                .iter()
                .any(|row| row.id == credential_id)
            {
                client.counters.bump("scoped_not_found_still_listed");
                tracing::warn!(
                    target: "accounts",
                    "the vault refused credential {credential_id} for admission {admission_id} \
                     although its listing still names it"
                );
            }
        }
    });
}

// ------------------------------------------------------------------------------------------
// The vault listing
// ------------------------------------------------------------------------------------------

/// broca's latest `credential.list_scoped` answer, the source of the candidates it sends
/// routing. Kept for [`LISTING_MAX_AGE`]; an older one is still used while a refresh runs.
#[derive(Default)]
pub struct ListingCache {
    latest: Mutex<Option<(Arc<ScopedList>, std::time::Instant)>>,
    /// Set while a background refresh is in flight, so a burst of admissions starts one.
    refreshing: std::sync::atomic::AtomicBool,
    /// The credentials of the latest listing that lack account-selection metadata.
    unmapped: Arc<UnmappedCredentials>,
}

impl ListingCache {
    /// Claim the one background refresh; false when one is already running.
    pub fn begin_refresh(&self) -> bool {
        !self.refreshing.swap(true, Ordering::AcqRel)
    }

    pub fn end_refresh(&self) {
        self.refreshing.store(false, Ordering::Release);
    }

    /// Keep `listing` as the latest, and record (warning once per newly unmapped id) which of
    /// its credentials lack account-selection metadata.
    pub fn store(&self, listing: ScopedList) -> Arc<ScopedList> {
        self.unmapped.observe(&listing);
        let listing = Arc::new(listing);
        *self.latest.lock().unwrap() = Some((Arc::clone(&listing), std::time::Instant::now()));
        listing
    }

    /// The unmapped credentials of the latest listing, for `health.check`.
    pub fn unmapped(&self) -> Arc<UnmappedCredentials> {
        Arc::clone(&self.unmapped)
    }

    /// The cached listing and whether it is due for a refresh.
    pub fn get(&self) -> Option<(Arc<ScopedList>, bool)> {
        self.latest
            .lock()
            .unwrap()
            .as_ref()
            .map(|(listing, at)| (Arc::clone(listing), at.elapsed() >= LISTING_MAX_AGE))
    }
}

#[cfg(test)]
#[path = "account_routing_tests.rs"]
mod tests;
