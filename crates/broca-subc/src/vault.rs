//! The credential-vault consumer: a second outbound subc consumer (alongside
//! [`crate::SubcToolPlane`]) that resolves a provider credential from the
//! `claustrum` module via `credential.get`.
//!
//! ## Why this exists
//! OAuth credentials (anthropic/google/openai/xai) expire; their access tokens must be
//! refreshed from the refresh token before each run. The vault owns that lifecycle
//! entirely. broca names WHAT it wants with a stable, unguessable HANDLE (a bearer
//! secret) and gets back a fresh token — it never touches refresh/expiry/rotation. A
//! handle is the runtime scoping primitive (in the v1 trusted-unscoped model there is no
//! caller identity, so a guessable id would let any admitted consumer read other creds);
//! it is stable across resume (refresh updates the credential record BEHIND the handle,
//! not the handle).
//!
//! A credential can also be read WITHOUT a handle, by credential id, when the operator has
//! granted this process's supervised principal (`reserved:<module>`) `read` on it:
//! `credential.get_scoped`. The vault attributes the principal from the consumer identity the
//! route open presents, so a process without one is refused here before anything is sent.
//!
//! ## The wire contract (locked with claustrum)
//! Request on a bound management route, `FrameType::Request`, body:
//!   `{ "method": "credential.get", "params": { "handle", "min_ttl_ms"?, "force_refresh"? } }`
//!   `{ "method": "credential.get_scoped", "params": { "credential_id", "min_ttl_ms"? } }`
//! Both answer with the same reply shape, decoded below.
//!
//! Reply, decoded in TWO levels:
//!  - A `FrameType::Error` frame is normally a PROTOCOL error (the request was malformed or
//!    authoritatively rejected). The daemon's `unknown_channel` rejection is the exception:
//!    it identifies a stale route after module restart and is recovered as transport state.
//!  - A `FrameType::Response` frame carries the application result, ALWAYS shaped
//!    `{ "result": <outcome> }`. `<outcome>` is serde-UNTAGGED — decode by which key is
//!    present:
//!      * success: `{ "payload": [u8,..], "expires_at_ms": i64|null, "record_version": u64 }`
//!        (`payload` is a byte array — reassemble to the token string).
//!      * app error: `{ "error": { "code": "<snake_case>" } }` — INCLUDING `needs_reauth`,
//!        which comes back as a RESPONSE, not an Error frame. So we parse `{result}` FIRST
//!        and branch on the present key; an Error frame is never an auth state.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::connection::{
    route_failure_is_known_dead, ConnError, RouteHandle, RouteOpenRetry, SubcConnection,
    ROUTE_OPEN_ATTEMPT_TIMEOUT,
};

/// The widest wait between route-open attempts while the credentials module refuses with a
/// temporary code. The whole resolve fits in a few seconds, so a half-second cap keeps
/// attempts frequent enough to catch the module the moment it is back, at a handful of
/// route.open calls per second at most.
const VAULT_ROUTE_OPEN_MAX_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

/// Slack on top of the backstop in `resolve_route`, past the readiness deadline plus one
/// [`ROUTE_OPEN_ATTEMPT_TIMEOUT`]. The retry loop can start its last attempt AT the readiness
/// deadline, and that attempt ends on its own clock with an error naming the route-open
/// deadline; the slack lets that named error arrive instead of this client's generic one.
const VAULT_ROUTE_OPEN_REPLY_GRACE: std::time::Duration = std::time::Duration::from_millis(500);

/// The outcome of resolving a credential — the error variants map 1:1 to the loop actions
/// the caller takes. Kept as a typed enum (not a string) so the caller's match is
/// exhaustive and a new vault error code can't be silently mishandled.
/// The protocol and transport variants can contain upstream text, so `Debug` is implemented
/// manually to keep credentials out of diagnostics. Do not derive `Debug` for this type.
#[derive(Clone, PartialEq, Eq)]
pub enum VaultError {
    /// The refresh token itself is dead (expired/revoked) — the vault CANNOT mint a token;
    /// the user must re-auth. AUTHORITATIVE on the first `credential.get` (the vault maps a
    /// dead-token refresh to this on the same call, not the next). The caller pauses the
    /// run as `RunPaused{AuthRequired}` — recoverable: once the user re-auths, the run
    /// continues under the same run id from its last durable boundary.
    NeedsReauth,
    /// A transient refresh failure (transport/decode/5xx/unexpected status, or an
    /// entitlement lapse such as xai 403 — kept DISTINCT from a dead refresh token). The
    /// caller treats it as a transient error (retryable), not a re-auth event.
    RefreshFailed,
    /// The vault daemon is locked (its master key was rotated out from under it; it needs a
    /// restart). The caller backs off — not a per-credential fault.
    VaultLocked,
    /// No credential record for this handle — the handle is wrong or revoked. This is a
    /// CONFIG error on OUR side (the handle map), NOT a user re-auth event; fail loud so
    /// the misconfiguration is fixed, never silently paused.
    NotFound,
    /// The credential record is non-refreshable. Won't occur for the four OAuth creds; a
    /// config-class fault if seen.
    RefreshUnsupported,
    /// The stored credential record is corrupt — a record fault, fail loud.
    Corrupt,
    /// The vault reported too many items for a bulk request (not used by single `get`).
    TooManyItems,
    /// The vault could not mint a token satisfying our `min_ttl_ms` demand, even after a real
    /// upstream exchange proved a FRESH token still falls short.
    ///
    /// REDUCE AND RETRY, NEVER WAIT AND RETRY. Waiting cannot help — the demand is evaluated
    /// against a newly minted token, so a later attempt asks the same impossible question and
    /// spends another upstream mint doing it. A consumer that backs off and retries the same
    /// demand loops against the provider's mint budget forever.
    ///
    /// The demand is ALWAYS A NUMBER THIS CODEBASE CHOSE (`VAULT_MIN_TTL_MS`, and the vault
    /// applies no implicit floor), so "reduce the demand" is an instruction the operator can
    /// actually act on rather than advice about another system's policy.
    ///
    /// UNREACHABLE ON THE CURRENT CREDENTIAL INVENTORY, and named anyway. Measured with CKCRED
    /// 2026-09-09 across the three families broca reads: chatgpt:openai ~660m, oauth:anthropic
    /// 86-111m, antigravity:google ~49m, against a 10m demand — nearest margin ~5x. Two reasons
    /// it is worth a variant while it cannot fire: without it the code fails to DECODE and
    /// surfaces as "protocol error", which sends the reader to the handle map, the one thing
    /// that is not wrong; and the classification below was previously safe only BY ACCIDENT
    /// (see `ReadErrorCode`).
    TtlUnsatisfiable,
    /// A PROTOCOL error: the request was malformed (an `Error` frame), or the reply could
    /// not be decoded to the contract. A bug in THIS code or a contract drift — fail loud,
    /// never an auth state.
    Protocol(String),
    /// A transport failure reaching the vault module over subc.
    Transport(String),
    /// A credential-id read was refused HERE, before anything was sent: this process presents
    /// no consumer identity on its route opens, so the vault would see it as `Direct`, which no
    /// grant covers, and answer `not_found` with nothing to say why. Carries the credential id
    /// that was asked for. A configuration fault on this side: the read has to come from a
    /// process the daemon supervises.
    NoConsumerIdentity { credential_id: String },
}

impl std::fmt::Debug for VaultError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VaultError::NeedsReauth => f.write_str("NeedsReauth"),
            VaultError::RefreshFailed => f.write_str("RefreshFailed"),
            VaultError::VaultLocked => f.write_str("VaultLocked"),
            VaultError::NotFound => f.write_str("NotFound"),
            VaultError::RefreshUnsupported => f.write_str("RefreshUnsupported"),
            VaultError::Corrupt => f.write_str("Corrupt"),
            VaultError::TooManyItems => f.write_str("TooManyItems"),
            VaultError::TtlUnsatisfiable => f.write_str("TtlUnsatisfiable"),
            VaultError::Protocol(_) => f.debug_tuple("Protocol").field(&"<redacted>").finish(),
            VaultError::Transport(_) => f.debug_tuple("Transport").field(&"<redacted>").finish(),
            VaultError::NoConsumerIdentity { credential_id } => f
                .debug_struct("NoConsumerIdentity")
                .field("credential_id", credential_id)
                .finish(),
        }
    }
}

impl std::fmt::Display for VaultError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VaultError::NeedsReauth => write!(f, "credential needs re-authentication"),
            VaultError::RefreshFailed => write!(f, "credential refresh failed (transient)"),
            VaultError::VaultLocked => write!(f, "credential vault is locked"),
            VaultError::NotFound => write!(f, "credential handle not found"),
            VaultError::RefreshUnsupported => write!(f, "credential is not refreshable"),
            VaultError::Corrupt => write!(f, "credential record is corrupt"),
            VaultError::TooManyItems => write!(f, "too many credential items requested"),
            // NAMES THE DEMAND AND THE REPAIR, because the caller supplied the number. Without
            // this arm the code fails to DECODE and surfaces as "protocol error", which sends
            // the reader to the handle map -- the one thing that is not wrong.
            VaultError::TtlUnsatisfiable => write!(
                f,
                "vault cannot mint a token meeting the requested minimum lifetime; \
                 reduce the min_ttl_ms demand (retrying the same demand cannot succeed)"
            ),
            VaultError::Protocol(m) => write!(f, "vault protocol error: {m}"),
            VaultError::Transport(m) => write!(f, "vault transport error: {m}"),
            VaultError::NoConsumerIdentity { credential_id } => write!(
                f,
                "credential '{credential_id}' is read by grant (credential.get_scoped), but this \
                 process has no supervised consumer identity: it was not spawned by the subc \
                 daemon, so the vault would see an anonymous caller that no grant covers. \
                 Nothing was sent. Read it from the supervised broca module"
            ),
        }
    }
}

/// How one credential is addressed on the vault's read surface. The two forms are authorized
/// differently: a handle is a bearer secret that authorizes whoever holds it, and a credential
/// id is authorized by a read grant the operator gave this process's supervised principal.
///
/// No `Debug`: the handle is a secret.
#[derive(Clone, Copy)]
pub enum VaultAddress<'a> {
    /// A `ckh_` bearer handle from the handle file.
    Handle(&'a str),
    /// A vault credential id, read under the caller's grant.
    CredentialId(&'a str),
}

impl std::error::Error for VaultError {}

/// The outcome of one [`VaultClient::get`], together with whether the vault replied to it.
///
/// `vault_replied` is true iff a reply body from the vault module arrived for this request:
/// a token, or any answer the vault itself sent (not found, needs re-auth, locked, refresh
/// failed, or a body that fails to decode). It is false for everything that happened before
/// the request reached the vault or while its reply was lost: a refused or failed route open,
/// a daemon rejection of the request, a transport failure, a bad frame on the connection.
///
/// It is a separate field because the error cannot carry it: a daemon refusal and an
/// undecodable vault reply are both [`VaultError::Protocol`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VaultGet {
    pub result: Result<VaultToken, VaultError>,
    pub vault_replied: bool,
}

impl VaultGet {
    /// The vault answered; `result` is what the answer said.
    pub fn replied(result: Result<VaultToken, VaultError>) -> Self {
        Self {
            result,
            vault_replied: true,
        }
    }

    /// No reply from the vault arrived for this request.
    pub fn no_reply(error: VaultError) -> Self {
        Self {
            result: Err(error),
            vault_replied: false,
        }
    }
}

/// A successfully resolved credential: the token plus its post-refresh metadata.
///
/// The token is a bearer secret, so `Debug` is implemented manually to redact it. Do not derive
/// `Debug` for this type.
#[derive(Clone, PartialEq, Eq)]
pub struct VaultToken {
    /// The credential token bytes (e.g. an OAuth access token), reassembled from the wire
    /// `payload` byte array.
    pub token: String,
    /// The post-refresh expiry, when the provider supplies one (`None` e.g. for OpenAI).
    pub expires_at_ms: Option<i64>,
    /// A monotonic version of the credential record — usable as a cache key.
    pub record_version: u64,
    /// The cloud project bound to this credential — `Some` ONLY for a Google Antigravity
    /// credential (the vault resolves the effective project from the refresh record and
    /// returns the bare string; never the refresh token). `None` for every other credential
    /// kind. The Antigravity wire family needs it as a BODY render input (frozen at run
    /// start), so it travels alongside the token here rather than being looked up separately.
    pub project_id: Option<String>,
    /// The provider-side account this credential belongs to — `Some` for a ChatGPT
    /// credential (the vault parses it from the token at refresh time). Preferred over
    /// client-side JWT parsing for the `ChatGPT-Account-Id` header; `None` falls back to
    /// the local parse. Keyed to `record_version`: a re-login mints a new version, so any
    /// caller caching this binding must key on (handle, record_version), never handle alone.
    pub account_id: Option<String>,
    /// The vault's independent witness of which credential the handle resolved to.
    pub resolved_credential_id: Option<String>,
}

impl std::fmt::Debug for VaultToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VaultToken")
            .field("token", &"<redacted>")
            .field("expires_at_ms", &self.expires_at_ms)
            .field("record_version", &self.record_version)
            .finish()
    }
}

/// Plaintext vault metadata returned without decrypting credentials or minting tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialStatus {
    pub ready: bool,
    pub lease_held: bool,
    /// Required on the wire, with null meaning there is no error.
    #[serde(deserialize_with = "Option::deserialize")]
    pub last_error_code: Option<String>,
    /// Absent when the supplied handle or credential ID does not resolve a credential.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<String>,
    /// Absent for an unresolved handle; absence must not become version zero.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_version: Option<u64>,
    /// Absent for an unresolved handle, not an assertion that no repair is pending.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_pending: Option<bool>,
}

#[derive(Serialize)]
struct StatusRequest<'a> {
    method: &'a str,
    params: StatusParams<'a>,
}

// Exactly one address is sent. The vault refuses a request carrying both, because the two
// forms go through different authorization paths (a bearer handle, or a read grant).
#[derive(Serialize)]
struct StatusParams<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    handle: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    credential_id: Option<&'a str>,
}

impl<'a> StatusParams<'a> {
    fn at(address: VaultAddress<'a>) -> Self {
        match address {
            VaultAddress::Handle(handle) => Self {
                handle: Some(handle),
                credential_id: None,
            },
            VaultAddress::CredentialId(credential_id) => Self {
                handle: None,
                credential_id: Some(credential_id),
            },
        }
    }
}

#[derive(Deserialize)]
struct StatusResponse {
    result: CredentialStatus,
}

fn decode_status_reply(body: &serde_json::Value) -> Result<CredentialStatus, VaultError> {
    let parsed: StatusResponse = serde_json::from_value(body.clone())
        .map_err(|e| VaultError::Protocol(format!("decode credential.status result: {e}")))?;
    Ok(parsed.result)
}

// ── wire types (the locked credential.get contract; one shared shape, no drift) ──

#[derive(Serialize)]
struct GetRequest<'a> {
    method: &'a str,
    params: GetParams<'a>,
}

#[derive(Serialize)]
struct GetParams<'a> {
    handle: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_ttl_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    force_refresh: Option<bool>,
}

/// `credential.get_scoped`: addressed by credential id, authorized by the caller's read grant.
/// It has no `force_refresh`: the vault never mints on demand for a grant-addressed read.
#[derive(Serialize)]
struct GetScopedRequest<'a> {
    method: &'a str,
    params: GetScopedParams<'a>,
}

#[derive(Serialize)]
struct GetScopedParams<'a> {
    credential_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_ttl_ms: Option<i64>,
}

#[derive(Serialize)]
struct ReportRequest<'a> {
    method: &'a str,
    params: ReportParams<'a>,
}

#[derive(Serialize)]
struct ReportParams<'a> {
    /// Exactly one of `handle` and `credential_id` is sent; see [`StatusParams`].
    #[serde(skip_serializing_if = "Option::is_none")]
    handle: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    credential_id: Option<&'a str>,
    provider_status: u16,
    /// Where broca saw the provider status. Claustrum records it for forensics from a closed
    /// vocabulary (`direct`, `relay_status_field`, `relay_message_parse`); broca always reads
    /// the status off its own HTTP response, so it always sends `direct`.
    reporter_source: &'a str,
    /// The record_version of the credential THIS consumer was actually served (from
    /// `GetResult.record_version`). The vault CAS-invalidates: the report flips the
    /// credential to needs_reauth ONLY if this still matches the current version; a
    /// report for a superseded version is a silent no-op. This closes the refresh race
    /// (a stale 401 report cannot kill a token the vault already refreshed) and bounds a
    /// buggy consumer to invalidating only the version it was served.
    record_version: u64,
}

/// The reply envelope: `{ "result": <outcome> }`.
#[derive(Deserialize)]
struct GetResponse {
    result: GetOutcome,
}

/// The untagged outcome: a `payload` key means success; an `error` key means an app error.
///
/// A FIELD THE VAULT SENDS THAT IS NOT NAMED HERE IS DISCARDED WITHOUT TRACE. Not filtered --
/// there is no list it was omitted from, so a search of this repository for its name returns
/// nothing, and nothing reads as "we do not use it" rather than "it arrives and is dropped".
/// Adding `deny_unknown_fields` would be wrong here (it would refuse a newer vault outright,
/// and untagged enums cannot use it anyway), so the property stands: this struct is the ONLY
/// record of what we consume from that wire.
///
/// Measured on the other side of the same seam, 2026-08-13: I shipped `usage_policy_version`
/// on my export facts and the consumer's fixed DTO discarded it exactly this way. I verified
/// the field end to end -- constant, construction site, a stamp read off a live fact, a
/// predicate against my own store -- and every check was inside my own boundary, so all four
/// said yes while the field reached nobody. If you need to know whether a field you send or
/// receive is USED, query the receiving side; verifying to the edge of your own system reads
/// exactly like verifying the mechanism.
#[derive(Deserialize)]
#[serde(untagged)]
enum GetOutcome {
    Success {
        payload: Vec<u8>,
        #[serde(default)]
        expires_at_ms: Option<i64>,
        record_version: u64,
        // Present (and non-null) only for an Antigravity credential; absent on the wire for
        // every other credential kind (the vault serde-skips it when None).
        #[serde(default)]
        project_id: Option<String>,
        // Present only for a ChatGPT credential (vault-parsed from the token). Absent on
        // the wire elsewhere.
        #[serde(default)]
        account_id: Option<String>,
        // Absence is deliberate information-hiding on unresolved addresses: no witness
        // is available, not evidence of drift or a malformed reply.
        #[serde(default)]
        credential_id: Option<String>,
    },
    AppError {
        error: ReadError,
    },
}

#[derive(Deserialize)]
struct ReadError {
    code: ReadErrorCode,
}

/// The vault's `ReadError` codes (serde `rename_all = "snake_case"`), mapped 1:1 to
/// [`VaultError`] variants by [`ReadErrorCode::into_error`].
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ReadErrorCode {
    NotFound,
    NeedsReauth,
    RefreshUnsupported,
    RefreshFailed,
    VaultLocked,
    Corrupt,
    TooManyItems,
    TtlUnsatisfiable,
}

impl ReadErrorCode {
    fn into_error(self) -> VaultError {
        match self {
            ReadErrorCode::NotFound => VaultError::NotFound,
            ReadErrorCode::NeedsReauth => VaultError::NeedsReauth,
            ReadErrorCode::RefreshUnsupported => VaultError::RefreshUnsupported,
            ReadErrorCode::RefreshFailed => VaultError::RefreshFailed,
            ReadErrorCode::VaultLocked => VaultError::VaultLocked,
            ReadErrorCode::Corrupt => VaultError::Corrupt,
            ReadErrorCode::TooManyItems => VaultError::TooManyItems,
            ReadErrorCode::TtlUnsatisfiable => VaultError::TtlUnsatisfiable,
        }
    }
}

/// Decode a `credential.get` reply body (`{ "result": <outcome> }`) into a token or a typed
/// error. Pure (no IO) so the full contract — untagged success-vs-error, snake_case codes,
/// the `payload`-is-bytes detail — is unit-tested against fixed wire bodies. A decode
/// failure is a `Protocol` error (contract drift), never an auth state.
fn decode_get_reply(body: &serde_json::Value) -> Result<VaultToken, VaultError> {
    let parsed: GetResponse = serde_json::from_value(body.clone())
        .map_err(|e| VaultError::Protocol(format!("decode credential.get result: {e}")))?;
    match parsed.result {
        GetOutcome::Success {
            payload,
            expires_at_ms,
            record_version,
            project_id,
            account_id,
            credential_id,
        } => {
            let token = String::from_utf8(payload).map_err(|_| {
                VaultError::Protocol("credential payload is not valid UTF-8".to_string())
            })?;
            Ok(VaultToken {
                token,
                expires_at_ms,
                record_version,
                project_id,
                // Older vaults may serialize a missing ChatGPT account id as an
                // empty string. Normalize that wire spelling so callers use their
                // JWT fallback or fail loudly instead of sending an empty header.
                account_id: account_id.filter(|value| !value.trim().is_empty()),
                // Empty ids carry no witness either; preserve non-empty ids verbatim,
                // but normalize empty spellings just like account_id above.
                resolved_credential_id: credential_id.filter(|value| !value.trim().is_empty()),
            })
        }
        GetOutcome::AppError { error } => Err(error.code.into_error()),
    }
}

/// The `reporter_source` broca puts on every failure report; see [`ReportParams`].
const REPORTER_SOURCE_DIRECT: &str = "direct";

// ── credential.list_scoped: every credential this process's read grants cover ──

/// One credential row from `credential.list_scoped`. Metadata only: no payload, no token.
///
/// Rows are NOT filtered by state. A credential that `needs_reauth` is listed with that state,
/// because a caller auditing its credentials needs to see a dead one, not have it disappear.
///
/// Unknown fields are ignored, so a newer vault that adds a column still decodes. The fields
/// the vault always writes are required; the ones it omits when empty are optional.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedCredential {
    /// The vault credential id.
    pub id: String,
    pub categories: Vec<String>,
    /// The record kind, for example `oauth` or `apikey`.
    #[serde(rename = "type")]
    pub credential_type: String,
    /// The model vendors reachable through this credential.
    pub serves: Vec<String>,
    /// The provider protocol the credential refreshes through; absent for a static credential.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_adapter: Option<String>,
    /// The lifecycle state, for example `active` or `needs_reauth`. Kept as the vault's word,
    /// not an enum, so a state added later is shown rather than refused.
    pub state: String,
    pub record_version: u64,
    /// Which operations the caller's grants allow on this row, for example `read`.
    pub operations: Vec<String>,
    /// The provider account, where the vault captured one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org_name: Option<String>,
    /// The catalog provider ids this credential serves. Empty when the vault predates the field,
    /// so an older vault still decodes; broca does not route by it yet.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_ids: Vec<String>,
    /// How the credential authenticates (for example `oauth`, `apikey`, `chatgpt`), where the
    /// vault can say. Absent on an older vault and on rows the vault cannot classify.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_method: Option<String>,
}

/// One grant that produced the listing: a selector and the operation it allows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedGrant {
    pub selector_kind: String,
    pub selector: String,
    pub operation: String,
}

/// The `credential.list_scoped` answer: the rows, the grants behind them, and `view`, the
/// vault's digest of both. Two listings with the same `view` covered the same rows under the
/// same grants, so a caller can tell cheaply whether anything changed between two audits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedList {
    pub credentials: Vec<ScopedCredential>,
    /// How many grants the caller holds.
    pub grants: usize,
    pub grant_tuples: Vec<ScopedGrant>,
    pub view: String,
}

#[derive(Deserialize)]
struct ListScopedResponse {
    result: ListScopedOutcome,
}

/// Success is the listing; an app error is `{ "error": { "code" } }`, the same shape and codes
/// as a read's. The listing is tried first: an error body has no `credentials` key.
#[derive(Deserialize)]
#[serde(untagged)]
enum ListScopedOutcome {
    Listed(ScopedList),
    AppError { error: ReadError },
}

/// Decode a `credential.list_scoped` reply body. Pure, so the decoder is tested against the
/// exact bytes claustrum's own contract test writes.
fn decode_list_scoped_reply(body: &serde_json::Value) -> Result<ScopedList, VaultError> {
    let parsed: ListScopedResponse = serde_json::from_value(body.clone())
        .map_err(|e| VaultError::Protocol(format!("decode credential.list_scoped result: {e}")))?;
    match parsed.result {
        ListScopedOutcome::Listed(list) => Ok(list),
        ListScopedOutcome::AppError { error } => Err(error.code.into_error()),
    }
}

/// A consumer of the credential vault over a subc management route. Holds a route handle
/// bound to the credentials module's management surface; `get` issues `credential.get` on
/// it. Cheap to keep alive for the process — credentials are HANDLE-keyed, not
/// session-keyed, so one route serves all providers.
///
/// The route handle is a CACHE, not a permanent binding: when the credentials module is
/// drain-restarted, the daemon frees its slot and the next request on it is rejected
/// `unknown_channel`. Every call therefore resolves the route on demand and, on a
/// transport failure or `unknown_channel`, evicts the cached handle and retries ONCE against
/// a fresh route — a vault restart costs one transparent retry instead of a terminal error on
/// every send until this process is bounced. (The same learned-dead-route discipline the
/// tool plane uses.)
#[async_trait]
trait VaultConnection: Send + Sync {
    async fn catalog_contains(&self, module_id: &str) -> Result<bool, ConnError>;

    /// Open the vault's management route, retrying a temporary refusal until `retry` runs out.
    async fn route_open_management(
        &self,
        module_id: &str,
        identity: &broca_protocol::BindIdentity,
        retry: RouteOpenRetry,
    ) -> Result<RouteHandle, ConnError>;

    /// Send one request and return the body of the Response frame that answered it, undecoded.
    /// `Ok` means a reply from the vault module arrived; decoding it is the caller's job, so a
    /// reply that fails to decode is still known to be a reply.
    async fn route_request(&self, route: &RouteHandle, body: Vec<u8>)
        -> Result<Vec<u8>, ConnError>;

    async fn route_close(&self, route: &RouteHandle) -> Result<(), ConnError>;

    /// The module id of the consumer identity route opens present, if any. Only a route
    /// opened with one is attested as a `Reserved` principal that the vault's grants can name.
    fn consumer_module_id(&self) -> Option<String>;
}

#[async_trait]
impl VaultConnection for SubcConnection {
    fn consumer_module_id(&self) -> Option<String> {
        SubcConnection::consumer_module_id(self).map(str::to_string)
    }

    async fn catalog_contains(&self, module_id: &str) -> Result<bool, ConnError> {
        self.catalog_list()
            .await
            .map(|modules| modules.iter().any(|module| module.module_id == module_id))
    }

    async fn route_open_management(
        &self,
        module_id: &str,
        identity: &broca_protocol::BindIdentity,
        retry: RouteOpenRetry,
    ) -> Result<RouteHandle, ConnError> {
        SubcConnection::route_open_management_within(self, module_id, identity, retry).await
    }

    async fn route_request(
        &self,
        route: &RouteHandle,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, ConnError> {
        SubcConnection::route_request_raw(self, route, body).await
    }

    async fn route_close(&self, route: &RouteHandle) -> Result<(), ConnError> {
        SubcConnection::route_close(self, route).await
    }
}

enum RouteState {
    Empty,
    Opening(Arc<RouteOpenAttempt>),
    Ready(RouteHandle),
}

struct RouteOpenAttempt {
    result: std::sync::Mutex<Option<Result<RouteHandle, VaultError>>>,
    ready: tokio::sync::Notify,
}

impl RouteOpenAttempt {
    fn new() -> Self {
        Self {
            result: std::sync::Mutex::new(None),
            ready: tokio::sync::Notify::new(),
        }
    }

    fn complete(&self, result: Result<RouteHandle, VaultError>) {
        *self.result.lock().unwrap() = Some(result);
        self.ready.notify_waiters();
    }

    async fn wait(&self) -> Result<RouteHandle, VaultError> {
        loop {
            let notified = self.ready.notified();
            if let Some(result) = self.result.lock().unwrap().clone() {
                return result;
            }
            notified.await;
        }
    }
}

pub struct VaultClient {
    conn: Arc<dyn VaultConnection>,
    module_id: String,
    identity: broca_protocol::BindIdentity,
    ready_timeout: std::time::Duration,
    route: std::sync::Mutex<RouteState>,
    /// Read once from the connection at construction: the identity its route opens present.
    /// `None` refuses every credential-id request locally; see [`VaultError::NoConsumerIdentity`].
    consumer_module_id: Option<String>,
}

impl VaultClient {
    /// Construct a credential consumer without contacting the credentials module. Route
    /// readiness is resolved lazily by the first vault call, so a missing dependency cannot
    /// prevent this consumer's own module from registering and serving health checks.
    pub fn new(
        conn: Arc<SubcConnection>,
        module_id: &str,
        identity: &broca_protocol::BindIdentity,
        ready_timeout: std::time::Duration,
    ) -> Self {
        let consumer_module_id = VaultConnection::consumer_module_id(conn.as_ref());
        Self {
            conn,
            module_id: module_id.to_string(),
            identity: identity.clone(),
            ready_timeout,
            route: std::sync::Mutex::new(RouteState::Empty),
            consumer_module_id,
        }
    }

    /// The principal the vault sees this client's routes as, spelled as a grant names it
    /// (`reserved:<module>`), or `None` when this process presents no consumer identity and so
    /// cannot read anything by grant.
    pub fn scoped_principal(&self) -> Option<String> {
        self.consumer_module_id
            .as_ref()
            .map(|module_id| format!("reserved:{module_id}"))
    }

    /// Refuse a credential-id request before it is sent when this process has no consumer
    /// identity. Sent anyway, it would arrive as `Direct` and come back `not_found`, which is
    /// indistinguishable from a missing grant or a missing credential.
    fn require_consumer_identity(&self, credential_id: &str) -> Result<(), VaultError> {
        match self.consumer_module_id {
            Some(_) => Ok(()),
            None => Err(VaultError::NoConsumerIdentity {
                credential_id: credential_id.to_string(),
            }),
        }
    }

    fn require_identity_for(&self, address: VaultAddress<'_>) -> Result<(), VaultError> {
        match address {
            VaultAddress::Handle(_) => Ok(()),
            VaultAddress::CredentialId(credential_id) => {
                self.require_consumer_identity(credential_id)
            }
        }
    }

    /// Wait for catalog readiness, then open one route. ONE budget, the configured readiness
    /// timeout, covers both phases: the route open retries a temporary refusal (a restarting
    /// credentials module answers `target_unavailable` for tens of seconds) only for whatever
    /// the catalog wait left over, not for a fresh timeout of its own. The budget decides when
    /// to stop STARTING opens; an open in flight when it runs out may still finish, within
    /// [`ROUTE_OPEN_ATTEMPT_TIMEOUT`].
    async fn resolve_route(&self) -> Result<RouteHandle, VaultError> {
        let deadline = tokio::time::Instant::now() + self.ready_timeout;
        let catalog_live = async {
            loop {
                if self
                    .conn
                    .catalog_contains(&self.module_id)
                    .await
                    .unwrap_or(false)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        };
        tokio::time::timeout_at(deadline, catalog_live)
            .await
            .map_err(|_| {
                VaultError::Transport(format!(
                    "credentials module '{}' did not become catalog-live within {:?}",
                    self.module_id, self.ready_timeout
                ))
            })?;
        let retry = RouteOpenRetry {
            deadline,
            max_interval: VAULT_ROUTE_OPEN_MAX_INTERVAL,
        };
        let open = self
            .conn
            .route_open_management(&self.module_id, &self.identity, retry);
        // The readiness deadline bounds STARTING route-open attempts, not finishing one. An
        // open already in flight when it passes is left to its own per-attempt reply deadline,
        // ROUTE_OPEN_ATTEMPT_TIMEOUT: the daemon may still be relaying its bind to the
        // credentials module, and cutting it off would report a successful open as a failure
        // (the route would then be committed and closed again unused).
        //
        // So this cap is only a backstop for a connection that does not enforce that deadline
        // itself. It sits one attempt timeout, plus a little slack, past the readiness deadline,
        // so on a real connection the attempt's own named timeout always arrives first.
        let backstop = deadline + ROUTE_OPEN_ATTEMPT_TIMEOUT + VAULT_ROUTE_OPEN_REPLY_GRACE;
        tokio::time::timeout_at(backstop, open)
            .await
            .map_err(|_| {
                VaultError::Transport(format!(
                    "route.open to credentials module '{}' did not complete within the {:?} \
                     readiness budget plus one ROUTE_OPEN_ATTEMPT_TIMEOUT ({:?})",
                    self.module_id, self.ready_timeout, ROUTE_OPEN_ATTEMPT_TIMEOUT
                ))
            })?
            .map_err(map_conn_err)
    }

    /// Return the cached route or share one in-flight open with every concurrent
    /// caller. Sharing the result matters on outages as much as on success: all
    /// waiters observe the same failed attempt instead of opening a route herd.
    async fn route(&self) -> Result<RouteHandle, VaultError> {
        let (attempt, opener) = {
            let mut state = self.route.lock().unwrap();
            match &*state {
                RouteState::Ready(channel) => return Ok(*channel),
                RouteState::Opening(attempt) => (Arc::clone(attempt), false),
                RouteState::Empty => {
                    let attempt = Arc::new(RouteOpenAttempt::new());
                    *state = RouteState::Opening(Arc::clone(&attempt));
                    (attempt, true)
                }
            }
        };

        if !opener {
            return attempt.wait().await;
        }

        let result = self.resolve_route().await;
        {
            let mut state = self.route.lock().unwrap();
            if matches!(&*state, RouteState::Opening(current) if Arc::ptr_eq(current, &attempt)) {
                *state = match &result {
                    Ok(channel) => RouteState::Ready(*channel),
                    Err(_) => RouteState::Empty,
                };
            }
        }
        attempt.complete(result.clone());
        result
    }

    /// Forget `failed_channel` only if it is still the cached route. A concurrent request may
    /// already have installed a replacement, which must not be evicted by a late stale failure.
    fn invalidate_route(&self, failed_route: RouteHandle) -> Option<RouteHandle> {
        let mut state = self.route.lock().unwrap();
        if matches!(*state, RouteState::Ready(route) if route == failed_route) {
            *state = RouteState::Empty;
            Some(failed_route)
        } else {
            None
        }
    }

    fn close_route_best_effort(&self, handle: RouteHandle) {
        let conn = Arc::clone(&self.conn);
        tokio::spawn(async move {
            let _ = conn.route_close(&handle).await;
        });
    }

    fn invalidate_failed_route(&self, route: RouteHandle, error: &ConnError) {
        if let Some(handle) = self.invalidate_route(route) {
            if !route_failure_is_known_dead(error) {
                self.close_route_best_effort(handle);
            }
        }
    }

    /// Send one request on the resolved route, retrying once only when the route
    /// is known stale (`unknown_channel`) or the connection itself failed. Other
    /// daemon rejections and protocol errors are authoritative and are not retried.
    ///
    /// `Ok` carries the vault's undecoded reply body: it means the vault answered, whatever the
    /// body turns out to say.
    async fn route_request_with_retry(&self, body: Vec<u8>) -> Result<Vec<u8>, VaultError> {
        let route = self.route().await?;
        match self.conn.route_request(&route, body.clone()).await {
            Ok(reply) => Ok(reply),
            Err(first) if conn_error_is_retryable(&first) => {
                self.invalidate_failed_route(route, &first);
                let retry_route = self.route().await?;
                match self.conn.route_request(&retry_route, body).await {
                    Ok(reply) => Ok(reply),
                    Err(second) => {
                        if conn_error_is_retryable(&second) {
                            self.invalidate_failed_route(retry_route, &second);
                        }
                        Err(map_exhausted_conn_err(second))
                    }
                }
            }
            Err(error) => Err(map_conn_err(error)),
        }
    }

    /// Resolve a credential by handle. `min_ttl_ms` demands a token valid for at least that
    /// long (pass the run's expected wall-clock budget so a mid-run expiry can't strand the
    /// run); the vault refreshes if the current token would expire sooner. `force_refresh`
    /// bypasses the freshness check (forces a refresh from the refresh token).
    ///
    /// The returned [`VaultGet`] also says whether the vault's reply to this request was
    /// received, which the error variant alone cannot tell.
    pub async fn get(
        &self,
        handle: &str,
        min_ttl_ms: Option<i64>,
        force_refresh: bool,
    ) -> VaultGet {
        let req = GetRequest {
            method: "credential.get",
            params: GetParams {
                handle,
                min_ttl_ms,
                force_refresh: force_refresh.then_some(true),
            },
        };
        let body = match serde_json::to_vec(&req) {
            Ok(body) => body,
            Err(e) => {
                return VaultGet::no_reply(VaultError::Protocol(format!(
                    "encode credential.get: {e}"
                )))
            }
        };
        match self.route_request_with_retry(body).await {
            Ok(reply) => VaultGet::replied(
                decode_reply_json(&reply).and_then(|reply| decode_get_reply(&reply)),
            ),
            Err(error) => VaultGet::no_reply(error),
        }
    }

    /// Resolve a credential by credential id under this process's read grant
    /// (`credential.get_scoped`), with no handle. `min_ttl_ms` means what it means on
    /// [`Self::get`]; there is no `force_refresh`, because the operation has none.
    ///
    /// Refused locally, with nothing sent and `vault_replied` false, when this process has no
    /// consumer identity ([`VaultError::NoConsumerIdentity`]). A `NotFound` reply means either
    /// that no credential has this id or that this principal holds no read grant on it: the
    /// vault answers both the same way on purpose, so a caller cannot probe which ids exist.
    pub async fn get_scoped(&self, credential_id: &str, min_ttl_ms: Option<i64>) -> VaultGet {
        if let Err(error) = self.require_consumer_identity(credential_id) {
            return VaultGet::no_reply(error);
        }
        let req = GetScopedRequest {
            method: "credential.get_scoped",
            params: GetScopedParams {
                credential_id,
                min_ttl_ms,
            },
        };
        let body = match serde_json::to_vec(&req) {
            Ok(body) => body,
            Err(e) => {
                return VaultGet::no_reply(VaultError::Protocol(format!(
                    "encode credential.get_scoped: {e}"
                )))
            }
        };
        // The reply has the same shape as credential.get's; the vault serves both through one
        // result type, so the same strict decoder applies.
        match self.route_request_with_retry(body).await {
            Ok(reply) => VaultGet::replied(
                decode_reply_json(&reply).and_then(|reply| decode_get_reply(&reply)),
            ),
            Err(error) => VaultGet::no_reply(error),
        }
    }

    /// List every credential this process's read grants cover (`credential.list_scoped`),
    /// with each row's state, including rows that need re-authentication. Decrypts nothing and
    /// mints nothing.
    ///
    /// Refused locally, with nothing sent, when this process has no consumer identity: the
    /// vault would see an anonymous caller that no grant covers. The vault answers an
    /// uncovered principal `not_found` rather than an empty list, so an empty `credentials`
    /// here really means the grants cover no rows.
    pub async fn list_scoped(&self) -> Result<ScopedList, VaultError> {
        self.require_consumer_identity("(every credential: credential.list_scoped)")?;
        let body = serde_json::to_vec(&serde_json::json!({
            "method": "credential.list_scoped",
            "params": {},
        }))
        .map_err(|e| VaultError::Protocol(format!("encode credential.list_scoped: {e}")))?;
        let reply = self.route_request_with_retry(body).await?;
        decode_list_scoped_reply(&decode_reply_json(&reply)?)
    }

    /// Read a handle's metadata without decrypting or refreshing its credential.
    /// Uses the same route deadlines and single stale-route retry as `get`; never
    /// reports authentication failures. Gauge policy belongs to the calling layer,
    /// not this transport client, which owns no health gauge.
    pub async fn status(&self, handle: &str) -> Result<CredentialStatus, VaultError> {
        self.status_at(VaultAddress::Handle(handle)).await
    }

    /// As [`Self::status`], for either address form. A credential-id address is refused
    /// locally when this process has no consumer identity, exactly as on [`Self::get_scoped`].
    pub async fn status_at(
        &self,
        address: VaultAddress<'_>,
    ) -> Result<CredentialStatus, VaultError> {
        self.require_identity_for(address)?;
        let req = StatusRequest {
            method: "credential.status",
            params: StatusParams::at(address),
        };
        let body = serde_json::to_vec(&req)
            .map_err(|e| VaultError::Protocol(format!("encode credential.status: {e}")))?;
        let reply = self.route_request_with_retry(body).await?;
        decode_status_reply(&decode_reply_json(&reply)?)
    }

    /// Report a wire-auth failure for a handle so the vault can invalidate a dead credential
    /// (marks it `needs_reauth`) instead of serving it again every run. Only a 401/403
    /// actually invalidates vault-side; other statuses are accepted-but-noop (a 5xx/429 is a
    /// provider hiccup, not a dead credential — so the caller should report ONLY on a genuine
    /// auth failure, not all errors). FIRE-AND-FORGET: the success reply is unit `{}`; this
    /// returns once the report is sent. The vault rate-limits reports on a handle, so a
    /// caller must NOT retry-storm it.
    /// NEVER SENT IN PRODUCTION. Zero invocations across the whole daemon log, against 1668
    /// `credential.get` round-trips over the same route in the same window -- so the sibling
    /// surface is proven continuously and this one is proven by nothing. It fires only when a
    /// run reaches a terminal carrying a wire 401/403, and no production run has.
    ///
    /// That makes its correctness an opinion rather than a fact: the request shape here is
    /// hand-written against a reading of claustrum's contract, and no receiver has ever
    /// accepted or refused these bytes. A field-name drift on either side would be invisible
    /// until the first genuine dead credential, which is exactly the moment the report matters.
    /// ASTRO found precisely this on their side -- the one surface no receiver had accepted was
    /// the one carrying a wrong field name for weeks, which is the same fact twice rather than
    /// a coincidence.
    ///
    /// So do not read a green test near this as verification: a local test proves the encoder
    /// matches this struct, and this struct is the thing in doubt. Only real bytes crossing to
    /// claustrum settle it.
    pub async fn report_auth_failure(
        &self,
        handle: &str,
        provider_status: u16,
        record_version: u64,
    ) -> Result<(), VaultError> {
        self.report_auth_failure_at(
            VaultAddress::Handle(handle),
            provider_status,
            record_version,
        )
        .await
    }

    /// As [`Self::report_auth_failure`], for either address form. The `record_version` CAS is
    /// the same on both. A credential-id address is refused locally when this process has no
    /// consumer identity, exactly as on [`Self::get_scoped`].
    pub async fn report_auth_failure_at(
        &self,
        address: VaultAddress<'_>,
        provider_status: u16,
        record_version: u64,
    ) -> Result<(), VaultError> {
        self.require_identity_for(address)?;
        let (handle, credential_id) = match address {
            VaultAddress::Handle(handle) => (Some(handle), None),
            VaultAddress::CredentialId(credential_id) => (None, Some(credential_id)),
        };
        let req = ReportRequest {
            method: "credential.report_auth_failure",
            params: ReportParams {
                handle,
                credential_id,
                provider_status,
                reporter_source: REPORTER_SOURCE_DIRECT,
                record_version,
            },
        };
        let body = serde_json::to_vec(&req)
            .map_err(|e| VaultError::Protocol(format!("encode report_auth_failure: {e}")))?;
        // The reply is `{"result": {}}` on success or an app-error shape; we only need to know
        // it was accepted, so a decode failure is a protocol error but the unit body is ignored.
        let reply = self.route_request_with_retry(body).await?;
        decode_reply_json(&reply)?;
        Ok(())
    }
}

/// Parse a vault reply body as JSON. A body that is not JSON is contract drift, a `Protocol`
/// error, exactly as when the connection layer did this parse.
fn decode_reply_json(body: &[u8]) -> Result<serde_json::Value, VaultError> {
    serde_json::from_slice(body)
        .map_err(|e| VaultError::Protocol(format!("decode route response: {e}")))
}

fn conn_error_is_retryable(error: &ConnError) -> bool {
    matches!(
        error,
        ConnError::Transport(_) | ConnError::StaleRouteHandle(_)
    ) || matches!(error, ConnError::Rejected(message) if crate::rejection_is_unknown_channel(message))
}

/// A second `unknown_channel` means the replacement route also disappeared during a
/// module restart. Surface that exhausted recovery as transport/transient, not as a
/// permanent protocol fault, so admission can retry after the module stabilizes.
fn map_exhausted_conn_err(error: ConnError) -> VaultError {
    match error {
        ConnError::Rejected(message) if crate::rejection_is_unknown_channel(&message) => {
            VaultError::Transport(format!("vault route remained unavailable: {message}"))
        }
        other => map_conn_err(other),
    }
}

/// Map a consumer-connection error to a vault error. An `Error` FRAME (subc `Rejected`) is a
/// PROTOCOL error unless the retry path has identified an exhausted `unknown_channel` restart
/// window. Other Error frames indicate a malformed request or another authoritative rejection.
fn map_conn_err(e: ConnError) -> VaultError {
    match e {
        ConnError::Rejected(m) => {
            VaultError::Protocol(format!("daemon rejected the vault request: {m}"))
        }
        ConnError::Transport(m) => VaultError::Transport(m),
        ConnError::Protocol(m) => VaultError::Protocol(m),
        error @ ConnError::StreamLagged { .. } => VaultError::Transport(error.to_string()),
        ConnError::StaleRouteHandle(handle) => VaultError::Transport(format!(
            "vault route handle became stale (channel {}, epoch {})",
            handle.channel, handle.epoch
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        path::PathBuf,
        sync::atomic::{AtomicU16, AtomicUsize, Ordering},
    };

    use super::*;
    use serde_json::json;
    use subc_control::{CatalogEntry, ClientControlRequest, ClientControlResponse};
    use subc_protocol::{Flags, Frame, FrameType, Priority};
    use subc_transport::{
        authenticate_server,
        connection_file::{self, ConnectionInfo, Endpoint, SCHEMA_VERSION},
        read_frame, write_frame,
    };
    use tokio::{
        net::{TcpListener, TcpStream},
        sync::{Barrier, Notify},
        task::{JoinHandle, JoinSet},
    };

    struct FailingOpenConnection {
        attempts: AtomicUsize,
        started: Notify,
        release: Notify,
    }

    #[async_trait]
    impl VaultConnection for FailingOpenConnection {
        async fn catalog_contains(&self, _module_id: &str) -> Result<bool, ConnError> {
            Ok(true)
        }

        async fn route_open_management(
            &self,
            _module_id: &str,
            _identity: &broca_protocol::BindIdentity,
            _retry: RouteOpenRetry,
        ) -> Result<RouteHandle, ConnError> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            self.release.notified().await;
            Err(ConnError::Transport("vault offline".into()))
        }

        async fn route_request(
            &self,
            _route: &RouteHandle,
            _body: Vec<u8>,
        ) -> Result<Vec<u8>, ConnError> {
            unreachable!("open failure test does not send a request")
        }

        async fn route_close(&self, _route: &RouteHandle) -> Result<(), ConnError> {
            unreachable!("open failure test does not close a route")
        }

        fn consumer_module_id(&self) -> Option<String> {
            None
        }
    }

    struct ScriptedConnection {
        next_channel: AtomicU16,
        opens: AtomicUsize,
        requests: AtomicUsize,
        catalog_live: bool,
        replies: std::sync::Mutex<VecDeque<Result<serde_json::Value, ConnError>>>,
        closes: std::sync::Mutex<Vec<RouteHandle>>,
        sent: std::sync::Mutex<Vec<(RouteHandle, Vec<u8>)>>,
    }

    #[async_trait]
    impl VaultConnection for ScriptedConnection {
        async fn catalog_contains(&self, _module_id: &str) -> Result<bool, ConnError> {
            Ok(self.catalog_live)
        }

        async fn route_open_management(
            &self,
            _module_id: &str,
            _identity: &broca_protocol::BindIdentity,
            _retry: RouteOpenRetry,
        ) -> Result<RouteHandle, ConnError> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            Ok(RouteHandle::synthetic(
                self.next_channel.fetch_add(1, Ordering::SeqCst),
            ))
        }

        async fn route_request(
            &self,
            route: &RouteHandle,
            body: Vec<u8>,
        ) -> Result<Vec<u8>, ConnError> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            self.sent.lock().unwrap().push((*route, body));
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .unwrap()
                .map(|reply| serde_json::to_vec(&reply).unwrap())
        }

        async fn route_close(&self, route: &RouteHandle) -> Result<(), ConnError> {
            self.closes.lock().unwrap().push(*route);
            Ok(())
        }

        fn consumer_module_id(&self) -> Option<String> {
            None
        }
    }

    /// A credentials route whose one open answers after `answers_after`, or never when `None`.
    /// It stands in for the daemon still relaying the bind when the readiness budget runs out.
    struct SlowOpenConnection {
        answers_after: Option<std::time::Duration>,
        opens: AtomicUsize,
    }

    #[async_trait]
    impl VaultConnection for SlowOpenConnection {
        async fn catalog_contains(&self, _module_id: &str) -> Result<bool, ConnError> {
            Ok(true)
        }

        async fn route_open_management(
            &self,
            _module_id: &str,
            _identity: &broca_protocol::BindIdentity,
            _retry: RouteOpenRetry,
        ) -> Result<RouteHandle, ConnError> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            match self.answers_after {
                Some(delay) => {
                    tokio::time::sleep(delay).await;
                    Ok(RouteHandle::synthetic(33))
                }
                None => std::future::pending().await,
            }
        }

        async fn route_request(
            &self,
            _route: &RouteHandle,
            _body: Vec<u8>,
        ) -> Result<Vec<u8>, ConnError> {
            unreachable!("route-open tests do not send a request")
        }

        async fn route_close(&self, _route: &RouteHandle) -> Result<(), ConnError> {
            unreachable!("route-open tests do not close a route")
        }

        fn consumer_module_id(&self) -> Option<String> {
            None
        }
    }

    /// AN OPEN IN FLIGHT WHEN THE READINESS BUDGET RUNS OUT IS ALLOWED TO FINISH. The budget
    /// (1s here) decides when to stop starting opens; this one started at once and the daemon
    /// answers it at 11s, inside its 12s bind relay. It used to be abandoned half a second past
    /// the budget, reporting a successful open as a failure. Paused time: the seconds are
    /// virtual.
    #[tokio::test(start_paused = true)]
    async fn an_open_in_flight_at_the_readiness_budget_finishes_instead_of_being_abandoned() {
        let conn = Arc::new(SlowOpenConnection {
            answers_after: Some(std::time::Duration::from_secs(11)),
            opens: AtomicUsize::new(0),
        });
        let client = test_client(conn.clone(), RouteState::Empty);
        assert!(client.ready_timeout < std::time::Duration::from_secs(11));

        let route = client.route().await.unwrap_or_else(|error| {
            panic!("the in-flight open must be allowed to finish: {error}")
        });

        assert_eq!(route, RouteHandle::synthetic(33));
        assert_eq!(conn.opens.load(Ordering::SeqCst), 1);
        assert!(matches!(
            *client.route.lock().unwrap(),
            RouteState::Ready(_)
        ));
    }

    /// An open that never answers is still cut off, one attempt timeout past the readiness
    /// budget and no later, with an error that names both clocks. (A real connection ends the
    /// attempt itself at ROUTE_OPEN_ATTEMPT_TIMEOUT; this fake does not, so the client's own
    /// backstop is what fires.)
    #[tokio::test(start_paused = true)]
    async fn an_open_that_never_answers_is_cut_off_one_attempt_timeout_past_the_budget() {
        let conn = Arc::new(SlowOpenConnection {
            answers_after: None,
            opens: AtomicUsize::new(0),
        });
        let client = test_client(conn.clone(), RouteState::Empty);
        let bound =
            client.ready_timeout + ROUTE_OPEN_ATTEMPT_TIMEOUT + VAULT_ROUTE_OPEN_REPLY_GRACE;

        let started = tokio::time::Instant::now();
        let error = client.route().await.unwrap_err();
        let elapsed = started.elapsed();

        let message = match error {
            VaultError::Transport(message) => message,
            other => panic!("a silent open must fail as transport, got {other:?}"),
        };
        assert!(
            message.contains("ROUTE_OPEN_ATTEMPT_TIMEOUT"),
            "the error must name the route-open deadline: {message}"
        );
        assert!(
            elapsed >= bound && elapsed < bound + std::time::Duration::from_millis(100),
            "the open must be cut off at {bound:?}, took {elapsed:?}"
        );
        assert!(matches!(*client.route.lock().unwrap(), RouteState::Empty));
    }

    #[derive(Clone, Copy)]
    enum StubReply {
        Answer,
        WithholdAndIgnorePing,
        WithholdAndPong,
    }

    struct VaultTcpStub {
        connection_file: PathBuf,
        accepts: Arc<AtomicUsize>,
        route_opens: Arc<AtomicUsize>,
        requests: Arc<AtomicUsize>,
        pings: Arc<AtomicUsize>,
        errors: Arc<std::sync::Mutex<Vec<String>>>,
        server: JoinHandle<()>,
    }

    impl VaultTcpStub {
        async fn start(reply: StubReply) -> Self {
            static NEXT_STUB: AtomicUsize = AtomicUsize::new(1);

            let listener = TcpListener::bind(("127.0.0.1", 0))
                .await
                .expect("bind vault TCP stub");
            let port = listener.local_addr().expect("vault stub address").port();
            let key = vec![0x5a; 32];
            let daemon_id = [0xa5; 16];
            let connection_file = broca_test_support::scratch_root().join(format!(
                "broca-vault-timeout-stub-{}-{}.json",
                std::process::id(),
                NEXT_STUB.fetch_add(1, Ordering::Relaxed)
            ));
            connection_file::write_atomic(
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
                    daemon_ver: "vault-timeout-stub".into(),
                },
            )
            .expect("write vault stub connection file");

            let accepts = Arc::new(AtomicUsize::new(0));
            let route_opens = Arc::new(AtomicUsize::new(0));
            let requests = Arc::new(AtomicUsize::new(0));
            let pings = Arc::new(AtomicUsize::new(0));
            let errors = Arc::new(std::sync::Mutex::new(Vec::new()));
            let server = {
                let accepts = accepts.clone();
                let route_opens = route_opens.clone();
                let requests = requests.clone();
                let pings = pings.clone();
                let errors = errors.clone();
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
                        let route_opens = route_opens.clone();
                        let requests = requests.clone();
                        let pings = pings.clone();
                        let errors = errors.clone();
                        handlers.spawn(async move {
                            if let Err(error) = serve_vault_stub_connection(
                                stream,
                                key,
                                daemon_id,
                                reply,
                                route_opens,
                                requests,
                                pings,
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
                route_opens,
                requests,
                pings,
                errors,
                server,
            }
        }

        fn accept_count(&self) -> usize {
            self.accepts.load(Ordering::SeqCst)
        }

        fn route_open_count(&self) -> usize {
            self.route_opens.load(Ordering::SeqCst)
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
                "vault TCP stub failed: {:?}",
                self.errors.lock().unwrap().as_slice()
            );
        }
    }

    impl Drop for VaultTcpStub {
        fn drop(&mut self) {
            self.server.abort();
            let _ = std::fs::remove_file(&self.connection_file);
        }
    }

    async fn serve_vault_stub_connection(
        mut stream: TcpStream,
        key: Vec<u8>,
        daemon_id: [u8; 16],
        reply: StubReply,
        route_opens: Arc<AtomicUsize>,
        requests: Arc<AtomicUsize>,
        pings: Arc<AtomicUsize>,
    ) -> Result<(), String> {
        authenticate_server(
            &mut stream,
            &key,
            &daemon_id,
            "vault-timeout-stub",
            broca_test_support::STAND_IN_AUTH_DEADLINE,
        )
        .await
        .map_err(|error| format!("authenticate vault stub client: {error}"))?;

        let next_route = AtomicU16::new(10);
        while let Some(frame) = read_frame(&mut stream)
            .await
            .map_err(|error| format!("read vault stub frame: {error}"))?
        {
            match (frame.header.channel, frame.header.ty) {
                (0, FrameType::Ping) => {
                    pings.fetch_add(1, Ordering::SeqCst);
                    if matches!(reply, StubReply::Answer | StubReply::WithholdAndPong) {
                        let pong = Frame::build(
                            FrameType::Pong,
                            frame.header.flags,
                            0,
                            0,
                            frame.header.corr,
                            Vec::new(),
                        )
                        .map_err(|error| format!("build vault stub pong: {error}"))?;
                        write_frame(&mut stream, &pong)
                            .await
                            .map_err(|error| format!("write vault stub pong: {error}"))?;
                    }
                }
                (0, FrameType::Request) => {
                    let request = serde_json::from_slice::<ClientControlRequest>(&frame.body)
                        .map_err(|error| format!("decode vault control request: {error}"))?;
                    let response = match request {
                        ClientControlRequest::CatalogList { .. } => {
                            ClientControlResponse::CatalogList {
                                generation: 1,
                                // Control-op routing does not consult capabilities. This fixture
                                // represents a registered, ready vault without declaring them.
                                modules: vec![CatalogEntry::new(
                                    "credentials",
                                    Vec::new(),
                                    vec!["credential.get".into()],
                                )
                                .with_module_version(Some("stub".into()))
                                .with_ready(true)],
                                subc_ops: Vec::new(),
                            }
                        }
                        ClientControlRequest::RouteOpen { .. } => {
                            route_opens.fetch_add(1, Ordering::SeqCst);
                            ClientControlResponse::RouteOpen {
                                route_channel: next_route.fetch_add(1, Ordering::SeqCst),
                                route_epoch: 1,
                            }
                        }
                        other => {
                            return Err(format!("unexpected vault control request: {other:?}"))
                        }
                    };
                    write_stub_response(
                        &mut stream,
                        &frame,
                        Priority::Passive,
                        serde_json::to_vec(&response)
                            .map_err(|error| format!("encode vault control response: {error}"))?,
                    )
                    .await?;
                }
                (channel, FrameType::Request) if channel != 0 => {
                    requests.fetch_add(1, Ordering::SeqCst);
                    if matches!(reply, StubReply::Answer) {
                        write_stub_response(
                            &mut stream,
                            &frame,
                            Priority::Interactive,
                            serde_json::to_vec(&json!({
                                "result": {
                                    "payload": b"stub-token",
                                    "expires_at_ms": null,
                                    "record_version": 1
                                }
                            }))
                            .map_err(|error| format!("encode vault reply: {error}"))?,
                        )
                        .await?;
                    }
                }
                (_, FrameType::Goodbye) => {}
                (_, other) => return Err(format!("unexpected vault frame type: {other:?}")),
            }
        }
        Ok(())
    }

    async fn write_stub_response(
        stream: &mut TcpStream,
        request: &Frame,
        priority: Priority,
        body: Vec<u8>,
    ) -> Result<(), String> {
        let response = Frame::build(
            FrameType::Response,
            Flags::new(false, priority, false),
            request.header.channel,
            request.header.epoch,
            request.header.corr,
            body,
        )
        .map_err(|error| format!("build vault stub response: {error}"))?;
        write_frame(stream, &response)
            .await
            .map_err(|error| format!("write vault stub response: {error}"))
    }

    /// Delegates to a real `SubcConnection`, changing only the response-silence deadline so an
    /// intentionally silent peer does not add forty seconds to the unit suite.
    struct ShortDeadlineVaultConnection {
        conn: Arc<SubcConnection>,
    }

    #[async_trait]
    impl VaultConnection for ShortDeadlineVaultConnection {
        async fn catalog_contains(&self, module_id: &str) -> Result<bool, ConnError> {
            self.conn
                .catalog_list()
                .await
                .map(|modules| modules.iter().any(|module| module.module_id == module_id))
        }

        async fn route_open_management(
            &self,
            module_id: &str,
            identity: &broca_protocol::BindIdentity,
            retry: RouteOpenRetry,
        ) -> Result<RouteHandle, ConnError> {
            self.conn
                .route_open_management_within(module_id, identity, retry)
                .await
        }

        async fn route_request(
            &self,
            route: &RouteHandle,
            body: Vec<u8>,
        ) -> Result<Vec<u8>, ConnError> {
            self.conn
                .route_unary_bytes(route, body, std::time::Duration::from_millis(50))
                .await
        }

        async fn route_close(&self, route: &RouteHandle) -> Result<(), ConnError> {
            self.conn.route_close(route).await
        }

        fn consumer_module_id(&self) -> Option<String> {
            self.conn.consumer_module_id().map(str::to_string)
        }
    }

    async fn tcp_test_client(stub: &VaultTcpStub) -> VaultClient {
        let conn = SubcConnection::connect_with_probe_timeout(
            &stub.connection_file,
            std::time::Duration::from_millis(75),
        )
        .await
        .expect("connect to vault TCP stub");
        test_client(
            Arc::new(ShortDeadlineVaultConnection {
                conn: Arc::new(conn),
            }),
            RouteState::Empty,
        )
    }

    async fn wait_for_counter(counter: &AtomicUsize, expected: usize, label: &str) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while counter.load(Ordering::SeqCst) < expected {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{label} did not reach {expected}"));
    }

    fn test_client(conn: Arc<dyn VaultConnection>, route: RouteState) -> VaultClient {
        VaultClient {
            conn,
            module_id: "credentials".into(),
            identity: broca_protocol::BindIdentity::new("/tmp/project", "runner", "vault"),
            ready_timeout: std::time::Duration::from_secs(1),
            route: std::sync::Mutex::new(route),
            consumer_module_id: None,
        }
    }

    async fn wait_for_route_closes(conn: &ScriptedConnection, count: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if conn.closes.lock().unwrap().len() == count {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("best-effort route close should run promptly");
    }

    #[tokio::test]
    async fn answering_tcp_vault_stub_completes_get_control() {
        let stub = VaultTcpStub::start(StubReply::Answer).await;
        let client = tcp_test_client(&stub).await;

        let token = match client.get("test-handle", None, false).await.result {
            Ok(token) => token,
            Err(error) => {
                stub.assert_healthy();
                panic!("answering stub must complete credential.get: {error}");
            }
        };

        assert_eq!(token.token, "stub-token");
        assert_eq!(stub.accept_count(), 1);
        assert_eq!(stub.route_open_count(), 1);
        assert_eq!(stub.request_count(), 1);
        stub.assert_healthy();
    }

    #[tokio::test]
    async fn timed_out_vault_call_replaces_silent_tcp_connection() {
        let stub = VaultTcpStub::start(StubReply::WithholdAndIgnorePing).await;
        let client = tcp_test_client(&stub).await;
        assert_eq!(stub.accept_count(), 1, "the test must begin on one socket");

        let _error = client
            .get("test-handle", None, false)
            .await
            .result
            .expect_err("the stub intentionally withholds the vault response");
        wait_for_counter(&stub.accepts, 2, "TCP accept count").await;

        assert_eq!(
            stub.accept_count(),
            2,
            "a silent Ping window must replace the convicted TCP generation"
        );
        assert!(
            stub.request_count() >= 1,
            "the request must reach the first socket"
        );
        assert!(
            stub.ping_count() >= 1,
            "the timeout must send a liveness Ping"
        );
        stub.assert_healthy();
    }

    #[tokio::test]
    async fn slow_live_vault_handler_is_not_convicted() {
        let stub = VaultTcpStub::start(StubReply::WithholdAndPong).await;
        let client = tcp_test_client(&stub).await;
        assert_eq!(stub.accept_count(), 1, "the test must begin on one socket");

        let _error = client
            .get("test-handle", None, false)
            .await
            .result
            .expect_err("the slow handler intentionally remains silent");
        wait_for_counter(&stub.pings, 1, "Ping count").await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        assert_eq!(
            stub.accept_count(),
            1,
            "a Pong must preserve a socket even when the routed handler stays silent"
        );
        assert_eq!(
            stub.request_count(),
            2,
            "the vault retries its idempotent read once"
        );
        stub.assert_healthy();
    }

    #[tokio::test]
    async fn concurrent_failed_route_open_is_single_flight() {
        let conn = Arc::new(FailingOpenConnection {
            attempts: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
        });
        let client = Arc::new(test_client(conn.clone(), RouteState::Empty));
        let start = Arc::new(Barrier::new(3));
        let left = {
            let client = client.clone();
            let start = start.clone();
            tokio::spawn(async move {
                start.wait().await;
                client.route().await
            })
        };
        let right = {
            let client = client.clone();
            let start = start.clone();
            tokio::spawn(async move {
                start.wait().await;
                client.route().await
            })
        };
        start.wait().await;
        conn.started.notified().await;
        tokio::task::yield_now().await;
        conn.release.notify_waiters();

        assert!(matches!(left.await.unwrap(), Err(VaultError::Transport(_))));
        assert!(matches!(
            right.await.unwrap(),
            Err(VaultError::Transport(_))
        ));
        assert_eq!(conn.attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn first_route_resolve_is_bounded_while_catalog_is_absent() {
        let conn = Arc::new(ScriptedConnection {
            next_channel: AtomicU16::new(40),
            opens: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            catalog_live: false,
            replies: std::sync::Mutex::new(VecDeque::new()),
            closes: std::sync::Mutex::new(Vec::new()),
            sent: std::sync::Mutex::new(Vec::new()),
        });
        let mut client = test_client(conn.clone(), RouteState::Empty);
        client.ready_timeout = std::time::Duration::from_millis(20);

        let started = std::time::Instant::now();
        let error = client.route().await.unwrap_err();
        assert!(matches!(error, VaultError::Transport(_)));
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        assert_eq!(conn.opens.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn non_unknown_rejection_is_not_retried() {
        let conn = Arc::new(ScriptedConnection {
            next_channel: AtomicU16::new(40),
            opens: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            catalog_live: true,
            replies: std::sync::Mutex::new(VecDeque::from([Err(ConnError::Rejected(
                "permission_denied".into(),
            ))])),
            closes: std::sync::Mutex::new(Vec::new()),
            sent: std::sync::Mutex::new(Vec::new()),
        });
        let client = test_client(conn.clone(), RouteState::Ready(RouteHandle::synthetic(10)));
        let error = client.route_request_with_retry(vec![]).await.unwrap_err();
        assert!(matches!(error, VaultError::Protocol(_)));
        assert_eq!(conn.requests.load(Ordering::SeqCst), 1);
        assert_eq!(conn.opens.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn repeated_unknown_channel_is_transient_and_evicts_replacement() {
        let conn = Arc::new(ScriptedConnection {
            next_channel: AtomicU16::new(40),
            opens: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            catalog_live: true,
            replies: std::sync::Mutex::new(VecDeque::from([
                Err(ConnError::Rejected("unknown_channel — 10".into())),
                Err(ConnError::Rejected("unknown_channel — 40".into())),
            ])),
            closes: std::sync::Mutex::new(Vec::new()),
            sent: std::sync::Mutex::new(Vec::new()),
        });
        let client = test_client(conn.clone(), RouteState::Ready(RouteHandle::synthetic(10)));
        let error = client.route_request_with_retry(vec![]).await.unwrap_err();
        assert!(matches!(error, VaultError::Transport(_)));
        assert_eq!(conn.requests.load(Ordering::SeqCst), 2);
        assert_eq!(conn.opens.load(Ordering::SeqCst), 1);
        assert!(matches!(*client.route.lock().unwrap(), RouteState::Empty));
        assert!(
            conn.closes.lock().unwrap().is_empty(),
            "unknown_channel proves both daemon routes are already gone"
        );
    }

    #[tokio::test]
    async fn timeout_evicts_and_closes_the_possibly_live_vault_route() {
        let conn = Arc::new(ScriptedConnection {
            next_channel: AtomicU16::new(40),
            opens: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            catalog_live: true,
            replies: std::sync::Mutex::new(VecDeque::from([
                Err(ConnError::Transport("timed out after 10s".into())),
                Ok(json!({"result": {"error": {"code": "not_found"}}})),
            ])),
            closes: std::sync::Mutex::new(Vec::new()),
            sent: std::sync::Mutex::new(Vec::new()),
        });
        let stale = RouteHandle::synthetic(10);
        let client = test_client(conn.clone(), RouteState::Ready(stale));

        let _ = client.route_request_with_retry(vec![]).await.unwrap();
        wait_for_route_closes(&conn, 1).await;

        assert_eq!(conn.closes.lock().unwrap().as_slice(), &[stale]);
    }

    #[test]
    fn status_absent_resolution_fields_remain_none() {
        let status = decode_status_reply(&json!({"result": {
            "ready": false, "lease_held": false, "last_error_code": "not_found"
        }}))
        .unwrap();
        assert_eq!(status.credential_id, None);
        assert_eq!(status.record_version, None);
        assert_eq!(status.stale_pending, None);
        assert_eq!(
            serde_json::to_value(status).unwrap(),
            json!({
                "ready": false, "lease_held": false, "last_error_code": "not_found"
            })
        );
    }

    #[test]
    fn status_explicit_null_is_success_and_serializes_null() {
        let status = decode_status_reply(&json!({"result": {
            "ready": true, "lease_held": false, "last_error_code": null
        }}))
        .unwrap();
        assert_eq!(status.last_error_code, None);
        let encoded = serde_json::to_value(status).unwrap();
        assert!(encoded.as_object().unwrap().contains_key("last_error_code"));
        assert_eq!(encoded["last_error_code"], json!(null));
    }

    #[test]
    fn status_full_reply_decodes_exactly() {
        let status = decode_status_reply(&json!({"result": {
            "ready": false, "lease_held": true, "last_error_code": "needs_reauth",
            "credential_id": "oauth:example", "record_version": 18446744073709551615u64,
            "stale_pending": true
        }}))
        .unwrap();
        assert_eq!(
            status,
            CredentialStatus {
                ready: false,
                lease_held: true,
                last_error_code: Some("needs_reauth".into()),
                credential_id: Some("oauth:example".into()),
                record_version: Some(u64::MAX),
                stale_pending: Some(true),
            }
        );
    }

    #[test]
    fn status_required_fields_and_types_are_checked() {
        for result in [
            json!({"ready": true, "lease_held": false}),
            json!({"lease_held": false, "last_error_code": null}),
            json!({"ready": true, "last_error_code": null}),
            json!({"ready": true, "lease_held": false, "last_error_code": 4}),
            json!({"ready": true, "lease_held": false, "last_error_code": null, "record_version": -1}),
        ] {
            assert!(matches!(
                decode_status_reply(&json!({"result": result})),
                Err(VaultError::Protocol(_))
            ));
        }
    }

    fn status_connection(
        replies: Vec<Result<serde_json::Value, ConnError>>,
    ) -> Arc<ScriptedConnection> {
        Arc::new(ScriptedConnection {
            next_channel: AtomicU16::new(40),
            opens: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            catalog_live: true,
            replies: std::sync::Mutex::new(replies.into()),
            closes: std::sync::Mutex::new(Vec::new()),
            sent: std::sync::Mutex::new(Vec::new()),
        })
    }

    fn status_reply() -> serde_json::Value {
        json!({"result": {"ready": true, "lease_held": false, "last_error_code": null}})
    }

    #[tokio::test]
    async fn status_request_bytes_are_handle_only() {
        let conn = status_connection(vec![Ok(status_reply())]);
        let client = test_client(conn.clone(), RouteState::Empty);
        client.status("ckh_probe").await.unwrap();
        let sent = conn.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        let bytes: serde_json::Value = serde_json::from_slice(&sent[0].1).unwrap();
        assert_eq!(
            bytes,
            json!({"method": "credential.status", "params": {"handle": "ckh_probe"}})
        );
        assert!(!bytes["params"]
            .as_object()
            .unwrap()
            .contains_key("credential_id"));
    }

    /// A client whose connection presents the consumer identity of module `broca`.
    fn supervised_client(conn: Arc<ScriptedConnection>) -> VaultClient {
        VaultClient {
            consumer_module_id: Some("broca".into()),
            ..test_client(conn, RouteState::Empty)
        }
    }

    fn sent_bodies(conn: &ScriptedConnection) -> Vec<serde_json::Value> {
        conn.sent
            .lock()
            .unwrap()
            .iter()
            .map(|(_, body)| serde_json::from_slice(body).unwrap())
            .collect()
    }

    /// A scoped read sends the credential id and the ttl demand and nothing else: no handle,
    /// and no `force_refresh`, which the operation does not have.
    #[tokio::test]
    async fn get_scoped_sends_only_the_credential_id_and_the_ttl_demand() {
        let conn = status_connection(vec![Ok(json!({"result": {
            "payload": b"scoped-token",
            "expires_at_ms": null,
            "record_version": 4,
            "credential_id": "apikey:synthetic:main"
        }}))]);
        let client = supervised_client(conn.clone());
        assert_eq!(client.scoped_principal().as_deref(), Some("reserved:broca"));

        let got = client
            .get_scoped("apikey:synthetic:main", Some(600_000))
            .await;

        assert!(got.vault_replied);
        let token = got.result.expect("the scoped reply decodes as a get reply");
        assert_eq!(token.token, "scoped-token");
        assert_eq!(token.record_version, 4);
        assert_eq!(
            sent_bodies(&conn),
            vec![json!({"method": "credential.get_scoped", "params": {
                "credential_id": "apikey:synthetic:main",
                "min_ttl_ms": 600_000
            }})]
        );
    }

    /// Without a consumer identity the vault would see an anonymous caller and answer
    /// `not_found`. Every credential-id request is refused before anything is sent: no route
    /// is opened and no request leaves, so `vault_replied` is false.
    #[tokio::test]
    async fn credential_id_requests_without_a_consumer_identity_are_refused_before_sending() {
        let conn = status_connection(Vec::new());
        let client = test_client(conn.clone(), RouteState::Empty);
        assert_eq!(client.scoped_principal(), None);

        let got = client.get_scoped("apikey:synthetic", Some(600_000)).await;
        assert!(!got.vault_replied);
        let refusal = got.result.expect_err("no identity, no scoped read");
        assert_eq!(
            refusal,
            VaultError::NoConsumerIdentity {
                credential_id: "apikey:synthetic".into()
            }
        );
        let message = refusal.to_string();
        assert!(
            message.contains("no supervised consumer identity"),
            "{message}"
        );
        assert!(message.contains("supervised broca module"), "{message}");
        assert!(message.contains("apikey:synthetic"), "{message}");

        assert!(matches!(
            client
                .status_at(VaultAddress::CredentialId("apikey:synthetic"))
                .await,
            Err(VaultError::NoConsumerIdentity { .. })
        ));
        assert!(matches!(
            client
                .report_auth_failure_at(VaultAddress::CredentialId("apikey:synthetic"), 401, 3)
                .await,
            Err(VaultError::NoConsumerIdentity { .. })
        ));

        assert_eq!(conn.opens.load(Ordering::SeqCst), 0, "no route was opened");
        assert_eq!(conn.requests.load(Ordering::SeqCst), 0, "nothing was sent");
    }

    /// status and report_auth_failure address a credential by id, never by handle, on the
    /// scoped path; the report keeps its record_version CAS.
    #[tokio::test]
    async fn status_and_report_address_by_credential_id_on_the_scoped_path() {
        let conn = status_connection(vec![Ok(status_reply()), Ok(json!({"result": {}}))]);
        let client = supervised_client(conn.clone());

        client
            .status_at(VaultAddress::CredentialId("apikey:synthetic:main"))
            .await
            .unwrap();
        client
            .report_auth_failure_at(VaultAddress::CredentialId("apikey:synthetic:main"), 401, 9)
            .await
            .unwrap();

        assert_eq!(
            sent_bodies(&conn),
            vec![
                json!({"method": "credential.status", "params": {
                    "credential_id": "apikey:synthetic:main"
                }}),
                json!({"method": "credential.report_auth_failure", "params": {
                    "credential_id": "apikey:synthetic:main",
                    "provider_status": 401,
                    "reporter_source": "direct",
                    "record_version": 9
                }}),
            ]
        );
    }

    #[tokio::test]
    async fn status_evicts_route_and_retries_once() {
        for (failure, known_dead) in [
            (ConnError::Rejected("unknown_channel — 10".into()), true),
            (ConnError::Transport("offline".into()), false),
        ] {
            let conn = status_connection(vec![Err(failure), Ok(status_reply())]);
            let old = RouteHandle::synthetic(10);
            let client = test_client(conn.clone(), RouteState::Ready(old));
            assert!(client.status("ckh_probe").await.unwrap().ready);
            assert_eq!(conn.opens.load(Ordering::SeqCst), 1);
            {
                let sent = conn.sent.lock().unwrap();
                assert_eq!(sent.len(), 2);
                assert_eq!(sent[0].0, old);
                assert_eq!(sent[1].0, RouteHandle::synthetic(40));
                assert_eq!(sent[0].1, sent[1].1);
            }
            assert!(
                matches!(*client.route.lock().unwrap(), RouteState::Ready(route) if route == RouteHandle::synthetic(40))
            );
            if known_dead {
                assert!(conn.closes.lock().unwrap().is_empty());
            } else {
                wait_for_route_closes(&conn, 1).await;
                assert_eq!(*conn.closes.lock().unwrap(), vec![old]);
            }
        }
    }

    #[tokio::test]
    async fn status_exhausts_single_retry_and_evicts_replacement() {
        let conn = status_connection(vec![
            Err(ConnError::Rejected("unknown_channel — 10".into())),
            Err(ConnError::Rejected("unknown_channel — 40".into())),
            Ok(status_reply()),
        ]);
        let client = test_client(conn.clone(), RouteState::Ready(RouteHandle::synthetic(10)));
        assert!(matches!(
            client.status("ckh_probe").await,
            Err(VaultError::Transport(_))
        ));
        assert_eq!(conn.requests.load(Ordering::SeqCst), 2);
        assert_eq!(conn.opens.load(Ordering::SeqCst), 1);
        assert!(matches!(*client.route.lock().unwrap(), RouteState::Empty));
    }

    #[tokio::test]
    async fn status_bad_reply_never_reports_auth_failure_or_retries() {
        for reply in [
            Err(ConnError::Rejected("permission_denied".into())),
            Ok(json!({"result": {"error": {"code": "needs_reauth"}}})),
        ] {
            let conn = status_connection(vec![reply, Ok(status_reply())]);
            let client = test_client(conn.clone(), RouteState::Empty);
            assert!(matches!(
                client.status("ckh_probe").await,
                Err(VaultError::Protocol(_))
            ));
            assert_eq!(conn.requests.load(Ordering::SeqCst), 1);
            assert_eq!(conn.opens.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn vault_token_debug_redacts_secret() {
        let secret = "vault-secret-literal";
        let token = VaultToken {
            token: secret.to_string(),
            expires_at_ms: Some(1234),
            record_version: 7,
            project_id: Some("project-id".to_string()),
            account_id: None,
            resolved_credential_id: None,
        };
        let debug = format!("{token:?}");

        assert!(!debug.contains(secret));
        assert!(debug.contains("<redacted>"));
        assert!(debug.contains("expires_at_ms: Some(1234)"));
        assert!(debug.contains("record_version: 7"));
    }

    #[test]
    fn vault_error_debug_redacts_payloads() {
        let secret = "vault-error-secret-literal";
        for error in [
            VaultError::Protocol(secret.to_string()),
            VaultError::Transport(secret.to_string()),
        ] {
            let debug = format!("{error:?}");
            assert!(!debug.contains(secret));
            assert!(debug.contains("<redacted>"));
        }
    }

    /// Success: `payload` is a BYTE ARRAY reassembled to the token; metadata carried.
    #[test]
    fn decodes_success_payload_bytes_to_token() {
        // "tok" as bytes.
        let body = json!({ "result": {
            "payload": [116, 111, 107],
            "expires_at_ms": 1234,
            "record_version": 7
        }});
        let got = decode_get_reply(&body).unwrap();
        assert_eq!(
            got,
            VaultToken {
                token: "tok".to_string(),
                expires_at_ms: Some(1234),
                record_version: 7,
                // Absent on the wire (non-antigravity, non-chatgpt) → None.
                project_id: None,
                account_id: None,
                resolved_credential_id: None,
            }
        );
    }

    #[test]
    fn decodes_vault_resolved_credential_witness_verbatim() {
        let body = json!({"result": {
            "payload": [97], "record_version": 1,
            "credential_id": "apikey:kimi-for-coding"
        }});
        assert_eq!(
            decode_get_reply(&body)
                .unwrap()
                .resolved_credential_id
                .as_deref(),
            Some("apikey:kimi-for-coding")
        );
    }

    #[test]
    fn absent_or_empty_resolved_credential_is_no_witness_not_an_error() {
        let base = json!({"result": {"payload": [97], "record_version": 1}});
        assert_eq!(
            decode_get_reply(&base).unwrap().resolved_credential_id,
            None
        );
        for empty in [json!(null), json!(""), json!(" \t ")] {
            let mut body = base.clone();
            body["result"]["credential_id"] = empty;
            assert_eq!(
                decode_get_reply(&body).unwrap().resolved_credential_id,
                None
            );
        }
        let mut future = base;
        future["result"]["future_vault_field"] = json!(true);
        assert!(
            decode_get_reply(&future).is_ok(),
            "production stays forward-compatible"
        );
    }

    /// Success with a null expiry (e.g. OpenAI gives no expiry) decodes to `None`.
    #[test]
    fn decodes_success_with_null_expiry() {
        let body = json!({ "result": {
            "payload": [97],
            "expires_at_ms": null,
            "record_version": 1
        }});
        let got = decode_get_reply(&body).unwrap();
        assert_eq!(got.expires_at_ms, None);
        assert_eq!(got.token, "a");
        assert_eq!(got.project_id, None);
    }

    /// An Antigravity credential carries the bound `project_id` alongside the token.
    #[test]
    fn decodes_antigravity_project_id() {
        let body = json!({ "result": {
            "payload": [116, 111, 107],
            "expires_at_ms": 999,
            "record_version": 2,
            "project_id": "encouraging-env-qwp21"
        }});
        let got = decode_get_reply(&body).unwrap();
        assert_eq!(got.project_id.as_deref(), Some("encouraging-env-qwp21"));
        assert_eq!(got.token, "tok");
    }

    /// A ChatGPT credential carries the vault-parsed `account_id` alongside the token;
    /// a wire that omits the field (older vault, other credential kinds) decodes to `None`.
    #[test]
    fn decodes_chatgpt_account_id() {
        let body = json!({ "result": {
            "payload": [116, 111, 107],
            "expires_at_ms": 999,
            "record_version": 3,
            "account_id": "acct-77aa"
        }});
        let got = decode_get_reply(&body).unwrap();
        assert_eq!(got.account_id.as_deref(), Some("acct-77aa"));
        assert_eq!(got.project_id, None);
    }

    #[test]
    fn empty_wire_account_id_decodes_as_absent() {
        for account_id in ["", " \t "] {
            let body = json!({ "result": {
                "payload": [116, 111, 107],
                "expires_at_ms": null,
                "record_version": 4,
                "account_id": account_id
            }});
            assert_eq!(decode_get_reply(&body).unwrap().account_id, None);
        }
    }

    /// needs_reauth comes back as a RESPONSE (not an Error frame) and maps to the
    /// re-auth action — the load-bearing decode subtlety.
    #[test]
    fn decodes_needs_reauth_app_error() {
        let body = json!({ "result": { "error": { "code": "needs_reauth" } } });
        assert_eq!(decode_get_reply(&body), Err(VaultError::NeedsReauth));
    }

    /// Every app-error code maps to its typed variant (snake_case from the wire).
    #[test]
    fn maps_every_app_error_code() {
        let cases = [
            ("not_found", VaultError::NotFound),
            ("needs_reauth", VaultError::NeedsReauth),
            ("refresh_unsupported", VaultError::RefreshUnsupported),
            ("refresh_failed", VaultError::RefreshFailed),
            ("vault_locked", VaultError::VaultLocked),
            ("corrupt", VaultError::Corrupt),
            ("too_many_items", VaultError::TooManyItems),
        ];
        for (code, want) in cases {
            let body = json!({ "result": { "error": { "code": code } } });
            assert_eq!(decode_get_reply(&body), Err(want), "code {code}");
        }
    }

    /// An unknown error code is a protocol/contract-drift error, not a silently-dropped one.
    #[test]
    fn unknown_error_code_is_protocol_error() {
        let body = json!({ "result": { "error": { "code": "brand_new_code" } } });
        assert!(matches!(
            decode_get_reply(&body),
            Err(VaultError::Protocol(_))
        ));
    }

    /// A body missing the `result` envelope is a protocol error.
    #[test]
    fn missing_result_envelope_is_protocol_error() {
        let body = json!({ "payload": [1, 2, 3], "record_version": 1 });
        assert!(matches!(
            decode_get_reply(&body),
            Err(VaultError::Protocol(_))
        ));
    }

    /// report_auth_failure serializes to the exact handle-keyed contract shape, including
    /// the served record_version the vault CAS-invalidates against.
    #[test]
    fn report_request_serializes_to_contract_shape() {
        let req = ReportRequest {
            method: "credential.report_auth_failure",
            params: ReportParams {
                handle: Some("ckh_abc"),
                credential_id: None,
                provider_status: 401,
                reporter_source: REPORTER_SOURCE_DIRECT,
                record_version: 7,
            },
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({ "method": "credential.report_auth_failure", "params": { "handle": "ckh_abc", "provider_status": 401, "reporter_source": "direct", "record_version": 7 } })
        );
    }

    /// claustrum's own contract test writes this file; it is copied here unchanged so the
    /// decoder below is tested against bytes the vault really produced.
    const CLAUSTRUM_WIRE_CONTRACT: &str =
        include_str!("../tests/fixtures/claustrum_enrollment_wire_contract.json");

    /// The `credential.list_scoped` reply body from a claustrum wire-contract file.
    fn list_scoped_reply_in(contract: &str) -> serde_json::Value {
        let contract: serde_json::Value = serde_json::from_str(contract).unwrap();
        let entry = contract["operations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|op| op["op"] == "credential.list_scoped")
            .expect("the contract file pins credential.list_scoped");
        serde_json::from_str(entry["reply"].as_str().expect("a reply string")).unwrap()
    }

    #[test]
    fn list_scoped_decodes_claustrums_own_reply_bytes() {
        let list = decode_list_scoped_reply(&list_scoped_reply_in(CLAUSTRUM_WIRE_CONTRACT))
            .expect("claustrum's reply decodes");
        assert_eq!(list.view, "z6YPRoaEbsvZCVjeHcMgtww8l+K7efcxytUM+ysoaDI=");
        assert_eq!(list.grants, 2);
        assert_eq!(
            list.grant_tuples,
            vec![
                ScopedGrant {
                    selector_kind: "category".into(),
                    selector: "llm-provider".into(),
                    operation: "read".into(),
                },
                ScopedGrant {
                    selector_kind: "category".into(),
                    selector: "github-app-native".into(),
                    operation: "read".into(),
                },
            ]
        );
        let ids: Vec<&str> = list.credentials.iter().map(|row| row.id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "antigravity:google",
                "apikey:openrouter",
                "chatgpt:openai",
                "github_app:plex-alfonso",
                "oauth:anthropic",
            ]
        );
        let row = |id: &str| list.credentials.iter().find(|row| row.id == id).unwrap();
        let static_key = row("apikey:openrouter");
        assert_eq!(static_key.credential_type, "apikey");
        assert_eq!(static_key.state, "active");
        assert_eq!(static_key.refresh_adapter, None);
        assert_eq!(static_key.account_id, None);
        assert_eq!(static_key.provider_ids, ["openrouter"]);
        assert_eq!(static_key.auth_method.as_deref(), Some("apikey"));
        let oauth = row("oauth:anthropic");
        assert_eq!(oauth.credential_type, "oauth");
        assert_eq!(oauth.record_version, 232);
        assert_eq!(oauth.refresh_adapter.as_deref(), Some("anthropic"));
        assert_eq!(
            oauth.account_id.as_deref(),
            Some("00000000-0000-4000-8000-000000000000")
        );
        assert_eq!(oauth.operations, ["read"]);
        assert_eq!(oauth.provider_ids, ["anthropic", "claude-code"]);
        assert_eq!(oauth.auth_method.as_deref(), Some("oauth"));
        // A row may serve no catalog provider (empty `provider_ids`), and a row whose kind names
        // no authentication method (a GitHub App credential) carries no `auth_method` at all.
        assert!(row("chatgpt:openai").provider_ids.is_empty());
        assert_eq!(
            row("chatgpt:openai").auth_method.as_deref(),
            Some("chatgpt")
        );
        assert_eq!(row("github_app:plex-alfonso").auth_method, None);
    }

    /// A credential that needs re-authentication is listed with that state and kept. The
    /// reply is claustrum's own bytes with one row's state changed, so every other field is
    /// still exactly what the vault writes.
    #[test]
    fn list_scoped_keeps_a_needs_reauth_row_and_its_state() {
        let mut reply = list_scoped_reply_in(CLAUSTRUM_WIRE_CONTRACT);
        let rows = reply["result"]["credentials"].as_array_mut().unwrap();
        let count = rows.len();
        let oauth = rows
            .iter_mut()
            .find(|row| row["id"] == "oauth:anthropic")
            .unwrap();
        oauth["state"] = json!("needs_reauth");
        let list = decode_list_scoped_reply(&reply).unwrap();
        assert_eq!(list.credentials.len(), count, "no row is filtered by state");
        let oauth = list
            .credentials
            .iter()
            .find(|row| row.id == "oauth:anthropic")
            .unwrap();
        assert_eq!(oauth.state, "needs_reauth");
    }

    /// A newer vault may add fields; production decoding ignores them.
    #[test]
    fn list_scoped_tolerates_fields_it_does_not_read() {
        let mut reply = list_scoped_reply_in(CLAUSTRUM_WIRE_CONTRACT);
        reply["result"]["added_later"] = json!(true);
        reply["result"]["credentials"][0]["added_later"] = json!(1);
        assert_eq!(
            decode_list_scoped_reply(&reply).unwrap().credentials.len(),
            5
        );
    }

    #[test]
    fn list_scoped_maps_an_app_error_and_refuses_a_missing_view() {
        let refused = json!({"result": {"error": {"class": "permanent", "code": "not_found"}}});
        assert_eq!(
            decode_list_scoped_reply(&refused),
            Err(VaultError::NotFound)
        );
        let mut reply = list_scoped_reply_in(CLAUSTRUM_WIRE_CONTRACT);
        reply["result"].as_object_mut().unwrap().remove("view");
        assert!(matches!(
            decode_list_scoped_reply(&reply),
            Err(VaultError::Protocol(_))
        ));
    }

    // Strict, test-only mirrors of the list_scoped reply. Production decoding above ignores
    // unknown fields so a newer vault still works; these refuse them, so a field claustrum
    // adds shows up here as a failing test instead of being dropped without trace.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    #[allow(dead_code)]
    struct StrictListScopedReply {
        result: StrictListScopedResult,
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    #[allow(dead_code)]
    struct StrictListScopedResult {
        credentials: Vec<StrictScopedCredential>,
        grants: usize,
        grant_tuples: Vec<StrictScopedGrant>,
        view: String,
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    #[allow(dead_code)]
    struct StrictScopedCredential {
        id: String,
        categories: Vec<String>,
        #[serde(rename = "type")]
        credential_type: String,
        serves: Vec<String>,
        refresh_adapter: Option<String>,
        state: String,
        record_version: u64,
        operations: Vec<String>,
        account_id: Option<String>,
        email: Option<String>,
        org_name: Option<String>,
        provider_ids: Vec<String>,
        auth_method: Option<String>,
        // The operator's subscription label (claustrum f512290). Acknowledged here so this mirror
        // still catches the next new field; production decoding does not read it, because broca
        // chooses accounts through routing, not by plan.
        plan_tier: Option<String>,
        // How the vault learned `plan_tier` (operator-set or detected). Unread by broca for the
        // same reason as `plan_tier`.
        plan_tier_source: Option<String>,
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    #[allow(dead_code)]
    struct StrictScopedGrant {
        selector_kind: String,
        selector: String,
        operation: String,
    }

    fn assert_strict_list_scoped(contract: &str, source: &str) {
        let reply = list_scoped_reply_in(contract);
        if let Err(error) = serde_json::from_value::<StrictListScopedReply>(reply.clone()) {
            panic!(
                "{source}: the credential.list_scoped reply carries a field broca's decoder does \
                 not read, or changed a known one: {error}. Update ScopedList/ScopedCredential \
                 in broca-subc/src/vault.rs and this strict mirror"
            );
        }
        decode_list_scoped_reply(&reply).expect("production decoding accepts it too");
    }

    #[test]
    fn list_scoped_strict_mirror_matches_the_copied_fixture() {
        assert_strict_list_scoped(CLAUSTRUM_WIRE_CONTRACT, "copied fixture");
    }

    /// Against claustrum's tip. The checkout is named by `CK_CLAUSTRUM_CHECKOUT` when set (the
    /// local gate builds the binary into its own target dir, so the binary's path says nothing
    /// about where the source is). Otherwise CI's layout is assumed: it checks claustrum out
    /// beside broca and names its binary in `CK_CLAUSTRUM_BIN`
    /// (`<checkout>/target/debug/ck-claustrum`), three directories below the checkout. When either
    /// variable is set, the file must exist; with neither (a local run with no claustrum
    /// checkout) there is nothing to compare.
    #[test]
    fn list_scoped_strict_mirror_matches_claustrum_tip() {
        let non_empty = |name: &str| std::env::var_os(name).filter(|value| !value.is_empty());
        let checkout = if let Some(checkout) = non_empty("CK_CLAUSTRUM_CHECKOUT") {
            std::path::PathBuf::from(checkout)
        } else if let Some(bin) = non_empty("CK_CLAUSTRUM_BIN") {
            std::path::Path::new(&bin)
                .ancestors()
                .nth(3)
                .expect("CK_CLAUSTRUM_BIN is <checkout>/target/<profile>/ck-claustrum")
                .to_path_buf()
        } else {
            eprintln!("CK_CLAUSTRUM_CHECKOUT and CK_CLAUSTRUM_BIN unset: no claustrum checkout to compare against");
            return;
        };
        let path =
            checkout.join("crates/credentials-module/tests/fixtures/enrollment_wire_contract.json");
        let contract = std::fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!("claustrum's wire contract at {}: {error}", path.display())
        });
        assert_strict_list_scoped(&contract, &path.display().to_string());
    }

    /// list_scoped sends an empty params object and needs a consumer identity.
    #[tokio::test]
    async fn list_scoped_sends_empty_params_and_needs_an_identity() {
        let conn = status_connection(vec![Ok(list_scoped_reply_in(CLAUSTRUM_WIRE_CONTRACT))]);
        let client = supervised_client(conn.clone());
        let list = client.list_scoped().await.unwrap();
        assert_eq!(list.credentials.len(), 5);
        assert_eq!(
            sent_bodies(&conn),
            vec![json!({"method": "credential.list_scoped", "params": {}})]
        );

        let anonymous_conn = status_connection(Vec::new());
        let anonymous = test_client(anonymous_conn.clone(), RouteState::Empty);
        assert!(matches!(
            anonymous.list_scoped().await,
            Err(VaultError::NoConsumerIdentity { .. })
        ));
        assert_eq!(anonymous_conn.requests.load(Ordering::SeqCst), 0);
    }

    /// The request body serializes to the exact wire shape, omitting absent optionals.
    #[test]
    fn request_serializes_to_contract_shape() {
        let req = GetRequest {
            method: "credential.get",
            params: GetParams {
                handle: "ckh_abc",
                min_ttl_ms: Some(60000),
                force_refresh: None,
            },
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({ "method": "credential.get", "params": { "handle": "ckh_abc", "min_ttl_ms": 60000 } })
        );
    }
}
