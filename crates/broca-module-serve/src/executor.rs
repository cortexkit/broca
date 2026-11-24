use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_catalog::live::{AuthMode, CachePolicy};
use broca_core::{AllowAll, OutputEvent, RunError, RunSummary};
use broca_module::executor::{RunExecutor, RunKind};
use broca_protocol::caller::{PauseReason, RunFinishReason};
use broca_protocol::error::{ErrorClass, ProviderError};
use broca_protocol::{AuthRequest, BindIdentity};
use broca_provider::credential_refresh::{
    ExpiringRefresh, ExpiringRefreshFuture, RefreshFuture, RefreshedSecret, RejectedRefresh,
    SecretRefresher, SharedSecretRefresher,
};
use broca_provider::{build_provider_http_client, ProviderHttpClient};
use broca_subc::{SubcToolPlane, VaultError, VaultGet, VaultToken};
use tokio::sync::mpsc;

use broca_catalog::live::WireFamilyId;
use broca_module_serve::account_routing::{AccountChoice, RoutingClient};
use broca_module_serve::{AuthMethod, CacheTtlTable};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EpisodeAuthContext {
    FreshSession,
    NewEpisode,
    Resume,
}

fn episode_auth_context(
    fresh_episode: bool,
    session_has_prior_episode: bool,
) -> EpisodeAuthContext {
    if !fresh_episode {
        EpisodeAuthContext::Resume
    } else if session_has_prior_episode {
        EpisodeAuthContext::NewEpisode
    } else {
        EpisodeAuthContext::FreshSession
    }
}

fn caller_auth_for_episode(
    auth: Option<&AuthRequest>,
    context: EpisodeAuthContext,
) -> Result<(Option<AuthMethod>, Option<&str>), RunError> {
    let Some(auth) = auth else {
        return Ok((None, None));
    };
    if context == EpisodeAuthContext::Resume {
        return Err(RunError::Config(
            "auth pin refused: this send is a genuine resume of an unfinished episode; \
                 a fresh run (a new session or a new episode after the prior episode finishes) \
                 would honor the pin"
                .into(),
        ));
    }
    let method = AuthMethod::from_token(&auth.method).ok_or_else(|| {
        RunError::Config(format!(
            "unknown auth method '{}'; accepted methods are 'apikey', 'oauth', \
                 'antigravity', and 'chatgpt'",
            auth.method
        ))
    })?;
    Ok((Some(method), auth.account.as_deref()))
}

/// The wire family a method IMPLIES, when the method selects a distinct wire grammar.
/// This is the SINGLE authority mapping method → family override, consulted by BOTH the
/// family DRIVER (the catalog resolver's `family_override`, which builds the run with this
/// family) AND the run-start consistency check below — so the family the run renders with
/// and the family the guard asserts can never come from two sources, hence can never
/// drift to a mismatched (family, credential) pair.
///
/// A same-family method (`ApiKey`/`Oauth`) returns `None` → the catalog's family is used
/// unchanged. The catalog still receives the method separately because Anthropic API key
/// changes frozen quirks and auth headers without changing the family. A wire-changing method
/// returns its family: `Antigravity` renders the Code-Assist envelope (`GoogleCodeAssist`),
/// `Chatgpt` renders the codex `/responses` (`OpenAiResponsesChatgpt`).
pub use broca_module_serve::family_for_method;

fn preserve_billing_vault_snapshot(
    selection: &mut Option<broca_core::AuthSelection>,
    frozen: Option<&broca_core::AuthSelection>,
) {
    if let (Some(selection), Some(frozen)) = (selection, frozen) {
        selection.billing_vault_facts = frozen.billing_vault_facts.clone();
    }
}

/// The full auth resolution for a run: how to authenticate, whether to pause for re-auth,
/// what to freeze, and (for the consistency guard) which method was chosen. NO `Debug`
/// derive — `mode` can carry a secret token.
pub struct AuthResolution {
    /// How the provider authenticates (the shaped vault credential, or none for mocks).
    pub mode: AuthMode,
    /// Set when the credential needs re-auth: admit then pause (no provider call).
    pub pause: Option<ProviderError>,
    /// The credential id to FREEZE into a fresh run's RunConfig. `None` on the mock path;
    /// On a resume this is the already-frozen value (re-frozen is a no-op — the loop only
    /// writes it on a fresh RunStarted).
    pub selection: Option<broca_core::AuthSelection>,
    /// The resolved method used by provider-family selection and the consistency guard.
    pub method: Option<AuthMethod>,
    /// The cloud project the credential carries — `Some` only for an Antigravity
    /// credential (a body render input the google-codeassist family freezes). `None`
    /// otherwise. Off the secret path (it's a project name, not a token).
    pub project_id: Option<String>,
    /// The provider-side account the credential belongs to — `Some` only for a ChatGPT
    /// credential (the vault parses it from the token at refresh time, so it tracks
    /// re-logins). The authoritative `ChatGPT-Account-Id` header source; the catalog's
    /// local JWT decode is the fallback. Off the secret path (an account id, not a token).
    pub account_id: Option<String>,
    /// The record_version of the vault credential THIS run was served (from
    /// `GetResult.record_version`); `None` off the vault path or when no token was
    /// served. Echoed on a wire-auth-failure report so the vault CAS-invalidates only
    /// the version the provider actually rejected — a stale report cannot kill an
    /// already-refreshed credential. Off the secret path (a counter, not a token).
    pub record_version: Option<u64>,
    /// How this run gets a fresh token for its credential after a mid-run 401. Present when
    /// the vault served a token; `None` keeps a 401 pausing the run straight away.
    pub in_run_refresh: Option<InRunRefresh>,
    /// The hosts the vault listed with the credential. Every request carrying it must go to
    /// one of them; empty (none listed, or no credential) admits no host for a credential.
    pub credential_hosts: broca_provider::credential_hosts::CredentialHosts,
}

/// A run's in-step token refresh, plus the record_version of the token the run is using
/// NOW. That version starts as the admission-time one and moves when a refresh serves a new
/// token, so the terminal wire-auth report names the token the provider last rejected.
/// Cloning shares the same counters, so a clone taken before the run sees its moves.
#[derive(Clone)]
pub struct InRunRefresh {
    pub refresher: SharedSecretRefresher,
    /// The refresher moves it; the run-end report names the version the run's last attempt
    /// sent instead. Exposed so tests can see it move.
    #[cfg_attr(not(test), allow(dead_code))]
    pub served_version: Arc<AtomicU64>,
    /// When the token the run is using NOW expires (`None` for a token that never does,
    /// such as an API key). Moves together with `served_version`, so the pre-send check
    /// always judges the token the next request would carry. Only the refresher reads it
    /// in production; it is exposed here, like `served_version`, so tests can see it move.
    #[cfg_attr(not(test), allow(dead_code))]
    pub served_expires_at_ms: Arc<std::sync::Mutex<Option<i64>>>,
    /// The record_version the in-step refresh already reported to the vault, if it ran.
    /// The run-end report skips that version, so one rejected token is reported once.
    pub reported_version: Arc<std::sync::Mutex<Option<u64>>>,
    /// A rejection nothing has reported yet: the provider rejected the token the step had
    /// already refreshed, so no in-step refresh ran for it. The run-end report sends exactly
    /// this one, with its own status and version.
    pub unreported_rejection: Arc<std::sync::Mutex<Option<UnreportedRejection>>>,
}

/// One provider rejection still owed a vault report: the HTTP status the provider answered
/// and the record_version of the token it rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnreportedRejection {
    pub status: u16,
    pub record_version: u64,
}

/// Gets a fresh token for the ONE credential a run was admitted with, after the provider
/// rejected the token with a 401. Never reads another credential.
///
/// A scoped read cannot force a refresh (`credential.get_scoped` has no `force_refresh`, on
/// purpose: it would let any grant holder spend an upstream mint on demand). The vault's
/// route to a fresh token is REPORT, THEN RE-READ: the failure report marks the served
/// version stale if it is still current, and the next scoped read of a stale record
/// refreshes it and comes back at a higher record_version. If another reader already
/// refreshed, the report names a superseded version and changes nothing, and the re-read
/// simply returns the newer token; concurrent readers share one refresh.
///
/// The report sent here IS the refresh trigger, so it is the only report for the rejected
/// version: it is recorded in `reported_version` and the run-end report skips it.
///
/// It also replaces a token that is about to expire before each attempt, with a plain
/// re-read and no report (see `refresh_expiring_secret`).
struct VaultTokenRefresher {
    vault: Arc<VaultAuth>,
    credential_key: String,
    vault_id: String,
    served_version: Arc<AtomicU64>,
    served_expires_at_ms: Arc<std::sync::Mutex<Option<i64>>>,
    reported_version: Arc<std::sync::Mutex<Option<u64>>>,
    unreported_rejection: Arc<std::sync::Mutex<Option<UnreportedRejection>>>,
    /// Set by the first failed pre-send re-read, so a vault outage logs one warning per
    /// run rather than one per attempt.
    presend_failure_logged: std::sync::atomic::AtomicBool,
    no_expiry_logged: std::sync::atomic::AtomicBool,
}

/// How close to its expiry a held token may get before the next attempt replaces it.
///
/// The request and its whole stream must fit inside the token's remaining life. Providers
/// (Anthropic and Google, measured) check the token when a request arrives, not while it
/// streams, so a token that expires mid-stream fails only the NEXT request. Five minutes
/// covers any single request comfortably. It is half the lifetime demanded on every read,
/// so a token the vault just served is never immediately judged to be expiring.
const PRESEND_REFRESH_MARGIN_MS: i64 = VAULT_MIN_TTL_MS / 2;

/// Whether a token expiring at `expires_at_ms` should be replaced before a send at
/// `now_ms`. A token without an expiry (an API key) never is.
fn held_token_is_expiring(expires_at_ms: Option<i64>, now_ms: i64) -> bool {
    expires_at_ms.is_some_and(|expires_at_ms| {
        now_ms.saturating_add(PRESEND_REFRESH_MARGIN_MS) > expires_at_ms
    })
}

impl VaultTokenRefresher {
    fn served_expiry(&self) -> Option<i64> {
        *self
            .served_expires_at_ms
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn set_served_expiry(&self, expires_at_ms: Option<i64>) {
        *self
            .served_expires_at_ms
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = expires_at_ms;
    }

    /// Accounts for a failed vault read in the cache and the health gauge, the same way for
    /// both refresh paths.
    fn note_read_failure(&self, error: &VaultError) {
        let error_class = lkg_error_class(error);
        evict_if_vault_authoritative(&self.vault.cache, &self.credential_key, error_class);
        if error_class == LkgErrorClass::Connectivity {
            self.vault.gauge.stamp_error();
        } else {
            self.vault.gauge.stamp_ok();
        }
    }
}

impl SecretRefresher for VaultTokenRefresher {
    fn refresh_rejected_secret(&self, status: u16) -> RefreshFuture<'_, RefreshedSecret> {
        Box::pin(async move {
            let rejected = self.served_version.load(Ordering::SeqCst);
            // The provider's rejection is authoritative for the version it rejected: never
            // let this process's cache serve it again, whatever the vault answers next.
            evict_for_provider_rejection(&self.vault.cache, &self.credential_key, status, rejected);
            // Recorded before sending, so the run-end report is skipped even when this one
            // is lost in transit: the rule is at most one report per rejected version.
            *self
                .reported_version
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = Some(rejected);
            if let Err(error) = self
                .vault
                .client
                .report_auth_failure_at(
                    broca_subc::VaultAddress::CredentialId(&self.vault_id),
                    status,
                    rejected,
                )
                .await
            {
                // The re-read below still runs: a newer token may already exist.
                tracing::warn!(
                    target: "credentials",
                    "in-run {status} report for '{}' v{rejected} failed: {error}",
                    self.vault_id
                );
            }
            let min_ttl_ms = Some(VAULT_MIN_TTL_MS);
            let got = self
                .vault
                .client
                .get_scoped(&self.vault_id, min_ttl_ms)
                .await;
            match got.result {
                Ok(token) => {
                    self.vault.gauge.stamp_ok();
                    // RE-SEND ONLY FOR A NEWER TOKEN. An unchanged version means the vault
                    // did not refresh: the token broca holds is the current one, so the 401
                    // is not staleness and sending the same token again cannot succeed.
                    // The run pauses as it did before in-step refresh existed.
                    if token.record_version <= rejected {
                        tracing::warn!(target: "credentials", "credential reread outcome=same_version vault_id='{}' rejected_version={rejected} served_version={} expires_at_ms={}", self.vault_id, token.record_version, token.expires_at_ms.map_or_else(|| "none".to_string(), |value| value.to_string()));
                        return RejectedRefresh::Unavailable;
                    }
                    tracing::warn!(target: "credentials", "credential reread outcome=refreshed vault_id='{}' old_version={rejected} new_version={}", self.vault_id, token.record_version);
                    self.vault
                        .cache
                        .insert(self.credential_key.clone(), token.clone());
                    self.served_version
                        .store(token.record_version, Ordering::SeqCst);
                    self.set_served_expiry(token.expires_at_ms);
                    RejectedRefresh::Token(RefreshedSecret {
                        evidence: broca_protocol::CredentialRefresh {
                            vault_op: broca_core::VaultReadOp::GetScoped.as_str().to_string(),
                            min_ttl_ms,
                            // A scoped read carries no force flag; the refresh came from
                            // the failure report marking the rejected version stale.
                            force_refresh: false,
                            record_version: Some(token.record_version),
                            reason: Some(broca_protocol::CredentialRefresh::REASON_REJECTED.into()),
                        },
                        account_ref: token.account_id,
                        hosts: broca_provider::credential_hosts::CredentialHosts::from_record(
                            token.hosts.as_deref(),
                        ),
                        secret: token.token,
                    })
                }
                // Only the vault's definite sign-in loss can authorize account continuation.
                // Any other read failure proves nothing about the sign-in: the provider's 401
                // stays the run's error and the run pauses for sign-in as before.
                Err(error) => {
                    self.note_read_failure(&error);
                    tracing::warn!(target: "credentials", "credential reread outcome=reread_failed vault_id='{}' rejected_version={rejected} error_class={:?} vault_replied={}", self.vault_id, lkg_error_class(&error), got.vault_replied);
                    if matches!(error, VaultError::NeedsReauth) {
                        RejectedRefresh::NeedsReauth
                    } else {
                        RejectedRefresh::Unavailable
                    }
                }
            }
        })
    }

    fn note_unrecovered_rejection(&self, status: u16) {
        // The transport calls this only after the step already refreshed once and re-sent
        // with the refreshed token, so the token held now is the one just rejected.
        let record_version = self.served_version.load(Ordering::SeqCst);
        *self
            .unreported_rejection
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(UnreportedRejection {
            status,
            record_version,
        });
    }

    /// A plain scoped re-read when the held token is close to expiry. The vault refreshes a
    /// record that cannot meet `min_ttl_ms`, so the read returns a token with at least that
    /// much life left. NO failure report is sent: the token was not rejected, and a report
    /// would make the vault count an ordinary expiry as a provider rejection.
    fn refresh_expiring_secret(&self) -> ExpiringRefreshFuture<'_, RefreshedSecret> {
        Box::pin(async move {
            let expires_at_ms = self.served_expiry();
            if expires_at_ms.is_none() {
                if !self
                    .no_expiry_logged
                    .swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    tracing::info!(target: "credentials", "held credential for '{}' has no expiry; pre-send refresh cannot fire", self.vault_id);
                }
                return ExpiringRefresh::Unavailable;
            }
            if !held_token_is_expiring(expires_at_ms, epoch_millis()) {
                return ExpiringRefresh::Unavailable;
            }
            let held = self.served_version.load(Ordering::SeqCst);
            let min_ttl_ms = Some(VAULT_MIN_TTL_MS);
            let got = self
                .vault
                .client
                .get_scoped(&self.vault_id, min_ttl_ms)
                .await;
            match got.result {
                Ok(token) => {
                    self.vault.gauge.stamp_ok();
                    if token.record_version < held {
                        // Older than what this run already holds: never step backwards.
                        return ExpiringRefresh::Unavailable;
                    }
                    self.vault
                        .cache
                        .insert(self.credential_key.clone(), token.clone());
                    self.set_served_expiry(token.expires_at_ms);
                    // The same version is the token already held. Its expiry is taken from
                    // the vault's answer above, so the check is not repeated on stale data,
                    // but there is nothing new to send.
                    if token.record_version == held {
                        tracing::info!(target: "credentials", "credential reread outcome=same_version vault_id='{}' served_version={} expires_at_ms={}", self.vault_id, token.record_version, token.expires_at_ms.map_or_else(|| "none".to_string(), |value| value.to_string()));
                        return ExpiringRefresh::Unavailable;
                    }
                    self.served_version
                        .store(token.record_version, Ordering::SeqCst);
                    ExpiringRefresh::Token(RefreshedSecret {
                        evidence: broca_protocol::CredentialRefresh {
                            vault_op: broca_core::VaultReadOp::GetScoped.as_str().to_string(),
                            min_ttl_ms,
                            force_refresh: false,
                            record_version: Some(token.record_version),
                            reason: Some(broca_protocol::CredentialRefresh::REASON_EXPIRING.into()),
                        },
                        account_ref: token.account_id,
                        hosts: broca_provider::credential_hosts::CredentialHosts::from_record(
                            token.hosts.as_deref(),
                        ),
                        secret: token.token,
                    })
                }
                // A definite sign-in loss is distinct from a read that could not tell.
                // The transport can still use the held token until its own expiry.
                Err(error) => {
                    self.note_read_failure(&error);
                    if matches!(error, VaultError::NeedsReauth) {
                        return ExpiringRefresh::NeedsReauth {
                            held_expires_at_ms: expires_at_ms,
                        };
                    }
                    if !self
                        .presend_failure_logged
                        .swap(true, std::sync::atomic::Ordering::SeqCst)
                    {
                        tracing::warn!(
                            target: "credentials",
                            "pre-send refresh of expiring token for '{}' v{held} failed; \
                             sending with the held token: {error}",
                            self.vault_id
                        );
                    }
                    ExpiringRefresh::Unavailable
                }
            }
        })
    }
}

fn resolve_provider_for_episode(
    model_id: &str,
    provider_base_url: Option<&str>,
    max_output_tokens: Option<u32>,
    resolution: &AuthResolution,
    options: &broca_catalog::live::ResolveOptions<'_>,
    client: ProviderHttpClient,
    render: broca_catalog::live::EpisodeRenderConfig<'_>,
) -> Result<broca_catalog::live::ProviderResolution, broca_catalog::live::ResolutionError> {
    if resolution.pause.is_some()
        && options.family_override == Some(WireFamilyId::OpenAiResponsesChatgpt)
    {
        broca_catalog::live::resolve_chatgpt_pause_episode_with_client(
            model_id,
            max_output_tokens,
            options,
            client,
            render,
        )
    } else if let Some(base_url) = provider_base_url.filter(|url| !url.is_empty()) {
        broca_catalog::live::resolve_catalog_episode_at_base_url(
            model_id,
            base_url,
            max_output_tokens,
            &resolution.mode,
            options,
            client,
            render,
        )
    } else {
        broca_catalog::live::resolve_catalog_episode_with_client(
            model_id,
            max_output_tokens,
            &resolution.mode,
            options,
            client,
            render,
        )
    }
}

/// What the executor does about auth for THIS run, decided from the credential source's
/// result before the loop. Kept as a typed enum so the mapping is exhaustive + unit-
/// tested in isolation from the live vault connection. Deliberately NO `Debug` derive —
/// `Proceed` can carry a secret token, which must never reach a log line.
pub enum AuthDecision {
    /// Resolve the provider with this auth mode and run normally.
    Proceed(AuthMode),
    /// The credential needs re-auth (authoritative, before any provider call): admit the
    /// run then pause it for re-auth (carrying this error), making NO provider call. The
    /// run is resumable under the same id once the user re-auths.
    PauseForReauth(Box<ProviderError>),
    /// Fail admission permanently (no RunStarted, no ghost run) because retrying cannot
    /// repair the credential configuration.
    FailAdmission(String),
    /// Fail admission transiently (no RunStarted, no ghost run). There is no durable run to
    /// pause yet, so the caller retries through its normal transient-error policy.
    FailAdmissionTransient(String),
}

/// Map a credential.get result into the run's auth decision. ONLY `needs_reauth` is a
/// resumable pause (the refresh token is dead → the user must re-auth, and the run waits
/// for them). Connectivity and lock failures reject admission as transient because no run
/// exists to pause yet; malformed/missing credentials remain permanent configuration faults.
/// `mode` when it fits the credential's method: a gateway key, and only a gateway key, comes
/// with its gateway's address. Anything else would send the company's key to the provider's
/// own address, or an ordinary key to a gateway, so the run is refused before it starts.
fn gateway_mode_for(
    method: AuthMethod,
    mode: AuthMode,
    credential: &str,
) -> Result<AuthMode, RunError> {
    match (method, &mode) {
        (AuthMethod::Gateway, AuthMode::Gateway { .. }) => Ok(mode),
        (AuthMethod::Gateway, _) => Err(RunError::Config(format!(
            "gateway credential '{credential}' came without its gateway's address"
        ))),
        (_, AuthMode::Gateway { .. }) => Err(RunError::Config(format!(
            "credential '{credential}' has a gateway's address but is not a gateway key"
        ))),
        _ => Ok(mode),
    }
}

pub fn vault_outcome_to_decision(result: Result<VaultToken, VaultError>) -> AuthDecision {
    match result {
        Ok(VaultToken {
            token,
            gateway: Some(endpoint),
            ..
        }) => AuthDecision::Proceed(AuthMode::Gateway {
            key: token,
            endpoint,
        }),
        Ok(token) => AuthDecision::Proceed(AuthMode::Secret(token.token)),
        Err(VaultError::NeedsReauth) => AuthDecision::PauseForReauth(Box::new(ProviderError {
            provider_details: None,
            cause: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Auth,
            message: "credential needs re-authentication (vault: refresh token expired \
                      or revoked)"
                .to_string(),
            status: None,
            retry_after_secs: None,
            provider_code: Some("needs_reauth".to_string()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
            account_continue: None,
        })),
        Err(e @ VaultError::Transport(_)) => AuthDecision::FailAdmissionTransient(format!(
            "credentials service unreachable (transient — retry): {e}"
        )),
        Err(VaultError::VaultLocked) => AuthDecision::FailAdmissionTransient(
            "credentials service locked (transient — retry)".to_string(),
        ),
        Err(e @ VaultError::RefreshFailed) => AuthDecision::FailAdmissionTransient(format!(
            "credential refresh failed (transient — retry): {e}"
        )),
        Err(e) => {
            AuthDecision::FailAdmission(format!("credential not configured or unusable: {e}"))
        }
    }
}

/// A minimum token lifetime to demand from the vault on `credential.get`, so a token
/// stays valid for the whole run (the vault refreshes if it would expire sooner). A run
/// is a single turn (model steps + tool calls) — minutes at most — so a generous budget
/// avoids a mid-run expiry without forcing needless refreshes.
const VAULT_MIN_TTL_MS: i64 = 10 * 60 * 1000;

/// Error classes relevant to deciding whether a cached credential may stand in for the
/// vault. Authoritative credential replies never lose to an older cached token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LkgErrorClass {
    Connectivity,
    /// The vault ANSWERED, and the answer makes this credential unusable.
    ///
    /// Renamed from `NeedsReauth`, which named ONE INSTANCE of the class and invited the
    /// mapping to carry only that instance -- which is exactly what happened. `NotFound` fell
    /// through to `Other`, so a revoked handle left its cached token in place, and a LATER
    /// VAULT OUTAGE served it.
    ///
    /// The incoherence is what makes that a defect rather than a liberal cache: the same
    /// credential was REFUSED while the vault was reachable and ACCEPTED once it was not.
    CredentialUnusable,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LkgDecision {
    Serve,
    Refuse,
}

/// The minimum information needed to decide cache eligibility without touching token bytes.
struct CachedCredentialMeta {
    expires_at_ms: Option<i64>,
}

/// Decide whether an in-memory token remains safe to serve during a vault outage. Tokens
/// without an expiry are API keys and remain eligible; expiring tokens must clear the same
/// minimum-TTL margin requested from the normal vault path.
fn lkg_decision(
    cached: Option<&CachedCredentialMeta>,
    now_ms: i64,
    min_ttl_ms: i64,
    error_class: LkgErrorClass,
) -> LkgDecision {
    if error_class != LkgErrorClass::Connectivity {
        return LkgDecision::Refuse;
    }
    let Some(cached) = cached else {
        return LkgDecision::Refuse;
    };
    match cached.expires_at_ms {
        None => LkgDecision::Serve,
        Some(expires_at_ms) => {
            if now_ms.saturating_add(min_ttl_ms) <= expires_at_ms {
                LkgDecision::Serve
            } else {
                LkgDecision::Refuse
            }
        }
    }
}

/// Process-only token cache. It deliberately has no `Debug` implementation and is never
/// persisted or logged; entries are cloned only into a run's existing secret auth path.
#[derive(Default)]
pub struct VaultLkgCache {
    entries: Mutex<HashMap<String, VaultToken>>,
}

impl VaultLkgCache {
    fn insert(&self, credential_id: String, token: VaultToken) {
        let mut entries = self.entries.lock().unwrap();
        // Reads and refreshes from different runs can finish out of order. Compare
        // while holding the insertion lock so a late reply cannot roll back the cache.
        if entries
            .get(&credential_id)
            .is_some_and(|current| current.record_version > token.record_version)
        {
            return;
        }
        entries.insert(credential_id, token);
    }

    fn get(&self, credential_id: &str) -> Option<VaultToken> {
        self.entries.lock().unwrap().get(credential_id).cloned()
    }

    fn evict(&self, credential_id: &str) {
        self.entries.lock().unwrap().remove(credential_id);
    }

    fn evict_version(&self, credential_id: &str, record_version: u64) {
        // A completed run can report after another run refreshes this entry, so the
        // version check and removal must happen while holding the same lock.
        let mut entries = self.entries.lock().unwrap();
        if entries
            .get(credential_id)
            .is_some_and(|token| token.record_version == record_version)
        {
            entries.remove(credential_id);
        }
    }
}

fn evict_if_vault_authoritative(
    cache: &VaultLkgCache,
    credential_id: &str,
    error_class: LkgErrorClass,
) {
    if error_class == LkgErrorClass::CredentialUnusable {
        cache.evict(credential_id);
    }
}

fn evict_for_provider_rejection(
    cache: &VaultLkgCache,
    credential_id: &str,
    status: u16,
    record_version: u64,
) {
    if matches!(status, 401 | 403) {
        cache.evict_version(credential_id, record_version);
    }
}

fn epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn lkg_error_class(error: &VaultError) -> LkgErrorClass {
    match error {
        VaultError::Transport(_) | VaultError::Protocol(_) | VaultError::VaultLocked => {
            LkgErrorClass::Connectivity
        }
        // AUTHORITATIVE NEGATIVES: the vault replied, and the reply says this credential
        // cannot be used -- the grant needs re-auth, or the handle does not exist at all.
        // Both evict, because the doc on LkgErrorClass promises an authoritative reply never
        // loses to an older cached token.
        //
        // A scoped NotFound is byte-identical for "no grant", "credential absent" and "the
        // vault's store failed during the grant lookup" (claustrum pins the last equal to the
        // first, so the reply can't be used to probe which credentials exist). Evicting on it
        // is still right, per claustrum's owner: revoking a grant or removing a credential
        // means "stop using it now", and a cache that served until expiry would turn that into
        // "within the hour", or never for static keys with no expiry. The store-fault case
        // needs the vault's own SQLite to be failing, when the next fetch would fail anyway;
        // none was recorded in auth_events from 2026-08-11 to 2026-09-30.
        VaultError::NeedsReauth | VaultError::NotFound => LkgErrorClass::CredentialUnusable,
        // EVERYTHING ELSE STAYS `Other`, AND THE RESTRAINT IS DELIBERATE. These are refusals
        // about the REQUEST or the VAULT, not verdicts on the credential, so evicting on them
        // would discard a token that is still good:
        //   RefreshFailed       transient by its own definition
        //   TtlUnsatisfiable    about the min_ttl_ms THIS CODE chose, not the credential
        //   TooManyItems        a request-shape refusal
        //   RefreshUnsupported  a cached non-refreshable token is valid until it expires
        //   Corrupt             the record is unreadable NOW; the token already minted from
        //                       it may still work, and eviction removes the only copy
        _ => LkgErrorClass::Other,
    }
}

/// The request to freeze on the run, decided from the vault's RAW answer -- before the
/// cache fallback in `resolve_vault_outcome` can turn a failure into a served token.
///
/// PRESENT IFF THE VAULT'S REPLY WAS RECEIVED. `vault_request` exists so a vault's own
/// record of a credential.get can be joined against the run that made it, so it must mean
/// "the vault received this request and answered it". Every reply qualifies, including an
/// authoritative refusal (NotFound, NeedsReauth), a locked vault, or a reply broca could
/// not decode: in each of those the vault did get the request.
///
/// Everything else is recorded as None: a route open the daemon refused or that failed, a
/// daemon rejection of the request before delivery, a transport failure, a bad frame on
/// the connection. In those the vault either never saw the request or broca cannot tell,
/// so it claims nothing. Recording the request there was wrong: when the vault was
/// unreachable and a cached token was served, the export fact claimed a request the vault
/// never saw, and the join found a broca row with no vault row behind it.
///
/// Read from `VaultGet::vault_replied`, which the vault client sets from whether a reply
/// body arrived, NEVER from the error variant: a daemon refusal and an undecodable vault
/// reply are both `VaultError::Protocol`, so the variant cannot say who answered.
fn recorded_vault_request(
    outcome: &VaultGet,
    request: broca_core::VaultRequest,
) -> Option<broca_core::VaultRequest> {
    outcome.vault_replied.then_some(request)
}

fn resolve_vault_outcome(
    cache: &VaultLkgCache,
    gauge: &broca_module_serve::serve::VaultGauge,
    credential_id: &str,
    outcome: Result<VaultToken, VaultError>,
    now_ms: i64,
    min_ttl_ms: i64,
) -> Result<VaultToken, VaultError> {
    match outcome {
        Ok(token) => {
            gauge.stamp_ok();
            cache.insert(credential_id.to_string(), token.clone());
            Ok(token)
        }
        // Refused in this process before anything was sent: it says nothing about the vault,
        // so the gauge is left alone, and nothing about the credential, so the cache is too.
        Err(error @ VaultError::NoConsumerIdentity { .. }) => Err(error),
        Err(error) => {
            let error_class = lkg_error_class(&error);
            // Authoritative re-auth state must invalidate older process memory.
            evict_if_vault_authoritative(cache, credential_id, error_class);
            if error_class != LkgErrorClass::Connectivity {
                gauge.stamp_ok();
                return Err(error);
            }

            let cached = cache.get(credential_id);
            let cached_meta = cached.as_ref().map(|token| CachedCredentialMeta {
                expires_at_ms: token.expires_at_ms,
            });
            if lkg_decision(cached_meta.as_ref(), now_ms, min_ttl_ms, error_class)
                == LkgDecision::Serve
            {
                gauge.stamp_serving_cached();
                Ok(cached.expect("Serve requires a cached credential"))
            } else {
                gauge.stamp_error();
                Err(error)
            }
        }
    }
}

fn validate_routed_account(
    token: &VaultToken,
    routed_vault_id: &str,
    routed_account: &str,
) -> Result<(), RunError> {
    if token
        .account_id
        .as_deref()
        .is_some_and(|served| served != routed_account)
    {
        return Err(RunError::Config(format!(
            "account_identity_mismatch: routing chose account {routed_account} \
                 for credential {routed_vault_id}, but the vault served account {}",
            token.account_id.as_deref().unwrap_or_default()
        )));
    }
    Ok(())
}

fn should_check_wire_auth(outcome: &Result<RunSummary, RunError>) -> bool {
    matches!(
        outcome,
        Ok(summary)
            if summary.paused == Some(PauseReason::AuthRequired)
                || summary.reason == RunFinishReason::Error
    )
}

/// The terminal auth status to report to the vault. `terminal_status` reads it from the
/// session's storage and is awaited only when neither the outcome nor `cached_error`
/// settles the answer.
async fn post_run_wire_auth_status(
    outcome: &Result<RunSummary, RunError>,
    terminal_status: impl std::future::Future<Output = Option<u16>>,
    cached_error: Option<&Option<ProviderError>>,
) -> Option<u16> {
    // A provider 401/403 reaches the loop only as an AuthRequired pause or a wire-error
    // terminal. Completed, limited, interrupted, and infrastructure-error outcomes cannot
    // carry a rejected-credential status, so do not read their terminal error.
    if !should_check_wire_auth(outcome) {
        return None;
    }
    // Some(None) means routing already read an error-free terminal. Do not replay it again.
    if let Some(error) = cached_error {
        return error.as_ref().and_then(wire_auth_status);
    }
    terminal_status.await
}

/// The HTTP status of a run's terminal AUTH error, but ONLY for an Auth-class 401 or 403 —
/// the statuses that can mean a credential the provider rejected (so the vault should
/// invalidate it). `terminal_error` reads the error of the session's last terminal from its
/// durable storage: an Auth-class failure pauses the run, so the status lives on the
/// terminal pause's error (or a finish's, if a future path finalizes instead of pausing).
/// Returns `None` for any other terminal, including a Permanent 403, so the caller reports
/// ONLY a genuine wire auth rejection, never a forbidden or transient error.
async fn terminal_wire_auth_status(
    terminal_error: impl std::future::Future<Output = Option<ProviderError>>,
) -> Option<u16> {
    terminal_error.await.as_ref().and_then(wire_auth_status)
}

/// Share the durable terminal's error between routing and vault reporting. API-key auth
/// pauses block the rejected key without ending the paused run or releasing its admission.
/// `terminal_error` reads the error from the session's storage and is awaited only when the
/// outcome can carry a terminal transient or refusal worth reporting.
async fn report_routed_terminal_error(
    reporter: &broca_module_serve::account_routing::RunReporter,
    selection: Option<&broca_core::AuthSelection>,
    outcome: &Result<RunSummary, RunError>,
    terminal_error: impl std::future::Future<Output = Option<ProviderError>>,
    terminal_seq: u64,
) -> Option<Option<ProviderError>> {
    let selection = selection.filter(|selection| {
        selection.account_decision.as_ref().is_some_and(|decision| {
            decision.path == broca_records::record::ACCOUNT_DECISION_ROUTED
                && decision.admission_id.is_some()
        })
    })?;
    let summary = outcome.as_ref().ok()?;
    let apikey = selection.auth_method.as_deref() == Some("apikey");
    let finished_error = summary.paused.is_none() && summary.reason == RunFinishReason::Error;
    if !(finished_error || (apikey && summary.paused == Some(PauseReason::AuthRequired))) {
        return None;
    }
    let error = terminal_error.await;
    if let Some(error) = &error {
        reporter.terminal_credential_refused(
            terminal_seq,
            error,
            selection.auth_method.as_deref(),
            Some(&selection.credential_id),
        );
        if finished_error {
            reporter.terminal_transient(terminal_seq, error);
        }
    }
    Some(error)
}

/// The reportable wire-auth status of a terminal error: an Auth-class `401` or `403` only.
/// A Permanent 403 is an authorization denial, not a dead credential, and must not be
/// reported to the vault. Pure, so the class and status filter is unit-tested without log
/// plumbing.
fn wire_auth_status(err: &ProviderError) -> Option<u16> {
    if err.class != ErrorClass::Auth {
        return None;
    }
    match err.status {
        Some(s @ (401 | 403)) => Some(s),
        _ => None,
    }
}

struct PriorRunView {
    run_id: broca_protocol::RunId,
    config: broca_records::record::RunConfig,
    idle: bool,
    /// The run moved to another account at a quota wall: its config names the account it
    /// moved to. Only a run in the session store moves.
    moved: bool,
    /// The run is paused where its resume can durably record a change of account: for a
    /// sign-in or for a restart. Its resume writes the account and the config that names it
    /// before it calls the provider. Any other resume continues on the account it recorded.
    records_account_change: bool,
}

/// What a run's admission takes from its job.
struct EpisodeJob<'a> {
    req: &'a broca_core::RunRequest,
    kind: RunKind,
    auth: &'a Option<broca_protocol::AuthRequest>,
    /// A queued send's saved conversation key, which outranks the session's.
    conversation_key: &'a Option<String>,
    /// A queued send's service tier level; routing may choose a different one.
    queued_service_tier: &'a mut Option<String>,
    media_profile: &'a Option<Box<broca_provider::media_profile::MediaProfile>>,
    clock: Option<&'a broca_core::Clock>,
    /// For a run moving to another account at a quota wall: the account routing offered,
    /// which the run is admitted to in place of asking routing (see `AccountMoves`).
    continue_offer: Option<&'a AccountChoice>,
    /// Whether routing serves continuation (see `resume_asks_routing`).
    routing_serves_continuation: bool,
    /// The session's first run since it moved here (see `RunControl::moved_here`).
    moved_here: bool,
}

/// What a run's admission reads from the session's history.
struct EpisodeHistory<'a> {
    /// The session's latest run, unless the job appends a new episode.
    prior_run: Option<PriorRunView>,
    /// The latest run's config, for an appended episode too.
    last_config: Option<&'a broca_records::record::RunConfig>,
    /// The session's durable head: 0 when it holds nothing yet.
    head_seq: u64,
    /// Where a fresh Claude Code persona reads the session's earlier prompts from, when the
    /// latest run recorded neither its `cc_version` suffix nor its prompt ordinal.
    persona: PersonaHistory,
}

/// The session history a fresh Claude Code persona falls back on: what the session already
/// holds in its folds.
struct PersonaHistory {
    /// The session's first user message, when it is still in the open window. `None` with
    /// `first_user_sealed` means it's in sealed history, which a run can't read; the suffix
    /// is then left unpinned.
    first_user: Option<broca_protocol::content::Message>,
    first_user_sealed: bool,
    /// The human prompts the session's runs started with (the conversation fold's
    /// `human_prompts`).
    human_prompts: u64,
}

/// What [`LiveLoopExecutor::admit_episode`] decided.
struct EpisodeAdmission {
    fresh_episode: bool,
    episode_context_management: broca_protocol::request::ContextManagement,
    resolution: AuthResolution,
    account_choice: AccountChoice,
    /// Routing refused the caller's decision; the run pauses on it without a model call.
    routing_decision_error: Option<ProviderError>,
    resolved: broca_catalog::live::ProviderResolution,
    /// The catalog provider whose rows the run borrowed, when its model's provider is one
    /// the catalog doesn't carry (`RunConfig::served_provider`).
    served_provider: Option<String>,
}

/// The prompt ordinal a fresh persona freezes: the session's earlier prompt count plus one
/// when the new input opens a prompt. A new user send opens one prompt, even if its input
/// contains several messages.
fn prompt_ordinal_after(req: &broca_core::RunRequest, count: u64) -> Result<u64, RunError> {
    let admitted =
        u64::from(broca_module_serve::claude_code_identity::human_prompts(&req.input) > 0);
    count
        .checked_add(admitted)
        .ok_or_else(|| RunError::Config("Claude Code prompt ordinal overflow".into()))
}

/// The prompt ordinal the latest run's persona recorded, if it had one.
fn recorded_prompt_ordinal(last_frozen: Option<&serde_json::Value>) -> Option<u64> {
    last_frozen
        .and_then(|config| config.pointer("/claude_code_persona/prompt_ordinal"))
        .and_then(serde_json::Value::as_u64)
}

/// The prompt ordinal a fresh persona freezes for a session whose conversation fold counts
/// its human prompts: the latest run's recorded ordinal, else that count.
fn prompt_ordinal_from(
    req: &broca_core::RunRequest,
    last_frozen: Option<&serde_json::Value>,
    human_prompts: u64,
) -> Result<u64, RunError> {
    prompt_ordinal_after(
        req,
        recorded_prompt_ordinal(last_frozen).unwrap_or(human_prompts),
    )
}

/// The `cc_version` suffix pinned for a new run's session: the latest run's suffix, else one
/// computed from the session's first user message. When that message is in sealed history,
/// which a run can't read, the suffix is left unpinned and the renderer computes it from
/// each request.
fn store_cc_version_suffix(
    req: &broca_core::RunRequest,
    last_frozen: Option<&serde_json::Value>,
    first_user: Option<&broca_protocol::content::Message>,
    first_user_sealed: bool,
) -> Option<String> {
    use broca_module_serve::claude_code_identity::{
        first_durable_cc_version_suffix, inherited_cc_version_suffix,
    };
    if let Some(inherited) = inherited_cc_version_suffix(last_frozen) {
        return Some(inherited);
    }
    if first_user_sealed {
        tracing::warn!(
            session = %req.session.session,
            "the session's first user message is sealed; its Claude Code cc_version suffix is not pinned"
        );
        return None;
    }
    first_durable_cc_version_suffix(first_user.cloned().as_slice(), &req.input)
}

/// The loop's seams for a run kept in the session store, from the run's controls.
#[allow(clippy::too_many_arguments)]
fn store_seams<'a, P: broca_core::provider::Provider>(
    provider: &'a P,
    tools: &'a SubcToolPlane,
    req: &broca_core::RunRequest,
    out: &'a mpsc::Sender<OutputEvent>,
    control: &broca_core::run::RunControl,
    held: Option<&'a dyn broca_core::tool_plane::ProviderHeldLifecycle>,
    compaction_plane: Option<&'a dyn broca_core::CompactionPlane>,
    now: &'a (dyn Fn() -> u64 + Sync),
    account_reports: Option<Arc<dyn broca_session_loop::AccountReports>>,
    auth_selection: Option<broca_core::AuthSelection>,
    max_same_account_wait: broca_core::SameAccountWait,
) -> broca_session_loop::Seams<'a, P, SubcToolPlane> {
    broca_session_loop::Seams {
        person_steers: control.person_steers.clone(),
        provider,
        tools,
        bind: req.session.clone(),
        out: Some(out),
        cancel: control.cancel.clone().unwrap_or_default(),
        // Keep tool waits alive during grace so eligible calls can hand back a held reply.
        stop: control.stop_requested.clone().unwrap_or_default(),
        held,
        approval: &AllowAll,
        hook_plane: control.hook_plane.clone(),
        now_ms: now,
        session_address: Some(broca_core::session_addr(&req.session)),
        compaction_plane,
        // The session's actor keeps its compaction state for the session's life; without it,
        // a run restores the state from the session's records.
        compaction_session: control.compaction_session.clone(),
        // Every run tells the session's compaction provider about changes to the session outside
        // its steps (`RunControl::compaction_changes`).
        compaction_changes: true,
        // A run's keep-warm, which the session's actor builds for a run whose keep-warm is
        // `auto`, is set by the caller.
        keep_warm: None,
        account_reports,
        max_same_account_wait,
        auth_selection,
        // The actor's in-flight step, which `run.status` serves for its active run.
        step_progress: control.step_progress.clone(),
        model_call_ready: None,
        pause_for_auth_at_start: control.pause_for_auth_at_start.clone(),
        account_continue: None,
    }
}

/// What a store run's admission reads from the session: its latest run and that run's
/// config, rebuilt from the run's start record and setup parts, the persona's history and
/// the seq of its last record. The caller builds an [`EpisodeHistory`] from it.
struct StoreHistory {
    prior_run: Option<PriorRunView>,
    persona: PersonaHistory,
    head_seq: u64,
}

/// The config a store session's latest run is read with for the next admission.
///
/// An unfinished run is resumed under what it froze, refreezes included: in particular a
/// run that moved to another account at a quota wall resumes on the account it moved to,
/// with that account's render config. A resume never asks routing (only a fresh run does,
/// see `choose_account_for_episode`), and routing would today answer a moved run's admission
/// with its original decision, the account at its wall; so the recorded selection is the
/// one to resume on. A finished run is read from its start, as before, because a fresh
/// run inherits from it.
fn store_prior_config(
    run: &broca_session_store::fold::operations::Run,
    start: &broca_session_store::record::RunStart,
    parts: &dyn broca_session_loop::parts::Parts,
) -> Result<broca_records::record::RunConfig, broca_session_loop::LoopError> {
    if run.finished.is_none() {
        broca_session_loop::run::run_config_of_run(run, parts)
    } else {
        broca_session_loop::run::run_config_of_start(start, parts)
    }
}

/// Whether a resumed run asks routing for its account again. A run that never moved resumes
/// on the account it froze without asking, as every resume does. A run that moved to another
/// account at a quota wall asks only when routing serves continuation: then routing answers a
/// moved run's repeated admission with the run's current account. Any other router answers
/// with the run's original decision, the account at its wall, so the run keeps the account
/// from its own record. `routing_serves_continuation` is true only when routing declares
/// `account_continue_resume` and `account_continue`; unknown counts as false.
///
/// Only a resume that can record a change of account asks (`records_account_change`: a
/// sign-in or restart pause), since a resume that sent on another account without recording
/// it would bill and report the old one. A run waiting on a held tool call, a plain resume
/// after a crash, and a timed pause on its account's limit (which may only continue on its
/// own account) keep the account they recorded.
fn resume_asks_routing(
    moved: bool,
    records_account_change: bool,
    routing_serves_continuation: bool,
) -> bool {
    moved && records_account_change && routing_serves_continuation
}

/// What a resume that asked routing for its account goes on with. Only an account routing
/// chose (`AccountChoice::Routed`) is a usable answer. Anything else (no answer in the
/// budget, routing unreachable, a hold, a refusal, a conflict, every candidate needing a
/// sign-in, or a static pick when selection is off or one candidate is listed) leaves a moved
/// run's resume on the account and decision it recorded, as a resume that doesn't ask:
/// when broca can't tell which account routing holds the run on, it keeps the one it wrote
/// down. The usual sign-in and account checks then apply to that account. `resume_asks` is
/// cleared then, so nothing after this follows an account routing didn't give. A choice no
/// resume asked for passes unchanged.
fn resumed_choice(
    chosen: Result<AccountChoice, RunError>,
    resume_asks: &mut bool,
    run_id: &broca_protocol::RunId,
) -> Result<AccountChoice, RunError> {
    match chosen {
        Ok(routed @ AccountChoice::Routed { .. }) => Ok(routed),
        unusable if *resume_asks => {
            let answer = match &unusable {
                Ok(AccountChoice::AuthRequired(_)) => "every candidate needs a sign-in".into(),
                Ok(AccountChoice::Static(_)) => "a static pick, not routing's choice".into(),
                Ok(_) => "no choice".into(),
                Err(error) => error.to_string(),
            };
            tracing::warn!(
                target: "accounts",
                run_id = %run_id.0,
                answer = %answer,
                "routing gave the resumed run no account; it keeps the account it recorded"
            );
            *resume_asks = false;
            Ok(AccountChoice::NotApplicable)
        }
        chosen => chosen,
    }
}

async fn store_history(
    writer: &broca_session_loop::shared::ActorWriter,
    parts: &dyn broca_session_loop::parts::Parts,
) -> Result<StoreHistory, broca_session_loop::LoopError> {
    use broca_session_loop::writer::Writer as _;
    // A refreeze of the account marks a move at a quota wall only when no resume wrote it
    // (see `moved_at_a_wall`). An unfinished run's records are all in the open log; one that
    // can't be read counts as no move, so the run keeps the account it recorded.
    let moved = match writer.operations().run.as_ref() {
        Some(run) if run.refrozen_auth.is_some() && run.finished.is_none() => {
            writer.open_log_records().await.is_ok_and(|records| {
                broca_session_loop::account_move::moved_at_a_wall(&run.run_id, &records)
            })
        }
        _ => false,
    };
    let prior_run = match writer.operations().run.as_ref() {
        Some(run) => {
            let start = run.start.as_ref().ok_or_else(|| {
                broca_session_loop::LoopError::Parts(format!(
                    "the run {} has no start in the session",
                    run.run_id
                ))
            })?;
            let config = store_prior_config(run, start, parts)?;
            // The session's record of a schema refusal outlives the process memo of refusals.
            broca_provider::structured_output::remember_session_refusal(
                &config.frozen_render_config,
                run.structured_downgraded,
            );
            Some(PriorRunView {
                run_id: broca_protocol::RunId(run.run_id.clone()),
                config,
                idle: run.finished.is_some(),
                moved,
                records_account_change:
                    broca_session_loop::account_move::resume_can_record_account_change(run),
            })
        }
        None => None,
    };
    let conversation = writer.conversation();
    // The session's first run is the first of its sealed runs, if any were sealed.
    let first_run = conversation
        .sealed_episodes
        .iter()
        .chain(conversation.episodes.iter())
        .find(|episode| episode.first_user.is_some());
    let (first_user, first_user_sealed) = match first_run.and_then(|episode| episode.first_user) {
        None => (None, false),
        // In the window, read back whole if its content was released.
        Some(ordinal) if ordinal >= conversation.window_start() => {
            let message = writer
                .window_message_whole(ordinal)
                .await
                .map_err(broca_session_loop::LoopError::Session)?;
            (
                Some(broca_protocol::content::Message::clone(&message)),
                false,
            )
        }
        Some(_) => (None, true),
    };
    Ok(StoreHistory {
        prior_run,
        persona: PersonaHistory {
            first_user,
            first_user_sealed,
            human_prompts: conversation.human_prompts,
        },
        head_seq: writer.next_seq().saturating_sub(1),
    })
}

/// This execution's newest run-end or pause error, never a previous invocation's pause.
/// A run's end is always in the open log:
/// sealing never moves an unfinished run's records, and a run's end is written before any
/// later seal.
async fn store_terminal_error(
    writer: &broca_session_loop::shared::ActorWriter,
    execution_start_seq: u64,
) -> Option<ProviderError> {
    use broca_session_loop::writer::Writer as _;
    use broca_session_store::record::Record;
    let records = writer.open_log_records().await.ok()?;
    records
        .iter()
        .rev()
        .take_while(|(seq, _)| *seq >= execution_start_seq)
        .find_map(|(_, record)| match record {
            Record::RunEnd(end) => Some(end.error.clone()),
            Record::Pause(pause) => Some(pause.error.clone()),
            _ => None,
        })?
        .and_then(|error| serde_json::from_value(error.parse()).ok())
}

type CredentialUseKey = (String, Option<String>, Option<String>);

/// Versions prepared for this execution are not used until an attempt records them.
#[derive(Default)]
struct SentCredentials {
    versions: HashMap<CredentialUseKey, u64>,
    last: Option<(broca_core::AuthSelection, u64)>,
}

impl SentCredentials {
    fn key(selection: &broca_core::AuthSelection) -> CredentialUseKey {
        (
            selection.credential_id.clone(),
            selection.account_ref.clone(),
            selection
                .account_decision
                .as_ref()
                .and_then(|decision| decision.decision_id.clone()),
        )
    }
    fn prepared(&mut self, selection: Option<&broca_core::AuthSelection>, version: Option<u64>) {
        if let (Some(selection), Some(version)) = (selection, version) {
            self.versions.insert(Self::key(selection), version);
        }
    }
    fn attempted(
        &mut self,
        selection: Option<&broca_core::AuthSelection>,
        attempt: &broca_protocol::ModelAttempt,
    ) {
        let Some(selection) = selection else {
            return;
        };
        let key = Self::key(selection);
        let version = attempt
            .credential_refresh
            .as_ref()
            .and_then(|refresh| refresh.record_version)
            .or_else(|| self.versions.get(&key).copied());
        if let Some(version) = version {
            self.versions.insert(key, version);
            self.last = Some((selection.clone(), version));
        }
    }
}

/// Forward routing observations and retain the credential version of actual attempts.
struct StoreAccountReports {
    routing: Option<Arc<broca_module_serve::account_routing::RunReporter>>,
    sent: Arc<std::sync::Mutex<SentCredentials>>,
}

impl broca_session_loop::AccountReports for StoreAccountReports {
    fn run_started(&self, _event_id: &str, seq: u64) {
        if let Some(reporter) = &self.routing {
            broca_core::AccountReporter::run_started(&**reporter, seq);
        }
    }

    fn attempt_recorded(&self, _event_id: &str, seq: u64, attempt: &broca_protocol::ModelAttempt) {
        if let Some(reporter) = &self.routing {
            broca_core::AccountReporter::attempt_recorded(&**reporter, seq, attempt);
        }
    }

    fn attempt_recorded_on(
        &self,
        _event_id: &str,
        seq: u64,
        attempt: &broca_protocol::ModelAttempt,
        selection: Option<&broca_core::AuthSelection>,
    ) {
        self.sent.lock().unwrap().attempted(selection, attempt);
        if let Some(reporter) = &self.routing {
            reporter.attempt_recorded_on(seq, attempt, selection);
        }
    }

    fn account_moved(&self, _event_id: &str, seq: u64, selection: &broca_core::AuthSelection) {
        if let Some(reporter) = &self.routing {
            reporter.moved(seq, selection);
        }
    }
}

/// What a run that moved reports at its end in place of what it was admitted with: the
/// credential it sends on now and the token version and refresher the vault served for it.
struct MovedCredential {
    selection: broca_core::AuthSelection,
    record_version: Option<u64>,
    in_run_refresh: Option<InRunRefresh>,
}

/// A store run's moves to another account at a quota wall (see
/// `broca_session_loop::account_move`). It asks routing with `mode: "continue"`, and admits
/// the offered account the way a resume of the run on that account would be admitted: the
/// offered credential read from the vault and checked to belong to the offered account, and
/// a provider built from the run's recorded render config, with the Claude Code persona, when
/// the config has one, refrozen for the new account.
struct AccountMoves<'a> {
    executor: &'a LiveLoopExecutor,
    req: &'a broca_core::RunRequest,
    clock: Option<&'a broca_core::Clock>,
    /// The media renderer the run's provider was given, which the new provider needs too.
    media: Option<Arc<broca_module::store_media::LazyRenderer>>,
    /// The end of each wall's budget, by the refused attempt's seq: a second question at the
    /// same wall shares the first one's budget.
    deadlines: std::sync::Mutex<HashMap<u64, tokio::time::Instant>>,
    /// The credentials of moves prepared but not yet durable, and of the newest durable one.
    prepared: std::sync::Mutex<Vec<MovedCredential>>,
    adopted: std::sync::Mutex<Option<MovedCredential>>,
    recheck_sign_in: bool,
    probe_only: bool,
    sent: Option<Arc<std::sync::Mutex<SentCredentials>>>,
}

impl<'a> AccountMoves<'a> {
    fn new(
        executor: &'a LiveLoopExecutor,
        req: &'a broca_core::RunRequest,
        clock: Option<&'a broca_core::Clock>,
        media: Option<Arc<broca_module::store_media::LazyRenderer>>,
    ) -> Self {
        Self {
            executor,
            req,
            clock,
            media,
            deadlines: Default::default(),
            prepared: Default::default(),
            adopted: Default::default(),
            recheck_sign_in: true,
            probe_only: false,
            sent: None,
        }
    }

    /// The credential of the run's newest durable move, if it moved.
    fn moved(&self) -> Option<MovedCredential> {
        self.adopted.lock().unwrap().take()
    }

    /// Routing's answer at `wall`, read as the loop acts on it, and the offer if there is
    /// one to admit.
    async fn offer(
        &self,
        wall: &broca_session_loop::account_move::Wall<'_>,
    ) -> Result<
        cortexkit_role_routing::account_continue::ContinueOffer,
        broca_session_loop::account_move::Answer<Box<dyn broca_core::provider::Provider>>,
    > {
        use broca_module_serve::account_routing::{ContinueOutcome, CONTINUE_BUDGET};
        use broca_session_loop::account_move::Answer;
        use cortexkit_role_routing::account_continue::ContinueAnswer;
        let Some(routing) = self.executor.routing.as_ref() else {
            return Err(Answer::Ignored);
        };
        let Some(request) = broca_module_serve::account_routing::continue_request(wall) else {
            tracing::warn!(
                target: "accounts",
                run_id = wall.run_id,
                "the run's selection lacks the admission, conversation key or decision \
                 routing knows it by, or no fresh selection id could be made; continuation was not sent"
            );
            return Err(Answer::Ignored);
        };
        let deadline = *self
            .deadlines
            .lock()
            .unwrap()
            .entry(wall.attempt_seq)
            .or_insert_with(|| tokio::time::Instant::now() + CONTINUE_BUDGET);
        let outcome = routing.account_continue_if_served(&request, deadline).await;
        if wall.limit_window == "signed_out"
            && !matches!(
                &outcome,
                ContinueOutcome::NotAsked | ContinueOutcome::NotDeclared
            )
        {
            tracing::info!(target: "accounts", run_id = wall.run_id, "routing sign-in continuation exchange completed");
        }
        let answer = match outcome {
            ContinueOutcome::Answered(answer) => answer,
            ContinueOutcome::NotDeclared | ContinueOutcome::NotAsked => {
                return Err(Answer::Ignored)
            }
            outcome => {
                tracing::warn!(
                    target: "accounts",
                    run_id = wall.run_id,
                    ?outcome,
                    limit_window = wall.limit_window,
                    "routing gave no answer to the run's account-continuation request"
                );
                return Err(Answer::Stay(None));
            }
        };
        // Routing's answer travels on the run's error, which only the caller reads; the log
        // keeps it beside the run for an operator.
        match answer {
            ContinueAnswer::Offer(offer) => Ok(offer),
            ContinueAnswer::Hold(hold) => {
                tracing::info!(
                    target: "accounts",
                    run_id = wall.run_id,
                    routing_code = %hold.routing_code,
                    until_ms = ?hold.until_ms,
                    limit_window = wall.limit_window,
                    "routing held the run's account-continuation request"
                );
                Err(Answer::Hold(broca_protocol::error::AccountContinueAnswer {
                    routing_code: hold.routing_code,
                    until_ms: hold.until_ms,
                }))
            }
            ContinueAnswer::Refuse(refusal) => {
                tracing::warn!(
                    target: "accounts",
                    run_id = wall.run_id,
                    routing_code = %refusal.routing_code,
                    limit_window = wall.limit_window,
                    "routing refused the run's account-continuation request"
                );
                Err(Answer::Stay(Some(
                    broca_protocol::error::AccountContinueAnswer {
                        routing_code: refusal.routing_code,
                        until_ms: None,
                    },
                )))
            }
            other => {
                tracing::warn!(
                    target: "accounts",
                    run_id = wall.run_id,
                    answer = ?other,
                    "routing answered the run's account-continuation request with nothing \
                     broca can act on"
                );
                Err(Answer::Ignored)
            }
        }
    }

    /// The run's recorded config with the offered account in place of the walled one: the
    /// selection names the offered credential and account under the offer's decision, and a
    /// Claude Code persona, which the request body carries, is refrozen for that account.
    /// The persona keeps its version, session id, pinned `cc_version` suffix and prompt
    /// ordinal; its device id and account id are the new account's, and its prompt id is
    /// new, as a fresh run on that account would freeze them.
    fn moved_config(
        &self,
        wall: &broca_session_loop::account_move::Wall<'_>,
        offer: &cortexkit_role_routing::account_continue::ContinueOffer,
        decision: &broca_core::AccountDecision,
    ) -> Result<broca_records::record::RunConfig, String> {
        let mut config = wall.run_config.clone();
        config.auth_selection = Some(broca_core::AuthSelection {
            credential_id: offer.credential_id.clone(),
            account_ref: Some(offer.account.clone()),
            resolved_credential_id: Some(offer.credential_id.clone()),
            vault_request: None,
            account_decision: Some(decision.clone()),
            auth_method: wall.selection.auth_method.clone(),
            billing_vault_facts: None,
        });
        let recorded = config
            .frozen_render_config
            .get("claude_code_persona")
            .filter(|persona| !persona.is_null())
            .cloned();
        if let Some(recorded) = recorded {
            let persona: broca_provider::wire::ClaudeCodePersona = serde_json::from_value(recorded)
                .map_err(|error| format!("the recorded persona doesn't decode: {error}"))?;
            let install_id = self
                .executor
                .claude_code_install_id
                .as_deref()
                .ok_or("no installation id to derive the new account's persona from")?;
            let persona = broca_module_serve::claude_code_identity::moved_persona(
                &persona,
                install_id,
                Some(&offer.account),
                &offer.credential_id,
                &self.req.session,
            )
            .map_err(|error| format!("the new account's persona: {error}"))?;
            refreeze_persona(
                &mut config.frozen_render_config,
                serde_json::to_value(&persona)
                    .map_err(|error| format!("the new account's persona: {error}"))?,
            );
        }
        Ok(config)
    }
}

#[async_trait]
impl broca_session_loop::account_move::AccountContinue<Box<dyn broca_core::provider::Provider>>
    for AccountMoves<'_>
{
    async fn ask(
        &self,
        wall: &broca_session_loop::account_move::Wall<'_>,
    ) -> broca_session_loop::account_move::Answer<Box<dyn broca_core::provider::Provider>> {
        use broca_session_loop::account_move::{Answer, Move};
        let offer = match self.offer(wall).await {
            Ok(offer) => offer,
            Err(answer) => return answer,
        };
        let refuse = |reason: &str| {
            tracing::warn!(
                target: "accounts",
                run_id = wall.run_id,
                step_id = wall.step_id,
                offered = %offer.account,
                limit_window = wall.limit_window,
                "routing offered an account for continuation, and the run does not take it: \
                 {reason}"
            );
            Answer::Ignored
        };
        // The loop checks the reservation just before it writes the move, and asks again
        // once if it lapsed; an account that refused this step is never offered usefully.
        if wall.refused_accounts.contains(&offer.account) {
            return refuse("the offered account already refused this step");
        }
        let method = wall
            .selection
            .auth_method
            .as_deref()
            .and_then(AuthMethod::from_token)
            .or_else(|| {
                broca_module_serve::auth_routing::CredentialId::parse(&wall.selection.credential_id)
                    .map(|id| id.method)
            });
        let (Some(method), Some(previous)) = (method, wall.selection.account_decision.as_ref())
        else {
            return refuse("the run's credential names no auth method");
        };
        let Some(vault) = self.executor.vault.as_ref() else {
            return refuse("the offered credential's vault metadata is unavailable");
        };
        // Credential ids encode <method>:<provider>:<label>; changing a row's type is an
        // operator edit bounded by the listing refresh, not ordinary token renewal.
        // Use the last-known listing while its normal background refresh runs; refuse
        // without a listing or compatible row. Provider-unmapped API keys cannot move.
        let Some(listing) = latest_listing(Arc::clone(vault)).await else {
            return refuse("the offered credential's vault metadata is unavailable");
        };
        let provider = wall.run_config.model.provider_module_id.as_str();
        if !listing.credentials.iter().any(|row| {
            row.id == offer.credential_id
                && row.auth_method.as_deref() == Some(method.as_token())
                && row.provider_ids.iter().any(|id| id == provider)
        }) {
            return refuse(
                "the offered credential's provider or auth method does not match the run",
            );
        }
        let decision = broca_module_serve::account_routing::routed_decision(
            &broca_core::AccountDecision {
                decision_id: Some(offer.decision_id.clone()),
                ..previous.clone()
            },
            &offer.account,
            &offer.credential_id,
        );
        let config = match self.moved_config(wall, &offer, &decision) {
            Ok(config) => config,
            Err(error) => return refuse(&error),
        };
        let choice = AccountChoice::Routed {
            account_id: offer.account.clone(),
            credential_id: offer.credential_id.clone(),
            method,
            decision,
            // The listing digest of the run's first choice isn't kept; routing names the
            // offer by its decision instead.
            view_digest: String::new(),
            service_tier_chosen: None,
        };
        let history = EpisodeHistory {
            prior_run: Some(PriorRunView {
                run_id: self.req.run_id.clone(),
                config: config.clone(),
                idle: false,
                moved: true,
                records_account_change: false,
            }),
            last_config: Some(&config),
            head_seq: 0,
            persona: PersonaHistory {
                first_user: None,
                first_user_sealed: false,
                human_prompts: 0,
            },
        };
        // A fresh run's admission moved the send's model out of its request, so the move
        // re-admits the run with the model its config froze.
        let req = broca_core::RunRequest {
            model: wall.run_config.model.clone(),
            ..self.req.clone()
        };
        let mut queued_service_tier = None;
        let admitted = self
            .executor
            .admit_episode(
                EpisodeJob {
                    req: &req,
                    kind: RunKind::FreshOrResume,
                    // The offer authorizes this account; the fresh send's auth hints
                    // are admission-only, not a new pin on this resumed episode.
                    auth: &None,
                    conversation_key: &None,
                    queued_service_tier: &mut queued_service_tier,
                    media_profile: &None,
                    clock: self.clock,
                    // Only a run in the session store moves.
                    continue_offer: Some(&choice),
                    // The offer is the account: the moved config already names it, so
                    // the resume rule that would ask routing again doesn't apply.
                    routing_serves_continuation: false,
                    moved_here: false,
                },
                &history,
            )
            .await;
        // The vault refusing the credential, or serving it for another account than the
        // offer names, ends here: nothing is sent on an account routing didn't offer.
        let admitted = match admitted {
            Ok(admitted) => admitted,
            Err(error) => return refuse(&format!("its credential wasn't admitted: {error}")),
        };
        let selection = match admitted.resolution.selection.clone() {
            Some(selection)
                if !admitted.fresh_episode
                    && admitted.resolution.pause.is_none()
                    && admitted.routing_decision_error.is_none()
                    && selection.account_ref.as_deref() == Some(offer.account.as_str()) =>
            {
                selection
            }
            _ => return refuse("its credential isn't usable for the offered account"),
        };
        let provider = admitted.resolved.provider;
        provider.set_chatgpt_https_policy(broca_module_serve::transport_config::default_policy());
        if let Some(media) = &self.media {
            provider.set_media_request_renderer(media.renderer());
        }
        let blob = provider.render_config_blob();
        let renderer_version = provider.renderer_version();
        let render = (blob != wall.run_config.frozen_render_config
            || renderer_version != wall.run_config.renderer_version)
            .then_some((renderer_version, blob));
        self.prepared.lock().unwrap().push(MovedCredential {
            selection: selection.clone(),
            record_version: admitted.resolution.record_version,
            in_run_refresh: admitted.resolution.in_run_refresh,
        });
        Answer::Move(Box::new(Move {
            selection,
            account: offer.account,
            valid_until_ms: offer.valid_until_ms,
            render,
            provider,
        }))
    }

    fn adopted(&self, selection: &broca_core::AuthSelection) {
        let mut prepared = self.prepared.lock().unwrap();
        if let Some(position) = prepared
            .iter()
            .position(|moved| moved.selection == *selection)
        {
            let moved = prepared.swap_remove(position);
            if let Some(sent) = &self.sent {
                sent.lock()
                    .unwrap()
                    .prepared(Some(selection), moved.record_version);
            }
            *self.adopted.lock().unwrap() = Some(moved);
        }
        prepared.clear();
    }

    fn recheck_sign_in(&self) -> bool {
        self.recheck_sign_in
    }
    fn probe_only(&self) -> bool {
        self.probe_only
    }
}

fn frozen_render_config(
    config: &broca_records::record::RunConfig,
) -> Option<broca_catalog::live::FrozenRenderConfig> {
    serde_json::from_value(config.frozen_render_config.clone()).ok()
}

fn frozen_cache_policy(config: &broca_records::record::RunConfig) -> Option<CachePolicy> {
    frozen_render_config(config).and_then(|frozen| frozen.cache)
}

fn frozen_prompt_cache_key(config: &broca_records::record::RunConfig) -> Option<String> {
    frozen_render_config(config).and_then(|frozen| frozen.prompt_cache_key)
}

/// The cache policy and caller cache key an episode takes as frozen instead of resolving its
/// own. `Some` for a new episode (not a resume) whose request has `inherit_frozen_cache` set,
/// which a steer or interrupt that omitted `cache` gets: the values come from `last`, the
/// config of the session's latest episode. `None` otherwise, including when the session has
/// no episode yet.
fn inherited_cache(
    req: &broca_core::RunRequest,
    fresh_episode: bool,
    last: Option<&broca_records::record::RunConfig>,
) -> Option<(Option<CachePolicy>, Option<String>)> {
    if !(req.inherit_frozen_cache && fresh_episode) {
        return None;
    }
    let config = last?;
    Some((frozen_cache_policy(config), frozen_prompt_cache_key(config)))
}

/// Decode the whole admission bundle so upstream routing changes cannot move a resumed run.
/// Refreezes `frozen`'s Claude Code persona for the account `selection` names, when that
/// differs from `previous_account`, the account the run last ran on (see
/// `claude_code_identity::moved_persona`). A config without a persona, or a selection on the
/// same account, is left as it is.
fn persona_for_resumed_account(
    frozen: &mut broca_provider::wire::FrozenRenderConfig,
    install_id: Option<&str>,
    previous_account: Option<&str>,
    selection: Option<&broca_core::AuthSelection>,
    session: &broca_protocol::BindIdentity,
) -> Result<(), String> {
    let (Some(persona), Some(selection)) = (frozen.claude_code_persona.as_ref(), selection) else {
        return Ok(());
    };
    let account = selection.account_ref.as_deref();
    if account.is_none() || account == previous_account {
        return Ok(());
    }
    let install_id =
        install_id.ok_or("no installation id to derive the new account's persona from")?;
    let moved = broca_module_serve::claude_code_identity::moved_persona(
        persona,
        install_id,
        account,
        &selection.credential_id,
        session,
    )
    .map_err(|error| format!("the resumed account's persona: {error}"))?;
    // The refusal-fallback model's config carries the same persona as the source, so a step
    // re-sent there after the change names the new account too.
    if let Some(fallback) = frozen.refusal_fallback.as_mut() {
        if fallback.frozen.claude_code_persona.is_some() {
            fallback.frozen.claude_code_persona = Some(moved.clone());
        }
    }
    frozen.claude_code_persona = Some(moved);
    Ok(())
}

/// Puts `persona`, refrozen for another account, in a recorded render config: the config's
/// own `claude_code_persona`, and its refusal-fallback model's
/// (`refusal_fallback.frozen.claude_code_persona`) when that carries one, since the fallback
/// renders with the same persona as the source. Every other byte of the config is kept.
fn refreeze_persona(render_config: &mut serde_json::Value, persona: serde_json::Value) {
    const PERSONA: &str = "claude_code_persona";
    if let Some(fallback) = render_config
        .get_mut("refusal_fallback")
        .and_then(|fallback| fallback.get_mut("frozen"))
        .and_then(serde_json::Value::as_object_mut)
        .filter(|fallback| {
            fallback
                .get(PERSONA)
                .is_some_and(|persona| !persona.is_null())
        })
    {
        fallback.insert(PERSONA.into(), persona.clone());
    }
    render_config[PERSONA] = persona;
}

fn decode_recorded_render_config(
    config: &broca_records::record::RunConfig,
) -> Result<broca_provider::wire::FrozenRenderConfig, String> {
    serde_json::from_value(config.frozen_render_config.clone())
        .map_err(|error| format!("recorded provider render config cannot be decoded: {error}"))
}

/// An unreadable bundle has no renderer or transport. Passing this null identity to
/// core preserves its durable, permanent frozen_render_config_unverifiable refusal.
struct UnverifiableRecordedProvider;

#[async_trait]
impl broca_core::Provider for UnverifiableRecordedProvider {
    async fn step(
        &self,
        _: broca_protocol::StepId,
        _: &broca_protocol::CallOptions,
    ) -> mpsc::Receiver<broca_protocol::NormalizedEvent> {
        let (_, rx) = mpsc::channel(1);
        rx
    }
    fn render_request(&self, _: &broca_protocol::CallOptions) -> Vec<u8> {
        Vec::new()
    }
    fn renderer_version(&self) -> u32 {
        0
    }
    fn reproduces_renderer_version(&self, _: u32) -> bool {
        false
    }
    fn render_config_blob(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

fn unverifiable_recorded_resolution(
    model: &str,
    method: Option<AuthMethod>,
    family_override: Option<WireFamilyId>,
) -> broca_catalog::live::ProviderResolution {
    // This family value satisfies the executor's auth-method consistency check only;
    // the null provider cannot render or send a request for any family.
    let family = family_override.unwrap_or(WireFamilyId::OpenAiChat);
    broca_catalog::live::ProviderResolution {
        provider: Box::new(UnverifiableRecordedProvider),
        auth_method: method,
        family_id: family,
        family: format!("{family:?}"),
        base_url: "<unverifiable>".into(),
        wire_model: model.into(),
        auth_desc: "no transport for an unverifiable render config".into(),
        provider_tag: model
            .split_once('/')
            .map_or("openai-compatible", |(provider, _)| provider)
            .into(),
        context_limit: None,
    }
}

fn context_limit_for_episode(
    fresh_episode: bool,
    caller_context_limit: Option<u64>,
    catalog_context_limit: Option<u64>,
    frozen_context_limit: Option<u64>,
) -> Option<u64> {
    if fresh_episode {
        caller_context_limit.or(catalog_context_limit)
    } else {
        frozen_context_limit
    }
}

/// The opened credential-vault consumer plus how each credential is addressed. Present only
/// when the auth source is the vault AND the connection opened at startup; absent for the
/// mock credential-free path. Process-global (credentials are not session-keyed), so one client
/// serves every session and provider.
pub struct VaultAuth {
    pub client: broca_subc::VaultClient,
    /// Per credential key: the vault credential id to read by grant.
    pub addressing: broca_module_serve::VaultAddressing,
    /// Default methods from the same cached rows used for credential addressing.
    pub methods: broca_module_serve::MethodConfig,
    /// The health gauge this executor stamps from vault-call outcomes, so the
    /// `health.check` reply reports live vault reachability without probing anything.
    pub gauge: std::sync::Arc<broca_module_serve::serve::VaultGauge>,
    /// Process-only last-known-good credentials keyed by canonical credential id.
    pub cache: VaultLkgCache,
    /// The latest `credential.list_scoped` answer: the candidate accounts broca offers
    /// routing when it asks which account a run should use.
    pub listing: Arc<broca_module_serve::account_routing::ListingCache>,
}

/// List the vault's credentials now and keep the answer as the latest listing.
pub async fn refresh_listing(vault: &VaultAuth) -> Option<Arc<broca_subc::vault::ScopedList>> {
    match vault.client.list_scoped().await {
        Ok(listing) => Some(vault.listing.store(listing)),
        Err(error) => {
            tracing::warn!(
                target: "accounts",
                "credential.list_scoped failed; account candidates keep the previous listing: {error}"
            );
            None
        }
    }
}

/// Return cached rows immediately, refreshing in the background if absent or stale.
async fn latest_listing(vault: Arc<VaultAuth>) -> Option<Arc<broca_subc::vault::ScopedList>> {
    match vault.listing.get() {
        Some((listing, stale)) => {
            if stale && vault.listing.begin_refresh() {
                tokio::spawn(async move {
                    refresh_listing(&vault).await;
                    vault.listing.end_refresh();
                });
            }
            Some(listing)
        }
        None => {
            if vault.listing.begin_refresh() {
                tokio::spawn(async move {
                    refresh_listing(&vault).await;
                    vault.listing.end_refresh();
                });
            }
            None
        }
    }
}

impl broca_module_serve::vault_audit::VaultAuditor for VaultAuth {
    fn audit(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = broca_module_serve::wire::VaultAuditResponse>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let listing = self.client.list_scoped().await;
            if let Ok(list) = &listing {
                self.listing.store(list.clone());
            }
            let referenced = listing
                .as_ref()
                .map(broca_module_serve::vault_audit::referenced_credentials)
                .unwrap_or_default();
            broca_module_serve::vault_audit::audit_listing(
                self.client.scoped_principal(),
                listing,
                &referenced,
            )
        })
    }
}

/// Report a provider's rejection of a served token to the vault, by the same credential id
/// the read used, under the same grant. A selection whose vault id cannot be named was
/// never read, so there is nothing to report.
async fn report_wire_auth_failure(
    vault: &VaultAuth,
    selection: &broca_core::AuthSelection,
    status: u16,
    record_version: u64,
) -> Result<(), VaultError> {
    let Some(vault_id) = vault
        .addressing
        .vault_id_for_selection(selection)
        .map_err(|error| VaultError::Protocol(error.to_string()))?
    else {
        return Ok(());
    };
    vault
        .client
        .report_auth_failure_at(
            broca_subc::VaultAddress::CredentialId(&vault_id),
            status,
            record_version,
        )
        .await
}

/// The rejection a run that ended on a wire auth error still owes the vault, if any: ONE
/// REPORT PER REJECTION, naming the version the provider actually rejected. `sent` is the
/// version the run's last attempt sent, measured from its recorded attempts, so neither an
/// unused admission token nor a newer token fetched after a rejection can be reported.
/// - When the in-step refresher recorded a rejection of `sent` without reporting it, report
///   exactly that status and version.
/// - When the refresher already reported `sent`, nothing is owed.
/// - Otherwise report `sent`, with the terminal status.
fn run_end_report_target(
    in_run: Option<&InRunRefresh>,
    sent: u64,
    terminal_status: u16,
) -> Option<UnreportedRejection> {
    if let Some(in_run) = in_run {
        if let Some(unreported) = *in_run
            .unreported_rejection
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
        {
            if unreported.record_version == sent {
                return Some(unreported);
            }
        }
        if *in_run
            .reported_version
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            == Some(sent)
        {
            return None;
        }
    }
    Some(UnreportedRejection {
        status: terminal_status,
        record_version: sent,
    })
}

/// After a run whose terminal is a wire auth rejection with `terminal_status`: evict the
/// rejected version from this process's cache and report it to the vault, unless the
/// in-step refresh already did (see [`run_end_report_target`]).
async fn report_rejection_after_run(
    vault: &VaultAuth,
    selection: &broca_core::AuthSelection,
    version: u64,
    in_run: Option<&InRunRefresh>,
    terminal_status: u16,
) {
    let Some(rejection) = run_end_report_target(in_run, version, terminal_status) else {
        return;
    };
    // Provider rejection is authoritative even if the best-effort vault report cannot be
    // delivered; never let this process serve the rejected token again.
    // The cache is keyed by the selection's credential id, exactly as the read stored it.
    evict_for_provider_rejection(
        &vault.cache,
        &selection.credential_id,
        rejection.status,
        rejection.record_version,
    );
    let _ = report_wire_auth_failure(vault, selection, rejection.status, rejection.record_version)
        .await;
}

/// The auth method for a provider with no configured default — the openai-compatible
/// long tail (deepseek/cerebras/...) is api-key, so api-key is the safe fallback.
const FALLBACK_METHOD: broca_module_serve::AuthMethod = broca_module_serve::AuthMethod::ApiKey;

/// Report that THIS RUN pays a marker write premium it cannot itself read back.
///
/// Markers cost tokens to write and repay on a later request that re-sends the same prefix.
/// A run declaring no tools makes exactly one provider call, so nothing later in THIS run
/// can read what it wrote.
///
/// IT DOES NOT FOLLOW THAT THE WRITE IS WASTED, and the earlier wording of this warning said
/// it was. Anthropic's prompt cache is server-side and keyed on the CONTENT PREFIX -- not on
/// a session, a run, or a connection -- so a later run re-sending the same leading bytes hits
/// the cache this run wrote, within its TTL. ALF's spec aggregator is exactly that shape: a
/// fold header re-sent across rounds, each round a separate tool-less run, every round after
/// the first reading the first one's write. The old text told them their intended use was
/// impossible (2026-09-05).
///
/// So the honest scope is one run, and broca cannot see past it: whether a later request
/// re-sends this prefix is the caller's plan, not a fact available here. Report the premium,
/// name the condition that repays it, and let the caller decide.
///
/// This only reports. The cache shape is the caller's decision, and refusing the request
/// here would trade a rare, recoverable waste for a permanent ambiguity about who decides
/// it on a path where byte-stability matters. Hint policies are exempt -- they carry a
/// cache key rather than paid breakpoints, so a single-step run costs nothing extra.
fn single_call_cache_marker_warning(
    policy: &Option<broca_provider::wire::CachePolicy>,
    tools: &[broca_protocol::request::Tool],
) -> Option<String> {
    let policy = policy.as_ref()?;
    if !tools.is_empty() {
        return None;
    }
    let broca_provider::wire::CacheClass::Markers { ttl, omit_ttl, .. } = &policy.class else {
        return None;
    };
    // Only the no-request Anthropic default sets `omit_ttl`. That policy is broca's own
    // choice, applied to every such send, so a warning addressed to a caller about markers
    // they "requested" would be both wrong and on nearly every tool-less send.
    if *omit_ttl {
        return None;
    }
    Some(format!(
        "cache markers requested on a run with no tools (ttl={ttl:?}); this run makes \
             one provider call, so it pays the write premium without reading it back -- expected \
             if a later request re-sends the same prefix within the ttl, wasted if not"
    ))
}

fn warn_single_call_cache_markers(
    policy: &Option<broca_provider::wire::CachePolicy>,
    tools: &[broca_protocol::request::Tool],
) {
    if let Some(message) = single_call_cache_marker_warning(policy, tools) {
        tracing::warn!(target: "cache", "{message}");
    }
}

struct EpisodeAccountPreference<'a> {
    pin: (Option<AuthMethod>, Option<&'a str>),
    pool: Option<&'a str>,
    credential_id: Option<&'a str>,
    /// On a moved session's first run here, the credential its last run used (see
    /// `RunControl::moved_here`).
    warm_credential: Option<&'a str>,
}

/// Drives the real agentic loop for each run: resolves the provider from the catalog
/// (or a base_url override), dispatches tools through the shared `SubcToolPlane`, and
/// threads the admission ack so the coordinator admits a run only once it's durable.
pub struct LiveLoopExecutor {
    /// broca's state root. It keeps the random seed this installation derives its Claude Code
    /// and ChatGPT device ids from, so subscription requests name the same device every run.
    state_root: PathBuf,
    /// The SHARED tool plane (one consumer connection, per-session routes). Shared so
    /// every session's tool dispatch rides one outbound subc connection.
    plane: Arc<SubcToolPlane>,
    /// The actor uses this adapter before accepting sends; core uses the same adapter for
    /// the remaining phases and resumed work against the frozen declarations.
    hooks: Option<Arc<broca_subc::SubcHookPlane>>,
    compaction_plane: Option<Arc<broca_subc::SubcCompactionPlane>>,
    compaction_views: Arc<broca_core::ViewReadyInbox>,
    /// Where provider-held calls wait for their late results; see `held_runtime`.
    provider_held_lifecycle: Option<Arc<broca_module::held_slots::HeldSlots>>,
    /// One process-long HTTP client. Every per-run provider gets a cheap clone so sequential
    /// episodes retain the same DNS, TCP, TLS, and HTTP connection pool.
    provider_client: ProviderHttpClient,
    /// The same clock the retention sweep uses for time-dependent decisions.
    clock: broca_core::clock::Clock,
    /// An optional provider base_url override (the aimock / openai-compatible path).
    /// `None` ⇒ resolve the run's model id through the catalog.
    provider_base_url: Option<String>,
    /// Configured hosts change only the endpoint. Keeping them out of provider_base_url
    /// prevents them from disabling credentials and cache planning like the mock path.
    provider_overrides: broca_module_serve::provider_overrides::ProviderOverrides,
    /// Vault access shared with the provider transport's in-step token refresher.
    /// Absent only for mock providers and executors not yet configured for real calls.
    vault: Option<Arc<VaultAuth>>,
    /// The per-session context-management defaults, used when an episode is admitted or
    /// resumed to decide (or refuse) the value it runs under.
    context_management: broca_module_serve::context_management::ContextManagementConfig,
    /// Prompt-cache defaults loaded once at boot. Resolution is pure and runs only when a
    /// fresh episode is admitted; resumes reuse the policy already frozen in the log.
    cache_table: CacheTtlTable,
    /// The routing module broca asks which provider account a new run uses, and reports
    /// each run's account evidence to. `None` without the vault credential source.
    routing: Option<Arc<RoutingClient>>,
    /// This installation's id, the root of the Claude Code persona's device id (see
    /// `broca_module_serve::claude_code_identity`). `None` admits Anthropic OAuth episodes
    /// without a persona, in the shape they had before it existed.
    claude_code_install_id: Option<String>,
    /// The operator switches and refusal-resend model mapping for Anthropic OAuth runs.
    /// Consulted only at admission; a resume keeps its recorded policy. Both switches default off.
    anthropic_fallbacks: broca_module_serve::config::AnthropicFallbackConfig,
    tool_name_aliases: broca_module_serve::config::ToolNameAliasesConfig,
    /// Boot-loaded wait bound shared by routing admission and new account-limit pauses.
    max_same_account_wait: broca_core::SameAccountWait,
    /// Fork attempts whose routing admission `call_fork` couldn't release, by admission id,
    /// until the fork manager asks (`fork_release_failed`).
    unreleased_forks: std::sync::Mutex<std::collections::HashSet<String>>,
}

impl LiveLoopExecutor {
    pub fn new(
        state_root: impl Into<PathBuf>,
        plane: Arc<SubcToolPlane>,
        provider_base_url: Option<String>,
    ) -> Self {
        Self {
            state_root: state_root.into(),
            plane,
            hooks: None,
            compaction_plane: None,
            compaction_views: Arc::default(),
            provider_held_lifecycle: None,
            provider_client: build_provider_http_client(),
            clock: broca_core::clock::Clock::default(),
            provider_base_url,
            provider_overrides: Default::default(),
            vault: None,
            context_management: Default::default(),
            cache_table: CacheTtlTable::shipped_defaults(),
            routing: None,
            claude_code_install_id: None,
            anthropic_fallbacks: Default::default(),
            tool_name_aliases: Default::default(),
            max_same_account_wait: Default::default(),
            unreleased_forks: Default::default(),
        }
    }

    /// Install the id the Claude Code persona's device id derives from.
    pub fn with_claude_code_install_id(mut self, install_id: Option<String>) -> Self {
        self.claude_code_install_id = install_id;
        self
    }

    /// Install the operator switches for Anthropic content-filter fallback.
    pub fn with_tool_name_aliases(
        mut self,
        config: broca_module_serve::config::ToolNameAliasesConfig,
    ) -> Self {
        self.tool_name_aliases = config;
        self
    }

    pub fn with_anthropic_fallbacks(
        mut self,
        config: broca_module_serve::config::AnthropicFallbackConfig,
    ) -> Self {
        self.anthropic_fallbacks = config;
        self
    }

    pub fn with_max_same_account_wait(mut self, setting: broca_core::SameAccountWait) -> Self {
        self.max_same_account_wait = setting;
        self
    }

    pub fn with_hook_plane(mut self, hooks: Arc<broca_subc::SubcHookPlane>) -> Self {
        self.hooks = Some(hooks);
        self
    }

    pub fn with_compaction_plane(
        mut self,
        compaction: Arc<broca_subc::SubcCompactionPlane>,
    ) -> Self {
        self.compaction_plane = Some(compaction);
        self
    }

    /// Install the slots provider-held calls wait in, so a run resumed for a late result
    /// can take it (the same slots the tool plane's held replies wait in).
    pub fn with_provider_held_lifecycle(
        mut self,
        slots: Arc<broca_module::held_slots::HeldSlots>,
    ) -> Self {
        self.provider_held_lifecycle = Some(slots);
        self
    }

    /// Ask `routing` which account each new subscription-account run uses.
    pub fn with_routing(mut self, routing: Arc<RoutingClient>) -> Self {
        self.routing = Some(routing);
        self
    }

    /// List the vault's credentials once at boot, in the background, so the first
    /// admission finds its candidates ready instead of listing inside its budget.
    pub fn prime_account_listing(&self) {
        let Some(vault) = self.vault.clone() else {
            return;
        };
        tokio::spawn(async move {
            refresh_listing(&vault).await;
        });
    }

    /// How a fresh run's account is chosen. Only a fresh episode on the vault path with a
    /// routing client asks; a resume reuses its frozen decision and never asks again.
    /// Decides everything a run's admission freezes before the run's start is written: its
    /// context management, the credential and account (routing included), the cache policy
    /// and key, a Claude Code persona and the provider's resolved render config. The session's
    /// `history` says what it holds. An error means nothing was written and no model was
    /// called; the caller answers the admission with it.
    async fn admit_episode(
        &self,
        job: EpisodeJob<'_>,
        history: &EpisodeHistory<'_>,
    ) -> Result<EpisodeAdmission, RunError> {
        // The run's model id is `<provider>/<model>` from the wire request. A base_url
        // override for the mock endpoint (`BROCA_PROVIDER_BASE_URL`) sends the model
        // verbatim as OpenAI-compatible.
        let prior_run = &history.prior_run;
        let fresh_episode = matches!(job.kind, RunKind::AppendEpisode)
            || prior_run
                .as_ref()
                .is_none_or(|prior| prior.idle && prior.run_id != job.req.run_id);
        // A provider the catalog doesn't carry (`anthropic-platform`, a company gateway)
        // takes the rows of the catalog provider its vault credentials serve: the run's
        // facts, wire family and quirks come from that row, under the provider the caller
        // named. A resume keeps what its run froze and never resolves it again.
        let served_provider = if fresh_episode {
            self.served_provider_for(job.req.model.provider_module_id.as_str())
                .await?
        } else {
            prior_run
                .as_ref()
                .and_then(|prior| prior.config.served_provider.clone())
        };
        let catalog_provider = served_provider
            .as_deref()
            .unwrap_or(job.req.model.provider_module_id.as_str());
        let model_id = format!("{catalog_provider}/{}", job.req.model.model_id);
        // Context management is decided before auth so a refused value costs no credential
        // read. The last episode's config is read from `history.last_config` because an
        // appended episode has no `prior_run` view, yet is exactly the case that inherits.
        let episode_context_management = match self.context_management.for_episode(
            fresh_episode,
            job.req.context_management.as_ref(),
            history.last_config,
            &job.req.session.project_root.to_string_lossy(),
        ) {
            Ok(decided) => decided,
            Err(error) => {
                return Err(error);
            }
        };
        let auth_context = episode_auth_context(fresh_episode, prior_run.is_some());
        let (pinned_method, pinned_account) =
            match caller_auth_for_episode(job.auth.as_ref(), auth_context) {
                Ok(pin) => pin,
                Err(error) => {
                    return Err(error);
                }
            };
        // On a RESUME, re-use the credential the prior run FROZE (read from the durable
        // RunConfig of the last episode) — never re-resolve the method from config/router
        // (a drift would change the wire family and bust the cache). A NEW episode
        // (append) resolves fresh — it's a new run with its own method. A fresh run / an
        // A prior run without a frozen credential id resolves from config.
        let frozen_credential_id: Option<String> = if fresh_episode {
            None
        } else {
            prior_run
                .as_ref()
                .and_then(|prior| prior.config.auth_selection.clone())
                .map(|s| s.credential_id)
        };
        // Present when the frozen credential id is the vault's own id (see
        // `AuthSelection::auth_method`): the resume then reads it verbatim with this method.
        let frozen_auth_method: Option<String> = if fresh_episode {
            None
        } else {
            prior_run
                .as_ref()
                .and_then(|prior| prior.config.auth_selection.as_ref())
                .and_then(|selection| selection.auth_method.clone())
        };

        // Authentication comes from no credential for mock, a fresh vault token, or the
        // credential source. If the vault says the credential needs re-authentication, we
        // still resolve the provider without auth to record the correct RunStarted, then
        // pause before any provider call. Auth attaches after rendering and is excluded
        // from the config hash, so omitting it does not change the frozen config.
        // A fresh subscription-account run asks routing to choose an account within the
        // routing time limit set by ADMISSION_BUDGET; a resume reuses the account decision
        // saved in that episode's RunConfig. This session's persisted checkpoint contains
        // its latest episode's conversation key. A new episode inherits that key, or uses
        // the legacy session-derived fallback if the latest episode predates key recording,
        // without scanning older log history. If this job executes a previously queued
        // send, its saved key arrives in job.conversation_key and takes precedence.
        let inherited_key = history
            .last_config
            .and_then(|config| config.auth_selection.as_ref())
            .and_then(|selection| selection.account_decision.as_ref())
            .and_then(|decision| decision.conversation_key.as_deref());
        let mut routing_decision_error = None;
        // A run moving at a quota wall takes the account routing already offered it.
        // A resumed run that moved to another account asks routing for its account again
        // only where routing answers that with the run's current account.
        let moved = !fresh_episode && prior_run.as_ref().is_some_and(|prior| prior.moved);
        let records_account_change = prior_run
            .as_ref()
            .is_some_and(|prior| prior.records_account_change);
        let mut resume_asks = resume_asks_routing(
            moved,
            records_account_change,
            job.routing_serves_continuation,
        );
        let chosen = match job.continue_offer {
            Some(offer) => Ok(offer.clone()),
            None => {
                self.choose_account_for_episode(
                    job.req,
                    fresh_episode || resume_asks,
                    EpisodeAccountPreference {
                        pin: (pinned_method, pinned_account),
                        pool: job.auth.as_ref().and_then(|auth| auth.pool.as_deref()),
                        credential_id: job
                            .auth
                            .as_ref()
                            .and_then(|auth| auth.credential_id.as_deref()),
                        // A moved session's first run here names the credential it last ran on.
                        // A resume keeps its run's account, so only a fresh episode sends it.
                        warm_credential: (job.moved_here && fresh_episode)
                            .then(|| history.last_config?.auth_selection.as_ref())
                            .flatten()
                            .map(|selection| selection.credential_id.as_str()),
                    },
                    (job.conversation_key.as_deref(), inherited_key),
                    broca_module_serve::account_routing::account_hold_cap(
                        self.max_same_account_wait,
                        history.last_config,
                        job.clock
                            .map_or_else(|| epoch_millis().max(0) as u64, |clock| clock.now_ms()),
                        |account| {
                            self.provider_client
                                .session_last_accepted_at_ms(&job.req.session, account)
                        },
                    ),
                    job.queued_service_tier.as_deref(),
                )
                .await
            }
        };
        let chosen = resumed_choice(chosen, &mut resume_asks, &job.req.run_id);
        let mut account_choice = match chosen {
            Ok(choice) => choice,
            Err(RunError::AccountSelectionRefused(refusal))
                if job.req.route_decision_id.is_some()
                    && refusal.routing_code == "unknown_decision" =>
            {
                routing_decision_error = Some(serde_json::from_value::<ProviderError>(serde_json::json!({
                        "class":"permanent", "provider_code":"routing_decision_refused",
                        "provider": self.routing.as_ref().map(|routing| routing.config().routing_module.as_deref().unwrap_or(cortexkit_role_routing::PROVIDES)).unwrap_or("routing"),
                        "reason":"unknown_decision", "message":"Routing refused the caller's unknown or expired decision; no model request was sent."
                    })).expect("routing refusal is a provider error"));
                AccountChoice::NotApplicable
            }
            Err(refusal) => {
                return Err(refusal);
            }
        };
        if let (
            AccountChoice::Routed {
                service_tier_chosen: Some(tier),
                ..
            },
            Some(level),
        ) = (&account_choice, job.queued_service_tier.as_mut())
        {
            level.clone_from(tier);
        }
        let frozen_decision = if fresh_episode {
            None
        } else {
            prior_run
                .as_ref()
                .and_then(|prior| prior.config.auth_selection.as_ref())
                .and_then(|selection| selection.account_decision.clone())
        };
        let mut resolution = if let Some(error) = &routing_decision_error {
            AuthResolution {
                mode: AuthMode::None,
                pause: Some(error.clone()),
                selection: None,
                method: pinned_method,
                project_id: None,
                account_id: None,
                record_version: None,
                in_run_refresh: None,
                credential_hosts: Default::default(),
            }
        } else {
            let recorded_resolved_id = if fresh_episode {
                None
            } else {
                prior_run
                    .as_ref()
                    .and_then(|prior| prior.config.auth_selection.as_ref())
                    .and_then(|selection| selection.resolved_credential_id.as_deref())
            };
            macro_rules! decide_auth {
                ($choice:expr) => {
                    self.decide_auth_with_account(
                        job.req.model.provider_module_id.as_str(),
                        frozen_credential_id.as_deref(),
                        frozen_auth_method.as_deref(),
                        recorded_resolved_id,
                        fresh_episode,
                        pinned_method,
                        pinned_account,
                        $choice,
                        frozen_decision.as_ref(),
                    )
                    .await
                };
            }
            let mut decided = decide_auth!(&account_choice);
            // A moved run's resume whose routed account can't be admitted (the vault doesn't
            // serve its credential here, or serves it for another account than routing
            // named) keeps the account it recorded, as for any other unusable answer (see
            // `resumed_choice`).
            if resume_asks && matches!(account_choice, AccountChoice::Routed { .. }) {
                if let Err(error) = &decided {
                    tracing::warn!(
                        target: "accounts",
                        run_id = %job.req.run_id.0,
                        %error,
                        "the account routing gave the resumed run can't be admitted; it keeps \
                         the account it recorded"
                    );
                    resume_asks = false;
                    account_choice = AccountChoice::NotApplicable;
                    decided = decide_auth!(&account_choice);
                }
            }
            match decided {
                Ok(v) => v,
                Err(e) => {
                    self.provider_client
                        .chatgpt_connections()
                        .retire_session(&job.req.session);
                    return Err(e);
                }
            }
        };
        // A resolve supplies current token evidence, not a replacement for frozen billing facts.
        // If routing gives a resumed run a different credential, that is a credential change:
        // the recorded `billing_vault_facts` (the vault's billing metadata for the credential,
        // which usage export bills by) describe the old credential, so the selection keeps the
        // facts this resolve read from the vault for the new one. A credential changed after a
        // sign-in is handled the same way: its facts are taken fresh, not copied.
        let recorded_selection = prior_run
            .as_ref()
            .and_then(|prior| prior.config.auth_selection.as_ref());
        let resumed_on_another_credential = resume_asks
            && resolution
                .selection
                .as_ref()
                .zip(recorded_selection)
                .is_some_and(|(now, recorded)| {
                    now.credential_id != recorded.credential_id
                        || now.account_ref != recorded.account_ref
                });
        if !resumed_on_another_credential {
            preserve_billing_vault_snapshot(&mut resolution.selection, recorded_selection);
        }
        if resolution.pause.is_some() || resolution.method != Some(AuthMethod::Chatgpt) {
            self.provider_client
                .chatgpt_connections()
                .retire_session(&job.req.session);
        }
        // Public sends enter as FreshOrResume because the wire cannot know whether the
        // credential has been repaired. Once auth succeeds, mark the core attempt as
        // auth-resolved so an AuthRequired replay continues; core ignores that signal for
        // fresh runs and every other replay resolution.
        // DRIVE the wire family from the resolved auth method: a method that selects a
        // distinct grammar (Antigravity → google-codeassist, ChatGPT → chatgpt-responses)
        // overrides the catalog provider's family. `family_for_method` is the single
        // authority — the same one the consistency guard below consults — so the family
        // and the credential can't drift to a mismatched pair. The Antigravity project
        // (a body render input) is frozen here too.
        let family_override = resolution.method.and_then(family_for_method);
        // A steer or interrupt that omitted `cache` keeps the session's cache: the policy and
        // key the latest episode froze, exactly as a resume of that episode would render.
        let inherited = inherited_cache(job.req, fresh_episode, history.last_config);
        let cache_policy = match &inherited {
            Some((policy, _)) => Ok(policy.clone()),
            None => self.cache_policy_for_episode(
                fresh_episode,
                family_override,
                job.req,
                catalog_provider,
                prior_run.as_ref(),
            ),
        };
        let cache_policy = match cache_policy {
            Ok(policy) => policy,
            Err(error) => {
                return Err(RunError::Config(error));
            }
        };
        let prompt_cache_key = if let Some((_, key)) = inherited {
            key
        } else if fresh_episode {
            self.prompt_cache_key_for_fresh_episode(job.req, catalog_provider)
        } else {
            prior_run
                .as_ref()
                .and_then(|prior| frozen_prompt_cache_key(&prior.config))
        };
        // The Claude Code persona a fresh episode freezes; the catalog keeps it only when
        // the episode resolves to the Anthropic OAuth shape. A resume restores the persona
        // its episode recorded, or its recorded absence, below.
        let speaks_as_claude_code = catalog_provider == "anthropic"
            && !matches!(
                resolution.method,
                Some(AuthMethod::ApiKey | AuthMethod::Gateway)
            );
        let fresh_persona = match (&self.claude_code_install_id, fresh_episode) {
            (Some(install_id), true) if speaks_as_claude_code => {
                let last_frozen = history
                    .last_config
                    .map(|config| &config.frozen_render_config);
                let cc_version_suffix = store_cc_version_suffix(
                    job.req,
                    last_frozen,
                    history.persona.first_user.as_ref(),
                    history.persona.first_user_sealed,
                );
                // Like every other failure before the loop, this one is reported through
                // the admission ack. Returning without it would drop the ack unsent, and
                // the actor would then see only "run ended before admission" instead of
                // this permanent Config cause.
                let prompt_ordinal = match prompt_ordinal_from(
                    job.req,
                    last_frozen,
                    history.persona.human_prompts,
                ) {
                    Ok(ordinal) => ordinal,
                    Err(error) => {
                        return Err(error);
                    }
                };
                match broca_module_serve::claude_code_identity::fresh_persona(
                    install_id,
                    resolution.account_id.as_deref(),
                    resolution
                        .selection
                        .as_ref()
                        .map_or(job.req.model.provider_module_id.as_str(), |selection| {
                            selection.credential_id.as_str()
                        }),
                    &job.req.session,
                    cc_version_suffix,
                    prompt_ordinal,
                ) {
                    Ok(persona) => Some(persona),
                    Err(error) => {
                        tracing::warn!(%error, "Claude Code persona unavailable for this episode");
                        None
                    }
                }
            }
            _ => None,
        };
        let opencode_go_session_id = if job.req.model.provider_module_id.as_str() == "opencode-go" {
            Some(broca_module_serve::opencode_go_identity::for_session(
                &job.req.session,
            ))
        } else {
            None
        };
        let codeassist_identity = if fresh_episode {
            match broca_module_serve::codeassist_identity::for_episode(
                &job.req.session,
                None,
                history.head_seq,
                epoch_millis().max(0) as u64,
            ) {
                Ok(identity) => identity,
                Err(error) => {
                    let error = format!("Code Assist identity unavailable: {error}");
                    return Err(RunError::Config(error));
                }
            }
        } else {
            None
        };
        let alias_route = resolution
            .method
            .map(|method| format!("{}:{catalog_provider}", method.as_token()));
        let mut admitted_tools: Vec<_> = job
            .req
            .tools
            .iter()
            .filter(|tool| job.req.policy_allowed.contains(&tool.name))
            .cloned()
            .collect();
        // Operator aliases cover every declaration the model sees, including runner-owned
        // tools. Only fresh episodes freeze this map; resumes keep their recorded names.
        if let Some(so) = &job.req.structured_output {
            if so.mode == broca_protocol::structured_output::Mode::SubmitTool {
                admitted_tools.push(broca_provider::structured_output::submit_tool(
                    so.schema.clone(),
                ));
            }
        }
        let aliases = alias_route
            .as_deref()
            .map(|route| self.tool_name_aliases.for_tools(route, &admitted_tools))
            .unwrap_or_default();
        let resolve_options = broca_catalog::live::ResolveOptions {
            quota_observer: broca_module_serve::usage_observe::observer(
                job.req.model.provider_module_id.as_str(),
                resolution.method.map(|method| method.as_token()),
                // The account the run sends on: routing's answer on a fresh run and on a
                // moved run's resume that asked for it, else the one the run recorded.
                if fresh_episode || resume_asks {
                    resolution.selection.as_ref()
                } else {
                    prior_run
                        .as_ref()
                        .and_then(|prior| prior.config.auth_selection.as_ref())
                },
            ),
            tool_name_aliases: fresh_episode.then_some((&admitted_tools, &aliases)),
            media: fresh_episode,
            media_profile: job.media_profile.as_deref().filter(|_| fresh_episode),
            claude_code_persona: fresh_persona.as_ref(),
            auth_method: resolution.method,
            family_override,
            project_id: resolution.project_id.as_deref(),
            // Use the codex session-id header when no request-level caller scope is fixed for this run.
            session_id: Some(job.req.session.session.as_str()),
            account_id: resolution.account_id.as_deref(),
            cache_policy: cache_policy.clone(),
            prompt_cache_key: prompt_cache_key.as_deref(),
            // Only fresh admissions resolve caller render choices; resume uses the bundle.
            variant: job.req.variant.as_deref().filter(|_| fresh_episode),
            service_tier: job.req.service_tier.filter(|_| fresh_episode),
            queued_service_tier: job.queued_service_tier.as_deref().filter(|_| fresh_episode),
            clock: Some(&self.clock),
            frozen_account_ref: if fresh_episode || resume_asks {
                resolution
                    .selection
                    .as_ref()
                    .and_then(|s| s.account_ref.as_deref())
            } else {
                prior_run
                    .as_ref()
                    .and_then(|p| p.config.auth_selection.as_ref())
                    .and_then(|s| s.account_ref.as_deref())
            },
            secret_refresher: resolution
                .in_run_refresh
                .as_ref()
                .map(|refresh| refresh.refresher.clone()),
            credential_hosts: resolution.credential_hosts.clone(),
            structured_output: job.req.structured_output.as_ref().filter(|_| fresh_episode),
            // Off unless an operator turned it on; a resume keeps its recorded value.
            anthropic_server_side_fallback: self.anthropic_fallbacks.server_side,
            // Offered only to a fresh episode, and only with the operator switch on; the
            // catalog then decides whether the episode is eligible and freezes it.
            refusal_fallback_model: self
                .anthropic_fallbacks
                .refusal_resend
                .offer_for(catalog_provider, job.req.model.model_id.as_str())
                .filter(|_| fresh_episode),
        };
        let chatgpt_wire = broca_module_serve::chatgpt_identity::for_episode(
            family_override.filter(|_| fresh_episode),
            None,
            || {
                let seed = match &self.claude_code_install_id {
                    Some(seed) => seed.clone(),
                    None => broca_module_serve::claude_code_identity::load_or_create_install_id(
                        &self.state_root,
                    )
                    .map_err(|error| {
                        format!("ChatGPT installation identity unavailable: {error}")
                    })?,
                };
                broca_module_serve::chatgpt_identity::fresh(
                    &seed,
                    &job.req.session,
                    &job.req.run_id.0,
                    epoch_millis().max(0) as u64,
                )
            },
        );
        let chatgpt_wire = match chatgpt_wire {
            Ok(wire) => wire,
            Err(error) => {
                return Err(RunError::Config(error));
            }
        };
        // Endpoint, renderer and backend ids are admission inputs. Re-reading the current
        // catalog would move or refuse an already admitted run after an upstream change.
        let recorded_render_config = prior_run
            .as_ref()
            .filter(|_| !fresh_episode)
            .map(|prior| decode_recorded_render_config(&prior.config));
        // A moved run whose resume asked routing again may be given another account than
        // the one it ran on. The recorded Claude Code persona names that account in the
        // request body, so it is refrozen for the account routing gave, as a move inside the
        // run refreezes it; the loop records the config with the account.
        let recorded_render_config = match recorded_render_config {
            Some(Ok(mut frozen)) if resume_asks => match persona_for_resumed_account(
                &mut frozen,
                self.claude_code_install_id.as_deref(),
                prior_run
                    .as_ref()
                    .and_then(|prior| prior.config.auth_selection.as_ref())
                    .and_then(|selection| selection.account_ref.as_deref()),
                resolution.selection.as_ref(),
                &job.req.session,
            ) {
                Ok(()) => Some(Ok(frozen)),
                Err(error) => return Err(RunError::Config(error)),
            },
            other => other,
        };
        // For a ChatGPT auth-at-start pause, this provider exists solely to satisfy the
        // core's provider type until resume rebuilds real credentials and runs the pre-call
        // refreeze check. The core durably writes RunStarted and RunPaused without rendering or
        // calling the pause-only provider. Verifiable non-pause attempts use strict auth;
        // an unreadable bundle uses an inert identity solely for core's refusal path.
        let resolved = match if let Some(Err(error)) = &recorded_render_config {
            tracing::warn!(%error, "recorded render config is unverifiable");
            Ok(unverifiable_recorded_resolution(
                &model_id,
                resolution.method,
                family_override,
            ))
        } else {
            resolve_provider_for_episode(
                &model_id,
                self.provider_base_url.as_deref(),
                // Renderers use u32 caps; saturation preserves the caller's u64 in the
                // admitted identity while never raising the effective request bound.
                job.req
                    .generation
                    .max_output_tokens
                    .map(|limit| limit.min(u64::from(u32::MAX)) as u32),
                &resolution,
                &resolve_options,
                self.provider_client.clone(),
                broca_catalog::live::EpisodeRenderConfig {
                    recorded_config: recorded_render_config
                        .as_ref()
                        .and_then(|config| config.as_ref().ok()),
                    opencode_go_session_id: opencode_go_session_id.as_deref(),
                    codeassist_identity: codeassist_identity.as_ref(),
                    chatgpt_wire: chatgpt_wire.as_ref(),
                    // A gateway key goes to its gateway's own address, which the run freezes
                    // like any other endpoint, so a resume keeps it.
                    base_url: if fresh_episode {
                        match &resolution.mode {
                            AuthMode::Gateway { endpoint, .. } => Some(endpoint.base_url.as_str()),
                            _ => self.provider_overrides.base_url(catalog_provider),
                        }
                    } else {
                        None
                    },
                    ..Default::default()
                },
            )
        } {
            Ok(r) => r,
            Err(e) => {
                // Pre-loop resolution failure → fire the admission ack with the error and
                // return WITHOUT touching the log (no RunStarted, no ghost run).
                return Err(e.into());
            }
        };

        if let Some(revision) = resolution.record_version {
            resolved.provider.set_credential_revision(revision);
        }

        // (provider, auth-method) CONSISTENCY GUARD: a method that IMPLIES a wire family
        // (the wire-changing methods) must match the family the provider actually
        // resolved — else a frozen mismatch (e.g. a code-assist token against the plain
        // Gemini endpoint) would render a token through the wrong wire shape and 400.
        // `family_for_method` is the single authority both this guard and the future
        // family driver consult, so the pair can never drift. Same-family method differences
        // are frozen into the render config by catalog resolution; fail admission (no
        // RunStarted is written, so no half-started run) if a family-changing method resolves
        // an inconsistent family.
        if let Some(method) = resolution.method {
            if let Some(implied) = family_for_method(method) {
                if resolved.family_id != implied {
                    let msg = format!(
                        "auth method '{}' implies wire family {:?} but provider resolved \
                             {:?} — refusing to freeze a mismatched credential/family pair",
                        method.as_token(),
                        implied,
                        resolved.family_id
                    );
                    return Err(RunError::Config(msg));
                }
            }
        }
        // A Code Assist request names the Google Cloud project that comes with the
        // Antigravity sign-in, and its renderer cannot write one without it. Refuse a fresh
        // run whose sign-in came without a project here, by name and before any run record,
        // rather than letting its first request fail to render. A run paused for sign-in has
        // no token yet and is checked when it resumes.
        if fresh_episode
            && resolution.pause.is_none()
            && resolved.family_id == WireFamilyId::GoogleCodeAssist
            && resolution.project_id.is_none()
        {
            return Err(RunError::Config(format!(
                "the Antigravity sign-in for model '{model_id}' came without a Google Cloud \
                     project, and Code Assist requests name one; sign in to Antigravity again"
            )));
        }

        Ok(EpisodeAdmission {
            fresh_episode,
            episode_context_management,
            resolution,
            account_choice,
            routing_decision_error,
            resolved,
            served_provider,
        })
    }

    /// The loop's controls for an admitted run: the routing refusal or sign-in pause it
    /// starts with, the frozen credential selection, the reporter of its account evidence
    /// to routing, and its frozen context limit. Returns the reporter too, for the run's
    /// end. `durable_head` is the seq of the session's last durable record: the reporter
    /// names only records at or below it. `store_session` is a run in the session store's
    /// session id, which its event ids are named by.
    #[allow(clippy::too_many_arguments)]
    fn episode_control(
        &self,
        mut control: broca_core::run::RunControl,
        run_id: &broca_protocol::RunId,
        durable_head: Arc<std::sync::atomic::AtomicU64>,
        routing_decision_error: Option<ProviderError>,
        resolution: &AuthResolution,
        account_choice: &AccountChoice,
        frozen_context_limit: Option<u64>,
        served_provider: Option<String>,
        store_session: Option<String>,
    ) -> (
        broca_core::run::RunControl,
        Option<Arc<broca_module_serve::account_routing::RunReporter>>,
    ) {
        // Thread the pause-for-reauth instruction + the frozen auth-selection into the
        // loop's controls. The pause admits the run then writes RunPaused{AuthRequired}
        // (resumable). The selection is frozen into a fresh RunStarted's RunConfig so a
        // later resume re-gets the SAME credential (the loop ignores it on a resume —
        // the value is already durable). Mock stays None; a real run carries the
        // method-qualified vault credential id.
        control.max_same_account_wait = self.max_same_account_wait;
        control.account_limit_pause = self.max_same_account_wait.enabled();
        control.hook_plane = self.hook_plane();
        // Module subscribers source ordinary AssistantMessage/ToolCall/ToolResult units from
        // the durable log projector. The actor's live-output task discards those same units,
        // so suppress their payload clones while retaining display, lifecycle, pause, and
        // error controls needed by the actor and direct loop consumers.
        control.emit_control_payloads = false;
        control.pause_for_auth_at_start = if routing_decision_error.is_some() {
            None
        } else {
            resolution.pause.clone()
        };
        control.pre_model_error = routing_decision_error.or(control.pre_model_error);
        control.auth_selection = resolution.selection.clone();
        // Report only an allocation routing granted to this runner. A static choice never
        // holds an admission, even when an older frozen decision carries an invented id.
        let account_reporter = match (
            self.routing.as_ref(),
            resolution
                .selection
                .as_ref()
                .and_then(|selection| selection.account_decision.as_ref()),
        ) {
            (Some(routing), Some(decision)) => {
                let view_digest = match &account_choice {
                    AccountChoice::Routed { view_digest, .. } => Some(view_digest.clone()),
                    _ => None,
                };
                broca_module_serve::account_routing::RunReporter::start_for_decision(
                    Arc::clone(routing),
                    decision,
                    run_id.0.clone(),
                    view_digest,
                    durable_head,
                    store_session,
                )
            }
            _ => None,
        };
        if let (
            Some(reporter),
            AccountChoice::Routed {
                account_id,
                credential_id,
                ..
            },
        ) = (&account_reporter, &account_choice)
        {
            reporter.admitted_on(account_id.clone(), credential_id.clone());
        }
        control.account_reporter = account_reporter
            .clone()
            .map(|reporter| reporter as Arc<dyn broca_core::AccountReporter>);
        // Freeze the context limit onto a fresh run's RunStarted. A fresh run prefers the
        // caller override over the catalog; a resume uses only the log value.
        control.admitted_context_limit = frozen_context_limit;
        control.served_provider = served_provider;
        (control, account_reporter)
    }

    /// Reports an admitted run's end: its terminal refusal and finish to routing, and a
    /// provider's rejection of the vault credential it used to the vault. `terminal_error`
    /// reads the run's terminal error from the session's storage, only when an outcome
    /// needs it, and at most once per report. `durable_head` names the run's terminal.
    #[allow(clippy::too_many_arguments)]
    async fn report_run_end<F>(
        &self,
        summary: &Result<RunSummary, RunError>,
        account_reporter: Option<&broca_module_serve::account_routing::RunReporter>,
        report_selection: Option<broca_core::AuthSelection>,
        used_credential: Option<(broca_core::AuthSelection, u64)>,
        in_run_refresh: Option<&InRunRefresh>,
        terminal_error: impl Fn() -> F,
        durable_head: &std::sync::atomic::AtomicU64,
        session: &BindIdentity,
    ) where
        F: std::future::Future<Output = Option<ProviderError>>,
    {
        // The loop made its pause or finish durable and nothing has been appended since:
        // the durable head names that terminal, including a rejected API key's auth pause.
        let routed_error = if let Some(reporter) = account_reporter {
            report_routed_terminal_error(
                reporter,
                report_selection.as_ref(),
                summary,
                terminal_error(),
                durable_head.load(Ordering::Acquire),
            )
            .await
        } else {
            None
        };
        if let (Some(reporter), Ok(finished)) = (&account_reporter, summary) {
            if finished.paused.is_none() {
                reporter.finish(durable_head.load(Ordering::Acquire), finished.reason);
            }
        }

        // Report a WIRE auth rejection to the vault so it invalidates a stale token
        // (marks it needs_reauth) instead of serving it again next run. This fires only
        // when the provider REJECTED a token we got from the vault — i.e. the vault
        // thought the credential was fresh but the provider returned 401 mid-run (the run
        // then paused AuthRequired, recording the status). Fire-and-forget; a 5xx/429 is a
        // provider hiccup rather than a dead credential — see `terminal_wire_auth_status`.
        //
        // IN PRACTICE ONLY 401 REACHES HERE. This comment used to say 401/403, and
        // `wire_auth_status` still matches both, but HTTP 403 now classifies Permanent
        // rather than Auth (transport.rs, "forbidden" is an entitlement refusal, not a stale
        // token) and the class check rejects it before the status match runs. No shipped
        // classifier currently pairs Auth with a 403.
        //
        // KEEP THE 403 ARM. It is not dead code, it is code waiting on a decision that is
        // still open: whether a given provider's 403 means "entitlement refused" or "token
        // is stale" is a judgment, and if one is ever reclassified back to Auth this arm
        // starts firing correctly with no other change. Deleting it would make that future
        // flip silently incomplete.
        //
        // Worth stating because nothing failed when that reclassification landed: the guard
        // kept compiling and kept passing its tests while the condition it named moved
        // elsewhere. An arm whose description outlives its trigger reads as coverage.
        if let (Some(vault), Some((used_selection, admitted_version))) =
            (self.vault.as_ref(), used_credential)
        {
            if let Some(status) = post_run_wire_auth_status(
                summary,
                terminal_wire_auth_status(terminal_error()),
                routed_error.as_ref(),
            )
            .await
            {
                self.provider_client
                    .chatgpt_connections()
                    .retire_session(session);
                report_rejection_after_run(
                    vault,
                    &used_selection,
                    admitted_version,
                    in_run_refresh,
                    status,
                )
                .await;
            }
        }
    }

    /// [`RunExecutor::execute_on_store`]'s run. A seal calls no model. A fresh run and a resume
    /// make `execute`'s admission decisions, with the session's latest run, its config and the
    /// persona's history read from the folds.
    #[allow(clippy::too_many_arguments)]
    async fn run_on_store(
        &self,
        req: &mut broca_core::RunRequest,
        auth: &Option<broca_protocol::AuthRequest>,
        control: broca_core::run::RunControl,
        writer: &mut broca_session_loop::shared::ActorWriter,
        parts: &mut dyn broca_session_loop::parts::Parts,
        mode: broca_module::executor::StoreRunMode,
        media: Option<broca_module::store_media::StoreRunMediaWait>,
        mut queued_service_tier: Option<String>,
        keep_warm: Option<
            Arc<dyn broca_core::keep_warm_wait::KeepWarmDriver<broca_session_loop::run::Admission>>,
        >,
        out: &mpsc::Sender<OutputEvent>,
    ) -> Result<broca_session_loop::Finished, broca_module::executor::StoreRunError> {
        use broca_module::executor::{StoreRunError, StoreRunMode};
        use broca_session_loop::writer::Writer as _;
        let clock = control.clock.clone();
        let now = || {
            clock
                .as_ref()
                .map_or_else(|| epoch_millis().max(0) as u64, |clock| clock.now_ms())
        };
        let tools = self.plane.as_ref();
        let held = control.provider_held_lifecycle.clone();
        let compaction_plane = control.compaction_plane.clone();
        let cancelled_resume = matches!(
            mode,
            StoreRunMode::Resume | StoreRunMode::RecheckSignIn | StoreRunMode::ProbeSignIn
        ) && control
            .cancel
            .as_ref()
            .is_some_and(tokio_util::sync::CancellationToken::is_cancelled);
        if matches!(mode, StoreRunMode::Seal(_)) || cancelled_resume {
            // Neither sealing nor cancelling a parked run needs a model call. In particular
            // cancellation must work even when sign-in or provider access is unavailable.
            let provider = broca_core::provider::StubProvider::new(Vec::new());
            let seams = store_seams(
                &provider,
                tools,
                req,
                out,
                &control,
                held.as_deref(),
                if cancelled_resume {
                    None
                } else {
                    compaction_plane.as_deref()
                },
                &now,
                None,
                None,
                self.max_same_account_wait,
            );
            // Routing holds a lease for a run it admitted until it hears the run end, so a
            // seal or cancellation reports the end under the run's frozen account decision.
            let reporter = {
                let selection = writer
                    .operations()
                    .run
                    .as_ref()
                    .filter(|run| run.finished.is_none())
                    .and_then(|run| {
                        broca_session_loop::account_move::current_auth_selection(run, parts)
                            .ok()
                            .flatten()
                    });
                match (
                    self.routing.as_ref(),
                    selection
                        .as_ref()
                        .and_then(|selection| selection.account_decision.as_ref()),
                ) {
                    (Some(routing), Some(decision)) => {
                        broca_module_serve::account_routing::RunReporter::start_for_decision(
                            Arc::clone(routing),
                            decision,
                            req.run_id.0.clone(),
                            None,
                            writer.durable_head(),
                            Some(writer.session_id()),
                        )
                    }
                    _ => None,
                }
            };
            let durable_head = writer.durable_head();
            let finished = match mode {
                StoreRunMode::Seal(cause) => {
                    broca_session_loop::seal(writer, parts, &seams, cause).await
                }
                _ => Box::pin(broca_session_loop::resume(writer, parts, &seams)).await,
            }
            .map_err(StoreRunError::Loop)?;
            if let Some(reporter) = reporter.filter(|_| finished.paused.is_none()) {
                reporter.finish(durable_head.load(Ordering::Acquire), finished.reason);
            }
            return Ok(finished);
        }
        let stored = store_history(writer, parts)
            .await
            .map_err(StoreRunError::Loop)?;
        // Whether a moved run's resume may ask routing for its account: only when routing
        // serves continuation (see `resume_asks_routing`). Routing's description is read only
        // for such a resume, so no other run waits on it.
        let routing_serves_continuation = stored
            .prior_run
            .as_ref()
            .is_some_and(|prior| prior.moved && prior.records_account_change && !prior.idle)
            && self.routing_serves_continuation().await;
        let last_config = stored.prior_run.as_ref().map(|prior| prior.config.clone());
        if let Some(config) = &last_config {
            if let Some(account) = config
                .auth_selection
                .as_ref()
                .and_then(|auth| auth.account_ref.as_deref())
            {
                seed_store_accepted_send(
                    &self.provider_client,
                    writer,
                    &req.session,
                    account,
                    &config.frozen_render_config,
                    now(),
                )
                .await
                .map_err(|error| StoreRunError::Loop(error.into()))?;
            }
        }
        let history = EpisodeHistory {
            prior_run: stored.prior_run,
            last_config: last_config.as_ref(),
            head_seq: stored.head_seq,
            persona: stored.persona,
        };
        let admitted = self
            .admit_episode(
                EpisodeJob {
                    req,
                    kind: RunKind::FreshOrResume,
                    auth,
                    conversation_key: &control.conversation_key,
                    queued_service_tier: &mut queued_service_tier,
                    media_profile: &None,
                    clock: control.clock.as_ref(),
                    continue_offer: None,
                    routing_serves_continuation,
                    moved_here: control.moved_here,
                },
                &history,
            )
            .await
            .map_err(StoreRunError::Admission)?;
        let EpisodeAdmission {
            fresh_episode,
            episode_context_management,
            resolution,
            account_choice,
            routing_decision_error,
            resolved,
            served_provider,
        } = admitted;
        if fresh_episode != (mode == StoreRunMode::Fresh) {
            return Err(StoreRunError::Admission(RunError::Config(format!(
                "the store actor asked for a {mode:?} run, but the session's latest run says \
                     otherwise"
            ))));
        }
        let holds_media = req
            .input
            .iter()
            .any(|message| !broca_session_store::session::message_media(message).is_empty())
            || writer
                .conversation()
                .messages
                .iter()
                .any(|message| message.has_media());
        let mut model_call_ready = None;
        let mut media_renderer = None;
        if resolved.provider.supports_media_renderer() && (holds_media || !req.tools.is_empty()) {
            // Use the renderer installed by `execute` for a run that may carry media: the
            // engine kept by the session's actor as the session's image index (see
            // `broca_module::store_media`), reading the session's own media objects. The
            // session's first such run may still be waiting for the index to be built; each
            // model call waits for it first, and a request whose media can't be rendered is
            // refused by name before anything is sent.
            let media = media.ok_or_else(|| {
                StoreRunError::Admission(RunError::Log(
                    "store_media_unavailable: the run was started without its media index".into(),
                ))
            })?;
            let lazy = broca_module::store_media::LazyRenderer::new(
                media,
                Box::new(|media| Ok(media.renderer())),
            );
            resolved
                .provider
                .set_media_request_renderer(lazy.renderer());
            media_renderer = Some(Arc::clone(&lazy));
            model_call_ready = Some(lazy as Arc<dyn broca_session_loop::run::ModelCallReady>);
        }
        if broca_provider::person_media::references(&req.input)
            .next()
            .is_some()
        {
            // Admit a send's attachments against the media the resolved model takes, as its
            // frozen render config states them.
            let frozen = serde_json::from_value::<broca_provider::wire::FrozenRenderConfig>(
                resolved.provider.render_config_blob(),
            )
            .ok();
            let model = format!("{}/{}", req.model.provider_module_id, req.model.model_id);
            broca_module::person_media::check(
                frozen.as_ref().and_then(|frozen| frozen.media.as_ref()),
                &model,
                &req.input,
            )
            .map_err(StoreRunError::Admission)?;
        }
        let frozen_context_limit = context_limit_for_episode(
            fresh_episode,
            req.context_limit,
            resolved.context_limit,
            history
                .prior_run
                .as_ref()
                .and_then(|prior| prior.config.context_limit),
        );
        let report_selection = resolution.selection.clone();
        let durable_head = writer.durable_head();
        let (control, account_reporter) = self.episode_control(
            control,
            &req.run_id,
            Arc::clone(&durable_head),
            routing_decision_error,
            &resolution,
            &account_choice,
            frozen_context_limit,
            served_provider,
            Some(writer.session_id()),
        );
        req.context_management = Some(episode_context_management);
        let sent = Arc::new(std::sync::Mutex::new(SentCredentials::default()));
        sent.lock()
            .unwrap()
            .prepared(resolution.selection.as_ref(), resolution.record_version);
        resolved
            .provider
            .set_chatgpt_https_policy(broca_module_serve::transport_config::default_policy());
        let reports = Some(Arc::new(StoreAccountReports {
            routing: account_reporter.clone(),
            sent: sent.clone(),
        }) as Arc<dyn broca_session_loop::AccountReports>);
        let fresh_admission = if fresh_episode {
            match broca_session_loop::admit::admission(
                req,
                &control,
                &resolved.provider,
                writer.operations(),
                Some(now()),
            ) {
                Ok(admission) => Some(admission),
                Err(error) => return Err(StoreRunError::Admission(error)),
            }
        } else {
            None
        };
        let mut moves = AccountMoves::new(self, req, clock.as_ref(), media_renderer);
        moves.recheck_sign_in = matches!(
            mode,
            StoreRunMode::RecheckSignIn | StoreRunMode::ProbeSignIn
        );
        moves.probe_only = mode == StoreRunMode::ProbeSignIn;
        moves.sent = Some(sent.clone());
        let mut seams = store_seams(
            &resolved.provider,
            tools,
            req,
            out,
            &control,
            held.as_deref(),
            compaction_plane.as_deref(),
            &now,
            reports,
            resolution.selection.clone(),
            self.max_same_account_wait,
        );
        seams.model_call_ready = model_call_ready;
        seams.keep_warm = keep_warm;
        resolved
            .provider
            .set_cache_predecessor_loader(writer.cache_predecessor_loader());
        seams.account_continue = Some(&moves);
        let execution_start_seq = writer.next_seq();
        let finished = match fresh_admission {
            Some(admission) => {
                // Keep the loop body off the executor's poll frame, as the legacy loop
                // does, even though async_trait already boxes the outer executor.
                Box::pin(broca_session_loop::run(writer, parts, admission, &seams)).await
            }
            None => Box::pin(broca_session_loop::resume(writer, parts, &seams)).await,
        };
        // A run that moved to another account at a quota wall reports its end under the
        // credential it moved to.
        let moved = moves.moved();
        let report_selection = moved
            .as_ref()
            .map(|moved| moved.selection.clone())
            .or(report_selection);
        let used_credential = sent.lock().unwrap().last.clone();
        let in_run_refresh = match moved.as_ref().filter(|moved| {
            used_credential
                .as_ref()
                .is_some_and(|(selection, _)| *selection == moved.selection)
        }) {
            Some(moved) => moved.in_run_refresh.as_ref(),
            None => resolution.in_run_refresh.as_ref(),
        };
        // The run's end, for routing and the vault, as `execute` reports it. The reports
        // read only how the run ended or paused.
        let summary = match &finished {
            Ok(finished) => Ok(broca_core::RunSummary {
                reason: finished.reason,
                paused: finished.paused,
                steps: 0,
                usage: finished.usage.unwrap_or_default(),
                retries_used: finished.retries_used,
                final_step_finish_reason: None,
            }),
            Err(error) => Err(RunError::Config(format!("{error:?}"))),
        };
        let reader = &*writer;
        self.report_run_end(
            &summary,
            account_reporter.as_deref(),
            report_selection,
            used_credential,
            in_run_refresh,
            || store_terminal_error(reader, execution_start_seq),
            &durable_head,
            &req.session,
        )
        .await;
        finished.map_err(StoreRunError::Loop)
    }

    /// The catalog provider whose rows a fresh run on `provider` borrows: `None` when the
    /// catalog carries `provider` itself, when no vault lists credentials, or on the mock
    /// endpoint (`BROCA_PROVIDER_BASE_URL`), which serves every provider as named. Else the
    /// one catalog provider its active vault credentials serve. Refused when they name none
    /// or several, and as transient while the vault listing hasn't loaded.
    async fn served_provider_for(&self, provider: &str) -> Result<Option<String>, RunError> {
        use broca_module_serve::auth_routing::{catalog_provider, CatalogProvider};
        let Some(vault) = &self.vault else {
            return Ok(None);
        };
        if self.provider_base_url.is_some() {
            return Ok(None);
        }
        // With no catalog loaded nothing can be resolved: leave the provider as named, so
        // the provider resolution below refuses the run with the catalog's own error, in the
        // order admission has always refused it.
        let Ok(catalog) = broca_catalog::catalog() else {
            return Ok(None);
        };
        if catalog.serves_provider(provider) {
            return Ok(None);
        }
        let listing = latest_listing(Arc::clone(vault))
            .await
            .ok_or(RunError::CredentialsUnavailable)?;
        match catalog_provider(&listing, provider, |id| catalog.serves_provider(id)) {
            CatalogProvider::Own => Ok(None),
            CatalogProvider::Borrowed(served) => Ok(Some(served)),
            CatalogProvider::Unresolved(why) => Err(RunError::Config(why.describe(provider))),
        }
    }

    async fn choose_account_for_episode(
        &self,
        req: &broca_core::RunRequest,
        fresh_episode: bool,
        preference: EpisodeAccountPreference<'_>,
        conversation: (Option<&str>, Option<&str>),
        hold_cap: broca_module_serve::account_routing::AccountHoldCap,
        service_tier: Option<&str>,
    ) -> Result<AccountChoice, RunError> {
        if !fresh_episode || self.provider_base_url.is_some() {
            return Ok(AccountChoice::NotApplicable);
        }
        let (Some(vault), Some(routing)) = (self.vault.as_ref(), self.routing.as_ref()) else {
            return Ok(AccountChoice::NotApplicable);
        };
        let (pinned_method, pinned_account) = preference.pin;
        let listing = latest_listing(Arc::clone(vault))
            .await
            .ok_or(RunError::CredentialsUnavailable)?;
        let (conversation_key, inherited_conversation_key) = conversation;
        let static_credential = broca_module_serve::auth_routing::resolve_credential_id(
            req.model.provider_module_id.as_str(),
            pinned_method,
            pinned_account,
            &vault.methods,
            FALLBACK_METHOD,
        );
        let initial_input_bytes = serde_json::to_vec(&req.input)
            .map(|bytes| bytes.len() as u64)
            .unwrap_or_default();
        let deadline =
            tokio::time::Instant::now() + broca_module_serve::account_routing::ADMISSION_BUDGET;
        // With no catalog loaded no provider is a catalog provider, so a key listing no
        // provider ids belongs to none: it is left out rather than offered on a guess.
        let catalog = broca_catalog::catalog().ok();
        let catalog_serves = move |id: &str| {
            catalog
                .as_ref()
                .is_some_and(|catalog| catalog.serves_provider(id))
        };
        broca_module_serve::account_routing::choose_account(
            routing,
            broca_module_serve::account_routing::ChoiceInputs {
                catalog_serves: &catalog_serves,
                account_route: req.account_route.as_ref(),
                decision_id: req.route_decision_id.as_deref(),
                service_tier,
                pool: preference.pool,
                credential_id: preference.credential_id,
                conversation_key,
                inherited_conversation_key,
                static_credential: &static_credential,
                caller_named_account: pinned_account.is_some(),
                model: req.model.model_id.as_str(),
                session: &req.session,
                send_id: req.send_id.as_deref(),
                run_id: &req.run_id.0,
                initial_input_bytes,
                hold_cap,
                head_turn: req.head_turn,
                warm_credential: preference.warm_credential,
            },
            async { Some(listing) },
            deadline,
        )
        .await
        .map_err(RunError::from)
    }

    /// Whether routing serves continuation: it is known to declare both `account_continue`
    /// and `account_continue_resume`. No routing, or a description that can't be read,
    /// counts as no.
    async fn routing_serves_continuation(&self) -> bool {
        match self.routing.as_ref() {
            Some(routing) => routing.answers_moved_runs_with_current_account().await,
            None => false,
        }
    }

    /// Install the credential vault required for real provider calls.
    pub fn with_vault(mut self, vault: VaultAuth) -> Self {
        self.vault = Some(Arc::new(vault));
        self
    }

    /// What answers `vault.audit`: the same vault client, identity and routing
    /// configuration runs use. `None` without the vault auth source.
    pub fn vault_auditor(&self) -> Option<Arc<dyn broca_module_serve::vault_audit::VaultAuditor>> {
        self.vault
            .clone()
            .map(|vault| vault as Arc<dyn broca_module_serve::vault_audit::VaultAuditor>)
    }

    pub fn with_provider_overrides(
        mut self,
        overrides: broca_module_serve::provider_overrides::ProviderOverrides,
    ) -> Self {
        self.provider_overrides = overrides;
        self
    }

    /// Install the prompt-cache defaults loaded during process boot.
    pub fn with_cache_table(mut self, table: CacheTtlTable) -> Self {
        self.cache_table = table;
        self
    }

    pub fn with_clock(mut self, clock: broca_core::clock::Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Install the context-management defaults loaded at boot.
    pub fn with_context_management(
        mut self,
        config: broca_module_serve::context_management::ContextManagementConfig,
    ) -> Self {
        self.context_management = config;
        self
    }

    fn cache_policy_for_episode(
        &self,
        fresh_episode: bool,
        family_override: Option<WireFamilyId>,
        req: &broca_core::RunRequest,
        provider: &str,
        prior_run: Option<&PriorRunView>,
    ) -> Result<Option<CachePolicy>, String> {
        // The `BROCA_PROVIDER_BASE_URL` mock endpoint is opaque, so it gets no provider default:
        // only a lifetime the caller asked for, as it gets only a cache key the caller named.
        let opaque = self.provider_base_url.is_some();
        Ok(
            if opaque && req.cache.as_ref().and_then(|cache| cache.ttl).is_none() {
                None
            } else if fresh_episode {
                let family_for_cache = family_override.or_else(|| {
                    broca_catalog::catalog().ok().and_then(|catalog| {
                        catalog.wire_family(provider, req.model.model_id.as_str())
                    })
                });
                match family_for_cache {
                    Some(family) => {
                        let policy = broca_module_serve::resolve_cache_policy(
                            family,
                            provider,
                            req.cache.as_ref(),
                            &self.cache_table,
                            &req.session,
                        );
                        warn_single_call_cache_markers(&policy, &req.tools);
                        policy
                    }
                    None => None,
                }
            } else {
                prior_run.and_then(|prior| frozen_cache_policy(&prior.config))
            },
        )
    }

    /// The prompt-cache key a fresh episode freezes: the caller's `cache.key`, else the
    /// session-derived default for a provider the cache table lists. A resume never calls
    /// this; it re-applies the key frozen in the episode's render config. The
    /// `BROCA_PROVIDER_BASE_URL` mock endpoint is opaque, so like the cache policy it gets
    /// no provider default, only a key the caller named.
    fn prompt_cache_key_for_fresh_episode(
        &self,
        req: &broca_core::RunRequest,
        provider: &str,
    ) -> Option<String> {
        if self.provider_base_url.is_some() {
            return broca_module_serve::resolve_caller_cache_key(req.cache.as_ref());
        }
        broca_module_serve::resolve_prompt_cache_key(
            provider,
            req.cache.as_ref(),
            &self.cache_table,
            &req.session,
        )
    }

    /// Decide this run's auth: the mock path needs none; the vault path resolves the
    /// provider's method → credential id → handle → a fresh token (mapping the outcome);
    /// otherwise refusing a missing vault. Returns the resolution auth mode plus an optional
    /// pause-for-reauth error (set only when the vault is authoritative that the
    /// credential is dead — the run admits then pauses, making no provider call).
    ///
    /// A configured provider always resolves (api-key credentials included, because the
    /// vault is the single authority for all credential kinds). The only FailAdmission
    /// case is a genuinely unconfigured credential: a clear "no `<method>` credential for
    /// `<provider>`", never a silent fallback to a different method.
    ///
    /// `frozen_credential_id` is the prior run's frozen selection on a RESUME (read from
    /// the durable RunConfig). When present it is RE-USED verbatim — the method is never
    /// re-resolved from config (config/router could have drifted → a different method → a
    /// different family → a cache bust). `None` ⇒ a fresh run, which resolves a caller pin
    /// before config and freezes the result. `fresh_episode` distinguishes a fresh run from a
    /// resume of an old log that has no selection; the latter must not be retro-stamped.
    // Tests drive auth resolution without a routing decision through this form.
    #[cfg(test)]
    async fn decide_auth(
        &self,
        provider_id: &str,
        frozen_credential_id: Option<&str>,
        fresh_episode: bool,
        pinned_method: Option<AuthMethod>,
        pinned_account: Option<&str>,
    ) -> Result<AuthResolution, RunError> {
        self.decide_auth_with_account(
            provider_id,
            frozen_credential_id,
            None,
            None,
            fresh_episode,
            pinned_method,
            pinned_account,
            &AccountChoice::NotApplicable,
            None,
        )
        .await
    }

    /// Does what [`Self::decide_auth`] does, except that when `account` says routing chose
    /// a credential, that credential is read instead of the statically configured one, and
    /// the selection records how the account was chosen. `frozen_decision` is the prior
    /// run's recorded decision on a resume, carried unchanged so a resume's selection equals
    /// the one frozen at admission.
    #[allow(clippy::too_many_arguments)]
    async fn decide_auth_with_account(
        &self,
        provider_id: &str,
        frozen_credential_id: Option<&str>,
        frozen_auth_method: Option<&str>,
        frozen_resolved_id: Option<&str>,
        fresh_episode: bool,
        pinned_method: Option<AuthMethod>,
        pinned_account: Option<&str>,
        account: &AccountChoice,
        frozen_decision: Option<&broca_core::AccountDecision>,
    ) -> Result<AuthResolution, RunError> {
        if self.provider_base_url.is_some() {
            // The mock path needs no credential (and nothing to freeze/guard).
            return Ok(AuthResolution {
                mode: AuthMode::None,
                pause: None,
                selection: None,
                method: None,
                project_id: None,
                account_id: None,
                record_version: None,
                in_run_refresh: None,
                credential_hosts: Default::default(),
            });
        }
        let vault = self.vault.as_ref().ok_or_else(|| {
            RunError::Config("credential vault is required for real provider calls".into())
        })?;
        // Routed and already-frozen vault ids are read verbatim. Fresh keys and older
        // frozen keys use the same row resolver, with no file aliases or guessed ids.
        let routed = match account {
            AccountChoice::Routed {
                credential_id,
                account_id,
                method,
                decision,
                view_digest,
                ..
            } => Some((credential_id, account_id, decision, view_digest, *method)),
            _ => None,
        };
        let frozen_method = match (frozen_credential_id, frozen_auth_method) {
            (Some(id), Some(token)) => Some((
                id,
                AuthMethod::from_token(token).ok_or_else(|| {
                    RunError::Config(format!(
                        "the run's frozen credential selection names auth method '{token}', \
                             which this build does not know"
                    ))
                })?,
            )),
            _ => None,
        };
        let (credential_key, vault_id, credential_method, freezes_method) =
            match (&routed, frozen_method) {
                (Some((vault_id, _, _, _, method)), _) => {
                    ((*vault_id).clone(), (*vault_id).clone(), *method, true)
                }
                (None, Some((vault_id, method))) => (
                    vault_id.to_string(),
                    frozen_resolved_id.unwrap_or(vault_id).to_string(),
                    method,
                    true,
                ),
                (None, None) => {
                    if frozen_resolved_id.is_none() {
                        latest_listing(Arc::clone(vault))
                            .await
                            .ok_or(RunError::CredentialsUnavailable)?;
                    }
                    let key = broca_module_serve::auth_routing::select_credential_id(
                        frozen_credential_id,
                        provider_id,
                        pinned_method,
                        pinned_account,
                        &vault.methods,
                        FALLBACK_METHOD,
                    );
                    // Every credential is read with a scoped vault read, by its vault
                    // credential id. A
                    // scoped not_found is final: nothing retries it another way, which
                    // would hide the misconfiguration the operator needs to see.
                    let vault_id = match frozen_resolved_id {
                        Some(id) => id.to_owned(),
                        None => {
                            let row = if frozen_credential_id.is_none()
                                && pinned_method.is_none()
                                && vault.methods.method_for(provider_id).is_none()
                            {
                                None
                            } else {
                                vault.addressing.vault_id_for(&key)?
                            };
                            row.ok_or_else(|| {
                                RunError::Config(broca_module_serve::scoped_not_found_message(
                                    &vault
                                        .client
                                        .scoped_principal()
                                        .unwrap_or_else(|| "this process's principal".into()),
                                    &key.to_id_string(),
                                    &key.to_id_string(),
                                ))
                            })?
                        }
                    };
                    (key.to_id_string(), vault_id, key.method, false)
                }
            };
        // The request is built once and the call reads its arguments from it, so what the
        // selection records below is exactly what the vault was sent. get_scoped has no
        // force_refresh parameter, so none is recorded.
        let vault_request = broca_core::VaultRequest {
            min_ttl_ms: Some(VAULT_MIN_TTL_MS),
            force_refresh: None,
            op: Some(broca_core::VaultReadOp::GetScoped),
        };
        let (recorded_request, outcome) = if matches!(account, AccountChoice::AuthRequired(_)) {
            // Routing found every remaining candidate needing a fresh login. The run is
            // admitted (RunStarted is written) and paused for re-authentication, the same
            // outcome as the vault itself answering `needs_reauth`; no credential is read,
            // so no vault request is recorded.
            (None, Err(VaultError::NeedsReauth))
        } else {
            let got = vault
                .client
                .get_scoped(&vault_id, vault_request.min_ttl_ms)
                .await;
            // Decided from the vault's own answer, before the cache fallback below can
            // replace a failure with a token the vault did not serve for this request.
            let recorded_request = recorded_vault_request(&got, vault_request);
            // Reject a different account before it can enter the shared cache: static
            // admissions using the same credential key do not have a routed-account check.
            if let (Some((routed_vault_id, routed_account, ..)), Ok(token)) = (&routed, &got.result)
            {
                vault.gauge.stamp_ok();
                validate_routed_account(token, routed_vault_id, routed_account)?;
            }
            let outcome = resolve_vault_outcome(
                &vault.cache,
                vault.gauge.as_ref(),
                &credential_key,
                got.result,
                epoch_millis(),
                VAULT_MIN_TTL_MS,
            );
            (recorded_request, outcome)
        };
        if let Some((routed_vault_id, routed_account, decision, view_digest, _)) = &routed {
            match &outcome {
                // The vault does not serve the account routing chose to this principal. That
                // excludes the account for this admission only: routing is told, broca
                // re-lists, and the send is refused rather than spent on another account.
                Err(VaultError::NotFound) => {
                    if let Some(routing) = self.routing.as_ref() {
                        let relist_vault = Arc::clone(vault);
                        broca_module_serve::account_routing::report_scoped_not_found(
                            Arc::clone(routing),
                            decision.admission_id.clone().unwrap_or_default(),
                            (*routed_vault_id).clone(),
                            (*view_digest).clone(),
                            async move { refresh_listing(&relist_vault).await },
                        );
                    }
                    return Err(RunError::AccountSelectionRefused(
                        broca_core::AccountSelectionRefusal {
                            routing_code:
                                broca_module_serve::account_routing::ACCOUNT_NOT_AUTHORIZED
                                    .to_string(),
                            retry_after_s: None,
                            resets_at_ms: None,
                            routing_detail: None,
                            decision_id: decision.decision_id.clone(),
                            // As routing's own `account_not_authorized` is classed: operator
                            // policy keeps the credential from this principal, and signing in
                            // can't change that.
                            class: ErrorClass::Permanent,
                        },
                    ));
                }
                // A credential that resolves to another account than the one routing chose
                // would spend on an account nobody selected.
                Ok(token) => validate_routed_account(token, routed_vault_id, routed_account)?,
                _ => {}
            }
        }
        // The Antigravity cloud project travels alongside the token (Some only for an
        // antigravity credential); capture it before the outcome is consumed so it can be
        // frozen as a body render input.
        let project_id = outcome.as_ref().ok().and_then(|t| t.project_id.clone());
        let account_id = outcome.as_ref().ok().and_then(|t| t.account_id.clone());
        let record_version = outcome.as_ref().ok().map(|t| t.record_version);
        let expires_at_ms = outcome.as_ref().ok().and_then(|t| t.expires_at_ms);
        // Freeze the resolved id on a fresh run (the loop writes it only on a fresh
        // RunStarted). A resume with a frozen selection reuses it; an older resume without
        // one keeps None rather than retro-stamping its log.
        let selection = (fresh_episode || frozen_credential_id.is_some()).then(|| {
            broca_core::AuthSelection {
                billing_vault_facts: None,
                credential_id: credential_key.clone(),
                // Keep the vault-served identity when available. A definite sign-in failure
                // still needs routing's selected account to identify which account's
                // credential is unavailable.
                account_ref: account_id.clone().or_else(|| {
                    matches!(&outcome, Err(VaultError::NeedsReauth))
                        .then(|| routed.as_ref().map(|(_, account, ..)| account.to_string()))
                        .flatten()
                }),
                resolved_credential_id: Some(vault_id.clone()),
                // Present only if the vault replied; see `recorded_vault_request`.
                vault_request: recorded_request,
                // How the account was chosen; a resume carries the decision frozen at
                // admission.
                account_decision: match account {
                    AccountChoice::NotApplicable => frozen_decision.cloned(),
                    AccountChoice::Routed {
                        account_id,
                        credential_id,
                        decision,
                        ..
                    } => Some(broca_module_serve::account_routing::routed_decision(
                        decision,
                        account_id,
                        credential_id,
                    )),
                    chosen => chosen.decision().cloned(),
                },
                // Frozen only when `credential_id` is the vault's own id, so that a resume
                // takes the method from here and reads that id verbatim.
                auth_method: freezes_method.then(|| credential_method.as_token().to_owned()),
            }
        });
        let method = Some(credential_method);
        let in_run_refresh = record_version.map(|version| {
            let served_version = Arc::new(AtomicU64::new(version));
            let served_expires_at_ms = Arc::new(std::sync::Mutex::new(expires_at_ms));
            let reported_version = Arc::new(std::sync::Mutex::new(None));
            let unreported_rejection = Arc::new(std::sync::Mutex::new(None));
            InRunRefresh {
                refresher: SharedSecretRefresher(Arc::new(VaultTokenRefresher {
                    vault: Arc::clone(vault),
                    credential_key: credential_key.clone(),
                    vault_id: vault_id.clone(),
                    served_version: Arc::clone(&served_version),
                    served_expires_at_ms: Arc::clone(&served_expires_at_ms),
                    reported_version: Arc::clone(&reported_version),
                    unreported_rejection: Arc::clone(&unreported_rejection),
                    presend_failure_logged: std::sync::atomic::AtomicBool::new(false),
                    no_expiry_logged: std::sync::atomic::AtomicBool::new(false),
                })),
                served_version,
                served_expires_at_ms,
                reported_version,
                unreported_rejection,
            }
        });
        // A scoped not_found keeps not_found's class (a permanent admission failure) but
        // says what it can mean; see `scoped_not_found_message`.
        let credential_hosts = broca_provider::credential_hosts::CredentialHosts::from_record(
            outcome
                .as_ref()
                .ok()
                .and_then(|token| token.hosts.as_deref()),
        );
        let decision = match &outcome {
            Err(VaultError::NotFound) => {
                AuthDecision::FailAdmission(broca_module_serve::scoped_not_found_message(
                    &vault
                        .client
                        .scoped_principal()
                        .unwrap_or_else(|| "this process's principal".to_string()),
                    &credential_key,
                    &vault_id,
                ))
            }
            _ => vault_outcome_to_decision(outcome),
        };
        match decision {
            // A pause resolves a provider with no auth only to admit the resumable run.
            // Credential-bound render fields may be absent (Antigravity's project_id), so
            // this initial frozen config is not trusted for a call. On successful re-auth,
            // core replaces it durably before the first provider call; a mid-run pause keeps
            // the ordinary strict drift guard because a call already used its config.
            AuthDecision::PauseForReauth(err) => Ok(AuthResolution {
                mode: AuthMode::None,
                pause: Some(*err),
                selection,
                method,
                project_id,
                account_id,
                record_version,
                in_run_refresh: None,
                credential_hosts: Default::default(),
            }),
            AuthDecision::Proceed(mode) => Ok(AuthResolution {
                mode: gateway_mode_for(credential_method, mode, &credential_key)?,
                pause: None,
                selection,
                method,
                project_id,
                account_id,
                record_version,
                in_run_refresh,
                credential_hosts,
            }),
            AuthDecision::FailAdmission(msg) => Err(RunError::Config(msg)),
            AuthDecision::FailAdmissionTransient(msg) => Err(RunError::Transient(msg)),
        }
    }
}

impl LiveLoopExecutor {
    fn selection_family(
        &self,
        selection: &cortexkit_role_llm_runner::head_selection::PlanModel,
    ) -> Option<WireFamilyId> {
        let catalog = broca_catalog::catalog().ok()?;
        let (provider, model) = catalog.model_for_serving(&selection.provider, &selection.model)?;
        let method = self
            .vault
            .as_ref()
            .filter(|_| self.provider_base_url.is_none())
            .map(|vault| {
                broca_module_serve::auth_routing::resolve_credential_id(
                    &selection.provider,
                    None,
                    None,
                    &vault.methods,
                    FALLBACK_METHOD,
                )
                .method
            });
        let family = if let Some(family) = method.and_then(family_for_method) {
            family
        } else if catalog
            .routing_facts(&selection.provider, &selection.model)
            .is_some()
        {
            catalog
                .resolve_route(&selection.provider, &selection.model)
                .ok()?
                .0
        } else {
            provider.wire_family
        };
        if let Some(variant) = &selection.variant {
            let resolved = broca_provider::variant::resolve_variant(
                family,
                &format!("{}/{}", selection.provider, selection.model),
                &model.id,
                &model.reasoning_options,
                model.limits.max_output,
                variant,
            )
            .ok()?;
            if family == WireFamilyId::GoogleCodeAssist {
                catalog
                    .routing_facts(&selection.provider, &selection.model)?
                    .reasoning_wire_ids
                    .as_ref()?
                    .get(&resolved.name)
                    .filter(|id| !id.is_empty())?;
            }
        }
        Some(family)
    }
}

include!("fork_executor.rs");

#[async_trait]
impl RunExecutor for LiveLoopExecutor {
    fn joining_temperature(
        &self,
        model: &broca_protocol::request::ModelHandle,
        frozen_render_config: &serde_json::Value,
        requested: f64,
    ) -> Option<Option<f64>> {
        broca_catalog::live::joining_temperature(model, frozen_render_config, requested)
    }

    async fn fork_call(
        &self,
        job: broca_module::executor::ForkCallJob,
    ) -> broca_module::executor::ForkCallOutcome {
        self.call_fork(job).await
    }

    async fn fork_release(
        &self,
        job: broca_module::executor::ForkReleaseJob,
    ) -> Result<(), String> {
        self.release_fork(job).await
    }

    fn fork_release_failed(&self, admission_id: &str) -> bool {
        self.unreleased_forks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(admission_id)
    }
    fn head_selection_wire(
        &self,
        model: &cortexkit_role_llm_runner::head_selection::PlanModel,
    ) -> Option<String> {
        Some(
            match self.selection_family(model)? {
                WireFamilyId::OpenAiChat => "openai.chat_completions",
                WireFamilyId::OpenAiResponses => "openai.responses",
                WireFamilyId::OpenAiResponsesChatgpt => "openai.chatgpt_responses",
                WireFamilyId::AnthropicMessages => "anthropic.messages",
                WireFamilyId::Gemini | WireFamilyId::GoogleCodeAssist => "google.generate_content",
                WireFamilyId::BedrockConverse => "aws.bedrock_converse",
            }
            .into(),
        )
    }

    /// The limit a fresh run's resolution freezes (`broca_catalog::live::episode_context_limit`,
    /// the function that resolution fills `ProviderResolution::context_limit` from), with the
    /// run's endpoint and the wire family its auth method implies. The method is read from
    /// configuration alone, as `check_plan_selection` reads it: a refresh must never choose an
    /// account, read a credential or retire a connection. A send's own `context_limit` would
    /// win at the run, but a refresh carries none and no later send has to name one, so a
    /// refresh is checked against what the run resolves by itself.
    async fn compaction_context_limit(
        &self,
        model: &cortexkit_role_llm_runner::head_selection::PlanModel,
    ) -> Result<Option<u64>, String> {
        let method = self
            .vault
            .as_ref()
            .filter(|_| self.provider_base_url.is_none())
            .map(|vault| {
                broca_module_serve::auth_routing::resolve_credential_id(
                    &model.provider,
                    None,
                    None,
                    &vault.methods,
                    FALLBACK_METHOD,
                )
                .method
            });
        broca_catalog::live::episode_context_limit(
            &format!("{}/{}", model.provider, model.model),
            self.provider_base_url.as_deref(),
            method.and_then(family_for_method),
            model.variant.as_deref(),
        )
        .map_err(|error| error.to_string())
    }

    async fn check_plan_selection(
        &self,
        plan: &serde_json::Value,
    ) -> Result<
        Option<cortexkit_role_llm_runner::head_selection::PlanModel>,
        cortexkit_role_llm_runner::head_selection::PlanModelProblem,
    > {
        use cortexkit_role_llm_runner::head_selection::{self, PlanModelProblem};
        let listing = if plan.get("model").is_some() && self.provider_base_url.is_none() {
            match &self.vault {
                Some(vault) => latest_listing(Arc::clone(vault)).await,
                None => None,
            }
        } else {
            None
        };
        // A provider the catalog doesn't carry is checked against the rows of the catalog
        // provider its vault credentials serve, as admission resolves it.
        let catalog_model =
            |model: &head_selection::PlanModel| -> Option<head_selection::PlanModel> {
                use broca_module_serve::auth_routing::{catalog_provider, CatalogProvider};
                if self.provider_base_url.is_some() {
                    return Some(model.clone());
                }
                let catalog = broca_catalog::catalog().ok()?;
                match catalog_provider(listing.as_ref()?, &model.provider, |id| {
                    catalog.serves_provider(id)
                }) {
                    CatalogProvider::Own => Some(model.clone()),
                    CatalogProvider::Borrowed(served) => {
                        let mut served_model = model.clone();
                        served_model.provider = served;
                        Some(served_model)
                    }
                    CatalogProvider::Unresolved(_) => None,
                }
            };
        let selection = head_selection::check_plan(
            plan,
            &broca_wire::broca_role_describe(env!("CARGO_PKG_VERSION")).role,
            |model| {
                if self.provider_base_url.is_none() {
                    let vault = self.vault.as_ref()?;
                    let key = broca_module_serve::auth_routing::resolve_credential_id(
                        &model.provider,
                        Some(vault.methods.method_for(&model.provider)?),
                        None,
                        &vault.methods,
                        FALLBACK_METHOD,
                    );
                    let resolved = vault.addressing.vault_id_for(&key);
                    let listing = listing.as_ref()?;
                    // Every run asks routing for its account, so any active row routing could
                    // pick serves the plan. A subscription row needs its account id; an API
                    // key is offered by its credential id when it has none.
                    let routed = self.routing.is_some()
                        && listing.credentials.iter().any(|row| {
                            row.state == "active"
                                && row.operations.iter().any(|op| op == "read")
                                && (matches!(key.method, AuthMethod::ApiKey | AuthMethod::Gateway)
                                    || row.account_id.is_some())
                                && row.provider_ids.contains(&model.provider)
                                && row.auth_method.as_deref() == Some(key.method.as_token())
                        });
                    let available = routed
                        || resolved.ok().flatten().is_some_and(|id| {
                            listing.credentials.iter().any(|row| {
                                row.id == id && row.operations.iter().any(|op| op == "read")
                            })
                        });
                    if !available {
                        return None;
                    }
                }
                self.head_selection_wire(&catalog_model(model)?)
            },
        )?;
        if let Some(model) = &selection {
            if let Some(tier) = model.service_tier.filter(|tier| !tier.is_standard()) {
                let catalog =
                    broca_catalog::catalog().map_err(|_| PlanModelProblem::UnsupportedModel)?;
                let model = catalog_model(model).ok_or(PlanModelProblem::UnsupportedModel)?;
                broca_catalog::live::check_service_tier_level(
                    self.selection_family(&model)
                        .ok_or(PlanModelProblem::UnsupportedModel)?,
                    tier.as_str(),
                    &model.provider,
                    &format!("{}/{}", model.provider, model.model),
                    &catalog.priced_modes(&model.provider, &model.model),
                    &catalog.mode_wire_ids(&model.provider, &model.model),
                )
                .map_err(|_| PlanModelProblem::UnsupportedTier)?;
            }
        }
        Ok(selection)
    }

    fn check_service_tier(
        &self,
        request: &broca_protocol::request::SubmissionRequest,
    ) -> Result<(), String> {
        let Some(level) = request.service_tier_level.as_deref() else {
            return Ok(());
        };
        if level == "standard" {
            return Ok(());
        }
        let catalog = broca_catalog::catalog()
            .map_err(|error| format!("service_tier {level:?}: catalog unavailable: {error}"))?;
        let (provider, _) = catalog
            .model_for_serving(&request.model.provider_module_id, &request.model.model_id)
            .ok_or_else(|| {
                format!(
                    "service_tier {level:?}: unknown model {}/{}",
                    request.model.provider_module_id, request.model.model_id
                )
            })?;
        let (pin, account) =
            caller_auth_for_episode(request.auth.as_ref(), EpisodeAuthContext::FreshSession)
                .map_err(|error| format!("service_tier {level:?}: {error}"))?;
        let method = if self.provider_base_url.is_some() {
            None
        } else {
            self.vault
                .as_ref()
                .map(|vault| {
                    broca_module_serve::auth_routing::resolve_credential_id(
                        &request.model.provider_module_id,
                        pin,
                        account,
                        &vault.methods,
                        FALLBACK_METHOD,
                    )
                    .method
                })
                .or(pin)
        };
        let family = match method.and_then(family_for_method) {
            Some(family) => family,
            None if catalog
                .routing_facts(&request.model.provider_module_id, &request.model.model_id)
                .is_some() =>
            {
                catalog
                    .resolve_route(&request.model.provider_module_id, &request.model.model_id)
                    .map_err(|error| format!("service_tier {level:?}: {error}"))?
                    .0
            }
            None => provider.wire_family,
        };
        broca_catalog::live::check_service_tier_level(
            family,
            level,
            &provider.id,
            &format!(
                "{}/{}",
                request.model.provider_module_id, request.model.model_id
            ),
            &catalog.priced_modes(&request.model.provider_module_id, &request.model.model_id),
            &catalog.mode_wire_ids(&request.model.provider_module_id, &request.model.model_id),
        )
    }
    fn check_account_route(
        &self,
        request: &broca_protocol::request::SubmissionRequest,
    ) -> Result<(), String> {
        // Every sign-in method takes its account from routing, so only the auth is checked.
        caller_auth_for_episode(request.auth.as_ref(), EpisodeAuthContext::FreshSession)
            .map(|_| ())
            .map_err(|error| format!("account_route: {error}"))
    }
    fn cached_account_email(
        &self,
        selection: &broca_records::record::AuthSelection,
    ) -> Option<String> {
        let vault = self.vault.as_ref()?;
        let id = vault.addressing.vault_id_for_selection(selection).ok()??;
        let (listing, _) = vault.listing.get()?;
        listing
            .credentials
            .iter()
            .find(|row| row.id == id)?
            .email
            .clone()
    }
    async fn media_profile(
        &self,
        request: &broca_protocol::request::SubmissionRequest,
    ) -> Result<Option<Box<broca_provider::media_profile::MediaProfile>>, RunError> {
        let catalog =
            broca_catalog::catalog().map_err(|error| RunError::Transient(error.to_string()))?;
        let Some((provider, model)) =
            catalog.model_for_serving(&request.model.provider_module_id, &request.model.model_id)
        else {
            return Ok(None);
        };
        let (pin, account) =
            caller_auth_for_episode(request.auth.as_ref(), EpisodeAuthContext::FreshSession)?;
        let method = if self.provider_base_url.is_some() {
            None
        } else {
            self.vault
                .as_ref()
                .map(|vault| {
                    broca_module_serve::auth_routing::resolve_credential_id(
                        &request.model.provider_module_id,
                        pin,
                        account,
                        &vault.methods,
                        FALLBACK_METHOD,
                    )
                    .method
                })
                .or(pin)
        };
        let catalog_family = if catalog
            .routing_facts(&request.model.provider_module_id, &request.model.model_id)
            .is_some()
        {
            catalog
                .resolve_route(&request.model.provider_module_id, &request.model.model_id)
                .map_err(|error| RunError::Config(error.to_string()))?
                .0
        } else {
            provider.wire_family
        };
        let family = method.and_then(family_for_method).unwrap_or(catalog_family);
        Ok(Some(Box::new(
            broca_provider::media_profile::select_model_media_profile(
                family,
                provider.quirks.media.or_else(|| {
                    broca_provider::media_profile::media_quirk_for_catalog_provider(&provider.id)
                }),
                &model.id,
                &model,
            ),
        )))
    }

    fn provider_held_lifecycle(
        &self,
    ) -> Option<Arc<dyn broca_core::tool_plane::ProviderHeldLifecycle>> {
        self.provider_held_lifecycle
            .as_ref()
            .map(|slots| slots.clone() as Arc<dyn broca_core::tool_plane::ProviderHeldLifecycle>)
    }

    fn hook_plane(&self) -> Option<Arc<dyn broca_core::HookPlane>> {
        self.hooks
            .as_ref()
            .map(|hooks| hooks.clone() as Arc<dyn broca_core::HookPlane>)
    }

    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::compaction::CompactionPlane>> {
        self.compaction_plane
            .as_ref()
            .map(|plane| plane.clone() as Arc<dyn broca_core::compaction::CompactionPlane>)
    }

    fn compaction_views(&self) -> Option<Arc<dyn broca_core::CompactionViews>> {
        Some(self.compaction_views.clone())
    }

    fn warm_after_send_ms(&self, config: &broca_records::record::RunConfig) -> Option<u64> {
        broca_module_serve::cache_warm::warm_after_send_ms(config)
    }

    fn warm_costs(
        &self,
        config: &broca_records::record::RunConfig,
        prefix_tokens: u64,
        ttl_ms: u64,
    ) -> Result<broca_core::keep_warm::WarmCosts, String> {
        // The mock endpoint resolves no credential, so its runs froze none to price by.
        if self.provider_base_url.is_some() && config.auth_selection.is_none() {
            return broca_module_serve::cache_warm::credential_free_warm_prices(
                config,
                prefix_tokens,
                ttl_ms,
            );
        }
        broca_module_serve::cache_warm::automatic_warm_prices(config, prefix_tokens, ttl_ms)
    }

    fn responses_warm_cost(
        &self,
        config: &broca_records::record::RunConfig,
        usage: broca_protocol::Usage,
    ) -> Result<f64, String> {
        broca_module_serve::cache_warm::responses_warm_cost(config, usage)
    }

    fn failed_warm_cost(
        &self,
        config: &broca_records::record::RunConfig,
        reservation: &broca_records::record::AttemptReservation,
        envelope: &broca_records::record::AttemptEnvelope,
        ttl_ms: Option<u64>,
    ) -> Result<f64, String> {
        broca_module_serve::cache_warm::failed_warm_cost(config, reservation, envelope, ttl_ms)
    }

    async fn warm_quota(
        &self,
        config: &broca_records::record::RunConfig,
        headers: &std::collections::BTreeMap<String, String>,
    ) -> Result<(), String> {
        // The mock endpoint's runs froze no account, and it has no quota to ask about.
        if self.provider_base_url.is_some() && config.auth_selection.is_none() {
            return Ok(());
        }
        let (method, _) = config
            .auth_selection
            .as_ref()
            .and_then(|selection| {
                broca_module_serve::auth_routing::frozen_selection_credential(
                    selection,
                    config.model.provider_module_id.as_str(),
                )
            })
            .ok_or("account_unknown")?;
        if !method.is_subscription() {
            return Ok(());
        }
        let routing = self
            .routing
            .as_deref()
            .unwrap_or_else(|| broca_module_serve::keep_warm_quota::unavailable_routing_client());
        broca_module_serve::cache_warm::automatic_warm_quota_for_config(routing, config, headers)
            .await
    }
    /// A warm builds a provider over the config the warmed request was sent with, holding a
    /// credential read now exactly as a resume of the episode reads it, and sends one
    /// attempt. Nothing about the config is re-derived, so a recorded request renders to the
    /// bytes its step sent or the warm is refused.
    async fn warm(
        &self,
        job: broca_module::WarmJob,
    ) -> Result<broca_core::WarmSent, broca_module::WarmError> {
        use broca_module::WarmError;
        let config = &job.config;
        let provider_id = config.model.provider_module_id.as_str();
        let mut frozen = broca_module_serve::cache_warm::warmable_config(config)?;
        if provider_id == "opencode-go" {
            frozen.opencode_go_session_id = Some(
                broca_module_serve::opencode_go_identity::for_session(&job.session),
            );
        }
        let selection = config.auth_selection.as_ref();
        // The credential the episode froze is read again exactly as a resume reads it: no
        // account is chosen afresh, and the frozen decision is carried unchanged.
        let resolution = self
            .decide_auth_with_account(
                provider_id,
                selection.map(|selection| selection.credential_id.as_str()),
                selection.and_then(|selection| selection.auth_method.as_deref()),
                selection.and_then(|selection| selection.resolved_credential_id.as_deref()),
                false,
                None,
                None,
                &AccountChoice::NotApplicable,
                selection.and_then(|selection| selection.account_decision.as_ref()),
            )
            .await
            .map_err(|error| WarmError::Unavailable(error.to_string()))?;
        if let Some(pause) = resolution.pause {
            return Err(WarmError::Unavailable(format!(
                "the session's credential needs re-authentication: {}",
                pause.message
            )));
        }
        let provider = broca_catalog::live::provider_for_recorded_config_with_quota_observer(
            provider_id,
            &config.model.model_id,
            frozen,
            &resolution.mode,
            &resolution.credential_hosts,
            resolution.method,
            selection.and_then(|selection| selection.account_ref.as_deref()),
            resolution.account_id.as_deref(),
            self.provider_client.clone(),
            &self.clock,
            broca_module_serve::usage_observe::observer(
                provider_id,
                resolution.method.map(|method| method.as_token()),
                selection,
            ),
        )
        .map_err(WarmError::Unavailable)?;
        broca_module::executor::send_warm_with_hook(provider.as_ref(), job.base, job.attempt_hook)
            .await
    }

    /// Runs a session kept in the session store: the same admission decisions as
    /// `execute`, read from the session's folds instead of the session's log, then the new run
    /// loop (`broca_session_loop`) writing through the store actor.
    async fn execute_on_store(
        &self,
        job: broca_module::executor::StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> broca_module::executor::StoreRunEnd {
        let broca_module::executor::StoreRunJob {
            mut req,
            auth,
            control,
            mut writer,
            mut parts,
            mode,
            media,
            queued_service_tier,
            keep_warm,
        } = job;
        let result = self
            .run_on_store(
                &mut req,
                &auth,
                control,
                &mut writer,
                parts.as_mut(),
                mode,
                media,
                queued_service_tier,
                keep_warm,
                &out,
            )
            .await;
        broca_module::executor::StoreRunEnd { parts, result }
    }

    async fn on_session_close(&self, session: &BindIdentity) {
        self.retire_session_transport(session);
        // Retire this session's tool and role routes on their shared consumer connections.
        self.plane.close_session_routes(session).await;
        if let Some(hooks) = &self.hooks {
            hooks.close_session_routes(session).await;
        }
        if let Some(plane) = &self.compaction_plane {
            plane.close_session_routes(session).await;
        }
    }

    fn on_session_deleted(&self, session: &BindIdentity) {
        // Route ownership is removed synchronously; wire teardown survives the sweeper.
        let close = self.plane.close_session_routes(session);
        tokio::spawn(close);
    }

    fn retire_session_transport(&self, session: &BindIdentity) {
        self.provider_client
            .chatgpt_connections()
            .retire_session(session);
    }

    fn retire_all_transports(&self) {
        self.provider_client.chatgpt_connections().clear();
    }

    fn retire_idle_session_transport(&self, session: &BindIdentity) {
        self.provider_client
            .chatgpt_connections()
            .retire_idle_session(session);
    }
}

include!("store_cache_evidence.rs");

#[cfg(test)]
mod tests;
