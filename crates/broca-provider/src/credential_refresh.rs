//! Recovering, inside one step, from a provider rejecting the token a request was sent with.
//!
//! A run's credential is chosen once, at admission, and never changes. The TOKEN served for
//! that credential can still be revoked or rotated mid-run, and a provider then answers 401.
//! The credential source (the vault) can usually mint a fresh token for the same credential,
//! so the transport asks for one through these traits and re-sends the same request bytes once.
//!
//! A token can also simply run out of life during a long run. Before each attempt the transport
//! therefore asks whether the held token is about to expire and, if so, swaps it for a fresh one
//! before sending, so the request does not first go out with a token the provider will refuse.
//!
//! Two layers, because the pieces live in different crates: the credential source hands back a
//! raw secret ([`SecretRefresher`]), and whoever knows the provider's auth header shape turns it
//! into an [`AuthHandle`] ([`CredentialRefresher`], usually a [`ShapedSecretRefresher`]).
//!
//! None of these types implement `Debug` over secret material, and none may: a refreshed token
//! is as sensitive as the one it replaces.

use std::{future::Future, pin::Pin, sync::Arc};

use broca_protocol::CredentialRefresh;

use crate::credential_hosts::CredentialHosts;
use crate::wire::AuthHandle;

/// A rejected-token read must not confuse definite sign-in loss with an unavailable source.
pub enum RejectedRefresh<T> {
    Token(T),
    NeedsReauth,
    Unavailable,
}

impl<T> RejectedRefresh<T> {
    pub fn into_token(self) -> Option<T> {
        match self {
            Self::Token(token) => Some(token),
            _ => None,
        }
    }
}

pub type RefreshFuture<'a, T> = Pin<Box<dyn Future<Output = RejectedRefresh<T>> + Send + 'a>>;

/// A pre-send read distinguishes a definite sign-in loss from an unavailable source. A
/// still-unexpired held token can be sent even when the source cannot renew its sign-in.
pub enum ExpiringRefresh<T> {
    Token(T),
    NeedsReauth { held_expires_at_ms: Option<i64> },
    Unavailable,
}

pub type ExpiringRefreshFuture<'a, T> =
    Pin<Box<dyn Future<Output = ExpiringRefresh<T>> + Send + 'a>>;

#[cfg(feature = "live")]
pub(crate) fn needs_reauth_error() -> broca_protocol::ProviderError {
    broca_protocol::ProviderError {
        class: broca_protocol::ErrorClass::Auth,
        message: "credential needs re-authentication before another attempt".into(),
        provider_code: Some("needs_reauth".into()),
        status: None,
        retry_after_secs: None,
        provider_subcode: None,
        provider_details: None,
        cause: None,
        refusal: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        account_continue: None,
    }
}

/// A fresh raw secret for the run's frozen credential, plus what was asked to obtain it.
pub struct RefreshedSecret {
    pub secret: String,
    /// Recorded on the re-sent attempt. Carries request parameters and a version, never the
    /// secret.
    pub evidence: CredentialRefresh,
    /// Vault-parsed account belonging to this token; absent for accountless credentials.
    pub account_ref: Option<String>,
    /// The hosts the vault listed with this token, which the next request must go to.
    pub hosts: CredentialHosts,
}

/// A fresh, already-shaped credential for the run's frozen credential.
pub struct RefreshedCredential {
    pub auth: AuthHandle,
    pub evidence: CredentialRefresh,
    /// Vault-parsed account belonging to this token; absent for accountless credentials.
    pub account_ref: Option<String>,
    /// The hosts the vault listed with this token, which the next request must go to.
    pub hosts: CredentialHosts,
}

/// A credential source that can force-refresh the token of the ONE credential a run was
/// admitted with. Implementations must never substitute a different credential: the auth
/// selection frozen at admission is part of what the run is.
pub trait SecretRefresher: Send + Sync {
    /// Called after the provider rejected the most recently served token. `status` is the HTTP
    /// status of that rejection, so a report or cache eviction names what the provider said.
    fn refresh_rejected_secret(&self, status: u16) -> RefreshFuture<'_, RefreshedSecret>;

    /// Called when the provider rejected a token this step had already refreshed once, so no
    /// further refresh is tried and the rejection ends the step. `status` is that rejection's
    /// HTTP status. Nothing has reported this rejection to the source yet; an implementation
    /// that reports rejections at run end records it here so it can report exactly this token.
    ///
    /// The default does nothing.
    fn note_unrecovered_rejection(&self, _status: u16) {}

    /// Called before every attempt. Returns a fresh token only when the one served last is
    /// about to expire and the source could serve a newer one. A source reporting sign-in loss
    /// returns `NeedsReauth` with the held expiry; all other failures return `Unavailable`.
    /// Tokens that never expire need no read. Nothing was
    /// rejected, so implementations must not report a failure to the source here.
    ///
    /// The default never refreshes, for sources that cannot tell when a token expires.
    fn refresh_expiring_secret(&self) -> ExpiringRefreshFuture<'_, RefreshedSecret> {
        Box::pin(async { ExpiringRefresh::Unavailable })
    }
}

/// What the transport calls after an HTTP 401: a fresh credential shaped for this provider's
/// auth header, definite sign-in loss, or an unavailable source.
pub trait CredentialRefresher: Send + Sync {
    /// See [`SecretRefresher::refresh_rejected_secret`].
    fn refresh_rejected(&self, status: u16) -> RefreshFuture<'_, RefreshedCredential>;

    /// See [`SecretRefresher::note_unrecovered_rejection`]. The default does nothing.
    fn note_unrecovered_rejection(&self, _status: u16) {}

    /// What the transport calls before each attempt: a fresh credential when the held one is
    /// about to expire, or a typed sign-in/unavailability outcome (see
    /// [`SecretRefresher::refresh_expiring_secret`]). The default never refreshes.
    fn refresh_expiring(&self) -> ExpiringRefreshFuture<'_, RefreshedCredential> {
        Box::pin(async { ExpiringRefresh::Unavailable })
    }
}

/// A [`SecretRefresher`] that can sit in option structs which derive `Debug`, without the
/// trait object needing to be `Debug` itself.
#[derive(Clone)]
pub struct SharedSecretRefresher(pub Arc<dyn SecretRefresher>);

impl std::fmt::Debug for SharedSecretRefresher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SharedSecretRefresher(..)")
    }
}

/// Adapts a raw-secret source to a [`CredentialRefresher`] by shaping each fresh secret with the
/// same rule that shaped the admission-time secret, so a refreshed token lands in the same
/// header the rejected one was sent in.
pub struct ShapedSecretRefresher<S> {
    source: Arc<dyn SecretRefresher>,
    shape: S,
}

impl<S> ShapedSecretRefresher<S>
where
    S: Fn(String) -> AuthHandle + Send + Sync,
{
    pub fn new(source: Arc<dyn SecretRefresher>, shape: S) -> Self {
        Self { source, shape }
    }

    fn shaped(&self, refreshed: RefreshedSecret) -> RefreshedCredential {
        RefreshedCredential {
            auth: (self.shape)(refreshed.secret),
            evidence: refreshed.evidence,
            account_ref: refreshed.account_ref,
            hosts: refreshed.hosts,
        }
    }
}

impl<S> CredentialRefresher for ShapedSecretRefresher<S>
where
    S: Fn(String) -> AuthHandle + Send + Sync,
{
    fn refresh_rejected(&self, status: u16) -> RefreshFuture<'_, RefreshedCredential> {
        Box::pin(async move {
            match self.source.refresh_rejected_secret(status).await {
                RejectedRefresh::Token(token) => RejectedRefresh::Token(self.shaped(token)),
                RejectedRefresh::NeedsReauth => RejectedRefresh::NeedsReauth,
                RejectedRefresh::Unavailable => RejectedRefresh::Unavailable,
            }
        })
    }

    fn note_unrecovered_rejection(&self, status: u16) {
        self.source.note_unrecovered_rejection(status);
    }

    fn refresh_expiring(&self) -> ExpiringRefreshFuture<'_, RefreshedCredential> {
        Box::pin(async move {
            match self.source.refresh_expiring_secret().await {
                ExpiringRefresh::Token(token) => ExpiringRefresh::Token(self.shaped(token)),
                ExpiringRefresh::NeedsReauth { held_expires_at_ms } => {
                    ExpiringRefresh::NeedsReauth { held_expires_at_ms }
                }
                ExpiringRefresh::Unavailable => ExpiringRefresh::Unavailable,
            }
        })
    }
}
