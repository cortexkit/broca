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

use crate::wire::AuthHandle;

/// The future a refresher returns. `None` means no fresh token is available (the source
/// refused, the credential needs re-authentication, the source is unreachable, or this
/// credential cannot be force-refreshed), and the caller keeps the rejection it already has.
pub type RefreshFuture<'a, T> = Pin<Box<dyn Future<Output = Option<T>> + Send + 'a>>;

/// A fresh raw secret for the run's frozen credential, plus what was asked to obtain it.
pub struct RefreshedSecret {
    pub secret: String,
    /// Recorded on the re-sent attempt. Carries request parameters and a version, never the
    /// secret.
    pub evidence: CredentialRefresh,
    /// Vault-parsed account belonging to this token; absent for accountless credentials.
    pub account_ref: Option<String>,
}

/// A fresh, already-shaped credential for the run's frozen credential.
pub struct RefreshedCredential {
    pub auth: AuthHandle,
    pub evidence: CredentialRefresh,
    /// Vault-parsed account belonging to this token; absent for accountless credentials.
    pub account_ref: Option<String>,
}

/// A credential source that can force-refresh the token of the ONE credential a run was
/// admitted with. Implementations must never substitute a different credential: the auth
/// selection frozen at admission is part of what the run is.
pub trait SecretRefresher: Send + Sync {
    /// Called after the provider rejected the most recently served token with a 401.
    fn refresh_rejected_secret(&self) -> RefreshFuture<'_, RefreshedSecret>;

    /// Called before every attempt. Returns a fresh token only when the one served last is
    /// about to expire and the source could serve a newer one; `None` otherwise, including
    /// for tokens that never expire and when the source could not be reached. Nothing was
    /// rejected, so implementations must not report a failure to the source here.
    ///
    /// The default never refreshes, for sources that cannot tell when a token expires.
    fn refresh_expiring_secret(&self) -> RefreshFuture<'_, RefreshedSecret> {
        Box::pin(async { None })
    }
}

/// What the transport calls after an HTTP 401: a fresh credential shaped for this provider's
/// auth header, or `None` to let the rejection stand.
pub trait CredentialRefresher: Send + Sync {
    fn refresh_rejected(&self) -> RefreshFuture<'_, RefreshedCredential>;

    /// What the transport calls before each attempt: a fresh credential when the held one is
    /// about to expire, or `None` to send with the held one (see
    /// [`SecretRefresher::refresh_expiring_secret`]). The default never refreshes.
    fn refresh_expiring(&self) -> RefreshFuture<'_, RefreshedCredential> {
        Box::pin(async { None })
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
        }
    }
}

impl<S> CredentialRefresher for ShapedSecretRefresher<S>
where
    S: Fn(String) -> AuthHandle + Send + Sync,
{
    fn refresh_rejected(&self) -> RefreshFuture<'_, RefreshedCredential> {
        Box::pin(async move {
            let refreshed = self.source.refresh_rejected_secret().await?;
            Some(self.shaped(refreshed))
        })
    }

    fn refresh_expiring(&self) -> RefreshFuture<'_, RefreshedCredential> {
        Box::pin(async move {
            let refreshed = self.source.refresh_expiring_secret().await?;
            Some(self.shaped(refreshed))
        })
    }
}
