//! Where a System One provider's credential comes from: the bearer token, and for a provider
//! that addresses an account in its URL (Cloudflare Workers AI), the account id the vault
//! returns with it. The token is never logged, printed or put into a reply; the account
//! identity is retained only as the usage export's credential attribution.

use std::sync::Arc;

use async_trait::async_trait;

/// A resolved credential: the bearer token and the provider account it belongs to.
///
/// Both are secrets as far as broca's output goes, so `Debug` redacts them. Do not derive it.
#[derive(Clone)]
pub struct ProviderSecret {
    pub token: String,
    /// The provider-side account the vault returned with the token; `None` when it returned
    /// none.
    pub account_id: Option<String>,
    /// The vault's resolved pin, when returned; never a bearer secret.
    pub auth_selection: Option<String>,
}

impl std::fmt::Debug for ProviderSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderSecret")
            .field("token", &"<redacted>")
            .field(
                "account_id",
                &self.account_id.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Where a provider's credential comes from.
#[async_trait]
pub trait CredentialSource: Send + Sync {
    /// The credential's id (never its secret), for `auth_failed`'s detail.
    fn credential_id(&self) -> Option<String>;
    /// The bearer token, `None` to send no `authorization` header.
    async fn bearer(&self) -> Result<Option<String>, String>;
    /// The token with the account it belongs to; `None` when there is no credential at all.
    /// A source that knows no account (a plain API key) needs only [`Self::bearer`].
    async fn secret(&self) -> Result<Option<ProviderSecret>, String> {
        Ok(self.bearer().await?.map(|token| ProviderSecret {
            token,
            account_id: None,
            auth_selection: self.credential_id(),
        }))
    }
}

/// No credential: the requests carry no `authorization` header. Only for a rig whose stand-in
/// provider takes none (broca's mock provider path, where no vault is opened).
pub struct NoCredential;

#[async_trait]
impl CredentialSource for NoCredential {
    fn credential_id(&self) -> Option<String> {
        None
    }

    async fn bearer(&self) -> Result<Option<String>, String> {
        Ok(None)
    }
}

/// A credential read from the vault by grant, the way a session's run reads its own.
pub struct VaultCredential {
    pub client: Arc<broca_subc::VaultClient>,
    /// The credential's canonical id (for example `apikey:typesafe`), reported in
    /// `auth_failed`'s detail.
    pub credential_id: String,
    /// The vault's id for the credential: `credential_id`, or its configured alias.
    pub vault_id: String,
}

/// The shortest life a token may have left when it is handed out, so one provider attempt
/// never starts with a token about to expire.
const VAULT_MIN_TTL_MS: i64 = 60_000;

#[async_trait]
impl CredentialSource for VaultCredential {
    fn credential_id(&self) -> Option<String> {
        Some(self.credential_id.clone())
    }

    async fn bearer(&self) -> Result<Option<String>, String> {
        Ok(self.secret().await?.map(|secret| secret.token))
    }

    async fn secret(&self) -> Result<Option<ProviderSecret>, String> {
        self.client
            .get_scoped(&self.vault_id, Some(VAULT_MIN_TTL_MS))
            .await
            .result
            .map(|token| {
                Some(ProviderSecret {
                    token: token.token,
                    account_id: token.account_id,
                    auth_selection: token
                        .resolved_credential_id
                        .or_else(|| Some(self.credential_id.clone())),
                })
            })
            .map_err(|error| error.to_string())
    }
}
