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
    /// The hosts the vault listed with the token; a request carrying it goes only to one.
    pub hosts: broca_provider::credential_hosts::CredentialHosts,
}

impl ProviderSecret {
    /// The refusal message when `endpoint` is not a host this credential's record lists.
    pub fn refuse_endpoint(&self, endpoint: &str) -> Option<String> {
        self.hosts
            .check_url(endpoint)
            .err()
            .map(|error| error.message)
    }
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

/// Why a credential couldn't be read. Either way nothing was sent to the provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialError {
    /// The vault refused it: a new sign-in is needed, the record isn't there or isn't granted
    /// to broca, or it can't be used. Asking again won't help.
    Refused(String),
    /// It couldn't be read right now: the vault's listing hasn't loaded yet, or the vault
    /// couldn't be reached, was locked, or failed a refresh it may manage later.
    Unavailable(String),
}

impl From<String> for CredentialError {
    fn from(message: String) -> Self {
        Self::Refused(message)
    }
}

impl From<CredentialError> for super::ProviderReply {
    fn from(error: CredentialError) -> Self {
        match error {
            CredentialError::Refused(message) => Self::Credential(message),
            CredentialError::Unavailable(message) => Self::CredentialUnavailable(message),
        }
    }
}

/// Where a provider's credential comes from.
#[async_trait]
pub trait CredentialSource: Send + Sync {
    /// The credential's id (never its secret), for `auth_failed`'s detail.
    fn credential_id(&self) -> Option<String>;
    /// The bearer token, `None` to send no `authorization` header.
    async fn bearer(&self) -> Result<Option<String>, CredentialError>;
    /// The token with the account it belongs to; `None` when there is no credential at all.
    /// A source that knows no account (a plain API key) needs only [`Self::bearer`].
    async fn secret(&self) -> Result<Option<ProviderSecret>, CredentialError> {
        Ok(self.bearer().await?.map(|token| ProviderSecret {
            token,
            account_id: None,
            auth_selection: self.credential_id(),
            hosts: Default::default(),
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

    async fn bearer(&self) -> Result<Option<String>, CredentialError> {
        Ok(None)
    }
}

/// A credential read from the vault by grant, the way a session's run reads its own.
pub struct VaultCredential {
    pub client: Arc<broca_subc::VaultClient>,
    /// The credential's canonical id (for example `apikey:typesafe`), reported in
    /// `auth_failed`'s detail.
    pub credential_id: String,
    /// Fixed sources resolve a broca key from rows; routed sources already name a vault id.
    pub addressing: Option<crate::VaultAddressing>,
}

/// The shortest life a token may have left when it is handed out, so one provider attempt
/// never starts with a token about to expire.
const VAULT_MIN_TTL_MS: i64 = 60_000;

#[async_trait]
impl CredentialSource for VaultCredential {
    fn credential_id(&self) -> Option<String> {
        Some(self.credential_id.clone())
    }

    async fn bearer(&self) -> Result<Option<String>, CredentialError> {
        Ok(self.secret().await?.map(|secret| secret.token))
    }

    async fn secret(&self) -> Result<Option<ProviderSecret>, CredentialError> {
        let vault_id = match &self.addressing {
            Some(addressing) => {
                let key = crate::CredentialId::parse(&self.credential_id)
                    .ok_or_else(|| format!("invalid credential key {}", self.credential_id))?;
                addressing
                    .vault_id_for(&key)
                    .map_err(|error| match error {
                        broca_core::RunError::CredentialsUnavailable => {
                            CredentialError::Unavailable(error.to_string())
                        }
                        other => CredentialError::Refused(other.to_string()),
                    })?
                    .ok_or_else(|| format!("credential {} not found", self.credential_id))?
            }
            None => self.credential_id.clone(),
        };
        self.client
            .get_scoped(&vault_id, Some(VAULT_MIN_TTL_MS))
            .await
            .result
            .map(|token| {
                Some(ProviderSecret {
                    token: token.token,
                    account_id: token.account_id,
                    auth_selection: token
                        .resolved_credential_id
                        .or_else(|| Some(vault_id.clone())),
                    hosts: broca_provider::credential_hosts::CredentialHosts::from_record(
                        token.hosts.as_deref(),
                    ),
                })
            })
            .map_err(read_error)
    }
}

/// A failed vault read as System One answers it: a vault that couldn't answer now
/// (unreachable, locked, a refresh that failed transiently) is unavailable; every other answer
/// means this credential can't be used as it is.
fn read_error(error: broca_subc::VaultError) -> CredentialError {
    let message = error.to_string();
    match error {
        broca_subc::VaultError::RefreshFailed
        | broca_subc::VaultError::VaultLocked
        | broca_subc::VaultError::Transport(_) => CredentialError::Unavailable(message),
        _ => CredentialError::Refused(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_subc::VaultError;

    #[test]
    fn an_unavailable_credential_answers_as_unavailable_and_a_refused_one_as_refused() {
        use super::super::ProviderReply;
        assert!(matches!(
            ProviderReply::from(CredentialError::Unavailable("down".into())),
            ProviderReply::CredentialUnavailable(_)
        ));
        assert!(matches!(
            ProviderReply::from(CredentialError::Refused("revoked".into())),
            ProviderReply::Credential(_)
        ));
    }

    #[test]
    fn only_a_vault_that_cannot_answer_now_is_unavailable() {
        for error in [
            VaultError::RefreshFailed,
            VaultError::VaultLocked,
            VaultError::Transport("reset".into()),
        ] {
            assert!(matches!(read_error(error), CredentialError::Unavailable(_)));
        }
        for error in [
            VaultError::NeedsReauth,
            VaultError::NotFound,
            VaultError::Corrupt,
        ] {
            assert!(matches!(read_error(error), CredentialError::Refused(_)));
        }
    }
}
