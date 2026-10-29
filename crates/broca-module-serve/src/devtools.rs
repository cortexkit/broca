//! Capability-handle credential reads for foreground developer tools, not the daemon.

use std::{path::Path, sync::Arc, time::Duration};

use broca_protocol::BindIdentity;

use crate::{vault_config::load_devtools_handle, CredentialId};

/// The secret and optional account identity returned by the same vault read. Deliberately
/// neither `Debug` nor `Serialize`: developer probes must not print credentials or account data.
pub struct DevtoolsCredential {
    pub token: String,
    pub account_id: Option<String>,
}

/// Read a fresh vault token using the developer's explicitly provisioned handle.
/// Errors deliberately omit transport and payload details, which may contain secrets.
pub async fn credential(
    credential: &CredentialId,
    subc: &Path,
    identity: &BindIdentity,
) -> Result<String, String> {
    credential_with_metadata(credential, subc, identity)
        .await
        .map(|secret| secret.token)
}

/// Preserve the vault's post-refresh account identity for callers that prepare a provider
/// persona. The token-only entry point remains available to callers that do not need it.
pub async fn credential_with_metadata(
    credential: &CredentialId,
    subc: &Path,
    identity: &BindIdentity,
) -> Result<DevtoolsCredential, String> {
    let handle = load_devtools_handle(credential)?;
    let connection = broca_subc::SubcConnection::connect(subc)
        .await
        .map_err(|_| "connect developer credential consumer to subc failed".to_owned())?;
    let vault = broca_subc::VaultClient::new(
        Arc::new(connection),
        "claustrum",
        identity,
        Duration::from_secs(15),
    );
    let secret = vault
        .get(&handle, Some(60_000), false)
        .await
        .result
        .map_err(|_| "developer vault credential read failed".to_owned())?;
    from_vault(secret)
}

fn from_vault(secret: broca_subc::VaultToken) -> Result<DevtoolsCredential, String> {
    if secret.token.is_empty() {
        return Err("developer vault credential is empty".into());
    }
    Ok(DevtoolsCredential {
        token: secret.token,
        account_id: secret.account_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(account_id: Option<&str>) -> broca_subc::VaultToken {
        broca_subc::VaultToken {
            token: "synthetic-token".into(),
            expires_at_ms: Some(1_000),
            record_version: 7,
            project_id: None,
            account_id: account_id.map(str::to_owned),
            resolved_credential_id: Some("oauth:anthropic".into()),
        }
    }

    #[test]
    fn credential_metadata_preserves_account_identity_from_one_vault_result() {
        for account in [
            None,
            Some("00000000-0000-4000-8000-000000000001"),
            Some("opaque-account-id"),
        ] {
            let secret = from_vault(token(account))
                .unwrap_or_else(|_| panic!("synthetic credential rejected"));
            assert_eq!(secret.token, "synthetic-token");
            assert_eq!(secret.account_id.as_deref(), account);
        }
    }

    #[test]
    fn metadata_does_not_make_an_empty_token_usable() {
        let mut empty = token(Some("opaque-account-id"));
        empty.token.clear();
        assert_eq!(
            from_vault(empty).err().as_deref(),
            Some("developer vault credential is empty")
        );
    }
}
