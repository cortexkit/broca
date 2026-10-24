//! Capability-handle credential reads for foreground developer tools, not the daemon.

use std::{path::Path, sync::Arc, time::Duration};

use broca_protocol::BindIdentity;

use crate::{vault_config::load_devtools_handle, CredentialId};

/// Read a fresh vault token using the developer's explicitly provisioned handle.
/// Errors deliberately omit transport and payload details, which may contain secrets.
pub async fn credential(
    credential: &CredentialId,
    subc: &Path,
    identity: &BindIdentity,
) -> Result<String, String> {
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
    if secret.token.is_empty() {
        return Err("developer vault credential is empty".into());
    }
    Ok(secret.token)
}
