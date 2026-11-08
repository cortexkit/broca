//! The new session storage's deployment key: read from the vault when the module starts, kept in
//! memory only, and used to open the new storage's runtime.
//!
//! Each deployment key is a static vault record, `opaque:broca-deployment:<key-id>`. The
//! operator grants `reserved:broca` an exact `read` on it. Its value is 32 random bytes written
//! as 64 lowercase hex characters. `session_storage.deployment_key_id` in `broca.jsonc` names the
//! key the module uses (see `config::SessionStorageConfig`).
//!
//! The module starts without waiting for the key. Until the key loads, a session kept in the new
//! storage is refused `key_unavailable`, a transient refusal, while today's sessions are served
//! as before. The read is retried with backoff until it succeeds, and the runtime is then
//! installed with no restart. The key never reaches disk or a log: only its id is logged or
//! reported, and the vault's reply is never echoed, since a reply that fails to decode can
//! carry its value.

use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use broca_module::store_open::{StoreRuntimeSetup, StoreSlot};
use broca_session_store::keys::DeploymentKey;

/// The key id the module uses when `broca.jsonc` names none.
pub const DEFAULT_KEY_ID: u16 = 1;

/// The vault record that holds deployment key `key_id`.
pub fn credential_id(key_id: u16) -> String {
    format!("opaque:broca-deployment:{key_id}")
}

/// Why the deployment key isn't loaded. Says nothing of the key's value, and carries no text the
/// vault sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyUnavailable {
    /// The module reads no vault, so the key can't be read.
    NoVault,
    /// The vault holds no such record, or broca holds no read grant on it: the vault answers both
    /// the same way.
    NotFound,
    /// No answer came from the vault: it isn't running, isn't registered yet, or its route
    /// failed.
    Unreachable,
    /// This process has no supervised consumer identity, so no grant covers its read.
    NoConsumerIdentity,
    /// The vault refused the read some other way, for example while it is locked.
    Refused,
    /// The record's value isn't 64 lowercase hex characters.
    Malformed,
    /// The new storage couldn't be opened with the key: its setup parts didn't load.
    Open,
}

impl KeyUnavailable {
    /// The reason as logs name it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoVault => "no_vault",
            Self::NotFound => "not_found",
            Self::Unreachable => "unreachable",
            Self::NoConsumerIdentity => "no_consumer_identity",
            Self::Refused => "refused",
            Self::Malformed => "malformed",
            Self::Open => "open_failed",
        }
    }
}

/// The 32 bytes `value` encodes: exactly 64 lowercase hex characters. Anything else is
/// `Malformed`; the error never repeats the value.
pub fn decode_key(value: &str) -> Result<[u8; 32], KeyUnavailable> {
    let digits = value.as_bytes();
    if digits.len() != 64
        || !digits
            .iter()
            .all(|d| matches!(d, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(KeyUnavailable::Malformed);
    }
    let nibble = |digit: u8| match digit {
        b'0'..=b'9' => digit - b'0',
        _ => digit - b'a' + 10,
    };
    let mut key = [0u8; 32];
    let (pairs, _) = digits.as_chunks::<2>();
    for (byte, [high, low]) in key.iter_mut().zip(pairs) {
        *byte = (nibble(*high) << 4) | nibble(*low);
    }
    Ok(key)
}

/// Where the deployment key is read from: the vault, by grant (`credential.get_scoped`).
#[async_trait]
pub trait DeploymentKeySource: Send + Sync {
    async fn read(&self, credential_id: &str) -> broca_subc::VaultGet;
}

#[async_trait]
impl DeploymentKeySource for broca_subc::VaultClient {
    async fn read(&self, credential_id: &str) -> broca_subc::VaultGet {
        // A static record: nothing is minted or refreshed, so no lifetime is asked for.
        self.get_scoped(credential_id, None).await
    }
}

/// Reads deployment key `key_id` once. A missing record or grant is `NotFound`: a state of the
/// deployment, not a credential that went bad, so nothing is reported to the vault about it.
pub async fn read_key(
    source: &dyn DeploymentKeySource,
    key_id: u16,
) -> Result<DeploymentKey, KeyUnavailable> {
    use broca_subc::VaultError;
    let read = source.read(&credential_id(key_id)).await;
    let token = match read.result {
        Ok(token) => token,
        Err(VaultError::NotFound) => return Err(KeyUnavailable::NotFound),
        Err(VaultError::NoConsumerIdentity { .. }) => {
            return Err(KeyUnavailable::NoConsumerIdentity)
        }
        Err(_) if !read.vault_replied => return Err(KeyUnavailable::Unreachable),
        // The vault answered with something that doesn't decode, such as a payload that isn't
        // text: the record's value is malformed.
        Err(VaultError::Protocol(_)) => return Err(KeyUnavailable::Malformed),
        Err(_) => return Err(KeyUnavailable::Refused),
    };
    Ok(DeploymentKey::new(key_id, &decode_key(&token.token)?))
}

/// How long the loader waits between reads that didn't load the key: `first`, doubling up to
/// `max`.
#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    pub first: Duration,
    pub max: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            first: Duration::from_secs(1),
            max: Duration::from_secs(60),
        }
    }
}

/// Loads the deployment key into `slot` and installs the new storage's runtime there.
pub struct KeyLoader {
    /// `None` when the module reads no vault, so the key can't load.
    pub source: Option<Arc<dyn DeploymentKeySource>>,
    pub slot: Arc<StoreSlot>,
    pub setup: StoreRuntimeSetup,
    pub backoff: Backoff,
}

impl KeyLoader {
    /// Reads the key, at once and then with backoff until it loads or the module stops. It logs
    /// one WARN while the key is missing and one INFO when it arrives, each naming the key id
    /// only. Once the key has loaded, every session refused meanwhile is opened, so the recovery
    /// its store actor runs on open (a crashed run sealed, an owed run billed, a queued send
    /// drained) runs then, not only at that session's next command.
    pub async fn run(self, registry: broca_module::LineageRegistry) {
        let key_id = self.slot.key_id();
        let stopping = registry.stopping_signal();
        let Some(source) = self.source else {
            warn_missing(key_id, KeyUnavailable::NoVault);
            return;
        };
        let mut wait = self.backoff.first;
        let mut warned = false;
        loop {
            let loaded = match read_key(source.as_ref(), key_id).await {
                Ok(key) => self
                    .slot
                    .load(&self.setup, key)
                    .await
                    .map_err(|_| KeyUnavailable::Open),
                Err(reason) => Err(reason),
            };
            match loaded {
                Ok(()) => break,
                Err(reason) if !warned => {
                    warn_missing(key_id, reason);
                    warned = true;
                }
                Err(reason) => tracing::debug!(
                    target: "session_storage",
                    key_id,
                    reason = reason.as_str(),
                    "the new storage's deployment key is still not loaded"
                ),
            }
            tokio::select! {
                biased;
                _ = stopping.cancelled() => return,
                _ = tokio::time::sleep(wait) => {}
            }
            wait = (wait * 2).min(self.backoff.max);
        }
        tracing::info!(
            target: "session_storage",
            key_id,
            "the new storage's deployment key {key_id} has loaded; sessions kept in the new \
             storage are served"
        );
        let refused = self.slot.take_refused();
        if refused.is_empty() {
            return;
        }
        let (opened, failed) = registry.open_registered_sessions(refused, 4).await;
        tracing::info!(
            target: "session_storage",
            opened,
            failed = failed.len(),
            "opened the sessions refused before the deployment key loaded, so their recovery ran"
        );
        for (session, error) in failed {
            tracing::warn!(
                target: "session_storage",
                session = %session.session,
                "a session refused before the deployment key loaded could not be opened: {error}"
            );
        }
    }
}

fn warn_missing(key_id: u16, reason: KeyUnavailable) {
    tracing::warn!(
        target: "session_storage",
        key_id,
        reason = reason.as_str(),
        "the new storage's deployment key {key_id} is not loaded; sessions kept in the new \
         storage are refused key_unavailable until it loads"
    );
}

/// The slot health reports on, once the module has published one.
static PUBLISHED: OnceLock<RwLock<Option<Arc<StoreSlot>>>> = OnceLock::new();

/// Makes `slot` the one health reports on.
pub fn publish(slot: Arc<StoreSlot>) {
    *PUBLISHED
        .get_or_init(RwLock::default)
        .write()
        .unwrap_or_else(|e| e.into_inner()) = Some(slot);
}

/// `{key_id, loaded}` for the published slot: which deployment key the new storage is opened
/// with, and whether it has loaded. Null in a process that published none.
pub fn health() -> serde_json::Value {
    PUBLISHED
        .get()
        .and_then(|published| published.read().ok()?.clone())
        .map_or(serde_json::Value::Null, |slot| health_of(&slot))
}

/// `{key_id, loaded}` for `slot`.
pub fn health_of(slot: &StoreSlot) -> serde_json::Value {
    serde_json::json!({"key_id": slot.key_id(), "loaded": slot.key_loaded()})
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEX: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    #[test]
    fn a_key_is_exactly_64_lowercase_hex_characters() {
        let key = decode_key(HEX).unwrap();
        assert_eq!(key, std::array::from_fn::<u8, 32, _>(|i| i as u8));
        assert_eq!(decode_key(&"ff".repeat(32)).unwrap(), [0xff; 32]);
        for malformed in [
            String::new(),
            HEX[..62].to_string(),
            format!("{HEX}00"),
            HEX.to_uppercase(),
            format!("{}\n", &HEX[..63]),
            format!(" {}", &HEX[1..]),
            HEX.replacen('a', "g", 1),
            "é".repeat(32),
        ] {
            assert_eq!(decode_key(&malformed), Err(KeyUnavailable::Malformed));
        }
    }

    #[test]
    fn the_record_is_named_by_the_key_id() {
        assert_eq!(credential_id(1), "opaque:broca-deployment:1");
        assert_eq!(credential_id(DEFAULT_KEY_ID), "opaque:broca-deployment:1");
        assert_eq!(credential_id(12), "opaque:broca-deployment:12");
    }
}
