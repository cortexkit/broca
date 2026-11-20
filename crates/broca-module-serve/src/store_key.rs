//! The new session storage's deployment key: read from the vault when the module starts, kept in
//! memory only, and used to open the new storage's runtime.
//!
//! Each deployment key is a static vault record, `opaque:broca-deployment:<key-id>`. The
//! operator grants `reserved:broca` an exact `read` on it. Its value is 32 random bytes written
//! as 64 lowercase hex characters. `session_storage.deployment_key_id` in `broca.jsonc` names the
//! key the module uses (see `config::SessionStorageConfig`).
//!
//! The module starts without waiting for the key. Until the key loads, an encrypted session kept
//! in the new storage is refused `key_unavailable`, a transient refusal, while plain sessions
//! kept there and today's sessions are served as before. The read is retried with backoff until
//! it succeeds, and the key is then installed with no restart. The key never reaches disk or a
//! log: only its id is logged or reported, and the vault's reply is never echoed, since a reply
//! that fails to decode can carry its value.

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
        let runtime_ready = match self.slot.open_without_key(&self.setup).await {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(target: "lifecycle", %error, "could not open store recovery runtime");
                false
            }
        };
        // Plain sessions need no vault. Encrypted candidates stay registered until the
        // second pass can read them; refusals from client traffic are not our inventory. The
        // first pass runs beside the key read rather than before it: it reads every stored head
        // of the durable class, and every encrypted session would wait for that walk to end.
        let first_pass = tokio::spawn({
            let registry = registry.clone();
            async move { recover_sessions(&registry).await }
        });
        let stop_deferred = || {
            tracing::info!(target: "lifecycle", "store recovery deferred until next startup: module stopping before deployment key recovery");
        };
        let loaded = async {
            if stopping.is_cancelled() {
                stop_deferred();
                return false;
            }
            let Some(source) = self.source.as_ref() else {
                warn_missing(key_id, KeyUnavailable::NoVault);
                return false;
            };
            let mut wait = self.backoff.first;
            let mut warned = false;
            loop {
                if stopping.is_cancelled() {
                    stop_deferred();
                    return false;
                }
                let read = tokio::select! {
                    biased;
                    _ = stopping.cancelled() => {
                        stop_deferred();
                        return false;
                    }
                    read = read_key(source.as_ref(), key_id) => read,
                };
                let loaded = match read {
                    Ok(key) => self
                        .slot
                        .load(&self.setup, key)
                        .await
                        .map_err(|_| KeyUnavailable::Open),
                    Err(reason) => Err(reason),
                };
                match loaded {
                    Ok(()) => return true,
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
                    _ = stopping.cancelled() => {
                        stop_deferred();
                        return false;
                    },
                    _ = tokio::time::sleep(wait) => {}
                }
                wait = (wait * 2).min(self.backoff.max);
            }
        }
        .await;
        // The first pass always finishes, key or no key: plain sessions need none. The second
        // pass waits for it, so the two never recover one session at once.
        let deferred = first_pass.await.unwrap_or(true) || !runtime_ready;
        if !loaded {
            return;
        }
        tracing::info!(
            target: "session_storage",
            key_id,
            "the new storage's deployment key {key_id} has loaded; sessions kept in the new \
             storage are served"
        );
        let refused = self.slot.take_refused();
        if stopping.is_cancelled() {
            stop_deferred();
            return;
        }
        if deferred {
            recover_sessions(&registry).await;
        }
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

#[cfg(test)]
mod recovery_tests;

/// Opens durable recovery candidates, not just sessions refused by commands during key loading.
async fn recover_sessions(registry: &broca_module::LineageRegistry) -> bool {
    match registry.recover_store_crash_orphans().await {
        Ok(sweep) => {
            let opened_for = sweep
                .opened_for
                .iter()
                .map(|(owed, count)| format!("{owed}={count}"))
                .collect::<Vec<_>>()
                .join(",");
            tracing::info!(target: "lifecycle", inspected = sweep.inspected,
                recovered = sweep.recovered, retired = sweep.retired, lease_held = sweep.lease_held,
                deferred = sweep.deferred, failed = sweep.failed.len(), %opened_for,
                warm_armed = sweep.warm_armed,
                "store restart resume candidate sweep complete");
            for error in sweep.failed {
                tracing::warn!(target: "lifecycle", %error, "could not recover a store session");
            }
            sweep.deferred != 0
        }
        Err(error) => {
            tracing::warn!(target: "lifecycle", %error, "store recovery could not list candidate sessions");
            true
        }
    }
}

fn warn_missing(key_id: u16, reason: KeyUnavailable) {
    tracing::warn!(
        target: "session_storage",
        key_id,
        reason = reason.as_str(),
        "the new storage's deployment key {key_id} is not loaded; encrypted sessions kept in the \
         new storage are refused key_unavailable until it loads"
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

/// `{key_id, loaded, new_sessions, phases}` for the published slot: which deployment key the new storage
/// opens encrypted sessions with, whether it has loaded, and the mode the sessions it creates
/// take (`"encrypted"` or `"plain"`), and process-lifetime storage/run-loop timings and seal
/// statistics. Null in a process that published none.
pub fn health() -> serde_json::Value {
    PUBLISHED
        .get()
        .and_then(|published| published.read().ok()?.clone())
        .map_or(serde_json::Value::Null, |slot| health_of(&slot))
}

/// Deployment-key state and process-lifetime storage/run-loop timings and seal statistics.
pub fn health_of(slot: &StoreSlot) -> serde_json::Value {
    serde_json::json!({
        "key_id": slot.key_id(),
        "loaded": slot.key_loaded(),
        "new_sessions": slot.new_sessions().label(),
        "phases": broca_session_store::metrics::snapshot(),
    })
}

/// The zero-work phase shape pinned by both key-state and served-health goldens.
#[cfg(test)]
pub(crate) fn empty_phases_golden() -> serde_json::Value {
    let mut phases = serde_json::json!({});
    for phase in [
        "open",
        "append",
        "sync",
        "seal",
        "media_wait",
        "request_prepare",
        "media_fold",
        "read",
        "entries_page",
        "collection",
        "collection_sweep",
    ] {
        phases[phase] = serde_json::json!({"count": 0, "total_us": 0, "max_us": 0, "last_us": 0});
    }
    phases["seal"].as_object_mut().unwrap().extend(
        serde_json::json!({
            "transcript_records": 0, "ledger_records": 0, "kept_records": 0,
            "transcript_bytes": 0, "ledger_bytes": 0, "index_entries": 0,
            "merged_entries": 0, "forced_merges": 0,
        })
        .as_object()
        .unwrap()
        .clone(),
    );
    phases
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

    /// Health names the mode new sessions take, as the slot was configured.
    #[test]
    fn health_names_the_mode_of_new_sessions() {
        const CHILD_TEST: &str = "BROCA_ISOLATED_STORE_HEALTH_TEST";
        const TEST: &str = "store_key::tests::health_names_the_mode_of_new_sessions";
        if std::env::var(CHILD_TEST).as_deref() != Ok(TEST) {
            // Other tests can finish real storage work before this golden reads lifetime metrics.
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
                .env(CHILD_TEST, TEST)
                .stdin(std::process::Stdio::null())
                .output()
                .expect("spawn the isolated store health test");
            assert!(
                output.status.success()
                    && String::from_utf8_lossy(&output.stdout).contains("1 passed"),
                "store health golden failed: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        use broca_session_store::keys::Cipher;
        let root = std::path::Path::new("/unused");
        let phases = empty_phases_golden();
        assert_eq!(
            health_of(&StoreSlot::awaiting_key(root, 2)),
            serde_json::json!({"key_id": 2, "loaded": false, "new_sessions": "encrypted", "phases": phases})
        );
        assert_eq!(
            health_of(&StoreSlot::awaiting_key(root, 2).creating(Cipher::None)),
            serde_json::json!({"key_id": 2, "loaded": false, "new_sessions": "plain", "phases": phases})
        );
        publish(Arc::new(StoreSlot::awaiting_key(root, 2)));
        assert_eq!(
            health(),
            serde_json::json!({"key_id": 2, "loaded": false, "new_sessions": "encrypted", "phases": phases})
        );
    }
}
