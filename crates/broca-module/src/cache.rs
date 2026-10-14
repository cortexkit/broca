//! The prompt-cache facts a session reports: what its frozen config declares, and the
//! per-session clock that says when broca last refreshed the provider's cached prefix.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use broca_core::{CacheClock, CacheClockReading};
use broca_protocol::{BindIdentity, RunId};

/// What a session's frozen config declares about its prompt cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheDeclaration {
    pub ttl_ms: Option<u64>,
    pub ttl_options_ms: Vec<u64>,
    pub read_refreshes_ttl: Option<bool>,
    /// `Ok` when the session can be warmed, else why not.
    pub warm: Result<(), String>,
    pub basis: Option<String>,
}

impl CacheDeclaration {
    pub fn to_wire(&self) -> broca_wire::CacheDeclarationWire {
        broca_wire::CacheDeclarationWire {
            ttl_ms: self.ttl_ms,
            ttl_options_ms: self.ttl_options_ms.clone(),
            read_refreshes_ttl: self.read_refreshes_ttl,
            warm: Some(
                match self.warm {
                    Ok(()) => broca_wire::WARM_SUPPORTED,
                    Err(_) => broca_wire::WARM_UNSUPPORTED,
                }
                .to_owned(),
            ),
            basis: self.basis.clone(),
        }
    }
}

/// Reads what a frozen episode config declares about its prompt cache. Installed by the serving
/// layer, which knows the provider families; without one every session's cache is unknown and
/// no warm is supported.
pub trait CacheDeclarer: Send + Sync {
    /// `None` when the config cannot be read as any family's.
    fn declare(&self, config: &broca_wal::RunConfig) -> Option<CacheDeclaration>;
}

/// Target capacity for cache facts. Shared clocks may exceed this target because
/// forgetting a live clock would disconnect session.head from subsequent updates.
const MAX_SESSIONS: usize = 65_536;
const EVICTION_SCAN_LIMIT: usize = 256;

/// One session's cache facts held in memory.
#[derive(Default)]
struct SessionCache {
    clock: CacheClock,
    /// The declaration of the episode that admitted the session, with that episode's run id,
    /// for the reply to the admitting send.
    admitted: Option<(RunId, CacheDeclaration)>,
}

#[derive(Default)]
struct Sessions {
    entries: HashMap<BindIdentity, SessionCache>,
    eviction_order: VecDeque<BindIdentity>,
}

/// Every session's cache facts, shared by the registry and its actors.
#[derive(Default)]
pub(crate) struct CacheTable {
    sessions: Mutex<Sessions>,
    declarer: std::sync::OnceLock<Arc<dyn CacheDeclarer>>,
}

impl CacheTable {
    fn with_session<T>(&self, key: &BindIdentity, f: impl FnOnce(&mut SessionCache) -> T) -> T {
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if !sessions.entries.contains_key(key) {
            // Bound eviction work per insertion. Live entries rotate to the back;
            // if all inspected clocks are shared, retain them and retry next insertion.
            let scans = sessions.eviction_order.len().min(EVICTION_SCAN_LIMIT);
            for _ in 0..scans {
                if sessions.entries.len() < MAX_SESSIONS {
                    break;
                }
                let oldest = sessions
                    .eviction_order
                    .pop_front()
                    .expect("queued cache entry");
                if sessions
                    .entries
                    .get(&oldest)
                    .is_some_and(|entry| entry.clock.is_shared())
                {
                    sessions.eviction_order.push_back(oldest);
                } else {
                    sessions.entries.remove(&oldest);
                }
            }
            sessions.eviction_order.push_back(key.clone());
        }
        f(sessions.entries.entry(key.clone()).or_default())
    }

    pub(crate) fn set_declarer(&self, declarer: Arc<dyn CacheDeclarer>) {
        let _ = self.declarer.set(declarer);
    }

    pub(crate) fn has_declarer(&self) -> bool {
        self.declarer.get().is_some()
    }

    pub(crate) fn declare(&self, config: &broca_wal::RunConfig) -> Option<CacheDeclaration> {
        self.declarer.get()?.declare(config)
    }

    /// Remove facts when the session's lineage is deleted.
    pub(crate) fn remove(&self, key: &BindIdentity) {
        let mut sessions = self.sessions.lock().unwrap();
        sessions.entries.remove(key);
        sessions.eviction_order.retain(|queued| queued != key);
    }

    /// The session's clock, created on first use; clones share it.
    pub(crate) fn clock(&self, key: &BindIdentity) -> CacheClock {
        self.with_session(key, |session| session.clock.clone())
    }

    /// The session's clock reading, without creating an entry for a session never seen.
    pub(crate) fn reading(&self, key: &BindIdentity) -> Option<CacheClockReading> {
        self.sessions
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .entries
            .get(key)
            .and_then(|session| session.clock.read())
    }

    pub(crate) fn note_admitted(&self, key: &BindIdentity, run_id: RunId, decl: CacheDeclaration) {
        self.with_session(key, |session| session.admitted = Some((run_id, decl)));
    }

    pub(crate) fn admitted(&self, key: &BindIdentity, run_id: &RunId) -> Option<CacheDeclaration> {
        self.sessions
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .entries
            .get(key)
            .and_then(|session| session.admitted.as_ref())
            .filter(|(admitted, _)| admitted == run_id)
            .map(|(_, decl)| decl.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_evicts_only_inactive_clocks_and_reclaims_after_owners_drop() {
        let table = CacheTable::default();
        let mut held = Vec::new();
        for n in 0..MAX_SESSIONS {
            let key = BindIdentity::new("/cache", "runner", n.to_string());
            held.push((key.clone(), table.clock(&key)));
        }
        let extra = BindIdentity::new("/cache", "runner", "extra");
        drop(table.clock(&extra));
        assert_eq!(
            table.sessions.lock().unwrap().entries.len(),
            MAX_SESSIONS + 1
        );
        for (key, clock) in &held {
            clock.note_sent(123, None);
            assert_eq!(table.reading(key).unwrap().last_sent_at_ms, 123);
        }
        drop(held);
        let next = BindIdentity::new("/cache", "runner", "next");
        drop(table.clock(&next));
        let sessions = table.sessions.lock().unwrap();
        assert_eq!(sessions.entries.len(), MAX_SESSIONS);
        assert_eq!(sessions.eviction_order.len(), sessions.entries.len());
        drop(sessions);
        table.remove(&next);
        let sessions = table.sessions.lock().unwrap();
        assert_eq!(sessions.eviction_order.len(), sessions.entries.len());
        assert!(!sessions.eviction_order.contains(&next));
    }
}
