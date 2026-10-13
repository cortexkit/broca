//! The prompt-cache facts a session reports: what its frozen config declares, and the
//! per-session clock that says when broca last refreshed the provider's cached prefix.

use std::collections::HashMap;
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

/// The most sessions whose cache facts are kept at once; past it the table starts over. A
/// forgotten session is then in the same state as after a restart: its clock is unknown until
/// it sends again, or until a `session.warm` or automatic keep-warm rebuilds it from the WAL.
const MAX_SESSIONS: usize = 65_536;

/// One session's cache facts held in memory.
#[derive(Default)]
struct SessionCache {
    clock: CacheClock,
    /// The declaration of the episode that admitted the session, with that episode's run id,
    /// for the reply to the admitting send.
    admitted: Option<(RunId, CacheDeclaration)>,
}

/// Every session's cache facts, shared by the registry and its actors.
#[derive(Default)]
pub(crate) struct CacheTable {
    sessions: Mutex<HashMap<BindIdentity, SessionCache>>,
    declarer: std::sync::OnceLock<Arc<dyn CacheDeclarer>>,
}

impl CacheTable {
    fn with_session<T>(&self, key: &BindIdentity, f: impl FnOnce(&mut SessionCache) -> T) -> T {
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if !sessions.contains_key(key) && sessions.len() >= MAX_SESSIONS {
            sessions.clear();
        }
        f(sessions.entry(key.clone()).or_default())
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

    /// The session's clock, created on first use; clones share it.
    pub(crate) fn remove(&self, key: &BindIdentity) {
        self.sessions.lock().unwrap().remove(key);
    }

    pub(crate) fn clock(&self, key: &BindIdentity) -> CacheClock {
        self.with_session(key, |session| session.clock.clone())
    }

    /// The session's clock reading, without creating an entry for a session never seen.
    pub(crate) fn reading(&self, key: &BindIdentity) -> Option<CacheClockReading> {
        self.sessions
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
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
            .get(key)
            .and_then(|session| session.admitted.as_ref())
            .filter(|(admitted, _)| admitted == run_id)
            .map(|(_, decl)| decl.clone())
    }
}
