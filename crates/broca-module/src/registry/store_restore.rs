//! What restoring sessions from a backup needs from the registry: a place for the daemon to
//! install the restore (`store_restore` in `broca-module-serve`), which runs
//! `session.restore_from_backup`. The restore holds a session it repairs with
//! [`LineageRegistry::fence_for_restore`]: no actor serves it and no other owner holds its lease
//! while restore writes.

use std::sync::Arc;

use super::store_move::MoveRefusal;
use super::{LineageKey, LineageRegistry};

/// What runs `session.restore_from_backup` for the registry's sessions.
#[async_trait::async_trait]
pub trait StoreRestorer: Send + Sync {
    /// Restores the session `key` from the files its store holds after a backup came back, or,
    /// with `all`, every session kept in the session store; `key` is then only the route the
    /// operator called on. Returns one report per session; a refusal stops before any session.
    async fn restore_from_backup(
        &self,
        key: &LineageKey,
        all: bool,
    ) -> Result<serde_json::Value, MoveRefusal>;
}

impl LineageRegistry {
    /// Installs what runs `session.restore_from_backup`. Only the first installation counts.
    pub fn set_store_restorer(&self, restorer: Arc<dyn StoreRestorer>) {
        let _ = self.inner.store_restorer.set(restorer);
    }

    /// Runs `session.restore_from_backup` for `key`, or for every session kept in the new
    /// storage with `all`, or refuses `store_unavailable` when this module keeps no sessions in
    /// the session store.
    pub async fn restore_from_backup(
        &self,
        key: &LineageKey,
        all: bool,
    ) -> Result<serde_json::Value, MoveRefusal> {
        match self.inner.store_restorer.get() {
            Some(restorer) => restorer.restore_from_backup(key, all).await,
            None => Err(MoveRefusal::new(
                "store_unavailable",
                "this module keeps no sessions in the new storage",
            )),
        }
    }
}
