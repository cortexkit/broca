//! Legacy inventory adapters shared by serving and read-only tools.

use broca_core::{RecoveryDiscovery, SessionInventory, SessionRefResolver, SessionRefs};
use broca_protocol::BindIdentity;
use std::path::Path;

impl SessionRefResolver for crate::SqliteStore {
    fn resolve_refs(&self, refs: &[&str]) -> Result<SessionRefs, String> {
        self.session_refs(refs.iter().copied())
            .map_err(|e| e.to_string())
    }
    fn named_refs(&self, name: &str, limit: u32) -> Result<SessionRefs, String> {
        self.session_refs_named(name, limit)
            .map_err(|e| e.to_string())
    }
}

impl SessionInventory for crate::SqliteStore {
    fn prepare_inventory(&self, root: &Path) -> Result<(), String> {
        crate::session_refs::cleanup_legacy_inventory(root)
            .map(|_| ())
            .map_err(|e| format!("cleanup_legacy_inventory: {e}"))
    }
    fn rebuild_inventory(&self, root: &Path) -> Result<(), String> {
        crate::session_refs::migrate(
            &root.join("run-index.db"),
            &broca_core::ReadOnlyWalReader::new(root),
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
    }
    fn retained_sessions(&self) -> Result<Vec<BindIdentity>, String> {
        crate::SqliteStore::retained_sessions(self).map_err(|e| e.to_string())
    }
    fn forget_projection(&self, session: &BindIdentity) -> Result<(), String> {
        self.drop_session_projection(session)
    }
}

impl RecoveryDiscovery for crate::SqliteStore {
    fn withdrawal_candidates(&self) -> Result<Vec<broca_core::WithdrawalRun>, String> {
        broca_core::RunIndex::runs_with_withdrawals_owed(self)
    }
    fn active_runs(&self) -> Result<Vec<(String, BindIdentity)>, String> {
        crate::SqliteStore::active_runs(self).map_err(|e| e.to_string())
    }
    fn restart_resume_candidates(&self) -> Result<Vec<(String, BindIdentity)>, String> {
        crate::SqliteStore::restart_resume_candidates(self).map_err(|e| e.to_string())
    }
}

/// Unsupported markers never fall through to the legacy inventory, even read-only.
pub fn open_session_resolver(root: &Path) -> Result<Box<dyn SessionRefResolver>, String> {
    crate::diagnostics::operational_database_name(root)?;
    crate::SqliteStore::open_read_only(root.join("run-index.db"))
        .map(|db| Box::new(db) as Box<dyn SessionRefResolver>)
        .map_err(|e| e.to_string())
}
