//! Legacy inventory adapters shared by serving and read-only tools.

use broca_core::{SessionInventory, SessionRefResolver, SessionRefs};
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
    fn rebuild_inventory(&self, root: &Path) -> Result<(), String> {
        crate::session_refs::migrate(
            &root.join("run-index.db"),
            &crate::session_refs::StoredSessions::new(root),
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
    }
}

/// Refuses a state root marked with a format this build doesn't serve, so its legacy index is
/// never opened, even read-only.
fn refuse_marked_root(root: &Path) -> Result<(), String> {
    match std::fs::read(root.join("state-format")) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(bytes) => Err(format!(
            "unsupported state-format marker {:?}; start with a fresh state root",
            String::from_utf8_lossy(&bytes[..bytes.len().min(64)])
        )),
        Err(error) => Err(error.to_string()),
    }
}

/// Unsupported markers never fall through to the legacy inventory, even read-only.
pub fn open_session_resolver(root: &Path) -> Result<Box<dyn SessionRefResolver>, String> {
    refuse_marked_root(root)?;
    crate::SqliteStore::open_read_only(root.join("run-index.db"))
        .map(|db| Box::new(db) as Box<dyn SessionRefResolver>)
        .map_err(|e| e.to_string())
}
