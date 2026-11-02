use std::path::Path;

use broca_core::SessionRefIndex;
use broca_protocol::BindIdentity;
use broca_store::SessionAddressResolver;
use broca_wal::state_format::decide_state_root;

/// Resolve a rig's storage identity through the same inventory used by serving.
/// Incomplete inventory remains an error: a reader must not guess another WAL address.
pub fn resolve_session_address(root: &Path, claim: &BindIdentity) -> Result<BindIdentity, String> {
    with_inventory(root, |resolver, _| {
        resolver
            .resolve_session_address(claim, None)
            .map_err(|error| error.to_string())
    })
}

/// Enroll an authored legacy seed before writing or moving its WAL.
/// The inventory preserves that exact identity even when the file is later `.deleting`,
/// or when a fixture adds a seed after the daemon's startup census has finished.
pub fn register_legacy_session(root: &Path, seed: &BindIdentity) -> Result<BindIdentity, String> {
    with_inventory(root, |resolver, index| {
        index.record_session(&broca_core::session_addr(seed), seed)?;
        resolver
            .resolve_session_address(seed, None)
            .map_err(|error| error.to_string())
    })
}

fn with_inventory<T>(
    root: &Path,
    operation: impl FnOnce(&dyn SessionAddressResolver, &dyn SessionRefIndex) -> Result<T, String>,
) -> Result<T, String> {
    std::fs::create_dir_all(root).map_err(|error| error.to_string())?;
    let decision = decide_state_root(root).map_err(|error| error.to_string())?;
    if let Some(legacy) = decision.legacy() {
        let store = broca_store::SqliteStore::open_boot_legacy(legacy)
            .map_err(|error| error.to_string())?;
        operation(&store, &store)
    } else {
        let store = broca_store::db::OverhaulDb::open(root).map_err(|error| error.to_string())?;
        operation(&store, &store)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(name: &str) -> std::path::PathBuf {
        let root = crate::scratch_root().join(name);
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn unscoped_address_comes_from_the_inventory_not_the_claim_folder() {
        let root = root("address-unscoped");
        let store = broca_store::SqliteStore::open(root.join("run-index.db")).unwrap();
        broca_core::SessionInventory::rebuild_inventory(&store, &root).unwrap();
        let claim = BindIdentity::new(root.join("project"), "harness", "session");
        let resolved = resolve_session_address(&root, &claim).unwrap();
        assert_eq!(resolved.project_root, Path::new("broca-unscoped-v1"));
        assert_eq!(resolved.harness, "harness");
        assert_eq!(resolved.session, "session");
        let other_claim = BindIdentity::new(root.join("other"), "harness", "session");
        assert_eq!(
            resolve_session_address(&root, &other_claim).unwrap(),
            resolved
        );
    }

    #[test]
    fn legacy_seed_keeps_its_inventoried_address_without_a_live_wal() {
        let root = root("address-legacy");
        let seed = BindIdentity::new(root.join("legacy"), "harness", "session");
        assert_eq!(register_legacy_session(&root, &seed).unwrap(), seed);
        let claim = BindIdentity::new(root.join("other"), "harness", "session");
        assert_eq!(resolve_session_address(&root, &claim).unwrap(), seed);
        let path = broca_core::FileWalStore::new(root.join("wal")).wal_path(&seed);
        assert!(!path.exists());
    }

    #[test]
    fn incomplete_inventory_cannot_reserve_a_guessed_address() {
        let root = root("address-incomplete");
        let claim = BindIdentity::new(root.join("project"), "harness", "session");
        assert!(resolve_session_address(&root, &claim).is_err());
    }

    #[test]
    fn ambiguous_legacy_roots_are_not_chosen_by_the_helper() {
        let root = root("address-ambiguous");
        let first = BindIdentity::new(root.join("first"), "harness", "session");
        let second = BindIdentity::new(root.join("second"), "harness", "session");
        register_legacy_session(&root, &first).unwrap();
        assert!(register_legacy_session(&root, &second).is_err());
        assert!(resolve_session_address(&root, &first).is_err());
    }

    #[test]
    fn marked_roots_use_the_selected_inventory_not_a_legacy_index() {
        let root = root("address-overhaul");
        std::fs::write(root.join("state-format"), b"broca-state/2\n").unwrap();
        let seed = BindIdentity::new(root.join("legacy"), "harness", "session");
        assert_eq!(register_legacy_session(&root, &seed).unwrap(), seed);
        assert_eq!(resolve_session_address(&root, &seed).unwrap(), seed);
        assert!(root.join("broca.db").exists());
        assert!(!root.join("run-index.db").exists());
    }
}
