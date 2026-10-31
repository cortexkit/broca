/// Give a synthetic serve fixture its own complete, empty address inventory.
/// Fixtures exercising existing WALs supply their real index instead.
pub fn test_registry(registry: broca_module::LineageRegistry) -> broca_module::LineageRegistry {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = broca_test_support::scratch_root().join(format!(
        "serve-address-inventory-{}-{}-{}",
        module_path!().replace("::", "-"),
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = root.join("run-index.db");
    let inventory = std::sync::Arc::new(broca_store::SqliteStore::open(&db).unwrap());
    broca_store::session_refs::migrate(&db, &broca_core::ReadOnlyWalReader::new(&root)).unwrap();
    registry.set_session_address_inventory(inventory);
    registry
}
