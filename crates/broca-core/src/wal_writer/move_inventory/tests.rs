use super::*;

#[tokio::test]
async fn panic_finishes_inventory_incomplete() {
    let store = FileWalStore::new(PathBuf::from("unused-inventory-panic"));
    store.run_inventory_build(|| panic!("injected inventory panic"));
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        store.wait_move_inventory(),
    )
    .await
    .expect("panic must release inventory waiters");
    assert_eq!(store.move_inventory_health(), (true, 1));
    assert!(store.move_session_refs(std::iter::empty()).unreadable[0].contains("panicked"));
}

#[tokio::test]
async fn cancellation_finishes_inventory_incomplete() {
    let store = Arc::new(FileWalStore::new(PathBuf::from("unused-inventory-cancel")));
    let cancel = CancellationToken::new();
    cancel.cancel();
    store.start_move_inventory(cancel);
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        store.wait_move_inventory(),
    )
    .await
    .expect("cancellation must release inventory waiters");
    let (ready, unreadable) = store.move_inventory_health();
    assert!(ready);
    assert!(unreadable > 0);
    assert!(!store
        .move_session_refs(std::iter::empty())
        .unreadable
        .is_empty());
}
