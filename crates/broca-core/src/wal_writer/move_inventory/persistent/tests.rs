use super::*;

struct Rig {
    root: PathBuf,
    store: FileWalStore,
}
impl Rig {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "inventory-census-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(root.join("wal")).unwrap();
        Self {
            store: FileWalStore::new(root.join("wal")),
            root,
        }
    }
    fn fold(&self, address: &str, sealed: bool) {
        let dir = self.root.join("wal-archive");
        std::fs::create_dir_all(&dir).unwrap();
        let mut writer = broca_wal::archive::ArchiveWriter::create(dir.join("fold-1.ark"))
            .unwrap()
            .without_feature_floor();
        writer.append(address, &[]).unwrap();
        if sealed {
            writer.seal().unwrap();
        }
    }
}
impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn fold_between_listings_keeps_the_address() {
    let rig = Rig::new();
    let address = "0123456789abcdef";
    let live = rig.root.join("wal").join(format!("{address}.wal"));
    std::fs::write(&live, []).unwrap();
    let mut unreadable = Vec::new();
    let sources = locations_after_live(
        &rig.store,
        &CancellationToken::new(),
        &mut unreadable,
        &mut MoveInventoryBuildStats::default(),
        || {
            rig.fold(address, true);
            std::fs::remove_file(&live).unwrap();
        },
    )
    .unwrap();
    assert!(unreadable.is_empty());
    assert!(
        sources.contains_key(address),
        "a copy-then-delete fold must not disappear between census listings"
    );
}

#[test]
fn vanished_live_path_falls_back_to_archived_member() {
    let rig = Rig::new();
    let address = "0123456789abcdef";
    let live = rig.root.join("wal").join(format!("{address}.wal"));
    std::fs::write(&live, []).unwrap();
    let mut stats = MoveInventoryBuildStats::default();
    let source = locations(
        &rig.store,
        &CancellationToken::new(),
        &mut Vec::new(),
        &mut stats,
    )
    .unwrap()
    .remove(address)
    .unwrap();
    rig.fold(address, true);
    std::fs::remove_file(live).unwrap();
    let entry = scan_address(
        &rig.store,
        &CancellationToken::new(),
        address,
        source,
        None,
        &mut stats,
    )
    .unwrap()
    .expect("folded address still exists");
    assert!(matches!(entry.source, Source::Archive { .. }));
}

#[test]
fn unsealed_archive_cannot_prove_absence() {
    let rig = Rig::new();
    rig.fold("0123456789abcdef", false);
    build(&rig.store, &CancellationToken::new());
    assert_eq!(rig.store.move_inventory_health(), (true, 1));
    assert!(!rig
        .store
        .move_session_refs(std::iter::empty())
        .unreadable
        .is_empty());
}
