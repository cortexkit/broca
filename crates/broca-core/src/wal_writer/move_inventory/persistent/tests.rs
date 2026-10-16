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

fn terminal() -> WalRecord {
    WalRecord::RunFinished {
        reason: broca_protocol::RunFinishReason::Completed,
        ts_ms: None,
        usage: broca_protocol::Usage::default(),
        error: None,
        retries_used: 0,
    }
}

#[test]
#[cfg(unix)]
fn append_during_scan_keeps_only_the_stamped_strict_prefix() {
    let rig = Rig::new();
    let address = "0123456789abcdef";
    let path = rig.root.join("wal").join(format!("{address}.wal"));
    let mut wal = FileWal::open(&path).unwrap();
    wal.append(1, &terminal(), true).unwrap();
    let mut stats = MoveInventoryBuildStats::default();
    let source = live_source(path.clone(), &mut stats).unwrap();
    let stamped_len = std::fs::metadata(&path).unwrap().len();
    let entry = scan_after_stamp(&rig.store, address, source, None, &mut stats, || {
        wal.append(1, &terminal(), true).unwrap();
    })
    .expect("append-only growth must not make the inventory permanently unknown");
    assert_eq!(entry.last_seq, 1);
    assert_eq!(entry.committed_end, stamped_len);
    assert!(std::fs::metadata(path).unwrap().len() > entry.committed_end);
}

#[test]
fn shrink_during_scan_is_not_an_append_only_prefix() {
    let rig = Rig::new();
    let address = "0123456789abcdef";
    let path = rig.root.join("wal").join(format!("{address}.wal"));
    let mut wal = FileWal::open(&path).unwrap();
    wal.append(1, &terminal(), true).unwrap();
    let mut stats = MoveInventoryBuildStats::default();
    let source = live_source(path.clone(), &mut stats).unwrap();
    assert!(
        scan_after_stamp(&rig.store, address, source, None, &mut stats, || {
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_len(0)
                .unwrap();
        })
        .is_err()
    );
}

#[test]
fn deleted_address_is_dropped_after_a_complete_reresolution() {
    let rig = Rig::new();
    let address = "0123456789abcdef";
    let path = rig.root.join("wal").join(format!("{address}.wal"));
    std::fs::write(&path, []).unwrap();
    let mut stats = MoveInventoryBuildStats::default();
    let source = live_source(path.clone(), &mut stats).unwrap();
    std::fs::remove_file(path).unwrap();
    assert!(scan_address(
        &rig.store,
        &CancellationToken::new(),
        address,
        source,
        None,
        &mut stats
    )
    .unwrap()
    .is_none());
}

#[test]
fn changed_archive_stamp_is_reresolved_before_giving_up() {
    let rig = Rig::new();
    let address = "0123456789abcdef";
    rig.fold(address, true);
    let mut stats = MoveInventoryBuildStats::default();
    let mut source = locations(
        &rig.store,
        &CancellationToken::new(),
        &mut Vec::new(),
        &mut stats,
    )
    .unwrap()
    .remove(address)
    .unwrap();
    let Source::Archive { stamp, .. } = &mut source else {
        panic!("archive expected");
    };
    stamp.len += 1;
    assert!(scan_address(
        &rig.store,
        &CancellationToken::new(),
        address,
        source,
        None,
        &mut stats
    )
    .unwrap()
    .is_some());
    assert_eq!(stats.payload_reads, 2);
}

#[test]
fn final_pass_recovers_a_transiently_unreadable_address() {
    let rig = Rig::new();
    let path = rig.root.join("wal/0123456789abcdef.wal");
    std::fs::create_dir(&path).unwrap();
    build_with_hooks(
        &rig.store,
        &CancellationToken::new(),
        |pass| {
            if pass == 0 {
                std::fs::remove_dir(&path).unwrap();
                let mut wal = FileWal::open(&path).unwrap();
                wal.append(1, &terminal(), true).unwrap();
            }
        },
        |_| {},
    );
    assert_eq!(rig.store.move_inventory_health(), (true, 0));
    assert_eq!(rig.store.move_inventory_build_stats().payload_reads, 1);
}

#[test]
#[cfg(unix)]
fn interrupted_build_checkpoints_and_reuses_completed_addresses() {
    let rig = Rig::new();
    for i in 0..CHECKPOINT_ADDRESSES + 1 {
        std::fs::write(rig.root.join("wal").join(format!("{i:016x}.wal")), []).unwrap();
    }
    let cancel = CancellationToken::new();
    build_with_hooks(
        &rig.store,
        &cancel,
        |_| {},
        |completed| {
            if completed == CHECKPOINT_ADDRESSES {
                cancel.cancel();
            }
        },
    );
    assert!(cancel.is_cancelled());
    let saved = load(&rig.store, &mut MoveInventoryBuildStats::default());
    assert_eq!(
        saved.len(),
        CHECKPOINT_ADDRESSES,
        "cancelled boot must leave reusable completed hints"
    );
    let restarted = FileWalStore::new(rig.root.join("wal"));
    build(&restarted, &CancellationToken::new());
    assert_eq!(restarted.move_inventory_health(), (true, 0));
    assert_eq!(
        restarted.move_inventory_build_stats().reused,
        CHECKPOINT_ADDRESSES as u64
    );
    assert_eq!(restarted.move_inventory_build_stats().payload_reads, 1);
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
