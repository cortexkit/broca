use super::*;
use broca_protocol::StepId;
use broca_wal::{archive::ArchiveWriter, FramedRecord, WalRangeRecords};

struct Fixture {
    root: PathBuf,
    store: FileWalStore,
    session: BindIdentity,
}

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "broca-archive-routing-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        Self {
            store: FileWalStore::new(root.join("wal")),
            root,
            session: BindIdentity::new("/archive-test", "runner", "s"),
        }
    }

    fn live(&self) -> Vec<FramedRecord> {
        let mut wal = self.store.open(&self.session).unwrap();
        for n in 1..=3 {
            wal.append(1, &WalRecord::StepStarted { step_id: StepId(n) }, true)
                .unwrap();
        }
        wal.replay().unwrap()
    }

    fn archive(&self, stamp: u64, payload: &[u8]) -> PathBuf {
        let dir = self.root.join("wal-archive");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("fold-{stamp}.ark"));
        let mut writer = ArchiveWriter::create(&path)
            .unwrap()
            .without_feature_floor();
        writer
            .append(&session_addr(&self.session), payload)
            .unwrap();
        writer.seal().unwrap();
        path
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn complete(range: WalRangeRead) -> Vec<FramedRecord> {
    match range.into_records() {
        WalRangeRecords::Complete(records) => records,
        other => panic!("expected Complete, got {other:?}"),
    }
}

#[test]
fn admission_existence_counts_archived_members_and_keeps_them_portable() {
    let f = Fixture::new();
    let records = f.live();
    let portable = f.store.wal_path(&f.session);
    f.archive(1000, &std::fs::read(&portable).unwrap());
    std::fs::remove_file(&portable).unwrap();
    assert_eq!(f.store.resolve_live_path(&f.session).unwrap(), None);
    assert!(f.store.lineage_exists(&f.session, None).unwrap());
    let wal = f.store.open_with_retention(&f.session, Some(3600)).unwrap();
    assert_eq!(wal.replay().unwrap(), records);
    assert!(portable.exists());
    assert!(!f.store.ephemeral_wal_path(&f.session).exists());
}

#[test]
fn restored_live_log_wins_after_member_index_was_warmed() {
    let f = Fixture::new();
    let records = f.live();
    let live = f.store.wal_path(&f.session);
    f.archive(1000, &std::fs::read(&live).unwrap());
    std::fs::remove_file(&live).unwrap();
    assert_eq!(f.store.read_records(&f.session).unwrap(), records);
    #[cfg(unix)]
    assert!(broca_wal::archive::cached_member_index_stats(&f.root.join("wal-archive")).is_some());
    let mut wal = f.store.open(&f.session).unwrap();
    #[cfg(unix)]
    assert!(
        broca_wal::archive::cached_member_index_stats(&f.root.join("wal-archive")).is_some(),
        "restore must retain the index for other archived addresses"
    );
    wal.append(1, &WalRecord::StepStarted { step_id: StepId(4) }, true)
        .unwrap();
    let restored = wal.replay().unwrap();
    assert_eq!(restored.len(), records.len() + 1);
    assert_eq!(f.store.read_records(&f.session).unwrap(), restored);
}

/// The boot prewarm only fills the membership cache; it must not change any answer. With no
/// archive it consults nothing. With one it consults every container, then an archived
/// session still reads its archived transcript and a new session still reads as absent --
/// the two lookups the cache exists to answer, checked after it has been built by the prewarm.
#[test]
fn prewarm_builds_nothing_visible_and_leaves_both_lookups_correct() {
    let empty = Fixture::new();
    assert_eq!(empty.store.prewarm_archive_membership().unwrap(), 0);

    let f = Fixture::new();
    let archived = f.live();
    let payload = std::fs::read(f.store.wal_path(&f.session)).unwrap();
    std::fs::remove_file(f.store.wal_path(&f.session)).unwrap();
    f.archive(1_000, &payload);

    assert_eq!(f.store.prewarm_archive_membership().unwrap(), 1);
    assert_eq!(
        complete(
            f.store
                .read_records_range(&f.session, 0, None, None)
                .unwrap()
        ),
        archived,
        "an archived session must still be served from the archive after the prewarm"
    );
    let new_session = BindIdentity::new("/archive-test", "runner", "never-archived");
    assert!(matches!(
        f.store
            .read_records_range(&new_session, 0, None, None)
            .unwrap()
            .records(),
        WalRangeRecords::Absent
    ));
}

/// The prewarm builds every sealed container's membership, several at a time, while a
/// session lookup racing it still gets the right answers; an unsealed container is
/// consulted but never built.
#[cfg(unix)]
#[test]
fn prewarm_builds_every_sealed_container_while_lookups_race_it() {
    use broca_wal::archive::{cached_membership, CachedMembership};
    let f = Fixture::new();
    let archived = f.live();
    let payload = std::fs::read(f.store.wal_path(&f.session)).unwrap();
    std::fs::remove_file(f.store.wal_path(&f.session)).unwrap();
    let mut sealed = vec![f.archive(1_000, &payload)];
    for stamp in 1_001..1_006 {
        let path = f.root.join("wal-archive").join(format!("fold-{stamp}.ark"));
        let mut writer = ArchiveWriter::create(&path)
            .unwrap()
            .without_feature_floor();
        for n in 0..100u64 {
            writer
                .append(&format!("{:016x}", stamp * 1_000 + n), b"other")
                .unwrap();
        }
        writer.seal().unwrap();
        sealed.push(path);
    }
    let unsealed = f.root.join("wal-archive").join("fold-2000.ark");
    let mut writer = ArchiveWriter::create(&unsealed)
        .unwrap()
        .without_feature_floor();
    writer.append("00000000000000aa", b"interrupted").unwrap();
    drop(writer);

    let new_session = BindIdentity::new("/archive-test", "runner", "never-archived");
    std::thread::scope(|scope| {
        let prewarm = scope.spawn(|| f.store.prewarm_archive_membership().unwrap());
        assert_eq!(
            complete(
                f.store
                    .read_records_range(&f.session, 0, None, None)
                    .unwrap()
            ),
            archived
        );
        assert!(matches!(
            f.store
                .read_records_range(&new_session, 0, None, None)
                .unwrap()
                .records(),
            WalRangeRecords::Absent
        ));
        assert_eq!(prewarm.join().unwrap(), sealed.len() + 1);
    });
    // The current writer seals v2 indexes, so every container's membership comes from its
    // verified index without reading any member header.
    for path in &sealed {
        assert_eq!(
            cached_membership(path),
            Some(CachedMembership::Verified),
            "{}",
            path.display()
        );
    }
    assert_eq!(cached_membership(&unsealed), None);
}

#[test]
fn no_archive_directory_never_enters_member_resolution_and_new_open_works() {
    let f = Fixture::new();
    ARCHIVE_LOOKUPS.with(|count| count.set(0));
    assert_eq!(f.store.wal_present(&f.session), Some(false));
    assert!(
        matches!(f.store.read_records(&f.session), Err(WalError::Io(error)) if error.kind() == io::ErrorKind::NotFound)
    );
    assert!(matches!(
        f.store
            .read_records_range(&f.session, 0, None, None)
            .unwrap()
            .records(),
        WalRangeRecords::Absent
    ));
    let anchor = WalByteAnchor {
        file_id: 1,
        seq: 1,
        byte_end: 1,
    };
    assert!(f
        .store
        .read_records_range_from_valid_anchor(&f.session, 1, None, &anchor)
        .unwrap()
        .is_none());
    assert_eq!(f.store.file_identity(&f.session).unwrap(), None);
    assert_eq!(f.store.flush_for_read(&f.session).unwrap(), None);
    f.live();
    assert!(f.store.wal_path(&f.session).exists());
    ARCHIVE_LOOKUPS.with(|count| assert_eq!(count.get(), 0));
}

#[test]
fn live_wins_over_deliberately_different_archive_on_every_path() {
    let f = Fixture::new();
    let records = f.live();
    let live = f
        .store
        .read_records_range(&f.session, 0, None, None)
        .unwrap();
    // ANCHORS EXIST ONLY WHERE FILE IDENTITY DOES. `select_range` pushes one per record
    // only when `metadata_file_id` returned `Some`, and that is `None` off unix. The archived
    // reads below are unaffected because they take identity from the member's verified digest
    // rather than from the OS; only this LIVE read depends on the platform.
    //
    // The assertion below makes the absence platform-explained rather than skipped: if anchors
    // ever stop being produced ON UNIX, this fails instead of passing vacuously.
    let anchor = live.anchor_at(1);
    assert!(
        anchor.is_some() || cfg!(not(unix)),
        "a platform with file identity must produce anchors"
    );
    let different = broca_wal::framing::encode(
        1,
        99,
        &WalRecord::StepStarted {
            step_id: StepId(999),
        },
    )
    .unwrap();
    f.archive(1, &different);
    ARCHIVE_LOOKUPS.with(|count| count.set(0));
    assert_eq!(f.store.read_records(&f.session).unwrap(), records);
    assert_eq!(
        complete(
            f.store
                .read_records_range(&f.session, 0, None, None)
                .unwrap()
        ),
        records
    );
    if let Some(anchor) = anchor {
        assert_eq!(
            complete(
                f.store
                    .read_records_range_from_valid_anchor(&f.session, 1, Some(2), &anchor)
                    .unwrap()
                    .unwrap()
            ),
            records[1..2]
        );
    }
    assert_eq!(f.store.open(&f.session).unwrap().replay().unwrap(), records);
    assert_eq!(f.store.wal_present(&f.session), Some(true));
    assert_eq!(
        f.store.file_identity(&f.session).unwrap(),
        anchor.map(|anchor| anchor.file_id)
    );
    assert_eq!(
        f.store.flush_for_read(&f.session).unwrap(),
        anchor.map(|anchor| anchor.file_id)
    );
    ARCHIVE_LOOKUPS.with(|count| assert_eq!(count.get(), 0));
}

#[test]
fn archived_member_serves_all_read_methods_in_place() {
    let f = Fixture::new();
    let records = f.live();
    let path = f.store.wal_path(&f.session);
    let live_range = f
        .store
        .read_records_range(&f.session, 0, None, None)
        .unwrap();
    f.archive(9, &std::fs::read(&path).unwrap());
    std::fs::remove_file(&path).unwrap();
    assert_eq!(f.store.read_records(&f.session).unwrap(), records);
    assert!(!path.exists(), "reads must not extract");
    let range = f
        .store
        .read_records_range(&f.session, 0, None, None)
        .unwrap();
    assert!(!path.exists(), "reads must not extract");
    assert_eq!(range.lineage_id, live_range.lineage_id);
    let anchor = range.anchor_at(1).unwrap();
    assert_eq!(complete(range), records);
    assert_eq!(
        complete(
            f.store
                .read_records_range(&f.session, 1, Some(2), None)
                .unwrap()
        ),
        records[1..2]
    );
    assert!(!path.exists(), "reads must not extract");
    assert_eq!(
        complete(
            f.store
                .read_records_range_from_valid_anchor(&f.session, 1, Some(2), &anchor)
                .unwrap()
                .unwrap()
        ),
        records[1..2]
    );
    let mut stale = anchor;
    stale.byte_end += 1;
    assert!(f
        .store
        .read_records_range_from_valid_anchor(&f.session, 1, None, &stale)
        .unwrap()
        .is_none());
    assert_eq!(f.store.wal_present(&f.session), Some(true));
    assert_eq!(
        f.store.file_identity(&f.session).unwrap(),
        Some(anchor.file_id)
    );
    assert_eq!(
        f.store.flush_for_read(&f.session).unwrap(),
        Some(anchor.file_id)
    );
    assert!(!path.exists(), "reads must not extract");
}

/// An archived WAL that a newer build wrote is refused by the NewerWriter name on every read
/// method that decodes it, exactly as the live file would be.
#[test]
fn archived_newer_writer_member_is_refused_by_name_on_every_decoding_read() {
    let f = Fixture::new();
    let records = f.live();
    let path = f.store.wal_path(&f.session);
    // The anchor comes from a LIVE read, and a live anchor needs the OS's file identity, which
    // exists only on unix. Without one the anchored read cannot be called at all, so that arm has
    // nothing to refuse there. The assertion keeps the absence platform-explained: if unix ever
    // stops producing anchors, this fails instead of skipping the arm.
    let anchor = f
        .store
        .read_records_range(&f.session, 0, Some(1), None)
        .unwrap()
        .anchor_at(1);
    assert!(
        anchor.is_some() || cfg!(not(unix)),
        "a platform with file identity must produce anchors"
    );
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend(
        broca_wal::framing::encode_with_requires(
            records.len() as u64 + 1,
            1,
            &["from-the-future/v1"],
            &WalRecord::StepStarted { step_id: StepId(9) },
        )
        .unwrap(),
    );
    f.archive(9, &bytes);
    std::fs::remove_file(&path).unwrap();

    let is_newer = |error: WalError| matches!(error, WalError::NewerWriter { ver: 3, .. });
    assert!(is_newer(f.store.read_records(&f.session).unwrap_err()));
    assert!(is_newer(
        f.store
            .read_records_range(&f.session, 0, None, None)
            .unwrap_err()
    ));
    if let Some(anchor) = anchor {
        assert!(is_newer(
            f.store
                .read_records_range_from_valid_anchor(&f.session, 1, None, &anchor)
                .unwrap_err()
        ));
    }
}

/// An archived WAL holding a `RunStarted` that froze a scope (a gated frame requiring
/// `scope/v1`) reads on every decoding read method, with the scope intact, exactly as the
/// live file would.
#[test]
fn archived_scoped_run_started_decodes_on_every_decoding_read() {
    let f = Fixture::new();
    let records = f.live();
    let path = f.store.wal_path(&f.session);
    let scoped: WalRecord = serde_json::from_str(
        r#"{"type":"run_started","run_id":"run-9","session":{"project_root":"/archive-test","harness":"runner","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[],"origin":{"kind":"fresh"},"scope":{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"head-1","scope_epoch":3}}"#,
    )
    .unwrap();
    let frame = broca_wal::framing::encode(records.len() as u64 + 1, 1, &scoped).unwrap();
    assert_eq!(frame[4], broca_wal::framing::GATED_FRAMING_VERSION);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend(frame);
    f.archive(9, &bytes);
    std::fs::remove_file(&path).unwrap();

    let last = |records: Vec<FramedRecord>| records.last().unwrap().record.clone();
    assert_eq!(last(f.store.read_records(&f.session).unwrap()), scoped);
    assert_eq!(
        last(complete(
            f.store
                .read_records_range(&f.session, 0, None, None)
                .unwrap()
        )),
        scoped
    );
    // The anchored read, from an anchor the archived member itself produced.
    let anchor = f
        .store
        .read_records_range(&f.session, 0, Some(1), None)
        .unwrap()
        .anchor_at(1)
        .expect("an archived member produces anchors");
    assert_eq!(
        last(complete(
            f.store
                .read_records_range_from_valid_anchor(&f.session, 1, None, &anchor)
                .unwrap()
                .expect("an anchor from the member is usable")
        )),
        scoped
    );
}

/// An archived WAL holding a `RunStarted` that froze a plan manifest (a gated frame requiring
/// `plan-manifest/v1`) reads on every decoding read method, with the manifest intact, exactly
/// as the live file would.
#[test]
fn archived_manifest_run_started_decodes_on_every_decoding_read() {
    let f = Fixture::new();
    let records = f.live();
    let path = f.store.wal_path(&f.session);
    let manifest: WalRecord = serde_json::from_str(
        &include_str!("../../../broca-wal/tests/fixtures/plan_manifest_run_started.json")
            .trim_end()
            .replace("{RUN_ID}", "run-9"),
    )
    .unwrap();
    assert!(matches!(
        &manifest,
        WalRecord::RunStarted {
            manifest: Some(_),
            ..
        }
    ));
    let frame = broca_wal::framing::encode(records.len() as u64 + 1, 1, &manifest).unwrap();
    assert_eq!(frame[4], broca_wal::framing::GATED_FRAMING_VERSION);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend(frame);
    f.archive(9, &bytes);
    std::fs::remove_file(&path).unwrap();

    let last = |records: Vec<FramedRecord>| records.last().unwrap().record.clone();
    assert_eq!(last(f.store.read_records(&f.session).unwrap()), manifest);
    assert_eq!(
        last(complete(
            f.store
                .read_records_range(&f.session, 0, None, None)
                .unwrap()
        )),
        manifest
    );
    let anchor = f
        .store
        .read_records_range(&f.session, 0, Some(1), None)
        .unwrap()
        .anchor_at(1)
        .expect("an archived member produces anchors");
    assert_eq!(
        last(complete(
            f.store
                .read_records_range_from_valid_anchor(&f.session, 1, None, &anchor)
                .unwrap()
                .expect("an anchor from the member is usable")
        )),
        manifest
    );
}

#[test]
fn discovery_refreshes_newest_member_and_digest_failure_is_an_error() {
    let f = Fixture::new();
    let bytes = |step| {
        broca_wal::framing::encode(
            1,
            1,
            &WalRecord::StepStarted {
                step_id: StepId(step),
            },
        )
        .unwrap()
    };
    f.archive(9, &bytes(9));
    assert_eq!(
        f.store.read_records(&f.session).unwrap()[0].record,
        WalRecord::StepStarted { step_id: StepId(9) }
    );
    let path = f.archive(10, &bytes(10));
    assert_eq!(
        f.store.read_records(&f.session).unwrap()[0].record,
        WalRecord::StepStarted {
            step_id: StepId(10)
        }
    );
    let mut corrupt = std::fs::read(&path).unwrap();
    corrupt[64] ^= 1;
    std::fs::write(&path, corrupt).unwrap();
    assert!(f.store.read_records(&f.session).is_err());
    assert!(f
        .store
        .read_records_range(&f.session, 0, None, None)
        .is_err());
    assert!(f.store.file_identity(&f.session).is_err());
    assert!(f.store.flush_for_read(&f.session).is_err());
    assert_eq!(f.store.wal_present(&f.session), None);
    assert!(f.store.open(&f.session).is_err());
    assert!(!f.store.wal_path(&f.session).exists());
}

#[test]
fn archive_directory_without_matching_member_preserves_absence_and_new_open() {
    let f = Fixture::new();
    f.archive(1, &[]);
    let missing = BindIdentity::new("/archive-test", "runner", "missing");
    assert_eq!(f.store.wal_present(&missing), Some(false));
    assert!(
        matches!(f.store.read_records(&missing), Err(WalError::Io(error)) if error.kind() == io::ErrorKind::NotFound)
    );
    assert!(matches!(
        f.store
            .read_records_range(&missing, 0, None, None)
            .unwrap()
            .records(),
        WalRangeRecords::Absent
    ));
    assert_eq!(f.store.file_identity(&missing).unwrap(), None);
    assert_eq!(f.store.flush_for_read(&missing).unwrap(), None);
    assert_eq!(f.store.open(&missing).unwrap().last_seq(), 0);
    assert!(f
        .store
        .read_records_range(&missing, 0, None, None)
        .unwrap()
        .lineage_id
        .is_some());
    assert!(f.store.wal_path(&missing).exists());
}

#[test]
fn live_unlinked_after_stat_resolves_sealed_member() {
    for method in ["records", "range", "anchor", "flush", "identity"] {
        let f = Fixture::new();
        let records = f.live();
        let path = f.store.wal_path(&f.session);
        let payload = std::fs::read(&path).unwrap();
        f.archive(1, &payload);
        // Obtain an archive anchor without changing the live pathname. Live inode
        // anchors intentionally become invalid when the backing file changes.
        let member = broca_wal::archive::ArchiveReader::open(
            f.root.join("wal-archive/fold-1.ark"),
            u64::MAX,
        )
        .unwrap()
        .read_member(&session_addr(&f.session))
        .unwrap()
        .unwrap();
        let archive_range = member_range(&member, 0, None).unwrap();
        let anchor = archive_range.anchor_at(1).unwrap();
        let removed = std::rc::Rc::new(std::cell::Cell::new(false));
        let observed = removed.clone();
        AFTER_LIVE_STAT.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                std::fs::remove_file(path).unwrap();
                observed.set(true);
            }))
        });
        match method {
            "records" => assert_eq!(f.store.read_records(&f.session).unwrap(), records),
            "range" => assert_eq!(
                complete(
                    f.store
                        .read_records_range(&f.session, 0, None, None)
                        .unwrap()
                ),
                records
            ),
            "anchor" => assert_eq!(
                complete(
                    f.store
                        .read_records_range_from_valid_anchor(&f.session, 1, None, &anchor)
                        .unwrap()
                        .unwrap()
                ),
                records[1..]
            ),
            "flush" => assert_eq!(
                f.store.flush_for_read(&f.session).unwrap(),
                Some(anchor.file_id)
            ),
            "identity" => assert_eq!(
                f.store.file_identity(&f.session).unwrap(),
                Some(anchor.file_id)
            ),
            _ => unreachable!(),
        }
        assert!(
            removed.get(),
            "{method} must execute the stat/unlink interleaving"
        );
    }
}

#[test]
fn extraction_preserves_lineage_identity_and_sequence_and_runs_once() {
    let f = Fixture::new();
    let before = f.live();
    let path = f.store.wal_path(&f.session);
    let lineage = f
        .store
        .read_records_range(&f.session, 0, None, None)
        .unwrap()
        .lineage_id
        .unwrap();
    let payload = std::fs::read(&path).unwrap();
    let archive = f.archive(1, &payload);
    let container = std::fs::read(&archive).unwrap();
    std::fs::remove_file(&path).unwrap();
    let count = std::rc::Rc::new(std::cell::Cell::new(0));
    let observed = count.clone();
    extraction::OBSERVE.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |op| {
            if op == "renamed" {
                observed.set(observed.get() + 1);
            }
        }))
    });
    let mut wal = f.store.open(&f.session).unwrap();
    assert_eq!(
        f.store
            .read_records_range(&f.session, 0, None, None)
            .unwrap()
            .lineage_id,
        Some(lineage)
    );
    assert_eq!(
        wal.append(2, &WalRecord::StepStarted { step_id: StepId(4) }, true)
            .unwrap(),
        before.last().unwrap().seq + 1
    );
    drop(wal);
    let post_append = std::fs::read(&path).unwrap();
    drop(f.store.open(&f.session).unwrap());
    extraction::OBSERVE.with(|hook| *hook.borrow_mut() = None);
    assert_eq!(count.get(), 1);
    assert_eq!(std::fs::read(path).unwrap(), post_append);
    assert_eq!(std::fs::read(archive).unwrap(), container);
}

#[test]
fn extraction_crash_cuts_follow_observed_operations_and_keep_session_readable() {
    // Discover cut points from a completed extraction's operation trace, including every
    // payload chunk, rather than duplicating a fixed list of crash windows in the test.
    for absent_directory in [false, true] {
        let f = Fixture::new();
        let mut records = f.live();
        let path = f.store.wal_path(&f.session);
        let mut wal = f.store.open(&f.session).unwrap();
        for n in 4..=200 {
            wal.append(1, &WalRecord::StepStarted { step_id: StepId(n) }, false)
                .unwrap();
        }
        wal.sync().unwrap();
        records.extend(wal.replay().unwrap().into_iter().skip(3));
        drop(wal);
        let payload = std::fs::read(&path).unwrap();
        let archive = f.archive(1, &payload);
        let container = std::fs::read(&archive).unwrap();
        std::fs::remove_file(&path).unwrap();
        if absent_directory {
            std::fs::remove_dir(path.parent().unwrap()).unwrap();
        }
        let trace = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let observed = trace.clone();
        extraction::OBSERVE.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |op| {
                observed.borrow_mut().push(op.to_string())
            }))
        });
        drop(f.store.open(&f.session).unwrap());
        extraction::OBSERVE.with(|hook| *hook.borrow_mut() = None);
        let trace = trace.borrow().clone();
        assert!(trace.iter().filter(|op| *op == "temp written").count() > 1);
        let rename = trace.iter().position(|op| op == "renamed").unwrap();
        assert_eq!(trace.last().unwrap(), "parent synced");
        for cut in 0..trace.len() {
            std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
            if !absent_directory {
                std::fs::create_dir(path.parent().unwrap()).unwrap();
            }
            let seen = std::rc::Rc::new(std::cell::Cell::new(0));
            let observed = seen.clone();
            extraction::OBSERVE.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |_| {
                    let n = observed.get();
                    observed.set(n + 1);
                    assert_ne!(n, cut, "injected extraction crash");
                }))
            });
            let crash =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f.store.open(&f.session)));
            extraction::OBSERVE.with(|hook| *hook.borrow_mut() = None);
            assert!(crash.is_err(), "cut {cut} must execute");
            assert_eq!(seen.get(), cut + 1);
            assert_eq!(path.exists(), cut >= rename, "cut {cut}: {}", trace[cut]);
            if path.exists() {
                assert_eq!(std::fs::read(&path).unwrap(), payload);
            } else if cut >= trace.iter().position(|op| op == "temp created").unwrap() {
                let temps: Vec<_> = std::fs::read_dir(path.parent().unwrap())
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .collect();
                assert_eq!(
                    temps.len(),
                    1,
                    "the temp must be in the canonical directory"
                );
                let partial = std::fs::read(&temps[0]).unwrap();
                let written_chunks = trace[..=cut]
                    .iter()
                    .filter(|op| *op == "temp written")
                    .count();
                assert_eq!(partial.len(), (written_chunks * 4096).min(payload.len()));
                assert!(payload.starts_with(&partial));
            }
            assert_eq!(f.store.read_records(&f.session).unwrap(), records);
            assert_eq!(std::fs::read(&archive).unwrap(), container);
            // Retry after the crash must tolerate orphan temps and finish without a mint.
            drop(f.store.open(&f.session).unwrap());
            assert_eq!(std::fs::read(&path).unwrap(), payload);
        }
    }
}

#[cfg(unix)]
#[test]
fn extraction_parent_sync_failure_aborts_admission() {
    let f = Fixture::new();
    f.live();
    let path = f.store.wal_path(&f.session);
    let payload = std::fs::read(&path).unwrap();
    f.archive(1, &payload);
    std::fs::remove_file(&path).unwrap();
    let parent = path.parent().unwrap().to_path_buf();
    let moved = f.root.join("moved-wal");
    let source = parent.clone();
    let destination = moved.clone();
    // Remove the directory's old name after rename so the real directory sync fails
    // to open it. This exercises error propagation, not a synthetic return from a probe.
    extraction::OBSERVE.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |op| {
            if op == "renamed" {
                std::fs::rename(&source, &destination).unwrap();
            }
        }))
    });
    let result = f.store.open(&f.session);
    extraction::OBSERVE.with(|hook| *hook.borrow_mut() = None);
    assert!(
        result.is_err(),
        "a failed parent sync must abort before FileWal open"
    );
    assert!(
        !parent.exists(),
        "FileWal must not create a replacement lineage"
    );
    assert_eq!(f.store.read_records(&f.session).unwrap().len(), 3);
    std::fs::rename(moved, parent).unwrap();
    assert_eq!(std::fs::read(path).unwrap(), payload);
}

// Keep the inode so a previously valid anchor remains usable after restoration. The
// path still disappears before open and returns only after that open reports absence.
fn restore_live_during_retry(f: &Fixture) -> std::rc::Rc<std::cell::Cell<usize>> {
    let path = f.store.wal_path(&f.session);
    let saved = f.root.join("saved-live.wal");
    let executed = std::rc::Rc::new(std::cell::Cell::new(0));
    let observed = executed.clone();
    AFTER_LIVE_STAT.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            std::fs::rename(&path, &saved).unwrap();
            BEFORE_ARCHIVE_RETRY.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    assert!(!path.exists(), "the first live open must miss");
                    std::fs::rename(saved, path).unwrap();
                    observed.set(observed.get() + 1);
                }));
            });
        }));
    });
    executed
}

fn restoration_fixture() -> (Fixture, Vec<FramedRecord>, Option<WalByteAnchor>) {
    let f = Fixture::new();
    let records = f.live();
    let range = f
        .store
        .read_records_range(&f.session, 0, None, None)
        .unwrap();
    let anchor = range.anchor_at(1);
    assert!(anchor.is_some() || cfg!(not(unix)));
    // A retry that takes the archive instead of the restored live file must fail too.
    let different = broca_wal::framing::encode(
        1,
        1,
        &WalRecord::StepStarted {
            step_id: StepId(999),
        },
    )
    .unwrap();
    f.archive(1, &different);
    (f, records, anchor)
}

#[test]
fn restored_live_retry_returns_original_records() {
    let (f, records, _) = restoration_fixture();
    let executed = restore_live_during_retry(&f);
    let actual = f.store.read_records(&f.session).unwrap();
    assert_eq!(executed.get(), 1);
    assert_eq!(actual, records);
}

#[test]
fn restored_live_retry_range_returns_original_records() {
    let (f, records, _) = restoration_fixture();
    let executed = restore_live_during_retry(&f);
    let actual = f
        .store
        .read_records_range(&f.session, 0, None, None)
        .unwrap();
    assert_eq!(executed.get(), 1);
    // Model the read caller's empty-WAL interpretation to expose transcript loss.
    let actual = match actual.into_records() {
        WalRangeRecords::Complete(records) => records,
        WalRangeRecords::Absent => Vec::new(),
        other => panic!("unexpected source: {other:?}"),
    };
    assert_eq!(actual, records);
}

#[cfg(unix)]
#[test]
fn restored_live_retry_valid_anchor_returns_original_suffix() {
    let (f, records, anchor) = restoration_fixture();
    let anchor = anchor.unwrap();
    let executed = restore_live_during_retry(&f);
    let actual = f
        .store
        .read_records_range_from_valid_anchor(&f.session, 1, Some(2), &anchor)
        .unwrap();
    assert_eq!(executed.get(), 1);
    assert_eq!(
        complete(actual.expect("restored anchor is valid")),
        records[1..2]
    );
}

#[cfg(unix)]
#[test]
fn restored_live_retry_flush_returns_live_identity() {
    let (f, records, anchor) = restoration_fixture();
    let executed = restore_live_during_retry(&f);
    let actual = f.store.flush_for_read(&f.session).unwrap();
    assert_eq!(executed.get(), 1);
    assert_eq!(
        actual,
        anchor.map(|anchor| anchor.file_id),
        "identity must follow the restored live file, and be absent where the platform \
         has none rather than skipped"
    );
    assert_eq!(f.store.read_records(&f.session).unwrap(), records);
}

#[cfg(unix)]
#[test]
fn wal_present_answers_from_one_observation_so_a_restore_cannot_race_it() {
    // The sixth instance of this class, reported by the worker that fixed the other five:
    // wal_present used to stat live and THEN consult the archive, so an extraction landing
    // between the two answered `Some(false)` about a log at the canonical path. Rather than
    // handle that interleaving, the window was removed -- archived_member makes the live
    // check itself, so one call answers both halves and there is no gap to race.
    //
    // That is why this test does NOT install an interleaving hook: there is no longer a
    // point between two observations at which one could fire. It pins the three states the
    // single observation must distinguish.
    let f = Fixture::new();
    let records = f.live();
    let path = f.store.wal_path(&f.session);
    let bytes = std::fs::read(&path).unwrap();

    // live present
    assert_eq!(f.store.wal_present(&f.session), Some(true));

    // live absent, no archive at all -- a confirmed miss, not ignorance
    std::fs::remove_file(&path).unwrap();
    assert_eq!(f.store.wal_present(&f.session), Some(false));

    // live absent, archived -- present, and the records are still reachable
    f.archive(7, &bytes);
    assert_eq!(f.store.wal_present(&f.session), Some(true));
    assert_eq!(f.store.read_records(&f.session).unwrap(), records);

    // live restored beside the archive -- live wins, still present
    std::fs::write(&path, &bytes).unwrap();
    assert_eq!(f.store.wal_present(&f.session), Some(true));
}

#[test]
fn restored_live_retry_identity_returns_live_identity() {
    let (f, records, anchor) = restoration_fixture();
    let executed = restore_live_during_retry(&f);
    let actual = f.store.file_identity(&f.session).unwrap();
    assert_eq!(executed.get(), 1);
    assert_eq!(
        actual,
        anchor.map(|anchor| anchor.file_id),
        "identity must follow the restored live file, and be absent where the platform \
         has none rather than skipped"
    );
    assert_eq!(f.store.read_records(&f.session).unwrap(), records);
}

#[test]
fn restored_extraction_retry_returns_original_transcript() {
    for ranged in [false, true] {
        let f = Fixture::new();
        let records = f.live();
        let path = f.store.wal_path(&f.session);
        let payload = std::fs::read(&path).unwrap();
        f.archive(1, &payload);
        let executed = std::rc::Rc::new(std::cell::Cell::new(0));
        let observed = executed.clone();
        AFTER_LIVE_STAT.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                std::fs::remove_file(&path).unwrap();
                BEFORE_ARCHIVE_RETRY.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || {
                        assert!(!path.exists());
                        extraction::restore(
                            &path,
                            &payload,
                            extraction::RestoreAuthority::GuardedStandalone,
                        )
                        .unwrap();
                        observed.set(observed.get() + 1);
                    }));
                });
            }));
        });
        let actual = if ranged {
            match f
                .store
                .read_records_range(&f.session, 0, None, None)
                .unwrap()
                .into_records()
            {
                WalRangeRecords::Complete(records) => records,
                WalRangeRecords::Absent => Vec::new(),
                other => panic!("unexpected source: {other:?}"),
            }
        } else {
            f.store.read_records(&f.session).unwrap()
        };
        assert_eq!(executed.get(), 1);
        assert_eq!(actual, records, "ranged={ranged}");
    }
}

#[test]
fn repeated_live_disappearance_is_an_error_not_an_empty_read() {
    for ranged in [false, true] {
        let f = Fixture::new();
        f.live();
        let path = f.store.wal_path(&f.session);
        let payload = std::fs::read(&path).unwrap();
        f.archive(1, &payload);
        let executed = std::rc::Rc::new(std::cell::Cell::new(0));
        let observed = executed.clone();
        AFTER_LIVE_STAT.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                std::fs::remove_file(&path).unwrap();
                BEFORE_ARCHIVE_RETRY.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || {
                        extraction::restore(
                            &path,
                            &payload,
                            extraction::RestoreAuthority::GuardedStandalone,
                        )
                        .unwrap();
                        AFTER_LIVE_STAT.with(|hook| {
                            *hook.borrow_mut() = Some(Box::new(move || {
                                std::fs::remove_file(path).unwrap();
                                observed.set(observed.get() + 1);
                            }));
                        });
                    }));
                });
            }));
        });
        let result = if ranged {
            f.store
                .read_records_range(&f.session, 0, None, None)
                .map(|_| ())
        } else {
            f.store.read_records(&f.session).map(|_| ())
        };
        assert_eq!(executed.get(), 1);
        assert!(
            matches!(result, Err(WalError::Io(error)) if error.kind() == io::ErrorKind::NotFound)
        );
    }
}

#[test]
fn empty_live_wal_remains_a_successful_empty_read() {
    let f = Fixture::new();
    drop(f.store.open(&f.session).unwrap());
    f.archive(1, &[]);
    assert!(f.store.read_records(&f.session).unwrap().is_empty());
    assert!(complete(
        f.store
            .read_records_range(&f.session, 0, None, None)
            .unwrap()
    )
    .is_empty());
}

#[test]
fn full_record_read_distinguishes_absent_from_present_empty_wal() {
    let f = Fixture::new();
    assert!(matches!(f.store.read_records(&f.session),
        Err(WalError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound));
    std::fs::create_dir_all(f.store.wal_path(&f.session).parent().unwrap()).unwrap();
    std::fs::write(f.store.wal_path(&f.session), []).unwrap();
    assert!(f.store.read_records(&f.session).unwrap().is_empty());
    assert!(!f.store.is_retention_lineage(&f.session).unwrap());
}

#[test]
fn owned_writer_open_restores_an_archived_log_only_under_the_sessions_tenure() {
    use crate::lease::{lease_key_for, FileLeaseStore, LeaseStore};
    let f = Fixture::new();
    let records = f.live();
    let path = f.store.wal_path(&f.session);
    let payload = std::fs::read(&path).unwrap();
    let archive = f.archive(1, &payload);
    let container = std::fs::read(&archive).unwrap();
    std::fs::remove_file(&path).unwrap();
    let leases = FileLeaseStore::new(f.root.join("leases"));

    // A lease for another session is not this open's authority: the restore refuses
    // before it writes, so the archive stays the only copy.
    let other = BindIdentity::new("/archive-test", "runner", "other");
    let foreign = leases.acquire(&lease_key_for(&other)).unwrap();
    let error = match f.store.open_owned_writer(&f.session, None, None, foreign) {
        Ok(_) => panic!("a foreign tenure must not restore the live log"),
        Err(error) => error,
    };
    assert!(
        matches!(&error, WalError::Io(io) if io.kind() == io::ErrorKind::PermissionDenied),
        "unexpected error: {error}"
    );
    assert!(!path.exists());
    assert_eq!(std::fs::read(&archive).unwrap(), container);

    // The session's own tenure restores it, and the open hands that same lease on.
    let own = leases.acquire(&lease_key_for(&f.session)).unwrap();
    let epoch = own.epoch();
    let (wal, lease) = f
        .store
        .open_owned_writer(&f.session, None, None, own)
        .expect("the session's tenure restores and opens its log");
    assert_eq!(lease.epoch(), epoch);
    assert_eq!(wal.replay().unwrap(), records);
    assert_eq!(std::fs::read(&path).unwrap(), payload);
    drop(wal);
    drop(lease);
}
