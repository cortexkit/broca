use super::*;
use crate::retention::tests::{frames, started};
use broca_wal::Wal;

pub(crate) fn write_decoded_fixture(path: &Path, records: &[broca_wal::FramedRecord]) {
    if path.exists() {
        std::fs::remove_file(path).unwrap();
    }
    let first = started("header", None);
    let mut wal = FileWal::open(path).unwrap();
    wal.append(1, &first, true).unwrap();
    drop(wal);
    let mut bytes = std::fs::read(path).unwrap();
    let broca_wal::framing::DecodeStep::Lineage { consumed, .. } =
        broca_wal::framing::decode_next(&bytes)
    else {
        panic!("fixture begins with a lineage frame");
    };
    let lineage_bytes = bytes[..consumed].to_vec();
    bytes.truncate(consumed);
    // Retention's feature is enabled only by the complete reader-release slice.
    // Feed already-decoded fixture payloads here; broca-wal separately pins the
    // gated bytes and verifies that an incomplete reader refuses those frames.
    for frame in records {
        bytes.extend(
            broca_wal::framing::encode_payload(
                frame.seq,
                frame.fence,
                &serde_json::to_vec(&frame.record).unwrap(),
            )
            .unwrap(),
        );
        bytes.extend_from_slice(&lineage_bytes);
    }
    std::fs::write(path, bytes).unwrap();
}

struct Fixture {
    root: PathBuf,
    store: FileWalStore,
    session: BindIdentity,
}
impl Fixture {
    fn new(name: &str) -> Self {
        let root = broca_test_support::scratch_root()
            .join(format!("core-dual-wal-{}-{name}", std::process::id()));
        std::fs::create_dir_all(root.join("wal")).unwrap();
        Self {
            store: FileWalStore::new(root.join("wal")),
            session: BindIdentity::new("/retention-test", "runner", name),
            root,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn ephemeral_wal_resolves_through_every_store_reader() {
    let f = Fixture::new("readers");
    let path = f.store.ephemeral_wal_path(&f.session);
    let records = frames(Some(3600));
    write_decoded_fixture(&path, &records);
    assert_eq!(
        f.store.resolve_live_path(&f.session).unwrap(),
        Some(path.clone())
    );
    assert_eq!(f.store.read_records(&f.session).unwrap(), records);
    assert_eq!(
        f.store
            .read_address_records(&session_addr(&f.session))
            .unwrap(),
        records
    );
    assert!(f.store.is_retention_lineage(&f.session).unwrap());
    assert_eq!(f.store.wal_present(&f.session), Some(true));
    assert!(f.store.live_wal_last_write(&f.session).unwrap().is_some());
    let range = f
        .store
        .read_records_range(&f.session, 0, None, None)
        .unwrap();
    // Byte anchors need a stable file identity, which only unix provides. Elsewhere the range
    // has none and readers fall back to decoding from the start, so only unix checks the
    // anchored read.
    match range.anchor_at(2) {
        Some(anchor) => {
            let anchored = f
                .store
                .read_records_range_from_valid_anchor(&f.session, 2, None, &anchor)
                .unwrap()
                .unwrap();
            assert_eq!(
                anchored.records(),
                broca_wal::WalRangeRecords::Complete(&records[2..])
            );
        }
        #[cfg(unix)]
        None => panic!("a unix WAL range must carry byte anchors for the ephemeral root too"),
        #[cfg(not(unix))]
        None => {}
    }
    assert_eq!(
        f.store.flush_for_read(&f.session).unwrap(),
        f.store.file_identity(&f.session).unwrap()
    );
    assert_eq!(
        f.store.addresses().unwrap(),
        std::collections::BTreeSet::from([session_addr(&f.session)])
    );
    let opened = f.store.open(&f.session).unwrap();
    assert_eq!(opened.last_seq(), 5);
    drop(opened);
    assert!(!f.store.wal_path(&f.session).exists());
    assert_eq!(f.store.manifest_path(), f.root.join(WAL_MANIFEST_FILENAME));
    assert_eq!(
        f.store.feature_floor_path(),
        f.root.join(broca_wal::FEATURE_FLOOR_FILENAME)
    );
}

#[test]
fn dual_live_wals_are_an_error_naming_both_paths() {
    let f = Fixture::new("conflict");
    let portable = f.store.wal_path(&f.session);
    let ephemeral = f.store.ephemeral_wal_path(&f.session);
    write_decoded_fixture(&portable, &frames(None));
    write_decoded_fixture(&ephemeral, &frames(Some(3600)));
    let assert_conflict = |error: WalError| {
        let message = error.to_string();
        assert!(message.contains(portable.to_str().unwrap()), "{message}");
        assert!(message.contains(ephemeral.to_str().unwrap()), "{message}");
    };
    assert_conflict(f.store.resolve_live_path(&f.session).unwrap_err());
    assert_conflict(f.store.read_records(&f.session).unwrap_err());
    assert_conflict(
        f.store
            .read_records_range(&f.session, 0, None, None)
            .unwrap_err(),
    );
    assert_conflict(f.store.live_wal_last_write(&f.session).unwrap_err());
    assert_conflict(f.store.flush_for_read(&f.session).unwrap_err());
    assert_conflict(f.store.file_identity(&f.session).unwrap_err());
    assert_conflict(f.store.addresses().unwrap_err());
    assert_conflict(f.store.lineage_exists(&f.session, None).unwrap_err());
    assert_conflict(
        f.store
            .open(&f.session)
            .err()
            .expect("open refuses conflict"),
    );
    assert_eq!(f.store.wal_present(&f.session), None);
}

#[test]
fn lineage_creation_directory_is_fixed_and_deletion_files_are_not_live() {
    let f = Fixture::new("directory");
    let ephemeral = f.store.ephemeral_wal_path(&f.session);
    drop(f.store.open_with_retention(&f.session, Some(3600)).unwrap());
    assert!(ephemeral.exists());
    drop(f.store.open_with_retention(&f.session, None).unwrap());
    assert!(!f.store.wal_path(&f.session).exists());
    let lineage = f
        .store
        .read_records_range(&f.session, 0, None, None)
        .unwrap()
        .lineage_id
        .unwrap();
    let deleting = f.store.deleting_path(&f.session, lineage);
    assert_eq!(deleting.extension().unwrap(), "deleting");
    std::fs::rename(&ephemeral, &deleting).unwrap();
    let tombstone = f.store.tombstone_path(&f.session, &lineage.to_string());
    std::fs::create_dir_all(tombstone.parent().unwrap()).unwrap();
    std::fs::write(tombstone, "{}").unwrap();
    assert_eq!(f.store.resolve_live_path(&f.session).unwrap(), None);
    assert!(!f.store.lineage_exists(&f.session, None).unwrap());
    assert!(f.store.addresses().unwrap().is_empty());
    drop(f.store.open(&f.session).unwrap());
    assert!(f.store.wal_path(&f.session).exists());
    assert!(
        deleting.exists(),
        "creating a fresh lineage must not touch the old deletion"
    );
}

#[test]
fn admission_existence_counts_meta_unless_its_own_lineage_is_tombstoned() {
    let f = Fixture::new("meta");
    let mut meta = crate::AdmissionStoreSnapshot {
        watermark_seq: 5,
        lineage_id: Some("old-lineage".into()),
        anchor: None,
        checkpoint: None,
    };
    assert!(!f.store.lineage_exists(&f.session, None).unwrap());
    assert!(f.store.lineage_exists(&f.session, Some(&meta)).unwrap());
    let tombstone = f.store.tombstone_path(&f.session, "old-lineage");
    std::fs::create_dir_all(tombstone.parent().unwrap()).unwrap();
    std::fs::write(tombstone, "{}").unwrap();
    assert!(!f.store.lineage_exists(&f.session, Some(&meta)).unwrap());
    meta.lineage_id = Some("fresh-forever".into());
    assert!(f.store.lineage_exists(&f.session, Some(&meta)).unwrap());
    meta.lineage_id = None;
    assert!(f.store.lineage_exists(&f.session, Some(&meta)).unwrap());
    drop(f.store.open(&f.session).unwrap());
    assert!(f.store.lineage_exists(&f.session, None).unwrap());
}

#[test]
fn retained_terminal_reserve_covers_maximal_error_detail_and_accounting() {
    let overhead = broca_wal::framing::LINEAGE_FRAME_LEN as u64;
    let usage = broca_protocol::Usage {
        input_tokens: Some(u64::MAX),
        cached_input_tokens: Some(u64::MAX),
        cache_write_tokens: Some(u64::MAX),
        output_tokens: Some(u64::MAX),
        reasoning_tokens: Some(u64::MAX),
    };
    for cap in [1, 4096, u64::MAX] {
        let record = retained_size::terminal(cap, Some(u64::MAX), usage, u32::MAX);
        let actual = broca_wal::framing::encode(u64::MAX, u64::MAX, &record)
            .unwrap()
            .len() as u64
            + overhead;
        assert!(actual <= retained_size::reserve_bytes(overhead));
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn unretained_append_never_crosses_recovery_cap() {
    assert_eq!(broca_wal::DEFAULT_MAX_READ_BYTES, 1_073_741_824);
    let mut f = Fixture::new("forever-cap");
    let cap = 8192;
    f.store = FileWalStore::new(f.root.join("wal")).with_read_cap_for_test(cap);
    let wal = f.store.open(&f.session).unwrap();
    assert_eq!(wal.append_budget().unwrap().0, cap);
    let writer = WalHandle::spawn(wal, 1).unwrap();
    assert!(matches!(
        writer.record(started("forever", None)),
        RecordOutcome::Accepted(1)
    ));
    writer.sync().await.unwrap();
    let before = std::fs::read(f.store.wal_path(&f.session)).unwrap();
    let seq = writer.next_record_seq();
    let mut record = started("forever", None);
    if let WalRecord::RunStarted { title, .. } = &mut record {
        *title = Some("z".repeat(8192));
    }
    assert!(matches!(
        writer.record(record),
        RecordOutcome::Refused(WalWriteError::SessionTooLarge { max_bytes: 8192 })
    ));
    assert!(matches!(
        writer.sync().await,
        Err(WalWriteError::SessionTooLarge { .. })
    ));
    assert_eq!(writer.next_record_seq(), seq);
    assert_eq!(std::fs::read(f.store.wal_path(&f.session)).unwrap(), before);
    assert!(!writer.is_poisoned());
    writer
        .record_size_refusal_terminal(cap, Some(1), Default::default(), 0)
        .unwrap();
    writer.sync().await.unwrap();
    // The ceiling refusal must leave a writer that still shuts down cleanly.
    writer.shutdown().await.unwrap();
    let frames = f.store.read_records(&f.session).unwrap();
    assert!(
        matches!(&frames.last().unwrap().record, WalRecord::RunFinished { reason: broca_protocol::RunFinishReason::Error, error: Some(error), .. } if error.provider_code.as_deref() == Some("session_too_large") && error.class == broca_protocol::ErrorClass::Permanent)
    );
    assert!(
        std::fs::metadata(f.store.wal_path(&f.session))
            .unwrap()
            .len()
            <= cap
    );
    assert_eq!(f.store.open(&f.session).unwrap().last_seq(), 2);
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn record_group_reports_partial_refusal_without_reserving_dropped_sequences() {
    let mut f = Fixture::new("group-refusal");
    let first = WalRecord::StepStarted {
        step_id: broca_protocol::StepId(1),
    };
    let overhead = broca_wal::framing::LINEAGE_FRAME_LEN as u64;
    let cap = overhead
        + retained_size::reserve_bytes(overhead)
        + broca_wal::framing::encode(1, 1, &first).unwrap().len() as u64
        + overhead;
    f.store = FileWalStore::new(f.root.join("wal")).with_read_cap_for_test(cap);
    let writer = WalHandle::spawn(
        f.store.open_with_retention(&f.session, Some(10)).unwrap(),
        1,
    )
    .unwrap();
    let result = writer.record_group(vec![
        first,
        WalRecord::StepStarted {
            step_id: broca_protocol::StepId(2),
        },
        WalRecord::StepStarted {
            step_id: broca_protocol::StepId(3),
        },
    ]);
    assert!(matches!(
        result,
        Err(WalWriteError::RetainedSessionTooLarge { .. })
    ));
    assert_eq!(writer.next_record_seq(), 2);
    assert_eq!(
        writer
            .record_size_refusal_terminal(cap, Some(1), Default::default(), 0)
            .unwrap(),
        2
    );
    assert_eq!(writer.sync().await.unwrap(), 2);
}
