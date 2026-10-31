use super::*;
use broca_core::{ExportIndex, MaterializedStore, RunIndex, RunIndexState};
use broca_protocol::{RunFinishReason, RunId};
use broca_wal::Wal;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const MARKER: &str = "RETENTION-MODULE-CONTENT-SENTINEL-59137";

fn archive_forever_sessions(rig: &Rig, count: usize) -> Vec<BindIdentity> {
    let archive = rig.root.join("wal-archive/fold-1000.ark");
    fs::create_dir_all(archive.parent().unwrap()).unwrap();
    let mut writer = broca_wal::archive::ArchiveWriter::create(&archive)
        .unwrap()
        .without_feature_floor();
    let mut keys = Vec::new();
    for n in 0..count {
        let key = BindIdentity::new(rig.root.join("project"), "runner", format!("archive-{n}"));
        let mut wal = rig.service.wal.open(&key).unwrap();
        wal.append(
            1,
            &started(&key, &format!("archive-run-{n}"), None, 100),
            false,
        )
        .unwrap();
        for _ in 0..4 {
            wal.append(1, &large_expiry_step(), false).unwrap();
        }
        wal.append(1, &finished(200), true).unwrap();
        drop(wal);
        let path = rig.service.wal.wal_path(&key);
        writer
            .append(&broca_core::session_addr(&key), &fs::read(&path).unwrap())
            .unwrap();
        fs::remove_file(path).unwrap();
        keys.push(key);
    }
    writer.seal().unwrap();
    keys
}

#[cfg(unix)]
#[test]
fn archived_expiry_checks_bound_member_decodes() {
    let rig = Rig::new();
    let keys = archive_forever_sessions(&rig, 10);
    let before = broca_wal::archive::expiry_io::counts();
    let start = std::time::Instant::now();
    for n in 0..1000 {
        let key = &keys[n % keys.len()];
        let run = (n % 2 == 0).then_some("archive-run-0");
        assert_eq!(rig.service.expired(key, run).unwrap(), None);
    }
    let after = broca_wal::archive::expiry_io::counts();
    let (opens, loads, decodes) = (after.0 - before.0, after.1 - before.1, after.2 - before.2);
    eprintln!(
        "archived expiry: 1000 checks, {} sessions, {:?}, {opens} archive opens, {loads} payload loads, {decodes} WAL member decodes",
        keys.len(), start.elapsed()
    );
    assert!(decodes <= keys.len() as u64, "{decodes} member decodes");
    // One open per session's member read, plus one per container the archive member index
    // reads once to build itself (this rig seals a single container). The bound is what
    // matters: opens track sessions and containers, never the 1,000 checks.
    assert!(opens <= keys.len() as u64 + 1, "{opens} archive opens");
}

#[cfg(feature = "test-clock")]
#[test]
fn archived_expiry_uses_one_lookup_and_inventory_listing() {
    let rig = Rig::new();
    let key = archive_forever_sessions(&rig, 1).pop().unwrap();
    // A consulted miss needs the same reuse as a member hit, including the flush,
    // absent-range and lineage-existence fallbacks used to resolve tombstones.
    for key in [&rig.key, &key] {
        let before = broca_core::FileWalStore::archive_lookup_counts();
        assert_eq!(rig.service.expired(key, None).unwrap(), None);
        let after = broca_core::FileWalStore::archive_lookup_counts();
        assert_eq!((after.0 - before.0, after.1 - before.1), (1, 1));
    }
}

/// Opt-in read-only measurement against a supplied archive copy, never a live store.
#[cfg(all(unix, feature = "test-clock"))]
#[test]
#[ignore = "requires BROCA_EXPIRY_ARCHIVE_COPY pointing at a supplied archive copy"]
fn production_archive_expiry_measurement() {
    use broca_wal::archive::{expiry_io, read_newest_member, ArchiveReader};
    let copy = PathBuf::from(std::env::var_os("BROCA_EXPIRY_ARCHIVE_COPY").expect("archive copy"));
    let count: usize = std::env::var("BROCA_EXPIRY_CHECKS")
        .unwrap_or_else(|_| "1000".into())
        .parse()
        .unwrap();
    let rig = Rig::new();
    fs::create_dir_all(rig.root.join("wal")).unwrap();
    for name in ["wal-archive", "wal-archive-durable"] {
        std::os::unix::fs::symlink(copy.join(name), rig.root.join(name)).unwrap();
    }
    let mut paths = fs::read_dir(copy.join("wal-archive"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "ark"))
        .collect::<Vec<_>>();
    paths.sort();
    let mut addresses = std::collections::BTreeSet::new();
    for path in &paths {
        let mut reader = ArchiveReader::open(path, u64::MAX).unwrap();
        let members = reader.verified_members().unwrap().unwrap_or_else(|| {
            reader
                .rescan()
                .unwrap()
                .into_iter()
                .map(|info| broca_wal::archive::IndexedMember {
                    session_addr: info.session_addr,
                    offset: info.offset,
                    payload_len: info.payload_len,
                })
                .collect()
        });
        addresses.extend(members.into_iter().map(|member| member.session_addr));
    }
    let mut keys = Vec::new();
    let mut skipped = 0;
    for address in &addresses {
        let member = read_newest_member(&paths, address, u64::MAX)
            .unwrap()
            .unwrap();
        let read =
            broca_wal::file_wal::read_verified_member_range(&member.payload, 0, 0, None).unwrap();
        let key = frames(&read).iter().find_map(|frame| match &frame.record {
            WalRecord::RunStarted { session, .. }
                if broca_core::session_addr(session) == *address =>
            {
                Some(session.clone())
            }
            _ => None,
        });
        let Some(key) = key else {
            skipped += 1;
            continue;
        };
        keys.push(key);
        if keys.len() == count {
            break;
        }
    }
    assert_eq!(keys.len(), count);
    eprintln!(
        "archive_expiry inventory_addresses={} skipped_without_matching_bind={skipped}",
        addresses.len()
    );
    // Production also prewarms archive membership before these expiry checks.
    rig.service.wal.prewarm_archive_membership().unwrap();
    for phase in ["cold-facts", "warm-facts"] {
        let before = FileWalStore::archive_lookup_counts();
        let stats = FileWalStore::archive_metadata_count() as u64 + expiry_io::metadata_count();
        let io = expiry_io::counts();
        let start = std::time::Instant::now();
        for key in &keys {
            rig.service.expired(key, None).unwrap();
        }
        let elapsed = start.elapsed();
        let after = FileWalStore::archive_lookup_counts();
        eprintln!("archive_expiry phase={phase} checks={} containers={} wall_us={} archive_stats={} inventory_listings={} directory_read_attempts={} archive_lookups={} opens={} decodes={}",
            keys.len(), paths.len(), elapsed.as_micros(),
            FileWalStore::archive_metadata_count() as u64 + expiry_io::metadata_count() - stats,
            after.1 - before.1, 2 * (after.1 - before.1), after.0 - before.0,
            expiry_io::counts().0 - io.0, expiry_io::counts().2 - io.2);
    }
}

#[tokio::test]
async fn archived_expiry_cache_keeps_tombstones_and_newer_lineages_authoritative() {
    let rig = Rig::new();
    let key = archive_forever_sessions(&rig, 1).pop().unwrap();
    let registry = rig.registry(None);
    let id = rig
        .service
        .wal
        .read_records_range(&key, 0, None, None)
        .unwrap()
        .lineage_id
        .unwrap()
        .to_string();
    for _ in 0..2 {
        registry.check_expired(&key, None).await.unwrap();
        registry
            .check_expired(&key, Some("archive-run-0"))
            .await
            .unwrap();
    }
    let stone = Tombstone {
        next_tag: None,
        next_notice_seq: None,
        lineage_id: id,
        expired_at_ms: 1234,
        seq: 2,
        completed_at_ms: None,
    };
    rig.service.write_tombstone(&key, &stone).unwrap();
    for run in [None, Some("archive-run-0")] {
        assert!(matches!(
            registry.check_expired(&key, run).await,
            Err(crate::registry::OpenError::Expired {
                expired_at_ms: 1234
            })
        ));
    }

    // A restored or newly written live lineage wins over a cached archived predecessor.
    let path = rig.service.wal.ephemeral_wal_path(&key);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let now = rig.service.clock.now_ms();
    let mut wal = broca_wal::FileWal::open(&path)
        .unwrap()
        .with_feature_floor(rig.service.wal.feature_floor_path());
    wal.append(
        1,
        &started(&key, "newer", Some(RETAINED_SECONDS), now),
        false,
    )
    .unwrap();
    wal.append(1, &finished(now), true).unwrap();
    drop(wal);
    registry.check_expired(&key, None).await.unwrap();
    registry.check_expired(&key, Some("newer")).await.unwrap();

    // Publish that newer generation under the same container name, not just a new path.
    let archive = rig.root.join("wal-archive/fold-1000.ark");
    let old_archive = fs::read(&archive).unwrap();
    let replacement = rig.root.join("fold-1000.ark");
    let mut writer = broca_wal::archive::ArchiveWriter::create(&replacement)
        .unwrap()
        .without_feature_floor();
    writer
        .append(&broca_core::session_addr(&key), &fs::read(&path).unwrap())
        .unwrap();
    writer.seal().unwrap();
    fs::rename(replacement, &archive).unwrap();
    fs::remove_file(path).unwrap();
    for _ in 0..2 {
        registry.check_expired(&key, None).await.unwrap();
        registry.check_expired(&key, Some("newer")).await.unwrap();
    }

    fs::write(&archive, old_archive).unwrap();
    assert!(matches!(
        registry.check_expired(&key, None).await,
        Err(crate::registry::OpenError::Expired {
            expired_at_ms: 1234
        })
    ));

    fs::remove_file(rig.root.join("wal-archive/fold-1000.ark")).unwrap();
    assert!(matches!(
        registry.check_expired(&key, None).await,
        Err(crate::registry::OpenError::Expired {
            expired_at_ms: 1234
        })
    ));
}

#[cfg(feature = "test-clock")]
#[test]
fn archived_expiry_cache_rechecks_retention_rows_clock_and_run_membership() {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    let id = rig.seed(RETAINED_SECONDS, now, Some(now));
    rig.service.rederive(&rig.key).unwrap();
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    let archive = rig.root.join("wal-archive/fold-1000.ark");
    fs::create_dir_all(archive.parent().unwrap()).unwrap();
    let mut writer = broca_wal::archive::ArchiveWriter::create(&archive)
        .unwrap()
        .without_feature_floor();
    writer
        .append(
            &broca_core::session_addr(&rig.key),
            &fs::read(&path).unwrap(),
        )
        .unwrap();
    writer.seal().unwrap();
    fs::remove_file(path).unwrap();
    let before = broca_wal::archive::expiry_io::counts();
    assert_eq!(rig.service.expired(&rig.key, None).unwrap(), None);
    assert_eq!(rig.service.expired(&rig.key, Some("r1")).unwrap(), None);
    let mut row = rig.store.lookup_retention_lineage(&id).unwrap().unwrap();
    let deadline = row.expires_at_ms.unwrap();
    fs::write(rig.root.join("test-clock"), br#"{"offset_ms":3600001}"#).unwrap();
    rig.service.clock.refresh().unwrap();
    assert_eq!(rig.service.expired(&rig.key, None).unwrap(), Some(deadline));
    assert_eq!(
        rig.service.expired(&rig.key, Some("r1")).unwrap(),
        Some(deadline)
    );
    assert_eq!(
        rig.service.expired(&rig.key, Some("unknown")).unwrap(),
        None
    );
    row.expires_at_ms = None;
    row.expired_at_ms = Some(deadline);
    rig.store.upsert_retention_lineage(&row, &[]).unwrap();
    assert_eq!(rig.service.expired(&rig.key, None).unwrap(), Some(deadline));
    let after = broca_wal::archive::expiry_io::counts();
    if cfg!(unix) {
        assert_eq!(after.2 - before.2, 1);
    }
}

#[test]
fn archived_expiry_cache_invalidates_on_publication_conflict_and_corruption() {
    let rig = Rig::new();
    let key = archive_forever_sessions(&rig, 1).pop().unwrap();
    let archive = rig.root.join("wal-archive/fold-1000.ark");
    assert_eq!(rig.service.expired(&key, None).unwrap(), None);
    assert_eq!(rig.service.expired(&key, None).unwrap(), None);
    let newer = rig.root.join("wal-archive/fold-2000.ark");
    fs::copy(&archive, &newer).unwrap();
    assert_eq!(rig.service.expired(&key, None).unwrap(), None);
    fs::create_dir_all(rig.root.join("wal-archive-durable")).unwrap();
    let conflict = rig.root.join("wal-archive-durable/fold-3000.ark");
    fs::copy(&archive, &conflict).unwrap();
    assert!(rig
        .service
        .expired(&key, None)
        .unwrap_err()
        .contains("conflicting archived WALs"));
    fs::remove_file(conflict).unwrap();
    assert_eq!(rig.service.expired(&key, None).unwrap(), None);
    let mut bytes = fs::read(&newer).unwrap();
    // Keep the filename and length unchanged: metadata identity must catch in-place damage.
    bytes[100] ^= 1;
    fs::write(&newer, bytes).unwrap();
    assert!(rig.service.expired(&key, None).is_err());
}

fn large_expiry_step() -> WalRecord {
    WalRecord::ModelStepFinished {
        fallback: Default::default(),
        written_at_ms: None,
        tags: None,
        hooks: None,
        step_id: broca_protocol::StepId(1),
        assistant_message: broca_protocol::AssistantMessage {
            message_id: broca_protocol::MessageId("m".into()),
            content: vec![broca_protocol::ContentBlock::Text {
                text: "x".repeat(100_000),
            }],
        }
        .into(),
        usage: Default::default(),
        finish_reason: Some(broca_protocol::FinishReason::Stop),
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn status_poll_large_nonretained_session_does_not_read_wal() {
    for durable in [false, true] {
        let rig = Rig::new();
        let mut wal = rig.service.wal.open(&rig.key).unwrap();
        let now = rig.service.clock.now_ms();
        wal.append(1, &started(&rig.key, "r1", None, now), false)
            .unwrap();
        let record = large_expiry_step();
        for _ in 0..100 {
            wal.append(1, &record, false).unwrap();
        }
        wal.append(1, &finished(now), true).unwrap();
        drop(wal);
        if durable {
            fs::create_dir_all(rig.service.wal.durable_root()).unwrap();
            fs::rename(
                rig.service.wal.wal_path(&rig.key),
                rig.service.wal.durable_wal_path(&rig.key),
            )
            .unwrap();
        }
        let registry = rig.registry(None);
        registry.actor_for(&rig.key).await.unwrap();
        registry.route_status(&rig.key).await.unwrap();
        let before = rig.service.wal.range_reads_for_test();
        for _ in 0..3 {
            registry.route_status(&rig.key).await.unwrap();
            // Include both expiry variants even when the actor already knows its status.
            registry.check_expired(&rig.key, None).await.unwrap();
            registry.check_expired(&rig.key, Some("r1")).await.unwrap();
        }
        assert_eq!(rig.service.wal.range_reads_for_test(), before);
        assert!(registry.stop_and_seal().await);
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn retained_deadline_refuses_status_read_and_subscribe() {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    rig.seed(1, now, Some(now));
    rig.service.rederive(&rig.key).unwrap();
    fs::write(rig.root.join("test-clock"), br#"{"offset_ms":2000}"#).unwrap();
    rig.service.clock.refresh().unwrap();
    let registry = rig.registry(None);
    let before = rig.service.wal.range_reads_for_test();
    assert!(matches!(registry.route_status(&rig.key).await,
        Err(crate::registry::OpenError::Expired { expired_at_ms }) if expired_at_ms == now + 1000));
    assert!(
        matches!(registry.route_run_state(&rig.key, RunId("r1".into())).await,
        Err(crate::registry::OpenError::Expired { expired_at_ms }) if expired_at_ms == now + 1000)
    );
    assert!(matches!(registry.route_read(&rig.key, None, None).await,
        Err(crate::registry::OpenError::Expired { expired_at_ms }) if expired_at_ms == now + 1000));
    let (sink, _deliveries) = tokio::sync::mpsc::channel(8);
    assert!(
        matches!(registry.route_subscribe(&rig.key, crate::fanout::FromCursor::Start, sink).await,
        Err(crate::registry::OpenError::Expired { expired_at_ms }) if expired_at_ms == now + 1000)
    );
    assert_eq!(rig.service.wal.range_reads_for_test(), before);
}

#[cfg(feature = "test-clock")]
#[test]
fn stale_retention_row_replays_wal_and_refreshes_summary() {
    for initially_finished in [false, true] {
        let rig = Rig::new();
        let now = rig.service.clock.now_ms();
        let id = rig.seed(1, now, initially_finished.then_some(now));
        rig.service.rederive(&rig.key).unwrap();
        let old_row = rig
            .store
            .lookup_live_retention_lineage(&rig.key)
            .unwrap()
            .unwrap();
        let mut wal = rig.service.wal.open(&rig.key).unwrap();
        if initially_finished {
            wal.append(1, &started(&rig.key, "r2", Some(1), now + 2000), true)
                .unwrap();
        } else {
            wal.append(1, &finished(now), true).unwrap();
        }
        drop(wal);
        fs::write(rig.root.join("test-clock"), br#"{"offset_ms":2000}"#).unwrap();
        rig.service.clock.refresh().unwrap();
        let before = rig.service.wal.range_reads_for_test();
        let expected = (!initially_finished).then_some(now + 1000);
        assert_eq!(rig.service.expired(&rig.key, None).unwrap(), expected);
        assert_eq!(rig.service.wal.range_reads_for_test(), before + 1);
        let row = rig
            .store
            .lookup_live_retention_lineage(&rig.key)
            .unwrap()
            .unwrap();
        assert!(row.wal_bytes > old_row.wal_bytes);
        assert_eq!(row.lineage_id, id);
        assert_eq!(row.expires_at_ms, expected);
        assert_eq!(rig.service.expired(&rig.key, None).unwrap(), expected);
        assert_eq!(rig.service.wal.range_reads_for_test(), before + 1);
    }
}

#[cfg(feature = "test-clock")]
#[test]
fn retained_summary_does_not_include_records_past_durable_head() {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    rig.seed(1, now, Some(now));
    rig.service.rederive(&rig.key).unwrap();
    fs::write(rig.root.join("test-clock"), br#"{"offset_ms":2000}"#).unwrap();
    rig.service.clock.refresh().unwrap();
    assert_eq!(
        rig.service.expired_head(&rig.key, None, Some(1)).unwrap(),
        None
    );
    assert_eq!(
        rig.service.expired_head(&rig.key, None, Some(2)).unwrap(),
        Some(now + 1000)
    );
}

#[cfg(feature = "test-clock")]
#[test]
fn missing_retention_summary_is_refreshed_without_rewriting_run_state() {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    // Freeze the clock: the seed expires one second after `now`, and WAL setup on a
    // slow runner can take longer than that between sampling `now` and checking expiry,
    // which would report the seed as already expired. A fixed clock keeps the deadline
    // in the future until the test moves it explicitly.
    fs::write(
        rig.root.join("test-clock"),
        format!(r#"{{"now_ms":{now}}}"#),
    )
    .unwrap();
    rig.service.clock.refresh().unwrap();
    rig.seed(1, now, Some(now));
    assert!(rig
        .store
        .lookup_live_retention_lineage(&rig.key)
        .unwrap()
        .is_none());
    let before = rig.service.wal.range_reads_for_test();
    assert_eq!(rig.service.expired(&rig.key, None).unwrap(), None);
    let row = rig
        .store
        .lookup_live_retention_lineage(&rig.key)
        .unwrap()
        .unwrap();
    assert_eq!(row.expires_at_ms, Some(now + 1000));
    assert!(rig.store.lookup_run("r1").unwrap().is_none());
    assert_eq!(rig.service.expired(&rig.key, None).unwrap(), None);
    assert_eq!(rig.service.wal.range_reads_for_test(), before + 1);
}

#[test]
fn expiry_refuses_ambiguous_paths_and_corrupt_stale_retained_wals() {
    let rig = Rig::new();
    rig.seed(1, 100, Some(200));
    rig.service.rederive(&rig.key).unwrap();
    let retained = rig.service.wal.ephemeral_wal_path(&rig.key);
    fs::create_dir_all(rig.service.wal.root()).unwrap();
    fs::copy(&retained, rig.service.wal.wal_path(&rig.key)).unwrap();
    assert!(rig.service.expired(&rig.key, None).is_err());
    fs::remove_file(rig.service.wal.wal_path(&rig.key)).unwrap();
    let mut bytes = fs::read(&retained).unwrap();
    bytes[10] ^= 1;
    bytes.push(0);
    fs::write(&retained, bytes).unwrap();
    assert!(rig.service.expired(&rig.key, None).is_err());
}

#[tokio::test]
async fn periodic_sweep_repairs_stale_discovery_without_new_wal_bytes() {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    rig.seed(RETAINED_SECONDS, now, Some(now));
    rig.service.rederive(&rig.key).unwrap();
    rig.store.index_run_withdrawals_owed("r1", true).unwrap();
    let registry = rig.registry(None);
    registry.sweep_once(now).await.unwrap();
    assert!(
        !rig.store
            .lookup_run("r1")
            .unwrap()
            .unwrap()
            .withdrawals_owed
    );
}

/// Retention for live sends in tests that are not about expiry. It must outlast any slow
/// runner so status polling remains readable. Tests about expiry move the test clock
/// past this window instead of waiting for it.
const RETAINED_SECONDS: u64 = 3600;

struct Rig {
    root: PathBuf,
    key: BindIdentity,
    service: Arc<RetentionService>,
    store: Arc<SqliteStore>,
    leases: Arc<broca_core::FileLeaseStore>,
}

#[test]
fn retention_deletion_drops_media_manifest_after_wal_unlink_and_leaves_collection_to_sweep() {
    let rig = Rig::new();
    rig.seed(1, 1000, Some(1000));
    let bytes = b"%PDF-1.7\nretention fixture\n%%EOF";
    let reference = broca_core::media_validation::validate_media(bytes, "application/pdf").unwrap();
    let media = broca_core::media_store::MediaStore::open(
        &rig.root,
        broca_core::media_store::MediaStoreClass::Ephemeral,
    )
    .unwrap();
    media
        .session(&rig.key)
        .unwrap()
        .register_and_append(&reference, bytes, || Ok(()))
        .unwrap();
    let manifest = rig
        .root
        .join("media-ephemeral/refs")
        .join(broca_core::session_addr(&rig.key));
    assert!(manifest.exists());
    let _lease = rig.service.lease(&rig.key).unwrap();
    assert!(rig
        .service
        .delete_expired(&rig.key, 2001)
        .unwrap()
        .is_some());
    assert!(!rig.service.wal.ephemeral_wal_path(&rig.key).exists());
    assert!(!manifest.exists());
    assert_eq!(media.read_verified(&reference).unwrap(), bytes);
    drop(_lease);
    assert_eq!(
        broca_core::media_store::sweep(&rig.root)
            .unwrap()
            .objects_removed,
        1
    );
}
impl Rig {
    #[cfg(any(unix, windows))]
    fn overhaul() -> Self {
        let mut rig = Self::new();
        fs::write(
            rig.root.join(broca_wal::state_format::MARKER_FILENAME),
            b"broca-state/2\n",
        )
        .unwrap();
        let wal = Arc::new(FileWalStore::with_config(
            rig.root.join("wal"),
            broca_wal::WalConfig::OVERHAUL,
        ));
        let resolver = Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1));
        rig.service = Arc::new(
            RetentionService::new(
                wal,
                rig.store.clone(),
                rig.leases.clone(),
                rig.store.clone(),
                resolver,
            )
            .unwrap(),
        );
        rig
    }

    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "module-retention-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&root).unwrap();
        let key = BindIdentity::new(root.join("project"), "runner", "session");
        let wal = Arc::new(FileWalStore::new(root.join("wal")));
        let store = Arc::new(SqliteStore::open(root.join("run-index.db")).unwrap());
        let leases = Arc::new(broca_core::FileLeaseStore::new(root.join("leases")));
        let resolver = Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1));
        let service = Arc::new(
            RetentionService::new(wal, store.clone(), leases.clone(), store.clone(), resolver)
                .unwrap(),
        );
        Self {
            root,
            key,
            service,
            store,
            leases,
        }
    }
    fn seed(&self, seconds: u64, start: u64, finish: Option<u64>) -> String {
        self.seed_run("r1", seconds, start, finish)
    }
    fn seed_run(&self, run: &str, seconds: u64, start: u64, finish: Option<u64>) -> String {
        let _lease = self.service.lease(&self.key).unwrap();
        let mut wal = self
            .service
            .wal
            .open_with_retention(&self.key, Some(seconds))
            .unwrap();
        wal.append(1, &started(&self.key, run, Some(seconds), start), true)
            .unwrap();
        if let Some(ts) = finish {
            wal.append(1, &finished(ts), true).unwrap();
        }
        self.service.remember_session(&self.key).unwrap();
        wal.lineage_id().unwrap().to_string()
    }
    fn registry(&self, index: Option<Arc<dyn RunIndex>>) -> crate::registry::LineageRegistry {
        self.registry_with_reply(index, "done".into())
    }
    fn registry_with_reply(
        &self,
        index: Option<Arc<dyn RunIndex>>,
        reply: String,
    ) -> crate::registry::LineageRegistry {
        let executor = Arc::new(RealExecutor {
            reply,
            wal: self.service.wal.clone(),
            store: self.store.clone(),
        });
        let opener = if self.service.wal.wal_config().is_overhaul() {
            crate::opener::DefaultOpener::with_file_leases(
                self.root.join("leases"),
                self.service.wal.clone(),
                executor,
            )
            .with_lease_domain(broca_core::lease::WalLeaseDomain::new(
                self.root.join("leases"),
            ))
            .unwrap()
        } else {
            crate::opener::DefaultOpener::new(
                self.leases.clone(),
                self.service.wal.clone(),
                executor,
            )
        }
        .with_run_index(index.unwrap_or(self.store.clone()))
        .with_materialized_store(self.store.clone());
        let opener = Arc::new(opener);
        let registry = crate::registry::LineageRegistry::new_with_reader(
            opener,
            self.service.wal.clone(),
            Some(self.store.clone()),
        );
        registry.set_retention_service(self.service.clone());
        registry
    }
}

#[cfg(any(unix, windows))]
#[tokio::test]
async fn relocation_incomplete_preflight_never_expires_or_completes_deletion() {
    let rig = Rig::overhaul();
    let wal = rig
        .service
        .wal
        .open_with_retention(&rig.key, Some(60))
        .unwrap();
    let id = wal.lineage_id().unwrap().to_string();
    drop(wal);
    let source = rig.service.wal.ephemeral_wal_path(&rig.key);
    let alias = rig.service.wal.wal_path(&rig.key);
    fs::create_dir_all(alias.parent().unwrap()).unwrap();
    fs::hard_link(&source, &alias).unwrap();
    rig.service.remember_session(&rig.key).unwrap();
    rig.service
        .write_tombstone(
            &rig.key,
            &Tombstone {
                lineage_id: id.clone(),
                expired_at_ms: 1,
                seq: 1,
                completed_at_ms: None,
                next_tag: None,
                next_notice_seq: None,
            },
        )
        .unwrap();
    rig.service
        .index
        .deletion_pending(&id, &rig.key, true)
        .unwrap();
    let before_bytes = fs::read(&source).unwrap();
    let before_stones = serde_json::to_value(rig.service.tombstones(&rig.key).unwrap()).unwrap();
    let before_outbox = rig.store.pending_retention_withdrawals().unwrap().len();
    let registry = rig.registry(None);
    let error = registry.check_expired(&rig.key, None).await.unwrap_err();
    assert!(
        matches!(&error, crate::registry::OpenError::Classified { class: broca_protocol::ErrorClass::Transient, reason } if reason.contains("wal_relocation_incomplete"))
    );
    assert_eq!(
        rig.service
            .delete_expired_typed(&rig.key, u64::MAX)
            .unwrap(),
        None
    );
    assert!(rig.service.resume_deletions(u64::MAX).unwrap().is_empty());
    assert!(
        rig.service.resume_deletions(u64::MAX).unwrap().is_empty(),
        "the next pass retries the deferred key"
    );
    assert_eq!(rig.service.relocation_incomplete_skips(), 1);
    assert_eq!(rig.service.overhaul_deferred_attempts(), 3);
    assert_eq!(fs::read(&source).unwrap(), before_bytes);
    assert_eq!(fs::read(&alias).unwrap(), before_bytes);
    assert_eq!(
        serde_json::to_value(rig.service.tombstones(&rig.key).unwrap()).unwrap(),
        before_stones
    );
    assert_eq!(
        rig.store.pending_retention_withdrawals().unwrap().len(),
        before_outbox
    );
    assert!(!rig.service.deleting(&rig.key, &id).exists());
}

#[tokio::test]
async fn legacy_same_inode_conflicts_keep_the_existing_retention_error_class() {
    let rig = Rig::new();
    rig.seed(1, 1000, Some(1000));
    let source = rig.service.wal.ephemeral_wal_path(&rig.key);
    let alias = rig.service.wal.wal_path(&rig.key);
    fs::create_dir_all(alias.parent().unwrap()).unwrap();
    fs::hard_link(&source, &alias).unwrap();
    let error = rig
        .registry(None)
        .check_expired(&rig.key, None)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, crate::registry::OpenError::Open(reason) if reason.starts_with("wal io: conflicting live WALs:") && !reason.contains("wal_relocation_incomplete"))
    );
    assert_eq!(rig.service.relocation_incomplete_skips(), 0);
    assert!(source.exists() && alias.exists());
}
impl Drop for Rig {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[cfg(all(unix, feature = "test-clock"))]
#[tokio::test]
async fn phase_one_overhaul_retention_defers_expiry_and_deletion() {
    let rig = Rig::overhaul();
    fs::write(rig.root.join("test-clock"), br#"{"now_ms":3000}"#).unwrap();
    rig.service.clock.refresh().unwrap();
    let writer = due_overhaul_writer(&rig, Arc::new(broca_wal::fs_ops::RealFs)).await;
    writer.shutdown().await.unwrap();
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    let before = fs::read(&path).unwrap();
    let _lease = rig.service.lease(&rig.key).unwrap();
    assert_eq!(
        rig.service.delete_expired_typed(&rig.key, 3000).unwrap(),
        None
    );
    assert_eq!(
        rig.service.expired(&rig.key, None).unwrap(),
        None,
        "phase-one overhaul expiry is deferred even when the legacy deadline is due"
    );
    assert!(rig.service.resume_deletions(3000).unwrap().is_empty());
    assert_eq!(rig.service.overhaul_deferred_attempts(), 3);
    assert_eq!(fs::read(&path).unwrap(), before);
    assert!(rig.service.tombstones(&rig.key).unwrap().is_empty());
    assert!(rig
        .store
        .pending_retention_withdrawals()
        .unwrap()
        .is_empty());
}

#[cfg(all(unix, feature = "test-clock"))]
async fn due_overhaul_writer(
    rig: &Rig,
    fs: Arc<dyn broca_wal::fs_ops::FsOps>,
) -> broca_core::WalHandle {
    let lease = rig.service.lease(&rig.key).unwrap();
    let fence = lease.epoch();
    let writer = broca_wal::fs_ops::with_fs_ops(fs.clone(), || {
        let wal = rig
            .service
            .wal
            .open_with_retention(&rig.key, Some(1))
            .unwrap();
        broca_core::WalHandle::spawn_in_store_with_fs_for_test(
            wal,
            rig.service.wal.as_ref(),
            &rig.key,
            fence,
            Some(lease),
            fs,
        )
        .unwrap()
    });
    writer
        .record(started(&rig.key, "r1", Some(1), 1000))
        .into_result()
        .unwrap();
    writer.record(finished(1000)).into_result().unwrap();
    writer.sync().await.unwrap();
    writer
}

#[cfg(all(unix, feature = "test-clock"))]
mod overhaul_close_regressions {
    use super::*;
    use broca_wal::fs_ops::{FileOps, FsOps, OpenOptions, RealFs};
    use std::io::{Read, Seek, SeekFrom, Write};

    #[derive(Default)]
    struct Faults {
        head: AtomicBool,
        close: AtomicBool,
    }
    struct FaultFs(Arc<Faults>);
    struct FaultFile {
        inner: Box<dyn FileOps>,
        path: PathBuf,
        faults: Arc<Faults>,
    }
    impl FsOps for FaultFs {
        fn open(&self, path: &Path, options: &OpenOptions) -> io::Result<Box<dyn FileOps>> {
            Ok(Box::new(FaultFile {
                inner: RealFs.open(path, options)?,
                path: path.to_path_buf(),
                faults: self.0.clone(),
            }))
        }
        fn metadata(&self, path: &Path) -> io::Result<fs::Metadata> {
            RealFs.metadata(path)
        }
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            RealFs.create_dir_all(path)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            RealFs.rename(from, to)
        }
        fn remove_file(&self, path: &Path) -> io::Result<()> {
            RealFs.remove_file(path)
        }
        fn sync_dir(&self, path: &Path) -> io::Result<()> {
            RealFs.sync_dir(path)
        }
        fn create_dir(&self, path: &Path) -> io::Result<()> {
            RealFs.create_dir(path)
        }
        fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()> {
            RealFs.hard_link(from, to)
        }
        fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
            RealFs.canonicalize(path)
        }
        fn read_dir(&self, path: &Path) -> io::Result<broca_wal::fs_ops::ReadDir> {
            RealFs.read_dir(path)
        }
        fn validate_if_exists(&self, path: &Path) -> io::Result<()> {
            RealFs.validate_if_exists(path)
        }
    }
    impl Read for FaultFile {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            self.inner.read(bytes)
        }
    }
    impl Seek for FaultFile {
        fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
            self.inner.seek(from)
        }
    }
    impl Write for FaultFile {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.path.extension().is_some_and(|ext| ext == "head")
                && self.faults.head.swap(false, Ordering::SeqCst)
            {
                return Err(io::Error::other("injected close-publication failure"));
            }
            if self.path.extension().is_some_and(|ext| ext == "wal")
                && self.faults.close.load(Ordering::SeqCst)
                && matches!(broca_wal::framing::decode_next(bytes), broca_wal::framing::DecodeStep::Record { framed, .. } if matches!(framed.record, WalRecord::WriterClosed { .. }))
            {
                self.faults.close.store(false, Ordering::SeqCst);
                return Err(io::Error::other("injected interrupted close"));
            }
            self.inner.write(bytes)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()
        }
    }
    impl FileOps for FaultFile {
        fn metadata(&self) -> io::Result<fs::Metadata> {
            self.inner.metadata()
        }
        fn set_len(&self, len: u64) -> io::Result<()> {
            self.inner.set_len(len)
        }
        fn sync_all(&self) -> io::Result<()> {
            self.inner.sync_all()
        }
        fn try_clone(&self) -> io::Result<Box<dyn FileOps>> {
            Ok(Box::new(Self {
                inner: self.inner.try_clone()?,
                path: self.path.clone(),
                faults: self.faults.clone(),
            }))
        }
        fn lock(&self) -> io::Result<()> {
            self.inner.lock()
        }
        fn unlock(&self) -> io::Result<()> {
            self.inner.unlock()
        }
        fn try_lock(&self) -> Result<(), fs::TryLockError> {
            self.inner.try_lock()
        }
        fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
            self.inner.read_exact_at(bytes, offset)
        }
    }

    async fn next_send_reopens_due_history(publication_failure: bool) {
        let rig = Rig::overhaul();
        fs::write(rig.root.join("test-clock"), br#"{"now_ms":3000}"#).unwrap();
        rig.service.clock.refresh().unwrap();
        let faults = Arc::new(Faults::default());
        let writer = due_overhaul_writer(&rig, Arc::new(FaultFs(faults.clone()))).await;
        let lineage = writer.lineage_id().unwrap();
        if publication_failure {
            faults.head.store(true, Ordering::SeqCst);
        } else {
            faults.close.store(true, Ordering::SeqCst);
        }
        assert!(writer.shutdown().await.is_err());
        let path = rig.service.wal.ephemeral_wal_path(&rig.key);
        assert!(path.exists());
        assert!(fs::read_dir(rig.root.join("open-writers"))
            .unwrap()
            .next()
            .is_some());
        let before = rig.service.wal.read_records(&rig.key).unwrap();
        assert_eq!(
            before
                .iter()
                .filter(|frame| matches!(frame.record, WalRecord::WriterClosed { .. }))
                .count(),
            usize::from(publication_failure)
        );
        let registry = rig.registry(None);
        let mut next = request(&rig.key, "r2");
        next.retention = Some(1);
        let outcome = registry
            .route_send(&rig.key, next, crate::executor::RunKind::FreshOrResume)
            .await
            .unwrap();
        assert!(
            matches!(outcome, crate::actor::SendOutcome::Active { .. }),
            "{outcome:?}"
        );
        let observed = rig
            .service
            .wal
            .read_records_range(&rig.key, 0, None, None)
            .unwrap();
        assert_eq!(
            observed.lineage_id,
            Some(lineage),
            "the due retained log must not be deleted before its successor opens"
        );
        let records = rig.service.wal.read_records(&rig.key).unwrap();
        assert!(records.iter().any(|frame| matches!(&frame.record, WalRecord::RunStarted { run_id, .. } if run_id.0 == "r1")));
        assert!(records.iter().any(|frame| matches!(frame.record, WalRecord::WriterOpened { recovered_through_seq, previous_writer_id: Some(_), .. } if recovered_through_seq >= before.last().unwrap().seq)));
        assert!(rig.service.tombstones(&rig.key).unwrap().is_empty());
        assert!(rig.service.overhaul_deferred_attempts() > 0);
        registry.stop_and_seal().await;
    }

    #[tokio::test]
    async fn overdue_failed_close_publication_is_reopened_on_next_send() {
        next_send_reopens_due_history(true).await;
    }
    #[tokio::test]
    async fn overdue_interrupted_close_is_reopened_on_next_send() {
        next_send_reopens_due_history(false).await;
    }
}

fn started(key: &BindIdentity, run: &str, retention: Option<u64>, ts: u64) -> WalRecord {
    let mut record: WalRecord = serde_json::from_str(include_str!(
        "../../broca-wal/tests/golden/pre_step_transforms/run_started.json"
    ))
    .unwrap();
    if let WalRecord::RunStarted {
        session,
        run_id,
        retention: value,
        ts_ms,
        title,
        ..
    } = &mut record
    {
        *session = key.clone();
        *run_id = RunId(run.into());
        *value = retention;
        *ts_ms = Some(ts);
        *title = Some(MARKER.into());
    }
    record
}
fn finished(ts: u64) -> WalRecord {
    WalRecord::RunFinished {
        interrupt_reason: None,
        ts_ms: Some(ts),
        reason: RunFinishReason::Completed,
        usage: Default::default(),
        error: None,
        retries_used: 0,
    }
}
fn framed(records: Vec<WalRecord>) -> Vec<FramedRecord> {
    records
        .into_iter()
        .enumerate()
        .map(|(n, record)| FramedRecord {
            seq: n as u64 + 1,
            fence: 1,
            record,
        })
        .collect()
}
fn request(key: &BindIdentity, run: &str) -> broca_core::RunRequest {
    let submission = serde_json::from_value(serde_json::json!({
        "model": {"provider_module_id":"stub", "model_id":"m"},
        "input":[{"role":"user", "content":[{"type":"text", "text": MARKER}]}],
        "stop_when":[{"type":"step_count", "n":3}]
    }))
    .unwrap();
    let mut req = broca_core::RunRequest::from_submission(
        submission,
        RunId(run.into()),
        key.clone(),
        broca_protocol::SubmissionId(String::new()),
    );
    req.submission_id = None;
    req.title = Some(MARKER.into());
    req.retention = Some(RETAINED_SECONDS);
    req
}
struct RealExecutor {
    reply: String,
    wal: Arc<FileWalStore>,
    store: Arc<SqliteStore>,
}
#[async_trait::async_trait]
impl crate::executor::RunExecutor for RealExecutor {
    async fn execute(
        &self,
        job: crate::executor::RunJob,
        out: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
    ) -> Result<broca_core::RunSummary, broca_core::RunError> {
        let mode = match job.kind {
            crate::executor::RunKind::FreshOrResume => broca_core::StartMode::FreshOrResume,
            crate::executor::RunKind::AppendEpisode => broca_core::StartMode::AppendEpisode,
            crate::executor::RunKind::ResumeAfterAuth => broca_core::StartMode::ResumeAfterAuth,
        };
        broca_core::run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            &job.writer,
            mode,
            &broca_core::StubProvider::new(vec![vec![
                broca_protocol::NormalizedEvent::Control(
                    broca_protocol::ProviderControl::AssistantMessage {
                        message: broca_protocol::content::AssistantMessage {
                            message_id: broca_protocol::MessageId("m1".into()),
                            content: vec![broca_protocol::content::ContentBlock::Text {
                                text: self.reply.clone(),
                            }],
                        },
                    },
                ),
                broca_protocol::NormalizedEvent::Control(
                    broca_protocol::ProviderControl::Finished {
                        usage: Default::default(),
                        finish_reason: broca_protocol::FinishReason::Stop,
                        retries_used: 0,
                    },
                ),
            ]]),
            &broca_core::StubToolPlane::new(),
            &broca_core::AllowAll,
            self.wal.as_ref(),
            Some(self.store.clone()),
            job.control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        )
        .await
    }
}

#[test]
fn zero_is_refused() {
    assert_eq!(
        validate_retention(Some(0), None, false).unwrap_err(),
        InvalidRetention::new()
    );
}
#[test]
fn above_max_names_the_bound() {
    assert_eq!(
        validate_retention(Some(MAX_RETENTION_SECONDS + 1), None, false).unwrap_err(),
        InvalidRetention::new().with_max_seconds(MAX_RETENTION_SECONDS)
    );
}
#[test]
fn lengthening_is_refused() {
    assert!(validate_retention(Some(11), Some(10), true).is_err());
}
#[test]
fn forever_to_retention_is_refused() {
    assert!(validate_retention(Some(1), None, true).is_err());
}
#[test]
fn equal_and_absent_retention_are_noops() {
    assert_eq!(validate_retention(Some(10), Some(10), true), Ok(None));
    assert_eq!(validate_retention(None, Some(10), true), Ok(None));
    assert_eq!(validate_retention(Some(5), Some(10), true), Ok(Some(5)));
}

#[test]
fn maintenance_is_not_activity_and_nonterminal_runs_never_expire() {
    let rig = Rig::new();
    let active = framed(vec![started(&rig.key, "r1", Some(1), 100)]);
    assert_eq!(policy(&active), Some((1, 100, None)));
    let paused = framed(vec![
        started(&rig.key, "r1", Some(1), 100),
        WalRecord::RunPaused {
            run_id: RunId("r1".into()),
            ts_ms: Some(200),
            reason: broca_protocol::PauseReason::AuthRequired,
            error: None,
        },
    ]);
    assert_eq!(policy(&paused), Some((1, 200, None)));
    let terminal = framed(vec![started(&rig.key, "r1", Some(1), 100), finished(200)]);
    assert_eq!(policy(&terminal), Some((1, 200, Some(1200))));
    let mut warmed = terminal;
    warmed.extend(framed(vec![serde_json::from_value(serde_json::json!({
        "type":"cache_warmed", "run_id":"r1", "step_id":1,
        "request_body_sha256":"hash", "warm_body_sha256":"warm", "usage":{},
        "sent_at_ms":10000
    }))
    .unwrap()]));
    assert_eq!(policy(&warmed), Some((1, 200, Some(1200))));
}

#[test]
fn repair_reconstructs_missing_run_rows_and_generation_removal_is_checked() {
    let rig = Rig::new();
    let id = rig.seed(1, 100, Some(200));
    assert!(rig.store.lookup_run("r1").unwrap().is_none());
    rig.service.rederive(&rig.key).unwrap();
    let row = rig.store.lookup_run("r1").unwrap().unwrap();
    assert_eq!(row.state, RunIndexState::Completed);
    assert_eq!(row.title, None);
    assert_eq!(
        rig.store
            .lookup_run_retention("r1")
            .unwrap()
            .unwrap()
            .lineage_id,
        Some(id)
    );
    let repairs = Repairs::default();
    repairs.insert(&rig.key);
    let (_, generation) = repairs.snapshot().pop().unwrap();
    repairs.insert(&rig.key);
    repairs.remove_if_unchanged(&rig.key, generation);
    assert_eq!(repairs.snapshot().len(), 1);
    repairs.remove_if_unchanged(&rig.key, generation + 1);
    assert!(repairs.snapshot().is_empty());
}

#[test]
fn enrollment_skips_matching_bytes_and_replays_mismatches() {
    let rig = Rig::new();
    rig.seed(1, 100, Some(200));
    assert_eq!(rig.service.enroll().unwrap(), 1);
    assert_eq!(rig.service.enroll().unwrap(), 0);
    let mut wal = rig.service.wal.open(&rig.key).unwrap();
    wal.append(1, &started(&rig.key, "r2", None, 300), true)
        .unwrap();
    assert_eq!(rig.service.enroll().unwrap(), 1);
    assert_eq!(
        rig.store
            .lookup_live_retention_lineage(&rig.key)
            .unwrap()
            .unwrap()
            .expires_at_ms,
        None
    );
}

fn assert_enrollment_deferred_log(
    capture: &broca_test_support::LogCapture,
    kind: &str,
    level: tracing::Level,
) {
    let event = capture
        .events()
        .into_iter()
        .find(|event| {
            event.target.starts_with("retention") && event.message() == "enrollment deferred"
        })
        .expect("enrollment deferral must be logged");
    assert_eq!(event.level, level);
    assert_eq!(event.fields.get("kind").map(String::as_str), Some(kind));
    assert!(event
        .fields
        .get("error")
        .is_some_and(|error| !error.is_empty()));
    if level == tracing::Level::DEBUG {
        assert_eq!(event.fields.get("counter").map(String::as_str), Some("1"));
    }
}

#[test]
fn enrollment_defers_a_vanished_wal_at_debug_with_a_counter() {
    let rig = Rig::new();
    rig.seed(1, 100, Some(200));
    let service = rig.service.clone();
    *service.before_enroll.lock().unwrap() = Some(Arc::new(move |path| {
        fs::remove_file(path).unwrap();
    }));
    let capture = broca_test_support::LogCapture::new();

    capture.in_scope(|| rig.service.enroll().unwrap());

    assert_enrollment_deferred_log(&capture, "vanished_file", tracing::Level::DEBUG);
}

#[test]
fn enrollment_warns_with_kind_for_a_mid_write_wal() {
    let rig = Rig::new();
    rig.seed(1, 100, Some(200));
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    fs::remove_file(path.with_extension("session")).unwrap();
    let mut bytes = fs::read(&path).unwrap();
    let oversized_tail = u32::try_from(bytes.len() + 1).unwrap();
    bytes[..4].copy_from_slice(&oversized_tail.to_le_bytes());
    fs::write(&path, bytes).unwrap();
    assert!(matches!(
        rig.service.read_enrollment_path(&path),
        Err(EnrollmentFailure {
            kind: EnrollmentFailureKind::MidWrite,
            ..
        })
    ));
    let capture = broca_test_support::LogCapture::new();

    capture.in_scope(|| rig.service.enroll().unwrap());

    assert_enrollment_deferred_log(&capture, "mid_write", tracing::Level::WARN);
}

#[test]
fn enrollment_warns_with_kind_for_an_unreadable_wal() {
    let rig = Rig::new();
    rig.seed(1, 100, Some(200));
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    fs::remove_file(path.with_extension("session")).unwrap();
    let mut bytes = fs::read(&path).unwrap();
    bytes[5..13].copy_from_slice(&2u64.to_le_bytes());
    fs::write(&path, bytes).unwrap();
    assert!(matches!(
        rig.service.read_enrollment_path(&path),
        Err(EnrollmentFailure {
            kind: EnrollmentFailureKind::UnreadableFile,
            ..
        })
    ));
    let capture = broca_test_support::LogCapture::new();

    capture.in_scope(|| rig.service.enroll().unwrap());

    assert_enrollment_deferred_log(&capture, "unreadable_file", tracing::Level::WARN);
}

#[test]
fn live_writer_deferral_waits_for_writer_release_before_retrying() {
    let rig = Rig::new();
    rig.seed(1, 100, Some(200));
    let lease = rig.service.lease(&rig.key).unwrap();
    let wal = rig
        .service
        .wal
        .open_with_retention(&rig.key, Some(1))
        .unwrap();
    rig.service.file_work.store(0, Ordering::Relaxed);
    let walks_before = rig.service.directory_walks.load(Ordering::Relaxed);
    let capture = broca_test_support::LogCapture::new();

    capture.in_scope(|| assert_eq!(rig.service.enroll_changes().unwrap(), 0));

    let logs: Vec<_> = capture
        .events()
        .into_iter()
        .filter(|event| {
            event.target == "retention.enrollment" && event.message() == "enrollment deferred"
        })
        .collect();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert_eq!(logs[0].level, tracing::Level::DEBUG);
    assert_eq!(logs[0].fields["kind"], "live_writer");
    let first_pass_work = rig.service.file_work.load(Ordering::Relaxed);
    assert!(
        first_pass_work > 0,
        "the initial attempt must inspect the WAL before discovering the held lease"
    );

    let (pending_changes, overflow) = rig.service.wal.take_retention_changes();
    assert!(
        pending_changes.is_empty() && !overflow,
        "{pending_changes:?}, overflow={overflow}"
    );
    rig.service.enroll_changes().unwrap();
    assert_eq!(
        rig.service.directory_walks.load(Ordering::Relaxed),
        walks_before + 1,
        "recovery does not rescan the directory after its first pass"
    );
    assert_eq!(
        rig.service.file_work.load(Ordering::Relaxed),
        first_pass_work,
        "a held writer is not reopened on each sweep"
    );

    drop(wal);
    drop(lease);
    assert_eq!(rig.service.enroll_changes().unwrap(), 1);
    assert!(rig
        .store
        .lookup_live_retention_lineage(&rig.key)
        .unwrap()
        .is_some());
}

#[test]
fn enrollment_defers_a_busy_store_at_debug_with_a_counter() {
    let mut rig = Rig::new();
    rig.seed(1, 100, Some(200));
    let index = Arc::new(FailingIndex {
        store: rig.store.clone(),
        fail: AtomicBool::new(false),
        busy_live: AtomicBool::new(true),
    });
    rig.service = Arc::new(
        RetentionService::new(
            rig.service.wal.clone(),
            index,
            rig.leases.clone(),
            rig.store.clone(),
            Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1)),
        )
        .unwrap(),
    );
    let capture = broca_test_support::LogCapture::new();

    capture.in_scope(|| rig.service.enroll().unwrap());

    assert_enrollment_deferred_log(&capture, "store_busy", tracing::Level::DEBUG);
}

#[test]
fn enrollment_classifies_a_real_sqlite_busy_error_as_store_busy() {
    let rig = Rig::new();
    rig.seed(1, 100, Some(200));
    let blocker = rusqlite::Connection::open(rig.root.join("run-index.db")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let capture = broca_test_support::LogCapture::new();

    capture.in_scope(|| rig.service.enroll().unwrap());

    assert_enrollment_deferred_log(&capture, "store_busy", tracing::Level::DEBUG);
    let event = capture
        .events()
        .into_iter()
        .find(|event| event.fields.get("kind").map(String::as_str) == Some("store_busy"))
        .expect("the real SQLite busy error must be recorded");
    let error = event
        .fields
        .get("error")
        .expect("the store error is logged");
    assert!(
        error.contains("database is locked") || error.contains("database is busy"),
        "expected SQLite's busy error, got: {error}"
    );
}

#[test]
fn sweep_confirms_wal_before_tombstoning_a_stale_candidate() {
    let rig = Rig::new();
    let id = rig.seed(1, 100, Some(200));
    rig.service.rederive(&rig.key).unwrap();
    let mut wal = rig.service.wal.open(&rig.key).unwrap();
    wal.append(1, &started(&rig.key, "r2", None, 1000), true)
        .unwrap();
    wal.append(1, &finished(1500), true).unwrap();
    let _lease = rig.service.lease(&rig.key).unwrap();
    assert_eq!(rig.service.delete_expired(&rig.key, 1600).unwrap(), None);
    assert!(!rig.service.wal.tombstone_path(&rig.key, &id).exists());
    assert_eq!(
        rig.store
            .lookup_retention_lineage(&id)
            .unwrap()
            .unwrap()
            .expires_at_ms,
        Some(2500)
    );
}

/// Deleting a tagged lineage records the tag its next part would have had, and the address's
/// next lineage counts on from there, so no tag is reused across the deletion.
#[test]
fn deletion_records_the_next_tag_and_a_later_lineage_starts_past_it() {
    let rig = Rig::new();
    {
        let _lease = rig.service.lease(&rig.key).unwrap();
        let mut wal = rig
            .service
            .wal
            .open_with_retention(&rig.key, Some(1))
            .unwrap();
        let mut start = started(&rig.key, "r1", Some(1), 100);
        if let WalRecord::RunStarted { tags, .. } = &mut start {
            *tags = Some(Arc::new(vec![broca_protocol::tags::MessageTags::new(
                vec![41],
            )]));
        }
        wal.append(1, &start, true).unwrap();
        wal.append(1, &finished(200), true).unwrap();
        rig.service.remember_session(&rig.key).unwrap();
    }
    assert_eq!(rig.service.wal.retired_tag_floor(&rig.key).unwrap(), 1);
    let _lease = rig.service.lease(&rig.key).unwrap();
    assert!(rig
        .service
        .delete_expired(&rig.key, 1200)
        .unwrap()
        .is_some());
    let stone = rig.service.tombstones(&rig.key).unwrap().pop().unwrap();
    assert_eq!(stone.next_tag, Some(42));
    assert_eq!(rig.service.wal.retired_tag_floor(&rig.key).unwrap(), 42);
    // The next lineage at the address is unstarted; seeded with the floor, its first part gets
    // the next unused tag rather than 1.
    assert_eq!(
        broca_core::LineageTags::unstarted()
            .with_floor(rig.service.wal.retired_tag_floor(&rig.key).unwrap())
            .next(),
        Some(42)
    );
}

/// Deleting a lineage that recorded compaction notices records the sequence number its next
/// notice would have had, and keeps the higher number of an earlier tombstone, so the address's
/// next lineage numbers its notices past every notice the session published.
#[test]
fn deletion_records_the_next_notice_seq_and_keeps_the_highest() {
    let rig = Rig::new();
    let notice = |seq: u64| WalRecord::CompactionNotice {
        seq,
        headers: std::collections::BTreeMap::from([("seq".to_owned(), seq.to_string())]),
    };
    {
        let _lease = rig.service.lease(&rig.key).unwrap();
        let mut wal = rig
            .service
            .wal
            .open_with_retention(&rig.key, Some(1))
            .unwrap();
        wal.append(1, &started(&rig.key, "r1", Some(1), 100), true)
            .unwrap();
        for seq in 1..=3 {
            wal.append(1, &notice(seq), true).unwrap();
        }
        wal.append(1, &finished(200), true).unwrap();
        rig.service.remember_session(&rig.key).unwrap();
    }
    assert_eq!(rig.service.wal.retired_notice_floor(&rig.key).unwrap(), 1);
    let _lease = rig.service.lease(&rig.key).unwrap();
    assert!(rig
        .service
        .delete_expired(&rig.key, 1200)
        .unwrap()
        .is_some());
    let stone = rig.service.tombstones(&rig.key).unwrap().pop().unwrap();
    assert_eq!(stone.next_notice_seq, Some(4));
    assert_eq!(rig.service.wal.retired_notice_floor(&rig.key).unwrap(), 4);
}

#[test]
fn sweep_deletes_and_keeps_terminal_content_free_rows() {
    let rig = Rig::new();
    let id = rig.seed(1, 100, Some(200));
    let _lease = rig.service.lease(&rig.key).unwrap();
    assert_eq!(
        rig.service.delete_expired(&rig.key, 1200).unwrap(),
        Some(id.clone())
    );
    assert!(rig
        .service
        .wal
        .resolve_live_path(&rig.key)
        .unwrap()
        .is_none());
    let row = rig.store.lookup_run("r1").unwrap().unwrap();
    assert_eq!(row.state, RunIndexState::Completed);
    assert_eq!(row.title, None);
    assert_eq!(
        rig.service.expired(&rig.key, Some("r1")).unwrap(),
        Some(1200)
    );
    assert_eq!(
        rig.service
            .expired(&rig.key, Some("never-existed"))
            .unwrap(),
        None
    );
    let stone = rig.service.tombstones(&rig.key).unwrap().pop().unwrap();
    assert_eq!(stone.seq, 1);
    assert_eq!(stone.expired_at_ms, 1200);
    assert_eq!(stone.completed_at_ms, Some(1200));
    for path in [
        rig.root.join("run-index.db"),
        rig.root.join("run-index.db-wal"),
        rig.root.join("run-index.db-shm"),
    ] {
        if let Ok(bytes) = fs::read(path) {
            assert!(!bytes
                .windows(MARKER.len())
                .any(|bytes| bytes == MARKER.as_bytes()));
        }
    }
}

#[tokio::test]
async fn all_delivery_modes_validate_before_any_wal_write() {
    let rig = Rig::new();
    let registry = rig.registry(None);
    for delivery in [
        broca_protocol::Delivery::Queue,
        broca_protocol::Delivery::Steer,
        broca_protocol::Delivery::Interrupt,
    ] {
        for seconds in [0, MAX_RETENTION_SECONDS + 1] {
            let req = request(&rig.key, "bad");
            let result = registry
                .route_submission_with_retention(
                    &rig.key,
                    req.run_id.clone(),
                    req.to_submission(),
                    crate::executor::RunKind::FreshOrResume,
                    crate::actor::SendKey::Minted,
                    true,
                    delivery,
                    None,
                    None,
                    req.title,
                    Some(seconds),
                    false,
                )
                .await;
            assert!(matches!(
                result,
                Err(crate::registry::OpenError::RetentionInvalid(_))
            ));
            assert!(rig
                .service
                .wal
                .resolve_live_path(&rig.key)
                .unwrap()
                .is_none());
        }
    }
    rig.seed(
        RETAINED_SECONDS,
        rig.service.clock.now_ms(),
        Some(rig.service.clock.now_ms()),
    );
    let before = fs::metadata(rig.service.wal.ephemeral_wal_path(&rig.key))
        .unwrap()
        .len();
    for delivery in [
        broca_protocol::Delivery::Queue,
        broca_protocol::Delivery::Steer,
        broca_protocol::Delivery::Interrupt,
    ] {
        let req = request(&rig.key, "bad");
        assert!(matches!(
            registry
                .route_submission_with_retention(
                    &rig.key,
                    req.run_id.clone(),
                    req.to_submission(),
                    crate::executor::RunKind::AppendEpisode,
                    crate::actor::SendKey::Minted,
                    true,
                    delivery,
                    None,
                    None,
                    req.title,
                    Some(RETAINED_SECONDS + 1),
                    false,
                )
                .await,
            Err(crate::registry::OpenError::RetentionInvalid(_))
        ));
        assert_eq!(
            fs::metadata(rig.service.wal.ephemeral_wal_path(&rig.key))
                .unwrap()
                .len(),
            before
        );
    }
}

async fn wait_idle(registry: &crate::registry::LineageRegistry, key: &BindIdentity) {
    broca_test_support::event_timeout(
        "wait_idle",
        async {
            loop {
                if registry.route_status(key).await.unwrap() == crate::actor::LineageStatus::Idle {
                    return;
                }
                tokio::task::yield_now().await;
            }
        },
        || format!("slot={:?}", registry.slot_state(key)),
    )
    .await
    .expect("run finishes");
}

#[tokio::test]
async fn first_retained_send_records_policy_and_never_indexes_the_title() {
    let rig = Rig::new();
    let registry = rig.registry(None);
    let req = request(&rig.key, "real-run");
    assert!(matches!(
        registry
            .route_send(&rig.key, req, crate::executor::RunKind::FreshOrResume)
            .await
            .unwrap(),
        crate::actor::SendOutcome::Active { .. }
    ));
    wait_idle(&registry, &rig.key).await;
    let records = rig.service.wal.read_records(&rig.key).unwrap();
    assert!(
        matches!(&records[0].record, WalRecord::RunStarted { retention: Some(RETAINED_SECONDS), title: Some(title), .. } if title == MARKER)
    );
    let retention = rig
        .store
        .lookup_live_retention_lineage(&rig.key)
        .unwrap()
        .unwrap();
    assert_eq!(
        retention.wal_bytes,
        fs::metadata(rig.service.wal.ephemeral_wal_path(&rig.key))
            .unwrap()
            .len(),
        "byte progress includes trailing lineage frames"
    );
    let row = rig.store.lookup_run("real-run").unwrap().unwrap();
    assert_eq!(row.title, None);
    assert_eq!(row.state, RunIndexState::Completed);
    assert!(rig
        .store
        .read_admission_snapshot(&rig.key)
        .unwrap()
        .is_none());
    assert!(rig
        .store
        .lookup_run_retention("real-run")
        .unwrap()
        .unwrap()
        .lineage_id
        .is_some());
    for path in [
        rig.root.join("run-index.db"),
        rig.root.join("run-index.db-wal"),
        rig.root.join("run-index.db-shm"),
    ] {
        if let Ok(bytes) = fs::read(path) {
            assert!(!bytes
                .windows(MARKER.len())
                .any(|bytes| bytes == MARKER.as_bytes()));
        }
    }
    assert!(registry.stop_and_seal().await);
}

/// A later run in a retained session must carry the lineage on its run-index row even when
/// admission takes the bounded retention sync, which projects no run rows once the WAL is
/// longer than the 256 KiB tail it may read. A run row without a lineage is invisible to
/// withdrawal discovery and to expiry accounting that reads runs by lineage.
#[tokio::test]
async fn later_retained_run_in_a_long_session_records_its_lineage_at_admission() {
    let rig = Rig::new();
    let send = |registry: &crate::registry::LineageRegistry,
                run: &'static str,
                kind: crate::executor::RunKind| {
        let registry = registry.clone();
        let key = rig.key.clone();
        async move {
            assert!(matches!(
                registry
                    .route_send(&key, request(&key, run), kind)
                    .await
                    .unwrap(),
                crate::actor::SendOutcome::Active { .. }
            ));
            wait_idle(&registry, &key).await;
        }
    };
    // A short first run, as in production, so its admission projects the lineage summary.
    let short = rig.registry(None);
    send(&short, "first-run", crate::executor::RunKind::FreshOrResume).await;
    assert!(short.stop_and_seal().await);
    // Long replies push the WAL past the bounded tail, as long sidekick sessions do.
    let long = rig.registry_with_reply(None, "x".repeat(400 * 1024));
    send(&long, "growth-run", crate::executor::RunKind::AppendEpisode).await;
    assert!(
        fs::metadata(rig.service.wal.ephemeral_wal_path(&rig.key))
            .unwrap()
            .len()
            > 256 * 1024,
        "the session must outgrow the bounded retention tail"
    );
    send(&long, "later-run", crate::executor::RunKind::AppendEpisode).await;
    let lineage = rig
        .store
        .lookup_live_retention_lineage(&rig.key)
        .unwrap()
        .unwrap()
        .lineage_id;
    for run in ["first-run", "growth-run", "later-run"] {
        let retention = rig.store.lookup_run_retention(run).unwrap().unwrap();
        assert_eq!(
            retention.lineage_id.as_deref(),
            Some(lineage.as_str()),
            "later retained run lost its lineage: {run}"
        );
        let row = rig.store.lookup_run(run).unwrap().unwrap();
        assert_eq!(row.state, RunIndexState::Completed);
        assert_eq!(row.title, None, "retained run rows never store a title");
    }
    let indexed: Vec<_> = rig
        .store
        .retention_run_rows(&lineage)
        .unwrap()
        .into_iter()
        .map(|run| run.run.run_id)
        .collect();
    assert_eq!(indexed, ["first-run", "growth-run", "later-run"]);
    assert!(long.stop_and_seal().await);
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn expiry_timer_ends_subscriptions_releases_lease_and_fresh_send_completes_deletion() {
    let rig = Rig::new();
    let registry = rig.registry(None);
    registry
        .route_send(
            &rig.key,
            request(&rig.key, "r1"),
            crate::executor::RunKind::FreshOrResume,
        )
        .await
        .unwrap();
    wait_idle(&registry, &rig.key).await;
    let (sink, mut deliveries) = tokio::sync::mpsc::channel(64);
    registry
        .route_subscribe(&rig.key, crate::fanout::FromCursor::Start, sink)
        .await
        .unwrap();
    // The run finished at or before the real time now, so moving the clock forward by the
    // whole retention window puts its deadline in the past. The actor re-arms its expiry
    // timer on the clock change; no wall-clock wait is involved.
    fs::write(
        rig.root.join("test-clock"),
        serde_json::to_vec(&serde_json::json!({"offset_ms": RETAINED_SECONDS * 1000})).unwrap(),
    )
    .unwrap();
    rig.service.clock.refresh().unwrap();
    let expired_at = broca_test_support::event_timeout(
        "expiry_timer_ends_subscriptions_releases_lease_and_fresh_send_completes_deletion",
        async {
            loop {
                if let Some(crate::fanout::Delivery::Expired { expired_at_ms }) =
                    deliveries.recv().await
                {
                    return expired_at_ms;
                }
            }
        },
        || {
            broca_test_support::record_kinds(
                rig.service
                    .wal
                    .read_records(&rig.key)
                    .unwrap()
                    .iter()
                    .map(|f| &f.record),
            )
        },
    )
    .await
    .expect("subscriber does not keep lineage alive");
    let old_id = rig
        .store
        .lookup_run_retention("r1")
        .unwrap()
        .unwrap()
        .lineage_id
        .unwrap();
    broca_test_support::event_timeout(
        "expiry_timer_ends_subscriptions_releases_lease_and_fresh_send_completes_deletion",
        async {
            while registry.slot_counts().open + registry.slot_counts().closing > 0 {
                tokio::task::yield_now().await;
            }
        },
        || {
            broca_test_support::record_kinds(
                rig.service
                    .wal
                    .read_records(&rig.key)
                    .unwrap()
                    .iter()
                    .map(|f| &f.record),
            )
        },
    )
    .await
    .expect("expiry finishes actor teardown");
    let lease = rig
        .service
        .lease(&rig.key)
        .expect("timer released the lease");
    drop(lease);
    assert!(
        matches!(registry.route_read(&rig.key, None, None).await, Err(crate::registry::OpenError::Expired { expired_at_ms }) if expired_at_ms == expired_at)
    );
    assert!(matches!(
        registry.route_warm(&rig.key, None).await,
        Err(crate::registry::OpenError::Expired { .. })
    ));
    assert!(matches!(
        registry.route_cancel(&rig.key, RunId("r1".into())).await,
        Err(crate::registry::OpenError::Expired { .. })
    ));
    assert!(matches!(
        registry
            .route_retract(&rig.key, broca_protocol::SubmissionId("sub-r1".into()))
            .await,
        Err(crate::registry::OpenError::Expired { .. })
    ));
    assert!(matches!(
        registry.route_run_state(&rig.key, RunId("r1".into())).await,
        Err(crate::registry::OpenError::Expired { .. })
    ));
    let mut fresh = request(&rig.key, "r2");
    fresh.retention = None;
    registry
        .route_send(&rig.key, fresh, crate::executor::RunKind::FreshOrResume)
        .await
        .unwrap();
    wait_idle(&registry, &rig.key).await;
    let stone = rig
        .service
        .tombstones(&rig.key)
        .unwrap()
        .into_iter()
        .find(|stone| stone.lineage_id == old_id)
        .unwrap();
    assert!(stone.completed_at_ms.is_some());
    assert_eq!(
        rig.service.expired(&rig.key, Some("r1")).unwrap(),
        Some(expired_at)
    );
    assert_eq!(rig.service.expired(&rig.key, None).unwrap(), None);
    assert!(rig.service.wal.wal_path(&rig.key).exists());
    assert!(!rig.service.wal.ephemeral_wal_path(&rig.key).exists());
    assert!(registry.route_read(&rig.key, None, None).await.is_ok());
    assert!(registry.stop_and_seal().await);
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn status_of_expired_session_refuses_without_opening_or_deleting() {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    rig.seed(1, now, Some(now));
    fs::write(rig.root.join("test-clock"), br#"{"offset_ms":2000}"#).unwrap();
    rig.service.clock.refresh().unwrap();
    let registry = rig.registry(None);
    assert!(matches!(registry.route_status(&rig.key).await,
        Err(crate::registry::OpenError::Expired { expired_at_ms }) if expired_at_ms == now + 1000));
    assert!(rig.service.wal.ephemeral_wal_path(&rig.key).exists());
    assert!(rig.service.tombstones(&rig.key).unwrap().is_empty());
}

/// A closed log above the status read limit is answered through a status-mode actor open
/// rather than a direct WAL read, so the opener itself must refuse an expired session before
/// its ordinary open path deletes the lineage and reports Idle.
#[cfg(feature = "test-clock")]
#[tokio::test]
async fn status_of_expired_large_session_refuses_in_the_status_opener() {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    {
        let _lease = rig.service.lease(&rig.key).unwrap();
        let mut wal = rig
            .service
            .wal
            .open_with_retention(&rig.key, Some(1))
            .unwrap();
        wal.append(1, &started(&rig.key, "r1", Some(1), now), true)
            .unwrap();
        for step in 1..=160 {
            wal.append(
                1,
                &WalRecord::ModelStepFinished {
                    fallback: Default::default(),
                    written_at_ms: None,
                    tags: None,
                    hooks: None,
                    step_id: broca_protocol::StepId(step),
                    assistant_message: broca_protocol::AssistantMessage {
                        message_id: broca_protocol::MessageId(format!("answer-{step}")),
                        content: vec![broca_protocol::ContentBlock::Text {
                            text: "x".repeat(8192),
                        }],
                    }
                    .into(),
                    usage: Default::default(),
                    finish_reason: Some(broca_protocol::FinishReason::Stop),
                    retries_used: 0,
                    service_tier_used: None,
                    speed_used: None,
                },
                false,
            )
            .unwrap();
        }
        wal.append(1, &finished(now), true).unwrap();
        rig.service.remember_session(&rig.key).unwrap();
    }
    assert!(
        rig.service.wal.live_wal_bytes(&rig.key).unwrap().unwrap()
            > crate::registry::STATUS_WAL_READ_MAX_BYTES,
        "the fixture must exceed the direct status read limit to reach the status opener"
    );
    fs::write(rig.root.join("test-clock"), br#"{"offset_ms":2000}"#).unwrap();
    rig.service.clock.refresh().unwrap();
    let registry = rig.registry(None);
    let result = registry.route_status(&rig.key).await;
    assert!(
        matches!(result, Err(crate::registry::OpenError::Expired { expired_at_ms }) if expired_at_ms == now + 1000),
        "{result:?}"
    );
    assert!(rig.service.wal.ephemeral_wal_path(&rig.key).exists());
    assert!(rig.service.tombstones(&rig.key).unwrap().is_empty());
    assert_eq!(registry.slot_counts().open, 0);
}

#[test]
fn sidecar_removal_failure_leaves_completion_retryable() {
    let rig = Rig::new();
    let id = rig.seed(1, 100, Some(200));
    let side = rig
        .service
        .wal
        .ephemeral_wal_path(&rig.key)
        .with_extension("session");
    let temp = side.with_extension("session.tmp");
    let stone = Tombstone {
        next_tag: None,
        next_notice_seq: None,
        lineage_id: id.clone(),
        expired_at_ms: 1200,
        seq: 1,
        completed_at_ms: None,
    };
    rig.service.write_tombstone(&rig.key, &stone).unwrap();
    fs::create_dir(&temp).unwrap();
    {
        let _lease = rig.service.lease(&rig.key).unwrap();
        assert!(rig.service.complete(&rig.key, stone, 1200).is_err());
    }
    let stone = rig.service.tombstones(&rig.key).unwrap().pop().unwrap();
    assert_eq!(stone.lineage_id, id);
    assert_eq!(
        stone.completed_at_ms, None,
        "a cut before sidecar removal must remain retryable"
    );
    fs::remove_dir(&temp).unwrap();
    fs::write(&temp, serde_json::to_vec(&rig.key).unwrap()).unwrap();
    assert_eq!(
        rig.service.resume_deletions(1300).unwrap().as_slice(),
        std::slice::from_ref(&rig.key)
    );
    assert!(!side.exists() && !temp.exists());
    assert_eq!(
        rig.service.tombstones(&rig.key).unwrap()[0].completed_at_ms,
        Some(1300)
    );
}

#[test]
fn matching_tombstone_precedes_the_clock_and_latest_sequence_wins() {
    let rig = Rig::new();
    let id = rig.seed(30, 100, Some(200));
    let stone = Tombstone {
        next_tag: None,
        next_notice_seq: None,
        lineage_id: id.clone(),
        expired_at_ms: u64::MAX - 10,
        seq: 2,
        completed_at_ms: None,
    };
    rig.service.write_tombstone(&rig.key, &stone).unwrap();
    assert_eq!(
        rig.service.expired(&rig.key, None).unwrap(),
        Some(u64::MAX - 10)
    );
    // With no live WAL, the highest tombstone sequence wins even when its expiry
    // timestamp is earlier than an older tombstone's because the clock went backwards.
    fs::remove_file(rig.service.wal.ephemeral_wal_path(&rig.key)).unwrap();
    rig.service
        .write_tombstone(
            &rig.key,
            &Tombstone {
                next_tag: None,
                next_notice_seq: None,
                lineage_id: "a".repeat(32),
                expired_at_ms: 1,
                seq: 3,
                completed_at_ms: Some(2),
            },
        )
        .unwrap();
    assert_eq!(rig.service.expired(&rig.key, None).unwrap(), Some(1));
}

#[test]
fn older_tombstone_does_not_shadow_fresh_retained_lineage() {
    let rig = Rig::new();
    let old = rig.seed(1, 100, Some(200));
    let lease = rig.service.lease(&rig.key).unwrap();
    assert_eq!(
        rig.service.delete_expired(&rig.key, 1200).unwrap(),
        Some(old.clone())
    );
    drop(lease);
    let stone = rig.service.tombstones(&rig.key).unwrap().pop().unwrap();
    assert_eq!(stone.lineage_id, old);
    assert!(stone.completed_at_ms.is_some());

    let now = rig.service.clock.now_ms();
    let fresh = rig.seed_run("fresh", RETAINED_SECONDS, now, Some(now));
    assert_ne!(fresh, old);
    assert_eq!(
        rig.service.wal.resolve_live_path(&rig.key).unwrap(),
        Some(rig.service.wal.ephemeral_wal_path(&rig.key))
    );
    // No summary exists for the fresh lineage: expiry must read its WAL and compare
    // that lineage's identity with the older tombstone before refreshing the summary.
    assert!(rig
        .store
        .lookup_live_retention_lineage(&rig.key)
        .unwrap()
        .is_none());
    assert_eq!(rig.service.expired(&rig.key, None).unwrap(), None);
    let row = rig
        .store
        .lookup_live_retention_lineage(&rig.key)
        .unwrap()
        .unwrap();
    assert_eq!(row.lineage_id, fresh);
    assert_eq!(rig.service.expired(&rig.key, None).unwrap(), None);
}

#[test]
fn unfinished_deletion_resumes_each_disk_boundary_and_never_moves_a_new_lineage() {
    for cut in 0..3 {
        let rig = Rig::new();
        let id = rig.seed(1, 100, Some(200));
        let stone = Tombstone {
            next_tag: None,
            next_notice_seq: None,
            lineage_id: id.clone(),
            expired_at_ms: 1200,
            seq: 1,
            completed_at_ms: None,
        };
        rig.service.write_tombstone(&rig.key, &stone).unwrap();
        let moved = rig.service.deleting(&rig.key, &id);
        if cut >= 1 {
            fs::rename(rig.service.wal.ephemeral_wal_path(&rig.key), &moved).unwrap();
        }
        if cut >= 2 {
            let read = rig.service.read_path(&moved).unwrap();
            let (row, runs) = derive(
                &rig.key,
                id.clone(),
                frames(&read),
                fs::metadata(&moved).unwrap().len(),
                Some(1200),
            )
            .unwrap();
            rig.service.index.upsert(&row, &runs).unwrap();
            fs::remove_file(&moved).unwrap();
        }
        if cut >= 1 {
            let mut fresh = rig.service.wal.open(&rig.key).unwrap();
            fresh
                .append(1, &started(&rig.key, "fresh", None, 5000), true)
                .unwrap();
        }
        let _lease = rig.service.lease(&rig.key).unwrap();
        rig.service.complete(&rig.key, stone, 1300).unwrap();
        assert!(!moved.exists());
        assert!(rig.service.tombstones(&rig.key).unwrap()[0]
            .completed_at_ms
            .is_some());
        assert_eq!(
            rig.store.lookup_run("r1").unwrap().unwrap().state,
            RunIndexState::Completed
        );
        if cut >= 1 {
            let records = rig.service.wal.read_records(&rig.key).unwrap();
            assert!(
                matches!(&records[0].record, WalRecord::RunStarted { run_id, retention: None, .. } if run_id.0 == "fresh")
            );
            assert_eq!(rig.service.expired(&rig.key, None).unwrap(), None);
        }
    }
}

struct FailingIndex {
    store: Arc<SqliteStore>,
    fail: AtomicBool,
    busy_live: AtomicBool,
}
impl RetentionIndex for FailingIndex {
    fn live(&self, key: &BindIdentity) -> Result<Option<RetentionLineageRow>, String> {
        if self.busy_live.load(Ordering::Relaxed) {
            return Err("database is locked".into());
        }
        RetentionIndex::live(self.store.as_ref(), key)
    }
    fn lineage(&self, id: &str) -> Result<Option<RetentionLineageRow>, String> {
        RetentionIndex::lineage(self.store.as_ref(), id)
    }
    fn candidates(&self, now: u64) -> Result<Vec<RetentionLineageRow>, String> {
        RetentionIndex::candidates(self.store.as_ref(), now)
    }
    fn run(&self, id: &str) -> Result<Option<RunRetention>, String> {
        RetentionIndex::run(self.store.as_ref(), id)
    }
    fn upsert(&self, row: &RetentionLineageRow, runs: &[RetentionRunRow]) -> Result<(), String> {
        if self.fail.load(Ordering::Relaxed) {
            return Err("injected index failure".into());
        }
        RetentionIndex::upsert(self.store.as_ref(), row, runs)
    }
}
impl RunIndex for FailingIndex {
    fn index_run_active(&self, _: &RunIndexRow) -> Result<(), String> {
        Err("injected initial active failure".into())
    }
    fn index_run_terminal(
        &self,
        id: &str,
        state: RunIndexState,
        usage: Option<broca_protocol::Usage>,
        reason: Option<broca_protocol::FinishReason>,
        retries: u32,
    ) -> Result<(), String> {
        self.store
            .index_run_terminal(id, state, usage, reason, retries)
    }
    fn lookup_run(&self, id: &str) -> Result<Option<RunIndexRow>, String> {
        self.store.lookup_run(id)
    }
}

#[tokio::test]
async fn failed_enrollment_and_active_write_are_repaired_without_a_read_or_restart() {
    let mut rig = Rig::new();
    let index = Arc::new(FailingIndex {
        store: rig.store.clone(),
        fail: AtomicBool::new(true),
        busy_live: AtomicBool::new(false),
    });
    rig.service = Arc::new(
        RetentionService::new(
            rig.service.wal.clone(),
            index.clone(),
            rig.leases.clone(),
            rig.store.clone(),
            Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1)),
        )
        .unwrap(),
    );
    let registry = rig.registry(Some(index.clone()));
    assert_eq!(registry.retention_repairs_pending(), 0);
    registry
        .route_send(
            &rig.key,
            request(&rig.key, "r1"),
            crate::executor::RunKind::FreshOrResume,
        )
        .await
        .unwrap();
    // Wait for the session actor to clear the lineage registry's in-flight count,
    // without a read that could itself repair the missing run-index row.
    broca_test_support::event_timeout(
        "failed_enrollment_and_active_write_are_repaired_without_a_read_or_restart",
        async {
            while registry.slot_counts().runs_in_flight > 0 {
                tokio::task::yield_now().await;
            }
        },
        || {
            broca_test_support::record_kinds(
                rig.service
                    .wal
                    .read_records(&rig.key)
                    .unwrap()
                    .iter()
                    .map(|f| &f.record),
            )
        },
    )
    .await
    .unwrap();
    assert!(rig.store.lookup_run("r1").unwrap().is_none());
    assert_eq!(registry.retention_repairs_pending(), 1);
    index.fail.store(false, Ordering::Relaxed);
    registry
        .sweep_once(rig.service.clock.now_ms())
        .await
        .unwrap();
    assert_eq!(
        rig.store.lookup_run("r1").unwrap().unwrap().state,
        RunIndexState::Completed
    );
    assert_eq!(registry.retention_repairs_pending(), 0);
    assert!(rig
        .store
        .lookup_run_retention("r1")
        .unwrap()
        .unwrap()
        .lineage_id
        .is_some());
    assert!(registry.stop_and_seal().await);
    registry
        .sweep_once(rig.service.clock.now_ms() + RETAINED_SECONDS * 1000 + 1000)
        .await
        .unwrap();
    let expiry = rig
        .store
        .lookup_run_retention("r1")
        .unwrap()
        .unwrap()
        .expired_at_ms
        .unwrap();
    assert_eq!(
        rig.service.expired(&rig.key, Some("r1")).unwrap(),
        Some(expiry)
    );
}

struct OrderedExport {
    store: Arc<SqliteStore>,
    wal: Arc<FileWalStore>,
    key: BindIdentity,
    calls: Mutex<Vec<String>>,
    fail: AtomicBool,
}
impl ExportIndex for OrderedExport {
    fn missing_terminal_runs(
        &self,
    ) -> Result<Vec<broca_core::MissingExportRun>, broca_core::ExportError> {
        self.store.missing_terminal_runs()
    }
    fn emit_run(
        &self,
        records: &[FramedRecord],
        run: &RunId,
        resolver: &dyn broca_core::ChargeBasisResolver,
    ) -> Result<(), broca_core::ExportError> {
        assert!(
            !self.wal.tombstone_path(&self.key, "").exists(),
            "export precedes tombstone"
        );
        self.calls.lock().unwrap().push(run.0.clone());
        if self.fail.load(Ordering::Relaxed) {
            return Err(broca_core::ExportError::Storage(
                "injected export failure".into(),
            ));
        }
        self.store.emit_run(records, run, resolver)
    }
    fn read_page(
        &self,
        since: Option<broca_core::ExportCursor>,
        limit: u64,
    ) -> Result<broca_core::ExportPage, broca_core::ExportError> {
        ExportIndex::read_page(self.store.as_ref(), since, limit)
    }
    fn high_water(&self) -> u64 {
        self.store.high_water()
    }
    fn source_epoch(&self) -> u64 {
        self.store.source_epoch()
    }
    fn source_instance_id(&self) -> &str {
        self.store.source_instance_id()
    }
}

#[test]
fn export_failure_warns_with_run_ids_and_does_not_delay_deletion() {
    let mut rig = Rig::new();
    let id = rig.seed(1, 100, Some(200));
    let mut wal = rig.service.wal.open(&rig.key).unwrap();
    wal.append(1, &started(&rig.key, "r2", None, 300), true)
        .unwrap();
    wal.append(1, &finished(400), true).unwrap();
    let export = Arc::new(OrderedExport {
        store: rig.store.clone(),
        wal: rig.service.wal.clone(),
        key: rig.key.clone(),
        calls: Default::default(),
        fail: AtomicBool::new(true),
    });
    rig.service = Arc::new(
        RetentionService::new(
            rig.service.wal.clone(),
            export.clone(),
            rig.leases.clone(),
            export.clone(),
            Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1)),
        )
        .unwrap(),
    );
    let _lease = rig.service.lease(&rig.key).unwrap();
    let logs = broca_test_support::LogCapture::new();
    let result = logs.in_scope(|| rig.service.delete_expired(&rig.key, 1400));
    assert_eq!(result.unwrap(), Some(id.clone()));
    assert!(!rig.service.wal.ephemeral_wal_path(&rig.key).exists());
    assert!(rig.service.tombstones(&rig.key).unwrap()[0]
        .completed_at_ms
        .is_some());
    assert_eq!(*export.calls.lock().unwrap(), ["r1", "r2"]);
    let warnings: Vec<_> = logs
        .events()
        .into_iter()
        .filter(|event| event.level == tracing::Level::WARN)
        .collect();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    let warning = format!("{:?}", warnings[0]);
    assert!(
        warning.contains("r1") && warning.contains("r2"),
        "{warning}"
    );
    assert!(warning.contains("session"), "{warning}");
    assert!(!warning.contains(MARKER));
    assert!(!warning.contains("injected export failure"));
}

#[tokio::test]
async fn resumed_episode_keeps_the_first_run_retention_and_provider_privacy_flag() {
    let rig = Rig::new();
    let lease = rig.service.lease(&rig.key).unwrap();
    let (writer, lease) = broca_core::WalHandle::open_and_spawn_observed_with_retention(
        rig.service.wal.clone(),
        rig.key.clone(),
        lease,
        None,
        Some(RETAINED_SECONDS),
    )
    .await
    .unwrap();
    let mut req = request(&rig.key, "paused");
    let (out, _events) = tokio::sync::mpsc::channel(128);
    let error =
        serde_json::from_value(serde_json::json!({"class":"auth", "message":"reauthenticate"}))
            .unwrap();
    let control = broca_core::RunControl {
        clock: Some(rig.service.clock.clone()),
        pause_for_auth_at_start: Some(error),
        ..Default::default()
    };
    broca_core::run_loop_on_writer(
        req.clone(),
        &writer,
        broca_core::StartMode::FreshOrResume,
        &broca_core::StubProvider::new(vec![]),
        &broca_core::StubToolPlane::new(),
        &broca_core::AllowAll,
        rig.service.wal.as_ref(),
        Some(rig.store.clone()),
        control,
        &out,
    )
    .await
    .unwrap();
    req.retention = Some(1000);
    let provider = broca_core::StubProvider::new(vec![Vec::new()]);
    let mismatch = broca_core::run_loop_on_writer(
        req.clone(),
        &writer,
        broca_core::StartMode::ResumeAfterAuth,
        &provider,
        &broca_core::StubToolPlane::new(),
        &broca_core::AllowAll,
        rig.service.wal.as_ref(),
        Some(rig.store.clone()),
        broca_core::RunControl::default(),
        &out,
    )
    .await;
    assert!(matches!(mismatch, Err(broca_core::RunError::Config(_))));
    assert!(provider.calls().is_empty());
    req.retention = None;
    broca_core::run_loop_on_writer(
        req,
        &writer,
        broca_core::StartMode::ResumeAfterAuth,
        &provider,
        &broca_core::StubToolPlane::new(),
        &broca_core::AllowAll,
        rig.service.wal.as_ref(),
        Some(rig.store.clone()),
        broca_core::RunControl {
            clock: Some(rig.service.clock.clone()),
            ..Default::default()
        },
        &out,
    )
    .await
    .unwrap();
    let records = rig.service.wal.read_records(&rig.key).unwrap();
    let starts: Vec<_> = records
        .iter()
        .filter_map(|frame| match &frame.record {
            WalRecord::RunStarted { retention, .. } => Some(*retention),
            _ => None,
        })
        .collect();
    assert_eq!(starts, [Some(RETAINED_SECONDS)]);
    assert_eq!(writer.is_retention_lineage(), Some(true));
    assert_eq!(provider.calls().len(), 1);
    assert!(provider.calls()[0].ephemeral_session);
    assert!(rig
        .store
        .read_admission_snapshot(&rig.key)
        .unwrap()
        .is_none());
    let _ = writer.shutdown().await;
    drop(lease);
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn injected_clock_stamps_start_pause_and_terminal_without_advancing_maintenance() {
    let rig = Rig::new();
    fs::write(rig.root.join("test-clock"), br#"{"offset_ms":5000000}"#).unwrap();
    rig.service.clock.refresh().unwrap();
    let registry = rig.registry(None);
    let before = rig.service.clock.now_ms();
    registry
        .route_send(
            &rig.key,
            request(&rig.key, "clocked"),
            crate::executor::RunKind::FreshOrResume,
        )
        .await
        .unwrap();
    wait_idle(&registry, &rig.key).await;
    let after = rig.service.clock.now_ms();
    let records = rig.service.wal.read_records(&rig.key).unwrap();
    let mut stamped = 0;
    for frame in records {
        match frame.record {
            WalRecord::RunStarted { ts_ms, .. } | WalRecord::RunFinished { ts_ms, .. } => {
                stamped += 1;
                assert!(
                    (before..=after).contains(&ts_ms.unwrap()),
                    "activity uses the injected domain"
                );
            }
            _ => {}
        }
    }
    // An empty read would pass the loop above without checking anything.
    assert_eq!(stamped, 2, "the run's start and finish were both checked");
    assert!(registry.stop_and_seal().await);
}

struct HeldExport {
    store: Arc<SqliteStore>,
    entered: tokio::sync::Notify,
    released: (Mutex<bool>, std::sync::Condvar),
}
impl ExportIndex for HeldExport {
    fn missing_terminal_runs(
        &self,
    ) -> Result<Vec<broca_core::MissingExportRun>, broca_core::ExportError> {
        self.store.missing_terminal_runs()
    }
    fn emit_run(
        &self,
        records: &[FramedRecord],
        run: &RunId,
        resolver: &dyn broca_core::ChargeBasisResolver,
    ) -> Result<(), broca_core::ExportError> {
        self.entered.notify_one();
        let (state, condition) = &self.released;
        let mut released = state.lock().unwrap();
        while !*released {
            released = condition.wait(released).unwrap();
        }
        self.store.emit_run(records, run, resolver)
    }
    fn read_page(
        &self,
        since: Option<broca_core::ExportCursor>,
        limit: u64,
    ) -> Result<broca_core::ExportPage, broca_core::ExportError> {
        ExportIndex::read_page(self.store.as_ref(), since, limit)
    }
    fn high_water(&self) -> u64 {
        self.store.high_water()
    }
    fn source_epoch(&self) -> u64 {
        self.store.source_epoch()
    }
    fn source_instance_id(&self) -> &str {
        self.store.source_instance_id()
    }
}

#[tokio::test]
async fn send_waits_for_in_progress_deletion_to_complete_before_opening_fresh_lineage() {
    let mut rig = Rig::new();
    let old = rig.seed(1, 100, Some(200));
    rig.service.rederive(&rig.key).unwrap();
    let export = Arc::new(HeldExport {
        store: rig.store.clone(),
        entered: Default::default(),
        released: (Mutex::new(false), std::sync::Condvar::new()),
    });
    rig.service = Arc::new(
        RetentionService::new(
            rig.service.wal.clone(),
            rig.store.clone(),
            rig.leases.clone(),
            export.clone(),
            Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1)),
        )
        .unwrap(),
    );
    let registry = rig.registry(None);
    let sweep = tokio::spawn({
        let registry = registry.clone();
        async move { registry.sweep_once(1200).await }
    });
    broca_test_support::event_timeout(
        "send_waits_for_in_progress_deletion_to_complete_before_opening_fresh_lineage",
        export.entered.notified(),
        || {
            broca_test_support::record_kinds(
                rig.service
                    .wal
                    .read_records(&rig.key)
                    .unwrap()
                    .iter()
                    .map(|f| &f.record),
            )
        },
    )
    .await
    .unwrap();
    let mut send = tokio::spawn({
        let registry = registry.clone();
        let key = rig.key.clone();
        async move {
            registry
                .route_send(
                    &key,
                    request(&key, "new"),
                    crate::executor::RunKind::FreshOrResume,
                )
                .await
        }
    });
    let premature = tokio::time::timeout(std::time::Duration::from_millis(100), &mut send).await;
    *export.released.0.lock().unwrap() = true;
    export.released.1.notify_all();
    assert!(
        premature.is_err(),
        "a racing send waits, rather than refusing or writing into the expired WAL"
    );
    sweep.await.unwrap().unwrap();
    broca_test_support::event_timeout(
        "send_waits_for_in_progress_deletion_to_complete_before_opening_fresh_lineage",
        send,
        || {
            broca_test_support::record_kinds(
                rig.service
                    .wal
                    .read_records(&rig.key)
                    .unwrap()
                    .iter()
                    .map(|f| &f.record),
            )
        },
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    wait_idle(&registry, &rig.key).await;
    let stone = rig
        .service
        .tombstones(&rig.key)
        .unwrap()
        .into_iter()
        .find(|stone| stone.lineage_id == old)
        .unwrap();
    assert!(stone.completed_at_ms.is_some());
    assert_ne!(
        rig.store
            .lookup_run_retention("new")
            .unwrap()
            .unwrap()
            .lineage_id,
        Some(old)
    );
    assert!(registry.stop_and_seal().await);
}

/// The opener waits on a held lease only while `expired` reports expiry. A deletion that
/// writes its tombstone and removes the WAL while `expired` is between reading the tombstones
/// and finding the WAL gone must still be reported, or a racing send refuses with LeaseHeld.
#[test]
fn expiry_check_overlapping_a_completing_deletion_still_reports_expiry() {
    type Deletion = Box<dyn FnOnce() + Send>;
    struct DeleteOnLiveLookup {
        store: Arc<SqliteStore>,
        during: Mutex<Option<Deletion>>,
    }
    impl RetentionIndex for DeleteOnLiveLookup {
        fn live(&self, key: &BindIdentity) -> Result<Option<RetentionLineageRow>, String> {
            let deletion = self.during.lock().unwrap().take();
            if let Some(deletion) = deletion {
                deletion();
            }
            RetentionIndex::live(self.store.as_ref(), key)
        }
        fn lineage(&self, id: &str) -> Result<Option<RetentionLineageRow>, String> {
            RetentionIndex::lineage(self.store.as_ref(), id)
        }
        fn runs(&self, id: &str) -> Result<Vec<RetentionRunRow>, String> {
            RetentionIndex::runs(self.store.as_ref(), id)
        }
        fn candidates(&self, now: u64) -> Result<Vec<RetentionLineageRow>, String> {
            RetentionIndex::candidates(self.store.as_ref(), now)
        }
        fn run(&self, id: &str) -> Result<Option<RunRetention>, String> {
            RetentionIndex::run(self.store.as_ref(), id)
        }
        fn upsert(
            &self,
            row: &RetentionLineageRow,
            runs: &[RetentionRunRow],
        ) -> Result<(), String> {
            RetentionIndex::upsert(self.store.as_ref(), row, runs)
        }
    }
    let rig = Rig::new();
    let old = rig.seed(1, 100, Some(200));
    rig.service.rederive(&rig.key).unwrap();
    let index = Arc::new(DeleteOnLiveLookup {
        store: rig.store.clone(),
        during: Mutex::new(None),
    });
    let checker = RetentionService::new(
        rig.service.wal.clone(),
        index.clone(),
        rig.leases.clone(),
        rig.store.clone(),
        Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1)),
    )
    .unwrap();
    // The checker has already resolved the live WAL and read the (still empty) tombstone
    // directory when it consults the index; the whole deletion runs at that point.
    *index.during.lock().unwrap() = Some(Box::new({
        let deleter = rig.service.clone();
        let key = rig.key.clone();
        let old = old.clone();
        move || {
            let _lease = deleter.lease(&key).unwrap();
            assert_eq!(deleter.delete_expired(&key, 1200).unwrap(), Some(old));
        }
    }));
    let verdict = checker.expired(&rig.key, None).unwrap();
    assert!(
        index.during.lock().unwrap().is_none(),
        "the deletion ran inside the expiry check"
    );
    assert!(!rig.service.wal.ephemeral_wal_path(&rig.key).exists());
    let stone = rig
        .service
        .tombstones(&rig.key)
        .unwrap()
        .into_iter()
        .find(|stone| stone.lineage_id == old)
        .unwrap();
    assert!(stone.completed_at_ms.is_some());
    assert_eq!(verdict, Some(stone.expired_at_ms));
}

struct QuotaRefusalExecutor {
    config: broca_wal::RunConfig,
    observed: tokio::sync::mpsc::Sender<broca_core::keep_warm_wait::WarmTick>,
}
#[async_trait::async_trait]
impl crate::executor::RunExecutor for QuotaRefusalExecutor {
    async fn warm_quota(
        &self,
        _: &broca_wal::RunConfig,
        _: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        Err("WARM-PRIVATE-MARKER".into())
    }
    async fn execute(
        &self,
        job: crate::executor::RunJob,
        _: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
    ) -> Result<broca_core::RunSummary, broca_core::RunError> {
        let tick = job
            .control
            .keep_warm
            .as_ref()
            .unwrap()
            .warm(&self.config, &RunId("r1".into()))
            .await;
        self.observed.send(tick).await.unwrap();
        job.writer.record(WalRecord::RunPaused {
            run_id: RunId("r1".into()),
            reason: broca_protocol::PauseReason::AuthRequired,
            error: None,
            ts_ms: Some(200),
        });
        job.writer.sync().await?;
        if let Some(reply) = job.on_admitted {
            let _ = reply.send(Ok(RunId("r1".into())));
        }
        Ok(broca_core::RunSummary {
            reason: RunFinishReason::Interrupted,
            paused: Some(broca_protocol::PauseReason::AuthRequired),
            steps: 0,
            usage: Default::default(),
            retries_used: 0,
            final_step_finish_reason: None,
        })
    }
}
struct WarmDeclaration;
impl crate::cache::CacheDeclarer for WarmDeclaration {
    fn declare(&self, _: &broca_wal::RunConfig) -> Option<crate::cache::CacheDeclaration> {
        Some(crate::cache::CacheDeclaration {
            ttl_ms: Some(600_000),
            ttl_options_ms: vec![],
            read_refreshes_ttl: Some(true),
            warm: Ok(()),
            basis: None,
        })
    }
}

#[tokio::test]
async fn resumed_retention_actor_redacts_warm_detail_before_the_run_seeds_its_writer() {
    let rig = Rig::new();
    let mut start = started(&rig.key, "r1", Some(1), 100);
    let config = if let WalRecord::RunStarted {
        keep_warm, config, ..
    } = &mut start
    {
        *keep_warm = Some(broca_protocol::request::KeepWarm::Auto);
        *config.clone()
    } else {
        unreachable!()
    };
    let mut wal = rig
        .service
        .wal
        .open_with_retention(&rig.key, Some(1))
        .unwrap();
    wal.append(1, &start, true).unwrap();
    drop(wal);
    let (tx, mut observed) = tokio::sync::mpsc::channel(1);
    let executor = Arc::new(QuotaRefusalExecutor {
        config,
        observed: tx,
    });
    let opener = Arc::new(
        crate::opener::DefaultOpener::new(rig.leases.clone(), rig.service.wal.clone(), executor)
            .with_run_index(rig.store.clone()),
    );
    let registry = crate::registry::LineageRegistry::new_with_reader(
        opener,
        rig.service.wal.clone(),
        Some(rig.store.clone()),
    );
    registry.set_retention_service(rig.service.clone());
    registry.set_cache_declarer(Arc::new(WarmDeclaration));
    registry.actor_for(&rig.key).await.unwrap();
    let tick = broca_test_support::event_timeout(
        "resumed_retention_actor_redacts_warm_detail_before_the_run_seeds_its_writer",
        observed.recv(),
        || {
            broca_test_support::record_kinds(
                rig.service
                    .wal
                    .read_records(&rig.key)
                    .unwrap()
                    .iter()
                    .map(|f| &f.record),
            )
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        matches!(tick, broca_core::keep_warm_wait::WarmTick::Stop { detail: Some(detail), .. } if detail == "quota")
    );
    assert!(registry.stop_and_seal().await);
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn sweep_retries_sidecarless_boot_enrollment_after_lease_release_within_bound() {
    let rig = Rig::new();
    let before = rig.service.clock.now_ms();
    let id = rig.seed(1, before, Some(before));
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    fs::remove_file(path.with_extension("session")).unwrap();
    fs::write(rig.root.join("test-clock"), br#"{"offset_ms":1000}"#).unwrap();
    rig.service.clock.refresh().unwrap();
    let deadline = before + 1000;
    let registry = rig.registry(None);
    let lease = rig.service.lease(&rig.key).unwrap();
    let wal = rig
        .service
        .wal
        .open_with_retention(&rig.key, Some(1))
        .unwrap();
    assert_eq!(registry.boot_retention().await.unwrap(), 0);
    assert_eq!(registry.sweep_once(deadline).await.unwrap(), 0);
    assert!(path.exists());
    assert!(!path.with_extension("session").exists());
    assert!(rig
        .store
        .lookup_live_retention_lineage(&rig.key)
        .unwrap()
        .is_none());
    assert!(rig.store.lookup_run("r1").unwrap().is_none());
    assert!(rig.service.tombstones(&rig.key).unwrap().is_empty());

    drop(wal);
    drop(lease);
    fs::write(
        rig.root.join("test-clock"),
        serde_json::to_vec(&serde_json::json!({"offset_ms": 1000 + SWEEP_INTERVAL_MS})).unwrap(),
    )
    .unwrap();
    rig.service.clock.refresh().unwrap();
    let bound = deadline + SWEEP_INTERVAL_MS;
    assert_eq!(registry.sweep_once(bound).await.unwrap(), 1);
    assert!(!path.exists());
    let stone = rig.service.tombstones(&rig.key).unwrap().pop().unwrap();
    assert_eq!(stone.lineage_id, id);
    assert_eq!(stone.expired_at_ms, deadline);
    assert_eq!(stone.completed_at_ms, Some(bound));
    assert_eq!(
        rig.store.lookup_run("r1").unwrap().unwrap().state,
        RunIndexState::Completed
    );
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn sweep_retries_deferred_enrollment_without_touching_a_fresh_lineage_at_the_same_address() {
    let rig = Rig::new();
    let before = rig.service.clock.now_ms();
    let old_id = rig.seed(1, before, Some(before));
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    fs::remove_file(path.with_extension("session")).unwrap();
    fs::write(rig.root.join("test-clock"), br#"{"offset_ms":1000}"#).unwrap();
    rig.service.clock.refresh().unwrap();
    let deadline = before + 1000;
    let registry = rig.registry(None);
    let lease = rig.service.lease(&rig.key).unwrap();
    assert_eq!(registry.boot_retention().await.unwrap(), 0);

    assert_eq!(
        rig.service.delete_expired(&rig.key, deadline).unwrap(),
        Some(old_id.clone())
    );
    // Recovery may revisit an old tombstone after the address already has a new WAL.
    let mut stone = rig.service.tombstones(&rig.key).unwrap().pop().unwrap();
    stone.completed_at_ms = None;
    rig.service.write_tombstone(&rig.key, &stone).unwrap();
    let mut fresh = rig
        .service
        .wal
        .open_with_retention(&rig.key, Some(3600))
        .unwrap();
    fresh
        .append(1, &started(&rig.key, "fresh", Some(3600), deadline), true)
        .unwrap();
    fresh.append(1, &finished(deadline), true).unwrap();
    let fresh_id = fresh.lineage_id().unwrap().to_string();
    assert_ne!(fresh_id, old_id);
    drop(fresh);
    rig.service.rederive(&rig.key).unwrap();
    let fresh_bytes = fs::read(&path).unwrap();
    assert_eq!(registry.sweep_once(deadline).await.unwrap(), 0);
    assert_eq!(fs::read(&path).unwrap(), fresh_bytes);

    drop(lease);
    fs::write(
        rig.root.join("test-clock"),
        serde_json::to_vec(&serde_json::json!({"offset_ms": 1000 + SWEEP_INTERVAL_MS})).unwrap(),
    )
    .unwrap();
    rig.service.clock.refresh().unwrap();
    assert_eq!(
        registry
            .sweep_once(deadline + SWEEP_INTERVAL_MS)
            .await
            .unwrap(),
        1
    );
    assert_eq!(fs::read(&path).unwrap(), fresh_bytes);
    assert!(!rig.service.wal.tombstone_path(&rig.key, &fresh_id).exists());
    assert_eq!(rig.service.expired(&rig.key, None).unwrap(), None);
    let row = rig
        .store
        .lookup_live_retention_lineage(&rig.key)
        .unwrap()
        .unwrap();
    assert_eq!(row.lineage_id, fresh_id);
    assert_eq!(row.expired_at_ms, None);
    assert_eq!(
        rig.store.lookup_run("fresh").unwrap().unwrap().state,
        RunIndexState::Completed
    );
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn sweep_refreshes_the_clock_before_nominating_expired_wals() {
    let rig = Rig::new();
    let before = rig.service.clock.now_ms();
    rig.seed(1, before, Some(before));
    rig.service.rederive(&rig.key).unwrap();
    let registry = rig.registry(None);
    fs::write(rig.root.join("test-clock"), br#"{"offset_ms":2000}"#).unwrap();
    assert_eq!(registry.sweep_once(before).await.unwrap(), 1);
    assert!(!rig.service.wal.ephemeral_wal_path(&rig.key).exists());
    assert_eq!(
        rig.store
            .lookup_run_retention("r1")
            .unwrap()
            .unwrap()
            .expired_at_ms,
        Some(before + 1000)
    );
}

#[test]
fn retention_byte_progress_uses_the_durable_prefix_not_a_concurrent_tail() {
    let rig = Rig::new();
    rig.seed(1, 100, Some(200));
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    let durable_bytes = fs::metadata(&path).unwrap().len();
    let mut wal = rig.service.wal.open(&rig.key).unwrap();
    wal.append(1, &started(&rig.key, "r2", None, 300), true)
        .unwrap();
    wal.append(1, &finished(400), true).unwrap();
    assert!(fs::metadata(&path).unwrap().len() > durable_bytes);
    assert_eq!(durable_prefix_bytes(&path, 2).unwrap(), durable_bytes);
}

#[test]
fn a_failed_queued_admission_keeps_a_content_free_expired_run_row() {
    let rig = Rig::new();
    rig.seed(1, 100, Some(200));
    let submission = broca_protocol::SubmissionId("sub-failed".into());
    let mut queued: WalRecord = serde_json::from_str(include_str!(
        "../../broca-wal/tests/golden/pre_step_transforms/queued.json"
    ))
    .unwrap();
    if let WalRecord::Queued {
        submission_id,
        ts_ms,
        ..
    } = &mut queued
    {
        *submission_id = submission.clone();
        *ts_ms = Some(300);
    }
    let mut wal = rig.service.wal.open(&rig.key).unwrap();
    wal.append(1, &queued, true).unwrap();
    wal.append(
        1,
        &WalRecord::Retracted {
            submission_id: submission,
            failure: Some(broca_wal::QueuedAdmissionFailure {
                run_id: RunId("failed".into()),
                error: serde_json::from_value(
                    serde_json::json!({"class":"permanent", "message":MARKER}),
                )
                .unwrap(),
            }),
        },
        true,
    )
    .unwrap();
    let _lease = rig.service.lease(&rig.key).unwrap();
    rig.service.delete_expired(&rig.key, 1300).unwrap();
    let row = rig.store.lookup_run("failed").unwrap().unwrap();
    assert_eq!(row.state, RunIndexState::Error);
    assert_eq!(row.title, None);
    assert_eq!(
        rig.service.expired(&rig.key, Some("failed")).unwrap(),
        Some(1300)
    );
}

#[tokio::test]
async fn expiry_frees_cached_sender_and_ownership_for_the_next_lineage() {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    let id = rig.seed(RETAINED_SECONDS, now, Some(now));
    let registry = rig.registry(None);
    assert_eq!(registry.frozen_sender(&rig.key).await.unwrap(), Some(None));
    assert_eq!(
        registry
            .session_ownership_for_bind(&rig.key)
            .await
            .unwrap()
            .episodes,
        1
    );
    let _lease = rig.service.lease(&rig.key).unwrap();
    rig.service
        .write_tombstone(
            &rig.key,
            &Tombstone {
                next_tag: None,
                next_notice_seq: None,
                lineage_id: id,
                expired_at_ms: now + RETAINED_SECONDS * 1000,
                seq: 1,
                completed_at_ms: None,
            },
        )
        .unwrap();
    assert_eq!(registry.frozen_sender(&rig.key).await.unwrap(), None);
    assert_eq!(
        registry
            .session_ownership_for_bind(&rig.key)
            .await
            .unwrap()
            .episodes,
        0
    );
}

impl RetentionIndex for OrderedExport {
    fn live(&self, key: &BindIdentity) -> Result<Option<RetentionLineageRow>, String> {
        RetentionIndex::live(self.store.as_ref(), key)
    }
    fn lineage(&self, id: &str) -> Result<Option<RetentionLineageRow>, String> {
        RetentionIndex::lineage(self.store.as_ref(), id)
    }
    fn candidates(&self, now: u64) -> Result<Vec<RetentionLineageRow>, String> {
        RetentionIndex::candidates(self.store.as_ref(), now)
    }
    fn run(&self, id: &str) -> Result<Option<RunRetention>, String> {
        RetentionIndex::run(self.store.as_ref(), id)
    }
    fn upsert(&self, row: &RetentionLineageRow, runs: &[RetentionRunRow]) -> Result<(), String> {
        if row.expired_at_ms.is_some() {
            let stone: Tombstone = serde_json::from_slice(
                &fs::read(self.wal.tombstone_path(&self.key, &row.lineage_id))
                    .expect("tombstone precedes row update"),
            )
            .unwrap();
            assert!(
                stone.completed_at_ms.is_none(),
                "row update precedes completion"
            );
            let moved = self.wal.ephemeral_root().join(format!(
                "{}.{}.deleting",
                broca_core::session_addr(&self.key),
                row.lineage_id
            ));
            assert!(
                moved.exists(),
                "move-aside precedes row update, which precedes file deletion"
            );
        }
        RetentionIndex::upsert(self.store.as_ref(), row, runs)
    }
}

#[test]
fn enrollment_recovers_a_damaged_or_wrong_address_identity_sidecar() {
    let rig = Rig::new();
    rig.seed(1, 100, Some(200));
    let path = rig
        .service
        .wal
        .ephemeral_wal_path(&rig.key)
        .with_extension("session");
    for bytes in [
        b"{partial".to_vec(),
        serde_json::to_vec(&BindIdentity::new("/wrong", "runner", "wrong")).unwrap(),
    ] {
        std::fs::write(&path, bytes).unwrap();
        assert_eq!(rig.service.enroll().unwrap(), 1);
        let restored: BindIdentity =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(restored, rig.key);
        assert_eq!(rig.service.enroll().unwrap(), 0);
    }
}

fn seed_owed_question(rig: &Rig) -> (String, String) {
    seed_owed_question_at(rig, 10, u64::MAX, None)
}

fn seed_owed_question_at(rig: &Rig, start: u64, expiry: u64, id: Option<&str>) -> (String, String) {
    seed_owed_question_with_scope_at(
        rig,
        start,
        expiry,
        id,
        serde_json::json!({"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"head","scope_epoch":1}),
    )
}

fn seed_owed_question_with_scope_at(
    rig: &Rig,
    start: u64,
    expiry: u64,
    id: Option<&str>,
    scope: serde_json::Value,
) -> (String, String) {
    use sha2::{Digest, Sha256};
    let lineage = rig.seed(1, start, None);
    let mut wal = rig.service.wal.open(&rig.key).unwrap();
    let call = broca_protocol::ToolCall {
        thought_signature: None,
        tool_call_id: broca_protocol::ToolCallId("c".into()),
        tool_name: "edit".into(),
        input: serde_json::json!({"marker": MARKER}),
    };
    wal.append(
        1,
        &WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: broca_protocol::StepId(1),
            assistant_message: broca_protocol::AssistantMessage {
                message_id: broca_protocol::MessageId("m".into()),
                content: vec![broca_protocol::ContentBlock::ToolCall(call)],
            }
            .into(),
            usage: Default::default(),
            finish_reason: Some(broca_protocol::FinishReason::ToolCalls),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        },
        true,
    )
    .unwrap();
    wal.append(
        1,
        &WalRecord::ToolBatchStarted {
            batch_id: broca_protocol::BatchId("b".into()),
            step_id: broca_protocol::StepId(1),
            planned_calls: vec![broca_protocol::ToolCallId("c".into())],
        },
        true,
    )
    .unwrap();
    let args = serde_json::json!({"marker": MARKER}).to_string();
    let hash = format!("{:x}", Sha256::digest(args.as_bytes()));
    let seq = wal.append(1, &serde_json::from_value(serde_json::json!({"type":"tool_call_deferred","batch_id":"b","call_id":"c","executor":"broca","tool":"edit","args_jcs":args,"args_hash":hash,"scope":scope,"ask":{"prompt":MARKER,"expires_at_ms":expiry,"on_expiry":"deny","material_damage":false,"late_execution":"notify_only"}})).unwrap(), true).unwrap();
    let key = format!("{lineage}:{seq}");
    if let Some(id) = id {
        wal.append(
            1,
            &WalRecord::AwaitingApproval {
                batch_id: broca_protocol::BatchId("b".into()),
                pending: vec![broca_wal::AwaitingApprovalEntry {
                    call_key: key.clone(),
                    call_id: broca_protocol::ToolCallId("c".into()),
                    elicitation_id: Some(id.into()),
                    expires_at_ms: expiry,
                }],
            },
            true,
        )
        .unwrap();
    }
    wal.append(
        1,
        &WalRecord::ToolResult {
            finished_at_ms: None,
            tags: None,
            batch_id: broca_protocol::BatchId("b".into()),
            result: broca_core::not_dispatched_result(&broca_protocol::ToolCallId("c".into()))
                .into(),
            outcome: broca_wal::DispatchOutcome::Interrupted,
            reason: None,
            hooks: None,
            withdraw_elicitation: None,
            provider_held: None,
        },
        true,
    )
    .unwrap();
    wal.append(
        1,
        &WalRecord::ToolBatchFinished {
            batch_id: broca_protocol::BatchId("b".into()),
        },
        true,
    )
    .unwrap();
    wal.append(
        1,
        &WalRecord::TurnFinished {
            step_id: broca_protocol::StepId(1),
        },
        true,
    )
    .unwrap();
    wal.append(1, &finished(start + 10), true).unwrap();
    (lineage, key)
}

#[test]
fn retention_terminal_enrollment_derives_unprojected_withdrawal_debt() {
    let rig = Rig::new();
    seed_owed_question(&rig);
    rig.service.rederive(&rig.key).unwrap();
    assert!(
        rig.store
            .lookup_run("r1")
            .unwrap()
            .unwrap()
            .withdrawals_owed
    );
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn retention_expiry_deletes_owed_content_and_withdraws_from_identity_only_record() {
    use broca_store::retention_withdrawals::RetentionQuestionId;
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    let (lineage, call_key) = seed_owed_question_at(&rig, now, now + 60_000, Some("question"));
    rig.service.rederive(&rig.key).unwrap();
    let deadline = now + 1010;
    fs::write(rig.root.join("test-clock"), br#"{"offset_ms":2000}"#).unwrap();
    rig.service.clock.refresh().unwrap();
    assert_eq!(rig.service.expired(&rig.key, None).unwrap(), Some(deadline));
    let registry = rig.registry(None);
    assert!(matches!(
        registry.route_read(&rig.key, None, None).await,
        Err(crate::registry::OpenError::Expired { .. })
    ));
    assert!(matches!(
        registry.route_run_state(&rig.key, RunId("r1".into())).await,
        Err(crate::registry::OpenError::Expired { .. })
    ));
    assert_eq!(
        registry
            .sweep_once(rig.service.clock.now_ms())
            .await
            .unwrap(),
        1
    );
    assert!(rig.service.clock.now_ms() - deadline < 3_600_000);
    assert!(rig
        .service
        .wal
        .resolve_live_path(&rig.key)
        .unwrap()
        .is_none());
    assert!(!rig.service.deleting(&rig.key, &lineage).exists());
    assert!(!rig
        .service
        .wal
        .ephemeral_wal_path(&rig.key)
        .with_extension("session")
        .exists());
    let rows = rig.store.pending_retention_withdrawals().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].call_key, call_key);
    assert_eq!(rows[0].provider_id, "cingulate");
    assert!(!format!("{rows:?}").contains(MARKER));
    assert_eq!(
        rows[0].question,
        RetentionQuestionId::Known("question".into())
    );
    let questions = Arc::new(RetainedQuestions::default());
    rig.service.set_withdrawal_adapter(Arc::new(
        crate::retention_withdrawals::ScopedRetentionWithdrawalAdapter(questions.clone()),
    ));
    assert!(rig.service.withdraw_retained_questions().await.unwrap());
    {
        let sent = questions.withdrawals.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0.bind, rig.key);
        assert_eq!(
            sent[0].0.scope.as_ref(),
            Some(&rows[0].scope.as_ref().unwrap().to_scope())
        );
        assert_eq!(sent[0].1, "question");
        assert!(!format!("{sent:?}").contains(MARKER));
    }
    assert!(rig
        .store
        .pending_retention_withdrawals()
        .unwrap()
        .is_empty());
    assert_no_marker(&rig.root);
    let mut fresh = request(&rig.key, "r2");
    fresh.retention = None;
    registry
        .route_send(&rig.key, fresh, crate::executor::RunKind::FreshOrResume)
        .await
        .unwrap();
    wait_idle(&registry, &rig.key).await;
    let fresh_id = rig
        .service
        .wal
        .open(&rig.key)
        .unwrap()
        .lineage_id()
        .unwrap()
        .to_string();
    assert_ne!(fresh_id, lineage, "expired lineage reopened for a new send");
    assert!(registry.stop_and_seal().await);
}

#[test]
fn interrupted_retention_deletion_exports_owed_identity_without_restoring_wal() {
    let rig = Rig::new();
    let (lineage, _) = seed_owed_question(&rig);
    let _lease = rig.service.lease(&rig.key).unwrap();
    rig.service.rederive(&rig.key).unwrap();
    let stone = Tombstone {
        next_tag: None,
        next_notice_seq: None,
        lineage_id: lineage.clone(),
        expired_at_ms: 1020,
        seq: 1,
        completed_at_ms: None,
    };
    rig.service.write_tombstone(&rig.key, &stone).unwrap();
    let path = rig
        .service
        .wal
        .resolve_live_path(&rig.key)
        .unwrap()
        .unwrap();
    fs::rename(path, rig.service.deleting(&rig.key, &lineage)).unwrap();
    drop(_lease);
    rig.service.resume_deletions(2000).unwrap();
    assert!(
        rig.service
            .wal
            .resolve_live_path(&rig.key)
            .unwrap()
            .is_none(),
        "owed content was restored after expiry"
    );
    assert!(
        !rig.store
            .lookup_run("r1")
            .unwrap()
            .unwrap()
            .withdrawals_owed
    );
    assert_eq!(rig.store.pending_retention_withdrawals().unwrap().len(), 1);
    assert!(rig.service.tombstones(&rig.key).unwrap()[0]
        .completed_at_ms
        .is_some());
    // A second crash cut loses only tombstone completion, after the WAL and sidecar
    // are gone. Recovery discovers the address from the committed retention row.
    rig.service.write_tombstone(&rig.key, &stone).unwrap();
    assert_eq!(
        rig.service.resume_deletions(2001).unwrap(),
        vec![rig.key.clone()]
    );
    assert!(rig.service.tombstones(&rig.key).unwrap()[0]
        .completed_at_ms
        .is_some());
    assert_eq!(rig.store.pending_retention_withdrawals().unwrap().len(), 1);
}

#[cfg(feature = "test-clock")]
fn assert_no_marker(path: &Path) {
    if path.is_dir() {
        for entry in fs::read_dir(path).unwrap() {
            assert_no_marker(&entry.unwrap().path());
        }
    } else if let Ok(bytes) = fs::read(path) {
        assert!(
            !bytes
                .windows(MARKER.len())
                .any(|window| window == MARKER.as_bytes()),
            "expired content marker in {}",
            path.display()
        );
    }
}

#[derive(Default)]
struct RetainedQuestions {
    withdrawals: Mutex<Vec<(broca_core::BatchCtx, String)>>,
    replies: Mutex<std::collections::VecDeque<broca_core::tool_plane::ApprovalWithdrawal>>,
}
#[async_trait::async_trait]
impl broca_core::tool_plane::ElicitationPlane for RetainedQuestions {
    async fn request(
        &self,
        _: &broca_core::BatchCtx,
        _: &str,
        _: &WalRecord,
        _: u64,
    ) -> Result<broca_core::tool_plane::ApprovalFiling, broca_core::tool_plane::ApprovalRouteError>
    {
        panic!("expired session must never re-file a question");
    }
    async fn answers(
        &self,
        _: &broca_core::BatchCtx,
        _: Option<&broca_core::tool_plane::ApprovalCursor>,
    ) -> Result<broca_core::tool_plane::ApprovalPage, broca_core::tool_plane::ApprovalRouteError>
    {
        panic!("content-free worker only withdraws");
    }
    async fn withdraw(
        &self,
        ctx: &broca_core::BatchCtx,
        id: &str,
    ) -> broca_core::tool_plane::ApprovalWithdrawal {
        self.withdrawals
            .lock()
            .unwrap()
            .push((ctx.clone(), id.into()));
        self.replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(broca_core::tool_plane::ApprovalWithdrawal::Acknowledged)
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn content_free_unknown_id_is_not_refiled_and_is_dropped_at_ask_expiry() {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    let expires_at_ms = now + 3000;
    seed_owed_question_at(&rig, now, expires_at_ms, None);
    let _lease = rig.service.lease(&rig.key).unwrap();
    rig.service.delete_expired(&rig.key, now + 2000).unwrap();
    let questions = Arc::new(RetainedQuestions::default());
    rig.service.set_withdrawal_adapter(Arc::new(
        crate::retention_withdrawals::ScopedRetentionWithdrawalAdapter(questions.clone()),
    ));
    fs::write(
        rig.root.join("test-clock"),
        serde_json::json!({"now_ms": expires_at_ms - 1}).to_string(),
    )
    .unwrap();
    rig.service.clock.refresh().unwrap();
    assert!(!rig.service.withdraw_retained_questions().await.unwrap());
    let rows = rig.store.pending_retention_withdrawals().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].provider_id, "cingulate");
    assert_eq!(rows[0].expires_at_ms, expires_at_ms);
    assert!(!format!("{rows:?}").contains(MARKER));
    assert!(matches!(
        rows[0].question,
        broca_store::retention_withdrawals::RetentionQuestionId::Unknown
    ));
    assert!(questions.withdrawals.lock().unwrap().is_empty());
    fs::write(
        rig.root.join("test-clock"),
        serde_json::json!({"now_ms": expires_at_ms}).to_string(),
    )
    .unwrap();
    rig.service.clock.refresh().unwrap();
    assert_eq!(rig.service.clock.now_ms(), expires_at_ms);
    assert!(rig.service.withdraw_retained_questions().await.unwrap());
    assert!(rig
        .store
        .pending_retention_withdrawals()
        .unwrap()
        .is_empty());
    assert!(questions.withdrawals.lock().unwrap().is_empty());
    assert_no_marker(&rig.root);
}

#[tokio::test]
async fn content_free_withdrawal_retries_routes_but_settles_ended_scopes_and_unknown_questions() {
    use broca_core::tool_plane::{ApprovalRouteError, ApprovalWithdrawal};
    for code in ["scope_ended", "session_ended", "elicitation_not_found"] {
        let rig = Rig::new();
        let now = rig.service.clock.now_ms();
        seed_owed_question_at(&rig, now, now + 60_000, Some("question"));
        let _lease = rig.service.lease(&rig.key).unwrap();
        rig.service.delete_expired(&rig.key, now + 2000).unwrap();
        let questions = Arc::new(RetainedQuestions::default());
        questions.replies.lock().unwrap().extend([
            ApprovalWithdrawal::Retry(ApprovalRouteError {
                code: "elicitation_unavailable".into(),
                retryable: true,
            }),
            ApprovalWithdrawal::NotSent {
                reason: code.into(),
            },
        ]);
        rig.service.set_withdrawal_adapter(Arc::new(
            crate::retention_withdrawals::ScopedRetentionWithdrawalAdapter(questions.clone()),
        ));
        assert!(!rig.service.withdraw_retained_questions().await.unwrap());
        assert!(
            rig.service.withdraw_retained_questions().await.unwrap(),
            "terminal code {code} retried"
        );
        assert!(rig.service.withdraw_retained_questions().await.unwrap());
        assert_eq!(questions.withdrawals.lock().unwrap().len(), 2);
    }
}

fn retained_rig_with_replies(
    replies: impl IntoIterator<Item = broca_core::tool_plane::ApprovalWithdrawal>,
) -> (Rig, Arc<RetainedQuestions>) {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    // The question never expires, so only the worker's own retry window can end retries.
    seed_owed_question_at(&rig, now, u64::MAX, Some("question"));
    {
        let _lease = rig.service.lease(&rig.key).unwrap();
        rig.service.delete_expired(&rig.key, now + 2000).unwrap();
    }
    assert_eq!(rig.store.pending_retention_withdrawals().unwrap().len(), 1);
    let questions = Arc::new(RetainedQuestions::default());
    questions.replies.lock().unwrap().extend(replies);
    rig.service.set_withdrawal_adapter(Arc::new(
        crate::retention_withdrawals::ScopedRetentionWithdrawalAdapter(questions.clone()),
    ));
    (rig, questions)
}

#[tokio::test]
async fn content_free_structured_route_refusal_settles_on_the_first_attempt() {
    use broca_core::tool_plane::{ApprovalRouteError, ApprovalWithdrawal};
    for reply in [
        ApprovalWithdrawal::Retry(ApprovalRouteError {
            code: "module_rejected".into(),
            retryable: false,
        }),
        ApprovalWithdrawal::NotSent {
            reason: "unknown_module".into(),
        },
    ] {
        let (rig, questions) = retained_rig_with_replies([reply]);
        let (result, logs) =
            broca_test_support::capture_logs(rig.service.withdraw_retained_questions()).await;
        assert!(result.unwrap());
        assert_eq!(questions.withdrawals.lock().unwrap().len(), 1);
        assert!(rig
            .store
            .pending_retention_withdrawals()
            .unwrap()
            .is_empty());
        let events: Vec<_> = logs
            .iter()
            .filter(|event| event.target == "retention" && event.fields.contains_key("call_key"))
            .collect();
        assert_eq!(events.len(), 2, "{events:?}");
        assert_eq!(events[0].level, tracing::Level::WARN);
        assert!(events[0].fields.contains_key("code"));
        assert!(events[1].message().contains("settled after refusals"));
    }
}

/// The plane's retryable flag decides, as in the per-session worker: a code that would
/// settle when final is retried while the plane marks it retryable.
#[tokio::test]
async fn content_free_retryable_reply_is_retried_whatever_its_code() {
    use broca_core::tool_plane::{ApprovalRouteError, ApprovalWithdrawal};
    let (rig, questions) =
        retained_rig_with_replies([ApprovalWithdrawal::Retry(ApprovalRouteError {
            code: "elicitation_not_found".into(),
            retryable: true,
        })]);
    assert!(!rig.service.withdraw_retained_questions().await.unwrap());
    assert_eq!(rig.store.pending_retention_withdrawals().unwrap().len(), 1);
    assert!(rig.service.withdraw_retained_questions().await.unwrap());
    assert_eq!(questions.withdrawals.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn content_free_retryable_refusal_without_expiry_gives_up_at_the_retry_window() {
    use broca_core::tool_plane::{ApprovalRouteError, ApprovalWithdrawal};
    let warming = || {
        ApprovalWithdrawal::Retry(ApprovalRouteError {
            code: "module_warming".into(),
            retryable: true,
        })
    };
    let (rig, questions) = retained_rig_with_replies([warming(), warming(), warming()]);
    let question = rig
        .store
        .pending_retention_withdrawals()
        .unwrap()
        .pop()
        .unwrap();
    tokio::time::pause();
    let window = crate::approval::WITHDRAWAL_RETRY_WINDOW;
    let mut refusals = crate::approval::RefusalWindows::default();
    let ((), logs) = broca_test_support::capture_logs(async {
        let service = &rig.service;
        assert!(!service
            .withdraw_retained_questions_with(&mut refusals)
            .await
            .unwrap());
        tokio::time::advance(window - std::time::Duration::from_secs(1)).await;
        assert!(!service
            .withdraw_retained_questions_with(&mut refusals)
            .await
            .unwrap());
        assert_eq!(rig.store.pending_retention_withdrawals().unwrap().len(), 1);
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        assert!(service
            .withdraw_retained_questions_with(&mut refusals)
            .await
            .unwrap());
    })
    .await;
    assert!(rig
        .store
        .pending_retention_withdrawals()
        .unwrap()
        .is_empty());
    let reopened = SqliteStore::open(rig.root.join("run-index.db")).unwrap();
    assert_eq!(
        reopened
            .retention_withdrawal_stop_reason(&question.lineage_id, &question.call_key)
            .unwrap()
            .as_deref(),
        Some(crate::approval::WITHDRAWAL_RETRY_EXHAUSTED),
        "giving up must leave a durable reason after the pending row is removed"
    );
    assert_eq!(
        questions.withdrawals.lock().unwrap().len(),
        2,
        "the pass after the window gives up without another attempt"
    );
    let events: Vec<_> = logs
        .iter()
        .filter(|event| event.target == "retention" && event.fields.contains_key("call_key"))
        .collect();
    assert_eq!(events.len(), 2, "{events:?}");
    assert!(events[0].message().contains("refused"), "{events:?}");
    assert_eq!(events[0].fields["code"], "module_warming");
    assert!(events[1].message().contains("given up"), "{events:?}");
    assert_eq!(
        events[1].fields["reason"],
        crate::approval::WITHDRAWAL_RETRY_EXHAUSTED
    );
}

/// A retention index whose content-free withdrawal table can never be read.
struct WithdrawalStoreDown {
    store: Arc<SqliteStore>,
    reads: std::sync::atomic::AtomicUsize,
}
impl RetentionIndex for WithdrawalStoreDown {
    fn live(&self, session: &BindIdentity) -> Result<Option<RetentionLineageRow>, String> {
        RetentionIndex::live(self.store.as_ref(), session)
    }
    fn lineage(&self, id: &str) -> Result<Option<RetentionLineageRow>, String> {
        RetentionIndex::lineage(self.store.as_ref(), id)
    }
    fn candidates(&self, now_ms: u64) -> Result<Vec<RetentionLineageRow>, String> {
        RetentionIndex::candidates(self.store.as_ref(), now_ms)
    }
    fn run(&self, id: &str) -> Result<Option<RunRetention>, String> {
        RetentionIndex::run(self.store.as_ref(), id)
    }
    fn upsert(&self, row: &RetentionLineageRow, runs: &[RetentionRunRow]) -> Result<(), String> {
        RetentionIndex::upsert(self.store.as_ref(), row, runs)
    }
    fn pending_withdrawals(
        &self,
    ) -> Result<Vec<broca_store::retention_withdrawals::RetentionWithdrawal>, String> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Err("injected withdrawal table failure".into())
    }
}

/// A store failure that outlasts the retry window stops the content-free worker with one
/// ERROR, after one WARN at the first failure. The worker parks rather than returning,
/// because the retention task ends when the worker does.
#[tokio::test(start_paused = true)]
async fn content_free_store_failure_stops_the_worker_with_one_warning_and_one_error() {
    let (rig, _) = retained_rig_with_replies([]);
    let index = Arc::new(WithdrawalStoreDown {
        store: rig.store.clone(),
        reads: std::sync::atomic::AtomicUsize::new(0),
    });
    let resolver = Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1));
    let service = Arc::new(
        RetentionService::new(
            Arc::new(FileWalStore::new(rig.root.join("store-down-wal"))),
            index.clone(),
            rig.leases.clone(),
            rig.store.clone(),
            resolver,
        )
        .unwrap(),
    );
    let capture = broca_test_support::LogCapture::new();
    let worker = tokio::spawn(capture.scope_future({
        let service = service.clone();
        async move { service.run_withdrawal_worker().await }
    }));
    let window = crate::approval::WITHDRAWAL_RETRY_WINDOW;
    tokio::time::sleep(window + std::time::Duration::from_secs(10)).await;
    let reads = index.reads.load(Ordering::SeqCst);
    assert!(reads > 1, "the worker never retried");
    tokio::time::sleep(window).await;
    assert_eq!(
        index.reads.load(Ordering::SeqCst),
        reads,
        "the worker kept reading after it gave up"
    );
    assert!(
        !worker.is_finished(),
        "the worker returned and would end retention"
    );
    worker.abort();
    let _ = worker.await;
    let levels: Vec<_> = capture
        .events()
        .iter()
        .filter(|event| event.target == "retention")
        .map(|event| event.level)
        .collect();
    assert_eq!(levels, [tracing::Level::WARN, tracing::Level::ERROR]);
    let reopened = SqliteStore::open(rig.root.join("run-index.db")).unwrap();
    assert_eq!(
        reopened.pending_retention_withdrawals().unwrap().len(),
        1,
        "storage failure must not settle durable debt"
    );
}

#[cfg(feature = "test-clock")]
#[tokio::test(start_paused = true)]
async fn actor_expiry_hands_owed_withdrawals_to_content_free_worker_before_releasing_lease() {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    // WAL setup can outlast the one-second retention policy on slow filesystems. Keep the
    // seed unexpired until its actor owns it, rather than letting admission delete it first.
    fs::write(
        rig.root.join("test-clock"),
        serde_json::json!({"now_ms": now}).to_string(),
    )
    .unwrap();
    rig.service.clock.refresh().unwrap();
    seed_owed_question_at(&rig, now, now + 60_000, Some("question"));
    rig.service.rederive(&rig.key).unwrap();
    let registry = rig.registry(None);
    registry.actor_for(&rig.key).await.unwrap();
    assert_eq!(registry.slot_counts().open, 1);
    fs::write(
        rig.root.join("test-clock"),
        serde_json::json!({"now_ms": now + 2000}).to_string(),
    )
    .unwrap();
    rig.service.clock.refresh().unwrap();
    // The test clock wakes expiry independently of Tokio time; wait for the registry's close
    // notification instead of guessing how long the actor's teardown will take.
    registry.wait_for_slot_closed(&rig.key).await;
    assert_eq!(registry.slot_state(&rig.key), None);
    // The actor logs only that deletion was deferred, so when the WAL survives, ask the
    // service directly and put its error in the failure: that names which step refused.
    if let Some(path) = rig.service.wal.resolve_live_path(&rig.key).unwrap() {
        let retried = rig
            .service
            .delete_expired(&rig.key, rig.service.clock.now_ms());
        panic!("expired WAL still present at {path:?} after the actor closed; a direct delete_expired now returns {retried:?}");
    }
    assert_eq!(rig.store.pending_retention_withdrawals().unwrap().len(), 1);
    let questions = Arc::new(RetainedQuestions::default());
    rig.service.set_withdrawal_adapter(Arc::new(
        crate::retention_withdrawals::ScopedRetentionWithdrawalAdapter(questions.clone()),
    ));
    assert!(rig.service.withdraw_retained_questions().await.unwrap());
    assert_eq!(questions.withdrawals.lock().unwrap().len(), 1);
    assert_no_marker(&rig.root);
    assert!(registry.stop_and_seal().await);
}

#[tokio::test(start_paused = true)]
async fn content_free_worker_retries_without_actor_or_wal_traffic() {
    use broca_core::tool_plane::{ApprovalRouteError, ApprovalWithdrawal};
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    seed_owed_question_at(&rig, now, now + 60_000, Some("question"));
    {
        let _lease = rig.service.lease(&rig.key).unwrap();
        rig.service.delete_expired(&rig.key, now + 2000).unwrap();
    }
    let questions = Arc::new(RetainedQuestions::default());
    questions
        .replies
        .lock()
        .unwrap()
        .push_back(ApprovalWithdrawal::Retry(ApprovalRouteError {
            code: "elicitation_unavailable".into(),
            retryable: true,
        }));
    rig.service.set_withdrawal_adapter(Arc::new(
        crate::retention_withdrawals::ScopedRetentionWithdrawalAdapter(questions.clone()),
    ));
    let service = rig.service.clone();
    let worker = tokio::spawn(async move { service.run_withdrawal_worker().await });
    broca_test_support::event_timeout(
        "content_free_worker_retries_without_actor_or_wal_traffic",
        async {
            while !rig
                .store
                .pending_retention_withdrawals()
                .unwrap()
                .is_empty()
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        },
        || {
            broca_test_support::record_kinds(
                rig.service
                    .wal
                    .read_records(&rig.key)
                    .unwrap()
                    .iter()
                    .map(|f| &f.record),
            )
        },
    )
    .await
    .expect("content-free retry parked without session traffic");
    worker.abort();
    let _ = worker.await;
    assert_eq!(questions.withdrawals.lock().unwrap().len(), 2);
    assert!(rig
        .service
        .wal
        .resolve_live_path(&rig.key)
        .unwrap()
        .is_none());
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn unknown_scope_principal_payload_never_reaches_retention_and_expires_unservable() {
    use broca_store::retention_withdrawals::RetentionQuestionId;
    for unknown_owner in [true, false] {
        let rig = Rig::new();
        // Freeze the clock: the question expires 3 s after `now`, and a slow runner can spend
        // longer than that between seeding it and asking for withdrawals, which made the
        // "not yet expired" step below race real time.
        let now = rig.service.clock.now_ms();
        fs::write(
            rig.root.join("test-clock"),
            format!(r#"{{"now_ms":{now}}}"#),
        )
        .unwrap();
        rig.service.clock.refresh().unwrap();
        let unknown = serde_json::json!({"kind":"future_principal", "payload":MARKER});
        let mut scope = serde_json::json!({"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"head","scope_epoch":1});
        if unknown_owner {
            scope["owner"] = unknown;
        } else {
            scope["flow_id"] = "flow".into();
            scope["sender"] = unknown;
        }
        let (_, call_key) =
            seed_owed_question_with_scope_at(&rig, now, now + 3000, Some("question"), scope);
        {
            let _lease = rig.service.lease(&rig.key).unwrap();
            rig.service.delete_expired(&rig.key, now + 2000).unwrap();
        }
        assert!(rig
            .service
            .wal
            .resolve_live_path(&rig.key)
            .unwrap()
            .is_none());
        assert_no_marker(&rig.root);
        let rows = rig.store.pending_retention_withdrawals().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].call_key, call_key);
        assert!(
            rows[0].scope.is_none(),
            "unknown scope became a servable identity"
        );
        assert_eq!(
            rows[0].question,
            RetentionQuestionId::Known("question".into())
        );
        let questions = Arc::new(RetainedQuestions::default());
        rig.service.set_withdrawal_adapter(Arc::new(
            crate::retention_withdrawals::ScopedRetentionWithdrawalAdapter(questions.clone()),
        ));
        assert!(!rig.service.withdraw_retained_questions().await.unwrap());
        assert!(questions.withdrawals.lock().unwrap().is_empty());
        fs::write(
            rig.root.join("test-clock"),
            format!(r#"{{"now_ms":{}}}"#, now + 4000),
        )
        .unwrap();
        rig.service.clock.refresh().unwrap();
        assert!(rig.service.withdraw_retained_questions().await.unwrap());
        assert!(rig.service.withdraw_retained_questions().await.unwrap());
        assert!(rig
            .store
            .pending_retention_withdrawals()
            .unwrap()
            .is_empty());
        assert!(questions.withdrawals.lock().unwrap().is_empty());
        assert_no_marker(&rig.root);
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn retained_size_refusal_seals_error_and_expires_within_deletion_bound() {
    let rig = Rig::new();
    let cap = 8192;
    fs::write(rig.root.join("test-retained-wal-cap"), cap.to_string()).unwrap();
    let registry = rig.registry_with_reply(None, "x".repeat(cap * 2));
    let mut req = request(&rig.key, "limited-run");
    req.retention = Some(10);
    assert!(matches!(
        registry
            .route_send(&rig.key, req, crate::executor::RunKind::FreshOrResume)
            .await
            .unwrap(),
        crate::actor::SendOutcome::Active { .. }
    ));
    wait_idle(&registry, &rig.key).await;
    let records = rig.service.wal.read_records(&rig.key).unwrap();
    assert!(matches!(&records.last().unwrap().record,
        WalRecord::RunFinished { reason: RunFinishReason::Error, error: Some(error), .. }
        if error.class == broca_protocol::ErrorClass::Permanent && error.provider_code.as_deref() == Some("retained_session_too_large") && error.message.contains("8192")));
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    assert!(fs::metadata(&path).unwrap().len() <= cap as u64);
    let deadline = policy(&records).unwrap().2.unwrap();
    fs::write(rig.root.join("test-clock"), br#"{"offset_ms":11000}"#).unwrap();
    rig.service.clock.refresh().unwrap();
    assert!(matches!(
        registry.route_status(&rig.key).await,
        Err(crate::registry::OpenError::Expired { .. })
    ));
    broca_test_support::event_timeout(
        "retained_size_refusal_seals_error_and_expires_within_deletion_bound",
        async {
            while path.exists() {
                registry
                    .sweep_once(rig.service.clock.now_ms())
                    .await
                    .unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        },
        || {
            broca_test_support::record_kinds(
                rig.service
                    .wal
                    .read_records(&rig.key)
                    .unwrap()
                    .iter()
                    .map(|f| &f.record),
            )
        },
    )
    .await
    .expect("expiry releases the actor lease and deletion completes");
    assert!(!path.exists());
    assert!(rig.service.clock.now_ms() - deadline < 3_600_000);
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn size_refusal_crash_cut_reopens_with_room_for_orphan_seal() {
    let rig = Rig::new();
    let cap = 4096;
    fs::write(rig.root.join("test-retained-wal-cap"), cap.to_string()).unwrap();
    let now = rig.service.clock.now_ms();
    rig.seed(10, now, None);
    rig.service.rederive(&rig.key).unwrap();
    {
        let writer =
            broca_core::WalHandle::spawn(rig.service.wal.open(&rig.key).unwrap(), 1).unwrap();
        // As admission would: the seeded lineage started without tags.
        writer.seed_lineage_tags(Some(broca_core::LineageTags::Off));
        let mut oversized = started(&rig.key, "r2", None, now);
        if let WalRecord::RunStarted { title, .. } = &mut oversized {
            *title = Some("x".repeat(cap * 2));
        }
        writer.record(oversized);
        assert!(matches!(
            writer.sync().await,
            Err(broca_core::WalWriteError::RetainedSessionTooLarge { max_bytes: 4096 })
        ));
        assert!(!writer.is_poisoned());
        // Simulate a crash after refusing the oversized append but before writing RunFinished.
        // Startup sees the accepted unfinished run and must commit an Interrupted terminal
        // using the space reserved before the refusal.
    }
    let registry = rig.registry(None);
    let sweep = registry.mark_crash_orphans(
        rig.service.wal.as_ref(),
        vec![(RunId("r1".into()), rig.key.clone())],
        std::time::SystemTime::now() + std::time::Duration::from_secs(1),
    );
    assert_eq!(registry.seal_marked_crash_orphans(sweep).await.sealed, 1);
    let records = rig.service.wal.read_records(&rig.key).unwrap();
    assert!(matches!(
        records.last().unwrap().record,
        WalRecord::RunFinished {
            reason: RunFinishReason::Interrupted,
            ..
        }
    ));
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    assert!(fs::metadata(&path).unwrap().len() <= cap as u64);
    fs::write(rig.root.join("test-clock"), br#"{"offset_ms":11000}"#).unwrap();
    rig.service.clock.refresh().unwrap();
    broca_test_support::event_timeout(
        "size_refusal_crash_cut_reopens_with_room_for_orphan_seal",
        async {
            while path.exists() {
                registry
                    .sweep_once(rig.service.clock.now_ms())
                    .await
                    .unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        },
        || {
            broca_test_support::record_kinds(
                rig.service
                    .wal
                    .read_records(&rig.key)
                    .unwrap()
                    .iter()
                    .map(|f| &f.record),
            )
        },
    )
    .await
    .expect("expiry releases the actor lease and deletion completes");
    assert!(!path.exists());
}

#[cfg(feature = "test-clock")]
fn refuse_retention_reader(rig: &mut Rig) {
    rig.service = Arc::new(
        RetentionService::new(
            Arc::new(FileWalStore::new(rig.root.join("wal")).with_read_cap_for_test(64)),
            rig.store.clone(),
            rig.leases.clone(),
            rig.store.clone(),
            Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1)),
        )
        .unwrap(),
    );
}

#[cfg(feature = "test-clock")]
#[test]
fn refused_reader_deletes_current_projection_without_content_or_withdrawals() {
    let mut rig = Rig::new();
    let id = rig.seed(1, 100, Some(200));
    rig.service.rederive(&rig.key).unwrap();
    refuse_retention_reader(&mut rig);
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    assert!(matches!(
        broca_wal::replay_file(&path, 64),
        Err(broca_wal::WalError::TooLarge { .. })
    ));
    let logs = broca_test_support::LogCapture::new();
    let _lease = rig.service.lease(&rig.key).unwrap();
    assert_eq!(
        logs.in_scope(|| rig.service.delete_expired(&rig.key, 1200))
            .unwrap(),
        Some(id)
    );
    assert!(!path.exists() && !path.with_extension("session").exists());
    assert_eq!(
        rig.store
            .lookup_run_retention("r1")
            .unwrap()
            .unwrap()
            .expired_at_ms,
        Some(1200)
    );
    let warnings: Vec<_> = logs
        .events()
        .into_iter()
        .filter(|event| event.level == tracing::Level::WARN)
        .collect();
    assert_eq!(
        warnings
            .iter()
            .filter(|event| event
                .message()
                .contains("withdrawal identities unavailable"))
            .count(),
        1
    );
    let rendered = format!("{warnings:?}");
    assert!(!rendered.contains(MARKER) && !rendered.contains("TooLarge"));
    assert_eq!(rig.service.unreadable_sessions(), 0);
}

#[cfg(feature = "test-clock")]
#[test]
fn unreadable_stale_projection_keeps_log_and_counts_one_content_free_error() {
    let mut rig = Rig::new();
    rig.seed(1, 100, Some(200));
    rig.service.rederive(&rig.key).unwrap();
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    let mut bytes = fs::read(&path).unwrap();
    bytes.extend_from_slice(MARKER.as_bytes());
    fs::write(&path, bytes).unwrap();
    refuse_retention_reader(&mut rig);
    let logs = broca_test_support::LogCapture::new();
    let _lease = rig.service.lease(&rig.key).unwrap();
    for _ in 0..2 {
        assert_eq!(
            logs.in_scope(|| rig.service.delete_expired(&rig.key, 1200))
                .unwrap(),
            None
        );
    }
    assert!(path.exists());
    assert_eq!(rig.service.unreadable_sessions(), 1);
    assert_eq!(rig.registry(None).retention_unreadable_sessions(), 1);
    let errors: Vec<_> = logs
        .events()
        .into_iter()
        .filter(|event| event.level == tracing::Level::ERROR)
        .collect();
    assert_eq!(errors.len(), 1);
    assert!(errors[0].fields.contains_key("session"));
    assert!(!format!("{errors:?}").contains(MARKER));
}

#[cfg(feature = "test-clock")]
#[test]
fn unreadable_same_size_foreign_lineage_never_uses_indexed_deadline() {
    let mut rig = Rig::new();
    rig.seed(1, 100, Some(200));
    rig.service.rederive(&rig.key).unwrap();
    let other = Rig::new();
    other.seed(1, 100, Some(200));
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    let mut bytes = fs::read(&path).unwrap();
    let header = fs::read(other.service.wal.ephemeral_wal_path(&other.key)).unwrap();
    bytes[..broca_wal::framing::LINEAGE_FRAME_LEN]
        .copy_from_slice(&header[..broca_wal::framing::LINEAGE_FRAME_LEN]);
    fs::write(&path, bytes).unwrap();
    refuse_retention_reader(&mut rig);
    let _lease = rig.service.lease(&rig.key).unwrap();
    assert_eq!(rig.service.delete_expired(&rig.key, 1200).unwrap(), None);
    assert!(path.exists());
    assert_eq!(rig.service.unreadable_sessions(), 1);
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn resumed_size_refusal_seals_the_admitted_id_not_the_request_id() {
    let rig = Rig::new();
    fs::write(rig.root.join("test-retained-wal-cap"), "8192").unwrap();
    let lease = rig.service.lease(&rig.key).unwrap();
    let (writer, _lease) = broca_core::WalHandle::open_and_spawn_observed_with_retention(
        rig.service.wal.clone(),
        rig.key.clone(),
        lease,
        None,
        Some(10),
    )
    .await
    .unwrap();
    let mut req = request(&rig.key, "admitted-run");
    req.retention = Some(10);
    let (out, mut events) = tokio::sync::mpsc::channel(128);
    broca_core::run_loop_on_writer(
        req.clone(),
        &writer,
        broca_core::StartMode::FreshOrResume,
        &broca_core::StubProvider::new(vec![]),
        &broca_core::StubToolPlane::new(),
        &broca_core::AllowAll,
        rig.service.wal.as_ref(),
        Some(rig.store.clone()),
        broca_core::RunControl {
            clock: Some(rig.service.clock.clone()),
            pause_for_auth_at_start: Some(
                serde_json::from_value(
                    serde_json::json!({"class":"auth","message":"reauthenticate"}),
                )
                .unwrap(),
            ),
            ..Default::default()
        },
        &out,
    )
    .await
    .unwrap();
    while events.try_recv().is_ok() {}
    req.run_id = RunId("resuming-request-id".into());
    req.retention = None;
    let provider = broca_core::StubProvider::new(vec![vec![
        broca_protocol::NormalizedEvent::Control(
            broca_protocol::ProviderControl::AssistantMessage {
                message: broca_protocol::content::AssistantMessage {
                    message_id: broca_protocol::MessageId("large".into()),
                    content: vec![broca_protocol::content::ContentBlock::Text {
                        text: "x".repeat(16384),
                    }],
                },
            },
        ),
        broca_protocol::NormalizedEvent::Control(broca_protocol::ProviderControl::Finished {
            usage: Default::default(),
            finish_reason: broca_protocol::FinishReason::Stop,
            retries_used: 0,
        }),
    ]]);
    let result = broca_core::run_loop_on_writer(
        req,
        &writer,
        broca_core::StartMode::ResumeAfterAuth,
        &provider,
        &broca_core::StubToolPlane::new(),
        &broca_core::AllowAll,
        rig.service.wal.as_ref(),
        Some(rig.store.clone()),
        broca_core::RunControl {
            clock: Some(rig.service.clock.clone()),
            ..Default::default()
        },
        &out,
    )
    .await;
    assert!(matches!(
        result,
        Ok(broca_core::RunSummary {
            reason: RunFinishReason::Error,
            ..
        })
    ));
    let records = rig.service.wal.read_records(&rig.key).unwrap();
    assert!(matches!(
        records.last().unwrap().record,
        WalRecord::RunFinished {
            reason: RunFinishReason::Error,
            ..
        }
    ));
    assert!(
        policy(&records).unwrap().2.is_some(),
        "a resumed size refusal must get an expiry deadline"
    );
    let mut finished = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let broca_core::OutputEvent::Control(broca_protocol::ControlUnit::RunFinished {
            run_id,
            ..
        }) = event
        {
            finished.push(run_id.unwrap().0);
        }
    }
    assert_eq!(finished, ["admitted-run"]);
}

#[cfg(feature = "test-clock")]
async fn assert_tight_crash_recovery(open_batch: bool) {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    rig.seed(10, now, None);
    if open_batch {
        let mut wal = rig.service.wal.open(&rig.key).unwrap();
        wal.append(
            1,
            &WalRecord::StepStarted {
                step_id: broca_protocol::StepId(1),
            },
            true,
        )
        .unwrap();
        wal.append(
            1,
            &WalRecord::ModelStepFinished {
                fallback: Default::default(),
                written_at_ms: None,
                tags: None,
                hooks: None,
                step_id: broca_protocol::StepId(1),
                assistant_message: broca_protocol::AssistantMessage {
                    message_id: broca_protocol::MessageId("m".into()),
                    content: vec![broca_protocol::ContentBlock::ToolCall(
                        broca_protocol::ToolCall {
                            thought_signature: None,
                            tool_call_id: broca_protocol::ToolCallId("c".into()),
                            tool_name: "edit".into(),
                            input: serde_json::json!({}),
                        },
                    )],
                }
                .into(),
                usage: Default::default(),
                finish_reason: Some(broca_protocol::FinishReason::ToolCalls),
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            true,
        )
        .unwrap();
        wal.append(
            1,
            &WalRecord::ToolBatchStarted {
                batch_id: broca_protocol::BatchId("b".into()),
                step_id: broca_protocol::StepId(1),
                planned_calls: vec![broca_protocol::ToolCallId("c".into())],
            },
            true,
        )
        .unwrap();
        wal.append(
            1,
            &WalRecord::ToolDispatchIntent {
                dispatched_at_ms: None,
                batch_id: broca_protocol::BatchId("b".into()),
                tool_call_id: broca_protocol::ToolCallId("c".into()),
                tool_name: Some("edit".into()),
                args: Arc::new(serde_json::json!({})),
                module_id: None,
                schema_pin: None,
                approval_digest: None,
                call_key: None,
                hooks: None,
            },
            true,
        )
        .unwrap();
    }
    let raw: Vec<_> = rig
        .service
        .wal
        .read_records(&rig.key)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    let prior = broca_core::replay(&raw).unwrap();
    assert_eq!(
        !broca_core::close_out_open_batch(&prior, |_| broca_core::DeferredCloseKind::NotDispatched)
            .is_empty(),
        open_batch,
        "fixture must execute the requested recovery path"
    );
    rig.service.rederive(&rig.key).unwrap();
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    let prefix_bytes = fs::metadata(&path).unwrap().len();
    let cap = prefix_bytes + broca_core::WalHandle::terminal_reserve_for_test();
    fs::write(rig.root.join("test-retained-wal-cap"), cap.to_string()).unwrap();
    {
        let writer =
            broca_core::WalHandle::spawn(rig.service.wal.open(&rig.key).unwrap(), 1).unwrap();
        writer.record(WalRecord::StepStarted {
            step_id: broca_protocol::StepId(99),
        });
        assert!(matches!(
            writer.sync().await,
            Err(broca_core::WalWriteError::RetainedSessionTooLarge { .. })
        ));
        assert_eq!(fs::metadata(&path).unwrap().len(), prefix_bytes);
    }
    let registry = rig.registry(None);
    let sweep = registry.mark_crash_orphans(
        rig.service.wal.as_ref(),
        vec![(RunId("r1".into()), rig.key.clone())],
        std::time::SystemTime::now() + std::time::Duration::from_secs(1),
    );
    let logs = broca_test_support::LogCapture::observe(|_| true);
    let sweep = broca_test_support::event_timeout(
        "assert_tight_crash_recovery",
        registry.seal_marked_crash_orphans(sweep),
        || {
            broca_test_support::record_kinds(
                rig.service
                    .wal
                    .read_records(&rig.key)
                    .unwrap()
                    .iter()
                    .map(|f| &f.record),
            )
        },
    )
    .await
    .expect("tight-cap crash recovery terminates");
    assert_eq!(
        sweep.sealed,
        1,
        "open batch: {open_batch}; {:?}",
        logs.events()
    );
    let frames = rig.service.wal.read_records(&rig.key).unwrap();
    assert!(matches!(
        frames.last().unwrap().record,
        WalRecord::RunFinished { .. }
    ));
    let deadline = policy(&frames).unwrap().2.unwrap();
    assert!(fs::metadata(&path).unwrap().len() <= cap);
    assert!(registry.stop_and_seal().await);
    fs::write(
        rig.root.join("test-clock"),
        serde_json::json!({"now_ms":deadline + 1}).to_string(),
    )
    .unwrap();
    rig.service.clock.refresh().unwrap();
    registry.sweep_once(deadline + 1).await.unwrap();
    assert!(
        !path.exists(),
        "a capped crash seal receives a deadline and deletes"
    );
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn exactly_reserved_prefix_reopens_and_seals_plain_crash_cut() {
    assert_tight_crash_recovery(false).await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn exactly_reserved_prefix_reopens_and_seals_open_batch_crash_cut() {
    assert_tight_crash_recovery(true).await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn a_size_sealed_run_returns_summary_and_delivers_subscriber_terminal() {
    let rig = Rig::new();
    fs::write(rig.root.join("test-retained-wal-cap"), "8192").unwrap();
    let lease = rig.service.lease(&rig.key).unwrap();
    let (writer, _lease) = broca_core::WalHandle::open_and_spawn_observed_with_retention(
        rig.service.wal.clone(),
        rig.key.clone(),
        lease,
        None,
        Some(10),
    )
    .await
    .unwrap();
    let mut req = request(&rig.key, "size-summary");
    req.retention = Some(10);
    let (out, mut events) = tokio::sync::mpsc::channel(128);
    let provider = broca_core::StubProvider::new(vec![vec![
        broca_protocol::NormalizedEvent::Control(
            broca_protocol::ProviderControl::AssistantMessage {
                message: broca_protocol::AssistantMessage {
                    message_id: broca_protocol::MessageId("large".into()),
                    content: vec![broca_protocol::ContentBlock::Text {
                        text: "x".repeat(16384),
                    }],
                },
            },
        ),
        broca_protocol::NormalizedEvent::Control(broca_protocol::ProviderControl::Finished {
            usage: Default::default(),
            finish_reason: broca_protocol::FinishReason::Stop,
            retries_used: 0,
        }),
    ]]);
    let result = broca_core::run_loop_on_writer(
        req,
        &writer,
        broca_core::StartMode::FreshOrResume,
        &provider,
        &broca_core::StubToolPlane::new(),
        &broca_core::AllowAll,
        rig.service.wal.as_ref(),
        Some(rig.store.clone()),
        broca_core::RunControl {
            clock: Some(rig.service.clock.clone()),
            ..Default::default()
        },
        &out,
    )
    .await;
    let summary =
        result.expect("a durable Error terminal is a completed run, not infrastructure failure");
    assert_eq!(summary.reason, RunFinishReason::Error);
    let mut terminal = false;
    let mut error = false;
    while let Ok(event) = events.try_recv() {
        match event {
            broca_core::OutputEvent::Control(broca_protocol::ControlUnit::Error { .. }) => {
                error = true
            }
            broca_core::OutputEvent::Control(broca_protocol::ControlUnit::RunFinished {
                reason: RunFinishReason::Error,
                ..
            }) => terminal = true,
            _ => {}
        }
    }
    assert!(error && terminal);
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn refused_retained_first_admission_does_not_cap_a_later_forever_send() {
    let rig = Rig::new();
    fs::write(rig.root.join("test-retained-wal-cap"), "4096").unwrap();
    let registry = rig.registry(None);
    let mut req = request(&rig.key, "too-large-retained");
    req.retention = Some(10);
    req.input = vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text {
            text: "x".repeat(8192),
        }],
    )];
    assert!(matches!(
        registry
            .route_send(&rig.key, req, crate::executor::RunKind::FreshOrResume)
            .await
            .unwrap(),
        crate::actor::SendOutcome::RetainedSessionTooLarge { .. }
    ));
    let mut req = request(&rig.key, "forever-after-refusal");
    req.retention = None;
    req.input = vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text {
            text: "y".repeat(8192),
        }],
    )];
    assert!(matches!(
        registry
            .route_send(&rig.key, req, crate::executor::RunKind::FreshOrResume)
            .await
            .unwrap(),
        crate::actor::SendOutcome::Active { .. }
    ));
    wait_idle(&registry, &rig.key).await;
    let frames = rig.service.wal.read_records(&rig.key).unwrap();
    assert!(matches!(
        &frames[0].record,
        WalRecord::RunStarted {
            retention: None,
            ..
        }
    ));
}

#[tokio::test]
async fn fresh_bind_is_known_empty_without_a_warning() {
    let rig = Rig::new();
    let registry = rig.registry(None);
    let logs = broca_test_support::LogCapture::new();
    let ownership = logs
        .scope_future(registry.session_ownership_for_bind(&rig.key))
        .await;
    assert_eq!(ownership, Some(broca_core::SessionOwnershipV1::default()));
    assert!(logs.events().is_empty(), "{:?}", logs.events());
    assert!(rig
        .service
        .wal
        .resolve_live_path(&rig.key)
        .unwrap()
        .is_none());
}

#[test]
fn readable_record_first_legacy_deletion_accepts_live_and_moved_layouts() {
    for moved in [false, true] {
        let rig = Rig::new();
        let id = rig.seed(1, 100, Some(200));
        let path = rig.service.wal.ephemeral_wal_path(&rig.key);
        let bytes = fs::read(&path).unwrap();
        let header_len = broca_wal::framing::LINEAGE_FRAME_LEN;
        fs::write(&path, &bytes[header_len..]).unwrap();
        assert_eq!(
            rig.service
                .read_path(&path)
                .unwrap()
                .lineage_id
                .unwrap()
                .to_string(),
            id
        );
        if moved {
            let stone = Tombstone {
                next_tag: None,
                next_notice_seq: None,
                lineage_id: id.clone(),
                expired_at_ms: 1200,
                seq: 1,
                completed_at_ms: None,
            };
            rig.service.write_tombstone(&rig.key, &stone).unwrap();
            fs::rename(&path, rig.service.deleting(&rig.key, &id)).unwrap();
            let _lease = rig.service.lease(&rig.key).unwrap();
            rig.service.complete(&rig.key, stone, 1200).unwrap();
        } else {
            let _lease = rig.service.lease(&rig.key).unwrap();
            assert_eq!(
                rig.service.delete_expired(&rig.key, 1200).unwrap(),
                Some(id.clone())
            );
        }
        assert!(!path.exists() && !rig.service.deleting(&rig.key, &id).exists());
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn refused_attempt_is_never_reported_at_the_reserved_terminal_sequence() {
    #[derive(Default)]
    struct Reports(Mutex<Vec<u64>>);
    impl broca_core::AccountReporter for Reports {
        fn run_started(&self, _: Option<broca_wal::WalLineageId>, _: u64) {}
        fn attempt_recorded(
            &self,
            _: Option<broca_wal::WalLineageId>,
            seq: u64,
            _: &broca_protocol::ModelAttempt,
        ) {
            self.0.lock().unwrap().push(seq);
        }
    }
    let rig = Rig::new();
    fs::write(rig.root.join("test-retained-wal-cap"), "8192").unwrap();
    let lease = rig.service.lease(&rig.key).unwrap();
    let (writer, _lease) = broca_core::WalHandle::open_and_spawn_observed_with_retention(
        rig.service.wal.clone(),
        rig.key.clone(),
        lease,
        None,
        Some(10),
    )
    .await
    .unwrap();
    let mut req = request(&rig.key, "attempt-refusal");
    req.retention = Some(10);
    let attempt = serde_json::from_value(serde_json::json!({
        "attempt":0,"request_body_len":1,"request_body_sha256":"aa","http_status":200,
        "response_body_len":1,"response_body_sha256":"x".repeat(16384),"outcome":"complete","ts_ms":1
    })).unwrap();
    let provider = broca_core::StubProvider::new(vec![vec![
        broca_protocol::NormalizedEvent::Control(
            broca_protocol::ProviderControl::AttemptFinished { attempt },
        ),
        broca_protocol::NormalizedEvent::Control(broca_protocol::ProviderControl::Finished {
            usage: Default::default(),
            finish_reason: broca_protocol::FinishReason::Stop,
            retries_used: 0,
        }),
    ]]);
    let reports = Arc::new(Reports::default());
    let (out, _events) = tokio::sync::mpsc::channel(128);
    let _result = broca_core::run_loop_on_writer(
        req,
        &writer,
        broca_core::StartMode::FreshOrResume,
        &provider,
        &broca_core::StubToolPlane::new(),
        &broca_core::AllowAll,
        rig.service.wal.as_ref(),
        Some(rig.store.clone()),
        broca_core::RunControl {
            clock: Some(rig.service.clock.clone()),
            account_reporter: Some(reports.clone()),
            ..Default::default()
        },
        &out,
    )
    .await;
    let reported = reports.0.lock().unwrap().clone();
    assert!(reported.is_empty(), "refused attempt reports: {reported:?}");
    let frames = rig.service.wal.read_records(&rig.key).unwrap();
    assert!(matches!(
        frames.last().unwrap().record,
        WalRecord::RunFinished {
            reason: RunFinishReason::Error,
            ..
        }
    ));
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn attached_subscriber_receives_error_and_run_finished_for_size_refusal() {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    rig.seed(10, now, Some(now));
    fs::write(rig.root.join("test-retained-wal-cap"), "8192").unwrap();
    let registry = rig.registry_with_reply(None, "x".repeat(16384));
    let (sink, mut deliveries) = tokio::sync::mpsc::channel(128);
    registry
        .route_subscribe(&rig.key, crate::fanout::FromCursor::Start, sink)
        .await
        .unwrap();
    let mut req = request(&rig.key, "subscriber-size");
    req.retention = None;
    assert!(matches!(
        registry
            .route_send(&rig.key, req, crate::executor::RunKind::FreshOrResume)
            .await
            .unwrap(),
        crate::actor::SendOutcome::Active { .. }
    ));
    broca_test_support::event_timeout(
        "attached_subscriber_receives_error_and_run_finished_for_size_refusal",
        async {
            let mut error = false;
            loop {
                let delivery = deliveries
                    .recv()
                    .await
                    .expect("subscriber remains attached through terminal");
                if let crate::fanout::Delivery::Control(event) = delivery {
                    match &event.1 {
                        broca_protocol::ControlUnit::Error { .. } => error = true,
                        broca_protocol::ControlUnit::RunFinished {
                            run_id: Some(id),
                            reason: RunFinishReason::Error,
                            ..
                        } if id.0 == "subscriber-size" => {
                            assert!(error);
                            break;
                        }
                        _ => {}
                    }
                }
            }
        },
        || {
            broca_test_support::record_kinds(
                rig.service
                    .wal
                    .read_records(&rig.key)
                    .unwrap()
                    .iter()
                    .map(|f| &f.record),
            )
        },
    )
    .await
    .expect("attached subscriber receives terminal");
    assert!(registry.stop_and_seal().await);
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn cold_status_rechecks_clock_without_the_mutating_opener() {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    fs::write(
        rig.root.join("test-clock"),
        serde_json::json!({"now_ms":now}).to_string(),
    )
    .unwrap();
    rig.service.clock.refresh().unwrap();
    rig.seed(1, now, Some(now));
    let registry = rig.registry(None);
    let service = rig.service.clone();
    let root = rig.root.clone();
    registry.status_open_probe_for_test(Arc::new(move || {
        fs::write(
            root.join("test-clock"),
            serde_json::json!({"now_ms":now + 1001}).to_string(),
        )
        .unwrap();
        service.clock.refresh().unwrap();
    }));
    let result = registry.route_status(&rig.key).await;
    assert!(
        matches!(result, Err(crate::registry::OpenError::Expired { expired_at_ms }) if expired_at_ms == now + 1000),
        "{result:?}"
    );
    assert!(rig.service.wal.ephemeral_wal_path(&rig.key).exists());
    assert!(rig.service.tombstones(&rig.key).unwrap().is_empty());
    assert_eq!(registry.slot_counts().open, 0);
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn status_never_expires_a_new_active_run_from_a_captured_old_head() {
    struct Open {
        head: Arc<AtomicU64>,
        active: Arc<AtomicBool>,
    }
    #[async_trait::async_trait]
    impl crate::registry::LineageOpener for Open {
        async fn open(
            &self,
            _: &BindIdentity,
            gen: u64,
            _: crate::registry::RegistryHandle,
        ) -> Result<crate::registry::ActorHandle, crate::registry::OpenError> {
            let (tx, mut rx) = tokio::sync::mpsc::channel(4);
            let active = self.active.clone();
            tokio::spawn(async move {
                while let Some(cmd) = rx.recv().await {
                    if let crate::actor::LineageCmd::Status { reply } = cmd {
                        let value = if active.load(Ordering::Acquire) {
                            crate::actor::LineageStatus::Active {
                                run_id: RunId("r2".into()),
                                step: 0,
                            }
                        } else {
                            crate::actor::LineageStatus::Idle
                        };
                        let _ = reply.send(value);
                    }
                }
            });
            Ok(crate::registry::ActorHandle {
                media_head: None,
                gen,
                tx,
                durable_head: self.head.clone(),
                run_in_flight: Arc::new(AtomicU64::new(0)),
                first_episode: Arc::default(),
                approvals: Arc::default(),
            })
        }
    }
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    fs::write(
        rig.root.join("test-clock"),
        serde_json::json!({"now_ms":now}).to_string(),
    )
    .unwrap();
    rig.service.clock.refresh().unwrap();
    rig.seed(1, now - 500, Some(now - 500));
    let old = rig
        .service
        .wal
        .read_records(&rig.key)
        .unwrap()
        .last()
        .unwrap()
        .seq;
    let head = Arc::new(AtomicU64::new(old));
    let active = Arc::new(AtomicBool::new(false));
    let registry = crate::registry::LineageRegistry::new_with_reader(
        Arc::new(Open {
            head: head.clone(),
            active: active.clone(),
        }),
        rig.service.wal.clone(),
        Some(rig.store.clone()),
    );
    registry.set_retention_service(rig.service.clone());
    registry.actor_for(&rig.key).await.unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    let (release, rx) = std::sync::mpsc::channel();
    let rx = Mutex::new(rx);
    registry.expiry_probe_for_test(Arc::new(move || {
        signal.notify_one();
        let _ = rx.lock().unwrap().recv();
    }));
    let copy = registry.clone();
    let key = rig.key.clone();
    let mut pending = tokio::spawn(async move { copy.route_status(&key).await });
    let result = broca_test_support::event_timeout("open", async {
        let captured = tokio::select! {
            _ = entered.notified() => true,
            result = &mut pending => { assert_eq!(result.unwrap().unwrap(), crate::actor::LineageStatus::Idle); false },
        };
        let mut wal = rig.service.wal.open(&rig.key).unwrap();
        let seq = wal.append(1, &started(&rig.key, "r2", None, now), true).unwrap();
        head.store(seq, Ordering::Release); active.store(true, Ordering::Release);
        fs::write(rig.root.join("test-clock"), serde_json::json!({"now_ms":now + 501}).to_string()).unwrap(); rig.service.clock.refresh().unwrap();
        release.send(()).ok();
        if captured { pending.await.unwrap() } else { registry.route_status(&rig.key).await }
    }, || broca_test_support::record_kinds(rig.service.wal.read_records(&rig.key).unwrap().iter().map(|f| &f.record))).await.expect("status interleaving terminates");
    assert!(
        matches!(result, Ok(crate::actor::LineageStatus::Active { run_id, .. }) if run_id.0 == "r2")
    );
}

#[tokio::test]
async fn missing_materialized_history_is_not_known_empty_ownership() {
    let rig = Rig::new();
    let mut wal = rig.service.wal.open(&rig.key).unwrap();
    wal.append(1, &started(&rig.key, "forever", None, 100), true)
        .unwrap();
    wal.append(
        1,
        &WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: broca_protocol::StepId(1),
            assistant_message: broca_protocol::AssistantMessage {
                message_id: broca_protocol::MessageId("history-message".into()),
                content: vec![broca_protocol::ContentBlock::Text {
                    text: "durable history".into(),
                }],
            }
            .into(),
            usage: Default::default(),
            finish_reason: Some(broca_protocol::FinishReason::Stop),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        },
        true,
    )
    .unwrap();
    wal.append(
        1,
        &WalRecord::TurnFinished {
            step_id: broca_protocol::StepId(1),
        },
        true,
    )
    .unwrap();
    wal.append(1, &finished(200), true).unwrap();
    drop(wal);
    let frames = rig.service.wal.read_records(&rig.key).unwrap();
    rig.store.project(&rig.key, &frames).unwrap();
    assert!(
        rig.store
            .read_admission_snapshot(&rig.key)
            .unwrap()
            .is_some(),
        "history fixture must create materialized metadata"
    );
    fs::remove_file(rig.service.wal.wal_path(&rig.key)).unwrap();
    let registry = rig.registry(None);
    let logs = broca_test_support::LogCapture::new();
    assert_eq!(
        logs.scope_future(registry.session_ownership_for_bind(&rig.key))
            .await,
        None
    );
    assert!(logs
        .events()
        .iter()
        .any(|event| event.level == tracing::Level::WARN));
}

#[tokio::test]
async fn zero_record_wal_does_not_hide_materialized_ownership_history() {
    for header_only in [false, true] {
        let rig = Rig::new();
        let mut wal = rig.service.wal.open(&rig.key).unwrap();
        wal.append(1, &started(&rig.key, "forever", None, 100), true)
            .unwrap();
        wal.append(
            1,
            &WalRecord::ModelStepFinished {
                fallback: Default::default(),
                written_at_ms: None,
                tags: None,
                hooks: None,
                step_id: broca_protocol::StepId(1),
                assistant_message: broca_protocol::AssistantMessage {
                    message_id: broca_protocol::MessageId("old".into()),
                    content: vec![broca_protocol::ContentBlock::Text {
                        text: "durable history".into(),
                    }],
                }
                .into(),
                usage: Default::default(),
                finish_reason: Some(broca_protocol::FinishReason::Stop),
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            true,
        )
        .unwrap();
        wal.append(
            1,
            &WalRecord::TurnFinished {
                step_id: broca_protocol::StepId(1),
            },
            true,
        )
        .unwrap();
        wal.append(1, &finished(200), true).unwrap();
        drop(wal);
        let frames = rig.service.wal.read_records(&rig.key).unwrap();
        rig.store.project(&rig.key, &frames).unwrap();
        assert!(rig
            .store
            .read_admission_snapshot(&rig.key)
            .unwrap()
            .is_some());
        let path = rig.service.wal.wal_path(&rig.key);
        if header_only {
            fs::remove_file(&path).unwrap();
            drop(broca_wal::FileWal::open(&path).unwrap());
        } else {
            fs::write(&path, []).unwrap();
        }
        assert!(rig.service.wal.read_records(&rig.key).unwrap().is_empty());
        let logs = broca_test_support::LogCapture::new();
        let ownership = logs
            .scope_future(rig.registry(None).session_ownership_for_bind(&rig.key))
            .await;
        assert_eq!(
            ownership, None,
            "lost history cannot become fresh ownership; header-only: {header_only}"
        );
        assert!(logs
            .events()
            .iter()
            .any(|event| event.level == tracing::Level::WARN));
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn append_repairs_inferred_prior_terminal_before_refusing_new_admission_at_cap() {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    rig.seed(10, now, None);
    let mut wal = rig.service.wal.open(&rig.key).unwrap();
    wal.append(
        1,
        &WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: broca_protocol::StepId(1),
            assistant_message: broca_protocol::AssistantMessage {
                message_id: broca_protocol::MessageId("old-final".into()),
                content: vec![broca_protocol::ContentBlock::Text {
                    text: "done".into(),
                }],
            }
            .into(),
            usage: Default::default(),
            finish_reason: Some(broca_protocol::FinishReason::Stop),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        },
        true,
    )
    .unwrap();
    wal.append(
        1,
        &WalRecord::TurnFinished {
            step_id: broca_protocol::StepId(1),
        },
        true,
    )
    .unwrap();
    drop(wal);
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    let prefix_bytes = fs::metadata(&path).unwrap().len();
    let cap = prefix_bytes + broca_core::WalHandle::terminal_reserve_for_test();
    fs::write(rig.root.join("test-retained-wal-cap"), cap.to_string()).unwrap();
    let _lease = rig.service.lease(&rig.key).unwrap();
    let writer = broca_core::WalHandle::spawn(rig.service.wal.open(&rig.key).unwrap(), 1).unwrap();
    let raw: Vec<_> = rig
        .service
        .wal
        .read_records(&rig.key)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    let prior = broca_core::replay(&raw).unwrap();
    assert!(matches!(
        prior.resolution,
        broca_core::ReplayResolution::LineageIdle { .. }
    ));
    assert!(!prior.run_finished_durable);
    let mut req = request(&rig.key, "new-turn");
    req.retention = None;
    let (out, _events) = tokio::sync::mpsc::channel(128);
    let provider = broca_core::StubProvider::new(vec![]);
    let result = broca_core::run_loop_on_writer(
        req,
        &writer,
        broca_core::StartMode::AppendEpisode,
        &provider,
        &broca_core::StubToolPlane::new(),
        &broca_core::AllowAll,
        rig.service.wal.as_ref(),
        Some(rig.store.clone()),
        broca_core::RunControl {
            clock: Some(rig.service.clock.clone()),
            ..Default::default()
        },
        &out,
    )
    .await;
    assert!(
        matches!(
            result,
            Err(broca_core::RunError::RetainedSessionTooLarge { .. })
        ),
        "{result:?}"
    );
    let frames = rig.service.wal.read_records(&rig.key).unwrap();
    assert!(
        matches!(frames.last().unwrap().record, WalRecord::RunFinished { .. }),
        "inferred prior episode must be durably repaired"
    );
    assert!(!frames.iter().any(|frame| matches!(&frame.record, WalRecord::RunStarted { run_id, .. } if run_id.0 == "new-turn")));
    assert!(policy(&frames).unwrap().2.is_some());
    assert!(provider.calls().is_empty());
    assert!(fs::metadata(path).unwrap().len() <= cap);
}

#[tokio::test]
async fn cold_forever_status_uses_bounded_wal_without_opening_actor() {
    struct Count {
        inner: Arc<FileWalStore>,
        full: Arc<AtomicU64>,
    }
    impl WalStore for Count {
        fn is_retention_lineage(&self, key: &BindIdentity) -> Result<bool, broca_wal::WalError> {
            self.inner.is_retention_lineage(key)
        }
        fn open(
            &self,
            key: &BindIdentity,
        ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
            self.inner.open(key)
        }
        fn read_records(
            &self,
            key: &BindIdentity,
        ) -> Result<Vec<FramedRecord>, broca_wal::WalError> {
            self.full.fetch_add(1, Ordering::SeqCst);
            self.inner.read_records(key)
        }
        fn read_records_range(
            &self,
            key: &BindIdentity,
            after: u64,
            through: Option<u64>,
            anchor: Option<&broca_wal::WalByteAnchor>,
        ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
            self.inner.read_records_range(key, after, through, anchor)
        }
        fn live_wal_bytes(&self, key: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
            self.inner.live_wal_bytes(key)
        }
        fn flush_for_read(&self, key: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
            self.inner.flush_for_read(key)
        }
        fn file_identity(&self, key: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
            self.inner.file_identity(key)
        }
        fn lineage_exists(
            &self,
            key: &BindIdentity,
            meta: Option<&broca_core::AdmissionStoreSnapshot>,
        ) -> Result<bool, broca_wal::WalError> {
            self.inner.lineage_exists(key, meta)
        }
        fn wal_present(&self, key: &BindIdentity) -> Option<bool> {
            self.inner.wal_present(key)
        }
    }
    let rig = Rig::new();
    let mut wal = rig.service.wal.open(&rig.key).unwrap();
    wal.append(1, &started(&rig.key, "forever", None, 100), true)
        .unwrap();
    wal.append(
        1,
        &WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: broca_protocol::StepId(1),
            assistant_message: broca_protocol::AssistantMessage {
                message_id: broca_protocol::MessageId("message".into()),
                content: vec![broca_protocol::ContentBlock::Text {
                    text: "x".repeat(4096),
                }],
            }
            .into(),
            usage: Default::default(),
            finish_reason: Some(broca_protocol::FinishReason::Stop),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        },
        true,
    )
    .unwrap();
    wal.append(
        1,
        &WalRecord::TurnFinished {
            step_id: broca_protocol::StepId(1),
        },
        true,
    )
    .unwrap();
    wal.append(1, &finished(200), true).unwrap();
    drop(wal);
    rig.store
        .project(&rig.key, &rig.service.wal.read_records(&rig.key).unwrap())
        .unwrap();
    assert!(rig
        .store
        .read_admission_snapshot(&rig.key)
        .unwrap()
        .unwrap()
        .checkpoint
        .is_some());
    let full = Arc::new(AtomicU64::new(0));
    let counted = Arc::new(Count {
        inner: rig.service.wal.clone(),
        full: full.clone(),
    });
    let opener = Arc::new(
        crate::opener::DefaultOpener::new(
            rig.leases.clone(),
            counted.clone(),
            Arc::new(RealExecutor {
                reply: "done".into(),
                wal: rig.service.wal.clone(),
                store: rig.store.clone(),
            }),
        )
        .with_run_index(rig.store.clone())
        .with_materialized_store(rig.store.clone()),
    );
    let registry =
        crate::registry::LineageRegistry::new_with_reader(opener, counted, Some(rig.store.clone()));
    registry.set_retention_service(rig.service.clone());
    assert_eq!(
        registry.route_status(&rig.key).await.unwrap(),
        crate::actor::LineageStatus::Idle
    );
    assert_eq!(
        full.load(Ordering::SeqCst),
        0,
        "cold status must use bounded WAL reads rather than the full-history reader"
    );
    assert_eq!(
        registry.slot_counts().open,
        0,
        "status must not create an idle actor"
    );
    assert!(registry.stop_and_seal().await);
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn status_classifies_created_expired_retention_without_opening_actor() {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    fs::write(
        rig.root.join("test-clock"),
        serde_json::json!({"now_ms":now}).to_string(),
    )
    .unwrap();
    rig.service.clock.refresh().unwrap();
    let registry = rig.registry(None);
    let service = rig.service.clone();
    let key = rig.key.clone();
    registry.status_open_probe_for_test(Arc::new(move || {
        let _lease = service.lease(&key).unwrap();
        let mut wal = service.wal.open_with_retention(&key, Some(1)).unwrap();
        wal.append(
            1,
            &started(&key, "created-between", Some(1), now - 2000),
            true,
        )
        .unwrap();
        wal.append(1, &finished(now - 1500), true).unwrap();
        service.remember_session(&key).unwrap();
    }));
    let result = registry.route_status(&rig.key).await;
    assert!(
        matches!(result, Err(crate::registry::OpenError::Expired { expired_at_ms }) if expired_at_ms == now - 500),
        "{result:?}"
    );
    assert!(
        rig.service.wal.ephemeral_wal_path(&rig.key).exists(),
        "status must not delete the newly expired retained log"
    );
    assert!(rig.service.tombstones(&rig.key).unwrap().is_empty());
    assert_eq!(
        registry.slot_counts().open,
        0,
        "status must not create its writer actor"
    );
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn refused_append_does_not_reseal_an_already_finished_prior_episode() {
    let rig = Rig::new();
    let now = rig.service.clock.now_ms();
    rig.seed(10, now, Some(now));
    let before = rig.service.wal.read_records(&rig.key).unwrap();
    let cap = fs::metadata(rig.service.wal.ephemeral_wal_path(&rig.key))
        .unwrap()
        .len()
        + broca_core::WalHandle::terminal_reserve_for_test();
    fs::write(rig.root.join("test-retained-wal-cap"), cap.to_string()).unwrap();
    let _lease = rig.service.lease(&rig.key).unwrap();
    let writer = broca_core::WalHandle::spawn(rig.service.wal.open(&rig.key).unwrap(), 1).unwrap();
    let mut req = request(&rig.key, "unadmitted-append");
    req.retention = None;
    let (out, _events) = tokio::sync::mpsc::channel(128);
    let result = broca_core::run_loop_on_writer(
        req,
        &writer,
        broca_core::StartMode::AppendEpisode,
        &broca_core::StubProvider::new(vec![]),
        &broca_core::StubToolPlane::new(),
        &broca_core::AllowAll,
        rig.service.wal.as_ref(),
        Some(rig.store.clone()),
        broca_core::RunControl {
            clock: Some(rig.service.clock.clone()),
            ..Default::default()
        },
        &out,
    )
    .await;
    assert!(matches!(
        result,
        Err(broca_core::RunError::RetainedSessionTooLarge { .. })
    ));
    assert_eq!(
        rig.service.wal.read_records(&rig.key).unwrap(),
        before,
        "an append refusal does not own the earlier finished run"
    );
}

#[tokio::test]
async fn new_zero_record_writer_without_metadata_is_known_empty() {
    let rig = Rig::new();
    drop(rig.service.wal.open(&rig.key).unwrap());
    assert!(rig
        .store
        .read_admission_snapshot(&rig.key)
        .unwrap()
        .is_none());
    let registry = rig.registry(None);
    let logs = broca_test_support::LogCapture::new();
    assert_eq!(
        logs.scope_future(registry.session_ownership_for_bind(&rig.key))
            .await,
        Some(broca_core::SessionOwnershipV1::default())
    );
    assert!(logs.events().is_empty());
}

#[tokio::test]
async fn retained_head_reports_expiry_activity_not_mtime_or_cache_warm() {
    use crate::serve::ServeHandler;
    let rig = Rig::new();
    let activity = rig.service.clock.now_ms() + 5_000_000;
    rig.seed(RETAINED_SECONDS, activity - 1, Some(activity));
    let registry = rig.registry(None);
    assert_eq!(
        registry.head(&rig.key).await.unwrap().updated_at,
        Some(activity)
    );
    {
        let _lease = rig.service.lease(&rig.key).unwrap();
        let mut wal = rig.service.wal.open_with_retention(&rig.key, None).unwrap();
        wal.append(
            1,
            &serde_json::from_value(serde_json::json!({
                "type":"cache_warmed", "run_id":"r1", "step_id":1,
                "request_body_sha256":"hash", "warm_body_sha256":"warm", "usage":{},
                "sent_at_ms":activity + 1_000_000
            }))
            .unwrap(),
            true,
        )
        .unwrap();
    }
    assert_eq!(
        registry.head(&rig.key).await.unwrap().updated_at,
        Some(activity)
    );
    assert_eq!(
        policy(&rig.service.wal.read_records(&rig.key).unwrap()),
        Some((
            RETAINED_SECONDS,
            activity,
            Some(activity + RETAINED_SECONDS * 1000)
        ))
    );
}

#[tokio::test]
async fn a_later_retained_send_shortens_policy_in_the_same_lineage() {
    let rig = Rig::new();
    let registry = rig.registry(None);
    let first = request(&rig.key, "first-retained");
    registry
        .route_send(&rig.key, first, crate::executor::RunKind::FreshOrResume)
        .await
        .unwrap();
    wait_idle(&registry, &rig.key).await;
    let lineage = rig
        .store
        .lookup_live_retention_lineage(&rig.key)
        .unwrap()
        .unwrap()
        .lineage_id;
    let mut second = request(&rig.key, "shortened");
    second.retention = Some(RETAINED_SECONDS / 2);
    let sent = registry
        .route_send(&rig.key, second, crate::executor::RunKind::FreshOrResume)
        .await
        .unwrap();
    assert!(
        matches!(sent, crate::actor::SendOutcome::Active { .. }),
        "{sent:?}"
    );
    wait_idle(&registry, &rig.key).await;
    assert_eq!(
        rig.store
            .lookup_live_retention_lineage(&rig.key)
            .unwrap()
            .unwrap()
            .lineage_id,
        lineage
    );
    let records = rig.service.wal.read_records(&rig.key).unwrap();
    let policies: Vec<_> = records
        .iter()
        .filter_map(|frame| match &frame.record {
            WalRecord::RunStarted { retention, .. } => *retention,
            _ => None,
        })
        .collect();
    assert_eq!(policies, vec![RETAINED_SECONDS, RETAINED_SECONDS / 2]);
    assert_eq!(
        rig.store.lookup_run("shortened").unwrap().unwrap().state,
        RunIndexState::Completed
    );
    assert!(registry.stop_and_seal().await);
}

#[test]
fn migration_sidecars_prevent_retention_deletion_and_boot_enrollment() {
    let rig = Rig::new();
    rig.seed(1, 100, Some(200));
    let portable = rig.service.wal.wal_path(&rig.key);
    let retained = rig.service.wal.ephemeral_wal_path(&rig.key);
    let before = std::fs::read(&retained).unwrap();
    broca_wal::migration::freeze(
        &rig.root,
        &portable,
        &broca_wal::migration::Freeze {
            cut_id: "cut".into(),
            end_offset: before.len() as u64,
            lineage_id: "lineage".into(),
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(rig.service.enroll().unwrap(), 0);
    let _lease = rig
        .leases
        .acquire(&broca_core::lease_key_for(&rig.key))
        .unwrap();
    assert!(rig.service.delete_expired(&rig.key, 1200).is_err());
    assert!(
        rig.service.tombstones(&rig.key).unwrap().is_empty(),
        "migration protection must refuse deletion before writing a tombstone"
    );
    assert_eq!(std::fs::read(&retained).unwrap(), before);
    assert!(portable.with_extension("moving").exists());
}

#[tokio::test]
async fn sweep_handles_listed_wal_disappearance_and_enrolls_later_keys() {
    let mut rig = Rig::new();
    let mut keys = Vec::new();
    for name in ["a", "b", "c"] {
        rig.key = BindIdentity::new(rig.root.join("project"), "runner", name);
        rig.seed_run(name, 1, 100, Some(200));
        keys.push(rig.key.clone());
    }
    let removed = Arc::new(Mutex::new(None));
    *rig.service.before_enroll.lock().unwrap() = Some(Arc::new({
        let removed = removed.clone();
        move |path| {
            let mut removed = removed.lock().unwrap();
            if removed.is_none() {
                fs::remove_file(path).unwrap();
                *removed = Some(path.to_owned());
            }
        }
    }));
    assert_eq!(rig.registry(None).sweep_once(1200).await.unwrap(), 2);
    let removed = removed.lock().unwrap().clone().unwrap();
    for key in keys {
        let path = rig.service.wal.ephemeral_wal_path(&key);
        assert!(!path.exists());
        if path != removed {
            assert!(rig.service.tombstones(&key).unwrap()[0]
                .completed_at_ms
                .is_some());
        }
    }
    assert_eq!(rig.service.unreadable_sessions(), 0);
}

#[test]
fn deletion_recovery_isolates_persistent_bad_tombstone_and_evicts_good_keys() {
    let mut rig = Rig::new();
    let mut keys = Vec::new();
    for name in ["a", "b", "c"] {
        rig.key = BindIdentity::new(rig.root.join("project"), "runner", name);
        let id = rig.seed_run(name, 1, 100, Some(200));
        rig.service
            .write_tombstone(
                &rig.key,
                &Tombstone {
                    next_tag: None,
                    next_notice_seq: None,
                    lineage_id: id.clone(),
                    expired_at_ms: 1200,
                    seq: 1,
                    completed_at_ms: None,
                },
            )
            .unwrap();
        if name == "b" {
            fs::write(
                rig.service.wal.tombstone_path(&rig.key, &id),
                b"bad tombstone",
            )
            .unwrap();
        }
        keys.push(rig.key.clone());
    }
    let evicted = Arc::new(Mutex::new(Vec::new()));
    rig.service.set_evictor(Arc::new({
        let evicted = evicted.clone();
        move |key| evicted.lock().unwrap().push(key.clone())
    }));
    let completed = rig.service.resume_deletions(1200).unwrap();
    assert_eq!(completed.len(), 2);
    for key in [&keys[0], &keys[2]] {
        assert!(completed.contains(key));
        assert!(evicted.lock().unwrap().contains(key));
        assert!(!rig.service.wal.ephemeral_wal_path(key).exists());
        assert!(rig.service.tombstones(key).unwrap()[0]
            .completed_at_ms
            .is_some());
    }
    assert!(rig.service.wal.ephemeral_wal_path(&keys[1]).exists());
    assert!(!evicted.lock().unwrap().contains(&keys[1]));
    assert!(rig.service.resume_deletions(1200).unwrap().is_empty());
}

#[cfg(feature = "test-clock")]
#[test]
fn successful_export_discovery_does_not_query_fallback_before_tombstone() {
    struct RunsUnavailable(Arc<SqliteStore>);
    impl RetentionIndex for RunsUnavailable {
        fn live(&self, key: &BindIdentity) -> Result<Option<RetentionLineageRow>, String> {
            RetentionIndex::live(self.0.as_ref(), key)
        }
        fn lineage(&self, id: &str) -> Result<Option<RetentionLineageRow>, String> {
            RetentionIndex::lineage(self.0.as_ref(), id)
        }
        fn runs(&self, _: &str) -> Result<Vec<RetentionRunRow>, String> {
            Err("fallback unavailable".into())
        }
        fn candidates(&self, now: u64) -> Result<Vec<RetentionLineageRow>, String> {
            RetentionIndex::candidates(self.0.as_ref(), now)
        }
        fn run(&self, id: &str) -> Result<Option<RunRetention>, String> {
            RetentionIndex::run(self.0.as_ref(), id)
        }
        fn upsert(
            &self,
            row: &RetentionLineageRow,
            runs: &[RetentionRunRow],
        ) -> Result<(), String> {
            RetentionIndex::upsert(self.0.as_ref(), row, runs)
        }
    }
    let mut rig = Rig::new();
    let id = rig.seed(1, 100, Some(200));
    rig.service.rederive(&rig.key).unwrap();
    refuse_retention_reader(&mut rig);
    rig.service = Arc::new(
        RetentionService::new(
            rig.service.wal.clone(),
            Arc::new(RunsUnavailable(rig.store.clone())),
            rig.leases.clone(),
            rig.store.clone(),
            Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1)),
        )
        .unwrap(),
    );
    let _lease = rig.service.lease(&rig.key).unwrap();
    // Deletion completion needs indexed runs when the WAL is unreadable. But a
    // successful export-discovery query needs no fallback run-index query: an
    // unavailable fallback must not prevent the deletion tombstone from being written.
    assert!(rig.service.delete_expired(&rig.key, 1200).is_err());
    assert!(rig.service.wal.tombstone_path(&rig.key, &id).exists());
}

#[cfg(target_os = "linux")]
#[path = "retention_bench.rs"]
mod retention_bench;

#[tokio::test]
async fn steady_sweep_does_no_per_file_work() {
    let rig = Rig::new();
    rig.seed(3600, 100, Some(200));
    rig.service.rederive(&rig.key).unwrap();
    let registry = rig.registry(None);
    registry.boot_retention().await.unwrap();
    rig.service.file_work.store(0, Ordering::Relaxed);
    let walks = rig.service.directory_walks.load(Ordering::Relaxed);
    assert_eq!(registry.sweep_once(1000).await.unwrap(), 0);
    assert_eq!(rig.service.file_work.load(Ordering::Relaxed), 0);
    assert_eq!(rig.service.directory_walks.load(Ordering::Relaxed), walks);
}

#[tokio::test]
async fn raw_writer_enrolls_without_walk_and_expires_on_deadline() {
    let rig = Rig::new();
    let registry = rig.registry(None);
    registry.boot_retention().await.unwrap();
    let walks = rig.service.directory_walks.load(Ordering::Relaxed);
    let id = rig.seed(1, 100, Some(200));
    assert!(rig
        .store
        .lookup_live_retention_lineage(&rig.key)
        .unwrap()
        .is_none());
    assert_eq!(registry.sweep_once(1199).await.unwrap(), 0);
    assert_eq!(
        rig.store
            .lookup_live_retention_lineage(&rig.key)
            .unwrap()
            .unwrap()
            .lineage_id,
        id
    );
    assert_eq!(rig.service.directory_walks.load(Ordering::Relaxed), walks);
    assert!(rig.service.wal.ephemeral_wal_path(&rig.key).exists());
    assert_eq!(registry.sweep_once(1200).await.unwrap(), 1);
    assert!(!rig.service.wal.ephemeral_wal_path(&rig.key).exists());
    assert!(rig.store.retention_pending_deletions().unwrap().is_empty());
}

/// Production closes a session writer before it releases the session lease. A sweep that
/// runs between those two steps cannot enroll the changed session, because enrollment needs
/// the lease. Enrollment must still follow once the lease is released, without waiting for a
/// recovery directory walk.
#[tokio::test]
async fn sweep_between_writer_close_and_lease_release_still_enrolls_after_release() {
    let rig = Rig::new();
    let registry = rig.registry(None);
    registry.boot_retention().await.unwrap();
    let walks = rig.service.directory_walks.load(Ordering::Relaxed);
    let lease = rig.service.lease(&rig.key).unwrap();
    let (writer, lease) = broca_core::WalHandle::open_and_spawn_observed_with_retention(
        rig.service.wal.clone(),
        rig.key.clone(),
        lease,
        None,
        Some(1),
    )
    .await
    .unwrap();
    rig.service.remember_session(&rig.key).unwrap();
    writer.record(started(&rig.key, "r1", Some(1), 100));
    writer.record(finished(200));
    writer.sync().await.unwrap();
    writer.shutdown().await.unwrap();

    assert_eq!(registry.sweep_once(1199).await.unwrap(), 0);
    assert!(
        rig.store
            .lookup_live_retention_lineage(&rig.key)
            .unwrap()
            .is_none(),
        "enrollment cannot happen while the session lease is still held"
    );

    drop(lease);
    assert_eq!(registry.sweep_once(1199).await.unwrap(), 0);
    assert!(
        rig.store
            .lookup_live_retention_lineage(&rig.key)
            .unwrap()
            .is_some(),
        "releasing the lease must make the changed session eligible for the next sweep"
    );
    assert_eq!(rig.service.directory_walks.load(Ordering::Relaxed), walks);
    assert_eq!(registry.sweep_once(1200).await.unwrap(), 1);
    assert!(!rig.service.wal.ephemeral_wal_path(&rig.key).exists());
}

#[tokio::test]
async fn boot_recovers_rowless_wal_after_losing_writer_hints() {
    let rig = Rig::new();
    rig.seed(1, 100, Some(200));
    fs::remove_file(
        rig.service
            .wal
            .ephemeral_wal_path(&rig.key)
            .with_extension("session"),
    )
    .unwrap();
    let wal = Arc::new(FileWalStore::new(rig.root.join("wal")));
    assert_eq!(wal.take_retention_changes(), (vec![], false));
    let service = Arc::new(
        RetentionService::new(
            wal,
            rig.store.clone(),
            rig.leases.clone(),
            rig.store.clone(),
            Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1)),
        )
        .unwrap(),
    );
    assert_eq!(service.recover_enrollment().unwrap(), 1);
    assert!(rig
        .store
        .lookup_live_retention_lineage(&rig.key)
        .unwrap()
        .is_some());
    let _lease = service.lease(&rig.key).unwrap();
    assert!(service.delete_expired(&rig.key, 1200).unwrap().is_some());
}

#[tokio::test]
async fn boot_length_hit_skips_identity_and_move_reads() {
    let rig = Rig::new();
    rig.seed(3600, 100, Some(200));
    rig.service.rederive(&rig.key).unwrap();
    fs::remove_file(
        rig.service
            .wal
            .ephemeral_wal_path(&rig.key)
            .with_extension("session"),
    )
    .unwrap();
    rig.service.file_work.store(0, Ordering::Relaxed);
    assert_eq!(rig.service.recover_enrollment().unwrap(), 0);
    assert_eq!(
        rig.service.file_work.load(Ordering::Relaxed),
        1,
        "only the recovery directory walk"
    );
    assert!(!rig
        .service
        .wal
        .ephemeral_wal_path(&rig.key)
        .with_extension("session")
        .exists());
}

#[tokio::test]
async fn default_raw_writer_placement_also_enqueues_retention() {
    let rig = Rig::new();
    rig.service.recover_enrollment().unwrap();
    let mut wal = rig.service.wal.open(&rig.key).unwrap();
    wal.append(1, &started(&rig.key, "r1", Some(1), 100), true)
        .unwrap();
    wal.append(1, &finished(200), true).unwrap();
    drop(wal);
    let walks = rig.service.directory_walks.load(Ordering::Relaxed);
    assert_eq!(rig.service.enroll_changes().unwrap(), 1);
    assert_eq!(rig.service.directory_walks.load(Ordering::Relaxed), walks);
    assert!(rig
        .store
        .lookup_live_retention_lineage(&rig.key)
        .unwrap()
        .is_some());
}
