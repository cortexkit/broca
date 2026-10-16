use super::*;

fn restart(rig: &mut Rig) {
    rig.wal = Arc::new(FileWalStore::new(rig.root.join("wal")));
    rig.service = Arc::new(AgentDataService::new(
        rig.root.clone(),
        rig.registry.as_ref().clone(),
        rig.wal.clone(),
        rig.index.clone(),
        rig.door.clone(),
    ));
}

fn reference(session: &BindIdentity) -> String {
    contract::session_ref(
        &session.harness,
        &session.project_root.to_string_lossy(),
        &session.session,
    )
}

fn cached_refs(rig: &Rig, session: &BindIdentity) -> usize {
    rig.wal
        .move_session_refs(std::iter::once(reference(session).as_str()))
        .sessions
        .len()
}

#[tokio::test]
async fn second_boot_reuses_unchanged_wals_without_payload_reads() {
    let mut rig = Rig::new("persistent-inventory-reuse", Arc::new(FakeDoor::default()));
    for i in 0..32 {
        rig.seed(
            &rig.session(&format!("session-{i}")),
            &format!("run-{i}"),
            true,
        );
    }
    restart(&mut rig);
    rig.service.wait_inventory().await;
    let first = rig.wal.move_inventory_build_stats();
    assert_eq!(first.payload_reads, 32);
    assert!(first.payload_bytes > 0);
    let cache_path = rig.root.join("move-inventory.json");
    assert!(cache_path.exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&cache_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    restart(&mut rig);
    let incomplete = rig.wal.move_session_refs(std::iter::once(
        reference(&rig.session("session-0")).as_str(),
    ));
    assert!(!incomplete.unreadable.is_empty());
    rig.service.wait_inventory().await;
    let second = rig.wal.move_inventory_build_stats();
    assert_eq!(
        second.payload_reads, 0,
        "unchanged WALs must not be decoded at every restart"
    );
    assert_eq!(second.payload_bytes, 0);
    assert_eq!(second.reused, 32);
    assert_eq!(rig.wal.move_read_count(), 0);
    assert_eq!(cached_refs(&rig, &rig.session("session-0")), 1);
}

#[tokio::test]
#[cfg(unix)]
async fn cache_persists_raw_device_and_inode() {
    use std::os::unix::fs::MetadataExt;
    let mut rig = Rig::new("persistent-raw-identity", Arc::new(FakeDoor::default()));
    let session = rig.session("present");
    rig.seed(&session, "run", true);
    restart(&mut rig);
    rig.service.wait_inventory().await;
    let metadata = std::fs::metadata(rig.wal.wal_path(&session)).unwrap();
    let cache: Value =
        serde_json::from_slice(&std::fs::read(rig.root.join("move-inventory.json")).unwrap())
            .unwrap();
    assert_eq!(
        cache["addresses"][broca_core::session_addr(&session)]["source"]["stamp"]["file_id"],
        json!([metadata.dev(), metadata.ino()])
    );
}

#[tokio::test]
#[cfg(unix)]
async fn grown_wal_scans_only_from_its_cached_committed_end() {
    let mut rig = Rig::new("persistent-inventory-growth", Arc::new(FakeDoor::default()));
    let current = rig.session("current");
    let historical = rig.session("historical");
    let mut wal = rig.wal.open(&current).unwrap();
    wal.append(1, &started(&historical, "old-run"), true)
        .unwrap();
    restart(&mut rig);
    rig.service.wait_inventory().await;
    assert_eq!(cached_refs(&rig, &current), 0);
    let before = std::fs::metadata(rig.wal.wal_path(&current)).unwrap().len();
    wal.append(1, &started(&current, "new-run"), true).unwrap();
    let after = std::fs::metadata(rig.wal.wal_path(&current)).unwrap().len();
    drop(wal);
    restart(&mut rig);
    rig.service.wait_inventory().await;
    let stats = rig.wal.move_inventory_build_stats();
    assert_eq!(stats.payload_reads, 1);
    assert_eq!(stats.suffixes, 1);
    assert_eq!(
        stats.payload_bytes,
        after - before,
        "do not reread the historical prefix"
    );
    assert_eq!(cached_refs(&rig, &current), 1);
    assert_eq!(cached_refs(&rig, &historical), 0);
}

#[tokio::test]
#[cfg(unix)]
async fn appending_writer_during_cold_build_finishes_complete() {
    let mut rig = Rig::new("persistent-growing-build", Arc::new(FakeDoor::default()));
    for i in 0..64 {
        rig.seed(&rig.session(&format!("small-{i}")), "run", true);
    }
    let session = rig.session("active");
    rig.seed(&session, "active-run", false);
    let record = WalRecord::RunFinished {
        reason: broca_protocol::RunFinishReason::Completed,
        ts_ms: None,
        usage: Default::default(),
        error: None,
        retries_used: 0,
    };
    let mut wal = rig.wal.open(&session).unwrap();
    for _ in 0..20_000 {
        wal.append(1, &record, false).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);
    restart(&mut rig);
    let mut wal = rig.wal.open(&session).unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_stop = stop.clone();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        wal.append(1, &record, false).unwrap();
        ready_tx.send(()).unwrap();
        let mut appends = 1;
        while !writer_stop.load(Ordering::Relaxed) {
            wal.append(1, &record, false).unwrap();
            appends += 1;
            std::thread::park_timeout(std::time::Duration::from_millis(1));
        }
        appends
    });
    ready_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        rig.service.wait_inventory(),
    )
    .await;
    stop.store(true, Ordering::Relaxed);
    let appends = writer.join().unwrap();
    result.expect("cold inventory must finish while the writer keeps appending");
    assert!(appends > 1);
    assert_eq!(rig.wal.move_inventory_health(), (true, 0));
    assert_eq!(cached_refs(&rig, &session), 1);
}

#[tokio::test]
async fn replaced_wal_lineage_is_rescanned_instead_of_reusing_identities() {
    let mut rig = Rig::new(
        "persistent-inventory-replacement",
        Arc::new(FakeDoor::default()),
    );
    let session = rig.session("current");
    rig.seed(&session, "original-run", false);
    restart(&mut rig);
    rig.service.wait_inventory().await;
    assert_eq!(cached_refs(&rig, &session), 1);
    let path = rig.wal.wal_path(&session);
    let before = std::fs::metadata(&path).unwrap();
    let replacement = rig.root.join("replacement.wal");
    let mut wal = broca_wal::FileWal::open(&replacement).unwrap();
    broca_wal::Wal::append(
        &mut wal,
        1,
        &started(&rig.session("history"), "original-run"),
        true,
    )
    .unwrap();
    drop(wal);
    assert_eq!(std::fs::metadata(&replacement).unwrap().len(), before.len());
    // Preserve inode, length and timestamp deliberately. Only the embedded
    // lineage distinguishes this replacement from the cached contents.
    std::fs::copy(&replacement, &path).unwrap();
    std::fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(before.modified().unwrap()))
        .unwrap();
    std::fs::remove_file(replacement).unwrap();
    restart(&mut rig);
    rig.service.wait_inventory().await;
    assert_eq!(rig.wal.move_inventory_build_stats().payload_reads, 1);
    assert_eq!(
        cached_refs(&rig, &session),
        0,
        "a replacement does not inherit the old triples"
    );
}

#[tokio::test]
async fn corrupt_cache_rebuilds_from_wals_and_finds_the_session() {
    let mut rig = Rig::new(
        "persistent-inventory-corrupt",
        Arc::new(FakeDoor::default()),
    );
    let session = rig.session("present");
    rig.seed(&session, "run", true);
    rig.service.wait_inventory().await;
    std::fs::write(rig.root.join("move-inventory.json"), b"{corrupt cache").unwrap();
    restart(&mut rig);
    rig.service.wait_inventory().await;
    assert_eq!(rig.wal.move_inventory_build_stats().payload_reads, 1);
    assert_eq!(cached_refs(&rig, &session), 1);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("busy", json!({"refs":rig.params(CUT, &[session])["refs"]}))
            .await
            .unwrap()["refs"][0]["state"],
        "idle"
    );
}

#[tokio::test]
async fn unknown_cache_version_rebuilds_instead_of_refusing_inventory() {
    let mut rig = Rig::new(
        "persistent-inventory-version",
        Arc::new(FakeDoor::default()),
    );
    let session = rig.session("present");
    rig.seed(&session, "run", true);
    rig.service.wait_inventory().await;
    let path = rig.root.join("move-inventory.json");
    let mut cache: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    cache["version"] = json!(999);
    std::fs::write(path, serde_json::to_vec(&cache).unwrap()).unwrap();
    restart(&mut rig);
    rig.service.wait_inventory().await;
    assert_eq!(rig.wal.move_inventory_build_stats().payload_reads, 1);
    assert_eq!(cached_refs(&rig, &session), 1);
    assert!(rig
        .wal
        .move_session_refs(std::iter::once(reference(&session).as_str()))
        .unreadable
        .is_empty());
}

#[tokio::test]
async fn cached_wal_that_becomes_unreadable_cannot_prove_absence() {
    let mut rig = Rig::new(
        "persistent-inventory-unreadable",
        Arc::new(FakeDoor::default()),
    );
    let session = rig.session("unreadable");
    rig.seed(&session, "run", true);
    rig.service.wait_inventory().await;
    unreadable_wal(&rig, &session);
    restart(&mut rig);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("diverged", rig.params(CUT, &[session]))
            .await
            .unwrap()["refs"][0]["state"],
        "unknown"
    );
    wire.send(0, 0, 12, FrameType::Request, json!({"op":"health.check"}))
        .await;
    let health: Value = serde_json::from_slice(&wire.next().await.body).unwrap();
    assert_eq!(
        health["metrics"]["agent_data_inventory"],
        json!({"ready":true,"unreadable":1})
    );
}

#[tokio::test]
async fn stale_cached_length_is_detected_and_rescanned() {
    let mut rig = Rig::new("persistent-inventory-stale", Arc::new(FakeDoor::default()));
    let session = rig.session("present");
    rig.seed(&session, "run", true);
    rig.service.wait_inventory().await;
    let path = rig.root.join("move-inventory.json");
    let mut cache: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let entry = &mut cache["addresses"][broca_core::session_addr(&session)];
    entry["committed_end"] = json!(entry["committed_end"].as_u64().unwrap() + 17);
    // Recompute the envelope checksum: this exercises WAL validation, not merely
    // malformed-JSON/checksum fallback. Empty triples would falsely prove absence.
    entry["triples"] = json!([]);
    rewrite_cache(&path, cache);
    restart(&mut rig);
    rig.service.wait_inventory().await;
    assert_eq!(rig.wal.move_inventory_build_stats().payload_reads, 1);
    assert_eq!(cached_refs(&rig, &session), 1);
}

fn rewrite_cache(path: &std::path::Path, mut cache: Value) {
    let addresses = serde_json::to_vec(&cache["addresses"]).unwrap();
    cache["checksum"] = json!(broca_wal::migration::digest(&addresses));
    std::fs::write(path, serde_json::to_vec(&cache).unwrap()).unwrap();
}

#[tokio::test]
async fn reader_feature_changes_invalidate_only_dependent_entries() {
    let mut rig = Rig::new("persistent-reader-features", Arc::new(FakeDoor::default()));
    let a = rig.session("a");
    let b = rig.session("b");
    rig.seed(&a, "a-run", true);
    rig.seed(&b, "b-run", true);
    restart(&mut rig);
    rig.service.wait_inventory().await;
    let path = rig.root.join("move-inventory.json");
    let mut cache: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    cache["reader_features"] = json!(["future-reader/v99"]);
    rewrite_cache(&path, cache);
    restart(&mut rig);
    rig.service.wait_inventory().await;
    assert_eq!(
        rig.wal.move_inventory_build_stats().payload_reads,
        0,
        "capability-table changes alone must not cause a cold boot"
    );
    let mut cache: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let entry = &mut cache["addresses"][broca_core::session_addr(&a)];
    entry["used_features"] = json!(["future-frame/v99"]);
    entry["triples"] = json!([]);
    rewrite_cache(&path, cache);
    restart(&mut rig);
    rig.service.wait_inventory().await;
    assert_eq!(rig.wal.move_inventory_build_stats().payload_reads, 1);
    assert_eq!(rig.wal.move_inventory_build_stats().reused, 1);
    assert_eq!(cached_refs(&rig, &a), 1);
    assert_eq!(cached_refs(&rig, &b), 1);
}

#[tokio::test]
async fn cache_records_explicit_frame_requirements_not_just_record_features() {
    let mut rig = Rig::new("persistent-used-features", Arc::new(FakeDoor::default()));
    let session = rig.session("gated");
    let record = started(&session, "run");
    let frame =
        broca_wal::framing::encode_with_requires(1, 1, &["compaction/v1"], &record).unwrap();
    let mut wal = rig.wal.open(&session).unwrap();
    wal.append_preencoded(1, &record, true, frame).unwrap();
    restart(&mut rig);
    rig.service.wait_inventory().await;
    let path = rig.root.join("move-inventory.json");
    let cache: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(
        cache["addresses"][broca_core::session_addr(&session)]["used_features"],
        json!(["compaction/v1"])
    );
    wal.append(1, &record, true).unwrap();
    drop(wal);
    restart(&mut rig);
    rig.service.wait_inventory().await;
    let cache: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(
        cache["addresses"][broca_core::session_addr(&session)]["used_features"],
        json!(["compaction/v1"]),
        "suffix reads must retain the prefix's declared requirements"
    );
}

#[tokio::test]
#[cfg(unix)]
async fn legacy_wal_without_lineage_reuses_unchanged_identity() {
    let mut rig = Rig::new("persistent-legacy", Arc::new(FakeDoor::default()));
    let session = rig.session("legacy");
    std::fs::create_dir_all(rig.root.join("wal")).unwrap();
    let bytes = broca_wal::framing::encode(1, 1, &started(&session, "legacy-run")).unwrap();
    std::fs::write(rig.wal.wal_path(&session), bytes).unwrap();
    restart(&mut rig);
    rig.service.wait_inventory().await;
    assert_eq!(rig.wal.move_inventory_build_stats().payload_reads, 1);
    assert_eq!(cached_refs(&rig, &session), 1);
    restart(&mut rig);
    rig.service.wait_inventory().await;
    assert_eq!(rig.wal.move_inventory_build_stats().payload_reads, 0);
    assert_eq!(rig.wal.move_inventory_build_stats().reused, 1);
    assert_eq!(cached_refs(&rig, &session), 1);
}

#[tokio::test]
async fn cache_does_not_define_the_complete_address_inventory() {
    let mut rig = Rig::new(
        "persistent-inventory-not-authority",
        Arc::new(FakeDoor::default()),
    );
    rig.service.wait_inventory().await;
    let session = rig.session("created-between-processes");
    rig.seed(&session, "new-run", true);
    restart(&mut rig);
    rig.service.wait_inventory().await;
    assert_eq!(
        cached_refs(&rig, &session),
        1,
        "an address absent from the cache still exists on disk"
    );
    assert!(rig
        .wal
        .move_session_refs(std::iter::once(reference(&session).as_str()))
        .unreadable
        .is_empty());
}

#[tokio::test]
async fn second_boot_reuses_archive_index_identity_without_payload_reads() {
    let mut rig = Rig::new(
        "persistent-inventory-archive",
        Arc::new(FakeDoor::default()),
    );
    let session = rig.session("archived");
    rig.seed(&session, "run", true);
    let directory = rig.root.join("wal-archive");
    std::fs::create_dir_all(&directory).unwrap();
    let mut archive = broca_wal::archive::ArchiveWriter::create(directory.join("fold-1.ark"))
        .unwrap()
        .without_feature_floor();
    archive
        .append(&broca_core::session_addr(&session), &rig.bytes(&session))
        .unwrap();
    archive.seal().unwrap();
    std::fs::remove_file(rig.wal.wal_path(&session)).unwrap();
    restart(&mut rig);
    rig.service.wait_inventory().await;
    assert_eq!(rig.wal.move_inventory_build_stats().payload_reads, 1);
    restart(&mut rig);
    rig.service.wait_inventory().await;
    assert_eq!(rig.wal.move_inventory_build_stats().payload_reads, 0);
    assert_eq!(rig.wal.move_inventory_build_stats().reused, 1);
    assert_eq!(cached_refs(&rig, &session), 1);
}

#[tokio::test]
#[ignore = "synthetic 3000-session cold/warm restart timing experiment"]
async fn persistent_inventory_thousands_session_timing() {
    let mut rig = Rig::new("persistent-inventory-timing", Arc::new(FakeDoor::default()));
    let mut wal_bytes = 0;
    for i in 0..3000 {
        let session = rig.session(&format!("session-{i}"));
        rig.seed(&session, &format!("run-{i}"), true);
        wal_bytes += std::fs::metadata(rig.wal.wal_path(&session)).unwrap().len();
    }
    restart(&mut rig);
    rig.service.wait_inventory().await;
    let first = rig.wal.move_inventory_build_stats();
    assert_eq!(first.payload_reads, 3000);
    assert_eq!(first.payload_bytes, wal_bytes);
    restart(&mut rig);
    rig.service.wait_inventory().await;
    let second = rig.wal.move_inventory_build_stats();
    assert_eq!(second.payload_reads, 0);
    assert_eq!(second.payload_bytes, 0);
    assert_eq!(second.reused, 3000);
    eprintln!("persistent inventory: sessions=3000 wal_bytes={wal_bytes} cold_ms={:.3} cold_payload_bytes={} cold_lineage_bytes={} cold_cache_bytes={} warm_ms={:.3} warm_payload_bytes={} warm_lineage_bytes={} warm_cache_bytes={} warm_archive_metadata_bytes={} warm_reused={}",
        first.elapsed_us as f64 / 1000.0, first.payload_bytes, first.lineage_bytes,
        first.cache_bytes, second.elapsed_us as f64 / 1000.0, second.payload_bytes, second.lineage_bytes,
        second.cache_bytes, second.archive_metadata_bytes, second.reused);
}
