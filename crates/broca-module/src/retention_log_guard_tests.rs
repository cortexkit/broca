use super::*;

fn registry(rig: &Rig<broca_store::db::OverhaulDb>) -> crate::registry::LineageRegistry {
    let registry = rig.registry(None);
    registry.set_session_address_resolver(rig.store.clone());
    registry
}

#[tokio::test]
async fn overhaul_log_guard_ordinary_live_log_never_enters_the_database() {
    let rig = Rig::overhaul();
    rig.seed(1, 100, Some(200));
    let registry = registry(&rig);
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    assert!(fs::metadata(&path).unwrap().len() > broca_wal::framing::LINEAGE_FRAME_LEN as u64);
    rig.store
        .index(&broca_store::db::PublishedSession {
            bind: &rig.key,
            session_ref: &broca_core::agent_session_ref(&rig.key),
            location: broca_store::db::LogLocation::Live,
            bytes: &fs::read(&path).unwrap(),
        })
        .unwrap();
    let before = rig.store.snapshot_reads_for_conformance();
    assert!(registry.check_session_log(&rig.key).await.is_ok());
    assert_eq!(rig.store.snapshot_reads_for_conformance(), before);
    assert!(!rig.root.join("run-index.db").exists());
}

#[tokio::test]
async fn overhaul_log_guard_recorded_run_refuses_a_lineage_only_replacement() {
    use broca_store::db::{LogLocation, PublishedSession};
    let rig = Rig::overhaul();
    let old_lineage = rig.seed(1, 100, Some(200));
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    let original = fs::read(&path).unwrap();
    rig.store
        .index(&PublishedSession {
            bind: &rig.key,
            session_ref: &broca_core::agent_session_ref(&rig.key),
            location: LogLocation::Live,
            bytes: &original,
        })
        .unwrap();
    let saved = rig.root.join("historical.wal");
    fs::rename(&path, &saved).unwrap();
    let lease = rig.service.lease(&rig.key).unwrap();
    let stub = rig
        .service
        .wal
        .open_with_retention(&rig.key, Some(1))
        .unwrap();
    assert_ne!(stub.lineage_id().unwrap().to_string(), old_lineage);
    drop(stub);
    drop(lease);
    let stub_bytes = fs::read(&path).unwrap();
    assert_eq!(stub_bytes.len(), broca_wal::framing::LINEAGE_FRAME_LEN);
    let registry = registry(&rig);
    let result = registry.check_session_log(&rig.key).await;
    assert!(
        matches!(
            result,
            Err(crate::registry::OpenError::SessionLogMissing { .. })
        ),
        "{result:?}"
    );
    assert_eq!(fs::read(&path).unwrap(), stub_bytes);
    assert_eq!(fs::read(saved).unwrap(), original);
    assert!(!rig.root.join("run-index.db").exists());
}

#[tokio::test]
async fn overhaul_log_guard_brand_new_session_is_allowed_without_creating_a_log() {
    let rig = Rig::overhaul();
    broca_core::SessionRefIndex::record_session(
        rig.store.as_ref(),
        &broca_core::session_addr(&rig.key),
        &rig.key,
    )
    .unwrap();
    let registry = registry(&rig);
    let before = rig.store.snapshot_reads_for_conformance();
    assert!(registry.check_session_log(&rig.key).await.is_ok());
    assert!(rig.store.snapshot_reads_for_conformance() > before);
    assert_eq!(rig.service.wal.wal_present(&rig.key), Some(false));
    assert!(!rig.service.wal.wal_path(&rig.key).exists());
    assert!(!rig.service.wal.ephemeral_wal_path(&rig.key).exists());
    assert!(!rig.root.join("run-index.db").exists());
}

#[tokio::test]
async fn overhaul_log_guard_retired_runs_allow_a_fresh_subscriber_lineage() {
    use broca_store::db::{LogLocation, PublishedSession};
    let rig = Rig::overhaul();
    let old = rig.seed(1, 100, Some(200));
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    rig.store
        .index(&PublishedSession {
            bind: &rig.key,
            session_ref: &broca_core::agent_session_ref(&rig.key),
            location: LogLocation::Live,
            bytes: &fs::read(&path).unwrap(),
        })
        .unwrap();
    let mut row = broca_store::retention::RetentionLineageRow {
        lineage_id: old,
        session: rig.key.clone(),
        retention_s: 1,
        last_activity_ms: 200,
        expires_at_ms: None,
        wal_bytes: fs::metadata(&path).unwrap().len(),
        expired_at_ms: Some(1200),
    };
    rig.store.upsert_retention_lineage(&row, &[]).unwrap();
    fs::rename(&path, rig.root.join("retired.wal")).unwrap();
    let lease = rig.service.lease(&rig.key).unwrap();
    let fresh = rig
        .service
        .wal
        .open_with_retention(&rig.key, Some(1))
        .unwrap();
    row.lineage_id = fresh.lineage_id().unwrap().to_string();
    row.expired_at_ms = None;
    row.expires_at_ms = Some(2200);
    row.last_activity_ms = 1200;
    row.wal_bytes = broca_wal::framing::LINEAGE_FRAME_LEN as u64;
    drop(fresh);
    drop(lease);
    rig.store.upsert_retention_lineage(&row, &[]).unwrap();
    let registry = registry(&rig);
    let result = registry.check_session_log(&rig.key).await;
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(fs::metadata(path).unwrap().len(), row.wal_bytes);
}
