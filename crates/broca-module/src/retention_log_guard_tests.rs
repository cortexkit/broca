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

fn publication_guard_fixture(
    empty_publication: bool,
) -> (
    Rig<broca_store::db::OverhaulDb>,
    crate::registry::LineageRegistry,
    broca_wal::file_wal::DurablePublication,
) {
    use broca_store::db::LogLocation;
    let rig = Rig::overhaul();
    rig.seed(1, 100, Some(200));
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    let lease = rig.service.lease(&rig.key).unwrap();
    let writer = rig
        .service
        .wal
        .open_with_retention(&rig.key, Some(1))
        .unwrap();
    writer.sync().unwrap();
    let original = writer.durable_publication().unwrap();
    rig.store
        .publish(
            &rig.key,
            &broca_core::agent_session_ref(&rig.key),
            LogLocation::Live,
            &original,
        )
        .unwrap();
    drop(writer);
    drop(lease);
    fs::rename(&path, rig.root.join("publication-original.wal")).unwrap();
    let publication = if empty_publication {
        let lease = rig.service.lease(&rig.key).unwrap();
        let writer = rig
            .service
            .wal
            .open_with_retention(&rig.key, Some(1))
            .unwrap();
        writer.sync().unwrap();
        let publication = writer.durable_publication().unwrap();
        drop(writer);
        drop(lease);
        assert_eq!(publication.sequence(), 0);
        publication
    } else {
        assert!(original.sequence() > 0);
        original
    };
    let expected_publication = publication.clone();
    let bundle = Arc::new(crate::registry::RawPageTestBundle {
        db: rig.store.clone(),
        publications: std::sync::Mutex::new(std::collections::HashMap::from([(
            rig.key.clone(),
            publication,
        )])),
    });
    let registry = registry(&rig)
        .with_raw_pages_for_test(bundle.clone())
        .with_episode_source(Arc::new(crate::registry::EpisodeReadSource::new(
            rig.store.clone(),
            Arc::new(move |key, _| {
                bundle
                    .publications
                    .lock()
                    .unwrap()
                    .get(key)
                    .cloned()
                    .ok_or_else(|| "publication missing".into())
            }),
        )));
    (rig, registry, expected_publication)
}

#[tokio::test]
async fn overhaul_empty_publication_refuses_before_rebuilding_historical_rows() {
    for model in [false, true] {
        let (rig, registry, _) = publication_guard_fixture(true);
        let before = rig
            .store
            .read_snapshot(|tx| {
                Ok(tx.query_row("SELECT count(*) FROM run", [], |row| row.get::<_, u64>(0))?)
            })
            .unwrap();
        assert_eq!(before, 1);
        let result = if model {
            registry
                .route_read_model_page(&rig.key, &Default::default())
                .await
                .map(|_| ())
        } else {
            registry
                .route_read_page(&rig.key, None, None, false)
                .await
                .map(|_| ())
        };
        assert!(
            matches!(
                result,
                Err(crate::registry::OpenError::SessionLogMissing { .. })
            ),
            "{result:?}"
        );
        let after = rig
            .store
            .read_snapshot(|tx| {
                Ok(tx.query_row("SELECT count(*) FROM run", [], |row| row.get::<_, u64>(0))?)
            })
            .unwrap();
        assert_eq!(
            after, before,
            "a refused read must not erase historical run evidence"
        );
    }
}

#[tokio::test]
async fn overhaul_nonempty_publication_keeps_its_original_missing_source_refusal() {
    let (rig, registry, publication) = publication_guard_fixture(false);
    assert!(!rig.service.wal.ephemeral_wal_path(&rig.key).exists());
    let expected = rig
        .store
        .read_raw_page(
            &rig.key,
            &broca_core::agent_session_ref(&rig.key),
            broca_store::db::LogLocation::Live,
            &publication,
            broca_store::db::RawPageRequest {
                from_ordinal: None,
                limit: None,
                include_tools: false,
            },
        )
        .err()
        .expect("the original source read must fail")
        .to_string();
    let raw = registry
        .route_read_page(&rig.key, None, None, false)
        .await
        .err()
        .expect("the routed source read must fail");
    assert!(
        matches!(raw, crate::registry::OpenError::Open(ref reason) if reason == &expected),
        "{raw:?}"
    );
    let expected = rig
        .store
        .episode_reader(rig.key.clone(), publication)
        .map_err(|error| error.to_string())
        .and_then(|reader| {
            reader
                .read_model_page(&Default::default())
                .map_err(|error| error.to_string())
        })
        .unwrap_err();
    let model = registry
        .route_read_model_page(&rig.key, &Default::default())
        .await
        .unwrap_err();
    assert!(
        matches!(model, crate::registry::OpenError::Open(ref reason) if reason == &expected),
        "{model:?}"
    );
    assert!(!rig.service.wal.ephemeral_wal_path(&rig.key).exists());
}
