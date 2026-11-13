use super::*;
use broca_core::{ServedHistory, SessionRefIndex};

fn inventory(rig: &Rig, identities: &[BindIdentity]) -> broca_store::SqliteStore {
    let path = rig.dir.join("run-index.db");
    let store = broca_store::SqliteStore::open(&path).unwrap();
    for identity in identities {
        store
            .record_session(&broca_core::session_addr(identity), identity)
            .unwrap();
    }
    let connection = rusqlite::Connection::open(path).unwrap();
    connection
        .execute_batch(
            "INSERT OR REPLACE INTO agent_data_projection VALUES(1,1);
         INSERT OR IGNORE INTO agent_data_census_done VALUES(1);
         UPDATE agent_data_census SET covered=1;",
        )
        .unwrap();
    assert!(store.session_refs_named("s", 101).unwrap().complete);
    store
}

async fn converts_as(effective: BindIdentity, tag: &str, unrelated_name: bool) {
    let rig = Rig::start(tag).await;
    let records = session_records(&rig.wal());
    rig.write(&records);
    let path = rig.wal_store.wal_path(&effective);
    if effective != key() {
        std::fs::rename(rig.wal(), &path).unwrap();
    }
    let original = std::fs::read(&path).unwrap();
    let mut identities = vec![effective.clone()];
    if unrelated_name {
        identities.push(BindIdentity::new("/unrelated", "broca", "s"));
    }
    let store = inventory(&rig, &identities);
    let frames = broca_wal::replay_file(&path, u64::MAX).unwrap();
    let lineage = broca_wal::FileWal::open(&path).unwrap().lineage_id();
    let history_version = store
        .served_snapshot(
            &broca_core::session_addr(&effective),
            true,
            lineage.as_ref().map(|id| id.to_string()).as_deref(),
            None,
        )
        .unwrap()
        .history_version;
    let engine = engine(&rig, false, true);
    engine
        .run_live(&deployment(), Some(&rig.runtime), None)
        .await
        .unwrap();
    let row = engine.ledger.rows().unwrap().remove(0);
    assert_eq!(row.outcome.as_deref(), Some("converted"), "{row:?}");
    assert_eq!(std::fs::read(&row.source.backup).unwrap(), original);
    assert!(!path.exists());
    let view = rig.runtime.current_view(&effective).await.unwrap().unwrap();
    assert_eq!(
        view.head().triple,
        broca_session_convert::stage::triple(&effective)
    );
    assert_eq!(view.head().history_version, history_version);
    for run in RUNS {
        let start = view.find_run_start(run).await.unwrap().unwrap();
        let provenance = start
            .run_choices
            .as_ref()
            .and_then(|choices| choices.pointer("/record/session"));
        if effective == key() {
            assert_eq!(provenance, None);
        } else {
            assert_eq!(provenance, Some(&serde_json::to_value(key()).unwrap()));
        }
    }
    let request =
        broca_module::serve::SessionReadRequest::new(effective.clone(), Some(1), Some(100));
    let lineage_text = lineage.map(|id| id.to_string());
    let today = broca_module::store_read::todays::Todays::new(&frames, lineage_text.as_deref())
        .read(&request)
        .unwrap();
    let parts = rig
        .runtime
        .parts_for(StorageClass::Ordinary, view.head().cipher());
    let converted = broca_module::store_read::serve_view(&view, parts, &request)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(converted.messages).unwrap(),
        serde_json::to_value(today.messages).unwrap()
    );
}

#[tokio::test]
async fn an_archived_alias_is_superseded_only_after_its_canonical_session_reads() {
    alias_case("cutover-alias", false, false, "superseded_alias").await;
}

#[tokio::test]
async fn an_archived_alias_needs_an_identical_prefix_and_a_readable_canonical_head() {
    alias_case("cutover-alias-different", true, false, "refused").await;
    alias_case("cutover-alias-unreadable", false, true, "refused").await;
}

async fn alias_case(tag: &str, different: bool, missing_head: bool, outcome: &str) {
    let rig = Rig::start(tag).await;
    let mut bytes = rig.write_session();
    let canonical = broca_core::session_addr(&key());
    let engine = engine(&rig, true, true);
    engine
        .run_live(&deployment(), Some(&rig.runtime), None)
        .await
        .unwrap();
    if different {
        let other = rig.dir.join("other.wal");
        let mut wal = broca_wal::FileWal::open(&other).unwrap();
        for record in session_records(&rig.dir.join("unused.wal")) {
            wal.append(1, &record, true).unwrap();
        }
        drop(wal);
        bytes = std::fs::read(&other).unwrap();
    }
    if missing_head {
        std::fs::remove_file(broca_wal::session_storage_head(
            &rig.dir,
            StorageClass::Ordinary,
            &canonical,
        ))
        .unwrap();
    }
    let archive = rig.dir.join("wal-archive/fold-1000.ark");
    std::fs::create_dir_all(archive.parent().unwrap()).unwrap();
    let mut writer = broca_wal::archive::ArchiveWriter::create(&archive)
        .unwrap()
        .with_feature_floor(rig.dir.join(broca_wal::FEATURE_FLOOR_FILENAME));
    let alias = "0011223344556677";
    writer.append(alias, &bytes).unwrap();
    writer.seal().unwrap();
    let original = std::fs::read(&archive).unwrap();
    engine
        .run_archives(&deployment(), Some(&rig.runtime), None, Duration::ZERO)
        .await
        .unwrap();
    let row = engine
        .ledger
        .rows()
        .unwrap()
        .into_iter()
        .find(|row| row.source.member.as_deref() == Some(alias))
        .unwrap();
    assert_eq!(row.outcome.as_deref(), Some(outcome), "{row:?}");
    if outcome == "superseded_alias" {
        assert!(row.detail.as_deref().unwrap().contains(&canonical));
    }
    assert_eq!(std::fs::read(&row.source.backup).unwrap(), original);
    assert!(!broca_wal::session_storage_head(&rig.dir, StorageClass::Ordinary, alias).exists());
}

#[tokio::test]
async fn an_old_root_live_log_converts_at_its_unique_filename_identity() {
    let rig = Rig::start("cutover-old-root").await;
    rig.write(&session_records(&rig.wal()));
    let effective = BindIdentity::new("/older-root", "broca", "s");
    let moved_inventory = BindIdentity::new("/new-root", "broca", "s");
    let path = rig.wal_store.wal_path(&effective);
    std::fs::rename(rig.wal(), &path).unwrap();
    let original = std::fs::read(&path).unwrap();
    let root_witness = BindIdentity::new("/older-root", "runner", "another-session");
    let _store = inventory(&rig, &[moved_inventory.clone(), root_witness]);
    let engine = engine(&rig, false, true);
    engine
        .run_live(&deployment(), Some(&rig.runtime), None)
        .await
        .unwrap();
    let row = engine.ledger.rows().unwrap().remove(0);
    assert_eq!(row.outcome.as_deref(), Some("converted"), "{row:?}");
    assert_eq!(std::fs::read(&row.source.backup).unwrap(), original);
    let view = rig.runtime.current_view(&effective).await.unwrap().unwrap();
    assert_eq!(
        view.head().triple,
        broca_session_convert::stage::triple(&effective)
    );
    for run in RUNS {
        let start = view.find_run_start(run).await.unwrap().unwrap();
        let provenance = start
            .run_choices
            .as_ref()
            .unwrap()
            .pointer("/record/session")
            .unwrap();
        assert_eq!(
            serde_json::from_value::<BindIdentity>(provenance.clone()).unwrap(),
            key()
        );
        assert_eq!(provenance["inventory"], json!([moved_inventory]));
    }
}

#[tokio::test]
async fn harness_rekey_converts_under_inventory_identity() {
    converts_as(
        BindIdentity::new("/project", "broca", "s"),
        "cutover-harness-rekey",
        false,
    )
    .await;
}

#[tokio::test]
async fn root_and_harness_rekey_converts_under_inventory_identity() {
    converts_as(
        BindIdentity::new("/renamed-project", "broca", "s"),
        "cutover-root-harness-rekey",
        false,
    )
    .await;
}

#[tokio::test]
async fn matching_record_identity_keeps_ordinary_conversion() {
    converts_as(key(), "cutover-matching-identity", false).await;
}

#[tokio::test]
async fn a_unique_filename_match_converts_among_unrelated_inventory_names() {
    converts_as(
        BindIdentity::new("/project", "broca", "s"),
        "cutover-unrelated-name",
        true,
    )
    .await;
}

#[tokio::test]
async fn unresolved_rekeys_are_refused_by_reason_not_damage() {
    for (tag, identities, detail) in [
        ("missing", vec![], "no inventory-root candidate"),
        (
            "different",
            vec![BindIdentity::new("/other-project", "broca", "s")],
            "no inventory-root candidate",
        ),
    ] {
        let rig = Rig::start(&format!("cutover-rekey-{tag}")).await;
        let records = session_records(&rig.wal());
        rig.write(&records);
        let effective = BindIdentity::new("/project", "broca", "s");
        let path = rig.wal_store.wal_path(&effective);
        std::fs::rename(rig.wal(), &path).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let _store = inventory(&rig, &identities);
        let engine = engine(&rig, false, true);
        engine
            .run_live(&deployment(), Some(&rig.runtime), None)
            .await
            .unwrap();
        let row = engine.ledger.rows().unwrap().remove(0);
        assert_eq!(row.outcome.as_deref(), Some("refused"), "{row:?}");
        assert_eq!(
            row.refusal.as_deref(),
            Some("identity_unresolved"),
            "{row:?}"
        );
        assert!(row.detail.as_deref().unwrap().contains(detail), "{row:?}");
        assert_eq!(std::fs::read(&row.source.backup).unwrap(), bytes);
        assert!(!broca_wal::session_storage_head(
            &rig.dir,
            StorageClass::Ordinary,
            &broca_core::session_addr(&effective)
        )
        .exists());
    }
}

#[tokio::test]
async fn unresolved_live_rekey_is_deferred_unfenced_with_a_visible_reason() {
    let rig = Rig::start("cutover-rekey-deferred").await;
    rig.write(&session_records(&rig.wal()));
    let effective = BindIdentity::new("/project", "broca", "s");
    let path = rig.wal_store.wal_path(&effective);
    std::fs::rename(rig.wal(), &path).unwrap();
    let original = std::fs::read(&path).unwrap();
    let engine = engine(&rig, true, false);
    engine
        .run_archives(
            &deployment(),
            Some(&rig.runtime),
            Some(&rig.registry),
            Duration::ZERO,
        )
        .await
        .unwrap();
    let row = engine.ledger.rows().unwrap().remove(0);
    assert_eq!(row.outcome, None, "{row:?}");
    assert!(
        row.detail
            .as_deref()
            .unwrap()
            .contains("identity_unresolved: source"),
        "{row:?}"
    );
    assert_eq!(engine.health_for_test().deferred_no_identity, 1);
    assert_eq!(std::fs::read(path).unwrap(), original);
    assert!(!Path::new(&row.source.backup).exists());
}
