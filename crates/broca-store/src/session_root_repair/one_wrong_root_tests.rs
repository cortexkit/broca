use super::*;

fn one_wrong_root_fixture(with_stub: bool) -> Fixture {
    use broca_core::SessionRefIndex;
    let fixture = Fixture::new();
    fixture.seed_orphaned_projection(false);
    let wal = broca_core::FileWalStore::new(fixture.root.join("wal"));
    let lineage = wal
        .read_records_range(&fixture.live, 0, None, None)
        .unwrap()
        .lineage_id
        .unwrap()
        .to_string();
    {
        let conn = fixture.store.conn.lock().unwrap();
        conn.execute("INSERT INTO agent_data_writer VALUES(1)", [])
            .unwrap();
        conn.execute(
            "UPDATE run_index SET session=?1,lineage_id=?2",
            params![serde_json::to_string(&fixture.stale).unwrap(), lineage],
        )
        .unwrap();
        conn.execute(
            "DELETE FROM agent_data_session_ref WHERE address=?1",
            [session_addr(&fixture.live)],
        )
        .unwrap();
        conn.execute(
            "DELETE FROM agent_data_session_ref_v2 WHERE address=?1",
            [session_addr(&fixture.live)],
        )
        .unwrap();
        conn.execute("DELETE FROM agent_data_writer", []).unwrap();
    }
    let hint = BindIdentity::new(
        &fixture.live.project_root,
        &fixture.live.harness,
        "root-hint",
    );
    fixture
        .store
        .record_session(&session_addr(&hint), &hint)
        .unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(fixture.wal_path())
        .unwrap()
        .set_times(
            std::fs::FileTimes::new()
                .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(10)),
        )
        .unwrap();
    if with_stub {
        drop(wal.open(&fixture.stale).unwrap());
    }
    fixture
}

#[test]
fn one_wrong_root_repoints_history_and_preserves_stub_aside_idempotently() {
    for stub in [false, true] {
        let fixture = one_wrong_root_fixture(stub);
        let wal = broca_core::FileWalStore::new(fixture.root.join("wal"));
        let real = std::fs::read(fixture.wal_path()).unwrap();
        let old_bytes = std::fs::read(wal.wal_path(&fixture.stale)).ok();
        let before = fixture.snapshot();
        let plan = fixture.preview().unwrap();
        assert_eq!(plan.repoint_runs.len(), 24);
        assert!(plan.add_surviving_ref.is_some());
        assert_eq!(plan.move_stub_aside.is_some(), stub);
        assert_eq!(fixture.snapshot(), before);
        if let Some(aside) = &plan.move_stub_aside {
            assert!(!aside.to.exists());
        }
        fixture.store.apply_session_root_repair(&plan).unwrap();
        assert!(!wal.wal_path(&fixture.stale).exists());
        if let Some(aside) = &plan.move_stub_aside {
            assert_eq!(std::fs::read(&aside.to).unwrap(), old_bytes.unwrap());
        }
        assert_eq!(std::fs::read(fixture.wal_path()).unwrap(), real);
        assert_eq!(
            fixture
                .store
                .resolve_session_address(&fixture.stale, None, None)
                .unwrap(),
            fixture.live
        );
        assert_eq!(fixture.store.audit_session_refs(|_| Ok(())).unwrap(), 0);
        let after = fixture.snapshot();
        let repeated = fixture.preview().unwrap();
        assert!(repeated.stale_identity.is_none());
        assert!(repeated.move_stub_aside.is_none());
        fixture.store.apply_session_root_repair(&repeated).unwrap();
        assert_eq!(fixture.snapshot(), after);
        assert_eq!(std::fs::read(fixture.wal_path()).unwrap(), real);
    }
}

#[test]
fn one_wrong_root_stub_with_any_record_refuses_unchanged() {
    let fixture = one_wrong_root_fixture(true);
    let wal = broca_core::FileWalStore::new(fixture.root.join("wal"));
    let mut stub = wal.open(&fixture.stale).unwrap();
    stub.append(
        1,
        &WalRecord::RunFinished {
            interrupt_reason: None,
            reason: RunFinishReason::Cancelled,
            ts_ms: None,
            usage: Default::default(),
            error: None,
            retries_used: 0,
        },
        true,
    )
    .unwrap();
    drop(stub);
    let before = std::fs::read(wal.wal_path(&fixture.stale)).unwrap();
    fixture.refuses_unchanged("stub_not_lineage_only");
    assert_eq!(std::fs::read(wal.wal_path(&fixture.stale)).unwrap(), before);
    assert!(!fixture.root.join("wal/repair-aside").exists());
}

#[test]
fn one_wrong_root_stub_older_than_real_last_write_refuses_unchanged() {
    let fixture = one_wrong_root_fixture(true);
    std::fs::OpenOptions::new()
        .write(true)
        .open(fixture.wal_path())
        .unwrap()
        .set_times(
            std::fs::FileTimes::new()
                .set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(3600)),
        )
        .unwrap();
    fixture.refuses_unchanged("stub_not_newer_than_real_log");
    assert!(!fixture.root.join("wal/repair-aside").exists());
}

#[test]
fn one_wrong_root_lineage_mismatch_refuses_unchanged() {
    let fixture = one_wrong_root_fixture(true);
    let unrelated = broca_core::FileWalStore::new(fixture.root.join("wal"));
    let other = BindIdentity::new("/unrelated", "broca", "unrelated");
    let other_id = unrelated
        .open(&other)
        .unwrap()
        .lineage_id()
        .unwrap()
        .to_string();
    fixture
        .store
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE run_index SET lineage_id=?1 WHERE run_id='run-00'",
            [other_id],
        )
        .unwrap();
    fixture.refuses_unchanged("real_log_lineage_mismatch");
}

#[test]
fn one_wrong_root_copied_stub_with_reset_creation_time_refuses_unchanged() {
    let fixture = one_wrong_root_fixture(true);
    let wal = broca_core::FileWalStore::new(fixture.root.join("wal"));
    std::fs::OpenOptions::new()
        .write(true)
        .open(wal.wal_path(&fixture.stale))
        .unwrap()
        .set_times(
            std::fs::FileTimes::new()
                .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1)),
        )
        .unwrap();
    fixture.refuses_unchanged("stub_creation_time_unreliable");
    assert!(wal.wal_path(&fixture.stale).exists());
    assert!(!fixture.root.join("wal/repair-aside").exists());
}

#[test]
fn one_wrong_root_aside_destination_is_never_overwritten() {
    let fixture = one_wrong_root_fixture(true);
    let plan = fixture.preview().unwrap();
    let aside = plan.move_stub_aside.unwrap();
    std::fs::create_dir_all(aside.to.parent().unwrap()).unwrap();
    std::fs::copy(&aside.from, &aside.to).unwrap();
    let bytes = std::fs::read(&aside.to).unwrap();
    fixture.refuses_unchanged("repair_aside_exists");
    assert_eq!(std::fs::read(&aside.to).unwrap(), bytes);
    assert_eq!(std::fs::read(&aside.from).unwrap(), bytes);
}

#[test]
fn one_wrong_root_preview_rechecks_last_write_proof_before_moving() {
    let fixture = one_wrong_root_fixture(true);
    let plan = fixture.preview().unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(fixture.wal_path())
        .unwrap()
        .set_times(
            std::fs::FileTimes::new()
                .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(20)),
        )
        .unwrap();
    let before = fixture.snapshot();
    let error = fixture
        .store
        .apply_session_root_repair(&plan)
        .unwrap_err()
        .to_string();
    assert!(error.contains("preview changed"), "{error}");
    assert_eq!(fixture.snapshot(), before);
    assert!(plan.move_stub_aside.as_ref().unwrap().from.exists());
    assert!(!fixture.root.join("wal/repair-aside").exists());
}
