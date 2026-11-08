use super::*;
use broca_core::SessionRefIndex;

#[path = "legacy_projection_tests.rs"]
mod projections;

struct LegacyFork {
    root: PathBuf,
    store: SqliteStore,
    stale: BindIdentity,
    live: BindIdentity,
    records: Vec<WalRecord>,
}

impl LegacyFork {
    fn new(real_log: bool) -> Self {
        let root = broca_test_support::scratch_root().join(format!(
            "legacy-root-fork-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(root.join("wal")).unwrap();
        let store = SqliteStore::open(root.join("run-index.db")).unwrap();
        let stale = BindIdentity::new(
            "/Users/ufukaltinok/Work/Projects/CortexKit/brocatui",
            "broca",
            "cktui-foreign-a",
        );
        let live = BindIdentity::new(
            "/Users/ufukaltinok/Work/Projects/CortexKit/alfonso-tui",
            "broca",
            "cktui-foreign-a",
        );
        assert_eq!(session_addr(&stale), "9bd641331b207d91");
        assert_eq!(session_addr(&live), "0b8cfb928f02c715");
        let mut records = Vec::new();
        for n in 0..3 {
            let run = format!("legacy-{n}");
            records.push(
                crate::conformance::run_started(
                    0,
                    &run,
                    &stale,
                    "legacy input",
                    ModelHandle::new("stub", "model"),
                )
                .record,
            );
            records.extend(
                crate::conformance::text_turn(0, 1, "legacy answer")
                    .into_iter()
                    .map(|frame| frame.record),
            );
            records.push(WalRecord::RunFinished {
                interrupt_reason: None,
                reason: RunFinishReason::Completed,
                ts_ms: Some(2),
                usage: Default::default(),
                error: None,
                retries_used: 0,
            });
            store
                .index_run_active(&RunIndexRow {
                    run_id: run,
                    session: stale.clone(),
                    submission_id: None,
                    request_identity_digest: None,
                    state: RunIndexState::Completed,
                    usage: None,
                    final_step_finish_reason: None,
                    retries_used: 0,
                    title: None,
                    withdrawals_owed: false,
                })
                .unwrap();
        }
        let fixture = Self {
            root,
            store,
            stale,
            live,
            records,
        };
        fixture.hint(&fixture.live);
        if real_log {
            fixture.write(&fixture.live, 3);
        }
        let counts = session_refs::migrate(
            &fixture.root.join("run-index.db"),
            &ReadOnlyWalReader::new(&fixture.root),
        )
        .unwrap();
        assert!(counts.complete);
        let wal = broca_core::FileWalStore::new(fixture.root.join("wal"));
        drop(wal.open(&fixture.stale).unwrap());
        assert_eq!(
            std::fs::metadata(wal.wal_path(&fixture.stale))
                .unwrap()
                .len(),
            69
        );
        assert_eq!(
            fixture
                .store
                .conn
                .lock()
                .unwrap()
                .query_row(
                    "SELECT count(*) FROM run_index WHERE lineage_id IS NULL",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            3
        );
        fixture
    }

    fn hint(&self, identity: &BindIdentity) {
        let hint = BindIdentity::new(
            &identity.project_root,
            &identity.harness,
            format!("hint-{}", session_addr(identity)),
        );
        self.store
            .record_session(&session_addr(&hint), &hint)
            .unwrap();
    }

    fn write(&self, identity: &BindIdentity, runs: usize) {
        let path = self
            .root
            .join("wal")
            .join(format!("{}.wal", session_addr(identity)));
        crate::conformance::write_legacy_filewal(&path, &self.records[..runs * 4]);
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_times(
                std::fs::FileTimes::new()
                    .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(10)),
            )
            .unwrap();
    }

    fn findings(&self) -> Vec<serde_json::Value> {
        let mut findings = Vec::new();
        self.store
            .audit_session_refs(|finding| {
                findings.push(serde_json::to_value(finding)?);
                Ok(())
            })
            .unwrap();
        findings
    }

    fn preview(&self) -> Result<SessionRootRepair> {
        self.store
            .preview_session_root_repair(&self.stale.harness, &self.stale.session)
    }
}

#[test]
fn legacy_run_ids_find_and_repair_a_lineage_only_fork_without_lineage_evidence() {
    let fixture = LegacyFork::new(true);
    let wal = broca_core::FileWalStore::new(fixture.root.join("wal"));
    assert!(wal
        .read_records_range(&fixture.live, 0, None, None)
        .unwrap()
        .lineage_id
        .is_none());
    let before_real = std::fs::read(wal.wal_path(&fixture.live)).unwrap();
    let before_stub = std::fs::read(wal.wal_path(&fixture.stale)).unwrap();
    let findings = fixture.findings();
    assert_eq!(
        findings.len(),
        1,
        "the legacy fork must not be an empty healthy session"
    );
    assert_eq!(findings[0]["state"], "forked_stub");
    assert_eq!(findings[0]["alternatives"].as_array().unwrap().len(), 1);
    assert_eq!(
        findings[0]["alternatives"][0]["address"],
        session_addr(&fixture.live)
    );
    assert_eq!(
        findings[0]["alternatives"][0]["identity"]["project_root"],
        fixture.live.project_root.to_string_lossy().as_ref()
    );
    let plan = fixture.preview().unwrap();
    assert_eq!(plan.repoint_runs.len(), 3);
    assert!(plan.live_log_lineage.is_none());
    let aside = plan.move_stub_aside.as_ref().unwrap().to.clone();
    fixture.store.apply_session_root_repair(&plan).unwrap();
    assert_eq!(
        fixture
            .store
            .resolve_session_address(&fixture.stale, None, None)
            .unwrap(),
        fixture.live
    );
    assert_eq!(std::fs::read(&aside).unwrap(), before_stub);
    assert_eq!(
        std::fs::read(wal.wal_path(&fixture.live)).unwrap(),
        before_real
    );
    assert!(!wal.wal_path(&fixture.stale).exists());
    assert!(fixture.findings().is_empty());
    assert!(fixture.preview().unwrap().stale_identity.is_none());
}

#[test]
fn legacy_fork_without_an_alternative_stays_missing_and_is_not_repaired() {
    let fixture = LegacyFork::new(false);
    let findings = fixture.findings();
    assert_eq!(findings.len(), 1);
    assert!(findings[0]["alternatives"].as_array().unwrap().is_empty());
    let error = fixture.preview().unwrap_err().to_string();
    assert!(error.contains("real_log_not_found"), "{error}");
    assert!(!fixture.root.join("wal/repair-aside").exists());
    let wal = broca_core::FileWalStore::new(fixture.root.join("wal"));
    std::fs::remove_file(wal.wal_path(&fixture.stale)).unwrap();
    let findings = fixture.findings();
    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0]["state"], "missing");
    assert!(findings[0]["alternatives"].as_array().unwrap().is_empty());
    let error = fixture.preview().unwrap_err().to_string();
    assert!(error.contains("real_log_not_found"), "{error}");
    assert!(!wal.wal_path(&fixture.stale).exists());
    assert!(!fixture.root.join("wal/repair-aside").exists());
}

#[test]
fn legacy_run_id_match_does_not_override_a_known_run_lineage() {
    let fixture = LegacyFork::new(true);
    fixture
        .store
        .conn
        .lock()
        .unwrap()
        .execute("UPDATE run_index SET lineage_id='known-lineage'", [])
        .unwrap();
    let error = fixture.preview().unwrap_err().to_string();
    assert!(error.contains("real_log_lineage_mismatch"), "{error}");
    assert!(!fixture.root.join("wal/repair-aside").exists());
}

#[test]
fn legacy_fork_with_two_complete_alternatives_refuses_ambiguity() {
    let fixture = LegacyFork::new(true);
    let other = BindIdentity::new(
        "/second/complete/root",
        &fixture.stale.harness,
        &fixture.stale.session,
    );
    fixture.hint(&other);
    fixture.write(&other, 3);
    let findings = fixture.findings();
    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0]["alternatives"].as_array().unwrap().len(), 2);
    let error = fixture.preview().unwrap_err().to_string();
    assert!(error.contains("real_log_ambiguous"), "{error}");
    assert!(!fixture.root.join("wal/repair-aside").exists());
}

#[test]
fn legacy_partial_history_under_a_third_root_is_not_an_alternative() {
    let fixture = LegacyFork::new(true);
    let partial = BindIdentity::new(
        "/third/partial/root",
        &fixture.stale.harness,
        &fixture.stale.session,
    );
    fixture.hint(&partial);
    fixture.write(&partial, 2);
    let findings = fixture.findings();
    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0]["alternatives"].as_array().unwrap().len(), 1);
    assert_eq!(
        findings[0]["alternatives"][0]["address"],
        session_addr(&fixture.live)
    );
    assert_eq!(fixture.preview().unwrap().surviving_identity, fixture.live);
}

#[test]
fn legacy_archived_history_is_reported_as_a_run_id_qualified_alternative() {
    let fixture = LegacyFork::new(true);
    let path = fixture
        .root
        .join("wal")
        .join(format!("{}.wal", session_addr(&fixture.live)));
    let archive = fixture.root.join("wal-archive");
    std::fs::create_dir_all(&archive).unwrap();
    let mut writer = broca_wal::archive::ArchiveWriter::create(archive.join("fold-1.ark"))
        .unwrap()
        .without_feature_floor();
    writer
        .append(&session_addr(&fixture.live), &std::fs::read(&path).unwrap())
        .unwrap();
    writer.seal().unwrap();
    std::fs::remove_file(path).unwrap();
    let findings = fixture.findings();
    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0]["alternatives"].as_array().unwrap().len(), 1);
    assert_eq!(
        findings[0]["alternatives"][0]["address"],
        session_addr(&fixture.live)
    );
    assert_eq!(
        findings[0]["alternatives"][0]["lineage_id"],
        serde_json::Value::Null
    );
}
