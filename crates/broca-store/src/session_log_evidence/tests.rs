use super::*;
use broca_core::{FileWalStore, RunIndex, RunIndexRow, RunIndexState};
use broca_protocol::{BindIdentity, ModelHandle, RunFinishReason};

#[test]
fn retirement_proves_an_unidentified_run_only_from_its_own_deleting_log() {
    for newer_run in [false, true] {
        let root = broca_test_support::scratch_root().join(format!(
            "retirement-proof-{}-{newer_run}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let store = SqliteStore::open(root.join("run-index.db")).unwrap();
        let session = BindIdentity::new("/project", "broca", "session");
        let wal_store = FileWalStore::new(root.join("wal"));
        let mut wal = wal_store.open_with_retention(&session, Some(60)).unwrap();
        let mut start = crate::conformance::run_started(
            0,
            "old",
            &session,
            "old input",
            ModelHandle::new("stub", "model"),
        )
        .record;
        if let broca_wal::WalRecord::RunStarted { retention, .. } = &mut start {
            *retention = Some(60);
        }
        wal.append(1, &start, true).unwrap();
        wal.append(
            1,
            &broca_wal::WalRecord::RunFinished {
                interrupt_reason: None,
                reason: RunFinishReason::Completed,
                ts_ms: Some(1000),
                usage: Default::default(),
                error: None,
                retries_used: 0,
            },
            true,
        )
        .unwrap();
        let lineage = wal.lineage_id().unwrap();
        drop(wal);
        let row = |id: &str| RunIndexRow {
            run_id: id.into(),
            session: session.clone(),
            submission_id: None,
            request_identity_digest: None,
            state: RunIndexState::Completed,
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
            title: None,
            withdrawals_owed: false,
        };
        store.index_run_active(&row("old")).unwrap();
        if newer_run {
            store
                .conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE run_index SET expired_at_ms=61000,lineage_id=?1 WHERE run_id='old'",
                    [lineage.to_string()],
                )
                .unwrap();
            store
                .index_run_active(&RunIndexRow {
                    state: RunIndexState::Active,
                    ..row("new")
                })
                .unwrap();
        }
        store
            .retention_deletion_pending(&lineage.to_string(), &session, true)
            .unwrap();
        let stone = wal_store.tombstone_path(&session, &lineage.to_string());
        std::fs::create_dir_all(stone.parent().unwrap()).unwrap();
        serde_json::to_writer(
            std::fs::File::create(stone).unwrap(),
            &serde_json::json!({
                "lineage_id": lineage.to_string(), "expired_at_ms": 61000, "seq": 1
            }),
        )
        .unwrap();
        std::fs::rename(
            wal_store.ephemeral_wal_path(&session),
            wal_store.deleting_path(&session, lineage),
        )
        .unwrap();
        let expected = if newer_run {
            SessionLogState::Missing
        } else {
            SessionLogState::Removed
        };
        assert_eq!(
            store.session_log_state(&session, &wal_store).unwrap(),
            expected,
            "newer run: {newer_run}"
        );
    }
}
