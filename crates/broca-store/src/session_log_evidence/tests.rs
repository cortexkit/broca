use super::*;
use broca_core::{FileWalStore, RunIndex, RunIndexRow, RunIndexState, WalStore};
use broca_protocol::{BindIdentity, ModelHandle, RunFinishReason};

#[test]
fn ordinary_log_state_does_not_wait_for_the_store_connection() {
    struct ReleaseOnDrop(std::sync::mpsc::Sender<()>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    let root =
        broca_test_support::scratch_root().join(format!("log-state-lock-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let store = SqliteStore::open(root.join("run-index.db")).unwrap();
    let session = BindIdentity::new("/project", "broca", "ordinary");
    let wal_store = FileWalStore::new(root.join("wal"));
    let mut wal = wal_store.open(&session).unwrap();
    let start = crate::conformance::run_started(
        0,
        "ordinary",
        &session,
        "ordinary input",
        ModelHandle::new("stub", "model"),
    );
    wal.append(1, &start.record, true).unwrap();
    drop(wal);

    std::thread::scope(|threads| {
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let held_store = &store;
        threads.spawn(move || {
            let _connection = held_store.conn.lock().unwrap();
            locked_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        locked_rx.recv().unwrap();
        // Release the holder if the completion probe panics, so joining the
        // blocked reader thread cannot deadlock the test.
        let _release = ReleaseOnDrop(release_tx);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let reader = &store;
        let wal = &wal_store;
        let key = &session;
        threads.spawn(move || {
            let _ = done_tx.send(reader.session_log_state(key, wal));
        });
        let state = broca_test_support::wait_until_sync(
            "ordinary log admission while the store connection is held",
            || done_rx.try_recv().map_err(|error| error.to_string()),
        );
        assert_eq!(state.unwrap(), SessionLogState::Available);
    });
}

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
