use super::*;
use broca_core::{FileWalStore, WalStore};
use broca_protocol::{BindIdentity, ModelHandle};

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
