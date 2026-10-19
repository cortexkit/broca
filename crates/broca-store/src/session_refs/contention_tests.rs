use super::*;
use broca_core::{ExportIndex, RunIndex, RunIndexRow, RunIndexState, WalStore};
use broca_protocol::{ModelHandle, RunFinishReason, RunId, Usage};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc, Barrier,
};

struct CountingIndex {
    store: Arc<SqliteStore>,
    failures: AtomicUsize,
}
impl SessionRefIndex for CountingIndex {
    fn record_session(&self, address: &str, session: &BindIdentity) -> Result<(), String> {
        let result = self.store.record_session(address, session);
        if result.is_err() {
            self.failures.fetch_add(1, Ordering::SeqCst);
        }
        result
    }
}
fn fixture(tag: &str) -> (std::path::PathBuf, Arc<SqliteStore>) {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let root = broca_test_support::scratch_root().join(format!(
        "projection-contention-{}-{}-{tag}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(root.join("wal")).unwrap();
    let store = Arc::new(SqliteStore::open(root.join("run-index.db")).unwrap());
    // Seed SQLite ref rows for empty legacy WALs. This exercises census queue
    // and coverage updates under SQLite write locks without payload decoding.
    for i in 0..512 {
        let session = BindIdentity::new("/project/legacy", "broca", format!("legacy-{i}"));
        store
            .record_session(&session_addr(&session), &session)
            .unwrap();
        std::fs::write(
            root.join("wal")
                .join(format!("{}.wal", session_addr(&session))),
            [],
        )
        .unwrap();
    }
    backfill(&store.conn.lock().unwrap()).unwrap();
    (root, store)
}
/// Opens a census connection the way production `census::migrate` does: with the store's
/// busy timeout. A bare connection fails at the first lock overlap, which slower Windows
/// runners hit, and would test a configuration production never uses.
fn census_connection(path: &std::path::Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.busy_timeout(crate::STORE_BUSY_TIMEOUT).unwrap();
    conn
}

fn receive<T>(rx: &mpsc::Receiver<T>, event: &str) -> T {
    rx.recv_timeout(broca_test_support::EVENT_HANG_GUARD)
        .unwrap_or_else(|_| panic!("timed out waiting for {event}"))
}

#[test]
fn ref_write_waits_during_census_without_marker_or_rearming() {
    let (root, store) = fixture("wait");
    let index = Arc::new(CountingIndex {
        store: store.clone(),
        failures: AtomicUsize::new(0),
    });
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    wal.set_session_ref_index(index.clone());
    let session = BindIdentity::new("/project/new", "broca", "waiting-ref");
    let mut writer = wal.open(&session).unwrap();
    let record = crate::conformance::run_started(
        1,
        "waiting-run",
        &session,
        "hello",
        ModelHandle::new("stub", "model"),
    )
    .record;
    let (census_started_tx, census_started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let census_root = root.clone();
    let migration = std::thread::spawn(move || {
        let mut conn = census_connection(&census_root.join("run-index.db"));
        batches::AFTER_WRITE_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                census_started_tx.send(()).unwrap();
                receive(&resume_rx, "resume census");
            }))
        });
        census::run(
            &mut conn,
            &broca_core::ReadOnlyWalReader::new(census_root),
            None,
            true,
        )
        .unwrap()
    });
    receive(&census_started_rx, "active census");
    let connection = store.conn.lock().unwrap();
    let (waiting_tx, waiting_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let admission = std::thread::spawn(move || {
        REF_WAIT_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                waiting_tx.send(()).unwrap();
                receive(&release_rx, "release serving connection");
            }))
        });
        writer.append(1, &record, true).unwrap();
    });
    receive(&waiting_rx, "ref writer waiting on serving connection");
    drop(connection);
    release_tx.send(()).unwrap();
    admission.join().unwrap();
    resume_tx.send(()).unwrap();
    let first = migration.join().unwrap();
    assert_eq!(index.failures.load(Ordering::SeqCst), 0);
    assert!(!broca_core::session_projection_incomplete(&root).unwrap());
    assert!(first.complete);
    assert!(first.addresses > 0);
    let mut conn = census_connection(&root.join("run-index.db"));
    let second = census::run(
        &mut conn,
        &broca_core::ReadOnlyWalReader::new(&root),
        None,
        true,
    )
    .unwrap();
    assert!(second.complete);
    assert_eq!(
        (
            second.addresses,
            second.bytes_read,
            second.writes.transactions
        ),
        (0, 0, 0)
    );
    let reference = agent_session_ref(&session);
    assert!(store
        .session_refs([reference.as_str()].into_iter())
        .unwrap()
        .sessions
        .contains_key(&reference));
}

// SQLite trace/profile callbacks bracket statements on the serving and census
// connections. Leave the busy handler unchanged so its normal lock waits remain visible.
fn lock_probe(connection: &str, event: &str, sql: &str, elapsed: std::time::Duration) {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let now = START.get_or_init(std::time::Instant::now).elapsed();
    let sql = sql.trim_start();
    if [
        "BEGIN", "COMMIT", "ROLLBACK", "INSERT", "UPDATE", "DELETE", "PRAGMA",
    ]
    .iter()
    .any(|prefix| sql.starts_with(prefix))
    {
        eprintln!(
            "CENSUS_LOCK_PROBE t_us={} conn={connection} thread={:?} event={event} elapsed_us={} sql={}",
            now.as_micros(),
            std::thread::current().id(),
            elapsed.as_micros(),
            sql.chars().take(160).collect::<String>().replace('\n', " "),
        );
    }
}

#[test]
fn census_with_ref_projection_and_export_writers_has_no_failures_or_markers() {
    let (root, store) = fixture("writers");
    {
        let mut conn = store.conn.lock().unwrap();
        conn.trace(Some(|sql| {
            lock_probe("serving", "start", sql, std::time::Duration::ZERO)
        }));
        conn.profile(Some(|sql, elapsed| {
            lock_probe("serving", "done", sql, elapsed)
        }));
    }
    let index = Arc::new(CountingIndex {
        store: store.clone(),
        failures: AtomicUsize::new(0),
    });
    let (started_tx, started_rx) = mpsc::channel();
    let active = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let migration_active = active.clone();
    let census_root = root.clone();
    let migration = std::thread::spawn(move || {
        let mut conn = census_connection(&census_root.join("run-index.db"));
        conn.trace(Some(|sql| {
            lock_probe("census", "start", sql, std::time::Duration::ZERO)
        }));
        conn.profile(Some(|sql, elapsed| {
            lock_probe("census", "done", sql, elapsed)
        }));
        batches::AFTER_WRITE_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                eprintln!("CENSUS_LOCK_PROBE census AFTER_WRITE_HOOK send (after commit)");
                started_tx.send(()).unwrap();
            }))
        });
        let result = census::run(
            &mut conn,
            &broca_core::ReadOnlyWalReader::new(census_root),
            None,
            true,
        );
        eprintln!("CENSUS_LOCK_PROBE census result={result:?}");
        let counts = result.unwrap();
        migration_active.store(false, Ordering::SeqCst);
        counts
    });
    receive(&started_rx, "census writes");
    let writers = Arc::new(Barrier::new(3));
    let projection_store = store.clone();
    let projection_start = writers.clone();
    let projections = std::thread::spawn(move || {
        projection_start.wait();
        for i in 0..80 {
            let session =
                BindIdentity::new("/project/projection", "broca", format!("projection-{i}"));
            let run = RunId(format!("projection-{i}"));
            let mut records = vec![crate::conformance::run_started(
                0,
                &run.0,
                &session,
                "hello",
                ModelHandle::new("stub", "model"),
            )];
            records.extend(crate::conformance::text_turn(1, 0, "reply"));
            records.push(broca_wal::FramedRecord {
                seq: 3,
                fence: 1,
                record: broca_wal::WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ts_ms: Some(2),
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                },
            });
            projection_store
                .index_run_active(&RunIndexRow {
                    run_id: run.0.clone(),
                    session: session.clone(),
                    submission_id: None,
                    request_identity_digest: None,
                    state: RunIndexState::Active,
                    usage: None,
                    final_step_finish_reason: None,
                    retries_used: 0,
                    title: None,
                    withdrawals_owed: false,
                })
                .unwrap();
            projection_store.project(&session, &records).unwrap();
            projection_store
                .index_run_terminal(
                    &run.0,
                    RunIndexState::Completed,
                    Some(Usage::default()),
                    None,
                    0,
                )
                .unwrap();
            projection_store
                .emit_run(&records, &run, &|_: &str, _: &str| {
                    (broca_core::ChargeBasis::Unknown, 1)
                })
                .unwrap();
            std::thread::yield_now();
        }
    });
    let ref_root = root.clone();
    let ref_index = index.clone();
    let ref_start = writers.clone();
    let refs = std::thread::spawn(move || {
        let wal = broca_core::FileWalStore::new(ref_root.join("wal"));
        wal.set_session_ref_index(ref_index);
        ref_start.wait();
        let mut overlapping = 0;
        for i in 0..80 {
            if active.load(Ordering::SeqCst) {
                overlapping += 1;
            }
            let session = BindIdentity::new("/project/new", "broca", format!("new-{i}"));
            let record = crate::conformance::run_started(
                1,
                &format!("new-run-{i}"),
                &session,
                "hello",
                ModelHandle::new("stub", "model"),
            )
            .record;
            wal.open(&session)
                .unwrap()
                .append(1, &record, true)
                .unwrap();
        }
        overlapping
    });
    writers.wait();
    let projection_result = projections.join();
    let ref_result = refs.join();
    let migration_result = migration.join();
    projection_result.unwrap();
    assert!(
        ref_result.unwrap() > 0,
        "ref writes must overlap an active census"
    );
    let counts = migration_result.unwrap();
    assert!(counts.complete);
    assert_eq!(index.failures.load(Ordering::SeqCst), 0);
    assert!(!broca_core::session_projection_incomplete(&root).unwrap());
    use broca_core::MaterializedStore;
    let projected = store
        .read_prefix_with_watermark(&BindIdentity::new(
            "/project/projection",
            "broca",
            "projection-79",
        ))
        .unwrap();
    assert_eq!(projected.watermark_seq, 2);
    assert_eq!(projected.prefix.len(), 2);
    assert_eq!(store.high_water(), 80);
    assert_eq!(
        store.lookup_run("projection-79").unwrap().unwrap().state,
        RunIndexState::Completed
    );
    assert!(
        store
            .session_refs(
                [
                    agent_session_ref(&BindIdentity::new("/project/new", "broca", "new-79"))
                        .as_str()
                ]
                .into_iter()
            )
            .unwrap()
            .complete
    );
}

#[test]
fn backfill_resumes_committed_pages_without_certifying_partial_absence() {
    let (root, store) = fixture("backfill-resume");
    let conn = store.conn.lock().unwrap();
    conn.execute("DELETE FROM agent_data_projection", [])
        .unwrap();
    conn.execute("DELETE FROM agent_data_backfill_cursor", [])
        .unwrap();
    for i in 0..150 {
        let session = BindIdentity::new("/project/backfill", "broca", format!("row-{i}"));
        conn.execute(
            "INSERT INTO run_index(run_id,session,state) VALUES(?1,?2,'completed')",
            params![
                format!("row-{i:04}"),
                serde_json::to_string(&session).unwrap()
            ],
        )
        .unwrap();
    }
    backfill::run_limited(&conn, Some(73)).unwrap();
    assert!(!conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM agent_data_projection)",
            [],
            |row| row.get::<_, bool>(0)
        )
        .unwrap());
    assert_eq!(
        conn.query_row(
            "SELECT last_run_id FROM agent_data_backfill_cursor",
            [],
            |row| row.get::<_, String>(0)
        )
        .unwrap(),
        "row-0072"
    );
    drop(conn);
    drop(store);
    let reopened = SqliteStore::open(root.join("run-index.db")).unwrap();
    backfill(&reopened.conn.lock().unwrap()).unwrap();
    for i in 0..150 {
        let session = BindIdentity::new("/project/backfill", "broca", format!("row-{i}"));
        assert!(reopened
            .session_refs([agent_session_ref(&session).as_str()].into_iter())
            .unwrap()
            .sessions
            .contains_key(&agent_session_ref(&session)));
    }
}

#[test]
fn interrupted_candidate_batch_is_replayed_before_census_completion() {
    let (root, store) = fixture("inflight");
    let session = BindIdentity::new("/project/interrupted", "broca", "candidate");
    let address = session_addr(&session);
    let record = crate::conformance::run_started(
        1,
        "inflight-run",
        &session,
        "hello",
        ModelHandle::new("stub", "model"),
    )
    .record;
    let bytes = broca_wal::framing::encode(1, 1, &record).unwrap();
    let path = root.join("wal").join(format!("{address}.wal"));
    let mut corrupt = b"damaged prefix before intact frame".to_vec();
    corrupt.extend_from_slice(&bytes);
    std::fs::write(&path, &corrupt).unwrap();
    store.record_session(&address, &session).unwrap();
    let mut conn = census_connection(&root.join("run-index.db"));
    conn.execute(
        "INSERT INTO agent_data_census(address) VALUES(?1)",
        [&address],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO agent_data_census_inflight VALUES(?1)",
        [&address],
    )
    .unwrap();
    assert!(
        census::run(
            &mut conn,
            &broca_core::ReadOnlyWalReader::new(&root),
            None,
            false
        )
        .is_err(),
        "a partial candidate is not proof that its WAL was completely read"
    );
    assert!(
        !store
            .session_refs([agent_session_ref(&session).as_str()].into_iter())
            .unwrap()
            .complete
    );
    std::fs::write(&path, &bytes).unwrap();
    let counts = census::run(
        &mut conn,
        &broca_core::ReadOnlyWalReader::new(&root),
        None,
        false,
    )
    .unwrap();
    assert!(counts.complete);
    assert_eq!(counts.not_represented, 1);
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM agent_data_census_inflight",
            [],
            |row| row.get::<_, u64>(0)
        )
        .unwrap(),
        0
    );
}

#[test]
fn persistent_sqlite_contention_waits_until_busy_timeout() {
    let (root, store) = fixture("sqlite-timeout");
    let blocker = census_connection(&root.join("run-index.db"));
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let session = BindIdentity::new("/project/new", "broca", "locked-for-budget");
    let started = std::time::Instant::now();
    assert!(store
        .record_session(&session_addr(&session), &session)
        .is_err());
    // This lower-bound check measures the actual busy handler, not an event
    // deadline: an immediate failure must not masquerade as an exhausted budget.
    assert!(started.elapsed() + std::time::Duration::from_millis(100) >= crate::STORE_BUSY_TIMEOUT);
    blocker.execute_batch("ROLLBACK").unwrap();
    store
        .record_session(&session_addr(&session), &session)
        .unwrap();
}
