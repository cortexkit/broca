use super::tests::{head_path, stored};
use super::*;
use rusqlite::Connection;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc,
};

struct CountingIndex {
    store: Arc<SqliteStore>,
    failures: AtomicUsize,
}
impl SessionRefIndex for CountingIndex {
    fn record_session(&self, address: &str, session: &BindIdentity) -> Result<(), String> {
        let result = self.store.record_session(address, session);
        if let Err(error) = &result {
            eprintln!("session-ref projection failed: {error}");
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
    std::fs::create_dir_all(&root).unwrap();
    let store = Arc::new(SqliteStore::open(root.join("run-index.db")).unwrap());
    // Seed stored sessions the index already names. This exercises census queue and coverage
    // updates under SQLite write locks without recording a candidate.
    for i in 0..256 {
        let session = BindIdentity::new("/project/indexed", "broca", format!("indexed-{i}"));
        store
            .record_session(&session_addr(&session), &session)
            .unwrap();
        stored(&root, &session);
    }
    (root, store)
}
/// All store writers share short lock polls so the census and foreground commits
/// can find gaps in one another's write traffic.
fn census_connection(path: &std::path::Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    crate::busy::configure_writer_connection(&conn).unwrap();
    conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    conn
}

fn receive<T>(rx: &mpsc::Receiver<T>, event: &str) -> T {
    rx.recv_timeout(broca_test_support::EVENT_HANG_GUARD)
        .unwrap_or_else(|_| panic!("timed out waiting for {event}"))
}

thread_local! {
    static PROJECTION_WRITER_BUSY_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[test]
fn ref_write_waits_during_census_without_marker_or_rearming() {
    let (root, store) = fixture("wait");
    let index = Arc::new(CountingIndex {
        store: store.clone(),
        failures: AtomicUsize::new(0),
    });
    let session = BindIdentity::new("/project/new", "broca", "waiting-ref");
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
        census::run(&mut conn, &StoredSessions::new(&census_root), None, true).unwrap()
    });
    receive(&census_started_rx, "active census");
    let connection = store.conn.lock().unwrap();
    let (waiting_tx, waiting_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let admitted = session.clone();
    let admission_root = root.clone();
    let admission_index = index.clone();
    let admission = std::thread::spawn(move || {
        REF_WAIT_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                waiting_tx.send(()).unwrap();
                receive(&release_rx, "release serving connection");
            }))
        });
        let admission = broca_core::record_candidate(
            Some(admission_index.as_ref()),
            &admission_root,
            &session_addr(&admitted),
            &admitted,
        )
        .unwrap();
        assert!(
            admission.is_none(),
            "the ref was recorded, so no gap is held"
        );
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
    let second = census::run(&mut conn, &StoredSessions::new(&root), None, true).unwrap();
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

#[test]
fn interrupted_candidate_batch_is_replayed_before_census_completion() {
    let (root, store) = fixture("inflight");
    let session = BindIdentity::new("/project/interrupted", "broca", "candidate");
    let address = session_addr(&session);
    stored(&root, &session);
    let path = head_path(&root, &session);
    let bytes = std::fs::read(&path).unwrap();
    std::fs::write(&path, b"damaged head").unwrap();
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
    let unreadable = census::run(&mut conn, &StoredSessions::new(&root), None, false).unwrap();
    assert!(
        !unreadable.complete && unreadable.unreadable == 1,
        "a partial candidate is not proof that its session was read"
    );
    assert!(
        !store
            .session_refs([agent_session_ref(&session).as_str()].into_iter())
            .unwrap()
            .complete
    );
    std::fs::write(&path, &bytes).unwrap();
    let counts = census::run(&mut conn, &StoredSessions::new(&root), None, false).unwrap();
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
