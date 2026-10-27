use super::busy::{configure_writer_connection, BUSY_SLEEP_HOOK, STORE_BUSY_TIMEOUT};
use broca_core::{agent_session_ref, session_addr, SessionRefIndex};
use broca_protocol::BindIdentity;
use rusqlite::{ffi, Connection, OptionalExtension};
use std::sync::mpsc;
use std::time::Duration;

#[test]
fn ref_index_write_finds_gaps_missed_by_default_backoff() {
    let (result, stored, expected) = with_regrabbing_writer("ref-index", |conn| {
        configure_writer_connection(&conn).unwrap();
        let store = super::SqliteStore {
            conn: std::sync::Mutex::new(conn),
        };
        let session = BindIdentity::new("/project", "broca", "new-session");
        let result = store.record_session(&session_addr(&session), &session);
        let conn = store.conn.lock().unwrap();
        // Both references a session is found by: the folder-based legacy reference and the
        // external (v2) reference.
        let stored = ["agent_data_session_ref", "agent_data_session_ref_v2"].map(|table| {
            conn.query_row(&format!("SELECT session_ref FROM {table}"), [], |row| {
                row.get::<_, String>(0)
            })
            .optional()
            .unwrap()
        });
        let expected = [
            broca_core::wal_writer::legacy_session_ref("broca", "/project", "new-session"),
            agent_session_ref(&session),
        ];
        (result, stored, expected)
    });
    result.expect("ref-index short polls must find an unlocked gap");
    assert_eq!(stored, expected.map(Some));
}

#[test]
fn migration_busy_poll_finds_gaps_missed_by_default_backoff() {
    for migration_policy in [false, true] {
        let result = with_regrabbing_writer(&format!("migration-{migration_policy}"), |conn| {
            if migration_policy {
                configure_writer_connection(&conn).unwrap();
            } else {
                conn.busy_timeout(STORE_BUSY_TIMEOUT).unwrap();
            }
            let result =
                conn.execute_batch("BEGIN IMMEDIATE; INSERT INTO sample VALUES(1); COMMIT;");
            if result.is_ok() {
                assert_eq!(
                    conn.query_row("SELECT count(*) FROM sample", [], |row| row
                        .get::<_, u64>(0))
                        .unwrap(),
                    1
                );
            }
            result
        });
        if migration_policy {
            result.expect("short polls must find an unlocked gap");
        } else {
            assert!(
                matches!(result, Err(rusqlite::Error::SqliteFailure(error, _)) if error.code == ffi::ErrorCode::DatabaseBusy),
                "default backoff must exhaust its budget despite the unlocked gaps"
            );
        }
    }
}

fn with_regrabbing_writer<T>(tag: &str, write: impl FnOnce(Connection) -> T) -> T {
    super::vfs::register();
    let path = broca_test_support::scratch_root()
        .join(format!("writer-busy-gaps-{}-{tag}.db", std::process::id()));
    let foreground = Connection::open(&path).unwrap();
    foreground
        .execute_batch(
            "PRAGMA journal_mode=WAL;
         CREATE TABLE sample(id INTEGER PRIMARY KEY);
         -- The two session-reference tables as broca-store's session_refs::init_schema
         -- creates them. Its full schema can't be used here: its triggers name run_index.
         CREATE TABLE agent_data_session_ref (
             session_ref TEXT PRIMARY KEY CHECK(length(session_ref)=32 AND session_ref NOT GLOB '*[^0-9a-f]*'),
             address TEXT NOT NULL, project_root TEXT NOT NULL, harness TEXT NOT NULL, session TEXT NOT NULL
         ) WITHOUT ROWID;
         CREATE TABLE agent_data_session_ref_v2 (
             session_ref TEXT NOT NULL, address TEXT NOT NULL,
             PRIMARY KEY(session_ref,address)
         ) WITHOUT ROWID;
         BEGIN IMMEDIATE;",
        )
        .unwrap();
    let conn = Connection::open_with_flags_and_vfs(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        "migration-scheduled-sleep",
    )
    .unwrap();
    let (advance_tx, advance_rx) = mpsc::channel::<Duration>();
    let (ready_tx, ready_rx) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        let mut clock_us = 0;
        let mut transactions = 0;
        while let Ok(delay) = advance_rx.recv() {
            clock_us += delay.as_micros();
            foreground.execute_batch("COMMIT").unwrap();
            transactions += 1;
            // Open one-ms windows at 7, 17, ... through the first virtual second.
            // Default backoff skips these phases; closing later windows prevents
            // its final truncated sleep from accidentally landing in a gap.
            // The channel handshake controls lock release and reacquisition,
            // so the test does not depend on wall-clock timing.
            let gap = clock_us < 1_000_000 && (7_000..8_000).contains(&(clock_us % 10_000));
            if !gap {
                foreground.execute_batch("BEGIN IMMEDIATE").unwrap();
            }
            ready_tx.send(()).unwrap();
        }
        if !foreground.is_autocommit() {
            foreground.execute_batch("ROLLBACK").unwrap();
        }
        transactions
    });
    BUSY_SLEEP_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |delay| {
            advance_tx.send(delay).unwrap();
            ready_rx
                .recv_timeout(broca_test_support::EVENT_HANG_GUARD)
                .unwrap();
        }));
    });
    let result = write(conn);
    BUSY_SLEEP_HOOK.with(|hook| *hook.borrow_mut() = None);
    let transactions = writer.join().unwrap();
    assert!(!super::vfs::sleep_panicked(), "VFS sleep scheduler failed");
    assert!(
        transactions > 1,
        "writer must repeatedly reacquire the lock"
    );
    result
}

#[test]
fn ref_index_terminal_sql_error_does_not_poll() {
    let conn = Connection::open_in_memory().unwrap();
    configure_writer_connection(&conn).unwrap();
    let store = super::SqliteStore {
        conn: std::sync::Mutex::new(conn),
    };
    let session = BindIdentity::new("/project", "broca", "missing-schema");
    let polls = std::rc::Rc::new(std::cell::Cell::new(0));
    let observed = polls.clone();
    BUSY_SLEEP_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |_| observed.set(observed.get() + 1)));
    });
    let error = store.record_session(&session_addr(&session), &session);
    BUSY_SLEEP_HOOK.with(|hook| *hook.borrow_mut() = None);
    assert!(error.unwrap_err().contains("no such table"));
    assert_eq!(
        polls.get(),
        0,
        "terminal errors must not enter the busy poll"
    );
}
