use super::busy::{configure_migration_connection, BUSY_SLEEP_HOOK, STORE_BUSY_TIMEOUT};
use rusqlite::{ffi, Connection};
use std::sync::mpsc;
use std::time::Duration;

#[test]
fn migration_busy_poll_finds_gaps_missed_by_default_backoff() {
    super::vfs::register();
    for migration_policy in [false, true] {
        let path = broca_test_support::scratch_root().join(format!(
            "migration-busy-gaps-{}-{migration_policy}.db",
            std::process::id()
        ));
        let foreground = Connection::open(&path).unwrap();
        foreground.execute_batch(
            "PRAGMA journal_mode=WAL; CREATE TABLE sample(id INTEGER PRIMARY KEY); BEGIN IMMEDIATE;"
        ).unwrap();
        let conn = Connection::open_with_flags_and_vfs(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            "migration-scheduled-sleep",
        )
        .unwrap();
        if migration_policy {
            configure_migration_connection(&conn).unwrap();
        } else {
            conn.busy_timeout(STORE_BUSY_TIMEOUT).unwrap();
        }
        let (advance_tx, advance_rx) = mpsc::channel::<Duration>();
        let (ready_tx, ready_rx) = mpsc::channel();
        let writer = std::thread::spawn(move || {
            let mut clock_us = 0;
            let mut transactions = 0;
            while let Ok(delay) = advance_rx.recv() {
                clock_us += delay.as_micros();
                foreground.execute_batch("COMMIT").unwrap();
                transactions += 1;
                // Leave a one-ms gap at 9, 19, 29, ... virtual milliseconds.
                // SQLite's growing default sleeps skip these windows; fixed
                // one-ms polls reach them. Actual SQLite locks are released and
                // reacquired here, before the waiting connection resumes.
                if clock_us % 10_000 < 9_000 {
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
        let result = conn.execute_batch("BEGIN IMMEDIATE; INSERT INTO sample VALUES(1); COMMIT;");
        BUSY_SLEEP_HOOK.with(|hook| *hook.borrow_mut() = None);
        let transactions = writer.join().unwrap();
        assert!(!super::vfs::sleep_panicked(), "VFS sleep scheduler failed");
        assert!(
            transactions > 1,
            "the foreground must repeatedly reacquire the write lock"
        );
        if migration_policy {
            result.expect("short polls must find an unlocked gap");
            assert_eq!(
                conn.query_row("SELECT count(*) FROM sample", [], |row| row
                    .get::<_, u64>(0))
                    .unwrap(),
                1
            );
        } else {
            assert!(
                matches!(result, Err(rusqlite::Error::SqliteFailure(error, _)) if error.code == ffi::ErrorCode::DatabaseBusy),
                "default backoff must exhaust its budget despite the unlocked gaps"
            );
        }
    }
}
