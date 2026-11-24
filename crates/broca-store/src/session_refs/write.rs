use crate::{SessionInventory, StoreError};
use broca_core::{agent_session_ref, session_addr, SessionRefIndex};
use broca_protocol::BindIdentity;
use rusqlite::{params, Connection, Transaction, TransactionBehavior};

pub(crate) const CONNECTION_POISONED: &str = "run-index connection mutex poisoned";

/// Whether the index has the alias count table. A standalone inventory can write into an index
/// created before the count existed; startup creates the table and recounts, so such an index
/// has nothing to keep in step.
fn has_rule_table(conn: &Connection) -> Result<bool, rusqlite::Error> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='agent_data_session_ref_v2_rule')",
        [],
        |row| row.get(0),
    )
}

pub(crate) fn insert(
    conn: &Connection,
    address: &str,
    session: &BindIdentity,
) -> Result<usize, StoreError> {
    // A re-keyed log can retain older project/harness/session identities. Only
    // the identity whose derived address matches the log's address owns it.
    if session_addr(session) != address {
        return Ok(0);
    }
    // Inventory rebuilds and address reservations already own a transaction. A standalone
    // log writer must commit both aliases and the count together so startup can trust the count.
    let tx = conn
        .is_autocommit()
        .then(|| Transaction::new_unchecked(conn, TransactionBehavior::Immediate))
        .transpose()?;
    let conn = tx.as_deref().unwrap_or(conn);
    let inserted = conn.execute(
        "INSERT OR IGNORE INTO agent_data_session_ref(session_ref,address,project_root,harness,session) VALUES(?1,?2,?3,?4,?5)",
        params![broca_core::session_refs::legacy_session_ref(&session.harness, &session.project_root.to_string_lossy(), &session.session),address,session.project_root.to_string_lossy(),session.harness,session.session],
    )?;
    insert_v2(conn, address, session)?;
    if inserted != 0 && has_rule_table(conn)? {
        // Increment rather than recount: an older writer's unaliased row must still leave
        // the count short so the next startup repairs it.
        conn.execute(
            "UPDATE agent_data_session_ref_v2_rule SET sessions=sessions+1 WHERE id=1",
            [],
        )?;
    }
    if let Some(tx) = tx {
        tx.commit()?;
    }
    Ok(inserted)
}

pub(super) fn insert_v2(
    conn: &Connection,
    address: &str,
    session: &BindIdentity,
) -> Result<(), StoreError> {
    conn.execute(
        "INSERT OR IGNORE INTO agent_data_session_ref_v2(session_ref,address) VALUES(?1,?2)",
        params![agent_session_ref(session), address],
    )?;
    Ok(())
}

#[cfg(test)]
thread_local! {
    pub(crate) static REF_WAIT_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

impl SessionRefIndex for SessionInventory {
    fn record_session(&self, address: &str, session: &BindIdentity) -> Result<(), String> {
        let deadline = std::time::Instant::now() + crate::STORE_BUSY_TIMEOUT;
        let conn = loop {
            match self.conn.try_lock() {
                Ok(conn) => break conn,
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(CONNECTION_POISONED.into());
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    #[cfg(test)]
                    REF_WAIT_HOOK.with(|hook| {
                        if let Some(hook) = hook.borrow_mut().take() {
                            hook();
                        }
                    });
                    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                    if remaining.is_zero() {
                        return Err("session-ref store busy timeout waiting for connection".into());
                    }
                    std::thread::sleep(remaining.min(std::time::Duration::from_millis(1)));
                }
            }
        };
        // Share one bounded budget between the connection mutex and SQLite's
        // write lock. Routine migration contention must not create a log gap.
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err("session-ref store busy timeout before write".into());
        }
        crate::busy::with_write_deadline(deadline, || insert(&conn, address, session))
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}
