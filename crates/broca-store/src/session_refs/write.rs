use crate::{SqliteStore, StoreError};
use broca_core::{agent_session_ref, session_addr, SessionRefIndex};
use broca_protocol::BindIdentity;
use rusqlite::{params, Connection};

pub(crate) fn insert(
    conn: &Connection,
    address: &str,
    session: &BindIdentity,
) -> Result<usize, StoreError> {
    // A re-keyed WAL can retain older project/harness/session identities. Only
    // the identity whose derived address matches the WAL's address owns it.
    if session_addr(session) != address {
        return Ok(0);
    }
    Ok(conn.execute(
        "INSERT OR IGNORE INTO agent_data_session_ref(session_ref,address,project_root,harness,session) VALUES(?1,?2,?3,?4,?5)",
        params![agent_session_ref(session),address,session.project_root.to_string_lossy(),session.harness,session.session],
    )?)
}

#[cfg(test)]
thread_local! {
    pub(crate) static REF_WAIT_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

impl SessionRefIndex for SqliteStore {
    fn record_session(&self, address: &str, session: &BindIdentity) -> Result<(), String> {
        let deadline = std::time::Instant::now() + crate::STORE_BUSY_TIMEOUT;
        let conn = loop {
            match self.conn.try_lock() {
                Ok(conn) => break conn,
                Err(std::sync::TryLockError::Poisoned(error)) => {
                    return Err(format!("session-ref store poisoned: {error}"))
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
        // write lock. Routine migration contention must not create a WAL gap.
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err("session-ref store busy timeout before write".into());
        }
        crate::busy::with_write_deadline(deadline, || insert(&conn, address, session))
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}
