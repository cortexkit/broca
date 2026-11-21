//! The session-address inventory, read without the store that keeps it.

use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use rusqlite::Connection;

use crate::KeeperOpenError;

/// A connection to the existing address and scope inventory in run-index.db.
/// Creating its tables and running the census stay with [`crate::SqliteStore`].
pub struct SessionInventory {
    pub(crate) conn: Arc<Mutex<Connection>>,
}

impl SessionInventory {
    /// Open an existing database without creating or migrating any table or row.
    /// A missing file or inventory table is an error, never evidence of absence.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, KeeperOpenError> {
        let conn = crate::keeper_connection::open(
            path.as_ref(),
            "SessionInventory",
            &[
                "agent_data_session_ref",
                "agent_data_session_ref_v2",
                "session_scope_reservation",
                "agent_data_census",
                "agent_data_census_inflight",
                "agent_data_census_unreadable",
                "agent_data_census_done",
            ],
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub(crate) fn shared(conn: &Arc<Mutex<Connection>>) -> Self {
        Self {
            conn: Arc::clone(conn),
        }
    }
}
