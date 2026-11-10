//! Existing-only connections for usage export tables and the session-address inventory.
//! Opening these handles does not initialize the run_index or materialized message tables.

use std::path::Path;

use rusqlite::{config::DbConfig, Connection, OpenFlags};

/// An existing keeper database could not be opened or lacks a required table.
#[derive(Debug)]
pub struct KeeperOpenError(String);

impl std::fmt::Display for KeeperOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for KeeperOpenError {}

pub(crate) fn open(
    path: &Path,
    owner: &str,
    tables: &[&str],
) -> Result<Connection, KeeperOpenError> {
    let failure =
        |reason| KeeperOpenError(format!("{owner} cannot open {}: {reason}", path.display()));
    #[cfg(windows)]
    super::validate_private_sqlite(path).map_err(|error| failure(error.to_string()))?;
    // No CREATE flag: these handles must never turn a missing inventory into an empty one.
    super::sqlite_open_observed();
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)
        .map_err(|error| failure(error.to_string()))?;
    crate::busy::configure_writer_connection(&conn).map_err(|error| failure(error.to_string()))?;
    for table in tables {
        let present: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
                [table],
                |row| row.get(0),
            )
            .map_err(|error| failure(error.to_string()))?;
        if !present {
            return Err(failure(format!("required table {table} is missing")));
        }
    }
    if let Err(error) = conn.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true) {
        tracing::warn!(%error, "could not retain SQLite WAL sidecars on close");
    }
    super::restrict_to_owner(path);
    Ok(conn)
}

pub(crate) fn metadata_error(path: &Path, owner: &str, error: rusqlite::Error) -> KeeperOpenError {
    KeeperOpenError(format!("{owner} cannot read {}: {error}", path.display()))
}
