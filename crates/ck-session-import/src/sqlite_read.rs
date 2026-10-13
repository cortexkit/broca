use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{Connection, Error as SqliteError, ErrorCode, OpenFlags};

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct ReadOnlyDatabase {
    pub(crate) connection: Connection,
    pub(crate) fallback_warning: Option<String>,
    // Read by the dormant-fallback test, which needs unix permission bits to force the
    // fallback open; Windows test builds compile the field but cannot exercise it.
    #[cfg(test)]
    #[cfg_attr(windows, allow(dead_code))]
    pub(crate) used_immutable_fallback: bool,
}

/// Open a live harness database without blocking its writer.
///
/// A normal read-only connection participates in SQLite's WAL snapshot protocol and therefore
/// sees committed frames that have not reached the main database. Immutable mode is retained only
/// for dormant WAL-mode databases that cannot create their missing shared-memory file.
pub(crate) fn open_read_only_database(
    path: &Path,
    harness_name: &str,
) -> rusqlite::Result<ReadOnlyDatabase> {
    match open_uri(&read_only_uri(path)) {
        Ok(connection) => Ok(ReadOnlyDatabase {
            connection,
            fallback_warning: None,
            #[cfg(test)]
            used_immutable_fallback: false,
        }),
        Err(primary_error) if immutable_fallback_allowed(&primary_error) => {
            let connection = open_uri(&immutable_uri(path))?;
            Ok(ReadOnlyDatabase {
                connection,
                fallback_warning: fallback_warning(path, harness_name),
                #[cfg(test)]
                used_immutable_fallback: true,
            })
        }
        Err(error) => Err(error),
    }
}

fn immutable_fallback_allowed(error: &SqliteError) -> bool {
    matches!(
        error,
        SqliteError::SqliteFailure(details, _)
            if matches!(details.code, ErrorCode::ReadOnly | ErrorCode::CannotOpen)
    )
}

fn open_uri(uri: &str) -> rusqlite::Result<Connection> {
    let connection = Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    connection.busy_timeout(BUSY_TIMEOUT)?;

    // sqlite3_open_v2 can succeed before SQLite tries to create/read the WAL shared-memory file.
    // Pin a read transaction now so a dormant read-only WAL database fails here and can use the
    // immutable fallback, while every later query observes one consistent live snapshot.
    connection.execute_batch("BEGIN DEFERRED")?;
    connection.query_row("SELECT COUNT(*) FROM sqlite_schema", [], |row| {
        row.get::<_, i64>(0)
    })?;
    Ok(connection)
}

pub(crate) fn read_only_uri(path: &Path) -> String {
    format!("file:{}?mode=ro", escaped_path(path))
}

pub(crate) fn immutable_uri(path: &Path) -> String {
    format!("file:{}?immutable=1", escaped_path(path))
}

fn escaped_path(path: &Path) -> String {
    path.to_string_lossy()
        .replace('%', "%25")
        .replace(' ', "%20")
        .replace('?', "%3F")
        .replace('#', "%23")
}

fn fallback_warning(path: &Path, harness_name: &str) -> Option<String> {
    let wal_path = sidecar_path(path, "-wal");
    match std::fs::metadata(&wal_path) {
        Ok(metadata) if metadata.len() > 0 => Some(format!(
            "WARNING: {harness_name} database required SQLite immutable fallback; {} contains {} bytes of WAL that may be invisible to this import",
            wal_path.display(),
            metadata.len()
        )),
        Ok(_) => None,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => Some(format!(
            "WARNING: {harness_name} database required SQLite immutable fallback, but {} could not be inspected ({error}); committed WAL data may be invisible to this import",
            wal_path.display()
        )),
    }
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = OsString::from(path.as_os_str());
    value.push(suffix);
    PathBuf::from(value)
}
