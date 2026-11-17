//! A portable single-session SQLite snapshot, never a copy of the host's whole database.

use std::error::Error;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use rusqlite::{params_from_iter, types::Value, Connection, OptionalExtension};
use sha2::{Digest, Sha256};

use crate::opencode::sqlite_snapshot_error;
use crate::sqlite_read::open_read_only_database;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

/// The source descriptor Core passes to broca, plus the source's message count and newest id.
#[derive(Debug)]
pub struct Extracted {
    pub path: PathBuf,
    pub sha256: String,
    pub messages: u64,
    pub newest_mid: Option<String>,
}

/// Extracts only `session`, `message` and `part`, preserving their table schemas, columns and
/// explicit indexes. The source connection pins one read-only snapshot, including committed
/// WAL frames. Busy or torn snapshots report the same retryable error as the transcript reader.
pub fn extract(db: &Path, session: &str, out_dir: &Path) -> Result<Extracted> {
    let source = open_read_only_database(db, "OpenCode")
        .map_err(|error| sqlite_snapshot_error("open the extract snapshot", error))?;
    if let Some(warning) = source.fallback_warning {
        // An extract must be complete, not silently omit a WAL it could not read.
        return Err(warning.into());
    }
    let connection = source.connection;
    let exists = connection
        .query_row("SELECT 1 FROM session WHERE id=?1", [session], |_| Ok(()))
        .optional()
        .map_err(|error| sqlite_snapshot_error("find the extracted session", error))?;
    if exists.is_none() {
        return Err(format!("OpenCode session {session:?} was not found").into());
    }
    broca_owner_only::create_dir_all(out_dir)?;
    // A digest of the id is a filename, never an untrusted path component.
    let name = format!("opencode1-{:x}.db", Sha256::digest(session.as_bytes()));
    let path = out_dir.join(name);
    if path.exists() {
        return Err(format!("extract destination {} already exists", path.display()).into());
    }
    let temp = out_dir.join(format!(
        ".opencode1-{}-{}.tmp",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let file = broca_owner_only::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    drop(file);
    let result = (|| {
        let mut target = Connection::open(&temp)?;
        // The extract keeps three tables, and their foreign keys name tables it leaves out
        // (OpenCode's `session.project_id` references `project`). Enforced here, those keys
        // would refuse every copied row; the source database enforced them when it wrote them.
        target.execute_batch(
            "PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA foreign_keys=OFF;",
        )?;
        let transaction = target.transaction()?;
        for (table, predicate) in [
            ("session", "id"),
            ("message", "session_id"),
            ("part", "session_id"),
        ] {
            let schema: String = connection
                .query_row(
                    "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1",
                    [table],
                    |row| row.get(0),
                )
                .map_err(|error| sqlite_snapshot_error("read the extract schema", error))?;
            transaction.execute_batch(&schema)?;
            let mut select = connection
                .prepare(&format!(
                    "SELECT * FROM \"{table}\" WHERE \"{predicate}\"=?1"
                ))
                .map_err(|error| sqlite_snapshot_error("prepare the extract rows", error))?;
            let columns = select.column_count();
            let placeholders = vec!["?"; columns].join(",");
            let mut insert =
                transaction.prepare(&format!("INSERT INTO \"{table}\" VALUES ({placeholders})"))?;
            let mut rows = select
                .query([session])
                .map_err(|error| sqlite_snapshot_error("read the extract rows", error))?;
            while let Some(row) = rows
                .next()
                .map_err(|error| sqlite_snapshot_error("copy the extract rows", error))?
            {
                let values = (0..columns)
                    .map(|index| row.get::<_, Value>(index))
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                insert.execute(params_from_iter(values))?;
            }
            let mut indexes = connection.prepare("SELECT sql FROM sqlite_schema WHERE type='index' AND tbl_name=?1 AND sql IS NOT NULL")?;
            for sql in indexes.query_map([table], |row| row.get::<_, String>(0))? {
                transaction.execute_batch(&sql?)?;
            }
        }
        transaction.commit()?;
        target.close().map_err(|(_, error)| error)?;
        // Opened for writing: Windows refuses to flush a read-only handle.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&temp)?
            .sync_all()?;
        let messages = connection.query_row(
            "SELECT COUNT(*) FROM message WHERE session_id=?1",
            [session],
            |row| row.get(0),
        )?;
        let newest_mid = connection.query_row("SELECT id FROM message WHERE session_id=?1 ORDER BY time_created DESC, id DESC LIMIT 1", [session], |row| row.get(0)).optional()?;
        let sha256 = sha256_file(&temp)?;
        std::fs::rename(&temp, &path)?;
        broca_records::fs_ops::sync_dir(out_dir)?;
        Ok(Extracted {
            path,
            sha256,
            messages,
            newest_mid,
        })
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

/// Hashes a source with bounded memory, before a format reader opens it.
pub fn sha256_file(path: &Path) -> std::io::Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        digest.write_all(&buffer[..n])?;
    }
    Ok(format!("{:x}", digest.finalize()))
}
