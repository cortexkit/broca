//! Temporary cutover bookkeeping, independent of the export and address inventory database.
//! Each intent commits before its filesystem operation, so recovery can finish an interrupted
//! publish or backup without converting a second copy. Remove this database with the converter
//! after its final counts have been recorded in the release notes.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

pub const FILE: &str = "conversion-ledger.db";

/// One source log: a live file, or one member of a sealed archive container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub id: String,
    pub path: String,
    pub member: Option<String>,
    pub class: String,
    pub destination: String,
    pub backup: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub source: Source,
    pub step: String,
    pub outcome: Option<String>,
    pub refusal: Option<String>,
    /// Bound under the session fence before staging; empty while only discovered/recovering.
    pub source_sha256: String,
}

#[derive(Debug, Default, Clone, Serialize, PartialEq, Eq)]
pub struct Progress {
    pub sources: u64,
    pub steps: BTreeMap<String, u64>,
    pub outcomes: BTreeMap<String, u64>,
    pub refusals: BTreeMap<String, u64>,
}

pub struct Ledger {
    connection: Mutex<Connection>,
}

impl Ledger {
    pub fn open(root: &Path) -> Result<Self, String> {
        let path = root.join(FILE);
        broca_store::create_private_sqlite(&path).map_err(|error| error.to_string())?;
        broca_owner_only::validate(&path).map_err(|error| error.to_string())?;
        let connection = Connection::open(path).map_err(|error| error.to_string())?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=FULL;
                 CREATE TABLE IF NOT EXISTS sources (
                     source_id TEXT PRIMARY KEY,
                     source_path TEXT NOT NULL,
                     member TEXT,
                     storage_class TEXT NOT NULL,
                     step TEXT NOT NULL,
                     outcome TEXT,
                     refusal_class TEXT,
                     source_sha256 TEXT NOT NULL,
                     destination TEXT NOT NULL,
                     backup TEXT NOT NULL,
                     created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                     updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
                 );
                 CREATE INDEX IF NOT EXISTS sources_path ON sources(source_path);
                 CREATE INDEX IF NOT EXISTS sources_path_step ON sources(source_path,step);",
            )
            .map_err(|error| error.to_string())?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    /// Discovery is idempotent: a restart never resets a committed intent or final outcome.
    pub fn discover(&self, source: &Source) -> Result<(), String> {
        self.connection
            .lock()
            .map_err(|error| error.to_string())?
            .execute(
                "INSERT INTO sources
                    (source_id,source_path,member,storage_class,step,destination,backup,source_sha256)
                 VALUES (?1,?2,?3,?4,'discovered',?5,?6,'')
                 ON CONFLICT(source_id) DO NOTHING",
                params![
                    source.id,
                    source.path,
                    source.member,
                    source.class,
                    source.destination,
                    source.backup
                ],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Record the next intent before beginning it. A failed row retains its refusal class so
    /// session admission can explain why the source remains a legacy WAL.
    pub fn advance(
        &self,
        id: &str,
        step: &str,
        outcome: Option<&str>,
        refusal: Option<&str>,
    ) -> Result<(), String> {
        let changed = self
            .connection
            .lock()
            .map_err(|error| error.to_string())?
            .execute(
                "UPDATE sources SET step=?2,outcome=?3,refusal_class=?4,
                    updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE source_id=?1",
                params![id, step, outcome, refusal],
            )
            .map_err(|error| error.to_string())?;
        if changed != 1 {
            return Err(format!("conversion source {id} is absent from the ledger"));
        }
        Ok(())
    }

    pub fn bind_source(&self, id: &str, sha256: &str) -> Result<(), String> {
        let changed = self.connection.lock().map_err(|error| error.to_string())?.execute(
            "UPDATE sources SET source_sha256=?2,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE source_id=?1",
            params![id,sha256],
        ).map_err(|error| error.to_string())?;
        if changed != 1 {
            return Err(format!("conversion source {id} is absent from the ledger"));
        }
        Ok(())
    }

    pub fn row(&self, id: &str) -> Result<Option<Row>, String> {
        self.connection
            .lock()
            .map_err(|error| error.to_string())?
            .query_row(
                "SELECT source_id,source_path,member,storage_class,destination,backup,
                        step,outcome,refusal_class,source_sha256 FROM sources WHERE source_id=?1",
                [id],
                read_row,
            )
            .optional()
            .map_err(|error| error.to_string())
    }

    pub fn unfinished_path(&self, path: &str) -> Result<bool, String> {
        self.connection.lock().map_err(|error| error.to_string())?.query_row("SELECT EXISTS(SELECT 1 FROM sources WHERE source_path=?1 AND step IN ('discovered','recovering','staging','verifying','verified','publishing','failed'))", [path], |row| row.get(0)).map_err(|error| error.to_string())
    }

    pub fn rows_for_path(&self, path: &str) -> Result<Vec<Row>, String> {
        let connection = self.connection.lock().map_err(|error| error.to_string())?;
        let mut statement = connection.prepare("SELECT source_id,source_path,member,storage_class,destination,backup,step,outcome,refusal_class,source_sha256 FROM sources WHERE source_path=?1 ORDER BY source_id").map_err(|error| error.to_string())?;
        let rows = statement
            .query_map([path], read_row)
            .map_err(|error| error.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        Ok(rows)
    }

    pub fn rows(&self) -> Result<Vec<Row>, String> {
        let connection = self.connection.lock().map_err(|error| error.to_string())?;
        let mut statement = connection
            .prepare(
                "SELECT source_id,source_path,member,storage_class,destination,backup,
                             step,outcome,refusal_class,source_sha256 FROM sources ORDER BY source_id",
            )
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map([], read_row)
            .map_err(|error| error.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        Ok(rows)
    }

    /// SQL aggregation keeps a health read bounded by the ledger, not by WAL replay.
    pub fn progress(&self) -> Result<Progress, String> {
        let connection = self.connection.lock().map_err(|error| error.to_string())?;
        let mut progress = Progress::default();
        for (column, counts) in [
            ("step", &mut progress.steps),
            ("outcome", &mut progress.outcomes),
            ("refusal_class", &mut progress.refusals),
        ] {
            let mut statement = connection
                .prepare(&format!("SELECT {column},count(*) FROM sources WHERE {column} IS NOT NULL GROUP BY {column}"))
                .map_err(|error| error.to_string())?;
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
                })
                .map_err(|error| error.to_string())?;
            for row in rows {
                let (name, count) = row.map_err(|error| error.to_string())?;
                counts.insert(name, count);
            }
        }
        progress.sources = progress.steps.values().sum();
        Ok(progress)
    }
}

fn read_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Row> {
    Ok(Row {
        source: Source {
            id: row.get(0)?,
            path: row.get(1)?,
            member: row.get(2)?,
            class: row.get(3)?,
            destination: row.get(4)?,
            backup: row.get(5)?,
        },
        step: row.get(6)?,
        outcome: row.get(7)?,
        refusal: row.get(8)?,
        source_sha256: row.get(9)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversion_ledger_resumes_committed_intents_and_counts_refusals() {
        let root = broca_test_support::scratch_root().join("conversion-ledger");
        std::fs::create_dir_all(&root).unwrap();
        let source = Source {
            id: "wal/one.wal".into(),
            path: "wal/one.wal".into(),
            member: None,
            class: "ordinary".into(),
            destination: "session-store/ordinary".into(),
            backup: "/backup/wal/one.wal".into(),
        };
        {
            let ledger = Ledger::open(&root).unwrap();
            ledger.discover(&source).unwrap();
            ledger.bind_source(&source.id, &"a".repeat(64)).unwrap();
            ledger
                .advance(&source.id, "backing_up", Some("refused"), Some("damaged"))
                .unwrap();
        }
        let ledger = Ledger::open(&root).unwrap();
        ledger.discover(&source).unwrap();
        let row = ledger.row(&source.id).unwrap().unwrap();
        assert_eq!(row.step, "backing_up");
        assert_eq!(row.source_sha256, "a".repeat(64));
        assert_eq!(row.refusal.as_deref(), Some("damaged"));
        assert_eq!(ledger.rows().unwrap(), vec![row]);
        let progress = ledger.progress().unwrap();
        assert_eq!(progress.sources, 1);
        assert_eq!(progress.outcomes.get("refused"), Some(&1));
        assert_eq!(progress.refusals.get("damaged"), Some(&1));
        assert!(ledger.advance("missing", "done", None, None).is_err());
        let connection = ledger.connection.lock().unwrap();
        let mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        let sync: u64 = connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
        assert_eq!(sync, 2);
        broca_owner_only::validate(root.join(FILE)).unwrap();
    }
}
