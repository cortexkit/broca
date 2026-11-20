//! Retained spend and grants have their own checkpoint sequence. Advancing it must not
//! advance the message store's watermark, which would hide unmaterialized WAL messages.

use crate::{SqliteStore, StoreError};

pub(crate) fn init(conn: &rusqlite::Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS retained_head (
        lineage_id TEXT PRIMARY KEY, session TEXT NOT NULL, at_seq INTEGER NOT NULL,
        facts_json TEXT NOT NULL
    );",
    )?;
    let has_version = conn
        .prepare("PRAGMA table_info(retained_head)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .any(|name| name == "sender_admission_identity");
    if !has_version {
        // Older retained-only heads have no admission checkpoint to reveal a refresh they
        // ignored. Invalidate those caches once, without changing unrefreshed head JSON.
        conn.execute_batch("ALTER TABLE retained_head ADD COLUMN sender_admission_identity TEXT;")?;
    }
    Ok(())
}

impl SqliteStore {}
