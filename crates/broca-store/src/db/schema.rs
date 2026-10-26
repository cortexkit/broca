//! Content-free, rebuildable rows. Authoritative tables are owned by their writers.

use rusqlite::Connection;

pub(super) const SESSION_TABLES: &[&str] = &["message", "episode", "run", "setup"];
pub(super) const REBUILDABLE_TABLES: &[&str] = &[
    "message",
    "episode",
    "run",
    "setup",
    "session",
    "condemned",
    "address_floor",
];

pub(super) fn initialize(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS session (
            sid TEXT PRIMARY KEY, project_root TEXT NOT NULL, harness TEXT NOT NULL,
            session_key TEXT NOT NULL, session_ref TEXT NOT NULL,
            lineage_id TEXT, lineage_off INTEGER, location TEXT NOT NULL CHECK(location IN ('live','archived')),
            indexed_seq INTEGER, indexed_off INTEGER NOT NULL, indexed_end INTEGER NOT NULL,
            indexed_identity BLOB, head_ord INTEGER NOT NULL,
            tail_seed TEXT NOT NULL, admission_seed TEXT NOT NULL,
            frozen_policy INTEGER, last_activity_ms INTEGER, last_write_ms INTEGER,
            released_at_ms INTEGER, delete_after_ms INTEGER, title_off INTEGER,
            UNIQUE(project_root, harness, session_key)
        );
        CREATE TABLE IF NOT EXISTS episode (
            sid TEXT NOT NULL REFERENCES session(sid) ON DELETE CASCADE,
            n INTEGER NOT NULL, run_id TEXT, attributable INTEGER NOT NULL,
            run_started_seq INTEGER, run_started_off INTEGER,
            first_ord INTEGER NOT NULL, step_first_ord INTEGER, end_ord INTEGER NOT NULL,
            tag_seed TEXT NOT NULL, terminal_off INTEGER, terminal_kind TEXT,
            PRIMARY KEY(sid,n)
        );
        CREATE TABLE IF NOT EXISTS message (
            sid TEXT NOT NULL REFERENCES session(sid) ON DELETE CASCADE,
            ord INTEGER NOT NULL, lo_seq INTEGER NOT NULL, lo_off INTEGER NOT NULL,
            hi_off INTEGER NOT NULL, input_start INTEGER NOT NULL, input_end INTEGER NOT NULL,
            tag_base INTEGER, tag_n INTEGER NOT NULL, tag_high INTEGER NOT NULL, flags INTEGER NOT NULL,
            PRIMARY KEY(sid,ord)
        );
        CREATE TABLE IF NOT EXISTS run (
            run_id TEXT PRIMARY KEY, sid TEXT NOT NULL REFERENCES session(sid) ON DELETE CASCADE,
            episode INTEGER NOT NULL, state TEXT NOT NULL, state_seq INTEGER NOT NULL,
            last_step_off INTEGER, terminal_off INTEGER, submission_id TEXT,
            request_identity_digest TEXT
        );
        CREATE TABLE IF NOT EXISTS setup (
            sid TEXT NOT NULL REFERENCES session(sid) ON DELETE CASCADE,
            digest TEXT NOT NULL, off INTEGER NOT NULL, PRIMARY KEY(sid,digest)
        );
        CREATE TABLE IF NOT EXISTS condemned (
            addr TEXT NOT NULL, lineage_id TEXT NOT NULL, removed_at_ms INTEGER NOT NULL,
            PRIMARY KEY(addr,lineage_id)
        );
        CREATE TABLE IF NOT EXISTS address_floor (
            addr TEXT PRIMARY KEY, tag_floor INTEGER NOT NULL, notice_floor INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS log_reader (
            module_id TEXT PRIMARY KEY, first_seen_ms INTEGER NOT NULL,
            source TEXT NOT NULL CHECK(source IN ('config','declared'))
        );",
    )
}
