//! Literal pre-merge schema fragments shared by the SQLite and Postgres compatibility tests.
//! These do not read the current migrations: omitting a new migration must leave a missing
//! column/table in one of the two upgrade paths.

/// Common tables before approval projection and retention were added. The integer type and
/// retry-column declaration differ between the two database engines.
pub fn common_schema(integer: &str, meta_retries: &str) -> String {
    format!(
        "CREATE TABLE meta (
            session TEXT PRIMARY KEY, watermark_seq {integer} NOT NULL, lineage_id TEXT,
            control_head_wal_seq {integer}, control_head_sub_index {integer},
            context_epoch {integer} NOT NULL, completed_steps {integer} NOT NULL DEFAULT 0,
            usage_json TEXT NOT NULL DEFAULT '{{}}', retries_used {meta_retries},
            last_step_usage_json TEXT, last_step_finish_reason_json TEXT, last_run_id TEXT,
            last_episode_usage_json TEXT, wal_anchor_file_id TEXT, wal_anchor_seq {integer},
            wal_anchor_byte_end {integer}, prefix_len {integer}, admission_checkpoint_json TEXT,
            last_title TEXT
        );
        CREATE TABLE message (
            session TEXT NOT NULL, ord {integer} NOT NULL, json TEXT NOT NULL,
            PRIMARY KEY(session, ord)
        );
        CREATE TABLE run_index (
            run_id TEXT PRIMARY KEY, session TEXT NOT NULL, submission_id TEXT,
            request_identity_digest TEXT, state TEXT NOT NULL, usage_json TEXT,
            final_step_finish_reason_json TEXT, retries_used {integer} NOT NULL DEFAULT 0,
            state_changed_ms {integer}, title TEXT
        );
        INSERT INTO run_index(run_id, session, state, title) VALUES
            ('existing', '{{}}', 'completed', 'keep this title');"
    )
}

/// Retention-only release: no approval projection columns yet.
pub fn retention_schema(integer: &str) -> String {
    format!(
        "ALTER TABLE run_index ADD COLUMN lineage_id TEXT;
        ALTER TABLE run_index ADD COLUMN expired_at_ms {integer};
        CREATE TABLE retention_lineage (
            lineage_id TEXT PRIMARY KEY, session TEXT NOT NULL,
            retention_s {integer} NOT NULL, last_activity_ms {integer} NOT NULL,
            expires_at_ms {integer}, wal_bytes {integer} NOT NULL, expired_at_ms {integer}
        );
        CREATE INDEX retention_lineage_expires_idx ON retention_lineage(expires_at_ms)
            WHERE expires_at_ms IS NOT NULL;
        CREATE UNIQUE INDEX retention_lineage_live_session_idx ON retention_lineage(session)
            WHERE expired_at_ms IS NULL;
        INSERT INTO retention_lineage VALUES ('old', 'triple', 60, 100, 60100, 1024, NULL);"
    )
}

/// Approval-projection-only release: no retention table or run-index expiry columns yet.
pub fn deferred_schema(flag: &str) -> String {
    format!(
        "ALTER TABLE meta ADD COLUMN deferred_calls_json TEXT;
        ALTER TABLE run_index ADD COLUMN withdrawals_owed {flag};"
    )
}
