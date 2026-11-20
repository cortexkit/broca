//! Shared additive index definitions for bounded session-history evidence reads.

use crate::SqliteStore;
use rusqlite::Connection;

pub(crate) const RUNS: &str = "session_log_evidence_runs";
pub(crate) const DELETION_IDENTITIES: &str = "session_log_evidence_deletion_identity";
pub(crate) const RETAINED: &str = "session_log_evidence_retained";
pub(crate) const RETENTION: &str = "session_log_evidence_retention";
pub(crate) const DELETION_SESSIONS: &str = "session_log_evidence_deletion_session";
const IDENTITY: &str = "json_extract(session,'$.project_root'),json_extract(session,'$.harness'),json_extract(session,'$.session')";

struct Definition {
    name: &'static str,
    table: &'static str,
    keys: Keys,
    optional_lineage: bool,
}

enum Keys {
    Runs,
    DeletionIdentity,
    SessionLineage,
    Retention,
}

const DEFINITIONS: &[Definition] = &[
    Definition {
        name: RUNS,
        table: "run_index",
        keys: Keys::Runs,
        optional_lineage: false,
    },
    Definition {
        name: DELETION_IDENTITIES,
        table: "retention_deletion",
        keys: Keys::DeletionIdentity,
        optional_lineage: false,
    },
    Definition {
        name: RETAINED,
        table: "retained_head",
        keys: Keys::SessionLineage,
        optional_lineage: false,
    },
    Definition {
        name: RETENTION,
        table: "retention_lineage",
        keys: Keys::Retention,
        optional_lineage: false,
    },
    Definition {
        name: DELETION_SESSIONS,
        table: "retention_deletion",
        keys: Keys::SessionLineage,
        optional_lineage: false,
    },
    Definition {
        name: "session_log_evidence_message",
        table: "message",
        keys: Keys::SessionLineage,
        optional_lineage: true,
    },
    Definition {
        name: "session_log_evidence_message_tag",
        table: "message_tag",
        keys: Keys::SessionLineage,
        optional_lineage: true,
    },
    Definition {
        name: "session_log_evidence_episode_span",
        table: "episode_span",
        keys: Keys::SessionLineage,
        optional_lineage: true,
    },
];

impl Definition {
    fn sql(&self) -> String {
        let (keys, predicate) = match self.keys {
            Keys::Runs => (
                format!("{IDENTITY},expired_at_ms,lineage_id COLLATE BINARY,run_id"),
                " WHERE json_valid(session)",
            ),
            Keys::DeletionIdentity => (
                format!("{IDENTITY},lineage_id COLLATE BINARY"),
                " WHERE json_valid(session)",
            ),
            Keys::SessionLineage => (
                "session,lineage_id COLLATE BINARY".into(),
                if self.optional_lineage {
                    " WHERE lineage_id IS NOT NULL"
                } else {
                    ""
                },
            ),
            Keys::Retention => ("session,expired_at_ms,lineage_id COLLATE BINARY".into(), ""),
        };
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} ({keys}){predicate}",
            self.name, self.table
        )
    }

    fn applicable(&self, conn: &Connection) -> rusqlite::Result<bool> {
        if !self.optional_lineage {
            return Ok(true);
        }
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name='lineage_id')",
            [self.table],
            |row| row.get(0),
        )
    }
}

/// Normal initialization and explicit offline preparation execute this same DDL.
pub(crate) fn install(conn: &Connection) -> rusqlite::Result<()> {
    for definition in DEFINITIONS {
        if definition.applicable(conn)? {
            conn.execute_batch(&definition.sql())?;
        }
    }
    Ok(())
}

pub(crate) fn lineage_index(table: &str) -> Option<&'static str> {
    DEFINITIONS
        .iter()
        .find(|definition| {
            definition.table == table
                && matches!(definition.keys, Keys::SessionLineage | Keys::Retention)
        })
        .map(|definition| definition.name)
}

impl SqliteStore {}
