//! Durable identity of the history snapshots a session has exposed.

use broca_protocol::BindIdentity;

pub const HISTORY_REWRITING: &str = "history_rewriting";
pub const HISTORY_CHANGED: &str = "history_changed";

/// Kept outside logs so deleting or repairing a log cannot reuse a served version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServedSnapshot {
    pub history_version: u64,
    pub incarnation: String,
}

pub trait ServedHistory: Send + Sync {
    /// Read-only preflight: refuses a rewrite fence before any transcript page is read.
    fn history_read_version(&self, address: &str) -> Result<Option<u64>, String>;

    /// Register a log before serving it. The outer expected version distinguishes an
    /// unguarded maintenance lookup from a read that observed no row at preflight.
    /// An absent log never creates a row.
    fn served_snapshot(&self, address: &str, has_log: bool, lineage: Option<&str>, expected_version: Option<Option<u64>>) -> Result<ServedSnapshot, String>;

    /// Called under the writer lease before a destructive rewrite; version and fence sync together.
    fn history_rewritten(&self, session: &BindIdentity, lineage: Option<&str>) -> Result<(), String>;

    /// Clear only after the rewritten log and recovery closure are durable.
    fn history_rewrite_finished(&self, session: &BindIdentity) -> Result<(), String>;

    /// Indexed boot discovery of only sessions whose writer must finish a crashed rewrite.
    fn rewriting_sessions(&self) -> Result<Vec<BindIdentity>, String>;
}
