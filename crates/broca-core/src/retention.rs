//! The first run, not the most recent send, fixes whether a lineage may persist content.

use broca_records::record::WalRecord;
use broca_wal::FramedRecord;
use serde::{Deserialize, Serialize};

/// A checkpoint's proof of the policy frozen by the first `RunStarted`.
/// Absence of this proof in an older checkpoint means unknown, never forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LineageRetention {
    Unstarted,
    Forever,
    Retained { seconds: u64 },
}

/// Decide from a complete lineage read. A later retention value only shortens the
/// policy; it must not change the directory or permit content projection.
pub fn is_retention_lineage(records: &[FramedRecord]) -> bool {
    is_retention_records(records.iter().map(|frame| &frame.record))
}

pub(crate) fn is_retention_records<'a>(records: impl IntoIterator<Item = &'a WalRecord>) -> bool {
    records.into_iter().find_map(|record| match record {
        WalRecord::RunStarted { retention, .. } => Some(retention.is_some()),
        _ => None,
    }) == Some(true)
}
