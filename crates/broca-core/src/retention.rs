//! The first run, not the most recent send, fixes whether a lineage may persist content.

use broca_wal::{FramedRecord, WalRecord};
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

/// Recognize both sequence origins supported by the materialized-store API.
pub(crate) fn starts_at_lineage_origin(records: &[FramedRecord]) -> bool {
    // The materialized-store API has always accepted zero-based record vectors;
    // file WALs start at one. Either origin can establish a complete prefix;
    // a later sequence is only a suffix and cannot establish its first run.
    records.first().is_some_and(|frame| frame.seq <= 1)
}

/// A complete prefix overrides stale metadata. A suffix requires a matching
/// checkpoint proof; an unstarted proof is usable only for a contiguous suffix.
pub(crate) fn projection_policy(
    records: &[FramedRecord],
    prior: Option<(LineageRetention, u64)>,
) -> Option<LineageRetention> {
    let includes_origin_run = records
        .first()
        .is_some_and(|frame| matches!(frame.record, WalRecord::RunStarted { .. }));
    let prior = if starts_at_lineage_origin(records) && (prior.is_none() || includes_origin_run) {
        Some(LineageRetention::Unstarted)
    } else {
        // An unstarted checkpoint proves absence only through its watermark.
        // A gapped suffix could skip the first run and cannot borrow that proof.
        prior
            .filter(|(policy, watermark)| {
                *policy != LineageRetention::Unstarted
                    || records
                        .first()
                        .is_some_and(|frame| frame.seq == watermark.saturating_add(1))
            })
            .map(|(policy, _)| policy)
    };
    match prior {
        Some(LineageRetention::Unstarted) => {
            let first = records.iter().find_map(|frame| match &frame.record {
                WalRecord::RunStarted { retention, .. } => Some(*retention),
                _ => None,
            });
            Some(if is_retention_lineage(records) {
                LineageRetention::Retained {
                    seconds: first.flatten().expect("the first run carries retention"),
                }
            } else if first.is_some() {
                LineageRetention::Forever
            } else {
                LineageRetention::Unstarted
            })
        }
        other => other,
    }
}

#[cfg(test)]
pub(crate) mod tests;
