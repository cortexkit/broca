//! Checks that loading from a checkpoint gives the same state as folding a whole history.
//!
//! A log's checkpoint carries the state its earlier records folded to, so a load folds only the
//! log's own records on top of it. That is correct only if, at every point a log could start,
//! "fold everything before it, write the checkpoint, read it back, fold the rest" ends in the
//! same state as "fold everything". This module runs that comparison on a converted log at
//! chosen split points, comparing the two states by their encoded bytes, which cover every
//! field of both folds.

use broca_session_store::checkpoint::Checkpoint;
use broca_session_store::codec::Message as _;
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::record::{Record, Stream};

fn fold_into(
    conversation: &mut Conversation,
    operations: &mut Operations,
    record: &Record,
) -> Result<(), String> {
    match record.stream() {
        Stream::Transcript => conversation
            .apply(record)
            .map_err(|e| format!("conversation: {e:?}")),
        Stream::Ledger => operations
            .apply(record)
            .map_err(|e| format!("operations: {e:?}")),
    }
}

/// The result of checking one log.
#[derive(Debug, Default)]
pub struct SplitCheck {
    /// Split points checked.
    pub points: u64,
    /// Split points where the state loaded from the checkpoint differs from the whole fold.
    pub differing: Vec<usize>,
}

/// Splits `records` before each index in `points` (an index equal to `records.len()` splits
/// after the last record) and checks each split against folding every record.
/// `appends_in_conversation` is the conversation fold's setting for this session. A fold that
/// refuses the records is an error, not a difference.
pub fn check_splits(
    records: &[Record],
    appends_in_conversation: bool,
    points: &[usize],
) -> Result<SplitCheck, String> {
    let mut conversation = Conversation::new(appends_in_conversation);
    let mut operations = Operations::default();
    for record in records {
        fold_into(&mut conversation, &mut operations, record)?;
    }
    let expected = (conversation.encode(), operations.encode());

    let mut check = SplitCheck::default();
    let mut running = (
        Conversation::new(appends_in_conversation),
        Operations::default(),
    );
    for split in 0..=records.len() {
        if points.contains(&split) {
            check.points += 1;
            let checkpoint = Checkpoint {
                covers_through: split as u64,
                next_seq: split as u64 + 1,
                conversation: running.0.clone(),
                operational: running.1.clone(),
                transcript_base: None,
                ledger_base: None,
            };
            let loaded = Checkpoint::decode(&checkpoint.encode())
                .map_err(|e| format!("checkpoint at {split} doesn't decode: {e}"))?;
            let (mut conversation, mut operations) = (loaded.conversation, loaded.operational);
            for record in &records[split..] {
                fold_into(&mut conversation, &mut operations, record)?;
            }
            if (conversation.encode(), operations.encode()) != expected {
                check.differing.push(split);
            }
        }
        if let Some(record) = records.get(split) {
            fold_into(&mut running.0, &mut running.1, record)?;
        }
    }
    Ok(check)
}

/// Every split point for a short log; for a long one, its two ends and `inner` points spread
/// evenly between them.
pub fn split_points(len: usize, every_up_to: usize, inner: usize) -> Vec<usize> {
    if len <= every_up_to {
        return (0..=len).collect();
    }
    let mut points: Vec<usize> = (0..=inner + 1).map(|i| i * len / (inner + 1)).collect();
    points.dedup();
    points
}

#[cfg(test)]
mod tests;
