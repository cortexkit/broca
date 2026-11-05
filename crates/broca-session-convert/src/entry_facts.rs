//! Entry facts for sessions on the current compaction contract.
//!
//! Today's `session.entries` derives what it serves about each message (its time, part numbers,
//! notice, usage, the view in force, who delivered text with its tool calls) by folding the whole
//! log every time. The new format decides these once, when the message is placed, and records
//! them in an `EntryFacts` transcript record. For a converted session the converter computes
//! them here with today's code, in the same steps today's projection takes
//! (`broca_core::compaction_v2::transcript`), so the new records serve exactly what today's
//! projection serves.

use std::collections::{BTreeMap, HashMap};

use broca_core::compaction_v2::metadata::{Fold, Metadata};
use broca_session_store::record::{EntryFacts, Notice, StepUsage, ViewRef};
use broca_wal::{FramedRecord, WalRecord};

/// What today's projection derives for a mode-3 log.
#[derive(Debug, Default)]
pub struct Legacy {
    /// By the message's position in today's conversation, from 0: its metadata, time and id.
    pub rows: BTreeMap<u64, (Metadata, u64, String)>,
    /// Who delivered the text a tool-result delivery carried, by the seq of today's record
    /// that delivered it.
    pub delivery_notices: HashMap<u64, Notice>,
    /// The tool-results message of an assistant message still waiting in today's projection,
    /// at the position after it. Today's projection places a tool turn when its batch ends; the
    /// new format places it when its last call has a result, so a log a crash cut between the
    /// two places this message only in the new format. Its facts are today's for that message
    /// when the batch ends: the part numbers so far, and no parts of its own.
    pub pending_results: Option<(u64, (Metadata, u64, String))>,
}

/// Today's facts for `old`, or `None` when it isn't a mode-3 log. `lineage` is the log's lineage
/// id, which today's message ids carry; a mode-3 log without one can't keep its ids.
pub fn legacy(old: &[FramedRecord], lineage: Option<&str>) -> Result<Option<Legacy>, String> {
    let raw: Vec<WalRecord> = old.iter().map(|frame| frame.record.clone()).collect();
    if !broca_core::is_mode3_log(&raw) {
        return Ok(None);
    }
    let stored = broca_core::replay(&raw)
        .map_err(|error| error.to_string())?
        .prompt
        .len() as u64;
    let mut times = broca_core::message_times::mode3_written_times(
        raw.iter(),
        broca_core::placement::Placement::default(),
        BTreeMap::new(),
        Default::default(),
    )?
    .times;
    let mut fold = Fold::default();
    let mut rows = BTreeMap::new();
    for frame in old {
        rows.extend(fold.observe(frame)?.rows);
    }
    let mut placed = stored;
    let mut pending_results = None;
    // An assistant message still waiting for its tool results is served at its reserved
    // position, with the time of the step that wrote it.
    if let Some((position, metadata)) = fold.pending_assistant() {
        if position != stored {
            return Err("pending assistant lost its reserved position".into());
        }
        let source = old
            .iter()
            .position(|frame| frame.seq == metadata.source_seq)
            .ok_or("pending assistant source is missing")?;
        if let WalRecord::ModelStepFinished { written_at_ms, .. } = &old[source].record {
            let prior = broca_core::message_times::mode3_written_times(
                old[..=source].iter().map(|frame| &frame.record),
                broca_core::placement::Placement::default(),
                BTreeMap::new(),
                Default::default(),
            )?;
            times.insert(
                position,
                written_at_ms
                    .or(prior.seed.last_record_ms)
                    .unwrap_or(prior.seed.last_entry_ms),
            );
        }
        rows.insert(position, metadata.clone());
        placed += 1;
        pending_results = Some((
            position + 1,
            Metadata {
                tag_base: fold.high(),
                source_seq: metadata.source_seq,
                render_tags: metadata.render_tags,
                ..Metadata::default()
            },
        ));
    }
    let mut legacy = Legacy::default();
    let mut prior = 0;
    for position in 0..placed {
        let metadata = rows
            .remove(&position)
            .ok_or_else(|| format!("metadata missing for message {position}"))?;
        prior = times.get(&position).copied().unwrap_or(prior);
        for delivery in &metadata.deliveries {
            legacy.delivery_notices.insert(
                delivery.seq,
                Notice {
                    sender: delivery.sender.clone(),
                    template: delivery.template.clone(),
                },
            );
        }
        let mid = broca_core::message_mid(true, lineage, position)?;
        legacy.rows.insert(position, (metadata, prior, mid));
    }
    if let Some((position, metadata)) = pending_results {
        let mid = broca_core::message_mid(true, lineage, position)?;
        legacy.pending_results = Some((position, (metadata, prior, mid)));
    }
    Ok(Some(legacy))
}

/// The record for the message at `ordinal` (counting from 1).
pub fn record(ordinal: u64, metadata: &Metadata, written_at_ms: u64, mid: String) -> EntryFacts {
    let usage = metadata.usage.map(|usage| {
        // `StepUsage` can't be built field by field outside its crate; its JSON form is its
        // contract.
        let value = serde_json::to_value(usage).expect("usage serializes");
        let count = |key: &str| value.get(key).and_then(serde_json::Value::as_u64);
        StepUsage {
            prompt_tokens: count("prompt_tokens"),
            cache_read_tokens: count("cache_read_tokens"),
            cache_write_tokens: count("cache_write_tokens"),
            output_tokens: count("output_tokens"),
            reasoning_tokens: count("reasoning_tokens"),
        }
    });
    EntryFacts {
        ordinal,
        written_at_ms,
        tag_base: metadata.tag_base,
        notice: metadata.notice.as_ref().map(|notice| Notice {
            sender: notice.sender.clone(),
            template: notice.template.clone(),
        }),
        usage,
        generated_under: metadata.generated_under.as_ref().map(|view| ViewRef {
            version: view.version,
            checksum: view.checksum.clone(),
        }),
        native: metadata.source_input.is_none(),
        render_tags: metadata.render_tags,
        clean_reply: metadata.clean_reply,
        mid,
    }
}
