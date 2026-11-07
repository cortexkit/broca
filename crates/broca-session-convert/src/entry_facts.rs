//! Entry facts: what `session.read`, and on the current compaction contract `session.entries`,
//! serve about each message.
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

/// Today's facts for one message: its metadata, its written time and its id.
pub type Row = (Metadata, Option<u64>, String);

/// What today's projections derive for a log.
#[derive(Debug, Default)]
pub struct Legacy {
    /// Whether the log is off the current compaction contract, so only `session.read` facts are
    /// derived: the metadata is empty, and native messages are told by their record.
    pub page_only: bool,
    /// By the message's position in today's conversation, from 0: its metadata, time and id.
    pub rows: BTreeMap<u64, Row>,
    /// Who delivered the text a tool-result delivery carried, by the seq of today's record
    /// that delivered it.
    pub delivery_notices: HashMap<u64, Notice>,
    /// The tool-results message of an assistant message still waiting in today's projection,
    /// at the position after it. Today's projection places a tool turn when its batch ends; the
    /// new format places it when its last call has a result, so a log a crash cut between the
    /// two places this message only in the new format. Its facts are today's for that message
    /// when the batch ends: the part numbers so far, and no parts of its own.
    pub pending_results: Option<(u64, Row)>,
    /// By position: the run today's read names as the message's run (`message_runs`, and the
    /// running tool turn's run for an assistant message still waiting).
    pub runs: BTreeMap<u64, String>,
    /// By position: the creation time today's read serves (`creation_times::project`).
    pub created: BTreeMap<u64, u64>,
    /// By position: how a delivered send reached the message, as today's read names it beside
    /// the message's run (`message_runs`).
    pub deliveries: BTreeMap<u64, broca_protocol::tags::DeliveryAttribution>,
}

/// Today's facts for `old`. `lineage` is the log's lineage id, which today's message ids carry;
/// a mode-3 log without one can't keep its ids.
pub fn legacy(old: &[FramedRecord], lineage: Option<&str>) -> Result<Option<Legacy>, String> {
    let raw: Vec<WalRecord> = old.iter().map(|frame| frame.record.clone()).collect();
    if !broca_core::is_mode3_log(&raw) {
        return page_only(&raw, lineage).map(Some);
    }
    let replayed = broca_core::replay(&raw).map_err(|error| error.to_string())?;
    let stored = replayed.prompt.len() as u64;
    let mut runs: BTreeMap<u64, String> = replayed
        .message_runs
        .iter()
        .enumerate()
        .filter_map(|(position, run)| {
            run.as_ref()
                .map(|run| (position as u64, run.run_id.clone()))
        })
        .collect();
    let mut deliveries: BTreeMap<u64, broca_protocol::tags::DeliveryAttribution> = replayed
        .message_runs
        .iter()
        .enumerate()
        .filter_map(|(position, run)| {
            run.as_ref()?
                .delivery
                .clone()
                .map(|delivery| (position as u64, delivery))
        })
        .collect();
    let created = broca_core::creation_times::project(
        raw.iter(),
        broca_core::placement::Placement::default(),
        BTreeMap::new(),
        Default::default(),
    )?
    .times;
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
        if let Some(run) = &replayed.open_tool_run {
            runs.insert(position, run.run_id.clone());
        }
        if let Some(delivery) = replayed
            .open_tool_run
            .as_ref()
            .and_then(|run| run.delivery.clone())
        {
            deliveries.insert(position, delivery);
        }
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
    let mut legacy = Legacy {
        runs,
        created,
        deliveries,
        ..Legacy::default()
    };
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
        legacy.rows.insert(position, (metadata, Some(prior), mid));
    }
    if let Some((position, metadata)) = pending_results {
        let mid = broca_core::message_mid(true, lineage, position)?;
        legacy.pending_results = Some((position, (metadata, Some(prior), mid)));
    }
    Ok(Some(legacy))
}

/// Today's `session.read` facts for a log off the current compaction contract, from the same
/// projections today's page builder uses: each message's own written time
/// (`message_times::fold` with `mode3` false), creation time, run and delivery, and id.
fn page_only(raw: &[WalRecord], lineage: Option<&str>) -> Result<Legacy, String> {
    let replayed = broca_core::replay(raw).map_err(|error| error.to_string())?;
    let mut runs = BTreeMap::new();
    let mut deliveries = BTreeMap::new();
    let mut attribution = |position: u64, run: &broca_core::RunAttribution| {
        runs.insert(position, run.run_id.clone());
        if let Some(delivery) = &run.delivery {
            deliveries.insert(position, delivery.clone());
        }
    };
    for (position, run) in replayed.message_runs.iter().enumerate() {
        if let Some(run) = run {
            attribution(position as u64, run);
        }
    }
    let stored = replayed.prompt.len() as u64;
    // An assistant message still waiting for its tool results is held at the position after
    // the stored ones from its step's end, as today's placement holds it, and the new format
    // places its tool-results message after it when the last call has a result, before
    // today's log closes the batch.
    let mut placement = broca_core::placement::Placement::default();
    for record in raw {
        placement.observe(record)?;
    }
    let waiting = placement.held_assistant_ordinal().is_some();
    if waiting {
        let run = replayed.open_tool_run.clone().or_else(|| {
            replayed
                .run_id
                .as_ref()
                .map(|run| broca_core::RunAttribution {
                    run_id: run.0.clone(),
                    episode: run.0.clone(),
                    is_final: false,
                    delivery: None,
                })
        });
        if let Some(run) = &run {
            attribution(stored, run);
        }
    }
    let times = broca_core::message_times::fold(
        raw.iter(),
        broca_core::placement::Placement::default(),
        BTreeMap::new(),
        false,
        Default::default(),
    )?
    .times;
    let created = broca_core::creation_times::project(
        raw.iter(),
        broca_core::placement::Placement::default(),
        BTreeMap::new(),
        Default::default(),
    )?
    .times;
    let mut legacy = Legacy {
        page_only: true,
        runs,
        created,
        deliveries,
        ..Legacy::default()
    };
    let row = |position: u64| -> Result<Row, String> {
        Ok((
            Metadata::default(),
            times.get(&position).copied(),
            broca_core::message_mid(false, lineage, position)?,
        ))
    };
    for position in 0..stored + u64::from(waiting) {
        legacy.rows.insert(position, row(position)?);
    }
    if waiting {
        legacy.pending_results = Some((stored + 1, row(stored + 1)?));
    }
    Ok(legacy)
}

/// The record for the message at `ordinal` (counting from 1), whose run and creation time are
/// `run_id` and `created_at_ms`.
// Each argument is one fact of the record, from a different projection; grouping them would only
// rename the record's own fields.
#[allow(clippy::too_many_arguments)]
pub fn record(
    ordinal: u64,
    metadata: &Metadata,
    written_at_ms: Option<u64>,
    native: bool,
    mid: String,
    run_id: Option<String>,
    created_at_ms: Option<u64>,
    delivery: Option<&broca_protocol::tags::DeliveryAttribution>,
) -> EntryFacts {
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
        native,
        render_tags: metadata.render_tags,
        clean_reply: metadata.clean_reply,
        mid,
        run_id,
        created_at_ms,
        // The mode's wire name, as the contract spells it.
        delivery_mode: delivery.and_then(|delivery| {
            serde_json::to_value(delivery.mode)
                .ok()
                .and_then(|mode| mode.as_str().map(str::to_owned))
        }),
        delivery_tool_call_id: delivery.and_then(|delivery| delivery.tool_call_id.clone()),
    }
}
