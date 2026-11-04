//! Translate the native WAL transcript into compaction protocol Entry values.
//! Retained sessions and stores without cached Entry pages use this fallback;
//! ordinary SQLite entry reads use their bounded materialized page instead.

use std::collections::BTreeMap;

use broca_protocol::Message;
use broca_wal::{FramedRecord, WalRecord};

use super::{metadata, protocol::read::Entry};

pub struct Transcript {
    pub stored: Vec<Message>,
    pub entries: Vec<Entry>,
}

pub fn project(records: &[FramedRecord], lineage: &str) -> Result<Transcript, String> {
    let raw: Vec<_> = records.iter().map(|frame| frame.record.clone()).collect();
    let replay = crate::replay(&raw).map_err(|error| error.to_string())?;
    let times = crate::message_times::mode3_written_times(
        raw.iter(),
        crate::placement::Placement::default(),
        BTreeMap::new(),
        Default::default(),
    )?
    .times;
    finish(
        records,
        lineage,
        0,
        metadata::Fold::default(),
        replay.prompt,
        times,
    )
}

pub fn tail(
    seed: &crate::ProjectionSeed,
    records: &[FramedRecord],
    lineage: &str,
) -> Result<Transcript, String> {
    let head = seed
        .admission_checkpoint
        .head
        .as_ref()
        .ok_or("compaction tail has no cached head")?;
    let continuation = head
        .compaction_metadata
        .clone()
        .ok_or("compaction tail has no metadata checkpoint")?;
    let replay =
        crate::replay::replay_projection_tail(seed, records).map_err(|error| error.to_string())?;
    let times = crate::message_times::mode3_written_times(
        records.iter().map(|frame| &frame.record),
        crate::placement::Placement::new(seed.prefix_len),
        head.queued_times().clone(),
        head.mode3_times.clone().unwrap_or_default(),
    )?
    .times;
    finish(
        records,
        lineage,
        seed.prefix_len,
        continuation,
        replay.messages,
        times,
    )
}

fn finish(
    records: &[FramedRecord],
    lineage: &str,
    first: u64,
    mut continuation: metadata::Fold,
    mut stored: Vec<Message>,
    mut times: BTreeMap<u64, u64>,
) -> Result<Transcript, String> {
    let mut rows = BTreeMap::new();
    for frame in records {
        let delta = continuation.observe(frame)?;
        rows.extend(delta.rows);
    }
    if let Some((position, metadata)) = continuation.pending_assistant() {
        if position != first + stored.len() as u64 {
            return Err("compaction pending assistant lost its reserved native position".into());
        }
        let message = records
            .iter()
            .find_map(|frame| {
                (frame.seq == metadata.source_seq)
                    .then_some(&frame.record)
                    .and_then(|record| match record {
                        WalRecord::ModelStepFinished {
                            assistant_message, ..
                        } => Some(Message::new(
                            broca_protocol::Role::Assistant,
                            assistant_message.content.clone(),
                        )),
                        _ => None,
                    })
            })
            .ok_or("compaction pending assistant source is missing")?;
        stored.push(message);
        let source = records
            .iter()
            .position(|frame| frame.seq == metadata.source_seq)
            .expect("source found above");
        if let WalRecord::ModelStepFinished { written_at_ms, .. } = &records[source].record {
            let prior = crate::message_times::mode3_written_times(
                records[..=source].iter().map(|frame| &frame.record),
                crate::placement::Placement::new(first),
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
    }
    let mut entries = Vec::with_capacity(stored.len());
    let mut prior_time = 0;
    for (index, message) in stored.iter_mut().enumerate() {
        let position = first + index as u64;
        message.ordinal = Some(position);
        let metadata = rows
            .get(&position)
            .ok_or_else(|| format!("compaction metadata missing for message {position}"))?;
        metadata::attach_native_tags(message, metadata)?;
        prior_time = times.get(&position).copied().unwrap_or(prior_time);
        let mid = crate::message_mid(true, Some(lineage), position)?;
        entries.push(metadata::project_entry(
            message,
            metadata,
            position,
            &mid,
            prior_time,
            metadata::source_frames(metadata, records)?,
        )?);
    }
    Ok(Transcript { stored, entries })
}
