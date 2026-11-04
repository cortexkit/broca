//! Recover which numbered parts a model request actually showed, and where
//! their stored payloads live. Output-reference snapshots freeze that payload
//! mapping so later compaction cannot silently retarget an earlier reference.
//! Wire adapters own descriptors; this module owns applied-view visibility.

use broca_protocol::{BindIdentity, ContentBlock};
use broca_wal::{FramedRecord, WalRecord};
use cortexkit_compaction_view::render::{self, PlanItem};
use std::collections::{BTreeMap, HashMap, VecDeque};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShownPart {
    pub mid: String,
    pub part: String,
    pub digest: String,
    /// Native zero-based message/block carrying the stored payload. A tool part
    /// names its assistant call, but its payload lives in the following result.
    pub payload_position: u64,
    pub payload_block: usize,
}

pub fn at_step(
    records: &[FramedRecord],
    session: &BindIdentity,
    lineage: &str,
    run_id: &str,
    step: u64,
) -> Result<BTreeMap<u64, ShownPart>, String> {
    let start = records.iter().position(|frame| matches!(&frame.record, WalRecord::RunStarted { run_id:run, .. } if run.0 == run_id)).ok_or("output reference writing run is missing")?;
    let run_end = records[start + 1..]
        .iter()
        .position(|frame| matches!(frame.record, WalRecord::RunStarted { .. }))
        .map_or(records.len(), |offset| start + 1 + offset);
    let boundary = records[start..run_end].iter().position(|frame| matches!(frame.record, WalRecord::StepStarted { step_id, .. } if step_id.0 == step)).map(|offset| start + offset).ok_or("output reference writing step is missing")?;
    let end = records[boundary + 1..run_end].iter().position(|frame| matches!(&frame.record, WalRecord::ModelAttemptFinished { step_id, .. } | WalRecord::ModelStepFinished { step_id, .. } if step_id.0 == step)).map_or(run_end, |offset| boundary + 1 + offset);
    let records = &records[..end];
    let state = super::session::load(session, records)?;
    if !matches!(&records[start].record, WalRecord::RunStarted { manifest:Some(manifest), .. } if crate::compaction::has_compaction(manifest))
    {
        return Ok(BTreeMap::new());
    }
    let transcript = super::transcript::project(records, lineage)?;
    let edits = state
        .view_for_step(step)
        .map_or(&[][..], |view| view.edits.as_slice());
    let edits = render::edits_for_transcript(&transcript.entries, edits);
    let plan = render::plan(&transcript.entries, &edits)
        .map_err(|error| format!("output reference render plan refused: {error:?}"))?;
    let mut shown = BTreeMap::new();
    for item in plan.items {
        let PlanItem::Entry { mid, parts, .. } = item else {
            continue;
        };
        let index = transcript
            .entries
            .iter()
            .position(|entry| entry.mid == mid)
            .ok_or("shown entry is missing")?;
        let entry = &transcript.entries[index];
        let original = &transcript.stored[index];
        let mut results: HashMap<&str, VecDeque<usize>> = HashMap::new();
        if let Some(next) = transcript
            .stored
            .get(index + 1)
            .filter(|message| message.role == broca_protocol::Role::Tool)
        {
            for (index, block) in next.content.iter().enumerate() {
                if let ContentBlock::ToolResult(result) = block {
                    results
                        .entry(&result.tool_call_id.0)
                        .or_default()
                        .push_back(index);
                }
            }
        }
        let locations: HashMap<_, _> = original
            .content
            .iter()
            .enumerate()
            .filter_map(|(index, block)| {
                let ContentBlock::ToolCall(call) = block else {
                    return None;
                };
                Some((
                    index,
                    results.get_mut(call.tool_call_id.0.as_str())?.pop_front()?,
                ))
            })
            .collect();
        for part in parts {
            let Some(tag) = part.tag.filter(|_| part.prefix) else {
                continue;
            };
            let block = part
                .id
                .parse::<usize>()
                .map_err(|_| "shown part has no native block location")?;
            if part
                .replace
                .iter()
                .any(|field| matches!(field.field.as_str(), "text" | "output"))
            {
                continue;
            }
            let neutral = entry
                .parts
                .iter()
                .find(|candidate| candidate.id == part.id)
                .ok_or("shown part is missing")?;
            let (payload_position, payload_block) = if let Some(result) = locations.get(&block) {
                (entry.ordinal, *result)
            } else {
                (entry.ordinal - 1, block)
            };
            shown.insert(
                tag,
                ShownPart {
                    mid: entry.mid.clone(),
                    part: part.id,
                    digest: neutral.digest.clone(),
                    payload_position,
                    payload_block,
                },
            );
        }
    }
    Ok(shown)
}
