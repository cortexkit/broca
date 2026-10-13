//! Model pages reflect the same full-prompt guidance fold and compaction slice as a send.
use super::*;
use crate::compaction::{AppliedCompaction, CompactionCheckpoint};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ModelEntry {
    /// Built from the pinned role's model_view_source for replacements.
    pub source: Value,
    pub message: Message,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ModelPage {
    pub messages: Vec<ModelEntry>,
    pub lineage_id: Option<String>,
    pub next_from_ordinal: Option<u64>,
    pub head: Option<crate::Cursor>,
    pub compaction_id: Option<String>,
    pub version: Option<u64>,
}
#[derive(Debug, Clone, Default)]
pub struct ModelPageRequest {
    pub from_ordinal: Option<u64>,
    pub after_mid: Option<String>,
    pub limit: Option<u64>,
    pub max_bytes: Option<u64>,
}
#[derive(Debug, Clone, PartialEq)]
pub enum ModelPageError {
    InvalidParams { field: &'static str },
    Replay(crate::ReplayError),
    CompleteRead(String),
}
impl std::fmt::Display for ModelPageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidParams { field } => write!(f, "invalid_params {{field: {field}}}"),
            Self::Replay(error) => error.fmt(f),
            Self::CompleteRead(reason) => reason.fmt(f),
        }
    }
}
impl std::error::Error for ModelPageError {}
impl From<crate::ReplayError> for ModelPageError {
    fn from(error: crate::ReplayError) -> Self {
        Self::Replay(error)
    }
}

/// `complete_read` reads the same bounded WAL head as `tail`. It is required when the
/// checkpoint was written by a reader release, or when sparse rows cannot reconstruct the
/// full episode counters. The snapshot state is folded through the tail, never overwritten
/// by a pre-watermark applied view. Raw reads do not use this function or decode its state.
pub fn read_session_model_page(
    snapshot: &StorePageSnapshot,
    tail: &[FramedRecord],
    request: &ModelPageRequest,
    complete_read: impl FnOnce() -> Result<Vec<FramedRecord>, String>,
) -> Result<ModelPage, ModelPageError> {
    if request.after_mid.is_some() {
        return Err(ModelPageError::InvalidParams { field: "view" });
    }
    let mut compaction = snapshot.compaction.clone();
    if let Some(state) = &mut compaction {
        for frame in tail {
            state.fold(&frame.record);
        }
    }
    let head = crate::last_control_cursor_through(tail, None).or(snapshot.control_head);
    // Rows do not carry episode spans. Even a complete prefix needs the WAL if the
    // single-observer test discovers guidance, because counters reset per episode.
    let mut raw: Vec<Message> = snapshot
        .rows
        .iter()
        .map(|row| {
            let mut message = row.message.clone();
            message.ordinal = Some(row.ordinal);
            message
        })
        .collect();
    let tail_state = crate::replay::replay_seeded(
        tail.iter().map(|frame| &frame.record),
        snapshot.trailing_origin.clone(),
        None,
        None,
        None,
        snapshot.prefix_len,
        false,
    )?;
    raw.extend(tail_state.prompt);
    let has_compaction_plan = tail.iter().any(|frame| match &frame.record {
        WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } => crate::compaction::has_compaction(manifest),
        _ => false,
    });
    let needs_full = (compaction.is_none() && (has_compaction_plan || snapshot.watermark_seq > 0))
        || snapshot.rows.len() as u64 != snapshot.prefix_len
        || raw.iter().any(|message| {
            message
                .content
                .iter()
                .any(|block| matches!(block, broca_protocol::ContentBlock::ToolResult(_)))
        });
    let lineage_id = snapshot.lineage_id.clone();
    if needs_full {
        let records = complete_read().map_err(ModelPageError::CompleteRead)?;
        // Full replay owns the spans and raw native final values. Never derive guidance from
        // page-local rows: a tail can start halfway through an episode's repeated calls.
        let mut prior = crate::replay::replay(
            records
                .iter()
                .map(|frame| frame.record.clone())
                .collect::<Vec<_>>()
                .as_slice(),
        )?;
        raw = std::mem::take(&mut prior.prompt);
        for span in prior.episode_step_spans.unwrap_or_default() {
            if span.end <= raw.len() {
                crate::run::restore_repetition_guidance(&mut raw[span]);
            }
        }
        if compaction.is_none() {
            compaction = Some(prior.compaction);
        }
    }
    broca_protocol::content::stamp_ordinals(&mut raw);
    let applied = compaction
        .as_ref()
        .and_then(CompactionCheckpoint::state)
        .and_then(|state| state.applied.as_ref());
    assemble(raw, applied, request, lineage_id, head)
}

fn assemble(
    raw: Vec<Message>,
    applied: Option<&AppliedCompaction>,
    request: &ModelPageRequest,
    lineage_id: Option<String>,
    head: Option<crate::Cursor>,
) -> Result<ModelPage, ModelPageError> {
    let limit = request
        .limit
        .unwrap_or(SESSION_READ_DEFAULT_LIMIT)
        .clamp(1, SESSION_READ_MAX_LIMIT) as usize;
    let max_bytes = request
        .max_bytes
        .unwrap_or(4 * 1024 * 1024)
        .clamp(1, 16 * 1024 * 1024) as usize;
    let mut groups: Vec<(u64, u64, Vec<ModelEntry>)> = Vec::new();
    if let Some(applied) = applied {
        let source = serde_json::to_value(
            applied
                .wire()
                .model_view_source()
                .expect("applied range is valid"),
        )
        .expect("role source JSON");
        let entries = applied
            .replacement
            .iter()
            .cloned()
            .map(|mut message| {
                message.ordinal = None;
                ModelEntry {
                    source: source.clone(),
                    message,
                }
            })
            .collect();
        groups.push((applied.range.from, applied.range.to, entries));
    }
    for message in raw {
        let ordinal = message.ordinal.expect("full prompt stamped");
        if applied.is_some_and(|applied| ordinal < applied.range.to) {
            continue;
        }
        let entry = ModelEntry {
            source: serde_json::json!({"kind":"message", "ordinal":ordinal, "mid": format!("m{ordinal}")}),
            message,
        };
        if let Some((anchor, end, entries)) = groups
            .last_mut()
            .filter(|(anchor, end, _)| *anchor == ordinal && *end == ordinal)
        {
            // An insertion always travels with the entry at its anchor, even over a cap.
            entries.push(entry);
            *end = ordinal.saturating_add(1);
            let _ = anchor;
        } else {
            groups.push((ordinal, ordinal.saturating_add(1), vec![entry]));
        }
    }
    let start = request.from_ordinal.unwrap_or_else(|| {
        let mut count = 0;
        groups
            .iter()
            .rev()
            .find_map(|(anchor, _, entries)| {
                count += entries.len();
                (count >= limit).then_some(*anchor)
            })
            .unwrap_or_else(|| groups.first().map_or(0, |group| group.0))
    });
    let mut messages = Vec::new();
    let mut bytes = 0usize;
    let mut next_from_ordinal = None;
    for (anchor, _, entries) in groups.into_iter().filter(|(anchor, _, _)| *anchor >= start) {
        let size = entries
            .iter()
            .map(|entry| serde_json::to_vec(entry).expect("native page JSON").len())
            .sum::<usize>();
        if !messages.is_empty()
            && (messages.len().saturating_add(entries.len()) > limit
                || bytes.saturating_add(size) > max_bytes)
        {
            next_from_ordinal = Some(anchor);
            break;
        }
        bytes = bytes.saturating_add(size);
        messages.extend(entries);
    }
    Ok(ModelPage {
        messages,
        lineage_id,
        next_from_ordinal,
        head,
        compaction_id: applied.map(|applied| applied.compaction_id.clone()),
        version: applied.map(|applied| applied.version),
    })
}
