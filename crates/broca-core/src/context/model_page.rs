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
    /// Atomic entry ranges in the returned page. Transport adapters use these to name a group
    /// when its complete reply frame cannot fit.
    pub atomic_groups: Vec<ModelAtomicGroup>,
    pub lineage_id: Option<String>,
    pub next_from_ordinal: Option<u64>,
    pub head: Option<crate::Cursor>,
    pub compaction_id: Option<String>,
    pub version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelAtomicGroup {
    pub kind: &'static str,
    pub from_ordinal: u64,
    pub to_ordinal: u64,
}

struct PageGroup {
    anchor: u64,
    next_ordinal: u64,
    kind: &'static str,
    entries: Vec<ModelEntry>,
}

#[derive(Serialize)]
struct SerializedCursor {
    wal_seq: u64,
    sub_index: u32,
}

#[derive(Serialize)]
struct SerializedPage<'a, 'b> {
    messages: &'a [&'b ModelEntry],
    #[serde(skip_serializing_if = "Option::is_none")]
    lineage_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_from_ordinal: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    head: Option<SerializedCursor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compaction_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<u64>,
}

fn serialized_page_bytes(
    messages: &[&ModelEntry],
    lineage_id: Option<&str>,
    next_from_ordinal: Option<u64>,
    head: Option<crate::Cursor>,
    compaction_id: Option<&str>,
    version: Option<u64>,
) -> u64 {
    #[cfg(test)]
    tests::note_entries_serialized(messages.len());
    broca_protocol::serialized_json_bytes(&SerializedPage {
        messages,
        lineage_id,
        next_from_ordinal,
        head: head.map(|cursor| SerializedCursor {
            wal_seq: cursor.wal_seq,
            sub_index: cursor.sub_index,
        }),
        compaction_id,
        version,
    })
}

/// Serialized bytes of one page entry, exactly as it appears inside the page's `messages` array.
fn entry_bytes(entry: &ModelEntry) -> u64 {
    #[cfg(test)]
    tests::note_entries_serialized(1);
    broca_protocol::serialized_json_bytes(entry)
}

/// Exact serialized size of a page holding `count` entries whose own serialized sizes sum to
/// `entry_bytes`, given the size of the same page with an empty `messages` array. Compact JSON
/// writes an array as `[` + entries joined by `,` + `]`, so the entries add their own bytes plus
/// one separator between each pair; the brackets are already in the empty envelope.
fn page_bytes(count: usize, entry_bytes: u64, empty_envelope_bytes: u64) -> u64 {
    empty_envelope_bytes
        .saturating_add(entry_bytes)
        .saturating_add(count.saturating_sub(1) as u64)
}

fn entry(message: Message) -> ModelEntry {
    let ordinal = message.ordinal.expect("full prompt stamped");
    ModelEntry {
        source: serde_json::json!({"kind":"message", "ordinal":ordinal, "mid": format!("m{ordinal}")}),
        message,
    }
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
        snapshot.tag_seed,
    )?;
    let history = snapshot.model_history.as_ref();
    if snapshot.watermark_seq == 0 && compaction.is_none() {
        compaction = Some(tail_state.compaction.clone());
    }
    let (tail_spans, _) = crate::replay::projected_episode_spans(
        &tail_state,
        snapshot.prefix_len,
        history.and_then(|history| history.open_span),
    );
    let mut spans = history.map_or_else(Vec::new, |history| {
        history
            .spans
            .iter()
            .filter(|span| Some(span.start) != history.open_span)
            .cloned()
            .collect()
    });
    spans.extend(tail_spans);
    let total_len = snapshot.prefix_len + tail_state.prompt.len() as u64;
    for (i, mut message) in tail_state.prompt.into_iter().enumerate() {
        message.ordinal = Some(snapshot.prefix_len + i as u64);
        raw.push(message);
    }
    let has_compaction_plan = tail.iter().any(|frame| match &frame.record {
        WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } => crate::compaction::has_compaction(manifest),
        _ => false,
    });
    let rows_complete = history.map_or(snapshot.prefix_len == 0, |history| {
        history
            .row_ranges
            .iter()
            .flat_map(|range| range.clone())
            .eq(snapshot.rows.iter().map(|row| row.ordinal))
    });
    let limit = request
        .limit
        .unwrap_or(SESSION_READ_DEFAULT_LIMIT)
        .clamp(1, SESSION_READ_MAX_LIMIT);
    let hidden_through = compaction
        .as_ref()
        .and_then(CompactionCheckpoint::state)
        .and_then(|state| state.applied.as_ref())
        .map_or(0, |applied| applied.range.to);
    let needed = request
        .from_ordinal
        .unwrap_or_else(|| total_len.saturating_sub(limit))
        .max(hidden_through);
    let end = needed
        .saturating_add(limit)
        .saturating_add(1)
        .min(total_len);
    let start_index = raw.partition_point(|message| message.ordinal.is_some_and(|o| o < needed));
    let window_complete = (needed..end).eq(raw[start_index..]
        .iter()
        .take(end.saturating_sub(needed) as usize)
        .filter_map(|message| message.ordinal));
    // A compaction recorded only in the WAL tail can move the visible window beyond the
    // page captured by the store. Replay that uncommon seam rather than invent a continuation.
    let needs_full = (compaction.is_none() && (has_compaction_plan || snapshot.watermark_seq > 0))
        || !rows_complete
        || !window_complete;
    let lineage_id = snapshot.lineage_id.clone();
    if needs_full {
        let records = complete_read().map_err(ModelPageError::CompleteRead)?;
        let (full_raw, full_compaction) = full_replay_raw(&records)?;
        raw = full_raw;
        if compaction.is_none() {
            compaction = Some(full_compaction);
        }
    } else {
        // Every displayed result is preceded by its episode's model-step history. Other
        // episodes reset the observer, so their hidden rows are unnecessary for these bytes.
        for span in spans {
            let from = raw.partition_point(|m| m.ordinal.is_some_and(|o| o < span.start));
            let to = raw.partition_point(|m| m.ordinal.is_some_and(|o| o < span.end));
            crate::run::restore_repetition_guidance(&mut raw[from..to]);
        }
    }
    let applied = compaction
        .as_ref()
        .and_then(CompactionCheckpoint::state)
        .and_then(|state| state.applied.as_ref());
    assemble(raw, applied, request, lineage_id, head)
}

/// Every message of the log, with each episode's repetition guidance restored, and the
/// compaction state the whole log folds to.
///
/// Full replay supplies the episode spans used to restore repetition guidance. Never derive
/// guidance from page-local rows: a tail can start halfway through an episode's repeated calls.
fn full_replay_raw(
    records: &[FramedRecord],
) -> Result<(Vec<Message>, CompactionCheckpoint), crate::ReplayError> {
    let mut prior = crate::replay::replay(
        records
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>()
            .as_slice(),
    )?;
    let mut raw = std::mem::take(&mut prior.prompt);
    for span in prior.episode_step_spans.unwrap_or_default() {
        if span.end <= raw.len() {
            crate::run::restore_repetition_guidance(&mut raw[span]);
        }
    }
    Ok((raw, prior.compaction))
}

/// The model page a replay of the whole log renders.
#[allow(dead_code, reason = "used once broca.db opens on overhaul state roots")]
fn full_replay_model_page(
    records: &[FramedRecord],
    request: &ModelPageRequest,
    lineage_id: Option<String>,
    head: Option<crate::Cursor>,
) -> Result<ModelPage, ModelPageError> {
    let (raw, compaction) = full_replay_raw(records)?;
    let applied = compaction.state().and_then(|state| state.applied.as_ref());
    assemble(raw, applied, request, lineage_id, head)
}

/// What a model page read by episode takes from the offset index.
///
/// Every field comes from one index snapshot taken after the index caught up with the log: a
/// compaction Setup in an unindexed tail renumbers tags that the rows below would still show
/// with their old numbers.
#[allow(dead_code, reason = "used once broca.db opens on overhaul state roots")]
pub(crate) struct EpisodePageIndex<'a> {
    /// Every `episode` row of the session, in episode order.
    pub episodes: &'a [crate::projection::IndexedEpisode],
    /// The transcript length (`head_ord`).
    pub head_ord: u64,
    /// The frames at the index's compaction offsets, in WAL order: the Setup and every later
    /// compaction record. Only these decide the applied compaction a page renders, so the
    /// page never replays the episodes they were written in.
    pub compaction_frames: &'a [FramedRecord],
    /// The `tag_base` of the message row at an ordinal, for messages numbered under the
    /// computed tag rules. Rendered tag numbers come from these rows: a later Setup's tag
    /// checkpoint renumbers every message from its cut, which may fall inside an earlier
    /// episode whose replay never sees that Setup.
    pub tag_base: &'a dyn Fn(u64) -> Option<u64>,
    /// The session's last control cursor.
    pub head: Option<crate::Cursor>,
    pub lineage_id: Option<String>,
}

/// The frames a model page read by episode decodes for the episodes it shows.
#[allow(dead_code, reason = "used once broca.db opens on overhaul state roots")]
pub(crate) struct EpisodeFrames {
    /// The committed projection boundary the replay seeds from (the last qualified one before
    /// the first episode's `RunStarted`), or `None` when `frames` start at the log's first record.
    pub seed: Option<crate::projection::EpisodeSeed>,
    /// Every frame from that boundary, or the log's first record, through the end of the last
    /// episode asked for.
    pub frames: Vec<crate::replay::PositionedFrame>,
}

/// A model page and how it was served.
#[allow(dead_code, reason = "used once broca.db opens on overhaul state roots")]
#[derive(Debug)]
pub(crate) struct EpisodeModelPage {
    pub page: ModelPage,
    /// Why the read replayed the whole log, or `None` when it was served by episode.
    pub fallback: Option<&'static str>,
}

/// Read a model page by replaying only the episodes it shows.
///
/// The page window is fixed by the request, the transcript length and the applied compaction,
/// exactly as [`read_session_model_page`] fixes it. `load` receives the range of `episodes` that
/// window touches and returns their frames with the seed they start from; the replay is
/// validated by [`crate::projection::replay_from_episode_start`]. Each shown episode is replayed
/// from its `RunStarted`, because repetition guidance folds over the episode's whole model-step
/// history even when the page starts partway into it (as it does when a later compaction hides
/// the start of an earlier episode). Any failed check, an unreadable range, or a window the
/// frames do not cover falls back to `complete_read` and a full replay, so the page is the same
/// either way.
#[allow(dead_code, reason = "used once broca.db opens on overhaul state roots")]
pub(crate) fn read_session_model_page_by_episode(
    read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
    index: &EpisodePageIndex<'_>,
    request: &ModelPageRequest,
    load: impl FnOnce(std::ops::Range<usize>) -> Result<EpisodeFrames, String>,
    complete_read: impl FnOnce() -> Result<Vec<FramedRecord>, String>,
) -> Result<EpisodeModelPage, ModelPageError> {
    if request.after_mid.is_some() {
        return Err(ModelPageError::InvalidParams { field: "view" });
    }
    let mut compaction = CompactionCheckpoint::None;
    for frame in index.compaction_frames {
        compaction.fold(&frame.record);
    }
    let applied = compaction.state().and_then(|state| state.applied.as_ref());
    match episode_page_messages(read_at, index, request, applied, load) {
        Ok(raw) => Ok(EpisodeModelPage {
            page: assemble(raw, applied, request, index.lineage_id.clone(), index.head)?,
            fallback: None,
        }),
        Err(reason) => {
            let records = complete_read().map_err(ModelPageError::CompleteRead)?;
            Ok(EpisodeModelPage {
                page: full_replay_model_page(
                    &records,
                    request,
                    index.lineage_id.clone(),
                    index.head,
                )?,
                fallback: Some(reason),
            })
        }
    }
}

/// The messages of the page window, replayed from the episodes holding them.
///
/// The window is the one [`read_session_model_page`] proves sufficient: from the page's first
/// visible ordinal through one message past a full page, so [`assemble`] can tell whether a
/// next page exists. Messages hidden by the applied compaction never enter it.
#[allow(dead_code, reason = "used once broca.db opens on overhaul state roots")]
fn episode_page_messages(
    read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
    index: &EpisodePageIndex<'_>,
    request: &ModelPageRequest,
    applied: Option<&AppliedCompaction>,
    load: impl FnOnce(std::ops::Range<usize>) -> Result<EpisodeFrames, String>,
) -> Result<Vec<Message>, &'static str> {
    let limit = request
        .limit
        .unwrap_or(SESSION_READ_DEFAULT_LIMIT)
        .clamp(1, SESSION_READ_MAX_LIMIT);
    let total_len = index.head_ord;
    let hidden_through = applied.map_or(0, |applied| applied.range.to);
    let needed = request
        .from_ordinal
        .unwrap_or_else(|| total_len.saturating_sub(limit))
        .max(hidden_through);
    let end = needed
        .saturating_add(limit)
        .saturating_add(1)
        .min(total_len);
    if needed >= end {
        return Ok(Vec::new());
    }
    // The message at an ordinal belongs to the last episode whose input starts at or before it.
    let first = index
        .episodes
        .iter()
        .rposition(|episode| episode.first_ord <= needed)
        .ok_or("no episode holds the page start")?;
    let last = index
        .episodes
        .iter()
        .rposition(|episode| episode.first_ord < end)
        .filter(|last| *last >= first)
        .ok_or("no episode holds the page end")?;
    let EpisodeFrames { seed, frames } =
        load(first..last + 1).map_err(|_| "episode frames unreadable")?;
    let replayed = crate::projection::replay_from_episode_start(
        read_at,
        seed.as_ref(),
        &index.episodes[first],
        &frames,
    )
    .map_err(|fallback| fallback.reason)?;
    let base = replayed.ordinal_base;
    let mut raw = replayed.state.prompt;
    if base.saturating_add(raw.len() as u64) < end {
        return Err("page window not covered by the episodes read");
    }
    // Messages before the first shown episode came from the end of the episode before it,
    // replayed only to reach the shown episode's start. Their guidance spans are partial and
    // their messages are dropped below, so only the shown episodes' spans are restored.
    let first_index = index.episodes[first]
        .first_ord
        .checked_sub(base)
        .ok_or("episode starts before the seed boundary")? as usize;
    for span in replayed.state.episode_step_spans.unwrap_or_default() {
        if span.start >= first_index && span.end <= raw.len() {
            crate::run::restore_repetition_guidance(&mut raw[span]);
        }
    }
    let (from, to) = ((needed - base) as usize, (end - base) as usize);
    let mut window: Vec<Message> = raw.drain(from..to).collect();
    for (offset, message) in window.iter_mut().enumerate() {
        let ordinal = needed + offset as u64;
        message.ordinal = Some(ordinal);
        renumber_from_row(message, (index.tag_base)(ordinal))?;
    }
    Ok(window)
}

/// Give a message numbered under the computed tag rules the numbers its index row holds.
///
/// Numbering a message is a pure function of its parts, its rules and the number it starts
/// after, so renumbering from the row's base reproduces exactly what a replay of the whole log
/// shows, including a renumbering by a Setup recorded after this message's episode. A message
/// whose numbers the stored ledger assigned keeps them.
#[allow(dead_code, reason = "used once broca.db opens on overhaul state roots")]
fn renumber_from_row(message: &mut Message, tag_base: Option<u64>) -> Result<(), &'static str> {
    let Some(computed) = message.part_tags().and_then(|tags| tags.computed) else {
        return Ok(());
    };
    let mut high = tag_base.ok_or("message row has no tag base")?;
    broca_protocol::tags::number_message(
        message,
        broca_protocol::tags::ComputedRules {
            render: computed.render,
            clean: computed.clean,
        },
        &mut high,
    )
    .map_err(|_| "tag numbering overflow")
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
        .unwrap_or(broca_protocol::READ_REPLY_MAX_BYTES)
        .min(broca_protocol::READ_REPLY_MAX_BYTES);
    let mut groups: Vec<PageGroup> = Vec::new();
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
        groups.push(PageGroup {
            anchor: applied.range.from,
            next_ordinal: applied.range.to,
            kind: if applied.range.from == applied.range.to {
                "insertion"
            } else {
                "replacement"
            },
            entries,
        });
    }
    let raw: Vec<_> = raw
        .into_iter()
        .filter(|message| {
            !applied.is_some_and(|applied| {
                message.ordinal.expect("full prompt stamped") < applied.range.to
            })
        })
        .collect();
    for message in raw {
        let ordinal = message.ordinal.expect("full prompt stamped");
        let next_ordinal = ordinal.saturating_add(1);
        let model_entry = entry(message);
        if let Some(insertion) = groups.last_mut().filter(|group| {
            group.kind == "insertion" && group.anchor == ordinal && group.next_ordinal == ordinal
        }) {
            // An insertion travels with the message at its anchor, even over a page cap.
            insertion.entries.push(model_entry);
            insertion.next_ordinal = next_ordinal;
            insertion.kind = "insertion_with_anchor";
        } else {
            groups.push(PageGroup {
                anchor: ordinal,
                next_ordinal,
                kind: "message",
                entries: vec![model_entry],
            });
        }
    }
    let start = request.from_ordinal.unwrap_or_else(|| {
        let mut count = 0;
        groups
            .iter()
            .rev()
            .find_map(|group| {
                count += group.entries.len();
                (count >= limit).then_some(group.anchor)
            })
            .unwrap_or_else(|| groups.first().map_or(0, |group| group.anchor))
    });
    let first_group = groups.partition_point(|group| group.anchor < start);
    let compaction_id = applied.map(|applied| applied.compaction_id.clone());
    let version = applied.map(|applied| applied.version);
    let mut messages = Vec::new();
    let mut atomic_groups = Vec::new();
    let mut next_from_ordinal = None;
    let groups_len = groups.len();
    // Serialized bytes of every entry accepted so far. Each entry is measured once; the page's
    // size is then this sum plus the envelope and the array's separators (see `page_bytes`).
    let mut accepted_entry_bytes = 0u64;
    for (group_index, mut group) in groups.into_iter().enumerate().skip(first_group) {
        let candidate_count = messages.len().saturating_add(group.entries.len());
        let candidate_next = (group_index + 1 < groups_len).then_some(group.next_ordinal);
        let candidate_entry_bytes = group
            .entries
            .iter()
            .map(entry_bytes)
            .fold(accepted_entry_bytes, u64::saturating_add);
        let candidate_bytes = page_bytes(
            candidate_count,
            candidate_entry_bytes,
            serialized_page_bytes(
                &[],
                lineage_id.as_deref(),
                candidate_next,
                head,
                compaction_id.as_deref(),
                version,
            ),
        );
        if !messages.is_empty() && (candidate_count > limit || candidate_bytes > max_bytes) {
            next_from_ordinal = Some(group.anchor);
            break;
        }
        let descriptor = ModelAtomicGroup {
            kind: group.kind,
            from_ordinal: group.anchor,
            to_ordinal: group.next_ordinal,
        };
        messages.append(&mut group.entries);
        atomic_groups.push(descriptor);
        accepted_entry_bytes = candidate_entry_bytes;
        next_from_ordinal = candidate_next;
    }
    Ok(ModelPage {
        messages,
        atomic_groups,
        lineage_id,
        next_from_ordinal,
        head,
        compaction_id,
        version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::{ContentBlock, Role};

    fn text(role: Role, text: String) -> Message {
        Message::new(role, vec![ContentBlock::Text { text }])
    }

    #[test]
    fn model_view_pages_use_the_full_body_budget_and_keep_replacement_groups_whole() {
        let mut replacement_a = text(Role::User, "a".repeat(1_250_000));
        let mut replacement_b = text(Role::Assistant, "b".repeat(1_250_000));
        let mut tail = text(Role::User, "t".repeat(1_250_000));
        replacement_a.ordinal = None;
        replacement_b.ordinal = None;
        tail.ordinal = Some(1);
        let applied = AppliedCompaction {
            compaction_id: "large-replacement".into(),
            version: 1,
            range: broca_wal::CompactionRange { from: 0, to: 1 },
            replacement: std::sync::Arc::new(vec![replacement_a, replacement_b]),
            sha256: "digest".into(),
            thinking_prefix_len: 0,
        };
        let raw = vec![tail];
        let request = ModelPageRequest {
            from_ordinal: Some(0),
            limit: Some(100),
            ..ModelPageRequest::default()
        };

        let first = assemble(
            raw.clone(),
            Some(&applied),
            &request,
            Some("lineage-1".into()),
            Some(crate::Cursor::new(7, 0)),
        )
        .unwrap();
        assert_eq!(first.messages.len(), 2);
        assert_eq!(first.atomic_groups.len(), 1);
        assert_eq!(first.atomic_groups[0].kind, "replacement");
        assert_eq!(
            (
                first.atomic_groups[0].from_ordinal,
                first.atomic_groups[0].to_ordinal
            ),
            (0, 1)
        );
        assert_eq!(first.next_from_ordinal, Some(1));
        let first_refs: Vec<_> = first.messages.iter().collect();
        let first_bytes = serialized_page_bytes(
            &first_refs,
            first.lineage_id.as_deref(),
            first.next_from_ordinal,
            first.head,
            first.compaction_id.as_deref(),
            first.version,
        );
        assert!(first_bytes < broca_protocol::READ_REPLY_MAX_BYTES);

        let second = assemble(
            raw,
            Some(&applied),
            &ModelPageRequest {
                from_ordinal: first.next_from_ordinal,
                ..ModelPageRequest::default()
            },
            Some("lineage-1".into()),
            Some(crate::Cursor::new(7, 0)),
        )
        .unwrap();
        assert_eq!(second.messages.len(), 1);
        assert_eq!(second.messages[0].source["ordinal"], 1);
        assert_eq!(second.next_from_ordinal, None);
        let second_refs: Vec<_> = second.messages.iter().collect();
        let second_bytes = serialized_page_bytes(
            &second_refs,
            second.lineage_id.as_deref(),
            second.next_from_ordinal,
            second.head,
            second.compaction_id.as_deref(),
            second.version,
        );
        assert!(second_bytes < broca_protocol::READ_REPLY_MAX_BYTES);
        assert_eq!(first.messages[0].source["kind"], "replacement");
        assert_eq!(first.messages[1].source["kind"], "replacement");
    }

    #[test]
    fn model_view_budget_counts_sources_and_continuation_metadata() {
        let mut first_message = text(Role::User, "a".repeat(1_572_700));
        let mut second_message = text(Role::User, "b".repeat(1_572_700));
        let mut tail_message = text(Role::User, "tail".into());
        first_message.ordinal = Some(0);
        second_message.ordinal = Some(1);
        tail_message.ordinal = Some(2);
        let first_entry = entry(first_message.clone());
        let second_entry = entry(second_message.clone());
        let entries_bytes = serde_json::to_vec(&vec![first_entry.clone(), second_entry.clone()])
            .unwrap()
            .len();
        assert!(entries_bytes < broca_protocol::READ_REPLY_MAX_BYTES as usize);
        let candidate_bytes = serialized_page_bytes(
            &[&first_entry, &second_entry],
            Some("lineage-1"),
            Some(2),
            Some(crate::Cursor::new(7, 0)),
            None,
            None,
        );
        assert!(candidate_bytes > broca_protocol::READ_REPLY_MAX_BYTES);

        let page = assemble(
            vec![first_message, second_message, tail_message],
            None,
            &ModelPageRequest {
                from_ordinal: Some(0),
                limit: Some(100),
                ..ModelPageRequest::default()
            },
            Some("lineage-1".into()),
            Some(crate::Cursor::new(7, 0)),
        )
        .unwrap();
        assert_eq!(page.messages.len(), 1);
        assert_eq!(page.next_from_ordinal, Some(1));
        let refs: Vec<_> = page.messages.iter().collect();
        assert!(
            serialized_page_bytes(
                &refs,
                page.lineage_id.as_deref(),
                page.next_from_ordinal,
                page.head,
                page.compaction_id.as_deref(),
                page.version,
            ) < broca_protocol::READ_REPLY_MAX_BYTES
        );
    }

    #[test]
    fn model_view_insertions_stay_with_their_anchored_message() {
        let mut anchor = text(Role::User, "anchor".into());
        anchor.ordinal = Some(0);
        let applied = AppliedCompaction {
            compaction_id: "insert-head".into(),
            version: 1,
            range: broca_wal::CompactionRange { from: 0, to: 0 },
            replacement: std::sync::Arc::new(vec![text(Role::User, "inserted".into())]),
            sha256: "digest".into(),
            thinking_prefix_len: 0,
        };
        let page = assemble(
            vec![anchor],
            Some(&applied),
            &ModelPageRequest {
                limit: Some(1),
                max_bytes: Some(1),
                ..ModelPageRequest::default()
            },
            Some("lineage-1".into()),
            None,
        )
        .unwrap();
        assert_eq!(page.messages.len(), 2);
        assert_eq!(page.atomic_groups.len(), 1);
        assert_eq!(page.atomic_groups[0].kind, "insertion_with_anchor");
        assert_eq!(
            (
                page.atomic_groups[0].from_ordinal,
                page.atomic_groups[0].to_ordinal
            ),
            (0, 1)
        );
        assert_eq!(page.next_from_ordinal, None);
    }

    #[test]
    fn a_model_view_group_over_the_page_target_stays_whole() {
        let mut large = text(Role::User, "x".repeat(3 * 1024 * 1024 + 128));
        large.ordinal = Some(0);
        let page = assemble(
            vec![large],
            None,
            &ModelPageRequest::default(),
            Some("lineage-1".into()),
            None,
        )
        .unwrap();
        assert_eq!(page.messages.len(), 1);
        let refs: Vec<_> = page.messages.iter().collect();
        let bytes = serialized_page_bytes(
            &refs,
            page.lineage_id.as_deref(),
            page.next_from_ordinal,
            page.head,
            page.compaction_id.as_deref(),
            page.version,
        );
        assert!(bytes > broca_protocol::READ_REPLY_MAX_BYTES);
        assert!(bytes + 32 < 4_194_304, "the complete frame still fits");
    }

    /// The model view shows a summary-bearing OpenAI Responses reasoning item exactly as stored,
    /// with no derived readable block: compaction views are built from these entries, and part
    /// positions must match the stored block indices.
    #[test]
    fn model_view_keeps_responses_reasoning_items_as_stored() {
        let item = r#"{"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":"first"},{"type":"summary_text","text":"second"}],"encrypted_content":"cipher"}"#;
        let opaque = ContentBlock::RedactedReasoning {
            data: item.into(),
            provider_format: Some(broca_protocol::EncryptedReasoningFormat::OpenAiResponses),
        };
        let answer = ContentBlock::Text {
            text: "answer".into(),
        };
        let mut message = Message::new(Role::Assistant, vec![opaque.clone(), answer.clone()]);
        message.ordinal = Some(0);
        let page = assemble(
            vec![message],
            None,
            &ModelPageRequest::default(),
            None,
            None,
        )
        .unwrap();
        assert_eq!(page.messages[0].message.content, [opaque, answer]);
    }

    thread_local! {
        /// Page entries serialized on this thread, by either the whole-page or per-entry path.
        static ENTRIES_SERIALIZED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    pub(super) fn note_entries_serialized(count: usize) {
        ENTRIES_SERIALIZED.with(|cell| cell.set(cell.get() + count));
    }

    fn entries_serialized_during(run: impl FnOnce()) -> usize {
        ENTRIES_SERIALIZED.with(|cell| cell.set(0));
        run();
        ENTRIES_SERIALIZED.with(std::cell::Cell::get)
    }

    fn numbered_messages(count: u64) -> Vec<Message> {
        (0..count)
            .map(|ordinal| {
                let mut message = text(Role::User, format!("message {ordinal}"));
                message.ordinal = Some(ordinal);
                message
            })
            .collect()
    }

    fn page_of(raw: Vec<Message>, limit: u64, max_bytes: Option<u64>) -> ModelPage {
        assemble(
            raw,
            None,
            &ModelPageRequest {
                from_ordinal: Some(0),
                limit: Some(limit),
                max_bytes,
                ..ModelPageRequest::default()
            },
            Some("lineage-1".into()),
            Some(crate::Cursor::new(7, 0)),
        )
        .unwrap()
    }

    fn reply_bytes(page: &ModelPage) -> u64 {
        let refs: Vec<_> = page.messages.iter().collect();
        serialized_page_bytes(
            &refs,
            page.lineage_id.as_deref(),
            page.next_from_ordinal,
            page.head,
            page.compaction_id.as_deref(),
            page.version,
        )
    }

    /// Sizing a page measures each entry once. Re-serializing every accepted entry for each
    /// added group made a full page cost quadratic serializer work on every model-view read.
    #[test]
    fn sizing_a_model_page_serializes_each_entry_once() {
        let raw = numbered_messages(200);
        let mut page = None;
        let serialized = entries_serialized_during(|| page = Some(page_of(raw, 500, None)));
        assert_eq!(page.unwrap().messages.len(), 200);
        assert!(
            serialized <= 200,
            "a 200-entry page serialized {serialized} entries while sizing"
        );
    }

    /// The running size is the exact size of the reply: a byte cap equal to a page's serialized
    /// size admits every group, and one byte less holds the last group back.
    #[test]
    fn model_page_sizing_matches_the_serialized_reply_to_the_byte() {
        let raw = numbered_messages(12);
        let full = page_of(raw.clone(), 500, None);
        assert_eq!(full.messages.len(), 12);
        assert_eq!(full.next_from_ordinal, None);
        let exact = reply_bytes(&full);

        let at_cap = page_of(raw.clone(), 500, Some(exact));
        assert_eq!(at_cap.messages.len(), 12, "a reply of exactly the cap fits");

        let under_cap = page_of(raw, 500, Some(exact - 1));
        assert_eq!(
            under_cap.messages.len(),
            11,
            "one byte under the cap holds the last group back"
        );
        assert_eq!(under_cap.next_from_ordinal, Some(11));
        assert!(reply_bytes(&under_cap) < exact);
    }
}

/// Model pages read by episode: each page equals a full replay's page at every cut of a corpus
/// whose later compaction Setup hides the start of an earlier episode and renumbers its tags
/// from a cut inside it, and decodes only the frames of the episodes the page shows.
#[cfg(test)]
mod episode_page_tests {
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use broca_protocol::{
        caller::RunFinishReason,
        content::{
            AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult,
        },
        request::ModelHandle,
        BatchId, BindIdentity, Delivery, MessageId, RunId, StepId, ToolCallId, Usage,
    };
    use broca_wal::{
        CompactionMessageKind, ComputedTagRules, DispatchOutcome, FramedRecord, RunConfig,
        RunOrigin, SetupTagCheckpoint, WalRecord,
    };
    use sha2::Digest;

    use super::{
        full_replay_model_page, read_session_model_page_by_episode, EpisodeFrames,
        EpisodeModelPage, EpisodePageIndex, ModelPageRequest,
    };
    use crate::projection::{decode_positioned_frames, EpisodeSeed, IndexedEpisode};
    use crate::projection_plan::{OffsetEpisode, OffsetSeed, OffsetSource};
    use crate::replay::SeedBoundary;

    fn user(text: &str) -> Message {
        Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
    }

    /// A run under the computed tag rules, so every message carries numbers a later Setup's
    /// tag checkpoint can renumber.
    fn run_started(run_id: &str, input: Vec<Message>) -> WalRecord {
        WalRecord::RunStarted {
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            ts_ms: None,
            run_id: RunId(run_id.into()),
            session: BindIdentity::new("/tmp/project", "runner", "session"),
            config: Box::new(RunConfig {
                turn_directives_v1: None,
                render_tags: true,
                computed_tags: Some(ComputedTagRules { checkpoint: 0 }),
                model: ModelHandle::new("stub", "m"),
                tools: vec![].into(),
                tool_choice: Default::default(),
                generation: Default::default(),
                response_format: Default::default(),
                provider_options: serde_json::Value::Null,
                cache_tiers: serde_json::Value::Null,
                context_epoch: 0,
                renderer_version: 1,
                frozen_render_config: serde_json::Value::Null,
                auth_selection: None,
                tools_were_omitted: false,
                budget: None,
                context_limit: None,
                work_class: None,
                variant: None,
                service_tier: None,
                stop_when: Vec::new(),
                context_management: None,
            }),
            input: input.into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        }
    }

    fn step_started(step: u64) -> WalRecord {
        WalRecord::StepStarted {
            step_id: StepId(step),
        }
    }

    fn step(step: u64, content: Vec<ContentBlock>) -> WalRecord {
        WalRecord::ModelStepFinished {
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(step),
            assistant_message: Arc::new(AssistantMessage {
                message_id: MessageId(format!("m{step}")),
                content,
            }),
            usage: Usage::default(),
            finish_reason: None,
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        }
    }

    fn text(text: &str) -> ContentBlock {
        ContentBlock::Text { text: text.into() }
    }

    fn call(id: &str, tool: &str, query: u64) -> ContentBlock {
        ContentBlock::ToolCall(ToolCall {
            tool_call_id: ToolCallId(id.into()),
            tool_name: tool.into(),
            input: serde_json::json!({ "q": query }),
            thought_signature: None,
        })
    }

    /// A committed tool turn after its model step: intents in plan order, results in
    /// `arrival` order, each `(call id, is_error)`.
    fn tool_turn(step: u64, batch: &str, arrival: &[(&str, bool)]) -> Vec<WalRecord> {
        let batch_id = BatchId(batch.into());
        let mut planned: Vec<&str> = arrival.iter().map(|(id, _)| *id).collect();
        planned.sort_unstable();
        let mut records = vec![WalRecord::ToolBatchStarted {
            batch_id: batch_id.clone(),
            step_id: StepId(step),
            planned_calls: planned.iter().map(|id| ToolCallId((*id).into())).collect(),
        }];
        for id in &planned {
            records.push(WalRecord::ToolDispatchIntent {
                dispatched_at_ms: None,
                hooks: None,
                batch_id: batch_id.clone(),
                tool_call_id: ToolCallId((*id).into()),
                tool_name: None,
                args: Arc::new(serde_json::json!({})),
                module_id: None,
                schema_pin: None,
                approval_digest: None,
                call_key: None,
            });
        }
        for (id, is_error) in arrival {
            records.push(WalRecord::ToolResult {
                finished_at_ms: None,
                tags: None,
                hooks: None,
                batch_id: batch_id.clone(),
                result: Arc::new(ToolResult {
                    tool_call_id: ToolCallId((*id).into()),
                    output: ToolOutput::Text {
                        text: if *is_error { "boom" } else { "ok" }.into(),
                    },
                    is_error: *is_error,
                }),
                outcome: DispatchOutcome::Completed,
                reason: None,
                withdraw_elicitation: None,
                provider_held: None,
            });
        }
        records.push(WalRecord::ToolBatchFinished { batch_id });
        records.push(WalRecord::TurnFinished {
            step_id: StepId(step),
        });
        records
    }

    fn run_finished() -> WalRecord {
        WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        }
    }

    /// The compaction Setup recorded in the third episode: it hides ordinals 0..2 behind a
    /// replacement and renumbers tags from `m3`. Both cuts fall inside the first episode.
    fn later_setup() -> WalRecord {
        WalRecord::CompactionMessage {
            provider: "test".into(),
            kind: CompactionMessageKind::Setup,
            compaction_id: "cmp-1".into(),
            version: 1,
            range: broca_wal::CompactionRange { from: 0, to: 2 },
            replacement: Arc::new(vec![Message::new(Role::Assistant, vec![text("summary")])]),
            sha256: "0".repeat(64),
            request_id: None,
            cursor: None,
            call_when: None,
            stability: None,
            ceiling: None,
            tag_checkpoint: Some(SetupTagCheckpoint {
                tag_checkpoint: 50,
                cut: "m3".into(),
            }),
            ride_wait_ms: None,
        }
    }

    /// Four episodes. The first repeats a failing call across two tool turns (results of the
    /// first turn arriving in reverse plan order), so its second turn's result carries
    /// repetition guidance that only a fold from the episode's first step reproduces; it ends
    /// with a text step and a steer placed at its close. The second is a non-attributable
    /// import. The third records the Setup that hides the first episode's start and renumbers
    /// its tags from inside it. The fourth follows the Setup.
    fn corpus() -> Vec<WalRecord> {
        let mut records = vec![
            run_started("r1", vec![user("task")]),
            step_started(1),
            step(1, vec![call("a", "probe", 1), call("b", "other", 2)]),
        ];
        records.extend(tool_turn(1, "r1-b1", &[("b", false), ("a", true)]));
        records.push(step_started(2));
        records.push(step(2, vec![call("c", "probe", 1)]));
        records.extend(tool_turn(2, "r1-b2", &[("c", true)]));
        records.push(step_started(3));
        records.push(step(3, vec![text("done")]));
        records.push(WalRecord::Steered {
            written_at_ms: None,
            tags: None,
            retention: None,
            ts_ms: None,
            title: None,
            send_id: Some("steer-late".into()),
            request_identity_digest: None,
            delivery: Some(Delivery::Steer),
            input: Arc::new(vec![user("late")]),
            hooks: None,
        });
        records.push(run_finished());
        let mut import = run_started("seed", vec![user("imported")]);
        if let WalRecord::RunStarted { origin, .. } = &mut import {
            *origin = RunOrigin::SeededFrom {
                origin_kind: broca_wal::SeedOriginKind::Import,
                source_ref: "fixture:import".into(),
                origin_device_pseudonym: None,
                base_seq: None,
            };
        }
        records.push(import);
        records.push(run_finished());
        records.extend([
            run_started("r3", vec![user("next")]),
            step_started(1),
            step(1, vec![text("answer")]),
            later_setup(),
            run_finished(),
            run_started("r4", vec![user("after")]),
            step_started(1),
            step(1, vec![text("last")]),
            run_finished(),
        ]);
        records
    }

    fn encode(records: &[WalRecord]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for (index, record) in records.iter().enumerate() {
            bytes.extend(broca_wal::framing::encode(index as u64 + 1, 1, record).unwrap());
        }
        bytes
    }

    fn reader(bytes: &[u8]) -> impl Fn(u64, u64) -> std::io::Result<Vec<u8>> + '_ {
        move |off, len| {
            let start = (off as usize).min(bytes.len());
            let end = (off.saturating_add(len) as usize).min(bytes.len());
            Ok(bytes[start..end].to_vec())
        }
    }

    /// The offset index of a log, built by the same fold the indexer runs, with the rows,
    /// episodes and boundaries the database would hold.
    struct TestIndex {
        episodes: Vec<IndexedEpisode>,
        head_ord: u64,
        rows: BTreeMap<u64, OffsetSource>,
        boundaries: Vec<SeedBoundary>,
        compaction_offsets: Vec<u64>,
    }

    fn index_log(bytes: &[u8]) -> TestIndex {
        let mut seed = OffsetSeed::default();
        let mut rows: BTreeMap<u64, OffsetSource> = BTreeMap::new();
        let mut episodes: BTreeMap<u64, OffsetEpisode> = BTreeMap::new();
        let mut boundaries: Vec<SeedBoundary> = Vec::new();
        let mut off = 0usize;
        while off < bytes.len() {
            let broca_wal::framing::DecodeStep::Record { framed, consumed } =
                broca_wal::framing::decode_next(&bytes[off..])
            else {
                panic!("test log holds only records");
            };
            let end = off + consumed;
            let delta = seed.observe(&framed, off as u64, end as u64).unwrap();
            if let Some(to) = delta.truncate {
                rows.retain(|ord, _| *ord < to);
            }
            rows.extend(delta.messages);
            if let Some(closed) = delta.closed_episode {
                episodes.insert(closed.n, closed);
            }
            if let Some(open) = &seed.episode {
                episodes.insert(open.n, open.clone());
            }
            for fact in delta.boundaries {
                if fact.terminal_seq.is_some() {
                    boundaries
                        .iter_mut()
                        .find(|boundary| boundary.seq == fact.seq)
                        .expect("a qualifier names an earlier candidate")
                        .qualified = true;
                } else {
                    boundaries.push(SeedBoundary {
                        lineage_id: None,
                        seq: fact.seq,
                        frame_off: off as u64,
                        frame_end: end as u64,
                        frame_identity: sha2::Sha256::digest(&bytes[off..end]).to_vec(),
                        qualified: fact.qualified,
                    });
                }
            }
            // The database renumbers the rows from a Setup's cut exactly like this.
            if let Some((cut, checkpoint)) = delta.reanchor {
                if let Some(first) = rows.get(&cut) {
                    let mut high = checkpoint.max(first.tag_base.unwrap_or(0));
                    for row in rows.range_mut(cut..).map(|(_, row)| row) {
                        if row.flags & 2 != 0 {
                            row.tag_base = Some(high);
                            high += row.tag_n;
                            row.tag_high = high;
                        } else {
                            high = high.max(row.tag_high);
                        }
                    }
                    seed.tag_high = high;
                }
            }
            off = end;
        }
        TestIndex {
            episodes: episodes
                .into_values()
                .map(|episode| IndexedEpisode {
                    run_started_seq: episode.run_started_seq,
                    run_started_off: episode.run_started_off,
                    first_ord: episode.first_ord,
                    end_ord: episode.end_ord,
                })
                .collect(),
            head_ord: seed.head_ord(),
            rows,
            boundaries,
            compaction_offsets: seed.admission.compaction.clone(),
        }
    }

    /// The tail seed the index holds at the last qualified boundary before the episode's
    /// `RunStarted`, taken from a replay of the log through that boundary.
    fn seed_before(
        index: &TestIndex,
        records: &[WalRecord],
        episode: &IndexedEpisode,
    ) -> Option<EpisodeSeed> {
        let start = episode.run_started_seq?;
        let boundary = index
            .boundaries
            .iter()
            .filter(|boundary| boundary.qualified && boundary.seq < start)
            .max_by_key(|boundary| boundary.seq)?
            .clone();
        let prefix = crate::replay::replay(&records[..boundary.seq as usize]).unwrap();
        Some(EpisodeSeed {
            ordinal_base: prefix.prompt.len() as u64,
            held_steers: prefix.pending_steers.clone(),
            tag_seed: crate::tags::TagSeed::from_prefix(&prefix.prompt),
            seed_origin: prefix
                .run_config
                .as_ref()
                .map(|config| config.model.clone()),
            seed_run_id: prefix.run_id.clone(),
            seed_budget: prefix
                .run_config
                .as_ref()
                .and_then(|config| config.budget.clone()),
            boundary,
        })
    }

    fn framed(records: &[WalRecord]) -> Vec<FramedRecord> {
        records
            .iter()
            .enumerate()
            .map(|(index, record)| FramedRecord {
                seq: index as u64 + 1,
                fence: 1,
                record: record.clone(),
            })
            .collect()
    }

    /// Read one page by episode over the log `records`, with `index` (possibly altered by the
    /// caller) standing in for the database, counting decoded frames into `decoded`.
    fn page_by_episode(
        records: &[WalRecord],
        index: &TestIndex,
        request: &ModelPageRequest,
        decoded: &Cell<u64>,
    ) -> (EpisodeModelPage, Option<std::ops::Range<usize>>) {
        let bytes = encode(records);
        let read_at = reader(&bytes);
        let all = framed(records);
        let compaction_frames: Vec<FramedRecord> = index
            .compaction_offsets
            .iter()
            .map(|off| {
                decode_positioned_frames(&read_at, *off, frame_end(&bytes, *off), None)
                    .unwrap()
                    .remove(0)
                    .framed
            })
            .collect();
        let tag_base = |ordinal: u64| {
            index
                .rows
                .get(&ordinal)
                .filter(|row| row.flags & 2 != 0)
                .and_then(|row| row.tag_base)
        };
        let page_index = EpisodePageIndex {
            episodes: &index.episodes,
            head_ord: index.head_ord,
            compaction_frames: &compaction_frames,
            tag_base: &tag_base,
            head: crate::last_control_cursor_through(&all, None),
            lineage_id: None,
        };
        let loaded = Cell::new(None);
        let page = read_session_model_page_by_episode(
            &read_at,
            &page_index,
            request,
            |episodes: std::ops::Range<usize>| {
                loaded.set(Some(episodes.clone()));
                let seed = seed_before(index, records, &index.episodes[episodes.start]);
                let from = seed.as_ref().map_or(0, |seed| seed.boundary.frame_end);
                let to = index
                    .episodes
                    .get(episodes.end)
                    .and_then(|next| next.run_started_off)
                    .unwrap_or(bytes.len() as u64);
                Ok(EpisodeFrames {
                    seed,
                    frames: decode_positioned_frames(&read_at, from, to, Some(decoded))?,
                })
            },
            || Ok(all.clone()),
        )
        .unwrap();
        (page, loaded.take())
    }

    fn frame_end(bytes: &[u8], off: u64) -> u64 {
        match broca_wal::framing::decode_next(&bytes[off as usize..]) {
            broca_wal::framing::DecodeStep::Record { consumed, .. } => off + consumed as u64,
            _ => panic!("offset names a record frame"),
        }
    }

    fn full_page(records: &[WalRecord], request: &ModelPageRequest) -> super::ModelPage {
        let all = framed(records);
        full_replay_model_page(
            &all,
            request,
            None,
            crate::last_control_cursor_through(&all, None),
        )
        .unwrap()
    }

    fn requests(total: u64) -> Vec<ModelPageRequest> {
        let mut requests = Vec::new();
        for limit in [1, 2, 3, 5, 50] {
            requests.push(ModelPageRequest {
                limit: Some(limit),
                ..Default::default()
            });
            for from in 0..=total + 1 {
                requests.push(ModelPageRequest {
                    from_ordinal: Some(from),
                    limit: Some(limit),
                    ..Default::default()
                });
            }
        }
        requests
    }

    #[test]
    fn corpus_exercises_guidance_and_a_later_setup_renumbering_an_earlier_episode() {
        // Pins what the equivalence test below depends on, so a corpus change cannot quietly
        // stop exercising it.
        let records = corpus();
        let full = crate::replay::replay(&records).unwrap();
        let index = index_log(&encode(&records));
        assert_eq!(index.episodes.len(), 4);
        assert_eq!(index.episodes[2].first_ord, 8, "the Setup's episode");
        // Both Setup cuts fall inside the first episode.
        assert!(index.episodes[0].first_ord < 2 && 3 < index.episodes[0].end_ord);
        let base_of = |message: &Message| message.part_tags().and_then(|tags| tags.computed);
        assert!(
            base_of(&full.prompt[3]).is_some_and(|computed| computed.base >= 50),
            "the Setup renumbers m3 after its checkpoint"
        );
        let page = full_page(
            &records,
            &ModelPageRequest {
                from_ordinal: Some(0),
                limit: Some(4),
                ..Default::default()
            },
        );
        assert_eq!(
            page.messages[0].message.role,
            Role::Assistant,
            "replacement"
        );
        assert_eq!(
            page.messages[1].message.ordinal,
            Some(2),
            "page starts inside r1"
        );
        let guidance = serde_json::to_string(&page.messages[3].message).unwrap();
        assert!(
            guidance.contains("identical arguments"),
            "m4 carries guidance folded from the hidden m1: {guidance}"
        );
    }

    #[test]
    fn model_page_by_episode_equals_full_replay_at_every_cut() {
        let records = corpus();
        let mut checked = 0;
        for cut in 1..=records.len() {
            let log = &records[..cut];
            let index = index_log(&encode(log));
            for request in requests(index.head_ord) {
                let decoded = Cell::new(0);
                let (by_episode, _) = page_by_episode(log, &index, &request, &decoded);
                assert_eq!(
                    by_episode.fallback, None,
                    "cut {cut} request {request:?} fell back"
                );
                assert_eq!(
                    by_episode.page,
                    full_page(log, &request),
                    "cut {cut} request {request:?}"
                );
                checked += 1;
            }
        }
        assert!(checked > 1000, "{checked} pages checked");
    }

    #[test]
    fn a_page_decodes_only_the_frames_of_its_episodes() {
        let records = corpus();
        let bytes = encode(&records);
        let index = index_log(&bytes);
        // The last two messages belong to the fourth episode alone.
        let request = ModelPageRequest {
            limit: Some(2),
            ..Default::default()
        };
        let decoded = Cell::new(0);
        let (by_episode, loaded) = page_by_episode(&records, &index, &request, &decoded);
        assert_eq!(by_episode.fallback, None);
        assert_eq!(by_episode.page, full_page(&records, &request));
        assert_eq!(loaded, Some(3..4));
        // From the third episode's committed text step: the Setup, its terminal, then the
        // fourth episode's four frames.
        assert_eq!(decoded.get(), 6, "of {} frames", records.len());

        // A page starting inside the first episode, behind the Setup, folds that episode from
        // its start and decodes nothing past the episodes it shows.
        let request = ModelPageRequest {
            from_ordinal: Some(0),
            limit: Some(3),
            ..Default::default()
        };
        let decoded = Cell::new(0);
        let (by_episode, loaded) = page_by_episode(&records, &index, &request, &decoded);
        assert_eq!(by_episode.fallback, None);
        assert_eq!(by_episode.page, full_page(&records, &request));
        assert_eq!(loaded, Some(0..1));
        let first_episode_frames = index.episodes[1].run_started_seq.unwrap() - 1;
        assert_eq!(decoded.get(), first_episode_frames);
    }

    #[test]
    fn stale_index_facts_fall_back_to_a_full_replay() {
        let records = corpus();
        let request = ModelPageRequest {
            limit: Some(2),
            ..Default::default()
        };
        let expected = full_page(&records, &request);

        let mut shifted = index_log(&encode(&records));
        shifted.episodes[3].first_ord -= 1;
        let (page, _) = page_by_episode(&records, &shifted, &request, &Cell::new(0));
        assert_eq!(page.fallback, Some("episode first ordinal mismatch"));
        assert_eq!(page.page, expected);

        let mut moved = index_log(&encode(&records));
        moved.episodes[3].run_started_off = moved.episodes[3].run_started_off.map(|off| off + 1);
        let (page, _) = page_by_episode(&records, &moved, &request, &Cell::new(0));
        assert_eq!(page.fallback, Some("episode start not in the frames read"));
        assert_eq!(page.page, expected);

        let mut forged = index_log(&encode(&records));
        for boundary in &mut forged.boundaries {
            boundary.frame_identity[0] ^= 1;
        }
        let (page, _) = page_by_episode(&records, &forged, &request, &Cell::new(0));
        assert_eq!(page.fallback, Some("boundary frame identity mismatch"));
        assert_eq!(page.page, expected);

        // Rows without tag bases cannot number the page.
        let mut untagged = index_log(&encode(&records));
        for row in untagged.rows.values_mut() {
            row.tag_base = None;
        }
        let (page, _) = page_by_episode(&records, &untagged, &request, &Cell::new(0));
        assert_eq!(page.fallback, Some("message row has no tag base"));
        assert_eq!(page.page, expected);
    }
}
