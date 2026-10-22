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
        snapshot.tag_seed,
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
    for (group_index, mut group) in groups.into_iter().enumerate().skip(first_group) {
        let candidate_count = messages.len().saturating_add(group.entries.len());
        let candidate_next = (group_index + 1 < groups_len).then_some(group.next_ordinal);
        let mut candidate: Vec<&ModelEntry> = messages.iter().collect();
        candidate.extend(group.entries.iter());
        let candidate_bytes = serialized_page_bytes(
            &candidate,
            lineage_id.as_deref(),
            candidate_next,
            head,
            compaction_id.as_deref(),
            version,
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
}
