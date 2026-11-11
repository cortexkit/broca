//! Counts from the same projection used for import, with refusals stripped of source text/ids.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use broca_protocol::{ContentBlock, Role};
use sha2::{Digest, Sha256};

use super::{HeadError, HeadHistory, MediaCheck};

/// Mutually exclusive kinds of projected tool results, before provider rendering.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ToolResults {
    pub completed: u64,
    pub error: u64,
    pub interrupted_as_result: u64,
    pub open_call_placeholders: u64,
}

/// Content-free totals for a successfully projected extract.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub source_rows_by_role: BTreeMap<&'static str, u64>,
    pub imported_messages_by_role: BTreeMap<&'static str, u64>,
    pub tool_calls: u64,
    pub tool_results: ToolResults,
    /// Distinct byte objects; repeated uses of the same bytes count once.
    pub media_objects: u64,
    pub media_occurrences: u64,
    pub dropped_system_rows: u64,
    /// Includes system rows and errored/empty rows the host projects as nothing.
    pub rows_projected_nothing: u64,
    pub finished_summaries: u64,
}

/// A check refusal names only the protocol code and a one-based source message ordinal.
/// File-level failures have no message ordinal. Neither field can carry source content or ids.
#[derive(Debug, PartialEq, Eq)]
pub struct Refusal {
    pub code: &'static str,
    pub message_ordinal: Option<u64>,
}

#[derive(Default)]
pub(super) struct Audit {
    pub(super) ordinals: HashMap<String, u64>,
    pub(super) counts: Counts,
}

/// Projects the whole extract and admits its media through `media`, without writing a store.
/// Any reader error is reduced to a content-free code and ordinal, never its diagnostic text.
pub fn check(path: &Path, session: &str, media: &mut MediaCheck<'_>) -> Result<Counts, Refusal> {
    check_with_history(path, session, media).map(|(_, counts)| counts)
}

/// The same check, retaining the already projected history for a subsequent offline seed.
pub fn check_with_history(
    path: &Path,
    session: &str,
    media: &mut MediaCheck<'_>,
) -> Result<(HeadHistory, Counts), Refusal> {
    let mut audit = Audit::default();
    let history = super::read_audited(path, session, media, &mut audit).map_err(|error| {
        let mid = match &error {
            HeadError::Row { mid, .. }
            | HeadError::OpenCall { mid, .. }
            | HeadError::OrphanResult { mid, .. }
            | HeadError::Media { mid, .. } => Some(mid),
            _ => None,
        };
        Refusal {
            code: error.code(),
            message_ordinal: mid.and_then(|mid| audit.ordinals.get(mid).copied()),
        }
    })?;
    let counts = &mut audit.counts;
    for role in ["user", "assistant", "system"] {
        counts.source_rows_by_role.entry(role).or_default();
    }
    for role in ["user", "assistant", "tool", "system"] {
        counts.imported_messages_by_role.entry(role).or_default();
    }
    let mut objects = HashSet::new();
    for message in &history.messages {
        let role = match message.message.role {
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::Tool => "tool",
            Role::System => "system",
        };
        *counts.imported_messages_by_role.entry(role).or_default() += 1;
        counts.tool_calls += message
            .message
            .content
            .iter()
            .filter(|block| matches!(block, ContentBlock::ToolCall(_)))
            .count() as u64;
        for medium in &message.media {
            counts.media_occurrences += 1;
            objects.insert(Sha256::digest(&medium.bytes));
        }
    }
    counts.media_objects = objects.len() as u64;
    counts.dropped_system_rows = history.dropped_system_messages as u64;
    counts.rows_projected_nothing = history
        .rows
        .iter()
        .filter(|row| row.message.is_none())
        .count() as u64;
    counts.finished_summaries = history
        .rows
        .iter()
        .filter(|row| row.finished_summary)
        .count() as u64;
    Ok((history, audit.counts))
}
