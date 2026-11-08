//! The log references edits only by byte equality against the latest accepted
//! view, never by the provider's `since` claim or the last applied version.

use broca_wal::record::StoredViewEdit;
use std::collections::HashMap;

use super::received::ReceivedView;

pub fn encode(previous: Option<&ReceivedView>, next: &ReceivedView) -> Vec<StoredViewEdit> {
    let mut prior = HashMap::new();
    if let Some(previous) = previous {
        for (index, edit) in previous.edits().iter().enumerate() {
            if let Ok(index) = u32::try_from(index) {
                prior.entry(edit.get()).or_insert(index);
            }
        }
    }
    next.edits()
        .iter()
        .map(|edit| match prior.get(edit.get()).copied() {
            Some(index) => StoredViewEdit::Previous { index },
            None => StoredViewEdit::Full { edit: edit.clone() },
        })
        .collect()
}

pub fn rebuild(
    previous: Option<&ReceivedView>,
    version: u64,
    checksum: String,
    edits: &[StoredViewEdit],
    built_through: Option<String>,
    new_signed: Option<String>,
) -> Result<ReceivedView, String> {
    #[derive(serde::Serialize)]
    struct Envelope<'a> {
        version: u64,
        checksum: &'a str,
        edits: &'a [Box<serde_json::value::RawValue>],
        #[serde(skip_serializing_if = "Option::is_none")]
        built_through: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        new_signed: Option<&'a str>,
    }
    let mut bytes = broca_protocol::serialized_json_bytes(&Envelope {
        version,
        checksum: &checksum,
        edits: &[],
        built_through: built_through.as_deref(),
        new_signed: new_signed.as_deref(),
    });
    let mut rebuilt = Vec::new();
    for edit in edits {
        let raw = match edit {
            StoredViewEdit::Full { edit } => edit,
            StoredViewEdit::Previous { index } => previous
                .and_then(|view| view.edits().get(*index as usize))
                .ok_or_else(|| {
                    format!("compaction view version {version} has bad previous edit index {index}")
                })?,
        };
        bytes += raw.get().len() as u64 + u64::from(!rebuilt.is_empty());
        if bytes > cortexkit_role_compaction::view::MAX_VIEW_BYTES as u64 {
            return Err(format!(
                "compaction view version {version} exceeds the maximum rebuilt size"
            ));
        }
        rebuilt.push(raw);
    }
    ReceivedView::rebuilt(
        version,
        checksum,
        rebuilt.into_iter().cloned().collect(),
        built_through,
        new_signed,
    )
}
