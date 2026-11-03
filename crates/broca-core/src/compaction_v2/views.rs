//! Checks shared by pushed views, ride answers and step-boundary application.

use cortexkit_compaction_view::{render, rules};
use cortexkit_role_compaction::{
    read::{Cursor, Detail, Entry, Head, ReadPage, ReadRequest, Select},
    rules::ProviderRules,
    view::{EditOp, View, ViewRefusal},
};

/// Check a whole incoming view before any durable write or acknowledgement.
pub fn check(
    entries: &[Entry],
    previous: Option<&View>,
    next: &View,
    provider_rules: &ProviderRules,
    accepts_images: bool,
) -> Result<(), ViewRefusal> {
    if !next.verify() {
        return Err(ViewRefusal::BadChecksum);
    }
    let latest = previous.map_or(0, |view| view.version);
    if next.version <= latest {
        return Err(ViewRefusal::StaleVersion {
            version: next.version,
            latest,
        });
    }
    render::check_carried(previous, next)?;
    check_boundary(entries, next, provider_rules, accepts_images)
}

/// Recheck against the current transcript: a running tool or a newly signed reply
/// may have appeared after acceptance. A refusal must leave the old view applied.
pub fn check_boundary(
    entries: &[Entry],
    view: &View,
    provider_rules: &ProviderRules,
    accepts_images: bool,
) -> Result<(), ViewRefusal> {
    for (index, edit) in view.edits.iter().enumerate() {
        if !accepts_images
            && matches!(&edit.op, EditOp::Insert { message, .. } if !message.images.is_empty())
        {
            return Err(ViewRefusal::UnsupportedEdit {
                index: index as u64,
            });
        }
    }
    render::plan(entries, &view.edits)?;
    rules::check(entries, &view.edits, provider_rules).map_err(ViewRefusal::Rule)
}

pub fn newest(entries: &[Entry]) -> Option<Cursor> {
    entries
        .last()
        .map(|entry| Cursor::new(entry.ordinal, &entry.mid, &entry.digest))
}

/// Size-bounded protocol paging. The caller loads only the selected transcript
/// projection page; cuts never affect which native entries remain readable.
pub fn page(entries: &[Entry], request: &ReadRequest, applied: Option<&View>) -> ReadPage {
    let head = {
        let mut head = Head::new();
        head.newest = newest(entries);
        head.applied_view = applied.map(View::applied);
        head
    };
    let matching = entries.iter().filter(|entry| match &request.select {
        Select::After { after_ordinal, .. } => entry.ordinal > after_ordinal.unwrap_or(0),
        Select::Mids { mids } => mids.contains(&entry.mid),
        Select::Tags { tags } => entry
            .parts
            .iter()
            .any(|part| part.tag.is_some_and(|tag| tags.contains(&tag))),
        _ => false,
    });
    let limit = match &request.select {
        Select::After { limit, .. } => limit.unwrap_or(u64::MAX),
        _ => u64::MAX,
    };
    let mut selected = Vec::new();
    let mut bytes = 2;
    let mut more = false;
    for entry in matching {
        let mut entry = entry.clone();
        if request.detail == Detail::Compact {
            entry.parts = entry.parts.iter().map(|part| part.compact()).collect();
        }
        let size = serde_json::to_vec(&entry)
            .expect("entry is serializable")
            .len() as u64;
        if !selected.is_empty()
            && (selected.len() as u64 >= limit
                || bytes + size + 1 > request.max_bytes.unwrap_or(u64::MAX))
        {
            more = true;
            break;
        }
        bytes += size + u64::from(!selected.is_empty());
        selected.push(entry);
    }
    let next = if more && matches!(request.select, Select::After { .. }) {
        selected.last().map(|entry| entry.ordinal)
    } else {
        None
    };
    let mut page = ReadPage::new(selected, head);
    page.next_after = next;
    page
}
