//! Checks shared by pushed views, ride answers and step-boundary application.

use cortexkit_compaction_view::{render, rules::Floor};
use cortexkit_role_compaction::{
    read::{Cursor, Detail, Entry, Head, ReadPage, ReadRequest, Select},
    rules::ProviderRules,
    view::{EditOp, View, ViewRefusal},
};

/// Check a whole incoming view before any durable write or acknowledgement. `floor` is the
/// session's signed floor (`host::view_floor`): a view keeping a signed part it covers is never
/// refused for that part.
pub fn check(
    entries: &[Entry],
    previous: Option<&View>,
    next: &View,
    provider_rules: &ProviderRules,
    floor: Floor<'_>,
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
    check_boundary(entries, next, provider_rules, floor)
}

/// The entry a moved-in session's move cut through: the one its compaction provider's first view
/// of the moved head cuts through, by the id the provider names it with and its ordinal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MovedThrough {
    pub mid: String,
    pub ordinal: u64,
}

/// The entry `view`'s cut goes through, if it has a cut.
pub fn cut_through(view: &View) -> Option<&str> {
    view.edits.iter().find_map(|edit| match &edit.op {
        EditOp::Cut { through } => Some(through.as_str()),
        _ => None,
    })
}

/// On a moved-in session, every view must cut through the entry the move cut through, or a later
/// one (the compaction contract's MV7): the parts before it were numbered under the source host's
/// rules, so no view may show them. A view with no cut, or cutting through an earlier entry,
/// refuses `cut_before_move`. `through_ordinal` is the ordinal of the entry the cut goes through,
/// which the history before the window holds too (the move's cut sealed it); `None` when the
/// session holds no such entry, which the view's own check refuses by name.
pub fn check_moved(
    view: &View,
    moved: Option<&MovedThrough>,
    through_ordinal: Option<u64>,
) -> Result<(), ViewRefusal> {
    let Some(moved) = moved else {
        return Ok(());
    };
    let refused = || ViewRefusal::CutBeforeMove {
        moved_through: moved.mid.clone(),
    };
    let through = cut_through(view).ok_or_else(refused)?;
    match through_ordinal {
        Some(ordinal) if through != moved.mid && ordinal < moved.ordinal => Err(refused()),
        _ => Ok(()),
    }
}

/// Recheck against the current transcript: a running tool or a newly signed reply
/// may have appeared after acceptance. A refusal must leave the old view applied.
///
/// A view's inserted image is accepted whatever the session stated it takes: a provider repeats
/// an insert it made before (Magic Context repeats its image of the session's memories until its
/// next fold), so refusing it would hold the session on an older view. Its request shows the
/// image's provenance marker, never the image (`host::image_reference`).
pub fn check_boundary(
    entries: &[Entry],
    view: &View,
    provider_rules: &ProviderRules,
    floor: Floor<'_>,
) -> Result<(), ViewRefusal> {
    for (index, edit) in view.edits.iter().enumerate() {
        let EditOp::Insert { message, .. } = &edit.op else {
            continue;
        };
        // The view crate checks each image's media type and base64 (the compaction contract's
        // rule W6.5); its bytes must also be the PNG they claim to be, or every request rendering
        // the view would fail.
        if message
            .images
            .iter()
            .any(|image| super::host::image_reference(image).is_err())
        {
            return Err(ViewRefusal::UnsupportedEdit {
                index: index as u64,
            });
        }
    }
    render::plan_view(entries, view, provider_rules, floor).map(|_| ())
}

pub fn newest(entries: &[Entry]) -> Option<Cursor> {
    entries
        .last()
        .map(|entry| Cursor::new(entry.ordinal, &entry.mid, &entry.digest))
}

/// Size-bounded protocol paging. The caller loads only the selected transcript
/// projection page; cuts never affect which native entries remain readable.
pub fn page(entries: &[Entry], request: &ReadRequest, head: Head) -> ReadPage {
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
            entry = super::entries::compact(&entry);
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
