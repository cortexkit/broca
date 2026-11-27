//! Recover which numbered parts a model request actually showed, and where
//! their stored payloads live. Output-reference snapshots freeze that payload
//! mapping so later compaction cannot silently retarget an earlier reference.
//! Wire adapters own descriptors; this module owns applied-view visibility.

use cortexkit_compaction_view::render::{self, PlanItem, RenderPlan};
use std::collections::HashMap;

use super::protocol::{notice::Conditions, read::Entry};

/// The numbers the session's next request shows, keyed by `(mid, part id)`. That request
/// renders the newest accepted view when it still passes the boundary check, else the applied
/// view: the same choice `State::apply` makes at the next step boundary. A part the view drops,
/// or one that shows no number, is absent.
pub fn next_request(
    state: &super::state::State,
    entries: &[Entry],
    conditions: &Conditions,
) -> Result<HashMap<(String, String), u64>, String> {
    let mut numbers = HashMap::new();
    for item in next_render_plan(state, entries, conditions, None)?.items {
        let PlanItem::Entry { mid, parts, .. } = item else {
            continue;
        };
        for part in parts {
            if let Some(tag) = part.tag.filter(|_| part.prefix) {
                numbers.insert((mid.clone(), part.id), tag);
            }
        }
    }
    Ok(numbers)
}

/// The view the next request would use with the transcript and conditions currently held.
pub fn next_request_view<'a>(
    state: &'a super::state::State,
    entries: &[Entry],
    conditions: &Conditions,
    step: Option<u64>,
) -> Option<&'a super::protocol::view::View> {
    let next_boundary = || {
        state
            .latest()
            .filter(|view| {
                super::views::check_boundary(
                    entries,
                    view,
                    &conditions.rules,
                    super::host::view_floor(conditions),
                )
                .is_ok()
            })
            .or_else(|| state.applied())
    };
    match step.filter(|step| state.step_is_frozen(*step)) {
        Some(step) => state.view_for_step(step),
        None => next_boundary(),
    }
}

fn next_render_plan(
    state: &super::state::State,
    entries: &[Entry],
    conditions: &Conditions,
    step: Option<u64>,
) -> Result<RenderPlan, String> {
    let view = next_request_view(state, entries, conditions, step);
    let edits = view.map_or(&[][..], |view| view.edits.as_slice());
    let edits = render::edits_for_transcript(entries, edits);
    render::plan(entries, &edits)
        .map_err(|error| format!("next request render plan refused: {error:?}"))
}

/// The number `numbers` holds for a template `part_tag` reference. `part` is either the part id
/// `session.entries` serves or the part key `<mid>#<id>` `session.read` serves; a key naming
/// another message matches nothing.
pub fn lookup(numbers: &HashMap<(String, String), u64>, mid: &str, part: &str) -> Option<u64> {
    let id = match part.split_once('#') {
        Some((key_mid, id)) if key_mid == mid => id,
        Some(_) => return None,
        None => part,
    };
    numbers.get(&(mid.to_owned(), id.to_owned())).copied()
}
