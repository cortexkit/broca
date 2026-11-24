//! Plan notes (llm-runner `plan_notes`) in a request. While a plan change waits under policy
//! `notes_until_rebuild`, the request keeps the applied plan's system text and tools, and the
//! model learns of the change from short notes: system-role messages placed right after the user
//! turn of the run that delivered them. They are runner records, not conversation entries, so
//! each request inserts them here, as a view of the request only, at the same place every time.

use broca_protocol::{
    content::{ContentBlock, Message, Role},
    CallOptions,
};
/// The fixed note for a tool the pending plan removes, which is also the gate's refusal of a
/// call to it, and the policy name.
pub use cortexkit_role_llm_runner::plan_notes::{tool_removal, NOTES_UNTIL_REBUILD};

/// `opts` with each note inserted right after the message of its anchor, the 1-based ordinal of
/// the conversation entry it follows. Notes sharing an anchor go out in the order given. A note
/// whose anchor the request doesn't hold (a compaction view dropped it) is left out. `None` when
/// no note goes out, so the caller keeps its options as they are.
///
/// Inserted messages carry no ordinal, as a compaction view's inserted messages don't. Positions
/// that index the prompt move with the messages they point at: each declared cache marker, and
/// each run's place in the episode list unless the list reads places as raw ordinals.
pub fn view<'a>(
    opts: &CallOptions,
    notes: impl IntoIterator<Item = (u64, &'a str)>,
) -> Option<CallOptions> {
    // The last message of each anchor's entry: an entry may render as more than one message.
    let mut last_of = std::collections::HashMap::new();
    for (index, message) in opts.prompt.iter().enumerate() {
        if let Some(ordinal) = message.ordinal {
            last_of.insert(ordinal + 1, index);
        }
    }
    let mut after: std::collections::BTreeMap<usize, Vec<&str>> = Default::default();
    for (anchor, text) in notes {
        if let Some(&index) = last_of.get(&anchor) {
            after.entry(index).or_default().push(text);
        }
    }
    if after.is_empty() {
        return None;
    }
    let added: usize = after.values().map(Vec::len).sum();
    let mut prompt = Vec::with_capacity(opts.prompt.len() + added);
    // `moved[i]` is where the message at index `i` lands.
    let mut moved = Vec::with_capacity(opts.prompt.len() + 1);
    for (index, message) in opts.prompt.iter().enumerate() {
        moved.push(prompt.len());
        prompt.push(message.clone());
        for text in after.get(&index).into_iter().flatten() {
            let mut note = Message::new(
                Role::System,
                vec![ContentBlock::Text {
                    text: (*text).into(),
                }],
            );
            note.turn_system = true;
            prompt.push(note);
        }
    }
    // A position one past the end stays one past the end.
    moved.push(prompt.len());
    let mut view = opts.with_prompt(prompt);
    if !opts.render_episodes.raw_ordinals {
        let mut history = opts.render_episodes.as_ref().clone();
        for episode in &mut history.episodes {
            episode.prompt_start = moved[episode.prompt_start.min(opts.prompt.len())];
            episode.first_user = episode
                .first_user
                .map(|index| moved[index.min(opts.prompt.len())]);
        }
        view.render_episodes = std::sync::Arc::new(history);
    }
    for marker in &mut view.declared_markers {
        marker.message_index = moved[marker.message_index.min(opts.prompt.len())];
    }
    Some(view)
}

#[cfg(test)]
mod tests;
