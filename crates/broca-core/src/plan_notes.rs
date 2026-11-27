//! Plan notes (llm-runner `plan_notes`) in a request. While a plan change waits under policy
//! `notes_until_rebuild`, the request keeps the applied plan's system text and tools, and the
//! model learns of the change from short notes: system-role messages placed right after the user
//! turn of the run that delivered them. They are runner records, not conversation entries, so
//! each request inserts them here, as a view of the request only, at the same place every time.

use broca_protocol::{
    content::{ContentBlock, Message, Role},
    tool_addition::{AdditionPart, ToolAddition, ToolAdditionForm},
    CallOptions,
};
/// The fixed note for a tool the pending plan removes, which is also the gate's refusal of a
/// call to it, and the policy name.
pub use cortexkit_role_llm_runner::plan_notes::{tool_removal, NOTES_UNTIL_REBUILD};

pub enum Note<'a> {
    Text(&'a str),
    LegacyText(&'a str),
    ToolAddition(
        ToolAdditionForm,
        broca_protocol::tool_addition::ToolDefinition,
    ),
}

/// `opts` with each note inserted right after the message of its anchor, the 1-based ordinal of
/// the conversation entry it follows. Notes sharing an anchor go out in the order given. A note
/// whose anchor the request doesn't hold (a compaction view dropped it) is left out. `None` when
/// no note goes out, so the caller keeps its options as they are.
///
/// Inserted messages carry no ordinal. Declared cache markers index the prompt and move with
/// their messages; episode positions still refer to the same transcript ordinals.
pub fn view<'a>(
    opts: &CallOptions,
    notes: impl IntoIterator<Item = (u64, Note<'a>)>,
) -> Option<CallOptions> {
    view_for(opts, notes, false)
}

/// Anthropic accepts only one system message at an anchor, including when all its notes are text.
pub fn view_for<'a>(
    opts: &CallOptions,
    notes: impl IntoIterator<Item = (u64, Note<'a>)>,
    anthropic: bool,
) -> Option<CallOptions> {
    // The last message of each anchor's entry: an entry may render as more than one message.
    let mut last_of = std::collections::HashMap::new();
    for (index, message) in opts.prompt.iter().enumerate() {
        if let Some(ordinal) = message.ordinal {
            last_of.insert(ordinal + 1, index);
        }
    }
    let mut after: std::collections::BTreeMap<usize, Vec<Note<'_>>> = Default::default();
    for (anchor, note) in notes {
        if let Some(&index) = last_of.get(&anchor) {
            after.entry(index).or_default().push(note);
        }
    }
    if after.is_empty() {
        return None;
    }
    let added: usize = after.values().map(Vec::len).sum();
    let mut prompt = Vec::with_capacity(opts.prompt.len() + added);
    // `moved[i]` is where the message at index `i` lands.
    let mut moved = Vec::with_capacity(opts.prompt.len() + 1);
    let mut legacy_positions = Vec::with_capacity(opts.prompt.len() + 1);
    let mut legacy_count = 0;
    for (index, message) in opts.prompt.iter().enumerate() {
        moved.push(prompt.len());
        legacy_positions.push(index + legacy_count);
        prompt.push(message.clone());
        if let Some(notes) = after.remove(&index) {
            legacy_count += notes
                .iter()
                .filter(|note| matches!(note, Note::LegacyText(_)))
                .count();
            append_notes(&mut prompt, notes, anthropic);
        }
    }
    // A position one past the end stays one past the end.
    moved.push(prompt.len());
    let mut view = opts.with_prompt(prompt);
    if legacy_count != 0 && !opts.render_episodes.raw_ordinals {
        legacy_positions.push(opts.prompt.len() + legacy_count);
        let position = |index: usize| legacy_positions[index.min(opts.prompt.len())];
        for episode in &mut std::sync::Arc::make_mut(&mut view.render_episodes).episodes {
            episode.prompt_start = position(episode.prompt_start);
            episode.first_user = episode.first_user.map(position);
        }
    }
    for marker in &mut view.declared_markers {
        marker.message_index = moved[marker.message_index.min(opts.prompt.len())];
    }
    Some(view)
}

fn append_notes(prompt: &mut Vec<Message>, notes: Vec<Note<'_>>, anthropic: bool) {
    use ToolAdditionForm::AnthropicToolAddition;
    if notes.iter().any(|note| matches!(note, Note::LegacyText(_))) {
        let mut blocks = Vec::new();
        for note in notes {
            match note {
                Note::LegacyText(text) => {
                    append_notes(prompt, std::mem::take(&mut blocks), anthropic);
                    let mut message = text_message(vec![ContentBlock::Text { text: text.into() }]);
                    message.tags.legacy_plan_note = true;
                    prompt.push(message);
                }
                other => blocks.push(other),
            }
        }
        append_notes(prompt, blocks, anthropic);
        return;
    }
    if notes.is_empty() {
        return;
    }
    // Anthropic takes one system message after the user turn; the notes delivered together can
    // share that message without altering any note already sent at another anchor.
    let native = notes
        .iter()
        .any(|note| matches!(note, Note::ToolAddition(AnthropicToolAddition, _)));
    if (anthropic || native)
        && !notes.iter().any(
            |note| matches!(note, Note::ToolAddition(form, _) if *form != AnthropicToolAddition),
        )
    {
        let parts: Vec<_> = notes
            .into_iter()
            .map(|note| match note {
                Note::Text(text) | Note::LegacyText(text) => AdditionPart::Text(text.into()),
                Note::ToolAddition(_, tool) => AdditionPart::Tool(tool),
            })
            .collect();
        if native {
            prompt.push(addition_message(AnthropicToolAddition, parts));
        } else {
            prompt.push(text_message(
                parts
                    .into_iter()
                    .filter_map(|part| match part {
                        AdditionPart::Text(text) => Some(ContentBlock::Text { text }),
                        AdditionPart::Tool(_) => None,
                    })
                    .collect(),
            ));
        }
    } else {
        prompt.extend(notes.into_iter().map(|note| match note {
            Note::Text(text) | Note::LegacyText(text) => {
                text_message(vec![ContentBlock::Text { text: text.into() }])
            }
            Note::ToolAddition(form, tool) => {
                addition_message(form, vec![AdditionPart::Tool(tool)])
            }
        }));
    }
}

fn text_message(content: Vec<ContentBlock>) -> Message {
    let mut message = Message::new(Role::System, content);
    message.turn_system = true;
    message.tags.plan_note = true;
    message
}

fn addition_message(form: ToolAdditionForm, parts: Vec<AdditionPart>) -> Message {
    let mut message = Message::new(Role::System, Vec::new());
    message.turn_system = true;
    message.tool_addition = Some(ToolAddition { form, parts });
    message
}
#[cfg(test)]
mod tests;
