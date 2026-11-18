//! Which stored blocks a render plan reads the content of, so the rest can leave memory.
//!
//! A long session keeps every window message in its stored prompt, while the compaction view
//! sends only some of them: it leaves parts out, or sends replacement text in their place. A block
//! whose content no render under the plan reads can be released ([`ContentBlock::released`]),
//! keeping only what the render and the part numberings read of it: its kind, its ids and whether
//! it is empty. The rules here follow [`super::render_entry`] and [`super::render_result`]:
//!
//! - a block whose part the plan leaves out is read only for whether it is empty text, which a
//!   released block keeps;
//! - a text block whose part the plan sends with replaced `text` renders the replacement;
//! - a tool result is rendered with the call it answers, so it is read only when the plan keeps
//!   that call's part, and then not when the plan replaces its `output` (its media parts, which
//!   a released result keeps in place, are what the render keeps of it then);
//! - a tool call whose part is kept is read even with some input fields replaced: the render
//!   parses the original input and replaces only the named fields;
//! - a part sent with its tag prefix is read: the prefix is written onto the original text.
//!
//! The window's newest message is never released: the next record may still change it.

use std::collections::{HashMap, VecDeque};

use broca_protocol::{ContentBlock, Message, Role as HostRole};
use cortexkit_compaction_view::render::{PlanItem, PlanPart, RenderPlan};
use cortexkit_role_compaction::read::Entry;

/// The blocks of `stored`, by message position and block index, whose content a render of
/// `plan` over `entries` doesn't read. Only blocks that hold content worth releasing are named
/// (see [`ContentBlock::released`]), and none of the newest message's.
pub fn releasable(
    stored: &[Message],
    entries: &[Entry],
    plan: &RenderPlan,
) -> Vec<(usize, Vec<usize>)> {
    let kept = kept_parts(entries, plan);
    let newest = stored.len().saturating_sub(1);
    let mut out = Vec::new();
    for (position, message) in stored.iter().enumerate().take(newest) {
        let read = read_blocks(stored, &kept, position);
        let blocks: Vec<usize> = message
            .content
            .iter()
            .enumerate()
            .filter(|(index, block)| {
                !read.contains(index) && !block.holds_released() && block.released().is_some()
            })
            .map(|(index, _)| index)
            .collect();
        if !blocks.is_empty() {
            out.push((position, blocks));
        }
    }
    out
}

/// The positions of `stored` whose messages a render of `plan` reads a released block of: each
/// must be read back whole before the render.
pub fn released_but_read(stored: &[Message], entries: &[Entry], plan: &RenderPlan) -> Vec<usize> {
    let kept = kept_parts(entries, plan);
    (0..stored.len())
        .filter(|&position| {
            let read = read_blocks(stored, &kept, position);
            stored[position]
                .content
                .iter()
                .enumerate()
                .any(|(index, block)| read.contains(&index) && block.holds_released())
        })
        .collect()
}

/// The parts the plan keeps of each entry, by the entry's position in `entries`.
fn kept_parts<'a>(entries: &[Entry], plan: &'a RenderPlan) -> HashMap<usize, &'a [PlanPart]> {
    plan.items
        .iter()
        .filter_map(|item| match item {
            PlanItem::Entry {
                ordinal,
                mid,
                parts,
            } => super::position_of(entries, *ordinal, mid).map(|at| (at, parts.as_slice())),
            PlanItem::Insert { .. } => None,
        })
        .collect()
}

/// The indexes of the blocks of message `position` whose content a render reads.
fn read_blocks(
    stored: &[Message],
    kept: &HashMap<usize, &[PlanPart]>,
    position: usize,
) -> Vec<usize> {
    let message = &stored[position];
    let mut read = Vec::new();
    if let Some(parts) = kept.get(&position) {
        for part in *parts {
            let Ok(index) = part.id.parse::<usize>() else {
                continue;
            };
            let replaced_text = part.replace.iter().any(|field| field.field == "text");
            let text = matches!(
                message.content.get(index),
                Some(ContentBlock::Text { .. } | ContentBlock::TextWithState { .. })
            );
            if !(text && replaced_text) {
                read.push(index);
            }
        }
    }
    // A tool message's results are read with the calls they answer, in the message before it.
    if message.role == HostRole::Tool && position > 0 {
        if let Some(parts) = kept.get(&(position - 1)) {
            for (call, result) in answered_calls(&stored[position - 1], message) {
                let part = parts.iter().find(|part| part.id == call.to_string());
                let output_replaced = part
                    .is_some_and(|part| part.replace.iter().any(|field| field.field == "output"));
                if part.is_some() && !output_replaced {
                    read.push(result);
                }
            }
        }
    }
    read
}

/// Each tool call block of `assistant` with the block of `results` that answers it, paired as the
/// render pairs them: by call id, in order.
fn answered_calls(assistant: &Message, results: &Message) -> Vec<(usize, usize)> {
    let mut queues: HashMap<&str, VecDeque<usize>> = HashMap::new();
    for (index, block) in results.content.iter().enumerate() {
        if let ContentBlock::ToolResult(result) = block {
            queues
                .entry(result.tool_call_id.0.as_str())
                .or_default()
                .push_back(index);
        }
    }
    assistant
        .content
        .iter()
        .enumerate()
        .filter_map(|(index, block)| match block {
            ContentBlock::ToolCall(call) => queues
                .get_mut(call.tool_call_id.0.as_str())
                .and_then(VecDeque::pop_front)
                .map(|result| (index, result)),
            _ => None,
        })
        .collect()
}
