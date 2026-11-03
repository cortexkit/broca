//! Apply the shared render plan to native blocks. Neutral parts are descriptors,
//! not replacements for provider-owned signatures, continuation state or item ids.

use std::collections::HashMap;

use broca_protocol::{
    ContentBlock, MediaRef, Message, Role as HostRole, ToolOutput, ToolOutputPart,
};
use cortexkit_compaction_view::{
    clean,
    render::{self, PlanItem, PlanPart},
    tags,
};
use cortexkit_role_compaction::{
    part::{PartBody, ToolPart},
    read::{Entry, Role},
    view::{Edit, EditOp, InsertAt, InsertedImage},
};

/// Images are captured into the runner's media store before provider rendering.
/// Keeping capture at the host boundary also makes resumed requests use the same
/// content-addressed reference, rather than a second encoding of the view's data.
pub fn prompt(
    stored: &[Message],
    entries: &[Entry],
    edits: &[Edit],
    mut image: impl FnMut(&InsertedImage) -> Result<MediaRef, String>,
) -> Result<Vec<Message>, String> {
    if stored.len() != entries.len() {
        return Err("compaction transcript projection does not match native message count".into());
    }
    let plan = render::plan(entries, edits)
        .map_err(|error| format!("compaction render plan refused: {error:?}"))?;
    let positions: HashMap<_, _> = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| (entry.mid.as_str(), index))
        .collect();
    let result_positions: HashMap<_, _> = stored
        .iter()
        .enumerate()
        .flat_map(|(position, message)| {
            message
                .content
                .iter()
                .enumerate()
                .filter_map(move |(index, block)| match block {
                    ContentBlock::ToolResult(result) => {
                        Some((result.tool_call_id.0.as_str(), (position, index)))
                    }
                    _ => None,
                })
        })
        .collect();
    let mut result_blocks: HashMap<usize, Vec<(usize, ContentBlock)>> = HashMap::new();
    let mut output = Vec::new();
    for (order, item) in plan.items.into_iter().enumerate() {
        match item {
            PlanItem::Insert {
                id,
                role,
                texts,
                images,
            } => {
                let position = edits
                    .iter()
                    .find_map(|edit| match &edit.op {
                        EditOp::Insert { at, message } if message.id == id => Some(match at {
                            InsertAt::Start => 0,
                            InsertAt::Before(before) => positions[before.as_str()],
                            _ => unreachable!("render plan checked insert anchor"),
                        }),
                        _ => None,
                    })
                    .expect("render plan insert belongs to an edit");
                let role = match role {
                    Role::User => HostRole::User,
                    Role::Assistant => HostRole::Assistant,
                    Role::System => HostRole::System,
                    _ => unreachable!("render plan checked insert role"),
                };
                let mut content: Vec<_> = texts
                    .into_iter()
                    .map(|text| ContentBlock::Text { text })
                    .collect();
                for inserted in &images {
                    content.push(ContentBlock::Media(image(inserted)?));
                }
                output.push((position, 0, order, Message::new(role, content)));
            }
            PlanItem::Entry { mid, parts, .. } => {
                let position = positions[mid.as_str()];
                let original = &stored[position];
                let entry = &entries[position];
                let mut message = original.clone();
                message.tags = Default::default();
                message.content.clear();
                let mut kept = HashMap::new();
                for part in parts {
                    let index = part
                        .id
                        .parse::<usize>()
                        .map_err(|_| "compaction part id is not a native block index")?;
                    let source = original
                        .content
                        .get(index)
                        .ok_or("compaction part id exceeds native block count")?;
                    let neutral = entry
                        .parts
                        .iter()
                        .find(|candidate| candidate.id == part.id)
                        .ok_or("compaction part missing from projection")?;
                    let mut block = source.clone();
                    match (&mut block, &neutral.body) {
                        (
                            ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. },
                            PartBody::Text { text: clean_text },
                        ) => {
                            let replaced = part.replace.iter().find(|field| field.field == "text");
                            if let Some(replaced) = replaced {
                                block = ContentBlock::Text {
                                    text: replaced.value.as_str().expect("checked text").into(),
                                };
                            } else if part.prefix {
                                *text = numbered_reply(
                                    text,
                                    clean_text,
                                    part.tag.expect("numbered text has a tag"),
                                    original.role == HostRole::Assistant,
                                );
                            } else {
                                *text = clean_text.clone();
                            }
                        }
                        (ContentBlock::ToolCall(call), PartBody::Tool(tool)) => {
                            for field in &part.replace {
                                if let Some(key) = field.field.strip_prefix("input.") {
                                    call.input[key] = field.value.clone();
                                }
                            }
                            if let Some(&(result_position, result_index)) =
                                result_positions.get(call.tool_call_id.0.as_str())
                            {
                                let ContentBlock::ToolResult(mut result) =
                                    stored[result_position].content[result_index].clone()
                                else {
                                    unreachable!()
                                };
                                render_result(&mut result.output, tool, &part);
                                result_blocks
                                    .entry(result_position)
                                    .or_default()
                                    .push((result_index, ContentBlock::ToolResult(result)));
                            }
                        }
                        (ContentBlock::Media(_), PartBody::File(_)) => {
                            if let Some(field) =
                                part.replace.iter().find(|field| field.field == "text")
                            {
                                block = ContentBlock::Text {
                                    text: field
                                        .value
                                        .as_str()
                                        .expect("checked file replacement")
                                        .into(),
                                };
                            }
                        }
                        _ => {}
                    }
                    kept.insert(index, block);
                }
                for (index, source) in original.content.iter().enumerate() {
                    if let Some(block) = kept.remove(&index) {
                        message.content.push(block);
                    } else if matches!(source, ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } if text.is_empty())
                    {
                        // Empty native separators have no neutral part or number, but can
                        // carry provider state between the kept signed blocks of a row.
                        message.content.push(source.clone());
                    }
                }
                output.push((position, 1, order, message));
            }
        }
    }
    for (position, mut blocks) in result_blocks {
        blocks.sort_by_key(|(index, _)| *index);
        let mut message = stored[position].clone();
        message.tags = Default::default();
        message.content = blocks.into_iter().map(|(_, block)| block).collect();
        output.push((position, 1, 0, message));
    }
    output.sort_by_key(|(position, phase, order, _)| (*position, *phase, *order));
    Ok(output
        .into_iter()
        .map(|(_, _, _, message)| message)
        .collect())
}

fn numbered_reply(original: &str, neutral: &str, tag: u64, assistant: bool) -> String {
    if assistant {
        let cleaned = clean::clean_reply(original);
        if cleaned.model_tag == Some(tag) {
            let after_open = original.strip_prefix('§').unwrap_or_default();
            if let Some((_, remainder)) = after_open.split_once('§') {
                if !remainder.contains('§') {
                    return original.into();
                }
            }
        }
    }
    format!("{}{neutral}", tags::tag_prefix(tag))
}

fn render_result(output: &mut ToolOutput, tool: &ToolPart, plan: &PlanPart) {
    let mut text = tool.output.clone().unwrap_or_default();
    let mut attachment_ids: Option<Vec<String>> = None;
    let mut delivery_indexes: Option<Vec<u64>> = None;
    for field in &plan.replace {
        match field.field.as_str() {
            "output" => text = field.value.as_str().expect("checked output").into(),
            "attachments" => {
                attachment_ids =
                    Some(serde_json::from_value(field.value.clone()).expect("checked attachments"))
            }
            "delivered" => {
                delivery_indexes =
                    Some(serde_json::from_value(field.value.clone()).expect("checked delivered"))
            }
            _ => {}
        }
    }
    if plan.prefix {
        if text.is_empty() {
            if let Some(label) = &plan.empty_result_label {
                text = label.clone();
            }
        }
        text.insert_str(
            0,
            &tags::tag_prefix(plan.tag.expect("numbered result has a tag")),
        );
    }
    // Delivered bytes are not output edits: a replace of output must not erase a
    // reminder, and a delivered edit can remove one without touching the result.
    for (index, delivered) in tool.delivered.iter().enumerate() {
        if delivery_indexes
            .as_ref()
            .is_none_or(|indexes| indexes.contains(&(index as u64)))
        {
            if let Some(delivered) = &delivered.text {
                text.push_str("\n\n");
                text.push_str(delivered);
            }
        }
    }
    match output {
        ToolOutput::Text { text: target } => *target = text,
        ToolOutput::Error { message, .. } => *message = text,
        ToolOutput::Denied { reason } => *reason = text,
        ToolOutput::Json { .. } => *output = ToolOutput::Text { text },
        ToolOutput::Parts { parts } => {
            let mut kept = vec![ToolOutputPart::Text { text }];
            kept.extend(parts.iter().enumerate().filter_map(|(index, part)| {
                if matches!(part, ToolOutputPart::Media { .. })
                    && attachment_ids
                        .as_ref()
                        .is_none_or(|ids| ids.contains(&index.to_string()))
                {
                    Some(part.clone())
                } else {
                    None
                }
            }));
            *parts = kept;
        }
    }
}
