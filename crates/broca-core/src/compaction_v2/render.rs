//! Apply the shared render plan to native blocks. Neutral parts are descriptors,
//! not replacements for provider-owned signatures, continuation state or item ids.

use std::collections::{HashMap, VecDeque};

use broca_protocol::{
    ContentBlock, MediaRef, Message, Role as HostRole, ToolOutput, ToolOutputPart,
};
use cortexkit_compaction_view::{
    clean,
    render::{self, PlanItem, PlanPart},
    tags,
};
use cortexkit_role_compaction::{
    part::{PartBody, ToolPart, ToolStatus},
    read::{Entry, Role},
    view::{Edit, EditOp, InsertAt, InsertedImage},
};

/// Assemble a mode-3 request from the durable transcript and the view applied to this step.
/// The boundary options retain live-only settings and decorated tool outputs; callers rebuilding
/// offline must restore those decorations before calling. Image capture belongs to the live
/// boundary, so this pure renderer only recreates content-addressed image references.
pub fn request_options(
    records: &[broca_wal::FramedRecord],
    lineage: &str,
    boundary: &broca_protocol::CallOptions,
    applied: Option<&cortexkit_role_compaction::view::View>,
) -> Result<broca_protocol::CallOptions, String> {
    let transcript = super::transcript::project(records, lineage)?;
    let (config, manifest) = super::host::frozen(records)?;
    let prompt = if manifest.is_some_and(crate::compaction::has_compaction) {
        prompt(
            &boundary.prompt,
            &transcript.entries,
            applied.map_or(&[], |view| view.edits.as_slice()),
            super::host::image_reference,
        )?
    } else {
        deliveries_only(&boundary.prompt, &transcript.entries)?
    };
    let mut opts = options(boundary, prompt);
    opts.ephemeral_session |=
        crate::retention::is_retention_records(records.iter().map(|frame| &frame.record));
    let globals = super::systems::frozen(true, &config, manifest)?;
    Ok(super::systems::view(&opts, &globals)?.unwrap_or(opts))
}

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
    let edits = render::edits_for_transcript(entries, edits);
    let plan = render::plan(entries, &edits)
        .map_err(|error| format!("compaction render plan refused: {error:?}"))?;
    let positions: HashMap<_, _> = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| (entry.mid.as_str(), index))
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
                let mut message = Message::new(role, content);
                message.turn_system = message.role == HostRole::System;
                output.push((position, 0, order, message));
            }
            PlanItem::Entry { mid, parts, .. } => {
                let position = positions[mid.as_str()];
                let original = &stored[position];
                let entry = &entries[position];
                let mut result_queues: HashMap<&str, VecDeque<usize>> = HashMap::new();
                if let Some(next) = stored
                    .get(position + 1)
                    .filter(|next| next.role == HostRole::Tool)
                {
                    for (index, block) in next.content.iter().enumerate() {
                        if let ContentBlock::ToolResult(result) = block {
                            result_queues
                                .entry(&result.tool_call_id.0)
                                .or_default()
                                .push_back(index);
                        }
                    }
                }
                let mut result_positions = HashMap::new();
                for (index, block) in original.content.iter().enumerate() {
                    if let ContentBlock::ToolCall(call) = block {
                        if let Some(result) = result_queues
                            .get_mut(call.tool_call_id.0.as_str())
                            .and_then(VecDeque::pop_front)
                        {
                            result_positions.insert(index, result);
                        }
                    }
                }
                let mut message = original.clone();
                message.ordinal = Some(entry.ordinal - 1);
                message.tags = Default::default();
                message.content.clear();
                let mut kept_indices = Vec::new();
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
                            if let Some(&result_index) = result_positions.get(&index) {
                                let result_position = position + 1;
                                let ContentBlock::ToolResult(mut result) =
                                    stored[result_position].content[result_index].clone()
                                else {
                                    unreachable!()
                                };
                                render_result(&mut result.output, tool, &part);
                                if matches!(tool.status, ToolStatus::Error | ToolStatus::Denied) {
                                    result.is_error = true;
                                }
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
                        kept_indices.push(index);
                        message.content.push(block);
                    } else if matches!(source, ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } if text.is_empty())
                    {
                        // Empty native separators have no neutral part or number, but can
                        // carry provider state between the kept signed blocks of a row.
                        kept_indices.push(index);
                        message.content.push(source.clone());
                    }
                }
                message.cache_prefix_blocks = remap_cache_prefix(original, &kept_indices);
                output.push((position, 1, order, message));
            }
        }
    }
    for (position, mut blocks) in result_blocks {
        blocks.sort_by_key(|(index, _)| *index);
        let mut message = stored[position].clone();
        message.ordinal = Some(entries[position].ordinal - 1);
        message.tags = Default::default();
        message.cache_prefix_blocks = remap_cache_prefix(
            &stored[position],
            &blocks.iter().map(|(index, _)| *index).collect::<Vec<_>>(),
        );
        message.content = blocks.into_iter().map(|(_, block)| block).collect();
        output.push((position, 1, 0, message));
    }
    output.sort_by_key(|(position, phase, order, _)| (*position, *phase, *order));
    Ok(output
        .into_iter()
        .map(|(_, _, _, message)| message)
        .collect())
}

/// Keep episode boundaries in native coordinates while translating explicit
/// marker indices to the rendered prompt. Inserted messages inherit neither.
/// Unnumbered sessions still render tool-result deliveries separately from the
/// stored output. Messages without a delivery remain exact native copies.
pub fn deliveries_only(stored: &[Message], entries: &[Entry]) -> Result<Vec<Message>, String> {
    if stored.len() != entries.len() {
        return Err("compaction delivery projection does not match native message count".into());
    }
    let mut prompt = stored.to_vec();
    for (position, entry) in entries.iter().enumerate() {
        let mut results: HashMap<&str, VecDeque<usize>> = HashMap::new();
        if let Some(next) = stored
            .get(position + 1)
            .filter(|message| message.role == HostRole::Tool)
        {
            for (index, block) in next.content.iter().enumerate() {
                if let ContentBlock::ToolResult(result) = block {
                    results
                        .entry(&result.tool_call_id.0)
                        .or_default()
                        .push_back(index);
                }
            }
        }
        for part in &entry.parts {
            let PartBody::Tool(tool) = &part.body else {
                continue;
            };
            let block = part
                .id
                .parse::<usize>()
                .map_err(|_| "compaction tool part has no native location")?;
            let Some(ContentBlock::ToolCall(call)) = stored[position].content.get(block) else {
                return Err("compaction tool part does not name a native call".into());
            };
            let result = results
                .get_mut(call.tool_call_id.0.as_str())
                .and_then(VecDeque::pop_front);
            if tool.delivered.is_empty() {
                continue;
            }
            let result = result.ok_or("compaction delivery has no native tool result")?;
            let ContentBlock::ToolResult(result) = &mut prompt[position + 1].content[result] else {
                unreachable!()
            };
            let mut text = super::entries::tool_output(&result.output).1;
            for delivery in &tool.delivered {
                if let Some(delivery) = &delivery.text {
                    text.push_str("\n\n");
                    text.push_str(delivery);
                }
            }
            set_result_text(&mut result.output, text, None);
            if matches!(tool.status, ToolStatus::Error | ToolStatus::Denied) {
                result.is_error = true;
            }
        }
    }
    Ok(prompt)
}

pub fn options(
    opts: &broca_protocol::CallOptions,
    prompt: Vec<Message>,
) -> broca_protocol::CallOptions {
    let positions: HashMap<_, _> = prompt
        .iter()
        .enumerate()
        .filter_map(|(index, message)| message.ordinal.map(|ordinal| (ordinal, index)))
        .collect();
    let markers = opts
        .declared_markers
        .iter()
        .filter_map(|marker| {
            let ordinal = opts.prompt.get(marker.message_index)?.ordinal?;
            Some(broca_protocol::request::DeclaredMessageMarker {
                message_index: *positions.get(&ordinal)?,
                rank: marker.rank,
            })
        })
        .collect();
    let mut view = opts.with_prompt(prompt);
    let mut history = opts.render_episodes.as_ref().clone();
    history.raw_ordinals = true;
    view.render_episodes = std::sync::Arc::new(history);
    view.declared_markers = markers;
    view
}

fn remap_cache_prefix(original: &Message, kept: &[usize]) -> Option<u32> {
    // The marker belongs to its original ending block. Removing that block
    // removes the marker instead of moving it onto another kept part.
    original.cache_prefix_blocks.map(|count| {
        let end = (count as usize).min(original.content.len());
        if end == 0 {
            return 0;
        }
        kept.iter()
            .position(|index| *index == end - 1)
            .map_or(0, |position| position as u32 + 1)
    })
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
    // The native request may carry frozen model-only guidance. Keep it unless
    // the view explicitly replaces output; validation still used the stored part.
    let mut text = super::entries::tool_output(output).1;
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
    set_result_text(output, text, attachment_ids.as_deref());
}

fn set_result_text(output: &mut ToolOutput, text: String, attachment_ids: Option<&[String]>) {
    match output {
        ToolOutput::Text { text: target } => *target = text,
        ToolOutput::Error { .. } | ToolOutput::Denied { .. } => *output = ToolOutput::error(text),
        ToolOutput::Json { .. } => *output = ToolOutput::Text { text },
        ToolOutput::Parts { parts } => {
            let mut kept = vec![ToolOutputPart::Text { text }];
            kept.extend(parts.iter().enumerate().filter_map(|(index, part)| {
                if matches!(part, ToolOutputPart::Media { .. })
                    && attachment_ids.is_none_or(|ids| ids.contains(&index.to_string()))
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
