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
    rules::ProviderRules,
    view::{Edit, EditOp, InsertAt, InsertedImage, View},
};

mod release;
pub use release::releasable;
pub use release::released_but_read;

/// Why a render refuses a part whose block left memory: the caller reads it back first
/// ([`released_but_read`]), so reaching this is a bug, and a refusal is safer than a request
/// with the released placeholder in it.
const RELEASED_READ: &str = "a part the view sends was released from memory and not read back";

pub fn prompt_with_rides(
    stored: &[Message],
    entries: &[Entry],
    rides: &super::entries::RideEnvelopes,
    edits: &[Edit],
    image: impl FnMut(&InsertedImage) -> Result<MediaRef, String>,
) -> Result<Vec<Message>, String> {
    let edits = render::edits_for_transcript(entries, edits);
    let plan = render::plan(entries, &edits)
        .map_err(|error| format!("compaction render plan refused: {error:?}"))?;
    render_planned(stored, entries, rides, &edits, plan, image, None)
}

/// Plan the complete applied view, including permission to strip newer signatures.
/// Reverts filter obsolete edit targets, but preserve `built_through`: it marks the
/// last message the provider had read when it authorized stripping newer signatures.
/// `floor` is the session's signed floor (`host::view_floor`), which the plan honours as the
/// view checks do; the request's own strip removes the parts it covers.
pub fn prompt_view_with_rides(
    stored: &[Message],
    entries: &[Entry],
    rides: &super::entries::RideEnvelopes,
    view: Option<&View>,
    rules: &ProviderRules,
    floor: cortexkit_compaction_view::rules::Floor<'_>,
    image: impl FnMut(&InsertedImage) -> Result<MediaRef, String>,
) -> Result<Vec<Message>, String> {
    prompt_view_kept(stored, entries, rides, view, rules, floor, image, None)
}

/// [`prompt_view_with_rides`], reusing what `kept` holds of the entries an earlier render
/// rendered, and keeping what this one renders for the next (see [`RenderCache`]).
#[allow(clippy::too_many_arguments)]
pub fn prompt_view_kept(
    stored: &[Message],
    entries: &[Entry],
    rides: &super::entries::RideEnvelopes,
    view: Option<&View>,
    rules: &ProviderRules,
    floor: cortexkit_compaction_view::rules::Floor<'_>,
    image: impl FnMut(&InsertedImage) -> Result<MediaRef, String>,
    kept: Option<&RenderCache>,
) -> Result<Vec<Message>, String> {
    let (edits, plan) = plan_for_render(entries, view, rules, floor)?;
    render_planned(stored, entries, rides, &edits, plan, image, kept)
}

/// Renders `plan`, which [`plan_for_render`] made with `edits`, over `stored`, as
/// [`prompt_view_kept`] does after planning: for a caller that reads the plan first (see
/// [`released_but_read`]).
pub fn prompt_planned(
    stored: &[Message],
    entries: &[Entry],
    rides: &super::entries::RideEnvelopes,
    edits: &[Edit],
    plan: render::RenderPlan,
    image: impl FnMut(&InsertedImage) -> Result<MediaRef, String>,
    kept: Option<&RenderCache>,
) -> Result<Vec<Message>, String> {
    render_planned(stored, entries, rides, edits, plan, image, kept)
}

/// What rendering `view` over `entries` keeps of them, before any message is rendered: the
/// view's edits whose targets the transcript still holds, and the plan the shared view crate
/// makes from them (no view keeps every entry whole). It checks the view's checksum first.
/// `floor` is the session's signed floor (`host::view_floor`).
pub fn plan_for_render(
    entries: &[Entry],
    view: Option<&View>,
    rules: &ProviderRules,
    floor: cortexkit_compaction_view::rules::Floor<'_>,
) -> Result<(Vec<Edit>, render::RenderPlan), String> {
    let Some(view) = view else {
        let edits = render::edits_for_transcript(entries, &[]);
        let plan = render::plan(entries, &edits)
            .map_err(|error| format!("compaction render plan refused: {error:?}"))?;
        return Ok((edits, plan));
    };
    if !view.verify() {
        return Err("compaction render plan refused: BadChecksum".into());
    }
    let filtered = View::seal_with_policy(
        view.version,
        render::edits_for_transcript(entries, &view.edits),
        view.built_through.clone(),
        view.new_signed.clone(),
    );
    let plan = render::plan_view(entries, &filtered, rules, floor)
        .map_err(|error| format!("compaction render plan refused: {error:?}"))?;
    Ok((filtered.edits, plan))
}

/// What a run's renders keep of each transcript entry they rendered: the entry's rendered
/// message and tool results, with everything they were rendered from. The next render reuses
/// an entry's whose inputs are unchanged (the entry by ordinal, id and digest, the parts the
/// plan keeps of it, its stored message and the tool message after it, and its rides'
/// envelopes) rather than rendering it again, and keeps only the entries it rendered. Debug
/// builds render every reused entry again and compare.
#[derive(Default)]
pub struct RenderCache {
    kept: std::sync::Mutex<HashMap<u64, RenderedEntry>>,
    rendered: std::sync::atomic::AtomicU64,
    shown: std::sync::Mutex<Vec<u64>>,
}

impl RenderCache {
    /// How many entries renders through this cache rendered rather than reused.
    pub fn rendered(&self) -> u64 {
        self.rendered.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The part numbers the latest render through this cache showed the model, ascending: each
    /// planned part whose number the render prefixes to its text or tool result. A step's last
    /// render is the request it sends.
    pub fn shown_tags(&self) -> Vec<u64> {
        self.shown.lock().expect("shown part numbers").clone()
    }
}

/// One entry as a render left it (see [`RenderCache`]).
struct RenderedEntry {
    mid: String,
    digest: String,
    parts: Vec<PlanPart>,
    original: Message,
    next: Option<Message>,
    rides: Vec<Option<Vec<Option<broca_records::record::RideMetadata>>>>,
    message: Message,
    results: Vec<(usize, ContentBlock)>,
    held: Vec<(usize, String)>,
    after_results: bool,
}

/// One entry's rendered message, the tool-message blocks that answer its kept calls (each with
/// its index in that message), and the ride texts the render held out of those results (each
/// with the index of the result that carries it; empty unless the rides render after results).
type RenderedParts = (Message, Vec<(usize, ContentBlock)>, Vec<(usize, String)>);

/// The ride texts a tool message holds out of its results, in the order the request shows them:
/// by the carrying result's place in the message, and each result's rides in delivery order.
/// That is delivery order: a batch's rides ride on its last carrying result, and batches commit
/// in result order. Probe only (see `RideEnvelopes::after_results`).
fn held_in_order(mut held: Vec<(usize, String)>) -> Vec<String> {
    held.sort_by_key(|(index, _)| *index);
    held.into_iter().map(|(_, text)| text).collect()
}

/// The inputs entry `position` renders from beyond itself: the tool message after its stored
/// message, and the ride envelopes of each of its parts.
#[allow(clippy::type_complexity)]
fn entry_inputs<'a>(
    stored: &'a [Message],
    entry: &Entry,
    rides: &'a super::entries::RideEnvelopes,
    position: usize,
) -> (
    Option<&'a Message>,
    Vec<Option<&'a Vec<Option<broca_records::record::RideMetadata>>>>,
) {
    let next = stored
        .get(position + 1)
        .filter(|next| next.role == HostRole::Tool);
    let rides = entry
        .parts
        .iter()
        .map(|part| rides.of_part(&entry.mid, &part.id))
        .collect();
    (next, rides)
}

/// The position of the entry with ordinal `ordinal` and id `mid` in `entries`, which come in
/// ordinal order; by its id alone, the last entry with it, should they not.
fn position_of(entries: &[Entry], ordinal: u64, mid: &str) -> Option<usize> {
    entries
        .binary_search_by_key(&ordinal, |entry| entry.ordinal)
        .ok()
        .filter(|at| entries[*at].mid == mid)
        .or_else(|| entries.iter().rposition(|entry| entry.mid == mid))
}

fn render_planned(
    stored: &[Message],
    entries: &[Entry],
    rides: &super::entries::RideEnvelopes,
    edits: &[Edit],
    plan: render::RenderPlan,
    mut image: impl FnMut(&InsertedImage) -> Result<MediaRef, String>,
    kept: Option<&RenderCache>,
) -> Result<Vec<Message>, String> {
    if stored.len() != entries.len() {
        return Err("compaction transcript projection does not match native message count".into());
    }
    // Where each message an insert is anchored before sits: found through the plan's own item
    // for it, by ordinal, rather than by indexing every entry.
    let mut anchors: HashMap<&str, usize> = HashMap::new();
    for edit in edits {
        if let EditOp::Insert {
            at: InsertAt::Before(before),
            ..
        } = &edit.op
        {
            let ordinal = plan.items.iter().find_map(|item| match item {
                PlanItem::Entry { ordinal, mid, .. } if mid == before => Some(*ordinal),
                _ => None,
            });
            let position = match ordinal {
                Some(ordinal) => position_of(entries, ordinal, before),
                None => entries.iter().rposition(|entry| &entry.mid == before),
            };
            if let Some(position) = position {
                anchors.insert(before.as_str(), position);
            }
        }
    }
    let mut previous =
        kept.map(|kept| std::mem::take(&mut *kept.kept.lock().expect("render cache")));
    let mut kept_now = HashMap::new();
    let mut result_blocks: HashMap<usize, Vec<(usize, ContentBlock)>> = HashMap::new();
    let mut held_rides: HashMap<usize, Vec<(usize, String)>> = HashMap::new();
    let mut output = Vec::new();
    if let Some(cache) = kept {
        let mut shown: Vec<u64> = plan
            .items
            .iter()
            .filter_map(|item| match item {
                PlanItem::Entry { parts, .. } => Some(parts),
                _ => None,
            })
            .flatten()
            .filter(|part| part.prefix)
            .filter_map(|part| part.tag)
            .collect();
        shown.sort_unstable();
        shown.dedup();
        *cache.shown.lock().expect("shown part numbers") = shown;
    }
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
                            InsertAt::Before(before) => anchors[before.as_str()],
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
                    .map(|text| ContentBlock::Text { text: text.into() })
                    .collect();
                for inserted in &images {
                    content.push(ContentBlock::Media(image(inserted)?));
                }
                let mut message = Message::new(role, content);
                message.turn_system = message.role == HostRole::System;
                output.push((position, 0, order, message));
            }
            PlanItem::Entry {
                ordinal,
                mid,
                parts,
            } => {
                let position = position_of(entries, ordinal, &mid)
                    .ok_or("compaction render plan names an entry the transcript doesn't hold")?;
                let entry = &entries[position];
                let Some(kept) = kept else {
                    let (message, results, held) =
                        render_entry(stored, entries, rides, position, &mid, &parts)?;
                    if !results.is_empty() {
                        result_blocks
                            .entry(position + 1)
                            .or_default()
                            .extend(results);
                    }
                    if !held.is_empty() {
                        held_rides.entry(position + 1).or_default().extend(held);
                    }
                    output.push((position, 1, order, message));
                    continue;
                };
                let (next, entry_rides) = entry_inputs(stored, entry, rides, position);
                let reused = previous
                    .as_mut()
                    .and_then(|previous| previous.remove(&ordinal))
                    .filter(|was| {
                        was.mid == entry.mid
                            && was.digest == entry.digest
                            && was.parts == parts
                            && was.after_results == rides.after_results()
                            && was.original == stored[position]
                            && was.next.as_ref() == next
                            && was.rides.len() == entry_rides.len()
                            && was
                                .rides
                                .iter()
                                .zip(&entry_rides)
                                .all(|(was, now)| was.as_ref() == *now)
                    });
                // An entry kept from the last render goes back into the cache as it was, its
                // inputs being equal to this render's; only an entry rendered now copies them.
                let entry_now = match reused {
                    Some(was) => {
                        #[cfg(debug_assertions)]
                        assert!(
                            render_entry(stored, entries, rides, position, &mid, &parts)?
                                == (was.message.clone(), was.results.clone(), was.held.clone()),
                            "a kept rendered entry differs from the entry rendered again"
                        );
                        was
                    }
                    None => {
                        let (message, results, held) =
                            render_entry(stored, entries, rides, position, &mid, &parts)?;
                        kept.rendered
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        RenderedEntry {
                            mid,
                            digest: entry.digest.to_string(),
                            parts,
                            original: stored[position].clone(),
                            next: next.cloned(),
                            rides: entry_rides
                                .into_iter()
                                .map(|rides| rides.cloned())
                                .collect(),
                            message,
                            results,
                            held,
                            after_results: rides.after_results(),
                        }
                    }
                };
                if !entry_now.results.is_empty() {
                    result_blocks
                        .entry(position + 1)
                        .or_default()
                        .extend(entry_now.results.iter().cloned());
                }
                if !entry_now.held.is_empty() {
                    held_rides
                        .entry(position + 1)
                        .or_default()
                        .extend(entry_now.held.iter().cloned());
                }
                output.push((position, 1, order, entry_now.message.clone()));
                kept_now.insert(ordinal, entry_now);
            }
        }
    }
    if let Some(kept) = kept {
        *kept.kept.lock().expect("render cache") = kept_now;
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
        if let Some(held) = held_rides.remove(&position) {
            message.tags.set_rides_after_results(held_in_order(held));
        }
        output.push((position, 1, 0, message));
    }
    output.sort_by_key(|(position, phase, order, _)| (*position, *phase, *order));
    Ok(output
        .into_iter()
        .map(|(_, _, _, message)| message)
        .collect())
}

/// Entry `position`'s message as the plan keeps it, with the parts `parts`, and the blocks of
/// the tool message after it that answer its kept calls, each with its index in that message.
fn render_entry(
    stored: &[Message],
    entries: &[Entry],
    rides: &super::entries::RideEnvelopes,
    position: usize,
    mid: &str,
    parts: &[PlanPart],
) -> Result<RenderedParts, String> {
    let mut results = Vec::new();
    let mut held = Vec::new();
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
    for part in parts.iter().cloned() {
        let index = part
            .id
            .parse::<usize>()
            .map_err(|_| "compaction part id is not a native block index")?;
        let source = original
            .content
            .get(index)
            .ok_or("compaction part id exceeds native block count")?;
        let replaced_text = matches!(
            source,
            ContentBlock::Text { .. } | ContentBlock::TextWithState { .. }
        ) && part.replace.iter().any(|field| field.field == "text");
        if source.holds_released() && !replaced_text {
            return Err(RELEASED_READ.into());
        }
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
                    )
                    .into();
                } else {
                    *text = clean_text.as_str().to_owned().into();
                }
            }
            (ContentBlock::ToolCall(call), PartBody::Tool(tool)) => {
                let mut input = None;
                for field in &part.replace {
                    if let Some(key) = field.field.strip_prefix("input.") {
                        input.get_or_insert_with(|| call.input.parse())[key] = field.value.clone();
                    }
                }
                if let Some(input) = input {
                    call.input = input.into();
                }
                if let Some(&result_index) = result_positions.get(&index) {
                    let result_position = position + 1;
                    let ContentBlock::ToolResult(mut result) =
                        stored[result_position].content[result_index].clone()
                    else {
                        unreachable!()
                    };
                    let output_replaced = part.replace.iter().any(|field| field.field == "output");
                    if result.output.holds_released() && !output_replaced {
                        return Err(RELEASED_READ.into());
                    }
                    for text in render_result(&mut result.output, tool, &part, mid, rides) {
                        held.push((result_index, text));
                    }
                    if matches!(tool.status, ToolStatus::Error | ToolStatus::Denied) {
                        result.is_error = true;
                    }
                    debug_assert_eq!(result_position, position + 1);
                    results.push((result_index, ContentBlock::ToolResult(result)));
                }
            }
            (ContentBlock::Media(_), PartBody::File(_)) => {
                if let Some(field) = part.replace.iter().find(|field| field.field == "text") {
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
    Ok((message, results, held))
}

pub fn deliveries_only_with_rides(
    stored: &[Message],
    entries: &[Entry],
    rides: &super::entries::RideEnvelopes,
) -> Result<Vec<Message>, String> {
    if stored.len() != entries.len() {
        return Err("compaction delivery projection does not match native message count".into());
    }
    let mut prompt = stored.to_vec();
    let mut held_rides: HashMap<usize, Vec<(usize, String)>> = HashMap::new();
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
            let Some(delivered) = tool.delivered() else {
                continue;
            };
            let result_index = result.ok_or("compaction delivery has no native tool result")?;
            let ContentBlock::ToolResult(result) = &mut prompt[position + 1].content[result_index]
            else {
                unreachable!()
            };
            let mut held = Vec::new();
            for (index, delivery) in delivered.iter().enumerate() {
                append_delivery(
                    &mut result.output,
                    delivery,
                    rides.get(&entry.mid, &part.id, index),
                    rides.after_results().then_some(&mut held),
                );
            }
            if !held.is_empty() {
                held_rides
                    .entry(position + 1)
                    .or_default()
                    .extend(held.into_iter().map(|text| (result_index, text)));
            }
            if matches!(tool.status, ToolStatus::Error | ToolStatus::Denied) {
                result.is_error = true;
            }
        }
    }
    for (position, held) in held_rides {
        prompt[position]
            .tags
            .set_rides_after_results(held_in_order(held));
    }
    Ok(prompt)
}

/// `opts` with `prompt`, a rendered view of its prompt: each declared marker moves to the
/// rendered message with its marked message's ordinal, and the episode list reads each run's
/// place as a raw ordinal. A list already read that way is shared, not copied.
pub fn options(
    opts: &broca_protocol::CallOptions,
    prompt: Vec<Message>,
) -> broca_protocol::CallOptions {
    let markers = if opts.declared_markers.is_empty() {
        Vec::new()
    } else {
        let positions: HashMap<_, _> = prompt
            .iter()
            .enumerate()
            .filter_map(|(index, message)| message.ordinal.map(|ordinal| (ordinal, index)))
            .collect();
        opts.declared_markers
            .iter()
            .filter_map(|marker| {
                let ordinal = opts.prompt.get(marker.message_index)?.ordinal?;
                Some(broca_protocol::request::DeclaredMessageMarker {
                    message_index: *positions.get(&ordinal)?,
                    rank: marker.rank,
                })
            })
            .collect()
    };
    let mut view = opts.with_prompt(prompt);
    view.render_episodes = raw_ordinals(&opts.render_episodes);
    view.declared_markers = markers;
    view
}

/// `episodes` reading each run's place as a raw ordinal: shared when it already does, a copy
/// that does otherwise.
pub fn raw_ordinals(
    episodes: &std::sync::Arc<broca_protocol::request::RenderHistory>,
) -> std::sync::Arc<broca_protocol::request::RenderHistory> {
    if episodes.raw_ordinals {
        return std::sync::Arc::clone(episodes);
    }
    let mut history = episodes.as_ref().clone();
    history.raw_ordinals = true;
    std::sync::Arc::new(history)
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
        // Pinned to plan version 3's cleaning: broca's request rendering must keep the cleaning a
        // run was recorded with, and moving to the newer rule is a separate change that freezes the
        // cleaning per run.
        let cleaned = clean::clean_reply_for_plan(3, original)
            .expect("the view library cleans as plan version 3 did");
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

/// Full-content render input for counting, before provider-specific message framing.
pub(super) fn counted_content(
    entry: &Entry,
    part: &cortexkit_role_compaction::part::Part,
) -> Option<String> {
    match &part.body {
        PartBody::Text { text } => Some(match part.tag {
            // The entry's original tag spelling has been normalized. Count the canonical
            // spelling here; a reply that originally omitted the space may differ by a token.
            Some(tag) => numbered_reply(text, text, tag, entry.role == Role::Assistant),
            None => text.as_str().to_owned(),
        }),
        PartBody::Tool(tool) => {
            let mut output = ToolOutput::Text {
                text: tool.output.as_deref()?.to_owned().into(),
            };
            let plan = PlanPart {
                id: part.id.clone(),
                tag: part.tag,
                prefix: part.tag.is_some(),
                empty_result_label: tags::empty_result_label(tool).map(str::to_owned),
                replace: Vec::new(),
                rides: Vec::new(),
            };
            // Default envelopes render rides in their result, so nothing is held out here.
            render_result(&mut output, tool, &plan, &entry.mid, &Default::default());
            let ToolOutput::Text { text } = output else {
                return None;
            };
            Some(text.to_string())
        }
        _ => None,
    }
}

/// Renders a tool part's result into `output`, with its kept rides. Returns the ride texts held
/// out of the result, in delivery order, when `rides` renders them after the turn's results;
/// otherwise they are appended to `output` and none are returned.
fn render_result(
    output: &mut ToolOutput,
    tool: &ToolPart,
    plan: &PlanPart,
    mid: &str,
    rides: &super::entries::RideEnvelopes,
) -> Vec<String> {
    let mut held = Vec::new();
    let delivered = tool.delivered().unwrap_or_default();
    let only_new = !delivered.is_empty()
        && delivered
            .iter()
            .enumerate()
            .all(|(index, _)| rides.get(mid, &plan.id, index).is_some());
    if only_new
        && matches!(output, ToolOutput::Parts { .. })
        && !plan.replace.iter().any(|field| field.field == "output")
    {
        if let Some(field) = plan
            .replace
            .iter()
            .find(|field| field.field == "attachments")
        {
            let ids: Vec<String> =
                serde_json::from_value(field.value.clone()).expect("checked attachments");
            if let ToolOutput::Parts { parts } = output {
                *parts = std::mem::take(parts)
                    .into_iter()
                    .enumerate()
                    .filter_map(|(index, part)| {
                        (!matches!(part, ToolOutputPart::Media { .. })
                            || ids.contains(&index.to_string()))
                        .then_some(part)
                    })
                    .collect();
            }
        }
        if plan.prefix {
            if let ToolOutput::Parts { parts } = output {
                let prefix = tags::tag_prefix(plan.tag.expect("numbered result has a tag"));
                if let Some(ToolOutputPart::Text { text }) = parts.first_mut() {
                    *text = format!("{prefix}{text}").into();
                } else {
                    parts.insert(
                        0,
                        ToolOutputPart::Text {
                            text: format!(
                                "{prefix}{}",
                                plan.empty_result_label.as_deref().unwrap_or("")
                            )
                            .into(),
                        },
                    );
                }
            }
        }
        for ride in &plan.rides {
            append_delivery(
                output,
                &ride.delivered,
                rides.get(mid, &plan.id, ride.index as usize),
                rides.after_results().then_some(&mut held),
            );
        }
        return held;
    }
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
    set_result_text(output, text, attachment_ids.as_deref());
    // Legacy deliveries keep their original output-edit rule; frozen rides follow
    // the recorded plan's selected indices. Iterate once to preserve delivery order.
    for (index, delivered) in tool.delivered().unwrap_or_default().iter().enumerate() {
        let ride = rides.get(mid, &plan.id, index);
        let kept = if ride.is_some() {
            plan.rides.iter().any(|ride| ride.index == index as u64)
        } else {
            delivery_indexes
                .as_ref()
                .is_none_or(|indexes| indexes.contains(&(index as u64)))
        };
        if kept {
            append_delivery(
                output,
                delivered,
                ride,
                rides.after_results().then_some(&mut held),
            );
        }
    }
    held
}

/// Adds one delivered text to the result `output` carries, by the ride rule its envelope names.
/// With `held`, the text is pushed there exactly as stored instead and `output` is left as the
/// tool returned it: the request shows it after the turn's results (probe only, see
/// `RideEnvelopes::after_results`).
fn append_delivery(
    output: &mut ToolOutput,
    delivery: &cortexkit_role_compaction::part::Delivered,
    envelope: Option<&broca_records::record::RideMetadata>,
    held: Option<&mut Vec<String>>,
) {
    let Some(text) = &delivery.text else {
        return;
    };
    if let Some(held) = held {
        held.push(text.as_str().to_owned());
        return;
    }
    match envelope.map(|envelope| envelope.form) {
        Some(broca_records::record::RideForm::Append) => {
            let envelope = envelope.unwrap();
            let ride = cortexkit_role_llm_runner::rides::Ride::new(
                &envelope.send_id,
                &envelope.sender,
                &envelope.call_key,
                text,
            );
            crate::delivery_order::append_ride_output(output, &[ride]);
        }
        None => {
            let ride = cortexkit_role_llm_runner::rides::Ride::new("", &delivery.sender, "", text);
            let rendered = cortexkit_role_llm_runner::rides::append(
                &super::entries::tool_output(output).1,
                &[ride],
            );
            set_result_text(output, rendered, None);
        }
    }
}

fn set_result_text(output: &mut ToolOutput, text: String, attachment_ids: Option<&[String]>) {
    match output {
        ToolOutput::Text { text: target } => *target = text.into(),
        ToolOutput::Error { .. } | ToolOutput::Denied { .. } => *output = ToolOutput::error(text),
        ToolOutput::Json { .. } => *output = ToolOutput::Text { text: text.into() },
        ToolOutput::Parts { parts } => {
            let mut kept = vec![ToolOutputPart::Text { text: text.into() }];
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
