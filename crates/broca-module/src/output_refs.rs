//! Output references (llm-runner §9.13): the tool outputs a model's reply names with
//! `$$tc(N)`, served by `session.output_refs` and `session.output_ref_read`.
//!
//! Nothing is stored for them. Every read derives a message's records again from the session
//! log, through the read paths `session.read` and `session.resolve` already use, and the
//! derivation is deterministic, so every read of one message answers the same records:
//!
//! - **Which messages have a set.** An assistant message a model step wrote (it carries run
//!   attribution) in a run that rendered tags under the computed rules (`runner-tags/v2`, its
//!   part tags say `render`). Imported messages, and messages of runs that showed the model no
//!   numbers, have none. A message whose text names nothing still has a set, an empty one.
//! - **The numbering.** The writing request showed the model every numbered part at an earlier
//!   ordinal, with the number it still carries: a number, once shown, never changes within a
//!   lineage (llm-runner §9.6). The exception is a compaction view: a request rendered under an
//!   applied view leaves out the parts it folded, replaced or edited, so for a session with a
//!   compaction provider the view in effect at the writing step is recovered from the log and
//!   those parts count as absent (`unknown_tag`). A part at the message's own ordinal or later
//!   was not in the request either.
//! - **The content.** A record's snapshot is the stored tool output the number named, never the
//!   rendered text with its tag prefix. It lives exactly as long as the message naming it:
//!   retention removes both together, and compaction removes neither.
//!
//! Set and reference ids carry the session's lineage id, so an id from another session never
//! names anything here and answers `unknown_ref`, exactly like an id that never existed.

use std::collections::{BTreeMap, BTreeSet};

use base64::Engine;
use broca_protocol::content::{ContentBlock, Message, Role, ToolOutput, ToolOutputPart};
use broca_protocol::tags::{exposed_part_text, part_key};
use cortexkit_role_llm_runner::display::{DisplayPart, DisplayRow};
use cortexkit_role_llm_runner::output_refs::{
    Descriptor, ListRequest, Page, ReadReply, ReadRequest, Record, ReferenceSlot, Resolution,
    Target, MAX_READ_BYTES,
};

use crate::registry::LineageKey;
use crate::serve::{ServeError, ServeHandler, SessionReadMessage, SessionReadRequest};

/// The reference dialect every set names: `cortexkit-rich-ui-syntax`'s `DIALECT`, the grammar
/// `output_refs::Resolution` scans with.
pub const DIALECT: &str = "rich-ui/1";

/// Why an output-reference request is refused. Each maps to one llm-runner error code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputRefRefusal {
    /// `unknown_ref`: no set or reference with this id in the caller's session, a cursor
    /// that is not a record of the set, or a record whose content was not retained.
    UnknownRef(String),
    /// `invalid_params` naming `field` (`offset` or `length`).
    InvalidParams {
        field: &'static str,
        message: String,
    },
    /// `reply_too_large`: one record alone does not fit a page.
    ReplyTooLarge(String),
}

/// One set: its descriptor, its records in encounter order, and the retained snapshots by
/// SHA-256.
#[derive(Debug, Clone, PartialEq)]
pub struct OutputRefSet {
    pub descriptor: Descriptor,
    pub records: Vec<Record>,
    snapshots: BTreeMap<String, Vec<u8>>,
}

/// Every set one message has: its reply set, and one set per opted-in tool call, keyed by the
/// call's `tool_call_id`, in block order.
#[derive(Debug, Clone, PartialEq)]
pub struct MessageOutputRefs {
    pub reply: OutputRefSet,
    pub calls: Vec<(String, OutputRefSet)>,
}

impl MessageOutputRefs {
    fn descriptors(&self) -> MessageOutputDescriptors {
        MessageOutputDescriptors {
            reply: self.reply.descriptor.clone(),
            calls: self
                .calls
                .iter()
                .map(|(id, set)| (id.clone(), set.descriptor.clone()))
                .collect(),
        }
    }

    /// `row` with this message's descriptors: the reply set's on the row, and each call set's on
    /// the tool-call part with the same `tool_call_id` at its stored position. A display read of this message's row calls
    /// this; nothing else carries descriptors (llm-runner §9.13: never the default `session.read`
    /// entry or a stored body).
    pub fn attach(&self, row: DisplayRow) -> DisplayRow {
        self.descriptors().attach(row)
    }
}

/// A display page retains descriptors only, not a copy of every row's resolved snapshots.
pub struct MessageOutputDescriptors {
    pub reply: Descriptor,
    pub calls: Vec<(String, Descriptor)>,
}

impl MessageOutputDescriptors {
    pub fn attach(&self, mut row: DisplayRow) -> DisplayRow {
        row.output_refs = Some(self.reply.clone());
        for (index, part) in row.parts.iter_mut().enumerate() {
            if let DisplayPart::ToolCall(call) = part {
                // A provider may reuse a call id. Canonical rows retain every stored part in
                // order, so the set's block index disambiguates without interpreting part keys.
                if let Some((_, set)) = self.calls.iter().find(|(id, set)| {
                    *id == call.tool_call_id
                        && parse_set(&set.set_id).is_some_and(|(_, key)| key.call == Some(index))
                }) {
                    call.output_refs = Some(set.clone());
                }
            }
        }
        row
    }
}

/// Where a set lives: its message's ordinal and, for a call set, the tool call's block index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SetKey {
    ordinal: u64,
    call: Option<usize>,
}

fn set_id(lineage: &str, key: SetKey) -> String {
    match key.call {
        None => format!("{lineage}/m{}", key.ordinal),
        Some(index) => format!("{lineage}/m{}/c{index}", key.ordinal),
    }
}

fn ref_id(set: &str, index: usize) -> String {
    format!("{set}/r{index}")
}

/// A decimal number spelled exactly as `to_string` spells it, so each set and record has one
/// id and no other string aliases it.
fn canonical(text: &str) -> Option<u64> {
    let number: u64 = text.parse().ok()?;
    (number.to_string() == text).then_some(number)
}

/// The lineage an id names and the set it names there. `None` for anything this module never
/// minted.
fn parse_set(id: &str) -> Option<(&str, SetKey)> {
    let (lineage, rest) = id.split_once("/m")?;
    let (ordinal, call) = match rest.split_once("/c") {
        Some((ordinal, call)) => (ordinal, Some(call)),
        None => (rest, None),
    };
    let call = match call {
        Some(call) => Some(usize::try_from(canonical(call)?).ok()?),
        None => None,
    };
    (!lineage.is_empty()).then_some((
        lineage,
        SetKey {
            ordinal: canonical(ordinal)?,
            call,
        },
    ))
}

fn parse_ref(id: &str) -> Option<(&str, SetKey, usize)> {
    let (set, index) = id.rsplit_once("/r")?;
    let (lineage, key) = parse_set(set)?;
    Some((lineage, key, usize::try_from(canonical(index)?).ok()?))
}

fn unknown_set(id: &str) -> OutputRefRefusal {
    OutputRefRefusal::UnknownRef(format!("no output reference set {id} in this session"))
}

fn unknown_ref(id: &str) -> OutputRefRefusal {
    OutputRefRefusal::UnknownRef(format!(
        "no output reference {id} with retained content in this session"
    ))
}

/// Reads single messages of one session's current lineage, each at most once.
struct Messages<'a, H: ?Sized> {
    handler: &'a H,
    session: &'a LineageKey,
    lineage: String,
    cache: BTreeMap<u64, Option<SessionReadMessage>>,
    locations: BTreeMap<u64, Option<broca_core::TagLocation>>,
    records: Option<Vec<broca_wal::WalRecord>>,
}

impl<H: ServeHandler + ?Sized> Messages<'_, H> {
    /// Read from `from` on, at most `limit` messages, within the lineage the derivation started
    /// on: a read after a lineage change is refused rather than mixed with the old one.
    async fn page(&self, from: u64, limit: u64) -> Result<Vec<SessionReadMessage>, ServeError> {
        let mut request = SessionReadRequest::new(self.session.clone(), Some(from), Some(limit));
        request.lineage_id = Some(self.lineage.clone());
        Ok(self.handler.read(request).await?.messages)
    }

    async fn entry(&mut self, ordinal: u64) -> Result<Option<&SessionReadMessage>, ServeError> {
        if !self.cache.contains_key(&ordinal) {
            let entry = self
                .page(ordinal, 1)
                .await?
                .into_iter()
                .find(|entry| entry.ordinal == ordinal);
            self.cache.insert(ordinal, entry);
        }
        Ok(self.cache[&ordinal].as_ref())
    }
}

/// Whether `message` was written by a model step of a run that showed the model numbers under
/// the computed rules: only such a message has sets.
fn has_sets(entry: &SessionReadMessage) -> bool {
    entry.message.role == Role::Assistant
        && entry.run.is_some()
        && entry
            .message
            .part_tags()
            .and_then(|tags| tags.computed)
            .is_some_and(|computed| computed.render)
}

/// The model's own text parts of `message`, in block order, as every exposure shows them.
fn reply_texts(message: &Message) -> Vec<String> {
    message
        .content
        .iter()
        .enumerate()
        .filter(|(_, block)| {
            matches!(
                block,
                ContentBlock::Text { .. } | ContentBlock::TextWithState { .. }
            )
        })
        .filter_map(|(index, _)| exposed_part_text(message, index))
        .collect()
}

/// A tool call of the message whose string arguments are scanned in declared slots.
struct CallScan {
    index: usize,
    tool_call_id: String,
    call_key: Option<String>,
    arguments: serde_json::Value,
    slots: Vec<ReferenceSlot>,
}

/// The stored bytes a tool result holds, never its rendering: `format`, `mime` and content.
/// A result of text parts is its texts joined by newlines, as a reader without media reads
/// it; a result carrying media is its stored parts as JSON, since broca keeps media by
/// reference and has no single byte string for it.
fn stored_output(output: &ToolOutput) -> (String, Option<String>, Vec<u8>) {
    let text = |bytes: &str| ("text".to_owned(), None, bytes.as_bytes().to_vec());
    match output {
        ToolOutput::Text { text: body } => text(body),
        ToolOutput::Error { message } => text(message),
        ToolOutput::Denied { reason } => text(reason),
        ToolOutput::Json { value } => (
            "text".to_owned(),
            None,
            serde_json::to_vec(value).expect("JSON values serialize"),
        ),
        ToolOutput::Parts { parts } => {
            let texts: Option<Vec<&str>> = parts
                .iter()
                .map(|part| match part {
                    ToolOutputPart::Text { text } => Some(text.as_str()),
                    ToolOutputPart::Media { .. } => None,
                })
                .collect();
            match texts {
                Some(texts) => text(&texts.join("\n")),
                None => (
                    "text".to_owned(),
                    None,
                    serde_json::to_vec(parts).expect("tool output parts serialize"),
                ),
            }
        }
    }
}

/// Stand-in for target metadata the log does not hold. `Target::with_tool` needs all three, so
/// a missing one is spelled empty here and nulled in the records afterwards; no real call key,
/// tool name or module id is empty.
const UNKNOWN: &str = "";

/// Dispatch attribution is per occurrence within one assistant message. A repeated provider
/// id can name a later synthetic refusal, and a later message can legitimately reuse the id.
fn call_attribution<'a>(
    entry: &'a SessionReadMessage,
    id: &str,
    call_occurrence: usize,
) -> Option<&'a broca_wire::SessionReadToolCall> {
    // Core refuses later same-id calls within a model message before dispatch. The legacy
    // read projection can repeat the first id's attribution; that is not a second dispatch.
    if call_occurrence != 0 {
        return None;
    }
    entry
        .tool_calls
        .iter()
        .filter(|call| call.tool_call_id == id)
        .nth(call_occurrence)
}

/// The target part `index` of `entry` is, as the writing request numbered it. `call_entry` is
/// the message before it, which holds a tool result's call.
fn target_of(
    entry: &SessionReadMessage,
    index: usize,
    call_entry: Option<&SessionReadMessage>,
) -> Option<Target> {
    let key = part_key(entry.ordinal, index);
    Some(match entry.message.content.get(index)? {
        ContentBlock::ToolResult(result) => {
            let id = &result.tool_call_id.0;
            let occurrence = entry.message.content[..index].iter().filter(|block| matches!(block, ContentBlock::ToolResult(result) if result.tool_call_id.0 == *id)).count();
            let tool = call_entry.and_then(|call| {
                call.message
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        ContentBlock::ToolCall(call) if call.tool_call_id.0 == *id => {
                            Some(call.tool_name.clone())
                        }
                        _ => None,
                    })
                    .nth(occurrence)
            });
            let dispatch = call_entry.and_then(|call| call_attribution(call, id, occurrence));
            let (format, mime, content) = stored_output(&result.output);
            let mut target = Target::text(key, content).with_tool(
                dispatch
                    .and_then(|d| d.call_key.clone())
                    .unwrap_or_else(|| UNKNOWN.into()),
                tool.unwrap_or_else(|| UNKNOWN.into()),
                dispatch
                    .and_then(|d| d.dispatched_to.clone())
                    .unwrap_or_else(|| UNKNOWN.into()),
            );
            target.format = format;
            target.mime = mime;
            target
        }
        _ => Target::text(key, exposed_part_text(&entry.message, index)?.into_bytes()),
    })
}

/// The numbers `scan` names, in the first [`MAX_REFERENCES`](cortexkit_role_llm_runner::output_refs::MAX_REFERENCES)
/// occurrences that can resolve.
fn named_numbers(scan: impl FnOnce(&mut Resolution, &BTreeMap<u64, Target>)) -> BTreeSet<u64> {
    let mut probe = Resolution::new();
    scan(&mut probe, &BTreeMap::new());
    probe
        .records
        .iter()
        .filter(|record| record.reason.as_deref() != Some("too_many_refs"))
        .filter_map(|record| record.tag.as_deref()?.parse().ok())
        .collect()
}

/// One set from `scan` over the writing request's targets, with ids minted under `set`.
fn resolve_set(
    set: String,
    mid: &str,
    call_key: Option<&str>,
    request: &BTreeMap<u64, Target>,
    scan: impl FnOnce(&mut Resolution, &BTreeMap<u64, Target>, &mut dyn FnMut() -> String),
) -> OutputRefSet {
    let mut resolution = Resolution::new();
    let mut next = 0usize;
    let mut mint = || {
        let id = ref_id(&set, next);
        next += 1;
        id
    };
    scan(&mut resolution, request, &mut mint);
    let Resolution {
        mut records,
        snapshots,
        ..
    } = resolution;
    for record in &mut records {
        for field in [&mut record.call_key, &mut record.tool, &mut record.provider] {
            if field.as_deref() == Some(UNKNOWN) {
                *field = None;
            }
        }
    }
    let mut descriptor = Descriptor::new(&set, mid, DIALECT, &records);
    if let Some(key) = call_key {
        descriptor = descriptor.with_call_key(key);
    }
    OutputRefSet {
        descriptor,
        records,
        snapshots,
    }
}

/// Whether the request a compaction `view` rendered showed part `index` of message `ordinal`
/// with its number: messages after the view's coverage are sent as written, a keep range sends
/// its messages except the parts it edits or drops (an edited part carries the provider's bytes,
/// with no number of the runner's), and everything else the view covers is replaced or left out
/// (compaction-provider/v1 §7, `broca_core::compaction::view::assemble`).
fn view_shows(
    view: &broca_core::compaction_provider::view::CompactionView,
    ordinal: u64,
    index: usize,
) -> bool {
    use broca_core::compaction_provider::view::ViewRange;
    if ordinal > view.covers_through.ordinal {
        return true;
    }
    let key = part_key(ordinal, index);
    view.ranges.iter().any(|range| match range {
        ViewRange::Keep { from, to, edits } => {
            (*from..*to).contains(&ordinal)
                && !edits
                    .iter()
                    .any(|edit| edit.ordinal == ordinal && edit.part == key)
        }
        _ => false,
    })
}

/// The compaction view in effect when the model step that wrote message `ordinal` sent its
/// request, from the session's whole log: `None` when no view had applied by then.
///
/// No record names the ordinal a step wrote, so the step is found by replay. Replaying the first
/// `k` records gives the transcript's extent; the smallest `k` whose extent passes `ordinal` ends
/// with the record that added the message (its `ModelStepFinished`, or the batch commit that
/// adds a tool turn), and the step that wrote it is the last `ModelStepFinished` at or before
/// that record. A view applies at a step boundary, before the step that first renders it, so
/// the view in effect is the last `CompactionViewApplied` before that step. Each probe replays a
/// prefix of the log, so this costs O(log n) replays of up to n records.
pub fn view_at_writing_step(
    records: &[broca_wal::WalRecord],
    ordinal: u64,
) -> Result<Option<broca_core::compaction_provider::view::CompactionView>, ServeError> {
    use broca_wal::WalRecord;
    let extent = |k: usize| -> Result<u64, ServeError> {
        broca_core::replay::replay(&records[..k])
            .map(|state| state.extent)
            .map_err(|error| {
                ServeError::Open(
                    format!("replaying the session log for output references: {error}"),
                    Some(broca_protocol::ErrorClass::Permanent),
                )
            })
    };
    let (mut low, mut high) = (0usize, records.len());
    if extent(high)? <= ordinal {
        return Ok(None);
    }
    // Invariant: extent(low) <= ordinal < extent(high).
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        if extent(middle)? > ordinal {
            high = middle;
        } else {
            low = middle;
        }
    }
    let Some(step) = records[..high]
        .iter()
        .rposition(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
    else {
        return Ok(None);
    };
    Ok(records[..step]
        .iter()
        .rev()
        .find_map(|record| match record {
            WalRecord::CompactionViewApplied { view, .. } => {
                serde_json::from_str(view.as_json_text()).ok()
            }
            _ => None,
        }))
}

/// Whether the session's frozen plan names a compaction provider, whose views can leave parts
/// out of a request.
fn plan_compacts(manifest: Option<&broca_wal::PlanManifest>) -> bool {
    manifest
        .and_then(|manifest| {
            serde_json::from_str::<serde_json::Value>(manifest.plan.as_json_text()).ok()
        })
        .is_some_and(|plan| {
            plan.get("compaction_item")
                .is_some_and(|item| !item.is_null())
        })
}

/// The targets the request that wrote message `ordinal` numbered, for the numbers in `wanted`:
/// each number's part, when that part sat at an earlier ordinal and the request showed it.
async fn writing_request<H: ServeHandler + ?Sized>(
    messages: &mut Messages<'_, H>,
    ordinal: u64,
    run_id: &str,
    wanted: &BTreeSet<u64>,
    compacts: bool,
) -> Result<BTreeMap<u64, Target>, ServeError> {
    let mut found: BTreeMap<u64, (u64, usize)> = BTreeMap::new();
    let mut missing = BTreeSet::new();
    let mut scan_from = ordinal;
    for &number in wanted {
        if !messages.locations.contains_key(&number) {
            let location = messages
                .handler
                .tag_location(messages.session, number)
                .await?;
            messages.locations.insert(number, location);
        }
        let location = messages.locations[&number];
        match location {
            Some(broca_core::TagLocation {
                found: Some((at, index)),
                ..
            }) => {
                // An indexed number at or after the message was not in its request; one before
                // it is checked against the message itself, since the index may predate a
                // lineage change.
                if at < ordinal {
                    let holds = messages.entry(at).await?.is_some_and(|entry| {
                        entry
                            .message
                            .part_tags()
                            .and_then(|tags| tags.parts.get(index).copied().flatten())
                            == Some(number)
                    });
                    if holds {
                        found.insert(number, (at, index));
                    } else {
                        missing.insert(number);
                        scan_from = 0;
                    }
                }
            }
            Some(location) => {
                missing.insert(number);
                scan_from = scan_from.min(location.indexed_through);
            }
            None => {
                missing.insert(number);
                scan_from = 0;
            }
        }
    }
    // Numbers the index does not hold: past its materialized prefix, or a store without one.
    // Read forward from where it ends to the message itself, never past it.
    let mut from = scan_from;
    while !missing.is_empty() && from < ordinal {
        let page = messages
            .page(from, broca_core::SESSION_READ_MAX_LIMIT)
            .await?;
        let Some(last) = page.last().map(|entry| entry.ordinal) else {
            break;
        };
        for entry in page.into_iter().filter(|entry| entry.ordinal < ordinal) {
            if let Some(tags) = entry.message.part_tags() {
                for (index, number) in tags.tagged() {
                    if missing.remove(&number) {
                        found.insert(number, (entry.ordinal, index));
                    }
                }
            }
            messages.cache.insert(entry.ordinal, Some(entry));
        }
        if last < from {
            break;
        }
        from = last + 1;
    }
    let view = if compacts && !found.is_empty() {
        if messages.records.is_none() {
            messages.records = Some(
                messages
                    .handler
                    .session_log(messages.session, run_id)
                    .await?
                    .ok_or_else(|| {
                        ServeError::Open(
                    "the session log is not readable here, so output references cannot tell \
                     which parts a compaction view left out"
                        .into(),
                    Some(broca_protocol::ErrorClass::Transient),
                )
                    })?,
            );
        }
        view_at_writing_step(
            messages.records.as_deref().expect("loaded session log"),
            ordinal,
        )?
    } else {
        None
    };
    let mut request = BTreeMap::new();
    for (number, (at, index)) in found {
        if view
            .as_ref()
            .is_some_and(|view| !view_shows(view, at, index))
        {
            continue;
        }
        let call_entry = match at.checked_sub(1) {
            Some(before) => messages.entry(before).await?.cloned(),
            None => None,
        };
        let Some(entry) = messages.entry(at).await? else {
            continue;
        };
        if let Some(target) = target_of(entry, index, call_entry.as_ref()) {
            request.insert(number, target);
        }
    }
    Ok(request)
}

/// The sets message `ordinal` of the caller's session has, and the session's lineage id;
/// `None` when the message does not exist or has none. With `lineage`, also `None` when the
/// session's lineage is another: an id minted for another session (or a lineage retention
/// deleted) names nothing here, and that is settled before any part's content is read.
pub async fn message_output_refs<H: ServeHandler + ?Sized>(
    handler: &H,
    session: &LineageKey,
    ordinal: u64,
    lineage: Option<&str>,
) -> Result<Option<(String, MessageOutputRefs)>, ServeError> {
    let page = handler
        .read(SessionReadRequest::new(
            session.clone(),
            Some(ordinal),
            Some(1),
        ))
        .await?;
    let Some(current) = page.lineage_id.clone() else {
        return Ok(None);
    };
    if lineage.is_some_and(|lineage| lineage != current) {
        return Ok(None);
    }
    let lineage = current;
    let Some(entry) = page
        .messages
        .into_iter()
        .find(|entry| entry.ordinal == ordinal)
    else {
        return Ok(None);
    };
    if !has_sets(&entry) {
        return Ok(None);
    }
    let mut messages = Messages {
        handler,
        session,
        lineage: lineage.clone(),
        cache: BTreeMap::new(),
        locations: BTreeMap::new(),
        records: None,
    };
    let manifest = handler.frozen_manifest(session).await?;
    let refs = derive_message(&mut messages, &entry, manifest.as_deref()).await?;
    Ok(Some((lineage, refs)))
}

/// Derive a display page's sets with one manifest read and a shared message cache. Page entries
/// seed the cache, so references to another row and repeated targets do not reread that message.
pub async fn page_output_refs<H: ServeHandler + ?Sized>(
    handler: &H,
    session: &LineageKey,
    lineage: &str,
    entries: &[SessionReadMessage],
    records: Option<Vec<broca_wal::WalRecord>>,
) -> Result<BTreeMap<u64, MessageOutputDescriptors>, ServeError> {
    let mut messages = Messages {
        handler,
        session,
        lineage: lineage.to_owned(),
        cache: entries
            .iter()
            .map(|entry| (entry.ordinal, Some(entry.clone())))
            .collect(),
        locations: BTreeMap::new(),
        records,
    };
    let manifest = handler.frozen_manifest(session).await?;
    let mut sets = BTreeMap::new();
    for entry in entries.iter().filter(|entry| has_sets(entry)) {
        sets.insert(
            entry.ordinal,
            derive_message(&mut messages, entry, manifest.as_deref())
                .await?
                .descriptors(),
        );
    }
    Ok(sets)
}

async fn derive_message<H: ServeHandler + ?Sized>(
    messages: &mut Messages<'_, H>,
    entry: &SessionReadMessage,
    manifest: Option<&broca_wal::PlanManifest>,
) -> Result<MessageOutputRefs, ServeError> {
    let ordinal = entry.ordinal;
    let lineage = messages.lineage.clone();
    let run_id = entry
        .run
        .as_ref()
        .map(|run| run.run_id.clone())
        .unwrap_or_default();
    let texts = reply_texts(&entry.message);
    let calls = call_scans(entry, manifest);
    let mut wanted = named_numbers(|probe, empty| {
        for text in &texts {
            probe.reply(text, empty, &mut String::new);
        }
    });
    for call in &calls {
        wanted.extend(named_numbers(|probe, empty| {
            probe.call(&call.arguments, &call.slots, true, empty, &mut String::new)
        }));
    }
    let request = if wanted.is_empty() {
        BTreeMap::new()
    } else {
        writing_request(messages, ordinal, &run_id, &wanted, plan_compacts(manifest)).await?
    };
    let mid = format!("m{ordinal}");
    let reply = resolve_set(
        set_id(
            &lineage,
            SetKey {
                ordinal,
                call: None,
            },
        ),
        &mid,
        None,
        &request,
        |resolution, request, mut mint| {
            for text in &texts {
                resolution.reply(text, request, &mut mint);
            }
        },
    );
    let calls = calls
        .into_iter()
        .map(|call| {
            let set = resolve_set(
                set_id(
                    &lineage,
                    SetKey {
                        ordinal,
                        call: Some(call.index),
                    },
                ),
                &mid,
                call.call_key.as_deref(),
                &request,
                |resolution, request, mut mint| {
                    resolution.call(&call.arguments, &call.slots, true, request, &mut mint)
                },
            );
            (call.tool_call_id, set)
        })
        .collect();
    Ok(MessageOutputRefs { reply, calls })
}

/// The set `id` names in the caller's session, or why there is none.
async fn find_set<H: ServeHandler + ?Sized>(
    handler: &H,
    session: &LineageKey,
    lineage: &str,
    key: SetKey,
) -> Result<Option<OutputRefSet>, ServeError> {
    let Some((_, mut sets)) =
        message_output_refs(handler, session, key.ordinal, Some(lineage)).await?
    else {
        return Ok(None);
    };
    Ok(match key.call {
        None => Some(sets.reply),
        Some(_) => {
            let id = set_id(lineage, key);
            sets.calls
                .iter()
                .position(|(_, set)| set.descriptor.set_id == id)
                .map(|position| sets.calls.swap_remove(position).1)
        }
    })
}

/// `session.output_refs`: one page of a set's records.
pub async fn list<H: ServeHandler + ?Sized>(
    handler: &H,
    session: &LineageKey,
    request: &ListRequest,
) -> Result<Result<Page, OutputRefRefusal>, ServeError> {
    let Some((lineage, key)) = parse_set(&request.set_id) else {
        return Ok(Err(unknown_set(&request.set_id)));
    };
    let Some(set) = find_set(handler, session, lineage, key).await? else {
        return Ok(Err(unknown_set(&request.set_id)));
    };
    Ok(
        Page::from_records(&set.records, request.after.as_deref()).map_err(|code| {
            if code == cortexkit_role_llm_runner::errors::REPLY_TOO_LARGE {
                OutputRefRefusal::ReplyTooLarge(format!(
                    "a record of set {} does not fit a page",
                    request.set_id
                ))
            } else {
                OutputRefRefusal::UnknownRef(format!(
                    "cursor {} is not a record of set {}",
                    request.after.as_deref().unwrap_or_default(),
                    request.set_id
                ))
            }
        }),
    )
}

/// `session.output_ref_read`: a byte range of one record's retained content.
pub async fn read<H: ServeHandler + ?Sized>(
    handler: &H,
    session: &LineageKey,
    request: &ReadRequest,
) -> Result<Result<ReadReply, OutputRefRefusal>, ServeError> {
    // The length bound needs no record, so it is answered before any lookup.
    if request.length > MAX_READ_BYTES {
        return Ok(Err(OutputRefRefusal::InvalidParams {
            field: "length",
            message: format!("length must be at most {MAX_READ_BYTES} bytes"),
        }));
    }
    let Some((lineage, key, index)) = parse_ref(&request.ref_id) else {
        return Ok(Err(unknown_ref(&request.ref_id)));
    };
    let Some(set) = find_set(handler, session, lineage, key).await? else {
        return Ok(Err(unknown_ref(&request.ref_id)));
    };
    let retained = set.records.get(index).and_then(|record| {
        let content = matches!(record.state.as_str(), "resolved" | "held")
            .then(|| set.snapshots.get(record.sha256.as_deref()?))
            .flatten()?;
        Some((record, content))
    });
    let Some((record, content)) = retained else {
        return Ok(Err(unknown_ref(&request.ref_id)));
    };
    let size = content.len() as u64;
    if let Err(field) = request.check(size) {
        return Ok(Err(OutputRefRefusal::InvalidParams {
            field,
            message: format!("{field} lies beyond the content's {size} bytes"),
        }));
    }
    let start = usize::try_from(request.offset).expect("offset is at most the content's size");
    let end = start
        .saturating_add(usize::try_from(request.length).unwrap_or(usize::MAX))
        .min(content.len());
    let data = base64::engine::general_purpose::STANDARD.encode(&content[start..end]);
    let mut reply = ReadReply::new(
        data,
        size,
        record.format.clone().unwrap_or_else(|| "text".into()),
    );
    if let Some(mime) = &record.mime {
        reply = reply.with_mime(mime);
    }
    Ok(Ok(reply))
}

/// The tool calls of `entry` whose arguments are scanned for references: each call of a tool
/// whose provider's plan item opted into output references and whose catalog declared reference
/// slots, as the session's manifest froze them at admission (admission freezes slots only for
/// an opted-in provider). A tool declared on a send has no plan item or catalog, so it is never
/// scanned.
fn call_scans(
    entry: &SessionReadMessage,
    manifest: Option<&broca_wal::PlanManifest>,
) -> Vec<CallScan> {
    let Some(manifest) = manifest else {
        return Vec::new();
    };
    let slots_of = |tool: &str| -> Option<Vec<ReferenceSlot>> {
        let provider = manifest
            .tools
            .iter()
            .find(|frozen| frozen.name == tool)?
            .module
            .as_deref()?;
        let fetch = manifest
            .fetches
            .iter()
            .find(|fetch| fetch.provider == provider)?;
        let broca_wal::FetchOutcome::Answered {
            reference_slots: Some(slots),
            ..
        } = &fetch.outcome
        else {
            return None;
        };
        let slots: Vec<_> = slots
            .get(tool)?
            .iter()
            .map(|slot| ReferenceSlot::new(&slot.name, &slot.kind))
            .collect();
        (!slots.is_empty()).then_some(slots)
    };
    entry
        .message
        .content
        .iter()
        .enumerate()
        .filter_map(|(index, block)| {
            let ContentBlock::ToolCall(call) = block else {
                return None;
            };
            let slots = slots_of(&call.tool_name)?;
            let id = call.tool_call_id.0.clone();
            let occurrence = entry.message.content[..index].iter().filter(|block| matches!(block, ContentBlock::ToolCall(call) if call.tool_call_id.0 == id)).count();
            Some(CallScan {
                index,
                call_key: call_attribution(entry, &id, occurrence)
                    .and_then(|dispatch| dispatch.call_key.clone()),
                tool_call_id: id,
                arguments: call.input.clone(),
                slots,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_core::compaction_provider::view::CompactionView;
    use cortexkit_role_llm_runner::display::{TextPart, ToolCallPart};
    use serde_json::json;

    fn view(covers_through: u64, ranges: serde_json::Value) -> CompactionView {
        serde_json::from_value(json!({
            "session_ref": "session", "view_version": 1, "lineage_id": "lineage",
            "covers_through": {"ordinal": covers_through, "mid": format!("m{covers_through}")},
            "ranges": ranges, "digest": "00",
        }))
        .unwrap()
    }

    /// A request rendered under a view shows a part with its number only where the view sends
    /// the stored message unedited: after its coverage, or inside a keep range without an edit
    /// of that part. Replaced, dropped, edited and uncovered parts were not in the request.
    #[test]
    fn a_view_shows_only_the_parts_it_sends_as_stored() {
        let view = view(
            6,
            json!([
                {"kind": "replace", "from": 0, "to": 3, "bytes": "[]"},
                {"kind": "keep", "from": 3, "to": 7, "edits": [
                    {"ordinal": 4, "mid": "m4", "part": "m4#1", "drop": true},
                    {"ordinal": 5, "mid": "m5", "part": "m5#0", "bytes": "{\"type\":\"text\",\"text\":\"x\"}"},
                ]},
            ]),
        );
        for (ordinal, index, shown) in [
            (0, 0, false),
            (2, 0, false),
            (3, 0, true),
            (4, 0, true),
            (4, 1, false),
            (5, 0, false),
            (5, 1, true),
            (6, 0, true),
            (7, 0, true),
            (90, 3, true),
        ] {
            assert_eq!(
                view_shows(&view, ordinal, index),
                shown,
                "m{ordinal}#{index}"
            );
        }
        // A covered ordinal no range names was left out.
        let gap = self::view(4, json!([{"kind": "keep", "from": 2, "to": 5}]));
        assert!(!view_shows(&gap, 1, 0));
        assert!(view_shows(&gap, 2, 0));
    }

    #[test]
    fn ids_parse_only_as_minted() {
        let lineage = "0190a6b2-0000-7000-8000-000000000001";
        for key in [
            SetKey {
                ordinal: 0,
                call: None,
            },
            SetKey {
                ordinal: 12,
                call: Some(3),
            },
        ] {
            let set = set_id(lineage, key);
            assert_eq!(parse_set(&set), Some((lineage, key)));
            assert_eq!(parse_ref(&ref_id(&set, 7)), Some((lineage, key, 7)));
            assert_eq!(
                parse_set(&ref_id(&set, 7)),
                None,
                "a ref id is not a set id"
            );
        }
        for alias in [
            format!("{lineage}/m01"),
            format!("{lineage}/m+1"),
            format!("{lineage}/m1/c"),
            format!("{lineage}/m1/c01"),
            format!("{lineage}/m"),
            "/m1".to_owned(),
            "m1".to_owned(),
        ] {
            assert_eq!(parse_set(&alias), None, "{alias}");
        }
        assert_eq!(parse_ref(&format!("{lineage}/m1/r01")), None);
    }

    fn set(id: &str, call_key: Option<&str>) -> OutputRefSet {
        let mut descriptor = Descriptor::new(id, "m3", DIALECT, &[]);
        if let Some(key) = call_key {
            descriptor = descriptor.with_call_key(key);
        }
        OutputRefSet {
            descriptor,
            records: Vec::new(),
            snapshots: BTreeMap::new(),
        }
    }

    /// The display seam puts the reply set's descriptor on the row and each call set's on the
    /// tool-call part at its stored position, even when a provider reused the call id.
    #[test]
    fn attach_puts_each_descriptor_on_its_row_or_call() {
        let refs = MessageOutputRefs {
            reply: set("l/m3", None),
            calls: vec![("call-b".into(), set("l/m3/c2", Some("l:9")))],
        };
        let row = DisplayRow::new(
            "assistant",
            vec![
                DisplayPart::Text(TextPart::new("m3#0", "see")),
                DisplayPart::ToolCall(ToolCallPart::new(
                    "m3#1",
                    "call-b",
                    "work",
                    &json!({}),
                    None,
                )),
                DisplayPart::ToolCall(ToolCallPart::new(
                    "m3#2",
                    "call-b",
                    "work",
                    &json!({}),
                    None,
                )),
            ],
        );
        let row = refs.attach(row);
        assert_eq!(row.output_refs.as_ref().unwrap().set_id, "l/m3");
        let calls: Vec<_> = row
            .parts
            .iter()
            .filter_map(|part| match part {
                DisplayPart::ToolCall(call) => Some(call.output_refs.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(calls[0], None);
        let attached = calls[1].as_ref().unwrap();
        assert_eq!(attached.set_id, "l/m3/c2");
        assert_eq!(attached.call_key.as_deref(), Some("l:9"));
    }

    #[test]
    fn a_repeated_result_id_keeps_its_own_tool_name() {
        let calls: SessionReadMessage = serde_json::from_value(json!({"ordinal":1,"mid":"m1","message":{"role":"assistant","content":[
            {"type":"tool_call","tool_call_id":"repeat","tool_name":"first","input":{}},
            {"type":"tool_call","tool_call_id":"repeat","tool_name":"second","input":{}}
        ]},"tool_calls":[
            {"tool_call_id":"repeat","call_key":"l:1","dispatched_to":"first-provider","indeterminate":false},
            {"tool_call_id":"repeat","indeterminate":false}
        ]})).unwrap();
        let results: SessionReadMessage = serde_json::from_value(json!({"ordinal":2,"mid":"m2","message":{"role":"tool","content":[
            {"type":"tool_result","tool_call_id":"repeat","output":{"kind":"text","text":"first output"}},
            {"type":"tool_result","tool_call_id":"repeat","output":{"kind":"error","message":"duplicate refused"},"is_error":true}
        ]}})).unwrap();
        let target = target_of(&results, 1, Some(&calls)).unwrap();
        let set = resolve_set(
            "l/m3".into(),
            "m3",
            None,
            &BTreeMap::from([(1, target)]),
            |resolution, request, mut mint| {
                resolution.reply("$$tc(1)", request, &mut mint);
            },
        );
        assert_eq!(set.records[0].tool.as_deref(), Some("second"));
        assert_eq!(set.records[0].call_key, None);
        assert_eq!(set.records[0].provider, None);
    }

    #[test]
    fn stored_output_is_the_stored_bytes() {
        let text = |output: serde_json::Value| {
            let output: ToolOutput = serde_json::from_value(output).unwrap();
            stored_output(&output)
        };
        assert_eq!(
            text(json!({"kind": "text", "text": "worked"})),
            ("text".into(), None, b"worked".to_vec())
        );
        assert_eq!(
            text(json!({"kind": "error", "message": "boom"})).2,
            b"boom".to_vec()
        );
        assert_eq!(
            text(json!({"kind": "denied", "reason": "no"})).2,
            b"no".to_vec()
        );
        assert_eq!(
            text(json!({"kind": "parts", "parts": [
                {"kind": "text", "text": "a"}, {"kind": "text", "text": "b"}
            ]}))
            .2,
            b"a\nb".to_vec()
        );
    }
}
