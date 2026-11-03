//! Facts kept beside native transcript rows. Historical reads do not scan the
//! log for a send's admission or a step's applied view. The continuation keeps
//! frozen reminder settings for shared placement, never transcript bodies.

use std::collections::{BTreeMap, HashMap, VecDeque};

use broca_protocol::{BindPrincipal, ContentBlock, DeliveryMode, Message, Role as HostRole, Usage};
use broca_wal::{FramedRecord, WalRecord};
use cortexkit_role_compaction::{
    part::{Delivered, PartBody, ToolPart, ToolStatus},
    read::{NoticeOrigin, Role, StepUsage},
    view::AppliedView,
};

use crate::placement::{AppendKind, HoldKind, Placement, PlacementAction};

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Metadata {
    pub tag_base: u64,
    pub source_seq: u64,
    pub tag_count: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub render_tags: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub clean_reply: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_input: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notice: Option<NoticeOrigin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<StepUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_under: Option<AppliedView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deliveries: Vec<DeliveryOrigin>,
}

/// Text remains in the SendDelivered frame. The index stores only attribution
/// and its source sequence, so an overhaul row remains a content-free locator.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DeliveryOrigin {
    pub seq: u64,
    pub submission_id: String,
    pub part: String,
    pub sender: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeliveryUpdate {
    pub ordinal: u64,
    pub part: String,
    pub delivered: Delivered,
}

#[derive(Default)]
pub struct Delta {
    pub rows: BTreeMap<u64, Metadata>,
    pub deliveries: Vec<DeliveryUpdate>,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct Origin {
    principal: Option<BindPrincipal>,
    template: Option<String>,
}

impl Origin {
    fn notice(&self) -> Option<NoticeOrigin> {
        let sender = super::entries::delivery_sender(self.principal.as_ref(), DeliveryMode::Steer)?;
        let mut notice = NoticeOrigin::new(sender);
        notice.template = self.template.clone();
        Some(notice)
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct HeldInput {
    metadata: Metadata,
    count: u64,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Assistant {
    ordinal: u64,
    metadata: Metadata,
    held: bool,
    parts: HashMap<String, Vec<String>>,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize)]
struct Positioned {
    placement: Placement,
    budget: Option<broca_protocol::request::BudgetConfig>,
}

impl<'de> serde::Deserialize<'de> for Positioned {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct Fields {
            placement: Placement,
            budget: Option<broca_protocol::request::BudgetConfig>,
        }
        let fields = <Fields as serde::Deserialize>::deserialize(deserializer)?;
        Ok(Self {
            placement: Placement::from_index_seed(fields.placement, fields.budget.clone()),
            budget: fields.budget,
        })
    }
}

impl Positioned {
    fn observe(&mut self, record: &WalRecord) -> Result<Vec<PlacementAction>, String> {
        if let WalRecord::RunStarted { config, .. } = record {
            self.budget = config.budget.clone();
        }
        self.placement.observe(record)
    }
    fn held_assistant_ordinal(&self) -> Option<u64> {
        self.placement.held_assistant_ordinal()
    }
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Fold {
    cache_version: u32,
    placement: Positioned,
    high: u64,
    origins: HashMap<String, Origin>,
    held_inputs: VecDeque<HeldInput>,
    assistant: Option<Assistant>,
    applied: Option<AppliedView>,
    step: Option<u64>,
    step_view: Option<AppliedView>,
    budget_counts: Vec<(broca_protocol::request::BudgetKind, u64, u64)>,
    import_high: Option<u64>,
    render_tags: bool,
    clean_replies: bool,
}

pub fn step_usage(usage: Usage) -> Option<StepUsage> {
    let any_input = usage.input_tokens.is_some()
        || usage.cached_input_tokens.is_some()
        || usage.cache_write_tokens.is_some();
    let prompt = any_input
        .then(|| {
            usage
                .input_tokens
                .unwrap_or(0)
                .saturating_add(usage.cached_input_tokens.unwrap_or(0))
                .saturating_add(usage.cache_write_tokens.unwrap_or(0))
        })
        .filter(|tokens| *tokens != 0);
    let mut out = StepUsage::new();
    out.prompt_tokens = prompt;
    out.cache_read_tokens = usage.cached_input_tokens;
    out.cache_write_tokens = usage.cache_write_tokens;
    out.output_tokens = usage.output_tokens;
    out.reasoning_tokens = usage.reasoning_tokens;
    (prompt.is_some()
        || out.cache_read_tokens.is_some()
        || out.cache_write_tokens.is_some()
        || out.output_tokens.is_some()
        || out.reasoning_tokens.is_some())
    .then_some(out)
}

/// Classification comes from the view library; host mapping only decides which
/// native blocks represent text and calls. Results take their call's number.
pub fn numbered_parts(message: &Message) -> u64 {
    numbered_flags(message)
        .into_iter()
        .filter(|numbered| *numbered)
        .count() as u64
}

fn numbered_flags(message: &Message) -> Vec<bool> {
    if message.role == HostRole::Tool {
        return vec![false; message.content.len()];
    }
    let role = match message.role {
        HostRole::User => Role::User,
        HostRole::Assistant => Role::Assistant,
        HostRole::System => Role::System,
        HostRole::Tool => unreachable!(),
    };
    message
        .content
        .iter()
        .map(|block| {
            let body = match block {
                ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. }
                    if !text.is_empty() =>
                {
                    PartBody::Text {
                        text: String::new(),
                    }
                }
                ContentBlock::ToolCall(_) => PartBody::Tool(ToolPart::new(
                    "",
                    "",
                    serde_json::Value::Null,
                    ToolStatus::Running,
                )),
                _ => return false,
            };
            cortexkit_compaction_view::tags::is_numbered(&role, &body)
        })
        .collect()
}

pub fn attach_native_tags(message: &mut Message, metadata: &Metadata) -> Result<(), String> {
    check_number_range(metadata)?;
    let flags = numbered_flags(message);
    if flags.iter().filter(|numbered| **numbered).count() as u64 != metadata.tag_count {
        return Err("compaction tag metadata disagrees with native message parts".into());
    }
    let mut high = metadata.tag_base;
    let parts = flags
        .into_iter()
        .map(|numbered| {
            if numbered {
                high += 1;
                Some(high)
            } else {
                None
            }
        })
        .collect();
    message.set_tags(Some(std::sync::Arc::new(broca_protocol::tags::PartTags {
        parts,
        computed: Some(broca_protocol::tags::ComputedTags {
            render: metadata.render_tags,
            clean: metadata.clean_reply,
            base: metadata.tag_base,
        }),
    })));
    Ok(())
}

impl Fold {
    pub fn is_current(&self) -> bool { self.cache_version == 1 }
    pub fn high(&self) -> u64 {
        self.high
    }

    fn allocate(&mut self, metadata: &mut Metadata, count: u64) -> Result<(), String> {
        metadata.tag_base = self.high;
        metadata.tag_count = count;
        metadata.render_tags = self.render_tags;
        self.high = self
            .high
            .checked_add(count)
            .filter(|high| *high <= 9_007_199_254_740_991)
            .ok_or("compaction tag numbers exceed the protocol integer range")?;
        Ok(())
    }

    pub fn observe(&mut self, frame: &FramedRecord) -> Result<Delta, String> {
        self.cache_version = 1;
        let record = &frame.record;
        let mut inputs = Vec::new();
        let mut assistant_metadata = None;
        let mut budget_count = 0;
        let mut delta = Delta::default();
        match record {
            WalRecord::DeliveryOrdered {
                submission_id,
                order,
                sent_by,
            } => {
                self.origins.insert(
                    submission_id.0.clone(),
                    Origin {
                        principal: sent_by.clone(),
                        template: order.template.as_ref().map(|template| template.id.clone()),
                    },
                );
            }
            WalRecord::Queued {
                submission_id,
                sent_by,
                principal,
                ..
            } => {
                let origin = self.origins.entry(submission_id.0.clone()).or_default();
                if origin.principal.is_none() {
                    origin.principal = sent_by.clone().or_else(|| principal.clone());
                }
            }
            WalRecord::Retracted { submission_id, .. }
            | WalRecord::SendEnded { submission_id, .. } => {
                self.origins.remove(&submission_id.0);
            }
            WalRecord::RunStarted {
                input,
                sent_by,
                principal,
                submission_id,
                origin,
                config,
                manifest,
                ..
            } => {
                self.held_inputs.clear();
                self.assistant = None;
                self.step = None;
                self.step_view = None;
                self.render_tags = config.render_tags
                    || manifest.as_ref().is_some_and(|manifest| {
                        serde_json::from_str::<serde_json::Value>(manifest.plan.as_json_text())
                            .ok()
                            .is_some_and(|plan| {
                                plan.get("compaction_item")
                                    .is_some_and(|item| !item.is_null())
                            })
                    });
                self.clean_replies = self.render_tags && !broca_protocol::structured_output::FrozenStructuredOutput::from_frozen_render_config(Some(&config.frozen_render_config)).is_some_and(|frozen| frozen.mode == broca_protocol::structured_output::Mode::FinalText);
                self.import_high = matches!(
                    origin,
                    broca_wal::RunOrigin::SeededFrom {
                        origin_kind: broca_wal::SeedOriginKind::Import,
                        ..
                    }
                )
                .then_some(self.high);
                let notice_input = matches!(origin, broca_wal::RunOrigin::Fresh);
                let origin = submission_id
                    .as_ref()
                    .and_then(|id| self.origins.remove(&id.0))
                    .unwrap_or_else(|| Origin {
                        principal: sent_by.clone().or_else(|| principal.clone()),
                        template: None,
                    });
                inputs = input
                    .iter()
                    .enumerate()
                    .map(|(index, message)| HeldInput {
                        metadata: Metadata {
                            source_input: Some(index as u64),
                            clean_reply: self.clean_replies && message.role == HostRole::Assistant,
                            notice: (notice_input && message.role == HostRole::User)
                                .then(|| origin.notice())
                                .flatten(),
                            ..Metadata::default()
                        },
                        count: numbered_parts(message),
                    })
                    .collect();
                self.budget_counts.clear();
                if let Some(budget) = &config.budget {
                    self.budget_counts.extend(budget.nudges.iter().map(|nudge| {
                        (
                            broca_protocol::request::BudgetKind::Nudge,
                            nudge.at_tool_calls,
                            u64::from(!nudge.text.is_empty()),
                        )
                    }));
                    self.budget_counts.push((
                        broca_protocol::request::BudgetKind::Finalize,
                        budget.finalize_at_tool_calls,
                        u64::from(!budget.finalize_text.is_empty()),
                    ));
                }
            }
            WalRecord::StepStarted { step_id, .. } => {
                if self.step != Some(step_id.0) {
                    self.step = Some(step_id.0);
                    self.step_view = self.applied.clone();
                }
            }
            WalRecord::CompactionViewAppliedV2 {
                step_id,
                version,
                checksum,
            } => {
                self.applied = Some(AppliedView::new(*version, checksum));
                if self.step == Some(*step_id) {
                    self.step_view = self.applied.clone();
                }
            }
            WalRecord::ModelStepFinished {
                step_id,
                assistant_message,
                usage,
                ..
            } => {
                let message = Message::new(HostRole::Assistant, assistant_message.content.clone());
                assistant_metadata = Some((
                    Metadata {
                        clean_reply: self.clean_replies,
                        usage: step_usage(*usage),
                        generated_under: if self.step == Some(step_id.0) {
                            self.step_view.clone()
                        } else {
                            self.applied.clone()
                        },
                        ..Metadata::default()
                    },
                    numbered_parts(&message),
                    &assistant_message.content,
                ));
                self.assistant = None;
            }
            WalRecord::Steered { input, sent_by, .. } => {
                let origin = Origin {
                    principal: sent_by.clone(),
                    template: None,
                };
                inputs = input
                    .iter()
                    .enumerate()
                    .map(|(index, message)| HeldInput {
                        metadata: Metadata {
                            source_input: Some(index as u64),
                            notice: (message.role == HostRole::User)
                                .then(|| origin.notice())
                                .flatten(),
                            ..Metadata::default()
                        },
                        count: numbered_parts(message),
                    })
                    .collect();
            }
            WalRecord::SendDelivered {
                submission_id,
                input: Some(input),
                ..
            } => {
                let origin = self.origins.remove(&submission_id.0).unwrap_or_default();
                inputs = input
                    .iter()
                    .enumerate()
                    .map(|(index, message)| HeldInput {
                        metadata: Metadata {
                            source_input: Some(index as u64),
                            notice: (message.role == HostRole::User)
                                .then(|| origin.notice())
                                .flatten(),
                            ..Metadata::default()
                        },
                        count: numbered_parts(message),
                    })
                    .collect();
            }
            WalRecord::BudgetFired {
                kind,
                threshold,
                caller_text,
                ..
            } => {
                budget_count = caller_text
                    .as_ref()
                    .map(|text| u64::from(!text.is_empty()))
                    .or_else(|| {
                        self.budget_counts
                            .iter()
                            .find(|(budget_kind, at, _)| budget_kind == kind && at == threshold)
                            .map(|(_, _, count)| *count)
                    })
                    .unwrap_or(0);
            }
            _ => {}
        }
        for input in &mut inputs {
            input.metadata.source_seq = frame.seq;
        }
        if let Some((metadata, _, _)) = assistant_metadata.as_mut() {
            metadata.source_seq = frame.seq;
        }
        let actions = self.placement.observe(record)?;
        for action in actions {
            match action {
                PlacementAction::Hold {
                    count,
                    kind: HoldKind::Steer,
                } => {
                    if inputs.len() as u64 != count {
                        return Err(
                            "compaction held-input metadata disagrees with placement".into()
                        );
                    }
                    self.held_inputs.extend(inputs.drain(..));
                }
                PlacementAction::Hold {
                    kind: HoldKind::Assistant,
                    ..
                } => {
                    let (mut metadata, count, content) = assistant_metadata
                        .take()
                        .ok_or("compaction held assistant has no model-step metadata")?;
                    self.allocate(&mut metadata, count)?;
                    let mut parts: HashMap<String, Vec<String>> = HashMap::new();
                    for (index, block) in content.iter().enumerate() {
                        if let ContentBlock::ToolCall(call) = block {
                            parts
                                .entry(call.tool_call_id.0.clone())
                                .or_default()
                                .push(index.to_string());
                        }
                    }
                    self.assistant = Some(Assistant {
                        ordinal: self
                            .placement
                            .held_assistant_ordinal()
                            .ok_or("compaction assistant position was not reserved")?,
                        metadata,
                        held: true,
                        parts,
                    });
                }
                PlacementAction::PlaceHeld { first, count } => {
                    for ordinal in first..first + count {
                        let mut held = self
                            .held_inputs
                            .pop_front()
                            .ok_or("compaction placed input has no held metadata")?;
                        self.allocate(&mut held.metadata, held.count)?;
                        delta.rows.insert(ordinal, held.metadata);
                    }
                }
                PlacementAction::Truncate { to } => {
                    delta.rows.retain(|ordinal, _| *ordinal < to);
                    self.high = self
                        .import_high
                        .ok_or("compaction transcript truncation has no import checkpoint")?;
                    self.assistant = None;
                    self.held_inputs.clear();
                }
                PlacementAction::Append {
                    first,
                    count,
                    kind: AppendKind::Input,
                } => {
                    if inputs.len() as u64 != count {
                        return Err("compaction input metadata disagrees with placement".into());
                    }
                    for (offset, mut input) in inputs.drain(..).enumerate() {
                        self.allocate(&mut input.metadata, input.count)?;
                        delta.rows.insert(first + offset as u64, input.metadata);
                    }
                }
                PlacementAction::Append {
                    first,
                    count,
                    kind: AppendKind::ToolTurn,
                } if count != 0 => {
                    let assistant = self
                        .assistant
                        .as_mut()
                        .ok_or("compaction tool turn has no assistant metadata")?;
                    if count != 2 || first != assistant.ordinal {
                        return Err(
                            "compaction pending assistant lost its reserved transcript position"
                                .into(),
                        );
                    }
                    assistant.held = false;
                    delta.rows.insert(first, assistant.metadata.clone());
                    delta.rows.insert(
                        first + 1,
                        Metadata {
                            tag_base: self.high,
                            source_seq: frame.seq,
                            render_tags: self.render_tags,
                            ..Metadata::default()
                        },
                    );
                }
                PlacementAction::Append {
                    first,
                    kind: AppendKind::Assistant | AppendKind::Finalize,
                    ..
                } => {
                    let (mut metadata, count, _) = assistant_metadata
                        .take()
                        .ok_or("compaction assistant has no model-step metadata")?;
                    self.allocate(&mut metadata, count)?;
                    delta.rows.insert(first, metadata);
                }
                PlacementAction::Append {
                    first,
                    kind: AppendKind::Budget,
                    ..
                } => {
                    let mut metadata = Metadata {
                        notice: Some(NoticeOrigin::new("broca")),
                        source_seq: frame.seq,
                        ..Metadata::default()
                    };
                    self.allocate(&mut metadata, budget_count)?;
                    delta.rows.insert(first, metadata);
                }
                _ => {}
            }
        }
        if let WalRecord::SendDelivered {
            submission_id,
            mode: DeliveryMode::ToolResult,
            tool_call_id: Some(call),
            appended_text: Some(text),
            ..
        } = record
        {
            let origin = self.origins.remove(&submission_id.0).unwrap_or_default();
            let assistant = self
                .assistant
                .as_mut()
                .ok_or("compaction tool delivery has no assistant entry")?;
            let part = assistant
                .parts
                .get(&call.0)
                .and_then(|parts| parts.last())
                .ok_or("compaction tool delivery names no tool part")?
                .clone();
            let sender = super::entries::delivery_sender(
                origin.principal.as_ref(),
                DeliveryMode::ToolResult,
            )
            .expect("tool delivery has a sender");
            assistant.metadata.deliveries.push(DeliveryOrigin {
                seq: frame.seq,
                submission_id: submission_id.0.clone(),
                part: part.clone(),
                sender: sender.clone(),
                template: origin.template.clone(),
            });
            let mut delivered = Delivered::new(sender, text.strip_prefix("\n\n").unwrap_or(text));
            delivered.template = origin.template;
            delta.deliveries.push(DeliveryUpdate {
                ordinal: assistant.ordinal,
                part,
                delivered,
            });
            if !assistant.held {
                delta
                    .rows
                    .insert(assistant.ordinal, assistant.metadata.clone());
            }
        }
        Ok(delta)
    }

    pub fn pending_assistant(&self) -> Option<(u64, &Metadata)> {
        self.assistant
            .as_ref()
            .filter(|assistant| assistant.held)
            .map(|assistant| (assistant.ordinal, &assistant.metadata))
    }
}

pub fn project_entry(
    message: &Message,
    metadata: &Metadata,
    position: u64,
    mid: &str,
    written_at: u64,
    frames: &[FramedRecord],
) -> Result<cortexkit_role_compaction::read::Entry, String> {
    check_number_range(metadata)?;
    if position >= 9_007_199_254_740_991 {
        return Err("compaction entry ordinal exceeds the protocol integer range".into());
    }
    use super::entries::{project, Deliveries, EntryFacts};
    let facts = EntryFacts {
        mid: mid.into(),
        ordinal: position + 1,
        tag_base: Some(metadata.tag_base),
        written_at,
        completed_at: (message.role == HostRole::Assistant && metadata.source_input.is_none()).then_some(written_at),
        notice: metadata.notice.clone(),
        usage: metadata.usage,
        generated_under: metadata.generated_under.clone(),
    };
    let mut messages = vec![message.clone()];
    let mut facts = vec![facts];
    if message.role == HostRole::Assistant
        && message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::ToolCall(_)))
    {
        let mut results: HashMap<&str, VecDeque<broca_protocol::ToolResult>> = HashMap::new();
        if let Some(index) = metadata.source_input {
            let input = frames
                .first()
                .and_then(|frame| match &frame.record {
                    WalRecord::RunStarted { input, .. }
                    | WalRecord::Steered { input, .. }
                    | WalRecord::SendDelivered {
                        input: Some(input), ..
                    } => Some(input),
                    _ => None,
                })
                .ok_or("compaction input entry has no native input source")?;
            if let Some(next) = input
                .get(index as usize + 1)
                .filter(|next| next.role == HostRole::Tool)
            {
                for block in &next.content {
                    if let ContentBlock::ToolResult(result) = block {
                        results
                            .entry(&result.tool_call_id.0)
                            .or_default()
                            .push_back(result.clone());
                    }
                }
            }
        } else {
            for frame in frames {
                let result = match &frame.record {
                    WalRecord::ToolResult { result, .. } => Some((**result).clone()),
                    WalRecord::ToolNoDispatch {
                        tool_call_id,
                        reason,
                        ..
                    } => Some(crate::replay::no_dispatch_result(tool_call_id, reason)),
                    WalRecord::ApprovalRecorded {
                        closing_result: Some(result),
                        ..
                    } => Some((**result).clone()),
                    _ => None,
                };
                if let Some(result) = result {
                    let id = match &frame.record {
                        WalRecord::ToolResult { result, .. } => result.tool_call_id.0.as_str(),
                        WalRecord::ToolNoDispatch { tool_call_id, .. } => tool_call_id.0.as_str(),
                        WalRecord::ApprovalRecorded {
                            closing_result: Some(result),
                            ..
                        } => result.tool_call_id.0.as_str(),
                        _ => unreachable!(),
                    };
                    results.entry(id).or_default().push_back(result);
                }
            }
        }
        let result_parts = message
            .content
            .iter()
            .filter_map(|block| {
                let ContentBlock::ToolCall(call) = block else {
                    return None;
                };
                results
                    .get_mut(call.tool_call_id.0.as_str())
                    .and_then(VecDeque::pop_front)
                    .map(ContentBlock::ToolResult)
            })
            .collect();
        messages.push(Message::new(HostRole::Tool, result_parts));
        facts.push(EntryFacts {
            mid: format!("{mid}:results"),
            ordinal: position + 2,
            tag_base: Some(metadata.tag_base),
            written_at,
            ..EntryFacts::default()
        });
    }
    let mut deliveries = Deliveries::new();
    for origin in &metadata.deliveries {
        let frame = frames
            .iter()
            .find(|frame| frame.seq == origin.seq)
            .ok_or_else(|| format!("compaction delivered source {} is missing", origin.seq))?;
        let WalRecord::SendDelivered {
            submission_id,
            appended_text: Some(text),
            ..
        } = &frame.record
        else {
            return Err(format!(
                "compaction delivered source {} has the wrong record type",
                origin.seq
            ));
        };
        if submission_id.0 != origin.submission_id {
            return Err(format!(
                "compaction delivered source {} has the wrong submission id",
                origin.seq
            ));
        }
        let mut delivered =
            Delivered::new(&origin.sender, text.strip_prefix("\n\n").unwrap_or(text));
        delivered.template = origin.template.clone();
        deliveries
            .entry((mid.into(), origin.part.clone()))
            .or_default()
            .push(delivered);
    }
    project(&messages, &facts, &deliveries, true)
        .into_iter()
        .next()
        .ok_or_else(|| "compaction entry projection is empty".into())
}

fn check_number_range(metadata: &Metadata) -> Result<(), String> {
    if metadata
        .tag_base
        .checked_add(metadata.tag_count)
        .is_none_or(|high| high > 9_007_199_254_740_991)
    {
        return Err("compaction tag metadata exceeds the protocol integer range".into());
    }
    Ok(())
}

pub fn source_frames<'a>(
    metadata: &Metadata,
    records: &'a [FramedRecord],
) -> Result<&'a [FramedRecord], String> {
    let start = records
        .binary_search_by_key(&metadata.source_seq, |frame| frame.seq)
        .map_err(|_| {
            format!(
                "compaction message source {} is missing",
                metadata.source_seq
            )
        })?;
    if matches!(records[start].record, WalRecord::ModelStepFinished { .. }) {
        let end = records[start + 1..]
            .iter()
            .position(|frame| {
                matches!(
                    frame.record,
                    WalRecord::ModelStepFinished { .. } | WalRecord::RunStarted { .. }
                )
            })
            .map_or(records.len(), |offset| start + 1 + offset);
        Ok(&records[start..end])
    } else {
        Ok(&records[start..start + 1])
    }
}

/// A delivery can arrive just above a projection watermark whose tool batch is
/// already materialized. Patch only that call, refreshing the shared digests.
pub fn apply_delivery(
    entry: &mut cortexkit_role_compaction::read::Entry,
    part: &str,
    delivered: Delivered,
) -> Result<(), String> {
    let target = entry
        .parts
        .iter_mut()
        .find(|candidate| candidate.id == part)
        .ok_or("compaction delivery names a missing native part")?;
    let PartBody::Tool(tool) = &mut target.body else {
        return Err("compaction delivery names a non-tool native part".into());
    };
    tool.delivered.push(delivered);
    target.digest = cortexkit_role_compaction::part::body_digest(target.seal.clone(), &target.body);
    entry.digest = cortexkit_role_compaction::read::entry_digest(
        entry.role.clone(),
        entry.notice.as_ref(),
        &entry.parts,
    );
    Ok(())
}
