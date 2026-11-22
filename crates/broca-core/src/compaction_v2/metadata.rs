//! Facts kept beside native transcript rows. Historical reads do not scan the
//! log for a send's admission or a step's applied view. The continuation keeps
//! frozen reminder settings for shared placement, never transcript bodies.

use std::collections::{BTreeMap, HashMap, VecDeque};

use broca_protocol::{BindPrincipal, ContentBlock, Message, Role as HostRole, Usage};
use broca_records::record::WalRecord;
use broca_wal::FramedRecord;
use cortexkit_role_compaction::{
    part::{Delivered, PartBody, ToolPart, ToolStatus},
    read::{NoticeOrigin, Role, SpeaksFor, StepUsage},
    view::AppliedView,
};

use crate::placement::Placement;

/// Compaction-view metadata and live display use the same frozen render configuration
/// to decide whether assistant replies are cleaned.
pub fn clean_replies(render_tags: bool, frozen_render_config: &serde_json::Value) -> bool {
    render_tags
        && !broca_protocol::structured_output::FrozenStructuredOutput::from_frozen_render_config(
            Some(frozen_render_config),
        )
        .is_some_and(|frozen| frozen.mode == broca_protocol::structured_output::Mode::FinalText)
}

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
    /// The agent whose words a marked send carried, on each of its user messages. Absent on
    /// every other row, so an unmarked row keeps its earlier bytes. Rows projected by a build
    /// before this field lack it even for a marked send.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speaks_for: Option<SpeaksFor>,
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
    /// The marks recorded with the send. Absent for every unmarked send, so a projection
    /// checkpoint without marks keeps its earlier bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    marks: Option<broca_protocol::MessageMarks>,
}

impl Origin {}

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

impl Positioned {}

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
    numbered_parts_of(message.role, message.content.iter().map(NumberingKind::of))
}

/// What the numbering reads of a block: whether it is text with something in it, or a tool
/// call. A caller holding only a block's shape (its content released from memory) numbers it
/// the same as the block itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumberingKind {
    /// Text that isn't empty.
    Text,
    ToolCall,
    /// Empty text and every other kind of block, which take no number.
    Other,
}

impl NumberingKind {
    pub fn of(block: &ContentBlock) -> Self {
        match block {
            ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. }
                if !text.is_empty() =>
            {
                Self::Text
            }
            ContentBlock::ToolCall(_) => Self::ToolCall,
            _ => Self::Other,
        }
    }
}

/// [`numbered_parts`] over a message's role and its blocks' numbering kinds.
pub fn numbered_parts_of(role: HostRole, kinds: impl IntoIterator<Item = NumberingKind>) -> u64 {
    numbered_flags_of(role, kinds)
        .into_iter()
        .filter(|numbered| *numbered)
        .count() as u64
}

fn numbered_flags_of(role: HostRole, kinds: impl IntoIterator<Item = NumberingKind>) -> Vec<bool> {
    let role = match role {
        HostRole::Tool => return kinds.into_iter().map(|_| false).collect(),
        HostRole::User => Role::User,
        HostRole::Assistant => Role::Assistant,
        HostRole::System => Role::System,
    };
    kinds
        .into_iter()
        .map(|kind| {
            let body = match kind {
                NumberingKind::Text => PartBody::Text {
                    text: Default::default(),
                },
                NumberingKind::ToolCall => PartBody::Tool(ToolPart::new(
                    "",
                    "",
                    serde_json::Value::Null,
                    ToolStatus::Running,
                )),
                NumberingKind::Other => return false,
            };
            cortexkit_compaction_view::tags::is_numbered(&role, &body)
        })
        .collect()
}

impl Fold {}

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
