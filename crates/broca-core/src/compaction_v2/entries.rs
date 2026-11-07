//! Projection of native messages into the compaction protocol's neutral model.

use std::collections::{HashMap, VecDeque};

use broca_protocol::content::{
    ContentBlock, EncryptedReasoningFormat, MediaRef, Message, Role as HostRole, ToolOutput,
    ToolOutputPart,
};
use cortexkit_compaction_view::{clean::clean_reply, tags};
use cortexkit_role_compaction::{
    part::{
        Attachment, AttachmentItem, Delivered, FileMedia, Opaque, OutputFormat, Part, PartBody,
        Seal, ToolPart, ToolStatus, Transport,
    },
    read::{Entry, NoticeOrigin, Role, StepUsage},
    view::AppliedView,
};

/// Durable facts supplied by the transcript projection, never inferred from rendered text.
#[derive(Clone, Debug, Default)]
pub struct EntryFacts {
    pub mid: String,
    pub ordinal: u64,
    pub tag_base: Option<u64>,
    pub written_at: u64,
    pub completed_at: Option<u64>,
    pub notice: Option<NoticeOrigin>,
    pub usage: Option<StepUsage>,
    pub generated_under: Option<AppliedView>,
}

/// Delivered text is kept apart from the original tool result. Identity is the
/// assistant entry's mid and native part index; call ids can repeat in later turns.
pub type Deliveries = HashMap<(String, String), Vec<Delivered>>;

/// Render-only metadata absent from the public compaction part. It stays aligned
/// with each part's delivered list and is recovered from the durable source frame.
#[derive(Clone, Debug, Default)]
pub struct RideEnvelopes(HashMap<(String, String), Vec<Option<broca_wal::record::RideMetadata>>>);

impl RideEnvelopes {
    pub fn get(
        &self,
        mid: &str,
        part: &str,
        index: usize,
    ) -> Option<&broca_wal::record::RideMetadata> {
        self.0.get(&(mid.into(), part.into()))?.get(index)?.as_ref()
    }

    pub(crate) fn push(
        &mut self,
        mid: &str,
        part: &str,
        ride: Option<broca_wal::record::RideMetadata>,
    ) {
        self.0
            .entry((mid.into(), part.into()))
            .or_default()
            .push(ride);
    }
}

pub(crate) fn attach_ride_envelope(
    delivered: &mut Delivered,
    ride: Option<&broca_wal::record::RideMetadata>,
) {
    if let Some(ride) = ride {
        delivered.sender.clone_from(&ride.sender);
        delivered.send_id = Some(ride.send_id.clone());
        delivered.forms = Some(
            ride.forms
                .iter()
                .filter_map(|form| match form.as_str() {
                    "tool_result" => Some(cortexkit_role_compaction::part::RideForm::ToolResult),
                    "turn_system" => Some(cortexkit_role_compaction::part::RideForm::TurnSystem),
                    _ => None,
                })
                .collect(),
        );
    }
}

pub fn compact(entry: &Entry) -> Entry {
    let mut entry = entry.clone();
    entry.parts = entry.parts.iter().map(Part::compact).collect();
    entry
}

/// Person input has no notice origin. A non-module tool-result send is placed
/// by the runner, so its required sender names broca rather than a made-up caller.
pub fn delivery_sender(
    principal: Option<&broca_protocol::BindPrincipal>,
    mode: broca_protocol::DeliveryMode,
) -> Option<String> {
    match principal {
        Some(broca_protocol::BindPrincipal::Reserved { module_id }) => Some(module_id.clone()),
        _ if mode == broca_protocol::DeliveryMode::ToolResult => Some("broca".into()),
        _ => None,
    }
}

/// Recover attribution from admission records, not the current plan or the run
/// that happened to carry the send. Records are joined by submission identity.
pub fn delivered_from_records<'a>(
    records: impl IntoIterator<Item = &'a broca_wal::WalRecord>,
    mut target: impl FnMut(
        &broca_protocol::RunId,
        Option<broca_protocol::StepId>,
        &broca_protocol::ToolCallId,
    ) -> Option<(String, String)>,
) -> Deliveries {
    use broca_protocol::{BindPrincipal, DeliveryMode};
    use broca_wal::WalRecord;
    let mut origins: HashMap<String, (Option<BindPrincipal>, Option<String>)> = HashMap::new();
    let mut deliveries = Deliveries::new();
    let mut step = None;
    for record in records {
        match record {
            WalRecord::RunStarted { .. } => step = None,
            WalRecord::ModelStepFinished { step_id, .. } => step = Some(*step_id),
            WalRecord::DeliveryOrdered {
                submission_id,
                order,
                sent_by,
            } => {
                let origin = origins.entry(submission_id.0.clone()).or_default();
                origin.0 = sent_by.clone();
                origin.1 = order.template.as_ref().map(|template| template.id.clone());
            }
            WalRecord::Queued {
                submission_id,
                sent_by,
                principal,
                ..
            } => {
                let origin = origins.entry(submission_id.0.clone()).or_default();
                if origin.0.is_none() {
                    origin.0 = sent_by.clone().or_else(|| principal.clone());
                }
            }
            WalRecord::SendDelivered {
                submission_id,
                ride,
                mode: DeliveryMode::ToolResult,
                tool_call_id: Some(call),
                appended_text: Some(text),
                run_id,
                ..
            } => {
                let Some(identity) = target(run_id, step, call) else {
                    continue;
                };
                let (principal, template) =
                    origins.get(&submission_id.0).cloned().unwrap_or_default();
                let sender = delivery_sender(principal.as_ref(), DeliveryMode::ToolResult)
                    .expect("tool-result delivery has a sender");
                let text = text.strip_prefix("\n\n").unwrap_or(text);
                let mut delivered = Delivered::new(sender, text);
                attach_ride_envelope(&mut delivered, ride.as_ref());
                delivered.template = template;
                deliveries.entry(identity).or_default().push(delivered);
            }
            _ => {}
        }
    }
    deliveries
}

/// Map the full stored transcript, including result-only messages as empty entries.
/// `facts` is positional and already contains backward-only timestamp fallbacks.
pub fn project(
    messages: &[Message],
    facts: &[EntryFacts],
    deliveries: &Deliveries,
    accepts_empty_text: bool,
) -> Vec<Entry> {
    assert_eq!(messages.len(), facts.len());
    let mut entries = Vec::with_capacity(messages.len());
    let mut high = 0;
    for (position, (message, facts)) in messages.iter().zip(facts).enumerate() {
        assert!(
            facts.ordinal > 0,
            "compaction entries have one-based native ordinals"
        );
        let mut results: HashMap<&str, VecDeque<_>> = HashMap::new();
        if let Some(next) = messages
            .get(position + 1)
            .filter(|next| next.role == HostRole::Tool)
        {
            for block in &next.content {
                if let ContentBlock::ToolResult(result) = block {
                    results
                        .entry(&result.tool_call_id.0)
                        .or_default()
                        .push_back(result);
                }
            }
        }
        let role = match message.role {
            HostRole::Assistant | HostRole::Tool => Role::Assistant,
            HostRole::User => Role::User,
            HostRole::System => Role::System,
        };
        let mut parts = Vec::new();
        for (index, block) in message.content.iter().enumerate() {
            let (body, seal) = match block {
                ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                    if text.is_empty() {
                        continue;
                    }
                    let text = if message.role == HostRole::Assistant {
                        neutral_reply(text, accepts_empty_text)
                    } else {
                        text.clone()
                    };
                    (PartBody::Text { text }, Seal::None)
                }
                ContentBlock::Reasoning { text, signature } => (
                    PartBody::Reasoning {
                        text: Some(text.clone()),
                        text_bytes: Some(text.len() as u64),
                    },
                    if signature.is_some() {
                        Seal::Signed
                    } else {
                        Seal::None
                    },
                ),
                ContentBlock::RedactedReasoning {
                    provider_format, ..
                } => (
                    PartBody::Reasoning {
                        text: None,
                        text_bytes: None,
                    },
                    match provider_format {
                        Some(
                            EncryptedReasoningFormat::OpenAiResponses
                            | EncryptedReasoningFormat::OpenAiResponsesChatgpt
                            | EncryptedReasoningFormat::OpenRouter,
                        ) => Seal::Encrypted,
                        _ => Seal::Signed,
                    },
                ),
                ContentBlock::ToolCall(call) => {
                    let mut tool = ToolPart::new(
                        call.tool_call_id.0.as_str(),
                        &call.tool_name,
                        call.input.clone(),
                        ToolStatus::Running,
                    );
                    if let Some(result) = results
                        .get_mut(call.tool_call_id.0.as_str())
                        .and_then(VecDeque::pop_front)
                    {
                        let (status, output, format, attachments) = tool_output(&result.output);
                        tool.status = status;
                        tool.output_bytes = Some(output.len() as u64);
                        tool.output = Some(output);
                        tool.output_format = format;
                        tool.attachments = attachments;
                    }
                    tool.delivered = deliveries
                        .get(&(facts.mid.clone(), index.to_string()))
                        .cloned()
                        .unwrap_or_default();
                    (
                        PartBody::Tool(tool),
                        if call.thought_signature.is_some() {
                            Seal::Signed
                        } else {
                            Seal::None
                        },
                    )
                }
                ContentBlock::ToolResult(_) => continue,
                ContentBlock::Media(media) => (PartBody::File(file(media)), Seal::None),
                ContentBlock::ProviderFallback { raw, .. } => {
                    let mut opaque = Opaque::new("anthropic.fallback");
                    if let Some(raw) = raw {
                        opaque = opaque.with_size(raw.len() as u64);
                    }
                    (PartBody::Opaque(opaque), Seal::None)
                }
            };
            parts.push(Part::new(index.to_string(), body).with_seal(seal));
        }
        let mut entry = Entry::new(facts.ordinal, &facts.mid, role, facts.written_at, parts);
        if let Some(notice) = &facts.notice {
            entry = entry.with_notice(notice.clone());
        }
        entry.completed_at =
            project_completion(message, messages.get(position + 1), facts.completed_at);
        entry.usage = facts.usage;
        entry.generated_under = facts.generated_under.clone();
        let base = facts.tag_base.unwrap_or(high);
        tags::assign_tags(base, std::slice::from_mut(&mut entry));
        high = entry
            .parts
            .iter()
            .filter_map(|part| part.tag)
            .max()
            .unwrap_or(base);
        entries.push(entry);
    }
    entries
}

/// Project the host finish only after every tool call has its stored final result.
/// Every stored ToolOutput variant is final; a held or running call has no result.
/// Compaction's session.entries projection and transcript display facts use this rule.
pub fn project_completion(
    message: &Message,
    following: Option<&Message>,
    at: Option<u64>,
) -> Option<u64> {
    if message.role != HostRole::Assistant || at.is_none() {
        return None;
    }
    let mut results = HashMap::<&str, usize>::new();
    if let Some(next) = following.filter(|next| next.role == HostRole::Tool) {
        for block in &next.content {
            if let ContentBlock::ToolResult(result) = block {
                *results.entry(&result.tool_call_id.0).or_default() += 1;
            }
        }
    }
    let final_tools = message.content.iter().all(|block| {
        let ContentBlock::ToolCall(call) = block else {
            return true;
        };
        let Some(count) = results.get_mut(call.tool_call_id.0.as_str()) else {
            return false;
        };
        if *count == 0 {
            return false;
        }
        *count -= 1;
        true
    });
    crate::message_times::completed_at(message.role, at, final_tools)
}

/// Project completion from already-folded host stamps and native source positions,
/// excluding supplied assistant input without reading another record or a clock.
pub fn completion_times(
    messages: &[Message],
    first: u64,
    times: &std::collections::BTreeMap<u64, u64>,
    native_assistants: &std::collections::BTreeSet<u64>,
) -> Vec<Option<u64>> {
    messages
        .iter()
        .enumerate()
        .map(|(index, message)| {
            let position = first + index as u64;
            let at = native_assistants
                .contains(&position)
                .then(|| times.get(&position).copied())
                .flatten();
            project_completion(message, messages.get(index + 1), at)
        })
        .collect()
}

pub(super) fn neutral_reply(text: &str, accepts_empty_text: bool) -> String {
    let cleaned = clean_reply(text);
    if cleaned.text.is_empty() && !accepts_empty_text {
        text.to_owned()
    } else {
        cleaned.text
    }
}

pub(super) fn file(media: &MediaRef) -> FileMedia {
    let mut file = FileMedia::new(&media.mime, Transport::Reference).with_size(media.byte_size);
    file.width = media.width.map(u64::from);
    file.height = media.height.map(u64::from);
    file
}

pub(super) fn tool_output(
    output: &ToolOutput,
) -> (ToolStatus, String, OutputFormat, Vec<Attachment>) {
    match output {
        ToolOutput::Text { text } => (
            ToolStatus::Completed,
            text.clone(),
            OutputFormat::Text,
            vec![],
        ),
        ToolOutput::Json { value } => (
            ToolStatus::Completed,
            serde_jcs::to_string(value).expect("stored JSON is canonicalizable"),
            OutputFormat::Json,
            vec![],
        ),
        ToolOutput::Error {
            message,
            module,
            code,
            detail,
            nothing_sent,
            retry,
        } => (
            ToolStatus::Error,
            ToolOutput::error_model_text(
                message,
                module.as_deref(),
                code.as_deref(),
                detail.as_deref(),
                *nothing_sent,
                retry.as_deref(),
            ),
            OutputFormat::Text,
            vec![],
        ),
        ToolOutput::Denied { reason } => (
            ToolStatus::Denied,
            reason.clone(),
            OutputFormat::Text,
            vec![],
        ),
        ToolOutput::Parts { parts } => {
            let text = parts
                .iter()
                .filter_map(|part| match part {
                    ToolOutputPart::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            let attachments = parts
                .iter()
                .enumerate()
                .filter_map(|(index, part)| match part {
                    ToolOutputPart::Media { media } => Some(Attachment::new(
                        index.to_string(),
                        AttachmentItem::File(file(media)),
                    )),
                    _ => None,
                })
                .collect();
            (ToolStatus::Completed, text, OutputFormat::Text, attachments)
        }
    }
}
