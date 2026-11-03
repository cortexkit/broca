//! Projection of native messages into the compaction protocol's neutral model.

use std::collections::HashMap;

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
    pub written_at: u64,
    pub completed_at: Option<u64>,
    pub notice: Option<NoticeOrigin>,
    pub usage: Option<StepUsage>,
    pub generated_under: Option<AppliedView>,
}

/// Delivered text is kept apart from the original tool result. The key is the native call id.
pub type Deliveries = HashMap<String, Vec<Delivered>>;

/// Recover attribution from admission records, not the current plan or the run
/// that happened to carry the send. Records are joined by submission identity.
pub fn delivered_from_records<'a>(
    records: impl IntoIterator<Item = &'a broca_wal::WalRecord>,
) -> Deliveries {
    use broca_protocol::{BindPrincipal, DeliveryMode};
    use broca_wal::WalRecord;
    let mut origins: HashMap<String, (Option<String>, Option<String>)> = HashMap::new();
    let mut deliveries = Deliveries::new();
    for record in records {
        match record {
            WalRecord::DeliveryOrdered {
                submission_id,
                order,
                sent_by,
            } => {
                let origin = origins.entry(submission_id.0.clone()).or_default();
                if let Some(BindPrincipal::Reserved { module_id }) = sent_by {
                    origin.0 = Some(module_id.clone());
                }
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
                    if let Some(BindPrincipal::Reserved { module_id }) =
                        sent_by.as_ref().or(principal.as_ref())
                    {
                        origin.0 = Some(module_id.clone());
                    }
                }
            }
            WalRecord::SendDelivered {
                submission_id,
                mode: DeliveryMode::ToolResult,
                tool_call_id: Some(call),
                appended_text: Some(text),
                ..
            } => {
                let Some((Some(sender), template)) = origins.get(&submission_id.0) else {
                    continue;
                };
                let text = text.strip_prefix("\n\n").unwrap_or(text);
                let mut delivered = Delivered::new(sender, text);
                delivered.template = template.clone();
                deliveries
                    .entry(call.0.clone())
                    .or_default()
                    .push(delivered);
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
    let results: HashMap<_, _> = messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            ContentBlock::ToolResult(result) => Some((result.tool_call_id.0.as_str(), result)),
            _ => None,
        })
        .collect();
    let mut entries = Vec::with_capacity(messages.len());
    for (position, (message, facts)) in messages.iter().zip(facts).enumerate() {
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
                    if let Some(result) = results.get(call.tool_call_id.0.as_str()) {
                        let (status, output, format, attachments) = tool_output(&result.output);
                        tool.status = status;
                        tool.output_bytes = Some(output.len() as u64);
                        tool.output = Some(output);
                        tool.output_format = format;
                        tool.attachments = attachments;
                    }
                    tool.delivered = deliveries
                        .get(call.tool_call_id.0.as_str())
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
                ContentBlock::ProviderFallback { raw, .. } => (
                    PartBody::Opaque(
                        Opaque::new("anthropic.fallback")
                            .with_size(raw.as_ref().map_or(0, |raw| raw.len() as u64)),
                    ),
                    Seal::None,
                ),
            };
            parts.push(Part::new(index.to_string(), body).with_seal(seal));
        }
        let final_tools = parts.iter().all(|part| !matches!(&part.body, PartBody::Tool(tool) if tool.status == ToolStatus::Running));
        let mut entry = Entry::new(
            position as u64 + 1,
            &facts.mid,
            role,
            facts.written_at,
            parts,
        );
        if let Some(notice) = &facts.notice {
            entry = entry.with_notice(notice.clone());
        }
        if message.role == HostRole::Assistant && final_tools {
            entry.completed_at = facts.completed_at;
        }
        entry.usage = facts.usage;
        entry.generated_under = facts.generated_under.clone();
        entries.push(entry);
    }
    tags::assign_tags(0, &mut entries);
    entries
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
        ToolOutput::Error { message, .. } => (
            ToolStatus::Error,
            message.clone(),
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
