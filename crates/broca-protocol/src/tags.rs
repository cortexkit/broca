//! Runner-owned tags: the `§N§` numbers a session's parts show the model ([`tag_mark`] is the
//! llm-runner contract's spelling).
//!
//! Under the stored-ledger rules (`runner-tags/v1`) a number was assigned to every part when
//! broca wrote the record carrying it, and stored in that record. Under the computed rules
//! (`runner-tags/v2`) no WAL record stores a number: a part shows a number only when it counts and its run
//! renders tags, numbered in the order a request renders the transcript ([`number_message`]),
//! and the model's own tag notation is cleaned wherever its text is exposed
//! ([`exposed_message`]).
//!
//! Tags are metadata, never message content: a message's stored bytes, its serialized form and
//! its equality ignore them. Only a run frozen with `render_tags` shows them to the model, in
//! the rendered request (see [`render_tags_into`]).

use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub use cortexkit_role_llm_runner::tags::NumberingOverflow;
use cortexkit_role_llm_runner::tags::{PartKind, RenderedPart, TextWriter};

use crate::content::{ContentBlock, Message, Role, ToolOutput};

/// The contract spelling of tag `tag`: `§N§`.
pub fn tag_mark(tag: u64) -> String {
    cortexkit_role_llm_runner::tags::format_tag(tag).expect("stored tag numbers are positive")
}

/// The tags of one message: one tag per content block, in block order. Serialized as a bare
/// array of numbers. A record written before its session's tags were on has no tags at all,
/// never a partial list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MessageTags {
    pub parts: Vec<u64>,
}

impl MessageTags {
    pub fn new(parts: Vec<u64>) -> Self {
        Self { parts }
    }

    /// The highest tag this message holds.
    pub fn max_tag(&self) -> Option<u64> {
        self.parts.iter().copied().max()
    }
}

/// The tags of one transcript message's parts, as the transcript holds them: one entry per
/// content block, in block order. Each part carries its own tag, so a message assembled from
/// several records (a tool turn's results, one record per call) has each result's tag in its
/// position. An entry is `None` for a part that shows no number: under the stored-ledger rules
/// only a part written before its session's tags were on, under the computed rules every part
/// that does not count or whose run renders no tags.
///
/// A message written under the stored-ledger rules (`runner-tags/v1`) carries only its parts'
/// stored numbers and serializes as a bare array of numbers and `null`s, the form the store has
/// always kept beside each message row. A message written under the computed rules
/// (`runner-tags/v2`) also carries [`ComputedTags`], and serializes as
/// `{"parts": [...], "computed": {...}}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartTags {
    pub parts: Vec<Option<u64>>,
    /// The computed rules this message was written and numbered under; `None` for a message
    /// whose numbers the stored ledger assigned.
    pub computed: Option<ComputedTags>,
}

/// How a message written under the computed tag rules is numbered and exposed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputedTags {
    /// Whether the run that wrote the message rendered tags. Only then do its counted parts
    /// carry numbers.
    pub render: bool,
    /// Whether the model's own text in the message is cleaned of tag notation on every
    /// exposure: the writing run rendered tags and was not a structured-output run whose final
    /// text is its output. Applies to assistant text only.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub clean: bool,
    /// The highest number shown before this message's first part, so its counted parts are
    /// numbered `base + 1`, `base + 2`, ... in block order. Derived from the transcript, never
    /// recorded in the WAL; it lets a reader that holds only the last stored message continue
    /// numbering after it.
    pub base: u64,
}

impl Serialize for PartTags {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.computed {
            None => self.parts.serialize(serializer),
            Some(computed) => {
                #[derive(Serialize)]
                struct Computed<'a> {
                    parts: &'a [Option<u64>],
                    computed: &'a ComputedTags,
                }
                Computed {
                    parts: &self.parts,
                    computed,
                }
                .serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for PartTags {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Stored {
            Ledger(Vec<Option<u64>>),
            Computed {
                parts: Vec<Option<u64>>,
                computed: ComputedTags,
            },
        }
        Ok(match Stored::deserialize(deserializer)? {
            Stored::Ledger(parts) => PartTags {
                parts,
                computed: None,
            },
            Stored::Computed { parts, computed } => PartTags {
                parts,
                computed: Some(computed),
            },
        })
    }
}

impl PartTags {
    pub fn new(parts: Vec<Option<u64>>) -> Self {
        Self {
            parts,
            computed: None,
        }
    }

    /// Every part tagged, in order.
    pub fn all(parts: impl IntoIterator<Item = u64>) -> Self {
        Self::new(parts.into_iter().map(Some).collect())
    }

    /// `(block index, tag)` for every tagged part.
    pub fn tagged(&self) -> impl Iterator<Item = (usize, u64)> + '_ {
        self.parts
            .iter()
            .enumerate()
            .filter_map(|(index, tag)| tag.map(|tag| (index, tag)))
    }

    /// The highest number shown once this message is numbered: its highest part number, or
    /// for a computed message whose parts show none, the number before it.
    pub fn high(&self) -> u64 {
        let parts = self.parts.iter().flatten().copied().max().unwrap_or(0);
        self.computed
            .map_or(parts, |computed| computed.base.max(parts))
    }

    /// Whether the model's text in this message is cleaned on every exposure.
    pub fn cleans_model_text(&self) -> bool {
        self.computed.is_some_and(|computed| computed.clean)
    }
}

impl From<&MessageTags> for PartTags {
    fn from(tags: &MessageTags) -> Self {
        Self::all(tags.parts.iter().copied())
    }
}

/// A message's tags as it is held in memory. Never serialized: the WAL keeps tags on the
/// records that carry the parts, and the store keeps them beside each message row.
///
/// Equality always holds, so two messages with the same content compare equal whether or not
/// their tags were attached. Tags are not content, and every equality check on messages in
/// this workspace is a check of content.
#[derive(Debug, Clone, Default)]
pub struct TagSlot {
    tags: Option<Arc<PartTags>>,
    /// Whether [`render_tags_into`] already prefixed this message's text, so that rendering
    /// the same prompt again leaves it unchanged.
    rendered: bool,
    delivery: Option<DeliveryAttribution>,
}

/// Delivery metadata projected beside a message, never into its stored content. The
/// carrying tool id is resolved to its durable dispatch key by the read adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryAttribution {
    pub mode: crate::DeliveryMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

impl PartialEq for TagSlot {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for TagSlot {}

impl TagSlot {
    pub fn new(tags: Option<Arc<PartTags>>) -> Self {
        Self {
            tags,
            rendered: false,
            delivery: None,
        }
    }

    pub fn delivery(&self) -> Option<&DeliveryAttribution> {
        self.delivery.as_ref()
    }

    pub fn set_delivery(&mut self, delivery: DeliveryAttribution) {
        self.delivery = Some(delivery);
    }

    pub fn get(&self) -> Option<&PartTags> {
        self.tags.as_deref()
    }

    pub fn shared(&self) -> Option<Arc<PartTags>> {
        self.tags.clone()
    }

    pub fn is_rendered(&self) -> bool {
        self.rendered
    }
}

/// The wire family whose assistant text blocks may show their tags (see [`TagRendering`]).
pub const ANTHROPIC_MESSAGES_FAMILY: &str = "anthropic_messages";

/// Which parts a run frozen with `render_tags` shows tags in, for the wire family it renders
/// to. Tagged user text blocks and tagged tool-result text outputs always show them. Assistant
/// text shows them only on Anthropic Messages, where a live check confirmed that a tagged text
/// block beside unchanged signed thinking is accepted and keeps the cache. Every other family's
/// assistant parts keep their tags as metadata only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TagRendering {
    /// Plain assistant `text` blocks show their tag. Never thinking, encrypted reasoning,
    /// tool calls or a text block carrying provider state.
    pub assistant_text: bool,
}

impl TagRendering {
    /// The rendering for the wire family `family_id` names (the frozen render config's
    /// `family_id`); an unknown or absent family shows no assistant tags.
    pub fn for_family(family_id: Option<&str>) -> Self {
        Self {
            assistant_text: family_id == Some(ANTHROPIC_MESSAGES_FAMILY),
        }
    }

    /// The rendering for a run whose provider froze `frozen_render_config`.
    pub fn for_render_config(frozen_render_config: &serde_json::Value) -> Self {
        Self::for_family(
            frozen_render_config
                .get("family_id")
                .and_then(serde_json::Value::as_str),
        )
    }
}

/// Show the tags of `messages`' tagged parts in place, as `rendering` allows: `§N§ ` before
/// each user text block and each tool-result text output, and, where the family allows it,
/// in each plain assistant text block. Used only to build the model's view of a run frozen with
/// `render_tags`; stored records are never changed.
///
/// An assistant text that already begins with a tag mark (the model wrote one, possibly the
/// wrong number) is shown with the part's own tag in that mark's place; any other text gets the
/// mark and a space prepended.
///
/// Untouched: system messages, thinking and encrypted reasoning, tool-call arguments, blocks
/// with provider state, tool-result JSON, denial and error outputs, assistant parts where the
/// family shows none, and any message without tags. A message is rendered at most once, so
/// calling this again over a prompt that already holds rendered messages changes nothing. Its
/// output depends only on the stored text and its tag, so every later render of the part, on a
/// resume or a cache warm too, is byte-identical.
pub fn render_tags_into(messages: &mut [Message], rendering: TagRendering) {
    for message in messages {
        render_message_tags(message, rendering);
    }
}

fn render_message_tags(message: &mut Message, rendering: TagRendering) {
    if message.tags.rendered {
        return;
    }
    let Some(tags) = message.tags.tags.clone() else {
        return;
    };
    message.tags.rendered = true;
    // A message written under the computed rules renders by them: its own numbers, the
    // model's text cleaned, an image-only result as its label. The stored-ledger rules below
    // stay exactly as they were for every message written under them.
    if let Some(computed) = tags.computed {
        render_computed(message, &tags, computed, true);
        return;
    }
    let role = message.role;
    for (block, tag) in message.content.iter_mut().zip(tags.parts.iter()) {
        let Some(tag) = tag else {
            continue;
        };
        match (role, block) {
            (Role::User, ContentBlock::Text { text }) => prefix(text, *tag),
            (Role::Assistant, ContentBlock::Text { text }) if rendering.assistant_text => {
                retag(text, *tag)
            }
            (Role::User | Role::Tool, ContentBlock::ToolResult(result)) => {
                if let ToolOutput::Text { text } = &mut result.output {
                    prefix(text, *tag);
                }
            }
            _ => {}
        }
    }
}

fn prefix(text: &mut String, tag: u64) {
    let mark = tag_mark(tag);
    let mut rendered = String::with_capacity(mark.len() + 1 + text.len());
    rendered.push_str(&mark);
    rendered.push(' ');
    rendered.push_str(text);
    *text = rendered;
}

/// The length in bytes of the `§N§` mark `text` begins with, if it begins with one.
fn leading_mark_len(text: &str) -> Option<usize> {
    let rest = text.strip_prefix('§')?;
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return None;
    }
    rest[digits..]
        .strip_prefix('§')
        .map(|_| 2 * '§'.len_utf8() + digits)
}

/// Show `tag` at the start of an assistant text: in place of a mark the text already begins
/// with, or else prepended with a space.
fn retag(text: &mut String, tag: u64) {
    match leading_mark_len(text) {
        Some(len) => text.replace_range(..len, &tag_mark(tag)),
        None => prefix(text, tag),
    }
}

/// The rules a run frozen under the computed tag rules (`runner-tags/v2`) writes its messages
/// by: whether it renders tags, and whether the model's text it writes is cleaned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComputedRules {
    pub render: bool,
    pub clean: bool,
}

/// What a part of a `role` message is, as tag numbering sees it (llm-runner §9.6), with
/// `clean` saying whether the model's text in it is cleaned on exposure. `None` for a part no
/// rule names and that never shows a number: a tool result whose output is structured JSON,
/// which a provider receives as a value a number cannot prefix.
pub fn part_kind(role: Role, block: &ContentBlock, clean: bool) -> Option<PartKind> {
    let text_kind = |text: &str, kind: PartKind| {
        if text.is_empty() {
            PartKind::EmptyText
        } else {
            kind
        }
    };
    Some(match block {
        // Runner tags currently assign no number to a standalone media block.
        ContentBlock::Media(_) => return None,
        // A server-side fallback handoff is provider bookkeeping, not text the model wrote, so
        // it never shows a number.
        ContentBlock::ProviderFallback { .. } => return None,
        ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => match role {
            Role::System => PartKind::TurnSystem,
            Role::Assistant => text_kind(&exposed_model_text(clean, text), PartKind::AssistantText),
            Role::Tool => text_kind(text, PartKind::ToolResultText),
            Role::User => text_kind(text, PartKind::UserText),
        },
        ContentBlock::Reasoning { .. } | ContentBlock::RedactedReasoning { .. } => {
            PartKind::Reasoning
        }
        ContentBlock::ToolCall(_) => PartKind::ToolCall,
        ContentBlock::ToolResult(result) => match &result.output {
            ToolOutput::Text { text } if crate::content::is_image_only_placeholder(text) => {
                PartKind::ToolResultImageOnly
            }
            ToolOutput::Text { text: message }
            | ToolOutput::Error { message }
            | ToolOutput::Denied { reason: message } => {
                text_kind(message, PartKind::ToolResultText)
            }
            ToolOutput::Json { .. } => return None,
            ToolOutput::Parts { parts } => parts_numbering(parts).0,
        },
    })
}

// Map storage once for the shared contract's classification and placement rules. Numbering
// and rendering must see the same blocks, including empty text and every media reference.
fn parts_numbering(parts: &[crate::ToolOutputPart]) -> (PartKind, Option<usize>) {
    use cortexkit_role_llm_runner::tags::{
        numbered_block_index, tool_result_kind, ToolResultBlock,
    };
    let blocks: Vec<_> = parts
        .iter()
        .map(|part| match part {
            crate::ToolOutputPart::Text { text } => ToolResultBlock::Text(text),
            crate::ToolOutputPart::Media { media } => match media.kind {
                crate::MediaKind::Image => ToolResultBlock::Image,
                _ => ToolResultBlock::Media,
            },
        })
        .collect();
    (tool_result_kind(&blocks), numbered_block_index(&blocks))
}

/// The model's text as every exposure shows it: cleaned of tag notation when `clean`, else as
/// stored (llm-runner `tags::exposed_model_text`).
pub fn exposed_model_text(clean: bool, text: &str) -> std::borrow::Cow<'_, str> {
    let writer = if clean {
        TextWriter::TagRenderingRun
    } else {
        TextWriter::UntaggedRun
    };
    cortexkit_role_llm_runner::tags::exposed_model_text(writer, text)
}

/// Number `message`, written under `rules`, after `high`, the highest number shown before it,
/// and raise `high` past its counted parts. A part counts exactly when [`part_kind`] says it
/// does; only a run that renders tags numbers anything. The parts are numbered in block order,
/// the order a request renders them, by llm-runner's `tags::try_number_parts`. Numbering that
/// would pass `u64::MAX` is refused with its `NumberingOverflow`, and the message is left as
/// it was: no two parts ever show one number.
pub fn number_message(
    message: &mut Message,
    rules: ComputedRules,
    high: &mut u64,
) -> Result<(), NumberingOverflow> {
    let clean = rules.clean && message.role == Role::Assistant;
    let base = *high;
    // The parts that count, with their block positions as their rendered order.
    let counted: Vec<(usize, RenderedPart)> = message
        .content
        .iter()
        .enumerate()
        .filter_map(|(index, block)| {
            let kind = part_kind(message.role, block, clean)?;
            (rules.render && cortexkit_role_llm_runner::tags::counts(kind))
                .then(|| (index, RenderedPart::new(kind, index as u64)))
        })
        .collect();
    let rendered: Vec<RenderedPart> = counted.iter().map(|(_, part)| *part).collect();
    let numbers = cortexkit_role_llm_runner::tags::try_number_parts(base, &rendered)?;
    let mut parts = vec![None; message.content.len()];
    for ((index, _), number) in counted.iter().zip(numbers) {
        parts[*index] = number;
    }
    *high = parts.iter().flatten().copied().max().unwrap_or(base);
    message.set_tags(Some(Arc::new(PartTags {
        parts,
        computed: Some(ComputedTags {
            render: rules.render,
            clean: rules.clean,
            base,
        }),
    })));
    Ok(())
}

/// The highest number `messages` show, numbered as they are: where numbering continues after
/// them.
pub fn high_after(messages: &[Message]) -> u64 {
    messages
        .iter()
        .filter_map(Message::part_tags)
        .map(PartTags::high)
        .max()
        .unwrap_or(0)
}

/// Whether `message`'s model text is cleaned on exposure: an assistant message a
/// tag-rendering run wrote under the computed rules.
pub fn cleans_model_text(message: &Message) -> bool {
    message.role == Role::Assistant && message.part_tags().is_some_and(PartTags::cleans_model_text)
}

/// `message` as every exposure shows it: the model's text cleaned where the run that wrote it
/// cleans ([`cleans_model_text`]), and otherwise unchanged. Never touches reasoning, tool-call
/// arguments or tool results.
pub fn exposed_message(message: &Message) -> std::borrow::Cow<'_, Message> {
    if !cleans_model_text(message) {
        return std::borrow::Cow::Borrowed(message);
    }
    let mut exposed = message.clone();
    for block in &mut exposed.content {
        if let ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } = block {
            *text = exposed_model_text(true, text).into_owned();
        }
    }
    std::borrow::Cow::Owned(exposed)
}

/// The exposed content of part `index` of `message`, as `session.resolve` answers it: a text
/// part's exposed text, or any other part's complete stored encoding.
pub fn exposed_part_text(message: &Message, index: usize) -> Option<String> {
    let block = message.content.get(index)?;
    Some(match block {
        ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
            exposed_model_text(cleans_model_text(message), text).into_owned()
        }
        other => part_text(other),
    })
}

/// Show a computed message's numbers, cleaned text and image labels in place, the way a run
/// that renders tags sends it (`render` true), or only its cleaned text (`render` false, a run
/// that renders no tags but resends replies an earlier tag-rendering run wrote).
fn render_computed(message: &mut Message, tags: &PartTags, computed: ComputedTags, render: bool) {
    let role = message.role;
    for (block, number) in message.content.iter_mut().zip(tags.parts.iter()) {
        let number = number.filter(|_| render);
        match (role, block) {
            (Role::Assistant, ContentBlock::Text { text })
            | (Role::Assistant, ContentBlock::TextWithState { text, .. }) => {
                let exposed = exposed_model_text(computed.clean, text).into_owned();
                *text = match number {
                    Some(number) => {
                        cortexkit_role_llm_runner::tags::render_counted_text(number, &exposed)
                    }
                    None => exposed,
                };
            }
            (Role::User, ContentBlock::Text { text }) => {
                if let Some(number) = number {
                    *text = cortexkit_role_llm_runner::tags::render_counted_text(number, text);
                }
            }
            (Role::User | Role::Tool, ContentBlock::ToolResult(result)) => {
                let Some(number) = number else { continue };
                match &mut result.output {
                    ToolOutput::Text { text }
                        if crate::content::is_image_only_placeholder(text) =>
                    {
                        *text = cortexkit_role_llm_runner::tags::image_label(number);
                    }
                    ToolOutput::Text { text: message }
                    | ToolOutput::Error { message }
                    | ToolOutput::Denied { reason: message } => {
                        *message =
                            cortexkit_role_llm_runner::tags::render_counted_text(number, message);
                    }
                    ToolOutput::Json { .. } => {}
                    ToolOutput::Parts { parts } => {
                        let (kind, index) = parts_numbering(parts);
                        if let Some(index) = index {
                            if let crate::ToolOutputPart::Text { text } = &mut parts[index] {
                                *text = cortexkit_role_llm_runner::tags::render_counted_text(
                                    number, text,
                                );
                            }
                        } else {
                            let label = match kind {
                                PartKind::ToolResultImageOnly => {
                                    Some(cortexkit_role_llm_runner::tags::image_label(number))
                                }
                                PartKind::ToolResultMediaOnly => {
                                    Some(cortexkit_role_llm_runner::tags::media_label(number))
                                }
                                _ => None,
                            };
                            if let Some(text) = label {
                                parts.insert(0, crate::ToolOutputPart::Text { text });
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

/// Clean, in place, the model's text that earlier tag-rendering runs wrote under the computed
/// rules, for a request of a run that renders no tags. Messages written under the stored-ledger
/// rules, or by runs that render no tags, are untouched. A message is handled at most once.
pub fn clean_computed_into(messages: &mut [Message]) {
    for message in messages {
        if message.tags.rendered {
            continue;
        }
        let Some(tags) = message.tags.tags.clone() else {
            continue;
        };
        let Some(computed) = tags.computed else {
            continue;
        };
        message.tags.rendered = true;
        if computed.clean {
            render_computed(message, &tags, computed, false);
        }
    }
}

/// A text part's text, or any other part's complete stored encoding. Resolving a non-text
/// part must preserve its metadata as well as its payload.
pub fn part_text(block: &ContentBlock) -> String {
    match block {
        ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => text.clone(),
        _ => serde_json::to_string(block).expect("a stored content block serializes"),
    }
}

/// The runner's opaque key for part `index` of the message at transcript ordinal `ordinal`:
/// `m<ordinal>#<index>`, the same `<mid>#<block index>` form a consumer already composes.
pub fn part_key(ordinal: u64, index: usize) -> String {
    format!("m{ordinal}#{index}")
}

#[cfg(test)]
mod tests {
    #[test]
    fn resolve_non_text_content_preserves_the_stored_encoding() {
        let block = super::ContentBlock::Reasoning {
            text: "thought".into(),
            signature: Some("signed".into()),
        };
        assert_eq!(
            super::part_text(&block),
            r#"{"type":"reasoning","text":"thought","signature":"signed"}"#
        );
        assert_eq!(
            super::part_text(&super::ContentBlock::Text {
                text: "héllo".into()
            }),
            "héllo"
        );
    }
    use super::*;
    use crate::content::{ToolCall, ToolResult};
    use crate::ids::ToolCallId;

    fn tagged(mut message: Message, parts: Vec<u64>) -> Message {
        message.set_tags(Some(Arc::new(PartTags::all(parts))));
        message
    }

    #[test]
    fn rendering_prefixes_user_and_tool_text_once_and_no_assistant_part_by_default() {
        let user = tagged(
            Message::new(
                Role::User,
                vec![
                    ContentBlock::Text { text: "hi".into() },
                    ContentBlock::Text {
                        text: "there".into(),
                    },
                ],
            ),
            vec![1, 2],
        );
        let assistant = tagged(
            Message::new(
                Role::Assistant,
                vec![
                    ContentBlock::Text {
                        text: "answer".into(),
                    },
                    ContentBlock::Reasoning {
                        text: "think".into(),
                        signature: Some("sig".into()),
                    },
                    ContentBlock::ToolCall(ToolCall {
                        tool_call_id: ToolCallId("c".into()),
                        tool_name: "t".into(),
                        input: serde_json::json!({"a": 1}),
                        thought_signature: Some("ts".into()),
                    }),
                ],
            ),
            vec![3, 4, 5],
        );
        let tool = tagged(
            Message::new(
                Role::Tool,
                vec![
                    ContentBlock::ToolResult(ToolResult {
                        tool_call_id: ToolCallId("c".into()),
                        output: ToolOutput::Text { text: "out".into() },
                        is_error: false,
                    }),
                    ContentBlock::ToolResult(ToolResult {
                        tool_call_id: ToolCallId("d".into()),
                        output: ToolOutput::Json {
                            value: serde_json::json!({"k": "v"}),
                        },
                        is_error: false,
                    }),
                ],
            ),
            vec![6, 7],
        );
        let untagged = Message::new(Role::User, vec![ContentBlock::Text { text: "x".into() }]);
        let original_assistant = assistant.clone();
        let mut prompt = vec![user, assistant, tool, untagged.clone()];
        render_tags_into(&mut prompt, TagRendering::default());
        let once = format!("{prompt:?}");
        render_tags_into(&mut prompt, TagRendering::default());
        assert_eq!(
            format!("{prompt:?}"),
            once,
            "rendering twice changes nothing"
        );

        assert_eq!(
            prompt[0].content,
            vec![
                ContentBlock::Text {
                    text: "§1§ hi".into()
                },
                ContentBlock::Text {
                    text: "§2§ there".into()
                },
            ]
        );
        assert_eq!(prompt[1].content, original_assistant.content);
        let ContentBlock::ToolResult(first) = &prompt[2].content[0] else {
            panic!("tool result")
        };
        assert_eq!(
            first.output,
            ToolOutput::Text {
                text: "§6§ out".into()
            }
        );
        let ContentBlock::ToolResult(second) = &prompt[2].content[1] else {
            panic!("tool result")
        };
        assert_eq!(
            second.output,
            ToolOutput::Json {
                value: serde_json::json!({"k": "v"})
            }
        );
        assert_eq!(prompt[3].content, untagged.content);
    }

    fn assistant(blocks: Vec<ContentBlock>, parts: Vec<u64>) -> Message {
        tagged(Message::new(Role::Assistant, blocks), parts)
    }

    fn text(text: &str) -> ContentBlock {
        ContentBlock::Text { text: text.into() }
    }

    /// Assistant text shows its tag on Anthropic Messages and on no other family.
    #[test]
    fn only_anthropic_shows_assistant_text_tags() {
        for family in [
            None,
            Some("openai_chat"),
            Some("openai_responses"),
            Some("openai_responses_chatgpt"),
            Some("gemini"),
            Some("google_codeassist"),
            Some("bedrock_converse"),
        ] {
            let mut prompt = vec![assistant(vec![text("answer")], vec![3])];
            render_tags_into(&mut prompt, TagRendering::for_family(family));
            assert_eq!(prompt[0].content, vec![text("answer")], "{family:?}");
        }
        let mut prompt = vec![assistant(vec![text("answer")], vec![3])];
        render_tags_into(
            &mut prompt,
            TagRendering::for_render_config(
                &serde_json::json!({"family_id": "anthropic_messages"}),
            ),
        );
        assert_eq!(prompt[0].content, vec![text("§3§ answer")]);
    }

    /// On Anthropic, only plain assistant text shows a tag. A mark the model wrote at the
    /// start is replaced by the part's own tag; thinking, encrypted reasoning, tool calls and
    /// text with provider state keep their bytes, and rendering twice changes nothing.
    #[test]
    fn anthropic_rendering_never_touches_thinking_tool_calls_or_provider_state() {
        use crate::content::{TextProviderFormat, TextProviderState};
        let untouched = vec![
            ContentBlock::Reasoning {
                text: "think".into(),
                signature: Some("sig".into()),
            },
            ContentBlock::RedactedReasoning {
                data: "opaque".into(),
                provider_format: None,
            },
            ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("c".into()),
                tool_name: "t".into(),
                input: serde_json::json!({"a": "§1§ arg"}),
                thought_signature: Some("ts".into()),
            }),
            ContentBlock::TextWithState {
                text: "stateful".into(),
                provider_state: TextProviderState {
                    format: TextProviderFormat::OpenAiResponses,
                    data: "{}".into(),
                },
            },
        ];
        let mut blocks = untouched.clone();
        blocks.extend([
            text("§99§ wrong mark"),
            text("§x§ not a mark"),
            text("§12§"),
            text(""),
        ]);
        let mut prompt = vec![assistant(blocks, vec![1, 2, 3, 4, 5, 6, 7, 8])];
        let anthropic = TagRendering::for_family(Some(ANTHROPIC_MESSAGES_FAMILY));
        render_tags_into(&mut prompt, anthropic);
        let once = prompt[0].content.clone();
        render_tags_into(&mut prompt, anthropic);
        assert_eq!(prompt[0].content, once, "rendering twice changes nothing");
        assert_eq!(&prompt[0].content[..4], untouched.as_slice());
        assert_eq!(
            &prompt[0].content[4..],
            [
                text("§5§ wrong mark"),
                text("§6§ §x§ not a mark"),
                text("§7§"),
                text("§8§ "),
            ]
        );
    }

    const RENDERING: ComputedRules = ComputedRules {
        render: true,
        clean: true,
    };

    fn image_result(id: &str) -> ContentBlock {
        ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId(id.into()),
            output: ToolOutput::Text {
                text: crate::content::non_text_tool_content_placeholder(
                    "image",
                    Some("image/png"),
                    Some(8),
                ),
            },
            is_error: false,
        })
    }

    /// Under the computed rules only the parts a request renders with a number count: the
    /// model's text and an image-only tool result do; reasoning, a tool call, empty text and a
    /// JSON tool output never do. Numbers run on from the number before the message.
    #[test]
    fn computed_numbering_counts_only_parts_rendered_with_a_number() {
        let mut high = 7;
        let mut assistant = Message::new(
            Role::Assistant,
            vec![
                ContentBlock::Reasoning {
                    text: "think".into(),
                    signature: Some("sig".into()),
                },
                text("answer"),
                text("§4§"),
                ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId("c".into()),
                    tool_name: "t".into(),
                    input: serde_json::json!({"a": 1}),
                    thought_signature: None,
                }),
            ],
        );
        number_message(&mut assistant, RENDERING, &mut high).unwrap();
        let tags = assistant.part_tags().unwrap();
        // "§4§" cleans to nothing, so it is empty text and shows no number.
        assert_eq!(tags.parts, vec![None, Some(8), None, None]);
        assert_eq!(tags.computed.unwrap().base, 7);
        let mut tool = Message::new(
            Role::Tool,
            vec![
                image_result("c"),
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("d".into()),
                    output: ToolOutput::Json {
                        value: serde_json::json!({"k": "v"}),
                    },
                    is_error: false,
                }),
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("e".into()),
                    output: ToolOutput::Error {
                        message: "failed".into(),
                    },
                    is_error: true,
                }),
            ],
        );
        number_message(&mut tool, RENDERING, &mut high).unwrap();
        assert_eq!(
            tool.part_tags().unwrap().parts,
            vec![Some(9), None, Some(10)]
        );
        assert_eq!(high, 10);
        // A run that renders no tags numbers nothing, but every part keeps its row.
        let mut quiet = Message::new(Role::User, vec![text("hi")]);
        number_message(
            &mut quiet,
            ComputedRules {
                render: false,
                clean: false,
            },
            &mut high,
        )
        .unwrap();
        assert_eq!(quiet.part_tags().unwrap().parts, vec![None]);
        assert_eq!(high, 10);
    }

    /// Numbering that would pass `u64::MAX` is refused with llm-runner's `NumberingOverflow`
    /// and leaves the message as it was; numbering that ends exactly at `u64::MAX` is accepted.
    /// No two parts ever show one number.
    #[test]
    fn numbering_past_u64_max_is_refused_never_repeated() {
        let two = || Message::new(Role::User, vec![text("a"), text("b")]);
        let mut message = two();
        let mut high = u64::MAX - 1;
        let overflow = number_message(&mut message, RENDERING, &mut high).unwrap_err();
        assert_eq!((overflow.checkpoint, overflow.counted), (u64::MAX - 1, 2));
        assert_eq!(high, u64::MAX - 1);
        assert!(message.part_tags().is_none(), "nothing numbered");
        let mut message = two();
        let mut high = u64::MAX - 2;
        number_message(&mut message, RENDERING, &mut high).unwrap();
        assert_eq!(
            message.part_tags().unwrap().parts,
            vec![Some(u64::MAX - 1), Some(u64::MAX)]
        );
    }

    /// A computed message renders by the computed rules: its own number before the text with
    /// leading tag notation stripped, the model's text cleaned, an image-only result as its
    /// label; a run that renders no tags still sends the model's text cleaned.
    #[test]
    fn computed_rendering_numbers_cleans_and_labels() {
        let mut high = 0;
        let mut user = Message::new(Role::User, vec![text("§9§ hello")]);
        number_message(&mut user, RENDERING, &mut high).unwrap();
        let mut assistant = Message::new(Role::Assistant, vec![text("§3§ hi as §1§ says")]);
        number_message(&mut assistant, RENDERING, &mut high).unwrap();
        let mut tool = Message::new(Role::Tool, vec![image_result("c")]);
        number_message(&mut tool, RENDERING, &mut high).unwrap();
        let mut prompt = vec![user, assistant.clone(), tool];
        render_tags_into(&mut prompt, TagRendering::default());
        assert_eq!(prompt[0].content, vec![text("§1§ hello")]);
        assert_eq!(prompt[1].content, vec![text("§2§ hi as  says")]);
        let ContentBlock::ToolResult(result) = &prompt[2].content[0] else {
            panic!("tool result")
        };
        assert_eq!(
            result.output,
            ToolOutput::Text {
                text: "§3§ [image]".into()
            }
        );
        let once = prompt.clone();
        render_tags_into(&mut prompt, TagRendering::default());
        assert_eq!(format!("{prompt:?}"), format!("{once:?}"));

        let mut later = vec![assistant];
        clean_computed_into(&mut later);
        assert_eq!(later[0].content, vec![text("hi as  says")]);
    }

    /// The model's text is exposed cleaned only when the run that wrote it cleans; the stored
    /// message keeps its raw text, and a reasoning part is never cleaned.
    #[test]
    fn exposed_text_is_cleaned_only_where_the_writing_run_cleans() {
        let raw = Message::new(
            Role::Assistant,
            vec![
                text("§3§ hi as §1§ says"),
                ContentBlock::Reasoning {
                    text: "§2§ thought".into(),
                    signature: None,
                },
            ],
        );
        for (clean, expected) in [(true, "hi as  says"), (false, "§3§ hi as §1§ says")] {
            let mut message = raw.clone();
            let mut high = 0;
            number_message(
                &mut message,
                ComputedRules {
                    render: true,
                    clean,
                },
                &mut high,
            )
            .unwrap();
            let exposed = exposed_message(&message);
            assert_eq!(exposed.content[0], text(expected), "clean={clean}");
            assert_eq!(exposed.content[1], raw.content[1]);
            assert_eq!(
                exposed_part_text(&message, 0).as_deref(),
                Some(expected),
                "clean={clean}"
            );
            assert_eq!(message.content, raw.content, "stored text stays raw");
        }
        // A message written under the stored ledger is never cleaned.
        let ledger = tagged(raw.clone(), vec![1, 2]);
        assert_eq!(exposed_message(&ledger).content, raw.content);
    }

    /// A message's tags keep the bare-array form under the stored ledger, so stored rows keep
    /// their bytes, and carry their computed rules beside the parts otherwise.
    #[test]
    fn part_tags_keep_the_ledger_form_and_carry_computed_rules() {
        let ledger = PartTags::new(vec![Some(1), None]);
        assert_eq!(serde_json::to_string(&ledger).unwrap(), "[1,null]");
        assert_eq!(
            serde_json::from_str::<PartTags>("[1,null]").unwrap(),
            ledger
        );
        let computed = PartTags {
            parts: vec![Some(5), None],
            computed: Some(ComputedTags {
                render: true,
                clean: true,
                base: 4,
            }),
        };
        let encoded = serde_json::to_string(&computed).unwrap();
        assert_eq!(
            encoded,
            r#"{"parts":[5,null],"computed":{"render":true,"clean":true,"base":4}}"#
        );
        assert_eq!(
            serde_json::from_str::<PartTags>(&encoded).unwrap(),
            computed
        );
        assert_eq!(computed.high(), 5);
    }

    #[test]
    fn tags_are_neither_serialized_nor_compared() {
        let plain = Message::new(Role::User, vec![ContentBlock::Text { text: "hi".into() }]);
        let with_tags = tagged(plain.clone(), vec![9]);
        assert_eq!(plain, with_tags);
        assert_eq!(
            serde_json::to_string(&plain).unwrap(),
            serde_json::to_string(&with_tags).unwrap()
        );
    }
}
