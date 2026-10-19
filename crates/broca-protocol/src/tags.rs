//! Runner-owned tags: a durable, per-session number for every message part broca stores.
//!
//! A tag is assigned when broca writes the record that carries the part, and it is stored in
//! that record. Numbers are strictly increasing in WAL order and never reused within a session,
//! across episodes, resumes, restarts, retention deletion and agent-data moves. The `§N§`
//! spelling ([`tag_mark`]) is part of the llm-runner contract.
//!
//! Tags are metadata, never message content: a message's stored bytes, its serialized form and
//! its equality ignore them. Only a run frozen with `render_tags` shows them to the model, by
//! prefixing user and tool-result text (and, on Anthropic Messages, assistant text) in the
//! rendered request (see [`render_tags_into`]).

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::content::{ContentBlock, Message, Role, ToolOutput};

/// The contract spelling of tag `tag`: `§N§`.
pub fn tag_mark(tag: u64) -> String {
    format!("§{tag}§")
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
/// position. An entry is `None` only for a part whose record was written before its session's
/// tags were on, which a tagged lineage never has. Serialized as an array of numbers (and
/// `null`s), the form the store keeps beside each message row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PartTags {
    pub parts: Vec<Option<u64>>,
}

impl PartTags {
    pub fn new(parts: Vec<Option<u64>>) -> Self {
        Self { parts }
    }

    /// Every part tagged, in order.
    pub fn all(parts: impl IntoIterator<Item = u64>) -> Self {
        Self {
            parts: parts.into_iter().map(Some).collect(),
        }
    }

    /// `(block index, tag)` for every tagged part.
    pub fn tagged(&self) -> impl Iterator<Item = (usize, u64)> + '_ {
        self.parts
            .iter()
            .enumerate()
            .filter_map(|(index, tag)| tag.map(|tag| (index, tag)))
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
}

impl PartialEq for TagSlot {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl TagSlot {
    pub fn new(tags: Option<Arc<PartTags>>) -> Self {
        Self {
            tags,
            rendered: false,
        }
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

/// The text a part holds, as stored: what `session.resolve` answers and what a character
/// range indexes. Text, reasoning and text tool outputs give their text; a tool call gives
/// its arguments, a JSON tool output its value, both as compact JSON; a denial or error gives
/// its message; an encrypted reasoning block gives its opaque data.
pub fn part_text(block: &ContentBlock) -> String {
    match block {
        ContentBlock::Text { text }
        | ContentBlock::TextWithState { text, .. }
        | ContentBlock::Reasoning { text, .. } => text.clone(),
        ContentBlock::RedactedReasoning { data, .. } => data.clone(),
        ContentBlock::ToolCall(call) => {
            serde_json::to_string(&call.input).expect("a JSON value serializes")
        }
        ContentBlock::ToolResult(result) => match &result.output {
            ToolOutput::Text { text } => text.clone(),
            ToolOutput::Json { value } => {
                serde_json::to_string(value).expect("a JSON value serializes")
            }
            ToolOutput::Denied { reason } => reason.clone(),
            ToolOutput::Error { message } => message.clone(),
        },
    }
}

/// The kind of a part, as `session.resolve` names it.
pub fn part_kind(block: &ContentBlock) -> &'static str {
    match block {
        ContentBlock::Text { .. } => "text",
        ContentBlock::TextWithState { .. } => "text",
        ContentBlock::Reasoning { .. } => "reasoning",
        ContentBlock::RedactedReasoning { .. } => "redacted_reasoning",
        ContentBlock::ToolCall(_) => "tool_call",
        ContentBlock::ToolResult(_) => "tool_result",
    }
}

/// The runner's opaque key for part `index` of the message at transcript ordinal `ordinal`:
/// `m<ordinal>#<index>`, the same `<mid>#<block index>` form a consumer already composes.
pub fn part_key(ordinal: u64, index: usize) -> String {
    format!("m{ordinal}#{index}")
}

#[cfg(test)]
mod tests {
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
