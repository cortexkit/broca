//! Runner-owned tags: a durable, per-session number for every message part broca stores.
//!
//! A tag is assigned when broca writes the record that carries the part, and it is stored in
//! that record. Numbers are strictly increasing in WAL order and never reused within a session,
//! across episodes, resumes, restarts, retention deletion and agent-data moves. The `§N§`
//! spelling ([`tag_mark`]) is part of the llm-runner contract.
//!
//! Tags are metadata, never message content: a message's stored bytes, its serialized form and
//! its equality ignore them. Only a run frozen with `render_tags` shows them to the model, by
//! prefixing user and tool-result text in the rendered request (see [`render_tags_into`]).

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

/// A message's tags as it is held in memory. Never serialized: the WAL keeps tags on the
/// records that carry the parts, and the store keeps them beside each message row.
///
/// Equality always holds, so two messages with the same content compare equal whether or not
/// their tags were attached. Tags are not content, and every equality check on messages in
/// this workspace is a check of content.
#[derive(Debug, Clone, Default)]
pub struct TagSlot {
    tags: Option<Arc<MessageTags>>,
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
    pub fn new(tags: Option<Arc<MessageTags>>) -> Self {
        Self {
            tags,
            rendered: false,
        }
    }

    pub fn get(&self) -> Option<&MessageTags> {
        self.tags.as_deref()
    }

    pub fn shared(&self) -> Option<Arc<MessageTags>> {
        self.tags.clone()
    }

    pub fn is_rendered(&self) -> bool {
        self.rendered
    }
}

/// Whether assistant text blocks may carry a rendered tag. Off: rendering into assistant
/// parts waits on a live check of how Anthropic treats edited assistant text, and it must
/// never touch signed thinking, tool-call arguments or any block carrying provider state.
pub const RENDER_ASSISTANT_TAGS: bool = false;

/// Prefix `§N§ ` to every tagged user text block and every tagged tool-result text output in
/// `messages`, in place. Used only to build the model's view of a run frozen with
/// `render_tags`; stored records are never changed.
///
/// Untouched: assistant messages (see [`RENDER_ASSISTANT_TAGS`]), system messages, blocks with
/// provider state, tool-result JSON, denial and error outputs, and any message without tags.
/// A message is rendered at most once, so calling this again over a prompt that already holds
/// rendered messages changes nothing. Its output depends only on the stored text and its tag,
/// so every later render of the part is byte-identical.
pub fn render_tags_into(messages: &mut [Message]) {
    for message in messages {
        render_message_tags(message);
    }
}

fn render_message_tags(message: &mut Message) {
    if message.tags.rendered {
        return;
    }
    let Some(tags) = message.tags.tags.clone() else {
        return;
    };
    message.tags.rendered = true;
    let role = message.role;
    for (block, tag) in message.content.iter_mut().zip(tags.parts.iter()) {
        match (role, block) {
            (Role::User, ContentBlock::Text { text }) => prefix(text, *tag),
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
        message.set_tags(Some(Arc::new(MessageTags::new(parts))));
        message
    }

    #[test]
    fn rendering_prefixes_user_and_tool_text_once_and_never_assistant_parts() {
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
        render_tags_into(&mut prompt);
        let once = format!("{prompt:?}");
        render_tags_into(&mut prompt);
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
