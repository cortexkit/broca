//! The conversation fold: the messages the model sees, rebuilt from transcript records alone.
//!
//! Transcript records are written in the order their messages take in the conversation, so the
//! fold appends them as they come, with one exception: an assistant message that calls tools is
//! held until every one of its calls has a reply. It then enters the conversation together with
//! one tool message carrying the replies in the order the assistant made the calls, which is
//! the shape every provider requires: a call is never sent without its result.
//!
//! Compaction and setup records don't change the conversation. A compaction view is applied when
//! a request is rendered, never to the conversation itself.

use std::collections::HashMap;

use broca_protocol::content::{ContentBlock, Message, Role, ToolResult};
use broca_protocol::ids::ToolCallId;

use crate::record::{Placement, Record, Stream};

/// Why a record can't be folded into the conversation. Each one means the records don't
/// describe a conversation a provider would accept, so the fold refuses rather than guess.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FoldError {
    /// A ledger record was handed to the conversation fold.
    LedgerRecord(&'static str),
    /// A reply for a call no held assistant message made.
    ReplyWithoutCall(String),
    /// A second reply for a call that already has one.
    SecondReply(String),
    /// A message other than a run's own input arrived while an assistant message was still
    /// waiting for replies, where it would land between a call and its result.
    InputInsideToolTurn,
    /// An assistant message arrived while another was still waiting for replies.
    AssistantInsideToolTurn,
}

/// An assistant message that called tools, held until each call has its reply.
#[derive(Debug, Clone)]
struct ToolTurn {
    assistant: Message,
    /// The calls in the order the assistant made them.
    calls: Vec<ToolCallId>,
    replies: HashMap<ToolCallId, ToolResult>,
}

/// The conversation as the model sees it.
#[derive(Debug, Clone)]
pub struct Conversation {
    /// The messages in conversation order. An assistant message whose calls aren't all answered
    /// isn't here yet.
    pub messages: Vec<Message>,
    open: Option<ToolTurn>,
    /// Whether text delivered with a tool result becomes part of that result in the
    /// conversation. Sessions on the first compaction protocol stored it that way; sessions on
    /// the current contract keep the result as returned and add the text when a request renders.
    appends_in_conversation: bool,
}

impl Conversation {
    pub fn new(appends_in_conversation: bool) -> Self {
        Self {
            messages: Vec::new(),
            open: None,
            appends_in_conversation,
        }
    }

    /// The assistant message still waiting for replies, if any.
    pub fn open_tool_turn(&self) -> Option<&Message> {
        self.open.as_ref().map(|turn| &turn.assistant)
    }

    pub fn apply(&mut self, record: &Record) -> Result<(), FoldError> {
        if record.stream() != Stream::Transcript {
            return Err(FoldError::LedgerRecord(record.name()));
        }
        match record {
            Record::Input(input) => {
                if self.open.is_some() {
                    // A new run's input ends a turn its interrupted run left unanswered: that
                    // turn never reached the conversation and never will. Any other input there
                    // would split a call from its result.
                    if input.placement != Placement::RunInput {
                        return Err(FoldError::InputInsideToolTurn);
                    }
                    self.open = None;
                }
                self.messages.push(input.message.clone());
            }
            Record::Assistant(assistant) => {
                if self.open.is_some() {
                    return Err(FoldError::AssistantInsideToolTurn);
                }
                let calls: Vec<ToolCallId> = assistant
                    .message
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        ContentBlock::ToolCall(call) => Some(call.tool_call_id.clone()),
                        _ => None,
                    })
                    .collect();
                if calls.is_empty() {
                    self.messages.push(assistant.message.clone());
                } else {
                    self.open = Some(ToolTurn {
                        assistant: assistant.message.clone(),
                        calls,
                        replies: HashMap::new(),
                    });
                }
            }
            Record::ToolReply(reply) => {
                let id = &reply.result.tool_call_id;
                let Some(turn) = self.open.as_mut().filter(|turn| turn.calls.contains(id)) else {
                    return Err(FoldError::ReplyWithoutCall(id.0.clone()));
                };
                if turn
                    .replies
                    .insert(id.clone(), reply.result.clone())
                    .is_some()
                {
                    return Err(FoldError::SecondReply(id.0.clone()));
                }
                if turn.replies.len() == turn.calls.len() {
                    let mut turn = self.open.take().expect("checked above");
                    let results = turn
                        .calls
                        .iter()
                        .map(|id| {
                            ContentBlock::ToolResult(
                                turn.replies.remove(id).expect("every call has a reply"),
                            )
                        })
                        .collect();
                    self.messages.push(turn.assistant);
                    self.messages.push(Message::new(Role::Tool, results));
                }
            }
            Record::ResultAppend(append) => {
                if self.appends_in_conversation {
                    append_to_newest_result(&mut self.messages, &append.tool_call_id, &append.text);
                }
            }
            Record::SetupStub(_) | Record::Compaction(_) => {}
            _ => unreachable!("every transcript kind is matched above"),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

/// Appends `text` to the named result in the newest tool message, if it holds that result.
/// Delivered text is only ever attached to a result of the batch that just finished.
fn append_to_newest_result(messages: &mut [Message], id: &ToolCallId, text: &str) {
    use broca_protocol::content::{ToolOutput, ToolOutputPart};
    let Some(message) = messages.iter_mut().rev().find(|m| m.role == Role::Tool) else {
        return;
    };
    for block in &mut message.content {
        if let ContentBlock::ToolResult(result) = block {
            if &result.tool_call_id == id {
                match &mut result.output {
                    ToolOutput::Text { text: output } => output.push_str(text),
                    ToolOutput::Error { message, .. } => message.push_str(text),
                    ToolOutput::Parts { parts } => parts.push(ToolOutputPart::Text {
                        text: text.to_owned(),
                    }),
                    ToolOutput::Json { .. } | ToolOutput::Denied { .. } => {}
                }
                return;
            }
        }
    }
}
