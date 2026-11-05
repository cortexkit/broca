//! The conversation fold: the messages the model sees, rebuilt from transcript records alone.
//!
//! Transcript records are written in the order their messages take in the conversation, so the
//! fold appends them as they come, with one exception: an assistant message that calls tools is
//! held until every one of its calls has a reply. A run's budget-finalize step is the other
//! exception: its calls are never run, so its message enters at once without them. It then enters the conversation together with
//! one tool message carrying the replies in the order the assistant made the calls, which is
//! the shape every provider requires: a call is never sent without its result.
//!
//! Compaction and setup records don't change the conversation. A compaction view is applied when
//! a request is rendered, never to the conversation itself.
//!
//! Beside each window message the fold keeps what `session.entries` serves about it: its
//! `EntryFacts` record, and the text delivered with its tool calls (from `ResultAppend`, in
//! sessions that keep delivered text apart from results). An assistant message still waiting for
//! its tool results already has an ordinal reserved, one past the last placed message, and its
//! facts are kept with it until it is placed.

use broca_protocol::content::{ContentBlock, Message, Role, ToolResult};
use broca_protocol::ids::ToolCallId;

use crate::fold::Bound;
use crate::record::{EntryFacts, Marker, Notice, Placement, Record, Stream};

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
    /// Entry facts for an ordinal the conversation hasn't placed or reserved.
    FactsWithoutEntry(u64),
}

crate::message! {
    /// Text delivered with one of a message's tool calls.
    #[derive(Debug, Clone, PartialEq)]
    pub struct Delivered {
        /// The tool call's position among the message's content blocks.
        1 req part: u32,
        2 opt notice: Notice,
        /// As delivered, before any rendering.
        3 req text: String,
    }
}

crate::message! {
    /// What `session.entries` serves about one window message besides the message itself.
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct EntryState {
        /// Absent for sessions written before entry facts were recorded.
        1 opt facts: EntryFacts,
        2 rep delivered: Delivered,
    }
}

crate::message! {
    /// An assistant message that called tools, held until each call has its reply.
    #[derive(Debug, Clone)]
    struct ToolTurn {
        1 req assistant: Message,
        /// The calls in the order the assistant made them.
        2 rep calls: ToolCallId,
        /// The replies in the order they arrived. A turn holds one step's calls, so finding
        /// one by its call id is a short scan.
        3 rep replies: ToolResult,
    }
}

state! {
    /// The conversation as the model sees it.
    #[derive(Debug, Clone)]
    pub struct Conversation {
        /// The messages in conversation order. An assistant message whose calls aren't all
        /// answered isn't here yet. In a stored session this starts at the open log's
        /// checkpoint, so it holds the active window, never the whole history.
        1 rep pub messages: Message => {
            owner: "run loop",
            reset: "never",
            bound: Bound::Window,
        },
        2 opt open: ToolTurn => {
            owner: "run loop",
            reset: "tool replies",
            bound: Bound::OpenWork("one assistant message's tool calls"),
        },
        /// Whether text delivered with a tool result becomes part of that result in the
        /// conversation. Sessions on the first compaction protocol stored it that way; sessions
        /// on the current contract keep the result as returned and add the text when a request
        /// renders.
        3 req pub appends_in_conversation: bool => {
            owner: "admission",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// The latest compaction marker: where the provider's cut falls. Requests render from
        /// its first kept entry.
        4 opt pub marker: Marker => {
            owner: "compaction",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// How many messages the conversation has ever placed, the window's included. A message's
        /// ordinal is its place in the whole conversation, counting from 1, so the window's first
        /// message has ordinal `placed - messages.len() + 1`, and older ones are in sealed
        /// segments.
        5 req pub placed: u64 => {
            owner: "run loop",
            reset: "never",
            bound: Bound::Fixed,
        },
        /// One per window message, in the same order.
        6 rep pub entries: EntryState => {
            owner: "run loop",
            reset: "never",
            bound: Bound::Window,
        },
        /// The held assistant message's entry, at its reserved ordinal.
        7 opt pub open_entry: EntryState => {
            owner: "run loop",
            reset: "tool replies",
            bound: Bound::OpenWork("one assistant message's tool calls"),
        },
        /// A revert into sealed history left no window: the messages before the new run are in
        /// sealed segments only. Reads serve them from there, and nothing may render until a
        /// window is copied back in, after the compaction provider answers an `over_limit`
        /// notice (not built yet).
        8 req pub window_pending: bool => {
            owner: "revert",
            reset: "a window copied back in",
            bound: Bound::Fixed,
        },
    }
}

impl Conversation {
    pub fn new(appends_in_conversation: bool) -> Self {
        Self {
            messages: Vec::new(),
            open: None,
            appends_in_conversation,
            marker: None,
            placed: 0,
            entries: Vec::new(),
            open_entry: None,
            window_pending: false,
        }
    }

    /// Empties the window, keeping the count of placed messages and any held tool turn. A
    /// checkpoint carries no window: it's rebuilt from the log's own records.
    pub fn clear_window(&mut self) {
        self.messages.clear();
        self.entries.clear();
    }

    /// The window message with `ordinal` and its entry state.
    pub fn window_entry(&self, ordinal: u64) -> Option<(&Message, &EntryState)> {
        let start = self.window_start();
        if ordinal < start {
            return None;
        }
        let at = (ordinal - start) as usize;
        self.messages.get(at).zip(self.entries.get(at))
    }

    /// The held assistant message, its reserved ordinal and its entry state.
    pub fn open_entry(&self) -> Option<(u64, &Message, &EntryState)> {
        let turn = self.open.as_ref()?;
        let entry = self.open_entry.as_ref()?;
        Some((self.placed + 1, &turn.assistant, entry))
    }

    /// The ordinal of the window's first message, or the next ordinal when the window is empty.
    pub fn window_start(&self) -> u64 {
        self.placed - self.messages.len() as u64 + 1
    }

    /// The window message with `ordinal`, if it's in the window.
    pub fn window_message(&self, ordinal: u64) -> Option<&Message> {
        let start = self.window_start();
        (ordinal >= start)
            .then(|| self.messages.get((ordinal - start) as usize))
            .flatten()
    }

    fn place(&mut self, message: Message, entry: EntryState) {
        self.messages.push(message);
        self.entries.push(entry);
        self.placed += 1;
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
                    self.open_entry = None;
                }
                self.place(input.message.clone(), EntryState::default());
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
                if assistant.budget_finalize == Some(true) {
                    // A run's budget-finalize step is its last, and the calls a provider emits in
                    // it anyway are never run, so they have no results. Its message enters at
                    // once without them, keeping its text.
                    let mut message = assistant.message.clone();
                    message
                        .content
                        .retain(|block| !matches!(block, ContentBlock::ToolCall(_)));
                    self.place(message, EntryState::default());
                } else if calls.is_empty() {
                    self.place(assistant.message.clone(), EntryState::default());
                } else {
                    self.open = Some(ToolTurn {
                        assistant: assistant.message.clone(),
                        calls,
                        replies: Vec::new(),
                    });
                    self.open_entry = Some(EntryState::default());
                }
            }
            Record::ToolReply(reply) => {
                let id = &reply.result.tool_call_id;
                let Some(turn) = self.open.as_mut().filter(|turn| turn.calls.contains(id)) else {
                    return Err(FoldError::ReplyWithoutCall(id.0.clone()));
                };
                if turn.replies.iter().any(|known| &known.tool_call_id == id) {
                    return Err(FoldError::SecondReply(id.0.clone()));
                }
                turn.replies.push(reply.result.clone());
                if turn.replies.len() == turn.calls.len() {
                    let mut turn = self.open.take().expect("checked above");
                    let results = turn
                        .calls
                        .iter()
                        .map(|id| {
                            let at = turn
                                .replies
                                .iter()
                                .position(|reply| &reply.tool_call_id == id)
                                .expect("every call has a reply");
                            ContentBlock::ToolResult(turn.replies.swap_remove(at))
                        })
                        .collect();
                    let entry = self.open_entry.take().unwrap_or_default();
                    self.place(turn.assistant, entry);
                    self.place(Message::new(Role::Tool, results), EntryState::default());
                }
            }
            Record::ResultAppend(append) => {
                if self.appends_in_conversation {
                    append_to_newest_result(&mut self.messages, &append.tool_call_id, &append.text);
                } else {
                    self.attach_delivery(append);
                }
            }
            Record::EntryFacts(facts) => {
                let start = self.window_start();
                if self.open.is_some() && facts.ordinal == self.placed + 1 {
                    self.open_entry
                        .get_or_insert_with(EntryState::default)
                        .facts = Some(facts.clone());
                } else if facts.ordinal >= start && facts.ordinal <= self.placed {
                    self.entries[(facts.ordinal - start) as usize].facts = Some(facts.clone());
                } else if facts.ordinal > self.placed {
                    return Err(FoldError::FactsWithoutEntry(facts.ordinal));
                }
                // Facts for a message below the window have nothing to attach to here, as the
                // message itself isn't held: a fold that keeps only counts drops both.
            }
            Record::Marker(marker) => self.marker = Some(marker.clone()),
            Record::SetupStub(_) | Record::Compaction(_) => {}
            _ => unreachable!("every transcript kind is matched above"),
        }
        Ok(())
    }
}

impl Conversation {
    /// Attaches delivered text to the newest window message that made the call. Delivered text is
    /// only ever attached to a result of a batch that already finished, so an unknown call is
    /// one whose message left the window, and the text goes with it.
    fn attach_delivery(&mut self, append: &crate::record::ResultAppend) {
        for (message, entry) in self.messages.iter().zip(&mut self.entries).rev() {
            let part = message.content.iter().position(|block| {
                matches!(block, ContentBlock::ToolCall(call) if call.tool_call_id == append.tool_call_id)
            });
            if let Some(part) = part {
                entry.delivered.push(Delivered {
                    part: part as u32,
                    notice: append.notice.clone(),
                    text: append.text.clone(),
                });
                return;
            }
        }
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
