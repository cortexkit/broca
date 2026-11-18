//! Releasing window content from memory (see `Conversation::release`), in every holder the
//! session has: the window message, and the kept records that placed it, which share its content.

use std::sync::Arc;

use broca_protocol::{ContentBlock, Message};

use crate::backend::SessionBackend;
use crate::record::Record;

use super::Session;

impl<B: SessionBackend> Session<B> {
    /// Releases from memory the content of the blocks `blocks` names, each by its message's
    /// window ordinal and its block indexes: in the window message, and in each kept record
    /// that shares that content. A block of the window's newest message, or of a message whose
    /// records aren't known, stays held. Released content reads back from the open log
    /// ([`Session::window_message_whole`]).
    pub fn release(&mut self, blocks: &[(u64, Vec<usize>)]) {
        for (ordinal, indexes) in blocks {
            let Some(before) = self
                .conversation
                .window_message(*ordinal)
                .and_then(|message| message.held().cloned())
            else {
                continue;
            };
            if !self.conversation.release(*ordinal, indexes) {
                continue;
            }
            let sources = self
                .conversation
                .window_message(*ordinal)
                .map(|message| message.sources().to_vec())
                .unwrap_or_default();
            for seq in sources {
                let released = self
                    .kept
                    .get_any(seq)
                    .and_then(|record| released_record(record, &before, indexes));
                if let Some(released) = released {
                    self.kept.release(seq, released);
                }
            }
        }
    }
}

/// `record`, which placed some of the window message `window` (as it was held), with the content
/// of the window's blocks at `indexes` in their released form; `None` when the record holds none
/// of that content. An input or an assistant message is the window message itself, the same
/// allocation; a tool reply holds the one result block of the window's tool message that answers
/// its call.
fn released_record(record: &Record, window: &Arc<Message>, indexes: &[usize]) -> Option<Record> {
    match record {
        Record::Input(input) if Arc::ptr_eq(&input.message, window) => {
            let mut input = input.clone();
            input.message = released_message(window, indexes)?;
            Some(Record::Input(input))
        }
        Record::Assistant(assistant) if Arc::ptr_eq(&assistant.message, window) => {
            let mut assistant = assistant.clone();
            assistant.message = released_message(window, indexes)?;
            Some(Record::Assistant(assistant))
        }
        Record::ToolReply(reply) => {
            let answers = indexes.iter().any(|&index| {
                matches!(
                    window.content.get(index),
                    Some(ContentBlock::ToolResult(result))
                        if result.tool_call_id == reply.result.tool_call_id
                )
            });
            if !answers {
                return None;
            }
            let ContentBlock::ToolResult(released) =
                ContentBlock::ToolResult(reply.result.clone()).released()?
            else {
                return None;
            };
            let mut reply = reply.clone();
            reply.result = released;
            Some(Record::ToolReply(reply))
        }
        _ => None,
    }
}

/// `message` with its blocks at `indexes` in their released form, or `None` when none of them
/// holds content to release.
fn released_message(message: &Message, indexes: &[usize]) -> Option<Arc<Message>> {
    let mut released = message.clone();
    let mut any = false;
    for &index in indexes {
        if let Some(block) = released.content.get_mut(index) {
            if let Some(form) = block.released() {
                *block = form;
                any = true;
            }
        }
    }
    any.then(|| Arc::new(released))
}
