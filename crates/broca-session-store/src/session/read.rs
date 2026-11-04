//! Reading a message by its ordinal, wherever it lives.
//!
//! A message in the window is served from memory. An older one is in a sealed transcript
//! segment: the directory finds the segment by ordinal, the segment's index finds the block, and
//! the block's records are folded from an empty conversation to rebuild its messages. A block
//! always starts a group (a user input or an assistant message with its tool results), so no tool
//! turn is open at its start and the block folds on its own. So an expand costs the directory's
//! pages, the segment's index and one block, whatever the session's age.
//!
//! A sealed message of a finished run has its opaque reasoning pruned (see `prune`); everything
//! else is as it was stored.

use broca_protocol::content::Message;

use crate::backend::SessionBackend;
use crate::directory::Pages;
use crate::fold::conversation::Conversation;
use crate::segment::Segment;

use super::{Session, SessionError};

impl<B: SessionBackend> Session<'_, B> {
    /// The message with `ordinal` (counting from 1 over the whole conversation), or `None` when
    /// the conversation has placed no such message.
    pub async fn message(&self, ordinal: u64) -> Result<Option<Message>, SessionError> {
        if ordinal == 0 || ordinal > self.conversation.placed {
            return Ok(None);
        }
        if let Some(message) = self.conversation.window_message(ordinal) {
            return Ok(Some(message.clone()));
        }
        let key = self.key.frame_key();
        let pages = Pages {
            backend: self.backend,
            addr: &self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: crate::directory::FANOUT,
        };
        // Below the window, every placed message is in a sealed segment, so a miss is damage.
        let damaged = || SessionError::Fold(format!("no sealed segment places message {ordinal}"));
        let entry = pages
            .find_ordinal(self.head.transcript_dir.as_ref(), ordinal)
            .await
            .map_err(SessionError::Directory)?
            .ok_or_else(damaged)?;
        let bytes = self
            .backend
            .get_object(&self.addr, &entry.object)
            .await?
            .ok_or_else(damaged)?;
        let segment =
            Segment::open(&bytes, &key, entry.object.as_bytes()).map_err(SessionError::Segment)?;
        let position = segment.block_for_ordinal(ordinal).ok_or_else(damaged)?;
        let first = segment.index.blocks[position]
            .first_ordinal
            .expect("block_for_ordinal only answers when every block has an ordinal");
        let mut conversation = Conversation::new(self.conversation.appends_in_conversation);
        conversation.placed = first - 1;
        for (_, record) in segment.block(position).map_err(SessionError::Segment)? {
            conversation
                .apply(&record)
                .map_err(|e| SessionError::Fold(format!("conversation: {e:?}")))?;
        }
        conversation
            .window_message(ordinal)
            .cloned()
            .map(Some)
            .ok_or_else(damaged)
    }
}

#[cfg(test)]
mod tests;
