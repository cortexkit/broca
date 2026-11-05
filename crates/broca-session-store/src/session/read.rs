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
//!
//! Also here: finding the ledger records that name an id (see `idindex`).

use std::collections::BTreeMap;

use broca_protocol::content::Message;

use crate::backend::SessionBackend;
use crate::directory::Pages;
use crate::fold::conversation::Conversation;
use crate::idindex::{self, IdKind, IndexPages, IndexRoot};
use crate::record::Record;
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

    /// Every ledger record that names `id` as a `kind`, sealed or in the open log, in seq
    /// order. A sealed record is found through the id index, then read from its segment and
    /// checked to name the id itself, so two ids whose hashes collide never answer for each
    /// other. A seq the index names that no segment holds is damage, never "not found".
    pub async fn find_id(
        &self,
        kind: IdKind,
        id: &str,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        let mut found = self.open_ids.find(kind, id);
        if let Some(root_name) = &self.head.id_index {
            let key = self.key.frame_key();
            let index = IndexPages {
                backend: self.backend,
                addr: &self.addr,
                session: self.head.session_id,
                key: &key,
            };
            let root: IndexRoot = index.read(root_name).await.map_err(SessionError::Index)?;
            let hash = idindex::id_hash(&idindex::hash_key(&self.key), kind, id);
            let seqs = index
                .lookup(&root, &hash)
                .await
                .map_err(SessionError::Index)?;
            for (seq, record) in self.ledger_records(&seqs).await? {
                if idindex::record_ids(&record)
                    .iter()
                    .any(|(named_kind, named)| *named_kind == kind && named == id)
                {
                    found.push((seq, record));
                }
            }
        }
        found.sort_by_key(|(seq, _)| *seq);
        Ok(found)
    }

    /// The sealed ledger records with `seqs`, each read from the one block that holds it.
    async fn ledger_records(&self, seqs: &[u64]) -> Result<Vec<(u64, Record)>, SessionError> {
        let key = self.key.frame_key();
        let pages = Pages {
            backend: self.backend,
            addr: &self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: crate::directory::FANOUT,
        };
        let damaged = |seq: u64| SessionError::Fold(format!("no sealed ledger record {seq}"));
        // Group by segment, so each segment is fetched once.
        let mut by_object: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        for &seq in seqs {
            let entry = pages
                .find(self.head.ledger_dir.as_ref(), seq)
                .await
                .map_err(SessionError::Directory)?
                .ok_or_else(|| damaged(seq))?;
            by_object.entry(entry.object).or_default().push(seq);
        }
        let mut records = Vec::new();
        for (object, seqs) in by_object {
            let bytes = self
                .backend
                .get_object(&self.addr, &object)
                .await?
                .ok_or_else(|| damaged(seqs[0]))?;
            let segment =
                Segment::open(&bytes, &key, object.as_bytes()).map_err(SessionError::Segment)?;
            for seq in seqs {
                let record = segment
                    .find(seq)
                    .map_err(SessionError::Segment)?
                    .ok_or_else(|| damaged(seq))?;
                records.push((seq, record));
            }
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests;
