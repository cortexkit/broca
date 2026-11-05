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
//! Entries (`session.entries`) are read the same way, a block at a time: each message comes with
//! its entry state (see `fold::conversation`), and an assistant message that called tools comes
//! with the tool message carrying its results. A sealed block always holds both, since a block
//! never splits an assistant message from its results. An assistant message still waiting for its tool results is
//! served last, at its reserved ordinal.
//!
//! Also here: finding the ledger records that name an id (see `idindex`).

use std::collections::BTreeMap;

use broca_protocol::content::Message;

use crate::backend::SessionBackend;
use crate::directory::Pages;
use crate::fold::conversation::{Conversation, EntryState};
use crate::idindex::{self, IdKind, IndexPages, IndexRoot};
use crate::record::Record;
use crate::segment::Segment;

use super::{Session, SessionError};

/// One message as `session.entries` reads it.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredEntry {
    pub ordinal: u64,
    pub message: Message,
    /// For an assistant message that called tools: the tool message with their results.
    pub next: Option<Message>,
    pub state: EntryState,
    /// An assistant message still waiting for its tool results, at its reserved ordinal.
    pub open: bool,
}

/// A decimal number as an id writes it: no sign, no leading zero, no overflow.
fn canonical_u64(digits: &str) -> Option<u64> {
    let canonical = !digits.is_empty()
        && (digits.len() == 1 || !digits.starts_with('0'))
        && digits.bytes().all(|byte| byte.is_ascii_digit());
    canonical.then(|| digits.parse().ok()).flatten()
}

/// The entries of `conversation`'s window from `from` on, at most `limit` of them.
fn window_entries(conversation: &Conversation, from: u64, limit: usize) -> Vec<StoredEntry> {
    let start = conversation.window_start().max(from);
    (start..=conversation.placed)
        .take(limit)
        .filter_map(|ordinal| {
            let (message, state) = conversation.window_entry(ordinal)?;
            let called = message.role == broca_protocol::content::Role::Assistant
                && message.content.iter().any(|block| {
                    matches!(block, broca_protocol::content::ContentBlock::ToolCall(_))
                });
            let next = conversation
                .window_message(ordinal + 1)
                .filter(|next| called && next.role == broca_protocol::content::Role::Tool);
            Some(StoredEntry {
                ordinal,
                message: message.clone(),
                next: next.cloned(),
                state: state.clone(),
                open: false,
            })
        })
        .collect()
}

impl<B: SessionBackend> Session<'_, B> {
    /// Up to `limit` entries from ordinal `from` on (counting from 1), in order, over the whole
    /// conversation: sealed history a block at a time, then the window, then the held assistant
    /// message, if any.
    pub async fn entries(&self, from: u64, limit: usize) -> Result<Vec<StoredEntry>, SessionError> {
        let mut out = Vec::new();
        let mut next = from.max(1);
        let key = self.key.frame_key();
        let pages = Pages {
            backend: self.backend,
            addr: &self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: crate::directory::FANOUT,
        };
        let window = self.conversation.window_start();
        while next < window && out.len() < limit {
            let damaged = || SessionError::Fold(format!("no sealed segment places message {next}"));
            let entry = pages
                .find_ordinal(self.head.transcript_dir.as_ref(), next)
                .await
                .map_err(SessionError::Directory)?
                .ok_or_else(damaged)?;
            let bytes = self
                .backend
                .get_object(&self.addr, &entry.object)
                .await?
                .ok_or_else(damaged)?;
            let segment = Segment::open(&bytes, &key, entry.object.as_bytes())
                .map_err(SessionError::Segment)?;
            let mut position = segment.block_for_ordinal(next).ok_or_else(damaged)?;
            // Read on through the segment's blocks while the page wants more.
            while position < segment.index.blocks.len() && next < window && out.len() < limit {
                let first = segment.index.blocks[position]
                    .first_ordinal
                    .expect("block_for_ordinal only answers when every block has an ordinal");
                let mut conversation = Conversation::new(self.conversation.appends_in_conversation);
                conversation.placed = first - 1;
                for (_, record) in segment
                    .block(position)
                    .map_err(SessionError::Segment)?
                    .into_iter()
                    .filter(|(seq, _)| entry.keeps(*seq))
                {
                    conversation
                        .apply(&record)
                        .map_err(|e| SessionError::Fold(format!("conversation: {e:?}")))?;
                }
                let wanted = limit - out.len();
                let read = window_entries(&conversation, next, wanted);
                if let Some(last) = read.last() {
                    next = last.ordinal + 1;
                }
                out.extend(read);
                position += 1;
                // A block that places nothing past `next` (its messages are all earlier) moves
                // on to the next block.
                next = next.max(conversation.placed + 1);
            }
        }
        if out.len() < limit {
            out.extend(window_entries(&self.conversation, next, limit - out.len()));
        }
        if out.len() < limit {
            if let Some((ordinal, message, state)) = self.conversation.open_entry() {
                if ordinal >= next {
                    out.push(StoredEntry {
                        ordinal,
                        message: message.clone(),
                        next: None,
                        state: state.clone(),
                        open: true,
                    });
                }
            }
        }
        Ok(out)
    }

    /// The ordinal of the message with id `mid`, if it's on the current branch. The id tells
    /// where to look: a new id names the seq of its `EntryFacts` record, a converted one its
    /// position in today's history. Either way, the entry found there must carry exactly that
    /// id, so an id from an abandoned branch, or from another session, never resolves to the
    /// message that took its place. Messages converted from a session without entry facts have
    /// none, and their `m<position>` ids resolve by position alone: no message written since
    /// lacks facts, so nothing else can sit at a converted message's ordinal.
    pub async fn find_message(&self, mid: &str) -> Result<Option<u64>, SessionError> {
        let candidate = if let Some(rest) = mid.strip_prefix('g') {
            let Some(seq) = rest.split_once('.').and_then(|(_, seq)| canonical_u64(seq)) else {
                return Ok(None);
            };
            self.facts_ordinal(seq).await?
        } else if let Some(rest) = mid.strip_prefix('m') {
            let digits = rest.split_once('.').map_or(rest, |(_, digits)| digits);
            canonical_u64(digits).and_then(|position| position.checked_add(1))
        } else {
            None
        };
        let Some(ordinal) = candidate else {
            return Ok(None);
        };
        let Some(entry) = self.entries(ordinal, 1).await?.into_iter().next() else {
            return Ok(None);
        };
        let matches = match &entry.state.facts {
            Some(facts) => facts.mid == mid,
            None => mid
                .strip_prefix('m')
                .is_some_and(|rest| !rest.contains('.')),
        };
        Ok((entry.ordinal == ordinal && matches).then_some(ordinal))
    }

    /// The ordinal an `EntryFacts` record at `seq` names, from the window or a sealed segment.
    async fn facts_ordinal(&self, seq: u64) -> Result<Option<u64>, SessionError> {
        if let Some(ordinal) = self.window_facts.get(&seq) {
            return Ok(Some(*ordinal));
        }
        let key = self.key.frame_key();
        let pages = Pages {
            backend: self.backend,
            addr: &self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: crate::directory::FANOUT,
        };
        let Some(entry) = pages
            .find(self.head.transcript_dir.as_ref(), seq)
            .await
            .map_err(SessionError::Directory)?
        else {
            return Ok(None);
        };
        let damaged = || SessionError::Fold(format!("directory names a missing segment for {seq}"));
        let bytes = self
            .backend
            .get_object(&self.addr, &entry.object)
            .await?
            .ok_or_else(damaged)?;
        let segment =
            Segment::open(&bytes, &key, entry.object.as_bytes()).map_err(SessionError::Segment)?;
        Ok(match segment.find(seq).map_err(SessionError::Segment)? {
            Some(Record::EntryFacts(facts)) => Some(facts.ordinal),
            _ => None,
        })
    }

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
        for (_, record) in segment
            .block(position)
            .map_err(SessionError::Segment)?
            .into_iter()
            .filter(|(seq, _)| entry.keeps(*seq))
        {
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
