//! Range reads shared by a folded session and a parked page's pinned storage reader.

use super::*;
use crate::directory::PageMemo;
use crate::session::stored::{read_block, read_layout, Place};
use crate::session::StoredLedger;

pub(super) struct SealedRange<'a, B: SessionBackend> {
    place: Place<'a, B>,
    rules: SealedConversation,
    window: u64,
    session: Option<&'a Session<B>>,
}

impl<'a, B: SessionBackend> SealedRange<'a, B> {
    pub(super) fn folded(session: &'a Session<B>) -> Self {
        Self {
            place: session.place(),
            rules: session.sealed_conversation(),
            window: session.conversation.window_start(),
            session: Some(session),
        }
    }

    fn stored(ledger: &'a StoredLedger<B>, rules: SealedConversation, window: u64) -> Self {
        Self {
            place: ledger.place(),
            rules,
            window,
            session: None,
        }
    }

    async fn layout(
        &self,
        object: &str,
        key: &crate::frame::FrameKey,
    ) -> Result<Option<Arc<SegmentLayout>>, SessionError> {
        match self.session {
            Some(session) => session.sealed_layout(object, key).await,
            None => read_layout(self.place.backend, self.place.addr, object, key)
                .await
                .map(|layout| layout.map(Arc::new)),
        }
    }

    pub(super) async fn scan(
        &self,
        from: u64,
        limit: usize,
        memo: &mut PageMemo,
    ) -> Result<(Vec<StoredEntry>, u64), SessionError> {
        let mut out = Vec::new();
        let mut next = from.max(1);
        let key = self.place.key.frame_key();
        let pages = Pages {
            backend: self.place.backend,
            addr: self.place.addr,
            session: self.place.head.session_id,
            key: &key,
            fanout: crate::directory::FANOUT,
        };
        while next < self.window && out.len() < limit {
            let sought = next;
            let damaged =
                || SessionError::Fold(format!("no sealed segment places message {sought}"));
            let entry = pages
                .find_ordinal_in(self.place.head.transcript_dir.as_ref(), next, memo)
                .await
                .map_err(SessionError::Directory)?
                .ok_or_else(damaged)?;
            let segment = self
                .layout(&entry.object, &key)
                .await?
                .ok_or_else(damaged)?;
            let mut position = segment.block_for_ordinal(next).ok_or_else(damaged)?;
            // Read on through the segment's blocks while the page wants more.
            while position < segment.index.blocks.len() && next < self.window && out.len() < limit {
                let first = segment.index.blocks[position]
                    .first_ordinal
                    .expect("block_for_ordinal only answers when every block has an ordinal");
                let mut conversation = self.rules.empty();
                conversation.placed = first - 1;
                for (_, record) in read_block(
                    self.place.backend,
                    self.place.addr,
                    &entry.object,
                    &segment,
                    position,
                    &key,
                )
                .await?
                .into_iter()
                .filter(|(seq, _)| entry.keeps(*seq))
                {
                    conversation
                        .apply(&record)
                        .map_err(|e| SessionError::Fold(format!("conversation: {e:?}")))?;
                }
                let read =
                    window_entries(&conversation, next, limit - out.len()).map_err(not_held)?;
                if let Some(last) = read.last() {
                    next = last.ordinal + 1;
                }
                out.extend(read);
                position += 1;
                // A block with only earlier messages advances to the next block.
                next = next.max(conversation.placed + 1);
            }
            if next == sought {
                return Err(damaged());
            }
        }
        Ok((out, next))
    }

    pub(super) async fn facts_ordinal(
        &self,
        seq: u64,
        mid: &str,
    ) -> Result<Option<u64>, SessionError> {
        let key = self.place.key.frame_key();
        let pages = Pages {
            backend: self.place.backend,
            addr: self.place.addr,
            session: self.place.head.session_id,
            key: &key,
            fanout: crate::directory::FANOUT,
        };
        let mut read = 0;
        for entry in pages
            .entries_from(self.place.head.transcript_dir.as_ref(), seq)
            .await
            .map_err(SessionError::Directory)?
        {
            let segment = self.layout(&entry.object, &key).await?.ok_or_else(|| {
                SessionError::Fold(format!("directory names a missing segment for {seq}"))
            })?;
            // Blocks before the one holding `seq` hold nothing at or after it.
            let start = segment.block_for_seq(seq).unwrap_or(0);
            for position in start..segment.index.blocks.len() {
                for (at, record) in read_block(
                    self.place.backend,
                    self.place.addr,
                    &entry.object,
                    &segment,
                    position,
                    &key,
                )
                .await?
                {
                    let Record::EntryFacts(facts) = record else {
                        continue;
                    };
                    if at < seq || !entry.keeps(at) {
                        continue;
                    }
                    if at == seq || facts.mid == mid {
                        return Ok(Some(facts.ordinal));
                    }
                    read += 1;
                    if read == ANNOUNCED_FACTS_READ {
                        return Ok(None);
                    }
                }
            }
        }
        Ok(None)
    }
    /// Imported tool results can live outside their assistant's block. Keep the joined turn even
    /// when its results are beyond the selected range.
    pub(super) async fn join_split_turns(
        &self,
        mut entries: Vec<StoredEntry>,
    ) -> Result<Vec<StoredEntry>, SessionError> {
        for index in 0..entries.len() {
            let entry = &entries[index];
            if entry.open
                || entry.next.is_some()
                || entry.ordinal >= self.window
                || !called_tools(&entry.message)
            {
                continue;
            }
            let ordinal = entry.ordinal + 1;
            let next = match entries
                .get(index + 1)
                .filter(|next| next.ordinal == ordinal)
            {
                Some(next) => Some(Arc::clone(&next.message)),
                None => {
                    let following = match self.session {
                        Some(session) => session.unjoined_entries(ordinal, 1).await?,
                        None if ordinal < self.window => {
                            self.scan(ordinal, 1, &mut PageMemo::default()).await?.0
                        }
                        None => {
                            return Err(SessionError::Fold(
                                "split tool turn needs the open window".into(),
                            ))
                        }
                    };
                    following
                        .into_iter()
                        .find(|next| next.ordinal == ordinal)
                        .map(|next| next.message)
                }
            };
            entries[index].next =
                next.filter(|next| next.role == broca_protocol::content::Role::Tool);
        }
        Ok(entries)
    }
}

impl<B: SessionBackend> StoredLedger<B> {
    /// Only a committed local position can reuse the closing actor's page facts.
    pub fn local_head(&self) -> Option<&crate::head::Head> {
        let head = self.place().head;
        matches!(head.residence, crate::head::Residence::Local(())).then_some(head)
    }

    /// Read a sealed ordinal range without loading the open log. `window` and `rules` must come
    /// from this pin's position. If a split tool turn needs the open window, the caller must use
    /// a full view instead of treating the results as absent.
    pub async fn sealed_entries(
        &self,
        rules: SealedConversation,
        window: u64,
        from: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, SessionError> {
        let range = SealedRange::stored(self, rules, window);
        let (entries, _) = range.scan(from, limit, &mut PageMemo::default()).await?;
        range.join_split_turns(entries).await
    }

    /// Look up a run start through the sealed ledger index, without consulting the open log.
    pub async fn sealed_run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<crate::record::RunStart>, SessionError> {
        self.place().find_indexed_run_start(run_id).await
    }

    /// Resolve a positional `m...` id or a native `g...` id (which names the record sequence
    /// that announced the message). An imported id or an open message may still resolve in
    /// the full view after a miss here.
    pub async fn sealed_message_ordinal(
        &self,
        rules: SealedConversation,
        window: u64,
        mid: &str,
    ) -> Result<Option<u64>, SessionError> {
        let range = SealedRange::stored(self, rules, window);
        let ordinal = if let Some(seq) = minted_seq(mid) {
            range.facts_ordinal(seq, mid).await?
        } else if let Some(rest) = mid.strip_prefix('m') {
            canonical_u64(rest.split_once('.').map_or(rest, |(_, digits)| digits))
                .and_then(|position| position.checked_add(1))
        } else {
            None
        };
        let Some(ordinal) = ordinal.filter(|ordinal| *ordinal < window) else {
            return Ok(None);
        };
        let (entries, _) = range.scan(ordinal, 1, &mut PageMemo::default()).await?;
        let matches = entries.first().is_some_and(|entry| {
            entry.ordinal == ordinal
                && match &entry.state.facts {
                    Some(facts) => facts.mid == mid,
                    None => mid
                        .strip_prefix('m')
                        .is_some_and(|rest| !rest.contains('.')),
                }
        });
        Ok(matches.then_some(ordinal))
    }
}
