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
pub(super) fn canonical_u64(digits: &str) -> Option<u64> {
    let canonical = !digits.is_empty()
        && (digits.len() == 1 || !digits.starts_with('0'))
        && digits.bytes().all(|byte| byte.is_ascii_digit());
    canonical.then(|| digits.parse().ok()).flatten()
}

/// The seq an id broca minted names (`g<generation>.<seq>`), or `None` for any other id.
pub(super) fn minted_seq(mid: &str) -> Option<u64> {
    mid.strip_prefix('g')?
        .split_once('.')
        .and_then(|(_, seq)| canonical_u64(seq))
}

/// How many `EntryFacts` records after a step's or a batch's start a lookup of the message it
/// announced reads before giving up. The message's facts are normally the first after it; a
/// retried step's are later, but a paused or refused step places nothing before its retry.
const ANNOUNCED_FACTS_READ: usize = 16;

/// The window's entries as `session.entries` serves them: the placed messages, then the
/// assistant message whose tool calls are still running, at the ordinal reserved for it.
pub fn served_window(conversation: &Conversation) -> Vec<StoredEntry> {
    let mut entries = window_entries(conversation, 1, usize::MAX);
    if let Some((ordinal, message, state)) = conversation.open_entry() {
        entries.push(StoredEntry {
            ordinal,
            message: message.clone(),
            next: None,
            state: state.clone(),
            open: true,
        });
    }
    entries
}

/// The entries of `conversation`'s window from `from` on, at most `limit` of them: one per
/// window message, so a request's prompt and its entries line up.
pub fn window_entries(conversation: &Conversation, from: u64, limit: usize) -> Vec<StoredEntry> {
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

/// What a session keeps of its sealed history while it's open, with the history version it was
/// read under. Sealed history never changes, except that a revert to a sealed message cuts the
/// history after it and moves the history version: everything read under an older version is
/// dropped together.
#[derive(Default)]
pub(super) struct SealedCaches {
    version: u64,
    /// Sealed entries read by message id (see `entry_by_mid`).
    entries: std::collections::HashMap<String, StoredEntry>,
    /// A value a reader derived from sealed history and asked the session to keep (see
    /// `keep_sealed_derived`).
    derived: Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>,
}

pub(super) type SealedEntries = std::sync::Mutex<SealedCaches>;

/// How far apart two named sealed entries may be and still be read in one pass: reading the
/// entries between them decodes blocks that one read per entry would decode anyway.
const READ_TOGETHER: u64 = 64;

impl<B: SessionBackend> Session<B> {
    /// An empty conversation that folds a sealed block of this session's history as the session
    /// folded it: joining delivered text into results exactly where the session does.
    fn block_conversation(&self) -> Conversation {
        let mut conversation = Conversation::new(self.conversation.appends_in_conversation);
        conversation.enrolled(self.operations.enrolled_through());
        conversation
    }

    /// The sealed caches, emptied first when the history version moved since they were filled.
    fn sealed_caches(&self) -> std::sync::MutexGuard<'_, SealedCaches> {
        let version = self.operations.history_version;
        let mut caches = self.sealed_entries.lock().expect("sealed caches");
        if caches.version != version {
            *caches = SealedCaches {
                version,
                ..SealedCaches::default()
            };
        }
        caches
    }

    /// The entry whose message id is `mid`, wherever it is: in the window, or read back from
    /// sealed history; `None` when no entry of the session has that id. The id is found as
    /// [`Session::find_message`] finds it, so an import's source id is found too. An entry read
    /// from sealed history is kept while the session is open, so asking for it again reads
    /// nothing. Entries are served as compaction reads them ([`Session::compaction_entries`]): a
    /// message an enrollment suppressed has none.
    pub async fn entry_by_mid(&self, mid: &str) -> Result<Option<StoredEntry>, SessionError> {
        if let Some(entry) = self.sealed_caches().entries.get(mid) {
            return Ok(Some(entry.clone()));
        }
        let version = self.operations.history_version;
        let Some(ordinal) = self.find_message(mid).await? else {
            return Ok(None);
        };
        let Some(entry) = self
            .compaction_entries(ordinal, 1)
            .await?
            .into_iter()
            .next()
            .filter(|entry| entry.ordinal == ordinal)
        else {
            return Ok(None);
        };
        if ordinal < self.conversation.window_start() {
            let mut caches = self.sealed_caches();
            if caches.version == version {
                caches.entries.insert(mid.to_owned(), entry.clone());
            }
        }
        Ok(Some(entry))
    }

    /// The entries whose message ids `mids` lists, in ordinal order, each once; an id no entry
    /// has is left out. The ids are found as [`Session::find_message`] finds them, and the
    /// entries are read together: named entries close to each other are read in one pass over
    /// the history between them, not one read each. Nothing is kept: a caller that reads the
    /// same entries at every step keeps what it derives from them (see `keep_sealed_derived`).
    /// Entries are served as compaction reads them ([`Session::compaction_entries`]).
    pub async fn entries_by_mid(&self, mids: &[String]) -> Result<Vec<StoredEntry>, SessionError> {
        let mut ordinals = Vec::with_capacity(mids.len());
        for mid in mids {
            if let Some(ordinal) = self.find_message(mid).await? {
                ordinals.push(ordinal);
            }
        }
        ordinals.sort_unstable();
        ordinals.dedup();
        let mut runs: Vec<(u64, u64)> = Vec::new();
        for ordinal in ordinals.iter().copied() {
            match runs.last_mut() {
                Some((_, end)) if ordinal - *end <= READ_TOGETHER => *end = ordinal,
                _ => runs.push((ordinal, ordinal)),
            }
        }
        let mut out = Vec::with_capacity(ordinals.len());
        for (start, end) in runs {
            let read = self
                .compaction_entries(start, (end - start + 1) as usize)
                .await?;
            out.extend(
                read.into_iter()
                    .filter(|entry| ordinals.binary_search(&entry.ordinal).is_ok()),
            );
        }
        Ok(out)
    }

    /// The value a reader asked the session to keep with [`Session::keep_sealed_derived`], unless
    /// the history version moved since.
    pub fn sealed_derived(&self) -> Option<std::sync::Arc<dyn std::any::Any + Send + Sync>> {
        self.sealed_caches().derived.clone()
    }

    /// Keeps `value`, which a reader derived from sealed history, while the session is open and
    /// its history version stays: what the compaction transcript projects from the sealed
    /// entries its views name, so a later step reads none of them again.
    pub fn keep_sealed_derived(&self, value: std::sync::Arc<dyn std::any::Any + Send + Sync>) {
        self.sealed_caches().derived = Some(value);
    }

    /// How many sealed entries the session keeps by message id now, for diagnostics.
    pub fn kept_sealed_entries(&self) -> usize {
        self.sealed_caches().entries.len()
    }

    /// Up to `limit` entries from ordinal `from` on (counting from 1), in order, over the whole
    /// conversation: sealed history a block at a time, then the window, then the held assistant
    /// message, if any.
    pub async fn entries(&self, from: u64, limit: usize) -> Result<Vec<StoredEntry>, SessionError> {
        let mut out = Vec::new();
        let mut next = from.max(1);
        let key = self.key.frame_key();
        let pages = Pages {
            backend: &self.backend,
            addr: &self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: crate::directory::FANOUT,
        };
        let window = self.conversation.window_start();
        while next < window && out.len() < limit {
            self.storage_read()?;
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
                let mut conversation = self.block_conversation();
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

    /// The entry whose numbered parts take part number `tag` under the runner's own numbering
    /// (`EntryFacts::runner_tag_base`), if the session's branch holds one: the last entry, in
    /// ordinal order, whose base is below `tag`, since a message's parts take the numbers after
    /// its base, in order. Those numbers never repeat in a session and only grow with the
    /// ordinal, so the entry is found by a search, never a scan: the held assistant message and
    /// the window from memory, then sealed history a segment at a time. Each probe reads the one
    /// segment the transcript directory names for an ordinal and rules out every ordinal that
    /// segment places, so a search reads O(log n) segments of a session with n sealed segments,
    /// and the directory's pages above each.
    ///
    /// Which of the entry's parts are numbered, and so whether its parts reach `tag`, depends on
    /// the rules its run numbered under, which the caller checks. An entry without a base (one
    /// no computed numbering placed, or converted from a session that predates the field) is
    /// taken to number nothing.
    pub async fn tagged_entry(&self, tag: u64) -> Result<Option<StoredEntry>, SessionError> {
        let base = |entry: &StoredEntry| {
            entry
                .state
                .facts
                .as_ref()
                .and_then(|facts| facts.runner_tag_base)
        };
        let below = |entry: &StoredEntry| base(entry).is_some_and(|base| base < tag);
        if let Some((ordinal, message, state)) = self.conversation.open_entry() {
            let held = StoredEntry {
                ordinal,
                message: message.clone(),
                next: None,
                state: state.clone(),
                open: true,
            };
            if below(&held) {
                return Ok(Some(held));
            }
        }
        let window = self.conversation.window_start();
        for ordinal in (window..=self.conversation.placed).rev() {
            let base = self
                .conversation
                .window_entry(ordinal)
                .and_then(|(_, state)| state.facts.as_ref())
                .and_then(|facts| facts.runner_tag_base);
            if base.is_some_and(|base| base < tag) {
                return Ok(window_entries(&self.conversation, ordinal, 1).pop());
            }
        }
        let key = self.key.frame_key();
        let pages = Pages {
            backend: &self.backend,
            addr: &self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: crate::directory::FANOUT,
        };
        let (mut low, mut high) = (1, window.saturating_sub(1));
        // The last entry below `tag` in the sealed segments searched so far, all before `low`.
        let mut found = None;
        while low <= high {
            self.storage_read()?;
            let middle = low + (high - low) / 2;
            let damaged =
                || SessionError::Fold(format!("no sealed segment places message {middle}"));
            let segment = pages
                .find_ordinal(self.head.transcript_dir.as_ref(), middle)
                .await
                .map_err(SessionError::Directory)?
                .ok_or_else(damaged)?;
            let (first, last) = (
                segment.first_ordinal.unwrap_or(middle),
                segment.last_ordinal.unwrap_or(middle),
            );
            let entries = self.segment_entries(&segment, &key).await?;
            let numbered: Vec<&StoredEntry> = entries
                .iter()
                .filter(|entry| base(entry).is_some())
                .collect();
            match (numbered.first(), numbered.last()) {
                // The segment's first base is already at or past `tag`: look before it.
                (Some(earliest), _) if !below(earliest) => {
                    if first <= 1 {
                        break;
                    }
                    high = first - 1;
                }
                // Its last base is below `tag`: the entry is its last, or one after it.
                (_, Some(latest)) if below(latest) => {
                    found = Some((*latest).clone());
                    low = last + 1;
                }
                // The segment holds the change from below `tag` to past it.
                (Some(_), Some(_)) => {
                    return Ok(numbered
                        .into_iter()
                        .rev()
                        .find(|entry| below(entry))
                        .cloned());
                }
                // A segment whose entries have no facts numbers nothing.
                _ => low = last + 1,
            }
        }
        Ok(found)
    }

    /// Every entry the sealed transcript segment `segment` places on the session's branch.
    async fn segment_entries(
        &self,
        segment: &crate::directory::DirEntry,
        key: &crate::frame::FrameKey,
    ) -> Result<Vec<StoredEntry>, SessionError> {
        let damaged =
            || SessionError::Fold(format!("sealed segment {} is missing", segment.object));
        let bytes = self
            .backend
            .get_object(&self.addr, &segment.object)
            .await?
            .ok_or_else(damaged)?;
        let opened =
            Segment::open(&bytes, key, segment.object.as_bytes()).map_err(SessionError::Segment)?;
        let mut out = Vec::new();
        for position in 0..opened.index.blocks.len() {
            let Some(first) = opened.index.blocks[position].first_ordinal else {
                continue;
            };
            let mut conversation = self.block_conversation();
            conversation.placed = first - 1;
            for (_, record) in opened
                .block(position)
                .map_err(SessionError::Segment)?
                .into_iter()
                .filter(|(seq, _)| segment.keeps(*seq))
            {
                conversation
                    .apply(&record)
                    .map_err(|e| SessionError::Fold(format!("conversation: {e:?}")))?;
            }
            out.extend(window_entries(&conversation, first, usize::MAX));
        }
        Ok(out)
    }

    /// The ordinal of the message with id `mid`, if it's on the current branch. The id tells
    /// where to look: a new id names the seq of the record that announced it, the `StepStart`
    /// or `BatchStart` of the step or batch that writes the message, or for any other message
    /// its `EntryFacts`; an id converted from a WAL log names its position in that log's
    /// history. Either way, the entry found there must carry exactly that id, so an id from an
    /// abandoned branch, or from another session, never resolves to the message that took its
    /// place. Messages converted from a session without entry facts have none, and their
    /// `m<position>` ids resolve by position alone: no message written since lacks facts, so
    /// nothing else can sit at a converted message's ordinal.
    pub async fn find_message(&self, mid: &str) -> Result<Option<u64>, SessionError> {
        let candidate = if mid.starts_with('g') {
            let Some(seq) = minted_seq(mid) else {
                return self.imported_ordinal_checked(mid).await;
            };
            self.facts_ordinal(seq, mid).await?
        } else if let Some(rest) = mid.strip_prefix('m') {
            let digits = rest.split_once('.').map_or(rest, |(_, digits)| digits);
            match canonical_u64(digits) {
                Some(position) => position.checked_add(1),
                None => return self.imported_ordinal_checked(mid).await,
            }
        } else {
            return self.imported_ordinal_checked(mid).await;
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

    /// The ordinal of an imported message with source id `mid`, by the import's id map, checked
    /// as `find_message` checks a broca id: the entry there must carry exactly that id.
    async fn imported_ordinal_checked(&self, mid: &str) -> Result<Option<u64>, SessionError> {
        let Some(ordinal) = self.imported_ordinal(mid).await? else {
            return Ok(None);
        };
        let Some(entry) = self.entries(ordinal, 1).await?.into_iter().next() else {
            return Ok(None);
        };
        let matches = entry
            .state
            .facts
            .as_ref()
            .is_some_and(|facts| facts.mid == mid);
        Ok((entry.ordinal == ordinal && matches).then_some(ordinal))
    }

    /// The ordinal of the message with id `mid`, whose id names seq `seq`: the `EntryFacts`
    /// record at `seq`, or the first `EntryFacts` carrying `mid` after the step's or batch's
    /// start at `seq` that announced it. Read from the window, else from sealed segments.
    async fn facts_ordinal(&self, seq: u64, mid: &str) -> Result<Option<u64>, SessionError> {
        if let Some(ordinal) = self.window.facts.get(&seq) {
            return Ok(Some(*ordinal));
        }
        self.storage_read()?;
        let key = self.key.frame_key();
        let pages = Pages {
            backend: &self.backend,
            addr: &self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: crate::directory::FANOUT,
        };
        let mut read = 0;
        for entry in pages
            .entries_from(self.head.transcript_dir.as_ref(), seq)
            .await
            .map_err(SessionError::Directory)?
        {
            let damaged =
                || SessionError::Fold(format!("directory names a missing segment for {seq}"));
            let bytes = self
                .backend
                .get_object(&self.addr, &entry.object)
                .await?
                .ok_or_else(damaged)?;
            let segment = Segment::open(&bytes, &key, entry.object.as_bytes())
                .map_err(SessionError::Segment)?;
            for position in 0..segment.index.blocks.len() {
                // A block whose successor starts at or before `seq` ends before it.
                let next = segment.index.blocks.get(position + 1);
                if next.is_some_and(|next| next.first_seq <= seq) {
                    continue;
                }
                for (at, record) in segment.block(position).map_err(SessionError::Segment)? {
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

    /// The message with `ordinal` (counting from 1 over the whole conversation), or `None` when
    /// the conversation has placed no such message.
    pub async fn message(&self, ordinal: u64) -> Result<Option<Message>, SessionError> {
        if ordinal == 0 || ordinal > self.conversation.placed {
            return Ok(None);
        }
        if let Some(message) = self.conversation.window_message(ordinal) {
            return Ok(Some(message.clone()));
        }
        self.storage_read()?;
        let key = self.key.frame_key();
        let pages = Pages {
            backend: &self.backend,
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
        let mut conversation = self.block_conversation();
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
            self.storage_read()?;
            let key = self.key.frame_key();
            let index = IndexPages {
                backend: &self.backend,
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

    /// The start record of the session's run `run_id`, sealed or in the open log, or `None` when
    /// the session has no such run. A run has exactly one start, so a start the open log holds is
    /// the answer and the sealed id index isn't read. Other records naming the run don't settle
    /// it: a run that spans a seal has its start sealed and its later records (its end, say) in
    /// the open log, so without an open start this reads the index as [`Self::find_id`] does.
    pub async fn find_run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<crate::record::RunStart>, SessionError> {
        let start_of = |(_, record): (u64, Record)| match record {
            Record::RunStart(start) => Some(*start),
            _ => None,
        };
        if let Some(start) = self
            .open_ids
            .find(IdKind::RunId, run_id)
            .into_iter()
            .find_map(start_of)
        {
            return Ok(Some(start));
        }
        Ok(self
            .find_id(IdKind::RunId, run_id)
            .await?
            .into_iter()
            .find_map(start_of))
    }

    /// The ledger record with `seq`, by lookup, never a scan: a sealed one from the one block of
    /// the one segment the ledger directory names for it (the directory's pages above it, then
    /// that segment), an open one from the records the session keeps in memory, else from the
    /// open log, which a seal bounds. `None` when the session holds no record with that seq, or
    /// the one it holds is a transcript record.
    pub async fn ledger_record(&self, seq: u64) -> Result<Option<Record>, SessionError> {
        let sealed = self
            .head
            .ledger_dir
            .as_ref()
            .is_some_and(|root| seq <= root.last_seq);
        if sealed {
            self.storage_read()?;
            let key = self.key.frame_key();
            let pages = Pages {
                backend: &self.backend,
                addr: &self.addr,
                session: self.head.session_id,
                key: &key,
                fanout: crate::directory::FANOUT,
            };
            let entry = pages
                .find(self.head.ledger_dir.as_ref(), seq)
                .await
                .map_err(SessionError::Directory)?;
            // A record a seal carried into the open log is read from there.
            if let Some(entry) = entry.filter(|entry| entry.keeps(seq)) {
                let bytes = self
                    .backend
                    .get_object(&self.addr, &entry.object)
                    .await?
                    .ok_or_else(|| {
                        SessionError::Fold(format!(
                            "the directory names a missing segment {}",
                            entry.object
                        ))
                    })?;
                let segment = Segment::open(&bytes, &key, entry.object.as_bytes())
                    .map_err(SessionError::Segment)?;
                if let Some(record) = segment.find(seq).map_err(SessionError::Segment)? {
                    return Ok(Some(record));
                }
            }
        }
        let kept = self
            .reads_kept()
            .then(|| self.kept.between(seq, seq))
            .flatten();
        let open = match kept {
            Some(kept) => kept,
            None => self.open_log_records().await?,
        };
        Ok(open
            .into_iter()
            .find(|(at, record)| *at == seq && record.stream() == crate::record::Stream::Ledger)
            .map(|(_, record)| record))
    }

    /// The sealed ledger records with `seqs`, each read from the one block that holds it.
    async fn ledger_records(&self, seqs: &[u64]) -> Result<Vec<(u64, Record)>, SessionError> {
        let key = self.key.frame_key();
        let pages = Pages {
            backend: &self.backend,
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

    /// Every record of both streams from seq `first` on, sealed or in the open log, in seq
    /// order: the sealed segments that reach `first`, every block of each, then the open log.
    /// A record a seal kept in the open log is read once.
    pub async fn records_from(&self, first: u64) -> Result<Vec<(u64, Record)>, SessionError> {
        self.records_between(first, u64::MAX).await
    }

    /// Every record of both streams from seq `first` through seq `last`, sealed or in the open
    /// log, in seq order: [`Self::records_from`] bounded at `last`, reading only the sealed
    /// segments that overlap the range. A directory whose records all end before `first` holds
    /// none of the range, so it isn't read; the open log's records come from memory when the
    /// session keeps them all from `first` on.
    pub async fn records_between(
        &self,
        first: u64,
        last: u64,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        let mut records = if self.place().sealed_reach(first) {
            self.storage_read()?;
            self.place().sealed_between(first, last).await?
        } else {
            BTreeMap::new()
        };
        let kept = self
            .reads_kept()
            .then(|| self.kept.between(first, last))
            .flatten();
        let open = match kept {
            Some(kept) => kept,
            None => self.open_log_records().await?,
        };
        for (seq, record) in open {
            if (first..=last).contains(&seq) {
                records.insert(seq, record);
            }
        }
        Ok(records.into_iter().collect())
    }
}

#[cfg(test)]
mod tests;
