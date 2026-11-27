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
use std::sync::Arc;

use broca_protocol::content::Message;

use crate::backend::SessionBackend;
use crate::directory::Pages;
use crate::fold::conversation::{Conversation, EntryState, NotHeld};
use crate::idindex::{self, IdKind, IndexPages, IndexRoot};
use crate::record::Record;
use crate::segment::SegmentLayout;

use super::{Session, SessionError};

/// One message as `session.entries` reads it. A window entry shares its message and facts with
/// the conversation, so serving it copies no content.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredEntry {
    pub ordinal: u64,
    pub message: Arc<Message>,
    /// For an assistant message that called tools: the tool message with their results.
    pub next: Option<Arc<Message>>,
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
pub fn served_window(conversation: &Conversation) -> Result<Vec<StoredEntry>, NotHeld> {
    let mut entries = window_entries(conversation, 1, usize::MAX)?;
    if let Some((ordinal, message, state)) = conversation.open_entry() {
        entries.push(StoredEntry {
            ordinal,
            message: Arc::clone(message),
            next: None,
            state: state.clone(),
            open: true,
        });
    }
    Ok(entries)
}

/// Whether `message` is an assistant message that called tools, whose results the tool message
/// after it holds.
fn called_tools(message: &broca_protocol::content::Message) -> bool {
    message.role == broca_protocol::content::Role::Assistant
        && message
            .content
            .iter()
            .any(|block| matches!(block, broca_protocol::content::ContentBlock::ToolCall(_)))
}

/// The session error for a window message whose content a read needed and didn't hold.
fn not_held(not: NotHeld) -> SessionError {
    SessionError::NotHeld(not.ordinal)
}

/// The entries of `conversation`'s window from `from` on, at most `limit` of them: one per
/// window message, so a request's prompt and its entries line up. Each entry carries its
/// message's content, so a message whose content isn't held refuses with [`NotHeld`].
pub fn window_entries(
    conversation: &Conversation,
    from: u64,
    limit: usize,
) -> Result<Vec<StoredEntry>, NotHeld> {
    window_entries_with(conversation, from, limit, |_| None)
}

/// [`window_entries`], taking the content of a window message that isn't held from `whole`,
/// which gives the message at an ordinal read back whole, or `None` when it wasn't read.
pub fn window_entries_with(
    conversation: &Conversation,
    from: u64,
    limit: usize,
    whole: impl Fn(u64) -> Option<Arc<Message>>,
) -> Result<Vec<StoredEntry>, NotHeld> {
    let held = |ordinal: u64, message: &crate::fold::conversation::WindowMessage| {
        message
            .held()
            .cloned()
            .or_else(|| whole(ordinal))
            .ok_or(NotHeld { ordinal })
    };
    let start = conversation.window_start().max(from);
    let mut entries = Vec::new();
    for ordinal in (start..=conversation.placed).take(limit) {
        let Some((message, state)) = conversation.window_entry(ordinal) else {
            continue;
        };
        let message = held(ordinal, message)?;
        let called = called_tools(&message);
        let next = match conversation.window_message(ordinal + 1) {
            Some(next) if called && next.role() == broca_protocol::content::Role::Tool => {
                Some(held(ordinal + 1, next)?)
            }
            _ => None,
        };
        entries.push(StoredEntry {
            ordinal,
            message,
            next,
            state: state.clone(),
            open: false,
        });
    }
    Ok(entries)
}

/// What a session keeps of its sealed history while it's open, with the history version it was
/// read under. Sealed history never changes, except that a revert to a sealed message cuts the
/// history after it and moves the history version: everything read under an older version is
/// dropped together.
#[derive(Default)]
pub(super) struct SealedCaches {
    version: u64,
    /// Sealed entries read by message id (see `entry_by_mid`), at most
    /// [`SEALED_ENTRIES_MAX_BYTES`] of them.
    entries: SealedEntryCache,
    /// A value a reader derived from sealed history and asked the session to keep (see
    /// `keep_sealed_derived`).
    derived: Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>,
    /// What reads of sealed history keep for the reads after them: segment layouts and
    /// directory pages. Boxed, so a session that never reads sealed history carries one pointer
    /// for both.
    kept: Option<Box<KeptReads>>,
}

/// What reads of sealed history keep for the reads after them.
#[derive(Default)]
struct KeptReads {
    /// The layouts (header and index) of the sealed segments read most recently, by object
    /// name, at most [`SEALED_LAYOUTS_KEPT`]: a page reads only its blocks after the first.
    layouts: KeptLayouts,
    /// Directory pages read before, by object name, at most [`DIRECTORY_PAGES_KEPT`]. A page's
    /// name is minted for the change that wrote it and never names other content, so a kept
    /// page is never stale. A search walks the same few pages for every probe and each read
    /// takes them from here.
    pages: crate::directory::PageMemo,
    /// The sealed run starts found before, by run id, at most [`RUN_STARTS_KEPT`], newest last.
    /// `run.admission` asks for a run's start on every call, and a sealed start costs an id-index
    /// lookup and a block read; a start never changes once sealed.
    run_starts: Vec<(String, Arc<crate::record::RunStart>)>,
}

/// How many sealed run starts a session keeps while open: the few runs its callers ask about.
const RUN_STARTS_KEPT: usize = 16;

/// How many sealed records an id may name for its lookup to read them while reads come from
/// memory only (see `Session::sealed_found`). An id that names more, such as a long run's id,
/// reads as many blocks, and refuses there as before.
const SEALED_LOOKUP_IN_MEMORY: usize = 32;

/// How many sealed messages a read may take from sealed segments while reads come from memory
/// only (see `Session::unjoined_entries`): a few blocks of sealed objects that never change. A
/// longer page refuses there, and is read from storage by the session's reader.
const SEALED_ENTRIES_IN_MEMORY: usize = 32;

/// Four of the store's 32-entry reads, including look-ahead. This caps work on the actor,
/// not the wire page: the caller redoes the read from storage if it needs more entries.
const COMPACTION_PAGE_ENTRIES_IN_MEMORY: usize = 4 * SEALED_ENTRIES_IN_MEMORY;

/// How many directory pages a session keeps while open: enough for the transcript and ledger
/// directories' paths that one page read walks.
const DIRECTORY_PAGES_KEPT: usize = 32;

/// Sealed segments' layouts, by object name, oldest read first.
type KeptLayouts = Vec<(String, Arc<SegmentLayout>)>;

/// How many sealed segments' layouts a session keeps while open. A layout holds one index entry
/// per block (a few tens of bytes per 16 KiB of history), so a session's largest segments fit.
const SEALED_LAYOUTS_KEPT: usize = 32;

pub(super) type SealedEntries = std::sync::Mutex<SealedCaches>;

/// The most a session keeps of the sealed entries it read by message id, counted by walking them
/// (see `crate::held`): 8 MiB. Past it the least recently read entry goes first, and reading it
/// again reads its sealed block from storage once more.
pub const SEALED_ENTRIES_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Sealed entries read by message id, least recently read dropped first past
/// [`SEALED_ENTRIES_MAX_BYTES`].
#[derive(Default)]
pub(super) struct SealedEntryCache {
    by_mid: std::collections::HashMap<String, CachedEntry>,
    /// The ids by when each was last read, oldest first.
    by_use: BTreeMap<u64, String>,
    /// The walked size of the entries kept and their ids, without the tables holding them.
    bytes: usize,
    clock: u64,
}

struct CachedEntry {
    entry: StoredEntry,
    bytes: usize,
    used: u64,
}

impl SealedEntryCache {
    /// The entry kept for `mid`, which counts as read now.
    pub(super) fn get(&mut self, mid: &str) -> Option<StoredEntry> {
        self.clock += 1;
        let cached = self.by_mid.get_mut(mid)?;
        self.by_use.remove(&cached.used);
        cached.used = self.clock;
        self.by_use.insert(cached.used, mid.to_owned());
        Some(cached.entry.clone())
    }

    /// What the cache holds as its cap counts it: the entries and their ids, and the tables
    /// holding them, each slot of the table by id included whether filled or not.
    pub(super) fn held_bytes(&self) -> usize {
        // A slot and its control byte per bucket; an entry of the table by use and its share of
        // the node it sits in.
        self.bytes
            + self.by_mid.capacity() * (std::mem::size_of::<(String, CachedEntry)>() + 1)
            + self.by_use.len() * (std::mem::size_of::<(u64, String)>() + 16)
    }

    /// Keeps `entry` for `mid`, then drops the least recently read entries until the cache fits
    /// its cap. An entry larger than the cap alone is not kept. The walk misses a little of what
    /// the allocator holds (the parsed JSON's nodes past their first, the tables' nodes), about
    /// 1% measured, so the walked size is kept a thirty-second below the cap and the real size
    /// stays under it.
    pub(super) fn insert(&mut self, mid: &str, entry: StoredEntry) {
        use crate::held::HeldBytes;
        self.clock += 1;
        let bytes = 2 * mid.len() + entry.heap_bytes();
        let used = self.clock;
        if let Some(old) = self
            .by_mid
            .insert(mid.to_owned(), CachedEntry { entry, bytes, used })
        {
            self.by_use.remove(&old.used);
            self.bytes -= old.bytes;
        }
        self.by_use.insert(used, mid.to_owned());
        self.bytes += bytes;
        while self.held_bytes() > SEALED_ENTRIES_MAX_BYTES - SEALED_ENTRIES_MAX_BYTES / 32 {
            let Some((_, oldest)) = self.by_use.pop_first() else {
                break;
            };
            if let Some(dropped) = self.by_mid.remove(&oldest) {
                self.bytes -= dropped.bytes;
            }
        }
    }

    pub(super) fn len(&self) -> usize {
        self.by_mid.len()
    }
}

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

    /// The layout of sealed segment `object`: its header and index, read by ranges (the leading
    /// bytes, the footer, then the index), never its blocks. Kept while the session is open, so
    /// later reads of the segment fetch only their blocks. `None` when no such object is stored.
    async fn sealed_layout(
        &self,
        object: &str,
        key: &crate::frame::FrameKey,
    ) -> Result<Option<Arc<SegmentLayout>>, SessionError> {
        let kept = self
            .sealed_caches()
            .kept
            .as_deref()
            .and_then(|kept| kept.layouts.iter().find(|(name, _)| name == object))
            .map(|(_, layout)| Arc::clone(layout));
        if kept.is_some() {
            return Ok(kept);
        }
        let version = self.operations.history_version;
        let Some(layout) =
            super::stored::read_layout(&self.backend, &self.addr, object, key).await?
        else {
            return Ok(None);
        };
        let layout = Arc::new(layout);
        let mut caches = self.sealed_caches();
        if caches.version == version {
            let layouts = &mut caches.kept.get_or_insert_with(Box::default).layouts;
            if layouts.len() == SEALED_LAYOUTS_KEPT {
                layouts.remove(0);
            }
            layouts.push((object.to_owned(), Arc::clone(&layout)));
        }
        Ok(Some(layout))
    }

    /// The records of block `position` of sealed segment `object`, whose layout is `layout`:
    /// fetches that block's bytes alone.
    async fn sealed_block(
        &self,
        object: &str,
        layout: &SegmentLayout,
        position: usize,
        key: &crate::frame::FrameKey,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        super::stored::read_block(&self.backend, &self.addr, object, layout, position, key).await
    }

    /// The directory pages the session keeps, taken for one read, which hands them back with
    /// [`Self::keep_pages`] when it's done. A read running at the same time starts with none.
    fn take_pages(&self) -> crate::directory::PageMemo {
        self.sealed_caches()
            .kept
            .as_mut()
            .map(|kept| std::mem::take(&mut kept.pages))
            .unwrap_or_default()
    }

    /// Keeps the pages a read took and added to, unless they've grown past
    /// [`DIRECTORY_PAGES_KEPT`]; then the next read starts again with none.
    fn keep_pages(&self, pages: crate::directory::PageMemo) {
        if pages.len() <= DIRECTORY_PAGES_KEPT {
            self.sealed_caches()
                .kept
                .get_or_insert_with(Box::default)
                .pages = pages;
        }
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
    /// message an enrollment suppressed is served with no content.
    pub async fn entry_by_mid(&self, mid: &str) -> Result<Option<StoredEntry>, SessionError> {
        if let Some(entry) = self.sealed_caches().entries.get(mid) {
            return Ok(Some(entry));
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
                caches.entries.insert(mid, entry.clone());
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

    /// What the sealed entries the session keeps by message id hold, as their cap counts them
    /// (see [`SEALED_ENTRIES_MAX_BYTES`]): read without walking them.
    pub fn sealed_entries_bytes(&self) -> usize {
        self.sealed_caches().entries.held_bytes()
    }

    /// The next ordinal batch for a compaction page, without exceeding a sealed read's limit.
    /// `total` includes look-ahead when known; `None` probes one more batch for a byte-only
    /// request. When the page exceeds the actor's work budget it is read from storage instead.
    pub fn compaction_page_chunk_size(
        &self,
        examined: usize,
        total: Option<usize>,
    ) -> Result<usize, SessionError> {
        let remaining = total.map_or(SEALED_ENTRIES_IN_MEMORY, |total| {
            total.saturating_sub(examined)
        });
        if examined.saturating_add(remaining) > COMPACTION_PAGE_ENTRIES_IN_MEMORY {
            self.storage_read()?;
        }
        Ok(remaining.min(SEALED_ENTRIES_IN_MEMORY))
    }

    /// Up to `limit` entries from ordinal `from` on (counting from 1), in order, over the whole
    /// conversation: sealed history a block at a time, then the window, then the held assistant
    /// message, if any.
    pub async fn entries(&self, from: u64, limit: usize) -> Result<Vec<StoredEntry>, SessionError> {
        let entries = self.unjoined_entries(from, limit).await?;
        self.join_split_turns(entries).await
    }

    /// Sealed history can hold an assistant message that called tools at the end of one block
    /// and its tool message at the start of the next: an imported tool message is its own input
    /// record, and sealed before `segment::groups` kept it with its call. Read a block at a time,
    /// such an entry has no results, and its calls would read as still running. Attach each such
    /// sealed entry's tool message from the entry after it in `entries`, or read that one entry
    /// when `entries` stops before it.
    async fn join_split_turns(
        &self,
        mut entries: Vec<StoredEntry>,
    ) -> Result<Vec<StoredEntry>, SessionError> {
        let window = self.conversation.window_start();
        for index in 0..entries.len() {
            let entry = &entries[index];
            if entry.open
                || entry.next.is_some()
                || entry.ordinal >= window
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
                None => self
                    .unjoined_entries(ordinal, 1)
                    .await?
                    .into_iter()
                    .find(|next| next.ordinal == ordinal)
                    .map(|next| next.message),
            };
            entries[index].next =
                next.filter(|next| next.role == broca_protocol::content::Role::Tool);
        }
        Ok(entries)
    }

    async fn unjoined_entries(
        &self,
        from: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, SessionError> {
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
        // Directory pages come from those the session keeps, so a page of entries after the first
        // reads only its blocks. A read that fails hands none back; the next starts with none.
        let mut memo = self.take_pages();
        if next < window && limit > SEALED_ENTRIES_IN_MEMORY {
            self.storage_read()?;
        }
        while next < window && out.len() < limit {
            let damaged = || SessionError::Fold(format!("no sealed segment places message {next}"));
            let sought = next;
            let entry = pages
                .find_ordinal_in(self.head.transcript_dir.as_ref(), next, &mut memo)
                .await
                .map_err(SessionError::Directory)?
                .ok_or_else(damaged)?;
            let segment = self
                .sealed_layout(&entry.object, &key)
                .await?
                .ok_or_else(damaged)?;
            let mut position = segment.block_for_ordinal(next).ok_or_else(damaged)?;
            // Read on through the segment's blocks while the page wants more.
            while position < segment.index.blocks.len() && next < window && out.len() < limit {
                let first = segment.index.blocks[position]
                    .first_ordinal
                    .expect("block_for_ordinal only answers when every block has an ordinal");
                let mut conversation = self.block_conversation();
                conversation.placed = first - 1;
                for (_, record) in self
                    .sealed_block(&entry.object, &segment, position, &key)
                    .await?
                    .into_iter()
                    .filter(|(seq, _)| entry.keeps(*seq))
                {
                    conversation
                        .apply(&record)
                        .map_err(|e| SessionError::Fold(format!("conversation: {e:?}")))?;
                }
                let wanted = limit - out.len();
                // A conversation folded from one sealed block holds all its content.
                let read = window_entries(&conversation, next, wanted).map_err(not_held)?;
                if let Some(last) = read.last() {
                    next = last.ordinal + 1;
                }
                out.extend(read);
                position += 1;
                // A block that places nothing past `next` (its messages are all earlier) moves
                // on to the next block.
                next = next.max(conversation.placed + 1);
            }
            // The directory named this segment for `sought`, yet none of its blocks placed it: the
            // directory and the segment disagree, and finding the segment again would loop forever.
            if next == sought {
                return Err(SessionError::Fold(format!(
                    "no sealed segment places message {sought}"
                )));
            }
        }
        self.keep_pages(memo);
        if out.len() < limit {
            out.extend(self.window_entries_whole(next, limit - out.len()).await?);
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
    /// the window from memory, then sealed history a block at a time. Each probe reads the one
    /// block that places an ordinal (found through the transcript directory and the segment's
    /// index) and rules out every ordinal that block places, so a search reads O(log n) blocks
    /// of a session with n sealed messages, and the directory's pages and segment indexes above
    /// them.
    ///
    /// Which of the entry's parts are numbered, and so whether its parts reach `tag`, depends on
    /// the rules its run numbered under, which the caller checks. An entry without a base (one
    /// no computed numbering placed, or converted from a session that predates the field) is
    /// taken to number nothing.
    pub async fn tagged_entry(&self, tag: u64) -> Result<Option<StoredEntry>, SessionError> {
        self.entry_numbering(tag, |facts| facts.runner_tag_base)
            .await
    }

    /// [`Self::tagged_entry`] under the numbering `session.entries` serves
    /// (`EntryFacts::tag_base`): the entry that numbers `tag` there, if any, is the last one
    /// whose base is below it, found by the same search. Whether its parts reach `tag` is the
    /// caller's to check, by projecting the entry.
    pub async fn compaction_tagged_entry(
        &self,
        tag: u64,
    ) -> Result<Option<StoredEntry>, SessionError> {
        self.entry_numbering(tag, |facts| Some(facts.tag_base))
            .await
    }

    /// The last entry whose numbering base, as `base` reads it from the entry's facts, is below
    /// `tag` (see [`Self::tagged_entry`]).
    async fn entry_numbering(
        &self,
        tag: u64,
        base: fn(&crate::record::EntryFacts) -> Option<u64>,
    ) -> Result<Option<StoredEntry>, SessionError> {
        let below = |entry: &StoredEntry| {
            entry
                .state
                .facts
                .as_ref()
                .and_then(|facts| base(facts))
                .is_some_and(|base| base < tag)
        };
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
                .and_then(|facts| base(facts));
            if base.is_some_and(|base| base < tag) {
                return Ok(self.window_entries_whole(ordinal, 1).await?.pop());
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
        let mut memo = self.take_pages();
        let searched = self
            .search_numbering(tag, base, &pages, &mut memo, window)
            .await;
        self.keep_pages(memo);
        match searched? {
            Some(found) => Ok(self.join_split_turns(vec![found]).await?.pop()),
            None => Ok(None),
        }
    }

    /// The sealed part of [`Self::entry_numbering`]'s search, over ordinals before `window`,
    /// reading directory pages through `memo`.
    async fn search_numbering(
        &self,
        tag: u64,
        base: fn(&crate::record::EntryFacts) -> Option<u64>,
        pages: &Pages<'_, B>,
        memo: &mut crate::directory::PageMemo,
        window: u64,
    ) -> Result<Option<StoredEntry>, SessionError> {
        let base = |entry: &StoredEntry| entry.state.facts.as_ref().and_then(|facts| base(facts));
        let below = |entry: &StoredEntry| base(entry).is_some_and(|base| base < tag);
        let key = self.key.frame_key();
        let (mut low, mut high) = (1, window.saturating_sub(1));
        // The last entry below `tag` in the sealed segments searched so far, all before `low`.
        let mut found = None;
        while low <= high {
            self.storage_read()?;
            let middle = low + (high - low) / 2;
            let damaged =
                || SessionError::Fold(format!("no sealed segment places message {middle}"));
            let segment = pages
                .find_ordinal_in(self.head.transcript_dir.as_ref(), middle, memo)
                .await
                .map_err(SessionError::Directory)?
                .ok_or_else(damaged)?;
            let (first, last, entries) = self.block_entries_at(&segment, middle, &key).await?;
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

    /// The entries the one block of sealed transcript segment `segment` that places ordinal
    /// `ordinal` puts on the session's branch, with the first and last ordinals that block
    /// places. Only that block is fetched, so a search probe costs one block, not a segment.
    /// A segment none of whose blocks places a message gives no entries over the segment's own
    /// ordinals.
    async fn block_entries_at(
        &self,
        segment: &crate::directory::DirEntry,
        ordinal: u64,
        key: &crate::frame::FrameKey,
    ) -> Result<(u64, u64, Vec<StoredEntry>), SessionError> {
        let damaged =
            || SessionError::Fold(format!("sealed segment {} is missing", segment.object));
        let layout = self
            .sealed_layout(&segment.object, key)
            .await?
            .ok_or_else(damaged)?;
        let starts: Vec<(usize, u64)> = layout
            .index
            .blocks
            .iter()
            .enumerate()
            .filter_map(|(position, block)| block.first_ordinal.map(|first| (position, first)))
            .collect();
        let segment_first = segment.first_ordinal.unwrap_or(ordinal);
        let segment_last = segment.last_ordinal.unwrap_or(ordinal);
        if starts.is_empty() {
            return Ok((segment_first, segment_last, Vec::new()));
        }
        // The last block whose first message is at or before `ordinal`; a block places the
        // ordinals from its first up to the next block's first.
        let at = starts
            .iter()
            .rposition(|(_, first)| *first <= ordinal)
            .unwrap_or(0);
        let (position, first) = starts[at];
        let last = starts
            .get(at + 1)
            .map_or(segment_last, |(_, next)| next.saturating_sub(1));
        let mut conversation = self.block_conversation();
        conversation.placed = first - 1;
        for (_, record) in self
            .sealed_block(&segment.object, &layout, position, key)
            .await?
            .into_iter()
            .filter(|(seq, _)| segment.keeps(*seq))
        {
            conversation
                .apply(&record)
                .map_err(|e| SessionError::Fold(format!("conversation: {e:?}")))?;
        }
        Ok((
            first,
            last,
            window_entries(&conversation, first, usize::MAX).map_err(not_held)?,
        ))
    }

    /// The ordinal of the message with id `mid`, if it's on the current branch. The id tells
    /// where to look: a new id names the seq of the record that announced it, the `StepStart`
    /// or `BatchStart` of the step or batch that writes the message, or for any other message
    /// its `EntryFacts`; an id carried over when the session was converted into the session
    /// store names its position in the session's history. Either way, the entry found there must
    /// carry exactly that id, so an id from an abandoned branch, or from another session, never
    /// resolves to the message that took its place. Messages converted from a session without
    /// entry facts have none, and their `m<position>` ids resolve by position alone: no message
    /// written since lacks facts, so nothing else can sit at a converted message's ordinal.
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
    ///
    /// The sealed read goes to the backend even while reads come from memory only (see
    /// [`Session::in_memory`]): it starts at the block holding `seq` and stops after
    /// [`ANNOUNCED_FACTS_READ`] facts, a few small reads of sealed objects that never change.
    async fn facts_ordinal(&self, seq: u64, mid: &str) -> Result<Option<u64>, SessionError> {
        if let Some(ordinal) = self.window.facts.get(&seq) {
            return Ok(Some(*ordinal));
        }
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
            let segment = self
                .sealed_layout(&entry.object, &key)
                .await?
                .ok_or_else(damaged)?;
            // Blocks before the one holding `seq` hold nothing at or after it.
            let start = segment.block_for_seq(seq).unwrap_or(0);
            for position in start..segment.index.blocks.len() {
                for (at, record) in self
                    .sealed_block(&entry.object, &segment, position, &key)
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

    /// The message with `ordinal` (counting from 1 over the whole conversation), or `None` when
    /// the conversation has placed no such message. A window message is shared, not copied.
    pub async fn message(&self, ordinal: u64) -> Result<Option<Arc<Message>>, SessionError> {
        if ordinal == 0 || ordinal > self.conversation.placed {
            return Ok(None);
        }
        if ordinal >= self.conversation.window_start() {
            // A window message whose content was released reads back from the open log.
            return Box::pin(self.window_message_whole(ordinal)).await.map(Some);
        }
        // One sealed message: a few blocks, read from memory only too.
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
        let segment = self
            .sealed_layout(&entry.object, &key)
            .await?
            .ok_or_else(damaged)?;
        let position = segment.block_for_ordinal(ordinal).ok_or_else(damaged)?;
        let first = segment.index.blocks[position]
            .first_ordinal
            .expect("block_for_ordinal only answers when every block has an ordinal");
        let mut conversation = self.block_conversation();
        conversation.placed = first - 1;
        for (_, record) in self
            .sealed_block(&entry.object, &segment, position, &key)
            .await?
            .into_iter()
            .filter(|(seq, _)| entry.keeps(*seq))
        {
            conversation
                .apply(&record)
                .map_err(|e| SessionError::Fold(format!("conversation: {e:?}")))?;
        }
        // A conversation folded from one sealed block holds all its content.
        conversation
            .held_message(ordinal)
            .map_err(|not| SessionError::NotHeld(not.ordinal))?
            .cloned()
            .map(Some)
            .ok_or_else(damaged)
    }

    /// Every ledger record that names `id` as a `kind`, sealed or in the open log, in seq
    /// order. A sealed record is found through the id index, then read from its segment and
    /// checked to name the id itself, so two ids whose hashes collide never answer for each
    /// other. A seq the index names that no segment holds is damage, never "not found".
    ///
    /// An open-log record is found through the open log's index, and checked the same way. A
    /// record written per tool call comes from memory while it is in the latest run, which the
    /// session keeps, and is otherwise read back from the open log, one ranged read each, which
    /// reading from memory only refuses. Every other open-log record comes from memory.
    pub async fn find_id(
        &self,
        kind: IdKind,
        id: &str,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        let mut found = self.open_found(kind, id).await?;
        // The sealed index's reads are boxed, so every future that looks an id up stays small.
        if self.head.id_index.is_some() {
            found.extend(Box::pin(self.sealed_found(kind, id)).await?);
        }
        found.sort_by_key(|(seq, _)| *seq);
        Ok(found)
    }

    /// The open log's ledger records that name `id` as a `kind`, in seq order.
    async fn open_found(&self, kind: IdKind, id: &str) -> Result<Vec<(u64, Record)>, SessionError> {
        let mut found = Vec::new();
        for (seq, at) in self.open_ids.find(kind, id) {
            let record = match at {
                idindex::OpenRecord::At(place) => match self.kept.get(seq) {
                    Some(record) => record.clone(),
                    // A cold read, boxed so it adds nothing to every future that looks an id up.
                    None => {
                        Box::pin(self.open_record(seq, place, crate::frame::FrameStream::Ledger))
                            .await?
                    }
                },
                _ => self.open_in_memory(seq, at)?.clone(),
            };
            if idindex::names(&record, kind, id) {
                found.push((seq, record));
            }
        }
        Ok(found)
    }

    /// An open-log record the id index takes from memory: held by the index, or by the kept
    /// records.
    fn open_in_memory<'a>(
        &'a self,
        seq: u64,
        at: idindex::OpenRecord<'a>,
    ) -> Result<&'a Record, SessionError> {
        let record = match at {
            idindex::OpenRecord::Held(record) => Some(record),
            idindex::OpenRecord::Kept => self.kept.get(seq),
            idindex::OpenRecord::At(_) | idindex::OpenRecord::Lost => None,
        };
        record.ok_or_else(|| {
            SessionError::Fold(format!(
                "the open log's id index names record {seq}, which the session doesn't hold"
            ))
        })
    }

    /// The record of `stream` at `seq` of the open log, read back from where its frame lies:
    /// one ranged read of the log. The writer appended it, so a frame that doesn't read back as
    /// that record is damage.
    ///
    /// This read goes to the backend even while reads come from memory only (see
    /// [`Session::in_memory`]): it reads one record's bytes, where refusing it sent the whole
    /// read to a load of the whole session.
    async fn open_record(
        &self,
        seq: u64,
        place: crate::log::FramePlace,
        stream: crate::frame::FrameStream,
    ) -> Result<Record, SessionError> {
        idindex::count_open_record_read();
        let bytes = self
            .backend
            .read_log(
                &self.addr,
                &self.log,
                place.at..place.at + u64::from(place.len),
            )
            .await?;
        let key = self.key.frame_key();
        let bind = super::log_bind(&self.head.session_id, &self.log);
        let (header, body) =
            match crate::frame::open(&bytes, |id| (id == key.id()).then_some(&key), &bind) {
                Ok(crate::frame::FrameRead::Frame { header, body, .. }) => (header, body),
                Ok(crate::frame::FrameRead::Incomplete) => {
                    return Err(SessionError::Fold(format!(
                        "the open log's frame for record {seq} at {} is cut short",
                        place.at
                    )))
                }
                Err(error) => {
                    return Err(SessionError::Damaged(crate::log::LogEnd::Damaged {
                        at: place.at,
                        damage: crate::log::Damage::Frame(error),
                    }))
                }
            };
        if header.seq != seq || header.stream != stream {
            return Err(SessionError::Fold(format!(
                "the open log's frame at {} holds record {}, not {stream:?} record {seq}",
                place.at, header.seq
            )));
        }
        Record::decode(u32::from(header.kind), &body).map_err(|error| {
            SessionError::Log(crate::log::LogError::Undecodable {
                offset: place.at,
                error,
            })
        })
    }

    /// The window message at `ordinal` with all its content. A message whose content was
    /// released from memory is read back from its transcript records in the open log, one ranged
    /// read each, and checked against what the window keeps of it.
    /// The open window's entries from `from` on, at most `limit` of them ([`window_entries`]),
    /// with each message whose content was released read back from the open log first.
    pub async fn window_entries_whole(
        &self,
        from: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, SessionError> {
        let start = self.conversation.window_start().max(from);
        // The entries look at `limit` ordinals from `start`, and at the tool message after the
        // last of them.
        let end = start.saturating_add(limit as u64);
        let mut whole = std::collections::HashMap::new();
        for ordinal in self.conversation.released_ordinals() {
            if (start..=end).contains(&ordinal) {
                whole.insert(ordinal, Box::pin(self.window_message_whole(ordinal)).await?);
            }
        }
        window_entries_with(&self.conversation, from, limit, |ordinal| {
            whole.get(&ordinal).cloned()
        })
        .map_err(not_held)
    }

    pub async fn window_message_whole(
        &self,
        ordinal: u64,
    ) -> Result<std::sync::Arc<broca_protocol::Message>, SessionError> {
        let message = self
            .conversation
            .window_message(ordinal)
            .ok_or_else(|| SessionError::Fold(format!("no message {ordinal} is in the window")))?;
        if let Some(held) = message.held() {
            return Ok(std::sync::Arc::clone(held));
        }
        let mut records = Vec::with_capacity(message.sources().len());
        for &seq in message.sources() {
            let place = self.open_ids.transcript_place(seq).ok_or_else(|| {
                SessionError::Fold(format!(
                    "window message {ordinal} was released, and its record {seq} isn't in the \
                     open log"
                ))
            })?;
            let record =
                Box::pin(self.open_record(seq, place, crate::frame::FrameStream::Transcript))
                    .await?;
            records.push(record);
        }
        self.conversation
            .read_back(ordinal, &records)
            .map_err(|error| {
                SessionError::Fold(format!(
                    "window message {ordinal} didn't read back from its records: {error:?}"
                ))
            })
    }

    /// The sealed ledger records that name `id` as a `kind`, through the sealed id index.
    ///
    /// This lookup reads the backend even while reads come from memory only (see
    /// [`Session::in_memory`]), as long as the id names at most [`SEALED_LOOKUP_IN_MEMORY`]
    /// records: the index pages and the blocks holding those records are a few small reads of
    /// sealed objects that never change. Most lookups find nothing there (a status poll asks
    /// after ids a session never used), and refusing them sent every such read on a session with
    /// sealed history to a load of the whole session.
    async fn sealed_found(
        &self,
        kind: IdKind,
        id: &str,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        let mut found = Vec::new();
        if let Some(root_name) = &self.head.id_index {
            let key = self.key.frame_key();
            let index = IndexPages {
                backend: &self.backend,
                addr: &self.addr,
                session: self.head.session_id,
                key: &key,
            };
            let root: IndexRoot = index.read(root_name).await.map_err(SessionError::Index)?;
            let hash = self.open_ids.hash(kind, id);
            let seqs = index
                .lookup(&root, &hash)
                .await
                .map_err(SessionError::Index)?;
            if seqs.len() > SEALED_LOOKUP_IN_MEMORY {
                self.storage_read()?;
            }
            for (seq, record) in self.ledger_records(&seqs).await? {
                if idindex::names(&record, kind, id) {
                    found.push((seq, record));
                }
            }
        }
        Ok(found)
    }

    /// The start record of the session's run `run_id`, sealed or in the open log, or `None` when
    /// the session has no such run. A run has exactly one start, so a start the open log holds is
    /// the answer, from memory, and the sealed id index isn't read. Other records naming the run
    /// don't settle it: a run that spans a seal has its start sealed and its later records (its
    /// end, say) in the open log, so without an open start this reads the sealed index as
    /// [`Self::find_id`] does. Every open-log run start is in memory, so that read skips the open
    /// log's other records, which hold no start.
    pub async fn find_run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<crate::record::RunStart>, SessionError> {
        for (seq, at) in self.open_ids.find(IdKind::RunId, run_id) {
            if matches!(at, idindex::OpenRecord::At(_)) {
                continue;
            }
            if let Record::RunStart(start) = self.open_in_memory(seq, at)? {
                if start.run_id == run_id {
                    return Ok(Some((**start).clone()));
                }
            }
        }
        if self.head.id_index.is_none() {
            return Ok(None);
        }
        if let Some(start) = self.sealed_run_start_kept(run_id) {
            return Ok(Some((*start).clone()));
        }
        let found = Box::pin(self.sealed_found(IdKind::RunId, run_id))
            .await?
            .into_iter()
            .find_map(|(_, record)| match record {
                Record::RunStart(start) => Some(*start),
                _ => None,
            });
        if let Some(start) = &found {
            self.keep_sealed_run_start(start);
        }
        Ok(found)
    }

    /// The sealed start of run `run_id` an earlier [`Self::find_run_start`] kept, if any.
    fn sealed_run_start_kept(&self, run_id: &str) -> Option<Arc<crate::record::RunStart>> {
        self.sealed_caches().kept.as_ref().and_then(|kept| {
            kept.run_starts
                .iter()
                .find(|(id, _)| id == run_id)
                .map(|(_, start)| Arc::clone(start))
        })
    }

    /// Keeps a sealed run start found for later finds, dropping the oldest past
    /// [`RUN_STARTS_KEPT`].
    fn keep_sealed_run_start(&self, start: &crate::record::RunStart) {
        let mut caches = self.sealed_caches();
        let kept = &mut caches.kept.get_or_insert_with(Box::default).run_starts;
        if kept.len() == RUN_STARTS_KEPT {
            kept.remove(0);
        }
        kept.push((start.run_id.clone(), Arc::new(start.clone())));
    }

    /// The ledger record with `seq`, by lookup, never a scan: a sealed one from the one block of
    /// the one segment the ledger directory names for it (the directory's pages above it, then
    /// that segment), an open one from the records the session keeps in memory, else from the
    /// stretch of the open log around its frame (see [`Self::open_ledger_record`]). `None` when
    /// the session holds no record with that seq, or the one it holds is a transcript record.
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
                if let Some(record) = self.sealed_record(&entry.object, seq, &key).await? {
                    return Ok(Some(record));
                }
            }
        }
        if self.reads_kept() {
            if let Some(kept) = self.kept.between(seq, seq) {
                return Ok(kept
                    .into_iter()
                    .find(|(at, record)| {
                        *at == seq && record.stream() == crate::record::Stream::Ledger
                    })
                    .map(|(_, record)| record));
            }
        }
        self.open_ledger_record(seq).await
    }

    /// The ledger record at `seq` in the open log, if the log holds one, read back from the
    /// stretch of the log between the nearest frames the session knows the places of on either
    /// side ([`idindex::OpenIds::places_around`]): one ranged read, usually of a few frames, never
    /// the whole log. The writer appended every frame there, so one that doesn't read back is
    /// damage.
    async fn open_ledger_record(&self, seq: u64) -> Result<Option<Record>, SessionError> {
        // A seq past the log's last record, or one a transcript record takes, names no ledger
        // record there.
        if seq >= self.encoder.next_seq() || self.open_ids.transcript_place(seq).is_some() {
            return Ok(None);
        }
        self.storage_read()?;
        let (before, after) = self.open_ids.places_around(seq);
        let start = before.map_or(0, |place| place.at + u64::from(place.len));
        let end = after.map_or(self.encoder.offset(), |place| place.at);
        let bytes = self
            .backend
            .read_log(&self.addr, &self.log, start..end)
            .await?;
        let key = self.key.frame_key();
        let bind = super::log_bind(&self.head.session_id, &self.log);
        let mut found = None;
        super::stored::read_frames(&bytes, start, &key, &bind, |at, header, body| {
            if header.seq < seq {
                return Ok(true);
            }
            if header.seq == seq && header.stream == crate::frame::FrameStream::Ledger {
                found = Some(super::stored::decode_frame(at, header, &body)?);
            }
            Ok(false)
        })?;
        Ok(found)
    }

    /// The sealed records of `first` through `last`, as [`Place::sealed_between`] reads them,
    /// fetching only the directory pages and blocks that overlap the range: a small range in a
    /// long history reads a few blocks, not every later page and whole segments.
    async fn sealed_between(
        &self,
        first: u64,
        last: u64,
    ) -> Result<BTreeMap<u64, Record>, SessionError> {
        let key = self.key.frame_key();
        let pages = Pages {
            backend: &self.backend,
            addr: &self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: crate::directory::FANOUT,
        };
        let mut records = BTreeMap::new();
        for dir in [&self.head.ledger_dir, &self.head.transcript_dir] {
            if dir.as_ref().is_none_or(|root| root.last_seq < first) {
                continue;
            }
            for entry in pages
                .entries_between(dir.as_ref(), first, last)
                .await
                .map_err(SessionError::Directory)?
            {
                let layout = self
                    .sealed_layout(&entry.object, &key)
                    .await?
                    .ok_or_else(|| {
                        SessionError::Fold(format!(
                            "the directory names a missing segment {}",
                            entry.object
                        ))
                    })?;
                let blocks = &layout.index.blocks;
                for position in 0..blocks.len() {
                    // A block whose successor starts at or before `first` ends before it, and a
                    // block that starts after `last` holds nothing in the range.
                    if blocks
                        .get(position + 1)
                        .is_some_and(|next| next.first_seq <= first)
                        || blocks[position].first_seq > last
                    {
                        continue;
                    }
                    for (seq, record) in self
                        .sealed_block(&entry.object, &layout, position, &key)
                        .await?
                    {
                        if (first..=last).contains(&seq) && entry.keeps(seq) {
                            records.insert(seq, record);
                        }
                    }
                }
            }
        }
        Ok(records)
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
        // Group by segment and then by block, so each block that holds a wanted record is
        // fetched once however many of the records it holds.
        let mut by_object: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        let mut memo = self.take_pages();
        let mut located = Ok(());
        for &seq in seqs {
            match pages
                .find_in(self.head.ledger_dir.as_ref(), seq, &mut memo)
                .await
            {
                Ok(Some(entry)) => by_object.entry(entry.object).or_default().push(seq),
                Ok(None) => {
                    located = Err(damaged(seq));
                    break;
                }
                Err(error) => {
                    located = Err(SessionError::Directory(error));
                    break;
                }
            }
        }
        self.keep_pages(memo);
        located?;
        let mut records = Vec::new();
        for (object, seqs) in by_object {
            let layout = self.sealed_layout(&object, &key).await?.ok_or_else(|| {
                SessionError::Fold(format!("the directory names a missing segment {object}"))
            })?;
            let mut by_block: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
            for seq in seqs {
                let position = layout.block_for_seq(seq).ok_or_else(|| damaged(seq))?;
                by_block.entry(position).or_default().push(seq);
            }
            for (position, wanted) in by_block {
                let block = self.sealed_block(&object, &layout, position, &key).await?;
                for seq in wanted {
                    let record = block
                        .iter()
                        .find(|(found, _)| *found == seq)
                        .map(|(_, record)| record.clone())
                        .ok_or_else(|| damaged(seq))?;
                    records.push((seq, record));
                }
            }
        }
        records.sort_by_key(|(seq, _)| *seq);
        Ok(records)
    }

    /// The record with `seq` in sealed segment `object`, read from the one block that can hold
    /// it, or `None` when the segment has no such record. A missing segment is damage.
    async fn sealed_record(
        &self,
        object: &str,
        seq: u64,
        key: &crate::frame::FrameKey,
    ) -> Result<Option<Record>, SessionError> {
        let layout = self.sealed_layout(object, key).await?.ok_or_else(|| {
            SessionError::Fold(format!("the directory names a missing segment {object}"))
        })?;
        let Some(position) = layout.block_for_seq(seq) else {
            return Ok(None);
        };
        Ok(self
            .sealed_block(object, &layout, position, key)
            .await?
            .into_iter()
            .find(|(found, _)| *found == seq)
            .map(|(_, record)| record))
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
        self.records_between_kept_as(first, last, false).await
    }

    /// [`Self::records_between`], except that a kept record whose content was released from
    /// memory comes in its released form (ids kept, content blocks released) rather than making
    /// the read go to storage. For a reader that reads only ids, kinds and counts, such as the
    /// walk of a run's status, so a poll of a long run stays in memory.
    pub async fn records_between_released(
        &self,
        first: u64,
        last: u64,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        self.records_between_kept_as(first, last, true).await
    }

    /// The records of `first` through `last`; with `released`, kept records whose content was
    /// released come in their released form instead of sending the read to storage.
    async fn records_between_kept_as(
        &self,
        first: u64,
        last: u64,
        released: bool,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        let mut records = if self.place().sealed_reach(first) {
            self.storage_read()?;
            self.sealed_between(first, last).await?
        } else {
            BTreeMap::new()
        };
        let kept = self
            .reads_kept()
            .then(|| {
                if released {
                    self.kept.between_released(first, last)
                } else {
                    self.kept.between(first, last)
                }
            })
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

impl crate::held::HeldBytes for StoredEntry {
    fn heap_bytes(&self) -> usize {
        let StoredEntry {
            ordinal: _,
            message,
            next,
            state,
            open: _,
        } = self;
        message.heap_bytes() + next.heap_bytes() + state.heap_bytes()
    }
}

impl SealedCaches {
    /// What the session keeps of its sealed history, estimated by walking it (see
    /// `crate::held`). A reader's derived value is opaque here, so only its pointer counts.
    pub(super) fn held_bytes(&self) -> usize {
        self.entries.held_bytes() + self.kept_held_bytes()
    }

    /// What sealed reads keep: each kept segment layout's index entries, its leading or
    /// trailing state and its header, the kept directory pages, and the box holding them.
    fn kept_held_bytes(&self) -> usize {
        let Some(kept) = &self.kept else {
            return 0;
        };
        let layouts = kept.layouts.iter().map(|(name, layout)| {
            name.capacity() + std::mem::size_of::<SegmentLayout>() + layout.held_bytes()
        });
        std::mem::size_of::<KeptReads>()
            + kept.layouts.capacity() * std::mem::size_of::<(String, Arc<SegmentLayout>)>()
            + layouts.sum::<usize>()
            + kept.pages.held_bytes()
    }
}

#[cfg(test)]
mod tests;
