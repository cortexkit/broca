//! The id index: which sealed ledger records name a given id.
//!
//! Run ids, submission ids, send ids, call keys and refresh generations are random or unordered,
//! so a directory can't find them by range. The index maps each one to the seqs of every sealed
//! ledger record that names it, and the caller reads those records through the ledger directory
//! and decides which of them answers its question. Ids in records still in the open log are
//! found in memory (see `Session::find_id`). A seal moves a record and its ids out of the open
//! log in the same change, so an id is always in one of the two places.
//!
//! Which records name which ids is [`record_ids`]. It matches every record kind by name, so a new
//! kind can't be added without deciding what it names.
//!
//! **Shape.** The index is a set of sorted runs in tiered levels, like an LSM tree. An entry is
//! (keyed hash of the id, seq), and a run holds its entries sorted by that pair, in data pages of
//! at most [`PAGE_ENTRIES`]. A ref page lists data pages with their hash range and a Bloom filter,
//! and a run is the list of its ref pages. The index root, the one object the head names, lists
//! every level's runs. A lookup reads, per run, only the ref page whose range holds the hash, and
//! only the data page whose filter may hold it, so a miss usually reads no data page at all.
//!
//! **Merging in bounded steps.** Each seal adds one run at level 0. When a level holds
//! [`FANIN`] runs, its oldest `FANIN` runs are merged into one run at the next level. A merge
//! never runs all at once: every seal gives each merging level a share of entries to write, the
//! merge's cursor (the next entry to write, and the ref pages written so far) is committed with
//! the root, and the next seal continues from it. The share is `FANIN` times the entries the seal
//! added, so a level finishes its merge before `FANIN` more runs arrive, and no level holds more
//! than `2 * FANIN` runs. If one ever would, its merge finishes in that change. A merge's inputs
//! stay in their level until it finishes, so lookups never read a half-written run.
//!
//! Index pages are encrypted under the session's frame key, with the session id and the page's
//! own name in their authenticated data, as directory pages are.

use std::collections::BTreeMap;

use crate::backend::{BackendError, SessionAddr, SessionBackend, Version};
use crate::codec::{ByteBuf, Id128};
use crate::frame::FrameKey;
use crate::record::Record;

/// Entries per data page.
pub const PAGE_ENTRIES: usize = 512;
/// Runs a level collects before its oldest `FANIN` are merged into one run at the next level.
pub const FANIN: usize = 8;
const PAGE_CONTEXT: &[u8] = b"broca-idindex-v1";
const HASH_CONTEXT: &[u8] = b"broca-idindex-hash-v1";
const FILTER_BITS_PER_ENTRY: usize = 10;
const FILTER_HASHES: u64 = 7;

crate::values! {
    /// The kinds of id the index answers for. Each is a namespace of its own: the same string as
    /// a run id and as a send id are two different keys.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub enum IdKind {
        /// A run's id, named by the run's start, end and structured-result records: what a
        /// caller asking for an old run's status or result needs.
        1 RunId,
        /// A queued send's submission id, named by every record of the send's life.
        2 SubmissionId,
        /// A caller's send id, the idempotency key of `session.send`, steers and the session
        /// operations that take one.
        3 SendId,
        /// A tool call's `call_key`, the key late results, approvals and withdrawals name the
        /// call by.
        4 CallKey,
        /// A session refresh's generation.
        5 RefreshGeneration,
    }
}

impl IdKind {
    fn code(self) -> u8 {
        match self {
            IdKind::RunId => 1,
            IdKind::SubmissionId => 2,
            IdKind::SendId => 3,
            IdKind::CallKey => 4,
            IdKind::RefreshGeneration => 5,
        }
    }
}

/// The ids `record` names. Only ledger records are indexed: every id a transcript record names
/// is also named by a ledger record of the same run or send.
///
/// Fields owned by other components (a steer's receipt, an event's body) are read only for the
/// idempotency keys this list names, by their field names in today's records.
pub fn record_ids(record: &Record) -> Vec<(IdKind, String)> {
    let mut ids = Vec::new();
    let mut push = |kind: IdKind, id: Option<&str>| {
        if let Some(id) = id.filter(|id| !id.is_empty()) {
            ids.push((kind, id.to_owned()));
        }
    };
    match record {
        Record::Input(_)
        | Record::Assistant(_)
        | Record::ToolReply(_)
        | Record::ResultAppend(_)
        | Record::SetupStub(_)
        | Record::Compaction(_)
        | Record::Marker(_) => {}
        Record::RunStart(start) => {
            push(IdKind::RunId, Some(&start.run_id));
            push(IdKind::SubmissionId, start.submission_id.as_deref());
            push(IdKind::SendId, start.send_id.as_deref());
        }
        Record::RunEnd(end) => push(IdKind::RunId, Some(&end.run_id)),
        Record::StructuredResult(result) => push(IdKind::RunId, Some(&result.run_id)),
        // Records inside a run are found through the run, never by an id of their own.
        Record::StepStart(_)
        | Record::StepEnd(_)
        | Record::Attempt(_)
        | Record::BatchStart(_)
        | Record::BatchEnd(_)
        | Record::ToolOutcome(_)
        | Record::Pause(_)
        | Record::BudgetFire(_)
        | Record::CancelRequest(_) => {}
        Record::ToolIntent(intent) => push(IdKind::CallKey, intent.call_key.as_deref()),
        Record::ToolHeld(held) => push(IdKind::CallKey, Some(&held.call_key)),
        Record::QueuedSend(send) => {
            push(IdKind::SubmissionId, Some(&send.submission_id));
            push(IdKind::SendId, send.send_id.as_deref());
        }
        Record::Delivery(delivery) => push(IdKind::SubmissionId, Some(&delivery.submission_id)),
        Record::Retraction(retraction) => {
            push(IdKind::SubmissionId, Some(&retraction.submission_id));
        }
        Record::SendEnded(ended) => push(IdKind::SubmissionId, Some(&ended.submission_id)),
        Record::DeliveryOrdered(ordered) => {
            push(IdKind::SubmissionId, Some(&ordered.submission_id));
        }
        Record::SessionRefresh(refresh) => {
            push(
                IdKind::RefreshGeneration,
                Some(&refresh.generation.to_string()),
            );
        }
        // A steer's send id is part of its receipt, which the queue owns.
        Record::Steer(steer) => push(IdKind::SendId, json_str(steer.receipt.as_ref(), "send_id")),
        // Events carry the old record's fields whole; their idempotency keys keep their names.
        Record::Event(event) => {
            let body = event.body.as_ref();
            push(IdKind::SendId, json_str(body, "send_id"));
            push(IdKind::SubmissionId, json_str(body, "submission_id"));
            push(IdKind::CallKey, json_str(body, "call_key"));
        }
        Record::View(_)
        | Record::ViewApplied(_)
        | Record::WriterOpened(_)
        | Record::ChangeIntent(_)
        | Record::ChangeAborted(_) => {}
    }
    ids
}

fn json_str<'a>(value: Option<&'a serde_json::Value>, key: &str) -> Option<&'a str> {
    value?.get(key)?.as_str()
}

/// The key an id is filed under: a keyed hash, so the index pages' plaintext names no id.
pub fn id_hash(key: &[u8; 32], kind: IdKind, id: &str) -> Id128 {
    let mut input = Vec::with_capacity(1 + id.len());
    input.push(kind.code());
    input.extend_from_slice(id.as_bytes());
    let digest = crate::prune::keyed_digest(key, &input);
    Id128(digest.0[..16].try_into().expect("16 of 32 bytes"))
}

/// The key `id_hash` uses, derived from the session's data key.
pub fn hash_key(data: &crate::keys::DataKey) -> [u8; 32] {
    data.derive(HASH_CONTEXT)
}

crate::message! {
    /// One indexed id: its keyed hash and the seq of a sealed ledger record that names it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Entry {
        1 req hash: Id128,
        2 req seq: u64,
    }
}

crate::message! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct DataPage {
        1 rep entries: Entry,
    }
}

crate::message! {
    /// A data page, as its ref page lists it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct PageInfo {
        1 req object: String,
        2 req first: Id128,
        3 req last: Id128,
        4 req count: u32,
        /// A Bloom filter of the page's hashes.
        5 req filter: ByteBuf,
    }
}

crate::message! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct RefPage {
        1 rep pages: PageInfo,
    }
}

crate::message! {
    /// A ref page, as its run lists it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct RefInfo {
        1 req object: String,
        2 req first: Id128,
        3 req last: Id128,
    }
}

crate::message! {
    /// A run: its ref pages, in hash order.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Run {
        1 req entries: u64,
        2 rep refs: RefInfo,
    }
}

crate::message! {
    /// A merge in progress: the level's oldest `inputs` runs into one run at the next level.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Merge {
        1 req inputs: u32,
        /// The output's ref pages written so far, in hash order.
        2 rep done: RefInfo,
        3 req written: u64,
        /// Every entry below this one is written. Absent before the first step.
        4 opt next: Entry,
    }
}

crate::message! {
    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct Level {
        /// Oldest first.
        1 rep runs: Run,
        2 opt merge: Merge,
    }
}

crate::message! {
    /// The index root, the object the head names.
    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct IndexRoot {
        1 rep levels: Level,
    }
}

crate::message! {
    /// The ref pages of runs a merge retired. Their data pages and the ref pages themselves are
    /// deleted after the grace period, by reading each ref page for its data pages first.
    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct Retired {
        1 rep refs: String,
    }
}

#[derive(Debug)]
pub enum IndexError {
    Backend(BackendError),
    /// A page the index names is missing.
    Missing(String),
    /// A page failed to authenticate or decode.
    Corrupt(String),
}

impl From<BackendError> for IndexError {
    fn from(error: BackendError) -> Self {
        Self::Backend(error)
    }
}

fn aad(session: &Id128, name: &str) -> Vec<u8> {
    let mut aad = PAGE_CONTEXT.to_vec();
    aad.extend_from_slice(&session.0);
    aad.extend_from_slice(&(name.len() as u32).to_le_bytes());
    aad.extend_from_slice(name.as_bytes());
    aad
}

fn filter_positions(hash: &Id128, bits: u64) -> impl Iterator<Item = u64> {
    let h1 = u64::from_le_bytes(hash.0[..8].try_into().expect("8 bytes"));
    let h2 = u64::from_le_bytes(hash.0[8..].try_into().expect("8 bytes")) | 1;
    (0..FILTER_HASHES).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % bits)
}

fn filter(entries: &[Entry]) -> Vec<u8> {
    let bytes = (entries.len() * FILTER_BITS_PER_ENTRY).div_ceil(8).max(8);
    let mut filter = vec![0u8; bytes];
    for entry in entries {
        for position in filter_positions(&entry.hash, bytes as u64 * 8) {
            filter[(position / 8) as usize] |= 1 << (position % 8);
        }
    }
    filter
}

fn may_hold(filter: &[u8], hash: &Id128) -> bool {
    if filter.is_empty() {
        return true;
    }
    filter_positions(hash, filter.len() as u64 * 8)
        .all(|position| filter[(position / 8) as usize] & (1 << (position % 8)) != 0)
}

fn in_range(first: &Id128, last: &Id128, hash: &Id128) -> bool {
    first <= hash && hash <= last
}

/// Reads and writes one session's index pages.
pub struct IndexPages<'a, B: SessionBackend> {
    pub backend: &'a B,
    pub addr: &'a SessionAddr,
    pub session: Id128,
    pub key: &'a FrameKey,
}

/// What one change did to the index.
#[derive(Debug, Default)]
pub struct Updated {
    pub root: IndexRoot,
    /// Ref pages of the runs merges retired in this change.
    pub retired: Vec<String>,
    /// Entries merges wrote in this change.
    pub merged_entries: u64,
    /// Merges that had to finish within one change because their level reached `2 * FANIN`.
    pub forced: u32,
}

impl<B: SessionBackend> IndexPages<'_, B> {
    pub async fn read<M: crate::codec::Message>(&self, name: &str) -> Result<M, IndexError> {
        let sealed = self
            .backend
            .get_object(self.addr, name)
            .await?
            .ok_or_else(|| IndexError::Missing(name.to_owned()))?;
        let plain = self
            .key
            .open_unit(&aad(&self.session, name), &sealed)
            .ok_or_else(|| IndexError::Corrupt(name.to_owned()))?;
        M::decode(&plain).map_err(|_| IndexError::Corrupt(name.to_owned()))
    }

    pub async fn write<M: crate::codec::Message>(
        &self,
        name: &str,
        message: &M,
        at: Version,
    ) -> Result<(), IndexError> {
        let sealed = self
            .key
            .seal_unit(&aad(&self.session, name), &message.encode());
        self.backend
            .put_object(self.addr, name, &sealed, at)
            .await?;
        Ok(())
    }

    /// Writes `entries`, already sorted, as data pages named `<prefix>-<n>` and one ref page
    /// named `<prefix>-r`. Returns the ref page, or `None` for no entries.
    async fn write_entries(
        &self,
        entries: &[Entry],
        prefix: &str,
        at: Version,
    ) -> Result<Option<RefInfo>, IndexError> {
        let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
            return Ok(None);
        };
        let mut pages = Vec::new();
        for (n, chunk) in entries.chunks(PAGE_ENTRIES).enumerate() {
            let object = format!("{prefix}-{n}");
            self.write(
                &object,
                &DataPage {
                    entries: chunk.to_vec(),
                },
                at,
            )
            .await?;
            pages.push(PageInfo {
                object,
                first: chunk[0].hash,
                last: chunk[chunk.len() - 1].hash,
                count: chunk.len() as u32,
                filter: ByteBuf(filter(chunk)),
            });
        }
        let object = format!("{prefix}-r");
        self.write(&object, &RefPage { pages }, at).await?;
        Ok(Some(RefInfo {
            object,
            first: first.hash,
            last: last.hash,
        }))
    }

    /// The seqs every run in `root` holds for `hash`, ascending.
    pub async fn lookup(&self, root: &IndexRoot, hash: &Id128) -> Result<Vec<u64>, IndexError> {
        let mut seqs = Vec::new();
        for level in &root.levels {
            for run in &level.runs {
                for info in run
                    .refs
                    .iter()
                    .filter(|info| in_range(&info.first, &info.last, hash))
                {
                    let refs: RefPage = self.read(&info.object).await?;
                    for page in refs.pages.iter().filter(|page| {
                        in_range(&page.first, &page.last, hash) && may_hold(&page.filter.0, hash)
                    }) {
                        let data: DataPage = self.read(&page.object).await?;
                        seqs.extend(
                            data.entries
                                .iter()
                                .filter(|entry| &entry.hash == hash)
                                .map(|entry| entry.seq),
                        );
                    }
                }
            }
        }
        seqs.sort_unstable();
        seqs.dedup();
        Ok(seqs)
    }

    /// Adds a run of `entries` at level 0, then gives every merging level its share of work.
    /// Every object is named under `prefix`, the change's own.
    pub async fn update(
        &self,
        root: IndexRoot,
        entries: Vec<Entry>,
        prefix: &str,
        at: Version,
    ) -> Result<Updated, IndexError> {
        let share = (entries.len().max(PAGE_ENTRIES) * FANIN) as u64;
        self.update_with_share(root, entries, prefix, at, share)
            .await
    }

    /// `update` with each merging level's share of entries given, so tests can make merges
    /// take many changes.
    pub(crate) async fn update_with_share(
        &self,
        mut root: IndexRoot,
        mut entries: Vec<Entry>,
        prefix: &str,
        at: Version,
        share: u64,
    ) -> Result<Updated, IndexError> {
        entries.sort_unstable();
        entries.dedup();
        if root.levels.is_empty() {
            root.levels.push(Level::default());
        }
        if let Some(info) = self
            .write_entries(&entries, &format!("{prefix}/i0"), at)
            .await?
        {
            root.levels[0].runs.push(Run {
                entries: entries.len() as u64,
                refs: vec![info],
            });
        }
        let mut updated = Updated::default();
        let mut level = 0;
        while level < root.levels.len() {
            let runs = root.levels[level].runs.len();
            if root.levels[level].merge.is_none() && runs >= FANIN {
                root.levels[level].merge = Some(Merge {
                    inputs: FANIN as u32,
                    done: Vec::new(),
                    written: 0,
                    next: None,
                });
            }
            if root.levels[level].merge.is_some() {
                let forced = runs >= 2 * FANIN;
                let budget = if forced { u64::MAX } else { share };
                updated.forced += u32::from(forced);
                let mut step = 0;
                let finished = loop {
                    let (written, finished) = self
                        .merge_step(
                            &mut root.levels[level],
                            budget,
                            &format!("{prefix}/m{level}-{step}"),
                            at,
                        )
                        .await?;
                    updated.merged_entries += written;
                    step += 1;
                    if finished || !forced {
                        break finished;
                    }
                };
                if finished {
                    let current = &mut root.levels[level];
                    let merge = current.merge.take().expect("a merge was running");
                    let inputs: Vec<Run> = current.runs.drain(..merge.inputs as usize).collect();
                    updated.retired.extend(
                        inputs
                            .iter()
                            .flat_map(|run| run.refs.iter().map(|info| info.object.clone())),
                    );
                    if root.levels.len() == level + 1 {
                        root.levels.push(Level::default());
                    }
                    if merge.written > 0 {
                        root.levels[level + 1].runs.push(Run {
                            entries: merge.written,
                            refs: merge.done,
                        });
                    }
                }
            }
            level += 1;
        }
        while root
            .levels
            .last()
            .is_some_and(|level| level.runs.is_empty())
        {
            root.levels.pop();
        }
        updated.root = root;
        Ok(updated)
    }

    /// Writes up to `budget` entries of `level`'s merge, as one ref page named
    /// `<prefix>-r`. Returns the entries written and whether the merge is finished.
    async fn merge_step(
        &self,
        level: &mut Level,
        budget: u64,
        prefix: &str,
        at: Version,
    ) -> Result<(u64, bool), IndexError> {
        let merge = level.merge.as_mut().expect("a merge is running");
        let from = merge.next.unwrap_or(Entry {
            hash: Id128([0; 16]),
            seq: 0,
        });
        let mut cursors = Vec::new();
        for run in &level.runs[..merge.inputs as usize] {
            cursors.push(RunCursor::seek(self, run, from).await?);
        }
        // A k-way merge over at most FANIN cursors, so a linear scan for the smallest is enough.
        let mut out = Vec::new();
        loop {
            let smallest = cursors
                .iter()
                .enumerate()
                .filter_map(|(at, cursor)| cursor.peek().map(|entry| (entry, at)))
                .min();
            let Some((entry, at_cursor)) = smallest else {
                break;
            };
            if out.len() as u64 >= budget {
                merge.next = Some(entry);
                break;
            }
            out.push(entry);
            cursors[at_cursor].advance(self).await?;
        }
        let finished = cursors.iter().all(|cursor| cursor.peek().is_none());
        if let Some(info) = self.write_entries(&out, prefix, at).await? {
            merge.done.push(info);
        }
        merge.written += out.len() as u64;
        Ok((out.len() as u64, finished))
    }

    /// Deletes the runs a merge retired: each ref page's data pages, then the ref page. A ref
    /// page already gone means an earlier collection deleted it and its data pages.
    pub async fn delete_retired(&self, refs: &[String], at: Version) -> Result<(), IndexError> {
        for object in refs {
            let page: RefPage = match self.read(object).await {
                Ok(page) => page,
                Err(IndexError::Missing(_)) => continue,
                Err(error) => return Err(error),
            };
            for data in &page.pages {
                self.backend
                    .delete_object(self.addr, &data.object, at)
                    .await?;
            }
            self.backend.delete_object(self.addr, object, at).await?;
        }
        Ok(())
    }

    /// Every object the index named by `root` consists of: for tests and audits.
    pub async fn objects(&self, root: &IndexRoot) -> Result<Vec<String>, IndexError> {
        let mut objects = Vec::new();
        for level in &root.levels {
            let merging = level.merge.iter().flat_map(|merge| merge.done.iter());
            for info in level
                .runs
                .iter()
                .flat_map(|run| run.refs.iter())
                .chain(merging)
            {
                let page: RefPage = self.read(&info.object).await?;
                objects.extend(page.pages.into_iter().map(|page| page.object));
                objects.push(info.object.clone());
            }
        }
        Ok(objects)
    }
}

/// Reads one run's entries in order, a data page at a time.
struct RunCursor {
    refs: Vec<RefInfo>,
    next_ref: usize,
    pages: Vec<PageInfo>,
    next_page: usize,
    entries: Vec<Entry>,
    at: usize,
}

impl RunCursor {
    /// A cursor at the run's first entry at or above `from`.
    async fn seek<B: SessionBackend>(
        pages: &IndexPages<'_, B>,
        run: &Run,
        from: Entry,
    ) -> Result<Self, IndexError> {
        let mut cursor = Self {
            next_ref: run.refs.partition_point(|info| info.last < from.hash),
            refs: run.refs.clone(),
            pages: Vec::new(),
            next_page: 0,
            entries: Vec::new(),
            at: 0,
        };
        cursor.load_ref(pages).await?;
        cursor.next_page = cursor.pages.partition_point(|page| page.last < from.hash);
        cursor.load_page(pages).await?;
        while cursor.peek().is_some_and(|entry| entry < from) {
            cursor.advance(pages).await?;
        }
        Ok(cursor)
    }

    fn peek(&self) -> Option<Entry> {
        self.entries.get(self.at).copied()
    }

    async fn advance<B: SessionBackend>(
        &mut self,
        pages: &IndexPages<'_, B>,
    ) -> Result<(), IndexError> {
        self.at += 1;
        if self.at < self.entries.len() {
            return Ok(());
        }
        self.load_page(pages).await
    }

    /// Loads the next data page, moving to the next ref page when this one is used up.
    async fn load_page<B: SessionBackend>(
        &mut self,
        pages: &IndexPages<'_, B>,
    ) -> Result<(), IndexError> {
        self.entries.clear();
        self.at = 0;
        while self.entries.is_empty() {
            if self.next_page >= self.pages.len() {
                if self.next_ref >= self.refs.len() {
                    return Ok(());
                }
                self.load_ref(pages).await?;
                self.next_page = 0;
                continue;
            }
            let page: DataPage = pages.read(&self.pages[self.next_page].object).await?;
            self.next_page += 1;
            self.entries = page.entries;
        }
        Ok(())
    }

    async fn load_ref<B: SessionBackend>(
        &mut self,
        pages: &IndexPages<'_, B>,
    ) -> Result<(), IndexError> {
        self.pages.clear();
        if let Some(info) = self.refs.get(self.next_ref) {
            let page: RefPage = pages.read(&info.object).await?;
            self.pages = page.pages;
            self.next_ref += 1;
        }
        Ok(())
    }
}

/// The ids of a set of ledger records, as index entries.
pub fn entries_for(key: &[u8; 32], records: &[(u64, Record)]) -> Vec<Entry> {
    let mut entries = Vec::new();
    for (seq, record) in records {
        for (kind, id) in record_ids(record) {
            entries.push(Entry {
                hash: id_hash(key, kind, &id),
                seq: *seq,
            });
        }
    }
    entries
}

/// The open log's id-naming ledger records, by id, so a lookup finds ids not sealed yet.
#[derive(Debug, Default, Clone)]
pub struct OpenIds {
    ids: BTreeMap<(IdKind, String), Vec<u64>>,
    records: BTreeMap<u64, Record>,
}

impl OpenIds {
    pub fn add(&mut self, seq: u64, record: &Record) {
        let ids = record_ids(record);
        if ids.is_empty() {
            return;
        }
        for id in ids {
            self.ids.entry(id).or_default().push(seq);
        }
        self.records.insert(seq, record.clone());
    }

    pub fn clear(&mut self) {
        self.ids.clear();
        self.records.clear();
    }

    pub fn find(&self, kind: IdKind, id: &str) -> Vec<(u64, Record)> {
        self.ids
            .get(&(kind, id.to_owned()))
            .into_iter()
            .flatten()
            .map(|seq| (*seq, self.records[seq].clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests;
