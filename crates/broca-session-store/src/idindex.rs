//! The id index: which sealed ledger records name a given id.
//!
//! Run ids, submission ids, send ids, call keys and refresh generations are random or unordered,
//! so a directory can't find them by range. The index maps each one to the seqs of every sealed
//! ledger record that names it, and the caller reads those records through the ledger directory
//! and decides which of them answers its question. Ids in records still in the open log are
//! found through the open log's own index in memory, [`OpenIds`] (see `Session::find_id`). A seal
//! moves a record and its ids out of the open log in the same change, so an id is always in one
//! of the two places.
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

use std::collections::{BTreeMap, BTreeSet};

use crate::backend::{BackendError, SessionAddr, SessionBackend, Version};
use crate::codec::{ByteBuf, Id128};
use crate::frame::FrameKey;
use crate::log::FramePlace;
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
        /// The `send_id` of a `session.refresh_policy`, a namespace of its own: the same string
        /// as a send's or a flush's send id is another key.
        6 RefreshPolicySendId,
        /// The `send_id` of a `session.flush_prefix`.
        7 PrefixFlushSendId,
        /// The generation of a refresh a run start applied, named by that application.
        8 AppliedGeneration,
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
            IdKind::RefreshPolicySendId => 6,
            IdKind::PrefixFlushSendId => 7,
            IdKind::AppliedGeneration => 8,
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
        | Record::Marker(_)
        | Record::EntryFacts(_)
        | Record::TurnFacts(_) => {}
        Record::RunStart(start) => {
            push(IdKind::RunId, Some(&start.run_id));
            push(IdKind::SubmissionId, start.submission_id.as_deref());
            push(IdKind::SendId, start.send_id.as_deref());
        }
        Record::RunEnd(end) => push(IdKind::RunId, Some(&end.run_id)),
        Record::Resumed(resumed) => push(IdKind::RunId, Some(&resumed.run_id)),
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
        | Record::CancelRequest(_)
        | Record::CeilingDecided(_)
        | Record::OutputCapLowered(_)
        | Record::StructuredDowngraded(_)
        | Record::Warmed(_)
        | Record::ConfigRefrozen(_)
        | Record::AuthRefrozen(_)
        | Record::StepRefused(_) => {}
        // Whether a run is billed is read from the operational fold's owed set, never looked up.
        Record::Billed(_) => {}
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
        Record::RefreshPolicy(policy) => {
            push(IdKind::RefreshPolicySendId, Some(&policy.send_id));
        }
        Record::PrefixFlush(flush) => push(IdKind::PrefixFlushSendId, Some(&flush.send_id)),
        Record::RefreshApplied(applied) => {
            push(
                IdKind::AppliedGeneration,
                Some(&applied.generation.to_string()),
            );
        }
        // A session enrolls once, and the operational fold keeps the enrollment whole.
        Record::CompactionEnrolled(_) => {}
        // A move names no id a read looks a record up by.
        Record::Moved(_) => {}
        // A fork's session is small and short-lived: its reads fold every record.
        Record::Fork(_) => {}
        // A steer's send id is part of its receipt, which the queue owns.
        Record::Steer(steer) => {
            let receipt = steer
                .receipt
                .as_ref()
                .map(broca_protocol::json::Json::parse);
            push(IdKind::SendId, json_str(receipt.as_ref(), "send_id"));
        }
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
        | Record::ChangeAborted(_)
        | Record::TranscriptReverted(_)
        | Record::RecoveryCut(_) => {}
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

pub(crate) fn aad(session: &Id128, name: &str) -> Vec<u8> {
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
        // A merge names the runs it reads by counting from its level's oldest, and a level's runs
        // stay until its merge finishes, so a count past them wasn't written by this code.
        if root.levels.iter().any(|level| {
            level
                .merge
                .as_ref()
                .is_some_and(|merge| merge.inputs as usize > level.runs.len())
        }) {
            return Err(IndexError::Corrupt(
                "index root: a merge counts more runs than its level holds".into(),
            ));
        }
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
        // The count so far comes from the stored root, so it is added to with a check, before
        // anything is written.
        let written = merge.written.checked_add(out.len() as u64).ok_or_else(|| {
            IndexError::Corrupt("index root: a merge's count of entries written overflows".into())
        })?;
        if let Some(info) = self.write_entries(&out, prefix, at).await? {
            merge.done.push(info);
        }
        merge.written = written;
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

/// Whether `record` names `id` as a `kind`: what a lookup checks of every record it reads back,
/// since two ids whose hashes collide are filed under the same key.
pub fn names(record: &Record, kind: IdKind, id: &str) -> bool {
    record_ids(record)
        .iter()
        .any(|(named_kind, named)| *named_kind == kind && named == id)
}

/// Whether a lookup reads a record that names `ids` back from the open log by its place, rather
/// than taking it from memory: when every id it names is a tool call's key. Those records (a
/// call's intent, a held call) are written per tool call, so they are the bulk of an open log's
/// id-naming records and grow with every call; a read from memory only looks up the calls of
/// the latest run, which the kept records hold. Every other record that names an id is written
/// per run, per send or per plan change (a run's start and end, a send's records, a refresh's),
/// so there are few, and reads from memory only look them up in any run.
fn found_by_place(ids: &[(IdKind, String)]) -> bool {
    ids.iter().all(|(kind, _)| *kind == IdKind::CallKey)
}

thread_local! {
    static OPEN_RECORD_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// How many records id lookups on this thread have read back from an open log, one at a time.
/// Every other lookup is answered from memory, so a test bounds with it what a step or a poll
/// reads.
pub fn open_record_reads() -> u64 {
    OPEN_RECORD_READS.with(std::cell::Cell::get)
}

pub(crate) fn count_open_record_read() {
    OPEN_RECORD_READS.with(|reads| reads.set(reads.get() + 1));
}

/// The ids the open log's ledger records name, so a lookup finds ids not sealed yet (see
/// `Session::find_id`).
///
/// Each id is filed under its keyed hash, as the sealed index files it, with the seq of the
/// record naming it, so the index holds no id's text. Where the record itself is depends on what
/// it names (see [`found_by_place`]):
/// - a record written per tool call is found by its seq: in the kept records while it is in the
///   latest run, otherwise read back from the open log at the place kept for it here. These are
///   the bulk, and each costs only its place;
/// - any other stays in memory: in the session's kept records while they hold it, and here once
///   they drop it, so these lookups never read storage.
///
/// The index is one pointer wide: a session, and every future and actor that carries one by
/// value, holds only the box.
#[derive(Clone)]
pub struct OpenIds(Box<Filed>);

/// What an [`OpenIds`] holds.
#[derive(Clone)]
struct Filed {
    key: [u8; 32],
    /// Every id filed, as its hash, with the seq of a record naming it.
    ids: BTreeSet<(Id128, u64)>,
    /// The records taken from memory that the kept records don't hold, by seq.
    held: BTreeMap<u64, Record>,
    /// The seqs of the records taken from memory that the kept records hold, in order.
    in_kept: Vec<u64>,
    /// Where the frame of each other record that names an id lies in the open log, in seq order.
    places: Vec<(u64, FramePlace)>,
}

/// Where a record a lookup found is (see [`OpenIds::find`]).
#[derive(Debug, Clone, Copy)]
pub enum OpenRecord<'a> {
    /// Held by the index.
    Held(&'a Record),
    /// Held by the session's kept records.
    Kept,
    /// In the kept records if they still hold its seq, otherwise only in the open log, here.
    At(FramePlace),
    /// Filed, yet neither held, kept nor placed: the index lost track of it, which a lookup
    /// reports as damage rather than as an id not found.
    Lost,
}

impl OpenIds {
    /// An empty index, filing ids under `key` (see [`hash_key`]).
    pub fn new(key: [u8; 32]) -> Self {
        Self(Box::new(Filed {
            key,
            ids: BTreeSet::new(),
            held: BTreeMap::new(),
            in_kept: Vec::new(),
            places: Vec::new(),
        }))
    }

    /// The hash `id` of `kind` is filed under, here and in the sealed index.
    pub fn hash(&self, kind: IdKind, id: &str) -> Id128 {
        id_hash(&self.0.key, kind, id)
    }

    /// Files the ids `record`, at `seq` with its frame at `place`, names, holding a copy of the
    /// record when a lookup takes it from memory: for a record the kept records don't hold.
    pub fn add(&mut self, seq: u64, record: &Record, place: FramePlace) {
        if self.file(seq, record, place) {
            self.hold(seq, record.clone());
        }
    }

    /// Files the ids `record`, at `seq` with its frame at `place`, names. Returns whether a
    /// lookup takes the record from memory: the caller then either leaves it with the kept
    /// records and says so ([`Self::kept_holds`]), or hands it over ([`Self::hold`]).
    pub(crate) fn file(&mut self, seq: u64, record: &Record, place: FramePlace) -> bool {
        let ids = record_ids(record);
        if ids.is_empty() {
            return false;
        }
        for (kind, id) in &ids {
            self.0.ids.insert((id_hash(&self.0.key, *kind, id), seq));
        }
        if !found_by_place(&ids) {
            return true;
        }
        match self.0.places.last() {
            Some((last, _)) if *last >= seq => {
                let at = self.0.places.partition_point(|(filed, _)| *filed < seq);
                if self
                    .0
                    .places
                    .get(at)
                    .is_some_and(|(filed, _)| *filed == seq)
                {
                    self.0.places[at].1 = place;
                } else {
                    self.0.places.insert(at, (seq, place));
                }
            }
            _ => self.0.places.push((seq, place)),
        }
        false
    }

    /// The record at `seq`, filed and taken from memory, is in the session's kept records.
    pub(crate) fn kept_holds(&mut self, seq: u64) {
        match self.0.in_kept.last() {
            Some(last) if *last >= seq => {
                if let Err(at) = self.0.in_kept.binary_search(&seq) {
                    self.0.in_kept.insert(at, seq);
                }
            }
            _ => self.0.in_kept.push(seq),
        }
    }

    /// Holds the record at `seq`, filed and taken from memory, which the kept records don't hold.
    pub(crate) fn hold(&mut self, seq: u64, record: Record) {
        self.0.held.insert(seq, record);
    }

    /// Takes over `record`, at `seq`, which the kept records just dropped, if they held it for
    /// this index, so it stays in memory with no second copy. Any other record is dropped.
    pub(crate) fn adopt(&mut self, seq: u64, record: Record) {
        if let Ok(at) = self.0.in_kept.binary_search(&seq) {
            self.0.in_kept.remove(at);
            self.0.held.insert(seq, record);
        }
    }

    /// Every record filed under the hash of `id` as a `kind`, in seq order, with where it is.
    /// Another id with the same hash finds the same records, so the caller checks each one names
    /// `id` ([`names`]).
    pub fn find(&self, kind: IdKind, id: &str) -> Vec<(u64, OpenRecord<'_>)> {
        let hash = self.hash(kind, id);
        self.0
            .ids
            .range((hash, 0)..=(hash, u64::MAX))
            .map(|(_, seq)| {
                let at = if let Some(record) = self.0.held.get(seq) {
                    OpenRecord::Held(record)
                } else if self.0.in_kept.binary_search(seq).is_ok() {
                    OpenRecord::Kept
                } else {
                    let at = self.0.places.partition_point(|(filed, _)| filed < seq);
                    match self.0.places.get(at) {
                        Some((filed, place)) if filed == seq => OpenRecord::At(*place),
                        _ => OpenRecord::Lost,
                    }
                };
                (*seq, at)
            })
            .collect()
    }

    /// What the index holds, for tests that measure it.
    #[cfg(test)]
    pub(crate) fn parts(&mut self) -> OpenIdsParts<'_> {
        OpenIdsParts {
            ids: &mut self.0.ids,
            held: &mut self.0.held,
            in_kept: &mut self.0.in_kept,
            places: &mut self.0.places,
        }
    }
}

/// What an [`OpenIds`] holds, for tests that measure it.
#[cfg(test)]
pub(crate) struct OpenIdsParts<'a> {
    /// The ids filed.
    pub ids: &'a mut BTreeSet<(Id128, u64)>,
    /// The records held in memory.
    pub held: &'a mut BTreeMap<u64, Record>,
    /// The seqs of the records the kept records hold for the index.
    pub in_kept: &'a mut Vec<u64>,
    /// The places of the records read back from the log.
    pub places: &'a mut Vec<(u64, FramePlace)>,
}

#[cfg(test)]
mod tests;

impl crate::held::HeldBytes for OpenIds {
    fn heap_bytes(&self) -> usize {
        // The box, then what it holds: the ids filed, the records held in memory, the log
        // sequence numbers the kept records hold for the index, and the places of the records
        // found by place.
        let filed = &self.0;
        std::mem::size_of::<Filed>()
            + filed.ids.heap_bytes()
            + filed.held.heap_bytes()
            + filed.in_kept.heap_bytes()
            + filed.places.capacity() * std::mem::size_of::<(u64, FramePlace)>()
    }
}
