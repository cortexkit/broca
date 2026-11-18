//! What a session keeps in memory so a read can be answered without touching storage.
//!
//! **The open log's records.** A writer keeps the records of its latest run, from the run's
//! start on, so the facts of the run a caller polls are walked from memory. A read-only view
//! keeps every record it read, so it never reads the log a second time. Each keeps exactly the
//! open log's records from one seq on, and every record appended after it, so a read that asks
//! for records from that seq on is answered whole.
//!
//! **Reading from memory only.** A session's owner can answer a read from what it holds without
//! waiting on storage: inside [`Session::in_memory`], every read that would touch the backend
//! refuses with [`SessionError::NotInMemory`] instead, before it starts any I/O. The caller then
//! reads that history itself, from a view loaded at the same head version and log length (see
//! [`Session::view_at`]): sealed objects never change, and the durable part of an open log only
//! grows.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::backend::SessionBackend;
use crate::record::Record;

use super::{Session, SessionError};

/// Where a view's open log keeps its checkpoint. The checkpoint is always the log's first frame,
/// so its length is enough to read it back. A frame is never empty, so its length is never zero,
/// and the `Option` that holds this uses zero to mean `None` rather than taking more room.
#[derive(Debug, Clone, Copy)]
struct CheckpointFrame {
    len: std::num::NonZeroU32,
    /// The last ledger seq the checkpoint folds in: a read from a seq at or below it starts in
    /// an older log.
    covers_through: u64,
}

/// The open log's records a session holds in memory: every one from seq `from` on.
///
/// The buffer is sized for what it keeps. A load hands it the log's records one by one, never
/// the whole log, and leaves a writer's buffer no larger than the records it kept; a new run
/// start drops the buffer that held the run before with its records, rather than clearing it and
/// keeping its capacity.
#[derive(Debug, Default)]
pub(crate) struct KeptRecords {
    /// Every record of the open log with a seq at or past this one is in `records`.
    from: u64,
    records: Vec<(u64, Record)>,
    /// No record of the open log is below `from`: `records` is the whole log after its
    /// checkpoint.
    whole: bool,
    /// The seq and run id of the latest run start, when it is in `records`.
    latest_start: Option<(u64, String)>,
    /// Whether a new run start drops the records before it, as a writer's copy does to stay
    /// bounded by one run. A view keeps everything it read.
    per_run: bool,
    /// Where a view's open log keeps its checkpoint, so the view can serve that log whole (see
    /// [`Self::view_checkpoint_for`]) without holding the checkpoint a second time: its state is
    /// already folded into the view.
    checkpoint: Option<CheckpointFrame>,
    /// The seqs, ascending, of the kept records whose message content left memory with the
    /// window message they placed (see [`Self::release`]). Such a record is kept in its released
    /// form, which holds its ids but not its content, so a read of records that needs content
    /// reads it from storage instead ([`Self::between`]).
    released: Vec<u64>,
}

impl KeptRecords {
    /// A writer's records of a log that holds none past `next_seq` yet, or whose earlier records
    /// aren't kept: the log a seal, a revert or a recovery has just written.
    pub(crate) fn from_next(next_seq: u64, whole: bool) -> Self {
        Self {
            from: next_seq,
            records: Vec::new(),
            whole,
            latest_start: None,
            per_run: true,
            checkpoint: None,
            released: Vec::new(),
        }
    }

    /// The records a session keeps of the log it is loading, taken in one by one with
    /// [`Self::load`] and finished with [`Self::loaded`]: a view keeps every record it reads
    /// (`per_run` false); a writer keeps those from the latest run start on, or none when the log
    /// holds no run start.
    pub(crate) fn loading(per_run: bool) -> Self {
        Self {
            from: 0,
            records: Vec::new(),
            whole: true,
            latest_start: None,
            per_run,
            checkpoint: None,
            released: Vec::new(),
        }
    }

    /// Takes in the loaded log's next record, at `seq`, and returns it when it isn't kept. A
    /// writer's copy keeps the records of the run it reads, from its start on, and at the next run
    /// start hands them to `dropped`; it keeps nothing before the first run start. While loading,
    /// the buffer is reused from run to run, and [`Self::loaded`] sizes it for the run it keeps.
    pub(crate) fn load(
        &mut self,
        seq: u64,
        record: Record,
        mut dropped: impl FnMut(u64, Record),
    ) -> Option<Record> {
        if !self.per_run {
            if let Record::RunStart(start) = &record {
                self.latest_start = Some((seq, start.run_id.clone()));
            }
            self.records.push((seq, record));
            return None;
        }
        match &record {
            Record::RunStart(start) => {
                // Only a run start that the log opens with leaves no record before it.
                self.whole &= self.records.is_empty();
                self.from = seq;
                self.latest_start = Some((seq, start.run_id.clone()));
                for (seq, record) in self.records.drain(..) {
                    dropped(seq, record);
                }
                self.released.clear();
            }
            _ if self.latest_start.is_some() => {}
            _ => {
                self.whole = false;
                return Some(record);
            }
        }
        self.records.push((seq, record));
        None
    }

    /// Ends a load whose log's next record takes seq `next_seq`, leaving a writer's buffer no
    /// larger than the records it keeps.
    pub(crate) fn loaded(&mut self, next_seq: u64) {
        if self.per_run {
            if self.latest_start.is_none() {
                // No run start: the writer keeps the records it appends from here on.
                self.from = next_seq;
            }
            self.records.shrink_to_fit();
        }
    }

    /// Takes in one appended record at `seq`. Returns the records a run start dropped, the run's
    /// before, which the caller may take over.
    pub(crate) fn note(&mut self, seq: u64, record: &Record) -> Vec<(u64, Record)> {
        let mut dropped = Vec::new();
        if let Record::RunStart(start) = record {
            if self.per_run {
                // The records before the new run belong to runs a caller reads from storage.
                self.whole &= self.records.is_empty() && self.from == seq;
                // The buffer goes with them: one sized for a long run must not outlive it.
                dropped = std::mem::take(&mut self.records);
                self.released = Vec::new();
                self.from = seq;
            }
            self.latest_start = Some((seq, start.run_id.clone()));
        }
        self.push(seq, record.clone());
        dropped
    }

    /// Keeps the appended `record` at `seq`. A full buffer grows by a quarter of its length, not
    /// by doubling as a vector does by default, so it never holds much more room than records:
    /// each slot costs a whole record's size, and a writer keeps its buffer for as long as the
    /// session is open.
    fn push(&mut self, seq: u64, record: Record) {
        if self.records.len() == self.records.capacity() {
            self.records.reserve_exact((self.records.len() / 4).max(4));
        }
        self.records.push((seq, record));
    }

    /// The kept record at `seq`, if one is kept whole.
    pub(crate) fn get(&self, seq: u64) -> Option<&Record> {
        if self.released.binary_search(&seq).is_ok() {
            return None;
        }
        self.records
            .binary_search_by_key(&seq, |(kept, _)| *kept)
            .ok()
            .map(|at| &self.records[at].1)
    }

    /// Every kept record from `first` through `last`, or `None` when records from `first` on
    /// aren't all kept whole.
    pub(crate) fn between(&self, first: u64, last: u64) -> Option<Vec<(u64, Record)>> {
        let start = self.released.partition_point(|seq| *seq < first);
        if self.released.get(start).is_some_and(|seq| *seq <= last) {
            return None;
        }
        self.between_released(first, last)
    }

    /// [`Self::between`], with each released record in its released form: its ids, and its
    /// content blocks in their released form (`ContentBlock::released`). For a reader that
    /// reads only what such a record keeps.
    pub(crate) fn between_released(&self, first: u64, last: u64) -> Option<Vec<(u64, Record)>> {
        if first < self.from && !self.whole {
            return None;
        }
        let start = self.records.partition_point(|(seq, _)| *seq < first);
        Some(
            self.records[start..]
                .iter()
                .take_while(|(seq, _)| *seq <= last)
                .cloned()
                .collect(),
        )
    }

    /// Every record of the open log, when they are all kept whole.
    pub(crate) fn whole(&self) -> Option<&[(u64, Record)]> {
        (self.whole && self.released.is_empty()).then_some(self.records.as_slice())
    }

    /// The kept record at `seq`, whole or released, if one is kept.
    pub(crate) fn get_any(&self, seq: u64) -> Option<&Record> {
        self.records
            .binary_search_by_key(&seq, |(kept, _)| *kept)
            .ok()
            .map(|at| &self.records[at].1)
    }

    /// Keeps `released` in place of the record at `seq`: the same record, its message content
    /// left in memory only as its released form. The window message it placed released the
    /// same content, so no holder in the session keeps it.
    pub(crate) fn release(&mut self, seq: u64, released: Record) {
        let Ok(at) = self.records.binary_search_by_key(&seq, |(kept, _)| *kept) else {
            return;
        };
        self.records[at].1 = released;
        if let Err(at) = self.released.binary_search(&seq) {
            self.released.insert(at, seq);
        }
    }

    /// Records where a view's open log keeps its checkpoint: in its first frame, `len` bytes
    /// long, covering the ledger through seq `covers_through`. A writer records none: it keeps
    /// one run's records, not the whole log.
    pub(crate) fn view_checkpoint(&mut self, len: std::num::NonZeroU32, covers_through: u64) {
        if !self.per_run {
            self.checkpoint = Some(CheckpointFrame {
                len,
                covers_through,
            });
        }
    }

    /// For a view whose open log holds `seq` (its checkpoint covers less than `seq`), the length
    /// of that log's checkpoint frame. The view kept every record of the log, so that one frame,
    /// read back, is all a read from `seq` on still needs.
    pub(crate) fn view_checkpoint_for(&self, seq: u64) -> Option<u32> {
        self.checkpoint
            .filter(|checkpoint| checkpoint.covers_through < seq)
            .map(|checkpoint| checkpoint.len.get())
    }

    /// The kept records through seq `last`. A view keeps them; only a writer releases any.
    pub(crate) fn through(&self, last: u64) -> Vec<(u64, Record)> {
        debug_assert!(
            self.released.is_empty(),
            "a view's records are never released"
        );
        let end = self.records.partition_point(|(seq, _)| *seq <= last);
        self.records[..end].to_vec()
    }

    /// The retained suffix, without cloning the rest of the run for a tail lookup. Its ledger
    /// records are whole: only transcript records are ever released.
    pub(super) fn tail(&self) -> (&[(u64, Record)], bool) {
        (&self.records, self.whole)
    }

    /// The seq of the run start of the run `run_id`, when it is the latest run start and kept.
    pub(crate) fn start_of(&self, run_id: &str) -> Option<u64> {
        self.latest_start
            .as_ref()
            .filter(|(_, id)| id == run_id)
            .map(|(seq, _)| *seq)
    }

    /// The records kept, for diagnostics and tests.
    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }
}

/// Reads from memory only while it lives (see [`Session::in_memory`]).
pub struct InMemory<'a> {
    flag: &'a AtomicBool,
    refused: &'a AtomicBool,
}

impl InMemory<'_> {
    /// Whether a read refused because it needed storage while this guard lived. A caller that
    /// swallowed the refusal still learns of it here, so it never serves what that read made of
    /// it.
    pub fn refused(&self) -> bool {
        self.refused.load(Ordering::SeqCst)
    }
}

impl Drop for InMemory<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::SeqCst);
    }
}

impl<B: SessionBackend> Session<B> {
    /// Answers reads from memory only until the returned guard drops: a read that would touch
    /// the backend refuses with [`SessionError::NotInMemory`] before it starts. The session's
    /// owner takes this around a read it answers between its own writes, so the read never waits
    /// on storage. Writes are never refused by it.
    pub fn in_memory(&self) -> InMemory<'_> {
        self.memory_refused.store(false, Ordering::SeqCst);
        self.memory_only.store(true, Ordering::SeqCst);
        InMemory {
            flag: &self.memory_only,
            refused: &self.memory_refused,
        }
    }

    /// Refuses a read that is about to touch the backend while reads come from memory only.
    pub(crate) fn storage_read(&self) -> Result<(), SessionError> {
        if self.memory_only.load(Ordering::SeqCst) {
            self.memory_refused.store(true, Ordering::SeqCst);
            return Err(SessionError::NotInMemory);
        }
        Ok(())
    }

    /// Where the session stands: its head version, its open log and the log's length so far.
    /// After a sync, every byte up to that length is durable.
    pub fn position(&self) -> super::Position {
        super::Position {
            version: self.version,
            log: self.log,
            len: self.encoder.offset(),
        }
    }

    /// Whether the kept records serve a read: always while reads come from memory only, and for a
    /// view, whose kept records are exactly what it read. A writer otherwise reads the log, so it
    /// still checks that the log reads clean under it.
    pub(crate) fn reads_kept(&self) -> bool {
        self.memory_only.load(Ordering::SeqCst) || !self.kept.per_run
    }

    /// The seq of the latest run's start, when that run is `run_id` and its start is in the open
    /// log and kept in memory. A run id names one run start, so this is the start
    /// [`Session::find_id`] would find, without the id index.
    pub fn kept_run_start(&self, run_id: &str) -> Option<u64> {
        self.kept.start_of(run_id)
    }

    /// Every record of the open log from seq `first` on, from memory only, or `None` when the
    /// session doesn't keep them all. A writer keeps every record from its latest run's start on
    /// (the records it appended since, and those it loaded), so a reader that follows the
    /// writer's appends takes them here rather than reading the log again. Unlike a writer's
    /// read ([`Session::records_between`]), this doesn't read the stored log back to check that
    /// it still decodes cleanly.
    pub fn kept_from(&self, first: u64) -> Option<Vec<(u64, Record)>> {
        self.kept.between(first, u64::MAX)
    }

    /// How many of the open log's records the session keeps in memory.
    pub fn kept_records(&self) -> usize {
        self.kept.len()
    }

    /// Whether records were appended since the log was last made durable.
    pub fn unsynced(&self) -> bool {
        self.encoder.unsynced()
    }

    /// Makes every record appended so far durable: a claim frame appended with a barrier. A
    /// session with nothing unsynced writes nothing. A failure poisons the writer, as any failed
    /// append does.
    pub async fn sync(&mut self) -> Result<(), SessionError> {
        if self.poisoned {
            return Err(SessionError::Poisoned);
        }
        if !self.encoder.unsynced() {
            return Ok(());
        }
        let encoded = self.encoder.claim()?;
        self.poisoned = true;
        self.backend
            .append_log(
                &self.addr,
                &self.log,
                &encoded.bytes,
                self.encoder.offset(),
                self.version,
                true,
            )
            .await?;
        self.encoder.written(&encoded);
        self.encoder.synced();
        self.poisoned = false;
        self.observed(super::Appended::Synced);
        Ok(())
    }
}

#[cfg(test)]
mod tests;

impl KeptRecords {
    /// What the kept records hold, estimated by walking them (see `crate::held`): their
    /// contents, each slot of the buffer they fill included, and the buffer's slots beyond them.
    /// A message or a message's facts a record shares with the conversation fold is the fold's
    /// to count, so `in_fold` (the addresses of what the fold holds behind its `Arc`s) leaves
    /// it out here.
    pub(crate) fn held_bytes(&self, in_fold: &std::collections::HashSet<usize>) -> (usize, usize) {
        use crate::held::HeldBytes;
        let slot = std::mem::size_of::<(u64, Record)>();
        let contents = self.records.len() * slot
            + self
                .records
                .iter()
                .map(|(_, record)| record.heap_bytes() - shared_with_fold(record, in_fold))
                .sum::<usize>()
            + self.latest_start.heap_bytes();
        let spare = (self.records.capacity() - self.records.len()) * slot;
        (contents, spare)
    }
}

/// The heap of `record` that it shares with the conversation fold: its message or its facts,
/// when the fold holds the same `Arc` (`in_fold` holds the addresses the fold's `Arc`s point to),
/// and a tool reply's output leaves, which the fold's tool message shares when it folds the reply.
fn shared_with_fold(record: &Record, in_fold: &std::collections::HashSet<usize>) -> usize {
    use crate::held::HeldBytes;
    use std::sync::Arc;
    fn held<T: HeldBytes>(value: &Arc<T>, in_fold: &std::collections::HashSet<usize>) -> usize {
        if in_fold.contains(&(Arc::as_ptr(value) as usize)) {
            value.heap_bytes()
        } else {
            0
        }
    }
    match record {
        Record::Input(input) => held(&input.message, in_fold),
        Record::Assistant(assistant) => held(&assistant.message, in_fold),
        Record::EntryFacts(facts) => held(facts, in_fold),
        Record::TurnFacts(turn) => held(turn, in_fold),
        Record::ToolReply(reply) => crate::held::shared_output_bytes(&reply.result.output),
        _ => 0,
    }
}
