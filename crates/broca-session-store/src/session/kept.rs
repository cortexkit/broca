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

/// The open log's records a session holds in memory: every one from seq `from` on.
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
        }
    }

    /// The records a view read: all of the open log's.
    pub(crate) fn whole_log(records: Vec<(u64, Record)>) -> Self {
        let latest_start = latest_start(&records);
        Self {
            from: 0,
            records,
            whole: true,
            latest_start,
            per_run: false,
        }
    }

    /// The records a writer keeps of the log it loaded: those from the latest run start on, or
    /// none when the log holds no run start.
    pub(crate) fn writer_of(mut records: Vec<(u64, Record)>, next_seq: u64) -> Self {
        match records
            .iter()
            .rposition(|(_, record)| matches!(record, Record::RunStart(_)))
        {
            Some(at) => {
                let whole = at == 0;
                records.drain(..at);
                let latest_start = latest_start(&records);
                Self {
                    from: records[0].0,
                    records,
                    whole,
                    latest_start,
                    per_run: true,
                }
            }
            None => Self::from_next(next_seq, records.is_empty()),
        }
    }

    /// Takes in one appended record at `seq`.
    pub(crate) fn note(&mut self, seq: u64, record: &Record) {
        if let Record::RunStart(start) = record {
            if self.per_run {
                // The records before the new run belong to runs a caller reads from storage.
                self.whole &= self.records.is_empty() && self.from == seq;
                self.records.clear();
                self.from = seq;
            }
            self.latest_start = Some((seq, start.run_id.clone()));
        }
        self.records.push((seq, record.clone()));
    }

    /// Every kept record from `first` through `last`, or `None` when records from `first` on
    /// aren't all kept.
    pub(crate) fn between(&self, first: u64, last: u64) -> Option<Vec<(u64, Record)>> {
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

    /// Every record of the open log, when they are all kept.
    pub(crate) fn whole(&self) -> Option<&[(u64, Record)]> {
        self.whole.then_some(self.records.as_slice())
    }

    /// The retained suffix, without cloning the rest of the run for a tail lookup.
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

fn latest_start(records: &[(u64, Record)]) -> Option<(u64, String)> {
    records.iter().rev().find_map(|(seq, record)| match record {
        Record::RunStart(start) => Some((*seq, start.run_id.clone())),
        _ => None,
    })
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
