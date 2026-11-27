//! Predicate-bounded ledger reads, from the open tail through sealed blocks newest first.

use std::collections::HashMap;

use crate::backend::{BackendError, SessionAddr, SessionBackend};
use crate::checkpoint::Checkpoint;
use crate::codec::{Id128, Message as _};
use crate::directory::Pages;
use crate::fold::operations::Run;
use crate::head::{self, Head, Residence, SessionTriple};
use crate::keys::{Cipher, DataKey, DeploymentKey};
use crate::log::LogReader;
use crate::record::{Record, RunStart, Stream};
use crate::segment::Segment;

use super::stored::{Kept, Place};
use super::{chain_base, log_bind, Position, Session, SessionError};

/// A read-only ledger pinned to one head version and one open-log byte boundary.
pub struct StoredLedger<B> {
    backend: B,
    addr: SessionAddr,
    head: Head,
    key: DataKey,
    position: Position,
    kept: Kept,
}

/// A validated ledger suffix and the context for looking up its runs at the same position.
pub struct LedgerTail<B> {
    ledger: StoredLedger<B>,
    records: Vec<(u64, Record)>,
    starts: HashMap<String, RunStart>,
}

/// The pinned ledger after checking that its open log replays, without loading a session view.
pub struct ReplayedLedger<B> {
    ledger: StoredLedger<B>,
    open: OpenLedger,
}

struct OpenLedger {
    covers_through: u64,
    records: Vec<(u64, Record)>,
    run: Option<Box<Run>>,
    source_lineage: Option<String>,
}

impl<B: SessionBackend> StoredLedger<B> {
    /// Pin the head, key and open-log end without loading a session view. No committed session
    /// (a missing head or an unfinished creation) is `None`; unreadable storage is an error.
    pub async fn open(
        backend: B,
        addr: SessionAddr,
        triple: &SessionTriple,
        deployment: &DeploymentKey,
    ) -> Result<Option<Self>, SessionError> {
        Self::pin(backend, addr, triple, deployment, true, None).await
    }

    /// Pin and replay the open log, including the fold checks a view makes. Only the ledger
    /// records, latest run and source lineage survive the replay.
    pub async fn open_replayed(
        backend: B,
        addr: SessionAddr,
        triple: &SessionTriple,
        deployment: &DeploymentKey,
    ) -> Result<Option<ReplayedLedger<B>>, SessionError> {
        Self::replay_at(backend, addr, triple, deployment, None).await
    }

    /// Replay the ledger at the position recorded by the actor, excluding later appends.
    pub async fn replay_at(
        backend: B,
        addr: SessionAddr,
        triple: &SessionTriple,
        deployment: &DeploymentKey,
        at: Option<Position>,
    ) -> Result<Option<ReplayedLedger<B>>, SessionError> {
        let Some(ledger) = Self::pin(backend, addr, triple, deployment, false, at).await? else {
            return Ok(None);
        };
        let open = ledger.read_open(true).await?;
        Ok(Some(ReplayedLedger { ledger, open }))
    }

    async fn pin(
        backend: B,
        addr: SessionAddr,
        triple: &SessionTriple,
        deployment: &DeploymentKey,
        require_log: bool,
        at: Option<Position>,
    ) -> Result<Option<Self>, SessionError> {
        let Some((stored, version)) = backend.read_head(&addr).await? else {
            return Ok(None);
        };
        if at.is_some_and(|at| at.version != version) {
            return Err(SessionError::Moved);
        }
        let (head, key) = head::open(&stored, deployment).map_err(SessionError::Head)?;
        if require_log && &head.triple != triple {
            return Err(SessionError::TripleMismatch);
        }
        match &head.residence {
            Residence::Local(()) | Residence::Moving(_) => {}
            Residence::Creating(_) => return Ok(None),
            Residence::Recovering(_) => return Err(SessionError::NeedsRecovery),
            Residence::Moved(moved) => {
                return Err(SessionError::SessionMoved {
                    holder: moved.holder_box.clone(),
                });
            }
        }
        let Some(open) = head.open_log.as_ref() else {
            // A committed local head always names its open log. With `require_log` (the
            // held-call withdrawal check), a head without one is damage. Otherwise it reads as
            // no session, as `Session::stored_position` reports it.
            return if require_log {
                Err(BackendError::Corrupt("local head without a log").into())
            } else {
                Ok(None)
            };
        };
        if &head.triple != triple {
            return Err(SessionError::TripleMismatch);
        }
        let log = open.change;
        let len = match at {
            Some(at) if at.log != log => return Err(SessionError::Moved),
            Some(at) => at.len,
            None => backend.log_end(&addr, &log).await?,
        };
        Ok(Some(Self {
            backend,
            addr,
            head,
            key,
            position: Position { version, log, len },
            kept: Kept::default(),
        }))
    }

    pub fn position(&self) -> Position {
        self.position
    }

    pub(super) fn place(&self) -> Place<'_, B> {
        Place {
            backend: &self.backend,
            addr: &self.addr,
            head: &self.head,
            key: &self.key,
            log: &self.position.log,
            kept: Some(&self.kept),
        }
    }

    /// Read ledger records newest first, including the first that satisfies `stop`. The open log
    /// is validated through the pinned byte boundary in one scan; transcript records are dropped
    /// as each batch is decoded. Older sealed records are read only if the predicate needs them.
    pub async fn ledger_tail_until(
        self,
        mut stop: impl FnMut(&Record) -> bool,
    ) -> Result<LedgerTail<B>, SessionError> {
        let OpenLedger {
            covers_through,
            records: open,
            ..
        } = self.read_open(false).await?;
        let mut starts = HashMap::new();
        for (seq, record) in &open {
            if let Record::RunStart(start) = record {
                // Keep only starts written after the checkpoint. A start the checkpoint covers
                // is found through the sealed head's id index (`find_run_start`), even when the
                // seal copied the record into this log too, so both lookups agree.
                if *seq > covers_through && !start.run_id.is_empty() {
                    starts
                        .entry(start.run_id.clone())
                        .or_insert_with(|| (**start).clone());
                }
            }
        }
        let records = self.suffix(open, &mut stop).await?;
        Ok(LedgerTail {
            ledger: self,
            records,
            starts,
        })
    }

    async fn suffix(
        &self,
        open: Vec<(u64, Record)>,
        mut stop: impl FnMut(&Record) -> bool,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        let mut records = Vec::new();
        for (seq, record) in open.into_iter().rev() {
            let done = stop(&record);
            records.push((seq, record));
            if done {
                return Ok(records);
            }
        }
        self.place()
            .sealed_ledger_tail_until(&mut records, stop)
            .await?;
        Ok(records)
    }

    async fn read_open(&self, replay: bool) -> Result<OpenLedger, SessionError> {
        let bytes = self
            .backend
            .read_log(&self.addr, &self.position.log, 0..self.position.len)
            .await?;
        let key = self.key.frame_key();
        let bind = log_bind(&self.head.session_id, &self.position.log);
        let synced_len = self
            .head
            .open_log
            .as_ref()
            .expect("pinned open log")
            .synced_len;
        read_ledger(&bytes, &key, &bind, synced_len, replay)
    }
}

fn read_ledger(
    bytes: &[u8],
    key: &crate::frame::FrameKey,
    bind: &[u8],
    synced_len: u64,
    replay: bool,
) -> Result<OpenLedger, SessionError> {
    let mut reader = LogReader::new(bytes, key, bind, synced_len, chain_base);
    let mut records = Vec::new();
    let mut checkpoint = None;
    let mut failure = None;
    while let Some(batch) = reader.next_batch()? {
        if replay && checkpoint.is_none() && failure.is_none() {
            match Checkpoint::decode(reader.checkpoint()) {
                Ok(state) => checkpoint = Some(Box::new(state)),
                Err(error) => failure = Some(SessionError::Checkpoint(error)),
            }
        }
        for (seq, record) in batch.records {
            let ledger = record.stream() == Stream::Ledger;
            if let Some(state) = checkpoint.as_mut().filter(|_| failure.is_none()) {
                // A seal can copy already folded ledger records into this log for history.
                if !ledger || seq > state.covers_through {
                    if let Err(error) = super::apply(
                        &mut state.conversation,
                        &mut state.operational,
                        seq,
                        &record,
                    ) {
                        failure = Some(SessionError::Fold(error));
                    }
                }
            }
            if ledger && failure.is_none() {
                records.push((seq, record));
            }
        }
    }
    // As for a view, report frame and boundary errors before a fold error.
    let read = match crate::damage::verdict(
        reader.finish(),
        bytes,
        key,
        bind,
        synced_len,
        chain_base,
        None,
    )? {
        crate::damage::Verdict::Opens(read) => read,
        crate::damage::Verdict::Debris(_) => return Err(SessionError::NeedsRecovery),
        crate::damage::Verdict::Corrupt { at, durable_bound } => {
            return Err(SessionError::Corrupt { at, durable_bound });
        }
    };
    if let Some(error) = failure {
        return Err(error);
    }
    let state = match checkpoint {
        Some(state) => state,
        None => Box::new(Checkpoint::decode(&read.checkpoint).map_err(SessionError::Checkpoint)?),
    };
    let Checkpoint {
        covers_through,
        operational,
        ..
    } = *state;
    let (run, source_lineage) = if replay {
        (operational.run.map(Box::new), operational.source_lineage)
    } else {
        (None, None)
    };
    Ok(OpenLedger {
        covers_through,
        records,
        run,
        source_lineage,
    })
}

impl<B: SessionBackend> ReplayedLedger<B> {
    pub fn session_id(&self) -> Id128 {
        self.ledger.head.session_id
    }

    pub fn source_lineage(&self) -> Option<&str> {
        self.open.source_lineage.as_deref()
    }

    pub fn take_run(&mut self) -> Option<Box<Run>> {
        self.open.run.take()
    }

    /// Intents filed in the open id index. Copied records belong to the sealed index instead.
    pub fn open_call_intents(&self) -> impl Iterator<Item = &str> {
        self.open.records.iter().filter_map(|(seq, record)| {
            if *seq > self.open.covers_through {
                if let Record::ToolIntent(intent) = record {
                    return intent.call_key.as_deref().filter(|id| !id.is_empty());
                }
            }
            None
        })
    }

    /// Whether the pinned sealed id index names a matching intent. Even when an open intent
    /// matched, check the sealed candidates so damage to an indexed record is not a miss.
    pub async fn has_sealed_call_intent(&self, key: &str) -> Result<bool, SessionError> {
        self.ledger.place().has_indexed_call_intent(key).await
    }

    /// Ledger records newest first, including the first that satisfies `stop`.
    pub async fn ledger_tail_until(
        self,
        stop: impl FnMut(&Record) -> bool,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        self.ledger.suffix(self.open.records, stop).await
    }
}

impl<B: SessionBackend> LedgerTail<B> {
    pub fn position(&self) -> Position {
        self.ledger.position()
    }

    pub fn cipher(&self) -> Cipher {
        self.ledger.head.cipher()
    }

    pub fn records(&self) -> &[(u64, Record)] {
        &self.records
    }

    /// The original run start, from the validated prefix or the pinned head's sealed id index.
    /// Calls of the same run in one batch need only look it up once.
    pub async fn find_run_start(&mut self, run_id: &str) -> Result<Option<RunStart>, SessionError> {
        if let Some(start) = self.starts.get(run_id) {
            return Ok(Some(start.clone()));
        }
        let start = self.ledger.place().find_indexed_run_start(run_id).await?;
        if let Some(start) = &start {
            self.starts.insert(run_id.to_owned(), start.clone());
        }
        Ok(start)
    }
}

impl<B: SessionBackend> Session<B> {
    /// Ledger records newest first, including the first record for which `stop` returns true.
    /// A retained open-log suffix is visited without cloning the rest of its run. If it is
    /// insufficient, the open log is read at most once, then sealed ledger segments and their
    /// bounded blocks are read newest first. No older segment is fetched after the predicate
    /// succeeds. A transcript record never satisfies the predicate or enters the result.
    pub async fn ledger_tail_until(
        &self,
        mut stop: impl FnMut(&Record) -> bool,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        let mut out = Vec::new();
        let (kept, whole) = self.kept.tail();
        for (seq, record) in kept
            .iter()
            .rev()
            .filter(|(_, r)| r.stream() == Stream::Ledger)
        {
            out.push((*seq, record.clone()));
            if stop(record) {
                return Ok(out);
            }
        }
        if !whole {
            self.storage_read()?;
            let first_kept = kept.first().map(|(seq, _)| *seq).unwrap_or(u64::MAX);
            for (seq, record) in self.place().open_log().await?.records.into_iter().rev() {
                if seq >= first_kept || record.stream() != Stream::Ledger {
                    continue;
                }
                let done = stop(&record);
                out.push((seq, record));
                if done {
                    return Ok(out);
                }
            }
        }
        if self.head.ledger_dir.is_some() {
            self.storage_read()?;
            self.place()
                .sealed_ledger_tail_until(&mut out, stop)
                .await?;
        }
        Ok(out)
    }
}

impl<B: SessionBackend> Place<'_, B> {
    async fn sealed_ledger_tail_until(
        &self,
        out: &mut Vec<(u64, Record)>,
        mut stop: impl FnMut(&Record) -> bool,
    ) -> Result<(), SessionError> {
        let Some(root) = self.head.ledger_dir.as_ref() else {
            return Ok(());
        };
        let key = self.key.frame_key();
        let pages = Pages {
            backend: self.backend,
            addr: self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: crate::directory::FANOUT,
        };
        let mut seen: std::collections::BTreeSet<_> = out.iter().map(|(seq, _)| *seq).collect();
        let mut cursor = root.last_seq;
        while let Some(entry) = pages
            .last_at_or_before(Some(root), cursor)
            .await
            .map_err(SessionError::Directory)?
        {
            let bytes = self
                .backend
                .get_object(self.addr, &entry.object)
                .await?
                .ok_or_else(|| {
                    SessionError::Fold(format!(
                        "the directory names a missing segment {}",
                        entry.object
                    ))
                })?;
            let segment = Segment::open(&bytes, &key, entry.object.as_bytes())
                .map_err(SessionError::Segment)?;
            for position in (0..segment.index.blocks.len()).rev() {
                if segment.index.blocks[position].first_seq > cursor {
                    continue;
                }
                for (seq, record) in segment
                    .block(position)
                    .map_err(SessionError::Segment)?
                    .into_iter()
                    .rev()
                {
                    if seq > cursor || !entry.keeps(seq) || record.stream() != Stream::Ledger {
                        continue;
                    }
                    // A seal can retain records in the open log; never return one twice.
                    if !seen.insert(seq) {
                        continue;
                    }
                    let done = stop(&record);
                    out.push((seq, record));
                    if done {
                        return Ok(());
                    }
                }
            }
            let Some(before) = entry.first_seq.checked_sub(1) else {
                break;
            };
            cursor = before;
        }
        Ok(())
    }
}
