//! Reading a session's stored records apart from its writer.
//!
//! A subscriber catching up on a session reads records it hasn't seen while the session's writer
//! goes on appending. It reads them through a [`StoredRecords`]: where the records were stored
//! when it attached (the head as the writer last committed it, and the open log), over a backend
//! handle of its own, so its reads never wait on the writer and the writer never waits on them.
//! Records appended after it attached are past the end it reads to, and a torn tail of the open
//! log reads as not yet written.

use std::collections::BTreeMap;

use crate::backend::{BackendError, SessionAddr, SessionBackend};
use crate::checkpoint::Checkpoint;
use crate::codec::{Id128, Message as _};
use crate::directory::Pages;
use crate::fold::conversation::Conversation;
use crate::head::Head;
use crate::keys::DataKey;
use crate::log::ReadLog;
use crate::record::{Record, Stream};
use crate::segment::Segment;

use super::{chain_base, log_bind, Session, SessionError};

/// Where a session's records were stored at one moment: the head as its writer last committed
/// it, and the open log.
pub(super) struct Place<'a, B> {
    pub(super) backend: &'a B,
    pub(super) addr: &'a SessionAddr,
    pub(super) head: &'a Head,
    pub(super) key: &'a DataKey,
    pub(super) log: &'a Id128,
}

impl<B: SessionBackend> Place<'_, B> {
    /// The open log read back: its checkpoint and its records. The writer appended every one of
    /// them, so a log that no longer reads clean means something else wrote it, and the read
    /// refuses.
    pub(super) async fn open_log(&self) -> Result<ReadLog, SessionError> {
        let open =
            self.head
                .open_log
                .clone()
                .ok_or(SessionError::Backend(BackendError::Corrupt(
                    "local head without a log",
                )))?;
        let end = self.backend.log_end(self.addr, self.log).await?;
        let bytes = self.backend.read_log(self.addr, self.log, 0..end).await?;
        let bind = log_bind(&self.head.session_id, self.log);
        match crate::damage::analyse(
            &bytes,
            &self.key.frame_key(),
            &bind,
            open.synced_len,
            chain_base,
            None,
        )? {
            crate::damage::Verdict::Opens(read) => Ok(read),
            _ => Err(SessionError::Fold(
                "the open log no longer reads clean under its writer".into(),
            )),
        }
    }

    /// Every record of both streams from seq `first` through seq `last`, sealed or in the open
    /// log, in seq order, reading only the sealed segments that overlap the range. A record a
    /// seal kept in the open log is read once.
    pub(super) async fn records_between(
        &self,
        first: u64,
        last: u64,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        let mut records = self.sealed_between(first, last).await?;
        for (seq, record) in self.open_log().await?.records {
            if (first..=last).contains(&seq) {
                records.insert(seq, record);
            }
        }
        Ok(records.into_iter().collect())
    }

    /// Whether a sealed segment may hold a record at or past `first`.
    pub(super) fn sealed_reach(&self, first: u64) -> bool {
        [&self.head.ledger_dir, &self.head.transcript_dir]
            .into_iter()
            .any(|dir| dir.as_ref().is_some_and(|root| root.last_seq >= first))
    }

    /// The sealed records of both streams from seq `first` through seq `last`, by seq, reading
    /// only the segments that overlap the range.
    pub(super) async fn sealed_between(
        &self,
        first: u64,
        last: u64,
    ) -> Result<BTreeMap<u64, Record>, SessionError> {
        let key = self.key.frame_key();
        let pages = Pages {
            backend: self.backend,
            addr: self.addr,
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
                for position in 0..segment.index.blocks.len() {
                    // A block whose successor starts at or before `first` ends before it, and a
                    // block that starts after `last` holds nothing in the range.
                    let next = segment.index.blocks.get(position + 1);
                    if next.is_some_and(|next| next.first_seq <= first)
                        || segment.index.blocks[position].first_seq > last
                    {
                        continue;
                    }
                    for (seq, record) in segment.block(position).map_err(SessionError::Segment)? {
                        if (first..=last).contains(&seq) && entry.keeps(seq) {
                            records.insert(seq, record);
                        }
                    }
                }
            }
        }
        Ok(records)
    }

    /// The checkpoint of the sealed log holding `seq`: the newest log start that covers less
    /// than `seq`. Each ledger segment carries the start of the log after the one it sealed;
    /// the session's first log started from empty state.
    async fn sealed_log_start(&self, seq: u64) -> Result<Checkpoint, SessionError> {
        let key = self.key.frame_key();
        let pages = Pages {
            backend: self.backend,
            addr: self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: crate::directory::FANOUT,
        };
        let mut before = seq.saturating_add(1);
        loop {
            let Some(entry) = pages
                .find_before(self.head.ledger_dir.as_ref(), before)
                .await
                .map_err(SessionError::Directory)?
            else {
                return Ok(first_log_checkpoint(self.head));
            };
            let bytes = self
                .backend
                .get_object(self.addr, &entry.object)
                .await?
                .ok_or(SessionError::CheckpointLost)?;
            let segment = Segment::open(&bytes, &key, entry.object.as_bytes())
                .map_err(SessionError::Segment)?;
            let start = segment
                .index
                .log_start
                .ok_or(SessionError::CheckpointLost)?;
            let checkpoint = Checkpoint::decode(&start.0).map_err(SessionError::Checkpoint)?;
            if checkpoint.covers_through < seq {
                return Ok(checkpoint);
            }
            before = entry.first_seq;
        }
    }
}

/// The checkpoint the session's first log starts from: empty state.
pub(super) fn first_log_checkpoint(head: &Head) -> Checkpoint {
    Checkpoint {
        covers_through: 0,
        next_seq: 1,
        conversation: Conversation::new(head.appends_in_conversation.unwrap_or(false)),
        operational: Default::default(),
        transcript_base: None,
        ledger_base: None,
        activity_ms: None,
        media: None,
    }
}

/// A session's stored records as they were when it was taken, read over a backend handle of the
/// reader's own (see the module docs).
pub struct StoredRecords<R> {
    backend: R,
    addr: SessionAddr,
    head: Head,
    key: DataKey,
    log: Id128,
}

/// The log holding a seq, from its start: the checkpoint it started from and its records from
/// there through a last seq.
pub struct LogFrom {
    pub checkpoint: Checkpoint,
    /// In seq order: every record past the checkpoint, and before them the log's own
    /// transcript records the checkpoint's conversation doesn't hold yet (those a seal at a cut
    /// kept in the log, with their seqs).
    pub records: Vec<(u64, Record)>,
}

impl<B: SessionBackend> Session<B> {
    /// This session's stored records as of now, read over `backend`, a handle on the same
    /// storage that only reads.
    pub fn stored_records<R: SessionBackend>(&self, backend: R) -> StoredRecords<R> {
        StoredRecords {
            backend,
            addr: self.addr.clone(),
            head: self.head.clone(),
            key: self.key.clone(),
            log: self.log,
        }
    }
}

impl<R: SessionBackend> StoredRecords<R> {
    pub(super) fn place(&self) -> Place<'_, R> {
        Place {
            backend: &self.backend,
            addr: &self.addr,
            head: &self.head,
            key: &self.key,
            log: &self.log,
        }
    }

    /// The head's incarnation, which changes when the session's history is replaced.
    pub fn incarnation(&self) -> [u8; 16] {
        self.head.incarnation.0
    }

    /// Every record of both streams from seq `first` through seq `last`, in seq order.
    pub async fn records_between(
        &self,
        first: u64,
        last: u64,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        self.place().records_between(first, last).await
    }

    /// The log holding `seq`, from its start through `last`: the newest log whose checkpoint
    /// covers less than `seq`, so folding its records from that checkpoint reaches `seq` without
    /// folding the session's earlier history. Records of later logs through `last` follow in seq
    /// order.
    pub async fn from_log_holding(&self, seq: u64, last: u64) -> Result<LogFrom, SessionError> {
        from_log_holding(&self.place(), seq, last).await
    }
}

/// [`StoredRecords::from_log_holding`] over `place`.
pub(super) async fn from_log_holding<B: SessionBackend>(
    place: &Place<'_, B>,
    seq: u64,
    last: u64,
) -> Result<LogFrom, SessionError> {
    let open = place.open_log().await?;
    let start = Checkpoint::decode(&open.checkpoint).map_err(SessionError::Checkpoint)?;
    if start.covers_through < seq {
        let records = open
            .records
            .into_iter()
            .filter(|(at, _)| *at <= last)
            .collect();
        return Ok(LogFrom {
            checkpoint: start,
            records,
        });
    }
    let checkpoint = place.sealed_log_start(seq).await?;
    let covers = checkpoint.covers_through;
    // A seal at a cut kept the transcript records after its cut in the log it started, with
    // their seqs: those after the last transcript record it sealed.
    let first = checkpoint
        .transcript_base
        .unwrap_or(covers)
        .min(covers)
        .saturating_add(1);
    let records = place
        .records_between(first, last)
        .await?
        .into_iter()
        .filter(|(at, record)| *at > covers || record.stream() == Stream::Transcript)
        .collect();
    Ok(LogFrom {
        checkpoint,
        records,
    })
}
