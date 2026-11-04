//! A session's writer: creating a session, loading it, and appending to its open log.
//!
//! **Creating** writes the head first, naming the log it is about to create, then creates that
//! log with its checkpoint, syncs it, and commits by a second head update. A crash in between
//! leaves a head in `Creating`, and the next load finishes exactly that log, so no log ever
//! exists that no head names.
//!
//! **Loading** reads the head, then the whole open log, and decides how the log ends before
//! writing anything. A clean log or a torn tail (an append that never finished) opens. A torn
//! tail that starts below the durable bound, or any damage, refuses by name: deciding between
//! debris of unsynced writes and corruption, and repairing the former, is crash recovery's job,
//! which isn't built yet. Opening the writer then commits a new generation in the head, cuts the
//! torn tail, and appends `WriterOpened` as a barrier before anything else. Both folds start from
//! the log's checkpoint and apply the log's records.
//!
//! **Appending** folds a batch into the in-memory state, writes it in one call, and syncs it when
//! it's a barrier. A writer whose fold or write fails is poisoned: it accepts nothing more, and
//! its caller loads the session again, so it never appends on top of an end it isn't sure of.

use crate::backend::{BackendError, SessionAddr, SessionBackend, Version};
use crate::checkpoint::Checkpoint;
use crate::codec::{ByteBuf, DecodeError, Id128, Message as _};
use crate::fold::conversation::Conversation;
use crate::fold::operations::Operations;
use crate::head::{self, Head, HeadError, OpenLog, Residence, SessionTriple, WrappedKey};
use crate::keys::{self, random_id, DataKey, DeploymentKey};
use crate::log::{self, ChainBase, LogEncoder, LogEnd, LogError, ReadLog};
use crate::record::{Record, Stream, WriterOpened};

/// The id of every session's first data key. The data key never rotates.
const DATA_KEY_ID: u16 = 1;

#[derive(Debug)]
pub enum SessionError {
    Backend(BackendError),
    Head(HeadError),
    Log(LogError),
    /// The session has no head.
    Missing,
    /// The head was created for another (project root, harness, session).
    TripleMismatch,
    /// The checkpoint authenticated but doesn't decode.
    Checkpoint(DecodeError),
    /// The open log is damaged, or acknowledged bytes are missing from it. Nothing was changed.
    Damaged(LogEnd),
    /// The records don't fold. The writer is poisoned.
    Fold(String),
    /// An earlier fold or write failed; load the session again.
    Poisoned,
}

impl From<BackendError> for SessionError {
    fn from(error: BackendError) -> Self {
        Self::Backend(error)
    }
}

impl From<LogError> for SessionError {
    fn from(error: LogError) -> Self {
        Self::Log(error)
    }
}

/// What frames authenticate together with their header: the session id and the log's name.
fn log_bind(session: &Id128, log: &Id128) -> Vec<u8> {
    let mut bind = session.0.to_vec();
    bind.extend_from_slice(&log.0);
    bind
}

fn apply(
    conversation: &mut Conversation,
    operations: &mut Operations,
    record: &Record,
) -> Result<(), String> {
    match record.stream() {
        Stream::Transcript => conversation
            .apply(record)
            .map_err(|e| format!("conversation: {e:?}")),
        Stream::Ledger => operations
            .apply(record)
            .map_err(|e| format!("operations: {e:?}")),
    }
}

/// A session open for writing. It holds the head version it last committed, which fences every
/// write it makes.
pub struct Session<'b, B: SessionBackend> {
    backend: &'b B,
    addr: SessionAddr,
    head: Head,
    version: Version,
    encoder: LogEncoder,
    log: Id128,
    poisoned: bool,
    pub conversation: Conversation,
    pub operations: Operations,
}

impl<'b, B: SessionBackend> Session<'b, B> {
    /// Creates a session at `addr`. `appends_in_conversation` is the conversation fold's
    /// setting: `false` for sessions on the current compaction contract, which keep text
    /// delivered with a tool result apart from the result.
    pub async fn create(
        backend: &'b B,
        addr: SessionAddr,
        triple: SessionTriple,
        deployment: &DeploymentKey,
        appends_in_conversation: bool,
    ) -> Result<Self, SessionError> {
        let session_id = random_id();
        let key = DataKey::generate(DATA_KEY_ID);
        let log = random_id();
        let head = Head {
            format: head::HEAD_FORMAT,
            session_id,
            triple,
            incarnation: random_id(),
            history_version: 0,
            residence: Residence::Creating(log),
            features: Vec::new(),
            keys: vec![WrappedKey {
                id: key.id,
                deployment: deployment.id(),
                wrapped: ByteBuf(keys::wrap(deployment, &session_id, &key)),
            }],
            generation: random_id(),
            next_seq: 1,
            open_log: None,
        };
        let version = backend
            .write_head(&addr, &head::seal(&head, &key), None, 1)
            .await?;
        let mut session = Self::create_log(
            backend,
            addr,
            head.clone(),
            version,
            &key,
            log,
            appends_in_conversation,
        )
        .await?;
        session.open_writer(&key).await?;
        Ok(session)
    }

    /// Creates log `log` holding only an empty checkpoint, syncs it, and commits the head that
    /// names it as the open log. Used by creation and by a load that finishes a crashed
    /// creation.
    async fn create_log(
        backend: &'b B,
        addr: SessionAddr,
        mut head: Head,
        version: Version,
        key: &DataKey,
        log: Id128,
        appends_in_conversation: bool,
    ) -> Result<Self, SessionError> {
        backend.create_log(&addr, &log, version).await?;
        let mut encoder = LogEncoder::new(
            key.frame_key(),
            log_bind(&head.session_id, &log),
            head.next_seq,
            ChainBase::default(),
        );
        let conversation = Conversation::new(appends_in_conversation);
        let operations = Operations::default();
        let checkpoint = Checkpoint {
            covers_through: 0,
            next_seq: head.next_seq,
            conversation: conversation.clone(),
            operational: operations.clone(),
            transcript_base: None,
            ledger_base: None,
        };
        let encoded = encoder.checkpoint(&checkpoint.encode())?;
        backend
            .append_log(&addr, &log, &encoded.bytes, 0, version, true)
            .await?;
        encoder.written(&encoded);
        encoder.synced();
        head.residence = Residence::Local(());
        head.open_log = Some(OpenLog {
            change: log,
            synced_len: encoder.offset(),
        });
        let version = backend
            .write_head(&addr, &head::seal(&head, key), Some(version), version + 1)
            .await?;
        Ok(Self {
            backend,
            addr,
            head,
            version,
            encoder,
            log,
            poisoned: false,
            conversation,
            operations,
        })
    }

    /// Loads the session at `addr` and opens it for writing. `triple` must be the one it was
    /// created under.
    pub async fn load(
        backend: &'b B,
        addr: SessionAddr,
        triple: &SessionTriple,
        deployment: &DeploymentKey,
    ) -> Result<Self, SessionError> {
        let (stored, version) = backend
            .read_head(&addr)
            .await?
            .ok_or(SessionError::Missing)?;
        let (head, key) = head::open(&stored, deployment).map_err(SessionError::Head)?;
        if &head.triple != triple {
            return Err(SessionError::TripleMismatch);
        }
        if let Residence::Creating(log) = head.residence {
            // Creation crashed before its commit, so the session has no records yet. New
            // sessions keep delivered text apart from tool results.
            let mut session =
                Self::create_log(backend, addr, head, version, &key, log, false).await?;
            session.open_writer(&key).await?;
            return Ok(session);
        }
        let open = head
            .open_log
            .clone()
            .ok_or(SessionError::Backend(BackendError::Corrupt(
                "local head without a log",
            )))?;
        let end = backend.log_end(&addr, &open.change).await?;
        let bytes = backend.read_log(&addr, &open.change, 0..end).await?;
        let bind = log_bind(&head.session_id, &open.change);
        let frame_key = key.frame_key();
        let mut checkpoint = None;
        let read = log::read(&bytes, &frame_key, &bind, open.synced_len, |body| {
            let decoded = Checkpoint::decode(body).map_err(|e| e.to_string())?;
            let base = ChainBase {
                transcript: decoded.transcript_base,
                ledger: decoded.ledger_base,
            };
            checkpoint = Some(decoded);
            Ok(base)
        })?;
        let checkpoint = checkpoint.ok_or(SessionError::Damaged(read.end.clone()))?;
        match read.end {
            LogEnd::Clean => {}
            LogEnd::TornTail { at } if at >= read.durable_bound => {}
            ref end => return Err(SessionError::Damaged(end.clone())),
        }

        let last_seq = read.records.last().map(|(seq, _)| *seq);
        let next_seq = head
            .next_seq
            .max(checkpoint.next_seq)
            .max(last_seq.map_or(0, |seq| seq + 1));
        let mut conversation = checkpoint.conversation;
        let mut operations = checkpoint.operational;
        for (seq, record) in &read.records {
            if record.stream() == Stream::Ledger && *seq <= checkpoint.covers_through {
                continue;
            }
            apply(&mut conversation, &mut operations, record).map_err(SessionError::Fold)?;
        }
        let mut session = Self {
            backend,
            addr,
            head,
            version,
            encoder: LogEncoder::resume(frame_key, bind, &read, next_seq),
            log: open.change,
            poisoned: false,
            conversation,
            operations,
        };
        session.open_writer_over(&key, &read, end).await?;
        Ok(session)
    }

    /// Opens the writer on a log nothing but its checkpoint was written to.
    async fn open_writer(&mut self, key: &DataKey) -> Result<(), SessionError> {
        let end = self.encoder.offset();
        let read = ReadLog {
            checkpoint: Vec::new(),
            records: Vec::new(),
            end: LogEnd::Clean,
            durable_bound: end,
            complete_through: end,
            frames: 1,
            last: self.encoder.last(),
        };
        self.open_writer_over(key, &read, end).await
    }

    /// Commits a new generation and next seq in the head, cuts a torn tail, and appends
    /// `WriterOpened` as a barrier. Only then may the writer mint identities from (generation,
    /// seq).
    async fn open_writer_over(
        &mut self,
        key: &DataKey,
        read: &ReadLog,
        end: u64,
    ) -> Result<(), SessionError> {
        self.head.generation = random_id();
        self.head.next_seq = self.encoder.next_seq();
        self.version = self
            .backend
            .write_head(
                &self.addr,
                &head::seal(&self.head, key),
                Some(self.version),
                self.version + 1,
            )
            .await?;
        if read.complete_through < end {
            self.backend
                .truncate_log(
                    &self.addr,
                    &self.log,
                    read.complete_through,
                    end,
                    self.version,
                )
                .await?;
        }
        let opened = Record::WriterOpened(WriterOpened {
            generation: self.head.generation,
        });
        self.append(&[opened], true).await?;
        Ok(())
    }

    /// Folds `records` into the state, then appends them as one batch, synced if `barrier`.
    /// Returns the seq of the first record.
    pub async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        if self.poisoned {
            return Err(SessionError::Poisoned);
        }
        let encoded = self.encoder.batch(records)?;
        self.poisoned = true;
        for record in records {
            apply(&mut self.conversation, &mut self.operations, record)
                .map_err(SessionError::Fold)?;
        }
        self.backend
            .append_log(
                &self.addr,
                &self.log,
                &encoded.bytes,
                self.encoder.offset(),
                self.version,
                barrier,
            )
            .await?;
        self.encoder.written(&encoded);
        if barrier {
            self.encoder.synced();
        }
        self.poisoned = false;
        Ok(encoded.first_seq.expect("a batch holds records"))
    }

    /// Appends a claim-only frame, so the log's last barrier counts toward its durable bound.
    /// A writer does this when it goes idle or closes.
    pub async fn close(mut self) -> Result<(), SessionError> {
        if self.poisoned {
            return Err(SessionError::Poisoned);
        }
        let encoded = self.encoder.claim()?;
        self.backend
            .append_log(
                &self.addr,
                &self.log,
                &encoded.bytes,
                self.encoder.offset(),
                self.version,
                false,
            )
            .await?;
        self.encoder.written(&encoded);
        Ok(())
    }

    pub fn head(&self) -> &Head {
        &self.head
    }

    /// The open log's change id and its current length.
    pub fn open_log(&self) -> (Id128, u64) {
        (self.log, self.encoder.offset())
    }
}

#[cfg(test)]
mod tests;
