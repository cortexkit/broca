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
//! A change's intent at the end of the log means that change never committed, so opening the
//! writer undoes it first (see `seal`).
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
use crate::idindex::OpenIds;
use crate::keys::{self, random_id, Cipher, DataKey, DeploymentKey};
use crate::log::{ChainBase, LogEncoder, LogEnd, LogError, ReadLog};
use crate::record::{ChangeAborted, Record, Stream, WriterOpened};

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
    /// A change's intent closed the log; only its abort may be appended.
    ChangeOpen,
    /// A cut that doesn't name the first record of an entry in the open log.
    BadCut(u64),
    /// A seal would move the records of a compaction view the session still names out of the
    /// open log without carrying a copy of it: see [`Carry::records`].
    ViewNotCarried,
    /// A change's new log would start from a state that no longer names the session's
    /// enrollment in the current compaction contract. Nothing was committed.
    EnrollmentNotCarried,
    Directory(crate::directory::DirectoryError),
    Segment(crate::segment::SegmentError),
    Index(crate::idindex::IndexError),
    /// A revert was refused; nothing was written.
    RevertRefused(revert::RevertRefusal),
    /// A medium's bytes couldn't be stored or read back.
    Media(media::MediaError),
    /// The open log holds debris, or a recovery a crash interrupted: run
    /// [`Session::recover`], then load again.
    NeedsRecovery,
    /// Bytes a barrier acknowledged are damaged, from `at`. Nothing was changed; an operator has
    /// to look.
    Corrupt {
        at: u64,
        durable_bound: u64,
    },
    /// The open log's checkpoint is damaged, and rebuilding it isn't built yet.
    CheckpointLost,
    /// The session belongs to an actor that has stopped, so a run writing through it can't
    /// append or read anything more. Nothing of the request was appended.
    OwnerGone,
    /// The session names an import's id map that is missing or doesn't read: damage, never an
    /// id the session doesn't hold.
    ImportedIdsDamaged(String),
    /// The session enrolled in the current compaction contract, and the object holding what its
    /// enrollment decided about the earlier messages is missing, doesn't read, or doesn't cover
    /// them (see `enrollment`): damage, never an empty overlay.
    EnrollmentDamaged(String),
    /// A read made while reads come from memory only (see [`Session::in_memory`]) needs history
    /// kept in storage. Nothing was read.
    NotInMemory,
    /// The session's head moved past the version a read was pinned at (see [`Session::view_at`]),
    /// or its open log is no longer the one pinned. Nothing was read; read it again.
    Moved,
    /// A move owns the session's writer until its cut aborts or releases.
    SessionMoving {
        cut_id: String,
    },
    /// The retained copy is fenced because another computer holds the session.
    SessionMoved {
        holder: String,
    },
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

/// The chain base a log's checkpoint names: each stream's last record before the log.
fn chain_base(body: &[u8]) -> Result<ChainBase, String> {
    let checkpoint = Checkpoint::decode(body).map_err(|e| e.to_string())?;
    Ok(ChainBase {
        transcript: checkpoint.transcript_base,
        ledger: checkpoint.ledger_base,
    })
}

/// What frames authenticate together with their header: the session id and the log's name.
fn log_bind(session: &Id128, log: &Id128) -> Vec<u8> {
    let mut bind = session.0.to_vec();
    bind.extend_from_slice(&log.0);
    bind
}

/// Folds one record, at `seq`, into whichever fold its stream belongs to. Appending uses it, and
/// so does a copy of the folds that must stay equal to a session's own. The conversation is told
/// of the session's enrollment in the current compaction contract, which the operational fold
/// keeps, before every transcript record it folds: where stored results stop carrying the text
/// delivered with them (`Conversation::enrolled`) depends on it, and a window folded again from
/// an older checkpoint must fold alike.
pub fn apply(
    conversation: &mut Conversation,
    operations: &mut Operations,
    seq: u64,
    record: &Record,
) -> Result<(), String> {
    match record.stream() {
        Stream::Transcript => {
            conversation.enrolled(operations.enrolled_through());
            conversation
                .apply(record)
                .map_err(|e| format!("conversation: {e:?}"))
        }
        Stream::Ledger => {
            operations
                .apply_at(Some(seq), record)
                .map_err(|e| format!("operations: {e:?}"))?;
            conversation.enrolled(operations.enrolled_through());
            Ok(())
        }
    }
}

/// Refuses entry facts that give a message another id than the one its step or batch announced
/// (`StepStart::mid`, `BatchStart::mid`): a live subscriber already saw the message under the
/// announced id, so two ids for one message mean the session's records disagree. The facts
/// checked are a model-written message's, for the newest message: the step's assistant
/// message, placed or held for its tool results, or the tool-results message the batch's last
/// reply placed. Facts written before announcements were recorded have nothing to match.
///
/// A writer checks facts as it appends them, against the folds just before them. A log read
/// back isn't checked again: a seal or a revert copies earlier messages' facts into the new log
/// with their first ids, while the folds they're replayed against already moved on to later
/// steps.
pub fn check_announced_mid(
    conversation: &Conversation,
    operations: &Operations,
    record: &Record,
) -> Result<(), String> {
    use broca_protocol::content::Role;
    let Record::EntryFacts(facts) = record else {
        return Ok(());
    };
    let Some(run) = operations.run.as_ref() else {
        return Ok(());
    };
    if !facts.native {
        return Ok(());
    }
    let held = conversation
        .open_entry()
        .filter(|(ordinal, ..)| *ordinal == facts.ordinal)
        .map(|(_, message, _)| message.role);
    let newest = (facts.ordinal == conversation.placed)
        .then(|| conversation.window_message(facts.ordinal))
        .flatten()
        .map(|message| message.role);
    let announced = match held.or(newest) {
        Some(Role::Assistant) => run.step_mid.as_ref(),
        Some(Role::Tool) => run
            .open_batch
            .as_ref()
            .and_then(|batch| batch.results_mid.as_ref()),
        _ => None,
    };
    match announced {
        Some(announced) if *announced != facts.mid => Err(format!(
            "message_id_mismatch: message {} has id {} but its start announced {announced}",
            facts.ordinal, facts.mid
        )),
        _ => Ok(()),
    }
}

/// What [`Session::read`] found: a creation that never committed, or a log read and folded.
enum Loaded<B: SessionBackend> {
    Creating {
        backend: B,
        addr: SessionAddr,
        // Boxed, as the open session is: a head is large, and the two variants should cost alike.
        head: Box<Head>,
        version: Version,
        key: DataKey,
        log: Id128,
    },
    Open {
        session: Box<Session<B>>,
        read: ReadLog,
        end: u64,
    },
}

/// A session loaded for reading only ([`Session::view`]). It gives the session's read methods,
/// which take `&self`, and none of its writes, which take `&mut self`.
pub struct SessionView<B: SessionBackend>(Session<B>);

impl<B: SessionBackend> std::ops::Deref for SessionView<B> {
    type Target = Session<B>;

    fn deref(&self) -> &Session<B> {
        &self.0
    }
}

/// A session open for writing. It holds the head version it last committed, which fences every
/// write it makes.
pub struct Session<B: SessionBackend> {
    backend: B,
    addr: SessionAddr,
    head: Head,
    version: Version,
    key: DataKey,
    encoder: LogEncoder,
    log: Id128,
    poisoned: bool,
    pub conversation: Conversation,
    pub operations: Operations,
    /// The open log's ledger records that name ids, so `find_id` finds ids not sealed yet.
    open_ids: OpenIds,
    /// What the open log's records name: each new message's ordinal, so `find_message` finds
    /// its id before it's sealed, and where each media digest's bytes are stored.
    window: OpenLogIndex,
    /// The import's id map, once read (see `import`).
    imported: import::IdMapCache,
    /// The enrollment's decisions, once read or written (see `enrollment`).
    enrollment: enrollment::OverlayCache,
    /// What the session keeps of its sealed history while it's open (see `entry_by_mid` and
    /// `keep_sealed_derived`).
    sealed_entries: read::SealedEntries,
    /// Told of every batch this writer appends (see [`Session::observe_appends`]).
    observer: Option<PositionedAppendObserver>,
    /// The open log's records kept in memory (see `kept`).
    kept: kept::KeptRecords,
    /// The media fold, and the media index the open log's checkpoint names (see `media_index`).
    media: media_index::MediaState,
    /// Set while reads come from memory only (see [`Session::in_memory`]).
    memory_only: std::sync::atomic::AtomicBool,
    /// Set when a read refused for needing storage while reads came from memory only.
    memory_refused: std::sync::atomic::AtomicBool,
    retired_tag_floor: u64,
}

/// Where a session stood when it was read: the head version, the open log, and how many of the
/// log's bytes were read. A view loaded at a position never changes, so it can be kept and served
/// again for as long as the session stays there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Position {
    pub version: Version,
    pub log: Id128,
    pub len: u64,
}

/// What a session's writer tells the observer of its appends (see [`Session::observe_appends`]).
#[derive(Clone, Copy)]
pub enum Appended<'a> {
    /// Records the writer appended, the first at seq `first`, told once their append returned.
    /// `durable` says the append ended with a barrier; records appended without one become
    /// durable at the next barrier, told as `Synced` or by the next durable `Records`.
    Records {
        first: u64,
        records: &'a [Record],
        durable: bool,
    },
    /// Every record appended so far is durable.
    Synced,
    /// A revert replaced the session's history after a cut: records told before may be gone
    /// from the branch the session now holds.
    Replaced,
}

/// The observer of a session's appends: it runs on the writer's task, right after each append
/// returns, so it sees the records in seq order and only once they're written.
pub type AppendObserver = Box<dyn FnMut(Appended<'_>) + Send + Sync>;

/// Writer-owned byte boundaries sampled when an append observer is called. `written_through`
/// bounds every record told by that callback; only `synced_through` is crash-durable.
#[derive(Clone, Copy)]
pub struct AppendPosition {
    pub log: Id128,
    pub written_through: u64,
    pub synced_through: u64,
}

pub type PositionedAppendObserver = Box<dyn FnMut(Appended<'_>, AppendPosition) + Send + Sync>;

impl<B: SessionBackend> Session<B> {
    /// Creates a session at `addr`, in the cipher mode `deployment` creates sessions in (see
    /// [`DeploymentKey::creating`]): the mode is recorded in the head and never changes.
    /// `appends_in_conversation` is the conversation fold's setting: `false` for sessions on the
    /// current compaction contract, which keep text delivered with a tool result apart from the
    /// result.
    pub async fn create(
        backend: B,
        addr: SessionAddr,
        triple: SessionTriple,
        deployment: &DeploymentKey,
        appends_in_conversation: bool,
    ) -> Result<Self, SessionError> {
        Self::create_with_history_version(
            backend,
            addr,
            triple,
            deployment,
            appends_in_conversation,
            0,
        )
        .await
    }

    /// Creates a converted session whose served-history counter continues from the source.
    /// The initial checkpoint and head carry the same version, including after creation recovery.
    pub async fn create_with_history_version(
        backend: B,
        addr: SessionAddr,
        triple: SessionTriple,
        deployment: &DeploymentKey,
        appends_in_conversation: bool,
        history_version: u64,
    ) -> Result<Self, SessionError> {
        let session_id = random_id();
        let (key, wrapped, cipher) = match deployment.new_sessions() {
            Cipher::XChaCha20Poly1305 => {
                let key = DataKey::generate(DATA_KEY_ID);
                let wrapped = keys::wrap(deployment, &session_id, &key)
                    .ok_or(SessionError::Head(HeadError::NotLoaded))?;
                let wrapped = vec![WrappedKey {
                    id: key.id,
                    deployment: deployment.id(),
                    wrapped: ByteBuf(wrapped),
                }];
                // An encrypted head leaves the field out, as every head did before plain mode.
                (key, wrapped, None)
            }
            Cipher::None => (DataKey::plain(), Vec::new(), Some(Cipher::None)),
        };
        let log = random_id();
        let head = Head {
            format: head::HEAD_FORMAT,
            session_id,
            triple,
            incarnation: random_id(),
            history_version,
            residence: Residence::Creating(log),
            features: Vec::new(),
            keys: wrapped,
            generation: random_id(),
            next_seq: 1,
            open_log: None,
            last_change: None,
            transcript_dir: None,
            ledger_dir: None,
            pending: Vec::new(),
            id_index: None,
            cold_since_ms: None,
            appends_in_conversation: appends_in_conversation.then_some(true),
            cipher,
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
        backend: B,
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
        let operations = Operations {
            history_version: head.history_version,
            ..Operations::default()
        };
        let checkpoint = Checkpoint {
            covers_through: 0,
            next_seq: head.next_seq,
            conversation: conversation.clone(),
            operational: operations.clone(),
            transcript_base: None,
            ledger_base: None,
            activity_ms: None,
            media: None,
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
        let head_next_seq = head.next_seq;
        Ok(Self {
            backend,
            addr,
            head,
            version,
            key: key.clone(),
            encoder,
            log,
            poisoned: false,
            conversation,
            operations,
            open_ids: OpenIds::default(),
            window: OpenLogIndex::default(),
            imported: Default::default(),
            enrollment: Default::default(),
            sealed_entries: Default::default(),
            observer: None,
            kept: kept::KeptRecords::from_next(head_next_seq, true),
            media: Default::default(),
            memory_only: Default::default(),
            memory_refused: Default::default(),
            retired_tag_floor: 1,
        })
    }

    /// Loads the session at `addr` and opens it for writing. `triple` must be the one it was
    /// created under.
    pub async fn load(
        backend: B,
        addr: SessionAddr,
        triple: &SessionTriple,
        deployment: &DeploymentKey,
    ) -> Result<Self, SessionError> {
        match Self::read(backend, addr, triple, deployment, None).await? {
            Loaded::Creating {
                backend,
                addr,
                head,
                version,
                key,
                log,
            } => {
                // Creation crashed before its commit, so the session has no records yet.
                let appends = head.appends_in_conversation.unwrap_or(false);
                let mut session =
                    Self::create_log(backend, addr, *head, version, &key, log, appends).await?;
                session.open_writer(&key).await?;
                Ok(session)
            }
            Loaded::Open {
                mut session,
                mut read,
                end,
            } => {
                if let Residence::Moving(cut_id) = &session.head.residence {
                    return Err(SessionError::SessionMoving {
                        cut_id: cut_id.clone(),
                    });
                }
                let key = session.key.clone();
                session.kept = kept::KeptRecords::writer_of(
                    std::mem::take(&mut read.records),
                    session.encoder.next_seq(),
                );
                session.open_writer_over(&key, &read, end).await?;
                Ok(*session)
            }
        }
    }

    /// Loads the session at `addr` for reading only, writing nothing. A torn tail reads as not
    /// yet written. A log that needs a recovery refuses `NeedsRecovery`, since only a writer
    /// may repair. A session whose creation never committed reads as `Missing`: it holds no
    /// record yet. A session a writer holds reads as of the writer's last write that reached
    /// the backend.
    pub async fn view(
        backend: B,
        addr: SessionAddr,
        triple: &SessionTriple,
        deployment: &DeploymentKey,
    ) -> Result<SessionView<B>, SessionError> {
        Self::loaded_view(Self::read(backend, addr, triple, deployment, None).await?)
    }

    /// Where the session at `addr` stands now (see [`Position`]), read from its head and its open
    /// log's length, without reading the log: what [`Session::view_at`] would load. `None` for a
    /// session that has no head or whose creation never committed.
    pub async fn stored_position(
        backend: &B,
        addr: &SessionAddr,
        deployment: &DeploymentKey,
    ) -> Result<Option<Position>, SessionError> {
        let Some((stored, version)) = backend.read_head(addr).await? else {
            return Ok(None);
        };
        let (head, _) = head::open(&stored, deployment).map_err(SessionError::Head)?;
        let Some(open) = head
            .open_log
            .as_ref()
            .filter(|_| matches!(head.residence, Residence::Local(()) | Residence::Moving(_)))
        else {
            return match head.residence {
                Residence::Recovering(_) => Err(SessionError::NeedsRecovery),
                Residence::Moved(moved) => Err(SessionError::SessionMoved {
                    holder: moved.holder,
                }),
                _ => Ok(None),
            };
        };
        let len = backend.log_end(addr, &open.change).await?;
        Ok(Some(Position {
            version,
            log: open.change,
            len,
        }))
    }

    /// Loads the session at `addr` for reading only, as [`Session::view`] does, exactly at
    /// `position`: the head must still be at its version, and only the open log's first
    /// `position.len` bytes are read, so what a writer appended since isn't seen. A head that
    /// moved refuses [`SessionError::Moved`].
    pub async fn view_at(
        backend: B,
        addr: SessionAddr,
        triple: &SessionTriple,
        deployment: &DeploymentKey,
        position: &Position,
    ) -> Result<SessionView<B>, SessionError> {
        Self::loaded_view(Self::read(backend, addr, triple, deployment, Some(position)).await?)
    }

    fn loaded_view(loaded: Loaded<B>) -> Result<SessionView<B>, SessionError> {
        match loaded {
            Loaded::Creating { .. } => Err(SessionError::Missing),
            Loaded::Open {
                mut session,
                mut read,
                ..
            } => {
                // No writer was opened, so nothing may be appended: every write checks this.
                session.poisoned = true;
                session.kept = kept::KeptRecords::whole_log(std::mem::take(&mut read.records));
                Ok(SessionView(*session))
            }
        }
    }

    /// Reads the session's head, checkpoint and open log and folds them, writing nothing. With
    /// `at`, the head must be at that version and its open log the one named, and only the log's
    /// first `at.len` bytes are read.
    async fn read(
        backend: B,
        addr: SessionAddr,
        triple: &SessionTriple,
        deployment: &DeploymentKey,
        at: Option<&Position>,
    ) -> Result<Loaded<B>, SessionError> {
        let (stored, version) = backend
            .read_head(&addr)
            .await?
            .ok_or(SessionError::Missing)?;
        if at.is_some_and(|at| at.version != version) {
            return Err(SessionError::Moved);
        }
        let (head, key) = head::open(&stored, deployment).map_err(SessionError::Head)?;
        if &head.triple != triple {
            return Err(SessionError::TripleMismatch);
        }
        if let Residence::Recovering(_) = head.residence {
            return Err(SessionError::NeedsRecovery);
        }
        if let Residence::Moved(moved) = &head.residence {
            return Err(SessionError::SessionMoved {
                holder: moved.holder.clone(),
            });
        }
        if let Residence::Creating(log) = head.residence {
            return Ok(Loaded::Creating {
                backend,
                addr,
                head: Box::new(head),
                version,
                key,
                log,
            });
        }
        let open = head
            .open_log
            .clone()
            .ok_or(SessionError::Backend(BackendError::Corrupt(
                "local head without a log",
            )))?;
        let end = match at {
            Some(at) if at.log != open.change => return Err(SessionError::Moved),
            Some(at) => at.len,
            None => backend.log_end(&addr, &open.change).await?,
        };
        let bytes = backend.read_log(&addr, &open.change, 0..end).await?;
        let bind = log_bind(&head.session_id, &open.change);
        let frame_key = key.frame_key();
        // A clean log, or one ending in a torn tail (a write cut short, or zero-filled blocks),
        // opens; debris needs a recovery first, and damage to acknowledged bytes refuses.
        let read = match crate::damage::analyse(
            &bytes,
            &frame_key,
            &bind,
            open.synced_len,
            chain_base,
            None,
        )? {
            crate::damage::Verdict::Opens(read) => read,
            crate::damage::Verdict::Debris(_) => return Err(SessionError::NeedsRecovery),
            crate::damage::Verdict::Corrupt { at, durable_bound } => {
                return Err(SessionError::Corrupt { at, durable_bound })
            }
        };
        let checkpoint = Checkpoint::decode(&read.checkpoint).map_err(SessionError::Checkpoint)?;

        let last_seq = read.records.last().map(|(seq, _)| *seq);
        let next_seq = head
            .next_seq
            .max(checkpoint.next_seq)
            .max(last_seq.map_or(0, |seq| seq + 1));
        let mut media = media_index::Loading::new(&checkpoint);
        let mut conversation = checkpoint.conversation;
        let mut operations = checkpoint.operational;
        let mut open_ids = OpenIds::default();
        let mut window = OpenLogIndex::from_checkpoint(checkpoint.activity_ms, checkpoint.next_seq);
        for (seq, record) in &read.records {
            window.note(*seq, record);
            media.note(*seq, record, &conversation, &operations);
            if record.stream() == Stream::Ledger {
                if *seq <= checkpoint.covers_through {
                    continue;
                }
                open_ids.add(*seq, record);
            }
            apply(&mut conversation, &mut operations, *seq, record).map_err(SessionError::Fold)?;
        }
        let session = Self {
            backend,
            addr,
            head,
            version,
            key: key.clone(),
            encoder: LogEncoder::resume(frame_key, bind, &read, next_seq),
            log: open.change,
            poisoned: false,
            conversation,
            operations,
            open_ids,
            window,
            imported: Default::default(),
            enrollment: Default::default(),
            sealed_entries: Default::default(),
            observer: None,
            // Set by whoever takes the loaded session: a view keeps every record, a writer its
            // latest run's.
            kept: kept::KeptRecords::default(),
            media: media.finish(),
            memory_only: Default::default(),
            memory_refused: Default::default(),
            retired_tag_floor: 1,
        };
        Ok(Loaded::Open {
            session: Box::new(session),
            read,
            end,
        })
    }

    /// Every record of the open log after its checkpoint, with its seq, read again from the
    /// backend: one read of the open log. For a reader that needs whole records the folds keep only
    /// a summary of, such as the compaction views (the fold keeps their version and checksum, the
    /// records their edits). The writer appended every one of them, so a log that no longer reads
    /// clean means something else wrote it, and the read refuses. A view, and a read from memory
    /// only, are answered from the records kept when those are the whole log.
    pub async fn open_log_records(&self) -> Result<Vec<(u64, Record)>, SessionError> {
        if self.reads_kept() {
            if let Some(records) = self.kept.whole() {
                return Ok(records.to_vec());
            }
        }
        self.storage_read()?;
        Ok(self.place().open_log().await?.records)
    }

    /// Where this session's records are stored now.
    fn place(&self) -> stored::Place<'_, B> {
        stored::Place {
            backend: &self.backend,
            addr: &self.addr,
            head: &self.head,
            key: &self.key,
            log: &self.log,
        }
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
            batches: Vec::new(),
            batch_starts: Vec::new(),
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
        // A writer opening the session wakes it, if it was cold.
        self.head.cold_since_ms = None;
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
        // A change whose intent ends the log never committed: a committed change's new log
        // would be the head's open log. Undo it: delete what it created, then reopen the log.
        let mut batch = Vec::new();
        if let Some(change) = self.operations.open_change {
            self.backend
                .delete_change(&self.addr, &change, self.version)
                .await?;
            if change != self.log {
                self.backend
                    .delete_log(&self.addr, &change, self.version)
                    .await?;
            }
            batch.push(Record::ChangeAborted(ChangeAborted { change }));
        }
        batch.push(opened);
        self.append(&batch, true).await?;
        Ok(())
    }

    /// Folds `records` into the state, then appends them as one batch, synced if `barrier`.
    /// Returns the seq of the first record.
    pub async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        let (first, media) = self.append_folding(records, barrier).await?;
        self.media.handed(media);
        Ok(first)
    }

    /// [`Session::append`], returning what the batch's records handed over to the image engine.
    async fn append_folding(
        &mut self,
        records: &[Record],
        barrier: bool,
    ) -> Result<(u64, MediaFeed), SessionError> {
        let timing = crate::metrics::Phase::Append
            .start()
            .with_session(|| Some(self.addr.0.clone()));
        if self.poisoned {
            return Err(SessionError::Poisoned);
        }
        // A closed log accepts only a batch that opens with its change's abort.
        if self.operations.open_change.is_some()
            && !matches!(records.first(), Some(Record::ChangeAborted(_)))
        {
            return Err(SessionError::ChangeOpen);
        }
        let encoded = self.encoder.batch(records)?;
        let first = encoded.first_seq.expect("a batch holds records");
        self.poisoned = true;
        let mut media = MediaFeed::default();
        for (position, record) in records.iter().enumerate() {
            check_announced_mid(&self.conversation, &self.operations, record)
                .map_err(SessionError::Fold)?;
            self.media.fold.apply(
                first + position as u64,
                record,
                &self.conversation,
                &self.operations,
                &mut media,
            );
            apply(
                &mut self.conversation,
                &mut self.operations,
                first + position as u64,
                record,
            )
            .map_err(SessionError::Fold)?;
        }
        self.media.fold.flush(&mut media);
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
        for (position, record) in records.iter().enumerate() {
            if record.stream() == Stream::Ledger {
                self.open_ids.add(first + position as u64, record);
            }
            self.window.note(first + position as u64, record);
            self.kept.note(first + position as u64, record);
        }
        self.poisoned = false;
        drop(timing);
        self.observed(Appended::Records {
            first,
            records,
            durable: barrier,
        });
        Ok((first, media))
    }

    /// Tells `observer` of every batch this writer appends from now on, right after its append
    /// returns: the session's own appends, a seal's change records and the records it carries
    /// into the new log, and a revert's replacing the history. One observer at a time; a new one
    /// replaces the last.
    pub fn observe_appends(&mut self, mut observer: AppendObserver) {
        self.observer = Some(Box::new(move |appended, _| observer(appended)));
    }

    /// Observe appends alongside the existing observer, without changing its records or order.
    pub fn add_append_observer(&mut self, mut observer: AppendObserver) {
        self.add_positioned_append_observer(Box::new(move |appended, _| observer(appended)));
    }

    /// Observe appends with the writer's actual written and synced byte boundaries. The
    /// snapshot is taken on the writer task, before any observer can hand off a live hint.
    pub fn add_positioned_append_observer(&mut self, mut observer: PositionedAppendObserver) {
        let prior = self.observer.take();
        self.observer = Some(match prior {
            Some(mut prior) => Box::new(move |appended, position| {
                prior(appended, position);
                observer(appended, position);
            }),
            None => observer,
        });
    }

    fn observed(&mut self, appended: Appended<'_>) {
        if let Some(observer) = self.observer.as_mut() {
            observer(
                appended,
                AppendPosition {
                    log: self.log,
                    written_through: self.encoder.offset(),
                    synced_through: self.encoder.synced_through(),
                },
            );
        }
    }

    /// The log holding `seq`, from its start through `last` (see
    /// [`StoredRecords::from_log_holding`]), read over this session's own backend. It reads
    /// storage, so a read from memory only refuses it.
    pub async fn from_log_holding(&self, seq: u64, last: u64) -> Result<LogFrom, SessionError> {
        self.storage_read()?;
        stored::from_log_holding(&self.place(), seq, last).await
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

    /// Deletes the session: its head, so the address reads as having no session, then its logs
    /// and objects (see [`SessionBackend::delete_session`]). Its writer does this, under the
    /// session's lease, when the session it created holds nothing anyone sent, so a refused
    /// creating send leaves no session behind. A poisoned writer may not: it can't tell what its
    /// failed append left in the log.
    pub async fn discard(self) -> Result<(), SessionError> {
        if self.poisoned {
            return Err(SessionError::Poisoned);
        }
        self.backend
            .delete_session(&self.addr, self.version)
            .await?;
        Ok(())
    }

    /// Deletes the stored session at `addr` whole, as [`Session::discard`] does: its head first,
    /// so the address reads as having no session, then its kept head versions, logs and objects,
    /// which hold its media and its id index. This is for a caller that holds the session's lease
    /// without having opened a writer: retention deleting a session whose policy has expired.
    /// `at` is the head version the caller read when it decided, so a head that has moved since
    /// refuses `Fenced` and nothing is deleted.
    pub async fn delete(backend: B, addr: &SessionAddr, at: Version) -> Result<(), SessionError> {
        backend.delete_session(addr, at).await?;
        Ok(())
    }

    /// The backend the session reads and writes through.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    pub fn head(&self) -> &Head {
        &self.head
    }

    /// The hashed storage address, suitable for diagnostics without exposing the project root,
    /// harness name or session name it was computed from.
    pub fn address(&self) -> &SessionAddr {
        &self.addr
    }

    /// Carries the legacy ledger floor across deletion, without changing the projection's
    /// high-water mark. The address's expiry markers supply it again on every load.
    pub fn raise_retired_tag_floor(&mut self, next_tag: u64) {
        self.retired_tag_floor = self.retired_tag_floor.max(next_tag);
    }

    pub fn retired_tag_floor(&self) -> u64 {
        self.retired_tag_floor
    }

    /// The key this session's subscribe cursors are digested under.
    pub fn cursor_key(&self) -> crate::keys::CursorKey {
        self.key.cursor_key()
    }

    /// The seq the next appended record will take.
    pub fn next_seq(&self) -> u64 {
        self.encoder.next_seq()
    }

    /// The open log's change id and its current length.
    pub fn open_log(&self) -> (Id128, u64) {
        (self.log, self.encoder.offset())
    }

    /// Known session activity: the checkpoint's baseline advanced by fresh open-log records
    /// (`Record::activity_ms`), or `None` when neither carries a time. Sealing preserves it.
    pub fn open_log_activity_ms(&self) -> Option<u64> {
        self.window.newest_activity_ms
    }

    /// The id for a new message whose `EntryFacts` record will be the `offset`th record (from 0)
    /// of the next batch: `g<generation>.<seq of that record>`. A seq can repeat after a crash,
    /// since a record that never became durable gives its number back, but never under the same
    /// writer generation, so the pair never repeats. Converted ids start with `m`, so the two
    /// forms never meet.
    pub fn message_id(&self, offset: u64) -> String {
        format!(
            "{}.{}",
            self.message_id_prefix(),
            self.encoder.next_seq() + offset
        )
    }

    /// What every new message id starts with under this writer: `g<generation>`, so the id of
    /// a message whose `EntryFacts` record takes seq `s` is `<prefix>.<s>`.
    pub fn message_id_prefix(&self) -> String {
        let generation = crate::codec::hex(&self.head.generation.0);
        format!("g{}", &generation[..16])
    }
}

mod kept;
pub mod moves;
pub use kept::InMemory;
mod media;
use media::OpenLogIndex;
pub use media::{
    message_media, record_objects, sent_messages, stored_media, MediaError, MediaObjects,
    NamedMedium, NewMedia,
};
mod import;
pub use import::{is_broca_id_form, NewInputMedia};
mod media_index;
pub use crate::fold::media::{
    limits_in, LimitsSource, MediaFeed, MediaFold, MediaIndexRef, MediaItem, MediaStart,
    RunStartNeedingLimits,
};
pub use media_index::{
    AcceptedMessage, HeldSkeleton, LoadedMedia, MediaIndexSave, MediaIndexSource, PlacedSkeleton,
    SavedIndex, ShownOccurrence, StoredAging, StoredBinding, StoredCapture, StoredMediaId,
    StoredMediaIndex, StoredOccurrence, StoredReplacement, StoredSelection, StoredSkeleton,
    StoredSlot, MEDIA_INDEX_VERSION,
};
mod enrollment;
pub use enrollment::EnrollmentOverlay;

mod read;
mod tail;
pub use read::{served_window, window_entries, StoredEntry};
mod stored;
pub use stored::{LogFrom, StoredRecords};
mod cold;
pub use cold::Cold;
mod recover;
pub use recover::{Recovered, Recovery, REMOVED_KEEP_MS};
mod revert;
pub use revert::{RevertRefusal, Reverted};
mod seal;
pub use seal::{Carry, Sealed, GRACE_MS};
mod collect;
pub use collect::{collect_stored, stored_due};
pub mod restore;

#[cfg(test)]
mod tests;
