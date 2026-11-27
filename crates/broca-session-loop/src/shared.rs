//! The run's writer when the session belongs to an actor.
//!
//! A session actor owns its `Session` for its whole life, and appends to it while a run is
//! active: a steer or a queued send is durable before its sender hears back, and that can't
//! wait for a model step to end. So a run doesn't hold the session. It writes through an
//! [`ActorWriter`], which sends each call to the actor and keeps its own copy of the two folds.
//!
//! **The copy.** It's cloned from the actor's folds when the run starts. Every reply the actor
//! sends carries the records the actor appended on its own since the copy was last brought up
//! to date, in seq order, and the copy folds them before anything else. So after each writer
//! call the copy equals the session's folds. Between calls it may lag them, as a snapshot would.
//! The actor tells the link which records were its own through [`RunLink::note_own`].
//!
//! **`append_with`.** Its batch is built from the folds as they stand, and must not miss a
//! record another appender added just before it. So the writer first asks the actor to hold:
//! the actor sends the records the copy lacks and then waits for the batch, appending nothing
//! else meanwhile. The writer brings its copy up to date, builds the batch and sends it. The
//! hold lasts only for that synchronous build.
//!
//! **Seals** run in the actor on the session itself, and replace the copy, since the window
//! changes at a cut. Reads that go past the folds are requests to the actor.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use broca_core::tool_media::MediaCapture;
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::record::{Record, RunStart};
use broca_session_store::session::{Carry, SessionError, StoredEntry};
use tokio::sync::{mpsc, oneshot};

use crate::writer::Writer;

/// Records the actor appended on its own, with their seqs, oldest first: what a run's copy of
/// the folds hasn't folded yet.
pub type Missed = Vec<(u64, Record)>;

/// The folds and next seq as a seal left them.
pub struct Snapshot {
    pub conversation: Conversation,
    pub operations: Operations,
    pub next_seq: u64,
    pub message_id_prefix: String,
}

type Reply<T> = oneshot::Sender<Result<T, SessionError>>;

/// The records the run's copy lacks and the private sender for its batch.
pub struct HoldReply {
    missed: Missed,
    batch: oneshot::Sender<HeldBatch>,
}

struct HeldBatch {
    records: Vec<Record>,
    barrier: bool,
    reply: Reply<u64>,
}

/// One call a run's writer makes to the actor that owns the session.
pub enum Request {
    Append {
        records: Vec<Record>,
        barrier: bool,
        reply: Reply<(Missed, u64)>,
    },
    /// Starts a hold: the actor sends a private continuation and waits for the batch or for the
    /// run to drop the continuation. Other requests stay queued until the hold ends.
    Hold {
        reply: Reply<HoldReply>,
    },
    /// Asks for the records the run's copy lacks, appending nothing.
    CaughtUp {
        reply: Reply<Missed>,
    },
    SendRecords {
        submission_id: String,
        reply: Reply<(Missed, Vec<Record>)>,
    },
    OpenLogRecords {
        reply: Reply<Vec<(u64, Record)>>,
    },
    ResponsesReasoningRemovals {
        run_id: String,
        before_seq: Option<u64>,
        reply: Reply<crate::responses_reasoning::ResponsesReasoningRemovals>,
    },
    CutRecords {
        reply: Reply<Vec<Record>>,
    },
    ApplyMayEraseNotes {
        generation: u64,
        reply: Reply<bool>,
    },
    RunHasAttempt {
        run_id: String,
        reply: Reply<bool>,
    },
    SigningConfig {
        run_id: String,
        reply: Reply<Option<crate::writer::SigningConfig>>,
    },
    OpenRunTail {
        run_id: String,
        first: u64,
        reply: Reply<Vec<(u64, Record)>>,
    },
    AcceptedSend {
        account: String,
        since_ms: u64,
        reply: Reply<broca_core::provider::AcceptedSendEvidence>,
    },
    CachePredecessor {
        reply: tokio::sync::oneshot::Sender<
            Result<Option<broca_core::provider::CachePredecessor>, String>,
        >,
    },
    RunStart {
        run_id: String,
        reply: Reply<Option<RunStart>>,
    },
    ToolIntent {
        call_key: String,
        reply: Reply<Option<broca_session_store::record::ToolIntent>>,
    },
    EntryByMid {
        mid: String,
        reply: Reply<Option<StoredEntry>>,
    },
    EntryAt {
        ordinal: u64,
        reply: Reply<Option<StoredEntry>>,
    },
    MovedThrough {
        reply: Reply<Option<(String, u64)>>,
    },
    EntriesByMid {
        mids: Vec<String>,
        reply: Reply<Vec<StoredEntry>>,
    },
    SealedDerived {
        reply: Reply<Option<Arc<dyn std::any::Any + Send + Sync>>>,
    },
    KeepSealedDerived {
        value: Arc<dyn std::any::Any + Send + Sync>,
        reply: Reply<()>,
    },
    Enrollment {
        reply: Reply<Option<broca_session_store::session::EnrollmentOverlay>>,
    },
    PutEnrollment {
        entries: broca_session_store::record::EnrolledEntries,
        reply: Reply<broca_session_store::record::EnrollmentOverlayRef>,
    },
    WindowMessageWhole {
        ordinal: u64,
        reply: Reply<Arc<broca_protocol::Message>>,
    },
    Release {
        blocks: Vec<(u64, Vec<usize>)>,
        reply: Reply<()>,
    },
    Seal {
        /// `Some` for a cut before the entry with this ordinal, `None` for a ledger seal.
        ordinal: Option<u64>,
        carry: Vec<Record>,
        window_repetition: Option<serde_json::Value>,
        now_ms: u64,
        reply: Reply<Snapshot>,
    },
}

/// The run's writer: a client of the actor that owns the session.
pub struct ActorWriter {
    message_ids: Option<crate::entries::MessageIds>,
    requests: mpsc::Sender<Request>,
    conversation: Conversation,
    operations: Operations,
    retired_tag_floor: u64,
    session_id: String,
    next_seq: u64,
    message_id_prefix: String,
    media: Option<Arc<dyn MediaCapture>>,
    /// The seq of the session's last record known durable, for reporters that must name only
    /// durable records.
    durable_head: Arc<AtomicU64>,
    /// Where the run's transcript handle goes for the actor's memory report, when it asks for one
    /// (see [`ActorWriter::sharing_window`]).
    window: Option<RunWindow>,
}

/// Where a run leaves its owner a weak handle on the transcript it keeps (see
/// [`Writer::share_window`]). The owner reads it for its memory report and clears it when the run
/// ends.
pub type RunWindow = Arc<std::sync::Mutex<Option<crate::compaction::WeakWindow>>>;

/// The actor's end of a run's writer.
pub struct RunLink {
    requests: mpsc::Receiver<Request>,
    /// What the actor appended on its own since the run's copy last caught up.
    missed: Missed,
}

struct PredecessorLoader(mpsc::Sender<Request>);

#[async_trait::async_trait]
impl broca_core::provider::CachePredecessorLoader for PredecessorLoader {
    async fn load(&self) -> Result<Option<broca_core::provider::CachePredecessor>, String> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        self.0
            .send(Request::CachePredecessor { reply })
            .await
            .map_err(|_| {
                "the session owner closed before reading the cache predecessor".to_owned()
            })?;
        receive
            .await
            .map_err(|_| "the session owner dropped the cache predecessor read".to_owned())?
    }
}

impl ActorWriter {
    /// A deferred read through the owning actor. The provider consumes it before recording
    /// its first attempt, so the newest durable attempt is still its predecessor.
    pub fn cache_predecessor_loader(
        &self,
    ) -> Arc<dyn broca_core::provider::CachePredecessorLoader> {
        Arc::new(PredecessorLoader(self.requests.clone()))
    }
    /// Shares final entry ids with the actor's live-event observer, without writing records.
    pub fn with_message_ids(mut self, ids: Option<crate::entries::MessageIds>) -> Self {
        self.message_ids = ids;
        self
    }

    /// A writer for a run, starting from the session's folds as they stand, and the actor's end
    /// of it. `media` is where a tool result's media is captured while its call runs; the actor
    /// stores those bytes when it appends the result's record.
    pub fn new<W: Writer>(session: &W, media: Option<Arc<dyn MediaCapture>>) -> (Self, RunLink) {
        // One request at a time: the writer waits for each reply before sending the next.
        let (sender, receiver) = mpsc::channel(1);
        (
            Self {
                message_ids: None,
                requests: sender,
                conversation: session.conversation().clone(),
                operations: session.operations().clone(),
                retired_tag_floor: session.retired_tag_floor(),
                session_id: session.session_id(),
                next_seq: session.next_seq(),
                message_id_prefix: session.message_id_prefix(),
                media,
                // A run starts after a barrier: the writer's open, a run's end or the actor's
                // own append, each synced. So every record the session holds is durable.
                durable_head: Arc::new(AtomicU64::new(session.next_seq().saturating_sub(1))),
                window: None,
            },
            RunLink {
                requests: receiver,
                missed: Vec::new(),
            },
        )
    }

    /// This writer leaves the run's transcript handle in `window` (see [`RunWindow`]).
    pub fn sharing_window(mut self, window: RunWindow) -> Self {
        self.window = Some(window);
        self
    }

    async fn ask<T>(&self, make: impl FnOnce(Reply<T>) -> Request) -> Result<T, SessionError> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(make(reply))
            .await
            .map_err(|_| SessionError::OwnerGone)?;
        answer.await.map_err(|_| SessionError::OwnerGone)?
    }

    /// The seq of the session's last record known durable. It moves on each barrier the run
    /// appends and each record the actor appends, which it syncs before anyone hears of it.
    pub fn durable_head(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.durable_head)
    }

    fn durable_through(&self, seq: u64) {
        self.durable_head.fetch_max(seq, Ordering::AcqRel);
    }

    /// Folds what the actor appended since the copy last caught up.
    fn catch_up(&mut self, missed: Missed) -> Result<(), SessionError> {
        for (seq, record) in &missed {
            self.fold(*seq, record)?;
            self.durable_through(*seq);
        }
        Ok(())
    }

    fn fold(&mut self, seq: u64, record: &Record) -> Result<(), SessionError> {
        broca_session_store::session::apply(
            &mut self.conversation,
            &mut self.operations,
            seq,
            record,
        )
        .map_err(SessionError::Fold)?;
        self.next_seq = seq + 1;
        Ok(())
    }

    /// Folds the run's own batch, appended at `first`.
    fn fold_own(&mut self, first: u64, records: &[Record]) -> Result<(), SessionError> {
        for (position, record) in records.iter().enumerate() {
            self.fold(first + position as u64, record)?;
        }
        Ok(())
    }

    fn replace(&mut self, snapshot: Snapshot) {
        self.conversation = snapshot.conversation;
        self.operations = snapshot.operations;
        self.next_seq = snapshot.next_seq;
        self.message_id_prefix = snapshot.message_id_prefix;
        // A seal commits the session's new log, synced, before it answers.
        self.durable_through(self.next_seq.saturating_sub(1));
    }

    async fn seal(
        &mut self,
        ordinal: Option<u64>,
        carry: &Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        let snapshot = self
            .ask(|reply| Request::Seal {
                ordinal,
                carry: carry.records.to_vec(),
                window_repetition: carry.window_repetition.clone(),
                now_ms,
                reply,
            })
            .await?;
        self.replace(snapshot);
        Ok(())
    }
}

impl Writer for ActorWriter {
    async fn cut_records(&self) -> Result<Vec<Record>, SessionError> {
        self.ask(|reply| Request::CutRecords { reply }).await
    }

    async fn apply_may_erase_notes(&self, generation: u64) -> Result<bool, SessionError> {
        self.ask(|reply| Request::ApplyMayEraseNotes { generation, reply })
            .await
    }

    async fn run_has_attempt(&self, run_id: &str) -> Result<bool, SessionError> {
        self.ask(|reply| Request::RunHasAttempt {
            run_id: run_id.into(),
            reply,
        })
        .await
    }

    async fn signing_config(
        &self,
        run_id: &str,
    ) -> Result<Option<crate::writer::SigningConfig>, SessionError> {
        self.ask(|reply| Request::SigningConfig {
            run_id: run_id.into(),
            reply,
        })
        .await
    }
    async fn responses_reasoning_removals(
        &self,
        run_id: &str,
        before_seq: Option<u64>,
    ) -> Result<crate::responses_reasoning::ResponsesReasoningRemovals, SessionError> {
        self.ask(|reply| Request::ResponsesReasoningRemovals {
            run_id: run_id.into(),
            before_seq,
            reply,
        })
        .await
    }

    async fn accepted_send(
        &self,
        account: &str,
        since_ms: u64,
    ) -> Result<broca_core::provider::AcceptedSendEvidence, SessionError> {
        self.ask(|reply| Request::AcceptedSend {
            account: account.to_owned(),
            since_ms,
            reply,
        })
        .await
    }

    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        // A batch that ends or pauses the run is a barrier whatever the caller asked, as in
        // `append_with`: only then is it durable when the run reports it.
        let barrier = barrier || crate::writer::ends_run(records);
        let (missed, first) = self
            .ask(|reply| Request::Append {
                records: records.to_vec(),
                barrier,
                reply,
            })
            .await?;
        self.catch_up(missed)?;
        self.fold_own(first, records)?;
        if barrier && !records.is_empty() {
            self.durable_through(first + records.len() as u64 - 1);
        }
        Ok(first)
    }

    async fn caught_up(&mut self) -> Result<(), SessionError> {
        let missed = self.ask(|reply| Request::CaughtUp { reply }).await?;
        self.catch_up(missed)
    }

    async fn send_records(&mut self, submission_id: &str) -> Result<Vec<Record>, SessionError> {
        let (missed, records) = self
            .ask(|reply| Request::SendRecords {
                submission_id: submission_id.to_owned(),
                reply,
            })
            .await?;
        self.catch_up(missed)?;
        Ok(records)
    }

    async fn append_with(
        &mut self,
        build: &mut (dyn for<'s> FnMut(&'s Conversation, &'s Operations, u64) -> Vec<Record>
                  + Send),
        barrier: bool,
    ) -> Result<Vec<Record>, SessionError> {
        let HoldReply { missed, batch } = self.ask(|reply| Request::Hold { reply }).await?;
        self.catch_up(missed)?;
        let records = build(&self.conversation, &self.operations, self.next_seq);
        let barrier = barrier || crate::writer::ends_run(&records);
        let (reply, receive) = oneshot::channel();
        batch
            .send(HeldBatch {
                records: records.clone(),
                barrier,
                reply,
            })
            .map_err(|_| SessionError::OwnerGone)?;
        let first = receive.await.map_err(|_| SessionError::OwnerGone)??;
        self.fold_own(first, &records)?;
        if barrier && !records.is_empty() {
            self.durable_through(first + records.len() as u64 - 1);
        }
        Ok(records)
    }

    fn conversation(&self) -> &Conversation {
        &self.conversation
    }

    fn operations(&self) -> &Operations {
        &self.operations
    }

    fn session_id(&self) -> String {
        self.session_id.clone()
    }

    fn next_seq(&self) -> u64 {
        self.next_seq
    }

    fn retired_tag_floor(&self) -> u64 {
        self.retired_tag_floor
    }

    fn message_id_prefix(&self) -> String {
        self.message_id_prefix.clone()
    }

    fn message_ids(&self) -> Option<crate::entries::MessageIds> {
        self.message_ids.clone()
    }

    async fn open_log_records(&self) -> Result<Vec<(u64, Record)>, SessionError> {
        self.ask(|reply| Request::OpenLogRecords { reply }).await
    }

    async fn open_run_tail(
        &self,
        run_id: &str,
        first: u64,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        self.ask(|reply| Request::OpenRunTail {
            run_id: run_id.to_owned(),
            first,
            reply,
        })
        .await
    }

    async fn run_start(&self, run_id: &str) -> Result<Option<RunStart>, SessionError> {
        self.ask(|reply| Request::RunStart {
            run_id: run_id.to_owned(),
            reply,
        })
        .await
    }

    async fn window_message_whole(
        &self,
        ordinal: u64,
    ) -> Result<Arc<broca_protocol::Message>, SessionError> {
        if let Some(held) = self
            .conversation
            .window_message(ordinal)
            .and_then(|message| message.held())
        {
            return Ok(Arc::clone(held));
        }
        self.ask(|reply| Request::WindowMessageWhole { ordinal, reply })
            .await
    }

    /// Releases the blocks in the run's copy of the window and in the session's, which share
    /// their content: either alone frees nothing.
    async fn release(&mut self, blocks: &[(u64, Vec<usize>)]) -> Result<(), SessionError> {
        for (ordinal, indexes) in blocks {
            self.conversation.release(*ordinal, indexes);
        }
        self.ask(|reply| Request::Release {
            blocks: blocks.to_vec(),
            reply,
        })
        .await
    }

    async fn enrollment(
        &self,
    ) -> Result<Option<broca_session_store::session::EnrollmentOverlay>, SessionError> {
        self.ask(|reply| Request::Enrollment { reply }).await
    }

    async fn put_enrollment(
        &mut self,
        entries: &broca_session_store::record::EnrolledEntries,
    ) -> Result<broca_session_store::record::EnrollmentOverlayRef, SessionError> {
        self.ask(|reply| Request::PutEnrollment {
            entries: entries.clone(),
            reply,
        })
        .await
    }

    async fn tool_intent(
        &self,
        call_key: &str,
    ) -> Result<Option<broca_session_store::record::ToolIntent>, SessionError> {
        self.ask(|reply| Request::ToolIntent {
            call_key: call_key.to_owned(),
            reply,
        })
        .await
    }

    async fn seal_at_entry(
        &mut self,
        ordinal: u64,
        carry: &Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        self.seal(Some(ordinal), carry, now_ms).await
    }

    async fn seal_ledger(&mut self, carry: &Carry<'_>, now_ms: u64) -> Result<(), SessionError> {
        self.seal(None, carry, now_ms).await
    }

    async fn entry_by_mid(&self, mid: &str) -> Result<Option<StoredEntry>, SessionError> {
        self.ask(|reply| Request::EntryByMid {
            mid: mid.to_owned(),
            reply,
        })
        .await
    }

    async fn entry_at(&self, ordinal: u64) -> Result<Option<StoredEntry>, SessionError> {
        self.ask(|reply| Request::EntryAt { ordinal, reply }).await
    }

    async fn moved_through(&self) -> Result<Option<(String, u64)>, SessionError> {
        self.ask(|reply| Request::MovedThrough { reply }).await
    }

    async fn entries_by_mid(&self, mids: &[String]) -> Result<Vec<StoredEntry>, SessionError> {
        self.ask(|reply| Request::EntriesByMid {
            mids: mids.to_vec(),
            reply,
        })
        .await
    }

    async fn sealed_derived(
        &self,
    ) -> Result<Option<Arc<dyn std::any::Any + Send + Sync>>, SessionError> {
        self.ask(|reply| Request::SealedDerived { reply }).await
    }

    async fn keep_sealed_derived(
        &self,
        value: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<(), SessionError> {
        self.ask(|reply| Request::KeepSealedDerived { value, reply })
            .await
    }

    fn media(&self) -> Option<Arc<dyn MediaCapture>> {
        self.media.clone()
    }

    fn share_window(&self, window: crate::compaction::WeakWindow) {
        if let Some(slot) = &self.window {
            if let Ok(mut slot) = slot.lock() {
                *slot = Some(window);
            }
        }
    }
}

impl RunLink {
    /// Records that the actor appended `records` on its own, starting at `first`, so the run's
    /// copy folds them at its next call.
    pub fn note_own(&mut self, first: u64, records: &[Record]) {
        self.missed.extend(
            records
                .iter()
                .enumerate()
                .map(|(position, record)| (first + position as u64, record.clone())),
        );
    }

    /// The run's next request, or `None` once its writer is gone.
    pub async fn next(&mut self) -> Option<Request> {
        self.requests.recv().await
    }

    /// Answers one request against `session`, the actor's own writer. A hold is answered whole
    /// here: this waits for the batch that ends it, so nothing else is appended meanwhile.
    pub async fn serve<W: Writer>(&mut self, session: &mut W, request: Request) {
        match request {
            Request::Append {
                records,
                barrier,
                reply,
            } => {
                let result = session.append(&records, barrier).await;
                let _ = reply.send(result.map(|first| (std::mem::take(&mut self.missed), first)));
            }
            Request::Hold { reply } => {
                let (batch, receive) = oneshot::channel();
                let _ = reply.send(Ok(HoldReply {
                    missed: std::mem::take(&mut self.missed),
                    batch,
                }));
                if let Ok(HeldBatch {
                    records,
                    barrier,
                    reply,
                }) = receive.await
                {
                    let result = if records.is_empty() {
                        Ok(session.next_seq())
                    } else {
                        session.append(&records, barrier).await
                    };
                    let _ = reply.send(result);
                }
            }
            Request::CaughtUp { reply } => {
                let _ = reply.send(Ok(std::mem::take(&mut self.missed)));
            }
            Request::SendRecords {
                submission_id,
                reply,
            } => {
                let result = session.send_records(&submission_id).await;
                let _ =
                    reply.send(result.map(|records| (std::mem::take(&mut self.missed), records)));
            }
            Request::OpenLogRecords { reply } => {
                let _ = reply.send(session.open_log_records().await);
            }
            Request::ResponsesReasoningRemovals {
                run_id,
                before_seq,
                reply,
            } => {
                let _ = reply.send(
                    session
                        .responses_reasoning_removals(&run_id, before_seq)
                        .await,
                );
            }
            Request::CutRecords { reply } => {
                let _ = reply.send(crate::writer::Writer::cut_records(session).await);
            }
            Request::ApplyMayEraseNotes { generation, reply } => {
                let _ = reply
                    .send(crate::writer::Writer::apply_may_erase_notes(session, generation).await);
            }
            Request::RunHasAttempt { run_id, reply } => {
                let _ = reply.send(crate::writer::Writer::run_has_attempt(session, &run_id).await);
            }
            Request::SigningConfig { run_id, reply } => {
                let _ = reply.send(crate::writer::Writer::signing_config(session, &run_id).await);
            }
            Request::OpenRunTail {
                run_id,
                first,
                reply,
            } => {
                let _ = reply.send(session.open_run_tail(&run_id, first).await);
            }
            Request::AcceptedSend {
                account,
                since_ms,
                reply,
            } => {
                let _ = reply.send(session.accepted_send(&account, since_ms).await);
            }
            Request::CachePredecessor { reply } => {
                let _ = reply.send(Err(
                    "the session owner does not rebuild recorded requests".into()
                ));
            }
            Request::WindowMessageWhole { ordinal, reply } => {
                let _ = reply.send(session.window_message_whole(ordinal).await);
            }
            Request::Release { blocks, reply } => {
                let _ = reply.send(session.release(&blocks).await);
            }
            Request::RunStart { run_id, reply } => {
                let _ = reply.send(session.run_start(&run_id).await);
            }
            Request::ToolIntent { call_key, reply } => {
                let _ = reply.send(session.tool_intent(&call_key).await);
            }
            Request::EntryByMid { mid, reply } => {
                let _ = reply.send(session.entry_by_mid(&mid).await);
            }
            Request::EntryAt { ordinal, reply } => {
                let _ = reply.send(session.entry_at(ordinal).await);
            }
            Request::MovedThrough { reply } => {
                let _ = reply.send(session.moved_through().await);
            }
            Request::EntriesByMid { mids, reply } => {
                let _ = reply.send(session.entries_by_mid(&mids).await);
            }
            Request::SealedDerived { reply } => {
                let _ = reply.send(session.sealed_derived().await);
            }
            Request::KeepSealedDerived { value, reply } => {
                let _ = reply.send(session.keep_sealed_derived(value).await);
            }
            Request::Enrollment { reply } => {
                let _ = reply.send(session.enrollment().await);
            }
            Request::PutEnrollment { entries, reply } => {
                let _ = reply.send(session.put_enrollment(&entries).await);
            }
            Request::Seal {
                ordinal,
                carry,
                window_repetition,
                now_ms,
                reply,
            } => {
                let carry = Carry {
                    records: &carry,
                    window_repetition,
                };
                let sealed = match ordinal {
                    Some(ordinal) => session.seal_at_entry(ordinal, &carry, now_ms).await,
                    None => session.seal_ledger(&carry, now_ms).await,
                };
                // The copy is replaced whole, so what it missed is in the snapshot.
                self.missed.clear();
                let _ = reply.send(sealed.map(|()| Snapshot {
                    conversation: session.conversation().clone(),
                    operations: session.operations().clone(),
                    next_seq: session.next_seq(),
                    message_id_prefix: session.message_id_prefix(),
                }));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::{poll_fn, Future};
    use std::pin::{pin, Pin};
    use std::task::Poll;

    use broca_protocol::content::{ContentBlock, Message, Role};
    use broca_session_store::backend::{MemoryBackend, SessionAddr};
    use broca_session_store::codec::Message as _;
    use broca_session_store::head::SessionTriple;
    use broca_session_store::keys::DeploymentKey;
    use broca_session_store::record::{Event, Input, Placement, Record};
    use broca_session_store::session::Session;

    use super::{ActorWriter, Conversation, Operations, Request, RunLink};
    use crate::writer::Writer;

    fn input(text: &str) -> Record {
        Record::Input(Input {
            media: Vec::new(),
            run_id: "r".into(),
            placement: Placement::RunInput,
            message: std::sync::Arc::new(Message::new(
                Role::User,
                vec![ContentBlock::Text { text: text.into() }],
            )),
            written_at_ms: None,
            submission_id: None,
            notice: None,
            tags_v1: None,
            receipt_seq: None,
            pruned: Vec::new(),
        })
    }

    fn texts(writer: &impl Writer) -> Vec<String> {
        writer
            .conversation()
            .held_messages()
            .unwrap()
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|block| match block {
                ContentBlock::Text { text } => Some(text.to_string()),
                _ => None,
            })
            .collect()
    }

    async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
        poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
    }

    async fn session(backend: &MemoryBackend) -> Session<&MemoryBackend> {
        Session::create(
            backend,
            SessionAddr("0123456789abcdef".into()),
            SessionTriple {
                project_root: "/project".into(),
                harness: "broca".into(),
                session: "s".into(),
            },
            &DeploymentKey::new(1, &[5; 32]),
            false,
        )
        .await
        .unwrap()
    }

    /// Serves the run's requests. Before each request it appends `own(request number)` as the
    /// actor's own records, the way a steer arrives while the run is waiting on its model or
    /// its tools.
    async fn actor(
        session: &mut Session<&MemoryBackend>,
        link: &mut RunLink,
        own: impl Fn(usize) -> Vec<Record>,
    ) {
        let mut served = 0;
        while let Some(request) = link.next().await {
            let records = own(served);
            if !records.is_empty() {
                let first = session.append(&records, true).await.unwrap();
                link.note_own(first, &records);
            }
            link.serve(&mut *session, request).await;
            served += 1;
        }
    }

    /// A plain append catches the run's copy up on what the actor appended first, so after the
    /// call the copy equals the session's folds and the next seq follows both.
    #[tokio::test]
    async fn an_append_folds_the_actors_records_first() {
        let backend = MemoryBackend::default();
        let mut session = session(&backend).await;
        let (mut writer, mut link) = ActorWriter::new(&session, None);
        let run = async {
            writer.append(&[input("run")], true).await.unwrap();
            assert_eq!(texts(&writer), ["steer", "run"]);
            writer.next_seq()
        };
        let next = tokio::select! {
            biased;
            next = run => next,
            () = actor(&mut session, &mut link, |n| if n == 0 { vec![input("steer")] } else { Vec::new() }) => unreachable!(),
        };
        assert_eq!(texts(&session), ["steer", "run"]);
        assert_eq!(next, session.next_seq());
        assert_eq!(
            writer.conversation().encode(),
            session.conversation().encode()
        );
        assert_eq!(writer.operations().encode(), session.operations().encode());
    }

    /// A batch that pauses or ends the run is a barrier even when its caller didn't ask for one,
    /// so the actor syncs it before the run reports it durable.
    #[tokio::test]
    async fn an_append_that_pauses_the_run_is_always_a_barrier() {
        let backend = MemoryBackend::default();
        let session = session(&backend).await;
        let (mut writer, mut link) = ActorWriter::new(&session, None);
        let pause = Record::Pause(broca_session_store::record::Pause {
            run_id: "r".into(),
            reason: broca_protocol::caller::PauseReason::Restart,
            ts_ms: None,
            error: None,
            account_limit: None,
        });
        let records = [pause];
        let request = tokio::select! {
            _ = writer.append(&records, false) => unreachable!("the actor never answers"),
            request = link.next() => request,
        };
        let asked = match request {
            Some(super::Request::Append { barrier, .. }) => barrier,
            _ => panic!("the run asked for an append"),
        };
        assert!(asked, "the pause was appended without a barrier");
    }

    /// A batch built during a hold sees what the actor appended just before it: the property
    /// that makes `append_with` safe when another appender shares the session.
    #[tokio::test]
    async fn a_batch_built_during_a_hold_sees_the_actors_records() {
        let backend = MemoryBackend::default();
        let mut session = session(&backend).await;
        let (mut writer, mut link) = ActorWriter::new(&session, None);
        let run = async {
            let mut seen = Vec::new();
            let mut next = 0;
            writer
                .append_with(
                    &mut |conversation, _, first_seq| {
                        seen = conversation
                            .held_messages()
                            .unwrap()
                            .iter()
                            .flat_map(|message| &message.content)
                            .filter_map(|block| match block {
                                ContentBlock::Text { text } => Some(text.clone()),
                                _ => None,
                            })
                            .collect::<Vec<_>>();
                        next = first_seq;
                        vec![input("built")]
                    },
                    false,
                )
                .await
                .unwrap();
            (seen, next)
        };
        let ((seen, next), after) = {
            let before = session.next_seq();
            let result = tokio::select! {
                biased;
                result = run => result,
                () = actor(&mut session, &mut link, |n| if n == 0 { vec![input("steer")] } else { Vec::new() }) => unreachable!(),
            };
            (result, before)
        };
        assert_eq!(
            seen,
            ["steer"],
            "the batch was built without the actor's record"
        );
        assert_eq!(next, after + 1, "the batch was built for the wrong seq");
        assert_eq!(texts(&session), ["steer", "built"]);
        assert_eq!(
            writer.conversation().encode(),
            session.conversation().encode()
        );
    }

    #[tokio::test]
    async fn a_cache_read_queued_during_a_hold_does_not_interrupt_the_batch() {
        let backend = MemoryBackend::default();
        let mut session = session(&backend).await;
        let (mut writer, mut link) = ActorWriter::new(&session, None);
        let loader = writer.cache_predecessor_loader();
        let mut load = pin!(loader.load());
        {
            let mut build = |_: &Conversation, _: &Operations, _| vec![input("built")];
            let mut append = pin!(writer.append_with(&mut build, true));
            assert!(poll_once(append.as_mut()).await.is_pending());
            let hold = link.next().await.unwrap();
            assert!(matches!(hold, Request::Hold { .. }));
            {
                let mut serve = pin!(link.serve(&mut session, hold));
                assert!(poll_once(serve.as_mut()).await.is_pending());
                assert!(poll_once(load.as_mut()).await.is_pending());
                assert!(
                    poll_once(serve.as_mut()).await.is_pending(),
                    "queuing a cache read must not end the hold"
                );
                assert!(poll_once(append.as_mut()).await.is_pending());
                serve.await;
            }
            assert_eq!(append.await.unwrap().len(), 1);
        }
        assert_eq!(texts(&session), ["built"]);
        assert_eq!(texts(&writer), ["built"]);

        // The owning actor answers cache reads outside RunLink::serve.
        let mut next = pin!(link.next());
        let Poll::Ready(Some(Request::CachePredecessor { reply })) = poll_once(next.as_mut()).await
        else {
            panic!("the cache read must still be queued after the hold");
        };
        assert!(reply.send(Ok(None)).is_ok());
        assert!(load.await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dropping_the_append_before_its_batch_releases_the_hold() {
        let backend = MemoryBackend::default();
        let mut session = session(&backend).await;
        let (mut writer, mut link) = ActorWriter::new(&session, None);
        let mut serve = {
            let mut build = |_: &Conversation, _: &Operations, _| {
                panic!("the dropped append must not build a batch")
            };
            let mut append = pin!(writer.append_with(&mut build, true));
            assert!(poll_once(append.as_mut()).await.is_pending());
            let hold = link.next().await.unwrap();
            assert!(matches!(hold, Request::Hold { .. }));
            let mut serve = Box::pin(link.serve(&mut session, hold));
            assert!(poll_once(serve.as_mut()).await.is_pending());
            serve
        };
        assert!(
            poll_once(serve.as_mut()).await.is_ready(),
            "dropping the append must release the hold without another request or writer drop"
        );
        drop(serve);

        let records = [input("after the hold")];
        let (result, ()) = tokio::join!(writer.append(&records, true), async {
            let request = link.next().await.unwrap();
            link.serve(&mut session, request).await;
        });
        result.unwrap();
        assert_eq!(texts(&session), ["after the hold"]);
    }

    /// Catching up folds what the actor appended since the run's last call and appends nothing,
    /// so a read of the run's copy between appends (the queue a boundary delivers from) sees
    /// the session's records.
    #[tokio::test]
    async fn catching_up_folds_the_actors_records_without_appending() {
        let backend = MemoryBackend::default();
        let mut session = session(&backend).await;
        let (mut writer, mut link) = ActorWriter::new(&session, None);
        let before = session.next_seq();
        let run = async {
            assert!(texts(&writer).is_empty());
            writer.caught_up().await.unwrap();
            texts(&writer)
        };
        let seen = tokio::select! {
            biased;
            seen = run => seen,
            () = actor(&mut session, &mut link, |n| if n == 0 { vec![input("queued")] } else { Vec::new() }) => unreachable!(),
        };
        assert_eq!(seen, ["queued"], "the copy missed the actor's record");
        assert_eq!(
            session.next_seq(),
            before + 1,
            "catching up appended a record"
        );
        assert_eq!(writer.next_seq(), session.next_seq());
        assert_eq!(writer.operations().encode(), session.operations().encode());
    }

    /// A seal runs on the session itself and replaces the run's copy, so the window the run sees
    /// after a cut is the session's.
    #[tokio::test]
    async fn a_seal_replaces_the_runs_copy() {
        let backend = MemoryBackend::default();
        let mut session = session(&backend).await;
        let (mut writer, mut link) = ActorWriter::new(&session, None);
        let run = async {
            writer.append(&[input("one")], true).await.unwrap();
            let carry = broca_session_store::session::Carry {
                records: &[],
                window_repetition: None,
            };
            writer.seal_ledger(&carry, 0).await.unwrap();
        };
        tokio::select! {
            biased;
            () = run => {}
            () = actor(&mut session, &mut link, |n| if n == 1 { vec![input("steer")] } else { Vec::new() }) => unreachable!(),
        }
        assert_eq!(
            writer.conversation().encode(),
            session.conversation().encode()
        );
        assert_eq!(writer.operations().encode(), session.operations().encode());
        assert_eq!(writer.next_seq(), session.next_seq());
        assert_eq!(texts(&writer), ["one", "steer"]);
    }

    /// A batch that places a message mints the message's id from the seq its entry facts take.
    /// When the actor appended one of its own records since the run's copy last caught up (a
    /// send queued while a model step was in flight, say), the id must still name that record,
    /// so the message is found by it again.
    #[tokio::test]
    async fn a_placed_messages_id_finds_it_after_the_actors_own_append() {
        let backend = MemoryBackend::default();
        let mut session = session(&backend).await;
        let (mut writer, mut link) = ActorWriter::new(&session, None);
        let run = async {
            let mut placed = crate::entries::Placed {
                inner: &mut writer,
                facts: crate::entries::Tags::default(),
                calls: Default::default(),
                rich_ui: None,
                runner: None,
                floor_mid: Default::default(),
            };
            placed.append(&[input("placed")], true).await.unwrap();
        };
        let queued = || {
            vec![Record::Event(Event {
                name: "queued_while_the_model_answers".into(),
                run_id: None,
                body: None,
            })]
        };
        tokio::select! {
            biased;
            () = run => {}
            () = actor(&mut session, &mut link, |n| if n == 0 { queued() } else { Vec::new() }) => unreachable!(),
        }
        let entries = session.entries(1, usize::MAX).await.unwrap();
        assert_eq!(entries.len(), 1);
        for entry in entries {
            let mid = entry
                .state
                .facts
                .expect("placed with its facts")
                .mid
                .clone();
            assert_eq!(
                session.find_message(&mid).await.unwrap(),
                Some(entry.ordinal),
                "message {} is stored as {mid}",
                entry.ordinal
            );
        }
    }

    /// Once the actor is gone, the run's writer refuses by name instead of hanging.
    #[tokio::test]
    async fn a_run_whose_actor_stopped_gets_owner_gone() {
        let backend = MemoryBackend::default();
        let session = session(&backend).await;
        let (mut writer, link) = ActorWriter::new(&session, None);
        drop(link);
        match writer.append(&[input("late")], true).await {
            Err(broca_session_store::session::SessionError::OwnerGone) => {}
            other => panic!("expected OwnerGone: {other:?}"),
        }
    }
}
