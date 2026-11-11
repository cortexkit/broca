//! The one place a run's records are appended.
//!
//! A session has a single writer. While a run is active the session actor also appends to it
//! (steers, queued sends, cancel requests), so the loop never holds the session alone: it asks
//! the writer to append, and reads fold state back from it. A record is in the folds only once
//! appended, so the loop and the actor always see one state.

use broca_session_store::backend::SessionBackend;
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::idindex::IdKind;
use broca_session_store::record::Record;
use broca_session_store::session::{MediaError, NewInputMedia, Session, SessionError};

/// Declared with native async methods, so whether a run's future is `Send` follows from the
/// concrete writer: a run over [`crate::shared::ActorWriter`] can be spawned on a multi-threaded
/// runtime, while a session's own backend may keep futures on one thread (WebAssembly).
#[allow(async_fn_in_trait)]
pub trait Writer {
    /// Appends `records` as one all-or-nothing batch and returns the first record's seq. With
    /// `barrier`, it returns only once the batch is durable.
    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError>;

    /// Appends `records` as [`Writer::append`] does, after storing the bytes of each medium
    /// `media` lists, durable before the batch, and recording in each record naming one where
    /// its bytes are (see `Session::append_input_media`). Returns the seq of the first record and
    /// the records as appended. A writer that keeps no media objects refuses a batch naming any,
    /// appending nothing.
    async fn append_input_media(
        &mut self,
        records: &[Record],
        media: Vec<NewInputMedia>,
        barrier: bool,
    ) -> Result<(u64, Vec<Record>), SessionError> {
        if media.is_empty() {
            let first = self.append(records, barrier).await?;
            return Ok((first, records.to_vec()));
        }
        Err(SessionError::Media(MediaError::NoStore))
    }

    /// Every ledger record that names the queued send `submission_id`, oldest first, from the
    /// open log or sealed history: what the loop reads to deliver the send at a boundary.
    async fn send_records(&mut self, submission_id: &str) -> Result<Vec<Record>, SessionError>;

    /// Appends the batch `build` makes from the folds as they stand at that moment, and returns
    /// it. A writer shared with other appenders (the session actor appends steers and queued
    /// sends during a run) calls `build` and appends its batch with nothing in between, so the
    /// batch can't miss a record another appender added. `build` also gets the seq the batch's
    /// first record will take, so a record in it can name another. An empty batch appends nothing. A
    /// batch that ends or pauses the run is appended with a barrier whatever `barrier` says: a
    /// run's end is durable before anyone learns of it.
    async fn append_with(
        &mut self,
        build: &mut (dyn for<'s> FnMut(&'s Conversation, &'s Operations, u64) -> Vec<Record>
                  + Send),
        barrier: bool,
    ) -> Result<Vec<Record>, SessionError>;

    /// The conversation as the folds hold it after every appended record.
    fn conversation(&self) -> &Conversation;

    /// The run state as the folds hold it after every appended record.
    fn operations(&self) -> &Operations;

    /// The first part number available after earlier sessions at this address were deleted.
    /// Supplied by the expiry markers, not by this session's records or projection numbering.
    fn retired_tag_floor(&self) -> u64;

    /// The session's id, which names the call keys of this session's tool calls.
    fn session_id(&self) -> String;

    /// The seq the next appended record will take.
    fn next_seq(&self) -> u64;

    /// What every new message id starts with under this writer: the id of a message whose
    /// `EntryFacts` record takes seq `s` is `<prefix>.<s>`.
    fn message_id_prefix(&self) -> String;

    /// Every record of the open log after its checkpoint, with its seq: what a reader needs
    /// whole when the folds keep only a summary of it, such as the compaction views.
    async fn open_log_records(&self) -> Result<Vec<(u64, Record)>, SessionError>;

    /// The start record of the session's run `run_id`, sealed or in the open log, found through
    /// the session's id lookup. `None` when the session has no such run.
    async fn run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<broca_session_store::record::RunStart>, SessionError>;

    /// The dispatch intent that recorded the call key `call_key`, sealed or in the open log,
    /// found through the session's id lookup. `None` when no intent of the session has it.
    async fn tool_intent(
        &self,
        call_key: &str,
    ) -> Result<Option<broca_session_store::record::ToolIntent>, SessionError>;

    /// What the session's enrollment in the current compaction contract decided about the
    /// messages placed before it, or `None` for a session that never enrolled (see
    /// `Session::enrollment`). Damage when the session names decisions it can't read.
    async fn enrollment(
        &self,
    ) -> Result<Option<broca_session_store::session::EnrollmentOverlay>, SessionError>;

    /// Stores an enrollment's decisions, durable before this returns, for the batch about to be
    /// appended to name in its `CompactionEnrolled` record (see `Session::put_enrollment`).
    async fn put_enrollment(
        &mut self,
        entries: &broca_session_store::record::EnrolledEntries,
    ) -> Result<broca_session_store::record::EnrollmentOverlayRef, SessionError>;

    /// Seals the session's history before the entry with ordinal `ordinal`, starting the new
    /// open log with `carry` (see `Session::seal_at_entry`).
    async fn seal_at_entry(
        &mut self,
        ordinal: u64,
        carry: &broca_session_store::session::Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError>;

    /// Seals the open log's ledger with no cut, starting the new log with what `carry` names
    /// (see `Session::seal_carrying`).
    async fn seal_ledger(
        &mut self,
        carry: &broca_session_store::session::Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError>;

    /// The entry whose message id is `mid`, read back from sealed history when it's no longer in
    /// the window. `None` when no entry of the session has that id.
    async fn entry_by_mid(
        &self,
        mid: &str,
    ) -> Result<Option<broca_session_store::session::StoredEntry>, SessionError>;

    /// The entries whose message ids `mids` lists, in ordinal order, each once; an id no entry
    /// has is left out. A session reads named entries close to each other in one pass.
    async fn entries_by_mid(
        &self,
        mids: &[String],
    ) -> Result<Vec<broca_session_store::session::StoredEntry>, SessionError> {
        let mut out = Vec::new();
        for mid in mids {
            if let Some(entry) = self.entry_by_mid(mid).await? {
                out.push(entry);
            }
        }
        out.sort_by_key(|entry| entry.ordinal);
        out.dedup_by_key(|entry| entry.ordinal);
        Ok(out)
    }

    /// What a reader derived from the session's sealed history and asked it to keep (see
    /// `Session::keep_sealed_derived`); `None` when nothing is kept, or the history version
    /// moved since. A writer that keeps nothing always answers `None`.
    async fn sealed_derived(
        &self,
    ) -> Result<Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>, SessionError> {
        Ok(None)
    }

    /// Asks the session to keep `value`, derived from its sealed history, while it's open and
    /// its history version stays.
    async fn keep_sealed_derived(
        &self,
        _value: std::sync::Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<(), SessionError> {
        Ok(())
    }

    /// Where the tool plane captures a result's media. A writer that offers one stores, when it
    /// appends a call's result, the bytes of every media part captured for that call, durable
    /// before the record. Without one nothing is captured, so every media part of a result
    /// becomes a provenance marker and no bytes are lost.
    fn media(&self) -> Option<std::sync::Arc<dyn broca_core::tool_media::MediaCapture>> {
        None
    }

    /// Hands the session's owner a weak handle on the transcript a run keeps, once, when the run
    /// starts, so the owner's memory report can size it while the run holds it. A writer whose
    /// owner reports nothing ignores it.
    fn share_window(&self, _window: crate::compaction::WeakWindow) {}
}

impl<B: SessionBackend> Writer for Session<B> {
    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        Session::append(self, records, barrier).await
    }

    async fn append_input_media(
        &mut self,
        records: &[Record],
        media: Vec<NewInputMedia>,
        barrier: bool,
    ) -> Result<(u64, Vec<Record>), SessionError> {
        Session::append_input_media(self, records, media, barrier).await
    }

    async fn send_records(&mut self, submission_id: &str) -> Result<Vec<Record>, SessionError> {
        Ok(self
            .find_id(IdKind::SubmissionId, submission_id)
            .await?
            .into_iter()
            .map(|(_, record)| record)
            .collect())
    }

    async fn append_with(
        &mut self,
        build: &mut (dyn for<'s> FnMut(&'s Conversation, &'s Operations, u64) -> Vec<Record>
                  + Send),
        barrier: bool,
    ) -> Result<Vec<Record>, SessionError> {
        let records = build(&self.conversation, &self.operations, self.next_seq());
        if !records.is_empty() {
            let barrier = barrier || ends_run(&records);
            Session::append(self, &records, barrier).await?;
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
        broca_session_store::codec::hex(&self.head().session_id.0)
    }

    fn next_seq(&self) -> u64 {
        Session::next_seq(self)
    }

    fn retired_tag_floor(&self) -> u64 {
        Session::retired_tag_floor(self)
    }

    fn message_id_prefix(&self) -> String {
        Session::message_id_prefix(self)
    }

    async fn open_log_records(&self) -> Result<Vec<(u64, Record)>, SessionError> {
        Session::open_log_records(self).await
    }

    async fn enrollment(
        &self,
    ) -> Result<Option<broca_session_store::session::EnrollmentOverlay>, SessionError> {
        Session::enrollment(self).await
    }

    async fn put_enrollment(
        &mut self,
        entries: &broca_session_store::record::EnrolledEntries,
    ) -> Result<broca_session_store::record::EnrollmentOverlayRef, SessionError> {
        Session::put_enrollment(self, entries).await
    }

    async fn seal_at_entry(
        &mut self,
        ordinal: u64,
        carry: &broca_session_store::session::Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        Session::seal_at_entry(self, ordinal, carry, now_ms)
            .await
            .map(|_| ())
    }

    async fn seal_ledger(
        &mut self,
        carry: &broca_session_store::session::Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        Session::seal_carrying(self, None, carry, now_ms)
            .await
            .map(|_| ())
    }

    async fn entry_by_mid(
        &self,
        mid: &str,
    ) -> Result<Option<broca_session_store::session::StoredEntry>, SessionError> {
        Session::entry_by_mid(self, mid).await
    }

    async fn entries_by_mid(
        &self,
        mids: &[String],
    ) -> Result<Vec<broca_session_store::session::StoredEntry>, SessionError> {
        Session::entries_by_mid(self, mids).await
    }

    async fn sealed_derived(
        &self,
    ) -> Result<Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>, SessionError> {
        Ok(Session::sealed_derived(self))
    }

    async fn keep_sealed_derived(
        &self,
        value: std::sync::Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<(), SessionError> {
        Session::keep_sealed_derived(self, value);
        Ok(())
    }

    async fn run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<broca_session_store::record::RunStart>, SessionError> {
        self.find_run_start(run_id).await
    }

    async fn tool_intent(
        &self,
        call_key: &str,
    ) -> Result<Option<broca_session_store::record::ToolIntent>, SessionError> {
        Ok(self
            .find_id(broca_session_store::idindex::IdKind::CallKey, call_key)
            .await?
            .into_iter()
            .find_map(|(_, record)| match record {
                Record::ToolIntent(intent) => Some(intent),
                _ => None,
            }))
    }
}

/// Whether `records` end or pause the run, so their batch must be durable when appended.
pub fn ends_run(records: &[Record]) -> bool {
    records
        .iter()
        .any(|record| matches!(record, Record::RunEnd(_) | Record::Pause(_)))
}
