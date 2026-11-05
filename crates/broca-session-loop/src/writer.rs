//! The one place a run's records are appended.
//!
//! A session has a single writer. While a run is active the session actor also appends to it
//! (steers, queued sends, cancel requests), so the loop never holds the session alone: it asks
//! the writer to append, and reads fold state back from it. A record is in the folds only once
//! appended, so the loop and the actor always see one state.

use async_trait::async_trait;
use broca_session_store::backend::SessionBackend;
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::record::Record;
use broca_session_store::session::{Session, SessionError};

/// Not `Send`: a session's backend may run where futures stay on one thread (WebAssembly).
#[async_trait(?Send)]
pub trait Writer {
    /// Appends `records` as one all-or-nothing batch and returns the first record's seq. With
    /// `barrier`, it returns only once the batch is durable.
    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError>;

    /// Appends the batch `build` makes from the folds as they stand at that moment, and returns
    /// it. A writer shared with other appenders (the session actor appends steers and queued
    /// sends during a run) calls `build` and appends its batch with nothing in between, so the
    /// batch can't miss a record another appender added. An empty batch appends nothing.
    async fn append_with(
        &mut self,
        build: &mut (dyn for<'s> FnMut(&'s Conversation, &'s Operations) -> Vec<Record> + Send),
        barrier: bool,
    ) -> Result<Vec<Record>, SessionError>;

    /// The conversation as the folds hold it after every appended record.
    fn conversation(&self) -> &Conversation;

    /// The run state as the folds hold it after every appended record.
    fn operations(&self) -> &Operations;

    /// The session's id, which names the call keys of this session's tool calls.
    fn session_id(&self) -> String;

    /// The seq the next appended record will take.
    fn next_seq(&self) -> u64;
}

#[async_trait(?Send)]
impl<B: SessionBackend> Writer for Session<'_, B> {
    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        Session::append(self, records, barrier).await
    }

    async fn append_with(
        &mut self,
        build: &mut (dyn for<'s> FnMut(&'s Conversation, &'s Operations) -> Vec<Record> + Send),
        barrier: bool,
    ) -> Result<Vec<Record>, SessionError> {
        let records = build(&self.conversation, &self.operations);
        if !records.is_empty() {
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
}
