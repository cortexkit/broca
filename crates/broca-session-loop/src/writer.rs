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

    /// The conversation as the folds hold it after every appended record.
    fn conversation(&self) -> &Conversation;

    /// The run state as the folds hold it after every appended record.
    fn operations(&self) -> &Operations;

    /// The session's id, which names the call keys of this session's tool calls.
    fn session_id(&self) -> String;
}

#[async_trait(?Send)]
impl<B: SessionBackend> Writer for Session<'_, B> {
    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        Session::append(self, records, barrier).await
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
}
