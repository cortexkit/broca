//! Setup parts made durable before any record names them.
//!
//! A run puts its setup parts (tool lists, render config, plan manifest) into a [`Parts`] store,
//! which answers at once from memory, then names them on a record it appends. [`Durable`] wraps a
//! session's writer so that every append first stores every part not yet durable, through a
//! [`PartStore`]. A part therefore reaches the deployment's setup store before any record that
//! names it, and a run started after a restart can read back every part its start names.

use broca_session_store::backend::{BackendError, SessionBackend};
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::record::{PartRef, Record};
use broca_session_store::session::{Carry, NewInputMedia, SessionError, StoredEntry};
use broca_session_store::setup::{SetupError, SetupKey, SetupStore};

use crate::media::HeldInputMedia;
use crate::parts::{Parts, SharedParts};
use crate::writer::Writer;

/// Where setup parts are stored durably.
#[allow(async_fn_in_trait)]
pub trait PartStore {
    /// Stores `bytes` as `part`, durable on return. Storing a part already stored writes nothing.
    async fn store(&self, part: &PartRef, bytes: &[u8]) -> Result<(), SessionError>;
}

/// The deployment's setup store, the one every session's parts go to.
pub struct SetupParts<B> {
    pub backend: B,
    pub key: SetupKey,
}

impl<B: SessionBackend> PartStore for SetupParts<B> {
    async fn store(&self, part: &PartRef, bytes: &[u8]) -> Result<(), SessionError> {
        let (stored, _) = SetupStore::new(&self.backend, &self.key)
            .put(part.kind, bytes)
            .await
            .map_err(setup_error)?;
        if stored.digest != part.digest {
            return Err(SessionError::Backend(BackendError::Corrupt(
                "a setup part's bytes don't match the digest it was put under",
            )));
        }
        Ok(())
    }
}

/// A setup store's failure as a session's.
pub fn setup_error(error: SetupError) -> SessionError {
    match error {
        SetupError::Backend(error) => SessionError::Backend(error),
        SetupError::Missing(_) => SessionError::Backend(BackendError::Corrupt(
            "a setup part a record names is missing",
        )),
        SetupError::Corrupt(_) | SetupError::CorruptStored(_) => SessionError::Backend(
            BackendError::Corrupt("a stored setup part failed its check"),
        ),
    }
}

/// Stores every part `parts` holds that isn't durable yet. Without a `store`, as in tests,
/// nothing is stored.
pub async fn store_unstored<S: PartStore>(
    parts: &mut SharedParts,
    store: Option<&S>,
) -> Result<(), SessionError> {
    let Some(store) = store else {
        return Ok(());
    };
    let unstored = parts.unstored();
    if unstored.is_empty() {
        return Ok(());
    }
    let mut stored = Vec::with_capacity(unstored.len());
    for (part, bytes) in &unstored {
        store.store(part, bytes).await?;
        stored.push(part.clone());
    }
    parts.mark_stored(&stored);
    Ok(())
}

/// A session's writer that stores the setup parts not yet durable before each append, so no
/// appended record names a part only memory holds. A failure to store one refuses the append
/// with nothing written. With `media`, an append also stores or points at each held medium its
/// records' messages name, so a send's media are durable before the records that place it.
/// Every other method is the inner writer's.
pub struct Durable<'a, W, S> {
    pub writer: &'a mut W,
    pub parts: &'a mut SharedParts,
    pub store: Option<&'a S>,
    /// The media of sends the writer's owner staged and no record stores yet.
    pub media: Option<&'a mut HeldInputMedia>,
}

impl<W: Writer, S: PartStore> Writer for Durable<'_, W, S> {
    async fn accepted_send(
        &self,
        account: &str,
        since_ms: u64,
    ) -> Result<broca_core::provider::AcceptedSendEvidence, SessionError> {
        self.writer.accepted_send(account, since_ms).await
    }

    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        store_unstored(self.parts, self.store).await?;
        match self.media.as_deref_mut() {
            Some(held) => Ok(held.append(self.writer, records, barrier).await?.0),
            None => self.writer.append(records, barrier).await,
        }
    }

    async fn append_input_media(
        &mut self,
        records: &[Record],
        media: Vec<NewInputMedia>,
        barrier: bool,
    ) -> Result<(u64, Vec<Record>), SessionError> {
        store_unstored(self.parts, self.store).await?;
        self.writer
            .append_input_media(records, media, barrier)
            .await
    }

    async fn caught_up(&mut self) -> Result<(), SessionError> {
        self.writer.caught_up().await
    }

    async fn send_records(&mut self, submission_id: &str) -> Result<Vec<Record>, SessionError> {
        self.writer.send_records(submission_id).await
    }

    async fn append_with(
        &mut self,
        build: &mut (dyn for<'s> FnMut(&'s Conversation, &'s Operations, u64) -> Vec<Record>
                  + Send),
        barrier: bool,
    ) -> Result<Vec<Record>, SessionError> {
        // `build` runs inside the inner append, after this store. A part it puts while building
        // would not be stored yet, so the batch it builds names only parts put before the call:
        // the loop puts a run's parts before it builds the batch naming them.
        store_unstored(self.parts, self.store).await?;
        self.writer.append_with(build, barrier).await
    }

    fn conversation(&self) -> &Conversation {
        self.writer.conversation()
    }

    fn operations(&self) -> &Operations {
        self.writer.operations()
    }

    fn session_id(&self) -> String {
        self.writer.session_id()
    }

    fn next_seq(&self) -> u64 {
        self.writer.next_seq()
    }

    fn retired_tag_floor(&self) -> u64 {
        self.writer.retired_tag_floor()
    }

    fn message_id_prefix(&self) -> String {
        self.writer.message_id_prefix()
    }

    fn message_ids(&self) -> Option<crate::entries::MessageIds> {
        self.writer.message_ids()
    }

    async fn open_log_records(&self) -> Result<Vec<(u64, Record)>, SessionError> {
        self.writer.open_log_records().await
    }

    async fn open_run_tail(
        &self,
        run_id: &str,
        first: u64,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        self.writer.open_run_tail(run_id, first).await
    }

    async fn enrollment(
        &self,
    ) -> Result<Option<broca_session_store::session::EnrollmentOverlay>, SessionError> {
        self.writer.enrollment().await
    }

    async fn put_enrollment(
        &mut self,
        entries: &broca_session_store::record::EnrolledEntries,
    ) -> Result<broca_session_store::record::EnrollmentOverlayRef, SessionError> {
        self.writer.put_enrollment(entries).await
    }

    async fn run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<broca_session_store::record::RunStart>, SessionError> {
        self.writer.run_start(run_id).await
    }

    async fn window_message_whole(
        &self,
        ordinal: u64,
    ) -> Result<std::sync::Arc<broca_protocol::Message>, SessionError> {
        self.writer.window_message_whole(ordinal).await
    }

    async fn release(&mut self, blocks: &[(u64, Vec<usize>)]) -> Result<(), SessionError> {
        self.writer.release(blocks).await
    }

    async fn tool_intent(
        &self,
        call_key: &str,
    ) -> Result<Option<broca_session_store::record::ToolIntent>, SessionError> {
        self.writer.tool_intent(call_key).await
    }

    async fn seal_at_entry(
        &mut self,
        ordinal: u64,
        carry: &Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        self.writer.seal_at_entry(ordinal, carry, now_ms).await
    }

    async fn seal_ledger(&mut self, carry: &Carry<'_>, now_ms: u64) -> Result<(), SessionError> {
        self.writer.seal_ledger(carry, now_ms).await
    }

    async fn entry_by_mid(&self, mid: &str) -> Result<Option<StoredEntry>, SessionError> {
        self.writer.entry_by_mid(mid).await
    }

    async fn entry_at(&self, ordinal: u64) -> Result<Option<StoredEntry>, SessionError> {
        self.writer.entry_at(ordinal).await
    }

    async fn moved_through(&self) -> Result<Option<(String, u64)>, SessionError> {
        self.writer.moved_through().await
    }

    async fn entries_by_mid(
        &self,
        mids: &[String],
    ) -> Result<Vec<broca_session_store::session::StoredEntry>, SessionError> {
        self.writer.entries_by_mid(mids).await
    }

    async fn sealed_derived(
        &self,
    ) -> Result<Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>, SessionError> {
        self.writer.sealed_derived().await
    }

    async fn keep_sealed_derived(
        &self,
        value: std::sync::Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<(), SessionError> {
        self.writer.keep_sealed_derived(value).await
    }

    fn media(&self) -> Option<std::sync::Arc<dyn broca_core::tool_media::MediaCapture>> {
        self.writer.media()
    }

    fn share_window(&self, window: crate::compaction::WeakWindow) {
        self.writer.share_window(window)
    }
}
