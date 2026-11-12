//! `session.history` on the new storage, read from a session view by the functions the
//! session's actor serves with: the store's side of the converter's history check and of the
//! history read-cost test. It is a serve handler, so `broca_module::history::read` runs over it
//! unchanged, as it runs over today's side (`broca_module::store_read::history_sides`).

use std::sync::Arc;

use async_trait::async_trait;
use broca_module::serve::{
    CancelResponse, RetractResponse, SendRequest, SendResponse, SessionReadRequest, StatusResponse,
    SubscribeFrom,
};
use broca_module::{LineageKey, ServeError, ServeHandler};
use broca_session_loop::parts::MemoryParts;
use broca_session_store::backend::MemoryBackend;
use broca_session_store::session::Session;
use futures::executor::block_on;
use serde_json::Value;

use crate::counting::Counting;

/// The backend the check's sessions are stored in: in memory, counting what each read fetches.
pub type Backend = Counting<MemoryBackend>;

/// `session.history` on the new storage, read from `session` by the functions the session's
/// actor serves with, with the session's setup parts `parts`. `B` is the session's backend: the
/// check's counting memory backend by default, or a session folder on disk (`LocalBackend`).
pub struct StoredHistory<'a, B: broca_session_store::backend::SessionBackend = &'a Backend> {
    pub session: &'a Session<B>,
    pub parts: &'a MemoryParts,
}

impl<B: broca_session_store::backend::SessionBackend> StoredHistory<'_, B> {
    async fn read_page(
        &self,
        req: &SessionReadRequest,
    ) -> Result<broca_module::serve::SessionReadResponse, ServeError> {
        broca_module::store_read::serve_view(self.session, self.parts, req).await
    }
}

/// A verifier's walk of an unchanged session. History uses the real page builder, but shares
/// the stream head across pages instead of reopening and projecting the open log for each one.
pub(crate) struct StoredHistoryWalk<'a, B: broca_session_store::backend::SessionBackend> {
    session: &'a Session<B>,
    parts: &'a MemoryParts,
    reader: broca_module::store_read::ViewReader<'a, B>,
}

impl<'a, B: broca_session_store::backend::SessionBackend> StoredHistoryWalk<'a, B> {
    pub(crate) async fn new(
        session: &'a Session<B>,
        parts: &'a MemoryParts,
    ) -> Result<Self, ServeError> {
        Ok(Self {
            session,
            parts,
            reader: broca_module::store_read::ViewReader::new(session, parts).await?,
        })
    }

    async fn read_page(
        &self,
        req: &SessionReadRequest,
    ) -> Result<broca_module::serve::SessionReadResponse, ServeError> {
        self.reader.read(req).await
    }
}

/// The serve handler for each backend a history is read from. Written once, for both: the
/// trait's futures must be `Send`, which a concrete backend's are and a generic one's needn't
/// be.
macro_rules! stored_history_handler {
    ($handler:ident, $backend:ty) => {
        #[async_trait]
        impl<'a> ServeHandler for $handler<'a, $backend> {
            async fn send(&self, _: SendRequest) -> Result<SendResponse, ServeError> {
                unreachable!("session.history sends nothing")
            }
            async fn retract(
                &self,
                _: &LineageKey,
                _: broca_protocol::SubmissionId,
            ) -> Result<RetractResponse, ServeError> {
                unreachable!("session.history retracts nothing")
            }
            async fn subscribe(
                &self,
                _: &LineageKey,
                _: SubscribeFrom,
                _: tokio::sync::mpsc::Sender<broca_module::Delivery>,
            ) -> Result<(), ServeError> {
                unreachable!("session.history subscribes to nothing")
            }
            async fn cancel(
                &self,
                _: &LineageKey,
                _: broca_protocol::RunId,
            ) -> Result<CancelResponse, ServeError> {
                unreachable!("session.history cancels nothing")
            }
            async fn status(&self, _: &LineageKey) -> Result<StatusResponse, ServeError> {
                unreachable!("session.history reads no status")
            }
            async fn run_status(
                &self,
                _: &LineageKey,
                _: broca_protocol::RunId,
            ) -> Result<broca_module::serve::RunStatusResponse, ServeError> {
                unreachable!("session.history reads no run status")
            }

            async fn read(
                &self,
                req: SessionReadRequest,
            ) -> Result<broca_module::serve::SessionReadResponse, ServeError> {
                self.read_page(&req).await
            }

            async fn store_history_head(
                &self,
                _: &LineageKey,
                mid: Option<&str>,
            ) -> Result<Option<broca_module::store_entries::HistoryHead>, ServeError> {
                broca_module::store_entries::history_head_of(self.session, mid)
                    .await
                    .map(Some)
            }

            async fn output_ref_history_version(
                &self,
                _: &LineageKey,
                served: u64,
            ) -> Result<u64, ServeError> {
                broca_module::store_exposure::stored_history_version(served)
                    .ok_or(ServeError::HistoryChanged)
            }

            async fn tag_location(
                &self,
                _: &LineageKey,
                tag: u64,
            ) -> Result<Option<broca_core::TagLocation>, ServeError> {
                broca_module::store_output_refs::tag_location(self.session, self.parts, tag)
                    .await
                    .map(Some)
                    .map_err(|error| ServeError::Open(error, None))
            }

            async fn store_writing_view(
                &self,
                _: &LineageKey,
                ordinal: u64,
            ) -> Result<
                Option<Option<broca_core::compaction_provider::view::CompactionView>>,
                ServeError,
            > {
                broca_module::store_output_refs::writing_view(self.session, ordinal)
                    .await
                    .map(Some)
                    .map_err(|error| ServeError::Open(error, None))
            }

            async fn frozen_manifest(
                &self,
                _: &LineageKey,
            ) -> Result<Option<Arc<broca_wal::PlanManifest>>, ServeError> {
                broca_session_loop::owner::ownership_with_first_episode(
                    &self.session.operations.owner,
                    self.parts,
                )
                .map(|ownership| ownership.first_episode.and_then(|first| first.manifest))
                .map_err(|error| ServeError::Open(error, None))
            }
        }
    };
}

stored_history_handler!(StoredHistory, &'a Backend);
stored_history_handler!(StoredHistory, broca_session_store::backend::LocalBackend);
stored_history_handler!(StoredHistoryWalk, &'a Backend);
stored_history_handler!(
    StoredHistoryWalk,
    broca_session_store::backend::LocalBackend
);

/// `session.history` with `params`, over `handler`.
pub fn history(handler: &dyn ServeHandler, name: &str, params: &Value) -> Result<Value, String> {
    let request = serde_json::from_value(params.clone()).map_err(|error| error.to_string())?;
    let key = broca_protocol::BindIdentity::new("/history", "history", name);
    block_on(broca_module::history::read(handler, &key, &request))
        .map_err(|error| format!("{error:?}"))
}
