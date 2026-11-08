//! The durability barrier precedes both provider acknowledgements and model I/O.

use async_trait::async_trait;
use broca_protocol::BindIdentity;
use broca_wal::{CompactionViewV2, WalRecord};
use cortexkit_role_compaction::view::View;

use super::{received::ReceivedView, state::Journal, storage};

pub struct WalJournal<'a> {
    pub writer: &'a crate::WalHandle,
    pub session: &'a BindIdentity,
}

impl WalJournal<'_> {
    async fn commit(&self, record: WalRecord) -> Result<(), String> {
        self.writer
            .record(record)
            .into_result()
            .map_err(|error| error.to_string())?;
        self.writer
            .sync()
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

#[async_trait]
impl Journal for WalJournal<'_> {
    fn view_refused(&self, publication: &super::diagnostics::Publication, code: &str) {
        publication.warn(self.session, code);
    }
    async fn accepted(
        &self,
        previous: Option<&ReceivedView>,
        received: &ReceivedView,
    ) -> Result<(), String> {
        let view = received.view();
        if view.built_through.is_some() || view.new_signed.is_some() {
            return Err(
                "writing compaction-view-meta/v1 requires the metadata writer release".into(),
            );
        }
        self.commit(WalRecord::CompactionViewV2(CompactionViewV2 {
            version: view.version,
            checksum: view.checksum.clone(),
            edits: storage::encode(previous, received),
            built_through: None,
            new_signed: None,
            applied_by: None,
        }))
        .await
    }

    async fn applied(&self, step: u64, view: &View) -> Result<(), String> {
        self.commit(WalRecord::CompactionViewAppliedV2 {
            step_id: step,
            version: view.version,
            checksum: view.checksum.clone(),
        })
        .await
    }

    async fn marker(
        &self,
        view: &View,
        first_kept_mid: &str,
        first_kept_ordinal: u64,
        checkpoint: u64,
    ) -> Result<(), String> {
        self.commit(WalRecord::CompactionMarker {
            harness: self.session.harness.clone(),
            session: self.session.session.clone(),
            view_version: view.version,
            first_kept_mid: first_kept_mid.into(),
            first_kept_ordinal,
            checkpoint,
        })
        .await
    }
}
