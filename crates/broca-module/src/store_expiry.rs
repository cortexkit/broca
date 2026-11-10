//! Read-side expiry and counter floors for the new storage's retained sessions.

use super::*;
use broca_session_store::expired::{self, Marker};

impl StoreRuntime {
    pub(crate) fn expiry_markers(&self, key: &BindIdentity) -> Result<Vec<Marker>, OpenError> {
        expired::read(
            &StorageClass::Ephemeral.store_root(&self.state_root),
            &SessionAddr(broca_core::session_addr(key)),
            &triple(key),
            self.deployment(),
        )
        .map_err(|error| match error {
            expired::Error::KeyUnavailable => key_unavailable(self.key_id),
            other => OpenError::Open(format!(
                "the session's expiry marker could not be read: {other:?}"
            )),
        })
    }

    /// The same deadline as the sweeper, including the expired identity left after deletion.
    pub(crate) async fn expired_at(
        &self,
        key: &BindIdentity,
        run: Option<&str>,
        now_ms: u64,
    ) -> Result<Option<u64>, OpenError> {
        let markers = self.expiry_markers(key)?;
        if now_ms == 0 && markers.is_empty() {
            return Ok(None);
        }
        if let Some(run) = run {
            if let Some(marker) = markers
                .iter()
                .find(|marker| marker.runs.iter().any(|id| id == run))
            {
                return Ok(Some(marker.expired_at_ms));
            }
        }
        let Some(view) = self.current_view(key).await? else {
            return Ok(run
                .is_none()
                .then(|| {
                    markers
                        .iter()
                        .max_by_key(|m| m.seq)
                        .map(|m| m.expired_at_ms)
                })
                .flatten());
        };
        if view
            .operations
            .owner
            .first_start
            .as_ref()
            .and_then(|start| start.retention_secs)
            .is_none()
        {
            return Ok(None);
        }
        let records = view.records_from(0).await.map_err(view_error)?;
        if run.is_some_and(|run| !records.iter().any(|(_, record)| {
            matches!(record, broca_session_store::record::Record::RunStart(start) if start.run_id == run)
        })) {
            return Ok(None);
        }
        Ok(crate::retention::store::store_policy(&records)
            .and_then(|(_, _, deadline)| deadline)
            .filter(|deadline| *deadline <= now_ms))
    }
}
