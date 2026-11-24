//! Read-side expiry and counter floors for the session store's retained sessions.

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
        self.expiry_with_view(key, run, now_ms, false)
            .await
            .map(|(deadline, _)| deadline)
    }

    /// The expiry decision and its loaded view, for the rest of a stored read to reuse.
    /// `actor_open` says the session's actor is open: its retention timer then decides the
    /// session's deadline and deletes the session when it comes (`store_actor::expiry`), so only
    /// the expiry markers of an earlier session at the address are read here.
    pub(crate) async fn expiry_with_view(
        &self,
        key: &BindIdentity,
        run: Option<&str>,
        now_ms: u64,
        actor_open: bool,
    ) -> Result<(Option<u64>, Option<crate::store_read::StoredLocalView>), OpenError> {
        let markers = self.expiry_markers(key)?;
        if now_ms == 0 && markers.is_empty() {
            return Ok((None, None));
        }
        // A session a storage policy or a release deleted is no longer there at all (llm-runner
        // `session_retention`): its runs aren't known, and with no session at the address since,
        // the session isn't either.
        let expired_marker = |marker: &&Marker| marker.policy_deleted != Some(true);
        if let Some(run) = run {
            if let Some(marker) = markers
                .iter()
                .filter(expired_marker)
                .find(|marker| marker.runs.iter().any(|id| id == run))
            {
                return Ok((Some(marker.expired_at_ms), None));
            }
        }
        // Only a session with a retention policy expires, and such a session is created in the
        // ephemeral root. One kept in any other root can't expire, and an open session's actor
        // decides its own expiry, so neither view is loaded: every operation runs this check,
        // and loading the whole view here cost each read, subscribe or send of a running
        // session a full read of its log, which its actor then answered anyway. A running
        // session's log moves on between two operations, so each load was a new one.
        if actor_open
            || self
                .class_of(key)?
                .is_some_and(|class| class != StorageClass::Ephemeral)
        {
            return Ok((None, None));
        }
        let Some(view) = self.current_view_for(key, "expiry").await? else {
            let newest = markers.iter().max_by_key(|m| m.seq);
            if newest.is_some_and(|marker| marker.policy_deleted == Some(true)) {
                return Err(OpenError::UnknownSession);
            }
            return Ok((
                run.is_none()
                    .then(|| newest.map(|m| m.expired_at_ms))
                    .flatten(),
                None,
            ));
        };
        if view
            .operations
            .owner
            .first_start
            .as_ref()
            .and_then(|start| start.retention_secs)
            .is_none()
        {
            return Ok((None, Some(view)));
        }
        let records = view.records_from(0).await.map_err(view_error)?;
        if run.is_some_and(|run| !records.iter().any(|(_, record)| {
            matches!(record, broca_session_store::record::Record::RunStart(start) if start.run_id == run)
        })) {
            return Ok((None, Some(view)));
        }
        Ok((
            crate::retention::store::store_policy(&records)
                .and_then(|(_, _, deadline)| deadline)
                .filter(|deadline| *deadline <= now_ms),
            Some(view),
        ))
    }
}
