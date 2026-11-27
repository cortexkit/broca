//! Read-side expiry and counter floors for the session store's retained sessions.

use super::*;
use broca_session_store::expired::{self, Marker};

#[derive(Default)]
pub(crate) struct ExpiryRead {
    pub(crate) deadline: Option<u64>,
    pub(crate) view: Option<crate::store_read::StoredLocalView>,
    /// The exact position at which a retained deadline proved the session wasn't due.
    pub(crate) unexpired_position: Option<Position>,
}

impl StoreRuntime {
    pub(crate) fn expiry_markers(&self, key: &BindIdentity) -> Result<Vec<Marker>, OpenError> {
        expired::read(
            &StorageClass::Ephemeral.store_root(&self.state_root),
            &SessionAddr(broca_core::session_addr(key)),
            &triple(key),
            self.deployment(),
        )
        .map_err(|error| match error {
            expired::Error::KeyUnavailable => super::stored_key_unavailable(),
            other => OpenError::Open(format!(
                "the session's expiry marker could not be read: {other:?}"
            )),
        })
    }

    /// The deadline [`Self::expiry_with_view`] decides for a session no actor holds, read without
    /// any deadline retention already knows.
    #[cfg(test)]
    pub(crate) async fn expired_at(
        &self,
        key: &BindIdentity,
        run: Option<&str>,
        now_ms: u64,
    ) -> Result<Option<u64>, OpenError> {
        self.expiry_with_view(key, run, now_ms, false, None, None)
            .await
            .map(|(deadline, _)| deadline)
    }

    pub(crate) async fn expiry_with_view(
        &self,
        key: &BindIdentity,
        run: Option<&str>,
        now_ms: u64,
        actor_open: bool,
        retention: Option<&crate::retention::RetentionService>,
        known_class: Option<Option<StorageClass>>,
    ) -> Result<(Option<u64>, Option<crate::store_read::StoredLocalView>), OpenError> {
        let read = self
            .expiry_with_position(key, run, now_ms, actor_open, retention, known_class)
            .await?;
        Ok((read.deadline, read.view))
    }

    /// The expiry decision and its loaded view, for the rest of a stored read to reuse: the same
    /// deadline as the sweeper, including the expired identity left after deletion.
    /// `actor_open` says the session's actor is open: its retention timer then decides the
    /// session's deadline and deletes the session when it comes (`store_actor::expiry`), so only
    /// the expiry markers of an earlier session at the address are read here. `retention` keeps
    /// the deadlines it already knows (see [`Self::deadline_of`]). `known_class` is the root the
    /// session is kept in when the caller already found it (`Some(None)`: no root holds it).
    pub(crate) async fn expiry_with_position(
        &self,
        key: &BindIdentity,
        run: Option<&str>,
        now_ms: u64,
        actor_open: bool,
        retention: Option<&crate::retention::RetentionService>,
        known_class: Option<Option<StorageClass>>,
    ) -> Result<ExpiryRead, OpenError> {
        let markers = self.expiry_markers(key)?;
        if now_ms == 0 && markers.is_empty() {
            return Ok(ExpiryRead::default());
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
                return Ok(ExpiryRead {
                    deadline: Some(marker.expired_at_ms),
                    ..Default::default()
                });
            }
        }
        // Only a session with a retention policy expires, and such a session is created in the
        // ephemeral root. One kept in any other root can't expire, and an open session's actor
        // decides its own expiry, so neither view is loaded: every operation runs this check,
        // and loading the whole view here cost each read, subscribe or send of a running
        // session a full read of its log, which its actor then answered anyway. A running
        // session's log moves on between two operations, so each load was a new one.
        if actor_open {
            return Ok(ExpiryRead::default());
        }
        let class = match known_class {
            Some(class) => class,
            None => self.class_of(key)?,
        };
        if class.is_some_and(|class| class != StorageClass::Ephemeral) {
            return Ok(ExpiryRead::default());
        }
        // A deadline retention already knows for the session's current head decides it without
        // its records: a pass or an earlier read worked it out, and the head hasn't moved since.
        // Without this, every open of a closed session in the ephemeral root (by now most of
        // them) read the whole session here and dropped it, before its actor read it again.
        if let (Some(class), Some(retention)) = (class, retention) {
            let addr = SessionAddr(broca_core::session_addr(key));
            if let Ok(Some(position)) =
                Session::stored_position(&self.backend(class), &addr, self.deployment()).await
            {
                if let Some(deadline) = retention.kept_deadline(&addr.0, &position) {
                    if deadline.is_none_or(|deadline| deadline > now_ms) {
                        return Ok(ExpiryRead {
                            unexpired_position: Some(position),
                            ..Default::default()
                        });
                    }
                }
            }
        }
        let view = match class {
            Some(class) => self
                .current_view_in(key, class, "expiry")
                .await
                .map_err(view_error)?,
            None => None,
        };
        let Some(view) = view else {
            let newest = markers.iter().max_by_key(|m| m.seq);
            if newest.is_some_and(|marker| marker.policy_deleted == Some(true)) {
                return Err(OpenError::UnknownSession);
            }
            return Ok(ExpiryRead {
                deadline: run
                    .is_none()
                    .then(|| newest.map(|m| m.expired_at_ms))
                    .flatten(),
                ..Default::default()
            });
        };
        let Some(deadline) = self
            .deadline_of(key, &view, retention)
            .await?
            .filter(|deadline| *deadline <= now_ms)
        else {
            return Ok(ExpiryRead {
                view: Some(view),
                ..Default::default()
            });
        };
        // A run this session never ran isn't expired with it. Only a session already due pays
        // for reading its records to tell.
        if let Some(run) = run {
            let records = view.records_from(0).await.map_err(view_error)?;
            if !records.iter().any(|(_, record)| {
                matches!(record, broca_session_store::record::Record::RunStart(start) if start.run_id == run)
            }) {
                return Ok(ExpiryRead { view: Some(view), ..Default::default() });
            }
        }
        Ok(ExpiryRead {
            deadline: Some(deadline),
            view: Some(view),
            ..Default::default()
        })
    }

    /// What an operation on a session whose deadline `expired_at_ms` has come is refused with,
    /// read as `view` when it still exists. One a storage policy or a release made due reads as
    /// it will once retention has deleted it, so a caller gets the same answer before and after
    /// the pass: not there at all (llm-runner `session_retention`; the expiry marker's
    /// `policy_deleted`). Any other is expired.
    pub(crate) fn expired_refusal(
        expired_at_ms: u64,
        view: Option<&crate::store_read::StoredView<LocalBackend>>,
    ) -> OpenError {
        if view.is_some_and(|view| {
            view.operations.retention_policy.is_some() || view.operations.released_at_ms.is_some()
        }) {
            return OpenError::UnknownSession;
        }
        OpenError::Expired { expired_at_ms }
    }

    /// The deadline of the session `key`, kept in the ephemeral root and read as `view`, by the
    /// rule retention's pass deletes it by (`retention::store::session_deadline`): a storage
    /// policy its starter froze, else its older retention seconds, ended sooner by a release.
    /// The session's records are read only when `retention` doesn't already know its deadline at
    /// the view's position; what they give is kept there, so each operation on a closed session
    /// doesn't read its whole history again.
    pub(crate) async fn deadline_of(
        &self,
        key: &BindIdentity,
        view: &crate::store_read::StoredView<LocalBackend>,
        retention: Option<&crate::retention::RetentionService>,
    ) -> Result<Option<u64>, OpenError> {
        let operations = &view.operations;
        // A session whose first run set no retention seconds, with no storage policy and no
        // release, has no deadline, which its folded state alone says.
        if operations.retention_policy.is_none()
            && operations.released_at_ms.is_none()
            && operations
                .owner
                .first_start
                .as_ref()
                .is_some_and(|start| start.retention_secs.is_none())
        {
            return Ok(None);
        }
        let addr = broca_core::session_addr(key);
        let position = view.position();
        if let Some(deadline) = retention.and_then(|known| known.kept_deadline(&addr, &position)) {
            return Ok(deadline);
        }
        let records = view.records_from(0).await.map_err(view_error)?;
        // A frozen policy that no longer decodes can't be decided, so it expires nothing, as the
        // pass deletes nothing it can't decide.
        let Ok(deadline) = crate::retention::store::session_deadline(&records, operations) else {
            return Ok(None);
        };
        if let Some(known) = retention {
            known.keep_deadline(&addr, position, deadline);
        }
        Ok(deadline)
    }
}
