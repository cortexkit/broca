//! Boot-only recovery of derived rows. No service should read the index until
//! the caller has reconciled its session inventory while all writers are stopped.

use std::path::Path;

use broca_protocol::BindIdentity;

use super::publication::{self, CheckedEnd};
use super::{index, DbError, IndexOutcome, LogLocation, OverhaulDb};

/// An existing session WAL from the boot inventory, not a live writer's path.
/// Archived WALs must be supplied as their uncompressed, immutable WAL file.
pub struct ReconcileSession<'a> {
    pub bind: &'a BindIdentity,
    pub session_ref: &'a str,
    pub location: LogLocation,
    pub path: &'a Path,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileOutcome {
    pub index: IndexOutcome,
    /// Bytes after the last complete frame. Recovery never truncates the WAL;
    /// the next writer open owns removal of this unpublished suffix.
    pub ignored_suffix_bytes: u64,
    pub publication: broca_wal::file_wal::DurablePublication,
}

impl OverhaulDb {
    /// Reconcile the caller's complete WAL inventory before enabling service
    /// reads or restart/orphan handling. Each session commits independently, so
    /// an interrupted boot can safely repeat the inventory from the beginning.
    pub fn reconcile_boot<'a>(
        &self,
        sessions: impl IntoIterator<Item = ReconcileSession<'a>>,
    ) -> Result<Vec<ReconcileOutcome>, DbError> {
        sessions
            .into_iter()
            .map(|session| self.catch_up_session(&session))
            .collect()
    }

    /// Authenticate the stored row cursor and consume only its new tail. Missing
    /// or invalid cursors rebuild this session, with a counted fallback reason.
    /// The caller must exclude writers, file replacement and prefix rewrites
    /// throughout recovery; same-lineage rewrites require `forget_session` first.
    pub fn catch_up_session(
        &self,
        session: &ReconcileSession<'_>,
    ) -> Result<ReconcileOutcome, DbError> {
        self.reconcile_session(session, false)
    }

    /// Explicitly regenerate one session's derived rows from its WAL. Neither
    /// authoritative tables nor any other session's index rows are changed.
    pub fn rebuild_session_from_wal(
        &self,
        session: &ReconcileSession<'_>,
    ) -> Result<ReconcileOutcome, DbError> {
        self.reconcile_session(session, true)
    }

    fn reconcile_session(
        &self,
        session: &ReconcileSession<'_>,
        force: bool,
    ) -> Result<ReconcileOutcome, DbError> {
        // Use one descriptor for authentication, tail recovery and sync. A
        // path-based EOF is not a durable publication witness.
        let recovery =
            broca_wal::file_wal::WalRecovery::open(session.path, broca_wal::DEFAULT_MAX_READ_BYTES)
                .map_err(DbError::Wal)?;
        let size = recovery.byte_len();
        let read = |off, len| recovery.read_at(off, len);
        self.transaction(false, |tx| {
            let lineage = recovery.lineage();
            let key = index::sid(session.bind)?;
            let prepared = publication::prepare_cursor(
                tx,
                &key,
                &lineage.to_string(),
                size,
                &read,
                Some(&recovery),
                Some(publication::RebuildRequest {
                    reason: if force {
                        "boot session rebuild requested"
                    } else {
                        "boot index missing or invalid"
                    },
                    force,
                }),
            )?;
            let (start, _) = prepared.start();
            // Only a rebuild starts at zero. Its origin read and every decode
            // remain in the same shared operation as bounded boot catch-up.
            let publication = recovery
                .recover_and_sync(prepared.recovery_cursor())
                .map_err(DbError::Wal)?;
            let end = publication.byte_end();
            let lineage = lineage.to_string();
            let checked = CheckedEnd {
                lineage: &lineage,
                end,
                seq: publication.sequence(),
            };
            // Reuse publication's validator and seed advancement. All hydration
            // is capped at the recovered end, never at the torn physical EOF.
            let bounded = |off: u64, len: u64| read(off, len.min(end.saturating_sub(off)));
            let mut index = publication::publish_checked_in_tx(
                tx,
                session.bind,
                session.session_ref,
                session.location,
                &checked,
                &bounded,
                prepared,
            )?;
            index.rebuilt |= start == 0;
            Ok(ReconcileOutcome {
                index,
                ignored_suffix_bytes: size - end,
                publication,
            })
        })
    }
}
