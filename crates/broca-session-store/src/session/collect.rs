//! Deleting what committed changes stopped naming, once it has gone unnamed for the grace period
//! ([`GRACE_MS`]).
//!
//! A committed change records in the head what it stopped naming (`head::PendingDeletion`): the
//! log it replaced, the directory pages it rewrote, and the lists of index pages and segments
//! it retired. Nothing the head names is ever among them. They're deleted only once they have
//! gone unnamed for the grace period, so until then a reader that loaded an older head can finish,
//! and a backup's walk that began before the change still finds every file of the head version it
//! may restore (see `restore`).
//!
//! Two ways in:
//! - [`Session::collect`]: the session's writer collects its own head.
//! - [`collect_stored`]: a session no writer holds. It reads the head, deletes what is due, and
//!   replaces the head only while it is still the version it read. Its caller holds the session's
//!   lease, so no writer opens the session meanwhile.

use crate::backend::{SessionAddr, SessionBackend, Version};
use crate::codec::Id128;
use crate::head::{self, Head, HeadError, PendingDeletion, Residence};
use crate::idindex::{self, IndexPages, Retired};
use crate::keys::{DataKey, DeploymentKey};

use super::{SessionError, GRACE_MS};

/// Whether `pending` has gone unnamed for the grace period at `now_ms`.
pub(super) fn is_due(pending: &PendingDeletion, now_ms: u64) -> bool {
    pending.unreferenced_at_ms.saturating_add(GRACE_MS) <= now_ms
}

/// The deletions `head` holds that are due at `now_ms`.
pub(super) fn due(head: &Head, now_ms: u64) -> Vec<PendingDeletion> {
    head.pending
        .iter()
        .filter(|pending| is_due(pending, now_ms))
        .cloned()
        .collect()
}

/// Whether the stored head `bytes` holds a deletion due at `now_ms`, read without checking the
/// head. Only for deciding whether to collect: [`collect_stored`] then opens the head and checks
/// it before deleting anything.
pub fn stored_due(bytes: &[u8], now_ms: u64) -> Result<bool, HeadError> {
    Ok(head::stored_pending(bytes)?
        .iter()
        .any(|pending| is_due(pending, now_ms)))
}

/// Collects the session at `addr`, which no writer holds: deletes what its head holds that is due
/// at `now_ms`, then writes the head without those entries. Returns how many changes' deletions
/// ran; 0, with nothing written, when none is due.
///
/// The caller holds the session's lease, so no writer opens the session while this runs. Each
/// deletion and the head write are fenced on the version read: a head that moved meanwhile
/// refuses them, and its own pending list still names whatever wasn't deleted yet. A session whose
/// creation or recovery hasn't committed is left to the load that finishes it.
pub async fn collect_stored<B: SessionBackend>(
    backend: &B,
    addr: &SessionAddr,
    deployment: &DeploymentKey,
    now_ms: u64,
) -> Result<usize, SessionError> {
    let Some((stored, version)) = backend.read_head(addr).await? else {
        return Ok(0);
    };
    let (mut head, key) = head::open(&stored, deployment).map_err(SessionError::Head)?;
    if !matches!(head.residence, Residence::Local(())) {
        return Ok(0);
    }
    let due = due(&head, now_ms);
    if due.is_empty() {
        return Ok(0);
    }
    delete(backend, addr, head.session_id, &key, version, &due).await?;
    let done: Vec<Id128> = due.iter().map(|pending| pending.change).collect();
    head.pending
        .retain(|pending| !done.contains(&pending.change));
    backend
        .write_head(addr, &head::seal(&head, &key), Some(version), version + 1)
        .await?;
    Ok(due.len())
}

/// Deletes every file the entries of `due` name, at head version `version`. A file already gone
/// counts as deleted, so a collection that stopped halfway runs again from the same entries.
pub(super) async fn delete<B: SessionBackend>(
    backend: &B,
    addr: &SessionAddr,
    session: Id128,
    key: &DataKey,
    version: Version,
    due: &[PendingDeletion],
) -> Result<(), SessionError> {
    let key = key.frame_key();
    let index = IndexPages {
        backend,
        addr,
        session,
        key: &key,
    };
    for pending in due {
        // A retired-runs list is read before anything it names is deleted, and deleted
        // last, so a collection that stops halfway runs again from the list.
        if let Some(list) = &pending.retired_index {
            match index.read::<Retired>(list).await {
                Ok(retired) => {
                    index
                        .delete_retired(&retired.refs, version)
                        .await
                        .map_err(SessionError::Index)?;
                    backend.delete_object(addr, list, version).await?;
                }
                Err(idindex::IndexError::Missing(_)) => {}
                Err(error) => return Err(SessionError::Index(error)),
            }
        }
        // A revert's list of abandoned objects, likewise read first and deleted last.
        if let Some(list) = &pending.abandoned {
            match index.read::<Retired>(list).await {
                Ok(abandoned) => {
                    for object in &abandoned.refs {
                        backend.delete_object(addr, object, version).await?;
                    }
                    backend.delete_object(addr, list, version).await?;
                }
                Err(idindex::IndexError::Missing(_)) => {}
                Err(error) => return Err(SessionError::Index(error)),
            }
        }
        for log in &pending.logs {
            backend.delete_log(addr, log, version).await?;
        }
        for object in &pending.objects {
            backend.delete_object(addr, object, version).await?;
        }
    }
    Ok(())
}
