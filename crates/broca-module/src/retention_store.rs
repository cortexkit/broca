//! Retention of the sessions kept in the new storage (`broca_session_store`).
//!
//! A session with a retention policy is placed in the new storage's ephemeral root
//! (`session-store-ephemeral/`), whether it was created there or moved there from today's
//! `wal-ephemeral/` (`broca_wal::placement`). Today's retention never reaches such a session: its
//! index enrolls only WALs, and each of its steps leaves alone a session the new storage owns. So
//! each pass of the sweep that expires today's sessions also walks that root, and deletes every
//! session whose policy has expired, by the same rule ([`super::policy_of`]), read from the
//! session's own records ([`store_policy`]).
//!
//! A session is deleted only under its lease, as today's are. An open store actor holds the lease
//! for its whole life, so a session in use is left alone and decided again on a later pass, and no
//! actor can open while the lease is held here: an open that finds it held waits for the deletion
//! to finish ([`RetentionService::store_deletion_in_flight`]). Deletion removes, in this order:
//! 1. what a move from today's storage left behind: the retired WAL in `wal-ephemeral/` and its
//!    identity sidecars, today's media manifest of the session, then the `moved` marker that
//!    refuses the address on today's storage;
//! 2. the session itself, through `Session::delete`: its head first, then its kept head versions,
//!    its logs and its objects, which hold its media and its id index.
//!
//! The head goes last, so a crash part way leaves the session in place, still expired, and the
//! next pass finishes the deletion. A crash inside the store's own deletion leaves at most logs
//! and objects that no head names, which nothing reads. Afterwards the address holds no session in
//! either storage, so the next send to it starts a new one.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Mutex;

use broca_core::WalStore as _;
use broca_protocol::BindIdentity;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::head::{self, SessionTriple};
use broca_session_store::keys::Cipher;
use broca_session_store::record::Record;
use broca_session_store::session::{Position, Session, SessionError};
use broca_wal::StorageClass;

use super::{policy_of, sync_dir, PolicyEvent, RetentionService};
use crate::store_open::StoreRuntime;

/// What retention keeps between passes about the sessions in the new storage.
#[derive(Default)]
pub(super) struct StoreRetention {
    /// Each session's deadline, as its records said at the stored position it was read at, by
    /// address. A pass reads a session's records again only once the session has changed.
    deadlines: Mutex<HashMap<String, (Position, Option<u64>)>>,
    /// The sessions being deleted now, under their lease.
    deleting: Mutex<HashSet<BindIdentity>>,
}

/// Marks a session as being deleted for as long as it lives, including when the deletion fails
/// or panics part way.
struct Deleting<'a> {
    sessions: &'a Mutex<HashSet<BindIdentity>>,
    key: BindIdentity,
}

impl<'a> Deleting<'a> {
    fn start(sessions: &'a Mutex<HashSet<BindIdentity>>, key: &BindIdentity) -> Self {
        sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key.clone());
        Self {
            sessions,
            key: key.clone(),
        }
    }
}

impl Drop for Deleting<'_> {
    fn drop(&mut self) {
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
    }
}

/// The retention seconds, last activity and deadline of the new storage's session whose records
/// are `records`, every one of both streams in seq order; `None` for a session whose first run
/// set no retention. The records today's rule reads keep their meaning in the new storage: a run
/// start carries its retention and time; a queued send its time, and in its receipt the retention
/// it set; a steer both in its receipt; a run's end and pause their time. A send queued or steered
/// by the store actor, and one converted from today's log, keep these under today's names
/// (`broca_session_loop::queue`).
pub(super) fn store_policy(records: &[(u64, Record)]) -> Option<(u64, u64, Option<u64>)> {
    // Whether a session may keep content is fixed by its first run, as it is for today's.
    let first = records.iter().find_map(|(_, record)| match record {
        Record::RunStart(start) => Some(start.retention_secs),
        _ => None,
    });
    if !matches!(first, Some(Some(_))) {
        return None;
    }
    let receipt_u64 = |receipt: &Option<serde_json::Value>, name: &str| {
        receipt
            .as_ref()
            .and_then(|receipt| receipt.get(name))
            .and_then(serde_json::Value::as_u64)
    };
    Some(policy_of(records.iter().filter_map(
        |(_, record)| match record {
            Record::RunStart(start) => Some(PolicyEvent::Started {
                retention: start.retention_secs,
                ts_ms: start.ts_ms,
            }),
            Record::QueuedSend(send) => Some(PolicyEvent::Sent {
                retention: receipt_u64(&send.receipt, "retention"),
                ts_ms: send.ts_ms,
            }),
            Record::Steer(steer) => Some(PolicyEvent::Sent {
                retention: receipt_u64(&steer.receipt, "retention"),
                ts_ms: receipt_u64(&steer.receipt, "ts_ms"),
            }),
            Record::RunEnd(end) => Some(PolicyEvent::RunFinished { ts_ms: end.ts_ms }),
            Record::Pause(pause) => Some(PolicyEvent::RunPaused { ts_ms: pause.ts_ms }),
            _ => None,
        },
    )))
}

fn described(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

impl RetentionService {
    /// Whether retention is deleting the session `key` from the new storage now, holding its
    /// lease. An open that finds the lease held waits for this, as it waits for today's deletion
    /// of an expired session, rather than refusing the send.
    pub(crate) fn store_deletion_in_flight(&self, key: &BindIdentity) -> bool {
        self.store
            .deleting
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(key)
    }

    /// Deletes every session in the new storage's ephemeral root whose retention policy expired
    /// at or before `now_ms`, each under its lease, and returns the sessions deleted. One that
    /// can't be decided or deleted now is logged and decided again on the next pass.
    pub(crate) async fn sweep_store(
        &self,
        runtime: &StoreRuntime,
        now_ms: u64,
    ) -> Vec<BindIdentity> {
        // On an overhaul root today's retention deletes nothing yet; neither does this.
        if self.wal.wal_config().is_overhaul() {
            return Vec::new();
        }
        let backend = LocalBackend::new(StorageClass::Ephemeral.store_root(runtime.state_root()));
        let addresses = match backend.stored_sessions() {
            Ok(addresses) => addresses,
            Err(error) => {
                tracing::warn!(target: "retention", error = ?error,
                    "retention pass over the new storage deferred; its ephemeral root can't be listed");
                return Vec::new();
            }
        };
        self.store
            .deadlines
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|address, _| addresses.iter().any(|addr| addr.0 == *address));
        let mut deleted = Vec::new();
        for addr in addresses {
            match self
                .expire_store_session(runtime, &backend, &addr, now_ms)
                .await
            {
                Ok(Some(key)) => deleted.push(key),
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(target: "retention", session = %addr.0, %error,
                        "deletion deferred; the new storage's session couldn't be decided or deleted");
                }
            }
        }
        deleted
    }

    /// Deletes the session at `addr` in the ephemeral root when its policy expired at or before
    /// `now_ms` and its lease is free, and returns who it was. `None` when it is kept.
    async fn expire_store_session(
        &self,
        runtime: &StoreRuntime,
        backend: &LocalBackend,
        addr: &SessionAddr,
        now_ms: u64,
    ) -> Result<Option<BindIdentity>, String> {
        let Some((stored, _)) = backend.read_stored_head(addr).map_err(described)? else {
            return Ok(None);
        };
        // A head names its session in the clear, so its lease can be named without the key. An
        // encrypted session's records can't be read before the deployment key has loaded.
        let triple = head::stored_triple(&stored).map_err(described)?;
        if head::stored_cipher(&stored).map_err(described)? != Cipher::None && !runtime.key_loaded()
        {
            return Ok(None);
        }
        let key = BindIdentity::new(
            triple.project_root.clone(),
            triple.harness.clone(),
            triple.session.clone(),
        );
        if broca_core::session_addr(&key) != addr.0 {
            return Err("the session's head names a session filed under another address".into());
        }
        // Decided first without the lease, so a session that hasn't expired costs no lease.
        let Some(deadline) = self
            .store_deadline(runtime, backend, addr, &triple, true)
            .await?
        else {
            return Ok(None);
        };
        if deadline > now_ms {
            return Ok(None);
        }
        let lease = match self.lease(&key) {
            Ok(lease) => lease,
            Err(_) => {
                // An open actor holds the lease for as long as it is open: the session is in use.
                tracing::warn!(target: "retention", session = %addr.0,
                    "deletion deferred; lease or durable operation unavailable");
                return Ok(None);
            }
        };
        let deleting = Deleting::start(&self.store.deleting, &key);
        let deleted = self
            .delete_expired_store_session(runtime, backend, addr, &key, &triple, now_ms)
            .await;
        drop(deleting);
        drop(lease);
        Ok(deleted?.then_some(key))
    }

    /// The deadline of the session at `addr`, from its records at its stored position: the cached
    /// one when `cached` and the session hasn't changed since it was read. `None` when it has no
    /// deadline: no retention policy, a run active or paused, or no record yet.
    async fn store_deadline(
        &self,
        runtime: &StoreRuntime,
        backend: &LocalBackend,
        addr: &SessionAddr,
        triple: &SessionTriple,
        cached: bool,
    ) -> Result<Option<u64>, String> {
        let deployment = runtime.deployment();
        let position = match Session::stored_position(backend, addr, deployment).await {
            Ok(Some(position)) => position,
            // A creation that never committed holds no record.
            Ok(None) => return Ok(None),
            Err(error) => return Err(self.unreadable_store_session(addr, error)),
        };
        if cached {
            let deadlines = self
                .store
                .deadlines
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some((at, deadline)) = deadlines.get(&addr.0) {
                if *at == position {
                    return Ok(*deadline);
                }
            }
        }
        let records =
            match Session::view_at(backend, addr.clone(), triple, deployment, &position).await {
                Ok(view) => view.records_from(0).await,
                Err(error) => Err(error),
            };
        let records = match records {
            Ok(records) => records,
            // A writer moved the head between the probe and the read: decided on the next pass.
            Err(SessionError::Moved) => return Ok(None),
            Err(error) => return Err(self.unreadable_store_session(addr, error)),
        };
        let deadline = store_policy(&records).and_then(|(_, _, deadline)| deadline);
        self.store
            .deadlines
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(addr.0.clone(), (position, deadline));
        Ok(deadline)
    }

    /// Counts the session at `addr` as unreadable, as today's retention counts a WAL it can't
    /// read, and says why.
    fn unreadable_store_session(&self, addr: &SessionAddr, error: SessionError) -> String {
        self.mark_unreadable_address(addr.0.clone());
        format!("the session's records could not be read: {error:?}")
    }

    /// Deletes the session `key` at `addr` when, read again now under its lease, its policy has
    /// expired at or before `now_ms`. Returns whether it was deleted. The caller holds the lease.
    async fn delete_expired_store_session(
        &self,
        runtime: &StoreRuntime,
        backend: &LocalBackend,
        addr: &SessionAddr,
        key: &BindIdentity,
        triple: &SessionTriple,
        now_ms: u64,
    ) -> Result<bool, String> {
        let state_root = runtime.state_root();
        // A move whose journal still names the session hasn't finished; the move, or startup,
        // finishes it first.
        if broca_wal::session_storage_move_journal(state_root, &addr.0)
            .try_exists()
            .map_err(|error| error.to_string())?
        {
            return Ok(false);
        }
        // A head in two roots is damage, which is never resolved by deleting one of them.
        if broca_wal::session_storage_class(state_root, &addr.0)
            .map_err(|error| error.to_string())?
            != Some(StorageClass::Ephemeral)
        {
            return Ok(false);
        }
        // Under the lease no writer moves the head, so the session read here is the one deleted:
        // the store refuses the deletion if the head has moved past this position since.
        let deployment = runtime.deployment();
        let Some(position) = Session::stored_position(backend, addr, deployment)
            .await
            .map_err(|error| self.unreadable_store_session(addr, error))?
        else {
            return Ok(false);
        };
        let records = Session::view_at(backend, addr.clone(), triple, deployment, &position)
            .await
            .map_err(described)?
            .records_from(0)
            .await
            .map_err(|error| self.unreadable_store_session(addr, error))?;
        let Some(deadline) = store_policy(&records).and_then(|(_, _, deadline)| deadline) else {
            return Ok(false);
        };
        if deadline > now_ms {
            return Ok(false);
        }
        let moved = records.iter().find_map(|(_, record)| match record {
            Record::Moved(moved) => Some(moved.move_id.as_str()),
            _ => None,
        });
        self.remove_move_leftovers(key, moved)?;
        Session::delete(backend, addr, position.version)
            .await
            .map_err(described)?;
        broca_wal::migration::unlink(&state_root.join("session-store-moves/streams").join(&addr.0))
            .map_err(|error| error.to_string())?;
        self.store
            .deadlines
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&addr.0);
        self.unreadable
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&addr.0);
        // Still under the lease, as today's deletion evicts, so no session created at the address
        // afterwards loses its routes.
        if let Some(evict) = self.evict.get() {
            evict(key);
        }
        crate::diagnostics::retention_deleted(key);
        tracing::info!(target: "retention", session = %addr.0, expired_at_ms = deadline,
            "expired session deleted from the new storage");
        Ok(true)
    }

    /// Removes what the move `move_id` left of the session `key` on today's storage: the retired
    /// WAL in `wal-ephemeral/` and its identity sidecars, today's media manifest of the session,
    /// and last the `moved` marker that refuses the address there. A session the new storage
    /// created (`move_id` is `None`) left nothing there. A marker of another move, or a WAL at the
    /// address that no marker of this move retires, isn't this session's, and is refused rather
    /// than deleted.
    fn remove_move_leftovers(
        &self,
        key: &BindIdentity,
        move_id: Option<&str>,
    ) -> Result<(), String> {
        let wal = self.wal.ephemeral_wal_path(key);
        // A move's sidecars stay at the session's address in `wal/`, whichever folder held it.
        let marker = self.wal.wal_path(key).with_extension("moved");
        let moved = broca_wal::migration::read_json::<broca_wal::migration::Moved>(&marker)
            .map_err(|error| error.to_string())?;
        let ours = moved
            .as_ref()
            .is_some_and(|moved| Some(moved.cut_id.as_str()) == move_id);
        if moved.is_some() && !ours {
            return Err("today's moved marker at the session's address names another move".into());
        }
        let retired = wal.try_exists().map_err(|error| error.to_string())?;
        if retired && !ours {
            return Err("a WAL at the session's address isn't one its move retired".into());
        }
        let Some(moved) = moved else {
            return Ok(());
        };
        if retired && self.initial_lineage(&wal)? != moved.lineage_id {
            return Err("the retired WAL isn't the lineage the session moved from".into());
        }
        let session = wal.with_extension("session");
        for path in [wal, session.clone(), session.with_extension("session.tmp")] {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.to_string()),
            }
        }
        sync_dir(&self.wal.ephemeral_root())?;
        let state_root = self
            .wal
            .root()
            .parent()
            .ok_or("WAL tree has no state root")?;
        if state_root
            .join("media-ephemeral")
            .try_exists()
            .map_err(|error| error.to_string())?
        {
            broca_core::media_store::MediaStore::open(
                state_root,
                broca_core::media_store::MediaStoreClass::Ephemeral,
            )
            .and_then(|store| store.remove_session_after_wal_removal(key))
            .map_err(|error| error.to_string())?;
        }
        broca_wal::migration::unlink(&marker).map_err(|error| error.to_string())
    }
}

#[cfg(test)]
#[path = "retention_store_tests.rs"]
mod tests;
