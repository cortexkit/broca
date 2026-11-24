//! Retention of the sessions kept in the session store (`broca_session_store`).
//!
//! A session with a retention policy is placed in the store's ephemeral root
//! (`session-store-ephemeral/`). Each pass of the sweep walks that root and deletes every
//! session whose policy has expired ([`super::policy_of`]), read from the session's own records
//! ([`store_policy`]).
//!
//! A session is deleted only under its lease. The sweep leaves a held lease alone; an open actor
//! expires itself once no run is active. No actor can open while deletion holds the lease: an
//! open that finds it held waits for the deletion to finish
//! ([`RetentionService::store_deletion_in_flight`]). Deletion first publishes a sealed,
//! content-free expiry marker under `expired/`, then removes the session through
//! `Session::delete`: its head first, then its kept head versions, its logs and its objects,
//! which hold its media and its id index.
//!
//! A crash part way leaves at most logs and objects that no head names, which nothing reads.
//! Afterwards reads stay expired through the marker, while the next send may start a new
//! session carrying the marker's counter floors.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use broca_protocol::BindIdentity;
use broca_records::session_storage::StorageClass;
use broca_session_store::backend::{HeadStamp, LocalBackend, SessionAddr};
use broca_session_store::head::{self, SessionTriple};
use broca_session_store::keys::Cipher;
use broca_session_store::record::Record;
use broca_session_store::session::{Position, Session, SessionError};

use super::{policy_of, PolicyEvent, RetentionService};
use crate::store_open::StoreRuntime;

#[path = "retention_store_counters.rs"]
pub(crate) mod counters;
#[path = "retention_store_held.rs"]
mod held;
#[path = "retention_store_deadlines.rs"]
mod kept;
#[path = "retention_store_marks.rs"]
pub(crate) mod marks;

/// What retention keeps between passes about the sessions in the session store.
#[derive(Default)]
pub(super) struct StoreRetention {
    /// Each session's deadline, as its records said at the stored position it was read at, by
    /// address. A pass reads a session's records again only once the session has changed. Kept
    /// on disk too ([`kept`]), so a restart's first pass doesn't read every session again.
    deadlines: Mutex<HashMap<String, (Position, Option<u64>)>>,
    /// The stamp each session's head file had when its deadline in `deadlines` was last confirmed,
    /// by address. A pass that finds the same stamp again knows the head didn't move, so it
    /// decides the session from its kept deadline without reading the head: tens of thousands
    /// of small head reads every pass otherwise cost more disk reads than anything else broca
    /// does while idle. Kept in memory only: a restart's first pass reads every head once.
    stamps: Mutex<HashMap<String, HeadStamp>>,
    /// The class of each session the last full pass found marked outside the ephemeral root
    /// ([`marks`]), by address, so a pass between full passes reads a due one from its own root.
    /// A session not named here lives in the ephemeral root.
    marked_classes: Mutex<HashMap<String, StorageClass>>,
    /// Whether `deadlines` was filled from the kept file, which the first pass does.
    loaded: std::sync::atomic::AtomicBool,
    /// Whether `deadlines` changed since it was last kept.
    changed: std::sync::atomic::AtomicBool,
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

/// The retention seconds, last activity and deadline of the session store's session whose records
/// are `records`, every one of both streams in seq order; `None` for a session whose first run
/// set no retention. The records' retention rules are: a run start carries its retention and
/// time; a queued send its time, and in its receipt the retention it set; a steer both in its
/// receipt; a run's end and pause their time. A send queued or steered by the store actor, or
/// converted from the old log format, uses these field names (`broca_session_loop::queue`).
///
/// A fork's session (see [`crate::fork_state`]) has no run: its retention seconds, last activity
/// and deadline come from its fork's records.
pub fn store_policy(records: &[(u64, Record)]) -> Option<(u64, u64, Option<u64>)> {
    // A fork's session runs nothing: its own records say how long it is kept
    // (`ForkState::retained_until_ms`). One whose records can't be folded has no deadline, so
    // nothing a reader can't decide is deleted.
    if !records
        .iter()
        .any(|(_, record)| matches!(record, Record::RunStart(_)))
    {
        if let Ok(Some(fork)) =
            crate::fork_state::ForkState::fold(records.iter().map(|(_, record)| record))
        {
            return Some((
                fork.retain_ms / 1_000,
                fork.last_at_ms(),
                Some(fork.retained_until_ms()),
            ));
        }
    }
    // Whether a session may keep content is fixed by its first run.
    let first = records.iter().find_map(|(_, record)| match record {
        Record::RunStart(start) => Some(start.retention_secs),
        _ => None,
    });
    if !matches!(first, Some(Some(_))) {
        return None;
    }
    Some(policy_of(
        records
            .iter()
            .filter_map(|(_, record)| policy_event(record)),
    ))
}

/// What `record` says about its session's retention clock, by the rules [`store_policy`] names;
/// `None` for a record that says nothing about it.
pub(crate) fn policy_event(record: &Record) -> Option<PolicyEvent> {
    let receipt_u64 = |receipt: &Option<broca_protocol::json::Json>, name: &str| {
        receipt
            .as_ref()
            .and_then(|receipt| receipt.parse().get(name)?.as_u64())
    };
    match record {
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
    }
}

/// When the session whose records are `records`, folded into `operations`, is due for deletion.
/// A session whose starter (the first send carrying one) froze a storage policy is kept by that policy (llm-runner
/// `session_retention`, [`super::policy::deadline`]); any other by its older `retention`
/// seconds ([`store_policy`]). A release ends either sooner. `None` when nothing makes it due.
/// A frozen policy that no longer decodes is refused: nothing a reader can't decide is deleted.
pub(crate) fn session_deadline(
    records: &[(u64, Record)],
    operations: &broca_session_store::fold::operations::Operations,
) -> Result<Option<u64>, String> {
    let legacy = || store_policy(records).and_then(|(_, _, deadline)| deadline);
    if operations.retention_policy.is_none() && operations.released_at_ms.is_none() {
        return Ok(legacy());
    }
    let legacy = operations.retention_policy.is_none().then(legacy).flatten();
    let starter = operations
        .retention_policy
        .as_ref()
        .map(cortexkit_role_llm_runner::session_retention::RetentionPolicy::from_value)
        .transpose()
        .map_err(|field| format!("the session's frozen retention policy is damaged at {field}"))?;
    let busy = operations
        .run
        .as_ref()
        .is_some_and(|run| run.finished.is_none())
        || !operations.queue.is_empty();
    let activity = records
        .iter()
        .filter_map(|(_, record)| record.activity_ms())
        .max();
    Ok(super::policy::deadline(
        starter.as_ref(),
        operations.released_at_ms,
        legacy,
        busy,
        activity,
    ))
}

/// What an operator's deletion of a session in the session store came to
/// ([`RetentionService::delete_store_session_for_operator`]).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum OperatorDeletion {
    Deleted,
    /// No session is stored at the address.
    Absent,
    /// The session holds work a deletion would lose; says which.
    Busy(&'static str),
    /// An actor has the session open, holding its lease, or a move is finishing it.
    Held,
}

fn described(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

impl RetentionService {
    /// Checks and deletes a stored session while its opener or actor still holds the lease.
    pub(crate) async fn delete_store_under_lease(
        &self,
        runtime: &StoreRuntime,
        key: &BindIdentity,
        now_ms: u64,
    ) -> Result<bool, String> {
        let _deleting = Deleting::start(&self.store.deleting, key);
        let addr = SessionAddr(broca_core::session_addr(key));
        // A session with a storage policy may be kept in any root; one with only the older
        // retention is in the ephemeral one.
        let Some(class) =
            broca_records::session_storage::session_storage_class(runtime.state_root(), &addr.0)
                .map_err(|error| error.to_string())?
        else {
            return Ok(false);
        };
        let backend = LocalBackend::new(class.store_root(runtime.state_root()));
        self.delete_expired_store_session(
            runtime,
            &backend,
            class,
            &addr,
            key,
            &crate::store_open::triple(key),
            now_ms,
        )
        .await
    }
    /// Whether retention is deleting the session `key` from the session store now, while holding
    /// its lease. An open that finds the lease held waits for this deletion rather than refusing
    /// the send.
    pub(crate) fn store_deletion_in_flight(&self, key: &BindIdentity) -> bool {
        self.store
            .deleting
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(key)
    }

    /// Deletes every session in the session store's ephemeral root, and every session marked in
    /// its other roots ([`marks`]), whose retention policy expired at or before `now_ms`, each
    /// under its lease, and returns the sessions deleted. One that can't be decided or deleted
    /// now is logged and decided again on the next pass. This is the full pass: it lists every
    /// session folder and mark, and checks every head for a change (see
    /// [`crate::retention::FULL_PASS_EVERY`]).
    pub(crate) async fn sweep_store(
        &self,
        runtime: &StoreRuntime,
        now_ms: u64,
    ) -> Vec<BindIdentity> {
        use std::sync::atomic::Ordering;
        let root = StorageClass::Ephemeral.store_root(runtime.state_root());
        if !self.store.loaded.swap(true, Ordering::Relaxed) {
            let kept = kept::load(&root);
            let mut deadlines = self
                .store
                .deadlines
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            for (address, found) in kept {
                deadlines.entry(address).or_insert(found);
            }
        }
        let backend = LocalBackend::new(&root);
        // Listing checks a head file in each of tens of thousands of folders, which takes a
        // second or more of blocking file system calls: kept off the async worker threads.
        let listing = {
            let root = root.clone();
            tokio::task::spawn_blocking(move || {
                LocalBackend::new(root).stored_sessions_and_deletions()
            })
            .await
        };
        let (addresses, cut_short) = match listing {
            Ok(Ok(found)) => found,
            Ok(Err(error)) => {
                tracing::warn!(target: "retention", error = ?error,
                    "retention pass over the new storage deferred; its ephemeral root can't be listed");
                return Vec::new();
            }
            Err(error) => {
                tracing::warn!(target: "retention", %error,
                    "retention pass over the new storage deferred; listing its ephemeral root failed");
                return Vec::new();
            }
        };
        let mut sessions: Vec<(StorageClass, SessionAddr)> = addresses
            .into_iter()
            .map(|addr| (StorageClass::Ephemeral, addr))
            .collect();
        for class in marks::MARKED {
            match marks::marked(&class.store_root(runtime.state_root())) {
                Ok(marked) => sessions.extend(marked.into_iter().map(|addr| (class, addr))),
                Err(error) => tracing::warn!(target: "retention", class = class.name(), %error,
                    "retention pass skips a root's marked sessions; their marks can't be listed"),
            }
        }
        // A set, not a scan of the list for each cached deadline: the ephemeral root holds tens
        // of thousands of sessions, and the scan cost each pass their count squared.
        let listed: std::collections::HashSet<String> =
            sessions.iter().map(|(_, addr)| addr.0.clone()).collect();
        {
            let mut deadlines = self
                .store
                .deadlines
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let before = deadlines.len();
            deadlines.retain(|address, _| listed.contains(address.as_str()));
            if deadlines.len() != before {
                self.store.changed.store(true, Ordering::Relaxed);
            }
            self.store
                .stamps
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .retain(|address, _| listed.contains(address.as_str()));
            *self
                .store
                .marked_classes
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = sessions
                .iter()
                .filter(|(class, _)| *class != StorageClass::Ephemeral)
                .map(|(class, addr)| (addr.0.clone(), *class))
                .collect();
        }
        for addr in cut_short {
            if let Err(error) = self.finish_store_deletion(runtime, &backend, &addr) {
                tracing::warn!(target: "retention", session = %addr.0, %error,
                    "a deletion a crash cut short is left to finish on the next pass");
            }
        }
        self.expire_each(runtime, &root, sessions, now_ms).await
    }

    /// A pass between full passes: deletes the sessions whose kept deadline is at or before
    /// `now_ms`, read again first, and lists nothing. A session no full pass has read yet, or one
    /// changed since the last, waits for the next full pass. Before the first full pass nothing
    /// is kept, so this reads nothing.
    pub(crate) async fn sweep_due_store(
        &self,
        runtime: &StoreRuntime,
        now_ms: u64,
    ) -> Vec<BindIdentity> {
        let root = StorageClass::Ephemeral.store_root(runtime.state_root());
        let due: Vec<SessionAddr> = self
            .store
            .deadlines
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|(_, (_, deadline))| deadline.is_some_and(|deadline| deadline <= now_ms))
            .map(|(address, _)| SessionAddr(address.clone()))
            .collect();
        let due = {
            let marked = self
                .store
                .marked_classes
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            due.into_iter()
                .map(|addr| {
                    let class = marked
                        .get(&addr.0)
                        .copied()
                        .unwrap_or(StorageClass::Ephemeral);
                    (class, addr)
                })
                .collect()
        };
        self.expire_each(runtime, &root, due, now_ms).await
    }

    /// Decides and deletes each session of `sessions`, each in the root of its class, then keeps
    /// the deadlines found in the ephemeral root `root` for the next restart.
    async fn expire_each(
        &self,
        runtime: &StoreRuntime,
        root: &std::path::Path,
        sessions: Vec<(StorageClass, SessionAddr)>,
        now_ms: u64,
    ) -> Vec<BindIdentity> {
        use std::sync::atomic::Ordering;
        let mut deleted = Vec::new();
        for (class, addr) in sessions {
            // A backend of its own for each session: a backend keeps every log it opens open
            // until it is dropped, and one shared across the whole pass held a file open for
            // each of the tens of thousands of sessions it read.
            let session_backend = LocalBackend::new(class.store_root(runtime.state_root()));
            match self
                .expire_store_session(runtime, &session_backend, class, &addr, now_ms)
                .await
            {
                Ok(Some(key)) => deleted.push(key),
                Ok(None) if class != StorageClass::Ephemeral => {
                    if let Err(error) = self
                        .unmark_settled(runtime, &session_backend, class, &addr)
                        .await
                    {
                        tracing::warn!(target: "retention", session = %addr.0, %error,
                            "a marked session's mark is kept; the session couldn't be read");
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(target: "retention", session = %addr.0, %error,
                        "deletion deferred; the new storage's session couldn't be decided or deleted");
                }
            }
        }
        if self.store.changed.swap(false, Ordering::Relaxed) {
            let deadlines = self
                .store
                .deadlines
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Err(error) = kept::save(root, &deadlines) {
                // Only the next restart pays: its first pass reads the sessions again.
                self.store.changed.store(true, Ordering::Relaxed);
                tracing::warn!(target: "retention", %error,
                    "the retention deadlines found couldn't be kept on disk");
            }
        }
        deleted
    }

    /// Removes what remains of the session at `addr` after a crash cut its deletion short: its
    /// head is gone, so it reads as expired, but its logs and objects are still on disk. Under the
    /// session's lease, so no session is created at the address meanwhile; a lease held elsewhere
    /// leaves it to the next pass.
    fn finish_store_deletion(
        &self,
        runtime: &StoreRuntime,
        backend: &LocalBackend,
        addr: &SessionAddr,
    ) -> Result<(), String> {
        let Some(stored) = backend.deleted_head(addr).map_err(described)? else {
            return Ok(());
        };
        let triple = head::stored_triple(&stored).map_err(described)?;
        let key = BindIdentity::new(
            triple.project_root.clone(),
            triple.harness.clone(),
            triple.session.clone(),
        );
        if broca_core::session_addr(&key) != addr.0 {
            return Err(
                "the deleted session's head names a session filed under another address".into(),
            );
        }
        let Ok(_lease) = self.lease(&key) else {
            return Ok(());
        };
        broca_records::move_fence::unlink(
            &runtime
                .state_root()
                .join("session-store-moves/streams")
                .join(&addr.0),
        )
        .map_err(|error| error.to_string())?;
        if backend.finish_deletion(addr).map_err(described)? {
            tracing::info!(target: "retention", session = %addr.0,
                "finished a deletion a crash had cut short");
        }
        Ok(())
    }

    /// Deletes the session at `addr` in the root of `class`, which `backend` reads, when its
    /// policy expired at or before `now_ms` and its lease is free, and returns who it was. `None`
    /// when it is kept.
    async fn expire_store_session(
        &self,
        runtime: &StoreRuntime,
        backend: &LocalBackend,
        class: StorageClass,
        addr: &SessionAddr,
        now_ms: u64,
    ) -> Result<Option<BindIdentity>, String> {
        // Taken before anything is read, so a write after this point gives a later pass another
        // stamp and the session is read again then.
        let Some(stamp) = backend.head_stamp(addr).map_err(described)? else {
            return Ok(None);
        };
        if self.unchanged_and_unexpired(addr, &stamp, now_ms) {
            return Ok(None);
        }
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
        let deadline = self
            .store_deadline(runtime, backend, addr, &triple, true)
            .await?;
        if self
            .store
            .deadlines
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&addr.0)
        {
            self.store
                .stamps
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(addr.0.clone(), stamp);
        }
        let Some(deadline) = deadline else {
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
            .delete_expired_store_session(runtime, backend, class, addr, &key, &triple, now_ms)
            .await;
        drop(deleting);
        drop(lease);
        Ok(deleted?.then_some(key))
    }

    /// Whether the session at `addr` is decided as kept without reading it: its head file still
    /// has the stamp it had when its deadline was confirmed, and that deadline is none or later
    /// than `now_ms`. A session whose kept deadline has passed is always read, so its deletion
    /// is decided from its records as they are now.
    fn unchanged_and_unexpired(&self, addr: &SessionAddr, stamp: &HeadStamp, now_ms: u64) -> bool {
        if self
            .store
            .stamps
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&addr.0)
            != Some(stamp)
        {
            return false;
        }
        self.store
            .deadlines
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&addr.0)
            .is_some_and(|(_, deadline)| deadline.is_none_or(|deadline| deadline > now_ms))
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
        // A writer moved the head between the probe and the read: decided on the next pass.
        let deadline =
            match Session::view_at(backend, addr.clone(), triple, deployment, &position).await {
                Ok(view) => match view.records_from(0).await {
                    Ok(records) => session_deadline(&records, &view.operations)?,
                    Err(SessionError::Moved) => return Ok(None),
                    Err(error) => return Err(self.unreadable_store_session(addr, error)),
                },
                Err(SessionError::Moved) => return Ok(None),
                Err(error) => return Err(self.unreadable_store_session(addr, error)),
            };
        let previous = self
            .store
            .deadlines
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(addr.0.clone(), (position, deadline));
        if previous != Some((position, deadline)) {
            self.store
                .changed
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(deadline)
    }

    /// Removes the mark of the session at `addr` in the root of `class`, which `backend` reads,
    /// once nothing can make it due: it is gone, or it is neither released nor kept by a
    /// starter's policy that ends. A session not readable yet keeps its mark.
    async fn unmark_settled(
        &self,
        runtime: &StoreRuntime,
        backend: &LocalBackend,
        class: StorageClass,
        addr: &SessionAddr,
    ) -> Result<(), String> {
        let root = class.store_root(runtime.state_root());
        let Some((stored, _)) = backend.read_stored_head(addr).map_err(described)? else {
            return marks::remove(&root, addr).map_err(|error| error.to_string());
        };
        let triple = head::stored_triple(&stored).map_err(described)?;
        if head::stored_cipher(&stored).map_err(described)? != Cipher::None && !runtime.key_loaded()
        {
            return Ok(());
        }
        let deployment = runtime.deployment();
        let Some(position) = Session::stored_position(backend, addr, deployment)
            .await
            .map_err(described)?
        else {
            return Ok(());
        };
        let view = Session::view_at(backend, addr.clone(), &triple, deployment, &position)
            .await
            .map_err(described)?;
        if !marks::needs_mark(class, &view.operations) {
            marks::remove(&root, addr).map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    /// Counts the session at `addr` as unreadable and says why.
    fn unreadable_store_session(&self, addr: &SessionAddr, error: SessionError) -> String {
        self.mark_unreadable_address(addr.0.clone());
        format!("the session's records could not be read: {error:?}")
    }

    /// Deletes the session `key` at `addr` when, read again now under its lease, its policy has
    /// expired at or before `now_ms`. Returns whether it was deleted. The caller holds the lease.
    #[allow(clippy::too_many_arguments)]
    async fn delete_expired_store_session(
        &self,
        runtime: &StoreRuntime,
        backend: &LocalBackend,
        class: StorageClass,
        addr: &SessionAddr,
        key: &BindIdentity,
        triple: &SessionTriple,
        now_ms: u64,
    ) -> Result<bool, String> {
        let state_root = runtime.state_root();
        // A head in two roots is damage, which is never resolved by deleting one of them.
        if broca_records::session_storage::session_storage_class(state_root, &addr.0)
            .map_err(|error| error.to_string())?
            != Some(class)
        {
            return Ok(false);
        }
        // A session frozen for a move, or moved away, waits for the move: deleting it here would
        // take the source a move still exports, or the moved head that refuses it here. Without
        // that head a later send could start the session again on this computer while it runs
        // on the other.
        if residence_blocks_deletion(runtime, key)?.is_some() {
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
        let view = Session::view_at(backend, addr.clone(), triple, deployment, &position)
            .await
            .map_err(described)?;
        let records = view
            .records_from(0)
            .await
            .map_err(|error| self.unreadable_store_session(addr, error))?;
        let Some(deadline) = session_deadline(&records, &view.operations)? else {
            return Ok(false);
        };
        if deadline > now_ms {
            return Ok(false);
        }
        // Spend not exported yet is recorded only in the session; it expires once the export
        // has it, which the session's next opening or run end retries. A runtime that exports no
        // spend never will, so there owed spend doesn't hold the session back: waiting would keep
        // a session that can't be deleted, which every later send to it would open again.
        if runtime.billing().is_some() && !view.operations.billing_owed.is_empty() {
            return Ok(false);
        }
        self.remove_read_session(
            runtime,
            backend,
            addr,
            key,
            triple,
            &view,
            &records,
            position.version,
            deadline,
        )
        .await?;
        if class != StorageClass::Ephemeral {
            marks::remove(&class.store_root(state_root), addr)
                .map_err(|error| error.to_string())?;
        }
        tracing::info!(target: "retention", session = %addr.0, expired_at_ms = deadline,
            "expired session deleted from the new storage");
        Ok(true)
    }

    /// Deletes the session `key` at `addr` for the operator, when it holds no work a deletion
    /// would lose: no run active or paused, no queued send, no spend still to export. With
    /// `paused_run`, a paused run is deleted too, unless it waits on calls a provider holds:
    /// that run is still waiting for their results, which the provider may yet deliver. It may be
    /// kept in either storage class, with or without a retention policy. Deletion is
    /// retention's: the expiry marker first (dated `now_ms`), so reads of its runs answer expired
    /// and a session created at the address later carries its counter floors, then what a move
    /// left, then the session. An actor holding the session open holds its lease, so the
    /// session is refused [`OperatorDeletion::Held`] until it closes idle.
    pub(crate) async fn delete_store_session_for_operator(
        &self,
        runtime: &StoreRuntime,
        key: &BindIdentity,
        now_ms: u64,
        paused_run: bool,
    ) -> Result<OperatorDeletion, String> {
        let state_root = runtime.state_root();
        let addr = SessionAddr(broca_core::session_addr(key));
        let Some(class) =
            broca_records::session_storage::session_storage_class(state_root, &addr.0)
                .map_err(|error| error.to_string())?
        else {
            return Ok(OperatorDeletion::Absent);
        };
        if let Some(why) = residence_blocks_deletion(runtime, key)? {
            return Ok(OperatorDeletion::Busy(why));
        }
        let Ok(lease) = self.lease(key) else {
            return Ok(OperatorDeletion::Held);
        };
        let deleting = Deleting::start(&self.store.deleting, key);
        let backend = LocalBackend::new(class.store_root(state_root));
        let triple = crate::store_open::triple(key);
        let deployment = runtime.deployment();
        let Some(position) = Session::stored_position(&backend, &addr, deployment)
            .await
            .map_err(described)?
        else {
            return Ok(OperatorDeletion::Absent);
        };
        let view = Session::view_at(&backend, addr.clone(), &triple, deployment, &position)
            .await
            .map_err(described)?;
        let operations = &view.operations;
        if let Some(run) = operations.run.as_ref().filter(|run| run.finished.is_none()) {
            let deletable = paused_run
                && run
                    .paused
                    .is_some_and(|reason| reason != broca_protocol::PauseReason::ToolHeld);
            if !deletable {
                return Ok(OperatorDeletion::Busy("a run is active or paused"));
            }
        }
        if !operations.queue.is_empty() {
            return Ok(OperatorDeletion::Busy("sends are queued"));
        }
        if !operations.billing_owed.is_empty() {
            return Ok(OperatorDeletion::Busy("a run's spend is not exported yet"));
        }
        let records = view.records_from(0).await.map_err(described)?;
        self.remove_read_session(
            runtime,
            &backend,
            &addr,
            key,
            &triple,
            &view,
            &records,
            position.version,
            now_ms,
        )
        .await?;
        // The session left no work, but the set of sessions to recover at startup may still name
        // it; a startup would find no session there.
        crate::store_open::recovery_index::remove(&class.store_root(state_root), &addr)
            .map_err(|error| error.to_string())?;
        drop(deleting);
        drop(lease);
        tracing::info!(target: "retention", session = %addr.0, "session deleted for the operator");
        Ok(OperatorDeletion::Deleted)
    }

    /// Deletes the session `key` at `addr`, read as `view` and `records` at head version
    /// `version`, under its lease: the expiry marker dated `deleted_at_ms`, then any artifacts
    /// left by a move, then the session, then what the module caches of it.
    #[allow(clippy::too_many_arguments)]
    async fn remove_read_session(
        &self,
        runtime: &StoreRuntime,
        backend: &LocalBackend,
        addr: &SessionAddr,
        key: &BindIdentity,
        triple: &SessionTriple,
        view: &broca_session_store::session::SessionView<&LocalBackend>,
        records: &[(u64, Record)],
        version: broca_session_store::backend::Version,
        deleted_at_ms: u64,
    ) -> Result<(), String> {
        let state_root = runtime.state_root();
        let deployment = runtime.deployment();
        // While the ledger still names them, so each withdrawal can say whose call it is.
        if let Some(withdraw) = self.withdraw.get() {
            let class = runtime
                .class_of(key)
                .map_err(|error| error.to_string())?
                .ok_or("the deleted session has no storage class")?;
            let calls = held::held_at_deletion(runtime, class, view.head().cipher(), records);
            if !calls.is_empty() {
                withdraw.withdraw_before_delete(calls).await;
            }
        }
        let next_tag = counters::next_tag(records)?.unwrap_or(1);
        let next_notice_seq = records
            .iter()
            .filter_map(|(_, record)| match record {
                Record::Compaction(record) => {
                    serde_json::from_value::<StoredNotice>(record.body.clone().parse()).ok()
                }
                _ => None,
            })
            .filter(|notice| notice.kind == "compaction_notice")
            .filter_map(|notice| notice.seq?.checked_add(1))
            .max()
            .unwrap_or(1);
        broca_session_store::expired::write(
            &StorageClass::Ephemeral.store_root(state_root),
            addr,
            &view.head().session_id,
            broca_session_store::expired::Marker {
                format: 1,
                triple: triple.clone(),
                expired_at_ms: deleted_at_ms,
                seq: 0,
                next_tag,
                next_notice_seq,
                runs: records
                    .iter()
                    .filter_map(|(_, record)| match record {
                        Record::RunStart(start) => Some(start.run_id.clone()),
                        _ => None,
                    })
                    .collect(),
                history_version: Some(view.head().history_version),
                // A session kept by a starter's policy, or released, reads as never there once
                // deleted (llm-runner `session_retention`), not as expired.
                policy_deleted: (view.operations.retention_policy.is_some()
                    || view.operations.released_at_ms.is_some())
                .then_some(true),
            },
            view.head().cipher(),
            deployment,
        )
        .map_err(described)?;
        // The cached move stream copies the session's content, so it goes before the session: a
        // crash between the two leaves the session to be deleted again, never the copy behind.
        broca_records::move_fence::unlink(
            &state_root.join("session-store-moves/streams").join(&addr.0),
        )
        .map_err(|error| error.to_string())?;
        Session::delete(backend, addr, version)
            .await
            .map_err(described)?;
        self.store
            .deadlines
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&addr.0);
        self.unreadable
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&addr.0);
        // Still under the lease, so no session created at the address afterwards loses its routes.
        if let Some(evict) = self.evict.get() {
            evict(key);
        }
        crate::diagnostics::retention_deleted(key);
        Ok(())
    }
}

/// The part of a stored compaction record that numbers notices: records written by the earlier
/// compaction protocol keep its advance notices as `{"type": "compaction_notice", "seq": N, ...}`,
/// and a session deleted with them records the next free number in its expiry marker
/// (`broca_session_store::expired::Marker`), so a session recreated at the address numbers on.
#[derive(serde::Deserialize)]
struct StoredNotice {
    #[serde(rename = "type")]
    kind: String,
    seq: Option<u64>,
}

#[cfg(test)]
#[path = "retention_store_tests.rs"]
mod tests;

/// Why the session `key` may not be deleted because of where it lives: it is frozen for a move
/// (a move between requests holds no lease, so the lease alone doesn't stop a deletion), or it
/// has moved to another computer and only its moved head is left here, refusing it so it can't
/// start again on both computers. `None` when it lives here.
fn residence_blocks_deletion(
    runtime: &StoreRuntime,
    key: &BindIdentity,
) -> Result<Option<&'static str>, String> {
    match runtime.check_move(key, true) {
        Ok(()) => Ok(None),
        Err(crate::registry::OpenError::SessionMoving { .. }) => {
            Ok(Some("the session is being moved to another computer"))
        }
        Err(crate::registry::OpenError::SessionMoved { .. }) => {
            Ok(Some("the session moved to another computer"))
        }
        Err(error) => Err(format!("{error:?}")),
    }
}
