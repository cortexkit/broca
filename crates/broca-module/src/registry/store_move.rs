//! What a move of a live session from today's storage into the new storage needs from the
//! registry: a fence that keeps the session idle while the move copies it, and the cache
//! eviction after the copy is published. The move itself runs in the daemon (`store_move` in
//! `broca-module-serve`), which installs it as this registry's [`StoreMover`].
//!
//! The fence, in order:
//! - **Stop new work.** The session is marked fenced, so a late result's delivery refuses
//!   (`resume_held_run`) and is retried on a later pull. The session's cut gate is taken, so no
//!   admission opens an actor for it.
//! - **Drain.** An open actor closes: it tears down its subscribers and replay tasks, closes and
//!   joins its WAL writer, and hands its still-held session lease to the fence. A busy actor
//!   refuses instead, by its reason. Lease-free WAL reads already in flight are waited out.
//! - **Lease.** With no actor, the fence takes the session lease itself.
//!
//! Dropping the fence releases the lease, then the gate, then the mark.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::{CutLockMode, LineageKey, LineageRegistry, MoveSessionGuard};

/// What runs `session.move_to_store` for the registry's sessions.
#[async_trait::async_trait]
pub trait StoreMover: Send + Sync {
    /// Moves the session `key` into the new storage: into the root of the class its WAL's
    /// folder has, or of `class` when the operator asked for one.
    async fn move_to_store(
        &self,
        key: &LineageKey,
        class: Option<broca_wal::StorageClass>,
    ) -> Result<serde_json::Value, MoveRefusal>;
}

/// A move refused or aborted: its error code, message and detail, as the client receives them.
#[derive(Debug, Clone, PartialEq)]
pub struct MoveRefusal {
    pub code: String,
    pub message: String,
    pub detail: Option<serde_json::Value>,
}

impl MoveRefusal {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_owned(),
            message: message.into(),
            detail: None,
        }
    }

    pub fn with_detail(mut self, detail: serde_json::Value) -> Self {
        self.detail = Some(detail);
        self
    }
}

/// The sessions moves hold, and the lease-free reads in flight for each session.
#[derive(Default)]
pub(crate) struct MoveFences {
    fenced: Mutex<HashSet<String>>,
    /// Sessions whose move startup couldn't settle (an unreadable journal, head or marker):
    /// nothing serves them from today's storage until an operator looks.
    unsettled: Mutex<HashSet<String>>,
    reads: Arc<Mutex<HashMap<String, usize>>>,
    read_ended: Arc<tokio::sync::Notify>,
}

/// One lease-free read counted against its session while it runs.
pub(crate) struct InFlightRead {
    address: String,
    reads: Arc<Mutex<HashMap<String, usize>>>,
    read_ended: Arc<tokio::sync::Notify>,
}

impl Drop for InFlightRead {
    fn drop(&mut self) {
        let mut reads = self.reads.lock().unwrap();
        if let Some(count) = reads.get_mut(&self.address) {
            *count -= 1;
            if *count == 0 {
                reads.remove(&self.address);
            }
        }
        drop(reads);
        self.read_ended.notify_waiters();
    }
}

impl MoveFences {
    /// Counts a lease-free read of `key` until the returned guard drops.
    pub(crate) fn read(&self, key: &LineageKey) -> InFlightRead {
        let address = broca_core::session_addr(key);
        *self
            .reads
            .lock()
            .unwrap()
            .entry(address.clone())
            .or_default() += 1;
        InFlightRead {
            address,
            reads: Arc::clone(&self.reads),
            read_ended: Arc::clone(&self.read_ended),
        }
    }

    /// How today's storage refuses `key` while a move in this process holds it, or startup
    /// couldn't settle its move: as moving, which a caller may retry.
    pub(crate) fn refusal(&self, key: &LineageKey) -> Option<super::OpenError> {
        let address = broca_core::session_addr(key);
        let held = self.fenced.lock().unwrap().contains(&address)
            || self.unsettled.lock().unwrap().contains(&address);
        held.then(|| {
            let error = broca_wal::WalError::SessionMoving {
                cut_id: "store-move".into(),
            };
            super::OpenError::from_wal_read(error.to_string(), &error)
        })
    }

    /// Whether a move holds `key`.
    pub(crate) fn fenced(&self, key: &LineageKey) -> bool {
        self.fenced
            .lock()
            .unwrap()
            .contains(&broca_core::session_addr(key))
    }

    /// Waits until no lease-free read of `address` is in flight, at most `limit`. False when
    /// one still is.
    async fn reads_drained(&self, address: &str, limit: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + limit;
        loop {
            let ended = self.read_ended.notified();
            if !self.reads.lock().unwrap().contains_key(address) {
                return true;
            }
            if tokio::time::timeout_at(deadline, ended).await.is_err() {
                return !self.reads.lock().unwrap().contains_key(address);
            }
        }
    }
}

/// How long a move waits for the session's lease-free reads in flight to end. A read is a
/// bounded file read; one that takes longer than this makes the move refuse as busy.
const READ_DRAIN: Duration = Duration::from_secs(10);

/// Why a session couldn't be fenced for a move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceRefusal {
    /// The session has work the move must not cut: the actor's reason (`run_active`,
    /// `send_queued`, `cache_warm`, `approval_open`, `unknown`), `lease_held` when another
    /// owner holds its lease, or `reads_in_flight`.
    Busy(String),
    /// The fence couldn't be taken.
    Unavailable(String),
}

/// A session held for a move; see the module's description. Dropping it ends the hold.
pub struct StoreMoveFence {
    // Field order is drop order: the lease the fence took itself, then the cut guard (which
    // releases a closed actor's lease and commits its slot closed before its gate opens), then
    // the mark.
    lease: Option<Box<dyn broca_core::LeaseHandle>>,
    guard: Option<MoveSessionGuard>,
    _mark: FenceMark,
}

struct FenceMark {
    registry: LineageRegistry,
    address: String,
}

impl Drop for FenceMark {
    fn drop(&mut self) {
        self.registry
            .inner
            .store_moves
            .fenced
            .lock()
            .unwrap()
            .remove(&self.address);
    }
}

impl StoreMoveFence {
    /// Whether the fence holds the session lease, taken itself or handed over by the actor it
    /// closed.
    pub fn holds_lease(&self) -> bool {
        self.lease.is_some() || self.guard.as_ref().is_some_and(|g| g.handed_tenures() > 0)
    }
}

impl LineageRegistry {
    /// Refuses the sessions at `addresses` (startup's unsettled moves) until the process
    /// restarts: their journals stay for an operator to look at.
    pub fn refuse_unsettled_moves(&self, addresses: impl IntoIterator<Item = String>) {
        self.inner
            .store_moves
            .unsettled
            .lock()
            .unwrap()
            .extend(addresses);
    }

    /// Installs what runs `session.move_to_store`. Only the first installation counts.
    pub fn set_store_mover(&self, mover: Arc<dyn StoreMover>) {
        let _ = self.inner.store_mover.set(mover);
    }

    /// Runs `session.move_to_store` for `key`, into the root of `class` when the operator asked
    /// for one, or refuses `store_unavailable` when this module keeps no sessions in the new
    /// storage.
    pub async fn move_to_store(
        &self,
        key: &LineageKey,
        class: Option<broca_wal::StorageClass>,
    ) -> Result<serde_json::Value, MoveRefusal> {
        match self.inner.store_mover.get() {
            Some(mover) => mover.move_to_store(key, class).await,
            None => Err(MoveRefusal::new(
                "store_unavailable",
                "this module keeps no sessions in the new storage",
            )),
        }
    }

    /// Fences `key` for a move; see the module's description. `leases` is the store the
    /// session's lease is taken from when no actor hands one over.
    pub async fn fence_store_move(
        &self,
        key: &LineageKey,
        leases: &dyn broca_core::LeaseStore,
    ) -> Result<StoreMoveFence, FenceRefusal> {
        let address = broca_core::session_addr(key);
        if !self
            .inner
            .store_moves
            .fenced
            .lock()
            .unwrap()
            .insert(address.clone())
        {
            return Err(FenceRefusal::Busy("session_moving".into()));
        }
        let mut fence = StoreMoveFence {
            lease: None,
            guard: None,
            _mark: FenceMark {
                registry: self.clone(),
                address: address.clone(),
            },
        };
        let guard = self
            .lock_move_sessions_with_mode(std::slice::from_ref(key), false, CutLockMode::Retire)
            .await
            .map_err(|error| FenceRefusal::Busy(format!("unknown: {error}")))?;
        let busy = guard.busy.iter().find_map(|(_, reason)| reason.clone());
        let handed = guard.holds_tenure(key);
        fence.guard = Some(guard);
        if let Some(reason) = busy {
            return Err(FenceRefusal::Busy(reason));
        }
        match self.slot_state(key) {
            // The actor closed and handed its lease to the guard; its slot stays `Closing`
            // until the guard drops.
            Some(_) if handed => {}
            Some(state) => return Err(FenceRefusal::Busy(format!("unknown: actor {state}"))),
            None => match leases.acquire(&broca_core::lease_key_for(key)) {
                Ok(lease) => fence.lease = Some(lease),
                Err(broca_core::LeaseError::Held { .. }) => {
                    return Err(FenceRefusal::Busy("lease_held".into()))
                }
                Err(error) => return Err(FenceRefusal::Unavailable(error.to_string())),
            },
        }
        if !self
            .inner
            .store_moves
            .reads_drained(&address, READ_DRAIN)
            .await
        {
            return Err(FenceRefusal::Busy("reads_in_flight".into()));
        }
        Ok(fence)
    }

    /// Forgets what the registry keeps of `key`'s copy in today's storage, once the new storage
    /// owns the session: its read proofs, run-state and dispatch facts, sender, ownership and
    /// notice caches, and its idle transport.
    pub fn forget_moved_session(&self, key: &LineageKey) {
        self.inner.opener.retire_session_transport(key);
        self.inner.read_only_lineages.lock().unwrap().remove(key);
        self.invalidate_history_caches_for_move(key);
        self.inner.cache.remove(key);
    }

    fn invalidate_history_caches_for_move(&self, key: &LineageKey) {
        self.inner.read_proofs.lock().unwrap().remove(key);
        self.inner.dispatch_cache.lock().unwrap().remove(key);
        self.inner.frozen_senders.lock().unwrap().remove(key);
        self.inner.mode3_logs.lock().unwrap().remove(key);
        self.inner.attribution_notices.lock().unwrap().remove(key);
        self.inner
            .unbound_cursor_notices
            .lock()
            .unwrap()
            .remove(key);
        self.inner.live_ownership.lock().unwrap().remove(key);
        self.inner.owner_snapshots.lock().unwrap().remove(key);
    }
}
