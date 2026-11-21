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

impl MoveFences {
    /// How today's storage refuses `key` while a move in this process holds it, or startup
    /// couldn't settle its move: as moving, which a caller may retry.
    pub(crate) fn refusal(&self, key: &LineageKey) -> Option<super::OpenError> {
        let address = broca_core::session_addr(key);
        let held = self.fenced.lock().unwrap().contains(&address)
            || self.unsettled.lock().unwrap().contains(&address);
        held.then(|| super::OpenError::SessionMoving {
            cut_id: "store-move".into(),
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

impl StoreMoveFence {}

impl LineageRegistry {
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
}
