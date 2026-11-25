//! What a restore from backup needs from the registry to hold a session while it rewrites the
//! session's files, and the refusal that store operations (restore, import staging, publish)
//! return to the client.
//!
//! The fence, in order:
//! - **Stop new work.** The session is marked fenced, so a late result's delivery refuses
//!   (`resume_held_run`) and is retried on a later pull. The session's cut gate is taken, so no
//!   admission opens an actor for it.
//! - **Drain.** An open actor closes: it tears down its subscribers and replay tasks, closes and
//!   joins its writer, and hands its still-held session lease to the fence. A busy actor
//!   refuses instead, by its reason.
//! - **Lease.** With no actor, the fence takes the session lease itself.
//!
//! Dropping the fence releases the lease, then the gate, then the mark.

use std::collections::HashSet;
use std::sync::Mutex;

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

/// The sessions restores hold.
#[derive(Default)]
pub(crate) struct RestoreFences {
    fenced: Mutex<HashSet<String>>,
}

impl RestoreFences {
    /// How `key` is refused while a restore in this process holds it: as moving, which a caller
    /// may retry. The refusal names `store-move` as its cut id.
    pub(crate) fn refusal(&self, key: &LineageKey) -> Option<super::OpenError> {
        self.fenced(key).then(|| super::OpenError::SessionMoving {
            cut_id: "store-move".into(),
        })
    }

    /// Whether a restore holds `key`.
    pub(crate) fn fenced(&self, key: &LineageKey) -> bool {
        self.fenced
            .lock()
            .unwrap()
            .contains(&broca_core::session_addr(key))
    }
}

/// Why a session couldn't be fenced for a restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceRefusal {
    /// The session has work the restore must not cut: the actor's reason (`run_active`,
    /// `send_queued`, `cache_warm`, `approval_open`, `unknown`), or `lease_held` when another
    /// owner holds its lease.
    Busy(String),
    /// The fence couldn't be taken.
    Unavailable(String),
}

/// A session held for a restore; see the module's description. Dropping it ends the hold.
pub struct RestoreFence {
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
            .restore_fences
            .fenced
            .lock()
            .unwrap()
            .remove(&self.address);
    }
}

impl LineageRegistry {
    /// Fences `key` for a restore; see the module's description. `leases` is the store the
    /// session's lease is taken from when no actor hands one over.
    pub async fn fence_for_restore(
        &self,
        key: &LineageKey,
        leases: &dyn broca_core::LeaseStore,
    ) -> Result<RestoreFence, FenceRefusal> {
        let address = broca_core::session_addr(key);
        if !self
            .inner
            .restore_fences
            .fenced
            .lock()
            .unwrap()
            .insert(address.clone())
        {
            return Err(FenceRefusal::Busy("session_moving".into()));
        }
        let mut fence = RestoreFence {
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
        if let Some(reason) = self.restore_subscriber_busy(key) {
            return Err(FenceRefusal::Busy(reason.into()));
        }
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
        Ok(fence)
    }
}
