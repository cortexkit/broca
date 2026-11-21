//! Session-owned memory shared by run boundaries and pushed-view admission.

use super::{runtime::Runtime, state::State};

#[async_trait::async_trait]
pub trait Changes: Send + Sync {
    async fn changed(&self) -> Result<(), String>;
}

#[derive(Default)]
pub struct Session {
    pub state: tokio::sync::Mutex<State>,
    pub runtime: Runtime,
    initialized: std::sync::atomic::AtomicBool,
    serving_started: std::sync::atomic::AtomicBool,
    /// The last cut this session's history was sealed at, and the ordinal the window started at
    /// after that seal (see `broca_session_loop::compaction::cut_to_seal`).
    sealed_at: std::sync::Mutex<Option<(u64, u64)>>,
}

impl Session {
    /// Whether sealing at the cut `target` with the window starting at `start` is worth asking
    /// the store for: not when this session last sealed at that cut and the window started there
    /// afterwards, since the store would find nothing more to move.
    pub fn seal_due(&self, target: u64, start: u64) -> bool {
        *self.sealed_at.lock().expect("sealed_at") != Some((target, start))
    }

    /// Records a seal at the cut `target` that left the window starting at `start`.
    pub fn sealed_at(&self, target: u64, start: u64) {
        *self.sealed_at.lock().expect("sealed_at") = Some((target, start));
    }

    pub fn claim_serving_start(&self) -> bool {
        !self
            .serving_started
            .swap(true, std::sync::atomic::Ordering::AcqRel)
    }
    pub fn is_initialized(&self) -> bool {
        self.initialized.load(std::sync::atomic::Ordering::Acquire)
    }
    /// The actor reloads at its serialized admission point, before spawning a
    /// run. Never replace this state from a stale snapshot while that run is active.
    pub async fn restore(&self, state: State) {
        self.runtime.restore_marker(state.marker_version());
        *self.state.lock().await = state;
        self.initialized
            .store(true, std::sync::atomic::Ordering::Release);
    }
}
