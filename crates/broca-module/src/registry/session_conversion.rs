//! Nonblocking admission for background conversion of closed, idle legacy sessions.

use super::{LineageKey, LineageRegistry};
use broca_core::LeaseStore;

/// The admission mark outlives the move fence, including when acquisition is cancelled.
pub struct IdleConversionFence {
    fence: Option<super::store_move::StoreMoveFence>,
    registry: LineageRegistry,
    address: String,
}

impl Drop for IdleConversionFence {
    fn drop(&mut self) {
        self.fence.take();
        self.registry
            .inner
            .background_conversions
            .lock()
            .unwrap()
            .remove(&self.address);
    }
}

impl LineageRegistry {
    pub(super) fn background_conversion_held(&self, key: &LineageKey) -> bool {
        self.inner
            .background_conversions
            .lock()
            .unwrap()
            .contains(&broca_core::session_addr(key))
    }

    /// Skip actors and contended gates rather than waiting for them to become idle. The
    /// durable-work check and admission mark share the actor-opening critical section.
    pub async fn try_fence_idle_conversion(
        &self,
        key: &LineageKey,
        leases: &dyn LeaseStore,
    ) -> Result<Option<IdleConversionFence>, String> {
        let Ok(gate) = self.cut_gate(key).try_lock_owned() else {
            return Ok(None);
        };
        if self.inner.stopping.is_cancelled() || self.inner.slots.lock().unwrap().contains_key(key)
        {
            return Ok(None);
        }
        let reader = self
            .inner
            .reader
            .as_ref()
            .ok_or("conversion requires a WAL reader")?;
        let wal = std::sync::Arc::clone(&reader.wal_store);
        let session = key.clone();
        let range = super::spawn_blocking(super::JobKind::WalRead, move || {
            wal.read_records_range(&session, 0, None, None)
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
        if crate::move_files::source_busy_reason(&range)
            .map_err(|error| error.message)?
            .is_some()
        {
            return Ok(None);
        }
        if self.inner.stopping.is_cancelled() {
            return Ok(None);
        }
        let address = broca_core::session_addr(key);
        if !self
            .inner
            .background_conversions
            .lock()
            .unwrap()
            .insert(address.clone())
        {
            return Ok(None);
        }
        let mut held = IdleConversionFence {
            fence: None,
            registry: self.clone(),
            address,
        };
        drop(gate);
        held.fence = match self.fence_store_move(key, leases).await {
            Ok(fence) => Some(fence),
            Err(super::store_move::FenceRefusal::Busy(_)) => return Ok(None),
            Err(error) => return Err(format!("{error:?}")),
        };
        Ok(Some(held))
    }
}
