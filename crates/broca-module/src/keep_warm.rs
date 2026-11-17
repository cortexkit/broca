//! The slot a session's prompt-cache refreshes share. Manual and automatic refreshes acquire
//! the same permit, so their provider requests cannot overlap.

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

type ObservedHeaders = (u64, BTreeMap<String, String>);

#[derive(Clone, Default)]
pub(crate) struct WarmSlot {
    occupied: Arc<AtomicBool>,
    headers: Arc<Mutex<Option<ObservedHeaders>>>,
}
pub(crate) struct WarmPermit(WarmSlot);
impl Drop for WarmPermit {
    fn drop(&mut self) {
        self.0.occupied.store(false, Ordering::SeqCst);
    }
}
impl WarmSlot {
    /// A slow response from an older send must not overwrite newer account usage evidence.
    pub(crate) fn note_headers(&self, sent_at_ms: u64, values: BTreeMap<String, String>) {
        let mut headers = self.headers.lock().unwrap();
        if headers
            .as_ref()
            .is_none_or(|(current, _)| *current <= sent_at_ms)
        {
            *headers = Some((sent_at_ms, values));
        }
    }
    pub(crate) fn latest_headers(&self) -> BTreeMap<String, String> {
        self.headers
            .lock()
            .unwrap()
            .as_ref()
            .map(|(_, values)| values.clone())
            .unwrap_or_default()
    }
    pub(crate) fn acquire(&self) -> Option<WarmPermit> {
        self.occupied
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .ok()
            .map(|_| WarmPermit(self.clone()))
    }
}
