use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, MutexGuard};

use broca_wire::ParkedSubscriberCounts;

use super::{LineageKey, LineageRegistry, RegistryHandle};
use crate::fanout::Delivery;
use crate::store_fanout::ParkedSubscription;

#[derive(Default)]
pub(super) struct Parked(Mutex<HashMap<LineageKey, Vec<ParkedSubscription>>>);

impl Parked {
    fn live(&self) -> MutexGuard<'_, HashMap<LineageKey, Vec<ParkedSubscription>>> {
        let mut parked = self.0.lock().unwrap();
        parked.retain(|_, subscriptions| {
            subscriptions.retain(|sub| !sub.sink.is_closed());
            !subscriptions.is_empty()
        });
        parked
    }
}

impl RegistryHandle {
    pub(crate) fn park_subscribers(
        &self,
        key: &LineageKey,
        mut subscriptions: Vec<ParkedSubscription>,
    ) {
        let mut parked = self.inner.parked_subscribers.live();
        subscriptions.retain(|sub| !sub.sink.is_closed());
        // The final shutdown seal drops parked sinks under the same lock. An actor already
        // closing at that point must not leave a new stream behind afterwards.
        if !subscriptions.is_empty() && !self.inner.seal.is_cancelled() {
            parked.entry(key.clone()).or_default().extend(subscriptions);
        }
    }

    pub(crate) fn take_parked_subscribers(&self, key: &LineageKey) -> Vec<ParkedSubscription> {
        self.inner
            .parked_subscribers
            .live()
            .remove(key)
            .unwrap_or_default()
    }

    pub(crate) fn note_actor_close(&self, reason: &'static str) {
        *self
            .inner
            .actor_close_reasons
            .lock()
            .unwrap()
            .entry(reason)
            .or_default() += 1;
    }
}

impl LineageRegistry {
    pub(crate) fn parked_subscriber_counts(&self) -> ParkedSubscriberCounts {
        let parked = self.inner.parked_subscribers.live();
        ParkedSubscriberCounts {
            sessions: parked.len() as u64,
            subscriptions: parked
                .values()
                .map(|subscribers| subscribers.len() as u64)
                .sum(),
        }
    }

    pub(crate) fn actor_close_reasons(&self) -> BTreeMap<String, u64> {
        self.inner
            .actor_close_reasons
            .lock()
            .unwrap()
            .iter()
            .map(|(&reason, &count)| (reason.to_owned(), count))
            .collect()
    }

    pub(super) fn clear_parked_subscribers(&self) {
        self.inner.parked_subscribers.0.lock().unwrap().clear();
    }

    /// A source move ends the stream as retiring an attached actor would: by closing the sink.
    pub(crate) fn end_parked_subscribers(&self, key: &LineageKey) {
        self.handle().take_parked_subscribers(key);
    }

    pub(super) fn expire_parked_subscribers(&self, key: &LineageKey, expired_at_ms: u64) {
        for sub in self.handle().take_parked_subscribers(key) {
            // A full sink must not make deletion wait under the session lease.
            tokio::spawn(async move {
                let _ = sub.sink.send(Delivery::Expired { expired_at_ms }).await;
            });
        }
    }

    /// Restore refuses a watched session whether its live subscriber is attached or parked:
    /// restoring an earlier head could invalidate a cursor already delivered on that stream.
    pub fn restore_subscriber_busy(&self, key: &LineageKey) -> Option<&'static str> {
        (self.idle_holds(key).contains(&"live_subscriber")
            || self.inner.parked_subscribers.live().contains_key(key))
        .then_some(
            "the session has live subscribers, which keep its actor open; stop the \
                    subscriber or restart broca first",
        )
    }
}
