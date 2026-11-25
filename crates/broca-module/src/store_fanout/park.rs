use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use broca_session_store::backend::SessionBackend;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{Live, LiveItem, StoreFanout};
use crate::fanout::{deliver, AttachFrom, ControlEvent, Delivery, FromCursor, RING_CAPACITY};

pub(crate) struct ParkedSubscription {
    pub(crate) sink: mpsc::Sender<Delivery>,
    pub(crate) from: AttachFrom,
}

pub(super) struct Subscriber {
    pub(super) cancel: CancellationToken,
    pub(super) task: JoinHandle<Option<ParkedSubscription>>,
    // Expiry upgrades the weak sender before cancelling. A strong sender here would keep a
    // finished stream open after its task ended.
    pub(super) terminal: mpsc::WeakSender<Delivery>,
    pub(super) progress: Arc<Progress>,
}

/// A position in the ring counts all its items, not just the durable controls. Parking cannot
/// overtake an access change or a detach, which aren't replayed from the log.
pub(super) struct LiveRing {
    sender: broadcast::Sender<LiveItem>,
    position: u64,
}

impl LiveRing {
    pub(super) fn new() -> Self {
        Self {
            sender: broadcast::channel(RING_CAPACITY).0,
            position: 1,
        }
    }

    pub(super) fn send(&mut self, item: LiveItem) {
        if self.sender.send(item).is_ok() {
            self.position += 1;
        }
    }

    pub(super) fn subscribe(&self) -> broadcast::Receiver<LiveItem> {
        self.sender.subscribe()
    }

    pub(super) fn position(&self) -> u64 {
        self.position
    }

    pub(super) fn len(&self) -> usize {
        self.sender.len()
    }
}

#[derive(Default)]
pub(super) struct Progress {
    // Zero until the opening replay and display snapshot finish. After that the position
    // advances only when a ring item has been handled, not merely read from the ring.
    delivered: AtomicU64,
    parking: AtomicBool,
}

impl Progress {
    pub(super) fn reached(&self, position: u64) {
        self.delivered.store(position, Ordering::Release);
    }

    pub(super) fn parking(&self) -> bool {
        self.parking.load(Ordering::Acquire)
    }
}

impl<B: SessionBackend + Send + Sync + 'static> StoreFanout<B> {
    /// Stops all subscribers once their opening replay and reliable deliveries are done. Don't
    /// wait for a blocked sink to catch up: the next idle deadline can try again. Display-only
    /// deliveries waiting on a sink can be cancelled; that lane is already lossy.
    pub(crate) async fn try_park(&mut self) -> Option<Vec<ParkedSubscription>> {
        self.has_live_subscribers();
        {
            let publisher = self.publisher.lock().unwrap();
            let end = publisher.ring.position();
            if !matches!(publisher.live, Live::Seeded(_))
                || self
                    .subscribers
                    .iter()
                    .any(|sub| sub.progress.delivered.load(Ordering::Acquire) != end)
            {
                return None;
            }
            // Keep publication locked until all tasks have been told to stop. Only the actor
            // can publish more after seeding, and it is about to close without another command.
            for sub in &self.subscribers {
                sub.progress.parking.store(true, Ordering::Release);
                sub.cancel.cancel();
            }
        }
        let mut parked = Vec::new();
        for sub in self.subscribers.drain(..) {
            if let Ok(Some(subscription)) = sub.task.await {
                parked.push(subscription);
            }
        }
        Some(parked)
    }
}

pub(super) async fn deliver_control(
    sink: &mpsc::Sender<Delivery>,
    cancel: &CancellationToken,
    event: Arc<ControlEvent>,
    resume: &mut AttachFrom,
) -> ControlFlow<()> {
    let cursor = event.0;
    deliver(sink, cancel, Delivery::Control(event)).await?;
    *resume = AttachFrom::Cursor(FromCursor::After(cursor));
    ControlFlow::Continue(())
}
