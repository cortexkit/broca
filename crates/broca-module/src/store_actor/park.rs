use std::time::Duration;

use broca_session_store::backend::SessionBackend;

use super::{Exit, StoreActor};
use crate::store_fanout::ParkedSubscription;

pub(super) const PARK_AFTER: Duration = Duration::from_secs(5 * 60);

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// Attach the waiting subscribers to the live ring before recovery or the first command
    /// can write or announce an admission change. No records were added while closed because
    /// only the actor writes them.
    pub(crate) fn with_parked(mut self, parked: Vec<ParkedSubscription>) -> Self {
        if let Some(fanout) = &mut self.fanout {
            for subscription in parked {
                fanout.attach(&self.session, subscription.from, subscription.sink);
            }
        }
        self
    }

    pub(super) async fn at_idle_deadline(&mut self, idle_for: Duration) -> Option<Exit> {
        match self.idle_holds().as_slice() {
            [] => Some(Exit::Idle),
            ["live_subscriber"] if idle_for >= PARK_AFTER && self.registry.is_some() => {
                let parked = self.fanout.as_mut()?.try_park().await?;
                Some(if parked.is_empty() {
                    Exit::Idle
                } else {
                    Exit::Parked(parked)
                })
            }
            _ => None,
        }
    }
}

impl Exit {
    pub(super) fn close_reason(&self) -> &'static str {
        match self {
            Self::Cut(_) => "cut",
            Self::Closed => "channel_closed",
            Self::Idle => "idle",
            Self::Parked(_) => "parked",
            Self::Stop => "shutdown",
            Self::Expired(_) => "expired",
        }
    }
}
