//! Routes a `route.open` committed after its waiter stopped waiting: their reply has no taker,
//! so the connection closes them rather than leave the daemon's route with no owner.

use std::sync::Arc;

use subc_control::ClientControlResponse;
use subc_protocol::{Frame, FrameType};
use tokio::sync::mpsc;

#[cfg(test)]
use super::LateGoodbyeHandoffStage;
use super::{
    prepare_goodbye, route_closer_for, ConnToken, ConnectionShared, ReaderGeneration, RouteHandle,
};

/// The receiver of one `route.open` attempt's reply. The reader hands the reply to the waiting
/// attempt and forgets the open, so an attempt dropped after its reply was queued but before it
/// was taken would orphan the route the reply names. Dropped while still holding such a reply,
/// this closes that route. [`Self::taken`] hands the receiver back once the attempt has its
/// answer.
pub(super) struct OpenReply<'a> {
    rx: Option<mpsc::UnboundedReceiver<Frame>>,
    shared: &'a Arc<ConnectionShared>,
    conn: ConnToken,
}

impl<'a> OpenReply<'a> {
    pub(super) fn new(
        rx: mpsc::UnboundedReceiver<Frame>,
        shared: &'a Arc<ConnectionShared>,
        conn: ConnToken,
    ) -> Self {
        Self {
            rx: Some(rx),
            shared,
            conn,
        }
    }

    pub(super) fn receiver(&mut self) -> &mut mpsc::UnboundedReceiver<Frame> {
        self.rx.as_mut().expect("the receiver is taken only once")
    }

    pub(super) fn taken(mut self) -> mpsc::UnboundedReceiver<Frame> {
        self.rx.take().expect("the receiver is taken only once")
    }
}

impl Drop for OpenReply<'_> {
    fn drop(&mut self) {
        let Some(rx) = &mut self.rx else {
            return;
        };
        while let Ok(frame) = rx.try_recv() {
            if frame.header.ty != FrameType::Response {
                continue;
            }
            if let Ok(ClientControlResponse::RouteOpen {
                route_channel,
                route_epoch,
                ..
            }) = serde_json::from_slice::<ClientControlResponse>(&frame.body)
            {
                let route = RouteHandle::new(route_channel, route_epoch, self.conn);
                if prepare_goodbye(self.shared, route).is_ok() {
                    route_closer_for(self.shared).enqueue_failed(route);
                }
            }
        }
    }
}

pub(super) fn retry_late_goodbye(generation: Option<&ReaderGeneration>, frame: Frame) {
    let Some(binding) = generation else {
        return;
    };
    let Some(shared) = binding.shared.upgrade() else {
        return;
    };
    let route = RouteHandle::new(frame.header.channel, frame.header.epoch, binding.conn);
    // Publish a non-dispatchable closing identity before the reader drops its live epoch.
    // No await and no egress wait occurs on ingress.
    if prepare_goodbye(&shared, route).is_ok() {
        let closer = route_closer_for(&shared);
        #[cfg(test)]
        let hook = shared.late_goodbye_handoff.lock().unwrap().take();
        #[cfg(test)]
        if let Some(hook) = &hook {
            hook(LateGoodbyeHandoffStage::Prepared);
        }
        let _admitted = closer.enqueue_failed(route);
        #[cfg(test)]
        if let Some(hook) = hook {
            hook(LateGoodbyeHandoffStage::Admitted(_admitted));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    /// A caller that stops waiting on a `route.open` just after its reply was queued for it,
    /// before it took the reply, still has the opened route closed: the reader already forgot
    /// the open, so nobody else would.
    #[tokio::test]
    async fn an_open_dropped_after_its_reply_was_queued_closes_the_route() {
        let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
        let bind = BindIdentity::new("/project", "broca", "worker");
        let mut opening = Box::pin(conn.route_open_dispatch_single_attempt(
            "aft",
            &bind,
            None,
            RouteShape {
                tool_provider_v1: true,
                ..Default::default()
            },
            false,
            None,
        ));
        let request = tokio::select! {
            biased;
            _ = &mut opening => panic!("the open finished without a reply"),
            request = daemon.next_request() => request,
        };
        // As the real reader does for an opened route: publish its epoch, then queue the reply.
        conn.shared
            .current
            .lock()
            .unwrap()
            .runtime
            .route_epochs
            .lock()
            .unwrap()
            .insert(7, 1);
        assert!(daemon.reply(
            &request,
            serde_json::to_vec(&ClientControlResponse::RouteOpen {
                route_channel: 7,
                route_epoch: 1,
                request_window: None,
                watch_window: None,
            })
            .unwrap()
        ));
        drop(opening);
        let goodbye = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let frame = daemon.write_rx.recv().await.expect("the writer stays open");
                if frame.header.ty == FrameType::Goodbye {
                    return frame;
                }
            }
        })
        .await
        .expect("the opened route is closed");
        assert_eq!((goodbye.header.channel, goodbye.header.epoch), (7, 1));
    }
}
