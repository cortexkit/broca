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
        let Some(mut rx) = self.rx.take() else {
            return;
        };
        // Refuse further replies before draining. The reader forgets the open before it sends
        // the reply, so a reply it sends from here on fails and the reader closes that route
        // itself; one it sent before is still queued here and closed below. Without the close, a
        // reply landing between the last receive and the receiver's destruction would be
        // dropped by both sides and its route would stay open with no owner.
        rx.close();
        let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
        let drained = loop {
            match rx.poll_recv(&mut cx) {
                std::task::Poll::Ready(Some(frame)) => {
                    close_opened_route(self.shared, self.conn, &frame)
                }
                std::task::Poll::Ready(None) => break true,
                std::task::Poll::Pending => break false,
            }
        };
        #[cfg(test)]
        tests::after_drain();
        if drained {
            return;
        }
        // A send that began before the close has not landed yet (or this task ran out of its
        // poll budget). Receiving on the runtime waits for that send, so the route it names is
        // still closed. Without a runtime the reader is gone too, and the socket with it.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let shared = Arc::clone(self.shared);
            let conn = self.conn;
            runtime.spawn(async move {
                while let Some(frame) = rx.recv().await {
                    close_opened_route(&shared, conn, &frame);
                }
            });
        }
    }
}

/// Close the route a `route.open` reply names, when it is a successful one.
fn close_opened_route(shared: &Arc<ConnectionShared>, conn: ConnToken, frame: &Frame) {
    if frame.header.ty != FrameType::Response {
        return;
    }
    if let Ok(ClientControlResponse::RouteOpen {
        route_channel,
        route_epoch,
        ..
    }) = serde_json::from_slice::<ClientControlResponse>(&frame.body)
    {
        let route = RouteHandle::new(route_channel, route_epoch, conn);
        if prepare_goodbye(shared, route).is_ok() {
            route_closer_for(shared).enqueue_failed(route);
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
    use std::cell::RefCell;

    use super::super::*;

    thread_local! {
        /// Runs once the drop of an [`OpenReply`] has drained what was queued, before the
        /// receiver goes away: a reply sent here is the latest one the reader can race the drop
        /// with.
        static AFTER_DRAIN: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
    }

    pub(super) fn after_drain() {
        if let Some(hook) = AFTER_DRAIN.with(|hook| hook.borrow_mut().take()) {
            hook();
        }
    }

    /// The reader removes a route.open's registration before it sends the reply, so the reply
    /// can reach the receiver after a dropped open drained it. That reply must either be
    /// refused, so the reader closes the route itself, or still be closed by the drop.
    #[tokio::test]
    async fn a_reply_sent_while_a_dropped_open_drains_is_refused_or_closed() {
        let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
        let runtime = Arc::clone(&conn.shared.current.lock().unwrap().runtime);
        runtime.route_epochs.lock().unwrap().insert(7, 1);
        let body = serde_json::to_vec(&ClientControlResponse::RouteOpen {
            route_channel: 7,
            route_epoch: 1,
            request_window: None,
            watch_window: None,
        })
        .unwrap();
        let reply = Frame::build(FrameType::Response, control_flags(), 0, 0, 1, body).unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let refused = Arc::new(Mutex::new(None));
        let seen = Arc::clone(&refused);
        AFTER_DRAIN.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                *seen.lock().unwrap() = Some(tx.send(reply).is_err());
            }))
        });
        drop(OpenReply::new(rx, &conn.shared, runtime.conn));
        let refused = refused
            .lock()
            .unwrap()
            .take()
            .expect("the drop drained the receiver");
        if refused {
            return;
        }
        let goodbye = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let frame = daemon.write_rx.recv().await.expect("the writer stays open");
                if frame.header.ty == FrameType::Goodbye {
                    return frame;
                }
            }
        })
        .await
        .expect("a reply the receiver accepted closes its route");
        assert_eq!((goodbye.header.channel, goodbye.header.epoch), (7, 1));
    }

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
