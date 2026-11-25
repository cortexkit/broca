//! An in-process connection whose only live route is one the test names, and the module end
//! of that route, so a test drives a tool call without a daemon.

use super::*;

/// The connection's own tests read the frames written to the route and the requests still
/// waiting, so those stay visible to them.
pub(crate) struct InProcessModuleEnd {
    pub(super) write_rx: mpsc::Receiver<Frame>,
    pub(super) pending: PendingMap,
}

impl SubcConnection {
    /// A connection whose only live route is `(channel, epoch)`, plus the module end of it.
    pub(crate) fn in_process_for_test(
        channel: u16,
        epoch: u32,
    ) -> (SubcConnection, RouteHandle, InProcessModuleEnd) {
        Self::in_process_with_capacity_for_test(channel, epoch, 16)
    }

    pub(crate) fn in_process_full_writer_for_test(
        channel: u16,
        epoch: u32,
    ) -> (SubcConnection, RouteHandle, InProcessModuleEnd) {
        let (conn, route, module) = Self::in_process_with_capacity_for_test(channel, epoch, 256);
        let runtime = conn.snapshot_route(&route).unwrap();
        for _ in 0..256 {
            runtime
                .write_tx
                .try_send(Frame::build(FrameType::Ping, control_flags(), 0, 0, 0, vec![]).unwrap())
                .unwrap();
        }
        (conn, route, module)
    }

    fn in_process_with_capacity_for_test(
        channel: u16,
        epoch: u32,
        capacity: usize,
    ) -> (SubcConnection, RouteHandle, InProcessModuleEnd) {
        let (write_tx, write_rx) = mpsc::channel(capacity);
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let conn_token = ConnToken(NEXT_CONNECTION_TOKEN.fetch_add(1, Ordering::Relaxed));
        let runtime = Arc::new(ConnectionRuntime {
            close_observer: Mutex::new(Weak::new()),
            closing_routes: Mutex::new(HashMap::new()),
            #[cfg(feature = "test-route-close")]
            fail_next_goodbye: std::sync::atomic::AtomicBool::new(false),
            write_tx,
            pending: Arc::clone(&pending),
            route_epochs: Arc::new(Mutex::new(HashMap::from([(channel, epoch)]))),
            write_confirmations: Arc::new(Mutex::new(HashMap::new())),
            conn: conn_token,
            shutdown: CancellationToken::new(),
            capabilities: tokio::sync::OnceCell::new(),
        });
        let (reconnect_tx, _reconnect_rx) = mpsc::unbounded_channel();
        let conn = SubcConnection {
            shared: Arc::new(ConnectionShared {
                aft_harness_answer: Mutex::new(None),
                steer_yields: Mutex::default(),
                catalog: Default::default(),
                #[cfg(test)]
                late_goodbye_handoff: Mutex::new(None),
                route_closer: std::sync::OnceLock::new(),
                connection_file_path: PathBuf::new(),
                current: Mutex::new(GenerationState::new(runtime)),
                corr: AtomicU64::new(1),
                reconnect_tx,
                probe_timeout: LIVENESS_PROBE_TIMEOUT,
                route_open_bind_relay_grace: ROUTE_OPEN_BIND_RELAY_GRACE,
                consumer_identity: None,
                reconnect_attempts: AtomicU64::new(0),
                probe_windows_started: AtomicU64::new(0),
            }),
            module_gates: ModuleGates::default(),
            #[cfg(feature = "test-clock")]
            route_open_attempts: None,
        };
        (
            conn,
            RouteHandle::new(channel, epoch, conn_token),
            InProcessModuleEnd { write_rx, pending },
        )
    }
}

impl InProcessModuleEnd {
    pub(crate) fn try_frame(&mut self) -> Option<Frame> {
        self.write_rx.try_recv().ok()
    }
    pub(crate) fn push(&self, request: &Frame, body: Vec<u8>) -> bool {
        self.terminal(request, FrameType::Push, body)
    }
    pub(crate) fn fill_pending_for_test(&self) {
        for corr in 0..MAX_PENDING_REQUESTS as u64 {
            let (sender, _rx) = mpsc::unbounded_channel();
            self.pending.lock().unwrap().insert(
                (9, 1, corr),
                PendingEntry::Live {
                    sender: PendingSender::Unary(sender),
                    route_open: false,
                    blocks_probe: true,
                },
            );
        }
    }

    pub(crate) fn close_writer_for_test(&mut self) {
        self.write_rx.close();
    }

    pub(crate) fn pending_count_for_test(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    pub(crate) fn take_written_for_test(&mut self) -> Vec<Frame> {
        let mut frames = Vec::new();
        while let Ok(frame) = self.write_rx.try_recv() {
            frames.push(frame);
        }
        frames
    }

    /// The next data-plane Request the connection sent. Other frames (a liveness Ping after a
    /// timeout, a route Goodbye) are skipped.
    pub(crate) async fn next_request(&mut self) -> Frame {
        loop {
            let frame = self
                .write_rx
                .recv()
                .await
                .expect("the connection keeps its writer open");
            if frame.header.ty == FrameType::Request {
                return frame;
            }
        }
    }

    /// Answer `request` with a Response carrying `body`. False when nobody is waiting any more
    /// (the caller already gave up), which is how a late reply looks on a real socket.
    pub(crate) fn reply(&self, request: &Frame, body: Vec<u8>) -> bool {
        self.terminal(request, FrameType::Response, body)
    }

    pub(crate) fn refuse(&self, request: &Frame, code: &str) -> bool {
        self.terminal(
            request,
            FrameType::Error,
            serde_json::to_vec(&serde_json::json!({"code": code, "message": "scripted refusal"}))
                .unwrap(),
        )
    }

    pub(super) fn terminal(&self, request: &Frame, ty: FrameType, body: Vec<u8>) -> bool {
        let key = (
            request.header.channel,
            request.header.epoch,
            request.header.corr,
        );
        let sender = {
            let mut pending = self.pending.lock().unwrap();
            // The real reader removes terminal route.open entries before waking their waiter.
            // Otherwise dropping the registration keeps an entry for a possible late route
            // bind, even though this test provider has already answered.
            if request.header.channel == 0
                && matches!(ty, FrameType::Response | FrameType::Error)
                && matches!(
                    pending.get(&key),
                    Some(PendingEntry::Live {
                        route_open: true,
                        ..
                    })
                )
            {
                match pending.remove(&key) {
                    Some(PendingEntry::Live { sender, .. }) => sender,
                    _ => unreachable!("the route.open entry was inspected under the same lock"),
                }
            } else {
                match pending.get(&key) {
                    Some(PendingEntry::Live { sender, .. }) => sender.clone(),
                    _ => return false,
                }
            }
        };
        let response = Frame::build(
            ty,
            data_flags(),
            request.header.channel,
            request.header.epoch,
            request.header.corr,
            body,
        )
        .expect("a test response frame builds");
        sender.send(response).is_ok()
    }
}
