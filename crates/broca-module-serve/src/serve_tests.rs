include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/common/address_inventory.rs"
));
use super::*;

#[test]
fn tool_bind_target_does_not_widen_management_or_internal_service_admission() {
    use subc_protocol::RouteTarget;
    let module_id = "broca".to_owned();
    let management = RouteTarget::ManagementSurface {
        module_id: module_id.clone(),
    };
    let tools = RouteTarget::ToolProvider {
        module_id: module_id.clone(),
    };
    let agent_data = RouteTarget::InternalService {
        module_id: module_id.clone(),
        service_id: broca_wire::agent_data::SERVICE_ID.into(),
    };
    let unrelated = RouteTarget::InternalService {
        module_id,
        service_id: "unrelated-service".into(),
    };
    for role in [
        RouteRole::LlmRunner,
        RouteRole::Classifier,
        RouteRole::ToolProvider,
        RouteRole::Permissions,
    ] {
        for enabled in [false, true] {
            assert!(serves_bind_target(&management, role, enabled));
            assert!(serves_bind_target(&agent_data, role, enabled));
            assert!(!serves_bind_target(&unrelated, role, enabled));
            assert_eq!(
                serves_bind_target(&tools, role, enabled),
                role == RouteRole::ToolProvider && enabled
            );
        }
    }
}
use broca_module::serve::{
    CancelResponse, RetractResponse, RunStatusResponse, SendRequest, SendResponse,
    SessionReadResponse, StatusResponse,
};

#[derive(Clone)]
struct RequestLogCapture(Arc<Mutex<Vec<CapturedRequestLog>>>);

#[derive(Clone, Debug)]
struct CapturedRequestLog {
    target: &'static str,
    level: tracing::Level,
    fields: std::collections::BTreeMap<String, String>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for RequestLogCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        #[derive(Default)]
        struct Fields(std::collections::BTreeMap<String, String>);

        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0
                    .insert(field.name().to_string(), format!("{value:?}"));
            }

            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                self.0.insert(field.name().to_string(), value.to_string());
            }

            fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                self.0.insert(field.name().to_string(), value.to_string());
            }

            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                self.0.insert(field.name().to_string(), value.to_string());
            }
        }

        let mut fields = Fields::default();
        event.record(&mut fields);
        self.0.lock().unwrap().push(CapturedRequestLog {
            target: event.metadata().target(),
            level: *event.metadata().level(),
            fields: fields.0,
        });
    }
}

fn request_logs_for_filter(filter: &str) -> Vec<CapturedRequestLog> {
    use tracing_subscriber::prelude::*;

    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(filter))
        .with(RequestLogCapture(events.clone()));
    tracing::subscriber::with_default(subscriber, || {
        emit_request_log(
            "session.read",
            "bind-session-id",
            None,
            "ok",
            "reply",
            true,
            17,
        );
    });
    let captured = events.lock().unwrap().clone();
    captured
}

#[test]
fn request_log_is_debug_only_and_carries_the_bound_session() {
    assert!(request_logs_for_filter("info").is_empty());

    let logs = request_logs_for_filter("info,requests=debug");
    assert_eq!(logs.len(), 1);
    let log = &logs[0];
    assert_eq!(log.target, "requests");
    assert_eq!(log.level, tracing::Level::DEBUG);
    assert_eq!(log.fields["operation"], "session.read");
    assert_eq!(log.fields["session"], "bind-session-id");
    assert_eq!(log.fields["actor_opened"], "true");
    assert_eq!(log.fields["elapsed_us"], "17");
    assert_eq!(log.fields["message"], "served management request");
}

#[test]
fn refused_session_send_logs_identity_code_and_class_without_error_text() {
    use tracing_subscriber::prelude::*;

    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info,session.send=info"))
        .with(RequestLogCapture(events.clone()));
    let error = ServeError::DeliveryRefused {
        code: "idle_start_not_granted",
        message: "private prompt and argument content".into(),
    };

    tracing::subscriber::with_default(subscriber, || {
        log_refused_send("session-ref-7", Some("send-9"), "steer", &error);
    });

    let logs = events.lock().unwrap().clone();
    assert_eq!(logs.len(), 1, "{logs:?}");
    let log = &logs[0];
    assert_eq!(log.target, "session.send");
    assert_eq!(log.level, tracing::Level::INFO);
    assert_eq!(log.fields["session_ref"], "session-ref-7");
    assert_eq!(log.fields["send_id"], "send-9");
    assert_eq!(log.fields["delivery_mode"], "steer");
    assert_eq!(log.fields["refusal_code"], "idle_start_not_granted");
    assert_eq!(log.fields["class"], "permanent");
    assert_eq!(log.fields["message"], "session.send refused");
    assert!(!format!("{:?}", log.fields).contains("private prompt and argument content"));
}

#[tokio::test(start_paused = true)]
async fn management_request_logs_refusal_code() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("requests=debug"))
        .with(RequestLogCapture(events.clone()));
    let (writer, mut replies) = broca_diagnostics::channel(4, Default::default());
    log_served_request(
        "session.head",
        &BrocaBindIdentity::new("/private/project", "runner", "session-ref-7"),
        Some("phone-reader"),
        "reply",
        send_error(&writer, 1, 1, 1, 1, "session_expired", "private content"),
    )
    .with_subscriber(subscriber)
    .await
    .unwrap();
    assert_eq!(replies.recv().await.unwrap().header.ty, FrameType::Error);
    let logs = events.lock().unwrap();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert_eq!(logs[0].fields["outcome"], "session_expired");
    assert_eq!(logs[0].fields["caller_module_id"], "phone-reader");
    assert!(!format!("{logs:?}").contains("/private/project"));
    assert!(!format!("{logs:?}").contains("private content"));
}

#[tokio::test(start_paused = true)]
async fn slow_management_request_warns_at_default_level() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info"))
        .with(RequestLogCapture(events.clone()));
    log_served_request(
        "session.read",
        &BrocaBindIdentity::new("/private/project", "runner", "session-ref-7"),
        None,
        "reply",
        async {
            tokio::time::advance(std::time::Duration::from_millis(2001)).await;
        },
    )
    .with_subscriber(subscriber)
    .await;
    let logs = events.lock().unwrap();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert_eq!(logs[0].level, tracing::Level::WARN);
    assert_eq!(logs[0].fields["elapsed_ms"], "2001");
}

#[tokio::test]
async fn panicked_management_request_logs_the_wire_refusal_code() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;
    async fn panicking_handler() -> Result<(), ServeIoError> {
        panic!("private handler state");
    }
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("requests=debug"))
        .with(RequestLogCapture(events.clone()));
    let (writer, mut frames) = broca_diagnostics::channel(4, Arc::default());
    let session = BrocaBindIdentity::new("/private/project", "runner", "panic");
    reply_on_panic(
        writer,
        (1, 1, 1, 1),
        log_served_request(OP_HEAD, &session, None, "reply", panicking_handler()),
    )
    .with_subscriber(subscriber)
    .await
    .unwrap();
    let frame = frames.recv().await.unwrap();
    assert_eq!(frame.header.ty, FrameType::Error);
    let refusal: ErrorBody = serde_json::from_slice(&frame.body).unwrap();
    assert_eq!(refusal.code, "transient");
    let events = events.lock().unwrap();
    let logs: Vec<_> = events
        .iter()
        .filter(|event| event.target == "requests")
        .collect();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].fields["outcome"], refusal.code);
    assert!(!format!("{logs:?}").contains("private handler state"));
}

#[test]
fn management_request_at_two_seconds_remains_debug() {
    use tracing_subscriber::prelude::*;
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("requests=debug"))
        .with(RequestLogCapture(events.clone()));
    tracing::subscriber::with_default(subscriber, || {
        emit_request_log("run.status", "hash", None, "ok", "reply", false, 2_000_000);
    });
    let logs = events.lock().unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].level, tracing::Level::DEBUG);
    assert_eq!(logs[0].fields["outcome"], "ok");
}

#[tokio::test(start_paused = true)]
async fn subscription_logs_open_and_close_not_frames() {
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::prelude::*;
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("requests=debug"))
        .with(RequestLogCapture(events.clone()));
    let (sink_tx, mut sink_rx) = mpsc::channel(1);
    let handler = Arc::new(MockSubscribeHandler {
        sink_tx,
        refuse_session: std::sync::atomic::AtomicBool::new(false),
        read_checks: Default::default(),
    });
    let (writer, mut frames) = broca_diagnostics::channel(8, Arc::default());
    let identity = BrocaBindIdentity::new("/private/project", "runner", "subscribe");
    let cancel = CancellationToken::new();
    log_served_request(
        OP_SUBSCRIBE,
        &identity,
        Some("reader"),
        "open",
        start_subscribe_stream(
            writer,
            1,
            1,
            1,
            1,
            identity.clone(),
            SubscribeFrom::Start,
            None,
            handler,
            cancel.clone(),
            Some("reader".into()),
            broca_module::reader_admission::ReadAccess::default(),
        ),
    )
    .with_subscriber(subscriber)
    .await
    .unwrap();
    let sink = sink_rx.recv().await.unwrap();
    for _ in 0..2 {
        sink.send(Delivery::Display(Arc::new(
            broca_protocol::DisplayEvent::DisplayReset,
        )))
        .await
        .unwrap();
        assert_eq!(
            frames.recv().await.unwrap().header.ty,
            FrameType::StreamData
        );
    }
    cancel.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(1), sink.closed())
        .await
        .unwrap();
    let logs = events.lock().unwrap();
    assert_eq!(logs.len(), 2, "{logs:?}");
    assert_eq!(logs[0].fields["phase"], "open");
    assert_eq!(logs[1].fields["phase"], "close");
    assert_eq!(logs[1].fields["outcome"], "cancelled");
    assert_eq!(logs[0].fields["session"], logs[1].fields["session"]);
}

#[tokio::test]
async fn watchdog_reader_phase_gates_name_stalls() {
    use crate::stall::{tests::Witness, Gate, State};
    use std::time::Duration;
    // Authorization and permit waits run in request tasks, not reader phases.
    for (phase, expected_name) in [
        (Phase::EgressSend, "egress_send"),
        (Phase::Control, "channel_0_control"),
    ] {
        let mut state = State::new();
        state.gate = Some(Gate {
            phase,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let witness = Witness::new(state);
        let (writer, _rx) = broca_diagnostics::channel(1, witness.state.egress.clone());
        let mut routes = Routes::new(true);
        let channel = 0;
        let body = serde_json::to_vec(&ModuleControlRequest::HealthCheck {}).unwrap();
        let frame = Frame::build(
            FrameType::Request,
            control_flags(),
            channel,
            if channel == 0 { 0 } else { 1 },
            1,
            body,
        )
        .unwrap();
        let state = witness.state.clone();
        let task = tokio::spawn(stall::READER.scope(state, async move {
            stall::frame_context(0, channel);
            let handler = Arc::new(GatedStatusHandler {
                gate_auth: false,
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            });
            let registry = test_registry(LineageRegistry::new(Arc::new(RefusingOpener)));
            handle_frame(
                frame,
                &writer,
                &mut routes,
                &handler,
                &registry,
                &VaultGauge::not_in_use(),
                None,
                &CancellationToken::new(),
                &Arc::new(Semaphore::new(64)),
                &mut JoinSet::new(),
                None,
            )
            .await
        }));
        tokio::time::timeout(
            Duration::from_secs(2),
            witness.state.gate.as_ref().unwrap().entered.notified(),
        )
        .await
        .unwrap_or_else(|_| panic!("reader did not reach {phase:?} gate"));
        let snapshot = witness.reader_phase_snapshot(phase).await;
        task.abort();
        let _ = task.await;
        assert_eq!(snapshot["kind"], "reader_phase", "{phase:?}");
        assert_eq!(snapshot["reader"]["phase_code"], phase as u8);
        assert_eq!(snapshot["reader"]["phase"], expected_name);
        assert_eq!(snapshot["reader"]["channel"], channel);
    }
}

#[tokio::test]
async fn watchdog_stalled_egress_write_and_flush_are_named() {
    use crate::stall::{tests::Witness, State};
    struct HeldFlush;
    impl AsyncWrite for HeldFlush {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Ok(bytes.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }
    for flush in [false, true] {
        let witness = Witness::new(State::new());
        let (tx, rx) = broca_diagnostics::channel(64, witness.state.egress.clone());
        let (socket, _held_peer) = tokio::io::duplex(32);
        let task = if flush {
            tokio::spawn(drain_writer(HeldFlush, rx))
        } else {
            tokio::spawn(drain_writer(socket, rx))
        };
        let frame = Frame::build(
            FrameType::Response,
            control_flags(),
            7,
            1,
            1,
            vec![b'x'; 16_384],
        )
        .unwrap();
        tx.send(frame).await.unwrap();
        let snapshot = witness.snapshot().await;
        task.abort();
        let _ = task.await;
        assert_eq!(snapshot["kind"], "egress_write");
        assert_eq!(
            snapshot["egress"]["operation"],
            if flush { "flush" } else { "write" }
        );
        assert_eq!(snapshot["egress"]["sent"], 1);
        assert_eq!(snapshot["egress"]["written"], if flush { 1 } else { 0 });
        assert_eq!(snapshot["egress"]["occupancy"], if flush { 0 } else { 1 });
        assert_eq!(
            witness.state.egress.operation.load(Ordering::Relaxed),
            0,
            "cancelled writer clears active operation"
        );
    }
}

/// A write half whose every write fails with one chosen I/O error kind.
struct FailingWriter(std::io::ErrorKind);

impl tokio::io::AsyncWrite for FailingWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Err(std::io::Error::from(self.0)))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Err(std::io::Error::from(self.0)))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

/// `body_len` picks which call meets the failure. drain_writer wraps the socket in an 8 KiB
/// BufWriter, so a small frame is only buffered and the failure surfaces at the FLUSH, while
/// a frame larger than the buffer is written straight through and fails at the WRITE. Both
/// sites have their own peer-gone arm, so each needs a case: the first version of this test
/// used only a small frame, and deleting the write-site arm left it green.
///
/// The frame is a route-channel response, a type that may carry a body: the frame writer
/// refuses a body on a header-only frame such as GOODBYE before writing a byte, so a large
/// GOODBYE would test that refusal instead of the write failure.
async fn drain_one_frame_into(
    kind: std::io::ErrorKind,
    body_len: usize,
) -> Result<(), ServeIoError> {
    let (tx, rx) = broca_diagnostics::channel(1, Arc::default());
    let frame = Frame::build(
        FrameType::Response,
        Flags::new(false, Priority::Passive, false),
        1,
        0,
        1,
        vec![b'x'; body_len],
    )
    .unwrap();
    tx.send(frame).await.unwrap();
    drop(tx);
    drain_writer(FailingWriter(kind), rx).await
}

/// The writer's rule for a connection the daemon reset, on Windows. On Windows CI the reset reached the WRITE half
/// first (os error 10054), which the TCP test in tests/it/wire.rs cannot exercise on macOS,
/// where the read half meets it first. So the writer's rule is pinned directly: a write the
/// daemon can no longer receive ends the writer Ok, and any other write failure is still an
/// error. The control arm is what stops "return Ok on every error" from passing.
#[tokio::test]
async fn the_writer_ends_ok_when_the_daemon_is_gone_and_errs_otherwise() {
    for (site, body_len) in [("flush", 0), ("write", 64 * 1024)] {
        for kind in [
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::BrokenPipe,
        ] {
            assert!(
                drain_one_frame_into(kind, body_len).await.is_ok(),
                "{kind:?} at the {site} means the daemon is gone; the writer must end Ok"
            );
        }
        assert!(
            drain_one_frame_into(std::io::ErrorKind::PermissionDenied, body_len)
                .await
                .is_err(),
            "a {site} failure that is not the daemon going away must stay an error"
        );
    }
}

#[test]
fn result_envelope_preserves_legacy_json_shape() {
    use broca_protocol::content::{ContentBlock, Message, Role};
    use broca_wire::{CursorWire, SessionReadLineageState, SessionReadMessage};

    let response = SessionReadResponse {
        history_version: 1,
        incarnation: "00112233445566778899aabbccddeeff".into(),
        next_late_after: None,
        deferred_calls: None,
        late_approvals: None,
        open_batch: None,
        // Absent deliberately: this test asserts the envelope still serialises to the
        // LEGACY json shape, so the new field must be skipped rather than emitted.
        lineage_id: None,
        messages: vec![
            SessionReadMessage {
                send_id: None,
                speaks_for: None,
                notice: false,
                keep: false,
                sender: None,
                notices: Vec::new(),
                created_at: None,
                completed_at: None,
                reasoning_summaries: Vec::new(),
                client_message_id: None,
                tool_times: Vec::new(),
                rich_ui: None,
                model_fallback: None,
                written_at: None,
                delivered_as: None,
                run: None,
                ordinal: 0,
                mid: "m0".into(),
                message: Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "hello, 世界 🧪".into(),
                    }],
                ),
                tool_calls: Vec::new(),
                part_keys: None,
                described: None,
                digest: None,
                part_digests: None,
            },
            SessionReadMessage {
                send_id: None,
                speaks_for: None,
                notice: false,
                keep: false,
                sender: None,
                notices: Vec::new(),
                created_at: None,
                completed_at: None,
                reasoning_summaries: Vec::new(),
                client_message_id: None,
                tool_times: Vec::new(),
                rich_ui: None,
                model_fallback: None,
                written_at: None,
                delivered_as: None,
                run: None,
                ordinal: 1,
                mid: "m1".into(),
                message: Message::new(
                    Role::Assistant,
                    vec![ContentBlock::Text {
                        text: "response".into(),
                    }],
                ),
                tool_calls: Vec::new(),
                part_keys: None,
                described: None,
                digest: None,
                part_digests: None,
            },
        ],
        next_from_ordinal: Some(2),
        head: Some(CursorWire::new(7, 1)),
        lineage_state: SessionReadLineageState {
            last_run_id: Some("run-1".into()),
            state: "idle".into(),
            reason: Some("completed".into()),
            error: None,
            usage: None,
        },
        // Absent: this models a read that did not set `include_tools`, so the legacy shape
        // must hold with the tool fields skipped.
        tools: None,
        tools_run_id: None,
    };

    let direct = serde_json::to_vec(&ResultEnvelope { result: &response }).unwrap();
    let legacy = serde_json::json!({ "result": &response });
    let direct: serde_json::Value = serde_json::from_slice(&direct).unwrap();
    assert_eq!(direct, legacy);
    let envelope = direct.as_object().unwrap();
    assert_eq!(envelope.len(), 1);
    assert!(envelope.contains_key("result"));
}

#[test]
fn completed_session_dispatch_lane_is_evicted_when_its_route_closes() {
    let session = BrocaBindIdentity::new("/tmp/project-a", "runner", "s1");
    let mut routes = Routes::new(false);
    routes
        .by_channel
        .insert(7, (1, session.clone(), BindObservation::default()));

    let ticket = routes.reserve_dispatch(&session);
    assert!(routes.dispatch_lanes.contains_key(&session));
    drop(ticket);

    routes.route_closed(7, 1);
    assert!(!routes.dispatch_lanes.contains_key(&session));
    assert!(routes.dispatch_lanes.is_empty());
}

#[test]
fn dispatch_lane_sweep_reclaims_a_completed_request_that_outlived_its_route() {
    let departed = BrocaBindIdentity::new("/tmp/project-a", "runner", "departed");
    let mut routes = Routes::new(false);
    routes
        .by_channel
        .insert(7, (1, departed.clone(), BindObservation::default()));
    let ticket = routes.reserve_dispatch(&departed);

    routes.route_closed(7, 1);
    assert!(routes.dispatch_lanes.contains_key(&departed));
    drop(ticket);

    for index in 0..DISPATCH_LANE_SWEEP_MIN {
        let session =
            BrocaBindIdentity::new("/tmp/project-a", "runner", format!("replacement-{index}"));
        drop(routes.reserve_dispatch(&session));
    }

    assert!(!routes.dispatch_lanes.contains_key(&departed));
    assert!(routes.dispatch_lanes.len() < DISPATCH_LANE_SWEEP_MIN);
}

#[tokio::test]
async fn route_reopen_keeps_fifo_while_the_departing_lane_has_an_outstanding_ticket() {
    let session = BrocaBindIdentity::new("/tmp/project-a", "runner", "s1");
    let mut routes = Routes::new(false);
    routes
        .by_channel
        .insert(7, (1, session.clone(), BindObservation::default()));
    let first_turn = routes.reserve_dispatch(&session).wait().await;

    routes.route_closed(7, 1);
    assert!(routes.dispatch_lanes.contains_key(&session));
    routes
        .by_channel
        .insert(8, (2, session.clone(), BindObservation::default()));
    let second_ticket = routes.reserve_dispatch(&session);

    let mut second_wait = Box::pin(second_ticket.wait());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), second_wait.as_mut())
            .await
            .is_err(),
        "the reopened route bypassed the departing route's in-flight request"
    );

    drop(first_turn);
    let _second_turn = tokio::time::timeout(std::time::Duration::from_secs(1), second_wait)
        .await
        .expect("the reopened route advances after the first request releases its turn");
}

/// A control unit whose encoded frame exceeds the transport limit is never put on the wire:
/// the stream ends with a typed error naming the unit's cursor, the frame size and the
/// limit, and no clean StreamEnd that would send the client back to the same cursor. The
/// served bytes are pinned by `tests/golden/subscribe_unit_undeliverable.too_large.json`.
#[tokio::test]
async fn an_oversized_subscribe_unit_ends_the_stream_with_a_typed_error() {
    let registry = Arc::new(test_registry(LineageRegistry::new(Arc::new(
        RefusingOpener,
    ))));
    let mut ingress = BindIngress::new(registry);
    ingress.bind(1, "published", false).await;
    next_written(&mut ingress.written).await;
    let sink = ingress.subscribe(1, 10).await;
    // 1 MiB of control characters: JSON escapes each as six bytes.
    let unit = broca_protocol::caller::ControlUnit::ToolResult {
        result: broca_protocol::ToolResult {
            tool_call_id: broca_protocol::ToolCallId("call-1".into()),
            output: broca_protocol::ToolOutput::Text {
                text: "\u{1}".repeat(1 << 20).into(),
            },
            is_error: false,
        },
    };
    sink.send(Delivery::Control(Arc::new((
        broca_core::Cursor::new(42, 1),
        unit,
        Some(cortexkit_role_llm_runner::live::LiveEvent::DisplayReset),
    ))))
    .await
    .unwrap();
    let frame = next_written(&mut ingress.written).await;
    assert_eq!(
        (frame.header.ty, frame.header.epoch, frame.header.corr),
        (FrameType::Error, 1, 10),
        "an oversized unit must end the stream with a typed error"
    );
    assert_eq!(
        String::from_utf8(frame.body.to_vec()).unwrap(),
        include_str!("../tests/golden/subscribe_unit_undeliverable.too_large.json").trim_end()
    );
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        ingress.written.try_recv().is_err(),
        "no StreamEnd or data frame follows the typed terminal"
    );
}

#[tokio::test]
async fn transient_admission_error_uses_transient_wire_code() {
    let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
    send_serve_error(
        &tx,
        PROTOCOL_VERSION,
        7,
        3,
        11,
        &ServeError::Transient("credentials service unreachable".into()),
    )
    .await
    .unwrap();

    let frame = rx.recv().await.expect("error frame");
    assert_eq!(frame.header.ty, FrameType::Error);
    let error: ErrorBody = serde_json::from_slice(&frame.body).unwrap();
    assert_eq!(error.code, "transient");
    assert!(error.message.contains("credentials service unreachable"));
}

#[tokio::test]
async fn variant_unsupported_error_body_pins_wire_bytes() {
    let refusal = broca_core::run::VariantRefusal {
            model: "anthropic/claude-opus-5".into(),
            variant: "max".into(),
            defined: vec!["none".into(), "low".into(), "medium".into(), "high".into()],
            reason: "model 'anthropic/claude-opus-5' does not define variant 'max'; its variants are [none, low, medium, high]".into(),
        };
    assert_eq!(
        broca_core::RunError::VariantUnsupported(refusal.clone()).admission_error_class(),
        broca_protocol::error::ErrorClass::Permanent
    );
    let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
    send_serve_error(
        &tx,
        PROTOCOL_VERSION,
        7,
        3,
        11,
        &ServeError::VariantUnsupported(refusal),
    )
    .await
    .unwrap();
    let frame = rx.recv().await.unwrap();
    assert_eq!(frame.header.ty, FrameType::Error);
    assert_eq!(frame.body.as_slice(), br#"{"code":"variant_unsupported","message":"open failed: run resolution failed: model 'anthropic/claude-opus-5' does not define variant 'max'; its variants are [none, low, medium, high]","detail":{"class":"permanent","defined":["none","low","medium","high"],"model":"anthropic/claude-opus-5","variant":"max"}}"#);
}

#[tokio::test]
async fn tool_alias_invalid_error_body_names_tool_and_permanent_class() {
    let refusal = broca_core::run::ToolAliasInvalid {
        tool: "bash".into(),
        reason: "alias collides".into(),
    };
    assert_eq!(
        broca_core::RunError::ToolAliasInvalid(refusal.clone()).admission_error_class(),
        broca_protocol::error::ErrorClass::Permanent
    );
    let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
    send_serve_error(
        &tx,
        PROTOCOL_VERSION,
        7,
        3,
        11,
        &ServeError::ToolAliasInvalid(refusal),
    )
    .await
    .unwrap();
    let frame = rx.recv().await.unwrap();
    assert_eq!(frame.header.ty, FrameType::Error);
    assert_eq!(frame.body.as_slice(), br#"{"code":"tool_alias_invalid","message":"tool_alias_invalid: tool \"bash\": alias collides","detail":{"class":"permanent","tool":"bash"}}"#);
}

#[tokio::test]
async fn reply_too_large_detail_bytes_match_published_vectors() {
    let vector = cortexkit_role_llm_runner::vectors::FILES
        .iter()
        .find_map(|(name, contents)| (*name == "reply-too-large.json").then_some(*contents))
        .expect("the role crate publishes reply-too-large.json");
    let vectors: serde_json::Value = serde_json::from_str(vector).unwrap();
    #[derive(serde::Serialize)]
    struct ExpectedError<'a> {
        code: &'a str,
        message: &'a str,
        detail: &'a cortexkit_role_llm_runner::errors::ReplyTooLargeDetail,
    }

    for case in vectors["refusals"].as_array().unwrap() {
        let refusal = &case["refusal"];
        let detail: cortexkit_role_llm_runner::errors::ReplyTooLargeDetail =
            serde_json::from_value(refusal["detail"].clone()).unwrap();
        assert_eq!(
            serde_json::to_value(&detail).unwrap(),
            refusal["detail"],
            "{}",
            case["name"]
        );
        let expected = serde_json::to_vec(&ExpectedError {
            code: refusal["code"].as_str().unwrap(),
            message: refusal["message"].as_str().unwrap(),
            detail: &detail,
        })
        .unwrap();
        let (writer, mut rx) = broca_diagnostics::channel(1, Arc::default());
        send_serve_error(
            &writer,
            PROTOCOL_VERSION,
            7,
            3,
            11,
            &ServeError::ReplyTooLarge(detail),
        )
        .await
        .unwrap();
        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.ty, FrameType::Error);
        assert_eq!(frame.body, expected, "{}", case["name"]);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap(),
            *refusal,
            "{}",
            case["name"]
        );
    }
}

#[tokio::test]
async fn a_source_less_run_result_uses_the_generic_frame_limit_error() {
    let oversized = "x".repeat(broca_wire::SUBC_MAX_FRAME_BYTES as usize);
    let run_result = broca_wire::RunResultResponse {
        awaits: None,
        finish: None,
        auth_selection: None,
        directives: None,
        context_ceiling: None,
        usage: Default::default(),
        detail: None,
        run_id: "run".into(),
        state: "active".into(),
        reason: Some(oversized),
        error: None,
        final_message: None,
        structured_output: None,
    };
    let expected =
        br#"{"code":"response_too_large","message":"response exceeds the transport frame limit"}"#;
    let (writer, mut rx) = broca_diagnostics::channel(2, Arc::default());
    send_json_response(&writer, PROTOCOL_VERSION, 7, 3, 12, &run_result)
        .await
        .unwrap();
    let frame = rx.recv().await.unwrap();
    assert_eq!(frame.header.ty, FrameType::Error);
    assert_eq!(frame.body, expected);
}

#[tokio::test]
async fn ended_ordered_send_retries_are_successful_terminal_replies() {
    let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
    for outcome in ["superseded", "expired"] {
        send_serve_error(
            &tx,
            PROTOCOL_VERSION,
            7,
            3,
            11,
            &ServeError::SendEnded {
                submission_id: "sub-note".into(),
                outcome,
            },
        )
        .await
        .unwrap();
        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.ty, FrameType::Response, "{outcome}");
        let value: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(
            value,
            serde_json::json!({"result":{"state":outcome,"submission_id":"sub-note"}})
        );
    }
}

#[tokio::test]
async fn plan_model_mismatch_bytes_are_pinned() {
    use sha2::{Digest, Sha256};
    let refusal = broca_fetch_plan::Refusal::PlanModelMismatch {
        provider: "prefrontal-core".into(),
        plan_model: serde_json::json!({"provider": "fake", "id": "other"}),
        send_model: serde_json::json!({"provider": "fake", "id": "fake"}),
    };
    let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
    send_serve_error(
        &tx,
        PROTOCOL_VERSION,
        7,
        3,
        11,
        &ServeError::PlanRefused(refusal),
    )
    .await
    .unwrap();
    let frame = rx.recv().await.unwrap();
    assert_eq!(frame.header.ty, FrameType::Error);
    let golden = std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../broca-wire/tests/golden/plan_model_mismatch.json"),
    )
    .unwrap();
    assert_eq!(frame.body, golden);
    assert_eq!(
        format!("{:x}", Sha256::digest(&golden)),
        "9f77fab81eaf92677ced37b818a08d2feefa8dbe873b2f82fd0e365a223376bd",
    );
}

#[tokio::test]
async fn compaction_preset_is_required_before_provider_fetch_or_actor_open() {
    struct AdmissionOnly;
    #[async_trait::async_trait]
    impl ServeHandler for AdmissionOnly {
        fn compaction_supported(&self) -> bool {
            true
        }
        async fn send(&self, _: SendRequest) -> Result<SendResponse, ServeError> {
            panic!("refused plan must not open an actor")
        }
        async fn retract(
            &self,
            _: &BrocaBindIdentity,
            _: broca_protocol::SubmissionId,
        ) -> Result<RetractResponse, ServeError> {
            unreachable!()
        }
        async fn subscribe(
            &self,
            _: &BrocaBindIdentity,
            _: SubscribeFrom,
            _: mpsc::Sender<Delivery>,
        ) -> Result<(), ServeError> {
            unreachable!()
        }
        async fn read(&self, _: SessionReadRequest) -> Result<SessionReadResponse, ServeError> {
            unreachable!()
        }
        async fn cancel(
            &self,
            _: &BrocaBindIdentity,
            _: broca_protocol::RunId,
        ) -> Result<CancelResponse, ServeError> {
            unreachable!()
        }
        async fn status(&self, _: &BrocaBindIdentity) -> Result<StatusResponse, ServeError> {
            unreachable!()
        }
        async fn run_status(
            &self,
            _: &BrocaBindIdentity,
            _: broca_protocol::RunId,
        ) -> Result<RunStatusResponse, ServeError> {
            unreachable!()
        }
    }
    let composition = serde_json::json!({"providers":[{"provider":"ck-mc","tools":[]}],"step_transform_items":[],"compaction":{"provider":"ck-mc"}});
    let plan = serde_json::json!({"tool_items":[],"system_text_items":[],"step_transform_items":[],"compaction_item":{"provider":"ck-mc","params":{}},"composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),"composition":composition});
    let session = BrocaBindIdentity::new("/synthetic/compaction-preset", "runner", "preset");
    let result = admit_send_plan(
        &AdmissionOnly,
        &session,
        &serde_json::json!({"plan":plan}),
        &plan,
        None,
        None,
        |_, _| Ok(()),
    )
    .await;
    let Err(refusal) = result else {
        panic!("missing compaction preset was admitted")
    };
    let body = refusal.wire_body();
    assert_eq!(body.code, "invalid_params");
    assert_eq!(
        body.detail,
        Some(serde_json::json!({"field":"plan.compaction_item.preset"}))
    );
}

#[tokio::test]
async fn pre_user_unavailable_wire_names_only_the_refusing_provider() {
    let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
    send_serve_error(
        &tx,
        PROTOCOL_VERSION,
        7,
        3,
        11,
        &ServeError::PreUserUnavailable {
            provider: "magic-context".into(),
        },
    )
    .await
    .unwrap();
    let frame = rx.recv().await.unwrap();
    assert_eq!(frame.header.ty, FrameType::Error);
    let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
    assert_eq!(body["code"], "pre_user_unavailable");
    assert_eq!(
        body["detail"],
        serde_json::json!({"provider": "magic-context"})
    );
    assert!(body.get("retryable").is_none());
}

#[tokio::test]
async fn retracted_send_error_uses_retracted_wire_code() {
    let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
    send_serve_error(
        &tx,
        PROTOCOL_VERSION,
        7,
        3,
        11,
        &ServeError::SendRetracted {
            submission_id: "sub-r2".into(),
        },
    )
    .await
    .unwrap();

    let frame = rx.recv().await.expect("error frame");
    assert_eq!(frame.header.ty, FrameType::Error);
    let error: ErrorBody = serde_json::from_slice(&frame.body).unwrap();
    assert_eq!(error.code, "send_retracted");
    assert!(error.message.contains("sub-r2"));
}

struct MockSubscribeHandler {
    sink_tx: mpsc::Sender<mpsc::Sender<Delivery>>,
    refuse_session: std::sync::atomic::AtomicBool,
    read_checks: std::sync::atomic::AtomicUsize,
}

#[test]
fn route_request_future_stays_small() {
    let (sink_tx, _) = mpsc::channel(1);
    let handler = Arc::new(MockSubscribeHandler {
        sink_tx,
        refuse_session: std::sync::atomic::AtomicBool::new(false),
        read_checks: Default::default(),
    });
    let (writer, _) = broca_diagnostics::channel(4, Default::default());
    let session = BrocaBindIdentity::new("/project", "runner", "session");
    let bind = BindObservation::default();
    let request = execute_route_request(
        RouteRequest {
            method: OP_SEND.into(),
            params: serde_json::json!({}),
        },
        &writer,
        1,
        1,
        1,
        1,
        &session,
        &bind,
        &handler,
    );
    let bytes = std::mem::size_of_val(&request);
    eprintln!("execute_route_request future: {bytes} bytes");
    assert!(bytes <= 4096, "route request future is {bytes} bytes");
}

#[async_trait::async_trait]
impl ServeHandler for MockSubscribeHandler {
    async fn authorize_session_read(
        &self,
        session: &BrocaBindIdentity,
        access: &broca_module::reader_admission::ReadAccess,
    ) -> Result<(), ServeError> {
        self.read_checks.fetch_add(1, Ordering::Relaxed);
        self.authorize_session(session, access.principal.as_ref())
            .await
    }

    async fn authorize_session(
        &self,
        _session: &BrocaBindIdentity,
        _principal: Option<&broca_protocol::BindPrincipal>,
    ) -> Result<(), ServeError> {
        if self.refuse_session.load(Ordering::Relaxed) {
            Err(ServeError::ScopeRefused(
                broca_core::ScopeRefusal::SenderNotOwner,
            ))
        } else {
            Ok(())
        }
    }

    async fn send(&self, _req: SendRequest) -> Result<SendResponse, ServeError> {
        unimplemented!()
    }
    async fn retract(
        &self,
        _session: &BrocaBindIdentity,
        _submission_id: broca_protocol::SubmissionId,
    ) -> Result<RetractResponse, ServeError> {
        unimplemented!()
    }
    async fn subscribe(
        &self,
        _session: &BrocaBindIdentity,
        _from: SubscribeFrom,
        sink: mpsc::Sender<Delivery>,
    ) -> Result<(), ServeError> {
        let _ = self.sink_tx.send(sink).await;
        Ok(())
    }
    async fn read(&self, _req: SessionReadRequest) -> Result<SessionReadResponse, ServeError> {
        unimplemented!()
    }
    async fn cancel(
        &self,
        _session: &BrocaBindIdentity,
        _run_id: broca_protocol::RunId,
    ) -> Result<CancelResponse, ServeError> {
        unimplemented!()
    }
    async fn status(&self, _session: &BrocaBindIdentity) -> Result<StatusResponse, ServeError> {
        unimplemented!()
    }
    async fn run_status(
        &self,
        _session: &BrocaBindIdentity,
        _run_id: broca_protocol::RunId,
    ) -> Result<RunStatusResponse, ServeError> {
        unimplemented!()
    }
}

#[tokio::test]
async fn subscribe_transport_terminals_keep_stream_sequence_and_event() {
    for terminal in [
        Delivery::Detached,
        Delivery::Error("broken replay".into()),
        Delivery::Expired {
            expired_at_ms: 1234,
        },
    ] {
        let detached = matches!(terminal, Delivery::Detached);
        let (sink_tx, mut sink_rx) = mpsc::channel(1);
        let handler = Arc::new(MockSubscribeHandler {
            sink_tx,
            refuse_session: std::sync::atomic::AtomicBool::new(false),
            read_checks: Default::default(),
        });
        let (writer, mut written) = broca_diagnostics::channel::<Frame>(8, Arc::default());
        start_subscribe_stream(
            writer,
            1,
            1,
            1,
            1,
            BrocaBindIdentity::new("/tmp/project-a", "runner", "terminal-sequence"),
            SubscribeFrom::Start,
            None,
            handler,
            CancellationToken::new(),
            None,
            broca_module::reader_admission::ReadAccess::default(),
        )
        .await
        .unwrap();
        let sink = sink_rx.recv().await.unwrap();
        sink.send(Delivery::Display(Arc::new(
            broca_protocol::DisplayEvent::DisplayReset,
        )))
        .await
        .unwrap();
        sink.send(terminal).await.unwrap();
        let first = tokio::time::timeout(std::time::Duration::from_secs(2), written.recv())
            .await
            .unwrap()
            .unwrap();
        let end = tokio::time::timeout(std::time::Duration::from_secs(2), written.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.header.ty, FrameType::StreamData);
        assert_eq!(
            end.header.ty,
            if detached {
                FrameType::StreamEnd
            } else {
                FrameType::Error
            }
        );
        for (frame, seq) in [(first, 1_u64), (end, 2)] {
            let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
            assert_eq!(body["seq"], seq);
            assert_eq!(body["event"]["type"], "display_reset");
        }
    }
}

#[tokio::test]
async fn test_subscribe_forward_task_cancel_while_blocked() {
    use broca_protocol::display::DisplayEvent;
    use std::time::Duration;

    let (sink_tx, mut sink_rx) = mpsc::channel(1);
    let handler = Arc::new(MockSubscribeHandler {
        sink_tx,
        refuse_session: std::sync::atomic::AtomicBool::new(false),
        read_checks: Default::default(),
    });

    // Bounded channel of size 1 for the writer.
    let (writer_tx, _writer_rx) = broca_diagnostics::channel::<Frame>(1, Arc::default());

    let cancel = CancellationToken::new();
    let session = BrocaBindIdentity::new("/tmp/project-a", "runner", "s1");

    let start_res = start_subscribe_stream(
        writer_tx,
        1, // ver
        1, // channel
        1, // epoch
        1, // corr
        session,
        SubscribeFrom::Start,
        None,
        handler,
        cancel.clone(),
        None,
        broca_module::reader_admission::ReadAccess::default(),
    )
    .await;
    assert!(start_res.is_ok());

    let delivery_sink = sink_rx.recv().await.unwrap();

    // Send first delivery to fill the writer channel buffer.
    let delivery1 = Delivery::Display(Arc::new(DisplayEvent::DisplayReset));
    delivery_sink.send(delivery1).await.unwrap();

    // Send second delivery. The forward task will receive it and block on writer.send(frame).await.
    let delivery2 = Delivery::Display(Arc::new(DisplayEvent::DisplayReset));
    delivery_sink.send(delivery2).await.unwrap();

    // Give the forward task a moment to block on writer.send.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Cancel the stream.
    cancel.cancel();

    // The forward task should unblock and exit, dropping the deliveries receiver.
    // We verify this by checking that sending a third delivery returns an error.
    let delivery3 = Delivery::Display(Arc::new(DisplayEvent::DisplayReset));

    let mut success = false;
    for _ in 0..20 {
        if delivery_sink.send(delivery3.clone()).await.is_err() {
            success = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        success,
        "forward task did not exit after cancel while blocked on send"
    );
}

/// A handler whose session status parks until the test releases it, so the test decides
/// exactly when a route request task finishes.
struct GatedStatusHandler {
    gate_auth: bool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl ServeHandler for GatedStatusHandler {
    async fn authorize_session(
        &self,
        _session: &BrocaBindIdentity,
        _principal: Option<&broca_protocol::BindPrincipal>,
    ) -> Result<(), ServeError> {
        if self.gate_auth {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(())
    }

    async fn send(&self, _req: SendRequest) -> Result<SendResponse, ServeError> {
        unimplemented!()
    }
    async fn retract(
        &self,
        _session: &BrocaBindIdentity,
        _submission_id: broca_protocol::SubmissionId,
    ) -> Result<RetractResponse, ServeError> {
        unimplemented!()
    }
    async fn subscribe(
        &self,
        _session: &BrocaBindIdentity,
        _from: SubscribeFrom,
        _sink: mpsc::Sender<Delivery>,
    ) -> Result<(), ServeError> {
        unimplemented!()
    }
    async fn read(&self, _req: SessionReadRequest) -> Result<SessionReadResponse, ServeError> {
        unimplemented!()
    }
    async fn cancel(
        &self,
        _session: &BrocaBindIdentity,
        _run_id: broca_protocol::RunId,
    ) -> Result<CancelResponse, ServeError> {
        unimplemented!()
    }
    async fn status(&self, session: &BrocaBindIdentity) -> Result<StatusResponse, ServeError> {
        if session.session == "panicking-status" {
            panic!("a request handler bug");
        }
        if session.session == "oversized-error-response" {
            return Err(ServeError::Transient(
                "x".repeat(broca_wire::SUBC_MAX_FRAME_BYTES as usize),
            ));
        }
        if session.session == "oversized-response" {
            return Ok(StatusResponse::Active {
                run_id: "x".repeat(broca_wire::SUBC_MAX_FRAME_BYTES as usize),
                step: 1,
            });
        }
        self.entered.notify_one();
        self.release.notified().await;
        Err(ServeError::Transient("released by the test".into()))
    }
    async fn run_status(
        &self,
        _session: &BrocaBindIdentity,
        _run_id: broca_protocol::RunId,
    ) -> Result<RunStatusResponse, ServeError> {
        unimplemented!()
    }
}

use super::test_openers::RefusingOpener;

#[tokio::test]
async fn headerless_control_deliveries_keep_streaming_without_a_resume_proof() {
    let handler = LineageRegistry::new(Arc::new(RefusingOpener));
    let session = BrocaBindIdentity::new("project", "runner", "headerless");
    let delivery = Delivery::Control(Arc::new((
        broca_core::Cursor::new(1, 0),
        broca_protocol::caller::ControlUnit::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
        serde_json::from_value(serde_json::json!({"type":"message_started","mid":"m1"})).unwrap(),
    )));
    let cursor = control_cursor_for_delivery(&handler, &session, &delivery)
        .await
        .expect("a headerless legacy log remains readable")
        .expect("control delivery retains its position");
    assert_eq!(
        serde_json::to_value(cursor).unwrap(),
        serde_json::json!({"wal_seq":1,"sub_index":0})
    );
}

async fn next_written(rx: &mut EgressReceiver<Frame>) -> Frame {
    tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("the serve loop wrote nothing within 5s")
        .expect("the serve loop's writer channel closed")
}

/// The session address inventory, with the first scope-reservation read held until the
/// test releases it. A stamped bind reads its session's reservation in its adoption check
/// (a session with no run yet belongs to whoever reserved it), so holding that read holds
/// the bind's authorization in flight. An unstamped bind never reads it. Later reads
/// answer at once.
struct HeldBindRead {
    inventory: Arc<broca_store::SqliteStore>,
    entered: tokio::sync::Notify,
    release: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    finished: tokio::sync::Notify,
    /// What the read answers in place of the inventory's own reservation: `None` reads the
    /// inventory, `Some(Ok(reservation))` reports that reservation, `Some(Err)` fails.
    answer: Option<Result<broca_store::session_address::ScopeReservation, String>>,
}

impl broca_store::SessionAddressResolver for HeldBindRead {
    fn resolve_session_address(
        &self,
        bind: &broca_protocol::BindIdentity,
        workdir: Option<&std::path::Path>,
        reservation: Option<&broca_store::session_address::ScopeReservation>,
    ) -> Result<broca_protocol::BindIdentity, broca_store::session_address::SessionAddressError>
    {
        broca_store::SessionAddressResolver::resolve_session_address(
            &*self.inventory,
            bind,
            workdir,
            reservation,
        )
    }

    fn scope_reservation(
        &self,
        session: &broca_protocol::BindIdentity,
    ) -> Result<
        Option<broca_store::session_address::ScopeReservation>,
        broca_store::session_address::SessionAddressError,
    > {
        let gate = self.release.lock().unwrap().take();
        if let Some(gate) = gate {
            self.entered.notify_one();
            gate.recv().unwrap();
            self.finished.notify_one();
        }
        match &self.answer {
            None => {
                broca_store::SessionAddressResolver::scope_reservation(&*self.inventory, session)
            }
            Some(Ok(reservation)) => Ok(Some(reservation.clone())),
            Some(Err(reason)) => {
                Err(broca_store::session_address::SessionAddressError::store_error(reason))
            }
        }
    }
}

/// A registry whose stamped binds' adoption checks wait on the returned [`HeldBindRead`]
/// until the returned release is dropped, then read `answer` (see [`HeldBindRead::answer`]).
fn held_bind_registry_answering(
    answer: Option<Result<broca_store::session_address::ScopeReservation, String>>,
) -> (Arc<LineageRegistry>, Arc<HeldBindRead>, ReleaseBindRead) {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = broca_test_support::scratch_root().join(format!(
        "serve-held-bind-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = root.join("run-index.db");
    let inventory = Arc::new(broca_store::SqliteStore::open(&db).unwrap());
    let stored = broca_store::session_refs::StoredSessions::new(&root);
    broca_store::session_refs::migrate(&db, &stored).unwrap();
    let (release, read_gate) = std::sync::mpsc::channel();
    let store = Arc::new(HeldBindRead {
        inventory,
        entered: tokio::sync::Notify::new(),
        release: std::sync::Mutex::new(Some(read_gate)),
        finished: tokio::sync::Notify::new(),
        answer,
    });
    let registry = LineageRegistry::new(Arc::new(RefusingOpener));
    registry.set_session_address_resolver(
            store.clone() as Arc<dyn broca_store::SessionAddressResolver>
        );
    (Arc::new(registry), store, ReleaseBindRead(Some(release)))
}

struct ReleaseBindRead(Option<std::sync::mpsc::Sender<()>>);
impl Drop for ReleaseBindRead {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

struct BindIngress {
    routes: Routes,
    registry: Arc<LineageRegistry>,
    handler: Arc<MockSubscribeHandler>,
    writer: EgressSender<Frame>,
    written: EgressReceiver<Frame>,
    sinks: mpsc::Receiver<mpsc::Sender<Delivery>>,
    requests: JoinSet<Result<(), ServeIoError>>,
    cancel: CancellationToken,
}

impl BindIngress {
    fn new(registry: Arc<LineageRegistry>) -> Self {
        let (writer, written) = broca_diagnostics::channel(16, Arc::default());
        let (sink_tx, sinks) = mpsc::channel(4);
        Self {
            routes: Routes::new(true),
            registry,
            handler: Arc::new(MockSubscribeHandler {
                sink_tx,
                refuse_session: std::sync::atomic::AtomicBool::new(false),
                read_checks: Default::default(),
            }),
            writer,
            written,
            sinks,
            requests: JoinSet::new(),
            cancel: CancellationToken::new(),
        }
    }

    async fn frame(&mut self, frame: Frame) {
        handle_frame(
            frame,
            &self.writer,
            &mut self.routes,
            &self.handler,
            &self.registry,
            &VaultGauge::not_in_use(),
            None,
            &self.cancel,
            &Arc::new(Semaphore::new(4)),
            &mut self.requests,
            None,
        )
        .await
        .unwrap();
    }

    async fn bind(&mut self, epoch: u32, session: &str, stamped: bool) {
        let owner = subc_protocol::Principal::Reserved {
            module_id: "agent".into(),
        };
        let bind = ModuleControlRequest::RouteBind {
            route_channel: 7,
            epoch,
            target: subc_protocol::RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new("runner", session),
            principal: Some(owner.clone()),
            consumer_capabilities: None,
            admission_facts: None,
            scope: stamped.then(|| {
                subc_protocol::scope::ScopeStamp::new(
                    owner,
                    "head",
                    1,
                    subc_protocol::scope::ScopeKind::Head,
                )
                .with_owner_authorized(true)
            }),
            role_versions: None,
            call_mode: None,
        };
        self.frame(
            Frame::build(
                FrameType::Request,
                control_flags(),
                0,
                0,
                u64::from(epoch),
                serde_json::to_vec(&bind).unwrap(),
            )
            .unwrap(),
        )
        .await;
        if !stamped {
            self.complete_bind().await;
        }
    }

    async fn complete_bind(&mut self) {
        let completed = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.routes.bind_tasks.join_next(),
        )
        .await
        .expect("bind authorization did not finish")
        .unwrap()
        .unwrap();
        finish_bind(completed, &self.writer, &mut self.routes)
            .await
            .unwrap();
    }

    async fn goodbye(&mut self, epoch: u32) {
        self.frame(Frame::build(FrameType::Goodbye, control_flags(), 7, epoch, 0, vec![]).unwrap())
            .await;
    }

    async fn subscribe(&mut self, epoch: u32, corr: u64) -> mpsc::Sender<Delivery> {
        let sink = self.subscribe_sink(epoch, corr).await;
        self.assert_delivery(&sink, epoch, corr).await;
        sink
    }

    async fn subscribe_sink(&mut self, epoch: u32, corr: u64) -> mpsc::Sender<Delivery> {
        self.frame(
            Frame::build(
                FrameType::Request,
                control_flags(),
                7,
                epoch,
                corr,
                serde_json::to_vec(&RouteRequest {
                    method: OP_SUBSCRIBE.into(),
                    params: serde_json::json!({}),
                })
                .unwrap(),
            )
            .unwrap(),
        )
        .await;
        tokio::time::timeout(std::time::Duration::from_secs(5), self.sinks.recv())
            .await
            .expect("subscription did not reach the handler")
            .unwrap()
    }

    async fn assert_delivery(&mut self, sink: &mpsc::Sender<Delivery>, epoch: u32, corr: u64) {
        sink.send(Delivery::Display(Arc::new(
            broca_protocol::display::DisplayEvent::DisplayReset,
        )))
        .await
        .expect("the original subscription was canceled");
        let delivered = next_written(&mut self.written).await;
        assert_eq!(
            (
                delivered.header.ty,
                delivered.header.epoch,
                delivered.header.corr
            ),
            (FrameType::StreamData, epoch, corr)
        );
    }
}

impl Drop for BindIngress {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

fn held_bind_registry() -> (BindIngress, Arc<HeldBindRead>, ReleaseBindRead) {
    let (registry, store, release) = held_bind_registry_answering(None);
    (BindIngress::new(registry), store, release)
}

#[tokio::test]
async fn pending_generation_goodbye_preserves_published_route_and_stream() {
    let (mut ingress, store, release) = held_bind_registry();
    ingress.bind(1, "published", false).await;
    assert_eq!(next_written(&mut ingress.written).await.header.corr, 1);
    let sink = ingress.subscribe(1, 10).await;
    ingress.bind(2, "pending", true).await;
    store.entered.notified().await;
    ingress.goodbye(2).await;
    assert!(!ingress.routes.pending_binds.contains_key(&7));
    assert_eq!(ingress.routes.by_channel[&7].0, 1);
    ingress.assert_delivery(&sink, 1, 10).await;
    drop(release);
    ingress.complete_bind().await;
    assert_eq!(ingress.routes.by_channel[&7].0, 1);
    assert!(
        ingress.written.try_recv().is_err(),
        "canceled bind acknowledged"
    );
}

#[tokio::test]
async fn published_generation_goodbye_preserves_pending_bind() {
    let (mut ingress, store, release) = held_bind_registry();
    ingress.bind(1, "published", false).await;
    next_written(&mut ingress.written).await;
    let _sink = ingress.subscribe(1, 10).await;
    let slot = ingress.routes.subscribe_cancel[&7].clone();
    ingress.bind(2, "pending", true).await;
    store.entered.notified().await;
    let pending = ingress.routes.pending_binds[&7].1.clone();
    ingress.goodbye(1).await;
    assert!(slot.route.is_cancelled());
    assert!(!ingress.routes.by_channel.contains_key(&7));
    assert_eq!(ingress.routes.pending_binds[&7].0, 2);
    assert!(!pending.is_cancelled());
    drop(release);
    ingress.complete_bind().await;
    assert_eq!(ingress.routes.by_channel[&7].0, 2);
    let mut ack = next_written(&mut ingress.written).await;
    if ack.header.corr == 10 {
        assert_eq!((ack.header.ty, ack.header.epoch), (FrameType::StreamEnd, 1));
        ack = next_written(&mut ingress.written).await;
    }
    assert_eq!((ack.header.ty, ack.header.corr), (FrameType::Response, 2));
}

#[tokio::test]
async fn accepted_replacement_makes_old_generation_goodbye_stale() {
    let (mut ingress, store, release) = held_bind_registry();
    ingress.bind(1, "published", false).await;
    next_written(&mut ingress.written).await;
    ingress.bind(2, "replacement", true).await;
    store.entered.notified().await;
    drop(release);
    ingress.complete_bind().await;
    assert_eq!(next_written(&mut ingress.written).await.header.corr, 2);
    let sink = ingress.subscribe(2, 20).await;
    ingress.goodbye(1).await;
    assert_eq!(ingress.routes.by_channel[&7].0, 2);
    ingress.assert_delivery(&sink, 2, 20).await;
}

/// Publishing a replacement generation ends the stream the replaced generation opened, whose
/// own GOODBYE is ignored from then on, while a stream opened under the replacement lives on.
#[tokio::test]
async fn accepted_replacement_retires_the_old_generation_stream() {
    let (mut ingress, store, release) = held_bind_registry();
    ingress.bind(1, "published", false).await;
    next_written(&mut ingress.written).await;
    let old = ingress.subscribe(1, 10).await;
    ingress.bind(2, "replacement", true).await;
    store.entered.notified().await;
    drop(release);
    ingress.complete_bind().await;
    // A cancelled stream's StreamEnd is best-effort and may precede the acknowledgement.
    let ack = loop {
        let frame = next_written(&mut ingress.written).await;
        if (frame.header.ty, frame.header.epoch) != (FrameType::StreamEnd, 1) {
            break frame;
        }
    };
    assert_eq!((ack.header.ty, ack.header.corr), (FrameType::Response, 2));
    // The forwarder drops its delivery receiver only when it ends.
    tokio::time::timeout(std::time::Duration::from_secs(5), old.closed())
        .await
        .expect("the replaced generation's stream must end when the replacement is published");
    while let Ok(frame) = ingress.written.try_recv() {
        assert_eq!(
            (frame.header.ty, frame.header.epoch, frame.header.corr),
            (FrameType::StreamEnd, 1, 10)
        );
    }
    let sink = ingress.subscribe(2, 20).await;
    ingress.goodbye(1).await;
    assert_eq!(ingress.routes.by_channel[&7].0, 2);
    ingress.assert_delivery(&sink, 2, 20).await;
}

#[tokio::test]
async fn refused_replacement_goodbye_preserves_published_route_and_stream() {
    // The session's address was reserved by another scope, so the stamped replacement's
    // adoption check refuses it.
    let (registry, store, release) =
        held_bind_registry_answering(Some(Ok(broca_store::session_address::ScopeReservation {
            owner: broca_protocol::BindPrincipal::Reserved {
                module_id: "someone-else".into(),
            },
            scope_ref: "other".into(),
        })));
    let mut ingress = BindIngress::new(registry);
    ingress.bind(1, "published", false).await;
    next_written(&mut ingress.written).await;
    let sink = ingress.subscribe(1, 10).await;
    ingress.bind(2, "published", true).await;
    store.entered.notified().await;
    drop(release);
    ingress.complete_bind().await;
    let refused = next_written(&mut ingress.written).await;
    assert_eq!(
        (refused.header.ty, refused.header.corr),
        (FrameType::Error, 2)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&refused.body).unwrap()["code"],
        "scope_owner_mismatch"
    );
    ingress.goodbye(2).await;
    assert_eq!(ingress.routes.by_channel[&7].0, 1);
    ingress.assert_delivery(&sink, 1, 10).await;
}

#[tokio::test]
async fn stamped_bind_with_unreadable_history_is_refused_not_published() {
    let (registry, _store, release) =
        held_bind_registry_answering(Some(Err("the inventory is unreadable".into())));
    drop(release);
    let mut ingress = BindIngress::new(registry);
    ingress.bind(1, "unreadable", true).await;
    ingress.complete_bind().await;
    let refused = next_written(&mut ingress.written).await;
    assert_eq!(
        (refused.header.ty, refused.header.corr),
        (FrameType::Error, 1)
    );
    let body = serde_json::from_slice::<serde_json::Value>(&refused.body).unwrap();
    assert_eq!(body["code"], "transient", "{body}");
    assert_eq!(body["detail"]["class"], "transient", "{body}");
    assert_eq!(body["detail"]["cause"], "open_failed", "{body}");
    assert!(
        !ingress.routes.by_channel.contains_key(&7),
        "a bind whose adoption check could not run must not be published"
    );
}

#[tokio::test]
async fn request_during_stamped_bind_authorization_is_unbound() {
    let (mut ingress, store, release) = held_bind_registry();
    ingress.bind(2, "pending", true).await;
    store.entered.notified().await;
    ingress
        .frame(
            Frame::build(
                FrameType::Request,
                control_flags(),
                7,
                2,
                20,
                serde_json::to_vec(&RouteRequest {
                    method: OP_SUBSCRIBE.into(),
                    params: serde_json::json!({}),
                })
                .unwrap(),
            )
            .unwrap(),
        )
        .await;
    assert!(ingress.routes.by_channel.is_empty());
    assert_eq!(ingress.routes.pending_binds[&7].0, 2);
    assert!(ingress.routes.subscribe_cancel.is_empty());
    assert!(
        ingress.requests.is_empty(),
        "unbound request dispatched to a handler task"
    );
    assert!(ingress.sinks.try_recv().is_err());
    assert!(ingress.written.try_recv().is_err());
    drop(release);
    ingress.complete_bind().await;
    assert_eq!(next_written(&mut ingress.written).await.header.corr, 2);
    assert!(
        ingress.requests.is_empty(),
        "unbound request was deferred until ack"
    );
    assert!(ingress.sinks.try_recv().is_err());
}

#[tokio::test]
async fn superseded_bind_late_authorization_preserves_replacement() {
    let (mut ingress, store, release) = held_bind_registry();
    ingress.bind(1, "superseded", true).await;
    store.entered.notified().await;
    ingress.bind(2, "replacement", false).await;
    assert_eq!(next_written(&mut ingress.written).await.header.corr, 2);
    let sink = ingress.subscribe(2, 20).await;
    drop(release);
    ingress.complete_bind().await;
    assert_eq!(ingress.routes.by_channel[&7].0, 2);
    assert_eq!(ingress.routes.by_channel[&7].1.session, "replacement");
    assert!(
        ingress.written.try_recv().is_err(),
        "superseded bind acknowledged"
    );
    ingress.assert_delivery(&sink, 2, 20).await;
}

#[tokio::test]
async fn subscribe_display_deltas_do_not_recheck_read_admission() {
    let registry = Arc::new(test_registry(LineageRegistry::new(Arc::new(
        RefusingOpener,
    ))));
    let mut ingress = BindIngress::new(registry);
    ingress.bind(1, "stored", false).await;
    next_written(&mut ingress.written).await;
    let sink = ingress.subscribe(1, 10).await;
    let boundary = |step| {
        Delivery::Control(Arc::new((
            broca_core::Cursor::new(step, 0),
            broca_protocol::caller::ControlUnit::StepStarted {
                step_id: broca_protocol::StepId(step),
            },
            None,
        )))
    };
    sink.send(boundary(1)).await.unwrap();
    assert_eq!(
        next_written(&mut ingress.written).await.header.ty,
        FrameType::StreamData
    );
    let delta = Delivery::Display(Arc::new(broca_protocol::display::DisplayEvent::TextDelta {
        mid: None,
        part: None,
        message_id: broca_protocol::MessageId("reply".into()),
        tool_call_id: None,
        delta: "text".into(),
    }));
    for _ in 0..64 {
        sink.send(delta.clone()).await.unwrap();
        assert_eq!(
            next_written(&mut ingress.written).await.header.ty,
            FrameType::StreamData
        );
    }
    sink.send(boundary(2)).await.unwrap();
    assert_eq!(
        next_written(&mut ingress.written).await.header.ty,
        FrameType::StreamData
    );
    assert_eq!(
        ingress.handler.read_checks.load(Ordering::Relaxed),
        4,
        "admission, first display, and two control boundaries"
    );
}

#[tokio::test]
async fn subscribe_ends_on_read_access_revocation() {
    let registry = Arc::new(test_registry(LineageRegistry::new(Arc::new(
        RefusingOpener,
    ))));
    let mut ingress = BindIngress::new(registry);
    ingress.bind(1, "stored", false).await;
    next_written(&mut ingress.written).await;
    let sink = ingress.subscribe(1, 10).await;
    ingress
        .handler
        .refuse_session
        .store(true, Ordering::Relaxed);
    sink.send(Delivery::AdmissionChanged).await.unwrap();
    let terminal = next_written(&mut ingress.written).await;
    assert_eq!(
        (terminal.header.ty, terminal.header.corr),
        (FrameType::Error, 10)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&terminal.body).unwrap()["code"],
        "sender_not_owner"
    );
    broca_test_support::wait_for(
        "revoked subscribe drops its delivery sink",
        sink.closed(),
        || "still open".into(),
    )
    .await;
}

#[tokio::test]
async fn subscribe_rechecks_access_before_its_first_display() {
    let registry = Arc::new(test_registry(LineageRegistry::new(Arc::new(
        RefusingOpener,
    ))));
    let mut ingress = BindIngress::new(registry);
    ingress.bind(1, "stored", false).await;
    next_written(&mut ingress.written).await;
    let sink = ingress.subscribe_sink(1, 10).await;
    assert_eq!(ingress.handler.read_checks.load(Ordering::Relaxed), 1);
    // The read policy changed between the open check and attachment, so no policy marker is
    // visible to this subscriber. Its first display must not be forwarded under the old policy.
    ingress
        .handler
        .refuse_session
        .store(true, Ordering::Relaxed);
    sink.send(Delivery::Display(Arc::new(
        broca_protocol::display::DisplayEvent::DisplayReset,
    )))
    .await
    .unwrap();
    let terminal = next_written(&mut ingress.written).await;
    assert_eq!(
        (terminal.header.ty, terminal.header.corr),
        (FrameType::Error, 10)
    );
    assert_eq!(
        serde_json::from_slice::<ErrorBody>(&terminal.body)
            .unwrap()
            .code,
        "sender_not_owner"
    );
}

#[test]
fn refused_send_logs_history_rewrite_and_context_ceiling_classes() {
    let history = ServeError::HistoryRewriting;
    let ceiling = ServeError::ContextCeiling(broca_protocol::ContextCeilingDetail::new(
        100,
        90,
        "view_timeout",
    ));
    assert_eq!(
        [refused_send_class(&history), refused_send_class(&ceiling)],
        ["transient", "context_overflow"]
    );
}

#[tokio::test]
async fn unauthorized_subscribe_reports_authorization_before_invalid_params() {
    let registry = Arc::new(test_registry(LineageRegistry::new(Arc::new(
        RefusingOpener,
    ))));
    let mut ingress = BindIngress::new(registry);
    ingress.bind(1, "published", false).await;
    next_written(&mut ingress.written).await;
    ingress
        .handler
        .refuse_session
        .store(true, Ordering::Relaxed);
    for (corr, params) in [
        (20, serde_json::json!({"unknown": true})),
        (21, serde_json::json!({"from": "not-an-attach-point"})),
    ] {
        ingress
            .frame(
                Frame::build(
                    FrameType::Request,
                    control_flags(),
                    7,
                    1,
                    corr,
                    serde_json::to_vec(&RouteRequest {
                        method: OP_SUBSCRIBE.into(),
                        params,
                    })
                    .unwrap(),
                )
                .unwrap(),
            )
            .await;
        let refused = next_written(&mut ingress.written).await;
        assert_eq!(
            (refused.header.ty, refused.header.corr),
            (FrameType::Error, corr)
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&refused.body).unwrap()["code"],
            "sender_not_owner"
        );
    }
    assert!(ingress.sinks.try_recv().is_err());
}

#[tokio::test]
async fn unauthorized_replacement_subscribe_preserves_original_stream_correlation() {
    let registry = Arc::new(test_registry(LineageRegistry::new(Arc::new(
        RefusingOpener,
    ))));
    let mut ingress = BindIngress::new(registry);
    ingress.bind(1, "published", false).await;
    next_written(&mut ingress.written).await;
    let sink = ingress.subscribe(1, 10).await;
    let original = ingress.routes.subscribe_cancel[&7]
        .current
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    ingress
        .handler
        .refuse_session
        .store(true, Ordering::Relaxed);
    ingress
        .frame(
            Frame::build(
                FrameType::Request,
                control_flags(),
                7,
                1,
                20,
                serde_json::to_vec(&RouteRequest {
                    method: OP_SUBSCRIBE.into(),
                    params: serde_json::json!({}),
                })
                .unwrap(),
            )
            .unwrap(),
        )
        .await;
    let refused = next_written(&mut ingress.written).await;
    assert_eq!(
        (refused.header.ty, refused.header.corr),
        (FrameType::Error, 20)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&refused.body).unwrap()["code"],
        "sender_not_owner"
    );
    assert!(!original.is_cancelled());
    assert!(
        ingress.sinks.try_recv().is_err(),
        "unauthorized replacement reached subscribe"
    );
    ingress.assert_delivery(&sink, 1, 10).await;
}

#[tokio::test]
async fn authorized_invalid_subscribe_preserves_original_stream_correlation() {
    let registry = Arc::new(test_registry(LineageRegistry::new(Arc::new(
        RefusingOpener,
    ))));
    let mut ingress = BindIngress::new(registry);
    ingress.bind(1, "published", false).await;
    next_written(&mut ingress.written).await;
    let sink = ingress.subscribe(1, 10).await;
    ingress
        .frame(
            Frame::build(
                FrameType::Request,
                control_flags(),
                7,
                1,
                20,
                serde_json::to_vec(&RouteRequest {
                    method: OP_SUBSCRIBE.into(),
                    params: serde_json::json!({"unknown": true}),
                })
                .unwrap(),
            )
            .unwrap(),
        )
        .await;
    let invalid = next_written(&mut ingress.written).await;
    assert_eq!(
        (invalid.header.ty, invalid.header.corr),
        (FrameType::Error, 20)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&invalid.body).unwrap()["code"],
        "invalid_params"
    );
    assert!(ingress.sinks.try_recv().is_err());
    ingress.assert_delivery(&sink, 1, 10).await;
}

#[tokio::test]
async fn reader_answers_ping_and_goodbye_while_bind_ownership_read_is_held() {
    let (registry, store, release) = held_bind_registry_answering(None);
    let (mut daemon, module) = tokio::io::duplex(64 * 1024);
    let (reader, writer) = tokio::io::split(module);
    let serve = tokio::spawn(serve_connection(
        reader,
        writer,
        "broca",
        registry.clone(),
        registry,
        VaultGauge::not_in_use(),
    ));
    bind_test_route(&mut daemon, "slow-bind").await;
    let owner = subc_protocol::Principal::Reserved {
        module_id: "agent".into(),
    };
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 2,
        target: subc_protocol::RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new("runner", "slow-bind"),
        principal: Some(owner.clone()),
        consumer_capabilities: None,
        admission_facts: None,
        scope: Some(
            subc_protocol::scope::ScopeStamp::new(
                owner,
                "head",
                1,
                subc_protocol::scope::ScopeKind::Head,
            )
            .with_owner_authorized(true),
        ),
        role_versions: None,
        call_mode: None,
    };
    write_frame(
        &mut daemon,
        &Frame::build(
            FrameType::Request,
            control_flags(),
            0,
            0,
            3,
            serde_json::to_vec(&bind).unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    store.entered.notified().await;
    write_frame(
        &mut daemon,
        &Frame::build(FrameType::Ping, control_flags(), 0, 0, 4, vec![]).unwrap(),
    )
    .await
    .unwrap();
    let pong = tokio::time::timeout(std::time::Duration::from_secs(5), read_frame(&mut daemon))
        .await
        .expect("held bind read blocked health ingress")
        .unwrap()
        .unwrap();
    assert_eq!((pong.header.ty, pong.header.corr), (FrameType::Pong, 4));
    write_frame(
        &mut daemon,
        &Frame::build(FrameType::Goodbye, control_flags(), 0, 0, 5, vec![]).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(serve.await.unwrap().unwrap(), ServeEnd::Goodbye);
    drop(release);
    store.finished.notified().await;
}

#[tokio::test]
async fn pending_bind_goodbye_cannot_publish_a_late_ack() {
    let mut routes = Routes::new(true);
    let cancel = CancellationToken::new();
    routes.pending_binds.insert(7, (1, cancel.clone()));
    let goodbye = Frame::build(FrameType::Goodbye, control_flags(), 7, 1, 0, vec![]).unwrap();
    let (writer, mut written) = broca_diagnostics::channel::<Frame>(4, Arc::default());
    let registry = Arc::new(test_registry(LineageRegistry::new(Arc::new(
        RefusingOpener,
    ))));
    handle_frame(
        goodbye,
        &writer,
        &mut routes,
        &registry,
        &registry,
        &VaultGauge::not_in_use(),
        None,
        &CancellationToken::new(),
        &Arc::new(Semaphore::new(1)),
        &mut JoinSet::new(),
        None,
    )
    .await
    .unwrap();
    let header = Frame::build(FrameType::Request, control_flags(), 0, 0, 2, vec![])
        .unwrap()
        .header;
    finish_bind(
        BindCompletion {
            role_refusal: None,
            address_refusal: None,
            header,
            channel: 7,
            epoch: 1,
            identity: BrocaBindIdentity::new("/tmp/project-a", "runner", "late-bind"),
            bind: BindObservation {
                management: true,
                agent_data: false,
                role: RouteRole::LlmRunner,
                project_id: None,
                principal: None,
                reader_principal: None,
                scope: broca_protocol::SendScope::Unstamped,
                stamped_project: None,
                consumer_format: Default::default(),
            },
            cancel,
            refusal: Ok(None),
        },
        &writer,
        &mut routes,
    )
    .await
    .unwrap();
    assert!(routes.by_channel.is_empty());
    assert!(
        written.try_recv().is_err(),
        "a canceled bind must not publish an ack"
    );
}

struct ResetWrite<W> {
    inner: W,
    reset: Arc<std::sync::atomic::AtomicBool>,
    observed: Arc<tokio::sync::Notify>,
}

impl<W: AsyncWrite + Unpin> AsyncWrite for ResetWrite<W> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.reset.load(Ordering::SeqCst) {
            self.observed.notify_one();
            return std::task::Poll::Ready(Err(std::io::ErrorKind::ConnectionReset.into()));
        }
        std::pin::Pin::new(&mut self.inner).poll_write(cx, bytes)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

async fn bind_test_route(daemon: &mut tokio::io::DuplexStream, session: &str) {
    let hello = read_frame(daemon).await.unwrap().unwrap();
    write_frame(
        daemon,
        &Frame::build(
            FrameType::HelloAck,
            control_flags(),
            0,
            0,
            hello.header.corr,
            serde_json::to_vec(&ModuleHelloAckBody {
                negotiated_ver: PROTOCOL_VERSION,
                subc_ops: vec![],
                subc_capabilities: vec![],
                storage: None,
                machine_id: None,
            })
            .unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: subc_protocol::RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new("runner", session),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_frame(
        daemon,
        &Frame::build(
            FrameType::Request,
            control_flags(),
            0,
            0,
            2,
            serde_json::to_vec(&bind).unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        read_frame(daemon).await.unwrap().unwrap().header.ty,
        FrameType::Response
    );
}

async fn test_request(daemon: &mut tokio::io::DuplexStream, method: &str, corr: u64) {
    write_frame(
        daemon,
        &Frame::build(
            FrameType::Request,
            control_flags(),
            7,
            1,
            corr,
            serde_json::to_vec(&RouteRequest {
                method: method.into(),
                params: serde_json::Value::Null,
            })
            .unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn oversized_tool_response_is_call_local() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (writer, mut written) =
                broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, Arc::default());
            let large = serde_json::to_vec(&serde_json::json!({
                "model":"typesafe/jev-1.13.0",
                "answers":{"q":{"type":"noul","noul":0.5,"provider_extra":{"text":"x".repeat(broca_wire::SUBC_MAX_FRAME_BYTES as usize)}}},
                "usage":{"input_tokens":0},"cost_usd":0,
            })).unwrap();
            let result: cortexkit_llm_tools::classify::Result = serde_json::from_slice(&large).unwrap();
            cortexkit_llm_tools::classify::validate_result(&result).unwrap();
            send_response_bytes(
                &writer,
                PROTOCOL_VERSION,
                1,
                1,
                1,
                large,
            )
            .await
            .unwrap();
            send_response_bytes(&writer, PROTOCOL_VERSION, 1, 1, 2, b"{}".to_vec())
                .await
                .unwrap();
            let refused = next_written(&mut written).await;
            assert_eq!(refused.header.ty, FrameType::Error);
            assert_eq!(refused.header.corr, 1);
            let error: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
            assert_eq!(error["code"], "response_too_large");
            let sibling = next_written(&mut written).await;
            assert_eq!(sibling.header.ty, FrameType::Response);
            assert_eq!(sibling.header.corr, 2);
            assert_eq!(sibling.body, b"{}");
        })
        .await
        .expect("the tool reply writer did not finish within its bound");
}

#[test]
fn reply_frame_size_uses_the_real_header_length_and_result_envelope() {
    let result = serde_json::json!({"answer":"ok"});
    let envelope = serde_json::to_vec(&serde_json::json!({"result":result})).unwrap();
    assert_eq!(
        broca_wire::serialized_result_frame_bytes(&result),
        envelope.len() as u64 + subc_protocol::HEADER_LEN as u64
    );
    assert_eq!(
        broca_wire::SUBC_FRAME_HEADER_BYTES,
        subc_protocol::HEADER_LEN as u64
    );
    assert_eq!(broca_wire::SUBC_MAX_FRAME_BYTES, 4_194_304);
}

#[tokio::test]
async fn oversized_response_fails_only_its_call_and_sibling_completes() {
    for session in ["oversized-response", "oversized-error-response"] {
        let (mut daemon, module) = tokio::io::duplex(64 * 1024);
        let (reader, writer) = tokio::io::split(module);
        let handler = Arc::new(GatedStatusHandler {
            gate_auth: false,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let serve = tokio::spawn(serve_connection(
            reader,
            writer,
            "broca",
            handler,
            Arc::new(test_registry(LineageRegistry::new(Arc::new(
                RefusingOpener,
            )))),
            VaultGauge::not_in_use(),
        ));
        bind_test_route(&mut daemon, session).await;
        test_request(&mut daemon, OP_STATUS, 3).await;
        test_request(&mut daemon, OP_ROLE_DESCRIBE, 4).await;
        let error = read_frame(&mut daemon)
            .await
            .unwrap()
            .expect("oversized call lost the connection");
        assert_eq!((error.header.ty, error.header.corr), (FrameType::Error, 3));
        assert_eq!(
            serde_json::from_slice::<ErrorBody>(&error.body)
                .unwrap()
                .code,
            "response_too_large"
        );
        let sibling = read_frame(&mut daemon)
            .await
            .unwrap()
            .expect("sibling lost the connection");
        assert_eq!(
            (sibling.header.ty, sibling.header.corr),
            (FrameType::Response, 4)
        );
        write_frame(
            &mut daemon,
            &Frame::build(FrameType::Goodbye, control_flags(), 0, 0, 5, vec![]).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(serve.await.unwrap().unwrap(), ServeEnd::Goodbye);
    }
}

#[tokio::test]
async fn write_half_reset_is_classified_even_with_a_request_in_flight() {
    let (mut daemon, module) = tokio::io::duplex(64 * 1024);
    let (reader, writer) = tokio::io::split(module);
    let reset = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = Arc::new(tokio::sync::Notify::new());
    let handler = Arc::new(GatedStatusHandler {
        gate_auth: false,
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let serve = tokio::spawn(serve_connection(
        reader,
        ResetWrite {
            inner: writer,
            reset: reset.clone(),
            observed: observed.clone(),
        },
        "broca",
        handler.clone(),
        Arc::new(test_registry(LineageRegistry::new(Arc::new(
            RefusingOpener,
        )))),
        VaultGauge::not_in_use(),
    ));
    bind_test_route(&mut daemon, "reset-response").await;
    test_request(&mut daemon, OP_STATUS, 3).await;
    handler.entered.notified().await;
    reset.store(true, Ordering::SeqCst);
    write_frame(
        &mut daemon,
        &Frame::build(FrameType::Ping, control_flags(), 0, 0, 4, vec![]).unwrap(),
    )
    .await
    .unwrap();
    observed.notified().await;
    handler.release.notify_one();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), serve)
        .await
        .expect("write reset left the reader waiting on an unusable connection")
        .unwrap();
    assert_eq!(result.unwrap(), ServeEnd::PeerReset);
}

#[tokio::test]
async fn reader_answers_health_ping_during_authorization_and_permit_waits() {
    let (mut daemon, mut module_read) = tokio::io::duplex(64 * 1024);
    let (writer, mut written) = broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, Arc::default());
    let handler = Arc::new(GatedStatusHandler {
        gate_auth: true,
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let loop_handler = handler.clone();
    let serve = tokio::spawn(async move {
        let registry = test_registry(LineageRegistry::new(Arc::new(RefusingOpener)));
        run_frame_loop(
            &mut module_read,
            writer,
            "broca",
            loop_handler,
            &registry,
            &VaultGauge::not_in_use(),
            None,
            CancellationToken::new(),
            &crate::consumer_routes::ConsumerRoutes::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
    });
    let hello = next_written(&mut written).await;
    write_frame(
        &mut daemon,
        &Frame::build(
            FrameType::HelloAck,
            control_flags(),
            0,
            0,
            hello.header.corr,
            serde_json::to_vec(&ModuleHelloAckBody {
                negotiated_ver: PROTOCOL_VERSION,
                subc_ops: vec![],
                subc_capabilities: vec![],
                storage: None,
                machine_id: None,
            })
            .unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    // Separate sessions occupy all permits without depending on task scheduling order.
    for index in 0..=MAX_INFLIGHT_ROUTE_REQUESTS {
        let channel = index as u16 + 1;
        let bind = ModuleControlRequest::RouteBind {
            route_channel: channel,
            epoch: 1,
            target: subc_protocol::RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new("runner", format!("s{index}")),
            principal: None,
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        };
        write_frame(
            &mut daemon,
            &Frame::build(
                FrameType::Request,
                control_flags(),
                0,
                0,
                2,
                serde_json::to_vec(&bind).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            next_written(&mut written).await.header.ty,
            FrameType::Response
        );
        write_frame(
            &mut daemon,
            &Frame::build(
                FrameType::Request,
                control_flags(),
                channel,
                1,
                3,
                serde_json::to_vec(&RouteRequest {
                    method: OP_STATUS.into(),
                    params: serde_json::Value::Null,
                })
                .unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        if index < MAX_INFLIGHT_ROUTE_REQUESTS {
            handler.entered.notified().await;
        }
        // The last request has no permit; earlier requests are inside authorization.
        write_frame(
            &mut daemon,
            &Frame::build(FrameType::Ping, control_flags(), 0, 0, 4, vec![]).unwrap(),
        )
        .await
        .unwrap();
        let pong = next_written(&mut written).await;
        assert_eq!((pong.header.ty, pong.header.corr), (FrameType::Pong, 4));
    }
    write_frame(
        &mut daemon,
        &Frame::build(FrameType::Goodbye, control_flags(), 0, 0, 5, vec![]).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(serve.await.unwrap().unwrap(), ServeEnd::Goodbye);
}

/// A handler whose session-log presence probe blocks its thread until the test releases it, and
/// records whether the release arrived in time.
struct BlockingProbeHandler {
    entered: tokio::sync::Notify,
    release: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    released: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl ServeHandler for BlockingProbeHandler {
    fn session_written(&self, _session: &BrocaBindIdentity) -> Result<bool, ServeError> {
        self.entered.notify_one();
        let release = self.release.lock().unwrap().take().unwrap();
        let released = release
            .recv_timeout(std::time::Duration::from_secs(2))
            .is_ok();
        self.released.store(released, Ordering::SeqCst);
        Ok(false)
    }
    async fn send(&self, _req: SendRequest) -> Result<SendResponse, ServeError> {
        unimplemented!()
    }
    async fn retract(
        &self,
        _session: &BrocaBindIdentity,
        _submission_id: broca_protocol::SubmissionId,
    ) -> Result<RetractResponse, ServeError> {
        unimplemented!()
    }
    async fn subscribe(
        &self,
        _session: &BrocaBindIdentity,
        _from: SubscribeFrom,
        _sink: mpsc::Sender<Delivery>,
    ) -> Result<(), ServeError> {
        unimplemented!()
    }
    async fn read(&self, _req: SessionReadRequest) -> Result<SessionReadResponse, ServeError> {
        unimplemented!()
    }
    async fn cancel(
        &self,
        _session: &BrocaBindIdentity,
        _run_id: broca_protocol::RunId,
    ) -> Result<CancelResponse, ServeError> {
        unimplemented!()
    }
    async fn status(&self, _session: &BrocaBindIdentity) -> Result<StatusResponse, ServeError> {
        unimplemented!()
    }
    async fn run_status(
        &self,
        _session: &BrocaBindIdentity,
        _run_id: broca_protocol::RunId,
    ) -> Result<RunStatusResponse, ServeError> {
        unimplemented!()
    }
}

mod cancel_admission {
    use super::*;
    include!("serve_cancel_admission_tests.rs");
}

struct RetractPrincipalProbe;

#[async_trait::async_trait]
impl ServeHandler for RetractPrincipalProbe {
    async fn authorize_session_owner(
        &self,
        _: &BrocaBindIdentity,
        _: Option<&broca_protocol::BindPrincipal>,
        _: &broca_protocol::SendScope,
    ) -> Result<(), ServeError> {
        Err(ServeError::ScopeRefused(
            broca_core::ScopeRefusal::SenderNotOwner,
        ))
    }
    async fn authorize_session_retract(
        &self,
        _: &BrocaBindIdentity,
        principal: Option<&broca_protocol::BindPrincipal>,
        _: &broca_protocol::SendScope,
        id: Option<&str>,
    ) -> Result<(), ServeError> {
        assert_eq!(id, Some("pending"));
        if principal
            == Some(&broca_protocol::BindPrincipal::Reserved {
                module_id: "actual".into(),
            })
        {
            Ok(())
        } else {
            Err(ServeError::ScopeRefused(
                broca_core::ScopeRefusal::SenderNotOwner,
            ))
        }
    }
    async fn retract(
        &self,
        _: &BrocaBindIdentity,
        _: broca_protocol::SubmissionId,
    ) -> Result<RetractResponse, ServeError> {
        Ok(RetractResponse::Retracted)
    }
    async fn send(&self, _: SendRequest) -> Result<SendResponse, ServeError> {
        unreachable!()
    }
    async fn subscribe(
        &self,
        _: &BrocaBindIdentity,
        _: SubscribeFrom,
        _: mpsc::Sender<Delivery>,
    ) -> Result<(), ServeError> {
        unreachable!()
    }
    async fn read(&self, _: SessionReadRequest) -> Result<SessionReadResponse, ServeError> {
        unreachable!()
    }
    async fn cancel(
        &self,
        _: &BrocaBindIdentity,
        _: broca_protocol::RunId,
    ) -> Result<CancelResponse, ServeError> {
        unreachable!("run.cancel must remain owner-only")
    }
    async fn status(&self, _: &BrocaBindIdentity) -> Result<StatusResponse, ServeError> {
        unreachable!()
    }
    async fn run_status(
        &self,
        _: &BrocaBindIdentity,
        _: broca_protocol::RunId,
    ) -> Result<RunStatusResponse, ServeError> {
        unreachable!()
    }
}

#[tokio::test]
async fn retract_own_send_uses_only_the_bound_daemon_principal_and_never_extends_run_cancel() {
    let (mut daemon, mut written, serve) =
        bound_frame_loop(Arc::new(RetractPrincipalProbe), "retract-principal").await;
    for (epoch, principal, sender, accepted) in [
        (1, None, "actual", false),
        (
            2,
            Some(subc_protocol::Principal::Reserved {
                module_id: "actual".into(),
            }),
            "forged",
            true,
        ),
        (
            3,
            Some(subc_protocol::Principal::Unverified),
            "actual",
            false,
        ),
        (4, Some(subc_protocol::Principal::Direct), "actual", false),
    ] {
        if epoch > 1 {
            let bind = ModuleControlRequest::RouteBind {
                route_channel: 7,
                epoch,
                target: subc_protocol::RouteTarget::ManagementSurface {
                    module_id: "broca".into(),
                },
                identity: SubcBindIdentity::new("runner", "retract-principal"),
                principal,
                consumer_capabilities: None,
                admission_facts: None,
                scope: None,
                role_versions: None,
                call_mode: None,
            };
            write_frame(
                &mut daemon,
                &Frame::build(
                    FrameType::Request,
                    control_flags(),
                    0,
                    0,
                    20,
                    serde_json::to_vec(&bind).unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(
                next_written(&mut written).await.header.ty,
                FrameType::Response
            );
        }
        for method in [OP_RETRACT, OP_CANCEL] {
            let params = serde_json::json!({"method":method,"params":{"submission_id":"pending","run_id":"running","sender":sender}});
            write_frame(
                &mut daemon,
                &Frame::build(
                    FrameType::Request,
                    control_flags(),
                    7,
                    epoch,
                    21,
                    serde_json::to_vec(&params).unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
            let reply = next_written(&mut written).await;
            assert_eq!(
                reply.header.ty,
                if accepted && method == OP_RETRACT {
                    FrameType::Response
                } else {
                    FrameType::Error
                }
            );
            if reply.header.ty == FrameType::Response {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&reply.body).unwrap()["result"],
                    "retracted"
                );
            }
        }
    }
    drop(daemon);
    serve.await.unwrap().unwrap();
}

/// Start a frame loop serving `handler`, complete its handshake, and bind route channel 7
/// to `session`. Returns the daemon end of the socket, the loop's egress, and the loop.
async fn bound_frame_loop<H: ServeHandler + 'static>(
    handler: Arc<H>,
    session: &str,
) -> (
    tokio::io::DuplexStream,
    EgressReceiver<Frame>,
    tokio::task::JoinHandle<Result<ServeEnd, ServeIoError>>,
) {
    let (mut daemon, mut module_read) = tokio::io::duplex(64 * 1024);
    let (writer, mut written) = broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, Arc::default());
    let serve = tokio::spawn(async move {
        let registry = test_registry(LineageRegistry::new(Arc::new(RefusingOpener)));
        run_frame_loop(
            &mut module_read,
            writer,
            "broca",
            handler,
            &registry,
            &VaultGauge::not_in_use(),
            None,
            CancellationToken::new(),
            &crate::consumer_routes::ConsumerRoutes::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
    });
    let hello = next_written(&mut written).await;
    write_frame(
        &mut daemon,
        &Frame::build(
            FrameType::HelloAck,
            control_flags(),
            0,
            0,
            hello.header.corr,
            serde_json::to_vec(&ModuleHelloAckBody {
                negotiated_ver: PROTOCOL_VERSION,
                subc_ops: vec![],
                subc_capabilities: vec![],
                storage: None,
                machine_id: None,
            })
            .unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: subc_protocol::RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new("runner", session),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_frame(
        &mut daemon,
        &Frame::build(
            FrameType::Request,
            control_flags(),
            0,
            0,
            2,
            serde_json::to_vec(&bind).unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        next_written(&mut written).await.header.ty,
        FrameType::Response
    );
    (daemon, written, serve)
}

/// The send path's store presence probe runs off the runtime worker. On this
/// single-threaded runtime a probe run inline would block the only worker, so the task that
/// releases it could never run.
#[tokio::test]
async fn the_send_path_wal_probe_does_not_block_the_runtime_worker() {
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let handler = Arc::new(BlockingProbeHandler {
        entered: tokio::sync::Notify::new(),
        release: std::sync::Mutex::new(Some(release_rx)),
        released: std::sync::atomic::AtomicBool::new(false),
    });
    let releaser = handler.clone();
    tokio::spawn(async move {
        releaser.entered.notified().await;
        let _ = release_tx.send(());
    });
    let (mut daemon, mut written, serve) = bound_frame_loop(handler.clone(), "probe").await;
    write_frame(
        &mut daemon,
        &Frame::build(
            FrameType::Request,
            control_flags(),
            7,
            1,
            3,
            serde_json::to_vec(&RouteRequest {
                method: OP_SEND.into(),
                params: serde_json::json!({"prompt": "steer me", "delivery": "steer"}),
            })
            .unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    // The session was never written, so the model-less steer is refused for its model.
    let reply = next_written(&mut written).await;
    assert_eq!((reply.header.ty, reply.header.corr), (FrameType::Error, 3));
    assert!(
        handler.released.load(Ordering::SeqCst),
        "the log presence probe must not run on the runtime worker"
    );
    serve.abort();
}

/// A request that has already sent its reply and then panics is not answered again: the
/// daemon sees exactly one terminal frame for the correlation, never an orphan Error that
/// could land on a newer request reusing it.
#[tokio::test]
async fn a_request_that_panics_after_replying_sends_one_terminal() {
    let (writer, mut written) = broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, Arc::default());
    let task_writer = writer.clone();
    let result = reply_on_panic(writer, (PROTOCOL_VERSION, 7, 1, 3), async move {
        send_json_response(
            &task_writer,
            PROTOCOL_VERSION,
            7,
            1,
            3,
            &serde_json::json!({"ok": true}),
        )
        .await?;
        if true {
            panic!("a bug after the reply");
        }
        Ok(())
    })
    .await;
    assert!(result.is_ok());
    let reply = next_written(&mut written).await;
    assert_eq!(
        (reply.header.ty, reply.header.corr),
        (FrameType::Response, 3)
    );
    assert!(
        written.try_recv().is_err(),
        "a request answers once: no second terminal after its reply"
    );
}

/// A panic inside one request's task is confined to that request: the request is answered
/// on its own correlation with a transient refusal carrying a fixed message, and the
/// connection keeps answering frames and ends normally on GOODBYE.
#[tokio::test]
async fn a_panicking_request_does_not_end_the_connection() {
    let (mut daemon, mut module_read) = tokio::io::duplex(64 * 1024);
    let (writer, mut written) = broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, Arc::default());
    let handler = Arc::new(GatedStatusHandler {
        gate_auth: false,
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let serve = tokio::spawn(async move {
        let registry = test_registry(LineageRegistry::new(Arc::new(RefusingOpener)));
        run_frame_loop(
            &mut module_read,
            writer,
            "broca",
            handler,
            &registry,
            &VaultGauge::not_in_use(),
            None,
            CancellationToken::new(),
            &crate::consumer_routes::ConsumerRoutes::default(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
    });
    let hello = next_written(&mut written).await;
    write_frame(
        &mut daemon,
        &Frame::build(
            FrameType::HelloAck,
            control_flags(),
            0,
            0,
            hello.header.corr,
            serde_json::to_vec(&ModuleHelloAckBody {
                negotiated_ver: PROTOCOL_VERSION,
                subc_ops: vec![],
                subc_capabilities: vec![],
                storage: None,
                machine_id: None,
            })
            .unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: subc_protocol::RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new("runner", "panicking-status"),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_frame(
        &mut daemon,
        &Frame::build(
            FrameType::Request,
            control_flags(),
            0,
            0,
            2,
            serde_json::to_vec(&bind).unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        next_written(&mut written).await.header.ty,
        FrameType::Response
    );
    test_request(&mut daemon, OP_STATUS, 3).await;
    write_frame(
        &mut daemon,
        &Frame::build(FrameType::Ping, control_flags(), 0, 0, 4, vec![]).unwrap(),
    )
    .await
    .unwrap();
    // The refusal and the PONG may arrive in either order: collect both and tell them
    // apart by correlation.
    let mut frames = Vec::new();
    while frames.len() < 2 {
        match tokio::time::timeout(std::time::Duration::from_secs(5), written.recv()).await {
            Ok(Some(frame)) => frames.push(frame),
            _ => break,
        }
    }
    let refusal = frames
        .iter()
        .find(|frame| frame.header.corr == 3)
        .expect("the panicked request must be answered on its own correlation");
    assert_eq!(
        (refusal.header.ty, refusal.header.channel),
        (FrameType::Error, 7)
    );
    let pong = frames
        .iter()
        .find(|frame| frame.header.corr == 4)
        .expect("the connection still answers PING");
    let body: ErrorBody = serde_json::from_slice(&refusal.body).unwrap();
    assert_eq!(body.code, "transient");
    assert_eq!(
        body.message,
        ServeError::Transient(REQUEST_PANICKED_MESSAGE.into()).to_string()
    );
    assert!(
        !body.message.contains("a request handler bug"),
        "the panic payload must not reach the caller"
    );
    assert_eq!((pong.header.ty, pong.header.corr), (FrameType::Pong, 4));
    write_frame(
        &mut daemon,
        &Frame::build(FrameType::Goodbye, control_flags(), 0, 0, 5, vec![]).unwrap(),
    )
    .await
    .unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), serve)
        .await
        .expect("the serve loop did not end on GOODBYE")
        .unwrap();
    assert_eq!(
        result.expect("a panicking request must not end the connection with an error"),
        ServeEnd::Goodbye
    );
}

/// The tool plane's `scope.describe` goes out on this registered module connection, only
/// once it has registered, and the daemon's reply is routed back to the describer instead
/// of the ordinary frame handling. Before registration the describer answers "can't tell".
#[tokio::test]
async fn a_scope_describe_rides_the_registered_module_connection() {
    let (mut daemon, mut module_read) = tokio::io::duplex(64 * 1024);
    let (writer_tx, mut written) =
        broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, Arc::default());
    let describer = Arc::new(broca_subc::ModuleScopeDescriber::new());
    let owner = subc_protocol::Principal::Reserved {
        module_id: "prefrontal".into(),
    };
    assert!(
        broca_subc::ScopeDescriber::describe(describer.as_ref(), &owner, "head")
            .await
            .is_err(),
        "no describe before the connection registers"
    );
    let loop_describer = Arc::clone(&describer);
    let handler = Arc::new(GatedStatusHandler {
        gate_auth: false,
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let serve = tokio::spawn(async move {
        let registry = test_registry(LineageRegistry::new(Arc::new(RefusingOpener)));
        let consumer_routes = crate::consumer_routes::ConsumerRoutes::default();
        run_frame_loop(
            &mut module_read,
            writer_tx,
            "broca",
            handler,
            &registry,
            &VaultGauge::not_in_use(),
            None,
            CancellationToken::new(),
            &consumer_routes,
            None,
            None,
            Some(loop_describer.as_ref()),
            None,
            None,
            None,
            None,
        )
        .await
    });

    let hello = next_written(&mut written).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        control_flags(),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut daemon, &ack).await.unwrap();

    let asking = {
        let describer = Arc::clone(&describer);
        tokio::spawn(async move {
            broca_subc::ScopeDescriber::describe(describer.as_ref(), &owner, "head").await
        })
    };
    let request = next_written(&mut written).await;
    assert_eq!(request.header.ty, FrameType::Request);
    assert_eq!(request.header.channel, 0);
    let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["op"], "scope.describe");
    assert_eq!(body["ref"], "head");

    let reply = Frame::build(
        FrameType::Response,
        control_flags(),
        0,
        0,
        request.header.corr,
        serde_json::to_vec(&serde_json::json!({
            "op": "scope.describe",
            "status": "ended",
            "scope_epoch": 4,
            "daemon_incarnation": "d1",
            "owner_synced": true,
            "owner_configured": true,
        }))
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut daemon, &reply).await.unwrap();
    let described = tokio::time::timeout(std::time::Duration::from_secs(5), asking)
        .await
        .expect("the reply reached the describer")
        .unwrap()
        .expect("a described scope");
    assert_eq!(described.status, subc_protocol::scope::ScopeStatus::Ended);
    assert_eq!(described.scope_epoch, Some(4));

    drop(daemon);
    let _ = serve.await;
}

/// A request task finishing while the loop is part-way through reading a frame must not
/// lose the bytes already read.
///
/// Measured 2026-09-26 00:09:26Z (v0.3.132): broca exited 1 with `unsupported envelope
/// version 32`, a space from a JSON body read where a frame header should start. The loop
/// selected between `join_next` and a fresh `read_frame` each turn; `read_frame` is not
/// cancellation-safe, so when `join_next` won while a frame was half-read, the half that
/// had been read was dropped with the future and the next read began mid-frame. This test
/// holds a frame half-written, lets a request task finish, then writes the rest.
#[tokio::test]
async fn a_request_task_finishing_mid_frame_does_not_desync_the_reader() {
    use tokio::io::AsyncWriteExt as _;

    let (mut daemon, mut module_read) = tokio::io::duplex(64 * 1024);
    let (writer_tx, mut written) =
        broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, Arc::default());
    let handler = Arc::new(GatedStatusHandler {
        gate_auth: false,
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let loop_handler = handler.clone();
    let serve = tokio::spawn(async move {
        let registry = test_registry(LineageRegistry::new(Arc::new(RefusingOpener)));
        let consumer_routes = crate::consumer_routes::ConsumerRoutes::default();
        run_frame_loop(
            &mut module_read,
            writer_tx,
            "broca",
            loop_handler,
            &registry,
            &VaultGauge::not_in_use(),
            None,
            CancellationToken::new(),
            &consumer_routes,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
    });

    let hello = next_written(&mut written).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        control_flags(),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut daemon, &ack).await.unwrap();

    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: subc_protocol::RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new("runner", "s1"),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    let bind = Frame::build(
        FrameType::Request,
        control_flags(),
        0,
        0,
        2,
        serde_json::to_vec(&bind).unwrap(),
    )
    .unwrap();
    write_frame(&mut daemon, &bind).await.unwrap();
    assert_eq!(
        next_written(&mut written).await.header.ty,
        FrameType::Response
    );

    // One route request, parked in the handler: this is the task that will finish mid-frame.
    let status = Frame::build(
        FrameType::Request,
        control_flags(),
        7,
        1,
        3,
        serde_json::to_vec(&RouteRequest {
            method: OP_STATUS.into(),
            params: serde_json::Value::Null,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut daemon, &status).await.unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        handler.entered.notified(),
    )
    .await
    .expect("the status request never reached the handler");

    // A health probe whose JSON body opens with spaces, so a reader restarted inside the
    // body sees version 32, the byte the production crash reported. Write the header and
    // the first few body bytes only, and give the loop time to take them into its read.
    let mut probe = vec![b' '; 256];
    probe.extend(serde_json::to_vec(&ModuleControlRequest::HealthCheck {}).unwrap());
    let probe = Frame::build(FrameType::Request, control_flags(), 0, 0, 4, probe).unwrap();
    let mut encoded = Vec::new();
    write_frame(&mut encoded, &probe).await.unwrap();
    let split = subc_protocol::HEADER_LEN + 10;
    daemon.write_all(&encoded[..split]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // The request task finishes now, while the probe is half-read.
    handler.release.notify_one();
    let reply = next_written(&mut written).await;
    assert_eq!((reply.header.channel, reply.header.corr), (7, 3));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    daemon.write_all(&encoded[split..]).await.unwrap();
    let answer = tokio::time::timeout(std::time::Duration::from_secs(5), written.recv()).await;
    drop(daemon);
    let loop_result = tokio::time::timeout(std::time::Duration::from_secs(5), serve)
        .await
        .expect("the serve loop did not end at EOF")
        .unwrap();
    let answer = answer.ok().flatten().unwrap_or_else(|| {
        panic!(
            "the half-read probe was lost when a request task finished mid-frame; \
                 the loop returned {loop_result:?}"
        )
    });
    assert_eq!(
        (answer.header.ty, answer.header.channel, answer.header.corr),
        (FrameType::Response, 0, 4)
    );
    assert!(
        loop_result.is_ok(),
        "EOF after the probe is a normal end, got {loop_result:?}"
    );
}

#[tokio::test]
async fn unbound_and_stale_module_frames_are_counted_without_replies() {
    let (mut daemon, mut module_read) = tokio::io::duplex(64 * 1024);
    let (writer_tx, mut written) =
        broca_diagnostics::channel::<Frame>(EGRESS_BUFFER, Arc::default());
    let handler = Arc::new(GatedStatusHandler {
        gate_auth: false,
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let loop_handler = handler.clone();
    let serve = tokio::spawn(async move {
        let registry = test_registry(LineageRegistry::new(Arc::new(RefusingOpener)));
        let consumer_routes = crate::consumer_routes::ConsumerRoutes::default();
        run_frame_loop(
            &mut module_read,
            writer_tx,
            "broca",
            loop_handler,
            &registry,
            &VaultGauge::not_in_use(),
            None,
            CancellationToken::new(),
            &consumer_routes,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
    });

    let hello = next_written(&mut written).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        control_flags(),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut daemon, &ack).await.unwrap();

    // A new connection reports both serve-loop counters before this test submits any drops.
    // Health also names the session store's deployment key and whether it has loaded.
    crate::store_key::publish(Arc::new(broca_module::store_open::StoreSlot::awaiting_key(
        std::path::Path::new("/unused"),
        7,
    )));
    let health = Frame::build(
        FrameType::Request,
        control_flags(),
        0,
        0,
        2,
        serde_json::to_vec(&ModuleControlRequest::HealthCheck {}).unwrap(),
    )
    .unwrap();
    write_frame(&mut daemon, &health).await.unwrap();
    let health = next_written(&mut written).await;
    assert_eq!(
        (health.header.ty, health.header.channel, health.header.corr),
        (FrameType::Response, 0, 2)
    );
    let health: serde_json::Value = serde_json::from_slice(&health.body).unwrap();
    // The phase counters are process-wide, and other tests in this binary run store work at
    // the same time, so only their shape is pinned here, every value read as zero.
    fn zeroed(value: &serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Number(_) => serde_json::json!(0),
            serde_json::Value::Object(members) => members
                .iter()
                .map(|(name, value)| (name.clone(), zeroed(value)))
                .collect(),
            other => other.clone(),
        }
    }
    let mut store = health["metrics"]["store"].clone();
    store["phases"] = zeroed(&store["phases"]);
    assert_eq!(
        store,
        serde_json::json!({"key_id": 7, "loaded": false, "new_sessions": "encrypted",
                "phases": crate::store_key::empty_phases_golden()}),
        "health.check reports the deployment key's id, that it hasn't loaded, the mode new \
             sessions take, and every phase counter"
    );
    assert!(
        health["metrics"]["scoped_role_route_refusals"].is_object(),
        "health.check includes scoped role-open refusal counts keyed by daemon code"
    );
    assert!(
        health["metrics"]["account_limit_stale_resets"]
            .as_u64()
            .is_some(),
        "health.check always exposes the stale account-reset counter"
    );
    // The retention repair queue served the old log format's deletions; it was removed with
    // that format, so its metric must not come back.
    assert!(
        health["metrics"].get("retention_repairs_pending").is_none(),
        "health.check no longer reports a retention repair backlog"
    );
    assert!(
        health["metrics"]["serve_frames_dropped_unbound_channel"]
            .as_u64()
            .is_some(),
        "health.check includes the unbound-channel counter, zero included"
    );
    assert!(
        health["metrics"]["serve_frames_dropped_stale_epoch"]
            .as_u64()
            .is_some(),
        "health.check includes the stale-epoch counter, zero included"
    );
    for counter in [
        "sessions_same",
        "sessions_differ",
        "sessions_without_workdir",
        "sessions_untracked",
    ] {
        assert!(
            health["metrics"]["scoped_session_workdirs"][counter]
                .as_u64()
                .is_some(),
            "health.check includes the scoped-session workdir counter {counter}, zero included"
        );
    }
    for reason in [
        "incomplete_inventory",
        "gap_marker",
        "store_error",
        "ambiguous",
        "invalid_identity",
    ] {
        assert!(
            health["metrics"]["session_address_refusals"][reason]
                .as_u64()
                .is_some(),
            "health.check includes the address refusal counter {reason}, zero included"
        );
    }

    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: subc_protocol::RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new("runner", "s1"),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    let bind = Frame::build(
        FrameType::Request,
        control_flags(),
        0,
        0,
        3,
        serde_json::to_vec(&bind).unwrap(),
    )
    .unwrap();
    write_frame(&mut daemon, &bind).await.unwrap();
    let bind_ack = next_written(&mut written).await;
    assert_eq!(
        (
            bind_ack.header.ty,
            bind_ack.header.channel,
            bind_ack.header.corr
        ),
        (FrameType::Response, 0, 3)
    );

    let unbound_before = SERVE_FRAMES_DROPPED_UNBOUND_CHANNEL.load(Ordering::Relaxed);
    let stale_before = SERVE_FRAMES_DROPPED_STALE_EPOCH.load(Ordering::Relaxed);
    let status_body = serde_json::to_vec(&RouteRequest {
        method: OP_STATUS.into(),
        params: serde_json::Value::Null,
    })
    .unwrap();
    let unbound = Frame::build(
        FrameType::Request,
        control_flags(),
        11,
        1,
        4,
        status_body.clone(),
    )
    .unwrap();
    let stale = Frame::build(
        FrameType::Request,
        control_flags(),
        7,
        2,
        5,
        status_body.clone(),
    )
    .unwrap();
    let correctly_bound =
        Frame::build(FrameType::Request, control_flags(), 7, 1, 6, status_body).unwrap();
    write_frame(&mut daemon, &unbound).await.unwrap();
    write_frame(&mut daemon, &stale).await.unwrap();
    write_frame(&mut daemon, &correctly_bound).await.unwrap();

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        handler.entered.notified(),
    )
    .await
    .expect("the correct-epoch request did not reach its bound route");
    // The counters are process-wide and other tests in this binary run in parallel, so
    // another test's drop can land between the two reads. Each counter must still have
    // risen by at least this test's one drop of its own cause: counting a drop under the
    // wrong cause, or not at all, leaves one of them unmoved.
    assert!(
        SERVE_FRAMES_DROPPED_UNBOUND_CHANNEL.load(Ordering::Relaxed) > unbound_before,
        "the unbound-channel request was not counted as an unbound-channel drop"
    );
    assert!(
        SERVE_FRAMES_DROPPED_STALE_EPOCH.load(Ordering::Relaxed) > stale_before,
        "the stale-epoch request was not counted as a stale-epoch drop"
    );

    handler.release.notify_one();
    let response = next_written(&mut written).await;
    assert_eq!(
        (
            response.header.ty,
            response.header.channel,
            response.header.corr
        ),
        (FrameType::Error, 7, 6),
        "the two dropped requests must not produce replies, and the bound route must still answer"
    );
    drop(daemon);
    let loop_result = tokio::time::timeout(std::time::Duration::from_secs(5), serve)
        .await
        .expect("the serve loop did not end at EOF")
        .unwrap();
    assert!(loop_result.is_ok(), "EOF is a normal end: {loop_result:?}");
    assert!(
        written.try_recv().is_err(),
        "a dropped request got an extra reply"
    );
}
