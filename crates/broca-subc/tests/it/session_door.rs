//! Fake ENGRAM session.cover/admit/read replies and refusals over authenticated subc frames.
//! Wire provenance: ENGRAM commit 50d65fc70ccc3e0082eaf1bde882ffaf7b2c2bcd. Unlike the real daemon,
//! this loopback stub records consumer identity but does not verify a module launch nonce.

use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use broca_protocol::BindIdentity;
use broca_subc::{session_door::MAX_ADMIT_BYTES, SessionDoorClient, SubcConnection};
use serde_json::{json, Value};
use subc_control::{ClientControlRequest, ClientControlResponse, ConsumerIdentity};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{
    authenticate_server,
    connection_file::{self, ConnectionInfo, Endpoint, SCHEMA_VERSION},
    read_frame, write_frame,
};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::{timeout, Instant},
};

const SESSION_REF: &str = "0123456789abcdef0123456789abcdef";
const CUT_ID: &str = "abcdef0123456789abcdef0123456789";
// SHA-256("abc"), independent of the client's decoding implementation.
const DIGEST: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

enum Answer {
    Result(Value),
    Refuse(Value),
    Envelope(Value),
}

struct FakeDoor {
    file: PathBuf,
    server: JoinHandle<()>,
    opens: Arc<Mutex<Vec<Value>>>,
    requests: Arc<Mutex<Vec<(Instant, Value)>>>,
}

impl Drop for FakeDoor {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_file(&self.file);
    }
}

impl FakeDoor {
    async fn start(answers: Vec<Answer>) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let file = broca_test_support::scratch_root().join(format!("door-stub-{port}.json"));
        let key = vec![0x5a; 32];
        let daemon_id = [0xa5; 16];
        connection_file::write_atomic(
            &file,
            &ConnectionInfo {
                schema: SCHEMA_VERSION,
                wire_version: None,
                endpoints: vec![Endpoint {
                    host: "127.0.0.1".into(),
                    port,
                }],
                key: key.clone(),
                daemon_id,
                pid: std::process::id(),
                daemon_ver: "fake-door".into(),
            },
        )
        .unwrap();
        let opens = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_opens = opens.clone();
        let server_requests = requests.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            authenticate_server(
                &mut stream,
                &key,
                &daemon_id,
                "fake-door",
                broca_test_support::STAND_IN_AUTH_DEADLINE,
            )
            .await
            .unwrap();
            let mut answers: VecDeque<_> = answers.into();
            while let Some(frame) = read_frame(&mut stream).await.unwrap() {
                match (frame.header.channel, frame.header.ty) {
                    (0, FrameType::Ping) => {
                        send(&mut stream, &frame, FrameType::Pong, Vec::new()).await
                    }
                    (0, FrameType::Request) => {
                        let value: Value = serde_json::from_slice(&frame.body).unwrap();
                        let control: ClientControlRequest =
                            serde_json::from_value(value.clone()).unwrap();
                        match control {
                            ClientControlRequest::RouteOpen { .. } => {
                                server_opens.lock().unwrap().push(value);
                                send(
                                    &mut stream,
                                    &frame,
                                    FrameType::Response,
                                    serde_json::to_vec(&ClientControlResponse::RouteOpen {
                                        route_channel: 10,
                                        route_epoch: 1,
                                    })
                                    .unwrap(),
                                )
                                .await;
                            }
                            other => panic!("unexpected fake-door control: {other:?}"),
                        }
                    }
                    (_, FrameType::Request) => {
                        let value: Value = serde_json::from_slice(&frame.body).unwrap();
                        server_requests
                            .lock()
                            .unwrap()
                            .push((Instant::now(), value));
                        let (ty, body) =
                            match answers.pop_front().expect("unexpected extra door request") {
                                Answer::Result(result) => {
                                    (FrameType::Response, json!({"result": result}))
                                }
                                Answer::Envelope(body) => (FrameType::Response, body),
                                Answer::Refuse(error) => {
                                    // ENGRAM's agent_sync::error_body removes the refusal's `error`
                                    // field and uses it as code and message. Only the remaining fields
                                    // become detail, so a bare refusal must not grow a detail object.
                                    let mut fields = error.as_object().unwrap().clone();
                                    let code = fields
                                        .remove("error")
                                        .and_then(|value| value.as_str().map(str::to_owned))
                                        .unwrap_or_else(|| "unavailable".into());
                                    let body = subc_protocol::ErrorBody {
                                        message: code.clone(),
                                        code,
                                        detail: (!fields.is_empty())
                                            .then_some(Value::Object(fields)),
                                    };
                                    (FrameType::Error, serde_json::to_value(body).unwrap())
                                }
                            };
                        send(&mut stream, &frame, ty, serde_json::to_vec(&body).unwrap()).await;
                    }
                    (_, FrameType::Goodbye) => {}
                    (_, other) => panic!("unexpected fake-door frame: {other:?}"),
                }
            }
        });
        Self {
            file,
            server,
            opens,
            requests,
        }
    }

    async fn client(&self, supervised: bool) -> SessionDoorClient {
        let consumer = supervised.then(|| ConsumerIdentity {
            module_id: "broca".into(),
            launch_nonce: "fake-broca-launch".into(),
        });
        let conn = SubcConnection::connect_with_consumer_identity(&self.file, consumer)
            .await
            .unwrap();
        SessionDoorClient::new(
            Arc::new(conn),
            BindIdentity {
                harness: "broca".into(),
                project_root: PathBuf::from("/project"),
                session: "bound-session".into(),
            },
        )
    }

    fn sent(&self) -> Vec<Value> {
        assert!(
            !self.server.is_finished(),
            "fake-door server unexpectedly stopped"
        );
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|(_, value)| value.clone())
            .collect()
    }
}

async fn send(stream: &mut TcpStream, request: &Frame, ty: FrameType, body: Vec<u8>) {
    let frame = Frame::build(
        ty,
        Flags::new(false, Priority::Interactive, false),
        request.header.channel,
        request.header.epoch,
        request.header.corr,
        body,
    )
    .unwrap();
    write_frame(stream, &frame).await.unwrap();
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn cover(durable: u64, cloud: u64) -> Answer {
    Answer::Result(json!({"durableOffset": durable, "cloudOffset": cloud}))
}
fn read_reply() -> Value {
    json!({"bytes": "616263", "fromOffset": 0, "endOffset": 3, "streamEnd": 3, "digest": DIGEST})
}

#[tokio::test]
async fn cover_opens_only_the_attested_internal_service_without_a_stream() {
    let door = FakeDoor::start(vec![cover(7, 4)]).await;
    let client = door.client(true).await;
    let result = client.cover(SESSION_REF, deadline()).await.unwrap();
    assert_eq!((result.durable_offset, result.cloud_offset), (7, 4));
    assert_eq!(
        door.sent(),
        vec![json!({"method": "session.cover", "params": {"sessionRef": SESSION_REF}})]
    );
    let opens = door.opens.lock().unwrap();
    assert_eq!(opens.len(), 1);
    assert_eq!(
        opens[0]["target"],
        json!({"kind": "internal_service", "module_id": "engram", "service_id": "session-door"})
    );
    assert_eq!(
        opens[0]["consumer_identity"],
        json!({"module_id": "broca", "launch_nonce": "fake-broca-launch"})
    );
    assert!(opens[0].get("scope").is_none_or(Value::is_null));
}

#[tokio::test]
async fn direct_consumers_are_refused_before_opening_or_reading() {
    let door = FakeDoor::start(vec![]).await;
    let client = door.client(false).await;
    assert!(client
        .cover(SESSION_REF, deadline())
        .await
        .unwrap_err()
        .message
        .contains("supervised"));
    assert!(door.opens.lock().unwrap().is_empty());
    assert!(door.sent().is_empty());
}

#[tokio::test]
async fn admit_chunks_at_the_broca_cap_and_resumes_from_cover() {
    // Literal lengths make this test independent of the client's cap constant.
    let wal: Vec<u8> = (0..3_145_702).map(|index| (index % 256) as u8).collect();
    let door = FakeDoor::start(vec![
        cover(2, 1),
        cover(3_145_699, 5),
        cover(3_145_702, 5),
        cover(3_145_702, 5),
    ])
    .await;
    let client = door.client(true).await;
    let result = client.admit(SESSION_REF, &wal, deadline()).await.unwrap();
    assert_eq!(result.durable_offset, wal.len() as u64);
    assert_eq!(
        result.cloud_offset, 5,
        "durable admission is not cloud coverage"
    );
    client.admit(SESSION_REF, &wal, deadline()).await.unwrap();
    let sent = door.sent();
    assert_eq!(
        sent.len(),
        4,
        "a repeat only covers, never resends durable bytes"
    );
    assert_eq!(sent[1]["params"]["fromOffset"], 2);
    assert_eq!(sent[2]["params"]["fromOffset"], 3_145_699);
    for (request, expected) in [
        (&sent[1], &wal[2..3_145_699]),
        (&sent[2], &wal[3_145_699..]),
    ] {
        assert_eq!(request["method"], "session.admit");
        assert_eq!(request["params"].as_object().unwrap().len(), 3);
        assert_eq!(request["params"]["sessionRef"], SESSION_REF);
        let hex = request["params"]["bytes"].as_str().unwrap();
        assert!(hex.len() <= 2 * 3_145_697);
        let decoded: Vec<_> = hex
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        assert_eq!(decoded, expected);
    }
    assert_eq!(MAX_ADMIT_BYTES, 3_145_697);
}

#[tokio::test]
async fn offset_mismatch_uses_expected_offset_and_a_fresh_cover() {
    let door = FakeDoor::start(vec![
        cover(0, 0),
        Answer::Refuse(json!({"error": "offset_mismatch", "expectedOffset": 1})),
        cover(2, 0),
        cover(3, 0),
    ])
    .await;
    let client = door.client(true).await;
    client.admit(SESSION_REF, b"abc", deadline()).await.unwrap();
    assert_eq!(
        door.sent(),
        vec![
            json!({"method": "session.cover", "params": {"sessionRef": SESSION_REF}}),
            json!({"method": "session.admit", "params": {"sessionRef": SESSION_REF, "fromOffset": 0, "bytes": "616263"}}),
            json!({"method": "session.cover", "params": {"sessionRef": SESSION_REF}}),
            json!({"method": "session.admit", "params": {"sessionRef": SESSION_REF, "fromOffset": 2, "bytes": "63"}}),
        ]
    );
}

#[tokio::test]
async fn offset_mismatch_without_detail_is_unavailable_and_never_guessed() {
    let door = FakeDoor::start(vec![
        cover(0, 0),
        Answer::Refuse(json!({"error": "offset_mismatch"})),
    ])
    .await;
    let client = door.client(true).await;
    let error = client
        .admit(SESSION_REF, b"abc", deadline())
        .await
        .unwrap_err();
    assert_eq!(error.door_code.as_deref(), Some("offset_mismatch"));
    assert_eq!(door.sent().len(), 2);
}

#[tokio::test]
async fn offset_mismatch_rejects_a_cover_behind_expected_offset() {
    let door = FakeDoor::start(vec![
        cover(0, 0),
        Answer::Refuse(json!({"error": "offset_mismatch", "expectedOffset": 2})),
        cover(1, 0),
    ])
    .await;
    let client = door.client(true).await;
    let error = client
        .admit(SESSION_REF, b"abc", deadline())
        .await
        .unwrap_err();
    assert_eq!(error.door_code.as_deref(), Some("offset_mismatch"));
    assert_eq!(door.sent().len(), 3);
}

#[tokio::test]
async fn lease_refusals_retry_with_increasing_backoff() {
    let door = FakeDoor::start(vec![
        cover(0, 0),
        Answer::Refuse(json!({"error": "lease_unavailable"})),
        Answer::Refuse(json!({"error": "lease_acquiring"})),
        cover(3, 0),
    ])
    .await;
    let client = door.client(true).await;
    client.admit(SESSION_REF, b"abc", deadline()).await.unwrap();
    let requests = door.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[1].1, requests[2].1);
    assert_eq!(requests[2].1, requests[3].1);
    assert!(requests[2].0 - requests[1].0 >= Duration::from_millis(50));
    assert!(requests[3].0 - requests[2].0 >= Duration::from_millis(100));
}

#[tokio::test]
async fn retryable_cover_and_read_refusals_are_retried() {
    let door = FakeDoor::start(vec![
        Answer::Refuse(json!({"error": "lease_acquiring"})),
        cover(3, 1),
        Answer::Refuse(json!({"error": "lease_unavailable"})),
        Answer::Result(read_reply()),
    ])
    .await;
    let client = door.client(true).await;
    client.cover(SESSION_REF, deadline()).await.unwrap();
    let read = client
        .read(SESSION_REF, 0, 3, CUT_ID, deadline())
        .await
        .unwrap();
    assert_eq!(read.bytes, b"abc");
    let sent = door.sent();
    assert_eq!(sent.len(), 4);
    assert_eq!(sent[0], sent[1]);
    assert_eq!(sent[2], sent[3]);
}

#[tokio::test]
async fn lease_retry_deadline_is_bounded_and_names_the_code() {
    let door = FakeDoor::start(vec![Answer::Refuse(json!({"error": "lease_unavailable"}))]).await;
    let client = door.client(true).await;
    let error = timeout(
        Duration::from_secs(2),
        client.cover(SESSION_REF, Instant::now() + Duration::from_millis(40)),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.door_code.as_deref(), Some("lease_unavailable"));
    assert!(error.message.contains("deadline"));
    assert_eq!(door.sent().len(), 1);
}

#[tokio::test]
async fn terminal_door_refusals_are_unavailable_with_the_code_without_retry() {
    for code in [
        "record_too_large",
        "lease_held_elsewhere",
        "lease_not_writable",
        "cut_pending_handoff",
        "container_handed_off",
        "future_refusal",
    ] {
        let door = FakeDoor::start(vec![cover(0, 0), Answer::Refuse(json!({"error": code}))]).await;
        let client = door.client(true).await;
        let error = client
            .admit(SESSION_REF, b"abc", deadline())
            .await
            .unwrap_err();
        assert_eq!(error.door_code.as_deref(), Some(code));
        assert!(error.to_string().contains("unavailable"));
        assert!(error.to_string().contains(code));
        assert_eq!(door.sent().len(), 2);
    }
}

#[tokio::test]
async fn cut_bound_reads_use_the_shipped_digest_and_exact_page_fields() {
    let door = FakeDoor::start(vec![Answer::Result(json!({"bytes": "61", "fromOffset": 0, "endOffset": 1, "streamEnd": 3, "digest": DIGEST})), Answer::Result(json!({"bytes": "6263", "fromOffset": 1, "endOffset": 3, "streamEnd": 3, "digest": DIGEST}))]).await;
    let client = door.client(true).await;
    let first = client
        .read(SESSION_REF, 0, 1, CUT_ID, deadline())
        .await
        .unwrap();
    let last = client
        .read(SESSION_REF, first.end_offset, 2, CUT_ID, deadline())
        .await
        .unwrap();
    assert_eq!([first.bytes, last.bytes].concat(), b"abc");
    assert_eq!(first.digest, DIGEST);
    assert_eq!(last.digest, DIGEST);
    assert_eq!(last.end_offset, last.stream_end);
    assert_eq!(
        door.sent(),
        vec![
            json!({"method": "session.read", "params": {"sessionRef": SESSION_REF, "fromOffset": 0, "limit": 1, "cutId": CUT_ID}}),
            json!({"method": "session.read", "params": {"sessionRef": SESSION_REF, "fromOffset": 1, "limit": 2, "cutId": CUT_ID}}),
        ]
    );
}

#[tokio::test]
async fn cut_bound_read_without_digest_is_unavailable_not_importable() {
    let mut reply = read_reply();
    reply.as_object_mut().unwrap().remove("digest");
    let door = FakeDoor::start(vec![Answer::Result(reply)]).await;
    let client = door.client(true).await;
    let error = client
        .read(SESSION_REF, 0, 3, CUT_ID, deadline())
        .await
        .unwrap_err();
    assert!(error.message.contains("digest"));
    assert_eq!(door.sent().len(), 1);
}

#[tokio::test]
async fn malformed_read_ranges_hex_and_digests_are_unavailable() {
    for (field, value) in [
        ("fromOffset", json!(1)),
        ("endOffset", json!(2)),
        ("streamEnd", json!(2)),
        ("bytes", json!("abc")),
        ("bytes", json!("zzzzzz")),
        ("bytes", json!("6162")),
        ("digest", json!("short")),
        ("digest", json!("g".repeat(64))),
    ] {
        let mut reply = read_reply();
        reply[field] = value;
        let door = FakeDoor::start(vec![Answer::Result(reply)]).await;
        let client = door.client(true).await;
        client
            .read(SESSION_REF, 0, 3, CUT_ID, deadline())
            .await
            .unwrap_err();
        assert_eq!(door.sent().len(), 1);
    }
}

#[tokio::test]
async fn cut_not_ready_and_offset_out_of_range_preserve_the_door_code() {
    for error in [
        json!({"error": "not_found"}),
        json!({"error": "offset_out_of_range", "streamEnd": 3}),
    ] {
        let code = error["error"].as_str().unwrap().to_owned();
        let door = FakeDoor::start(vec![Answer::Refuse(error)]).await;
        let client = door.client(true).await;
        let error = client
            .read(SESSION_REF, 4, 3, CUT_ID, deadline())
            .await
            .unwrap_err();
        assert_eq!(error.door_code, Some(code));
        assert_eq!(door.sent().len(), 1);
    }
}

#[tokio::test]
async fn cover_cannot_claim_bytes_past_the_frozen_wal() {
    let door = FakeDoor::start(vec![cover(4, 0)]).await;
    let client = door.client(true).await;
    let error = client
        .admit(SESSION_REF, b"abc", deadline())
        .await
        .unwrap_err();
    assert!(error.message.contains("endOffset"));
    assert_eq!(door.sent().len(), 1);
}

#[tokio::test]
async fn malformed_coverage_and_result_envelopes_are_unavailable() {
    for answer in [
        cover(1, 2),
        Answer::Result(json!({"cloudOffset": 0})),
        Answer::Envelope(json!({"durableOffset": 0, "cloudOffset": 0})),
    ] {
        let door = FakeDoor::start(vec![answer]).await;
        let client = door.client(true).await;
        client.cover(SESSION_REF, deadline()).await.unwrap_err();
        assert_eq!(door.sent().len(), 1);
    }
}
