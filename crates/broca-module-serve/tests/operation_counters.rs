use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_core::FileWalStore;
use broca_module::registry::{ActorHandle, LineageOpener, OpenError, RegistryHandle};
use broca_module::LineageRegistry;
use broca_module_serve::serve::VaultGauge;
use broca_module_serve::serve_connection;
use broca_protocol::BindIdentity;
use subc_protocol::session::{ModuleControlRequest, ModuleControlResponse};
use subc_protocol::{
    BindIdentity as SubcBindIdentity, Flags, Frame, FrameType, ModuleHelloAckBody, Priority,
    RouteTarget, PROTOCOL_VERSION,
};
use subc_transport::{read_frame, write_frame};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing_subscriber::prelude::*;

const MANAGEMENT_OPERATIONS: &[&str] = &[
    "session.send",
    "session.import",
    "session.retract",
    "run.cancel",
    "run.status",
    "run.event.get",
    "session.read",
    "compaction.ready",
    "role.describe",
    "session.resolve",
    "session.head",
    "run.result",
    "session.baseline",
    "session.warm",
    "cap.install",
    "spend.delta",
    "vault.audit",
    "session.subscribe",
];

#[derive(Clone, Debug)]
struct CapturedRequestLog {
    target: &'static str,
    level: tracing::Level,
    operation: String,
    session: String,
    actor_opened: bool,
}

#[derive(Clone)]
struct RequestLogCapture(Arc<Mutex<Vec<CapturedRequestLog>>>);

#[derive(Default)]
struct RequestLogFields {
    operation: Option<String>,
    session: Option<String>,
    actor_opened: Option<bool>,
}

impl tracing::field::Visit for RequestLogFields {
    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "operation" => self.operation = Some(value.to_owned()),
            "session" => self.session = Some(value.to_owned()),
            _ => {}
        }
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if field.name() == "actor_opened" {
            self.actor_opened = Some(value);
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for RequestLogCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() != "requests" {
            return;
        }
        let mut fields = RequestLogFields::default();
        event.record(&mut fields);
        self.0.lock().unwrap().push(CapturedRequestLog {
            target: event.metadata().target(),
            level: *event.metadata().level(),
            operation: fields.operation.expect("request log operation"),
            session: fields.session.expect("request log session"),
            actor_opened: fields.actor_opened.expect("request log actor flag"),
        });
    }
}

struct RejectingOpener;

#[async_trait]
impl LineageOpener for RejectingOpener {
    async fn open(
        &self,
        _key: &BindIdentity,
        _gen: u64,
        _registry: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        Err(OpenError::Open("deliberate test refusal".into()))
    }
}

async fn write_request<W: AsyncWrite + Unpin>(
    writer: &mut W,
    channel: u16,
    epoch: u32,
    corr: u64,
    body: serde_json::Value,
) {
    let frame = Frame::build(
        FrameType::Request,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        serde_json::to_vec(&body).unwrap(),
    )
    .unwrap();
    write_frame(writer, &frame).await.unwrap();
}

async fn read_one<R: AsyncRead + Unpin>(reader: &mut R) -> Frame {
    read_frame(reader).await.unwrap().expect("a frame")
}

async fn health_metrics<R, W>(
    reader: &mut R,
    writer: &mut W,
    corr: u64,
) -> serde_json::Map<String, serde_json::Value>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    write_request(
        writer,
        0,
        0,
        corr,
        serde_json::to_value(ModuleControlRequest::HealthCheck {}).unwrap(),
    )
    .await;
    let response = read_one(reader).await;
    assert_eq!(response.header.ty, FrameType::Response);
    let response: ModuleControlResponse = serde_json::from_slice(&response.body).unwrap();
    let ModuleControlResponse::HealthCheck { metrics, .. } = response else {
        panic!("expected health.check response");
    };
    metrics
        .expect("health.check metrics")
        .as_object()
        .expect("health.check metrics object")
        .clone()
}

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-operation-counters-{}-{tag}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    dir
}

async fn bind_route<W: AsyncWrite + Unpin, R: AsyncRead + Unpin>(
    writer: &mut W,
    reader: &mut R,
    project_root: &Path,
) {
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(project_root.to_path_buf(), "runner", "operation-counters"),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_request(writer, 0, 0, 2, serde_json::to_value(bind).unwrap()).await;
    assert_eq!(read_one(reader).await.header.ty, FrameType::Response);
}

#[tokio::test]
async fn health_check_stream_chunks_session_reads_and_actor_opens_start_at_zero_then_increment() {
    let (request_filter, filter_reload) =
        tracing_subscriber::reload::Layer::new(tracing_subscriber::EnvFilter::new("info"));
    let request_logs = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry()
        .with(request_filter)
        .with(RequestLogCapture(request_logs.clone()));
    tracing::subscriber::set_global_default(subscriber)
        .expect("the isolated counter test owns its tracing subscriber");

    let dir = tmpdir("health");
    let registry = Arc::new(LineageRegistry::new_with_reader(
        Arc::new(RejectingOpener),
        Arc::new(FileWalStore::new(dir.join("wal"))),
        None,
    ));
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut client_reader, mut client_writer) = tokio::io::split(client);
    let (server_reader, server_writer) = tokio::io::split(server);
    let serving_registry = registry.clone();
    let serve = tokio::spawn(async move {
        serve_connection(
            server_reader,
            server_writer,
            "broca",
            serving_registry.clone(),
            serving_registry,
            VaultGauge::not_in_use(),
        )
        .await
    });

    let hello = read_one(&mut client_reader).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
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
    write_frame(&mut client_writer, &ack).await.unwrap();

    let before = health_metrics(&mut client_reader, &mut client_writer, 1).await;
    assert_eq!(
        before["stream_chunks"],
        serde_json::json!(0),
        "stream_chunks must start at zero"
    );
    assert_eq!(
        before["session_reads"],
        serde_json::json!(0),
        "session_reads must start at zero"
    );
    assert_eq!(
        before["actor_opens"],
        serde_json::json!(0),
        "actor_opens must start at zero"
    );
    let mut expected_operation_counts: std::collections::BTreeMap<String, u64> =
        MANAGEMENT_OPERATIONS
            .iter()
            .map(|operation| ((*operation).to_string(), 0))
            .collect();
    assert_eq!(
        before["management_ops"],
        serde_json::json!(expected_operation_counts),
        "every manifest management operation has a zero-valued counter"
    );

    bind_route(&mut client_writer, &mut client_reader, &dir.join("proj")).await;

    let payloads = broca_provider::sse::SseDecoder::default()
        .push(b"data: {\"type\":\"message\"}\n\n")
        .unwrap();
    assert_eq!(payloads.len(), 1);

    write_request(
        &mut client_writer,
        7,
        1,
        3,
        serde_json::json!({"method": "session.read", "params": {}}),
    )
    .await;
    assert_eq!(
        read_one(&mut client_reader).await.header.ty,
        FrameType::Response
    );
    assert!(
        request_logs.lock().unwrap().is_empty(),
        "the request logger stays quiet at INFO"
    );
    filter_reload
        .reload(tracing_subscriber::EnvFilter::new("info,requests=debug"))
        .expect("enable the request component at DEBUG");

    // A cancel always goes through the registry's actor opening, which this rig refuses on
    // purpose. `run.status` no longer does: a never-written session answers `idle` from its
    // (absent) log without opening an actor.
    write_request(
        &mut client_writer,
        7,
        1,
        4,
        serde_json::json!({"method": "run.cancel", "params": {"run_id": "no-such-run"}}),
    )
    .await;
    assert_eq!(
        read_one(&mut client_reader).await.header.ty,
        FrameType::Error
    );
    let request_log = request_logs
        .lock()
        .unwrap()
        .first()
        .cloned()
        .expect("the authorized operation emits a request log");
    assert_eq!(request_log.target, "requests");
    assert_eq!(request_log.level, tracing::Level::DEBUG);
    assert_eq!(request_log.operation, "run.cancel");
    assert_eq!(request_log.session, "operation-counters");
    assert!(!request_log.actor_opened);

    let after = health_metrics(&mut client_reader, &mut client_writer, 5).await;
    assert_eq!(
        after["stream_chunks"],
        serde_json::json!(1),
        "stream_chunks must increment after one decoded payload"
    );
    assert_eq!(
        after["session_reads"],
        serde_json::json!(1),
        "session_reads must increment after one served request"
    );
    assert_eq!(
        after["actor_opens"],
        serde_json::json!(1),
        "actor_opens must increment after one registry opening"
    );
    expected_operation_counts.insert("session.read".to_string(), 1);
    expected_operation_counts.insert("run.cancel".to_string(), 1);
    assert_eq!(
        after["management_ops"],
        serde_json::json!(expected_operation_counts),
        "one session.read and one run.cancel were served"
    );

    for (index, operation) in MANAGEMENT_OPERATIONS
        .iter()
        .filter(|operation| !matches!(**operation, "session.read" | "run.cancel"))
        .enumerate()
    {
        write_request(
            &mut client_writer,
            7,
            1,
            10 + index as u64,
            serde_json::json!({"method": operation, "params": {}}),
        )
        .await;
        let reply = tokio::time::timeout(Duration::from_secs(2), read_one(&mut client_reader))
            .await
            .expect("each management request receives a reply");
        assert!(
            matches!(reply.header.ty, FrameType::Response | FrameType::Error),
            "{operation} returned unexpected frame type {:?}",
            reply.header.ty
        );
        *expected_operation_counts
            .get_mut(*operation)
            .expect("the operation has a zero counter") += 1;
        let counts =
            health_metrics(&mut client_reader, &mut client_writer, 100 + index as u64).await;
        assert_eq!(
            counts["management_ops"],
            serde_json::json!(expected_operation_counts),
            "{operation} increments its counter exactly once"
        );
    }

    drop(client_writer);
    drop(client_reader);
    let result = tokio::time::timeout(Duration::from_secs(2), serve)
        .await
        .expect("serve task exits");
    result.expect("serve task joins").expect("serve loop exits");
    let _ = std::fs::remove_dir_all(dir);
}
