use std::path::Path;
use std::sync::Arc;
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

    drop(client_writer);
    drop(client_reader);
    let result = tokio::time::timeout(Duration::from_secs(2), serve)
        .await
        .expect("serve task exits");
    result.expect("serve task joins").expect("serve loop exits");
    let _ = std::fs::remove_dir_all(dir);
}
