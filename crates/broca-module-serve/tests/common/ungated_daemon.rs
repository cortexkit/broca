//! A stand-in daemon without the settings gate, for tests that boot the real binary only as far
//! as its settings or its storage. It writes a connection file, accepts every connection,
//! answers broca's HELLO with an acknowledgement that offers no capabilities (so broca reads its
//! settings file, as it does under a daemon that doesn't manage settings), answers pings, and
//! refuses every request. It serves no modules or providers, so no application work can run.

use std::path::Path;

/// Writes `connection_file` naming a fresh listener and serves every connection made to it.
/// Abort the returned task to stop listening.
pub async fn spawn(connection_file: &Path) -> tokio::task::JoinHandle<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let key = vec![0x51; 32];
    let daemon_id = [0x15; 16];
    subc_transport::connection_file::write_atomic(
        connection_file,
        &subc_transport::connection_file::ConnectionInfo {
            schema: subc_transport::connection_file::SCHEMA_VERSION,
            wire_version: None,
            endpoints: vec![subc_transport::connection_file::Endpoint {
                host: "127.0.0.1".into(),
                port: listener.local_addr().unwrap().port(),
            }],
            key: key.clone(),
            daemon_id,
            pid: std::process::id(),
            daemon_ver: "ungated-stand-in".into(),
        },
    )
    .unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(serve(stream, key.clone(), daemon_id));
        }
    })
}

async fn serve(mut stream: tokio::net::TcpStream, key: Vec<u8>, daemon_id: [u8; 16]) {
    use subc_protocol::{Flags, Frame, FrameType, ModuleHelloAckBody, Priority, PROTOCOL_VERSION};
    if subc_transport::authenticate_server(
        &mut stream,
        &key,
        &daemon_id,
        "ungated-stand-in",
        broca_test_support::STAND_IN_AUTH_DEADLINE,
    )
    .await
    .is_err()
    {
        return;
    }
    while let Ok(Some(frame)) = subc_transport::read_frame(&mut stream).await {
        let (ty, body) = match frame.header.ty {
            FrameType::Hello => (
                FrameType::HelloAck,
                serde_json::to_vec(&ModuleHelloAckBody {
                    negotiated_ver: PROTOCOL_VERSION,
                    subc_ops: vec![],
                    subc_capabilities: vec![],
                    storage: None,
                    machine_id: None,
                })
                .unwrap(),
            ),
            FrameType::Request => (
                FrameType::Error,
                br#"{"code":"unknown_method","message":"no services in the stand-in"}"#.to_vec(),
            ),
            FrameType::Ping => (FrameType::Pong, vec![]),
            _ => continue,
        };
        let reply = Frame::build(
            ty,
            Flags::new(false, Priority::Passive, false),
            frame.header.channel,
            frame.header.epoch,
            frame.header.corr,
            body,
        )
        .unwrap();
        if subc_transport::write_frame(&mut stream, &reply)
            .await
            .is_err()
        {
            break;
        }
    }
}
