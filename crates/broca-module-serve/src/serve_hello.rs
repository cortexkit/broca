//! Registering broca with the daemon: HELLO and its acknowledgement.

use super::*;

/// broca's manifest. The classify tool's input schema takes image references when
/// `image_references` is on (`llm_tools.image_references`).
pub fn hello_manifest(
    module_id: &str,
    consumer_routes: &crate::consumer_routes::ConsumerRoutes,
    image_references: bool,
) -> subc_protocol::manifest::ModuleManifest {
    let mut manifest = crate::manifest::manifest_configured(module_id, consumer_routes);
    if image_references {
        for role in &mut manifest.provides {
            if let subc_protocol::manifest::ProviderRole::ToolProvider { tools, .. } = role {
                for tool in tools {
                    if tool.name == crate::llm_tools::CLASSIFY_TOOL_NAME {
                        tool.schema =
                            cortexkit_llm_tools::classify::input_schema_with_image_references();
                    }
                }
            }
        }
    }
    manifest
}

/// Registers broca unready, before anything is built from its settings, and returns the
/// daemon's acknowledgement (`settings::gate`). HELLO fixes some of what it declares for the
/// connection's life (the role kinds, the consumed modules, the capabilities), so the manifest
/// carries everything the settings could later name: `consumer_routes` must already name the
/// bus. The classify tool's schema and readiness follow in `catalog.update` once broca is built.
pub async fn register_unready<R, W>(
    reader: &mut R,
    writer: &mut W,
    module_id: &str,
    consumer_routes: &crate::consumer_routes::ConsumerRoutes,
) -> Result<ModuleHelloAckBody, ServeIoError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut manifest = hello_manifest(module_id, consumer_routes, false);
    manifest.ready = Some(false);
    let body = serde_json::to_vec(&ModuleHelloBody {
        manifest,
        protocol_ver: PROTOCOL_VERSION,
        control_ops: Some(vec![
            MODULE_CONTROL_OP_HEALTH_CHECK.to_string(),
            subc_protocol::config::CONFIG_REVIEW_OP.to_string(),
        ]),
        launch_nonce: hello_launch_nonce()?,
    })
    .map_err(|e| ServeIoError(e.to_string()))?;
    let frame = Frame::build(FrameType::Hello, control_flags(), 0, 0, HELLO_CORR, body)
        .map_err(|e| ServeIoError(e.to_string()))?;
    write_frame(writer, &frame)
        .await
        .map_err(|e| ServeIoError(e.to_string()))?;
    writer
        .flush()
        .await
        .map_err(|e| ServeIoError(e.to_string()))?;
    expect_hello_ack(reader).await
}

pub(super) async fn send_hello(
    writer: &EgressSender<Frame>,
    module_id: &str,
    consumer_routes: &crate::consumer_routes::ConsumerRoutes,
    image_references: bool,
) -> Result<(), ServeIoError> {
    let manifest = hello_manifest(module_id, consumer_routes, image_references);
    let body = serde_json::to_vec(&ModuleHelloBody {
        manifest,
        protocol_ver: PROTOCOL_VERSION,
        control_ops: Some(vec![MODULE_CONTROL_OP_HEALTH_CHECK.to_string()]),
        launch_nonce: hello_launch_nonce()?,
    })
    .map_err(|e| ServeIoError(e.to_string()))?;
    let frame = Frame::build(FrameType::Hello, control_flags(), 0, 0, HELLO_CORR, body)
        .map_err(|e| ServeIoError(e.to_string()))?;
    send(writer, frame).await
}

/// The launch nonce HELLO presents, from the process's one nonce accessor.
///
/// `main` has already made the first read, so this returns the cached value and never touches
/// the descriptor again. A named descriptor that could not be read fails HELLO with the
/// accessor's own message; it never falls back to the environment copy, because that would hide
/// a broken handoff and let a process that inherited only the variable register as broca.
fn hello_launch_nonce() -> Result<Option<String>, ServeIoError> {
    match subc_os::launch_nonce() {
        Ok(nonce) => Ok(nonce.map(|nonce| nonce.value().to_string())),
        Err(error) => Err(ServeIoError(format!("launch nonce: {error}"))),
    }
}

pub(super) async fn expect_hello_ack<R>(reader: &mut R) -> Result<ModuleHelloAckBody, ServeIoError>
where
    R: AsyncRead + Unpin,
{
    let frame = read_frame(reader)
        .await
        .map_err(|e| ServeIoError(e.to_string()))?
        .ok_or_else(|| ServeIoError("connection closed before HELLO_ACK".into()))?;
    match frame.header.ty {
        FrameType::HelloAck => {
            serde_json::from_slice(&frame.body).map_err(|e| ServeIoError(e.to_string()))
        }
        FrameType::Error => {
            let body: ErrorBody =
                serde_json::from_slice(&frame.body).map_err(|e| ServeIoError(e.to_string()))?;
            Err(ServeIoError(format!(
                "subc rejected HELLO: {} — {}",
                body.code, body.message
            )))
        }
        ty => Err(ServeIoError(format!(
            "unexpected frame {ty:?} awaiting HELLO_ACK"
        ))),
    }
}
