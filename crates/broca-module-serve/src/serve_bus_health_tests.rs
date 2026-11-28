use super::*;
use subc_protocol::session::HealthStatus;

pub(crate) async fn probe(
    connector: &NatsConnector,
    vault: &VaultGauge,
) -> (HealthStatus, Option<String>) {
    let registry = LineageRegistry::new(Arc::new(test_openers::RefusingOpener));
    let (writer, mut reader) = broca_diagnostics::channel(1, Default::default());
    let request = Frame::build(
        FrameType::Request,
        control_flags(),
        0,
        0,
        1,
        serde_json::to_vec(&ModuleControlRequest::HealthCheck {}).unwrap(),
    )
    .unwrap();
    handle_control_request(
        request,
        &writer,
        &mut Routes::new(false),
        &registry,
        vault,
        Some(connector),
        None,
    )
    .await
    .unwrap();
    let response = reader.recv().await.unwrap();
    let ModuleControlResponse::HealthCheck { status, detail, .. } =
        serde_json::from_slice(&response.body).unwrap()
    else {
        panic!("expected a health response");
    };
    (status, detail)
}
