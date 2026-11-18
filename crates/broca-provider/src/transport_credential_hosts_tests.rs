// The fixture listens on the local machine, which a rig build lets through for any record
// (`test-loopback-credentials`), so these tests run only in builds without that feature.
#[cfg(not(feature = "test-loopback-credentials"))]
use super::*;
#[cfg(not(feature = "test-loopback-credentials"))]
use crate::credential_hosts::{CredentialHosts, CREDENTIAL_HOST_NOT_ALLOWED};

/// Run one step of an Anthropic request to `server` with a key, and return the step's error.
#[cfg(not(feature = "test-loopback-credentials"))]
async fn step_error(server: &ScriptedServer, hosts: CredentialHosts) -> ProviderError {
    let mut frozen = crate::families::anthropic_frozen_config("m");
    frozen.base_url = Some(server.url.clone());
    let provider = LiveProvider::new(
        AnthropicMessages,
        frozen,
        AuthHandle::ApiKeyHeader {
            header: "x-api-key".into(),
            value: "key-from-the-vault".into(),
        },
    )
    .with_credential_hosts(hosts);
    let mut rx = provider.start_step(
        StepId(1),
        Arc::new(CallOptions::new(Vec::new())),
        CancellationToken::new(),
        None,
    );
    while let Some(event) = rx.recv().await {
        if let NormalizedEvent::Control(ProviderControl::Error { error, .. }) = event {
            return error;
        }
    }
    panic!("the step ended without an error")
}

#[cfg(not(feature = "test-loopback-credentials"))]
fn listed(hosts: &[&str]) -> CredentialHosts {
    let hosts: Vec<String> = hosts.iter().map(|host| (*host).to_owned()).collect();
    CredentialHosts::from_record(Some(&hosts))
}

/// A key goes only to a host its vault record lists. A step whose URL names another host ends
/// with a permanent `credential_host_not_allowed` error before anything is sent; a listed host
/// gets the request.
#[cfg(not(feature = "test-loopback-credentials"))]
#[tokio::test]
async fn a_key_reaches_only_a_host_its_record_lists() {
    for (hosts, admitted) in [
        (listed(&["api.anthropic.com"]), false),
        (CredentialHosts::from_record(None), false),
        (listed(&["127.0.0.1"]), true),
    ] {
        let server = ScriptedServer::start(vec![ScriptResponse::Http {
            status: 400,
            headers: vec![],
            body: b"invalid",
        }])
        .await;
        let error = step_error(&server, hosts.clone()).await;
        assert_eq!(
            error.provider_code.as_deref() == Some(CREDENTIAL_HOST_NOT_ALLOWED),
            !admitted,
            "{hosts:?}: {error:?}"
        );
        assert_eq!(server.request_bodies().len(), usize::from(admitted), "{hosts:?}");
        if !admitted {
            assert_eq!(error.class, ErrorClass::Permanent);
        }
    }
}
