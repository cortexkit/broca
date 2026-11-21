//! How a company gateway's key reaches a request: in the header and scheme its vault record
//! names, beside the gateway's fixed headers, at admission and after a refresh.

use super::*;
use broca_protocol::gateway::{GatewayEndpoint, GatewayScheme};
use broca_protocol::provider::CredentialRefresh;
use broca_provider::credential_refresh::{RefreshedSecret, RejectedRefresh, SecretRefresher};

fn endpoint() -> GatewayEndpoint {
    GatewayEndpoint {
        base_url: "https://gateway.example".into(),
        auth_header: "x-gateway-key".into(),
        auth_scheme: GatewayScheme::Raw,
        headers: vec![("x-team".into(), "engineering".into())],
    }
}

/// A vault that hands out one fresh key on a refresh.
struct FreshKey;

impl SecretRefresher for FreshKey {
    fn refresh_rejected_secret(
        &self,
        _status: u16,
    ) -> broca_provider::credential_refresh::RefreshFuture<'_, RefreshedSecret> {
        Box::pin(async {
            RejectedRefresh::Token(RefreshedSecret {
                secret: "fresh-key".into(),
                evidence: CredentialRefresh {
                    vault_op: "get_scoped".into(),
                    min_ttl_ms: None,
                    force_refresh: false,
                    record_version: Some(2),
                    reason: None,
                },
                account_ref: None,
                hosts: Default::default(),
            })
        })
    }
}

#[test]
fn a_gateway_key_goes_in_its_gateways_header_never_the_providers() {
    let gateway = AuthMode::Gateway {
        key: "gateway-key".into(),
        endpoint: endpoint(),
    };
    let anthropic = AuthKind::ApiKeyHeader {
        header: "x-api-key".into(),
    };
    let (handle, _, method) =
        resolve_auth(&gateway, &anthropic, "anthropic", Some(AuthMethod::Gateway)).unwrap();
    assert_eq!(method, Some(AuthMethod::Gateway));
    let AuthHandle::Headers(headers) = handle else {
        panic!("a gateway key is sent in its gateway's headers: {handle:?}");
    };
    assert_eq!(
        headers,
        [
            ("x-gateway-key".to_owned(), "gateway-key".to_owned()),
            ("x-team".to_owned(), "engineering".to_owned()),
        ]
    );
    let empty = AuthMode::Gateway {
        key: String::new(),
        endpoint: endpoint(),
    };
    assert!(resolve_auth(&empty, &anthropic, "anthropic", Some(AuthMethod::Gateway)).is_err());
}

#[tokio::test]
async fn a_refreshed_gateway_key_goes_in_the_same_headers() {
    let opts = ResolveOptions {
        secret_refresher: Some(SharedSecretRefresher(Arc::new(FreshKey))),
        ..Default::default()
    };
    let gateway = AuthMode::Gateway {
        key: "gateway-key".into(),
        endpoint: endpoint(),
    };
    let refresher = secret_refresher_for(
        &gateway,
        &opts,
        &AuthKind::ApiKeyHeader {
            header: "x-api-key".into(),
        },
        "anthropic",
        Some(AuthMethod::Gateway),
    )
    .expect("a gateway key can be refreshed");
    let RejectedRefresh::Token(refreshed) = refresher.refresh_rejected(401).await else {
        panic!("the fresh key is served");
    };
    let AuthHandle::Headers(headers) = refreshed.auth else {
        panic!("the fresh key is sent in the gateway's headers");
    };
    assert_eq!(
        headers,
        [
            ("x-gateway-key".to_owned(), "fresh-key".to_owned()),
            ("x-team".to_owned(), "engineering".to_owned()),
        ]
    );
}
