//! Loopback Workers AI credentials and a deployment key for the real broca process.

use std::path::Path;

use async_trait::async_trait;
use serde_json::{json, Value};
use subc_client_rs::{BindDecision, HandlerOutcome, ModuleHandler, RequestCtx, RouteBindRequest};
use subc_protocol::manifest::{
    Concurrency, ManagementOperation, ManagementOperationKind, ModuleManifest, ProviderRole,
};

pub(super) const ACCOUNT_ID: &str = "extractor-account";
pub(super) const TOKEN: &str = "synthetic-extractor-token";
const CREDENTIAL_ID: &str = "extractor-workers-ai";

pub(super) struct Service(tokio::task::JoinHandle<()>);

impl Drop for Service {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) async fn register(connection: &Path) -> Service {
    let manifest = ModuleManifest::builder("claustrum", env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ManagementSurface {
            operations: ["credential.list_scoped", "credential.get_scoped"]
                .into_iter()
                .map(|name| ManagementOperation {
                    name: name.into(),
                    kind: ManagementOperationKind::Mutate,
                    description: None,
                })
                .collect(),
            config_schema: json!({}),
            observability: vec![],
            identity_scope: vec![],
            concurrency: Concurrency::ModuleManaged,
        }])
        .build();
    let (_, serve) = subc_client_rs::serve_with_handle(connection, manifest, Vault)
        .await
        .expect("register extractor vault fixture");
    Service(tokio::spawn(async move {
        serve.await.expect("extractor vault transport");
    }))
}

struct Vault;

impl Vault {
    fn request(request: &Value) -> Value {
        match request["method"].as_str().unwrap() {
            "credential.list_scoped" => json!({
                "credentials": [{
                    "id": CREDENTIAL_ID,
                    "categories": ["llm-provider"],
                    "type": "apikey",
                    "serves": ["cloudflare-workers-ai"],
                    "provider_ids": ["cloudflare-workers-ai"],
                    "auth_method": "apikey",
                    "state": "active",
                    "record_version": 1,
                    "operations": ["read"],
                }],
                "grants": 1,
                "grant_tuples": [],
                "view": "extractor-fixture",
            }),
            "credential.get_scoped" => {
                let id = request["params"]["credential_id"].as_str().unwrap();
                // Broca needs a hex-encoded 32-byte deployment key to open the rig's storage.
                if id.starts_with("opaque:broca-deployment:") {
                    return json!({
                        "payload": "07".repeat(32).as_bytes(),
                        "expires_at_ms": null,
                        "record_version": 1,
                        "credential_id": id,
                        "account_id": null,
                    });
                }
                assert_eq!(id, CREDENTIAL_ID, "unexpected extractor vault credential");
                json!({
                    "payload": TOKEN.as_bytes(),
                    "expires_at_ms": null,
                    "record_version": 1,
                    "credential_id": id,
                    "account_id": ACCOUNT_ID,
                    "hosts": ["127.0.0.1", "localhost"],
                })
            }
            method => panic!("unsupported extractor vault method {method}"),
        }
    }
}

#[async_trait]
impl ModuleHandler for Vault {
    async fn on_bind(&self, request: &RouteBindRequest) -> BindDecision {
        if matches!(&request.principal, Some(subc_protocol::Principal::Reserved { module_id }) if module_id == "broca")
        {
            BindDecision::accept()
        } else {
            BindDecision::reject("unauthorized", "fixture grants cover broca only")
        }
    }

    async fn handle(&self, _: RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        let result = Self::request(&serde_json::from_slice(&body).unwrap());
        HandlerOutcome::Response(serde_json::to_vec(&json!({"result":result})).unwrap())
    }
}
