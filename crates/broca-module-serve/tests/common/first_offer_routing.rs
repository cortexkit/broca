//! A routing module for real-daemon rigs whose tests are about something other than account
//! choice. broca asks routing for the account of every run and refuses the send
//! `routing_unavailable` when no routing module answers, so a rig without one can run nothing.
//! This stand-in answers `route.account_for` with the first account broca offered, accepts
//! every report and observation, and declares no capability. The rig names it in its
//! `broca.jsonc` (`account_selection.routing_module`, [`ID`]): registered from the test process,
//! it has no supervisor launch nonce, so broca would not find it on its own.

use std::path::Path;

use serde_json::{json, Value};

/// The module id the stand-in registers under.
pub const ID: &str = "first-offer-routing";

/// The `account_selection` member a rig's `broca.jsonc` carries to send account decisions here.
pub fn broca_config() -> Value {
    json!({ "routing_module": ID })
}

struct FirstOffer;

#[async_trait::async_trait]
impl subc_client_rs::ModuleHandler for FirstOffer {
    async fn handle(
        &self,
        _ctx: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        let reply = match request["method"].as_str() {
            Some("role.describe") => json!({
                "result": cortexkit_role_routing::describe::RoleDescribe::new(vec![], false),
            }),
            Some("route.account_for") => {
                let params = &request["params"];
                let Some(first) = params["candidates"]["accounts"]
                    .as_array()
                    .and_then(|offered| offered.first())
                else {
                    return subc_client_rs::HandlerOutcome::ErrorWithDetail {
                        code: "no_route".into(),
                        message: "no account was offered".into(),
                        detail: json!({ "code": "no_route", "retry_after_s": null,
                            "decision_id": "acd-none", "exclusions": [] }),
                    };
                };
                let credential_id = first["credential_id"].as_str().unwrap_or_default();
                let provider = params["provider"].as_str().unwrap_or_default();
                json!({ "result": {
                    "admission": { "id": params["admission_id"], "state": "reserved" },
                    "account": {
                        "provider": provider,
                        "account_id": first["account_id"],
                        "credential_id": credential_id,
                        "refresh_adapter": provider,
                        "method": credential_id.split(':').next().unwrap_or_default(),
                    },
                    "pin": { "id": "pin-first-offer", "generation": 1, "mode": "run_frozen" },
                    "reason": { "code": "cold_best", "decision_id": "acd-first-offer",
                        "policy_version": 2 },
                }})
            }
            Some("route.account_observe" | "route.report") => json!({ "result": {} }),
            other => {
                return subc_client_rs::HandlerOutcome::ErrorWithDetail {
                    code: "unknown_method".into(),
                    message: format!("the first-offer routing stand-in serves no {other:?}"),
                    detail: json!({ "code": "unknown_method" }),
                };
            }
        };
        subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&reply).unwrap())
    }
}

/// Registers the stand-in with the daemon behind `connection_file`, serving until the task is
/// dropped or aborted.
pub async fn serve(connection_file: &Path) -> tokio::task::JoinHandle<()> {
    use subc_protocol::manifest::{
        Concurrency, ManagementOperation, ManagementOperationKind, ModuleManifest, ProviderRole,
    };
    let operations = [
        "role.describe",
        "route.account_for",
        "route.account_observe",
        "route.report",
    ]
    .into_iter()
    .map(|name| ManagementOperation {
        name: name.to_string(),
        kind: ManagementOperationKind::Mutate,
        description: None,
    })
    .collect();
    let manifest = ModuleManifest::builder(ID, env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ManagementSurface {
            operations,
            config_schema: json!({}),
            observability: Vec::new(),
            identity_scope: Vec::new(),
            concurrency: Concurrency::ModuleManaged,
        }])
        .build();
    let (_, serve) = subc_client_rs::serve_with_handle(connection_file, manifest, FirstOffer)
        .await
        .unwrap_or_else(|error| panic!("register the first-offer routing stand-in: {error}"));
    tokio::spawn(async move {
        let _ = serve.await;
    })
}
