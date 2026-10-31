//! A real scoped send must carry the daemon's workdir through to compaction Setup.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use subc_client_rs::{BindDecision, HandlerOutcome, ModuleHandler, RequestCtx, RouteBindRequest};

use super::*;

const PROVIDER: &str = "scoped-compaction";

#[derive(Default)]
struct Seen {
    binds: Mutex<HashMap<(u16, u32), Value>>,
    setups: Mutex<Vec<(Value, Value)>>,
}

struct Compactor(Arc<Seen>);

#[async_trait::async_trait]
impl ModuleHandler for Compactor {
    async fn on_bind(&self, request: &RouteBindRequest) -> BindDecision {
        self.0.binds.lock().unwrap().insert(
            (request.handle.channel, request.handle.epoch),
            json!({"identity":request.identity, "scope":request.scope, "roles":request.role_versions}),
        );
        BindDecision::accept()
    }

    async fn handle(&self, ctx: RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap();
        let answer = match request["method"].as_str() {
            Some("role.describe") => {
                let fixture: Value = serde_json::from_str(include_str!(
                    "../../../broca-subc/tests/fixtures/compaction-provider-v1/role-describe.json"
                ))
                .unwrap();
                fixture["canonical"][0]["answer"].clone()
            }
            Some("compaction.setup") => {
                let route = ctx.route_handle();
                let bind = self.0.binds.lock().unwrap()[&(route.channel, route.epoch)].clone();
                self.0.setups.lock().unwrap().push((bind, request.clone()));
                json!({"answer":"ready", "request_id":request["params"]["request_id"],
                    "initial":{"compaction_id":"empty", "version":1,
                    "range":{"from":0,"to":0}, "replacement":[]}})
            }
            Some("compaction.ride") => json!({"answer":"nothing"}),
            _ => panic!("unexpected compaction request: {request}"),
        };
        HandlerOutcome::Response(serde_json::to_vec(&answer).unwrap())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scoped_worker_send_stamps_compaction_setup_with_scope_workdir() {
    use subc_protocol::manifest::{Concurrency, ModuleManifest, ProviderRole};

    let Some(rig) = Rig::start().await else {
        return;
    };
    let checkout = rig.dir.join("worker-checkout");
    std::fs::create_dir_all(&checkout).unwrap();
    let checkout = checkout.canonicalize().unwrap();
    rig.sync_scopes(
        json!([{
            "ref":SCOPE_REF, "scope_epoch":SCOPE_EPOCH, "kind":"head", "workdir":checkout,
            "attributes":{}, "carriers":[{"principal":{"kind":"reserved", "module_id":"broca"}}]
        }]),
        1,
    )
    .await;
    let seen = Arc::new(Seen::default());
    let (_, serve) = subc_client_rs::serve_with_handle(
        &rig.connection_file,
        ModuleManifest::builder(PROVIDER, env!("CARGO_PKG_VERSION"))
            .provides(vec![ProviderRole::ToolProvider {
                tools: vec![],
                identity_scope: vec![],
                concurrency: Concurrency::ModuleManaged,
                emits_push: false,
                sub_supervises: false,
            }])
            .build(),
        Compactor(seen.clone()),
    )
    .await
    .unwrap();
    let serving = tokio::spawn(serve);
    let owner = rig.connect_as(OWNER, &rig.owner_nonce).await;
    let identity = rig.session("scoped-worker");
    let route = open_scoped(&owner, &identity).await.unwrap();
    let composition = json!({"providers":[{"provider":PROVIDER,"tools":[]}],
        "compaction":{"provider":PROVIDER}, "step_transform_items":[]});
    let reply = owner.route_request(&route, serde_json::to_vec(&json!({
        "method":"session.send", "params":{
            "prompt":"hi", "send_id":"scoped-compaction", "model":{"provider":"mock","model":"mock-model"},
            "plan":{"tool_items":[], "system_text_items":[], "step_transform_items":[],
                "compaction_item":{"provider":PROVIDER,"preset":"worker","params":{}},
                "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
                "composition":composition}
        }
    })).unwrap()).await.unwrap();
    assert_eq!(reply["result"]["state"], "active", "{reply}");
    broca_test_support::wait_until("scoped worker compaction Setup", || async {
        if seen.setups.lock().unwrap().is_empty() {
            Err("no Setup received".into())
        } else {
            Ok(())
        }
    })
    .await;
    let (bind, setup) = seen.setups.lock().unwrap()[0].clone();
    assert_eq!(
        bind["scope"]["owner"],
        json!({"kind":"reserved","module_id":OWNER})
    );
    assert_eq!(bind["scope"]["ref"], SCOPE_REF);
    assert_eq!(bind["scope"]["scope_epoch"], SCOPE_EPOCH);
    assert_eq!(bind["scope"]["workdir"], json!(checkout));
    assert_eq!(bind["roles"], json!({"compaction-provider":"v1"}));
    assert_eq!(bind["identity"]["harness"], identity.harness);
    assert_eq!(setup["params"]["harness"], identity.harness);
    assert_eq!(setup["params"]["session"], identity.session);
    let mut stored = identity.clone();
    stored.project_root = checkout;
    let records =
        broca_wal::replay_file(rig.wal_path(&stored), broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
    assert!(records.iter().any(|framed| matches!(&framed.record,
        broca_wal::WalRecord::RunStarted { scope:Some(scope), .. }
        if scope.scope_ref == SCOPE_REF && scope.scope_epoch == SCOPE_EPOCH)));
    serving.abort();
}
