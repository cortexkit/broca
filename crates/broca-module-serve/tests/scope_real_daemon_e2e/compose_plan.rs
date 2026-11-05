use std::collections::HashMap;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subc_client_rs::{BindDecision, HandlerOutcome, ModuleHandler, RequestCtx, RouteBindRequest};
use subc_protocol::manifest::{
    CapabilityDeclarations, Concurrency, ExecutionMode, IdentityScope, ModuleManifest,
    ProviderRole, Tool,
};
use tokio::io::AsyncWriteExt;

use super::{Rig, OWNER, SCOPE_EPOCH, SCOPE_REF};

#[derive(Default)]
struct Seen {
    binds: Mutex<HashMap<(u16, u32), Value>>,
    catalogs: Mutex<Vec<(Value, Value)>>,
}

struct Catalog(Arc<Seen>);

#[async_trait::async_trait]
impl ModuleHandler for Catalog {
    async fn on_bind(&self, request: &RouteBindRequest) -> BindDecision {
        self.0.binds.lock().unwrap().insert(
            (request.handle.channel, request.handle.epoch),
            json!({"scope": request.scope, "roles": request.role_versions}),
        );
        BindDecision::accept()
    }

    async fn handle(&self, ctx: RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(request["name"], "tool.catalog");
        let route = ctx.route_handle();
        let bind = self.0.binds.lock().unwrap()[&(route.channel, route.epoch)].clone();
        self.0.catalogs.lock().unwrap().push((bind, request));
        let schema = json!({"type": "object"});
        HandlerOutcome::Response(serde_json::to_vec(&json!({
            "generation": "1", "catalog_digest": "scoped-catalog",
            "tools": [{
                "name": "read", "description": "Read a file", "input_schema": schema,
                "schema_digest": cortexkit_role_tool_provider::catalog::schema_digest(&schema).unwrap(),
                "semantics": 1, "capabilities": ["code.read/v1"]
            }]
        })).unwrap())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compose_plan_direct_carrier_fetches_catalog_on_scoped_bind() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    let checkout = rig.dir.join("compose-checkout");
    std::fs::create_dir_all(&checkout).unwrap();
    let checkout = checkout.canonicalize().unwrap();
    let proof = [0xab; 32];
    rig.sync_scopes(
        json!([{
            "ref": SCOPE_REF, "scope_epoch": SCOPE_EPOCH, "kind": "head", "workdir": checkout,
            "attributes": {}, "bind_proof_sha256": format!("{:x}", Sha256::digest(proof)),
            "carriers": [{"principal": {"kind": "direct"}, "targets": ["aft"]}]
        }]),
        1,
    )
    .await;

    let manifest = ModuleManifest::builder("aft", env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ToolProvider {
            tools: vec![Tool {
                name: "read".into(),
                description: Some("Read a file".into()),
                execution_mode: ExecutionMode::Pure,
                schema: json!({"type": "object"}),
            }],
            identity_scope: vec![IdentityScope::Project, IdentityScope::Session],
            concurrency: Concurrency::ModuleManaged,
            emits_push: false,
            sub_supervises: false,
        }])
        // Like the real AFT: the daemon omits scope.workdir unless the recipient opts in, and
        // broca declares the tool-provider role on its routes only to a module that advertises
        // `tool-provider/v1`.
        .capabilities(Some(CapabilityDeclarations {
            provides: vec![
                subc_protocol::scope::SCOPE_WORKDIR_CAPABILITY.to_owned(),
                "tool-provider/v1".to_owned(),
            ],
            requires: vec![],
            must_never_reach: vec![],
        }))
        .build();
    let seen = Arc::new(Seen::default());
    let (_, serve) =
        subc_client_rs::serve_with_handle(&rig.connection_file, manifest, Catalog(seen.clone()))
            .await
            .unwrap();
    let serving = tokio::spawn(serve);
    let items = rig.dir.join("compose-items.json");
    std::fs::write(
        &items,
        serde_json::to_vec(&json!({
            "tool_items": [{"provider": "aft", "preset": "reader", "params": {}}],
            "system_text_items": [], "step_transform_items": []
        }))
        .unwrap(),
    )
    .unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_broca-session"))
        .arg("--subc")
        .arg(&rig.connection_file)
        .arg("--project-root")
        .arg(&checkout)
        .args(["--harness", "runner", "--session", "compose-scoped"])
        .arg("compose-plan")
        .arg("--items")
        .arg(&items)
        .arg("--scope")
        .arg(format!("{OWNER}/{SCOPE_REF}/{SCOPE_EPOCH}"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin
        .write_all(format!("{}\n", "ab".repeat(32)).as_bytes())
        .await
        .unwrap();
    drop(stdin);
    let output = child.wait_with_output().await.unwrap();
    assert!(
        output.status.success(),
        "compose-plan failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let plan: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        plan["composition"]["providers"][0]["tools"],
        json!([{"name": "read", "capabilities": ["code.read/v1"]}])
    );
    let catalogs = seen.catalogs.lock().unwrap();
    assert_eq!(catalogs.len(), 1);
    let (bind, request) = &catalogs[0];
    assert_eq!(
        bind["scope"]["owner"],
        json!({"kind": "reserved", "module_id": OWNER})
    );
    assert_eq!(bind["scope"]["ref"], SCOPE_REF);
    assert_eq!(bind["scope"]["scope_epoch"], SCOPE_EPOCH);
    assert_eq!(bind["scope"]["workdir"], json!(checkout));
    assert_eq!(bind["roles"], json!({"tool-provider": "v1"}));
    assert_eq!(
        request["arguments"],
        json!({"preset": "reader", "params": {}})
    );
    assert!(request.get("scope").is_none());
    serving.abort();
}
